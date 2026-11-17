package yggdrasil.forseti.cli.harness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.cli.recording.W1Form;
import yggdrasil.forseti.cli.replay.CompletionReader;
import yggdrasil.forseti.cli.replay.CutBarrierRecord;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.cli.replay.RecoveryControlStore;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.cli.replay.RunPreparation;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Delete;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Envelope;
import yggdrasil.forseti.core.journal.Pins;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.release.HeadVerifier;
import yggdrasil.forseti.core.release.ReleaseReader;
import yggdrasil.forseti.core.release.Refusal;
import yggdrasil.forseti.core.rules.RuleSet;
import yggdrasil.forseti.core.stage1.QuarantineRecord;
import yggdrasil.forseti.core.stage1.Routing;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.Incidents;
import yggdrasil.forseti.core.stage2.ProgressTracker;
import yggdrasil.forseti.core.stage2.SetCoverage;
import yggdrasil.forseti.core.stage2.Stage2;
import yggdrasil.forseti.replay.kafka.KafkaBatchWriter;
import yggdrasil.forseti.replay.kafka.KafkaCompletionReader;
import yggdrasil.forseti.replay.kafka.KafkaConfig;
import yggdrasil.forseti.replay.kafka.KafkaEvidenceCollector;
import yggdrasil.forseti.replay.kafka.KafkaGenerationStore;
import yggdrasil.forseti.replay.kafka.KafkaJournalDump;
import yggdrasil.forseti.replay.kafka.KafkaRecoveryControl;
import yggdrasil.forseti.replay.kafka.KafkaTopicManager;

/** The step-6 PR 3 test driver: the reference harness's own tasks ({@link StationTask}, {@link Stage2Task}, the two
 *  {@link RejectCollector}s, the {@link PartitionClock} of the Flink W1 form) run as two stages over Kafka instead of in one loop, with
 *  the replay-kafka harness around them, so that the transport, the barrier protocol on real partitions, the stores and the dump are
 *  checked against the oracle before any engine exists. It lives in this package (test scope of {@code forseti-replay-kafka}) only to
 *  reach the harness's package-private tasks; nothing of it is on a main class path.
 *
 *  <p>Harness side ({@code forseti-replay-kafka} main classes): the run's topics ({@link KafkaTopicManager}, settings checked), the
 *  generation registered by the operator CLI itself, {@code generation new --store kafka}, on {@code forseti.generations}
 *  ({@link KafkaGenerationStore}), the run preparation on {@code forseti.recovery-control} ({@link RunPreparation},
 *  {@link KafkaRecoveryControl}), then per batch: {@link KafkaBatchWriter} writes the batch and its barriers, {@link KafkaCompletionReader}
 *  waits for every progress of the cut; batch k+1 is written only after cut k is complete. After the last cut both stages stop,
 *  {@link KafkaJournalDump} writes the journals in recording offsets, and the harness writes {@code cuts.json} (from the progress records
 *  the completion reader saw, and the batch manifest's offsets), {@code run.json}, {@code offset-map.json} and {@code evidence.json}.
 *
 *  <p>Engine side (this class): stage 1 reads the three stage-1 input topics ({@code read_committed}), tells barriers by their header,
 *  parses every other record with origin offset = its broker offset, routes it, delivers each barrier to the station tasks that require
 *  its partition (never to a station it declares starved) and to the reject collector, and writes every closed cut's outputs and progress
 *  in one transaction (a fixed {@code transactional.id} per stage) to the station's partition. Stage 2 reads {@code forseti.work-records}
 *  and {@code forseti.samples} at every station partition and {@code quality.incidents}; a station applies a cut when its stage-1 progress
 *  is on both evidence topics and the six incident barriers of the cut (and of every cut it merges) are in, exactly as the reference
 *  harness hands them over: the cut's work-record outputs, then its sample outputs, then the progress. */
public final class KafkaReferenceDriver {
    private KafkaReferenceDriver() {}

    public static final String ENGINE = "core-harness-over-kafka";
    static final Duration CUT_TIMEOUT = Duration.ofSeconds(120);

    public record Result(Path run, Path recording, int cuts, String rulesDigest, String registeredRowSha256Cli, String registeredRowSha256Kafka,
                         long declarationOffset, Progress.InitEvidence firstInit, List<String> topicProblems, Map<String, Long> dumped, ObjectNode evidence,
                         long businessRecords, long barriers) {}

    public static Result run(String bootstrap, String prefix, Path scenario, Path registry, Path trustedKeys, Path work, W1Form form) throws IOException {
        Path recDir = work.resolve("recording"), state = work.resolve("state"), out = work.resolve("run");
        ScenarioBuilder.build(scenario, registry, recDir);
        Recording rec = Recording.open(recDir);
        rec.verifyDigests();
        rec.verifyBatches();
        JsonNode rule = Json.read(scenario).path("rule");

        // (1) the run's topics, then the generation: registered by the operator CLI on forseti.generations (--store kafka), the only
        //     writer of the control store (spec 485, 575-577)
        List<ReplayTopics.Spec> plan = ReplayTopics.plan(prefix);
        KafkaEvidenceCollector evidence = new KafkaEvidenceCollector();
        List<String> topicProblems;
        try (KafkaTopicManager admin = new KafkaTopicManager(bootstrap, "forseti-harness-admin")) {
            admin.create(plan);
            topicProblems = evidence.topics(admin, plan);
        }
        String generationsTopic = ReplayTopics.nameOf(plan, ReplayTopics.GENERATIONS), controlTopic = ReplayTopics.nameOf(plan, ReplayTopics.RECOVERY_CONTROL);
        String stream = rule.path("stream").asText();
        JsonNode statement = Json.read(Base64.getDecoder().decode(Json.read(registry.resolve("mappings/head-attestations/" + stream + ".dsse.json")).get("payload").asText()));
        Clock clock = Clock.fixed(Instant.parse(statement.path("predicate").path("signedAt").asText()).plusSeconds(60), ZoneOffset.UTC);
        ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
        int exit = GenerationCli.run(new String[] {"new", "--store", "kafka", "--bootstrap", bootstrap, "--generations-topic", generationsTopic,
            "--state", state.toString(), "--mappings", registry.resolve("mappings").toString(), "--trusted-keys", trustedKeys.toString(),
            "--stream", stream, "--rule", rule.path("attributionRuleId").asText() + "@" + rule.path("attributionRuleVersion").asText(), "--recording", recDir.toString()},
            new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8), clock);
        if (exit != 0) throw new IllegalStateException("generation new --store kafka exit " + exit + ": " + o.toString(StandardCharsets.UTF_8) + e.toString(StandardCharsets.UTF_8));
        String cliRowSha = Json.read(o.toByteArray()).path("registeredRowSha256").asText();
        try (KafkaGenerationStore store = new KafkaGenerationStore(bootstrap, generationsTopic, false);
             KafkaRecoveryControl control = new KafkaRecoveryControl(bootstrap, controlTopic, true);
             KafkaRecoveryControl reader = new KafkaRecoveryControl(bootstrap, controlTopic, false)) {
            evidence.client("generations-reader", store.readerConfig());
            evidence.client("recovery-control-reader", reader.readerConfig());
            String routingDigest = rec.routing().routingDigest();
            long declaration = RunPreparation.prepare(store, control, 1, routingDigest);
            return runOn(bootstrap, prefix, plan, rec, recDir, registry, store, reader, out, form, evidence, topicProblems,
                cliRowSha, declaration);
        }
    }

    private static Result runOn(String bootstrap, String prefix, List<ReplayTopics.Spec> plan, Recording rec, Path recDir, Path registry,
                                KafkaGenerationStore store, RecoveryControlStore control, Path out, W1Form form, KafkaEvidenceCollector evidence,
                                List<String> topicProblems, String fileRowSha, long declaration) throws IOException {
        // (2) the run's setup, as ReferenceEngine.run makes it, from the generation the Kafka store holds
        GenerationStore.Entry registered = store.registered(1).orElseThrow(() -> new IllegalStateException("generation 1 is not registered on forseti.generations"));
        Generation g = registered.row();
        String kafkaRowSha = store.registeredRowSha256(1).orElseThrow();
        HeadVerifier.Head head = ReferenceEngine.head(g.releaseEvidence());
        Path mappings = registry.resolve("mappings");
        RuleSet rules;
        ReleaseReader.Release release;
        try {
            release = ReleaseReader.read(mappings, head);
            JsonNode r = g.releaseEvidence().path("pins").path("rules").path("attribution");
            rules = RuleSet.load(release, Json.text(r, "id"), Json.text(r, "version"), rec.manifest().path("definitions"));
        } catch (Refusal r) { throw new IllegalStateException("release refused: " + r.reason); }
        if (!rules.rulesDigest().equals(g.rulesDigest())) throw new IllegalStateException("rules digest mismatch");
        long oo = rules.attribution().outOfOrdernessMs();
        RoutingManifest routing = rec.routing();
        List<String> stations = new ArrayList<>(rules.attribution().stations());
        Collections.sort(stations);
        boolean occupancyRead = !(rules.attribution().mode().equals("plc-stamped") && rules.attribution().crossCheck().equals("none"));
        Map<String, List<Integer>> subscribed = new LinkedHashMap<>();
        for (String t : ReferenceEngine.STAGE1_TOPICS) subscribed.put(t, t.equals(InputParser.OCCUPANCY) && !occupancyRead ? List.of() : routing.subscribed(t));
        List<Integer> incidentPartitions = routing.subscribed(InputParser.INCIDENTS);
        String runId = UUID.randomUUID().toString();
        Pins pins = ReferenceEngine.pins(rules, rec.manifest().path("definitions"),
            new Pins.Job(ENGINE, ReferenceEngine.ENGINE_VERSION, "ab".repeat(32), runId, "payload", TextNode.valueOf("off"), routing.routingDigest()));
        Map<String, String> names = new LinkedHashMap<>();
        for (ReplayTopics.Spec s : plan) names.put(s.base(), s.name());

        Stage1Job s1 = new Stage1Job(bootstrap, prefix, names, rules, g.generation(), stations, routing, subscribed, oo, pins, control, form);
        Stage2Job s2 = new Stage2Job(bootstrap, prefix, names, rules, g.generation(), stations, routing, incidentPartitions, pins, rec);
        Progress.InitEvidence firstInit = s1.firstInit;
        Thread t1 = new Thread(s1, "stage-1"), t2 = new Thread(s2, "stage-2");
        t1.start();
        t2.start();

        // (3) the batches, each written only after the previous cut completed
        Map<String, List<Integer>> written = new LinkedHashMap<>(subscribed);
        written.put(InputParser.INCIDENTS, incidentPartitions);
        Map<String, List<Integer>> subscribedAll = new LinkedHashMap<>(written);
        List<CompletionReader.Writer> all = new ArrayList<>();
        for (Recording.Batch b : rec.batches()) for (CompletionReader.Writer w : CompletionReader.expected(routing, List.of())) if (!all.contains(w)) all.add(w);
        ObjectNode cutLog = Json.putSafe(Json.object(), "generation", g.generation()).put("routingDigest", routing.routingDigest());
        if (form != W1Form.PER_STATION) cutLog.put("w1Form", form.wire);
        ArrayNode cuts = cutLog.putArray("cuts");
        Map<String, Map<Integer, CutLog.Range>> consumed = new TreeMap<>();
        Map<String, Long> previous = new HashMap<>();
        long businessRecords = 0, barriers = 0;
        Map<String, Long> dumped;
        KafkaBatchWriter writer = new KafkaBatchWriter(bootstrap, KafkaBatchWriter.inputNames(plan), rec, g.generation(), written);
        try (KafkaCompletionReader completion = new KafkaCompletionReader(bootstrap, names, all, g.generation())) {
            evidence.client("batch-writer", writer.config());
            evidence.client("completion-reader", completion.config());
            for (Recording.Batch b : rec.batches()) {
                var w = writer.write(b, 0, null);
                businessRecords += w.records();
                barriers += w.barriers();
                List<CutAssembler.Idle> idle = ReferenceEngine.idleInputs(b);
                Map<CompletionReader.Writer, JsonNode> done;
                try { done = completion.await(b.cutId(), CompletionReader.expected(routing, idle), CUT_TIMEOUT); }
                catch (IllegalStateException x) { throw new IllegalStateException(x.getMessage() + failures(s1, s2), x); }
                cuts.add(cutEntry(b, done, routing, written, previous, consumed));
            }
        } finally {
            s1.stop = true;
            s2.stop = true;
            join(t1);
            join(t2);
            writer.close();
        }
        if (s1.failure.get() != null || s2.failure.get() != null) throw new IllegalStateException("a stage failed:" + failures(s1, s2));

        // (4) the dump, the cut log, the run record and the evidence
        Files.createDirectories(out);
        try (KafkaJournalDump dump = new KafkaJournalDump(bootstrap, plan, g.generation())) {
            evidence.client("journal-dump", dump.config());
            dumped = dump.dump(out, writer.offsets());
        }
        Json.write(out.resolve("cuts.json"), cutLog);
        Json.write(out.resolve("offset-map.json"), writer.offsets().toJson());
        String manifestSha;
        try { manifestSha = Canonical.digestOf(Files.readAllBytes(recDir.resolve(Recording.MANIFEST))); } catch (IOException x) { throw new UncheckedIOException(x); }
        ObjectNode digests = Json.object();
        rec.digests().forEach(digests::put);
        ObjectNode record = RunRecord.build(new RunRecord.Facts(runId, g.generation(), kafkaRowSha, registered.scope(), ReferenceEngine.DEFAULT_SEED, false, null, null,
            pins, manifestSha, digests, head.releaseSha256(), rules.rulesDigest(), ReferenceEngine.jcsJava(), subscribedAll, consumed, cuts.size(),
            "kafka-reference-driver " + prefix, null));
        RunRecord.write(out, record);
        ObjectNode ev = evidence.evidence();
        Json.write(out.resolve("evidence.json"), ev);
        return new Result(out, recDir, cuts.size(), rules.rulesDigest(), fileRowSha, kafkaRowSha, declaration, firstInit, topicProblems, dumped, ev, businessRecords, barriers);
    }

    /** One cut of {@code cuts.json}, in recording offsets: the stations whose stage-1 progress closed this very cut, with the W1 and
     *  admissionW of that progress and the W2 of the stage-2 one; the batch manifest's half-open offset ranges; its incident offsets. */
    private static ObjectNode cutEntry(Recording.Batch b, Map<CompletionReader.Writer, JsonNode> done, RoutingManifest routing, Map<String, List<Integer>> written,
                                       Map<String, Long> previous, Map<String, Map<Integer, CutLog.Range>> consumed) {
        ObjectNode c = Json.putSafe(Json.object(), "cutId", b.cutId());
        ObjectNode ps = c.putObject("perStation");
        for (Map.Entry<String, Integer> s : new TreeMap<>(routing.stations()).entrySet()) {
            JsonNode wr = done.get(new CompletionReader.Writer(Stage1.WORK_RECORDS, s.getValue())), af = done.get(new CompletionReader.Writer(Stage2.AFFECTED, s.getValue()));
            if (wr == null || wr.path("cutId").asLong() != b.cutId()) continue;
            ObjectNode v = ps.putObject(s.getKey());
            v.set("W1", wr.get("W1"));
            v.set("admissionW", wr.get("admissionW"));
            v.set("W2", af.get("W2"));
            v.set("mergedCutIds", wr.get("mergedCutIds"));
        }
        ObjectNode ranges = c.putObject("offsetRanges");
        ArrayNode inc = c.putArray("incidentOffsets");
        for (Map.Entry<String, List<Integer>> t : written.entrySet()) {
            ObjectNode tn = null;
            for (int p : t.getValue()) {
                String k = t.getKey() + "/" + p;
                long from = previous.getOrDefault(k, -1L) + 1, through = b.through(t.getKey(), p);
                if (tn == null) tn = ranges.putObject(t.getKey());
                tn.putArray(Integer.toString(p)).add(from).add(through + 1);
                consumed.computeIfAbsent(t.getKey(), x -> new TreeMap<>()).merge(p, new CutLog.Range(from, through + 1), (a, x) -> new CutLog.Range(a.from(), x.to()));
                previous.put(k, through);
                if (t.getKey().equals(InputParser.INCIDENTS)) {
                    ObjectNode io = Json.putSafe(Json.object(), "cutId", b.cutId());
                    Json.putSafe(io, "partition", p);
                    inc.add(Json.putSafe(io, "throughOffset", through));
                }
            }
        }
        return c;
    }

    private static String failures(Stage1Job s1, Stage2Job s2) {
        StringBuilder b = new StringBuilder();
        if (s1.failure.get() != null) b.append(" stage 1: ").append(s1.failure.get());
        if (s2.failure.get() != null) b.append(" stage 2: ").append(s2.failure.get());
        return b.toString();
    }

    private static void join(Thread t) {
        try { t.join(30_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        if (t.isAlive()) t.interrupt();
    }

    // --- the two stages ---------------------------------------------------------------------------------------------------------

    /** A stage: one {@code read_committed} consumer over assigned partitions, one transactional producer, one transaction per poll that
     *  produced output. */
    private abstract static class Job implements Runnable {
        final Map<String, String> names, contractOf = new HashMap<>();
        final KafkaConsumer<byte[], byte[]> consumer;
        final KafkaProducer<byte[], byte[]> producer;
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        volatile boolean stop;

        Job(String bootstrap, String prefix, Map<String, String> names, String stage, List<TopicPartition> in) {
            this.names = names;
            names.forEach((c, n) -> contractOf.put(n, c));
            this.consumer = new KafkaConsumer<>(KafkaConfig.consumer(bootstrap, "forseti-ref-" + stage));
            consumer.assign(in);
            consumer.seekToBeginning(in);
            this.producer = new KafkaProducer<>(KafkaConfig.transactionalProducer(bootstrap, "forseti-ref-" + stage, prefix + "forseti-ref-" + stage));
            producer.initTransactions();
        }

        abstract void handle(String topic, ConsumerRecord<byte[], byte[]> r, List<ProducerRecord<byte[], byte[]>> out);

        @Override public void run() {
            try {
                while (!stop) {
                    List<ProducerRecord<byte[], byte[]>> out = new ArrayList<>();
                    for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(50))) handle(contractOf.get(r.topic()), r, out);
                    if (out.isEmpty()) continue;
                    producer.beginTransaction();
                    for (ProducerRecord<byte[], byte[]> x : out) producer.send(x);
                    producer.commitTransaction();
                }
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                consumer.close();
                producer.close(Duration.ofSeconds(5));
            }
        }

        void emit(List<ProducerRecord<byte[], byte[]>> out, String topic, int partition, ObjectNode line) {
            OutputRecords.Transport t = OutputRecords.transport(line);
            List<Header> hs = new ArrayList<>();
            t.headers().forEach((k, v) -> hs.add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8))));
            out.add(new ProducerRecord<>(names.get(topic), partition, null, t.key(), t.value(), hs));
        }

        /** The barrier a record is (null for a business input), checked against the partition it was consumed from (decision F6). */
        static CutBarrierRecord barrier(String topic, ConsumerRecord<byte[], byte[]> r, long generation, String routingDigest) {
            Header h = r.headers().lastHeader(CutBarrierRecord.TYPE_HEADER);
            if (h == null) return null;
            if (!CutBarrierRecord.TYPE.equals(new String(h.value(), StandardCharsets.UTF_8))) throw new IllegalStateException("unknown record type header");
            return CutBarrierRecord.consumed(topic, r.partition(), r.value(), generation, routingDigest);
        }
    }

    private static final class Stage1Job extends Job {
        final Routing router;
        final PartitionClock clock;
        final Map<String, StationTask> tasks = new LinkedHashMap<>();
        final Map<String, List<Progress.Origin>> required = new HashMap<>();
        final RejectCollector rejects;
        final List<Progress.Origin> rejectsRequired;
        final Map<Long, Integer> rejectBarriers = new HashMap<>();
        final long generation;
        final String routingDigest;
        final Progress.InitEvidence firstInit;

        Stage1Job(String bootstrap, String prefix, Map<String, String> names, RuleSet rules, long generation, List<String> stations, RoutingManifest routing,
                  Map<String, List<Integer>> subscribed, long oo, Pins pins, RecoveryControlStore control, W1Form form) {
            super(bootstrap, prefix, names, "stage1", partitions(names, subscribed));
            this.generation = generation;
            this.routingDigest = routing.routingDigest();
            Stage1 stage1 = new Stage1(rules, Controls.of(null), Envelope.ONLINE);
            this.router = new Routing(rules);
            this.clock = form == W1Form.MIN_OVER_PARTITIONS ? new PartitionClock(subscribed.get(InputParser.MESSAGES)) : null;
            Progress.InitEvidence first = null;
            for (String s : stations) {
                // every writer reads the recovery control at its own initialization, up to the end offset H
                Progress.InitEvidence init = control.read().initEvidence(generation, "none");
                if (first == null) first = init;
                List<Progress.Origin> req = ReferenceEngine.stationRequired(s, routing, subscribed);
                required.put(s, req);
                tasks.put(s, new StationTask(stage1, generation, s, routing.partitionOf(s), oo, pins, init, req, form, clock));
            }
            this.firstInit = first;
            this.rejectsRequired = ReferenceEngine.required(subscribed, null);
            this.rejects = new RejectCollector(generation, control.read().initEvidence(generation, "none"), rejectsRequired);
        }

        @Override void handle(String topic, ConsumerRecord<byte[], byte[]> r, List<ProducerRecord<byte[], byte[]>> out) {
            CutBarrierRecord br = barrier(topic, r, generation, routingDigest);
            if (br == null) {
                String key = r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8);
                InputParser.Read read = InputParser.read(new Inputs.Origin(topic, r.partition(), r.offset()), key, r.value());
                if (clock != null && topic.equals(InputParser.MESSAGES)) clock.delivered(read.record(), !read.rejected());
                Routing.Outcome o = router.route(read);
                if (o instanceof Routing.Rejected x) rejects.add(x);
                else tasks.get(((Routing.Routed) o).station()).add(read.record());
                return;
            }
            CutAssembler.Barrier b = br.barrier();
            if (br.generation() != generation || !br.routingDigest().equals(routingDigest)) throw new IllegalStateException("a barrier of another generation or routing");
            if (!b.origin().equals(new Progress.Origin(topic, r.partition()))) throw new IllegalStateException("a barrier of " + b.origin() + " on " + topic + "/" + r.partition());
            Set<String> starved = new TreeSet<>();
            for (CutAssembler.Idle i : b.idleInputs()) if (i.kind().equals(CutAssembler.STARVED)) starved.add(i.station());
            for (StationTask t : tasks.values()) {
                if (!starved.contains(t.station) && required.get(t.station).contains(b.origin())) t.barrier(b);
                Optional<StationTask.CutOutputs> closed = t.close();
                if (closed.isPresent()) write(out, closed.get().outputs(), t.partition);
            }
            rejects.barrier(b);
            if (rejectBarriers.merge(b.cutId(), 1, Integer::sum) == rejectsRequired.size()) {
                rejectBarriers.remove(b.cutId());
                for (Object x : rejects.close())
                    if (x instanceof Disposition d) emit(out, ReplayTopics.DISPOSITIONS1, RejectCollector.PARTITION, OutputRecords.dispositionLine(d));
                    else emit(out, ReplayTopics.DISPOSITIONS1, RejectCollector.PARTITION, OutputRecords.progressLine((Progress) x));
            }
        }

        private void write(List<ProducerRecord<byte[], byte[]>> out, List<Object> outputs, int partition) {
            for (Object x : outputs) {
                if (x instanceof Row r) emit(out, r.topic(), partition, OutputRecords.rowLine(r));
                else if (x instanceof Disposition d) emit(out, ReplayTopics.DISPOSITIONS1, partition, OutputRecords.dispositionLine(d));
                else if (x instanceof QuarantineRecord q) emit(out, ReplayTopics.QUARANTINE, partition, OutputRecords.quarantineLine(q));
                else if (x instanceof Progress.Stage1 p) { emit(out, Stage1.WORK_RECORDS, partition, OutputRecords.progressLine(p)); emit(out, Stage1.SAMPLES, partition, OutputRecords.progressLine(p)); }
                else if (x instanceof Progress.Ledger l) emit(out, ReplayTopics.DISPOSITIONS1, partition, OutputRecords.progressLine(l));
                else throw new IllegalStateException("not a stage-1 output: " + x);
            }
        }

        static List<TopicPartition> partitions(Map<String, String> names, Map<String, List<Integer>> subscribed) {
            List<TopicPartition> l = new ArrayList<>();
            subscribed.forEach((t, ps) -> { for (int p : ps) l.add(new TopicPartition(names.get(t), p)); });
            return l;
        }
    }

    private static final class Stage2Job extends Job {
        final RuleSet rules;
        final long generation;
        final String routingDigest;
        final Map<String, Stage2Task> tasks = new LinkedHashMap<>();
        final Map<Integer, String> stationOfPartition = new HashMap<>();
        final RejectCollector rejects2;
        final List<Integer> incidentPartitions;
        final Map<Integer, List<Inputs.InputRecord>> pendingIncidents = new HashMap<>();
        final Map<Long, List<Inputs.InputRecord>> cutIncidents = new HashMap<>();
        final Map<Long, Map<Integer, CutAssembler.Barrier>> incidentBarriers = new HashMap<>();
        final Set<Long> incidentReady = new HashSet<>();
        final Map<String, String> incidentStation = new HashMap<>();
        final Map<String, Map<String, List<Object>>> pending = new HashMap<>();
        final Map<String, Map<String, Progress.Stage1>> progress = new HashMap<>();

        Stage2Job(String bootstrap, String prefix, Map<String, String> names, RuleSet rules, long generation, List<String> stations, RoutingManifest routing,
                  List<Integer> incidentPartitions, Pins pins, Recording rec) {
            super(bootstrap, prefix, names, "stage2", partitions(names, stations, routing, incidentPartitions));
            this.rules = rules;
            this.generation = generation;
            this.routingDigest = routing.routingDigest();
            this.incidentPartitions = List.copyOf(incidentPartitions);
            Stage2 stage2 = new Stage2(rules, Controls.of(null), Envelope.ONLINE);
            ProgressTracker tracker = new ProgressTracker();
            Map<String, SetCoverage.EvidenceRange> evidenceRanges = new TreeMap<>();
            rec.evidenceRange().forEach((d, r) -> evidenceRanges.put(d, new SetCoverage.EvidenceRange(r.from(), r.to())));
            for (String s : stations) {
                tasks.put(s, new Stage2Task(stage2, tracker, generation, s, routing.partitionOf(s), pins, evidenceRanges));
                stationOfPartition.put(routing.partitionOf(s), s);
                pending.put(s, new HashMap<>(Map.of(Stage1.WORK_RECORDS, new ArrayList<>(), Stage1.SAMPLES, new ArrayList<>())));
                progress.put(s, new HashMap<>());
            }
            List<Progress.Origin> origins = new ArrayList<>();
            for (int p : incidentPartitions) origins.add(new Progress.Origin(InputParser.INCIDENTS, p));
            this.rejects2 = new RejectCollector(generation, 2, null, origins);
        }

        static List<TopicPartition> partitions(Map<String, String> names, List<String> stations, RoutingManifest routing, List<Integer> incidents) {
            List<TopicPartition> l = new ArrayList<>();
            for (String s : stations) for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES)) l.add(new TopicPartition(names.get(t), routing.partitionOf(s)));
            for (int p : incidents) l.add(new TopicPartition(names.get(InputParser.INCIDENTS), p));
            return l;
        }

        @Override void handle(String topic, ConsumerRecord<byte[], byte[]> r, List<ProducerRecord<byte[], byte[]>> out) {
            if (topic.equals(InputParser.INCIDENTS)) incident(r, out);
            else evidence(topic, r);
            for (String s : tasks.keySet()) tryCut(s, out);
        }

        private void incident(ConsumerRecord<byte[], byte[]> r, List<ProducerRecord<byte[], byte[]>> out) {
            CutBarrierRecord br = barrier(InputParser.INCIDENTS, r, generation, routingDigest);
            if (br == null) {
                String key = r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8);
                InputParser.Read read = InputParser.read(new Inputs.Origin(InputParser.INCIDENTS, r.partition(), r.offset()), key, r.value());
                Incidents.Outcome door = Incidents.door(rules, read);
                if (door instanceof Incidents.Rejected x) rejects2.add(x.record(), Disposition.Part.EVENT, x.body());
                else pendingIncidents.computeIfAbsent(r.partition(), x -> new ArrayList<>()).add(((Incidents.Routed) door).record());
                return;
            }
            if (br.generation() != generation || !br.routingDigest().equals(routingDigest)) throw new IllegalStateException("a barrier of another generation or routing");
            CutAssembler.Barrier b = br.barrier();
            long cut = b.cutId();
            cutIncidents.computeIfAbsent(cut, x -> new ArrayList<>()).addAll(pendingIncidents.getOrDefault(r.partition(), List.of()));
            pendingIncidents.remove(r.partition());
            Map<Integer, CutAssembler.Barrier> m = incidentBarriers.computeIfAbsent(cut, x -> new TreeMap<>());
            m.put(r.partition(), b);
            if (m.size() < incidentPartitions.size()) return;
            // every incident barrier of the cut: T1 routes the cut's incidents in the harness's order, every station and the stage-2
            // reject collector receive the barriers
            List<Inputs.InputRecord> routed = cutIncidents.remove(cut);
            if (routed == null) routed = new ArrayList<>();
            routed.sort(Stage2Task.INCIDENT_ORDER);
            for (Inputs.InputRecord x : routed) {
                Inputs.IncidentEvent i = (Inputs.IncidentEvent) x.parsed();
                String station = incidentStation.computeIfAbsent(Canonical.utf8(Canonical.bytes(Incidents.setKey(i.source(), i.incidentId()))), y -> i.stationRef());
                tasks.get(station).addIncident(x);
            }
            Map<Integer, Long> through = new TreeMap<>();
            m.forEach((p, x) -> through.put(p, x.throughOffset()));
            for (Stage2Task t : tasks.values()) t.incidentBarriers(cut, through);
            for (CutAssembler.Barrier x : m.values()) rejects2.barrier(x);
            for (Object x : rejects2.close())
                if (x instanceof Disposition d) emit(out, ReplayTopics.DISPOSITIONS2, RejectCollector.PARTITION, OutputRecords.dispositionLine(d));
                else emit(out, ReplayTopics.DISPOSITIONS2, RejectCollector.PARTITION, OutputRecords.progressLine((Progress) x));
            incidentBarriers.remove(cut);
            incidentReady.add(cut);
        }

        private void evidence(String topic, ConsumerRecord<byte[], byte[]> r) {
            String s = stationOfPartition.get(r.partition());
            Map<String, String> h = new TreeMap<>();
            for (Header x : r.headers()) h.put(x.key(), new String(x.value(), StandardCharsets.UTF_8));
            ObjectNode line = OutputRecords.line(new OutputRecords.Transport(r.key(), h, r.value()), generation);
            JsonNode v = line.get("value");
            switch (OutputRecords.kind(v)) {
                case PROGRESS -> {
                    Progress.Stage1 p = (Progress.Stage1) Progress.fromJson(v);
                    if (progress.get(s).put(topic, p) != null) throw new IllegalStateException(s + ": a second stage-1 progress on " + topic + " before its cut was applied");
                }
                case DELETE -> pending.get(s).get(topic).add(Delete.fromJson(v.get("delete")));
                case RECORD -> {
                    ObjectNode n = Json.object().put("topic", topic);
                    n.set("logicalKey", line.get("key"));
                    n.set("body", v.get("body"));
                    n.set("bodyDigest", v.get("bodyDigest"));
                    n.set("envelope", v.get("envelope"));
                    pending.get(s).get(topic).add(Row.fromJson(n));
                }
            }
        }

        private void tryCut(String s, List<ProducerRecord<byte[], byte[]>> out) {
            Progress.Stage1 wr = progress.get(s).get(Stage1.WORK_RECORDS), sm = progress.get(s).get(Stage1.SAMPLES);
            if (wr == null || sm == null) return;
            if (wr.cutId() != sm.cutId() || !wr.mergedCutIds().equals(sm.mergedCutIds())) throw new IllegalStateException(s + ": the evidence topics disagree on cut " + wr.cutId() + "/" + sm.cutId());
            if (!incidentReady.contains(wr.cutId())) return;
            for (long m : wr.mergedCutIds()) if (!incidentReady.contains(m)) return;
            List<Object> stage1 = new ArrayList<>(pending.get(s).get(Stage1.WORK_RECORDS));
            stage1.addAll(pending.get(s).get(Stage1.SAMPLES));
            stage1.add(wr);
            Stage2Task t = tasks.get(s);
            Stage2Task.CutOutputs c = t.cut(wr.cutId(), stage1);
            for (Object x : c.outputs()) {
                if (x instanceof Row r) emit(out, r.topic(), t.partition, OutputRecords.rowLine(r));
                else if (x instanceof Delete d) emit(out, Stage2.AFFECTED, t.partition, OutputRecords.deleteLine(d));
                else if (x instanceof Disposition d) emit(out, ReplayTopics.DISPOSITIONS2, t.partition, OutputRecords.dispositionLine(d));
                else if (x instanceof Progress.Stage2 p) { emit(out, Stage2.AFFECTED, t.partition, OutputRecords.progressLine(p)); emit(out, Stage2.AFFECTED_SETS, t.partition, OutputRecords.progressLine(p)); }
                else if (x instanceof Progress.Ledger l) emit(out, ReplayTopics.DISPOSITIONS2, t.partition, OutputRecords.progressLine(l));
                else throw new IllegalStateException("not a stage-2 output: " + x);
            }
            pending.get(s).get(Stage1.WORK_RECORDS).clear();
            pending.get(s).get(Stage1.SAMPLES).clear();
            progress.get(s).clear();
        }
    }
}
