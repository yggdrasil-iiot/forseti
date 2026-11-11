package yggdrasil.forseti.cli.harness;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.cli.generation.FileGenerationStore;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.NegativeControls;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Disposition;
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

/** The engine-independent reference adapter of stage 1 ({@code forseti run --engine core}). It owns only the adapter duties of
 *  Decision 12: partitioning (S1 routing to a {@link StationTask} or the {@link RejectCollector}), timers, state, side outputs and sinks;
 *  every judgement is {@link Stage1#decide}'s. It runs a registered generation from its pinned snapshot: it reads the generation row,
 *  rebuilds the head from {@code releaseEvidence} and lets {@link ReleaseReader} verify {@code release.json} and every payload file against
 *  those digests; it never verifies a head and never writes a generation row. Per batch of the recording it delivers the records of
 *  {@code (throughOffsets[k-1], throughOffsets[k]]} of every subscribed stage-1 partition, closes the cut at every station and the reject
 *  collector, and writes the journals, the cut log and the run record. */
public final class ReferenceEngine {
    private ReferenceEngine() {}

    public static final String ENGINE = "core-harness", ENGINE_VERSION = "0.1.0", RECOVERY_CONTROL = "recovery-control.jsonl";
    public static final long DEFAULT_SEED = 20261109L;
    static final List<String> STAGE1_TOPICS = List.of(InputParser.MESSAGES, InputParser.SESSIONS, InputParser.OCCUPANCY);

    /** A start-up refusal: a code ({@code generation-unregistered}, {@code generation-rules-mismatch}, or a reader refusal such as
     *  {@code provenance-mismatch}) and its evidence. */
    public static final class Refused extends RuntimeException {
        public final String code;
        public final ObjectNode evidence;
        Refused(String code, ObjectNode evidence) { super(code); this.code = code; this.evidence = evidence; }
        public ObjectNode toJson() { ObjectNode n = Json.object().put("refusal", code); n.setAll(evidence.deepCopy()); return n; }
    }

    /** {@code subscribe}: per topic, the partitions to read instead of the recording's (an empty list unsubscribes the topic);
     *  {@code artifactSha256}: null for the running jar's digest. */
    public record Options(Path recording, Path registry, Path state, long generation, Path out, long seed, boolean shuffle,
                          Map<String, List<Integer>> subscribe, String artifactSha256, String command) {
        public Options {
            subscribe = subscribe == null ? Map.of() : Map.copyOf(subscribe);
            if (command == null) command = "forseti run --engine core";
        }
    }

    public record Result(String runId, Path out, int cuts, ObjectNode runRecord) {}

    public static Result run(Options o) {
        // (1) the registered generation and its pinned snapshot
        GenerationStore store = new FileGenerationStore(o.state());
        List<GenerationStore.Entry> rows = store.byNumber(o.generation());
        if (rows.isEmpty()) throw new Refused("generation-unregistered", Json.putSafe(Json.object(), "generation", o.generation()));
        Generation g = rows.get(0).row();
        for (GenerationStore.Entry e : rows) if (Generation.REGISTERED.equals(e.row().status())) { g = e.row(); break; }
        JsonNode evidence = g.releaseEvidence();
        HeadVerifier.Head head = head(evidence);
        Path mappings = o.registry().resolve("mappings");
        Recording rec = Recording.open(o.recording());
        rec.verifyDigests();
        rec.verifyBatches();
        RuleSet rules;
        ReleaseReader.Release release;
        try {
            release = ReleaseReader.read(mappings, head);
            JsonNode rule = evidence.path("pins").path("rules").path("attribution");
            rules = RuleSet.load(release, Json.text(rule, "id"), Json.text(rule, "version"), rec.manifest().path("definitions"));
        } catch (Refusal r) {
            ObjectNode ev = Json.object();
            r.evidence.forEach((k, v) -> ev.set(k, Json.JSON.valueToTree(v)));
            throw new Refused(r.reason, ev);
        }
        if (!rules.rulesDigest().equals(g.rulesDigest()))
            throw new Refused("generation-rules-mismatch", Json.putSafe(Json.object(), "generation", g.generation())
                .put("registered", g.rulesDigest()).put("loaded", rules.rulesDigest()));

        // (2) routing, subscriptions, the first batch
        RoutingManifest routing = rec.routing();
        List<String> stations = new ArrayList<>(rules.attribution().stations());
        Collections.sort(stations);
        Map<String, Integer> expectedStations = new LinkedHashMap<>();
        for (int i = 0; i < stations.size(); i++) expectedStations.put(stations.get(i), i);
        if (!expectedStations.equals(routing.stations())) throw new IllegalArgumentException("malformed recording: routing stations " + routing.stations() + " differ from the rule's " + expectedStations);
        for (String d : routing.devices()) {
            Optional<String> s = rules.hierarchy().stationOf(rules.attribution().stations(), d);
            if (s.isEmpty() || !s.get().equals(routing.stationOf(d))) throw new IllegalArgumentException("malformed recording: routing places " + d + " at " + routing.stationOf(d) + ", the rule at " + s.orElse(null));
        }
        Map<String, List<Integer>> subscribed = new LinkedHashMap<>();
        for (String t : STAGE1_TOPICS) {
            List<Integer> have = routing.subscribed(t), want = o.subscribe().getOrDefault(t, have);
            for (int p : want) if (!have.contains(p)) throw new IllegalArgumentException("--subscribe " + t + "=" + p + ": the recording has no such partition");
            List<Integer> sorted = new ArrayList<>(new TreeSet<>(want));
            subscribed.put(t, List.copyOf(sorted));
        }
        for (String t : o.subscribe().keySet()) if (!STAGE1_TOPICS.contains(t)) throw new IllegalArgumentException("--subscribe " + t + ": not a stage-1 input topic");
        Recording.Batch first = rec.batches().get(0);
        for (int p : subscribed.get(InputParser.MESSAGES))
            if (first.through(InputParser.MESSAGES, p) < 0)
                throw new IllegalArgumentException("malformed recording: the first batch carries no telemetry on " + InputParser.MESSAGES + "/" + p + " (spec 406)");

        // writers: one initialization each, each reading the recovery control once
        String runId = UUID.randomUUID().toString();
        String artifact = o.artifactSha256() != null ? o.artifactSha256() : artifactSha256();
        Pins pins = pins(rules, rec.manifest().path("definitions"),
            new Pins.Job(ENGINE, ENGINE_VERSION, artifact, runId, "payload", TextNode.valueOf("off"), routing.routingDigest()));
        Stage1 stage1 = new Stage1(rules, NegativeControls.NONE);
        Routing router = new Routing(rules);
        long oo = rules.attribution().outOfOrdernessMs();
        Map<String, StationTask> tasks = new LinkedHashMap<>();
        for (String s : stations) tasks.put(s, new StationTask(stage1, g.generation(), s, routing.partitionOf(s), oo, pins, initEvidence(o.state(), g.generation())));
        RejectCollector rejects = new RejectCollector(g.generation(), initEvidence(o.state(), g.generation()));

        Map<String, List<byte[]>> raw = new HashMap<>();
        for (String t : STAGE1_TOPICS) for (int p : subscribed.get(t)) raw.put(t + "/" + p, rawValues(rec, t, p));

        // (3) the batches
        try { Files.createDirectories(o.out()); } catch (IOException e) { throw new UncheckedIOException(e); }
        CutLog cutLog = new CutLog(g.generation(), routing.routingDigest());
        Random random = new Random(o.seed());
        Map<String, Long> previous = new HashMap<>();
        Map<String, Map<Integer, CutLog.Range>> consumed = new TreeMap<>();
        try (JournalFiles journals = new JournalFiles(o.out())) {
            for (Recording.Batch b : rec.batches()) {
                long cutId = b.cutId();
                List<InputParser.Read> delivered = new ArrayList<>();
                Map<String, Map<Integer, CutLog.Range>> ranges = new TreeMap<>();
                for (String t : STAGE1_TOPICS) {
                    Map<Integer, CutLog.Range> tr = new TreeMap<>();
                    for (int p : subscribed.get(t)) {
                        String k = t + "/" + p;
                        long from = previous.getOrDefault(k, -1L) + 1, through = b.through(t, p);
                        List<Recording.Record> records = rec.records(t, p);
                        List<byte[]> bytes = raw.get(k);
                        for (long off = from; off <= through; off++) {
                            Recording.Record r = records.get((int) off);
                            delivered.add(InputParser.read(new Inputs.Origin(t, p, off), r.key(), bytes.get((int) off)));
                        }
                        tr.put(p, new CutLog.Range(from, through + 1));
                        consumed.computeIfAbsent(t, x -> new TreeMap<>()).merge(p, new CutLog.Range(from, through + 1),
                            (a, c) -> new CutLog.Range(a.from(), c.to()));
                        previous.put(k, through);
                    }
                    if (!tr.isEmpty()) ranges.put(t, tr);
                }
                if (o.shuffle()) ContractOrder.shuffle(delivered, random);
                for (InputParser.Read r : delivered) {
                    Routing.Outcome out = router.route(r);
                    if (out instanceof Routing.Rejected x) rejects.add(x);
                    else tasks.get(((Routing.Routed) out).station()).add(r.record());
                }
                Map<String, CutLog.StationCut> perStation = new LinkedHashMap<>();
                for (StationTask task : tasks.values()) {
                    StationTask.CutOutputs co = task.cut(cutId, stationBarriers(cutId, b, task, routing, subscribed));
                    write(journals, co.outputs(), task.partition);
                    perStation.put(task.station, new CutLog.StationCut(co.w1(), co.admissionW(), task.onlineVisits()));
                }
                for (Object x : rejects.cut(cutId, barriers(cutId, b, subscribed, null)))
                    if (x instanceof Disposition d) journals.disposition(d, RejectCollector.PARTITION);
                    else journals.progress(JournalFiles.DISPOSITIONS, (Progress) x, RejectCollector.PARTITION);
                Map<Integer, Long> incidents = new TreeMap<>();
                for (int p : routing.subscribed(InputParser.INCIDENTS)) incidents.put(p, b.through(InputParser.INCIDENTS, p));
                cutLog.cut(cutId, perStation, ranges, incidents);
            }
        }

        // (4) the cut log and the run record
        cutLog.write(o.out());
        String manifestSha;
        try { manifestSha = Canonical.digestOf(Files.readAllBytes(o.recording().resolve(Recording.MANIFEST))); } catch (IOException e) { throw new UncheckedIOException(e); }
        ObjectNode digests = Json.object();
        rec.digests().forEach(digests::put);
        ObjectNode record = RunRecord.build(new RunRecord.Facts(runId, g.generation(), o.seed(), o.shuffle(), pins, manifestSha, digests,
            head.releaseSha256(), rules.rulesDigest(), jcsJava(), subscribed, consumed, cutLog.size(), o.command()));
        RunRecord.write(o.out(), record);
        return new Result(runId, o.out(), cutLog.size(), record);
    }

    private static void write(JournalFiles journals, List<Object> outputs, int partition) {
        for (Object x : outputs) {
            if (x instanceof Row r) journals.row(r, partition);
            else if (x instanceof Disposition d) journals.disposition(d, partition);
            else if (x instanceof QuarantineRecord q) journals.quarantine(q, partition);
            else if (x instanceof Progress.Stage1 p) { journals.progress(Stage1.WORK_RECORDS, p, partition); journals.progress(Stage1.SAMPLES, p, partition); }
            else if (x instanceof Progress.Ledger l) journals.progress(JournalFiles.DISPOSITIONS, l, partition);
            else throw new IllegalStateException("not a stage-1 output: " + x);
        }
    }

    /** A station's required barriers: every subscribed raw telemetry partition, plus the session partitions of its devices and its
     *  occupancy partition when subscribed (the routing manifest fixes both). */
    static List<JsonNode> stationBarriers(long cutId, Recording.Batch b, StationTask task, RoutingManifest routing, Map<String, List<Integer>> subscribed) {
        Set<Integer> sessions = new TreeSet<>();
        for (String d : routing.devices()) if (routing.stationOf(d).equals(task.station)) sessions.add(routing.rawPartitionOf(d));
        Map<String, Set<Integer>> keep = Map.of(InputParser.MESSAGES, new TreeSet<>(subscribed.get(InputParser.MESSAGES)),
            InputParser.SESSIONS, sessions, InputParser.OCCUPANCY, Set.of(task.partition));
        return barriers(cutId, b, subscribed, keep);
    }

    /** {@code {cutId, origin {topic, partition}, throughOffset}} for every subscribed stage-1 partition {@code keep} admits (all when null). */
    static List<JsonNode> barriers(long cutId, Recording.Batch b, Map<String, List<Integer>> subscribed, Map<String, Set<Integer>> keep) {
        List<JsonNode> l = new ArrayList<>();
        for (String t : STAGE1_TOPICS) for (int p : subscribed.get(t)) {
            if (keep != null && !keep.get(t).contains(p)) continue;
            ObjectNode n = Json.putSafe(Json.object(), "cutId", cutId);
            n.putObject("origin").put("topic", t).put("partition", p);
            l.add(Json.putSafe(n, "throughOffset", b.through(t, p)));
        }
        return l;
    }

    /** The head the generation pinned, rebuilt from {@code releaseEvidence.head}; nothing in it is verified here (that was
     *  {@code generation new}): {@link ReleaseReader#read} verifies the release against these digests. */
    static HeadVerifier.Head head(JsonNode evidence) {
        JsonNode h = evidence.path("head");
        if (!h.isObject()) throw new IllegalArgumentException("releaseEvidence.head is not an object");
        return new HeadVerifier.Head(Json.text(h, "stream"), Json.text(h, "releaseId"), Json.text(h, "version"), Json.text(h, "releaseSha256"),
            h.path("headFile").textValue(), h.path("headSha256").textValue(), h.path("attestationFile").textValue(), h.path("attestationSha256").textValue(),
            Json.text(h, "statementSha256"), Json.safeLong(h.get("headVersion"), "releaseEvidence.head.headVersion", 1, Json.MAX_SAFE),
            h.path("signedAt").textValue(), h.path("expires").textValue(), h.path("publishedBy").textValue(), h.path("approvedBy").textValue());
    }

    /** One read of {@code <stateDir>/recovery-control.jsonl} at a writer's initialization: the offset of the latest declaration of this
     *  generation and the record count H at that read. Each line is one declaration ({@code {generation, injectionId, stage, epochBefore,
     *  targetEpoch, lastWrittenCutId, routingDigest}}). */
    static Progress.InitEvidence initEvidence(Path state, long generation) {
        Path f = state.resolve(RECOVERY_CONTROL);
        byte[] bytes;
        try { bytes = Files.readAllBytes(f); } catch (IOException e) { throw new IllegalStateException("no recovery control at " + f + " (generation new writes the initial declaration)"); }
        if (bytes.length > 0 && bytes[bytes.length - 1] != '\n') throw new IllegalStateException("control-log-corrupt: " + RECOVERY_CONTROL + ": the last line has no LF");
        long count = 0, found = -1;
        int start = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != '\n') continue;
            JsonNode d;
            try { d = Json.read(Arrays.copyOfRange(bytes, start, i)); } catch (IllegalArgumentException e) { throw new IllegalStateException("control-log-corrupt: " + RECOVERY_CONTROL + " line " + count); }
            start = i + 1;
            if (Json.safeLong(d.get("generation"), "generation", 1, Json.MAX_SAFE) == generation) found = count;
            count++;
        }
        if (found < 0) throw new IllegalStateException("no recovery declaration of generation " + generation + " in " + RECOVERY_CONTROL);
        return new Progress.InitEvidence(found, count, "none");
    }

    static Pins pins(RuleSet rules, JsonNode definitions, Pins.Job job) {
        ObjectNode p = rules.pins();
        Map<String, Pins.Definition> defs = new TreeMap<>();
        definitions.fields().forEachRemaining(e -> defs.put(e.getKey(),
            new Pins.Definition(Json.text(e.getValue(), "templateRef"), Json.text(e.getValue(), "version"), Json.text(e.getValue(), "sha256"))));
        return new Pins(p.get("rules"), p.get("hierarchy"), p.get("bindings"), defs, job);
    }

    /** The {@code value} bytes of every line of one partition file, as delivered (never re-serialized: a raw-bytes digest and an
     *  overflowing literal depend on them), checked against the record {@link Recording} parsed. */
    static List<byte[]> rawValues(Recording rec, String topic, int partition) {
        List<Recording.Record> records = rec.records(topic, partition);
        byte[] bytes;
        try { bytes = Files.readAllBytes(rec.dir().resolve(topic).resolve(partition + ".jsonl")); } catch (IOException e) { throw new UncheckedIOException(e); }
        List<byte[]> out = new ArrayList<>(records.size());
        int start = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != '\n') continue;
            byte[] v = valueOf(Arrays.copyOfRange(bytes, start, i));
            start = i + 1;
            if (!Json.read(v).equals(records.get(out.size()).value())) throw new IllegalArgumentException("malformed recording: " + topic + "/" + partition + " line " + out.size() + " value");
            out.add(v);
        }
        if (out.size() != records.size()) throw new IllegalArgumentException("malformed recording: " + topic + "/" + partition + " line count");
        return out;
    }

    private static byte[] valueOf(byte[] line) {
        try (JsonParser p = Json.JSON.getFactory().createParser(line)) {
            if (p.nextToken() != JsonToken.START_OBJECT) throw new IllegalArgumentException("malformed recording: a line is not an object");
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String name = p.getCurrentName();
                p.nextToken();
                if (!"value".equals(name)) { p.skipChildren(); continue; }
                int from = (int) p.getTokenLocation().getByteOffset();
                p.skipChildren();
                p.nextToken();   // the token after the value fixes its end
                int to = (int) p.getTokenLocation().getByteOffset();
                while (to > from && (line[to - 1] == ',' || line[to - 1] == ' ')) to--;
                return Arrays.copyOfRange(line, from, to);
            }
        } catch (IOException e) { throw new IllegalArgumentException("malformed recording: " + e.getMessage(), e); }
        throw new IllegalArgumentException("malformed recording: a line has no value");
    }

    static String artifactSha256() {
        try {
            Path p = Path.of(ReferenceEngine.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            return Files.isRegularFile(p) ? Canonical.digestOf(Files.readAllBytes(p)) : "unpacked";
        } catch (Exception e) { return "unpacked"; }
    }

    static String jcsJava() {
        try (java.io.InputStream in = ReferenceEngine.class.getResourceAsStream("/META-INF/maven/io.github.erdtman/java-json-canonicalization/pom.properties")) {
            if (in != null) { Properties pr = new Properties(); pr.load(in); String v = pr.getProperty("version"); if (v != null) return "io.github.erdtman:java-json-canonicalization:" + v; }
        } catch (IOException ignored) { /* fall through */ }
        return "io.github.erdtman:java-json-canonicalization:unknown";
    }
}
