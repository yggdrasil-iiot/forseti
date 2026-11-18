package yggdrasil.forseti.engine.flink.gate;

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
import java.util.stream.Stream;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.harness.Controls;
import yggdrasil.forseti.cli.harness.CutLog;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.harness.RunRecord;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.cli.recording.W1Form;
import yggdrasil.forseti.cli.replay.CompletionReader;
import yggdrasil.forseti.cli.replay.EngineContract;
import yggdrasil.forseti.cli.replay.ReplayEngine;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.cli.replay.RunPreparation;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.journal.Envelope;
import yggdrasil.forseti.core.journal.Pins;
import yggdrasil.forseti.core.release.HeadVerifier;
import yggdrasil.forseti.core.release.Refusal;
import yggdrasil.forseti.core.release.ReleaseReader;
import yggdrasil.forseti.core.rules.RuleSet;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.Stage2;
import yggdrasil.forseti.engine.flink.job.JobSettings;
import yggdrasil.forseti.engine.flink.job.Stage1Job;
import yggdrasil.forseti.engine.flink.job.Stage2Job;
import yggdrasil.forseti.replay.kafka.KafkaBatchWriter;
import yggdrasil.forseti.replay.kafka.KafkaCompletionReader;
import yggdrasil.forseti.replay.kafka.KafkaEvidenceCollector;
import yggdrasil.forseti.replay.kafka.KafkaGenerationStore;
import yggdrasil.forseti.replay.kafka.KafkaJournalDump;
import yggdrasil.forseti.replay.kafka.KafkaRecoveryControl;
import yggdrasil.forseti.replay.kafka.KafkaTopicManager;
import yggdrasil.forseti.replay.kafka.LocalProcessSupervisor;

/**
 * One replay run of one engine recording on Flink (the harness side, plan D1, D7, D10): the run's topics created and their settings
 * observed; the generations of the request registered by the operator CLI itself ({@code generation new --store kafka} on
 * {@code forseti.generations}); the run preparation on {@code forseti.recovery-control} (the initial declaration); both jobs started,
 * each in its own application-mode cluster, and awaited RUNNING (the stage-1 writers fix their epoch in {@code initializeState}, before
 * RUNNING); then per batch the business records and barriers written and the cut awaited from the committed progress records, batch k+1
 * only after cut k is complete. After the last cut the memory and the checkpoint statistics of both clusters and their observed job
 * settings are read over REST, both jobs are cancelled and their JVMs stopped, and one {@code read_committed} consumer dumps every output
 * journal into the step-3 journal format in recording offsets ({@code journals/}, {@code quarantine/}), beside {@code cuts.json} (with
 * each station's {@code onlineVisits} at each cut, from the stage-1 operators' cut evidence), {@code run.json} (with {@code jobSettings}
 * beside {@code pins}), {@code offset-map.json} and {@code evidence.json}. The comparison with the oracle runs afterwards, on those files
 * only. An event-time replay ({@code base} set) runs on topics of its own but registers its generation in the base run's
 * {@code forseti.generations} and declares in its {@code forseti.recovery-control}.
 */
final class FlinkRun {
  static final Duration CUT_TIMEOUT = Duration.ofSeconds(Long.getLong("forseti.flink.cutTimeoutSeconds", 600)), RUNNING_TIMEOUT = Duration.ofSeconds(180),
      STOP_TIMEOUT = Duration.ofSeconds(60);
  /** Above the longest restart the harness waits for (plan D5); the broker's default {@code transaction.max.timeout.ms} is 15 min. */
  static final long TRANSACTION_TIMEOUT_MS = 600_000;

  /** What a later run needs of this one: the run's topic names and plan, the generation and the offset map its dump translates with. */
  record Handle(String bootstrap, List<ReplayTopics.Spec> plan, Map<String, String> names, long generation, Path recording,
                yggdrasil.forseti.cli.replay.OffsetMap offsets) {}

  static ReplayEngine.Run run(String bootstrap, Path root, ReplayEngine.Request req, FlinkCluster.Memory stage1Memory, FlinkCluster.Memory stage2Memory) throws IOException {
    long t0 = System.nanoTime();
    Path work = req.work(), state = req.state(), out = work.resolve("run"), flink = work.resolve("flink");
    for (Path p : List.of(out, flink)) delete(p);
    Files.createDirectories(work);
    Files.createDirectories(state);
    Recording rec = Recording.open(req.recording());
    JsonNode rule = Json.read(req.scenario()).path("rule");
    Handle base = req.base() == null ? null : (Handle) req.base().handle();
    String control = req.negativeControl() != null && Controls.harness(req.negativeControl()) ? req.negativeControl() : null;

    // (1) the run's topics (an event-time replay shares its base run's generation and recovery control topics)
    String prefix = "flink-" + req.label().replaceAll("[^a-z0-9.-]", "-") + "-" + UUID.randomUUID().toString().substring(0, 8) + ".";
    List<ReplayTopics.Spec> own = ReplayTopics.plan(prefix), plan = new ArrayList<>(), create = new ArrayList<>();
    for (ReplayTopics.Spec s : own) {
      boolean shared = base != null && (s.base().equals(ReplayTopics.GENERATIONS) || s.base().equals(ReplayTopics.RECOVERY_CONTROL));
      ReplayTopics.Spec x = shared ? new ReplayTopics.Spec(s.base(), base.names().get(s.base()), s.role(), s.partitions(), s.configs()) : s;
      plan.add(x);
      if (!shared) create.add(x);
    }
    KafkaEvidenceCollector evidence = new KafkaEvidenceCollector();
    try (KafkaTopicManager admin = new KafkaTopicManager(bootstrap, "forseti-harness-admin")) {
      admin.create(create);
      evidence.topics(admin, plan);
    }
    Map<String, String> names = new LinkedHashMap<>();
    for (ReplayTopics.Spec s : plan) names.put(s.base(), s.name());
    String generationsTopic = names.get(ReplayTopics.GENERATIONS), controlTopic = names.get(ReplayTopics.RECOVERY_CONTROL);

    // (2) the generations of the request, registered by the operator CLI on forseti.generations (spec 575-577), the run's last
    Path registerWith = base != null ? base.recording() : req.recording();
    List<ReplayEngine.Registered> registered = new ArrayList<>();
    for (ReplayEngine.Registration g : req.registrations()) registered.add(register(bootstrap, generationsTopic, state, req.trustedKeys(), rule, g, registerWith));
    long generation = registered.get(registered.size() - 1).generation();

    try (KafkaGenerationStore store = new KafkaGenerationStore(bootstrap, generationsTopic, false);
         KafkaRecoveryControl rc = new KafkaRecoveryControl(bootstrap, controlTopic, true);
         LocalProcessSupervisor sup = new LocalProcessSupervisor()) {
      evidence.client("generations-reader", store.readerConfig());
      long declaration = RunPreparation.prepare(store, rc, generation, rec.routing().routingDigest());
      long controlEnd = rc.read().endOffset();

      // (3) the run's setup, from the generation the Kafka store holds (the same rules the jobs load)
      GenerationStore.Entry entry = store.registered(generation).orElseThrow(() -> new IllegalStateException("generation " + generation + " is not registered"));
      Generation g = entry.row();
      String rowSha = store.registeredRowSha256(generation).orElseThrow();
      HeadVerifier.Head head = EngineContract.head(g.releaseEvidence());
      Path mappings = req.registry().resolve("mappings");
      RuleSet rules;
      try {
        ReleaseReader.Release release = ReleaseReader.read(mappings, head);
        JsonNode r = g.releaseEvidence().path("pins").path("rules").path("attribution");
        rules = RuleSet.load(release, Json.text(r, "id"), Json.text(r, "version"), rec.manifest().path("definitions"));
      } catch (Refusal r) { throw new IllegalStateException("release refused: " + r.reason); }
      RoutingManifest routing = rec.routing();
      boolean occupancyRead = !(rules.attribution().mode().equals("plc-stamped") && rules.attribution().crossCheck().equals("none"));
      Map<String, List<Integer>> written = new LinkedHashMap<>();
      for (String t : EngineContract.STAGE1_TOPICS) written.put(t, t.equals(InputParser.OCCUPANCY) && !occupancyRead ? List.of() : routing.subscribed(t));
      written.put(InputParser.INCIDENTS, routing.subscribed(InputParser.INCIDENTS));
      String runId = UUID.randomUUID().toString(), artifact = JobSettings.engineArtifact();
      Pins pins = EngineContract.pins(rules, rec.manifest().path("definitions"),
          new Pins.Job(JobSettings.ENGINE, JobSettings.ENGINE_VERSION, artifact, runId, "payload", TextNode.valueOf("off"), routing.routingDigest()));

      // (4) both jobs, each in its own application-mode cluster; the first batch only once both are RUNNING
      Path jobEvidence = flink.resolve("evidence");
      List<FlinkCluster> clusters = new ArrayList<>();
      ObjectNode clusterRecords = Json.object(), observed = Json.object(), checkpoints = Json.object();
      long businessRecords = 0, barriers = 0, firstBatchAfterMs;
      ArrayNode cuts;
      ObjectNode cutLog = Json.putSafe(Json.object(), "generation", g.generation()).put("routingDigest", routing.routingDigest()).put("w1Form", W1Form.MIN_OVER_PARTITIONS.wire);
      Map<String, Map<Integer, CutLog.Range>> consumed = new TreeMap<>();
      ObjectNode memoryOut = Json.object();
      KafkaBatchWriter writer = new KafkaBatchWriter(bootstrap, KafkaBatchWriter.inputNames(plan), rec, g.generation(), written);
      try {
        for (int stage : List.of(2, 1)) {
          String name = "stage" + stage;
          Path dir = flink.resolve(name);
          Files.createDirectories(dir);
          ObjectNode spec = jobSpec(stage, bootstrap, prefix, names, g.generation(), mappings, rec, runId, artifact, jobEvidence, dir, control,
              req.causeKind());
          Path specFile = dir.resolve("job-spec.json");
          Json.write(specFile, spec);
          clusters.add(FlinkCluster.start(sup, name, dir, (stage == 1 ? Stage1Job.class : Stage2Job.class).getName(), specFile, Json.text(spec, "checkpointDir"),
              stage == 1 ? stage1Memory : stage2Memory));
        }
        for (FlinkCluster c : clusters) c.awaitRunning(RUNNING_TIMEOUT);
        firstBatchAfterMs = (System.nanoTime() - t0) / 1_000_000;

        // (5) the batches, each written only after the previous cut completed
        List<CompletionReader.Writer> all = CompletionReader.expected(routing, List.of());
        cuts = cutLog.putArray("cuts");
        Map<String, Long> previous = new HashMap<>();
        try (KafkaCompletionReader completion = new KafkaCompletionReader(bootstrap, names, all, g.generation())) {
          evidence.client("batch-writer", writer.config());
          evidence.client("completion-reader", completion.config());
          for (Recording.Batch b : rec.batches()) {
            var w = writer.write(b, 0, null);
            businessRecords += w.records();
            barriers += w.barriers();
            List<CutAssembler.Idle> idle = EngineContract.idleInputs(b);
            Map<CompletionReader.Writer, JsonNode> done = await(completion, b.cutId(), CompletionReader.expected(routing, idle), clusters);
            cuts.add(cutEntry(b, done, routing, written, previous, consumed));
          }
        }
        for (FlinkCluster c : clusters) {
          c.sampleHeap();
          memoryOut.set(c.name, c.memory());
          observed.set(c.name, c.observedSettings());
          checkpoints.set(c.name, c.checkpointStatistics());
        }
        for (FlinkCluster c : clusters) c.stop(STOP_TIMEOUT);
      } finally {
        for (FlinkCluster c : clusters) { c.close(); clusterRecords.set(c.name, c.record); }
        writer.close();
      }

      // (6) the dump, the cut log, the run record and the evidence
      Files.createDirectories(out);
      Map<String, Long> dumped;
      ObjectNode dumpConsumer = Json.object();
      try (KafkaJournalDump dump = new KafkaJournalDump(bootstrap, plan, g.generation())) {
        evidence.client("journal-dump", dump.config());
        new TreeMap<>(dump.config()).forEach((k, v) -> dumpConsumer.put(k.toString(), v.toString()));
        dumped = dump.dump(out, writer.offsets());
      }
      ObjectNode jobs = Json.object();
      if (Files.isDirectory(jobEvidence)) try (Stream<Path> s = Files.list(jobEvidence)) {
        for (Path p : s.filter(x -> x.toString().endsWith(".json")).sorted().toList()) jobs.set(p.getFileName().toString().replace(".json", ""), Json.read(p));
      }
      onlineVisits(cuts, jobs);
      Json.write(out.resolve("cuts.json"), cutLog);
      Json.write(out.resolve("offset-map.json"), writer.offsets().toJson());
      ObjectNode digests = Json.object();
      rec.digests().forEach(digests::put);
      String manifestSha = Canonical.digestOf(Files.readAllBytes(req.recording().resolve(Recording.MANIFEST)));
      boolean replay = Envelope.REPLAYED.equals(req.causeKind());
      String originalSha = replay && base != null ? Canonical.digestOf(Files.readAllBytes(base.recording().resolve(Recording.MANIFEST))) : null;
      ObjectNode record = RunRecord.build(new RunRecord.Facts(runId, g.generation(), rowSha, entry.scope(), ReferenceEngine.DEFAULT_SEED, false,
          replay ? ReferenceEngine.EVENT_TIME : null, originalSha, pins, manifestSha, digests, head.releaseSha256(), rules.rulesDigest(), ReferenceEngine.jcsJava(),
          written, consumed, cuts.size(), "forseti gate --engine flink (" + req.label() + ", topic prefix " + prefix + ")", control));
      RunRecord.write(out, JobSettingsRecord.insert(record, JobSettingsRecord.of(observed, jobs, dumpConsumer)));
      ObjectNode flinkEvidence = Json.object();
      flinkEvidence.set("clusters", clusterRecords);
      flinkEvidence.set("memory", memoryOut);
      flinkEvidence.set("checkpoints", checkpoints);
      flinkEvidence.set("observed", observed);
      flinkEvidence.set("jobs", jobs);
      ObjectNode prep = flinkEvidence.putObject("preparation");
      Json.putSafe(prep, "declarationOffset", declaration);
      Json.putSafe(prep, "controlEnd", controlEnd);
      prep.put("prefix", prefix).put("generationsTopic", generationsTopic).put("controlTopic", controlTopic);
      evidence.collect("flink", flinkEvidence);
      ObjectNode ev = evidence.evidence();
      Json.write(out.resolve("evidence.json"), ev);
      long wall = (System.nanoTime() - t0) / 1_000_000;
      ObjectNode summary = Json.object().put("label", req.label());
      Json.putSafe(summary, "batches", rec.batches().size());
      Json.putSafe(summary, "cuts", cuts.size());
      Json.putSafe(summary, "businessRecords", businessRecords);
      Json.putSafe(summary, "barriers", barriers);
      Json.putSafe(summary, "wallMs", wall);
      Json.putSafe(summary, "firstBatchAfterMs", firstBatchAfterMs);
      Json.putSafe(summary, "dumpedRecords", dumped.values().stream().mapToLong(Long::longValue).sum());
      summary.set("memory", memoryOut);
      summary.set("checkpoints", checkpoints);
      summary.set("configuredMemory", Json.object().<ObjectNode>set("stage1", stage1Memory.toJson()).set("stage2", stage2Memory.toJson()));
      if (cuts.size() != rec.batches().size()) throw new IllegalStateException("cuts " + cuts.size() + " of " + rec.batches().size() + " batches");
      return new ReplayEngine.Run(out, registered, ev, summary, new Handle(bootstrap, plan, names, g.generation(), req.recording(), writer.offsets()));
    }
  }

  /** The harness's job spec of one stage: no recording path (reviewer decision 3), the definitions, the routing manifest and the
   *  evidence ranges in the spec itself, the run's topic names, the engine path digest the job verifies at start (decision 2), the
   *  negative control the jobs apply and the cause kind of the rows. */
  static ObjectNode jobSpec(int stage, String bootstrap, String prefix, Map<String, String> names, long generation, Path mappings, Recording rec, String runId,
                            String artifact, Path jobEvidence, Path dir, String control, String causeKind) {
    ObjectNode spec = Json.object().put("stage", stage).put("bootstrap", bootstrap).put("prefix", prefix);
    ObjectNode topics = spec.putObject("topics");
    names.forEach(topics::put);
    Json.putSafe(spec, "generation", generation);
    spec.put("mappings", mappings.toAbsolutePath().toString());
    spec.set("definitions", rec.manifest().path("definitions"));
    spec.set("routing", rec.routing().node());
    ObjectNode er = spec.putObject("evidenceRanges");
    rec.evidenceRange().forEach((d, r) -> er.putArray(d).add(r.from()).add(r.to()));
    spec.put("runId", runId).put("artifactSha256", artifact).put("evidenceDir", jobEvidence.toAbsolutePath().toString())
        .put("checkpointDir", dir.resolve("checkpoints").toAbsolutePath().toUri().toString());
    if (control == null) spec.putNull("negativeControl"); else spec.put("negativeControl", control);
    spec.put("causeKind", causeKind == null ? Envelope.ONLINE : causeKind);
    Json.putSafe(spec, "transactionTimeoutMs", TRANSACTION_TIMEOUT_MS);
    return spec;
  }

  /** {@code generation new --store kafka} in-process with the registry's fixture clock (its head's signedAt + 60 s); the printed
   *  registration. */
  static ReplayEngine.Registered register(String bootstrap, String generationsTopic, Path state, Path trustedKeys, JsonNode rule, ReplayEngine.Registration g,
                                          Path recording) {
    String stream = rule.path("stream").asText();
    Path mappings = g.registry().resolve("mappings");
    JsonNode statement = Json.read(Base64.getDecoder().decode(Json.read(mappings.resolve("head-attestations/" + stream + ".dsse.json")).get("payload").asText()));
    Clock clock = Clock.fixed(Instant.parse(statement.path("predicate").path("signedAt").asText()).plusSeconds(60), ZoneOffset.UTC);
    String r = g.rule() != null ? g.rule() : rule.path("attributionRuleId").asText() + "@" + rule.path("attributionRuleVersion").asText();
    ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
    int exit = GenerationCli.run(new String[] {"new", "--store", "kafka", "--bootstrap", bootstrap, "--generations-topic", generationsTopic,
        "--state", state.toString(), "--mappings", mappings.toString(), "--trusted-keys", trustedKeys.toString(),
        "--stream", stream, "--rule", r, "--recording", recording.toString()},
        new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8), clock);
    String printed = o.toString(StandardCharsets.UTF_8).strip();
    if (exit != 0) throw new IllegalStateException("generation new --store kafka exit " + exit + ": " + printed + e.toString(StandardCharsets.UTF_8));
    JsonNode p = Json.read(printed.lines().reduce((a, b) -> b).orElse("").getBytes(StandardCharsets.UTF_8));
    return new ReplayEngine.Registered(p.get("generation").longValue(), Json.text(p, "rulesDigest"), Json.text(p, "registeredRowSha256"), Json.text(p, "scope"));
  }

  /** Each station's {@code onlineVisits} at each cut it completed, from the stage-1 operators' cut evidence
   *  ({@code Stage1State.onlineVisits()} at the close: cut-specific operator evidence, never derived from journal rows). */
  static void onlineVisits(ArrayNode cuts, ObjectNode jobs) {
    Map<String, Long> at = new HashMap<>();
    jobs.fields().forEachRemaining(e -> {
      if (!e.getKey().startsWith("stage1-stations-")) return;
      for (JsonNode c : e.getValue().path("cuts")) at.put(c.path("station").asText() + "\u0000" + c.path("cutId").asLong(), c.path("onlineVisits").asLong());
    });
    for (JsonNode c : cuts) {
      long id = c.path("cutId").asLong();
      c.path("perStation").fields().forEachRemaining(s -> {
        Long v = at.get(s.getKey() + "\u0000" + id);
        if (v != null) Json.putSafe((ObjectNode) s.getValue(), "onlineVisits", v);
      });
    }
  }

  /** Dumps the journals of {@code h}'s generation again into {@code out} with {@code read_committed}. */
  static void redump(Handle h, Path out) {
    try (KafkaJournalDump dump = new KafkaJournalDump(h.bootstrap(), h.plan(), h.generation())) { dump.dump(out, h.offsets()); }
  }

  /** Waits for the cut in slices, checking between them that both jobs are still RUNNING, so a failed job ends the wait at once. */
  private static Map<CompletionReader.Writer, JsonNode> await(KafkaCompletionReader completion, long cutId, List<CompletionReader.Writer> expected,
                                                             List<FlinkCluster> clusters) {
    long until = System.nanoTime() + CUT_TIMEOUT.toNanos();
    while (true) {
      try { return completion.await(cutId, expected, Duration.ofSeconds(2)); }
      catch (IllegalStateException timeout) {
        for (FlinkCluster c : clusters) c.requireAlive();
        for (FlinkCluster c : clusters) c.sampleHeap();
        if (System.nanoTime() > until) throw new IllegalStateException(timeout.getMessage() + " (within " + CUT_TIMEOUT + ")", timeout);
      }
    }
  }

  /** One cut of {@code cuts.json}, in recording offsets: the stations whose stage-1 progress closed this very cut, with the W1 and
   *  admissionW of that progress and the W2 of the stage-2 one; the batch manifest's half-open offset ranges; its incident offsets. */
  static ObjectNode cutEntry(Recording.Batch b, Map<CompletionReader.Writer, JsonNode> done, RoutingManifest routing, Map<String, List<Integer>> written,
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

  static void delete(Path root) {
    if (!Files.exists(root)) return;
    try (Stream<Path> w = Files.walk(root)) {
      for (Path p : w.sorted(Comparator.reverseOrder()).toList()) Files.delete(p);
    } catch (IOException e) { throw new UncheckedIOException(e); }
  }
}
