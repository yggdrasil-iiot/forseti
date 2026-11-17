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
import yggdrasil.forseti.cli.harness.CutLog;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.harness.RunRecord;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.cli.recording.W1Form;
import yggdrasil.forseti.cli.replay.CompletionReader;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.cli.replay.RunPreparation;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;
import yggdrasil.forseti.core.input.InputParser;
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
 * One replay run of one engine recording on Flink (the harness side, plan D1, D7, D10): the recording built and checked against its
 * lock; the run's topics created and their settings observed; the generation registered by the operator CLI itself
 * ({@code generation new --store kafka} on {@code forseti.generations}); the run preparation on {@code forseti.recovery-control} (the
 * initial declaration); both jobs started, each in its own application-mode cluster, and awaited RUNNING (the stage-1 writers fix their
 * epoch in {@code initializeState}, before RUNNING); then per batch the business records and barriers written and the cut awaited from
 * the committed progress records, batch k+1 only after cut k is complete. After the last cut the memory of both clusters is read, both
 * jobs are cancelled and their JVMs stopped, and one {@code read_committed} consumer dumps every output journal into the step-3 journal
 * format in recording offsets ({@code journals/}, {@code quarantine/}), beside {@code cuts.json}, {@code run.json},
 * {@code offset-map.json} and {@code evidence.json}. The comparison with the oracle runs afterwards, on those files only.
 */
final class FlinkRun {
  static final Duration CUT_TIMEOUT = Duration.ofSeconds(180), RUNNING_TIMEOUT = Duration.ofSeconds(180), STOP_TIMEOUT = Duration.ofSeconds(60);
  /** Above the longest restart the harness waits for (plan D5); the broker's default {@code transaction.max.timeout.ms} is 15 min. */
  static final long TRANSACTION_TIMEOUT_MS = 600_000;

  record Result(Path run, Path recording, String label, String lock, int batches, int cuts, String rulesDigest, long declarationOffset, long controlEnd,
                List<String> topicProblems, Map<String, Long> dumped, ObjectNode evidence, long businessRecords, long barriers, long wallMs,
                long firstBatchAfterMs, ObjectNode memory) {}

  static Result run(String bootstrap, Path root, String label, Path scenario, Path registry, Path trustedKeys, Path work, FlinkCluster.Memory memory)
      throws IOException {
    long t0 = System.nanoTime();
    Path recDir = work.resolve("recording"), state = work.resolve("state"), out = work.resolve("run"), flink = work.resolve("flink");
    for (Path p : List.of(recDir, state, out, flink)) delete(p);
    Files.createDirectories(work);
    ScenarioBuilder.Built built = ScenarioBuilder.build(scenario, registry, recDir);
    Path lockFile = ScenarioBuilder.lockOf(scenario);
    String lock = !Files.isRegularFile(lockFile) ? "missing" : Arrays.equals(Files.readAllBytes(lockFile), Json.pretty(built.lock())) ? "equal" : "differs";
    Recording rec = Recording.open(recDir);
    rec.verifyDigests();
    rec.verifyBatches();
    JsonNode rule = Json.read(scenario).path("rule");

    // (1) the run's topics; the generation, registered by the operator CLI on forseti.generations (spec 575-577)
    String prefix = "flink-" + label + "-" + UUID.randomUUID().toString().substring(0, 8) + ".";
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

    try (KafkaGenerationStore store = new KafkaGenerationStore(bootstrap, generationsTopic, false);
         KafkaRecoveryControl control = new KafkaRecoveryControl(bootstrap, controlTopic, true);
         LocalProcessSupervisor sup = new LocalProcessSupervisor()) {
      evidence.client("generations-reader", store.readerConfig());
      long declaration = RunPreparation.prepare(store, control, 1, rec.routing().routingDigest());
      long controlEnd = control.read().endOffset();

      // (2) the run's setup, from the generation the Kafka store holds (the same rules the jobs load)
      GenerationStore.Entry registered = store.registered(1).orElseThrow(() -> new IllegalStateException("generation 1 is not registered"));
      Generation g = registered.row();
      String rowSha = store.registeredRowSha256(1).orElseThrow();
      HeadVerifier.Head head = ReferenceEngine.head(g.releaseEvidence());
      RuleSet rules;
      try {
        ReleaseReader.Release release = ReleaseReader.read(registry.resolve("mappings"), head);
        JsonNode r = g.releaseEvidence().path("pins").path("rules").path("attribution");
        rules = RuleSet.load(release, Json.text(r, "id"), Json.text(r, "version"), rec.manifest().path("definitions"));
      } catch (Refusal r) { throw new IllegalStateException("release refused: " + r.reason); }
      RoutingManifest routing = rec.routing();
      boolean occupancyRead = !(rules.attribution().mode().equals("plc-stamped") && rules.attribution().crossCheck().equals("none"));
      Map<String, List<Integer>> written = new LinkedHashMap<>();
      for (String t : ReferenceEngine.STAGE1_TOPICS) written.put(t, t.equals(InputParser.OCCUPANCY) && !occupancyRead ? List.of() : routing.subscribed(t));
      written.put(InputParser.INCIDENTS, routing.subscribed(InputParser.INCIDENTS));
      String runId = UUID.randomUUID().toString(), artifact = artifactSha256();
      Pins pins = ReferenceEngine.pins(rules, rec.manifest().path("definitions"),
          new Pins.Job(JobSettings.ENGINE, JobSettings.ENGINE_VERSION, artifact, runId, "payload", TextNode.valueOf("off"), routing.routingDigest()));
      Map<String, String> names = new LinkedHashMap<>();
      for (ReplayTopics.Spec s : plan) names.put(s.base(), s.name());

      // (3) both jobs, each in its own application-mode cluster; the first batch only once both are RUNNING
      Path jobEvidence = flink.resolve("evidence");
      List<FlinkCluster> clusters = new ArrayList<>();
      ObjectNode clusterRecords = Json.object();
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
          ObjectNode spec = Json.object().put("stage", stage).put("bootstrap", bootstrap).put("prefix", prefix);
          Json.putSafe(spec, "generation", g.generation());
          spec.put("recording", recDir.toAbsolutePath().toString()).put("mappings", registry.resolve("mappings").toAbsolutePath().toString())
              .put("runId", runId).put("artifactSha256", artifact).put("evidenceDir", jobEvidence.toAbsolutePath().toString())
              .put("checkpointDir", dir.resolve("checkpoints").toAbsolutePath().toUri().toString());
          Json.putSafe(spec, "transactionTimeoutMs", TRANSACTION_TIMEOUT_MS);
          Path specFile = dir.resolve("job-spec.json");
          Json.write(specFile, spec);
          clusters.add(FlinkCluster.start(sup, name, dir, (stage == 1 ? Stage1Job.class : Stage2Job.class).getName(), specFile, Json.text(spec, "checkpointDir"), memory));
        }
        for (FlinkCluster c : clusters) c.awaitRunning(RUNNING_TIMEOUT);
        firstBatchAfterMs = (System.nanoTime() - t0) / 1_000_000;

        // (4) the batches, each written only after the previous cut completed
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
            List<CutAssembler.Idle> idle = ReferenceEngine.idleInputs(b);
            Map<CompletionReader.Writer, JsonNode> done = await(completion, b.cutId(), CompletionReader.expected(routing, idle), clusters);
            cuts.add(cutEntry(b, done, routing, written, previous, consumed));
          }
        }
        for (FlinkCluster c : clusters) c.sampleHeap();
        for (FlinkCluster c : clusters) memoryOut.set(c.name, c.memory());
        for (FlinkCluster c : clusters) c.stop(STOP_TIMEOUT);
      } finally {
        for (FlinkCluster c : clusters) { c.close(); clusterRecords.set(c.name, c.record); }
        writer.close();
      }

      // (5) the dump, the cut log, the run record and the evidence
      Files.createDirectories(out);
      Map<String, Long> dumped;
      try (KafkaJournalDump dump = new KafkaJournalDump(bootstrap, plan, g.generation())) {
        evidence.client("journal-dump", dump.config());
        dumped = dump.dump(out, writer.offsets());
      }
      Json.write(out.resolve("cuts.json"), cutLog);
      Json.write(out.resolve("offset-map.json"), writer.offsets().toJson());
      ObjectNode digests = Json.object();
      rec.digests().forEach(digests::put);
      String manifestSha = Canonical.digestOf(Files.readAllBytes(recDir.resolve(Recording.MANIFEST)));
      ObjectNode record = RunRecord.build(new RunRecord.Facts(runId, g.generation(), rowSha, registered.scope(), ReferenceEngine.DEFAULT_SEED, false, null, null,
          pins, manifestSha, digests, head.releaseSha256(), rules.rulesDigest(), ReferenceEngine.jcsJava(), written, consumed, cuts.size(),
          "forseti gate --engine flink (" + label + ", topic prefix " + prefix + ")", null));
      RunRecord.write(out, record);
      ObjectNode flinkEvidence = Json.object();
      flinkEvidence.set("clusters", clusterRecords);
      flinkEvidence.set("memory", memoryOut);
      ObjectNode jobs = flinkEvidence.putObject("jobs");
      if (Files.isDirectory(jobEvidence)) try (Stream<Path> s = Files.list(jobEvidence)) {
        for (Path p : s.filter(x -> x.toString().endsWith(".json")).sorted().toList()) jobs.set(p.getFileName().toString().replace(".json", ""), Json.read(p));
      }
      evidence.collect("flink", flinkEvidence);
      ObjectNode ev = evidence.evidence();
      Json.write(out.resolve("evidence.json"), ev);
      long wall = (System.nanoTime() - t0) / 1_000_000;
      return new Result(out, recDir, label, lock, rec.batches().size(), cuts.size(), rules.rulesDigest(), declaration, controlEnd, topicProblems, dumped, ev,
          businessRecords, barriers, wall, firstBatchAfterMs, memoryOut);
    }
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

  /** The SHA-256 of the engine jar the jobs run from, {@code unpacked} when they run from a classes directory. */
  static String artifactSha256() {
    try {
      Path p = Path.of(JobSettings.class.getProtectionDomain().getCodeSource().getLocation().toURI());
      return Files.isRegularFile(p) ? Canonical.digestOf(Files.readAllBytes(p)) : "unpacked";
    } catch (Exception e) { return "unpacked"; }
  }

  static void delete(Path root) {
    if (!Files.exists(root)) return;
    try (Stream<Path> w = Files.walk(root)) {
      for (Path p : w.sorted(Comparator.reverseOrder()).toList()) Files.delete(p);
    } catch (IOException e) { throw new UncheckedIOException(e); }
  }
}
