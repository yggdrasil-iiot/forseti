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
import yggdrasil.forseti.cli.replay.Restarts;
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
 * One replay run of one engine recording on Flink (the harness side, plan D1, D7, D10), on a generation the gate registered before
 * any run started (spec 577; {@link #register} is the operator CLI's {@code generation new --store kafka} on the store's
 * {@code forseti.generations}): the run's own input and output topics created and every planned topic's settings observed; the run
 * preparation on the store's {@code forseti.recovery-control} (the initial declaration of its generation); both jobs started, each in
 * its own application-mode cluster, and awaited RUNNING (the stage-1 writers fix their epoch in {@code initializeState}, before RUNNING);
 * then per batch the business records and barriers written and the cut awaited from the committed progress records, batch k+1 only
 * after cut k is complete. After the last cut the memory and the checkpoint statistics of both clusters and their observed job settings
 * are read over REST, both jobs are cancelled and their JVMs stopped, and one {@code read_committed} consumer dumps every output journal
 * into the step-3 journal format in recording offsets ({@code journals/}, {@code quarantine/}), beside {@code cuts.json} (with each
 * station's {@code onlineVisits} at each cut, from the stage-1 operators' cut evidence), {@code run.json} (with {@code jobSettings}
 * beside {@code pins}), {@code offset-map.json} and {@code evidence.json}. The comparison with the oracle runs afterwards, on those files
 * only.
 */
final class FlinkRun {
  static final Duration CUT_TIMEOUT = Duration.ofSeconds(Long.getLong("forseti.flink.cutTimeoutSeconds", 600)), RUNNING_TIMEOUT = Duration.ofSeconds(180),
      STOP_TIMEOUT = Duration.ofSeconds(60);
  /** Above the longest restart the harness waits for (plan D5); the broker's default {@code transaction.max.timeout.ms} is 15 min. */
  static final long TRANSACTION_TIMEOUT_MS = 600_000;

  /** What a later run needs of this one: the run's topic names and plan, the generation and the offset map its dump translates with. */
  record Handle(String bootstrap, List<ReplayTopics.Spec> plan, Map<String, String> names, long generation, Path recording,
                yggdrasil.forseti.cli.replay.OffsetMap offsets) {}

  static ReplayEngine.Run run(String bootstrap, Path root, ReplayEngine.Request req, FlinkCluster.Memory stage1Memory, FlinkCluster.Memory stage2Memory) throws Exception {
    Restarts.Restart restart = req.restart();
    if (restart == null) return attempt(bootstrap, root, req, req.generation(), stage1Memory, stage2Memory, null, 1, Json.JSON.createArrayNode());
    // a restart run (plan D8): each attempt a fresh run on a generation of its own; an invalid attempt is recorded and repeated, and
    // after the last one the injection is an error, never a pass
    ArrayNode attempts = Json.JSON.createArrayNode();
    for (int i = 0; i < restart.attempts().size(); i++) {
      try { return attempt(bootstrap, root, req, restart.attempts().get(i), stage1Memory, stage2Memory, restart, i + 1, attempts); }
      catch (RestartInjector.Invalid e) {
        ObjectNode a = attempts.addObject().put("attempt", i + 1).put("valid", false).put("reason", e.getMessage());
        Json.putSafe(a, "generation", restart.attempts().get(i).generation());
        Path flink = req.work().resolve("flink"), kept = req.work().resolve("flink-attempt-" + (i + 1));
        delete(kept);
        if (Files.exists(flink)) Files.move(flink, kept);
        a.put("logs", kept.getFileName().toString());
      }
    }
    throw new IllegalStateException("error: injection " + restart.kind().spec() + ": " + attempts.size() + " invalid attempts, never a pass (plan D8): " + attempts);
  }

  /** One fresh run on {@code gen}: the whole run of {@link #run}, with the restart injection of {@code restart} when it is one. */
  static ReplayEngine.Run attempt(String bootstrap, Path root, ReplayEngine.Request req, ReplayEngine.Registered gen, FlinkCluster.Memory stage1Memory,
                                  FlinkCluster.Memory stage2Memory, Restarts.Restart restart, int attemptNo, ArrayNode attempts) throws Exception {
    long t0 = System.nanoTime();
    Path work = req.work(), out = work.resolve("run"), flink = work.resolve("flink");
    for (Path p : List.of(out, flink)) delete(p);
    Files.createDirectories(work);
    Recording rec = Recording.open(req.recording());
    String control = req.negativeControl() != null && Controls.engine(req.negativeControl()) ? req.negativeControl() : null;
    ReplayEngine.ControlStore cs = gen.store();
    long generation = gen.generation();

    // (1) the run's own input and output topics; forseti.generations and forseti.recovery-control are its control store's (spec 577: the
    // generation was registered there by the gate, before any run started)
    String prefix = "flink-" + req.label().replaceAll("[^a-z0-9.-]", "-") + "-" + UUID.randomUUID().toString().substring(0, 8) + ".";
    List<ReplayTopics.Spec> plan = new ArrayList<>(), create = new ArrayList<>();
    for (ReplayTopics.Spec s : ReplayTopics.plan(prefix)) {
      String shared = s.base().equals(ReplayTopics.GENERATIONS) ? cs.generationsTopic() : s.base().equals(ReplayTopics.RECOVERY_CONTROL) ? cs.recoveryControlTopic() : null;
      ReplayTopics.Spec x = shared != null ? new ReplayTopics.Spec(s.base(), shared, s.role(), s.partitions(), s.configs()) : s;
      plan.add(x);
      if (shared == null) create.add(x);
    }
    KafkaEvidenceCollector evidence = new KafkaEvidenceCollector();
    try (KafkaTopicManager admin = new KafkaTopicManager(bootstrap, "forseti-harness-admin")) {
      admin.create(create);
      evidence.topics(admin, plan);
    }
    Map<String, String> names = new LinkedHashMap<>();
    for (ReplayTopics.Spec s : plan) names.put(s.base(), s.name());
    String generationsTopic = names.get(ReplayTopics.GENERATIONS), controlTopic = names.get(ReplayTopics.RECOVERY_CONTROL);

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
      if (!rowSha.equals(gen.registeredRowSha256()))
        throw new IllegalStateException("generation " + generation + " of " + generationsTopic + " has the row " + rowSha + ", the gate registered " + gen.registeredRowSha256());
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
      Map<Integer, ObjectNode> specs = new HashMap<>();
      ObjectNode manifest = null;
      ArrayNode cutEpochs = Json.JSON.createArrayNode();
      try {
        for (int stage : List.of(2, 1)) {
          String name = "stage" + stage;
          Path dir = flink.resolve(name);
          Files.createDirectories(dir);
          ObjectNode spec = jobSpec(stage, bootstrap, prefix, names, g.generation(), mappings, rec, runId, artifact, jobEvidence, dir, control,
              req.causeKind());
          specs.put(stage, spec);
          Path specFile = specFile(dir, spec, 0);
          clusters.add(FlinkCluster.start(sup, name, dir, (stage == 1 ? Stage1Job.class : Stage2Job.class).getName(), specFile, Json.text(spec, "checkpointDir"),
              stage == 1 ? stage1Memory : stage2Memory));
        }
        for (FlinkCluster c : clusters) c.awaitRunning(RUNNING_TIMEOUT);
        firstBatchAfterMs = (System.nanoTime() - t0) / 1_000_000;
        FlinkCluster s2c = clusters.get(0), s1c = clusters.get(1);

        // the restart injection of a restart run, placed at cut k (spec 418): never the last batch, nor a bracketing batch or the one before
        RestartInjector inj = null;
        long k = -1;
        if (restart != null) {
          k = Restarts.place(rec, written.get(InputParser.MESSAGES));
          inj = new RestartInjector(restart.kind(), k, Controls.NON_TRANSACTIONAL_OUTPUT.equals(control), sup.posix(), bootstrap, prefix, g.generation(),
              routing.routingDigest(), routing, names, s1c, s2c, rc, (stage, launch) -> {
                try { return specFile(flink.resolve("stage" + stage), specs.get(stage), launch); } catch (IOException e) { throw new UncheckedIOException(e); }
              });
        }

        // (5) the batches, each written only after the previous cut completed; after a stage-1 restart injection, batch k+1 once every
        // job is RUNNING, without waiting for cut k (spec 418), as the first batch of the new restartEpoch
        List<CompletionReader.Writer> all = CompletionReader.expected(routing, List.of());
        cuts = cutLog.putArray("cuts");
        Map<String, Long> previous = new HashMap<>();
        Map<String, Long> timings = new TreeMap<>();
        try (KafkaCompletionReader completion = new KafkaCompletionReader(bootstrap, names, all, g.generation())) {
          evidence.client("batch-writer", writer.config());
          evidence.client("completion-reader", completion.config());
          RestartInjector.Epoch epoch = new RestartInjector.Epoch(0, null);
          List<Recording.Batch> pending = new ArrayList<>();
          for (Recording.Batch b : rec.batches()) {
            if (inj != null && b.cutId() == k) inj.beforeBatch();
            var w = writer.write(b, epoch.epoch(), epoch.ref());
            ObjectNode ce = Json.putSafe(Json.object(), "cutId", b.cutId());
            Json.putSafe(ce, "restartEpoch", epoch.epoch());
            ce.put("recoveryRef", epoch.ref());
            cutEpochs.add(ce);
            businessRecords += w.records();
            barriers += w.barriers();
            pending.add(b);
            if (inj != null && b.cutId() == k) {
              epoch = inj.inject(epoch, completion, timings);
              if (restart.kind().stage1) continue;   // batch k+1 next, without waiting for cut k
            }
            for (Recording.Batch x : pending) {
              List<CutAssembler.Idle> idle = EngineContract.idleInputs(x);
              Map<CompletionReader.Writer, JsonNode> done = await(completion, x.cutId(), CompletionReader.expected(routing, idle), clusters);
              cuts.add(cutEntry(x, done, routing, written, previous, consumed));
            }
            pending.clear();
          }
          if (!pending.isEmpty()) throw new IllegalStateException("cuts " + pending.size() + " never awaited");
          if (inj != null) {
            manifest = inj.finish();
            ObjectNode m = manifest;
            m.put("kind", restart.kind().spec()).put("run", req.label()).put("status", "valid");
            Json.putSafe(m, "placedAt", k);
            Json.putSafe(m, "attempt", attemptNo);
            m.set("invalidAttempts", attempts.deepCopy());
            m.put("theta", Controls.NON_TRANSACTIONAL_OUTPUT.equals(control));
            m.set("cutEpochs", cutEpochs);
            ObjectNode sw = m.putObject("stationWriters");
            new TreeMap<>(routing.stations()).forEach(sw::put);
            ObjectNode ls = m.putObject("launches");
            for (FlinkCluster c : clusters) ls.set(c.name, c.record.path("launches"));
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
      com.fasterxml.jackson.databind.node.ArrayNode dumpStats;
      try (KafkaJournalDump dump = new KafkaJournalDump(bootstrap, plan, g.generation())) {
        evidence.client("journal-dump", dump.config());
        new TreeMap<>(dump.config()).forEach((k, v) -> dumpConsumer.put(k.toString(), v.toString()));
        dumped = dump.dump(out, writer.offsets());
        dumpStats = dump.stats();
      }
      ObjectNode jobs = Json.object();
      if (Files.isDirectory(jobEvidence)) try (Stream<Path> s = Files.list(jobEvidence)) {
        for (Path p : s.filter(x -> x.toString().endsWith(".json")).sorted().toList()) jobs.set(p.getFileName().toString().replace(".json", ""), Json.read(p));
      }
      onlineVisits(cuts, jobs);
      for (String stage : List.of("stage1", "stage2")) {
        // the pins of the latest launch under the name the job settings read (every launch's pins stay under their own)
        String latest = null;
        for (Iterator<String> it = jobs.fieldNames(); it.hasNext();) { String n = it.next(); if (n.startsWith(stage + "-pins-l") && (latest == null || launchOf(n) > launchOf(latest))) latest = n; }
        if (latest != null) jobs.set(stage + "-pins", jobs.get(latest).deepCopy());
      }
      Json.write(out.resolve("cuts.json"), cutLog);
      // every cut's restartEpoch and recoveryRef as the harness wrote its barriers (spec 412), and the restart run's injection manifest
      Json.write(out.resolve("cut-epochs.json"), Json.object().<ObjectNode>set("cuts", cutEpochs));
      if (manifest != null) Json.write(out.resolve(ReplayEngine.INJECTIONS), manifest);
      Json.write(out.resolve("offset-map.json"), writer.offsets().toJson());
      ObjectNode digests = Json.object();
      rec.digests().forEach(digests::put);
      String manifestSha = Canonical.digestOf(Files.readAllBytes(req.recording().resolve(Recording.MANIFEST)));
      boolean replay = Envelope.REPLAYED.equals(req.causeKind());
      JsonNode source = rec.manifest().path("source");
      String originalSha = replay && source.path("replayOf").isTextual() ? source.path("replayOf").asText() : null;
      ObjectNode record = RunRecord.build(new RunRecord.Facts(runId, g.generation(), rowSha, entry.scope(), ReferenceEngine.DEFAULT_SEED, false,
          replay ? ReferenceEngine.EVENT_TIME : null, originalSha, pins, manifestSha, digests, head.releaseSha256(), rules.rulesDigest(), ReferenceEngine.jcsJava(),
          written, consumed, cuts.size(), "forseti gate --engine flink (" + req.label() + ", topic prefix " + prefix + ")", control));
      RunRecord.write(out, JobSettingsRecord.insert(record, JobSettingsRecord.of(observed, jobs, dumpConsumer)));
      ObjectNode flinkEvidence = Json.object();
      flinkEvidence.set("clusters", clusterRecords);
      flinkEvidence.set("journalDump", dumpStats);
      flinkEvidence.set("memory", memoryOut);
      flinkEvidence.set("checkpoints", checkpoints);
      flinkEvidence.set("observed", observed);
      flinkEvidence.set("jobs", jobs);
      ObjectNode prep = flinkEvidence.putObject("preparation");
      Json.putSafe(prep, "declarationOffset", declaration);
      Json.putSafe(prep, "controlEnd", controlEnd);
      prep.put("prefix", prefix).put("generationsTopic", generationsTopic).put("controlTopic", controlTopic);
      prep.set("controlStore", cs.toJson());
      Json.putSafe(prep, "generation", generation);
      evidence.collect("flink", flinkEvidence);
      if (manifest != null) evidence.collect("restart", manifest);
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
      Json.putSafe(summary, "dumpMs", dumpStats.findValues("elapsedMs").stream().mapToLong(JsonNode::asLong).sum());
      ObjectNode starts = summary.putObject("clusterStarts");
      clusterRecords.fields().forEachRemaining(e -> Json.putSafe(starts, e.getKey(), e.getValue().path("starts").asLong()));
      summary.set("memory", memoryOut);
      summary.set("checkpoints", checkpoints);
      summary.set("segments", segmentSizes(jobs));
      summary.set("configuredMemory", Json.object().<ObjectNode>set("stage1", stage1Memory.toJson()).set("stage2", stage2Memory.toJson()));
      if (cuts.size() != rec.batches().size()) throw new IllegalStateException("cuts " + cuts.size() + " of " + rec.batches().size() + " batches");
      if (manifest != null) {
        ObjectNode rs = summary.putObject("restart").put("kind", restart.kind().spec());
        Json.putSafe(rs, "placedAt", manifest.path("placedAt").asLong());
        Json.putSafe(rs, "attempt", attemptNo);
        ArrayNode rt = rs.putArray("relaunches");
        for (JsonNode i : manifest.path("injections")) i.path("launches").fields().forEachRemaining(e -> {
          ObjectNode x = rt.addObject().put("injection", i.path("injectionId").asText()).put("job", e.getKey());
          Json.putSafe(x, "restoredCheckpointId", e.getValue().path("restoredCheckpointId").asLong());
          Json.putSafe(x, "runningAfterLaunchMs", e.getValue().path("record").path("runningAfterLaunchMs").asLong(-1));
        });
      }
      return new ReplayEngine.Run(out, gen, declaration, controlEnd, ev, summary, new Handle(bootstrap, plan, names, g.generation(), req.recording(), writer.offsets()));
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
    spec.put("launch", 0);
    return spec;
  }

  /** The job spec of launch {@code launch} of a stage: {@code spec} with its durable launch identity, written before the launch starts
   *  ({@code job-spec.json} at the first launch, {@code job-spec-l<n>.json} after). */
  static Path specFile(Path dir, ObjectNode spec, int launch) throws IOException {
    ObjectNode s = spec.deepCopy().put("launch", launch);
    Path f = dir.resolve(launch == 0 ? "job-spec.json" : "job-spec-l" + launch + ".json");
    Json.write(f, s);
    return f;
  }

  static int launchOf(String evidenceName) {
    int i = evidenceName.lastIndexOf("-l");
    try { return Integer.parseInt(evidenceName.substring(i + 2)); } catch (RuntimeException e) { return -1; }
  }

  /** {@code generation new --store kafka} in-process with the registry's fixture clock (its head's signedAt + 60 s); the printed
   *  registration. */
  static ReplayEngine.Registered register(String bootstrap, ReplayEngine.ControlStore cs, Path state, Path trustedKeys, JsonNode rule, ReplayEngine.Registration g,
                                          Path recording) throws IOException {
    String generationsTopic = cs.generationsTopic();
    Files.createDirectories(state);
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
    return new ReplayEngine.Registered(cs, p.get("generation").longValue(), Json.text(p, "rulesDigest"), Json.text(p, "registeredRowSha256"), Json.text(p, "scope"), g.variant());
  }

  /** Comparison item 8 and the D5 head bound: the largest segment and the largest head segment either stage's station operators
   *  wrote, over every subtask (the stage-1 head carries the pending deadlines, the stage-2 head the tracker and the incidents). */
  static ObjectNode segmentSizes(ObjectNode jobs) {
    ObjectNode n = Json.object();
    for (String stage : List.of("stage1", "stage2")) {
      long largest = -1, head = -1, written = 0, bytes = 0;
      for (Iterator<Map.Entry<String, JsonNode>> it = jobs.fields(); it.hasNext();) {
        Map.Entry<String, JsonNode> e = it.next();
        if (!e.getKey().startsWith(stage + "-stations-")) continue;
        JsonNode sg = e.getValue().path("segments");
        largest = Math.max(largest, sg.path("largestBytes").asLong(-1));
        head = Math.max(head, sg.path("largestHeadBytes").asLong(-1));
        written += sg.path("written").asLong();
        bytes += sg.path("writtenBytes").asLong();
      }
      ObjectNode x = n.putObject(stage);
      Json.putSafe(x, "largestBytes", largest);
      Json.putSafe(x, "largestHeadBytes", head);
      Json.putSafe(x, "written", written);
      Json.putSafe(x, "writtenBytes", bytes);
    }
    return n;
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
