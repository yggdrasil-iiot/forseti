package yggdrasil.forseti.engine.flink.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.replay.EngineProvider;
import yggdrasil.forseti.cli.replay.EngineProviders;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.engine.flink.job.RestoreEvidence;

/** The checks of the Flink gate on hand-made journals and evidence, and the provider's discovery on the engine path. */
class FlinkGateChecksTest {
  @TempDir Path tmp;

  private static String progress(long cutId, String init) {
    return "{\"key\":null,\"value\":{\"cutId\":" + cutId + (init == null ? "" : ",\"initEvidence\":" + init) + ",\"type\":\"progress\"}}";
  }

  private void journal(Path run, String topic, int p, String... lines) throws Exception {
    Path f = run.resolve("journals").resolve(topic).resolve(p + ".jsonl");
    Files.createDirectories(f.getParent());
    Files.writeString(f, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
  }

  private Path run(String firstInit, String secondInit, String stage2Init) throws Exception {
    Path run = tmp.resolve("run-" + System.nanoTime());
    for (String t : List.of("forseti.work-records", "forseti.samples", "forseti.dispositions-stage1"))
      journal(run, t, 0, progress(1, firstInit), "{\"key\":{},\"value\":{\"body\":{}}}", progress(2, secondInit));
    journal(run, "forseti.dispositions-stage1", 6, progress(1, firstInit), progress(2, null));
    for (String t : List.of("forseti.affected", "forseti.affected-sets", "forseti.dispositions-stage2"))
      journal(run, t, 0, progress(1, stage2Init), progress(2, null));
    return run;
  }

  private static String init(long decl, long end, String restore) {
    return "{\"declarationOffset\":" + decl + ",\"endOffset\":" + end + ",\"restore\":" + Json.JSON.valueToTree(restore) + "}";
  }

  @Test void theFirstProgressOfEveryStageOneWriterCarriesTheRunsInitEvidenceAndNothingElseDoes() throws Exception {
    String first = RestoreEvidence.firstStart().encode();
    assertEquals(List.of(), FlinkGate.initEvidence(run(init(0, 2, first), null, null), 0, 2));

    List<String> missing = FlinkGate.initEvidence(run(null, null, null), 0, 2);
    assertEquals(4, missing.size(), missing.toString());
    assertTrue(FlinkGate.initEvidence(run(init(0, 2, first), init(0, 2, first), null), 0, 2).stream().anyMatch(x -> x.contains("progress 2 carries initEvidence")));
    assertFalse(FlinkGate.initEvidence(run(init(0, 2, first), null, init(0, 2, first)), 0, 2).isEmpty(), "a stage-2 progress carries none");
    assertFalse(FlinkGate.initEvidence(run(init(1, 2, first), null, null), 0, 2).isEmpty(), "another declaration");
    assertFalse(FlinkGate.initEvidence(run(init(0, 3, first), null, null), 0, 2).isEmpty(), "another H");
    assertFalse(FlinkGate.initEvidence(run(init(0, 2, "none"), null, null), 0, 2).isEmpty(), "the reference harness's restore");
    assertFalse(FlinkGate.initEvidence(run(init(0, 2, new RestoreEvidence(0, true, 4L, 1).encode()), null, null), 0, 2).isEmpty(), "a restore at a first start");
  }

  @Test void everyRecordedConsumerReadsCommitted() {
    ObjectNode ev = Json.object();
    ev.putObject("clients").putObject("journal-dump").put("isolation.level", "read_committed");
    ObjectNode jobs = ev.putObject("flink").putObject("jobs");
    jobs.putObject("stage1-pins").putObject("sources").putObject("ot.messages").put("isolation.level", "read_committed");
    jobs.putObject("stage2-pins").putObject("sources").putObject("quality.incidents").put("isolation.level", "read_committed");
    assertEquals(List.of(), FlinkGate.readCommitted(ev));
    ((ObjectNode) jobs.path("stage2-pins").path("sources").path("quality.incidents")).put("isolation.level", "read_uncommitted");
    assertEquals(List.of("stage2-pins/quality.incidents"), FlinkGate.readCommitted(ev));
    jobs.remove("stage1-pins");
    assertTrue(FlinkGate.readCommitted(ev).contains("stage1-pins (no job pins recorded)"));
  }

  @Test void theProviderIsFoundByNameAndOffersTheGateAndTheRequirements() {
    Optional<EngineProviders.Loaded<EngineProvider>> p = EngineProviders.find("flink", List.of());
    assertTrue(p.isPresent(), "the flink provider on the class path");
    assertEquals(FlinkEngineProvider.class.getName(), p.get().provider().getClass().getName());
    PrintStream sink = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
    assertEquals(2, p.get().provider().run(new String[] {"run", "--engine", "flink"}, sink, sink));
    assertEquals(2, p.get().provider().requirements(new String[] {"requirements", "--engine", "flink"}, sink, sink), "--gate-report is required");
    assertEquals(2, p.get().provider().requirements(new String[] {"requirements", "--engine", "flink", "--shard", "x"}, sink, sink), "an unknown flag is a usage error");
    assertEquals(2, p.get().provider().gate(new String[] {"gate", "--engine", "flink", "--no-such-flag", "x"}, sink, sink), "an unknown flag is a usage error");
  }

  @Test void eachStationsOnlineVisitsComeFromItsOperatorsCutEvidence() {
    var cuts = Json.JSON.createArrayNode();
    ObjectNode c1 = cuts.addObject().put("cutId", 1);
    c1.putObject("perStation").putObject("s1").put("W1", 5);
    ObjectNode c2 = cuts.addObject().put("cutId", 2);
    c2.putObject("perStation").putObject("s1").put("W1", 6);
    ObjectNode jobs = Json.object();
    var ev = jobs.putObject("stage1-stations-3-attempt-0").putArray("cuts");
    ev.addObject().put("station", "s1").put("cutId", 1).put("onlineVisits", 2);
    FlinkRun.onlineVisits(cuts, jobs);
    assertEquals(2, c1.path("perStation").path("s1").path("onlineVisits").asInt());
    assertFalse(c2.path("perStation").path("s1").has("onlineVisits"), "a cut the operator recorded nothing for gets nothing");
  }

  @Test void jobSettingsFollowPinsAndHoldOnlyRunInvariantValues() {
    ObjectNode record = Json.object().put("runId", "r");
    record.putObject("pins").putObject("job").put("runId", "r");
    record.put("fixtureDigests", "x");
    ObjectNode observed = Json.object();
    ObjectNode s1 = observed.putObject("stage1");
    s1.putObject("checkpointConfig").put("mode", "EXACTLY_ONCE").put("interval", 1000).put("max_concurrent", 1);
    s1.putObject("configuration").put("execution.checkpointing.incremental", "true").put("state.backend.type", "rocksdb");
    s1.putObject("vertices").put("Source: stage1-source-ot.messages -> split-probe-stage1-source-ot.messages", 4);
    ObjectNode jobs = Json.object();
    ObjectNode pins = jobs.putObject("stage1-pins").put("engine", "flink").put("engineVersion", "2.2.1").put("watermarkEmission", "m").put("idleness", "off");
    pins.putObject("sources").putObject("ot.messages").put("isolation.level", "read_committed").put("group.id", "variant").put("client.id.prefix", "variant");
    pins.putObject("sinks").putObject("stage1-stations").put("deliveryGuarantee", "EXACTLY_ONCE").put("client.id", "variant").put("transactionalIdPrefix", "variant");
    jobs.putObject("stage1-stations-0-attempt-0").put("parallelism", 6).put("maxParallelism", 128);
    ObjectNode dump = Json.object().put("isolation.level", "read_committed").put("client.id", "variant").put("bootstrap.servers", "variant");
    ObjectNode settings = JobSettingsRecord.of(observed, jobs, dump);
    ObjectNode out = JobSettingsRecord.insert(record, settings);
    List<String> names = new java.util.ArrayList<>();
    out.fieldNames().forEachRemaining(names::add);
    assertEquals(List.of("runId", "pins", "jobSettings", "fixtureDigests"), names);
    assertFalse(settings.toString().contains("variant"), settings.toString());
    assertEquals(4, settings.path("stage1").path("sourceParallelism").path("ot.messages").asInt());
    assertEquals(1000, settings.path("stage1").path("checkpointing").path("intervalMs").asInt());
    assertEquals(6, settings.path("stage1").path("keyedParallelism").asInt());
    assertEquals("read_committed", settings.path("dumpConsumer").path("isolation.level").asText());
  }

  /** A run's own topics (the unsharded gate deletes them after the run): its inputs and outputs of every attempt, under the prefix
   *  its label gives; never a control store's, nor those of another run whose label starts with this one's. */
  @Test void aRunsOwnTopicsAreItsInputsAndOutputsOfEveryAttempt() {
    String a = "flink-smoke-0a1b2c3d.", b = "flink-smoke-deadbeef.";
    List<String> topics = new java.util.ArrayList<>();
    for (yggdrasil.forseti.cli.replay.ReplayTopics.Spec s : yggdrasil.forseti.cli.replay.ReplayTopics.plan(a)) topics.add(s.name());
    topics.add(b + "ot.messages");
    topics.addAll(List.of("flink-smoke-plc-0a1b2c3d.ot.messages", "flink-store-pipeline-0a1b2c3d.forseti.generations", "flink-smoke-0A1B2C3D.ot.messages",
        "flink-smoke-0a1b2c3.ot.messages", "flink-smoke-0a1b2c3d.forseti.lineage"));
    List<String> own = FlinkRun.ownTopics("smoke", topics);
    // 4 inputs and 7 outputs of the first attempt, the second attempt's input; its control topics are the store's (spec 577)
    assertEquals(12, own.size(), own.toString());
    assertTrue(own.contains(b + "ot.messages"));
    assertFalse(own.contains(a + "forseti.generations"));
    assertFalse(own.contains(a + "forseti.recovery-control"));
    assertFalse(own.stream().anyMatch(t -> t.startsWith("flink-smoke-plc-") || t.startsWith("flink-store-")), own.toString());
    assertEquals("flink-dedicated-baseline-", FlinkRun.labelPrefix("dedicated/baseline"));
    assertEquals("flink-horizon-planned-late-replay-", FlinkRun.labelPrefix("horizon-planned-late@replay"));
    assertEquals(List.of(), FlinkRun.ownTopics("horizon-planned-late", List.of("flink-horizon-planned-late-replay-0a1b2c3d.ot.messages")));
  }
}
