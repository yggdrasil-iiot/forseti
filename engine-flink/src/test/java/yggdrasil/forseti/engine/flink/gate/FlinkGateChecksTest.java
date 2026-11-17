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

/** The checks of the Flink smoke gate on hand-made journals and evidence, and the provider's discovery on the engine path. */
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
    String first = new RestoreEvidence(false, null, 0).encode();
    assertEquals(List.of(), FlinkGate.initEvidence(run(init(0, 2, first), null, null), 0, 2));

    List<String> missing = FlinkGate.initEvidence(run(null, null, null), 0, 2);
    assertEquals(4, missing.size(), missing.toString());
    assertTrue(FlinkGate.initEvidence(run(init(0, 2, first), init(0, 2, first), null), 0, 2).stream().anyMatch(x -> x.contains("progress 2 carries initEvidence")));
    assertFalse(FlinkGate.initEvidence(run(init(0, 2, first), null, init(0, 2, first)), 0, 2).isEmpty(), "a stage-2 progress carries none");
    assertFalse(FlinkGate.initEvidence(run(init(1, 2, first), null, null), 0, 2).isEmpty(), "another declaration");
    assertFalse(FlinkGate.initEvidence(run(init(0, 3, first), null, null), 0, 2).isEmpty(), "another H");
    assertFalse(FlinkGate.initEvidence(run(init(0, 2, "none"), null, null), 0, 2).isEmpty(), "the reference harness's restore");
    assertFalse(FlinkGate.initEvidence(run(init(0, 2, new RestoreEvidence(true, 4L, 1).encode()), null, null), 0, 2).isEmpty(), "a restore at a first start");
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

  @Test void theProviderIsFoundByNameAndOffersOnlyTheGate() {
    Optional<EngineProviders.Loaded<EngineProvider>> p = EngineProviders.find("flink", List.of());
    assertTrue(p.isPresent(), "the flink provider on the class path");
    assertEquals(FlinkEngineProvider.class.getName(), p.get().provider().getClass().getName());
    PrintStream sink = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
    assertEquals(2, p.get().provider().run(new String[] {"run", "--engine", "flink"}, sink, sink));
    assertEquals(2, p.get().provider().requirements(new String[] {"requirements", "--engine", "flink"}, sink, sink));
    assertEquals(2, p.get().provider().gate(new String[] {"gate", "--engine", "flink", "--shard", "smoke"}, sink, sink), "an unknown flag is a usage error");
  }
}
