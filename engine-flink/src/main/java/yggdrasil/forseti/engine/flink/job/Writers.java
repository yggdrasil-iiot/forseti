package yggdrasil.forseti.engine.flink.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import yggdrasil.forseti.cli.replay.RecoveryControlStore;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.replay.kafka.KafkaRecoveryControl;

/** Pieces every writer operator shares: the read of {@code forseti.recovery-control} at initialization, the codec of
 *  {@code initEvidence} in operator state, and the evidence files a writer leaves for the harness. */
final class Writers {
  private Writers() {}

  /** One fresh {@code read_committed} read of the control partition up to its end offset H (spec 416): never a cached value. */
  static RecoveryControlStore.Snapshot readControl(JobSettings s) {
    try (KafkaRecoveryControl c = new KafkaRecoveryControl(s.bootstrap(), s.runName(ReplayTopics.RECOVERY_CONTROL), false)) {
      return c.read();
    }
  }

  /** The control read a recovering writer makes when a barrier of a later epoch arrives. */
  static CutAssembler.Control control(JobSettings s) {
    return () -> readControl(s).assemblerDeclaration(s.generation());
  }

  static ObjectNode initJson(Progress.InitEvidence e) {
    ObjectNode n = Json.putSafe(Json.object(), "declarationOffset", e.declarationOffset());
    Json.putSafe(n, "endOffset", e.endOffset());
    return n.put("restore", e.restore());
  }

  static Progress.InitEvidence initOf(JsonNode n) {
    if (n == null || n.isNull()) return null;
    return new Progress.InitEvidence(Json.safeLong(n.get("declarationOffset"), "declarationOffset", Json.NON_NEGATIVE),
        Json.safeLong(n.get("endOffset"), "endOffset", 1, Json.MAX_SAFE), Json.text(n, "restore"));
  }

  /** Writes {@code n} to {@code <evidenceDir>/<name>.json} atomically (the harness reads these files after the run). */
  static void evidence(JobSettings s, String name, JsonNode n) {
    Path dir = s.evidenceDir();
    try {
      Files.createDirectories(dir);
      Path tmp = dir.resolve(name + ".json.tmp");
      Files.write(tmp, Json.pretty(n));
      Files.move(tmp, dir.resolve(name + ".json"), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) { throw new UncheckedIOException(e); }
  }

  static ObjectNode origins(List<Progress.Origin> l) {
    ObjectNode n = Json.object();
    var a = n.putArray("origins");
    for (Progress.Origin o : l) a.addObject().put("topic", o.topic()).put("partition", o.partition());
    return n;
  }
}
