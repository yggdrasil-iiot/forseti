package yggdrasil.forseti.engine.flink.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import java.util.Set;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/**
 * The engine's restore evidence of a stage-1 writer initialization (spec 416: {@code initEvidence.restore}), Flink's canonical encoding:
 * the {@code restore} string is the RFC 8785 text of {@code {engine: "flink", isRestored, restoredCheckpointId, attemptNumber}}, read
 * from the writer's {@code StateInitializationContext} ({@code isRestored()}, {@code getRestoredCheckpointId()}, null when none) and
 * its task's execution attempt. The progress record's shape is unchanged ({@code restore} stays a string, {@code "none"} in the step-3
 * reference harness); {@link #decode} reads it back strictly.
 */
public record RestoreEvidence(boolean isRestored, Long restoredCheckpointId, int attemptNumber) {
  private static final Set<String> FIELDS = Set.of("engine", "isRestored", "restoredCheckpointId", "attemptNumber");

  public RestoreEvidence {
    if (attemptNumber < 0) throw new IllegalArgumentException("attemptNumber " + attemptNumber);
    if (isRestored != (restoredCheckpointId != null)) throw new IllegalArgumentException("a restored writer names its checkpoint, a fresh one none");
  }

  public static RestoreEvidence of(boolean isRestored, OptionalLong checkpoint, int attemptNumber) {
    return new RestoreEvidence(isRestored, checkpoint.isPresent() ? checkpoint.getAsLong() : null, attemptNumber);
  }

  /** Whether the writer is reinitialized (spec 416: any initialization after its first start in the generation). */
  public boolean reinitialized() { return isRestored || attemptNumber > 0; }

  public String encode() {
    ObjectNode n = Json.object().put("engine", JobSettings.ENGINE).put("isRestored", isRestored);
    if (restoredCheckpointId == null) n.putNull("restoredCheckpointId"); else Json.putSafe(n, "restoredCheckpointId", restoredCheckpointId);
    n.put("attemptNumber", attemptNumber);
    return Canonical.utf8(Canonical.bytes(n));
  }

  public static RestoreEvidence decode(String s) {
    JsonNode n = Json.read(s.getBytes(StandardCharsets.UTF_8));
    if (!n.isObject() || n.size() != FIELDS.size()) throw new IllegalArgumentException("flink restore evidence has exactly " + FIELDS);
    n.fieldNames().forEachRemaining(f -> { if (!FIELDS.contains(f)) throw new IllegalArgumentException("flink restore evidence field " + f); });
    if (!JobSettings.ENGINE.equals(n.path("engine").textValue())) throw new IllegalArgumentException("restore evidence of engine " + n.path("engine"));
    if (!n.path("isRestored").isBoolean()) throw new IllegalArgumentException("isRestored is a boolean");
    JsonNode c = n.get("restoredCheckpointId");
    RestoreEvidence e = new RestoreEvidence(n.get("isRestored").booleanValue(), c.isNull() ? null : Json.safeLong(c, "restoredCheckpointId", 1, Json.MAX_SAFE),
        (int) Json.safeLong(n.get("attemptNumber"), "attemptNumber", 0, Integer.MAX_VALUE));
    if (!e.encode().equals(s)) throw new IllegalArgumentException("restore evidence is not its canonical text");
    return e;
  }
}
