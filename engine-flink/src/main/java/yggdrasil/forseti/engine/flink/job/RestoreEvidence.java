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
 * the {@code restore} string is the RFC 8785 text of {@code {engine: "flink", launch, isRestored, restoredCheckpointId, attemptNumber}},
 * read from the writer's {@code StateInitializationContext} ({@code isRestored()}, {@code getRestoredCheckpointId()}, null when none),
 * its task's execution attempt and the job's {@code launch}: the durable launch identity the harness gives each launch of the job's
 * cluster in its job spec and records before the start (0 at the first launch, plus one at every relaunch of an injection). The progress
 * record's shape is unchanged ({@code restore} stays a string, {@code "none"} in the step-3 reference harness); {@link #decode} reads it
 * back strictly.
 *
 * <p>A writer is reinitialized (spec 416: any (re)initialization of its task after its first start in the generation, whether or not a
 * checkpoint had anything to restore) when its job is a relaunch ({@code launch > 0}), its state was restored, or its task is a later
 * execution attempt. The attempt number alone would miss a fresh JVM launch without restored state, whose attempt starts at 0 again:
 * the launch identity is what tells it apart (reviewer decision 3 of PR 5).
 */
public record RestoreEvidence(int launch, boolean isRestored, Long restoredCheckpointId, int attemptNumber) {
  private static final Set<String> FIELDS = Set.of("engine", "launch", "isRestored", "restoredCheckpointId", "attemptNumber");

  public RestoreEvidence {
    if (launch < 0) throw new IllegalArgumentException("launch " + launch);
    if (attemptNumber < 0) throw new IllegalArgumentException("attemptNumber " + attemptNumber);
    if (isRestored != (restoredCheckpointId != null)) throw new IllegalArgumentException("a restored writer names its checkpoint, a fresh one none");
  }

  /** The restore evidence of a first start: launch 0, nothing restored, attempt 0. */
  public static RestoreEvidence firstStart() { return new RestoreEvidence(0, false, null, 0); }

  public static RestoreEvidence of(int launch, boolean isRestored, OptionalLong checkpoint, int attemptNumber) {
    return new RestoreEvidence(launch, isRestored, checkpoint.isPresent() ? checkpoint.getAsLong() : null, attemptNumber);
  }

  /** Whether the writer is reinitialized (spec 416: any initialization after its first start in the generation). */
  public boolean reinitialized() { return launch > 0 || isRestored || attemptNumber > 0; }

  /** The part of evidence file names that tells this incarnation from every other of the run (reviewer decision 2 of PR 5): the launch,
   *  the restored checkpoint and the attempt, so no incarnation's evidence overwrites another's. */
  public String incarnation() {
    return "l" + launch + "-c" + (restoredCheckpointId == null ? "none" : restoredCheckpointId.toString()) + "-a" + attemptNumber;
  }

  public String encode() {
    ObjectNode n = Json.object().put("engine", JobSettings.ENGINE).put("launch", launch).put("isRestored", isRestored);
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
    RestoreEvidence e = new RestoreEvidence((int) Json.safeLong(n.get("launch"), "launch", 0, Integer.MAX_VALUE), n.get("isRestored").booleanValue(),
        c.isNull() ? null : Json.safeLong(c, "restoredCheckpointId", 1, Json.MAX_SAFE), (int) Json.safeLong(n.get("attemptNumber"), "attemptNumber", 0, Integer.MAX_VALUE));
    if (!e.encode().equals(s)) throw new IllegalArgumentException("restore evidence is not its canonical text");
    return e;
  }
}
