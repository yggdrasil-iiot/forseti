package yggdrasil.forseti.engine.flink.job;

import static org.junit.jupiter.api.Assertions.*;

import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/** The canonical encoding of Flink's restore evidence in {@code initEvidence.restore} (spec 416). */
class RestoreEvidenceTest {
  @Test void aFirstStartIsItsCanonicalTextAndNotReinitialized() {
    RestoreEvidence e = RestoreEvidence.of(0, false, OptionalLong.empty(), 0);
    assertEquals("{\"attemptNumber\":0,\"engine\":\"flink\",\"isRestored\":false,\"launch\":0,\"restoredCheckpointId\":null}", e.encode());
    assertFalse(e.reinitialized());
    assertEquals(e, RestoreEvidence.firstStart());
    assertEquals(e, RestoreEvidence.decode(e.encode()));
    assertEquals("l0-cnone-a0", e.incarnation());
  }

  @Test void aRestoreOrAReattemptIsAReinitialization() {
    RestoreEvidence restored = RestoreEvidence.of(1, true, OptionalLong.of(17), 1);
    assertEquals("{\"attemptNumber\":1,\"engine\":\"flink\",\"isRestored\":true,\"launch\":1,\"restoredCheckpointId\":17}", restored.encode());
    assertTrue(restored.reinitialized());
    assertEquals(restored, RestoreEvidence.decode(restored.encode()));
    assertEquals("l1-c17-a1", restored.incarnation());
    assertTrue(RestoreEvidence.of(0, false, OptionalLong.empty(), 2).reinitialized(), "a new attempt without a checkpoint is still a reinitialization");
  }

  @Test void aRelaunchWithoutRestoredStateIsAReinitialization() {
    // reviewer decision 3 of PR 5: a fresh JVM launch with nothing to restore starts its attempt at 0 again; only the launch tells
    RestoreEvidence fresh = RestoreEvidence.of(2, false, OptionalLong.empty(), 0);
    assertTrue(fresh.reinitialized());
    assertNotEquals(RestoreEvidence.firstStart().incarnation(), fresh.incarnation(), "no incarnation overwrites another's evidence");
  }

  @Test void anInconsistentOrNonCanonicalTextIsRefused() {
    assertThrows(IllegalArgumentException.class, () -> new RestoreEvidence(0, true, null, 0), "restored without a checkpoint id");
    assertThrows(IllegalArgumentException.class, () -> new RestoreEvidence(0, false, 3L, 0), "a checkpoint id without a restore");
    assertThrows(IllegalArgumentException.class, () -> new RestoreEvidence(-1, false, null, 0), "a negative launch");
    assertThrows(IllegalArgumentException.class, () -> RestoreEvidence.decode("none"));
    assertThrows(IllegalArgumentException.class,
        () -> RestoreEvidence.decode("{\"engine\":\"flink\",\"attemptNumber\":0,\"isRestored\":false,\"launch\":0,\"restoredCheckpointId\":null}"),
        "the right fields in another order are not the canonical text");
    assertThrows(IllegalArgumentException.class,
        () -> RestoreEvidence.decode("{\"attemptNumber\":0,\"engine\":\"kafka-streams\",\"isRestored\":false,\"launch\":0,\"restoredCheckpointId\":null}"));
    assertThrows(IllegalArgumentException.class, () -> RestoreEvidence.decode("{\"attemptNumber\":0,\"engine\":\"flink\",\"isRestored\":false,\"restoredCheckpointId\":null}"),
        "the PR 5 text without the launch");
  }
}
