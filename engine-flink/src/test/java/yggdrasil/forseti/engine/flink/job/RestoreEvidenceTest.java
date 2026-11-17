package yggdrasil.forseti.engine.flink.job;

import static org.junit.jupiter.api.Assertions.*;

import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/** The canonical encoding of Flink's restore evidence in {@code initEvidence.restore} (spec 416). */
class RestoreEvidenceTest {
  @Test void aFirstStartIsItsCanonicalTextAndNotReinitialized() {
    RestoreEvidence e = RestoreEvidence.of(false, OptionalLong.empty(), 0);
    assertEquals("{\"attemptNumber\":0,\"engine\":\"flink\",\"isRestored\":false,\"restoredCheckpointId\":null}", e.encode());
    assertFalse(e.reinitialized());
    assertEquals(e, RestoreEvidence.decode(e.encode()));
  }

  @Test void aRestoreOrAReattemptIsAReinitialization() {
    RestoreEvidence restored = RestoreEvidence.of(true, OptionalLong.of(17), 1);
    assertEquals("{\"attemptNumber\":1,\"engine\":\"flink\",\"isRestored\":true,\"restoredCheckpointId\":17}", restored.encode());
    assertTrue(restored.reinitialized());
    assertEquals(restored, RestoreEvidence.decode(restored.encode()));
    assertTrue(RestoreEvidence.of(false, OptionalLong.empty(), 2).reinitialized(), "a new attempt without a checkpoint is still a reinitialization");
  }

  @Test void anInconsistentOrNonCanonicalTextIsRefused() {
    assertThrows(IllegalArgumentException.class, () -> new RestoreEvidence(true, null, 0), "restored without a checkpoint id");
    assertThrows(IllegalArgumentException.class, () -> new RestoreEvidence(false, 3L, 0), "a checkpoint id without a restore");
    assertThrows(IllegalArgumentException.class, () -> RestoreEvidence.decode("none"));
    assertThrows(IllegalArgumentException.class, () -> RestoreEvidence.decode("{\"engine\":\"flink\",\"attemptNumber\":0,\"isRestored\":false,\"restoredCheckpointId\":null}"),
        "the right fields in another order are not the canonical text");
    assertThrows(IllegalArgumentException.class, () -> RestoreEvidence.decode("{\"attemptNumber\":0,\"engine\":\"kafka-streams\",\"isRestored\":false,\"restoredCheckpointId\":null}"));
    assertThrows(IllegalArgumentException.class, () -> RestoreEvidence.decode("{\"attemptNumber\":0,\"engine\":\"flink\",\"isRestored\":false}"));
  }
}
