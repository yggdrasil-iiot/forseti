package yggdrasil.forseti.engine.flink.job;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.journal.Progress;

/** Stage 2 applies a station's cut only when the heads of its two evidence queues announce the same cut with the same merged cuts
 *  ({@link Stage2Stations#requireAgreement}); the torn publication of control theta (a cut announced on one journal only, then merged
 *  into the next) fails the job. */
class Stage2AgreementTest {
  static Progress.Stage1 progress(long cutId, List<Long> merged) {
    return new Progress.Stage1(1, "site-019:line2:station3", cutId, 0, 1_000L * cutId, null, List.of(), 0, null, merged, null);
  }

  @Test void equalHeadsAgree() {
    assertDoesNotThrow(() -> Stage2Stations.requireAgreement("s", progress(50, List.of()), progress(50, List.of())));
    assertDoesNotThrow(() -> Stage2Stations.requireAgreement("s", progress(51, List.of(50L)), progress(51, List.of(50L))));
  }

  @Test void aCutOnOneJournalOnlyThenMergedFails() {
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> Stage2Stations.requireAgreement("site-019:line2:station3", progress(50, List.of()), progress(51, List.of(50L))));
    assertEquals("site-019:line2:station3: the evidence topics disagree on cut 50/51 merged []/[50]", e.getMessage());
  }

  @Test void theSameCutWithOtherMergedCutsFails() {
    assertThrows(IllegalStateException.class, () -> Stage2Stations.requireAgreement("s", progress(51, List.of()), progress(51, List.of(50L))));
  }
}
