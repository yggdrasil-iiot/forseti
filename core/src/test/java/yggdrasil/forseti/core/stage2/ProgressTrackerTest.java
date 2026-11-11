package yggdrasil.forseti.core.stage2;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.stage1.DeadlineIndex;

/** Rule sheet T2 (spec 511, 426-435). */
class ProgressTrackerTest {
    static final String S = "synthetic-site:line2:station1", WR = ProgressTracker.WORK_RECORDS, SA = ProgressTracker.SAMPLES;

    static Progress.Stage1 p(long cutId, long w1, List<Long> merged) {
        return new Progress.Stage1(1, S, cutId, 0, w1, null, List.of(), 0, null, merged, null);
    }
    static Progress.Stage1 p(long cutId, long w1) { return p(cutId, w1, List.of()); }
    static void barriers(ProgressTracker t, long cutId) { for (int q = 0; q < 6; q++) t.observeIncidentBarrier(S, cutId, q, -1); }

    @Test void w2IsTheMinimumOfTheTwoTopics() {
        ProgressTracker t = new ProgressTracker();
        assertEquals(OptionalLong.empty(), t.w2(S));
        t.observe(WR, S, p(1, 5000));
        assertEquals(OptionalLong.empty(), t.w2(S), "no W2 before both topics carry progress");
        t.observe(SA, S, p(1, 4000));
        assertEquals(OptionalLong.of(4000), t.w2(S));
        t.observe(SA, S, p(2, 9000));
        assertEquals(OptionalLong.of(5000), t.w2(S));
        t.observeIncidentBarrier(S, 2, 0, 7);
        assertEquals(OptionalLong.of(5000), t.w2(S), "incident barriers never advance W2");
    }

    @Test void notReadyUntilBothTopicsCarryTheCutAndTheSixIncidentBarriers() {
        ProgressTracker t = new ProgressTracker();
        t.observe(WR, S, p(1, 5000));
        for (int q = 0; q < 5; q++) t.observeIncidentBarrier(S, 1, q, -1);
        assertFalse(t.ready(S, 1));
        t.observe(SA, S, p(1, 5000));
        assertFalse(t.ready(S, 1), "five of six incident barriers");
        t.observeIncidentBarrier(S, 1, 5, 3);
        assertTrue(t.ready(S, 1));
        assertFalse(t.ready(S, 2));
        assertEquals(List.of(new Progress.Stage1ProgressRef(WR, 0, 1), new Progress.Stage1ProgressRef(SA, 0, 1)), t.stage1ProgressRefs(S, 1, 0));
    }

    @Test void unequalMergedCutIdsIsAnIllegalStateException() {
        ProgressTracker t = new ProgressTracker();
        t.observe(WR, S, p(3, 5000, List.of(1L, 2L)));
        t.observe(SA, S, p(3, 5000, List.of(2L)));
        for (long c = 1; c <= 3; c++) barriers(t, c);
        assertThrows(IllegalStateException.class, () -> t.ready(S, 3));
    }

    @Test void aDecreasingProgressIsRefused() {
        ProgressTracker t = new ProgressTracker();
        t.observe(WR, S, p(2, 5000));
        assertThrows(IllegalArgumentException.class, () -> t.observe(WR, S, p(1, 6000)), "cutId decreasing");
        assertThrows(IllegalArgumentException.class, () -> t.observe(WR, S, p(3, 4999)), "W1 decreasing");
        assertThrows(IllegalArgumentException.class, () -> t.observe("forseti.affected", S, p(3, 6000)), "not a stage-1 evidence topic");
        t.observe(WR, S, p(3, 5000));
        assertEquals(3, t.lastProgress(WR, S).cutId());
    }

    @Test void incidentBarriersAreCounted6x1PlusMergedCutIds() {
        ProgressTracker t = new ProgressTracker();
        t.observe(WR, S, p(4, 5000, List.of(2L, 3L)));
        t.observe(SA, S, p(4, 5000, List.of(2L, 3L)));
        barriers(t, 2);
        barriers(t, 4);
        assertFalse(t.ready(S, 4), "the barriers of merged cut 3 are missing");
        barriers(t, 3);
        assertTrue(t.ready(S, 4));
        List<Progress.IncidentOffset> offsets = t.incidentOffsets(S, 4);
        assertEquals(6 * (1 + 2), offsets.size());
        assertEquals(new Progress.IncidentOffset(2, 0, -1), offsets.get(0));
        assertEquals(new Progress.IncidentOffset(4, 5, -1), offsets.get(17));
    }

    @Test void aProcessedBarrierIsNotAppliedTwice() {
        ProgressTracker t = new ProgressTracker();
        t.observe(WR, S, p(1, 5000));
        t.observe(SA, S, p(1, 5000));
        assertTrue(t.observeIncidentBarrier(S, 1, 2, 0));
        for (int q : new int[] {0, 1, 3, 4, 5}) assertTrue(t.observeIncidentBarrier(S, 1, q, -1));
        assertTrue(t.ready(S, 1));
        List<Progress.IncidentOffset> first = t.incidentOffsets(S, 1);
        t.markProcessed(S, 1);
        assertTrue(t.processed(S, 1));
        assertFalse(t.observeIncidentBarrier(S, 1, 2, 0), "a processed cut's barrier is kept and never applied again");
        assertEquals(first, t.incidentOffsets(S, 1), "processed barriers are kept as evidence");
        ProgressTracker back = ProgressTracker.fromJson(Json.read(Canonical.bytes(t.toJson())));
        assertArrayEquals(Canonical.bytes(t.toJson()), Canonical.bytes(back.toJson()));
        assertFalse(back.observeIncidentBarrier(S, 1, 2, 0));
    }

    @Test void timersFireInOrderUpToW2() {
        ProgressTracker t = new ProgressTracker();
        DeadlineIndex ix = new DeadlineIndex();
        JsonNode a = Json.object().put("k", "a"), b = Json.object().put("k", "b");
        ix.register(7000, b, "incident-settle");
        ix.register(3000, b, "basis-retention");
        ix.register(3000, a, "basis-retention");
        ix.register(5001, a, "incident-settle");
        t.track(S, ix);
        assertEquals(List.of(), t.fire(S), "no W2 yet");
        t.observe(WR, S, p(1, 5000));
        t.observe(SA, S, p(1, 6000));
        List<DeadlineIndex.Deadline> due = t.fire(S);
        assertEquals(2, due.size(), "deadline <= W2 = 5000");
        assertEquals(a, due.get(0).key());
        assertEquals(b, due.get(1).key());
        t.observe(WR, S, p(2, 8000));
        assertEquals(3, t.fire(S).size(), "W2 = 6000");
    }
}
