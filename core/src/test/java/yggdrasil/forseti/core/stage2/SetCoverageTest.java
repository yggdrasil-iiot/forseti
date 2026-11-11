package yggdrasil.forseti.core.stage2;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.stage2.SetCoverage.ActivityRow;
import yggdrasil.forseti.core.stage2.SetCoverage.EvidenceRange;

/** Rule sheet T8: the affected set's coverage object. */
class SetCoverageTest {
    static final String D = "synthetic-site:line2:station1:nutrunner1";
    static final long P = 1000, L = 720_000, WR = 604_800_000L;

    static Inputs.IncidentEvent incident(long from, Long to, String status, long revisionEventTime) {
        return new Inputs.IncidentEvent("inc-1", 1, "synthetic-site:line2:station1", D, "tool-check-failed", revisionEventTime, revisionEventTime, from, to, status, "ev", "qa");
    }
    static ActivityRow row(long t, String state, String reason, JsonNode active) { return new ActivityRow(t, state, reason, active); }
    static ActivityRow good(long t, boolean active) { return row(t, "attributed", null, BooleanNode.valueOf(active)); }
    static ActivityRow unattributedActive(long t) { return row(t, "unattributed", "no-occupancy", BooleanNode.TRUE); }

    static SetCoverage.Evidence evidence(List<ActivityRow> rows, EvidenceRange range) {
        Long last = rows.stream().map(ActivityRow::sourceTs).max(Long::compare).orElse(null);
        Map<String, Long> lastObs = new TreeMap<>();
        if (last != null) lastObs.put(D, last);
        Map<String, EvidenceRange> ranges = new TreeMap<>();
        if (range != null) ranges.put(D, range);
        return new SetCoverage.Evidence(Map.of(D, rows), lastObs, ranges);
    }
    static ObjectNode cover(Inputs.IncidentEvent inc, List<ActivityRow> rows, EvidenceRange range, Long w2) {
        return cover(inc, rows, range, w2, List.of(), 0, false);
    }
    static ObjectNode cover(Inputs.IncidentEvent inc, List<ActivityRow> rows, EvidenceRange range, Long w2, List<SetCoverage.Unit> units, int gaps, boolean ambiguous) {
        return SetCoverage.compute(inc, List.of(D), evidence(rows, range), w2, P, L, WR, units, gaps, ambiguous);
    }
    static List<ActivityRow> everySlot(long from, long to) { List<ActivityRow> l = new ArrayList<>(); for (long t = from; t < to; t += P) l.add(good(t, false)); return l; }

    @Test void slotsStartAtSuspectFrom() {
        List<ActivityRow> rows = List.of(unattributedActive(500), unattributedActive(1499), unattributedActive(1500));
        ObjectNode c = cover(incident(500, 3500L, "closed", 3500), rows, new EvidenceRange(0, 10_000), 10_000_000L);
        assertEquals(2, c.get("unattributedActivityPeriods").longValue(), "500 and 1499 share the slot [500, 1500)");
        assertEquals(1, c.get("unknownActivityPeriods").longValue(), "slot [2500, 3500) has no observation");
        assertEquals(3, SetCoverage.slots(500, 3500, P));
    }

    @Test void theLastPartialSlotCounts() {
        assertEquals(3, SetCoverage.slots(0, 2500, P));
        assertEquals(0, SetCoverage.slots(100, 100, P));
        ObjectNode closed = cover(incident(0, 2500L, "closed", 2500), List.of(), new EvidenceRange(0, 10_000), 10_000_000L);
        assertEquals(3, closed.get("unknownActivityPeriods").longValue());
        // open: [suspectFrom, last delivered observation + P) = [0, 5200): six slots, the observation at 4200 in the sixth
        ObjectNode open = cover(incident(0, null, "open", 0), List.of(good(4200, true)), new EvidenceRange(0, 10_000), 5000L);
        assertEquals(5, open.get("unknownActivityPeriods").longValue());
        assertTrue(open.get("open").booleanValue());
        assertFalse(open.get("settled").booleanValue());
    }

    @Test void unattributedActivityAndUnknownAreExclusive() {
        List<ActivityRow> rows = List.of(
            unattributedActive(0), row(100, "unattributed", "bad-quality", TextNode.valueOf("unknown")),   // slot 0: unattributed only
            row(1000, "unattributed", "bad-quality", TextNode.valueOf("unknown")),                          // slot 1: unknown
            row(2000, "unattributed", "session-ambiguous", BooleanNode.TRUE),                                // slot 2: unknown
            row(3000, "unattributed", "outside-session", BooleanNode.TRUE),                                  // slot 3: unknown
            good(4000, false),                                                                                // slot 4: known inactive
            row(5000, "unattributed", "ambiguous", BooleanNode.TRUE));                                       // slot 5: unattributed (spec 368)
        ObjectNode c = cover(incident(0, 6000L, "closed", 6000), rows, new EvidenceRange(0, 10_000), 10_000_000L);
        assertEquals(2, c.get("unattributedActivityPeriods").longValue());
        assertEquals(3, c.get("unknownActivityPeriods").longValue());
        assertEquals("unknown", c.get("status").textValue());
    }

    @Test void retentionGapFromTheManifestRange() {
        Inputs.IncidentEvent closed = incident(1000, 5000L, "closed", 5000);
        List<ActivityRow> rows = everySlot(1000, 5000);
        assertFalse(cover(closed, rows, new EvidenceRange(0, 5000), 10_000_000L).get("retentionGap").booleanValue());
        assertTrue(cover(closed, rows, new EvidenceRange(0, 4999), 10_000_000L).get("retentionGap").booleanValue());
        assertTrue(cover(closed, rows, new EvidenceRange(1001, 9000), 10_000_000L).get("retentionGap").booleanValue());
        assertTrue(cover(closed, rows, null, 10_000_000L).get("retentionGap").booleanValue(), "a device with no verified range is a gap");
        // a verified range in which the device delivered nothing still covers: first and last sample times are not used
        ObjectNode empty = cover(closed, List.of(), new EvidenceRange(0, 9000), 10_000_000L);
        assertFalse(empty.get("retentionGap").booleanValue());
        assertEquals(4, empty.get("unknownActivityPeriods").longValue());
        assertTrue(cover(closed, rows, new EvidenceRange(1000, 1000), 10_000_000L).get("retentionGap").booleanValue(), "a zero-length range covers no slot");
        // open: [suspectFrom, W2)
        Inputs.IncidentEvent open = incident(1000, null, "open", 1000);
        assertFalse(cover(open, rows, new EvidenceRange(0, 8000), 8000L).get("retentionGap").booleanValue());
        assertTrue(cover(open, rows, new EvidenceRange(0, 8000), 8001L).get("retentionGap").booleanValue());
    }

    @Test void beyondRetentionAtTheBoundaryOncePerRevision() {
        long ret = 1_000_000_000L;
        List<ActivityRow> rows = everySlot(ret - WR - 1, ret - WR + 2000);
        ObjectNode at = cover(incident(ret - WR, ret - WR + 2000, "closed", ret), rows, new EvidenceRange(0, ret * 2), ret);
        assertFalse(at.get("beyondRetention").booleanValue(), "suspectFrom == revisionEventTime - workRecordsMs is inside");
        ObjectNode out = cover(incident(ret - WR - 1, ret - WR + 2000, "closed", ret), rows, new EvidenceRange(0, ret * 2), ret);
        assertTrue(out.get("beyondRetention").booleanValue(), "one millisecond outside");
        for (long w2 : new long[] {ret, ret + WR, ret + 10 * WR})
            assertTrue(cover(incident(ret - WR - 1, ret - WR + 2000, "closed", ret), rows, new EvidenceRange(0, ret * 2), w2).get("beyondRetention").booleanValue(),
                "computed from the revision, not from W2");
        assertEquals("incomplete", out.get("status").textValue());
    }

    @Test void theStatusLadder() {
        Inputs.IncidentEvent closed = incident(0, 4000L, "closed", 4000);
        List<ActivityRow> rows = everySlot(0, 4000);
        EvidenceRange range = new EvidenceRange(0, 100_000);
        long settledW2 = 4000 + L;
        ObjectNode base = cover(closed, rows, range, settledW2);
        assertEquals("complete", base.get("status").textValue());
        assertEquals(List.of("status", "unattributedActivityPeriods", "unknownActivityPeriods", "provisionalUnits", "insufficientUnits", "sessionGapOverlaps",
            "open", "settled", "beyondRetention", "retentionGap"), names(base));
        assertTrue(base.get("settled").booleanValue());
        assertEquals("incomplete", cover(closed, rows, range, settledW2 - 1).get("status").textValue(), "not settled");
        assertEquals("incomplete", cover(closed, rows, range, settledW2, List.of(new SetCoverage.Unit("insufficient-evidence", "basis-provisional")), 0, false).get("status").textValue());
        ObjectNode insufficient = cover(closed, rows, range, settledW2, List.of(new SetCoverage.Unit("insufficient-evidence", "basis-insufficient"),
            new SetCoverage.Unit("insufficient-evidence", "basis-provisional"), new SetCoverage.Unit("affected-candidate", "equipment-activity")), 0, false);
        assertEquals(1, insufficient.get("provisionalUnits").longValue());
        assertEquals(1, insufficient.get("insufficientUnits").longValue());
        assertEquals("incomplete", cover(closed, rows, range, settledW2, List.of(), 1, false).get("status").textValue(), "a session gap overlap");
        assertEquals("unknown", cover(closed, rows, range, settledW2, List.of(), 1, true).get("status").textValue(), "a session-ambiguous basis is unknown");
        List<ActivityRow> holed = new ArrayList<>(rows);
        holed.remove(1);
        assertEquals("unknown", cover(closed, holed, range, settledW2, List.of(new SetCoverage.Unit("insufficient-evidence", "basis-provisional")), 0, false).get("status").textValue(),
            "unknown beats incomplete");
        List<ActivityRow> unattributed = new ArrayList<>(rows);
        unattributed.set(1, unattributedActive(1000));
        assertEquals("incomplete", cover(closed, unattributed, range, settledW2).get("status").textValue());
        ObjectNode open = cover(incident(0, 4000L, "open", 4000), rows, range, settledW2);
        assertTrue(open.get("open").booleanValue());
        assertEquals("incomplete", open.get("status").textValue(), "an open incident is never complete");
        ObjectNode withdrawn = cover(incident(0, 4000L, "withdrawn", 4000), rows, range, 0L);
        assertFalse(withdrawn.get("open").booleanValue());
        assertTrue(withdrawn.get("settled").booleanValue());
        assertEquals("incomplete", withdrawn.get("status").textValue(), "a withdrawn incident is never complete");
    }

    static List<String> names(ObjectNode n) { List<String> l = new ArrayList<>(); n.fieldNames().forEachRemaining(l::add); return l; }
}
