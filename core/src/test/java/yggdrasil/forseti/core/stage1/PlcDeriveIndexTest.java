package yggdrasil.forseti.core.stage1;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.core.stage1.Fixtures.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.Inputs;

/** {@link PlcStamped#derive} reads the per-equipment index of {@link Snapshots}; {@link #scan} is P2 as it read every stored snapshot
 *  (sorted again per call) before the index. On random snapshot sets with equal sourceTs, every kind of close, two unit IDs per visit,
 *  invalid snapshots and removals, both give the same lifecycles, spans and conflicting snapshots. */
class PlcDeriveIndexTest {
    static final String[] DEVICES = {S1, NR1}, VISITS = {"V1", "V2", "V3"}, UNITS = {"UNIT0000000000001", "UNIT0000000000002"};

    @Test
    void theIndexDerivesWhatAScanOfEverySnapshotDerives() {
        PlcStamped plc = new PlcStamped(Fixtures.plc());
        Random r = new Random(20261112);
        int compared = 0;
        for (int round = 0; round < 400; round++) {
            Stage1State s = new Stage1State(1, S1, 0, PlcStamped.MODE);
            List<String> keys = new ArrayList<>();
            int n = 1 + r.nextInt(40);
            for (int i = 0; i < n; i++) {
                String key = String.format("m%03d-%d", r.nextInt(1000), i);
                s.snapshots.put(key, snapshot(r));
                keys.add(key);
                if (r.nextInt(8) == 0) s.snapshots.put(keys.get(r.nextInt(keys.size())), snapshot(r));   // a key stored again
                same(scan(plc, s), plc.derive(s));
                compared++;
            }
            String gone = VISITS[r.nextInt(VISITS.length)];
            long bound = r.nextInt(12);
            s.snapshots.removeIf(x -> gone.equals(x.visitId()) || (x.sourceTs() != null && x.sourceTs() < bound));
            same(scan(plc, s), plc.derive(s));
            compared++;
        }
        assertTrue(compared > 4000);
    }

    static Stage1State.Snapshot snapshot(Random r) {
        String device = DEVICES[r.nextInt(DEVICES.length)];
        long ts = r.nextInt(12);   // few distinct sourceTs: ties are common
        if (r.nextInt(10) == 0) return new Stage1State.Snapshot(device, ts, false, false, null, null, null, null, "d", "r", Json.object());
        return new Stage1State.Snapshot(device, ts, true, true, UNITS[r.nextInt(5) == 0 ? 1 : 0], VISITS[r.nextInt(VISITS.length)], r.nextBoolean(),
            null, "d", "r", Json.object());
    }

    static void same(PlcStamped.Frame expected, PlcStamped.Frame actual) {
        assertEquals(expected.signature, actual.signature);
        assertEquals(text(expected.spans), text(actual.spans));
        assertEquals(expected.conflicting.keySet(), actual.conflicting.keySet());
        for (String k : expected.conflicting.keySet()) {
            List<Stage1State.Snapshot> a = expected.conflicting.get(k), b = actual.conflicting.get(k);
            assertEquals(a.size(), b.size(), k);
            for (int i = 0; i < a.size(); i++) assertSame(a.get(i), b.get(i), k + " #" + i);
        }
    }

    static String text(Map<String, List<PlcStamped.Span>> spans) {
        StringBuilder b = new StringBuilder();
        spans.forEach((d, l) -> l.forEach(sp -> b.append(d).append(' ').append(sp.visit().key).append(' ').append(sp.start()).append('-').append(sp.end()).append('\n')));
        return b.toString();
    }

    private record Part(String device, long start, Long close, boolean change, long lastTrue, boolean trueAfterClose, String unitAtStart) {}

    /** P2 over every stored snapshot, sorted again: the derivation before the index, kept as the reference. */
    static PlcStamped.Frame scan(PlcStamped plc, Stage1State s) {
        String station = s.station;
        Map<String, List<Stage1State.Snapshot>> byDevice = new TreeMap<>();
        for (Stage1State.Snapshot x : s.snapshots.values())
            if (x.valid() && station.equals(plc.stationOf(x.device()))) byDevice.computeIfAbsent(x.device(), d -> new ArrayList<>()).add(x);
        Comparator<Stage1State.Snapshot> order = Comparator.comparingLong(Stage1State.Snapshot::sourceTs);
        Map<String, List<Part>> parts = new TreeMap<>();
        Map<String, List<Stage1State.Snapshot>> ofVisit = new TreeMap<>();
        for (Map.Entry<String, List<Stage1State.Snapshot>> e : byDevice.entrySet()) {
            List<Stage1State.Snapshot> l = e.getValue();
            l.sort(order);
            SortedSet<String> ids = new TreeSet<>();
            for (Stage1State.Snapshot x : l) {
                ofVisit.computeIfAbsent(x.visitId(), k -> new ArrayList<>()).add(x);
                if (x.present()) ids.add(x.visitId());
            }
            for (String v : ids) {
                int i = 0;
                while (!(l.get(i).visitId().equals(v) && l.get(i).present())) i++;
                long start = l.get(i).sourceTs(), lastTrue = start;
                Long close = null;
                boolean change = false, after = false;
                for (int j = i + 1; j < l.size(); j++) {
                    Stage1State.Snapshot y = l.get(j);
                    if (y.sourceTs() <= start) continue;
                    if (y.visitId().equals(v)) {
                        if (y.present()) { if (close == null) lastTrue = y.sourceTs(); else after = true; }
                        else if (close == null) close = y.sourceTs();
                    } else if (y.present() && close == null) { close = y.sourceTs(); change = true; }
                }
                parts.computeIfAbsent(v, k -> new ArrayList<>()).add(new Part(e.getKey(), start, close, change, lastTrue, after, l.get(i).unitId()));
            }
        }
        List<VisitClaim> visits = new ArrayList<>();
        Map<String, List<PlcStamped.Span>> spans = new TreeMap<>();
        Map<String, List<Stage1State.Snapshot>> conflicting = new TreeMap<>();
        StringBuilder sig = new StringBuilder();
        for (Map.Entry<String, List<Part>> e : parts.entrySet()) {
            String v = e.getKey();
            ObjectNode ref = PlcStamped.visitRef(plc.namespace, station, v);
            String key = VisitClaim.keyOf(ref);
            if (s.expired.containsKey(key)) continue;
            List<Part> ps = e.getValue();
            Part first = ps.get(0);
            for (Part p : ps) if (p.start() < first.start()) first = p;
            VisitClaim c = VisitClaim.plc(ref, first.unitAtStart(), station, first.start());
            SortedSet<String> units = new TreeSet<>();
            for (Stage1State.Snapshot x : ofVisit.get(v)) units.add(x.unitId());
            c.inputConflict = units.size() > 1;
            Set<Long> starts = new TreeSet<>(), closes = new TreeSet<>();
            boolean allEnded = true, anyChange = false, after = false;
            Long changeAt = null, maxEnd = null, minClose = null;
            for (Part p : ps) {
                starts.add(p.start());
                if (p.trueAfterClose()) after = true;
                if (p.close() == null) { allEnded = false; continue; }
                closes.add(p.close());
                minClose = minClose == null ? p.close() : Math.min(minClose, p.close());
                if (p.change()) { allEnded = false; anyChange = true; changeAt = changeAt == null ? p.close() : Math.min(changeAt, p.close()); }
                else maxEnd = maxEnd == null ? p.close() : Math.max(maxEnd, p.close());
            }
            boolean stillPresent = false;
            if (minClose != null) for (Part p : ps) if (p.close() == null && p.lastTrue() >= minClose) stillPresent = true;
            c.inconsistentBoundary = starts.size() > 1 || closes.size() > 1 || after || stillPresent;
            if (allEnded) c.leave = maxEnd;
            else if (anyChange) { c.missingVisitClose = true; c.attributionEnd = changeAt; }
            visits.add(c);
            if (c.inputConflict) conflicting.put(c.key, ofVisit.get(v));
            sig.append(c.key).append('|').append(c.unitId).append('|').append(c.enter).append('|').append(c.leave).append('|').append(c.attributionEnd)
                .append('|').append(c.inputConflict).append(c.missingVisitClose).append(c.inconsistentBoundary);
            for (Part p : ps) {
                spans.computeIfAbsent(p.device(), d -> new ArrayList<>()).add(new PlcStamped.Span(c, p.start(), p.close()));
                sig.append('|').append(p.device()).append(':').append(p.start()).append('-').append(p.close());
            }
            sig.append('\n');
        }
        return new PlcStamped.Frame(visits, spans, conflicting, sig.toString(), s);
    }
}
