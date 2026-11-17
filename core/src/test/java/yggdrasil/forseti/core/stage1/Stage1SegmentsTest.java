package yggdrasil.forseti.core.stage1;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.core.stage1.Driver.*;
import static yggdrasil.forseti.core.stage1.Fixtures.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.segment.SegmentShadow;
import yggdrasil.forseti.core.segment.Segments;

/** The segmented stage-1 state ({@link Stage1State#segments}, {@link Stage1State#drainChanged}, {@link Stage1State#fromSegments}).
 *  Every {@link Driver} keeps a {@link SegmentShadow}: after each decide step the drain is applied to a map store, which must be the
 *  state's segments byte for byte and restore the live toJson. So every stage-1 scenario of the suite is a sequence checked here; the
 *  tests below add the recordings, randomized sequences over the change paths, the no-op drain, determinism and the size bound. */
class Stage1SegmentsTest {
    static final String OCC = InputParser.OCCUPANCY, SES = InputParser.SESSIONS, MSG = InputParser.MESSAGES;
    static final String U1 = "UNIT0000000000001", U2 = "UNIT0000000000002";
    static final long L = 720_000, H = 86_400_000;

    @Test void theRecordingsRestoreAtEveryStep() {
        Driver d = Driver.station1();
        d.runRecording(FIXTURES.resolve("recordings/smoke"));
        assertTrue(d.shadow.steps > 2000 && d.shadow.checked >= 28, "steps " + d.shadow.steps + " checked " + d.shadow.checked);
        assertEquals(1, d.shadow.fullDrains, "only the first drain is full");
        Driver p = new Driver(Fixtures.plc(), S1);
        p.runRecording(FIXTURES.resolve("recordings/smoke-plc"));
        assertTrue(p.shadow.steps > 1000, "steps " + p.shadow.steps);
    }

    @Test void theExpirySequenceRestoresAtEveryStepAndAfterTheStoreRoundTrip() {
        for (boolean roundTrip : List.of(false, true)) {
            Driver d = Stage1OccupancyJoinTest.sequence(roundTrip);
            assertTrue(d.shadow.steps > 200);
            assertTrue(d.state.toJson().get("expired").size() > 0, "the sequence ends past an expiry");
            if (!roundTrip) assertEquals(1, d.shadow.fullDrains, "only the first drain is full");
            else assertTrue(d.shadow.fullDrains > 1, "a state read back with fromJson drains in full once");
        }
    }

    /** Random occupancy-join sequences over every change path: scans with bad quality and dropped members, arrivals, departures,
     *  identical redeliveries, declarations, occupancy identity and correction conflicts, sample identity conflicts, measurement
     *  conflicts, session events, cuts, settlement and expiry jumps. */
    @Test void randomOccupancyJoinSequencesRestoreAtEveryStep() {
        long steps = 0;
        Set<String> kinds = new TreeSet<>();
        for (int seed = 0; seed < 8; seed++) {
            Driver d = Driver.station1();
            randomOccupancy(d, new Random(20261118L + seed), 260);
            steps += d.shadow.steps;
            kinds.addAll(kinds(d.state));
        }
        assertTrue(steps > 2500, "steps " + steps);
        assertTrue(kinds.containsAll(List.of("expired", "quarantined", "deliveries", "samples", "measurements", "messages", "sessions", "deadlines")), "kinds " + kinds);
    }

    /** Random plc-stamped sequences: visits open and close by VisitPresent and by VisitId changes, unit conflicts, incoherent and
     *  redelivered messages, cuts and expiry jumps. */
    @Test void randomPlcStampedSequencesRestoreAtEveryStep() {
        long steps = 0;
        for (int seed = 0; seed < 6; seed++) {
            Random r = new Random(7_000L + seed);
            Driver d = new Driver(r.nextBoolean() ? Fixtures.plc() : Fixtures.plcNoCheck(), S1);
            Stage1PlcStampedTest t = new Stage1PlcStampedTest();
            long now = T;
            int visit = 1;
            boolean present = true;
            String unit = U1;
            for (int i = 0; i < 160; i++) {
                int a = r.nextInt(100);
                if (a < 50) { for (String q : List.of(S1, NR1)) t.msg(d, q, now, "V" + visit, unit, present); now += 1000; }
                else if (a < 58) present = !present;
                else if (a < 64) { visit++; present = true; }
                else if (a < 68) unit = unit.equals(U1) ? U2 : U1;   // the same VisitId with another unit: an identity conflict
                else if (a < 72) t.msg(d, NR1, now, "V" + visit, unit, present, Stage1PlcStampedTest.tweak(NR1, "UnitId", m -> m.put("uaStatusCode", BAD)));
                else if (a < 76 && !t.sent.isEmpty()) d.input(MSG, t.sent.get(r.nextInt(t.sent.size())).value());   // identical redelivery
                else if (a < 80) d.input(OCC, arrival("occ-" + i, now, unit, S1));
                else if (a < 94) d.cut();
                else if (a < 97) d.cut(now + L);
                else { now += H + L; d.cut(now); }
            }
            d.cut();
            steps += d.shadow.steps;
        }
        assertTrue(steps > 1000, "steps " + steps);
    }

    static Set<String> kinds(Stage1State s) {
        Set<String> k = new TreeSet<>();
        for (String id : s.segmentIds()) k.add(Segments.kind(id));
        return k;
    }

    static void randomOccupancy(Driver d, Random r, int n) {
        long now = T;
        int visits = 0;
        String open = null, unit = U1;
        List<ObjectNode> arrivals = new ArrayList<>();
        List<Inputs.InputRecord> sent = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int a = r.nextInt(100);
            if (a < 40) {
                List<Driver.Override> o = new ArrayList<>();
                if (r.nextInt(8) == 0) o.add(over(NR1, "Torque", new DoubleNode(20.5), BAD));
                if (r.nextInt(10) == 0) o.add(over(NR2, "Running", null, 0));
                if (r.nextInt(12) == 0) o.add(over(NR1, "Torque", new DoubleNode(20.5), UNCERTAIN));
                sent.addAll(d.scan(now, o.toArray(new Driver.Override[0])));
                now += 1000;
            } else if (a < 47 && open == null) {
                open = "occ-" + (++visits);
                unit = r.nextBoolean() ? U1 : U2;
                ObjectNode arr = arrival(open, now, unit, S1);
                arrivals.add(arr);
                d.input(OCC, arr);
            } else if (a < 54 && open != null) {
                d.input(OCC, departure(open + "-d", now, unit, S1, open));
                open = null;
            } else if (a < 57 && !arrivals.isEmpty()) {
                d.input(OCC, arrivals.get(r.nextInt(arrivals.size())));   // identical redelivery
            } else if (a < 60 && !arrivals.isEmpty()) {
                ObjectNode x = arrivals.get(r.nextInt(arrivals.size()));
                d.input(OCC, declared(x, r.nextBoolean() ? DID_NOT_OCCUR : INCORRECT_DATA, now));   // a declaration, maybe a second one
            } else if (a < 62 && !arrivals.isEmpty()) {
                ObjectNode x = arrivals.get(r.nextInt(arrivals.size())).deepCopy().put("unitId", "UNIT0000000000009");
                d.input(OCC, x);   // same identity, other content: an identity conflict
            } else if (a < 66 && !sent.isEmpty()) {
                ObjectNode m = (ObjectNode) sent.get(r.nextInt(sent.size())).value().deepCopy();
                JsonNode first = m.get("members").get(0);
                ((ObjectNode) first).set("value", first.get("value").isBoolean() ? BooleanNode.valueOf(!first.get("value").booleanValue()) : new DoubleNode(99.5));
                d.input(MSG, m);   // one sampleId, two contents: a sample identity conflict
            } else if (a < 69 && !sent.isEmpty()) {
                Inputs.InputRecord x = sent.get(r.nextInt(sent.size()));
                Inputs.ProcessMessage pm = (Inputs.ProcessMessage) x.parsed();
                if (pm.device().equals(NR1)) {
                    Long ts = pm.members().get(0).sourceTs();
                    if (ts != null) d.input(MSG, d.message(NR1, ts, member("Torque", new DoubleNode(77.25), 0, ts), member("Running", BooleanNode.TRUE, 0, ts)));   // a measurement conflict
                }
            } else if (a < 70 && !sent.isEmpty()) {
                d.input(MSG, sent.get(r.nextInt(sent.size())).value());   // an identical message redelivery
            } else if (a < 72) {
                d.input(SES, session(NR2, "seq-gap", 25 + i, Json.putSafe(Json.putSafe(Json.object(), "from", now - 5_000), "to", now - 3_000).put("missing", 2)));
            } else if (a < 73) {
                d.input(SES, session(NR1, "session-down", 30 + i, Json.putSafe(Json.object(), "from", now - 2_000).putNull("to")));
            } else if (a < 74) {
                ObjectNode up = session(NR1, "session-up", 40 + i, Json.putSafe(Json.object(), "at", now).put("rebirth", false));
                Json.putSafe(up, "birthPublishTs", BIRTH + i);
                d.input(SES, up);
            } else if (a < 90) d.cut();
            else if (a < 95) d.cut(now + L);
            else if (a < 97) { now += H + L + 60_000; d.cut(now); }
            else d.cut(now + r.nextInt(120_000));
        }
        d.cut();
    }

    @Test void aRecomputeThatChangesNothingDrainsOnlyTheHead() {
        Driver d = Driver.station1();
        d.input(OCC, arrival("occ-1", T, U1, S1));
        d.scans(T, T + 60_000);
        d.input(OCC, departure("occ-2", T + 60_000, U1, S1, "occ-1"));
        d.cut();
        long w = d.w1;
        assertFalse(d.state.segmentIds().size() < 10);
        // a cut close at the same W1 recomputes every sample row, coverage, work record and deadline, and changes none of them: only the
        // head (progressCount) is written
        d.shadow = null;
        d.cut(w);
        d.state.drainChanged();
        d.cut(w);
        Segments.Drain drain = d.state.drainChanged();
        assertEquals(Set.of(Segments.HEAD), drain.written().keySet(), "a no-op cut close writes the head only");
        assertTrue(drain.removed().isEmpty());
        // nothing at all happens between two drains: nothing to drain
        assertTrue(d.state.drainChanged().isEmpty());
    }

    @Test void segmentsAreDeterministic() {
        Driver a = Stage1OccupancyJoinTest.sequence(false), b = Stage1OccupancyJoinTest.sequence(false);
        same(a.state.segments(), b.state.segments());
        Stage1State fromJson = Stage1State.fromJson(Json.read(Canonical.bytes(a.state.toJson())));
        same(a.state.segments(), fromJson.segments());
        Stage1State fromSegments = Stage1State.fromSegments(a.state.segments());
        same(a.state.segments(), fromSegments.segments());
        assertTrue(fromSegments.drainChanged().isEmpty(), "a state read from its segments has nothing to drain");
        for (byte[] seg : a.state.segments().values()) assertArrayEquals(seg, Canonical.bytes(Json.read(seg)), "segment bytes are JCS");
        for (String id : a.state.segmentIds()) assertTrue(id.matches("[a-z0-9][a-z0-9./-]*"), id);
    }

    @Test void aWrongSegmentMapIsRefused() {
        Driver a = Stage1OccupancyJoinTest.sequence(false);
        SortedMap<String, byte[]> m = new TreeMap<>(a.state.segments());
        String some = m.keySet().stream().filter(k -> !k.equals(Segments.HEAD)).findFirst().orElseThrow();
        SortedMap<String, byte[]> moved = new TreeMap<>(m);
        moved.put(some + "x", moved.remove(some));
        assertThrows(IllegalArgumentException.class, () -> Stage1State.fromSegments(moved));
        SortedMap<String, byte[]> headless = new TreeMap<>(m);
        headless.remove(Segments.HEAD);
        assertThrows(IllegalArgumentException.class, () -> Stage1State.fromSegments(headless));
        SortedMap<String, byte[]> versioned = new TreeMap<>(m);
        ObjectNode h = (ObjectNode) Json.read(m.get(Segments.HEAD));
        h.put("segmentVersion", 2);
        versioned.put(Segments.HEAD, Canonical.bytes(h));
        assertThrows(IllegalArgumentException.class, () -> Stage1State.fromSegments(versioned));
    }

    static void same(SortedMap<String, byte[]> a, SortedMap<String, byte[]> b) {
        assertEquals(a.keySet(), b.keySet());
        for (String k : a.keySet()) assertArrayEquals(a.get(k), b.get(k), k);
    }

    // ------------------------------------------------------------------------------------------------------- the size bound

    /** A synthetic station2 run: one Running scan every {@code scanMs}, a visit every 5 min (arrival, departure 4 min later), a cut
     *  every minute, each cut drained as an engine without a snapshot hook would. */
    record Run(long cuts, long totalBytes, long largest, String largestId, long segments, long maxDrain, double meanDrain, long fullBytes) {}

    static Run synthetic(long hours, long scanMs) {
        Driver d = new Driver(Fixtures.line2(), S2);
        d.shadow = null;
        long end = T + hours * 3_600_000L, maxDrain = 0, drained = 0, cuts = 0;
        d.state.drainChanged();
        int visit = 0;
        for (long t = T; t < end; t += 60_000) {
            if ((t - T) % 300_000 == 0) { visit++; d.input(OCC, arrival("v" + visit, t, visit % 2 == 0 ? U1 : U2, S2)); }
            if ((t - T) % 300_000 == 240_000) d.input(OCC, departure("v" + visit + "-d", t, visit % 2 == 0 ? U1 : U2, S2, "v" + visit));
            for (long s = t; s < t + 60_000; s += scanMs) d.scan(s);
            d.cut();
            Segments.Drain dr = d.state.drainChanged();
            long n = dr.writtenBytes();
            maxDrain = Math.max(maxDrain, n);
            drained += n;
            cuts++;
        }
        SortedMap<String, byte[]> all = d.state.segments();
        long total = 0, largest = 0;
        String largestId = null;
        for (Map.Entry<String, byte[]> e : all.entrySet()) {
            total += e.getValue().length;
            if (e.getValue().length > largest) { largest = e.getValue().length; largestId = e.getKey(); }
        }
        return new Run(cuts, total, largest, largestId, all.size(), maxDrain, (double) drained / cuts, Canonical.bytes(d.state.toJson()).length);
    }

    @Test void theLargestSegmentStaysBoundedWhileTheStateGrows() {
        Run shortRun = synthetic(1, 2_000), longRun = synthetic(6, 2_000);
        assertTrue(longRun.totalBytes() > 4 * shortRun.totalBytes(), shortRun + " / " + longRun);
        assertTrue(longRun.largest() <= shortRun.largest() * 3 / 2, "largest segment " + shortRun + " / " + longRun);
        assertTrue(longRun.maxDrain() < longRun.totalBytes() / 20, "a cut drains a small part of the state: " + longRun);
        metrics("synthetic station2, 2 s scans, a visit per 5 min, a cut per minute", Map.of("1 h", shortRun, "6 h", longRun));
    }

    /** With {@code -Dforseti.segmentMetrics=<file>} the runs are appended there as a markdown table (scratch evidence, not a check). */
    static void metrics(String title, Map<String, Run> runs) {
        String f = System.getProperty("forseti.segmentMetrics");
        if (f == null) return;
        StringBuilder b = new StringBuilder("\n### ").append(title).append("\n\n| run | cuts | segments | total bytes | full toJson bytes | largest segment | largest id | max drained / cut | mean drained / cut |\n|---|---|---|---|---|---|---|---|---|\n");
        new TreeMap<>(runs).forEach((k, r) -> b.append("| ").append(k).append(" | ").append(r.cuts()).append(" | ").append(r.segments()).append(" | ").append(r.totalBytes())
            .append(" | ").append(r.fullBytes()).append(" | ").append(r.largest()).append(" | `").append(r.largestId()).append("` | ").append(r.maxDrain())
            .append(" | ").append(Math.round(r.meanDrain())).append(" |\n"));
        try { Files.writeString(Path.of(f), b.toString(), StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    }

    /** The horizon-length run (25 h of event time, past the 24 h horizon and its expiry), only with {@code -Dforseti.segmentHorizon}. */
    @Test void horizonLengthMetrics() {
        if (System.getProperty("forseti.segmentHorizon") == null) return;
        // the hand-made sequence (two visits, a correction, settlement and expiry): drained bytes per decide step, with its 4 cuts
        Driver h = Stage1OccupancyJoinTest.sequence(false);
        SortedMap<String, byte[]> all = h.state.segments();
        long total = 0, largest = 0;
        String largestId = null;
        for (Map.Entry<String, byte[]> e : all.entrySet()) { total += e.getValue().length; if (e.getValue().length > largest) { largest = e.getValue().length; largestId = e.getKey(); } }
        long max = h.shadow.drainedPerStep.stream().mapToLong(Long::longValue).max().orElse(0);
        metrics("hand-made occupancy-join sequence (Stage1OccupancyJoinTest.sequence), drained after every decide step", Map.of("sequence",
            new Run(h.shadow.steps, total, largest, largestId, all.size(), max, (double) h.shadow.drainedBytes / h.shadow.steps, Canonical.bytes(h.state.toJson()).length)));
        metrics("horizon-length synthetic station2, 10 s scans, a visit per 5 min, a cut per minute", Map.of("25 h", synthetic(25, 10_000)));
    }
}
