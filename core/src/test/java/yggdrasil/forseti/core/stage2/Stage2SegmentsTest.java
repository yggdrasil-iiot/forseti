package yggdrasil.forseti.core.stage2;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.core.stage2.Stage2Test.*;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.Delete;
import yggdrasil.forseti.core.segment.Segments;

/** The segmented stage-2 state. Every {@link Stage2Test.Bench} keeps a segment shadow checked after each decide step, so every stage-2
 *  scenario of the suite is a checked sequence; these tests add randomized sequences over every change path, the no-op drain and
 *  determinism. */
class Stage2SegmentsTest {
    static final long DAY = 86_400_000L;

    @Test void theScenarioSequenceRestoresAtEveryStepAndAfterTheStoreRoundTrip() {
        for (boolean roundTrip : List.of(false, true)) {
            Bench b = sequence(roundTrip);
            assertTrue(b.shadow.checked > 60, "checked " + b.shadow.checked);
            if (!roundTrip) assertEquals(1, b.shadow.fullDrains);
        }
    }

    /** Random sequences: bases put, revised and deleted, activity and other sample rows, incidents with revisions, identical
     *  redeliveries, identity conflicts and station changes, cuts, settlement and retention jumps (bases and activity leave). */
    @Test void randomSequencesRestoreAtEveryStep() {
        long checked = 0, deletes = 0;
        Set<String> kinds = new TreeSet<>();
        for (int seed = 0; seed < 16; seed++) {
            Random r = new Random(9_100L + seed);
            Bench b = new Bench();
            long now = T, w2 = T;
            int visits = 0;
            Map<String, Long> revisions = new HashMap<>();
            List<ObjectNode> incidents = new ArrayList<>();
            for (int i = 0; i < 220; i++) {
                int a = r.nextInt(100);
                if (a < 12) {
                    visits++;
                    WR wr = r.nextBoolean() ? complete("U" + visits % 4, now, now + 40_000).nr1Active() : new WR("U" + visits % 4, now, null, "provisional").nr2Only();
                    b.basis(visit("a" + visits), wr);
                } else if (a < 16 && visits > 0) {
                    int v = 1 + r.nextInt(visits);
                    b.basis(visit("a" + v), new WR("U" + v % 4, now - 60_000, now, "retracted").retracted("error-declaration", INCORRECT_DATA).noTool());
                } else if (a < 19 && visits > 0) {
                    b.deleteBasis(visit("a" + (1 + r.nextInt(visits))));
                } else if (a < 40) {
                    b.running(r.nextBoolean() ? NR1 : NR2, now, now + 5_000, "attributed", null, BooleanNode.valueOf(r.nextBoolean()), visits == 0 ? null : visit("a" + visits));
                    if (r.nextInt(4) == 0) b.sample(NR1, "Torque", now, "attributed", null, BooleanNode.TRUE, null);
                    now += 5_000;
                } else if (a < 52) {
                    String id = "inc-" + r.nextInt(3);
                    long rev = revisions.merge(id, 1L, Long::sum);
                    boolean closed = r.nextBoolean();
                    ObjectNode n = inc(rev, r.nextBoolean() ? NR1 : NR2, now - 120_000, closed ? now : null, closed ? "closed" : "open", now, now + rev).put("incidentId", id);
                    incidents.add(n);
                    b.incident(n);
                } else if (a < 56 && !incidents.isEmpty()) {
                    b.incident(incidents.get(r.nextInt(incidents.size())));   // identical redelivery
                } else if (a < 59 && !incidents.isEmpty()) {
                    ObjectNode n = incidents.get(r.nextInt(incidents.size())).deepCopy();
                    Json.putSafe(n, "detectedAt", n.get("detectedAt").longValue() + 1);
                    b.incident(n);   // the same revision with other content: an identity conflict
                } else if (a < 61 && !incidents.isEmpty()) {
                    ObjectNode n = incidents.get(r.nextInt(incidents.size())).deepCopy();
                    Json.putSafe(n, "revision", revisions.merge(n.get("incidentId").asText(), 1L, Long::sum));
                    Json.putSafe(n, "revisionEventTime", now + 100);
                    b.incident(n.put("status", "withdrawn"));
                } else if (a < 85) { w2 = Math.max(w2, now); b.cut(w2); }
                else if (a < 92) { w2 = Math.max(w2, now) + L; b.cut(w2); }
                else { now += 40 * DAY; w2 = now; b.cut(w2); }   // past every retention: bases and activity leave
            }
            b.cut(Math.max(w2, now));
            checked += b.shadow.checked;
            deletes += b.outputs.stream().filter(o -> o instanceof Delete).count();
            for (String id : b.state.segmentIds()) kinds.add(Segments.kind(id));
        }
        assertTrue(checked > 3000, "checked " + checked);
        assertTrue(deletes > 0, "assessment deletes (revision deletes) were exercised");
        assertTrue(kinds.containsAll(List.of("head", "incidents", "units", "rev.assessments", "rev.sets", "rev.dispositions")), "kinds " + kinds);
    }

    @Test void aRecomputeThatChangesNothingDrainsOnlyTheHead() {
        Bench b = sequence(false);
        b.shadow = null;
        long w = b.state.lastW2();
        b.cut(w);
        b.state.drainChanged();
        b.cut(w);   // every incident re-judged, every set re-evaluated: nothing changes but progressCount
        Segments.Drain d = b.state.drainChanged();
        assertEquals(Set.of(Segments.HEAD), d.written().keySet());
        assertTrue(d.removed().isEmpty());
        assertTrue(b.state.drainChanged().isEmpty());
    }

    @Test void segmentsAreDeterministic() {
        Bench a = sequence(false), c = sequence(false);
        same(a.state.segments(), c.state.segments());
        same(a.state.segments(), Stage2State.fromJson(Json.read(Canonical.bytes(a.state.toJson()))).segments());
        Stage2State s = Stage2State.fromSegments(a.state.segments());
        same(a.state.segments(), s.segments());
        assertTrue(s.drainChanged().isEmpty());
        for (String id : a.state.segmentIds()) assertTrue(id.matches("[a-z0-9][a-z0-9./-]*"), id);
    }

    static void same(SortedMap<String, byte[]> a, SortedMap<String, byte[]> b) {
        assertEquals(a.keySet(), b.keySet());
        for (String k : a.keySet()) assertArrayEquals(a.get(k), b.get(k), k);
    }
}
