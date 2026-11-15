package yggdrasil.forseti.core.stage1;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.stage1.Coverage.Interval;

/** The end of a session-down and an unknown interval (rule sheet S8, spec 517): the first delivered session-up of a different session
 *  identity with {@code at >= max(from, birthPublishTs)}, whatever W1; W1 only while there is none; empty intervals are dropped. */
class SessionsTest {
    static final String EDGE = "edge-1", DEV = "dev-1";

    static void add(Stage1State s, String kind, Long birth, long bdSeq, ObjectNode payload) {
        ObjectNode id = Json.object().put("edge", EDGE).put("device", DEV);
        if (birth == null) id.putNull("birthPublishTs"); else id.put("birthPublishTs", birth);
        id.put("bdSeq", bdSeq).put("kind", kind).put("atSeq", s.sessions.size() + 1);
        Stage1State.Session x = new Stage1State.Session(id, DEV, Sessions.sessionKey(EDGE, DEV, birth, bdSeq), kind);
        x.list.add(new Stage1State.SessionDelivery(null, payload, payload.toString(), "0".repeat(64), "jcs", null));
        s.sessions.put(id.toString(), x);
    }
    static void down(Stage1State s, long from, long birth, long bdSeq) { add(s, "session-down", birth, bdSeq, Json.object().put("from", from).putNull("to")); }
    static void up(Stage1State s, long at, long birth, long bdSeq) { add(s, "session-up", birth, bdSeq, Json.object().put("at", at).put("rebirth", false)); }
    static Stage1State state() { return new Stage1State(1, "st", 0, "occupancy-join"); }

    @Test void aSessionUpOfADifferentIdentityEndsTheDownInterval() {
        // a rebirth: same bdSeq, another birthPublishTs
        Stage1State s = state();
        down(s, 5_000, 1_000, 0); up(s, 1_000, 1_000, 0); up(s, 8_000, 8_000, 0);
        Sessions.View v = Sessions.view(s, 20_000L);
        assertEquals(List.of(new Interval(5_000, 8_000)), v.down(DEV));
        assertEquals(List.of(new Interval(5_000, 8_000)), v.gaps(DEV));
    }

    @Test void theDeadSessionsOwnSessionUpNeverEndsItsInterval() {
        // a no-telemetry session: its down's from is its birth, the instant of its own session-up
        Stage1State s = state();
        down(s, 5_000, 5_000, 1); up(s, 5_000, 5_000, 1);
        assertEquals(List.of(new Interval(5_000, 20_000)), Sessions.view(s, 20_000L).down(DEV));
        up(s, 9_000, 9_000, 2);
        assertEquals(List.of(new Interval(5_000, 9_000)), Sessions.view(s, 20_000L).down(DEV));
    }

    @Test void aSessionUpAfterW1StillEndsTheInterval() {
        Stage1State s = state();
        down(s, 5_000, 1_000, 0); up(s, 9_000, 9_000, 1);
        assertEquals(List.of(new Interval(5_000, 9_000)), Sessions.view(s, 7_000L).down(DEV));
    }

    @Test void aSessionUpBeforeMaxOfFromAndBirthIsIgnored() {
        Stage1State s = state();
        down(s, 5_000, 1_000, 0); up(s, 4_000, 4_000, 1); up(s, 9_000, 9_000, 2);
        assertEquals(List.of(new Interval(5_000, 9_000)), Sessions.view(s, 20_000L).down(DEV));
        // at or after from, before the down's own birthPublishTs
        Stage1State t = state();
        down(t, 5_000, 6_000, 0); up(t, 5_500, 5_500, 1); up(t, 9_000, 9_000, 2);
        assertEquals(List.of(new Interval(5_000, 9_000)), Sessions.view(t, 20_000L).down(DEV));
    }

    @Test void anIntervalWhoseEndIsNotAfterItsStartIsDropped() {
        // W1 before from, no session-up delivered
        Stage1State s = state();
        down(s, 5_000, 1_000, 0);
        Sessions.View v = Sessions.view(s, 4_000L);
        assertEquals(List.of(), v.down(DEV));
        assertEquals(List.of(), v.gaps(DEV));
        // a successor born in the same millisecond as from
        Stage1State t = state();
        down(t, 5_000, 5_000, 0); up(t, 5_000, 5_000, 1);
        assertFalse(Sessions.view(t, 20_000L).any());
        // an unknown interval: its own identity excluded, the successor ends it; inverted against W1 it is dropped
        Stage1State u = state();
        ObjectNode amb = Json.object().put("detectedEventTime", 6_000).put("rawPublishTs", 0).put("bdSeq", 0).put("payloadDigest", "0".repeat(64));
        add(u, "session-ambiguous", 1_000L, 0, amb); up(u, 6_000, 1_000, 0); up(u, 7_000, 7_000, 1);
        assertEquals(List.of(new Interval(6_000, 7_000)), Sessions.view(u, 20_000L).unknown(DEV));
        Stage1State w = state();
        add(w, "session-ambiguous", 1_000L, 0, amb);
        assertEquals(List.of(), Sessions.view(w, 3_000L).unknown(DEV));
    }

    static void gap(Stage1State s, long from, long to) { add(s, "seq-gap", 1_000L, 0, Json.object().put("from", from).put("to", to).put("missing", 2)); }

    @Test void aSeqGapIsClosedAndReversalNormalised() {
        Stage1State s = state(); gap(s, 5_000, 7_000);
        Sessions.View v = Sessions.view(s, 20_000L);
        assertEquals(List.of(new Interval(5_000, 7_001)), v.gaps(DEV));
        assertTrue(v.gaps(DEV).get(0).overlaps(7_000, 9_000), "an evaluation interval starting exactly at to overlaps");
        assertFalse(v.gaps(DEV).get(0).overlaps(7_001, 9_000));
        assertEquals(List.of(), v.down(DEV), "a seq-gap rejects no sample");
        Stage1State e = state(); gap(e, 5_000, 5_000);   // to == from
        assertEquals(List.of(new Interval(5_000, 5_001)), Sessions.view(e, 20_000L).gaps(DEV));
        Stage1State r = state(); gap(r, 7_000, 5_000);   // to < from
        assertEquals(List.of(new Interval(5_000, 7_001)), Sessions.view(r, 20_000L).gaps(DEV));
    }
}
