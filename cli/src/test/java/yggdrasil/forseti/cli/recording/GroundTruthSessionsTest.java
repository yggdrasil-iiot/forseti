package yggdrasil.forseti.cli.recording;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;

/** The ground truth's session intervals (spec 517), the same rule as the core's and the oracle's: a session-down or unknown interval
 *  ends at the first delivered session-up of a different session identity with {@code at >= max(from, birthPublishTs)}, whatever W1;
 *  W1 only while there is none; an interval whose end is not after its start is dropped. */
class GroundTruthSessionsTest {
    static final String DEV = "dev-1";

    static GroundTruth.Delivery event(String kind, long birth, long bdSeq, ObjectNode payload) {
        ObjectNode v = Json.object().put("edge", "edge-1").put("device", DEV);
        v.put("birthPublishTs", birth).put("bdSeq", bdSeq).put("kind", kind).put("atSeq", 1);
        v.set("payload", payload);
        return new GroundTruth.Delivery("ot.sessions", v, 0);
    }
    static GroundTruth.Delivery down(long from, long birth, long bdSeq) { return event("session-down", birth, bdSeq, Json.object().put("from", from).putNull("to")); }
    static GroundTruth.Delivery up(long at, long birth, long bdSeq) { return event("session-up", birth, bdSeq, Json.object().put("at", at).put("rebirth", false)); }

    static GroundTruth.Intervals at(long w1, GroundTruth.Delivery... d) { return GroundTruth.sessionIntervals(List.of(d), 0, dev -> w1, Map.of()); }
    static List<List<Long>> list(Map<String, List<long[]>> m) {
        List<List<Long>> l = new ArrayList<>();
        for (long[] i : m.getOrDefault(DEV, List.of())) l.add(List.of(i[0], i[1]));
        return l;
    }

    @Test void aSessionUpOfADifferentIdentityEndsTheDownInterval() {
        GroundTruth.Intervals iv = at(20_000, down(5_000, 1_000, 0), up(1_000, 1_000, 0), up(8_000, 8_000, 0));   // a rebirth: same bdSeq
        assertEquals(List.of(List.of(5_000L, 8_000L)), list(iv.down()));
        assertEquals(List.of(List.of(5_000L, 8_000L)), list(iv.gaps()));
    }

    @Test void theDeadSessionsOwnSessionUpNeverEndsItsInterval() {
        assertEquals(List.of(List.of(5_000L, 20_000L)), list(at(20_000, down(5_000, 5_000, 1), up(5_000, 5_000, 1)).down()));
        assertEquals(List.of(List.of(5_000L, 9_000L)), list(at(20_000, down(5_000, 5_000, 1), up(5_000, 5_000, 1), up(9_000, 9_000, 2)).down()));
    }

    @Test void aSessionUpAfterW1StillEndsTheInterval() {
        assertEquals(List.of(List.of(5_000L, 9_000L)), list(at(7_000, down(5_000, 1_000, 0), up(9_000, 9_000, 1)).down()));
    }

    @Test void aSessionUpBeforeMaxOfFromAndBirthIsIgnored() {
        assertEquals(List.of(List.of(5_000L, 9_000L)), list(at(20_000, down(5_000, 1_000, 0), up(4_000, 4_000, 1), up(9_000, 9_000, 2)).down()));
        assertEquals(List.of(List.of(5_000L, 9_000L)), list(at(20_000, down(5_000, 6_000, 0), up(5_500, 5_500, 1), up(9_000, 9_000, 2)).down()));
    }

    @Test void anIntervalWhoseEndIsNotAfterItsStartIsDropped() {
        GroundTruth.Intervals iv = at(4_000, down(5_000, 1_000, 0));   // W1 before from, no session-up
        assertEquals(List.of(), list(iv.down()));
        assertEquals(List.of(), list(iv.gaps()));
        iv = at(20_000, down(5_000, 5_000, 0), up(5_000, 5_000, 1));   // a successor born in the same millisecond as from
        assertEquals(List.of(), list(iv.down()));
        ObjectNode amb = Json.object().put("detectedEventTime", 6_000).put("rawPublishTs", 0).put("bdSeq", 0).put("payloadDigest", "0".repeat(64));
        assertEquals(List.of(List.of(6_000L, 7_000L)), list(at(20_000, event("session-ambiguous", 1_000, 0, amb), up(6_000, 1_000, 0), up(7_000, 7_000, 1)).unknown()));
        assertEquals(List.of(), list(at(3_000, event("session-ambiguous", 1_000, 0, amb)).unknown()));
    }

    static GroundTruth.Delivery gap(long from, long to) { return event("seq-gap", 1_000, 0, Json.object().put("from", from).put("to", to).put("missing", 2)); }

    @Test void aSeqGapIsClosedAndReversalNormalised() {
        assertEquals(List.of(List.of(5_000L, 7_001L)), list(at(20_000, gap(5_000, 7_000)).gaps()));
        assertEquals(List.of(List.of(5_000L, 5_001L)), list(at(20_000, gap(5_000, 5_000)).gaps()));   // to == from
        assertEquals(List.of(List.of(5_000L, 7_001L)), list(at(20_000, gap(7_000, 5_000)).gaps()));   // to < from
        long[] g = at(20_000, gap(5_000, 7_000)).gaps().get(DEV).get(0);
        long jFrom = 7_000, jTo = 9_000;   // an evaluation interval starting exactly at to overlaps, as the work record counts it
        assertTrue(g[0] < jTo && jFrom < g[1]);
        assertFalse(g[0] < jTo && 7_001 < g[1]);
        assertEquals(List.of(), list(at(20_000, gap(5_000, 7_000)).down()));   // a seq-gap rejects no sample
    }
}
