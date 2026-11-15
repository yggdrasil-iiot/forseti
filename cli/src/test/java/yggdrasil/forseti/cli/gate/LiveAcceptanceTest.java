package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;

/** The visit acceptance of a live capture: every visit complete unless an injection touches it, a touched one insufficient with the
 *  injection's reason, and injection 25(a) visible as a stale-death in the captured session events. */
class LiveAcceptanceTest {
    static final String S1 = "site-071:line2:station1", S2 = "site-071:line2:station2", T1 = S1 + ":nutrunner1";
    static final long T0 = 1_794_700_000_000L;

    static JsonNode json(String s) { return Json.read(s.getBytes(StandardCharsets.UTF_8)); }
    static LiveAcceptance.Visit visit(String ref, String station, long enter, Long leave, String status, String... reasons) {
        return new LiveAcceptance.Visit(ref, station, T0 + enter, leave == null ? null : T0 + leave, status, List.of(reasons));
    }
    static List<String> judge(String injections, LiveAcceptance.Visit... visits) {
        return LiveAcceptance.judge(T0, json(injections), List.of(visits), Json.JSON.createArrayNode());
    }

    static final String BAD = "[{\"injection\":\"3\",\"kind\":\"bad-status\",\"equipment\":\"" + T1 + "\",\"station\":\"" + S1 + "\",\"from\":70000,\"to\":100000}]";
    static final String KILL = "[{\"injection\":\"8\",\"kind\":\"kill-muninn\",\"equipment\":\"" + S1 + "\",\"station\":\"" + S1 + "\",\"from\":440000,\"to\":460000}]";

    @Test
    void aBaselineNeedsEveryVisitComplete() {
        assertEquals(List.of(), judge("[]", visit("S1-V1", S1, 60000, 120000L, "complete"), visit("S2-V1", S2, 150000, 210000L, "complete")));
        List<String> p = judge("[]", visit("S1-V1", S1, 60000, 120000L, "insufficient", "missing-periods"), visit("S2-V1", S2, 150000, 210000L, "provisional"));
        assertEquals(List.of("S1-V1 at " + S1 + " is insufficient [missing-periods], touched by no injection",
            "S2-V1 at " + S2 + " is provisional [], touched by no injection"), p);
        assertEquals(List.of("the core wrote no work record"), judge("[]"));
    }

    @Test
    void anInjectionMakesExactlyTheVisitsItTouchesInsufficientWithItsReason() {
        LiveAcceptance.Visit v1 = visit("S1-V1", S1, 60000, 120000L, "insufficient", "bad-share-exceeded"), v2 = visit("S1-V2", S1, 240000, 300000L, "complete"),
            other = visit("S2-V1", S2, 60000, 120000L, "complete");
        ArrayNode out = Json.JSON.createArrayNode();
        assertEquals(List.of(), LiveAcceptance.judge(T0, json(BAD), List.of(v1, v2, other), out));
        assertEquals("[\"3 bad-status\"]", out.get(0).get("touchedBy").toString());
        assertEquals(0, out.get(2).get("touchedBy").size(), "another station's visit in the same window is not touched");
        assertEquals(List.of("S1-V1 at " + S1 + " is complete [], touched by [3 bad-status] (needs insufficient with [bad-share-exceeded])"),
            judge(BAD, visit("S1-V1", S1, 60000, 120000L, "complete"), v2));
        assertEquals(List.of("S1-V1 at " + S1 + " is insufficient [session-gap], touched by [3 bad-status] (needs insufficient with [bad-share-exceeded])"),
            judge(BAD, visit("S1-V1", S1, 60000, 120000L, "insufficient", "session-gap")));
        assertEquals(List.of(), judge(KILL, visit("S1-V3", S1, 420000, 480000L, "insufficient", "missing-periods", "session-gap")), "other reasons may stand beside it");
        assertEquals(List.of(), judge(KILL, visit("S1-V3", S1, 420000, null, "insufficient", "session-gap")), "an open visit reaches to the end");
        assertEquals(List.of(), judge(KILL, visit("S1-V4", S1, 460000, 520000L, "complete")), "a visit that starts at the window's end is not touched");
    }

    @Test
    void theHoldAndTheOffsetTouchNoVisitAndAnUnknownKindIsAProblem() {
        String inj = "[{\"injection\":\"1\",\"kind\":\"bridge-hold\",\"startMs\":60000,\"holdMs\":600000},{\"injection\":\"9\",\"kind\":\"bridge-source-offset\",\"offsetMs\":30000}]";
        assertEquals(List.of(), judge(inj, visit("S1-V1", S1, 60000, 120000L, "complete")));
        assertEquals(List.of("injection 7: unknown kind reboot"), judge("[{\"injection\":\"7\",\"kind\":\"reboot\"}]", visit("S1-V1", S1, 60000, 120000L, "complete")));
    }

    static JsonNode ev(String kind, String device, long birth, long bdSeq) {
        return json("{\"device\":\"" + device + "\",\"kind\":\"" + kind + "\",\"birthPublishTs\":" + birth + ",\"bdSeq\":" + bdSeq + "}");
    }

    @Test
    void aHostOfflineNeedsAStaleDeathNoDownOfThatSessionAndANewerSession() {
        long b1 = T0, b2 = T0 + 230_000;
        assertEquals(List.of(), LiveAcceptance.staleDeath(List.of(ev("session-up", T1, b1, 0), ev("stale-death", T1, b1, 0), ev("session-up", T1, b2, 1)), T1));
        assertEquals(List.of("host-offline " + T1 + ": no stale-death was captured"),
            LiveAcceptance.staleDeath(List.of(ev("session-up", T1, b1, 0), ev("session-down", T1, b1, 0), ev("session-up", T1, b2, 1), ev("stale-death", S1, b1, 0)), T1));
        assertEquals(List.of("host-offline " + T1 + ": the session that stale-died (bdSeq 0) also has a session-down",
                "host-offline " + T1 + ": no newer session came up after the stale-death (bdSeq 0)"),
            LiveAcceptance.staleDeath(List.of(ev("session-up", T1, b1, 0), ev("stale-death", T1, b1, 0), ev("session-down", T1, b1, 0)), T1));
    }

    @Test
    void aWorkRecordKeyNamesTheScenarioVisit() {
        assertEquals("S1-V1", LiveAcceptance.ref("{\"arrivalEventId\":\"S1-V1-arrive\",\"mode\":\"occupancy-join\",\"source\":\"mes-line2\"}"));
        assertEquals("S1-V1", LiveAcceptance.ref("{\"visitRef\":{\"mode\":\"plc-stamped\",\"namespace\":\"n\",\"station\":\"" + S1 + "\",\"visitId\":\"S1-V1\"}}"));
    }
}
