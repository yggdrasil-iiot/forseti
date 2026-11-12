package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;

/** The parts of the injection assertions that decide on plain JSON: the threshold of {@code J-AFTER-HORIZON-CUTS}, the retraction check
 *  of {@code I-6} and the hold-back lateness of {@code C-2-REFERENCE}. */
class InjectionsTest {
    static final long H = 86_400_000L, L = 120_000L;

    static JsonNode json(String s) { return Json.read(s.getBytes(StandardCharsets.UTF_8)); }

    @Test void theHorizonThresholdOfOpenVisitsIsTheirLatestEnterPlusHPlusL() {
        JsonNode e = json("{\"visits\": [{\"station\": \"a\", \"enter\": 100, \"leave\": 200}, {\"station\": \"a\", \"enter\": 300, \"leave\": null},"
            + " {\"station\": \"b\", \"enter\": 500, \"leave\": null}]}");
        Injections.Need n = Injections.afterHorizonNeed(e, H, L);
        assertEquals(500 + H + L, n.at());
        assertEquals("b", n.station());
        assertEquals("enter + H + L of the open visits", n.what());
    }

    @Test void withoutAnOpenVisitTheThresholdIsTheFirstOnlineExpiry() {
        // every visit closed: no open visit gives a threshold, the earliest leave + H + L does
        JsonNode e = json("{\"visits\": [{\"station\": \"a\", \"enter\": 100, \"leave\": 900}, {\"station\": \"b\", \"enter\": 300, \"leave\": 400}]}");
        Injections.Need n = Injections.afterHorizonNeed(e, H, L);
        assertEquals(400 + H + L, n.at());
        assertEquals("b", n.station());
        assertEquals("the first onlineExpiresAt (leave + H + L)", n.what());
    }

    @Test void aRetractionAbsentOrNullIsNone() {
        assertTrue(Injections.noRetraction(json("{\"status\": \"insufficient\"}")));
        assertTrue(Injections.noRetraction(json("{\"status\": \"insufficient\", \"retraction\": null}")));
        assertFalse(Injections.noRetraction(json("{\"status\": \"retracted\", \"retraction\": {\"reason\": \"overlap\"}}")));
        assertFalse(Injections.noRetraction(null));
    }

    @Test void theHoldBackLatenessIsTheDelayMinusOneScanMinusOMinusOne() {
        JsonNode m = json("{\"scanMs\": 1000, \"source\": {\"overlays\": [{\"type\": \"bad-status\"}, {\"type\": \"hold-back\", \"delayMs\": 600000}]}}");
        assertEquals(568_999L, Injections.heldBackLateness(m, 30_000L));   // held-back-10min: the gate measured 568999
        assertNull(Injections.heldBackLateness(json("{\"scanMs\": 1000, \"source\": {\"overlays\": []}}"), 30_000L));
    }
}
