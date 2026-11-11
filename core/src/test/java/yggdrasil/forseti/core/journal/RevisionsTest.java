package yggdrasil.forseti.core.journal;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

class RevisionsTest {
    static final JsonNode KEY = Json.object().put("station", "synthetic-site:line2:station1").put("visitId", "occ-1");

    static ObjectNode body(String status) { return Json.object().put("status", status).put("count", 59); }
    static String digest(JsonNode body) { return Canonical.digest(body); }
    static Pins pins(String runId) {
        return new Pins(Json.object().put("attribution", "r"), Json.object().put("id", "h"), Json.object().put("occupancy", "b"),
            Map.of("synthetic-site:line2:station1:nutrunner1", new Pins.Definition("tpl", "1.0.0", "ab".repeat(32))),
            new Pins.Job("core-harness", "0.1.0", "cd".repeat(32), runId, "payload", Json.JSON.getNodeFactory().textNode("off"), "ef".repeat(32)));
    }
    static Envelope envelope(long revision, String supersedes, String runId) {
        return new Envelope(1, revision, supersedes, Json.object().put("kind", "timer").put("deadline", 100L), 99L,
            null, Json.JSON.createArrayNode(), pins(runId));
    }

    @Test void theFirstRevisionIsOne() {
        assertEquals(Optional.of(new Revisions.Emission(1, null)), new Revisions().next(1, KEY, digest(body("provisional"))));
    }

    @Test void aChangedBodyTakesRevisionTwoAndSupersedesTheFirstDigest() {
        Revisions r = new Revisions();
        String d1 = digest(body("provisional")), d2 = digest(body("complete"));
        r.next(1, KEY, d1);
        assertEquals(Optional.of(new Revisions.Emission(2, d1)), r.next(1, KEY, d2));
    }

    @Test void anUnchangedBodyEmitsNothing() {
        Revisions r = new Revisions();
        r.next(1, KEY, digest(body("provisional")));
        assertEquals(Optional.empty(), r.next(1, KEY, digest(body("provisional"))));
        assertEquals(Optional.of(new Revisions.Emission(2, digest(body("provisional")))), r.next(1, KEY, digest(body("complete"))));
    }

    @Test void anEnvelopeOnlyChangeEmitsNothing() {
        Revisions r = new Revisions();
        Row a = Row.of("forseti.work-records", KEY, body("complete"), envelope(1, null, "run-a"));
        Row b = Row.of("forseti.work-records", KEY, body("complete"), envelope(1, null, "run-b"));
        assertNotEquals(a.envelope(), b.envelope());
        assertEquals(a.bodyDigest(), b.bodyDigest());   // the envelope is outside the body digest
        assertTrue(r.next(1, KEY, a.bodyDigest()).isPresent());
        assertTrue(r.next(1, KEY, b.bodyDigest()).isEmpty());
    }

    @Test void aDeleteTakesTheNextRevisionAndSupersedesTheLastDigest() {
        Revisions r = new Revisions();
        String d1 = digest(body("provisional")), d2 = digest(body("complete"));
        r.next(1, KEY, d1);
        r.next(1, KEY, d2);
        assertEquals(new Revisions.Emission(3, d2), r.delete(1, KEY));
        assertThrows(IllegalStateException.class, () -> r.delete(1, KEY), "nothing live to delete");
    }

    @Test void aReappearanceAfterADeleteContinuesAtTheNextNumber() {
        Revisions r = new Revisions();
        String d1 = digest(body("provisional"));
        r.next(1, KEY, d1);
        r.delete(1, KEY);
        assertEquals(Optional.of(new Revisions.Emission(3, null)), r.next(1, KEY, d1), "the same body as before the delete is a new revision");
        assertEquals(Optional.empty(), r.next(1, KEY, d1));
    }

    @Test void anotherGenerationStartsAtOne() {
        Revisions r = new Revisions();
        r.next(1, KEY, digest(body("provisional")));
        r.next(1, KEY, digest(body("complete")));
        assertEquals(Optional.of(new Revisions.Emission(1, null)), r.next(2, KEY, digest(body("complete"))));
    }
}
