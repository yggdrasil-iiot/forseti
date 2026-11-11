package yggdrasil.forseti.core.journal;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.journal.Disposition.Kind;
import yggdrasil.forseti.core.journal.Disposition.Reason;

class DispositionsTest {
    static final JsonNode IDENTITY = Json.object().put("source", "mes-line2").put("eventId", "occ-1");

    static List<String> fields(JsonNode n) { List<String> l = new ArrayList<>(); n.fieldNames().forEachRemaining(l::add); return l; }
    static Disposition.Body body(Reason reason) { return new Disposition.Body("occupancy", IDENTITY, "ab".repeat(32), reason.kind(), reason, Json.object()); }

    @Test void aDeliveryKeyCarriesItsPartInContractFieldOrder() {
        Disposition.Key event = new Disposition.Key(1, "mfg.occupancy", 0, 7, Disposition.Part.EVENT);
        Disposition.Key correction = new Disposition.Key(1, "mfg.occupancy", 0, 7, Disposition.Part.ERROR_DECLARATION);
        assertEquals(List.of("generation", "inputTopic", "inputPartition", "inputOffset", "part"), fields(event.toJson()));
        assertEquals("event", event.toJson().path("part").textValue());
        assertEquals("errorDeclaration", correction.toJson().path("part").textValue());
        assertNotEquals(event.toJson(), correction.toJson());
        assertEquals(event, Disposition.Key.fromJson(event.toJson()));
        assertThrows(IllegalArgumentException.class, () -> new Disposition.Key(1, "mfg.occupancy", 0, 7, "whole"));
        assertThrows(IllegalArgumentException.class, () -> new Disposition.Key(1, "mfg.occupancy", 0, 7, "member:"));
        Disposition d = new Disposition(event, body(Reason.ACCEPTED), 1);
        assertEquals(List.of("inputKind", "identity", "payloadDigest", "disposition", "reason", "refs"), fields(d.body().toJson()));
        assertEquals(d, Disposition.fromJson(d.toJson()));
    }

    @Test void aLaterConflictTakesTheNextRevisionUnderTheSameKey() {
        Disposition.Key key = new Disposition.Key(1, "mfg.occupancy", 2, 40, Disposition.Part.EVENT);
        Revisions revisions = new Revisions();
        Disposition.Body accepted = body(Reason.ACCEPTED);
        Disposition.Body conflict = new Disposition.Body("occupancy", IDENTITY, "ab".repeat(32), Kind.CONFLICT, Reason.IDENTITY_CONFLICT,
            Json.object().put("quarantine", "q-1"));
        Revisions.Emission first = revisions.next(1, key.toJson(), accepted.digest()).orElseThrow();
        Revisions.Emission later = revisions.next(1, key.toJson(), conflict.digest()).orElseThrow();
        assertEquals(new Revisions.Emission(1, null), first);
        assertEquals(new Revisions.Emission(2, accepted.digest()), later);
        assertEquals(Optional.empty(), revisions.next(1, key.toJson(), conflict.digest()));
        assertEquals("conflict", new Disposition(key, conflict, later.revision()).toJson().path("body").path("disposition").textValue());
    }

    @Test void memberPartsSeparateTheMembersOfOneMessage() {
        Disposition.Key torque = new Disposition.Key(1, "ot.messages", 3, 11, Disposition.Part.member("Torque"));
        Disposition.Key running = new Disposition.Key(1, "ot.messages", 3, 11, Disposition.Part.member("Running"));
        Disposition.Key whole = new Disposition.Key(1, "ot.messages", 3, 11, Disposition.Part.MESSAGE);
        assertEquals("member:Torque", torque.part());
        assertEquals(3, List.of(torque.toJson(), running.toJson(), whole.toJson()).stream().distinct().count());
        Revisions revisions = new Revisions();
        assertEquals(1, revisions.next(1, torque.toJson(), body(Reason.ACCEPTED).digest()).orElseThrow().revision());
        assertEquals(1, revisions.next(1, running.toJson(), body(Reason.ACCEPTED).digest()).orElseThrow().revision());
    }

    @Test void theVocabularyIsTheSpecsByteForByte() {
        assertEquals(List.of("accepted", "duplicate", "conflict", "rejected", "replay-required"), Arrays.stream(Kind.values()).map(Kind::wire).toList());
        assertEquals(List.of("accepted:accepted", "accepted:late-after-settlement", "duplicate:identical-redelivery", "duplicate:superseded",
                "conflict:identity-conflict", "conflict:measurement-conflict", "conflict:correction-conflict", "rejected:schema-invalid",
                "rejected:unknown-device", "rejected:unknown-readpoint", "rejected:definition-mismatch", "rejected:outside-incident-scope",
                "rejected:station-change", "rejected:no-source-timestamp", "replay-required:beyond-horizon", "replay-required:correction-beyond-horizon"),
            Arrays.stream(Reason.values()).map(r -> r.kind().wire() + ":" + r.wire()).toList());
        assertEquals(List.of("sources", "identity.pattern", "bizStep", "correction.reasons", "kinds", "revisionEventTime"),
            Arrays.stream(Disposition.ProfileCheck.values()).map(Disposition.ProfileCheck::wire).toList());
        assertThrows(IllegalArgumentException.class, () -> new Disposition.Body("occupancy", IDENTITY, "ab".repeat(32), Kind.ACCEPTED, Reason.SUPERSEDED, Json.object()));
        assertEquals(Reason.CORRECTION_BEYOND_HORIZON, Reason.of("correction-beyond-horizon"));
        assertThrows(IllegalArgumentException.class, () -> Reason.of("late"));
    }
}
