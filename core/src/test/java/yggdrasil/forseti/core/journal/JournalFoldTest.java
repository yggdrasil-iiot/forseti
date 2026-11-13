package yggdrasil.forseti.core.journal;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

class JournalFoldTest {
    static final String TOPIC = "forseti.work-records";
    static final JsonNode A = Json.object().put("visitId", "occ-1"), B = Json.object().put("visitId", "occ-3");

    static ObjectNode body(String status) { return Json.object().put("status", status); }
    static Pins pins() {
        return new Pins(Json.object(), Json.object(), Json.object(), Map.of(),
            new Pins.Job("core-harness", "0.1.0", "cd".repeat(32), "run-1", "payload", Json.JSON.getNodeFactory().textNode("off"), "ef".repeat(32)));
    }
    static Row row(long generation, JsonNode key, String status, long revision, String supersedes, long triggerOffset) {
        Envelope e = new Envelope(generation, revision, supersedes, Json.object().put("kind", "input").put("inputKind", "occupancy"), 10L,
            new Envelope.Trigger("mfg.occupancy", 0, triggerOffset), Json.JSON.createArrayNode(), pins());
        return Row.of(TOPIC, key, body(status), e);
    }
    static Progress.Stage1 progress(long cutId, long through) {
        return new Progress.Stage1(1, "synthetic-site:line2:station1", cutId, 0, 1000 * cutId - 1, cutId == 1 ? null : 1000 * (cutId - 1) - 1,
            List.of(new Progress.BarrierRef(cutId, new Progress.Origin("mfg.occupancy", 0), through)), 0, null, List.of(),
            cutId == 1 ? new Progress.InitEvidence(0, 1, "none") : null);
    }
    static String d(String status) { return Canonical.digest(body(status)); }
    /** Every entry goes through its JSON form first, as a journal file is read. */
    static List<JournalFold.Entry> viaJson(List<JournalFold.Entry> in) {
        List<JournalFold.Entry> out = new ArrayList<>();
        for (JournalFold.Entry e : in) {
            if (e instanceof Row r) out.add(Row.fromJson(Json.read(Canonical.bytes(r.toJson()))));
            else if (e instanceof Delete x) out.add(Delete.fromJson(Json.read(Canonical.bytes(x.toJson()))));
            else if (e instanceof Progress p) out.add(Progress.fromJson(Json.read(Canonical.bytes(p.toJson()))));
            else out.add(e);
        }
        assertEquals(in, out);
        return out;
    }

    @Test void theFoldKeepsTheHighestRevisionPerKeyDropsDeletedKeysAndIgnoresProgress() {
        List<JournalFold.Entry> journal = viaJson(List.of(
            row(1, A, "provisional", 1, null, 0),
            row(1, B, "provisional", 1, null, 1),
            row(2, A, "other-generation", 1, null, 1),
            progress(1, 1),
            row(1, A, "complete", 2, d("provisional"), 2),
            new Delete(1, B, 2, d("provisional")),
            progress(2, 2),
            row(1, B, "after-the-fold", 3, null, 3)));
        JournalFold.Result r = JournalFold.fold(journal, 1, 2);
        assertTrue(r.reachedProgress());
        assertTrue(r.violations().isEmpty(), r.violations().toString());
        assertEquals(List.of(Canonical.utf8(Canonical.bytes(A))), List.copyOf(r.live().keySet()));
        Row live = (Row) r.live().get(Canonical.utf8(Canonical.bytes(A)));
        assertEquals(2, live.envelope().revision());
        assertEquals(body("complete"), live.body());
        assertEquals(2, JournalFold.fold(journal, 1, 1).live().size(), "through cut 1 both keys are live");
    }

    @Test void aRevisionGapIsDetected() {
        JournalFold.Result r = JournalFold.fold(List.of(row(1, A, "provisional", 1, null, 0), row(1, A, "complete", 3, d("provisional"), 0), progress(1, 0)), 1, 1);
        assertEquals(1, r.violations().revisionGaps().size(), r.violations().toString());
        assertEquals(1, JournalFold.fold(List.of(row(1, A, "provisional", 2, null, 0), progress(1, 0)), 1, 1).violations().revisionGaps().size(),
            "a chain starts at revision 1");
    }

    @Test void equalConsecutiveDigestsAreDetected() {
        JournalFold.Result r = JournalFold.fold(List.of(row(1, A, "provisional", 1, null, 0), row(1, A, "provisional", 2, d("provisional"), 0), progress(1, 0)), 1, 1);
        assertEquals(1, r.violations().equalConsecutiveDigests().size(), r.violations().toString());
        assertEquals(0, r.violations().revisionGaps().size());
    }

    @Test void aDeleteWhoseSupersedesDigestIsNotTheLastDigestIsDetected() {
        JournalFold.Result r = JournalFold.fold(List.of(row(1, A, "provisional", 1, null, 0), row(1, A, "complete", 2, d("provisional"), 0),
            new Delete(1, A, 3, d("provisional")), progress(1, 0)), 1, 1);
        assertEquals(1, r.violations().deleteSupersedesMismatches().size(), r.violations().toString());
        assertTrue(r.live().isEmpty());
    }

    @Test void aBusinessRowAfterItsCutsProgressIsDetected() {
        JournalFold.Result r = JournalFold.fold(List.of(row(1, A, "provisional", 1, null, 0), progress(1, 1),
            row(1, B, "provisional", 1, null, 1), progress(2, 2)), 1, 2);   // offset 1 belongs to cut 1, yet its row follows progress 1
        assertEquals(1, r.violations().rowsAfterProgress().size(), r.violations().toString());
        assertTrue(JournalFold.fold(List.of(progress(1, 1), row(1, B, "provisional", 1, null, 2), progress(2, 2)), 1, 2).violations().isEmpty());
    }

    static Row timerRow(JsonNode key, String status, JsonNode cause) {
        Envelope e = new Envelope(1, 1, null, cause, 10L, null, Json.JSON.createArrayNode(), pins());
        return Row.of(TOPIC, key, body(status), e);
    }

    @Test void aReplayTimerRowIsJudgedByItsDeadlineLikeATimerRow() {
        // progress 1 closes W1 = 999: a timer row due at 999 belongs to cut 1, one due at 1000 to cut 2, whichever cause carries it
        for (String kind : List.of(Envelope.ONLINE, Envelope.REPLAYED)) {
            JsonNode due = Envelope.timerCause(kind, 999), later = Envelope.timerCause(kind, 1000);
            assertEquals(999L, Envelope.timerDeadline(due), kind);
            JournalFold.Result after = JournalFold.fold(List.of(progress(1, 1), timerRow(A, "complete", due), progress(2, 2)), 1, 2);
            assertEquals(1, after.violations().rowsAfterProgress().size(), kind + ": " + after.violations());
            JournalFold.Result next = JournalFold.fold(List.of(progress(1, 1), timerRow(A, "complete", later), progress(2, 2)), 1, 2);
            assertTrue(next.violations().isEmpty(), kind + ": " + next.violations());
        }
        assertEquals("{\"identity\":{\"deadline\":999},\"inputKind\":\"timer\",\"kind\":\"replayed\"}",
            Canonical.utf8(Canonical.bytes(Envelope.timerCause(Envelope.REPLAYED, 999))), "spec 583: the replayed timer cause");
        assertNull(Envelope.timerDeadline(Envelope.inputCause(Envelope.REPLAYED, "occupancy", Json.object().put("deadline", 999))), "a replayed input is no timer");
        assertNull(Envelope.timerDeadline(Envelope.inputCause(Envelope.ONLINE, "occupancy", A)));
        assertThrows(IllegalArgumentException.class, () -> Envelope.timerCause("timer", 1));
    }

    @Test void progressRecordsCarryTheSpecFieldsAndOnlySafeIntegers() {
        Progress.Stage1 p1 = progress(1, -1);
        assertEquals(List.of("type", "generation", "station", "cutId", "producerEpoch", "W1", "admissionW", "barrierRefs", "restartEpoch",
            "recoveryRef", "mergedCutIds", "initEvidence"), fields(p1.toJson()));
        assertFalse(progress(2, 3).toJson().has("initEvidence"), "initEvidence only on the first progress after an initialization");
        assertTrue(p1.toJson().path("admissionW").isNull());
        Progress.Stage2 p2 = new Progress.Stage2(1, "synthetic-site:line2:station1", 1, 0, 999,
            List.of(new Progress.Stage1ProgressRef("forseti.work-records", 0, 1), new Progress.Stage1ProgressRef("forseti.samples", 0, 1)),
            List.of(new Progress.IncidentOffset(1, 0, -1)), List.of());
        assertEquals(List.of("type", "generation", "station", "cutId", "producerEpoch", "W2", "stage1ProgressRefs", "incidentOffsets", "mergedCutIds"), fields(p2.toJson()));
        Progress.Ledger l = new Progress.Ledger(1, 1, 6, null, 1, 0, List.of(new Progress.BarrierRef(1, new Progress.Origin("ot.messages", 2), 41)), List.of(), 0, null, null);
        assertEquals(List.of("type", "generation", "stage", "partition", "station", "cutId", "producerEpoch", "barrierRefs", "mergedCutIds",
            "restartEpoch", "recoveryRef"), fields(l.toJson()));
        for (Progress p : List.of(p1, p2, l)) assertEquals(p, Progress.fromJson(Json.read(Canonical.bytes(p.toJson()))));

        ObjectNode bad = p1.toJson().deepCopy();
        bad.put("cutId", new java.math.BigInteger("9007199254740992"));
        assertThrows(IllegalArgumentException.class, () -> Progress.fromJson(bad));
        ObjectNode belowThrough = p1.toJson().deepCopy();
        ((ObjectNode) belowThrough.path("barrierRefs").get(0)).put("throughOffset", -2);
        assertThrows(IllegalArgumentException.class, () -> Progress.fromJson(belowThrough));
        assertThrows(IllegalArgumentException.class, () -> new Progress.InitEvidence(0, 0, "none"), "endOffset >= 1");
        assertThrows(IllegalStateException.class, () -> new Progress.Stage1(1, "s", 9007199254740992L, 0, 0, null, List.of(), 0, null, List.of(), null).toJson());
    }

    static List<String> fields(JsonNode n) { List<String> l = new ArrayList<>(); n.fieldNames().forEachRemaining(l::add); return l; }
}
