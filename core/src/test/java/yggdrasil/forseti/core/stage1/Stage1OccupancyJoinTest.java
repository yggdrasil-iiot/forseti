package yggdrasil.forseti.core.stage1;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.core.stage1.Driver.*;
import static yggdrasil.forseti.core.stage1.Fixtures.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Delete;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.rules.RuleSet;

class Stage1OccupancyJoinTest {
    static final String OCC = InputParser.OCCUPANCY, MSG = InputParser.MESSAGES, SES = InputParser.SESSIONS;
    static final String U1 = "UNIT0000000000001", U2 = "UNIT0000000000002";
    static final long L = 720_000, H = 86_400_000;

    /** {@code samplesDigest} of the smoke visit (station1, [T0 + 600 s, T0 + 660 s)); oracle/tests pins the same literal. */
    static final String SMOKE_SAMPLES_DIGEST = "e518a18ba8c533fb5e3fb00529aaf1ebff54d67a07c810a5012f7fe1a926096f";
    /** The body digest of the Station2 baseline visit below (rule line2-attribution 1.0.0 with the smoke recording's definitions, so
     *  {@code rulesDigest} is de19f15a...); oracle/tests pins the same literal. */
    static final String STATION2_BODY_DIGEST = "eb403937f1f8d2340f8410c8ef345bbea9b94e1a142250798992b42b311c727c";

    /** The baseline visit at station1: occ-1 arriving at T, occ-2 departing at T + 60 s, a scan every second in between. */
    static Driver baseline() {
        Driver d = Driver.station1();
        d.input(OCC, arrival("occ-1", T, U1, S1));
        d.scans(T, T + 60_000);
        d.input(OCC, departure("occ-2", T + 60_000, U1, S1, "occ-1"));
        return d;
    }
    /** JSON equality by canonical bytes (integral node types differ between parsed and built nodes). */
    static void same(JsonNode expected, JsonNode actual) { assertEquals(Canonical.utf8(Canonical.bytes(expected)), Canonical.utf8(Canonical.bytes(actual))); }
    static String status(JsonNode body) { return body.get("status").asText(); }
    static JsonNode text(String s) { return TextNode.valueOf(s); }

    @Test void aVisitIsProvisionalThenCompleteAtSettlement() {
        Driver d = baseline();
        d.cut();
        JsonNode b = d.workRecord("occ-1");
        assertEquals("provisional", status(b));
        assertEquals(List.of(), reasons(b));
        assertEquals(T, b.get("interval").get("enter").longValue());
        assertEquals(T + 60_000, b.get("interval").get("leave").longValue());
        assertTrue(b.get("interval").get("attributionEnd").isNull());
        assertEquals(U1, b.get("unitId").asText());
        assertEquals(S1, b.get("readPoint").asText());
        assertEquals("occupancy-join", b.get("attributionMode").asText());
        assertEquals(d.rules.rulesDigest(), b.get("rulesDigest").asText());
        ObjectNode cov = Json.object().put("expectedPeriodMs", 1000).put("observedPeriods", 60).put("missingPeriods", 0).put("badSlots", 0).put("sessionGaps", 0);
        same(cov, b.get("coverage"));
        assertFalse(b.has("retraction"));
        d.cut(T + 60_000 + L - 1);
        assertEquals("provisional", status(d.workRecord("occ-1")));
        d.cut(T + 60_000 + L);
        assertEquals("complete", status(d.workRecord("occ-1")));
        List<Row> rows = d.workRecordRows("occ-1");
        Row last = rows.get(rows.size() - 1);
        assertEquals("timer", last.envelope().cause().get("kind").asText());
        assertEquals(T + 60_000 + L, last.envelope().cause().get("deadline").longValue());
        assertNull(last.envelope().trigger());
        for (int i = 0; i < rows.size(); i++) assertEquals(i + 1, rows.get(i).envelope().revision());
    }

    @Test void samplesBeforeTheirArrivalAreAttributedLater() {
        Driver d = Driver.station1();
        List<Inputs.InputRecord> scan = new ArrayList<>();
        for (long t = T; t < T + 60_000; t += 1000) scan.addAll(d.scan(t));
        d.cut();
        Inputs.InputRecord nr1 = scan.get(31);   // the nutrunner1 message of scan T + 10 s
        assertEquals(NR1, ((Inputs.ProcessMessage) nr1.parsed()).device());
        JsonNode before = d.sampleRow(nr1, "Torque");
        assertEquals("unattributed", before.get("state").asText());
        assertEquals("no-occupancy", before.get("reason").asText());
        assertEquals(BooleanNode.TRUE, before.get("active"));
        d.input(OCC, arrival("occ-1", T, U1, S1));
        d.input(OCC, departure("occ-2", T + 60_000, U1, S1, "occ-1"));
        d.cut();
        JsonNode after = d.sampleRow(nr1, "Torque");
        assertEquals("attributed", after.get("state").asText());
        assertTrue(after.get("reason").isNull());
        assertEquals(visitRef("occ-1"), after.get("attributedTo"));
        assertEquals(T + 10_000, after.get("sourceTs").longValue());
        List<Row> rows = d.rows(Stage1.SAMPLES, sampleId(nr1, "Torque"));
        assertEquals(2, rows.size());
        assertEquals(2, rows.get(1).envelope().revision());
        assertEquals(rows.get(0).bodyDigest(), rows.get(1).envelope().supersedesDigest());
        assertEquals("provisional", status(d.workRecord("occ-1")));
    }

    @Test void anIdenticalRedeliveryIsADuplicateWithNoStateChange() {
        Driver d = baseline();
        d.cut();
        JsonNode body = d.workRecord("occ-1");
        Inputs.InputRecord again = d.input(OCC, arrival("occ-1", T, U1, S1));
        List<Object> out = d.admitNow(again);
        assertEquals(1, out.size());
        Disposition disp = (Disposition) out.get(0);
        assertEquals("event", disp.key().part());
        assertEquals(Disposition.Reason.IDENTICAL_REDELIVERY, disp.body().reason());
        assertEquals("occupancy", disp.body().inputKind());
        assertEquals(Json.object().put("source", SOURCE).put("eventId", "occ-1"), disp.body().identity());
        assertEquals(body, d.workRecord("occ-1"));
    }

    @Test void aResentArrivalWithAnotherUnitIsAConflict() {
        Driver d = baseline();
        Inputs.InputRecord first = d.pending.get(0);
        d.cut();
        Inputs.InputRecord second = d.input(OCC, arrival("occ-1", T, U2, S1));
        d.cut();
        Disposition a = d.disposition(first, "event"), b = d.disposition(second, "event");
        assertEquals(Disposition.Reason.IDENTITY_CONFLICT, a.body().reason());
        assertEquals(2, a.revision());
        assertEquals(Disposition.Reason.IDENTITY_CONFLICT, b.body().reason());
        assertEquals(1, b.revision());
        assertTrue(a.body().refs().has("quarantine"));
        assertTrue(b.body().refs().has("quarantine"));
        assertEquals(2, d.outputs.stream().filter(o -> o instanceof QuarantineRecord).count());
        JsonNode w = d.workRecord("occ-1");
        assertEquals("retracted", status(w));
        assertEquals("input-conflict", w.get("retraction").get("reason").asText());
        assertTrue(w.get("retraction").get("errorReason").isNull());
    }

    @Test void anErrorDeclarationRetractsTheVisit() {
        Driver d = baseline();
        d.cut();
        Inputs.InputRecord decl = d.input(OCC, declared(arrival("occ-1", T, U1, S1), DID_NOT_OCCUR, T + 100_000));
        d.cut();
        JsonNode w = d.workRecord("occ-1");
        assertEquals("retracted", status(w));
        assertEquals(Json.object().put("reason", "error-declaration").put("errorReason", DID_NOT_OCCUR), w.get("retraction"));
        Disposition disp = d.disposition(decl, "errorDeclaration");
        assertEquals(Disposition.Reason.ACCEPTED, disp.body().reason());
    }

    @Test void anErrorDeclarationBeforeItsOriginalIsATombstone() {
        Driver a = baseline();
        a.cut();
        a.input(OCC, declared(arrival("occ-1", T, U1, S1), DID_NOT_OCCUR, T + 100_000));
        a.cut();

        Driver b = Driver.station1();
        b.input(OCC, declared(arrival("occ-1", T, U1, S1), DID_NOT_OCCUR, T + 100_000));
        b.scans(T, T + 60_000);
        b.input(OCC, departure("occ-2", T + 60_000, U1, S1, "occ-1"));
        b.cut();
        Inputs.InputRecord original = b.input(OCC, arrival("occ-1", T, U1, S1));
        b.cut();
        assertEquals(Disposition.Reason.ACCEPTED, b.disposition(original, "event").body().reason());
        for (Row r : b.workRecordRows("occ-1")) assertEquals("retracted", status(r.body()), "a tombstoned visit is never provisional");
        assertEquals(Canonical.digest(a.workRecord("occ-1")), Canonical.digest(b.workRecord("occ-1")));
    }

    @Test void anErrorDeclarationOnTheDepartureReopensTheVisit() {
        Driver d = baseline();
        d.cut();
        Inputs.InputRecord decl = d.input(OCC, declared(departure("occ-2", T + 60_000, U1, S1, "occ-1"), DID_NOT_OCCUR, T + 100_000));
        d.cut();
        JsonNode w = d.workRecord("occ-1");
        assertTrue(w.get("interval").get("leave").isNull());
        assertEquals("provisional", status(w));
        assertEquals(List.of(), reasons(w));
        assertEquals(Disposition.Reason.ACCEPTED, d.disposition(decl, "errorDeclaration").body().reason());
    }

    @Test void twoDeparturesWithDifferentLeavesAreACorrectionConflict() {
        Driver d = baseline();
        Inputs.InputRecord other = d.input(OCC, departure("occ-3", T + 61_000, U1, S1, "occ-1"));
        d.cut();
        JsonNode w = d.workRecord("occ-1");
        assertTrue(w.get("interval").get("leave").isNull());
        assertEquals("insufficient", status(w));
        assertTrue(reasons(w).contains("correction-conflict"));
        assertEquals(Disposition.Reason.ACCEPTED, d.disposition(other, "event").body().reason());
    }

    @Test void aCancellationResolvesADepartureConflict() {
        Driver d = baseline();
        d.input(OCC, departure("occ-3", T + 61_000, U1, S1, "occ-1"));
        d.cut();
        assertTrue(reasons(d.workRecord("occ-1")).contains("correction-conflict"));
        d.input(OCC, declared(departure("occ-3", T + 61_000, U1, S1, "occ-1"), DID_NOT_OCCUR, T + 100_000));
        d.cut();
        JsonNode w = d.workRecord("occ-1");
        assertEquals(T + 60_000, w.get("interval").get("leave").longValue());
        assertEquals(List.of(), reasons(w));
        assertEquals("provisional", status(w));
    }

    static Driver overlapping() {
        Driver d = Driver.station1();
        d.input(OCC, arrival("occ-1", T, U1, S1));
        d.input(OCC, arrival("occ-11", T + 30_000, U2, S1));
        d.scans(T, T + 90_000);
        d.input(OCC, departure("occ-2", T + 60_000, U1, S1, "occ-1"));
        d.input(OCC, departure("occ-12", T + 90_000, U2, S1, "occ-11"));
        return d;
    }

    @Test void twoClosedOverlappingVisitsAreBothRetractedAndRecoverWhenOneIsCancelled() {
        Driver d = overlapping();
        d.cut();
        ObjectNode overlap = Json.object().put("reason", "overlap").putNull("errorReason");
        assertEquals(overlap, d.workRecord("occ-1").get("retraction"));
        assertEquals(overlap, d.workRecord("occ-11").get("retraction"));
        d.input(OCC, declared(arrival("occ-11", T + 30_000, U2, S1), DID_NOT_OCCUR, T + 100_000));
        d.cut();
        JsonNode a = d.workRecord("occ-1"), b = d.workRecord("occ-11");
        assertFalse(a.has("retraction"));
        assertEquals("provisional", status(a));
        assertEquals(List.of(), reasons(a));
        assertEquals("error-declaration", b.get("retraction").get("reason").asText());
    }

    @Test void aBadShareAboveTheRuleIsInsufficient() {
        Driver d = Driver.station1();
        d.input(OCC, arrival("occ-1", T, U1, S1));
        Inputs.InputRecord bad = null;
        for (long t = T; t < T + 60_000; t += 1000) {
            boolean b = t >= T + 5_000 && t < T + 12_000;
            List<Inputs.InputRecord> s = b ? d.scan(t, over(NR1, "Torque", new DoubleNode(20.5), BAD)) : d.scan(t);
            if (b && bad == null) bad = s.get(1);
        }
        d.input(OCC, departure("occ-2", T + 60_000, U1, S1, "occ-1"));
        d.cut();
        JsonNode w = d.workRecord("occ-1");
        ObjectNode m = memberEntry(w, "nutrunner1-torque");
        assertEquals(60, m.get("observedPeriods").longValue());
        assertEquals(0, m.get("missingPeriods").longValue());
        assertEquals(7, m.get("badSlots").longValue());
        assertEquals(53, m.get("count").longValue());
        assertEquals("insufficient", status(w));
        assertEquals(List.of("bad-share-exceeded"), reasons(w));
        assertEquals("incomplete", evidence(w, NR1).get("coverage").get("status").asText());
        assertEquals(7, w.get("coverage").get("badSlots").longValue());
        JsonNode row = d.sampleRow(bad, "Torque");
        assertEquals("unattributed", row.get("state").asText());
        assertEquals("bad-quality", row.get("reason").asText());
        assertTrue(row.get("attributedTo").isNull());
    }

    @Test void rejectedUncertainOnlyHasBadSlotsZero() {
        Driver d = Driver.station1();
        d.input(OCC, arrival("occ-1", T, U1, S1));
        for (long t = T; t < T + 60_000; t += 1000) {
            if (t >= T + 5_000 && t < T + 12_000) d.scan(t, over(NR1, "Torque", new DoubleNode(20.5), UNCERTAIN));
            else d.scan(t);
        }
        d.input(OCC, departure("occ-2", T + 60_000, U1, S1, "occ-1"));
        d.cut();
        JsonNode w = d.workRecord("occ-1");
        ObjectNode m = memberEntry(w, "nutrunner1-torque");
        assertEquals(60, m.get("observedPeriods").longValue());
        assertEquals(0, m.get("badSlots").longValue());
        assertEquals(53, m.get("count").longValue());
        assertFalse(reasons(w).contains("bad-share-exceeded"));
        assertEquals("provisional", status(w));
    }

    @Test void aSeqGapOverlappingTheVisitIsInsufficientSessionGap() {
        Driver d = baseline();
        d.input(SES, session(NR1, "seq-gap", 25, Json.putSafe(Json.putSafe(Json.object(), "from", T + 20_000), "to", T + 25_000).put("missing", 5)));
        d.cut();
        JsonNode w = d.workRecord("occ-1");
        assertEquals("insufficient", status(w));
        assertEquals(List.of("session-gap"), reasons(w));
        assertEquals(1, evidence(w, NR1).get("coverage").get("sessionGaps").longValue());
        assertEquals("incomplete", evidence(w, NR1).get("coverage").get("status").asText());
        assertEquals(1, w.get("coverage").get("sessionGaps").longValue());
    }

    static ObjectNode death(long detected) {
        ObjectNode p = Json.putSafe(Json.object(), "detectedEventTime", detected);
        Json.putSafe(p, "rawPublishTs", detected - 1000).put("bdSeq", 0).put("payloadDigest", "ab".repeat(32));
        return p;
    }

    @Test void aStaleDeathChangesNothing() {
        Driver control = baseline();
        control.cut();
        Driver d = baseline();
        Inputs.InputRecord s = d.input(SES, session(NR1, "stale-death", 30, death(T + 30_000)));
        d.cut();
        assertEquals(Disposition.Reason.ACCEPTED, d.disposition(s, "event").body().reason());
        assertEquals(Canonical.digest(control.workRecord("occ-1")), Canonical.digest(d.workRecord("occ-1")));
        assertEquals(control.workRecordRows("occ-1").size(), d.workRecordRows("occ-1").size());
    }

    @Test void aSessionAmbiguousIntervalMakesCoverageUnknown() {
        Driver d = Driver.station1();
        d.input(OCC, arrival("occ-1", T, U1, S1));
        List<Inputs.InputRecord> scans = new ArrayList<>();
        for (long t = T; t < T + 100_000; t += 1000) scans.addAll(d.scan(t));
        d.input(OCC, departure("occ-2", T + 60_000, U1, S1, "occ-1"));
        d.input(SES, session(NR1, "session-ambiguous", 30, death(T + 30_000)));
        d.cut();
        JsonNode w = d.workRecord("occ-1");
        assertEquals("insufficient", status(w));
        assertTrue(reasons(w).contains("session-ambiguous"));
        assertEquals("unknown", evidence(w, NR1).get("coverage").get("status").asText());
        JsonNode row = d.sampleRow(scans.get(3 * 40 + 1), "Torque");   // nutrunner1 at T + 40 s
        assertEquals("unattributed", row.get("state").asText());
        assertEquals("session-ambiguous", row.get("reason").asText());
    }

    @Test void aSampleInTwoNonRetractedWindowsIsAmbiguous() {
        Driver d = baseline();
        d.input(OCC, arrival("occ-11", T + 90_000, U2, S1));
        Inputs.InputRecord m = d.input(MSG, d.message(NR1, T + 70_000, member("Torque", new DoubleNode(1.0), 0, T + 70_000)));
        d.cut();
        JsonNode row = d.sampleRow(m, "Torque");
        assertEquals("unattributed", row.get("state").asText());
        assertEquals("ambiguous", row.get("reason").asText());
        assertFalse(d.workRecord("occ-1").has("retraction"));
        assertFalse(d.workRecord("occ-11").has("retraction"));
    }

    @Test void aSampleInOnlyARetractedWindowAttributesToIt() {
        Driver d = baseline();
        d.input(OCC, declared(arrival("occ-1", T, U1, S1), DID_NOT_OCCUR, T + 100_000));
        Inputs.InputRecord inside = d.input(MSG, d.message(NR1, T + 30_500, member("Torque", new DoubleNode(1.0), 0, T + 30_500)));
        Inputs.InputRecord outside = d.input(MSG, d.message(NR1, T + 70_000, member("Torque", new DoubleNode(1.0), 0, T + 70_000)));
        d.cut();
        assertEquals("retracted", status(d.workRecord("occ-1")));
        JsonNode in = d.sampleRow(inside, "Torque");
        assertEquals("attributed", in.get("state").asText());
        assertEquals(visitRef("occ-1"), in.get("attributedTo"));
        JsonNode out = d.sampleRow(outside, "Torque");
        assertEquals("unattributed", out.get("state").asText());
        assertEquals("ambiguous", out.get("reason").asText());
    }

    @Test void aShiftedClockMovesSamplesAcrossVisitsAsTheBandsSay() {
        Driver d = Driver.station1();
        d.input(OCC, arrival("occ-1", T, U1, S1));
        d.input(OCC, departure("occ-2", T + 60_000, U1, S1, "occ-1"));
        d.input(OCC, arrival("occ-11", T + 120_000, U2, S1));
        d.input(OCC, departure("occ-12", T + 180_000, U2, S1, "occ-11"));
        Map<Long, Inputs.InputRecord> shifted = new TreeMap<>();
        for (long t = T; t < T + 60_000; t += 1000)
            shifted.put(t + 90_000, d.input(MSG, d.message(NR1, t + 90_000, member("Torque", new DoubleNode(5.0), 0, t + 90_000))));
        d.cut();
        for (var e : shifted.entrySet()) {
            JsonNode row = d.sampleRow(e.getValue(), "Torque");
            if (e.getKey() < T + 105_000) {   // inside both windows: [T - 45 s, T + 105 s) and [T + 75 s, T + 225 s)
                assertEquals("unattributed", row.get("state").asText(), "at " + e.getKey());
                assertEquals("ambiguous", row.get("reason").asText());
            } else {
                assertEquals("attributed", row.get("state").asText(), "at " + e.getKey());
                assertEquals(visitRef("occ-11"), row.get("attributedTo"));
            }
        }
    }

    @Test void lateAfterSettlementIsAcceptedWithItsReason() {
        Driver d = baseline();
        d.cut();
        d.cut(T + 60_000 + L);
        assertEquals("complete", status(d.workRecord("occ-1")));
        Inputs.InputRecord late = d.input(OCC, declared(departure("occ-2", T + 60_000, U1, S1, "occ-1"), DID_NOT_OCCUR, T + 800_000));
        d.cut(T + 60_000 + L);
        Disposition disp = d.disposition(late, "errorDeclaration");
        assertEquals(Disposition.Kind.ACCEPTED, disp.body().disposition());
        assertEquals(Disposition.Reason.LATE_AFTER_SETTLEMENT, disp.body().reason());
        assertTrue(d.workRecord("occ-1").get("interval").get("leave").isNull());
    }

    /** 10f in one cut: the declaration on the departure is applied first (contract order), yet the corrective departure is admitted
     *  against the visit as it stood at the last committed progress, which was settled (S3, S4). */
    @Test void aCorrectionInOneCutIsJudgedAgainstTheCommittedVisit() {
        Driver d = baseline();
        d.cut();
        d.cut(T + 60_000 + L);
        assertEquals("complete", status(d.workRecord("occ-1")));
        Inputs.InputRecord decl = d.input(OCC, declared(departure("occ-2", T + 60_000, U1, S1, "occ-1"), INCORRECT_DATA, T + 800_000));
        Inputs.InputRecord corrective = d.input(OCC, departure("occ-12", T + 61_000, U1, S1, "occ-1"));
        d.cut(T + 60_000 + L);
        for (var x : List.of(new Object[]{decl, "errorDeclaration"}, new Object[]{corrective, "event"})) {
            Disposition disp = d.disposition((Inputs.InputRecord) x[0], (String) x[1]);
            assertEquals(Disposition.Kind.ACCEPTED, disp.body().disposition());
            assertEquals(Disposition.Reason.LATE_AFTER_SETTLEMENT, disp.body().reason());
        }
        assertEquals(T + 61_000, d.workRecord("occ-1").get("interval").get("leave").longValue());
    }

    static Driver uncertainScan(RuleSet rules, List<Inputs.InputRecord> marked) {
        Driver d = new Driver(rules, S1);
        d.input(OCC, arrival("occ-1", T, U1, S1));
        for (long t = T; t < T + 60_000; t += 1000) {
            if (t == T + 20_000) marked.addAll(d.scan(t, over(NR1, "Torque", new DoubleNode(30.0), UNCERTAIN), over(NR1, "Running", BooleanNode.TRUE, UNCERTAIN)));
            else d.scan(t);
        }
        d.input(OCC, departure("occ-2", T + 60_000, U1, S1, "occ-1"));
        d.cut();
        return d;
    }

    @Test void uncertainPropagateAggregatesAndSupportsActivity() {
        List<Inputs.InputRecord> mp = new ArrayList<>(), mr = new ArrayList<>();
        Driver p = uncertainScan(Fixtures.uncertain("propagate"), mp), r = uncertainScan(Fixtures.uncertain("reject"), mr);
        JsonNode wp = p.workRecord("occ-1"), wr = r.workRecord("occ-1");
        assertEquals(60, memberEntry(wp, "nutrunner1-torque").get("count").longValue());
        assertEquals(30.0, memberEntry(wp, "nutrunner1-torque").get("max").doubleValue());
        assertEquals(59, memberEntry(wr, "nutrunner1-torque").get("count").longValue());
        assertEquals(20.5, memberEntry(wr, "nutrunner1-torque").get("max").doubleValue());
        same(Json.read(("[[" + T + "," + (T + 60_000) + "]]").getBytes()), evidence(wp, NR1).get("activityIntervals"));
        assertEquals(0, evidence(wp, NR1).get("coverage").get("unknownActivityPeriods").longValue());
        same(Json.read(("[[" + T + "," + (T + 20_000) + "],[" + (T + 21_000) + "," + (T + 60_000) + "]]").getBytes()), evidence(wr, NR1).get("activityIntervals"));
        assertEquals(1, evidence(wr, NR1).get("coverage").get("unknownActivityPeriods").longValue());
        JsonNode runP = p.sampleRow(mp.get(1), "Running"), runR = r.sampleRow(mr.get(1), "Running");
        assertEquals("attributed", runP.get("state").asText());
        assertEquals(BooleanNode.TRUE, runP.get("active"));
        assertEquals("unattributed", runR.get("state").asText());
        assertEquals("bad-quality", runR.get("reason").asText());
        assertEquals(text("unknown"), runR.get("active"));
        assertEquals(text("unknown"), r.sampleRow(mr.get(1), "Torque").get("active"));
    }

    @Test void aRedeliveredMessageGetsOneDuplicatePerMember() {
        Driver d = Driver.station1();
        ObjectNode m = d.message(NR1, T, member("Torque", new DoubleNode(20.5), 0, T), member("Running", BooleanNode.TRUE, 0, T));
        Inputs.InputRecord first = d.input(MSG, m), second = d.input(MSG, m);
        d.cut();
        List<String> parts = new ArrayList<>();
        for (Disposition x : d.dispositions(second)) {
            parts.add(x.key().part());
            assertEquals(Disposition.Reason.IDENTICAL_REDELIVERY, x.body().reason());
            assertEquals("message", x.body().inputKind());
        }
        assertEquals(List.of("member:Torque", "member:Running"), parts);
        for (Disposition x : d.dispositions(first)) assertEquals(Disposition.Reason.ACCEPTED, x.body().reason());
    }

    @Test void aDisallowedCorrectionReasonIsSchemaInvalid() {
        Driver d = new Driver(Fixtures.onlyDidNotOccur(), S1);
        d.input(OCC, declared(arrival("occ-1", T, U1, S1), INCORRECT_DATA, T + 100_000));
        assertEquals(1, d.rejections.size());
        Routing.Rejected r = d.rejections.get(0);
        assertEquals("errorDeclaration", r.part());
        assertEquals(Disposition.Reason.SCHEMA_INVALID, r.body().reason());
        assertEquals("correction.reasons", r.body().refs().get("check").asText());
        assertEquals(Json.object().put("source", SOURCE).put("eventId", "occ-1"), r.body().identity());
        assertTrue(d.pending.isEmpty());
        d.input(OCC, declared(arrival("occ-1", T, U1, S1), DID_NOT_OCCUR, T + 100_000));
        assertEquals(1, d.rejections.size());
    }

    @Test void admissionBoundaries() {
        Driver d = Driver.station1();
        long a = T + H;
        d.cut(a);
        assertEquals(Long.valueOf(a), d.state.admissionW());
        Inputs.InputRecord at = d.input(MSG, d.message(NR1, a - H, member("Torque", new DoubleNode(1.0), 0, a - H)));
        Inputs.InputRecord above = d.input(MSG, d.message(NR1, a - H + 1, member("Torque", new DoubleNode(1.0), 0, a - H + 1)));
        Inputs.InputRecord below = d.input(MSG, d.message(NR1, a - H - 1, member("Torque", new DoubleNode(1.0), 0, a - H - 1)));
        d.cut(a);
        assertEquals(Disposition.Reason.ACCEPTED, d.disposition(at, "member:Torque").body().reason());
        assertEquals(Disposition.Reason.ACCEPTED, d.disposition(above, "member:Torque").body().reason());
        assertEquals(Disposition.Reason.BEYOND_HORIZON, d.disposition(below, "member:Torque").body().reason());
        JsonNode row = d.sampleRow(below, "Torque");
        assertEquals("replay-required", row.get("state").asText());
        assertEquals("beyond-horizon", row.get("reason").asText());
        assertEquals("no-occupancy", d.sampleRow(at, "Torque").get("reason").asText());
    }

    @Test void expiryDropsOnlineStateAndKeepsRows() {
        Driver d = baseline();
        d.cut();
        assertEquals(1, d.state.onlineVisits());
        int before = d.workRecordRows("occ-1").size();
        long expiry = T + 60_000 + H + L;
        d.cut(expiry);
        assertEquals(0, d.state.onlineVisits());
        assertEquals(0, d.state.onlineSamples());
        assertTrue(d.outputs.stream().noneMatch(o -> o instanceof Delete));
        assertEquals(before + 1, d.workRecordRows("occ-1").size(), "only the settlement row; expiry emits nothing");
        assertEquals("complete", status(d.workRecord("occ-1")));
        Inputs.InputRecord late = d.input(OCC, declared(departure("occ-2", T + 60_000, U1, S1, "occ-1"), DID_NOT_OCCUR, expiry));
        d.cut(expiry);
        assertEquals(Disposition.Reason.CORRECTION_BEYOND_HORIZON, d.disposition(late, "errorDeclaration").body().reason());
        assertEquals(before + 1, d.workRecordRows("occ-1").size());
    }

    @Test void smokeSamplesDigest() {
        Driver d = Driver.station1();
        d.runRecording(FIXTURES.resolve("recordings/smoke"));
        JsonNode w = d.workRecord("occ-1");
        assertEquals("complete", status(w));
        assertEquals(List.of(), reasons(w));
        Set<String> keys = new HashSet<>();
        for (Object o : d.outputs) if (o instanceof Row r && r.topic().equals(Stage1.SAMPLES)) keys.add(Canonical.utf8(Canonical.bytes(r.logicalKey())));
        assertEquals(812 * 5, keys.size());
        assertEquals(SMOKE_SAMPLES_DIGEST, w.get("samplesDigest").asText());
    }

    @Test void aStation2VisitBodyHasNoMeasurementMembers() {
        Driver d = new Driver(Fixtures.line2(), S2);
        d.input(OCC, arrival("occ-21", T, U1, S2));
        d.scans(T, T + 60_000);
        d.input(OCC, departure("occ-22", T + 60_000, U1, S2, "occ-21"));
        d.cut();
        d.cut(T + 60_000 + L);
        JsonNode w = d.workRecord("occ-21");
        assertEquals("complete", status(w));
        assertEquals(List.of(), reasons(w));
        assertEquals(0, w.get("members").size());
        assertEquals(1, w.get("equipmentEvidence").size());
        JsonNode e = evidence(w, S2);
        same(Json.read("[\"station2-running\"]".getBytes()), e.get("memberRefs"));
        same(Json.read(("[[" + T + "," + (T + 60_000) + "]]").getBytes()), e.get("activityIntervals"));
        assertEquals(0, e.get("inactiveIntervals").size());
        ObjectNode ec = Json.object().put("observedPeriods", 60).put("missingPeriods", 0).put("badSlots", 0).put("unknownActivityPeriods", 0)
            .put("sessionGaps", 0).put("status", "complete");
        same(ec, e.get("coverage"));
        ObjectNode cov = Json.object().put("expectedPeriodMs", 1000).put("observedPeriods", 60).put("missingPeriods", 0).put("badSlots", 0).put("sessionGaps", 0);
        same(cov, w.get("coverage"));
        assertEquals(STATION2_BODY_DIGEST, Canonical.digest(w));
    }

    @Test void visitSeqAboveInt32IsPreservedInTheBody() {
        Driver d = Driver.station1();
        ObjectNode a = arrival("occ-1", T, U1, S1);
        Json.putSafe(a, "visitSeq", 2147483648L);
        d.input(OCC, a);
        d.cut();
        JsonNode v = d.workRecord("occ-1").get("visitSeq");
        assertTrue(v.isIntegralNumber());
        assertEquals(2147483648L, v.longValue());
        assertTrue(Canonical.utf8(Canonical.bytes(d.workRecord("occ-1"))).contains("\"visitSeq\":2147483648"));
    }

    static Driver sequence(boolean roundTrip) {
        Driver d = Driver.station1();
        d.roundTrip = roundTrip;
        d.input(OCC, arrival("occ-1", T, U1, S1));
        d.input(OCC, arrival("occ-11", T + 30_000, U2, S1));
        for (long t = T; t < T + 90_000; t += 1000)
            if (t == T + 15_000) d.scan(t, over(NR1, "Torque", new DoubleNode(20.5), BAD)); else d.scan(t);
        d.input(SES, session(NR2, "seq-gap", 25, Json.putSafe(Json.putSafe(Json.object(), "from", T + 20_000), "to", T + 22_000).put("missing", 2)));
        d.cut();
        d.input(OCC, departure("occ-2", T + 60_000, U1, S1, "occ-1"));
        d.input(OCC, departure("occ-12", T + 90_000, U2, S1, "occ-11"));
        d.cut();
        d.input(OCC, declared(arrival("occ-11", T + 30_000, U2, S1), DID_NOT_OCCUR, T + 100_000));
        d.cut(T + 60_000 + L);
        d.cut(T + 60_000 + H + L);
        return d;
    }

    @Test void decideIsDeterministicAndReadsOnlyItsArguments() {
        Driver a = sequence(false), b = sequence(false), c = sequence(true);
        assertEquals(a.outputText(), b.outputText());
        assertEquals(a.outputText(), c.outputText());
        assertArrayEquals(Canonical.bytes(a.state.toJson()), Canonical.bytes(b.state.toJson()));
        assertArrayEquals(Canonical.bytes(a.state.toJson()), Canonical.bytes(c.state.toJson()));
        assertEquals(1, a.state.toJson().get("stateVersion").intValue());
        assertTrue(a.outputs.stream().anyMatch(o -> o instanceof Row));
        assertEquals(8, a.progress().size(), "a business and a ledger progress per cut");
    }

    /** The birthless session-ambiguous death of plan D4: no birth, no atSeq, no detectedEventTime (no telemetry or birth for the edge). */
    static ObjectNode birthlessDeath(String device, long rawPublishTs, String payloadDigest) {
        ObjectNode n = Json.object().put("edge", EDGE).put("device", device).putNull("birthPublishTs").put("bdSeq", 0).put("kind", "session-ambiguous");
        n.putNull("atSeq");
        ObjectNode p = n.putObject("payload").putNull("detectedEventTime");
        Json.putSafe(p, "rawPublishTs", rawPublishTs).put("bdSeq", 0).put("payloadDigest", payloadDigest);
        return n;
    }

    @Test void aBirthlessDeathWithoutEventTimeOpensNoUnknownIntervalAndChangesNothing() {
        Driver control = baseline();
        control.cut();
        Driver d = baseline();
        Inputs.InputRecord a = d.input(SES, birthlessDeath(NR1, T + 30_000, "aa".repeat(32)));
        Inputs.InputRecord b = d.input(SES, birthlessDeath(NR1, T + 30_000, "bb".repeat(32)));   // equal counters, other raw bytes
        d.cut();
        assertEquals(Disposition.Reason.ACCEPTED, d.disposition(a, "event").body().reason());
        assertEquals(Disposition.Reason.ACCEPTED, d.disposition(b, "event").body().reason(), "two deaths, not an identity conflict");
        assertEquals(Canonical.digest(control.workRecord("occ-1")), Canonical.digest(d.workRecord("occ-1")));
        assertEquals(control.workRecordRows("occ-1").size(), d.workRecordRows("occ-1").size());
        assertNotEquals("unknown", evidence(d.workRecord("occ-1"), NR1).get("coverage").get("status").asText());
        assertFalse(reasons(d.workRecord("occ-1")).contains("session-ambiguous"));
    }

    @Test void aNullTimeEventSortsFirstInItsCutAndNeverAdvancesEventTime() {
        Driver d = Driver.station1();
        Inputs.InputRecord occ = d.input(OCC, arrival("occ-1", T, U1, S1));
        Inputs.InputRecord death = d.input(SES, birthlessDeath(NR1, T + 30_000, "aa".repeat(32)));
        assertNull(Events.eventTime(death));
        assertTrue(Events.CONTRACT_ORDER.compare(death, occ) < 0);
        assertTrue(Events.CONTRACT_ORDER.compare(occ, death) > 0);
        List<Inputs.InputRecord> in = new ArrayList<>(List.of(occ, death));
        in.sort(Events.CONTRACT_ORDER);
        assertSame(death, in.get(0));
    }
}
