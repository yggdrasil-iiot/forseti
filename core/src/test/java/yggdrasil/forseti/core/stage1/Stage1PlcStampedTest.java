package yggdrasil.forseti.core.stage1;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.core.stage1.Driver.*;
import static yggdrasil.forseti.core.stage1.Fixtures.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.rules.RuleSet;

/** Rule sheet P1-P3 on the line2-plc rules: station1 and nutrunner1 stamp {@code UnitId}, {@code VisitId} and {@code VisitPresent} in
 *  every scan. Expected bodies are typed from the sheet; events are inline literals fed through the in-test {@link Driver}. */
class Stage1PlcStampedTest {
    static final String OCC = InputParser.OCCUPANCY, MSG = InputParser.MESSAGES;
    static final String NS = "urn:line2:visits", U1 = "UNIT0000000000001", U2 = "UNIT0000000000002";
    static final long L = 720_000, H = 86_400_000;

    static Driver plc() { return new Driver(Fixtures.plc(), S1); }
    static Driver plcNoCheck() { return new Driver(Fixtures.plcNoCheck(), S1); }

    static ObjectNode ref(String visitId) {
        return Json.object().put("mode", "plc-stamped").put("namespace", NS).put("station", S1).put("visitId", visitId);
    }
    static JsonNode workRecord(Driver d, String visitId) {
        List<Row> l = d.rows(Stage1.WORK_RECORDS, ref(visitId));
        return l.isEmpty() ? null : l.get(l.size() - 1).body();
    }
    static void same(JsonNode expected, JsonNode actual) { assertEquals(Canonical.utf8(Canonical.bytes(expected)), Canonical.utf8(Canonical.bytes(actual))); }
    static String status(JsonNode body) { return body.get("status").asText(); }
    static JsonNode intervals(long... bounds) {
        var a = Json.JSON.createArrayNode();
        for (int i = 0; i < bounds.length; i += 2) { var p = a.addArray(); p.add(bounds[i]); p.add(bounds[i + 1]); }
        return a;
    }

    /** One edit of a scan member: the device, the metric and the change to the member node. */
    /** Every message this test built, for {@link #messageAt}. */
    final List<Inputs.InputRecord> sent = new ArrayList<>();

    record Tweak(String device, String metric, Consumer<ObjectNode> edit) {}
    static Tweak tweak(String device, String metric, Consumer<ObjectNode> edit) { return new Tweak(device, metric, edit); }

    /** One stamped message of station1 ({@code Running, UnitId, VisitId, VisitPresent}) or nutrunner1 ({@code Torque 20.5} and the same). */
    Inputs.InputRecord msg(Driver d, String device, long t, String visitId, String unit, boolean present, Tweak... tweaks) {
        List<ObjectNode> ms = new ArrayList<>();
        if (device.equals(NR1)) ms.add(member("Torque", new DoubleNode(20.5), 0, t));
        ms.add(member("Running", BooleanNode.valueOf(present), 0, t));
        ms.add(member("UnitId", TextNode.valueOf(unit), 0, t));
        ms.add(member("VisitId", TextNode.valueOf(visitId), 0, t));
        ms.add(member("VisitPresent", BooleanNode.valueOf(present), 0, t));
        for (ObjectNode m : ms) for (Tweak w : tweaks) if (w.device().equals(device) && w.metric().equals(m.get("metric").asText())) w.edit().accept(m);
        Inputs.InputRecord r = d.input(MSG, d.message(device, t, ms.toArray(new ObjectNode[0])));
        sent.add(r);
        return r;
    }
    /** station1 and nutrunner1 every second over {@code [from, to)} with one stamp. */
    void scans(Driver d, long from, long to, String visitId, String unit, boolean present, Tweak... tweaks) {
        for (long t = from; t < to; t += 1000) for (String q : List.of(S1, NR1)) msg(d, q, t, visitId, unit, present, tweaks);
    }
    /** The baseline plc visit: V1 present over [T, T + 60 s), then V1 absent for 5 s. */
    Driver baseline(Driver d) {
        scans(d, T, T + 60_000, "V1", U1, true);
        scans(d, T + 60_000, T + 65_000, "V1", U1, false);
        return d;
    }

    @Test void aVisitIsFramedByTheEarliestTrueAndTheEarliestLaterFalse() {
        Driver d = baseline(plc());
        d.cut(T + 34_999);
        JsonNode b = workRecord(d, "V1");
        assertEquals("provisional", status(b));
        assertEquals(T, b.get("interval").get("enter").longValue());
        assertEquals(T + 60_000, b.get("interval").get("leave").longValue(), "the earliest later false, not the last");
        assertTrue(b.get("interval").get("attributionEnd").isNull());
        d.cut(T + 60_000 + L);
        b = workRecord(d, "V1");
        assertEquals("complete", status(b));
        assertEquals(List.of(), reasons(b));
        assertEquals(U1, b.get("unitId").asText());
        assertEquals(S1, b.get("readPoint").asText());
        assertTrue(b.get("visitSeq").isNull());
        assertEquals("plc-stamped", b.get("attributionMode").asText());
        assertFalse(b.has("retraction"));
        ObjectNode torque = memberEntry(b, "nutrunner1-torque");
        assertEquals(60, torque.get("observedPeriods").longValue());
        assertEquals(0, torque.get("missingPeriods").longValue());
        assertEquals(60, torque.get("count").longValue());
        assertEquals(1, b.get("members").size(), "the identity and activity members are not measurement members");
        same(Json.putSafe(Json.putSafe(Json.putSafe(Json.putSafe(Json.putSafe(Json.object(), "expectedPeriodMs", 1000), "observedPeriods", 60), "missingPeriods", 0), "badSlots", 0), "sessionGaps", 0),
            b.get("coverage"));
        JsonNode row = rowAt(d, NR1, T + 30_000, "Torque");
        assertEquals("attributed", row.get("state").asText());
        assertTrue(row.get("reason").isNull());
        same(ref("V1"), row.get("attributedTo"));
        assertEquals(1, keys(d, Stage1.WORK_RECORDS).size(), "one visit");
    }

    @Test void theEndSnapshotIsNotAttributed() {
        Driver d = plc();
        scans(d, T, T + 60_000, "V1", U1, true);
        scans(d, T + 60_000, T + 65_000, "V1", U1, false, tweak(NR1, "Torque", m -> m.put("value", 99.0)));
        d.cut(T + 60_000 + L);
        for (String metric : List.of("Torque", "Running", "UnitId", "VisitId", "VisitPresent")) {
            JsonNode end = rowAt(d, NR1, T + 60_000, metric);
            assertEquals("unattributed", end.get("state").asText(), metric);
            assertEquals("no-occupancy", end.get("reason").asText(), metric);
            assertTrue(end.get("attributedTo").isNull(), metric);
            JsonNode before = rowAt(d, NR1, T + 59_000, metric);
            assertEquals("attributed", before.get("state").asText(), metric);
        }
        JsonNode b = workRecord(d, "V1");
        assertEquals("complete", status(b));
        ObjectNode torque = memberEntry(b, "nutrunner1-torque");
        assertEquals(20.5, torque.get("max").doubleValue(), "the end snapshot's 99.0 is not attributed");
        assertEquals(60, torque.get("count").longValue());
        same(Json.JSON.createArrayNode().add("nutrunner1-present").add("nutrunner1-running").add("nutrunner1-torque").add("nutrunner1-unit").add("nutrunner1-visit"),
            evidence(b, NR1).get("memberRefs"));
        same(Json.JSON.createArrayNode().add("station1-present").add("station1-running").add("station1-unit").add("station1-visit"), evidence(b, S1).get("memberRefs"));
        same(intervals(T, T + 60_000), evidence(b, S1).get("activityIntervals"));
    }

    @Test void aLateEarlierStartRebuildsTheLifecycle() {
        Driver d = plc();
        scans(d, T + 10_000, T + 60_000, "V1", U1, true);
        scans(d, T + 60_000, T + 62_000, "V1", U1, false);
        d.cut(T + 20_000);
        JsonNode first = workRecord(d, "V1");
        assertEquals(T + 10_000, first.get("interval").get("enter").longValue());
        assertEquals(50, memberEntry(first, "nutrunner1-torque").get("observedPeriods").longValue());
        int revisions = d.rows(Stage1.WORK_RECORDS, ref("V1")).size();
        scans(d, T, T + 10_000, "V1", U1, true);   // the earliest true arrives late
        d.cut(T + 25_000);
        JsonNode b = workRecord(d, "V1");
        assertEquals(T, b.get("interval").get("enter").longValue(), "the earliest sourceTs, not the first to arrive");
        assertEquals(T + 60_000, b.get("interval").get("leave").longValue());
        assertEquals(60, memberEntry(b, "nutrunner1-torque").get("observedPeriods").longValue());
        assertTrue(d.rows(Stage1.WORK_RECORDS, ref("V1")).size() > revisions, "a new revision");
        same(ref("V1"), rowAt(d, NR1, T + 5_000, "Torque").get("attributedTo"));
        assertEquals(1, keys(d, Stage1.WORK_RECORDS).size());
    }

    @Test void aTrueAfterTheEndIsInconsistent() {
        Driver d = baseline(plc());
        scans(d, T + 65_000, T + 70_000, "V1", U1, true);
        d.cut(T + 70_000 + L);
        JsonNode b = workRecord(d, "V1");
        assertEquals("insufficient", status(b));
        assertEquals(List.of("inconsistent-visit-boundary"), reasons(b));
        assertEquals(T + 60_000, b.get("interval").get("leave").longValue(), "the end stays the earliest later false");
        JsonNode late = rowAt(d, NR1, T + 66_000, "Torque");
        assertEquals("unattributed", late.get("state").asText());
        assertEquals("no-occupancy", late.get("reason").asText());
        assertEquals(1, keys(d, Stage1.WORK_RECORDS).size(), "a true after the end opens no second visit");
    }

    @Test void anIdChangeWithoutFalseLeavesMissingVisitClose() {
        Driver d = plc();
        scans(d, T, T + 30_000, "V1", U1, true);
        scans(d, T + 30_000, T + 60_000, "V2", U2, true);
        scans(d, T + 60_000, T + 62_000, "V2", U2, false);
        d.cut(T + H);   // W1 >= enter + horizonMs: still no missing-departure, the change closed the visit
        JsonNode v1 = workRecord(d, "V1");
        assertEquals("insufficient", status(v1));
        assertEquals(List.of("missing-visit-close"), reasons(v1));
        assertTrue(v1.get("interval").get("leave").isNull());
        assertEquals(T + 30_000, v1.get("interval").get("attributionEnd").longValue());
        assertEquals(30, memberEntry(v1, "nutrunner1-torque").get("observedPeriods").longValue());
        assertEquals(0, memberEntry(v1, "nutrunner1-torque").get("missingPeriods").longValue());
        JsonNode v2 = workRecord(d, "V2");
        assertEquals("complete", status(v2));
        assertEquals(U2, v2.get("unitId").asText());
        assertEquals(T + 30_000, v2.get("interval").get("enter").longValue());
        assertEquals(T + 60_000, v2.get("interval").get("leave").longValue());
        same(ref("V2"), rowAt(d, NR1, T + 30_000, "Torque").get("attributedTo"));
        same(ref("V1"), rowAt(d, NR1, T + 29_000, "Torque").get("attributedTo"));
    }

    @Test void equipmentMustAgreeOnBoundaries() {
        Driver d = plc();
        for (long t = T; t < T + 62_000; t += 1000) {
            msg(d, S1, t, "V1", U1, t < T + 60_000);
            msg(d, NR1, t, "V1", U1, t < T + 59_000);   // nutrunner1 reports the end one scan early
        }
        d.cut(T + 60_000 + L);
        JsonNode b = workRecord(d, "V1");
        assertEquals("insufficient", status(b));
        assertEquals(List.of("inconsistent-visit-boundary"), reasons(b));
        assertEquals(T, b.get("interval").get("enter").longValue());
        assertEquals(T + 60_000, b.get("interval").get("leave").longValue());
        assertEquals("no-occupancy", rowAt(d, NR1, T + 59_000, "Torque").get("reason").asText(), "nutrunner1's own end snapshot");
        assertEquals("attributed", rowAt(d, S1, T + 59_000, "Running").get("state").asText(), "station1 is still in the visit");
    }

    @Test void aMixedSnapshotIsIncoherent() {
        Driver d = plc();
        long at = T + 30_000;
        for (long t = T; t < T + 60_000; t += 1000) {
            msg(d, S1, t, "V1", U1, true);
            if (t == at) msg(d, NR1, t, "V1", U1, true, tweak(NR1, "VisitId", m -> { Json.putSafe(m, "sourceTs", at - 500); Json.putSafe(m, "serverTs", at - 490); }));
            else msg(d, NR1, t, "V1", U1, true);
        }
        scans(d, T + 60_000, T + 62_000, "V1", U1, false);
        d.cut(T + 60_000 + L);
        for (String metric : List.of("Torque", "Running", "UnitId", "VisitPresent")) {
            JsonNode r = rowAt(d, NR1, at, metric);
            assertEquals("unattributed", r.get("state").asText(), metric);
            assertEquals("incoherent-snapshot", r.get("reason").asText(), metric);
            assertTrue(r.get("attributedTo").isNull(), metric);
        }
        JsonNode visitId = rowAt(d, NR1, at - 500, "VisitId");
        assertEquals("incoherent-snapshot", visitId.get("reason").asText());
        assertEquals("attributed", rowAt(d, S1, at, "Running").get("state").asText(), "station1's snapshot at the same scan is coherent");
        JsonNode b = workRecord(d, "V1");
        ObjectNode torque = memberEntry(b, "nutrunner1-torque");
        assertEquals(59, torque.get("observedPeriods").longValue());
        assertEquals(1, torque.get("missingPeriods").longValue());
        assertEquals(T, b.get("interval").get("enter").longValue());
        assertEquals(T + 60_000, b.get("interval").get("leave").longValue());
    }

    /** 11c as the scenario builds it: the mixed message's identity members repeat the previous coherent scan (same sourceTs, same
     *  values), so each is one observation delivered twice, attributed through the coherent message and incoherent in the mixed one. An
     *  observation that serves two judgements appears once in the digest: the later incoherent delivery does not remove it. */
    @Test void anIncoherentRedeliveryKeepsTheObservationsContribution() {
        long at = T + 30_000;
        Driver a = plc(), b = plc();
        Stage1PlcStampedTest ta = new Stage1PlcStampedTest(), tb = new Stage1PlcStampedTest();
        for (long t = T; t < T + 60_000; t += 1000) {
            ta.msg(a, S1, t, "V1", U1, true);
            tb.msg(b, S1, t, "V1", U1, true);
            if (t == at) {
                ta.msg(a, NR1, t, "V1", U1, true, tweak(NR1, "VisitId", m -> { Json.putSafe(m, "sourceTs", at - 500); Json.putSafe(m, "serverTs", at - 490); }));
                Consumer<ObjectNode> previous = m -> { Json.putSafe(m, "sourceTs", at - 1000); Json.putSafe(m, "serverTs", at - 990); };
                tb.msg(b, NR1, t, "V1", U1, true, tweak(NR1, "UnitId", previous), tweak(NR1, "VisitId", previous), tweak(NR1, "VisitPresent", previous));
            } else {
                ta.msg(a, NR1, t, "V1", U1, true);
                tb.msg(b, NR1, t, "V1", U1, true);
            }
        }
        ta.scans(a, T + 60_000, T + 62_000, "V1", U1, false);
        tb.scans(b, T + 60_000, T + 62_000, "V1", U1, false);
        a.cut(T + 60_000 + L);
        b.cut(T + 60_000 + L);
        JsonNode wa = workRecord(a, "V1"), wb = workRecord(b, "V1");
        assertEquals(evidence(wa, NR1).get("samplesDigest").asText(), evidence(wb, NR1).get("samplesDigest").asText(), "nutrunner1's samplesDigest");
        assertEquals(wa.get("samplesDigest").asText(), wb.get("samplesDigest").asText(), "the visit's samplesDigest");
    }

    @Test void crossCheckMarksIdentityMismatchWithoutChangingAttribution() {
        Driver a = baseline(plc()), b = baseline(plc());
        Inputs.InputRecord arr = a.input(OCC, arrival("occ-1", T + 20_000, U2, S1));
        a.input(OCC, departure("occ-2", T + 40_000, U2, S1, "occ-1"));
        a.cut(T + 60_000 + L);
        b.cut(T + 60_000 + L);
        assertEquals(Disposition.Reason.ACCEPTED, a.disposition(arr, "event").body().reason(), "the occupancy is read");
        JsonNode inside = rowAt(a, NR1, T + 30_000, "Torque");
        assertEquals("attributed", inside.get("state").asText());
        assertEquals("identity-mismatch", inside.get("reason").asText());
        same(ref("V1"), inside.get("attributedTo"));
        for (long t : new long[]{T + 10_000, T + 19_000, T + 40_000, T + 50_000}) {
            JsonNode r = rowAt(a, NR1, t, "Torque");
            assertEquals("attributed", r.get("state").asText());
            assertTrue(r.get("reason").isNull(), "outside the occupant's [enter, leave): " + (t - T));
        }
        same(workRecord(b, "V1"), workRecord(a, "V1"));
        assertEquals("complete", status(workRecord(a, "V1")));
        assertEquals(1, keys(a, Stage1.WORK_RECORDS).size(), "the occupancy visit has no work record under plc-stamped");
        // the same occupant marks nothing
        Driver c = baseline(plc());
        c.input(OCC, arrival("occ-1", T + 20_000, U1, S1));
        c.cut(T + 60_000 + L);
        assertTrue(rowAt(c, NR1, T + 30_000, "Torque").get("reason").isNull());
    }

    @Test void crossCheckNoneReadsNoOccupancy() {
        Driver a = baseline(plcNoCheck()), b = baseline(plcNoCheck());
        Inputs.InputRecord arr = a.input(OCC, arrival("occ-1", T + 20_000, U2, S1));
        Inputs.InputRecord dep = a.input(OCC, departure("occ-2", T + 40_000, U2, S1, "occ-1"));
        assertEquals("none", a.rules.attribution().crossCheck());
        a.cut(T + 60_000 + L);
        b.cut(T + 60_000 + L);
        assertTrue(a.dispositions(arr).isEmpty() && a.dispositions(dep).isEmpty(), "an unread input has no output");
        assertTrue(rowAt(a, NR1, T + 30_000, "Torque").get("reason").isNull());
        assertEquals(b.outputText(), a.outputText());
        assertArrayEquals(Canonical.bytes(b.state.toJson()), Canonical.bytes(a.state.toJson()));
        assertTrue(a.state.deliveries.isEmpty());
        assertEquals("complete", status(workRecord(a, "V1")));
    }

    @Test void theSameVisitIdWithAnotherUnitIsAConflict() {
        Driver d = plc();
        scans(d, T, T + 30_000, "V1", U1, true);
        d.cut(T + 10_000);
        scans(d, T + 30_000, T + 60_000, "V1", U2, true);
        scans(d, T + 60_000, T + 62_000, "V1", U2, false);
        d.cut(T + 60_000 + L);
        JsonNode b = workRecord(d, "V1");
        assertEquals("retracted", status(b));
        same(Json.object().put("reason", "input-conflict").putNull("errorReason"), b.get("retraction"));
        assertEquals(U1, b.get("unitId").asText(), "the unit at the visit's start");
        for (long t : new long[]{T + 10_000, T + 40_000}) {
            Inputs.InputRecord m = messageAt(S1, t);
            for (String metric : List.of("UnitId", "VisitId", "VisitPresent")) {
                Disposition disp = d.disposition(m, Disposition.Part.member(metric));
                assertEquals(Disposition.Reason.IDENTITY_CONFLICT, disp.body().reason(), metric + " at " + (t - T));
                assertTrue(disp.body().refs().has("quarantine"));
            }
            assertEquals(Disposition.Reason.ACCEPTED, d.disposition(m, Disposition.Part.member("Running")).body().reason(), "only identity samples");
        }
        List<Disposition> unit = new ArrayList<>();
        for (Disposition x : d.dispositions(messageAt(S1, T + 10_000))) if (x.key().part().equals(Disposition.Part.member("UnitId"))) unit.add(x);
        assertEquals(List.of(Disposition.Reason.ACCEPTED, Disposition.Reason.IDENTITY_CONFLICT), unit.stream().map(x -> x.body().reason()).toList(),
            "accepted first, the conflict is the next revision");
        assertTrue(d.outputs.stream().anyMatch(o -> o instanceof QuarantineRecord), "the raw bytes are quarantined");
        same(ref("V1"), rowAt(d, NR1, T + 10_000, "Torque").get("attributedTo"));
        same(ref("V1"), rowAt(d, NR1, T + 40_000, "Torque").get("attributedTo"));
    }

    @Test void runningFalseNeverEndsAVisit() {
        Driver d = plc();
        for (long t = T; t < T + 60_000; t += 1000) {
            boolean s1 = t < T + 30_000, nr1 = t < T + 20_000;
            msg(d, S1, t, "V1", U1, true, tweak(S1, "Running", m -> m.put("value", s1)));
            msg(d, NR1, t, "V1", U1, true, tweak(NR1, "Running", m -> m.put("value", nr1)));
        }
        scans(d, T + 60_000, T + 62_000, "V1", U1, false);
        d.cut(T + 60_000 + L);
        JsonNode b = workRecord(d, "V1");
        assertEquals("complete", status(b));
        assertEquals(T + 60_000, b.get("interval").get("leave").longValue());
        same(intervals(T, T + 20_000), evidence(b, NR1).get("activityIntervals"));
        same(intervals(T + 20_000, T + 60_000), evidence(b, NR1).get("inactiveIntervals"));
        same(intervals(T, T + 30_000), evidence(b, S1).get("activityIntervals"));
        same(intervals(T + 30_000, T + 60_000), evidence(b, S1).get("inactiveIntervals"));
        JsonNode r = rowAt(d, NR1, T + 50_000, "Torque");
        assertEquals("attributed", r.get("state").asText());
        assertEquals(BooleanNode.FALSE, r.get("active"));
    }

    // ============================================================================================================ helpers

    /** The latest sample row of {@code device}'s {@code metric} at {@code sourceTs}. */
    static JsonNode rowAt(Driver d, String device, long sourceTs, String metric) {
        JsonNode last = null;
        for (Object o : d.outputs) if (o instanceof Row r && r.topic().equals(Stage1.SAMPLES) && r.logicalKey().get("device").asText().equals(device)
            && r.logicalKey().get("metric").asText().equals(metric) && r.body().get("sourceTs").longValue() == sourceTs) last = r.body();
        assertNotNull(last, device + " " + metric + " at " + sourceTs);
        return last;
    }
    /** The message of {@code device} this test sent with members at {@code sourceTs}. */
    Inputs.InputRecord messageAt(String device, long sourceTs) {
        for (Inputs.InputRecord r : sent) if (((Inputs.ProcessMessage) r.parsed()).device().equals(device) && Events.eventTime(r) == sourceTs) return r;
        fail("no message of " + device + " at " + sourceTs);
        return null;
    }
    static Set<String> keys(Driver d, String topic) {
        Set<String> s = new TreeSet<>();
        for (Object o : d.outputs) if (o instanceof Row r && r.topic().equals(topic)) s.add(Canonical.utf8(Canonical.bytes(r.logicalKey())));
        return s;
    }
}
