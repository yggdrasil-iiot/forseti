package yggdrasil.forseti.core.stage2;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.NegativeControls;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.*;
import yggdrasil.forseti.core.release.*;
import yggdrasil.forseti.core.rules.RuleSet;
import yggdrasil.forseti.core.stage1.DeadlineIndex;

/** Rule sheet T1-T10 on inline stage-1 rows. The work record bodies carry the fields stage 2 reads (unitId, status, retraction,
 *  insufficiencyReasons, interval, equipmentEvidence) in the shape Task 10's builder writes; sample rows are typed from S9. */
class Stage2Test {
    static final long T = 1794183000000L, BIRTH = 1794182400000L, L = 720_000;
    static final String S1 = "synthetic-site:line2:station1", S2 = "synthetic-site:line2:station2";
    static final String NR1 = S1 + ":nutrunner1", NR2 = S1 + ":nutrunner2";
    static final String DID_NOT_OCCUR = "urn:epcglobal:cbv:er:did_not_occur", INCORRECT_DATA = "urn:epcglobal:cbv:er:incorrect_data";
    static final Path FIXTURES = TestKeys.FIXTURES;
    static final JsonNode DEFINITIONS = Json.read(FIXTURES.resolve("recordings/smoke/manifest.json")).get("definitions");
    private static RuleSet line2;

    // ============================================================================================ rule sets

    static Path tmp(String prefix) { try { return Files.createTempDirectory("forseti-stage2-" + prefix); } catch (IOException e) { throw new UncheckedIOException(e); } }
    static RuleSet load(Path mappings, TrustedKeys keys) {
        HeadVerifier.Head h = HeadVerifier.verify(mappings, "line2", keys, new FloorStore(tmp("floor")), TestKeys.signedAtMs(mappings, "line2") + 60_000);
        return RuleSet.load(ReleaseReader.read(mappings, h), "line2-attribution", "1.0.0", DEFINITIONS);
    }
    static synchronized RuleSet line2() {
        if (line2 == null) line2 = load(TestKeys.copy("registry", tmp("registry")), TestKeys.fixtureKeys());
        return line2;
    }
    /** A re-signed copy of line2-rules 1.0.0 with the impact rule edited. */
    static RuleSet impact(Consumer<ObjectNode> edit) {
        Path m = TestKeys.copy("registry", tmp("impact"));
        String path = "impact-rules/0.json";
        ObjectNode r = (ObjectNode) Json.read(m.resolve("releases/line2-rules/1.0.0/payload").resolve(path));
        edit.accept(r);
        TestKeys.rewritePayload(m, "line2-rules", "1.0.0", path, Json.pretty(r));
        KeyPair p = TestKeys.generate(), a = TestKeys.generate();
        TestKeys.republish(m, "line2", new TestKeys.Signer("proposer", p.getPrivate()), new TestKeys.Signer("approver", a.getPrivate()));
        return load(m, TestKeys.trusted(tmp("keys"), "proposer", p.getPublic(), "approver", a.getPublic()));
    }
    static RuleSet evidence(String field, String value) { return impact(r -> ((ObjectNode) r.path("evidence")).put(field, value)); }

    // ============================================================================================ inline stage-1 rows

    static ObjectNode visit(String arrival) { return Json.object().put("mode", "occupancy-join").put("source", "mes-line2").put("arrivalEventId", arrival); }
    static long[] iv(long from, long to) { return new long[] {from, to}; }

    /** A work record body in Task 10's field order; equipment evidence entries are kept sorted by path. */
    static final class WR {
        final String unit, status;
        final long enter;
        final Long leave;
        final SortedSet<String> reasons = new TreeSet<>();
        final TreeMap<String, ObjectNode> evidence = new TreeMap<>();
        String retraction, errorReason;
        WR(String unit, long enter, Long leave, String status) { this.unit = unit; this.enter = enter; this.leave = leave; this.status = status; }
        WR reasons(String... r) { reasons.addAll(List.of(r)); return this; }
        WR retracted(String reason, String error) { this.retraction = reason; this.errorReason = error; return this; }
        WR evidence(String path, List<long[]> active, List<long[]> inactive) {
            ObjectNode e = Json.object().put("equipmentPath", path);
            e.putArray("memberRefs").add(path.endsWith("nutrunner1") ? "nutrunner1-running" : path.endsWith("nutrunner2") ? "nutrunner2-running" : "station1-running");
            e.set("activityIntervals", intervals(active));
            e.set("inactiveIntervals", intervals(inactive));
            ObjectNode c = e.putObject("coverage");
            Json.putSafe(c, "observedPeriods", 60); Json.putSafe(c, "missingPeriods", 0); Json.putSafe(c, "badSlots", 0);
            Json.putSafe(c, "unknownActivityPeriods", 0); Json.putSafe(c, "sessionGaps", 0);
            c.put("status", "complete");
            e.put("samplesDigest", Canonical.digest(Json.JSON.createArrayNode()));
            evidence.put(path, e);
            return this;
        }
        /** NR1 active over the whole visit, NR2 inactive over it. */
        WR nr1Active() { long e = leave == null ? enter + 60_000 : leave; return evidence(NR1, List.of(iv(enter, e)), List.of()).evidence(NR2, List.of(), List.of(iv(enter, e))); }
        /** NR2 active, NR1 inactive. */
        WR nr2Only() { long e = leave == null ? enter + 60_000 : leave; return evidence(NR1, List.of(), List.of(iv(enter, e))).evidence(NR2, List.of(iv(enter, e)), List.of()); }
        /** Both tools inactive. */
        WR noTool() { long e = leave == null ? enter + 60_000 : leave; return evidence(NR1, List.of(), List.of(iv(enter, e))).evidence(NR2, List.of(), List.of(iv(enter, e))); }
        ObjectNode body() {
            ObjectNode b = Json.object().put("unitId", unit).put("readPoint", S1);
            Json.putSafe(b, "visitSeq", 1);
            b.put("status", status);
            if (retraction != null) b.putObject("retraction").put("reason", retraction).put("errorReason", errorReason);
            ArrayNode ir = b.putArray("insufficiencyReasons");
            reasons.forEach(ir::add);
            ObjectNode in = Json.putSafe(b.putObject("interval"), "enter", enter);
            if (leave == null) in.putNull("leave"); else Json.putSafe(in, "leave", leave);
            in.putNull("attributionEnd");
            b.putArray("members");
            ArrayNode ev = b.putArray("equipmentEvidence");
            evidence.values().forEach(ev::add);
            b.put("samplesDigest", Canonical.digest(Json.JSON.createArrayNode()));
            ObjectNode c = Json.putSafe(b.putObject("coverage"), "expectedPeriodMs", 1000);
            Json.putSafe(c, "observedPeriods", 60); Json.putSafe(c, "missingPeriods", 0); Json.putSafe(c, "badSlots", 0); Json.putSafe(c, "sessionGaps", 0);
            b.put("attributionMode", "occupancy-join").put("rulesDigest", line2().rulesDigest());
            return b;
        }
        static ArrayNode intervals(List<long[]> l) { ArrayNode a = Json.JSON.createArrayNode(); for (long[] x : l) a.addArray().add(x[0]).add(x[1]); return a; }
    }
    static WR complete(String unit, long enter, long leave) { return new WR(unit, enter, leave, "complete"); }

    static ObjectNode inc(long revision, String equipmentRef, long from, Long to, String status, long detectedAt, long revisionEventTime) {
        ObjectNode n = Json.object().put("incidentId", "inc-1");
        Json.putSafe(n, "revision", revision);
        n.put("stationRef", S1).put("equipmentRef", equipmentRef).put("kind", to == null || !"closed".equals(status) ? "gauge-drift" : "tool-check-failed");
        Json.putSafe(n, "detectedAt", detectedAt);
        Json.putSafe(n, "revisionEventTime", revisionEventTime);
        Json.putSafe(n, "suspectFrom", from);
        if (to == null) n.putNull("suspectTo"); else Json.putSafe(n, "suspectTo", to);
        return n.put("status", status).put("evidenceRef", "check-log:1").put("source", "qa-line2");
    }
    /** A closed tool-check-failed revision 1 over [from, to): suspectTo = detectedAt = revisionEventTime (spec 172). */
    static ObjectNode closed(long from, long to) { return inc(1, NR1, from, to, "closed", to, to); }

    // ============================================================================================ the bench

    /** One station's stage 2 fed by hand: stage-1 rows, incidents on partition 0 of quality.incidents, cuts with six barriers. */
    static final class Bench {
        final Stage2 stage2;
        Stage2State state;
        final List<Object> outputs = new ArrayList<>();
        final Map<String, Long> basisRevisions = new HashMap<>(), sampleRevisions = new HashMap<>();
        long incidentOffset, cutId;
        boolean roundTrip;

        Bench(RuleSet rules) {
            stage2 = new Stage2(rules, NegativeControls.NONE);
            state = stage2.newState(1, S1);
            Map<String, SetCoverage.EvidenceRange> ranges = new TreeMap<>();
            for (String d : List.of(S1, NR1, NR2)) ranges.put(d, new SetCoverage.EvidenceRange(T - 3_600_000, T + 3_600_000));
            state.evidenceRanges(ranges);
        }
        Bench() { this(line2()); }

        List<Object> apply(Events2.Event e, Long w2) {
            Stage2.Result r = stage2.decide(state, e, w2);
            state = r.state();
            outputs.addAll(r.outputs());
            return r.outputs();
        }
        static Envelope envelope(long revision) {
            return new Envelope(1, revision, null, Json.object().put("kind", "input"), null, null, Json.JSON.createArrayNode(), null);
        }
        List<Object> basis(ObjectNode visitRef, ObjectNode body) {
            long rev = basisRevisions.merge(Canonical.utf8(Canonical.bytes(visitRef)), 1L, Long::sum);
            return apply(new Events2.Basis(Row.of(Stage2.WORK_RECORDS, visitRef, body, envelope(rev))), state.lastW2());
        }
        List<Object> basis(ObjectNode visitRef, WR wr) { return basis(visitRef, wr.body()); }
        List<Object> deleteBasis(ObjectNode visitRef) {
            long rev = basisRevisions.merge(Canonical.utf8(Canonical.bytes(visitRef)), 1L, Long::sum);
            return apply(new Events2.Basis(new Delete(1, visitRef, rev, "x")), state.lastW2());
        }
        /** A sample row of a member of {@code device} at {@code t} (sampleId seq from t, so a later row at t revises it). */
        void sample(String device, String metric, long t, String state, String reason, JsonNode active, ObjectNode attributedTo) {
            ObjectNode key = Json.object().put("edge", "edge-line2").put("device", device);
            Json.putSafe(key, "birthPublishTs", BIRTH).put("metric", metric);
            Json.putSafe(key, "bdSeq", 0); Json.putSafe(key, "seq", (t - BIRTH) / 1000); Json.putSafe(key, "publishTs", t);
            ObjectNode body = Json.putSafe(Json.object(), "sourceTs", t).put("state", state).put("reason", reason);
            body.set("active", active);
            if (attributedTo == null) body.putNull("attributedTo"); else body.set("attributedTo", attributedTo);
            long rev = sampleRevisions.merge(Canonical.utf8(Canonical.bytes(key)), 1L, Long::sum);
            apply(new Events2.Sample(Row.of(Stage2.SAMPLES, key, body, envelope(rev))), this.state.lastW2());
        }
        void running(String device, long from, long to, String state, String reason, JsonNode active, ObjectNode attributedTo) {
            for (long t = from; t < to; t += 1000) sample(device, "Running", t, state, reason, active, attributedTo);
        }
        List<Object> incident(ObjectNode value) {
            byte[] raw;
            try { raw = Json.JSON.writeValueAsBytes(value); } catch (JsonProcessingException e) { throw new IllegalStateException(e); }
            Inputs.InputRecord r = InputParser.read(new Inputs.Origin(InputParser.INCIDENTS, 0, incidentOffset++), value.path("equipmentRef").asText(), raw).record();
            return apply(new Events2.Incident(r), state.lastW2());
        }
        void cut(long w2) {
            cutId++;
            while (true) {
                List<DeadlineIndex.Deadline> due = state.deadlines().due(w2);
                if (due.isEmpty()) break;
                DeadlineIndex.Deadline d = due.get(0);
                apply(new Events2.Timer(d.deadline(), d.key(), d.purpose()), w2);
            }
            List<Progress.IncidentOffset> offsets = new ArrayList<>();
            for (int q = 0; q < 6; q++) offsets.add(new Progress.IncidentOffset(cutId, q, q == 0 ? incidentOffset - 1 : -1));
            apply(new Events2.CutClose(cutId, w2, List.of(new Progress.Stage1ProgressRef(Stage2.WORK_RECORDS, 0, cutId),
                new Progress.Stage1ProgressRef(Stage2.SAMPLES, 0, cutId)), offsets), w2);
            if (roundTrip) state = Stage2State.fromJson(Json.read(Canonical.bytes(state.toJson())));
        }

        /** Live assessment rows by unitId. */
        Map<String, Row> assessments() {
            Map<String, Row> m = new TreeMap<>();
            for (Object o : outputs) {
                if (o instanceof Row r && r.topic().equals(Stage2.AFFECTED)) m.put(r.logicalKey().path("unitId").asText(), r);
                else if (o instanceof Delete d) m.remove(d.logicalKey().path("unitId").asText());
            }
            return m;
        }
        ObjectNode assessment(String unit) { Row r = assessments().get(unit); assertNotNull(r, "no live assessment for " + unit); return (ObjectNode) r.body(); }
        String statusOf(String unit) { ObjectNode b = assessment(unit); return b.get("status").asText() + " " + b.get("reason").asText(); }
        ObjectNode set() {
            ObjectNode b = null;
            for (Object o : outputs) if (o instanceof Row r && r.topic().equals(Stage2.AFFECTED_SETS)) b = (ObjectNode) r.body();
            assertNotNull(b, "no set row");
            return b;
        }
        ObjectNode coverage() { return (ObjectNode) set().get("coverage"); }
        List<String> setUnits() { List<String> l = new ArrayList<>(); set().get("units").forEach(u -> l.add(u.get("unitId").asText())); return l; }
        List<Disposition> dispositions() { List<Disposition> l = new ArrayList<>(); for (Object o : outputs) if (o instanceof Disposition d) l.add(d); return l; }
        Disposition last(List<Object> out) {
            Disposition d = null;
            for (Object o : out) if (o instanceof Disposition x) d = x;
            assertNotNull(d);
            return d;
        }
        long count(String topic) { return outputs.stream().filter(o -> o instanceof Row r && r.topic().equals(topic)).count(); }
        String text() { StringBuilder b = new StringBuilder(); for (Object o : outputs) b.append(Stage2.describe(o)).append('\n'); return b.toString(); }
    }

    static String wire(Disposition d) { return d.body().disposition().wire() + " " + d.body().reason().wire(); }

    // ============================================================================================ scenarios

    @Test void anIncidentAfterCompleteRecordsMarksToolVisitsCandidates() {
        Bench b = new Bench();
        ObjectNode a1 = visit("a1"), a2 = visit("a2");
        b.basis(a1, complete("U1", T, T + 60_000).evidence(NR1, List.of(iv(T + 5_000, T + 55_000)), List.of(iv(T, T + 5_000), iv(T + 55_000, T + 60_000)))
            .evidence(NR2, List.of(), List.of(iv(T, T + 60_000))));
        b.basis(a2, complete("U2", T + 60_000, T + 120_000).nr2Only());
        b.running(NR1, T, T + 60_000, "attributed", null, BooleanNode.TRUE, a1);
        b.running(NR1, T + 60_000, T + 120_000, "attributed", null, BooleanNode.FALSE, a2);
        b.cut(T + 200_000);
        List<Object> out = b.incident(closed(T, T + 120_000));
        assertEquals("accepted accepted", wire(b.last(out)));
        b.cut(T + 120_000 + L);
        assertEquals("affected-candidate equipment-activity", b.statusOf("U1"));
        assertEquals("not-affected-within-scope no-equipment-activity", b.statusOf("U2"), "16: a T2-only visit has no T1 activity");
        assertEquals(List.of("U1", "U2"), b.setUnits());
        ObjectNode c = b.coverage();
        assertTrue(c.get("settled").booleanValue());
        assertEquals("complete", c.get("status").textValue(), c.toString());
        ObjectNode u1 = b.assessment("U1");
        assertEquals(List.of("status", "reason", "incident", "basis", "evidenceWarnings", "rulesDigest"), names(u1));
        assertEquals(a1, u1.get("basis").get(0).get("visitRef"));
        assertEquals(12, u1.get("incident").size(), "the current revision's twelve fields");
        assertEquals(line2().rulesDigest(), u1.get("rulesDigest").textValue());
        assertEquals(List.of("incident", "units", "resolution", "coverage"), names(b.set()), "the affected set carries no rulesDigest");
        assertTrue(b.set().get("resolution").isNull());
    }

    @Test void anIncidentBeforeTheRecordsStartsEmptyAndGrows() {
        Bench b = new Bench();
        b.incident(closed(T, T + 60_000));
        b.cut(T + 30_000);
        assertEquals(List.of(), b.setUnits());
        assertFalse(b.coverage().get("settled").booleanValue());
        ObjectNode a1 = visit("a1");
        b.basis(a1, new WR("U1", T, T + 60_000L, "provisional").nr1Active());
        assertEquals("insufficient-evidence basis-provisional", b.statusOf("U1"));
        assertEquals(List.of("U1"), b.setUnits());
        b.basis(a1, complete("U1", T, T + 60_000).nr1Active());
        assertEquals("affected-candidate equipment-activity", b.statusOf("U1"));
        b.running(NR1, T, T + 60_000, "attributed", null, BooleanNode.TRUE, a1);
        b.cut(T + 60_000 + L - 1);
        assertFalse(b.coverage().get("settled").booleanValue());
        b.cut(T + 60_000 + L);
        assertTrue(b.coverage().get("settled").booleanValue());
        assertEquals("complete", b.coverage().get("status").textValue());
    }

    @Test void aNarrowedAndClosedIncidentMarksLaterVisitsIntervalNarrowed() {
        Bench b = new Bench();
        b.basis(visit("a1"), complete("U1", T, T + 40_000).nr1Active());
        b.basis(visit("a2"), complete("U2", T + 40_000, T + 80_000).nr2Only());
        b.basis(visit("a3"), complete("U3", T + 80_000, T + 120_000).noTool());
        b.basis(visit("a5"), complete("U5", T + 200_000, T + 240_000).nr1Active());
        b.incident(inc(1, NR1, T, null, "open", T + 150_000, T + 150_000));
        assertTrue(b.coverage().get("open").booleanValue());
        assertEquals("affected-candidate equipment-activity", b.statusOf("U5"), "open incident: a closed visit leaving after suspectFrom is related");
        b.incident(inc(2, NR1, T, T + 120_000L, "closed", T + 150_000, T + 300_000));
        assertEquals("affected-candidate equipment-activity", b.statusOf("U1"));
        assertEquals("not-affected-within-scope no-equipment-activity", b.statusOf("U2"));
        assertEquals("not-affected-within-scope no-equipment-activity", b.statusOf("U3"));
        assertEquals("not-affected-within-scope interval-narrowed", b.statusOf("U5"));
        assertEquals(0, b.assessment("U5").get("basis").size());
        assertFalse(b.coverage().get("open").booleanValue());
        assertEquals(2, b.set().get("incident").get("revision").longValue());
    }

    @Test void aWithdrawnIncidentFollowsOnWithdrawn() {
        for (String onWithdrawn : List.of("not-affected", "insufficient")) {
            Bench b = new Bench("not-affected".equals(onWithdrawn) ? line2() : evidence("onWithdrawn", onWithdrawn));
            b.basis(visit("a1"), complete("U1", T, T + 60_000).nr1Active());
            b.incident(closed(T, T + 60_000));
            b.incident(inc(2, NR1, T, T + 60_000L, "withdrawn", T + 60_000, T + 100_000));
            b.cut(T + 100_000);
            assertEquals(("not-affected".equals(onWithdrawn) ? "not-affected-within-scope" : "insufficient-evidence") + " incident-withdrawn", b.statusOf("U1"));
            assertEquals("withdrawn", b.set().get("resolution").textValue());
            ObjectNode c = b.coverage();
            assertFalse(c.get("open").booleanValue());
            assertTrue(c.get("settled").booleanValue());
            assertNotEquals("complete", c.get("status").textValue());
        }
    }

    @Test void unattributedActivityInsideTheIntervalKeepsCoverageIncomplete() {
        Bench b = new Bench();
        ObjectNode a1 = visit("a1");
        b.basis(a1, complete("U1", T, T + 60_000).nr1Active());
        b.running(NR1, T, T + 60_000, "attributed", null, BooleanNode.TRUE, a1);
        b.running(NR1, T + 60_000, T + 120_000, "unattributed", "no-occupancy", BooleanNode.TRUE, null);
        b.incident(closed(T, T + 120_000));
        b.cut(T + 120_000 + L);
        ObjectNode c = b.coverage();
        assertEquals(60, c.get("unattributedActivityPeriods").longValue());
        assertEquals(0, c.get("unknownActivityPeriods").longValue());
        assertEquals("incomplete", c.get("status").textValue());
        assertEquals("affected-candidate equipment-activity", b.statusOf("U1"));
    }

    @Test void aRetractedBasisWithDidNotOccurMakesTheCandidateNotAffected() {
        for (String onBasisRetracted : List.of("by-cause", "always-insufficient")) {
            Bench b = new Bench("by-cause".equals(onBasisRetracted) ? line2() : evidence("onBasisRetracted", onBasisRetracted));
            ObjectNode a1 = visit("a1");
            b.basis(a1, complete("U1", T, T + 60_000).nr1Active());
            b.incident(closed(T, T + 60_000));
            assertEquals("affected-candidate equipment-activity", b.statusOf("U1"));
            ObjectNode retracted = new WR("U1", T, T + 60_000L, "retracted").retracted("error-declaration", DID_NOT_OCCUR).nr1Active().body();
            b.basis(a1, retracted);
            assertEquals(("by-cause".equals(onBasisRetracted) ? "not-affected-within-scope" : "insufficient-evidence") + " basis-retracted-error-declaration", b.statusOf("U1"));
            assertEquals(Canonical.digest(retracted), b.assessment("U1").get("basis").get(0).get("bodyDigest").textValue(), "J-BASIS-CURRENT");
            assertEquals(1, b.assessment("U1").get("evidenceWarnings").size());
        }
    }

    @Test void aLateAttributionAddsTheUnit() {
        Bench b = new Bench();
        b.incident(closed(T, T + 60_000));
        b.running(NR1, T, T + 60_000, "unattributed", "no-occupancy", BooleanNode.TRUE, null);
        b.cut(T + 70_000);
        assertEquals(List.of(), b.setUnits());
        assertEquals(60, b.coverage().get("unattributedActivityPeriods").longValue());
        ObjectNode a1 = visit("a1");
        b.basis(a1, complete("U1", T, T + 60_000).nr1Active());
        b.running(NR1, T, T + 60_000, "attributed", null, BooleanNode.TRUE, a1);
        b.cut(T + 80_000);
        assertEquals(List.of("U1"), b.setUnits());
        assertEquals("affected-candidate equipment-activity", b.statusOf("U1"));
        assertEquals(0, b.coverage().get("unattributedActivityPeriods").longValue());
    }

    @Test void aProvisionalBasisIsInsufficientEvidence() {
        Bench b = new Bench();
        ObjectNode a1 = visit("a1");
        b.basis(a1, new WR("U1", T, T + 60_000L, "provisional").nr1Active());
        b.incident(closed(T, T + 60_000));
        assertEquals("insufficient-evidence basis-provisional", b.statusOf("U1"));
        JsonNode w = b.assessment("U1").get("evidenceWarnings");
        assertEquals(1, w.size());
        assertEquals(a1, w.get(0).get("visitRef"));
        assertEquals(0, w.get(0).get("insufficiencyReasons").size());
        assertEquals(1, b.coverage().get("provisionalUnits").longValue());
        assertEquals(0, b.coverage().get("insufficientUnits").longValue());
    }

    @Test void aDepartureThatNarrowsAnOpenVisitDeletesTheRow() {
        Bench b = new Bench();
        ObjectNode a1 = visit("a1");
        b.basis(a1, new WR("U1", T - 200_000, null, "provisional").nr1Active());
        b.incident(closed(T, T + 60_000));
        assertEquals("insufficient-evidence basis-provisional", b.statusOf("U1"), "an open visit entering before suspectFrom is possibly-related");
        Row first = b.assessments().get("U1");
        List<Object> out = b.basis(a1, new WR("U1", T - 200_000, T - 100_000L, "provisional").nr1Active());
        Delete d = (Delete) out.stream().filter(o -> o instanceof Delete).findFirst().orElseThrow();
        assertEquals(first.envelope().revision() + 1, d.revision());
        assertEquals(first.bodyDigest(), d.supersedesDigest());
        assertFalse(b.assessments().containsKey("U1"));
        assertEquals(List.of(), b.setUnits());
        b.basis(a1, new WR("U1", T - 200_000, null, "provisional").nr1Active());
        assertEquals(d.revision() + 1, b.assessments().get("U1").envelope().revision(), "a reappearing row continues the revisions");
        assertEquals(List.of("U1"), b.setUnits());
    }

    @Test void rowSetWidensAcrossRevisionsButJudgesTheCurrentOne() {
        Bench b = new Bench();
        b.basis(visit("a1"), complete("U1", T, T + 60_000).nr1Active());
        b.basis(visit("a2"), complete("U2", T + 200_000, T + 260_000).nr1Active());
        b.incident(inc(1, NR1, T, T + 300_000L, "closed", T + 300_000, T + 300_000));
        assertEquals("affected-candidate equipment-activity", b.statusOf("U2"));
        b.incident(inc(2, NR1, T, T + 120_000L, "closed", T + 300_000, T + 400_000));
        assertEquals("affected-candidate equipment-activity", b.statusOf("U1"));
        assertEquals("not-affected-within-scope interval-narrowed", b.statusOf("U2"), "kept by revision 1's interval, judged by revision 2");
        assertEquals(2, b.assessment("U1").get("incident").get("revision").longValue());
        assertEquals(List.of("U1", "U2"), b.setUnits());
    }

    @Test void rowSetIsTheSameInBothArrivalOrders() {
        ObjectNode wide = inc(1, NR1, T, T + 300_000L, "closed", T + 300_000, T + 300_000), narrow = inc(2, NR1, T, T + 120_000L, "closed", T + 300_000, T + 400_000);
        Bench one = new Bench(), two = new Bench();
        for (Bench b : List.of(one, two)) {
            b.basis(visit("a1"), complete("U1", T, T + 60_000).nr1Active());
            b.basis(visit("a2"), complete("U2", T + 200_000, T + 260_000).nr1Active());
        }
        one.incident(wide); one.incident(narrow);
        two.incident(narrow);
        assertEquals("duplicate superseded", wire(two.last(two.incident(wide))));
        for (Bench b : List.of(one, two)) b.cut(T + 400_000);
        assertEquals(one.assessments().keySet(), two.assessments().keySet());
        for (String u : one.assessments().keySet()) assertEquals(one.assessment(u), two.assessment(u), u);
        assertEquals(one.set(), two.set());
        assertEquals("not-affected-within-scope interval-narrowed", one.statusOf("U2"));
        assertEquals("not-affected-within-scope interval-narrowed", two.statusOf("U2"));
    }

    @Test void uncertainActivityUnderPropagateIsNotUnknownCoverage() {
        Bench propagate = new Bench(), reject = new Bench();
        // stage 1 under propagate: an Uncertain Running=true with no visit is unattributed activity; under reject the same samples are bad-quality
        propagate.running(NR1, T, T + 60_000, "unattributed", "no-occupancy", BooleanNode.TRUE, null);
        reject.running(NR1, T, T + 60_000, "unattributed", "bad-quality", TextNode.valueOf("unknown"), null);
        for (Bench b : List.of(propagate, reject)) { b.incident(closed(T, T + 60_000)); b.cut(T + 60_000 + L); }
        assertEquals(60, propagate.coverage().get("unattributedActivityPeriods").longValue());
        assertEquals(0, propagate.coverage().get("unknownActivityPeriods").longValue());
        assertEquals("incomplete", propagate.coverage().get("status").textValue());
        assertEquals(0, reject.coverage().get("unattributedActivityPeriods").longValue());
        assertEquals(60, reject.coverage().get("unknownActivityPeriods").longValue());
        assertEquals("unknown", reject.coverage().get("status").textValue());
    }

    @Test void noActivityIsJudgedInsideTheIntersectionOnly() {
        WR gap = complete("U1", T, T + 60_000).evidence(NR1, List.of(), List.of(iv(T, T + 10_000), iv(T + 12_000, T + 60_000))).evidence(NR2, List.of(), List.of(iv(T, T + 60_000)));
        Bench outside = new Bench(), inside = new Bench();
        outside.basis(visit("a1"), gap.body());
        outside.incident(inc(1, NR1, T + 20_000, T + 50_000L, "closed", T + 50_000, T + 50_000));
        assertEquals("not-affected-within-scope no-equipment-activity", outside.statusOf("U1"), "the 2-slot gap lies outside [enter, leave) intersect S");
        inside.basis(visit("a1"), gap.body());
        inside.incident(inc(1, NR1, T + 5_000, T + 50_000L, "closed", T + 50_000, T + 50_000));
        assertEquals("insufficient-evidence basis-insufficient", inside.statusOf("U1"), "the gap inside the judged interval proves nothing");
    }

    @Test void aStationChangeIsRejectedWithoutReplacingTheCurrentRevision() {
        Bench b = new Bench(impact(r -> ((ArrayNode) r.path("incidentScope")).add(S2)));
        b.basis(visit("a1"), complete("U1", T, T + 60_000).nr1Active());
        b.incident(closed(T, T + 60_000));
        long rows = b.count(Stage2.AFFECTED);
        ObjectNode moved = inc(2, S2, T, T + 60_000L, "closed", T + 60_000, T + 70_000).put("stationRef", S2);
        Disposition d = b.last(b.incident(moved));
        assertEquals("rejected station-change", wire(d));
        assertEquals(rows, b.count(Stage2.AFFECTED));
        assertEquals(1, b.assessment("U1").get("incident").get("revision").longValue());
        assertEquals(S1, b.set().get("incident").get("stationRef").textValue());
    }

    @Test void aLowerRevisionIsSuperseded() {
        Bench b = new Bench();
        b.basis(visit("a1"), complete("U1", T, T + 60_000).nr1Active());
        assertEquals("accepted accepted", wire(b.last(b.incident(inc(2, NR1, T, T + 60_000L, "closed", T + 60_000, T + 90_000)))));
        assertEquals("duplicate superseded", wire(b.last(b.incident(closed(T, T + 60_000)))));
        assertEquals(2, b.assessment("U1").get("incident").get("revision").longValue());
        assertEquals("duplicate identical-redelivery", wire(b.last(b.incident(closed(T, T + 60_000)))));
        List<Object> conflict = b.incident(inc(2, NR1, T, T + 60_000L, "closed", T + 60_000, T + 90_000).put("evidenceRef", "check-log:2"));
        assertEquals(2, conflict.stream().filter(o -> o instanceof Disposition x && x.body().reason() == Disposition.Reason.IDENTITY_CONFLICT).count(),
            "both variants of revision 2 are identity-conflict");
        assertEquals(1, b.assessment("U1").get("incident").get("revision").longValue(), "revision 2 is unusable: the highest remaining valid revision is current");
    }

    @Test void revisionEventTimeIsCheckedByRevisionNumber() {
        Bench b = new Bench();
        Disposition first = b.last(b.incident(inc(1, NR1, T, T + 60_000L, "closed", T + 60_000, T + 61_000)));
        assertEquals("rejected schema-invalid", wire(first));
        assertEquals("revisionEventTime", first.body().refs().path("check").asText());
        assertEquals(0, b.count(Stage2.AFFECTED_SETS), "a rejected revision is not applied");
        assertEquals("accepted accepted", wire(b.last(b.incident(closed(T, T + 60_000)))));
        assertEquals("accepted accepted", wire(b.last(b.incident(inc(3, NR1, T, T + 60_000L, "closed", T + 60_000, T + 80_000)))));
        Disposition two = b.last(b.incident(inc(2, NR1, T, T + 60_000L, "closed", T + 60_000, T + 90_000)));
        assertEquals("rejected schema-invalid", wire(two), "revision 2 above the accepted revision 3: the later-arriving 2 is rejected");
        assertEquals("revisionEventTime", two.body().refs().path("check").asText());
        assertEquals(3, b.set().get("incident").get("revision").longValue());
        assertEquals("duplicate superseded", wire(b.last(b.incident(inc(2, NR1, T, T + 60_000L, "closed", T + 60_000, T + 70_000)))));
    }

    @Test void aSetOnlyCoverageChangeDoesNotReviseUnits() {
        Bench b = new Bench();
        ObjectNode a1 = visit("a1");
        b.basis(a1, complete("U1", T, T + 60_000).nr1Active());
        b.running(NR1, T, T + 60_000, "attributed", null, BooleanNode.TRUE, a1);
        b.incident(closed(T, T + 120_000));
        b.cut(T + 120_000 + L);
        long units = b.count(Stage2.AFFECTED), sets = b.count(Stage2.AFFECTED_SETS);
        assertEquals(60, b.coverage().get("unknownActivityPeriods").longValue());
        b.running(NR1, T + 60_000, T + 120_000, "unattributed", "no-occupancy", BooleanNode.FALSE, null);
        b.cut(T + 130_000 + L);
        assertEquals(units, b.count(Stage2.AFFECTED), "no unit row revision");
        assertEquals(sets + 1, b.count(Stage2.AFFECTED_SETS));
        assertEquals(0, b.coverage().get("unknownActivityPeriods").longValue());
    }

    /** The nine rows of T6, each with a lower row also present so that the first match is shown. */
    @ParameterizedTest @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9})
    void eachDecisionRowInOrder(int row) {
        Bench b = new Bench();
        ObjectNode v = visit("v"), w = visit("w");
        String expected;
        switch (row) {
            case 1:
                b.basis(v, complete("U1", T, T + 60_000).nr1Active());
                b.incident(closed(T, T + 60_000));
                b.incident(inc(2, NR1, T, T + 60_000L, "withdrawn", T + 60_000, T + 90_000));
                expected = "not-affected-within-scope incident-withdrawn";
                break;
            case 2:
                b.basis(v, complete("U1", T, T + 30_000).nr1Active());
                b.basis(w, new WR("U1", T + 30_000, T + 60_000L, "provisional").nr1Active());
                b.incident(closed(T, T + 60_000));
                expected = "affected-candidate equipment-activity";
                assertEquals(w, b.assessment("U1").get("evidenceWarnings").get(0).get("visitRef"), "incomplete evidence goes to evidenceWarnings");
                break;
            case 3:
                b.basis(v, complete("U1", T + 100_000, T + 160_000).nr1Active());
                b.incident(inc(1, NR1, T, T + 300_000L, "closed", T + 300_000, T + 300_000));
                b.incident(inc(2, NR1, T, T + 60_000L, "closed", T + 300_000, T + 310_000));
                expected = "not-affected-within-scope interval-narrowed";
                break;
            case 4:
                b.basis(v, new WR("U1", T, T + 30_000L, "insufficient").reasons("correction-conflict").nr1Active());
                b.basis(w, new WR("U1", T + 30_000, T + 60_000L, "provisional").nr1Active());
                b.incident(closed(T, T + 60_000));
                expected = "insufficient-evidence correction-conflict";
                break;
            case 5:
                b.basis(v, new WR("U1", T, T + 30_000L, "provisional").nr1Active());
                b.basis(w, new WR("U1", T + 30_000, T + 60_000L, "retracted").retracted("overlap", null).nr1Active());
                b.incident(closed(T, T + 60_000));
                expected = "insufficient-evidence basis-provisional";
                break;
            case 6:
                b.basis(v, new WR("U1", T, T + 30_000L, "retracted").retracted("overlap", null).nr1Active());
                b.basis(w, complete("U1", T + 30_000, T + 60_000).noTool());
                b.incident(closed(T, T + 60_000));
                expected = "insufficient-evidence basis-retracted-overlap";
                break;
            case 7:
                b.basis(v, complete("U1", T, T + 30_000).noTool());
                b.basis(w, new WR("U1", T + 30_000, T + 60_000L, "retracted").retracted("error-declaration", DID_NOT_OCCUR).nr1Active());
                b.incident(closed(T, T + 60_000));
                expected = "not-affected-within-scope no-equipment-activity";
                break;
            case 8:
                b.basis(v, new WR("U1", T, T + 60_000L, "retracted").retracted("error-declaration", DID_NOT_OCCUR).nr1Active());
                b.incident(closed(T, T + 60_000));
                expected = "not-affected-within-scope basis-retracted-error-declaration";
                break;
            default:
                b.basis(v, complete("U1", T, T + 60_000).evidence(NR2, List.of(iv(T, T + 60_000)), List.of()));
                b.incident(closed(T, T + 60_000));
                expected = "insufficient-evidence basis-insufficient";
        }
        assertEquals(expected, b.statusOf("U1"), "row " + row);
    }

    @Test void decideIsDeterministicAndReadsOnlyItsArguments() {
        Bench a = sequence(false), b = sequence(false), c = sequence(true);
        assertEquals(a.text(), b.text());
        assertEquals(a.text(), c.text());
        assertArrayEquals(Canonical.bytes(a.state.toJson()), Canonical.bytes(b.state.toJson()));
        assertArrayEquals(Canonical.bytes(a.state.toJson()), Canonical.bytes(c.state.toJson()));
        assertEquals(Stage2State.VERSION, a.state.toJson().get("stateVersion").intValue());
        long progress = a.outputs.stream().filter(o -> o instanceof Progress.Stage2).count(), ledgers = a.outputs.stream().filter(o -> o instanceof Progress.Ledger).count();
        assertEquals(3, progress);
        assertEquals(3, ledgers);
        Progress.Stage2 p = (Progress.Stage2) a.outputs.stream().filter(o -> o instanceof Progress.Stage2).reduce((x, y) -> y).orElseThrow();
        assertEquals(6, p.incidentOffsets().size());
        assertEquals(List.of(), p.mergedCutIds());
        assertTrue(a.outputs.get(a.outputs.size() - 1) instanceof Progress.Ledger, "progress follows the cut's rows");
    }

    static Bench sequence(boolean roundTrip) {
        Bench b = new Bench();
        b.roundTrip = roundTrip;
        ObjectNode a1 = visit("a1"), a2 = visit("a2");
        b.basis(a1, new WR("U1", T, T + 40_000L, "provisional").nr1Active());
        b.running(NR1, T, T + 40_000, "attributed", null, BooleanNode.TRUE, a1);
        b.incident(inc(1, NR1, T, null, "open", T + 50_000, T + 50_000));
        b.cut(T + 50_000);
        b.basis(a1, complete("U1", T, T + 40_000).nr1Active());
        b.basis(a2, new WR("U2", T + 40_000, null, "provisional").nr2Only());
        b.running(NR1, T + 40_000, T + 80_000, "attributed", null, BooleanNode.FALSE, a2);
        b.incident(inc(2, NR1, T, T + 80_000L, "closed", T + 50_000, T + 90_000));
        b.cut(T + 100_000);
        b.basis(a2, new WR("U2", T + 40_000, T + 90_000L, "retracted").retracted("error-declaration", INCORRECT_DATA).nr2Only());
        b.cut(T + 80_000 + L);
        return b;
    }

    static List<String> names(ObjectNode n) { List<String> l = new ArrayList<>(); n.fieldNames().forEachRemaining(l::add); return l; }
}
