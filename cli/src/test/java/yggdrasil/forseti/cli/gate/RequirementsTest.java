package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.gate.RequirementsAccount.Status;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The accounting of {@code forseti requirements} (spec 705, plan D14) across pass, miss and error, the gate report it records 5 (a),
 *  5 (b) and the restart part of 5 (d) from, the common cuts its body comparisons use, and the three-run determinism comparison. */
class RequirementsTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();

    @TempDir Path tmp;

    static final List<String> IDS = List.of("R-A", "R-B", "R-C");

    @Test void everyRequirementRecordedExitsZeroWithAMissAndAnErrorAndTheEngineIsIneligible() {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        RequirementsAccount acc = new RequirementsAccount(IDS, new PrintStream(b, true, StandardCharsets.UTF_8));
        acc.record("R-A", Status.PASS, "held");
        acc.record("R-B", Status.MISS, "the dedicated placement did not settle within 60 s");
        acc.record("R-C", Status.ERROR, "the run crashed");
        assertEquals(0, acc.exitCode(), "a miss and an error are recorded results");
        assertEquals("requirements recorded 3 / expected 3", acc.line());
        String printed = b.toString(StandardCharsets.UTF_8);
        assertTrue(printed.contains("PASS R-A held") && printed.contains("MISS R-B") && printed.contains("ERROR R-C"), printed);
        RequirementsAccount.Eligibility e = acc.eligibility("flink", true, null);
        assertFalse(e.eligible());
        assertEquals(List.of("R-B"), e.missed());
        assertEquals(List.of("R-C"), e.errored());
        assertTrue(e.line().startsWith("engine eligibility: flink ineligible (semantic gate passed; requirements missed R-B; errors R-C)"), e.line());
        assertTrue(e.line().contains("not the completion of the delivery step"), e.line());
        JsonNode j = acc.toJson();
        assertEquals(List.of("pass", "miss", "error"), List.of(j.get(0).path("status").asText(), j.get(1).path("status").asText(), j.get(2).path("status").asText()));
    }

    @Test void everyRequirementPassedAndTheGatePassedMakesTheEngineEligible() {
        RequirementsAccount acc = new RequirementsAccount(IDS, null);
        for (String id : IDS) acc.record(id, Status.PASS, "held");
        assertEquals(0, acc.exitCode());
        assertTrue(acc.eligibility("flink", true, null).eligible());
        RequirementsAccount.Eligibility notGate = acc.eligibility("flink", false, "layout shard");
        assertFalse(notGate.eligible(), "every requirement passed, but without the semantic gate the engine is not eligible");
        assertTrue(notGate.line().contains("semantic gate not passed: layout shard"), notGate.line());
    }

    @Test void anUnrecordedRequirementExitsOneAndACrashRecordsTheRestAsErrors() {
        RequirementsAccount acc = new RequirementsAccount(IDS, null);
        acc.record("R-A", Status.PASS, "held");
        assertEquals(1, acc.exitCode());
        assertEquals(List.of("R-B", "R-C"), acc.missing());
        assertEquals("not recorded", acc.toJson().get(1).path("evidence").asText());
        assertFalse(acc.eligibility("flink", true, null).eligible());
        acc.errorUnrecorded("error: the requirements run did not complete");
        assertEquals(0, acc.exitCode());
        assertEquals(List.of("R-B", "R-C"), acc.with(Status.ERROR));
    }

    @Test void aRequirementRecordedTwiceOrNotInTheManifestIsRefused() {
        RequirementsAccount acc = new RequirementsAccount(IDS, null);
        acc.record("R-A", Status.MISS, "x");
        assertThrows(IllegalStateException.class, () -> acc.record("R-A", Status.PASS, "y"));
        assertThrows(IllegalStateException.class, () -> acc.record("R-Z", Status.PASS, "y"));
        assertThrows(IllegalArgumentException.class, () -> new RequirementsAccount(List.of("R-A", "R-A"), null));
        assertThrows(IllegalArgumentException.class, () -> new RequirementsAccount(List.of(), null));
    }

    // --- the gate report ------------------------------------------------------------------------------------------------------

    static ObjectNode report(String layout, long exit, Map<String, Boolean> results) {
        ObjectNode r = Json.object().put("kind", "gate").put("scope", "replay").put("engine", "flink").put("layout", layout).put("manifestSha256", "m")
            .put("engineArtifactSha256", "a").putNull("error");
        Json.putSafe(r, "exit", exit);
        Json.putSafe(r, "assertionsRun", 3);
        Json.putSafe(r, "assertionsExpected", 3);
        r.putArray("failed");
        r.putArray("notRun");
        ArrayNode a = r.putArray("results");
        results.forEach((id, ok) -> a.addObject().put("id", id).put("ok", ok).put("evidence", id + " evidence"));
        return r;
    }

    /** The current manifest of these tests: three ids. */
    static final List<String> MANIFEST = List.of("I-26", "I-7", "I-21");

    static ReplayRequirements.GateReport read(ObjectNode r) { return ReplayRequirements.GateReport.read(Canonical.bytes(r), "flink", "m", MANIFEST, "a"); }

    @Test void aRequirementFromTheGateIsPassMissOrErrorByTheGatesResult() {
        Map<String, Boolean> res = new LinkedHashMap<>();
        res.put("I-26", true); res.put("I-7", false); res.put("I-21", true);
        ReplayRequirements.GateReport g = read(report("aggregate", 1, res));
        assertEquals(Status.PASS, g.from(List.of("I-26")).status());
        assertTrue(g.from(List.of("I-26")).evidence().contains("PASS I-26 I-26 evidence"));
        assertEquals(Status.MISS, g.from(List.of("I-7")).status());
        assertEquals(Status.ERROR, g.from(List.of("I-21", "I-22")).status(), "I-22 is not in the report");
        assertTrue(g.from(List.of("I-21", "I-22")).evidence().contains("not in the gate report: [I-22]"));
    }

    @Test void onlyAFullOrAggregateGateReportThatExitedZeroIsTheSemanticGate() {
        Map<String, Boolean> res = Map.of("I-26", true, "I-7", true, "I-21", true);
        assertTrue(read(report("aggregate", 0, res)).passed());
        assertTrue(read(report("full", 0, res)).passed());
        ReplayRequirements.GateReport shard = read(report("shard", 0, res));
        assertFalse(shard.passed());
        assertTrue(shard.why().contains("one shard is not the semantic gate"), shard.why());
        assertEquals(Status.PASS, shard.from(List.of("I-26")).status(), "a shard's own results still decide its requirements");
        assertFalse(read(report("aggregate", 1, res)).passed());
    }

    @Test void theCountersAreNotTrustedEveryManifestIdNeedsExactlyOneSuccessfulResult() {
        // a truncated report whose counters claim every assertion: two of the three ids only
        ObjectNode truncated = report("aggregate", 0, Map.of("I-26", true, "I-7", true));
        ReplayRequirements.GateReport t = read(truncated);
        assertFalse(t.passed());
        assertTrue(t.why().contains("1 manifest ids without a result [I-21]"), t.why());
        // a result recorded twice: the report decides nothing
        ObjectNode twice = report("aggregate", 0, Map.of("I-26", true, "I-7", true, "I-21", true));
        ((ArrayNode) twice.get("results")).addObject().put("id", "I-26").put("ok", false).put("evidence", "again");
        ReplayRequirements.GateReport d = read(twice);
        assertFalse(d.passed());
        assertEquals(Status.ERROR, d.from(List.of("I-26")).status());
        assertTrue(d.why().contains("results recorded twice [I-26]"), d.why());
        // an id that is not in the current manifest
        ObjectNode extra = report("aggregate", 0, Map.of("I-26", true, "I-7", true, "I-21", true, "I-99", true));
        assertFalse(read(extra).passed());
        // a failed result
        assertFalse(read(report("aggregate", 0, Map.of("I-26", true, "I-7", false, "I-21", true))).passed());
        // an unknown engine artifact on the engine path decides nothing
        ReplayRequirements.GateReport noArtifact = ReplayRequirements.GateReport.read(Canonical.bytes(report("aggregate", 0, Map.of("I-26", true, "I-7", true, "I-21", true))),
            "flink", "m", MANIFEST, null);
        assertFalse(noArtifact.passed());
        assertEquals(Status.ERROR, noArtifact.from(List.of("I-26")).status());
        // an unreadable report
        ReplayRequirements.GateReport none = ReplayRequirements.GateReport.unreadable("no readable gate report at x");
        assertFalse(none.passed());
        assertEquals(Status.ERROR, none.from(List.of("I-7")).status());
        assertTrue(none.from(List.of("I-7")).evidence().contains("no readable gate report at x"));
    }

    @Test void aGateReportOfAnotherEngineManifestOrArtifactDecidesNothing() {
        Map<String, Boolean> res = Map.of("I-26", true);
        for (ObjectNode r : List.of(report("aggregate", 0, res).put("manifestSha256", "other"), report("aggregate", 0, res).put("engine", "kafka-streams"),
                report("aggregate", 0, res).put("engineArtifactSha256", "b"), report("aggregate", 0, res).put("kind", "negative-control"))) {
            ReplayRequirements.GateReport g = read(r);
            assertFalse(g.passed(), r.toString());
            assertEquals(Status.ERROR, g.from(List.of("I-26")).status(), r.toString());
        }
        ReplayRequirements.GateReport junk = ReplayRequirements.GateReport.read("not json".getBytes(StandardCharsets.UTF_8), "flink", "m", MANIFEST, "a");
        assertFalse(junk.passed());
        assertEquals(Status.ERROR, junk.from(List.of("I-26")).status());
    }

    @Test void theCommittedRequirementsManifestListsExactlyWhatTheCommandEvaluates() {
        ReplayRequirements.Manifest m = ReplayRequirements.Manifest.load(ROOT.resolve("gate/flink-requirements.json"), "flink");
        assertEquals(new TreeSet<>(ReplayRequirements.IDS), new TreeSet<>(m.ids()));
        assertEquals(new TreeSet<>(ReplayRequirements.RUNS), new TreeSet<>(m.runs()));
        for (String r : m.runs()) assertTrue(Files.isRegularFile(ROOT.resolve("engine-fixtures/scenarios/" + (r.startsWith("dedicated/") ? r : "shared/" + r) + ".json")), r);
        assertThrows(IllegalArgumentException.class, () -> ReplayRequirements.Manifest.load(ROOT.resolve("gate/flink-requirements.json"), "kafka-streams"));
    }

    // --- common cuts -------------------------------------------------------------------------------------------------------------

    static Map<Long, String> boundaries(String... keys) {
        Map<Long, String> m = new LinkedHashMap<>();
        for (int k = 0; k < keys.length; k++) m.put((long) k + 1, keys[k]);
        return m;
    }

    @Test void commonCutsAreTheCutsOfEqualReplayInputBoundaryThatOccurOnceInEachRun() {
        // the baseline at batch interval 60 s, and at x2: cut k of x2 delivers what baseline cut 2k delivers
        Map<Long, String> base = boundaries("a", "b", "c", "d", "e", "f"), x2 = boundaries("b", "d", "f");
        assertEquals(List.of("1/2", "2/4", "3/6"), ReplayRequirements.commonCuts(x2, base, Long.MIN_VALUE).stream().map(x -> x[0] + "/" + x[1]).toList());
        // a starved run: the withheld records come with the late burst, so the boundaries in between match nothing; a boundary repeated in
        // one run (a batch that delivered nothing) is not a common cut
        Map<Long, String> starved = boundaries("a", "b", "s1", "s2", "s2", "e", "f");
        assertEquals(List.of("6/5", "7/6"), ReplayRequirements.commonCuts(starved, base, 6).stream().map(x -> x[0] + "/" + x[1]).toList());
        assertEquals(List.of("1/1", "2/2", "6/5", "7/6"), ReplayRequirements.commonCuts(starved, base, Long.MIN_VALUE).stream().map(x -> x[0] + "/" + x[1]).toList());
    }

    @Test void theReplayInputBoundaryOfARecordingIsItsDeliveredMultisetOfRecords() {
        Path dir = tmp.resolve("rec");
        yggdrasil.forseti.cli.recording.ScenarioBuilder.build(ROOT.resolve("engine-fixtures/scenarios/dedicated/baseline.json"), ROOT.resolve("fixtures/live/registry"), dir);
        Path x2 = tmp.resolve("rec-x2");
        yggdrasil.forseti.cli.recording.ScenarioBuilder.build(ROOT.resolve("engine-fixtures/scenarios/dedicated/baseline-batch-x2.json"), ROOT.resolve("fixtures/live/registry"), x2);
        Map<Long, String> b = ReplayRequirements.boundaries(dir), x = ReplayRequirements.boundaries(x2);
        List<long[]> c = ReplayRequirements.commonCuts(x, b, Long.MIN_VALUE);
        assertTrue(c.size() >= x.size() - 1, c.size() + " common cuts of " + x.size());
        // the last cuts deliver the whole recording
        assertEquals(new ArrayList<>(b.values()).get(b.size() - 1), new ArrayList<>(x.values()).get(x.size() - 1));
    }

    // --- the crash path -------------------------------------------------------------------------------------------------------------

    @Test void aCrashOfTheRunsExitsOneWithEveryRequirementRecordedAndAMissingGateReportIsRecordedNotAUsageError() throws Exception {
        ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
        Map<String, String> flags = new HashMap<>(Map.of("engine", "flink", "gate-report", tmp.resolve("no-such-report.json").toString(), "out", tmp.resolve("req").toString(),
            "engine-artifact", "a"));
        int rc = ReplayRequirements.main(ROOT, "flink", flags, "forseti requirements --engine flink", f -> { throw new IllegalStateException("no Docker"); },
            new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8));
        assertEquals(1, rc, "spec 705: the requirements were not run");
        JsonNode rep = Json.read(tmp.resolve("req/" + ReplayRequirements.FILE));
        assertEquals(1, rep.path("exit").asInt());
        assertEquals(ReplayRequirements.IDS.size(), rep.path("requirementsRecorded").asInt(), "every requirement recorded");
        for (JsonNode r : rep.path("results")) assertEquals("error", r.path("status").asText(), r.toString());
        assertTrue(rep.path("results").get(1).path("evidence").asText().contains("no readable gate report"), rep.path("results").get(1).toString());
        assertTrue(rep.path("error").asText().contains("no Docker"));
        assertEquals(ReplayRequirements.RUNS.size(), rep.path("runsNotExecuted").size());
        assertFalse(rep.path("eligibility").path("eligible").asBoolean(true));
        String printed = o.toString(StandardCharsets.UTF_8);
        assertTrue(printed.contains("requirements recorded 13 / expected 13 (exit 1: the runs crashed)"), printed);
        assertTrue(printed.contains("engine eligibility: flink ineligible"), printed);
    }

    @Test void theExitIsZeroOnlyWhenEveryRequirementWasRunAndRecorded() {
        RequirementsAccount acc = new RequirementsAccount(IDS, null);
        for (String id : IDS) acc.record(id, Status.ERROR, "evaluated: no comparable cut");
        assertEquals(0, ReplayRequirements.exitCode(acc, null, List.of()), "an evaluated error is a recorded result");
        assertEquals(1, ReplayRequirements.exitCode(acc, "crashed", List.of()));
        assertEquals(1, ReplayRequirements.exitCode(acc, null, List.of("baseline: crashed: broker")));
    }

    // --- three-run determinism ----------------------------------------------------------------------------------------------------

    static ObjectNode comparison(String fp3) {
        ObjectNode n = Json.object().put("name", "baseline").put("label", "baseline");
        n.putArray("deadlines").add(25);
        ArrayNode cuts = n.putArray("cuts");
        long[] w = {10, 20, 30};
        String[] fps = {"a:1", "b:2", fp3};
        for (int k = 0; k < 3; k++) {
            ObjectNode c = Json.putSafe(Json.object(), "cutId", k + 1);
            ObjectNode s = c.putObject("perStation").putObject("s0");
            Json.putSafe(s, "W1", w[k]);
            Json.putSafe(s, "admissionW", w[k] - 10);
            c.putArray("open");
            c.putObject("fingerprints").put("forseti.work-records/0", fps[k]);
            cuts.add(c);
        }
        return n;
    }

    Path execution(String name, ObjectNode comparison, long exit) throws Exception {
        Path d = tmp.resolve(name);
        Files.createDirectories(d.resolve("runs/baseline"));
        ObjectNode r = Json.object().put("kind", "gate").put("layout", "shard").put("shard", "inj-baseline").put("engine", "flink").put("engineArtifactSha256", "a")
            .put("manifestSha256", "m");
        Json.putSafe(r, "exit", exit);
        Json.write(d.resolve("report.json"), r);
        Json.write(d.resolve("runs/baseline/" + ReplayGate.COMPARISON), comparison);
        return d;
    }

    @Test void threeExecutionsWithEqualBodiesAtEveryComparableCutHold() throws Exception {
        ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
        int rc = Determinism.main(List.of(execution("1", comparison("c:3"), 0), execution("2", comparison("c:3"), 0), execution("3", comparison("c:3"), 0)), tmp.resolve("out"),
            new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8));
        assertEquals(0, rc, o + "" + e);
        JsonNode rep = Json.read(tmp.resolve("out/" + Determinism.FILE));
        assertTrue(rep.path("held").asBoolean());
        assertEquals(9, rep.path("comparedCuts").asLong(), "3 cuts for each of the pairs 1-2, 1-3, 2-3");
    }

    @Test void aBodyDifferenceAtAComparableCutFailsAndAFailedExecutionNeverHolds() throws Exception {
        ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
        PrintStream po = new PrintStream(o, true, StandardCharsets.UTF_8), pe = new PrintStream(e, true, StandardCharsets.UTF_8);
        assertEquals(1, Determinism.main(List.of(execution("1", comparison("c:3"), 0), execution("2", comparison("c:3"), 0), execution("3", comparison("d:3"), 0)), tmp.resolve("o1"), po, pe));
        assertTrue(o.toString(StandardCharsets.UTF_8).contains("cut 3 forseti.work-records/0 c:3 vs d:3"), o.toString(StandardCharsets.UTF_8));
        assertEquals(1, Determinism.main(List.of(execution("4", comparison("c:3"), 0), execution("5", comparison("c:3"), 1), execution("6", comparison("c:3"), 0)), tmp.resolve("o2"), po, pe));
        assertEquals(2, Determinism.main(List.of(tmp.resolve("4"), tmp.resolve("5")), tmp.resolve("o3"), po, pe));
        // executions of another engine artifact are not three executions of one gate run
        Path other = execution("7", comparison("c:3"), 0);
        ObjectNode rep = (ObjectNode) Json.read(other.resolve("report.json"));
        Json.write(other.resolve("report.json"), rep.put("engineArtifactSha256", "b"));
        assertEquals(1, Determinism.main(List.of(execution("8", comparison("c:3"), 0), execution("9", comparison("c:3"), 0), other), tmp.resolve("o4"), po, pe));
        assertTrue(o.toString(StandardCharsets.UTF_8).contains("the executions differ in engineArtifactSha256"), o.toString(StandardCharsets.UTF_8));
    }

    @Test void aRestartRunsExecutionsMayCompleteDifferentCutsAndEveryPairIsCompared() {
        // execution 2 skipped cut 2 (a restart skip: no cut entry at all); 1-2 and 2-3 compare at cuts 1 and 3, 1-3 at all three
        ObjectNode a = comparison("c:3"), b = comparison("c:3");
        ((ArrayNode) b.get("cuts")).remove(1);
        Determinism.Pair p = Determinism.compare(a, b);
        assertEquals(2, p.compared());
        assertEquals(List.of(), p.problems());
        assertEquals(3, Determinism.compare(a, comparison("c:3")).compared());
    }

    @Test void aCutNotComparableIsNotCompared() {
        // execution 2 reports another W1 at cut 3, with the deadline 25 between the two admissionW values: cut 3 is not compared
        ObjectNode a = comparison("c:3"), b = comparison("d:3");
        ObjectNode s = (ObjectNode) b.path("cuts").get(2).path("perStation").path("s0");
        Json.putSafe(s, "admissionW", 26);
        Json.putSafe((ObjectNode) a.path("cuts").get(2).path("perStation").path("s0"), "admissionW", 24);
        Determinism.Pair p = Determinism.compare(a, b);
        assertEquals(2, p.compared());
        assertEquals(1, p.skipped());
        assertEquals(List.of(), p.problems());
    }
}
