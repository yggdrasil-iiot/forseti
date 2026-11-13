package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.cli.harness.Controls;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The eight engine-independent negative controls (spec Negative controls, plan Task 22) through {@link CoreGate} in-process: each one
 *  exits 1 with its intended ids in {@code failed}, for the intended reason and not by a crash; the committed control file
 *  ({@code gate/core-negative-controls.json}) names for every control the shards that own or feed its intended ids, and a file that
 *  does not is refused. The in-process runs make only the scenarios the intended assertions read ({@code -Dforseti.gate.only}); the
 *  shard runs CI makes are the jar gate's. Runs the harness and the oracle ({@code -Dforseti.python}, default {@code python}). */
class NegativeControlsTest {
    static final Path ROOT = CoreGateTest.ROOT, MANIFEST = CoreGateTest.MANIFEST;
    static final String PYTHON = CoreGateTest.PYTHON;

    @TempDir Path tmp;

    static PrintStream sink(ByteArrayOutputStream b) { return new PrintStream(b, true, StandardCharsets.UTF_8); }
    static List<String> list(JsonNode a) { List<String> l = new ArrayList<>(); a.forEach(x -> l.add(x.asText())); return l; }
    static List<String> manifestIds() { return CoreGate.manifestIds(Json.read(MANIFEST)); }

    // --- the committed control file ----------------------------------------------------------------------------------------------

    @Test void theCommittedControlsAreTheEightOnTheShardsThatOwnOrFeedTheirIntendedIds() {
        ControlPlan file = ControlPlan.load(ROOT);
        Shards.Plan plan = Shards.load(ROOT);
        assertEquals(List.of(), file.validate(plan, manifestIds()));
        assertEquals(Controls.NAMES, file.names());
        Map<String, List<String>> shards = new LinkedHashMap<>();
        for (ControlPlan.Control c : file.controls) shards.put(c.name(), c.shards());
        assertEquals(List.of("hand-made"), shards.get("processing-time"));
        assertEquals(List.of("horizon-planned-late"), shards.get("drop-late"));
        assertEquals(List.of("hand-made"), shards.get("no-cascade"));
        assertEquals(List.of("hand-made", "horizon-no-departure"), shards.get("lenient-oracle"));
        assertEquals(plan.names(), shards.get("wrong-rules-pin"), "every shard owns an injection");
        ControlPlan.Control pin = file.control("wrong-rules-pin");
        assertEquals(manifestIds().stream().filter(id -> id.startsWith("I-")).toList(), pin.intended(), "the pins cross-check inside every I-*");
        ControlPlan.Control late = file.control("drop-late");
        assertEquals(List.of("J-DISPOSITION-COVERAGE"), late.aggregateIds(plan));
        assertEquals(List.of(), late.owned(plan, "horizon-planned-late", false), "the feeding shard owns none: the aggregate control run proves it");
        assertEquals(List.of("J-DISPOSITION-COVERAGE"), late.owned(plan, null, true));
        ControlPlan.Control cascade = file.control("no-cascade");
        assertEquals(List.of("I-18"), cascade.owned(plan, "hand-made", false));
        assertEquals(List.of("J-BASIS-CURRENT"), cascade.owned(plan, null, true));
        assertEquals(List.of("I-6"), file.control("lenient-oracle").owned(plan, "horizon-no-departure", false));
        assertTrue(ControlPlan.feeds(plan, "horizon-planned-late", "J-DISPOSITION-COVERAGE"));
        assertFalse(ControlPlan.feeds(plan, "hand-made", "J-AFTER-HORIZON-CUTS"));
        assertFalse(ControlPlan.feeds(plan, "hand-made", "I-1"), "a shard id is owned, never fed");
    }

    /** A copy of the committed control file with one control's entry changed, validated against the committed plan and manifest. */
    List<String> problems(String name, java.util.function.Consumer<ObjectNode> change) throws Exception {
        Path root = Files.createDirectories(tmp.resolve("root-" + UUID.randomUUID()).resolve("gate")).getParent();
        ObjectNode f = (ObjectNode) Json.read(ROOT.resolve(CoreGate.CONTROLS));
        for (JsonNode c : f.get("controls")) if (c.path("name").asText().equals(name)) change.accept((ObjectNode) c);
        Json.write(root.resolve(CoreGate.CONTROLS), f);
        return ControlPlan.load(root).validate(Shards.load(ROOT), manifestIds());
    }

    static ArrayNode strings(ObjectNode c, String field, String... values) { ArrayNode a = c.putArray(field); for (String v : values) a.add(v); return a; }

    @Test void aControlWithoutAnOwningShardOrWithAShardThatOwnsNoneOfItsIdsIsRefused() throws Exception {
        assertTrue(problems("first-wins", c -> strings(c, "shards")).contains("control first-wins runs on no shard"));
        assertTrue(problems("first-wins", c -> strings(c, "shards")).contains("control first-wins: no listed shard owns I-4"));
        List<String> useless = problems("first-wins", c -> strings(c, "shards", "hand-made", "horizon-reopened"));
        assertEquals(List.of("control first-wins: shard horizon-reopened owns and feeds none of its intended ids [I-4]"), useless);
        assertEquals(List.of("control lenient-oracle: no listed shard owns I-6"), problems("lenient-oracle", c -> strings(c, "shards", "hand-made")));
        assertEquals(List.of("control drop-late: shard no-such-shard is not in " + Shards.PLAN, "control drop-late: no listed shard feeds its aggregate id J-DISPOSITION-COVERAGE"),
            problems("drop-late", c -> strings(c, "shards", "no-such-shard")));
        assertTrue(problems("no-cascade", c -> strings(c, "intended", "I-18")).get(0).startsWith("control no-cascade: intended [I-18], the implementation intends [I-18, J-BASIS-CURRENT]"));
        assertTrue(problems("first-wins", c -> c.put("name", "first-loses")).contains("control first-loses is not implemented by this gate"));
        assertTrue(problems("first-wins", c -> c.put("name", "first-loses")).contains("control first-wins is implemented but not listed"));
    }

    @Test void anUnlistedNameOrAShardTheControlDoesNotRunOnIsAUsageError() throws Exception {
        PrintStream none = sink(new ByteArrayOutputStream());
        Path out = tmp.resolve("usage");
        for (List<String> extra : List.of(List.of("--negative-control", "no-such-control"), List.of("--negative-control", "first-wins", "--shard", "horizon-reopened"),
                List.of("--negative-control", "first-wins", "--aggregate", Files.createDirectories(tmp.resolve("shards")).toString()))) {
            List<String> args = new ArrayList<>(List.of("gate", "--engine", "core", "--out", out.toString()));
            args.addAll(extra);
            ByteArrayOutputStream e = new ByteArrayOutputStream();
            assertEquals(2, Main.run(args.toArray(String[]::new), none, sink(e)), extra + ": " + e.toString(StandardCharsets.UTF_8));
            assertFalse(Files.exists(out.resolve("report.json")), "a usage error runs nothing");
        }
        // the harness takes the seven core controls only: lenient-oracle is the oracle's
        ByteArrayOutputStream e = new ByteArrayOutputStream();
        assertEquals(2, Main.run(new String[] {"run", "--engine", "core", "--recording", "r", "--registry", "g", "--state", "s", "--generation", "1", "--out", "o",
            "--negative-control", "lenient-oracle"}, none, sink(e)));
        assertTrue(e.toString(StandardCharsets.UTF_8).contains("not a control the harness applies"), e.toString(StandardCharsets.UTF_8));
    }

    // --- each control in-process -------------------------------------------------------------------------------------------------

    /** A full-layout control run of {@code control} on a manifest copy with only {@code ids}, making only {@code scenarios}. */
    JsonNode controlRun(String control, List<String> scenarios, List<String> ids) throws Exception {
        ObjectNode m = (ObjectNode) Json.read(MANIFEST);
        ArrayNode a = (ArrayNode) m.get("assertions");
        for (int i = a.size() - 1; i >= 0; i--) if (!ids.contains(a.get(i).path("id").asText())) a.remove(i);
        Path manifest = tmp.resolve("manifest-" + control + ".json"), out = tmp.resolve("out-" + control);
        Json.write(manifest, m);
        List<CoreGate.Check> checks = CoreGate.registered().stream().filter(c -> ids.contains(c.id())).toList();
        String before = System.getProperty("forseti.gate.only");
        System.setProperty("forseti.gate.only", String.join(",", scenarios));
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit;
        try {
            exit = CoreGate.run(new CoreGate.Options(ROOT, manifest, out, PYTHON, control, ids, CoreGate.DEFAULT_SEED, "forseti gate --engine core (NegativeControlsTest)"),
                checks, sink(so), sink(se));
        } finally {
            if (before == null) System.clearProperty("forseti.gate.only"); else System.setProperty("forseti.gate.only", before);
        }
        JsonNode r = Json.read(out.resolve("report.json"));
        String shown = se.toString(StandardCharsets.UTF_8) + so.toString(StandardCharsets.UTF_8);
        assertEquals(1, exit, shown);
        assertEquals("negative-control", r.path("kind").asText());
        assertEquals(control, r.path("negativeControl").asText());
        assertEquals(ids, list(r.path("intendedFailures")));
        assertTrue(new HashSet<>(list(r.path("failed"))).containsAll(ids), "intended " + ids + " not all in failed " + r.path("failed") + "\n" + shown);
        assertTrue(r.path("error").isNull(), r.path("error").asText());
        assertTrue(r.path("control").path("held").asBoolean(), r.path("control").toString());
        assertFalse(r.path("generations").isEmpty());
        for (JsonNode g : r.path("generations")) assertEquals("isolated", g.path("state").asText(), "a control run never registers a shared generation: " + g);
        for (JsonNode s : Json.read(out.resolve(Shards.SUMMARIES)).path("runs")) {
            assertTrue(s.path("failure").isNull(), "a crash never stands for the intended failure: " + s.path("failure"));
            assertEquals(s.path("plannedCutIds"), s.path("comparedCutIds"), "every cut is compared: " + s.path("name"));
        }
        return r;
    }

    static String evidence(JsonNode report, String id) {
        for (JsonNode x : report.path("results")) if (x.path("id").asText().equals(id)) return x.path("evidence").asText();
        throw new IllegalStateException("no result " + id);
    }

    @Test void processingTimeFailsI1() throws Exception {
        JsonNode r = controlRun("processing-time", List.of("held-back-10min", "baseline"), List.of("I-1"));
        assertTrue(evidence(r, "I-1").contains("differences"), evidence(r, "I-1"));
    }

    @Test void noCascadeFailsI18AndLeavesAStaleBasis() throws Exception {
        JsonNode r = controlRun("no-cascade", List.of("cascade"), List.of("I-18"));
        JsonNode summary = null;
        for (JsonNode s : Json.read(tmp.resolve("out-no-cascade").resolve(Shards.SUMMARIES)).path("runs")) if (s.path("name").asText().equals("cascade")) summary = s;
        assertNotNull(summary);
        assertTrue(Summaries.findingCount(summary, "J-BASIS-CURRENT") > 0, "the cascade run feeds J-BASIS-CURRENT a stale basis: " + summary.path("findings"));
        assertTrue(evidence(r, "I-18").contains("I-18") || !evidence(r, "I-18").isBlank());
    }

    @Test void lenientOracleFailsI2() throws Exception {
        JsonNode r = controlRun("lenient-oracle", List.of("corrections"), List.of("I-2"));
        assertTrue(evidence(r, "I-2").contains("differences"), "the oracle, not the core, changed: " + evidence(r, "I-2"));
    }

    @Test void firstWinsFailsI4() throws Exception {
        JsonNode r = controlRun("first-wins", List.of("overlap"), List.of("I-4"));
        assertTrue(evidence(r, "I-4").contains("expected retracted"), evidence(r, "I-4"));
    }

    @Test void stationGranularityFailsI16() throws Exception {
        controlRun("station-granularity", List.of("stage2"), List.of("I-16"));
    }

    @Test void ignoreSessionGapFailsI8() throws Exception {
        controlRun("ignore-session-gap", List.of("session"), List.of("I-8"));
    }

    /** wrong-rules-pin fails on the pins cross-check alone: every body still matches the oracle. */
    @Test void wrongRulesPinFailsThePinsCrossCheck() throws Exception {
        JsonNode r = controlRun("wrong-rules-pin", List.of("overlap"), List.of("I-4"));
        String ev = evidence(r, "I-4");
        assertTrue(ev.contains("cuts equal the oracle"), ev);
        assertTrue(ev.contains("PINS"), ev);
    }

    // --- the aggregate ids: an aggregate control run over the control's shard outputs ---------------------------------------------

    /** The aggregate control run of {@code control} over shard bundles whose {@code shard} is rewritten as that control's shard run, its
     *  run {@code scenario}'s summary given {@code finding} ({@code null}: none) and {@code failure}. */
    JsonNode aggregateControl(String control, String shard, String scenario, String finding, String failure) throws Exception {
        ObjectNode common = Json.object();
        common.put("manifestSha256", Canonical.digestOf(Files.readAllBytes(MANIFEST)));
        Json.putSafe(common, "seed", CoreGate.DEFAULT_SEED);
        Path base = Files.createDirectories(tmp.resolve("ctx-" + UUID.randomUUID()));
        common.set("pins", CoreGate.pins(new CoreGate.Context(ROOT, Files.createDirectories(base.resolve("tmp")), Files.createDirectories(base.resolve("logs")), PYTHON)));
        common.set("fixtureDigests", CoreGate.fixtureDigests(ROOT));
        GateBundles.Aggregate a = GateBundles.aggregate(tmp.resolve("agg-" + control + "-" + UUID.randomUUID()), GateBundles.Options.DEFAULT, common);
        GateBundles.Bundle b = a.shard(shard);
        ObjectNode summaries = b.summariesJson();
        for (JsonNode s : summaries.get("runs")) {
            if (!s.path("name").asText().equals(scenario)) continue;
            ObjectNode x = (ObjectNode) s;
            if (finding != null) {
                ((ObjectNode) x.get("findings")).putObject(finding).put("count", 1).put("describe", "cut 3 ot.messages/0/7: no disposition");
                x.put("ok", false);   // the integrity finding makes the run not hold; its comparison with the oracle stays equal
            }
            if (failure != null) x.put("failure", failure).put("ok", false);
        }
        ObjectNode report = b.reportJson();
        report.put("kind", "negative-control").put("negativeControl", control);
        ObjectNode verdict = report.putObject("control").put("name", control).put("held", true);
        verdict.putArray("problems");
        FixtureBundles.writeBound(b.dir(), report, summaries);
        Path out = tmp.resolve("agg-out-" + UUID.randomUUID());
        Shards.Plan plan = Shards.load(ROOT);
        List<String> owned = ControlPlan.load(ROOT).control(control).owned(plan, null, true);
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        CoreGate.run(new CoreGate.Options(ROOT, MANIFEST, out, PYTHON, control, owned, CoreGate.DEFAULT_SEED, "test", null, a.shards()), CoreGate.registered(), sink(so), sink(se));
        return Json.read(out.resolve("report.json"));
    }

    /** drop-late on horizon-planned-late: the dropped input has no disposition, which J-DISPOSITION-COVERAGE catches although every
     *  remaining body matches the oracle (the summary stays {@code equal}); the aggregate control run reads only that shard. */
    @Test void dropLateFailsDispositionCoverageEvenThoughTheRemainingBodiesMatch() throws Exception {
        JsonNode r = aggregateControl("drop-late", "horizon-planned-late", "horizon-planned-late", "J-DISPOSITION-COVERAGE", null);
        assertEquals(1, r.path("exit").asInt(), r.toString());
        assertEquals("aggregate", r.path("layout").asText());
        assertEquals("negative-control", r.path("kind").asText());
        assertEquals(List.of("J-DISPOSITION-COVERAGE"), list(r.path("intendedFailures")));
        assertTrue(list(r.path("failed")).contains("J-DISPOSITION-COVERAGE"), r.path("failed").toString());
        assertTrue(r.path("error").isNull(), r.path("error").asText());
        assertTrue(r.path("control").path("held").asBoolean(), r.path("control").toString());
        assertEquals(List.of("horizon-planned-late"), r.path("shards").findValuesAsText("name"), "only the control's shard is read");
        assertTrue(evidence(r, "J-DISPOSITION-COVERAGE").contains("no disposition"), evidence(r, "J-DISPOSITION-COVERAGE"));
    }

    @Test void noCascadeFailsBasisCurrentOverItsShard() throws Exception {
        JsonNode r = aggregateControl("no-cascade", "hand-made", "cascade", "J-BASIS-CURRENT", null);
        assertTrue(list(r.path("failed")).contains("J-BASIS-CURRENT"), r.path("failed").toString());
        assertTrue(r.path("control").path("held").asBoolean(), r.path("control").toString());
    }

    /** A crash of a run the aggregate id rests on never counts as its intended failure. */
    @Test void aCrashedRunNeverCountsAsTheIntendedFailure() throws Exception {
        JsonNode r = aggregateControl("drop-late", "horizon-planned-late", "horizon-planned-late", null, "crashed: java.lang.IllegalStateException: boom");
        assertEquals(1, r.path("exit").asInt());
        assertTrue(list(r.path("failed")).contains("J-DISPOSITION-COVERAGE"), "it fails, but on the crash");
        assertFalse(r.path("control").path("held").asBoolean(), r.path("control").toString());
        assertTrue(r.path("control").path("problems").toString().contains("boom"), r.path("control").toString());
    }

    /** Without the mutation the aggregate id holds, so the control run does not hold: a control proves nothing it did not break. */
    @Test void anAggregateIdTheControlDidNotBreakDoesNotHold() throws Exception {
        JsonNode r = aggregateControl("drop-late", "horizon-planned-late", "horizon-planned-late", null, null);
        assertFalse(list(r.path("failed")).contains("J-DISPOSITION-COVERAGE"));
        assertFalse(r.path("control").path("held").asBoolean());
        assertTrue(r.path("control").path("problems").toString().contains("J-DISPOSITION-COVERAGE held"), r.path("control").toString());
    }
}
