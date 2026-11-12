package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.core.Json;

/** The shard plan and the aggregate run's checks of the shard outputs, on the committed plan and hand-made shard outputs. */
class ShardsTest {
    static final Path ROOT = CoreGateTest.ROOT;
    @TempDir Path tmp;

    static Shards.Plan plan() { return Shards.load(ROOT); }
    static List<String> manifest() { return CoreGate.manifestIds(Json.read(ROOT.resolve(CoreGate.MANIFEST))); }
    static Set<String> aggregatable() { return new HashSet<>(Injections.AGGREGATE); }

    @Test void theCommittedPlanPartitionsTheManifestAndMakesEveryScenario() {
        Shards.Plan p = plan();
        assertEquals(List.of(), Shards.validate(p, manifest(), aggregatable()));
        assertEquals(new TreeSet<>(Injections.AGGREGATE), new TreeSet<>(p.aggregate()), "the aggregate run evaluates exactly the assertions over every run");
        Set<String> horizonShards = new TreeSet<>();
        for (Shards.Shard s : p.shards()) for (String sc : s.scenarios()) if (Scenarios.HORIZON.contains(sc)) horizonShards.add(s.name() + ":" + sc);
        assertEquals(Scenarios.HORIZON.size(), horizonShards.size(), "each horizon recording is made by one shard: " + horizonShards);
        for (Shards.Shard s : p.shards())
            assertTrue(s.scenarios().stream().filter(Scenarios.HORIZON::contains).count() <= 1, s.name() + " makes more than one horizon recording");
    }

    @Test void aPlanThatLeavesOutOrRepeatsAnythingIsRefused() {
        Shards.Plan p = plan();
        Shards.Shard first = p.shards().get(0);
        List<String> lessAssertions = new ArrayList<>(first.assertions());
        String dropped = lessAssertions.remove(0);
        List<Shards.Shard> l = new ArrayList<>(p.shards());
        l.set(0, new Shards.Shard(first.name(), first.scenarios(), first.shuffled(), lessAssertions));
        assertTrue(Shards.validate(new Shards.Plan(l, p.aggregate(), "x"), manifest(), aggregatable()).contains(dropped + " is in no shard and not in aggregate"));

        List<String> twice = new ArrayList<>(p.aggregate());
        twice.add(first.assertions().get(0));
        assertTrue(Shards.validate(new Shards.Plan(p.shards(), twice, "x"), manifest(), aggregatable()).stream().anyMatch(x -> x.contains("is in two places")));

        List<String> noBaseline = new ArrayList<>(first.scenarios());
        noBaseline.remove("stage2-early");
        l = new ArrayList<>(p.shards());
        l.set(0, new Shards.Shard(first.name(), noBaseline, first.shuffled(), first.assertions()));
        assertTrue(Shards.validate(new Shards.Plan(l, p.aggregate(), "x"), manifest(), aggregatable()).contains("no shard makes scenario stage2-early"));

        l = new ArrayList<>(p.shards());
        l.set(0, new Shards.Shard(first.name(), first.scenarios(), List.of("baseline"), first.assertions()));
        assertTrue(Shards.validate(new Shards.Plan(l, p.aggregate(), "x"), manifest(), aggregatable()).contains("no shard makes the shuffled runs of stage2"));

        l = new ArrayList<>(p.shards());
        List<String> foreign = new ArrayList<>(p.aggregate());
        foreign.add("I-1");
        List<String> noI1 = new ArrayList<>(first.assertions());
        noI1.remove("I-1");
        l.set(0, new Shards.Shard(first.name(), first.scenarios(), first.shuffled(), noI1));
        assertTrue(Shards.validate(new Shards.Plan(l, foreign, "x"), manifest(), aggregatable()).contains("I-1 is not an assertion the aggregate run can evaluate"));
    }

    // --- the aggregate run's reading of the shard outputs ------------------------------------------------------------------------

    static long lockCuts(String scenario) {
        return Json.read(ScenarioBuilder.lockOf(ROOT.resolve("fixtures").resolve("scenarios").resolve(scenario + ".json"))).path("counts").path("cuts").asLong();
    }

    static ObjectNode expect() {
        ObjectNode e = Json.object();
        e.put("manifestSha256", "m");
        Json.putSafe(e, "seed", 1L);
        e.putObject("pins").put("python", "3.13");
        e.putObject("fixtureDigests").put("scenarios", "d");
        return e;
    }

    /** A complete, passing set of shard outputs under {@code dir}; {@code edit(shard, {report, summaries})} changes one before it is written. */
    Path outputs(BiConsumer<String, ObjectNode[]> edit) throws Exception {
        Path dir = Files.createDirectories(tmp.resolve("shards-" + UUID.randomUUID()));
        Shards.Plan p = plan();
        for (Shards.Shard s : p.shards()) {
            ObjectNode report = expect().deepCopy(), summaries = Json.object();
            report.put("shard", s.name()).put("shardPlanSha256", p.sha256()).putNull("error");
            ArrayNode results = report.putArray("results");
            for (String id : s.assertions()) results.addObject().put("id", id).put("ok", true).put("evidence", "stand-in");
            ArrayNode runs = summaries.putArray("runs"), shuffled = summaries.putArray("shuffled");
            for (String sc : s.scenarios()) {
                ObjectNode r = runs.addObject();
                r.put("name", sc).putNull("failure").put("ok", true).put("summary", sc + ": stand-in").put("equal", true);
                Json.putSafe(r, "cutLogCuts", lockCuts(sc));
                Json.putSafe(r, "cutsCompared", lockCuts(sc));
                r.putObject("findings");
                r.putObject("rows").put("forseti.work-records", 3);
            }
            for (String sc : s.shuffled()) for (long seed : Scenarios.SHUFFLE_SEEDS) { ObjectNode x = shuffled.addObject().put("name", sc); Json.putSafe(x, "seed", seed); x.putNull("failure"); }
            ObjectNode[] both = {report, summaries};
            edit.accept(s.name(), both);
            Path d = Files.createDirectories(dir.resolve(s.name()));
            Json.write(d.resolve("report.json"), both[0]);
            Json.write(d.resolve(Shards.SUMMARIES), both[1]);
        }
        return dir;
    }

    record Read(Summaries summaries, List<Shards.Recorded> recorded, List<String> problems) {}
    Read read(Path dir) {
        List<Shards.Recorded> rec = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        Summaries s = Shards.read(ROOT, dir, plan(), expect(), rec, problems);
        return new Read(s, rec, problems);
    }

    @Test void completeShardOutputsMergeWithoutProblems() throws Exception {
        Read r = read(outputs((s, b) -> {}));
        assertEquals(List.of(), r.problems());
        assertEquals(List.of(), r.summaries().missing);
        assertEquals(Scenarios.all().size(), r.summaries().runs.size());
        assertEquals(Scenarios.SHUFFLED.size() * Scenarios.SHUFFLE_SEEDS.size(), r.summaries().shuffled.size());
        int ids = 0;
        for (Shards.Shard s : plan().shards()) ids += s.assertions().size();
        assertEquals(ids, r.recorded().size());
        assertEquals(manifest().size() - plan().aggregate().size(), r.recorded().size());
    }

    @Test void aMissingShardIsAProblemAndItsScenariosAreMissing() throws Exception {
        Path dir = outputs((s, b) -> {});
        CoreGate.delete(dir.resolve("horizon-planned-late"));
        Read r = read(dir);
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("shard horizon-planned-late: ")), r.problems().toString());
        assertTrue(r.summaries().missing.contains("horizon-planned-late not run: no shard summarised it"), r.summaries().missing.toString());
        assertTrue(r.problems().contains("horizon-planned-late not run: no shard summarised it"), "coverage names the missing scenario");
    }

    @Test void aShardRunAgainstOtherInputsIsAProblem() throws Exception {
        Read r = read(outputs((s, b) -> { if (s.equals("plc-horizon-no-end")) { b[0].putObject("pins").put("python", "3.12"); b[0].put("shardPlanSha256", "other"); } }));
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("shard plc-horizon-no-end: pins ")), r.problems().toString());
        assertTrue(r.problems().contains("shard plc-horizon-no-end: ran against another " + Shards.PLAN), r.problems().toString());
    }

    @Test void anUnrecordedOrForeignAssertionIsAProblem() throws Exception {
        Read r = read(outputs((s, b) -> {
            if (s.equals("horizon-reopened")) ((ArrayNode) b[0].get("results")).removeAll();
            if (s.equals("horizon-correction-beyond")) ((ArrayNode) b[0].get("results")).addObject().put("id", "I-1").put("ok", true).put("evidence", "");
        }));
        assertTrue(r.problems().contains("shard horizon-reopened did not record I-10c"), r.problems().toString());
        assertTrue(r.problems().contains("shard horizon-correction-beyond recorded I-1, which the plan does not give it"), r.problems().toString());
        assertTrue(r.recorded().stream().noneMatch(x -> x.shard().equals("horizon-correction-beyond") && x.id().equals("I-1")));
    }

    @Test void aScenarioNotComparedAtEveryPlannedCutIsAProblem() throws Exception {
        Read r = read(outputs((s, b) -> {
            if (!s.equals("horizon-no-departure")) return;
            for (JsonNode x : b[1].get("runs")) if (x.path("name").asText().equals("horizon-no-departure")) Json.putSafe((ObjectNode) x, "cutsCompared", lockCuts("horizon-no-departure") - 1);
        }));
        long n = lockCuts("horizon-no-departure");
        assertTrue(r.problems().contains("horizon-no-departure: " + (n - 1) + " cuts compared with the oracle and " + n + " completed, its lock plans " + n), r.problems().toString());
    }

    @Test void aScenarioThatDidNotHoldOrShardsThatDisagreeAreProblems() throws Exception {
        Read r = read(outputs((s, b) -> {
            for (JsonNode x : b[1].get("runs")) {
                if (s.equals("horizon-reopened") && x.path("name").asText().equals("corrections-10c")) ((ObjectNode) x.get("rows")).put("forseti.work-records", 4);
                if (s.equals("hand-made") && x.path("name").asText().equals("overlap")) ((ObjectNode) x).put("ok", false).put("summary", "overlap: 2 differences");
            }
        }));
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("corrections-10c: two shards disagree on rows")), r.problems().toString());
        assertTrue(r.problems().contains("overlap did not hold: overlap: 2 differences"), r.problems().toString());
    }

    @Test void aMissingShuffledRunIsAProblem() throws Exception {
        Read r = read(outputs((s, b) -> { if (s.equals("hand-made")) ((ArrayNode) b[1].get("shuffled")).remove(0); }));
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("shard hand-made has no summary of shuffled run baseline@")), r.problems().toString());
        assertTrue(r.summaries().missing.stream().anyMatch(x -> x.startsWith("baseline seed ")), r.summaries().missing.toString());
    }
}
