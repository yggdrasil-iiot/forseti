package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

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
        l.set(0, new Shards.Shard(first.name(), first.scenarios(), first.shuffled(), lessAssertions, first.generationRuns()));
        assertTrue(Shards.validate(new Shards.Plan(l, p.aggregate(), "x"), manifest(), aggregatable()).contains(dropped + " is in no shard and not in aggregate"));

        List<String> twice = new ArrayList<>(p.aggregate());
        twice.add(first.assertions().get(0));
        assertTrue(Shards.validate(new Shards.Plan(p.shards(), twice, "x"), manifest(), aggregatable()).stream().anyMatch(x -> x.contains("is in two places")));

        List<String> noBaseline = new ArrayList<>(first.scenarios());
        noBaseline.remove("stage2-early");
        l = new ArrayList<>(p.shards());
        l.set(0, new Shards.Shard(first.name(), noBaseline, first.shuffled(), first.assertions(), first.generationRuns()));
        assertTrue(Shards.validate(new Shards.Plan(l, p.aggregate(), "x"), manifest(), aggregatable()).contains("no shard makes scenario stage2-early"));

        l = new ArrayList<>(p.shards());
        l.set(0, new Shards.Shard(first.name(), first.scenarios(), List.of("baseline"), first.assertions(), first.generationRuns()));
        assertTrue(Shards.validate(new Shards.Plan(l, p.aggregate(), "x"), manifest(), aggregatable()).contains("no shard makes the shuffled runs of stage2"));

        l = new ArrayList<>(p.shards());
        List<String> foreign = new ArrayList<>(p.aggregate());
        foreign.add("I-1");
        List<String> noI1 = new ArrayList<>(first.assertions());
        noI1.remove("I-1");
        l.set(0, new Shards.Shard(first.name(), first.scenarios(), first.shuffled(), noI1, first.generationRuns()));
        assertTrue(Shards.validate(new Shards.Plan(l, foreign, "x"), manifest(), aggregatable()).contains("I-1 is not an assertion the aggregate run can evaluate"));
    }

    // --- the aggregate run's reading of the shard outputs ------------------------------------------------------------------------

    static JsonNode lock(String scenario) {
        return Json.read(ScenarioBuilder.lockOf(ROOT.resolve("fixtures").resolve("scenarios").resolve(scenario + ".json")));
    }
    static long lockCuts(String scenario) { return lock(scenario).path("counts").path("cuts").asLong(); }

    /** Cut ids 1..n, the form every committed recording plans (its manifest's batches). */
    static void ids(ObjectNode run, String field, long n) {
        ArrayNode a = run.putArray(field);
        for (long k = 1; k <= n; k++) a.add(k);
    }

    static ObjectNode run(ObjectNode summaries, String name) {
        for (JsonNode x : summaries.get("runs")) if (x.path("name").asText().equals(name)) return (ObjectNode) x;
        throw new IllegalStateException(name);
    }

    static final String ARTIFACT = "a".repeat(64), JCS = "io.github.erdtman:java-json-canonicalization:1.1", ROUTING = "r".repeat(64);
    static final Path REGISTRY = ROOT.resolve("fixtures/registry"), KEYS = ROOT.resolve("fixtures/trusted-keys.jsonl");

    static ObjectNode expect() {
        ObjectNode e = Json.object();
        e.put("manifestSha256", "m");
        Json.putSafe(e, "seed", 1L);
        ObjectNode pins = e.putObject("pins");
        pins.put("python", "3.13");
        pins.putObject("jcs").put("java", JCS);
        pins.putObject("forseti").put("artifactSha256", ARTIFACT);
        e.putObject("fixtureDigests").put("scenarios", "d");
        return e;
    }

    /** {@code generation new} in-process on {@code state} (stream line2, no recording) at the head's signedAt + 60 s: what it printed. */
    static JsonNode register(Path state) {
        ByteArrayOutputStream so = new ByteArrayOutputStream();
        Path m = REGISTRY.resolve("mappings");
        int exit = GenerationCli.run(new String[] {"new", "--state", state.toString(), "--mappings", m.toString(), "--trusted-keys", KEYS.toString(),
            "--stream", "line2", "--rule", "line2-attribution@1.0.0"}, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(new ByteArrayOutputStream()),
            Clock.fixed(Instant.ofEpochMilli(GateSigner.now(m, "line2")), ZoneOffset.UTC));
        assertEquals(0, exit, so.toString(StandardCharsets.UTF_8));
        return Json.read(so.toString(StandardCharsets.UTF_8).strip().getBytes(StandardCharsets.UTF_8));
    }

    /** The pinned pins and releaseSha256 of the line2 registration, the pin set every stand-in run record carries. */
    static JsonNode registeredRow(Path state, long n) {
        for (String l : readText(state.resolve("generations.jsonl")).split("\n")) {
            JsonNode r = Json.read(l.getBytes(StandardCharsets.UTF_8));
            if (r.path("value").path("generation").asLong() == n) return r.path("value");
        }
        throw new IllegalStateException("no row " + n);
    }
    static String readText(Path f) { try { return Files.readString(f, StandardCharsets.UTF_8); } catch (java.io.IOException e) { throw new IllegalStateException(e); } }

    /** One generation entry and its registration. */
    record Gen(ObjectNode entry, long generation, String digest) {}

    /** Writes the run record and cut log of a stand-in run of {@code scenario} on {@code g} under {@code bundle/runs/<name>} and returns its
     *  run entry: one cut per planned cut id, one {@code ot.messages} offset per cut. */
    static ObjectNode standInRun(Path bundle, String shard, String store, Gen g, String scenario, long seed, boolean shuffle, String name, JsonNode row) throws Exception {
        long n = lockCuts(scenario);
        String runId = "run-" + shard + "-" + store.replace(':', '-').replace('#', '-') + "-" + name;
        ObjectNode cutLog = Json.object();
        Json.putSafe(cutLog, "generation", g.generation()).put("routingDigest", ROUTING);
        ArrayNode cuts = cutLog.putArray("cuts");
        for (long k = 1; k <= n; k++) {
            ObjectNode c = Json.putSafe(Json.object(), "cutId", k);
            c.putObject("perStation");
            c.putObject("offsetRanges").putObject("ot.messages").putArray("0").add(k - 1).add(k);
            c.putArray("incidentOffsets");
            cuts.add(c);
        }
        ObjectNode rr = Json.object().put("runId", runId).put("engine", "core-harness").put("engineVersion", "0.1.0").put("artifactSha256", ARTIFACT);
        Json.putSafe(rr, "generation", g.generation());
        rr.put("registeredRowSha256", g.digest()).put("scope", "line2");
        Json.putSafe(rr, "seed", seed);
        rr.put("shuffle", shuffle).putNull("replay").putNull("originalRecordingManifestSha256");
        ObjectNode pins = row.path("releaseEvidence").path("pins").deepCopy();
        pins.putObject("job").put("runId", runId).put("routingDigest", ROUTING);
        rr.set("pins", pins);
        ObjectNode fd = rr.putObject("fixtureDigests");
        fd.put("recordingManifestSha256", lock(scenario).path("files").path("manifest.json").asText());
        fd.putObject("recordingFiles");
        fd.put("releaseSha256", row.path("releaseEvidence").path("releaseSha256").asText()).put("rulesDigest", row.path("rulesDigest").asText());
        rr.putObject("jcs").put("java", JCS);
        rr.putObject("subscribed").putArray("ot.messages").add(0);
        rr.putObject("offsetRanges").putObject("ot.messages").putArray("0").add(0).add(n);
        Json.putSafe(rr, "cuts", n);
        rr.put("command", "stand-in");
        Path runs = Files.createDirectories(bundle.resolve("runs"));
        byte[] rb = Json.pretty(rr), cb = Json.pretty(cutLog);
        Files.write(runs.resolve(name + ".run.json"), rb);
        Files.write(runs.resolve(name + ".cuts.json"), cb);
        ObjectNode e = Json.object().put("shard", shard).put("store", store).put("scenario", scenario).put("variant", "online");
        Json.putSafe(e, "seed", seed);
        e.put("shuffle", shuffle).put("runId", runId);
        Json.putSafe(e, "generation", g.generation());
        e.put("registeredRowSha256", g.digest()).put("state", g.entry().path("state").asText());
        e.put("runRecord", "runs/" + name + ".run.json").put("runRecordSha256", Canonical.digestOf(rb));
        e.put("cutLog", "runs/" + name + ".cuts.json").put("cutLogSha256", Canonical.digestOf(cb));
        e.put("recordingManifestSha256", lock(scenario).path("files").path("manifest.json").asText()).putNull("originalRecordingManifestSha256");
        e.set("offsetRanges", rr.get("offsetRanges").deepCopy());
        e.putNull("journals").putNull("journalsIndexSha256");
        return e;
    }

    static ObjectNode genEntry(String shard, String store, long n, String digest, String rulesDigest, String state, String scenario, String variant, String assertion) {
        ObjectNode g = Json.object().put("shard", shard).put("store", store);
        Json.putSafe(g, "generation", n);
        return g.put("scope", "line2").put("rulesDigest", rulesDigest).put("registeredRowSha256", digest).put("state", state).put("scenario", scenario)
            .put("variant", variant).put("assertion", assertion);
    }

    static ObjectNode summary(String sc) {
        ObjectNode r = Json.object();
        r.put("name", sc).putNull("failure").put("ok", true).put("summary", sc + ": stand-in");
        r.put("recordingManifestSha256", lock(sc).path("files").path("manifest.json").asText());
        ids(r, "plannedCutIds", lockCuts(sc));
        Json.putSafe(r, "cutLogCuts", lockCuts(sc));
        ids(r, "cutLogIds", lockCuts(sc));
        Json.putSafe(r, "cutsCompared", lockCuts(sc));
        ids(r, "comparedCutIds", lockCuts(sc));
        r.put("equal", true);
        r.putObject("findings");
        r.putObject("rows").put("forseti.work-records", 3);
        return r;
    }

    /** Edits one shard's report and summaries (before they are written; its run records and cut logs are written already under
     *  {@code bundle}). */
    interface Edit { void edit(String shard, ObjectNode[] both, Path bundle) throws Exception; }

    Path outputs(BiConsumer<String, ObjectNode[]> edit) throws Exception { return outputs((s, b, d) -> edit.accept(s, b)); }

    /** A complete, passing set of shard outputs under {@code dir}: every shard a bundle whose pipeline store registered one generation per
     *  scenario (shard {@code hand-made} for real, through {@code generation new} into its {@code state/}, {@code shared}; the others
     *  {@code isolated}), with one kept run per scenario and per shuffled seed and the summaries of decision 12, and its planned generation
     *  runs ({@link FixtureBundles#generationRuns}: the replay with its kept journals, the rule change in its assertion store). {@code edit} changes one
     *  shard before its report and summaries are written; the report's {@code summariesSha256} is the written file's unless the edit set it. */
    Path outputs(Edit edit) throws Exception {
        Path dir = Files.createDirectories(tmp.resolve("shards-" + UUID.randomUUID()));
        Path probe = Files.createDirectories(tmp.resolve("probe-" + UUID.randomUUID()));
        JsonNode printed = register(probe), row = registeredRow(probe, 1);
        String rules = printed.path("rulesDigest").asText();
        Shards.Plan p = plan();
        for (Shards.Shard s : p.shards()) {
            Path d = Files.createDirectories(dir.resolve(s.name()));
            boolean shared = s.name().equals("hand-made");
            ObjectNode report = expect().deepCopy(), summaries = Json.object();
            report.put("kind", "gate").put("layout", "shard").putNull("negativeControl");
            report.put("shard", s.name()).put("shardPlanSha256", p.sha256()).putNull("error");
            Json.putSafe(report, "exit", 0L);
            Json.putSafe(report, "assertionsExpected", (long) s.assertions().size());
            Json.putSafe(report, "assertionsRun", (long) s.assertions().size());
            report.putArray("failed");
            report.putArray("notRun");
            ArrayNode results = report.putArray("results");
            for (String id : s.assertions()) results.addObject().put("id", id).put("ok", true).put("evidence", "stand-in").put("shard", s.name());
            report.putArray("shards");
            ArrayNode gens = report.putArray("generations"), runEntries = report.putArray("runs");
            ArrayNode runs = summaries.putArray("runs"), shuffled = summaries.putArray("shuffled");
            Map<String, Gen> table = new HashMap<>();
            long n = 0;
            for (String sc : Scenarios.all()) {
                if (!s.scenarios().contains(sc)) continue;
                n++;
                String digest;
                if (shared) {
                    JsonNode g = register(d.resolve("state"));
                    assertEquals(n, g.path("generation").asLong());
                    digest = g.path("registeredRowSha256").asText();
                } else digest = Canonical.digest(Json.object().put("stand-in", s.name() + "/" + sc));
                ObjectNode e = genEntry(s.name(), "pipeline", n, digest, rules, shared ? "shared" : "isolated", sc, "online", null);
                gens.add(e);
                table.put(sc, new Gen(e, n, digest));
            }
            for (String sc : s.scenarios()) {
                runEntries.add(standInRun(d, s.name(), "pipeline", table.get(sc), sc, CoreGate.DEFAULT_SEED, false, sc, row));
                runs.add(summary(sc));
            }
            for (String sc : s.shuffled()) for (long seed : Scenarios.SHUFFLE_SEEDS) {
                runEntries.add(standInRun(d, s.name(), "pipeline", table.get(sc), sc, seed, true, sc + ".shuffle-" + seed, row));
                ObjectNode x = Json.object().put("name", sc);
                Json.putSafe(x, "seed", seed);
                x.setAll((ObjectNode) summary(sc).without(List.of("name")));
                x.put("summary", sc + " seed " + seed + ": stand-in");
                ObjectNode ordered = Json.object().put("name", sc);
                Json.putSafe(ordered, "seed", seed);
                ordered.setAll((ObjectNode) x.without(List.of("name", "seed")));
                ordered.putObject("ms");
                shuffled.add(ordered);
            }
            FixtureBundles.generationRuns(ROOT, d, s.name(), shared, s.generationRuns(), expect(), gens, runEntries, summaries);
            ObjectNode[] both = {report, summaries};
            edit.edit(s.name(), both, d);
            byte[] sb = Json.pretty(both[1]);
            Files.write(d.resolve(Shards.SUMMARIES), sb);
            if (!both[0].has("summariesSha256")) both[0].put("summariesSha256", Canonical.digestOf(sb));
            Json.write(d.resolve("report.json"), both[0]);
        }
        return dir;
    }

    record Read(Summaries summaries, List<Shards.Recorded> recorded, List<String> problems, ObjectNode merged) {}
    Read read(Path dir) {
        List<Shards.Recorded> rec = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        ObjectNode merged = Json.object();
        Summaries s = Shards.read(ROOT, dir, plan(), expect(), rec, problems, merged);
        return new Read(s, rec, problems, merged);
    }

    static ObjectNode entry(ObjectNode report, String store, String scenario, boolean shuffle) {
        for (JsonNode r : report.get("runs"))
            if (r.path("store").asText().equals(store) && r.path("scenario").asText().equals(scenario) && r.path("shuffle").asBoolean() == shuffle) return (ObjectNode) r;
        throw new IllegalStateException(store + " " + scenario);
    }

    /** Changes a kept JSON file of a run entry ({@code runRecord} or {@code cutLog}) and its digest in the entry. */
    static void rewrite(Path bundle, ObjectNode entry, String field, Consumer<ObjectNode> change) throws Exception {
        Path f = bundle.resolve(entry.path(field).asText());
        ObjectNode n = (ObjectNode) Json.read(f);
        change.accept(n);
        byte[] b = Json.pretty(n);
        Files.write(f, b);
        entry.put(field + "Sha256", Canonical.digestOf(b));
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

    /** Under a core-input key (CI sets FORSETI_INPUT_KEY) every shard carries the aggregate run's key, and no run counts from unpacked classes. */
    @Test void aKeyedAggregateRefusesAnotherKeyAndUnpackedClasses() throws Exception {
        String key = "k".repeat(64);
        ObjectNode keyed = expect();
        ((ObjectNode) keyed.path("pins").path("forseti")).put("inputKey", key);
        Path same = outputs((s, b) -> ((ObjectNode) b[0].path("pins").path("forseti")).put("inputKey", key));
        assertEquals(List.of(), readWith(same, keyed), "the same key on every shard");
        Path other = outputs((s, b) -> ((ObjectNode) b[0].path("pins").path("forseti")).put("inputKey", s.equals("hand-made") ? "o".repeat(64) : key));
        List<String> p = readWith(other, keyed);
        assertTrue(p.stream().anyMatch(x -> x.startsWith("shard hand-made: pins ")), p.toString());
        Path unpacked = outputs((s, b) -> {
            ObjectNode f = (ObjectNode) b[0].path("pins").path("forseti");
            f.put("inputKey", key);
            if (s.equals("hand-made")) f.put("artifactSha256", Shards.UNPACKED);
        });
        p = readWith(unpacked, keyed);
        assertTrue(p.contains("shard hand-made: ran from unpacked classes under core-input key " + key), p.toString());
        ObjectNode keyedUnpacked = keyed.deepCopy();
        ((ObjectNode) keyedUnpacked.path("pins").path("forseti")).put("artifactSha256", Shards.UNPACKED);
        p = readWith(same, keyedUnpacked);
        assertTrue(p.contains("the aggregate run itself ran from unpacked classes under core-input key " + key), p.toString());
        assertEquals(List.of(), read(outputs((s, b) -> {})).problems(), "without a key (local runs) nothing changes");
    }

    List<String> readWith(Path dir, ObjectNode expect) {
        List<String> problems = new ArrayList<>();
        Shards.read(ROOT, dir, plan(), expect, new ArrayList<>(), problems, Json.object());
        return problems;
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
        assertTrue(r.problems().contains("shard horizon-no-departure: horizon-no-departure: " + (n - 1) + " cuts compared with the oracle and " + n + " completed, its lock plans " + n), r.problems().toString());
    }

    @Test void aScenarioThatDidNotHoldOrShardsThatDisagreeAreProblems() throws Exception {
        Read r = read(outputs((s, b) -> {
            for (JsonNode x : b[1].get("runs")) {
                if (s.equals("horizon-reopened") && x.path("name").asText().equals("corrections-10c")) ((ObjectNode) x.get("rows")).put("forseti.work-records", 4);
                if (s.equals("hand-made") && x.path("name").asText().equals("overlap")) ((ObjectNode) x).put("ok", false).put("summary", "overlap: 2 differences");
            }
        }));
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("corrections-10c: two shards disagree on rows")), r.problems().toString());
        assertTrue(r.problems().contains("shard hand-made: overlap did not hold: overlap: 2 differences"), r.problems().toString());
    }

    /** A shard whose report says it failed, or whose accounting its results do not back, never passes the aggregate on its results. */
    @Test void aShardReportThatDisagreesWithItsResultsIsAProblem() throws Exception {
        Read r = read(outputs((s, b) -> {
            if (s.equals("horizon-reopened")) Json.putSafe(b[0], "exit", 1L);
            if (s.equals("horizon-no-departure")) ((ArrayNode) b[0].get("failed")).add("I-6");
            if (s.equals("plc-horizon-no-end")) ((ArrayNode) b[0].get("notRun")).add("I-11e");
            if (s.equals("horizon-correction-beyond")) Json.putSafe(b[0], "assertionsRun", 0L);
            if (s.equals("hand-made")) { ArrayNode res = (ArrayNode) b[0].get("results"); res.add(res.get(0).deepCopy()); }
        }));
        assertTrue(r.problems().contains("shard horizon-reopened: exited 1"), r.problems().toString());
        assertTrue(r.problems().contains("shard horizon-no-departure: its report lists failed [I-6], its results fail []"), r.problems().toString());
        assertTrue(r.problems().contains("shard plc-horizon-no-end: not run [I-11e]"), r.problems().toString());
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("shard horizon-correction-beyond: assertions run 0 / expected 1")), r.problems().toString());
        String first = plan().shard("hand-made").assertions().get(0);
        assertTrue(r.problems().contains("shard hand-made recorded " + first + " twice"), r.problems().toString());
    }

    /** The same number of cuts is not enough: a skipped cut made up by a repeated one, or a run on another recording, is a problem. */
    @Test void cutsAreCheckedByIdentityAndTheRecordingByItsLockedDigest() throws Exception {
        long n = lockCuts("horizon-reopened");
        Read r = read(outputs((s, b) -> {
            if (s.equals("horizon-reopened")) {
                ArrayNode c = run(b[1], "horizon-reopened").putArray("comparedCutIds");
                for (long k = 1; k < n; k++) c.add(k);
                c.add(n - 1);
            }
            if (s.equals("hand-made")) run(b[1], "overlap").put("recordingManifestSha256", "another");
        }));
        assertTrue(r.problems().contains("shard horizon-reopened: horizon-reopened: the gate compared cuts (at position " + (n - 1) + ": " + (n - 1) + " vs " + n + "; " + n + " vs " + n + " cuts) other than the recording plans"), r.problems().toString());
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("shard hand-made: overlap: its run read recording manifest another, its lock pins ")), r.problems().toString());
        assertTrue(r.problems().stream().noneMatch(x -> x.startsWith("shard horizon-reopened: horizon-reopened: ") && x.contains("cuts compared with the oracle and")), "the counts alone agree");
    }

    @Test void aRunOrShuffledRunSummarisedTwiceOrAnUnplannedSeedIsAProblem() throws Exception {
        Read r = read(outputs((s, b) -> {
            if (!s.equals("hand-made")) return;
            ArrayNode runs = (ArrayNode) b[1].get("runs"), sh = (ArrayNode) b[1].get("shuffled");
            runs.add(runs.get(0).deepCopy());
            sh.add(sh.get(0).deepCopy());
            ObjectNode odd = sh.addObject().put("name", sh.get(0).path("name").asText());
            Json.putSafe(odd, "seed", 999L);
            odd.putNull("failure");
        }));
        String firstRun = plan().shard("hand-made").scenarios().get(0);
        assertTrue(r.problems().contains("shard hand-made summarised " + firstRun + " twice"), r.problems().toString());
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("shard hand-made summarised shuffled run ") && x.endsWith(" twice")), r.problems().toString());
        assertTrue(r.problems().stream().anyMatch(x -> x.contains("@999, which the plan does not give it")), r.problems().toString());
    }

    @Test void twoShardsThatMadeTheSameScenarioMustAgreeOnEverySemanticField() throws Exception {
        Read r = read(outputs((s, b) -> {
            if (s.equals("horizon-reopened")) {
                run(b[1], "corrections-10c").putObject("measures").put("longestLatenessMs", 7);
                run(b[1], "corrections-10c").putObject("ms").put("oracle", 1);
            }
        }));
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("corrections-10c: two shards disagree on measures")), r.problems().toString());
        assertTrue(r.problems().stream().noneMatch(x -> x.contains("disagree on ms")), "timings may differ");
    }

    @Test void aMissingShuffledRunIsAProblem() throws Exception {
        Read r = read(outputs((s, b) -> { if (s.equals("hand-made")) ((ArrayNode) b[1].get("shuffled")).remove(0); }));
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("shard hand-made has no summary of shuffled run baseline@")), r.problems().toString());
        assertTrue(r.summaries().missing.stream().anyMatch(x -> x.startsWith("baseline seed ")), r.summaries().missing.toString());
    }

    /** A shard that owns no assertion is refused by the plan (Amendment 2026-11-13, decision 7): no code path writes an exit-0 "0 / 0" report. */
    @Test void aShardWithoutAssertionsIsRefusedByThePlan() throws Exception {
        Path root = Files.createDirectories(tmp.resolve("root"));
        Files.createDirectories(root.resolve("gate"));
        ObjectNode planJson = Json.object();
        ArrayNode shards = planJson.putArray("shards");
        ObjectNode all = shards.addObject().put("name", "everything");
        Scenarios.all().forEach(all.putArray("scenarios")::add);
        Scenarios.SHUFFLED.forEach(all.putArray("shuffled")::add);
        ArrayNode ids = all.putArray("assertions");
        for (String id : manifest()) if (!Injections.AGGREGATE.contains(id)) ids.add(id);
        all.putArray("generationRuns");
        Scenarios.GENERATION_RUNS.forEach(g -> ((ArrayNode) all.get("generationRuns")).add(g.name()));
        ObjectNode none = shards.addObject().put("name", "nothing");
        none.putArray("scenarios");
        none.putArray("shuffled");
        none.putArray("assertions");
        none.putArray("generationRuns");
        Injections.AGGREGATE.forEach(planJson.putArray("aggregate")::add);
        Json.write(root.resolve(Shards.PLAN), planJson);
        assertTrue(Shards.validate(Shards.load(root), manifest(), aggregatable()).contains("shard nothing owns no assertion"), Shards.validate(Shards.load(root), manifest(), aggregatable()).toString());
        Path out = tmp.resolve("out-nothing");
        java.io.ByteArrayOutputStream so = new java.io.ByteArrayOutputStream(), se = new java.io.ByteArrayOutputStream();
        int exit = CoreGate.run(new CoreGate.Options(root, ROOT.resolve(CoreGate.MANIFEST), out, CoreGateTest.PYTHON, null, List.of(), CoreGate.DEFAULT_SEED, "test",
            "nothing", null), CoreGate.registered(), new java.io.PrintStream(so, true, java.nio.charset.StandardCharsets.UTF_8), new java.io.PrintStream(se, true, java.nio.charset.StandardCharsets.UTF_8));
        JsonNode report = Json.read(out.resolve("report.json"));
        assertEquals(1, exit, so.toString(java.nio.charset.StandardCharsets.UTF_8) + report);
        assertEquals(1, report.path("exit").asInt());
        assertTrue(report.path("error").asText().contains("shard nothing owns no assertion"), report.toString());
        assertTrue(se.toString(java.nio.charset.StandardCharsets.UTF_8).contains("shard nothing owns no assertion"), se.toString(java.nio.charset.StandardCharsets.UTF_8));
    }

    // --- decisions 10 and 11: the evidence of every shard --------------------------------------------------------------------

    @Test void theAggregateReportBindsEveryShardReportAndSummariesDigest() throws Exception {
        Path dir = outputs((s, b) -> {});
        Read r = read(dir);
        assertEquals(List.of(), r.problems());
        JsonNode shards = r.merged().get("shards");
        assertEquals(plan().names(), shards.findValuesAsText("name"));
        for (JsonNode e : shards) {
            Path d = dir.resolve(e.path("name").asText());
            assertEquals(List.of("name", "exit", "reportSha256", "summariesSha256"), fields(e));
            assertEquals(0, e.path("exit").asInt());
            assertEquals(Canonical.digestOf(Files.readAllBytes(d.resolve("report.json"))), e.path("reportSha256").asText());
            assertEquals(Canonical.digestOf(Files.readAllBytes(d.resolve(Shards.SUMMARIES))), e.path("summariesSha256").asText());
        }
        Read other = read(outputs((s, b) -> { if (s.equals("horizon-reopened")) b[0].put("summariesSha256", "0".repeat(64)); }));
        assertTrue(other.problems().stream().anyMatch(x -> x.startsWith("shard horizon-reopened: its report binds summaries \"" + "0".repeat(64) + "\", summaries.json is ")), other.problems().toString());
    }

    static List<String> fields(JsonNode n) { List<String> l = new ArrayList<>(); n.fieldNames().forEachRemaining(l::add); return l; }

    @Test void generationsAndRunsAreMergedUnderTheirShard() throws Exception {
        Read r = read(outputs((s, b) -> {}));
        assertEquals(List.of(), r.problems());
        List<String> order = new ArrayList<>();
        int gens = 0, runs = 0;
        for (Shards.Shard s : plan().shards()) {
            gens += s.scenarios().size();
            runs += s.scenarios().size() + s.shuffled().size() * Scenarios.SHUFFLE_SEEDS.size() + s.generationRuns().size();
            for (String g : s.generationRuns()) gens += GateEvidence.PIPELINE.equals(Scenarios.generationRun(g).store()) ? 1 : 2;   // a rule change registers two
        }
        assertEquals(gens, r.merged().get("generations").size());
        assertEquals(runs, r.merged().get("runs").size());
        for (JsonNode g : r.merged().get("generations")) { String sh = g.path("shard").asText(); if (order.isEmpty() || !order.get(order.size() - 1).equals(sh)) order.add(sh); }
        assertEquals(plan().names(), order, "concatenated in plan order, each under its shard");
        Read wrong = read(outputs((s, b) -> { if (s.equals("plc-horizon-no-end")) ((ObjectNode) b[0].get("runs").get(0)).put("shard", "hand-made"); }));
        assertTrue(wrong.problems().contains("shard plc-horizon-no-end: a run entry names shard \"hand-made\""), wrong.problems().toString());
    }

    @Test void aSharedGenerationWhoseRowRunRecordOrCutLogIsNotInTheArtifactIsAProblem() throws Exception {
        Path dir = outputs((s, b) -> {});
        Files.delete(dir.resolve("hand-made/state/generations.jsonl"));
        assertTrue(read(dir).problems().contains("shard hand-made: shared generations, but no state/generations.jsonl in the bundle"), read(dir).problems().toString());

        dir = outputs((s, b) -> {});
        Files.delete(dir.resolve("hand-made/runs/overlap.run.json"));
        Read r = read(dir);
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("shard hand-made: run ") && x.endsWith(": runs/overlap.run.json is not in the bundle")), r.problems().toString());

        dir = outputs((s, b) -> {});
        Files.delete(dir.resolve("hand-made/runs/baseline.shuffle-7.cuts.json"));
        r = read(dir);
        assertTrue(r.problems().stream().anyMatch(x -> x.endsWith(": runs/baseline.shuffle-7.cuts.json is not in the bundle")), r.problems().toString());

        // a stored row that is not the listed registration
        r = read(outputs((s, b) -> { if (s.equals("hand-made")) ((ObjectNode) b[0].get("generations").get(0)).put("rulesDigest", "f".repeat(64)); }));
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("shard hand-made: shared generation 1: rulesDigest " + "f".repeat(64))), r.problems().toString());
    }

    @Test void aControlOrAggregateReportAmongTheShardsIsAProblem() throws Exception {
        Read r = read(outputs((s, b) -> {
            if (s.equals("horizon-reopened")) b[0].put("kind", "negative-control");
            if (s.equals("plc-horizon-no-end")) b[0].put("layout", "aggregate");
        }));
        assertTrue(r.problems().contains("shard horizon-reopened: its report is of kind \"negative-control\", not gate"), r.problems().toString());
        assertTrue(r.problems().contains("shard plc-horizon-no-end: its report has layout \"aggregate\", not shard"), r.problems().toString());
    }

    @Test void twoShardsMayRegisterTheSameScenarioUnderDifferentNumbers() throws Exception {
        Read r = read(outputs((s, b) -> {}));
        assertEquals(List.of(), r.problems());
        Map<String, Long> numbers = new TreeMap<>();
        for (JsonNode g : r.merged().get("generations")) if (g.path("scenario").asText().equals("corrections-10c")) numbers.put(g.path("shard").asText(), g.path("generation").asLong());
        assertEquals(Set.of("hand-made", "horizon-reopened"), numbers.keySet());
        assertNotEquals(numbers.get("hand-made"), numbers.get("horizon-reopened"), numbers.toString());
    }

    @Test void identicalRegistrationsInTwoStoresOfOneShardStayDistinct() throws Exception {
        Read r = read(outputs((s, b, d) -> {
            if (!s.equals("horizon-reopened")) return;
            ArrayNode gens = (ArrayNode) b[0].get("generations"), runs = (ArrayNode) b[0].get("runs");
            JsonNode row = registeredRow(Files.createDirectories(tmp.resolve("row-" + UUID.randomUUID())), 0, true);
            String digest = gens.get(0).path("registeredRowSha256").asText(), rules = gens.get(0).path("rulesDigest").asText();
            assertEquals(1, gens.get(0).path("generation").asLong());
            for (String store : List.of("assertion:S-SMOKE-VISIT", "assertion:S-DETERMINISM")) {
                ObjectNode e = genEntry(s, store, 1, digest, rules, "isolated", "smoke", "online", store.substring("assertion:".length()));
                gens.add(e);
                runs.add(standInRun(d, s, store, new Gen(e, 1, digest), "smoke", CoreGate.DEFAULT_SEED, false, GateEvidence.safe(store) + ".smoke", row));
            }
        }));
        assertEquals(List.of(), r.problems(), "three stores of one shard each hold generation 1, two with byte-identical rows");
        Set<String> stores = new TreeSet<>();
        for (JsonNode x : r.merged().get("runs")) if (x.path("shard").asText().equals("horizon-reopened") && x.path("generation").asLong() == 1) stores.add(x.path("store").asText());
        assertEquals(Set.of("pipeline", "assertion:S-SMOKE-VISIT", "assertion:S-DETERMINISM"), stores, "each run resolves inside its own store");
    }

    /** The pinned row of a fresh line2 registration ({@code n} 0: register one first). */
    static JsonNode registeredRow(Path state, long n, boolean register) {
        if (register) register(state);
        return registeredRow(state, 1);
    }

    @Test void aRunThatResolvesOnlyByDigestInAnotherStoreIsAProblem() throws Exception {
        Read r = read(outputs((s, b, d) -> {
            if (!s.equals("horizon-reopened")) return;
            JsonNode g = b[0].get("generations").get(0);
            JsonNode row = registeredRow(Files.createDirectories(tmp.resolve("row-" + UUID.randomUUID())), 0, true);
            ObjectNode e = genEntry(s, "assertion:S-W1", 1, g.path("registeredRowSha256").asText(), g.path("rulesDigest").asText(), "isolated", "smoke", "online", "S-W1");
            ObjectNode run = standInRun(d, s, "assertion:S-W1", new Gen(e, 1, g.path("registeredRowSha256").asText()), "smoke", CoreGate.DEFAULT_SEED, false, "orphan.smoke", row);
            ((ArrayNode) b[0].get("runs")).add(run);   // its generation is listed only in the pipeline store
        }));
        assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("shard horizon-reopened: run ") && x.contains("names no generation of store assertion:S-W1")), r.problems().toString());
    }

    @Test void aRunWithoutASuccessfulSummaryOrWithOtherCutIdsIsAProblem() throws Exception {
        JsonNode row = registeredRow(Files.createDirectories(tmp.resolve("row-" + UUID.randomUUID())), 0, true);
        Read noSummary = read(outputs((s, b, d) -> {
            if (!s.equals("plc-horizon-no-end")) return;
            JsonNode g = b[0].get("generations").get(0);
            ((ArrayNode) b[0].get("runs")).add(standInRun(d, s, "pipeline", new Gen((ObjectNode) g, 1, g.path("registeredRowSha256").asText()), "baseline",
                CoreGate.DEFAULT_SEED, false, "baseline", row));
        }));
        assertTrue(noSummary.problems().stream().anyMatch(x -> x.startsWith("shard plc-horizon-no-end: run ") && x.endsWith(": no summary baseline in summaries.json")), noSummary.problems().toString());

        Read notOk = read(outputs((s, b) -> { if (s.equals("horizon-reopened")) run(b[1], "corrections-10c").put("ok", false).put("summary", "corrections-10c: 1 difference"); }));
        assertTrue(notOk.problems().contains("shard horizon-reopened: corrections-10c did not hold: corrections-10c: 1 difference"), notOk.problems().toString());

        long n = lockCuts("horizon-reopened");
        Read shortList = read(outputs((s, b) -> { if (s.equals("horizon-reopened")) ((ArrayNode) run(b[1], "horizon-reopened").get("comparedCutIds")).remove((int) n - 1); }));
        assertTrue(shortList.problems().stream().anyMatch(x -> x.startsWith("shard horizon-reopened: horizon-reopened: the gate compared cuts (at position " + (n - 1) + ": none vs " + n)), shortList.problems().toString());

        Read otherCut = read(outputs((s, b, d) -> {
            if (!s.equals("horizon-reopened")) return;
            rewrite(d, entry(b[0], "pipeline", "corrections-10c", false), "cutLog", c -> Json.putSafe((ObjectNode) c.get("cuts").get(0), "cutId", 99L));
        }));
        assertTrue(otherCut.problems().stream().anyMatch(x -> x.startsWith("shard horizon-reopened: run ") && x.contains(": its kept cut log holds cuts (at position 0: 99 vs 1")), otherCut.problems().toString());
    }

    @Test void aRunRecordWhoseSubscriptionsPinsRoutingOrArtifactDifferIsAProblem() throws Exception {
        Map<String, Consumer<ObjectNode>> faults = new LinkedHashMap<>();
        faults.put("subscribes", rr -> ((ArrayNode) rr.get("subscribed").get("ot.messages")).add(1));
        faults.put("pins digest", rr -> ((ObjectNode) rr.get("pins")).put("extra", 1));
        faults.put("routing", rr -> ((ObjectNode) rr.get("pins").get("job")).put("routingDigest", "x".repeat(64)));
        faults.put("artifactSha256", rr -> rr.put("artifactSha256", "b".repeat(64)));
        Map<String, String> want = Map.of("subscribes", ": run record subscribes [ot.messages/0, ot.messages/1], its cut log's offset ranges cover [ot.messages/0]",
            "pins digest", ": run record pins digest ", "routing", ": run record routing \"" + "x".repeat(64) + "\", its cut log's",
            "artifactSha256", ": run record artifactSha256 \"" + "b".repeat(64) + "\", the report's \"" + ARTIFACT + "\"");
        for (Map.Entry<String, Consumer<ObjectNode>> f : faults.entrySet()) {
            Read r = read(outputs((s, b, d) -> { if (s.equals("horizon-correction-beyond")) rewrite(d, entry(b[0], "pipeline", "horizon-correction-beyond", false), "runRecord", f.getValue()); }));
            assertTrue(r.problems().stream().anyMatch(x -> x.startsWith("shard horizon-correction-beyond: run ") && x.contains(want.get(f.getKey()))), f.getKey() + ": " + r.problems());
        }
    }

    static ObjectNode shuffledSummary(ObjectNode summaries, String name, long seed) {
        for (JsonNode x : summaries.get("shuffled")) if (x.path("name").asText().equals(name) && x.path("seed").asLong() == seed) return (ObjectNode) x;
        throw new IllegalStateException(name + "@" + seed);
    }

    @Test void aShuffledSummaryWithoutItsOwnComparedCutIdsIsAProblem() throws Exception {
        Read old = read(outputs((s, b) -> {
            if (!s.equals("hand-made")) return;
            ObjectNode x = shuffledSummary(b[1], "baseline", 7);
            x.retain("name", "seed", "failure", "findings", "rows");   // the five-field summary of PR 4
        }));
        assertTrue(old.problems().contains("shard hand-made: baseline seed 7: no comparedCutIds of its own oracle comparison"), old.problems().toString());
        assertTrue(old.problems().stream().anyMatch(x -> x.startsWith("shard hand-made: baseline seed 7 did not hold")), old.problems().toString());

        Read absent = read(outputs((s, b) -> { if (s.equals("hand-made")) shuffledSummary(b[1], "stage2", 7).remove("comparedCutIds"); }));
        assertTrue(absent.problems().contains("shard hand-made: stage2 seed 7: no comparedCutIds of its own oracle comparison"), absent.problems().toString());

        long n = lockCuts("baseline");
        Read shortList = read(outputs((s, b) -> { if (s.equals("hand-made")) ((ArrayNode) shuffledSummary(b[1], "baseline", 20261110L).get("comparedCutIds")).remove((int) n - 1); }));
        assertTrue(shortList.problems().stream().anyMatch(x -> x.startsWith("shard hand-made: baseline seed 20261110: the gate compared cuts (at position " + (n - 1) + ": none vs " + n)), shortList.problems().toString());

        Read otherRecording = read(outputs((s, b) -> { if (s.equals("hand-made")) shuffledSummary(b[1], "stage2", 20261110L).put("recordingManifestSha256", "another"); }));
        assertTrue(otherRecording.problems().stream().anyMatch(x -> x.startsWith("shard hand-made: stage2 seed 20261110: its run read recording manifest another, its lock pins ")), otherRecording.problems().toString());
    }

    @Test void aShuffledSummaryWhoseComparisonFailedIsAProblem() throws Exception {
        Read unequal = read(outputs((s, b) -> { if (s.equals("hand-made")) shuffledSummary(b[1], "baseline", 7).put("equal", false); }));
        assertTrue(unequal.problems().contains("shard hand-made: baseline seed 7: its comparison with the oracle is not equal (equal false)"), unequal.problems().toString());
        Read failed = read(outputs((s, b) -> { if (s.equals("hand-made")) shuffledSummary(b[1], "stage2", 7).put("failure", "crashed: oracle exit 1"); }));
        assertTrue(failed.problems().contains("shard hand-made: stage2 seed 7: crashed: oracle exit 1"), failed.problems().toString());
    }

    // --- generation runs (plan Step 21.4) ----------------------------------------------------------------------------------------

    static final String REPLAY = "horizon-planned-late@replay", PLANNED_LATE = "horizon-planned-late";

    static ObjectNode generationSummary(ObjectNode summaries, String name) { return (ObjectNode) summaries.get("generationRuns").get(name); }

    @Test void aPlannedGenerationRunMissingFromItsShardIsAProblem() throws Exception {
        Read r = read(outputs((s, b) -> { if (s.equals(PLANNED_LATE)) ((ObjectNode) b[1].get("generationRuns")).remove(REPLAY); }));
        assertTrue(r.problems().contains("shard horizon-planned-late has no summary of generation run " + REPLAY), r.problems().toString());
        assertTrue(r.summaries().missing.contains(REPLAY + " not run: no shard summarised it"), r.summaries().missing.toString());
        Read array = read(outputs((s, b) -> { if (s.equals(PLANNED_LATE)) b[1].putArray("generationRuns").add(Json.object().put("name", REPLAY)); }));
        assertTrue(array.problems().contains("shard horizon-planned-late: summaries.json has no generationRuns object keyed by name"), array.problems().toString());
        // the plan: a generation run no shard makes, or two shards make, is refused
        Shards.Plan p = plan();
        List<Shards.Shard> none = new ArrayList<>(), two = new ArrayList<>();
        for (Shards.Shard s : p.shards()) {
            none.add(s.name().equals(PLANNED_LATE) ? new Shards.Shard(s.name(), s.scenarios(), s.shuffled(), s.assertions(), List.of()) : s);
            List<String> gr = new ArrayList<>(s.generationRuns());
            if (s.name().equals("hand-made")) gr.add(REPLAY);
            two.add(new Shards.Shard(s.name(), s.scenarios(), s.shuffled(), s.assertions(), gr));
        }
        assertTrue(Shards.validate(new Shards.Plan(none, p.aggregate(), "x"), manifest(), aggregatable()).contains("no shard makes generation run " + REPLAY));
        List<String> twice = Shards.validate(new Shards.Plan(two, p.aggregate(), "x"), manifest(), aggregatable());
        assertTrue(twice.contains("generation run " + REPLAY + " is made by two shards"), twice.toString());
        assertTrue(twice.contains("shard hand-made: generation run " + REPLAY + " needs its scenario horizon-planned-late in the shard"), twice.toString());
        assertEquals(List.of(), Shards.validate(p, manifest(), aggregatable()));
        for (Scenarios.GenerationRun g : Scenarios.GENERATION_RUNS) {
            long makers = p.shards().stream().filter(s -> s.generationRuns().contains(g.name())).count();
            assertEquals(1, makers, g.name());
        }
    }

    @Test void aReplayRunIsCheckedAgainstItsReplayLock() throws Exception {
        String original = lock(PLANNED_LATE).path("files").path("manifest.json").asText();
        String replay = FixtureBundles.replayLock(ROOT, PLANNED_LATE).path("files").path("manifest.json").asText();
        assertNotEquals(original, replay);
        Read ok = read(outputs((s, b) -> {}));
        assertEquals(List.of(), ok.problems());
        assertEquals(replay, ok.summaries().generationRuns.get(REPLAY).path("recordingManifestSha256").asText(), "the replay summary pins the replay lock's manifest");
        // the summary of the replay naming the original recording
        Read onOriginal = read(outputs((s, b) -> { if (s.equals(PLANNED_LATE)) generationSummary(b[1], REPLAY).put("recordingManifestSha256", original); }));
        assertTrue(onOriginal.problems().contains("shard horizon-planned-late: " + REPLAY + ": its run read recording manifest " + original + ", its lock pins " + replay),
            onOriginal.problems().toString());
        // a replay summary of other cut ids than the replay lock plans
        long n = FixtureBundles.replayLock(ROOT, PLANNED_LATE).path("counts").path("cuts").asLong();
        Read shortList = read(outputs((s, b) -> { if (s.equals(PLANNED_LATE)) ((ArrayNode) generationSummary(b[1], REPLAY).get("comparedCutIds")).remove((int) n - 1); }));
        assertTrue(shortList.problems().stream().anyMatch(x -> x.startsWith("shard horizon-planned-late: " + REPLAY + ": the gate compared cuts (at position " + (n - 1))),
            shortList.problems().toString());
        // a replay run that does not name its original recording, or whose run record is not a replay
        Read noOriginal = read(outputs((s, b, d) -> {
            if (!s.equals(PLANNED_LATE)) return;
            ObjectNode e = GateBundles.generationRunEntry(b[0], REPLAY);
            rewrite(d, e, "runRecord", rr -> rr.putNull("replay"));
            e.putNull("originalRecordingManifestSha256");
        }));
        assertTrue(noOriginal.problems().stream().anyMatch(x -> x.contains(": run record replay null for an event-time replay")), noOriginal.problems().toString());
        assertTrue(noOriginal.problems().stream().anyMatch(x -> x.contains(": originalRecordingManifestSha256 null, its run record's")), noOriginal.problems().toString());
        // two run entries for the one replay summary
        Read two = read(outputs((s, b) -> { if (s.equals(PLANNED_LATE)) ((ArrayNode) b[0].get("runs")).add(GateBundles.generationRunEntry(b[0], REPLAY).deepCopy().put("runId", "another")); }));
        assertTrue(two.problems().contains("shard horizon-planned-late: " + REPLAY + ": two run entries for one summary"), two.problems().toString());
    }

    static final String KEPT = "horizon-planned-late/journals/horizon-planned-late/";

    @Test void keptJournalsMissingOrChangedInTheArtifactAreAProblem() throws Exception {
        String file = "journals/" + RunFold.WR + "/0.jsonl";
        Path dir = outputs((s, b) -> {});
        assertEquals(List.of(), read(dir).problems());
        assertTrue(Files.isRegularFile(dir.resolve(KEPT + "online/" + file)) && Files.isRegularFile(dir.resolve(KEPT + "event-time-replay/" + file)), "both runs' journals are kept");
        Files.writeString(dir.resolve(KEPT + "event-time-replay/" + file), "{\"changed\":1}\n", StandardCharsets.UTF_8);
        Read changed = read(dir);
        assertTrue(changed.problems().stream().anyMatch(x -> x.endsWith(": journal journals/horizon-planned-late/event-time-replay/" + file + " changed")), changed.problems().toString());

        dir = outputs((s, b) -> {});
        Files.delete(dir.resolve(KEPT + "online/" + file));
        Read missing = read(dir);
        assertTrue(missing.problems().stream().anyMatch(x -> x.endsWith(": journal journals/horizon-planned-late/online/" + file + " is missing")), missing.problems().toString());

        dir = outputs((s, b) -> {});
        Files.writeString(dir.resolve(KEPT + "online/journals/extra.jsonl"), "{}\n", StandardCharsets.UTF_8);
        Read extra = read(dir);
        assertTrue(extra.problems().stream().anyMatch(x -> x.endsWith(": journal journals/horizon-planned-late/online/journals/extra.jsonl is not in its index")), extra.problems().toString());

        dir = outputs((s, b) -> {});
        Files.delete(dir.resolve(KEPT + "event-time-replay.index.json"));
        Read noIndex = read(dir);
        assertTrue(noIndex.problems().stream().anyMatch(x -> x.endsWith(": no journal index journals/horizon-planned-late/event-time-replay.index.json")), noIndex.problems().toString());

        Read unkept = read(outputs((s, b) -> { if (s.equals(PLANNED_LATE)) entry(b[0], "pipeline", PLANNED_LATE, false).putNull("journals").putNull("journalsIndexSha256"); }));
        assertTrue(unkept.problems().stream().anyMatch(x -> x.endsWith(": its journals are not kept, journals/horizon-planned-late/online expected")), unkept.problems().toString());
        Read foreign = read(outputs((s, b) -> { if (s.equals("hand-made")) entry(b[0], "pipeline", "baseline", false).put("journals", "journals/baseline/online").put("journalsIndexSha256", "0".repeat(64)); }));
        assertTrue(foreign.problems().stream().anyMatch(x -> x.endsWith(": journals \"journals/baseline/online\", none expected")), foreign.problems().toString());
    }

    /** A context whose aggregate summaries are {@code s}. */
    CoreGate.Context aggregateContext(Summaries s) throws Exception {
        Path base = Files.createDirectories(tmp.resolve("ctx-" + UUID.randomUUID()));
        CoreGate.Context c = new CoreGate.Context(ROOT, Files.createDirectories(base.resolve("tmp")), Files.createDirectories(base.resolve("out").resolve("logs")), CoreGateTest.PYTHON);
        c.aggregate = s;
        return c;
    }

    /** The {@code afterHorizon} values of a run of {@code scenario}: {@code need} from its visit facts (the scenario's visits, as the
     *  replay recording's {@code expected.visits} carry them) and the rule's H and L, then a last W1 past it, online visits falling and
     *  every visit's work record remaining. */
    static ObjectNode afterHorizonValues(String scenario) {
        JsonNode sc = Json.read(ROOT.resolve("fixtures/scenarios").resolve(scenario + ".json"));
        long t0 = sc.path("T0").asLong();
        ObjectNode expected = Json.object();
        ArrayNode visits = expected.putArray("visits");
        for (JsonNode v : sc.path("visits")) {
            ObjectNode x = visits.addObject().put("station", v.path("station").asText());
            Json.putSafe(x, "enter", t0 + v.path("enter").asLong());
            if (v.path("leave").isNumber()) Json.putSafe(x, "leave", t0 + v.path("leave").asLong()); else x.putNull("leave");
        }
        Injections.Need n = Injections.afterHorizonNeed(expected, 86400000L, 720000L);
        ObjectNode a = Json.object();
        Json.putSafe(a, "t0", t0);
        Json.putSafe(a, "need", n.at());
        a.put("station", n.station()).put("what", n.what());
        Json.putSafe(a, "lastW1", n.at() + 1000);
        Json.putSafe(a, "maxOnlineVisits", 3L);
        Json.putSafe(a, "lastOnlineVisits", 0L);
        Json.putSafe(a, "workRecords", (long) visits.size());
        Json.putSafe(a, "visits", (long) visits.size());
        return a;
    }

    Summaries withAfterHorizon(boolean replayToo) throws Exception {
        Summaries s = read(outputs((x, b) -> {})).summaries();
        for (String h : Scenarios.HORIZON) ((ObjectNode) s.runs.get(h)).set("afterHorizon", afterHorizonValues(h));
        if (replayToo) ((ObjectNode) s.generationRuns.get(REPLAY)).set("afterHorizon", afterHorizonValues(PLANNED_LATE));
        return s;
    }

    @Test void aReplaySummaryWithoutAfterHorizonValuesFailsJAfterHorizonCuts() throws Exception {
        CoreGate.Result r = Injections.afterHorizon(aggregateContext(withAfterHorizon(false)));
        assertFalse(r.ok(), r.evidence());
        assertTrue(r.evidence().contains(REPLAY + ": no afterHorizon values"), r.evidence());
        Summaries absent = withAfterHorizon(true);
        absent.generationRuns.remove(REPLAY);
        CoreGate.Result a = Injections.afterHorizon(aggregateContext(absent));
        assertFalse(a.ok(), a.evidence());
        assertTrue(a.evidence().contains(REPLAY + " not run: no afterHorizon values"), a.evidence());
        Summaries failed = withAfterHorizon(true);
        ((ObjectNode) failed.generationRuns.get(REPLAY)).put("failure", "afterHorizon: no visit facts").put("ok", false);
        CoreGate.Result f = Injections.afterHorizon(aggregateContext(failed));
        assertFalse(f.ok(), f.evidence());
        assertTrue(f.evidence().contains(REPLAY + ": afterHorizon: no visit facts"), f.evidence());
    }

    @Test void theAggregateReadsTheAfterHorizonValuesOfThePlannedLateReplay() throws Exception {
        CoreGate.Result r = Injections.afterHorizon(aggregateContext(withAfterHorizon(true)));
        assertTrue(r.ok(), r.evidence());
        assertTrue(r.evidence().contains(REPLAY + " last W1 T0 + "), "the evidence names the replay run: " + r.evidence());
        Map<String, Consumer<ObjectNode>> broken = new LinkedHashMap<>();
        broken.put("last W1 ", a -> Json.putSafe(a, "lastW1", a.path("need").asLong() - 1));
        broken.put("onlineVisits max 0", a -> Json.putSafe(a, "maxOnlineVisits", 0L));
        broken.put("onlineVisits max 3, last 3", a -> Json.putSafe(a, "lastOnlineVisits", 3L));
        broken.put("work records in the fold at the last cut", a -> Json.putSafe(a, "workRecords", a.path("visits").asLong() - 1));
        for (Map.Entry<String, Consumer<ObjectNode>> e : broken.entrySet()) {
            Summaries s = withAfterHorizon(true);
            e.getValue().accept((ObjectNode) s.generationRuns.get(REPLAY).get("afterHorizon"));
            CoreGate.Result x = Injections.afterHorizon(aggregateContext(s));
            assertFalse(x.ok(), e.getKey() + ": " + x.evidence());
            assertTrue(x.evidence().contains(REPLAY + (e.getKey().startsWith("work") ? " " : ": ") + e.getKey()), e.getKey() + ": " + x.evidence());
        }
    }
}
