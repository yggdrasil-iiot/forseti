package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.recording.ReplayRecording;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** Whole-gate evidence bundles made of stand-in runs, accepted by the gate's own validator ({@link GateEvidence#validate},
 *  {@link Shards#read}): a {@code full} bundle and an {@code aggregate} report with its shard bundles. Every pipeline generation of a kept
 *  store is registered for real through {@code generation new} (in-process, the fixture clock); the runs are stand-ins (a run record and a
 *  cut log per run, one cut per planned cut id, the kept journals of the event-time replay and of its scenario's online run one small
 *  file each). The positive bundles list every committed manifest id: they are validation fixtures, not delivery evidence. They serve the
 *  unit tests of the report reader and {@code F-GEN-PROMOTE-REQUIRES-REPORT}, which refuses changes of one such bundle without launching a
 *  gate. */
public final class FixtureBundles {
    private FixtureBundles() {}

    public static final String HAND_MADE = "hand-made", DECOY_SHARD = "horizon-no-departure";
    /** The stand-in pins of a report (and of every stand-in run record): {@code artifactSha256} and {@code jcs.java}. */
    public static final String ARTIFACT = "a".repeat(64), JCS = "io.github.erdtman:java-json-canonicalization:1.1", ROUTING = "r".repeat(64);

    /** Options of a bundle. {@code plcScenario}: a scenario whose generation is registered on stream {@code line2-plc} (null: every
     *  generation on {@code line2}). {@code decoy}: when non-null, the digest given to an {@code assertion:F-GEN-NEW} generation 1 beside the
     *  pipeline store and (aggregate) to generation 1 of shard {@value #DECOY_SHARD}'s pipeline; the literal {@code "target"} means the
     *  digest of the bundle's own pipeline generation 1. */
    public record Options(String plcScenario, String decoy) {
        public static final Options DEFAULT = new Options(null, null);
    }

    /** {@code manifestSha256} (the committed manifest's), {@code seed}, {@code pins} and {@code fixtureDigests} of the stand-in reports. */
    public static ObjectNode common(Path root) {
        ObjectNode e = Json.object();
        try { e.put("manifestSha256", Canonical.digestOf(Files.readAllBytes(root.resolve(CoreGate.MANIFEST)))); }
        catch (IOException x) { throw new IllegalStateException(x); }
        Json.putSafe(e, "seed", 1L);
        ObjectNode pins = e.putObject("pins");
        pins.put("python", "3.13");
        pins.putObject("jcs").put("java", JCS);
        pins.putObject("forseti").put("artifactSha256", ARTIFACT);
        e.putObject("fixtureDigests").put("scenarios", "d");
        return e;
    }

    // --- building blocks ---------------------------------------------------------------------------------------------------------

    static long clockOf(Path registry, String stream) { return GateSigner.now(registry.resolve("mappings"), stream); }

    /** {@code generation new} in-process on {@code state} without a recording at the head's signedAt + 60 s: what it printed. */
    public static JsonNode register(Path root, Path state, Path registry, String stream, String rule) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        Path m = registry.resolve("mappings");
        int exit = GenerationCli.run(new String[] {"new", "--state", state.toString(), "--mappings", m.toString(), "--trusted-keys", root.resolve("fixtures/trusted-keys.jsonl").toString(),
            "--stream", stream, "--rule", rule}, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8),
            Clock.fixed(Instant.ofEpochMilli(clockOf(registry, stream)), ZoneOffset.UTC));
        if (exit != 0) throw new IllegalStateException("generation new exit " + exit + ": " + so + se);
        return Json.read(so.toString(StandardCharsets.UTF_8).strip().getBytes(StandardCharsets.UTF_8));
    }

    /** The stored value of generation {@code n}'s first row in {@code state}. */
    public static JsonNode row(Path state, long n) {
        String text;
        try { text = Files.readString(state.resolve("generations.jsonl"), StandardCharsets.UTF_8); } catch (IOException e) { throw new IllegalStateException(e); }
        for (String l : text.split("\n")) {
            JsonNode r = Json.read(l.getBytes(StandardCharsets.UTF_8));
            if (r.path("value").path("generation").asLong() == n) return r.path("value");
        }
        throw new IllegalStateException("no row " + n);
    }

    public static JsonNode lock(Path root, String scenario) {
        return Json.read(ScenarioBuilder.lockOf(root.resolve("fixtures").resolve("scenarios").resolve(scenario + ".json")));
    }
    public static JsonNode replayLock(Path root, String scenario) {
        return Json.read(ReplayRecording.lockOf(root.resolve("fixtures").resolve("scenarios").resolve(scenario + ".json")));
    }
    /** The lock a run of {@code scenario} with {@code variant} reads: the replay lock for an event-time replay, else the scenario's. */
    static JsonNode lockOf(Path root, String scenario, String variant) { return GateEvidence.REPLAY.equals(variant) ? replayLock(root, scenario) : lock(root, scenario); }
    public static long lockCuts(Path root, String scenario) { return lock(root, scenario).path("counts").path("cuts").asLong(); }

    /** A registered generation: its entry, number, digest, scope and stored row (the pins its stand-in runs carry). */
    public record Gen(ObjectNode entry, long generation, String digest, String scope, JsonNode row) {}

    /** The run record and cut log of a stand-in run of {@code scenario} on {@code g} under {@code bundle/runs/<name>}, with
     *  {@code variant}; its run entry ({@code journals} null). */
    public static ObjectNode standInRun(Path root, Path bundle, String shard, String store, Gen g, String scenario, String variant, long seed, boolean shuffle, String name,
                                        ObjectNode common) throws IOException {
        JsonNode lock = lockOf(root, scenario, variant);
        long n = lock.path("counts").path("cuts").asLong();
        boolean replay = GateEvidence.REPLAY.equals(variant);
        String manifest = lock.path("files").path("manifest.json").asText(), original = replay ? lock(root, scenario).path("files").path("manifest.json").asText() : null;
        String artifact = common.path("pins").path("forseti").path("artifactSha256").asText(), jcs = common.path("pins").path("jcs").path("java").asText();
        String runId = "run-" + (shard == null ? "full" : shard) + "-" + store.replace(':', '-').replace('#', '-') + "-" + name;
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
        ObjectNode rr = Json.object().put("runId", runId).put("engine", "core-harness").put("engineVersion", "0.1.0").put("artifactSha256", artifact);
        Json.putSafe(rr, "generation", g.generation());
        rr.put("registeredRowSha256", g.digest()).put("scope", g.scope());
        Json.putSafe(rr, "seed", seed);
        rr.put("shuffle", shuffle).put("replay", replay ? "event-time" : null).put("originalRecordingManifestSha256", original);
        ObjectNode pins = g.row().path("releaseEvidence").path("pins").deepCopy();
        pins.putObject("job").put("runId", runId).put("routingDigest", ROUTING);
        rr.set("pins", pins);
        ObjectNode fd = rr.putObject("fixtureDigests");
        fd.put("recordingManifestSha256", manifest);
        fd.putObject("recordingFiles");
        fd.put("releaseSha256", g.row().path("releaseEvidence").path("releaseSha256").asText()).put("rulesDigest", g.row().path("rulesDigest").asText());
        rr.putObject("jcs").put("java", jcs);
        rr.putObject("subscribed").putArray("ot.messages").add(0);
        rr.putObject("offsetRanges").putObject("ot.messages").putArray("0").add(0).add(n);
        Json.putSafe(rr, "cuts", n);
        rr.put("command", "stand-in");
        Path runs = Files.createDirectories(bundle.resolve("runs"));
        byte[] rb = Json.pretty(rr), cb = Json.pretty(cutLog);
        Files.write(runs.resolve(name + ".run.json"), rb);
        Files.write(runs.resolve(name + ".cuts.json"), cb);
        ObjectNode e = Json.object().put("shard", shard).put("store", store).put("scenario", scenario).put("variant", variant);
        Json.putSafe(e, "seed", seed);
        e.put("shuffle", shuffle).put("runId", runId);
        Json.putSafe(e, "generation", g.generation());
        e.put("registeredRowSha256", g.digest()).put("state", g.entry().path("state").asText());
        e.put("runRecord", "runs/" + name + ".run.json").put("runRecordSha256", Canonical.digestOf(rb));
        e.put("cutLog", "runs/" + name + ".cuts.json").put("cutLogSha256", Canonical.digestOf(cb));
        e.put("recordingManifestSha256", manifest).put("originalRecordingManifestSha256", original);
        e.set("offsetRanges", rr.get("offsetRanges").deepCopy());
        e.putNull("journals").putNull("journalsIndexSha256");
        return e;
    }

    /** Stand-in kept journals of {@code entry} at {@code bundle/journals/<scenario>/<variant>} (one journal file) with their index; the
     *  entry then names both. */
    public static void journals(Path bundle, ObjectNode entry, String scenario, String variant) throws IOException {
        String rel = GateEvidence.JOURNALS + "/" + scenario + "/" + variant;
        Path dir = bundle.resolve(rel), file = dir.resolve("journals").resolve(RunFold.WR).resolve("0.jsonl");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "{\"stand-in\":\"" + entry.path("runId").asText() + "\"}\n", StandardCharsets.UTF_8);
        byte[] index = GateEvidence.journalIndex(dir);
        Files.write(bundle.resolve(rel + ".index.json"), index);
        entry.put("journals", rel).put("journalsIndexSha256", Canonical.digestOf(index));
    }

    public static ObjectNode genEntry(String shard, String store, long n, String digest, String scope, String rulesDigest, String state, String scenario, String variant,
                                      String assertion) {
        ObjectNode g = Json.object().put("shard", shard).put("store", store);
        Json.putSafe(g, "generation", n);
        return g.put("scope", scope).put("rulesDigest", rulesDigest).put("registeredRowSha256", digest).put("state", state).put("scenario", scenario)
            .put("variant", variant).put("assertion", assertion);
    }

    /** Cut ids 1..n into {@code field}. */
    static void ids(ObjectNode run, String field, long n) {
        ArrayNode a = run.putArray(field);
        for (long k = 1; k <= n; k++) a.add(k);
    }

    /** A stand-in run summary named {@code name} on {@code lock}'s recording, compared at every cut it plans. */
    public static ObjectNode summary(String name, JsonNode lock) {
        long n = lock.path("counts").path("cuts").asLong();
        ObjectNode r = Json.object();
        r.put("name", name).putNull("failure").put("ok", true).put("summary", name + ": stand-in");
        r.put("recordingManifestSha256", lock.path("files").path("manifest.json").asText());
        ids(r, "plannedCutIds", n);
        Json.putSafe(r, "cutLogCuts", n);
        ids(r, "cutLogIds", n);
        Json.putSafe(r, "cutsCompared", n);
        ids(r, "comparedCutIds", n);
        r.put("equal", true);
        r.putObject("findings");
        r.putObject("rows").put("forseti.work-records", 3);
        return r;
    }
    public static ObjectNode summary(Path root, String scenario) { return summary(scenario, lock(root, scenario)); }

    /** The generation runs {@code names} of one bundle (a shard's planned list, or every planned one for a full bundle), added to its
     *  {@code gens}, {@code runEntries} and {@code summaries.generationRuns} (created when absent): the event-time replay registered in the
     *  pipeline store after its scenarios (for real into {@code dir/state} when {@code shared}, else a stand-in digest), with the kept
     *  journals of the replay and of its scenario's online run; the rule change in its {@code assertion:} store, two isolated
     *  generations (generation 1 {@code self-test} on {@code registry}, generation 2 the run's, on {@code registry-next}). */
    public static void generationRuns(Path root, Path dir, String shard, boolean shared, List<String> names, ObjectNode common, ArrayNode gens, ArrayNode runEntries,
                                      ObjectNode summaries) throws IOException {
        ObjectNode out = summaries.has("generationRuns") ? (ObjectNode) summaries.get("generationRuns") : summaries.putObject("generationRuns");
        Path registry = root.resolve("fixtures/registry"), next = root.resolve("fixtures/registry-next");
        for (Scenarios.GenerationRun g : Scenarios.GENERATION_RUNS) {
            if (!names.contains(g.name())) continue;
            Gen gen;
            if (GateEvidence.PIPELINE.equals(g.store())) {
                long n = 0;
                for (JsonNode x : gens) if (GateEvidence.PIPELINE.equals(x.path("store").asText())) n = Math.max(n, x.path("generation").asLong());
                n++;
                String digest;
                JsonNode row;
                if (shared) {
                    JsonNode p = register(root, dir.resolve(GateEvidence.STATE), registry, "line2", "line2-attribution@1.0.0");
                    if (p.path("generation").asLong() != n) throw new IllegalStateException("expected generation " + n + ", got " + p);
                    digest = p.path("registeredRowSha256").asText();
                    row = row(dir.resolve(GateEvidence.STATE), n);
                } else {
                    Path probe = Files.createTempDirectory("forseti-probe-");
                    register(root, probe, registry, "line2", "line2-attribution@1.0.0");
                    row = row(probe, 1);
                    CoreGate.delete(probe);
                    digest = Canonical.digest(Json.object().put("stand-in", shard + "/" + g.name()));
                }
                ObjectNode e = genEntry(shard, GateEvidence.PIPELINE, n, digest, "line2", row.path("rulesDigest").asText(), shared ? GateEvidence.SHARED : GateEvidence.ISOLATED,
                    g.scenario(), g.variant(), null);
                gens.add(e);
                gen = new Gen(e, n, digest, "line2", row);
                for (JsonNode x : runEntries)
                    if (GateEvidence.PIPELINE.equals(x.path("store").asText()) && g.scenario().equals(x.path("scenario").asText()) && !x.path("shuffle").asBoolean()
                        && GateEvidence.ONLINE.equals(x.path("variant").asText())) journals(dir, (ObjectNode) x, g.scenario(), GateEvidence.ONLINE);
            } else {
                Path probe = Files.createTempDirectory("forseti-probe-");
                register(root, probe, registry, "line2", "line2-attribution@1.0.0");
                JsonNode p2 = register(root, probe, next, "line2", Scenarios.RULE_NEXT);
                JsonNode first = row(probe, 1), second = row(probe, 2);
                CoreGate.delete(probe);
                String d1 = Canonical.digest(Json.object().put("stand-in", shard + "/" + g.store() + "/1")), d2 = p2.path("registeredRowSha256").asText();
                gens.add(genEntry(shard, g.store(), 1, d1, "line2", first.path("rulesDigest").asText(), GateEvidence.ISOLATED, g.scenario(), GateEvidence.SELF_TEST, g.assertion()));
                ObjectNode e = genEntry(shard, g.store(), 2, d2, "line2", second.path("rulesDigest").asText(), GateEvidence.ISOLATED, g.scenario(), g.variant(), g.assertion());
                gens.add(e);
                gen = new Gen(e, 2, d2, "line2", second);
            }
            ObjectNode run = standInRun(root, dir, shard, g.store(), gen, g.scenario(), g.variant(), CoreGate.DEFAULT_SEED, false, g.label(), common);
            if (GateEvidence.REPLAY.equals(g.variant())) journals(dir, run, g.scenario(), g.variant());
            runEntries.add(run);
            out.set(g.name(), summary(g.name(), lockOf(root, g.scenario(), g.variant())));
        }
    }

    /** Fills {@code dir} with the generations, runs and summaries of {@code scenarios}, {@code shuffled} and {@code generationRuns}: real
     *  registrations into {@code dir/state} when {@code shared}, else stand-in digests (the row of a throwaway registration for the pins). */
    static void populate(Path root, Path dir, String shard, boolean shared, List<String> scenarios, List<String> shuffled, List<String> generationRuns, Options o,
                         ObjectNode common, ArrayNode gens, ArrayNode runEntries, ObjectNode summaries, Map<String, String> pipelineDigest1) throws IOException {
        ArrayNode runs = summaries.putArray("runs"), sh = summaries.putArray("shuffled");
        Path probe = Files.createTempDirectory("forseti-probe-"), registry = root.resolve("fixtures/registry");
        Map<String, Gen> table = new HashMap<>();
        long n = 0;
        for (String sc : Scenarios.all()) {
            if (!scenarios.contains(sc)) continue;
            n++;
            String stream = sc.equals(o.plcScenario()) ? "line2-plc" : "line2";
            String digest;
            JsonNode row;
            if (shared) {
                JsonNode p = register(root, dir.resolve(GateEvidence.STATE), registry, stream, "line2-attribution@1.0.0");
                if (p.path("generation").asLong() != n) throw new IllegalStateException("expected generation " + n + ", got " + p);
                digest = p.path("registeredRowSha256").asText();
                row = row(dir.resolve(GateEvidence.STATE), n);
            } else {
                Path ps = probe.resolve(stream);
                if (!Files.exists(ps.resolve("generations.jsonl"))) register(root, ps, registry, stream, "line2-attribution@1.0.0");
                row = row(ps, 1);
                digest = Canonical.digest(Json.object().put("stand-in", shard + "/" + sc));
                if (n == 1 && DECOY_SHARD.equals(shard) && o.decoy() != null) digest = o.decoy().equals("target") ? pipelineDigest1.get("target") : o.decoy();
            }
            if (n == 1) pipelineDigest1.putIfAbsent("target", digest);
            ObjectNode e = genEntry(shard, GateEvidence.PIPELINE, n, digest, stream, row.path("rulesDigest").asText(), shared ? GateEvidence.SHARED : GateEvidence.ISOLATED,
                sc, GateEvidence.ONLINE, null);
            gens.add(e);
            table.put(sc, new Gen(e, n, digest, stream, row));
        }
        if (o.decoy() != null && (shard == null || HAND_MADE.equals(shard))) {
            String d = o.decoy().equals("target") ? pipelineDigest1.get("target") : o.decoy();
            gens.add(genEntry(shard, "assertion:F-GEN-NEW", 1, d, "line2", table.values().iterator().next().row().path("rulesDigest").asText(), GateEvidence.ISOLATED,
                "smoke", GateEvidence.SELF_TEST, "F-GEN-NEW"));
        }
        for (String sc : scenarios) {
            runEntries.add(standInRun(root, dir, shard, GateEvidence.PIPELINE, table.get(sc), sc, GateEvidence.ONLINE, CoreGate.DEFAULT_SEED, false, sc, common));
            runs.add(summary(root, sc));
        }
        for (String sc : shuffled) for (long seed : Scenarios.SHUFFLE_SEEDS) {
            runEntries.add(standInRun(root, dir, shard, GateEvidence.PIPELINE, table.get(sc), sc, GateEvidence.ONLINE, seed, true, sc + ".shuffle-" + seed, common));
            ObjectNode x = Json.object().put("name", sc);
            Json.putSafe(x, "seed", seed);
            x.setAll((ObjectNode) summary(root, sc).without(List.of("name")));
            x.put("summary", sc + " seed " + seed + ": stand-in");
            x.putObject("ms");
            sh.add(x);
        }
        CoreGate.delete(probe);
        generationRuns(root, dir, shard, shared, generationRuns, common, gens, runEntries, summaries);
    }

    static ObjectNode report(ObjectNode common, String layout, String shard) {
        ObjectNode r = common.deepCopy();
        Json.putSafe(r, "exit", 0L);
        r.put("kind", "gate").put("layout", layout).putNull("negativeControl").put("scope", "core").put("engine", "core");
        r.putArray("failed");
        r.putArray("notRun");
        r.putArray("intendedFailures");
        r.putNull("error");
        r.put("shard", shard);
        return r;
    }

    public static void writeBound(Path dir, ObjectNode report, ObjectNode summaries) throws IOException {
        byte[] sb = Json.pretty(summaries);
        Files.write(dir.resolve(Shards.SUMMARIES), sb);
        report.put("summariesSha256", Canonical.digestOf(sb));
        Json.write(dir.resolve("report.json"), report);
    }

    static void sortRuns(ArrayNode runs) {
        List<JsonNode> l = new ArrayList<>();
        runs.forEach(l::add);
        l.sort(Comparator.comparing((JsonNode r) -> r.path("store").asText()).thenComparing(r -> r.path("runRecord").asText()));
        runs.removeAll();
        l.forEach(runs::add);
    }

    // --- full --------------------------------------------------------------------------------------------------------------------

    /** A complete full-layout bundle in {@code dir}: every scenario of {@link Scenarios#all()}, every shuffled run and every generation run,
     *  one shared pipeline store under {@code dir/state}, every manifest id held. */
    public static Path full(Path root, Path dir, Options o, ObjectNode common) throws IOException {
        Files.createDirectories(dir);
        ObjectNode report = report(common, "full", null), summaries = Json.object();
        List<String> ids = CoreGate.manifestIds(Json.read(root.resolve(CoreGate.MANIFEST)));
        Json.putSafe(report, "assertionsExpected", ids.size());
        Json.putSafe(report, "assertionsRun", ids.size());
        report.putNull("shardPlanSha256");
        ArrayNode results = report.putArray("results");
        for (String id : ids) results.addObject().put("id", id).put("ok", true).put("evidence", "stand-in").putNull("shard");
        report.putArray("shards");
        ArrayNode gens = report.putArray("generations"), runs = report.putArray("runs");
        List<String> generationRuns = Scenarios.GENERATION_RUNS.stream().map(Scenarios.GenerationRun::name).toList();
        populate(root, dir, null, true, Scenarios.all(), Scenarios.SHUFFLED, generationRuns, o, common, gens, runs, summaries, new HashMap<>());
        sortRuns(runs);
        writeBound(dir, report, summaries);
        List<String> p = GateEvidence.validate(root, dir);
        if (!p.isEmpty()) throw new IllegalStateException("the full bundle does not validate: " + p);
        return dir;
    }

    // --- aggregate ---------------------------------------------------------------------------------------------------------------

    /** A complete aggregate report at {@code dir/aggregate/report.json} with its shard bundles under {@code dir/shards}: shard
     *  {@value #HAND_MADE} keeps its shared store, the others are isolated. Returns the report. */
    public static Path aggregate(Path root, Path dir, Options o, ObjectNode common) throws IOException {
        Path shardsDir = Files.createDirectories(dir.resolve("shards"));
        Shards.Plan plan = Shards.load(root);
        Map<String, String> digest1 = new HashMap<>();
        List<Shards.Shard> order = new ArrayList<>(plan.shards());
        order.sort(Comparator.comparing((Shards.Shard s) -> !s.name().equals(HAND_MADE)));   // the target's digest first, for a decoy
        for (Shards.Shard s : order) {
            Path d = Files.createDirectories(shardsDir.resolve(s.name()));
            boolean shared = s.name().equals(HAND_MADE);
            ObjectNode report = report(common, "shard", s.name()), summaries = Json.object();
            Json.putSafe(report, "assertionsExpected", s.assertions().size());
            Json.putSafe(report, "assertionsRun", s.assertions().size());
            report.put("shardPlanSha256", plan.sha256());
            ArrayNode results = report.putArray("results");
            for (String id : s.assertions()) results.addObject().put("id", id).put("ok", true).put("evidence", "stand-in").put("shard", s.name());
            report.putArray("shards");
            ArrayNode gens = report.putArray("generations"), runs = report.putArray("runs");
            Map<String, String> mine = shared ? digest1 : new HashMap<>(Map.of("target", digest1.getOrDefault("target", "")));
            populate(root, d, s.name(), shared, s.scenarios(), s.shuffled(), s.generationRuns(), o, common, gens, runs, summaries, mine);
            sortRuns(runs);
            writeBound(d, report, summaries);
        }
        ObjectNode agg = report(common, "aggregate", null);
        GateBundle.Read read = GateBundle.readShards(root, shardsDir, agg);
        if (!read.problems().isEmpty()) throw new IllegalStateException("the shard bundles do not merge: " + read.problems());
        List<String> ids = CoreGate.manifestIds(Json.read(root.resolve(CoreGate.MANIFEST)));
        Json.putSafe(agg, "assertionsExpected", ids.size());
        Json.putSafe(agg, "assertionsRun", ids.size());
        agg.put("shardPlanSha256", plan.sha256()).putNull("summariesSha256");
        ArrayNode results = agg.putArray("results");
        Map<String, String> owner = new HashMap<>();
        for (GateBundle.Recorded r : read.recorded()) owner.put(r.id(), r.shard());
        for (String id : plan.aggregate()) owner.put(id, "aggregate");
        for (String id : ids) results.addObject().put("id", id).put("ok", true).put("evidence", "[shard " + owner.get(id) + "] stand-in").put("shard", owner.get(id));
        agg.set("shards", read.merged().get("shards"));
        agg.set("generations", read.merged().get("generations"));
        agg.set("runs", read.merged().get("runs"));
        Path report = dir.resolve("aggregate").resolve("report.json");
        Json.write(report, agg);
        return report;
    }

    // --- edits -------------------------------------------------------------------------------------------------------------------

    /** The run entry of {@code scenario} (not shuffled, online, pipeline) in {@code report}'s {@code runs[]} (of {@code shard} when non-null). */
    public static ObjectNode runEntry(JsonNode report, String shard, String scenario) {
        for (JsonNode r : report.get("runs"))
            if (r.path("scenario").asText().equals(scenario) && !r.path("shuffle").asBoolean() && GateEvidence.PIPELINE.equals(r.path("store").asText())
                && GateEvidence.ONLINE.equals(r.path("variant").asText()) && (shard == null || shard.equals(r.path("shard").asText()))) return (ObjectNode) r;
        throw new IllegalStateException("no run of " + scenario);
    }

    /** The pipeline generation entry of {@code scenario}'s online run (of {@code shard} when non-null). */
    public static ObjectNode generationEntry(JsonNode report, String shard, String scenario) {
        for (JsonNode g : report.get("generations"))
            if (g.path("scenario").asText().equals(scenario) && GateEvidence.PIPELINE.equals(g.path("store").asText()) && GateEvidence.ONLINE.equals(g.path("variant").asText())
                && (shard == null || shard.equals(g.path("shard").asText()))) return (ObjectNode) g;
        throw new IllegalStateException("no generation of " + scenario);
    }

    /** Edits a JSON file of a bundle and returns its new raw digest. */
    public static String rewrite(Path file, java.util.function.Consumer<ObjectNode> change) throws IOException {
        ObjectNode n = (ObjectNode) Json.read(file);
        change.accept(n);
        byte[] b = Json.pretty(n);
        Files.write(file, b);
        return Canonical.digestOf(b);
    }
}
