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
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** Whole-gate evidence bundles for the tests of {@code generation promote} and its report reader: a {@code full} bundle and an
 *  {@code aggregate} report with its shard bundles, both complete and accepted by the gate's own validator ({@link GateEvidence#validate},
 *  {@link Shards#read}). Every pipeline generation of a kept store is registered for real through {@code generation new} (in-process, the
 *  fixture clock); the runs are stand-ins (a run record and a cut log per run, one cut per planned cut id, as {@link ShardsTest} makes
 *  them). The positive bundles list every committed manifest id: they are validation fixtures, not delivery evidence. */
public final class GateBundles {
    private GateBundles() {}

    public static final Path ROOT = CoreGateTest.ROOT, REGISTRY = ShardsTest.REGISTRY, KEYS = ShardsTest.KEYS;
    public static final String HAND_MADE = "hand-made", DECOY_SHARD = "horizon-no-departure";

    /** Options of a bundle. {@code plcScenario}: a scenario whose generation is registered on stream {@code line2-plc} (null: every
     *  generation on {@code line2}). {@code decoy}: when non-null, the digest given to an {@code assertion:F-GEN-NEW} generation 1 beside the
     *  pipeline store and (aggregate) to generation 1 of shard {@value #DECOY_SHARD}'s pipeline; the literal {@code "target"} means the
     *  digest of the bundle's own pipeline generation 1. */
    public record Options(String plcScenario, String decoy) {
        public static final Options DEFAULT = new Options(null, null);
    }

    /** One bundle directory: {@code report.json}, {@code summaries.json}, {@code runs/}, and {@code state/} when its store was kept. */
    public record Bundle(Path dir) {
        public Path report() { return dir.resolve("report.json"); }
        public Path summaries() { return dir.resolve(Shards.SUMMARIES); }
        public Path state() { return dir.resolve(GateEvidence.STATE); }
        public ObjectNode reportJson() { return (ObjectNode) Json.read(report()); }
        public ObjectNode summariesJson() { return (ObjectNode) Json.read(summaries()); }
        public Path file(String relative) { return dir.resolve(relative); }
    }

    /** An aggregate report ({@code report}) and the shard bundles under {@code shards}. */
    public record Aggregate(Path report, Path shards) {
        public Bundle shard(String name) { return new Bundle(shards.resolve(name)); }
        public ObjectNode reportJson() { return (ObjectNode) Json.read(report); }
    }

    // --- building blocks ---------------------------------------------------------------------------------------------------------

    static long clockOf(String stream) { return GateSigner.now(REGISTRY.resolve("mappings"), stream); }

    /** {@code generation new} in-process on {@code state} without a recording at the head's signedAt + 60 s: what it printed. */
    public static JsonNode register(Path state, String stream) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        Path m = REGISTRY.resolve("mappings");
        int exit = GenerationCli.run(new String[] {"new", "--state", state.toString(), "--mappings", m.toString(), "--trusted-keys", KEYS.toString(),
            "--stream", stream, "--rule", "line2-attribution@1.0.0"}, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8),
            Clock.fixed(Instant.ofEpochMilli(clockOf(stream)), ZoneOffset.UTC));
        if (exit != 0) throw new IllegalStateException("generation new exit " + exit + ": " + so + se);
        return Json.read(so.toString(StandardCharsets.UTF_8).strip().getBytes(StandardCharsets.UTF_8));
    }

    /** The stored value of generation {@code n}'s first row. */
    static JsonNode row(Path state, long n) { return ShardsTest.registeredRow(state, n); }

    /** A registered generation: its entry, number, digest, scope and stored row (the pins its stand-in runs carry). */
    record Gen(ObjectNode entry, long generation, String digest, String scope, JsonNode row) {}

    /** The run record and cut log of a stand-in run of {@code scenario} on {@code g} under {@code bundle/runs/<name>}; its run entry. */
    static ObjectNode standInRun(Path bundle, String shard, String store, Gen g, String scenario, long seed, boolean shuffle, String name) throws IOException {
        long n = ShardsTest.lockCuts(scenario);
        String runId = "run-" + (shard == null ? "full" : shard) + "-" + store.replace(':', '-').replace('#', '-') + "-" + name;
        ObjectNode cutLog = Json.object();
        Json.putSafe(cutLog, "generation", g.generation()).put("routingDigest", ShardsTest.ROUTING);
        ArrayNode cuts = cutLog.putArray("cuts");
        for (long k = 1; k <= n; k++) {
            ObjectNode c = Json.putSafe(Json.object(), "cutId", k);
            c.putObject("perStation");
            c.putObject("offsetRanges").putObject("ot.messages").putArray("0").add(k - 1).add(k);
            c.putArray("incidentOffsets");
            cuts.add(c);
        }
        ObjectNode rr = Json.object().put("runId", runId).put("engine", "core-harness").put("engineVersion", "0.1.0").put("artifactSha256", ShardsTest.ARTIFACT);
        Json.putSafe(rr, "generation", g.generation());
        rr.put("registeredRowSha256", g.digest()).put("scope", g.scope());
        Json.putSafe(rr, "seed", seed);
        rr.put("shuffle", shuffle).putNull("replay").putNull("originalRecordingManifestSha256");
        ObjectNode pins = g.row().path("releaseEvidence").path("pins").deepCopy();
        pins.putObject("job").put("runId", runId).put("routingDigest", ShardsTest.ROUTING);
        rr.set("pins", pins);
        ObjectNode fd = rr.putObject("fixtureDigests");
        fd.put("recordingManifestSha256", ShardsTest.lock(scenario).path("files").path("manifest.json").asText());
        fd.putObject("recordingFiles");
        fd.put("releaseSha256", g.row().path("releaseEvidence").path("releaseSha256").asText()).put("rulesDigest", g.row().path("rulesDigest").asText());
        rr.putObject("jcs").put("java", ShardsTest.JCS);
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
        e.put("recordingManifestSha256", ShardsTest.lock(scenario).path("files").path("manifest.json").asText()).putNull("originalRecordingManifestSha256");
        e.set("offsetRanges", rr.get("offsetRanges").deepCopy());
        return e;
    }

    static ObjectNode genEntry(String shard, String store, long n, String digest, String scope, String rulesDigest, String state, String scenario, String variant,
                               String assertion) {
        ObjectNode g = Json.object().put("shard", shard).put("store", store);
        Json.putSafe(g, "generation", n);
        return g.put("scope", scope).put("rulesDigest", rulesDigest).put("registeredRowSha256", digest).put("state", state).put("scenario", scenario)
            .put("variant", variant).put("assertion", assertion);
    }

    static String manifestSha256() {
        try { return Canonical.digestOf(Files.readAllBytes(ROOT.resolve(CoreGate.MANIFEST))); } catch (IOException e) { throw new IllegalStateException(e); }
    }

    /** The fields every report of these bundles states alike: {@code manifestSha256}, {@code seed}, {@code pins}, {@code fixtureDigests}. */
    static ObjectNode common() {
        ObjectNode e = ShardsTest.expect();
        e.put("manifestSha256", manifestSha256());
        return e;
    }

    /** Fills {@code dir} with the generations, runs and summaries of {@code scenarios} and {@code shuffled}: real registrations into
     *  {@code dir/state} when {@code shared}, else stand-in digests (the row of a throwaway registration for the pins). */
    static void populate(Path dir, String shard, boolean shared, List<String> scenarios, List<String> shuffled, Options o, ArrayNode gens, ArrayNode runEntries,
                         ObjectNode summaries, Map<String, String> pipelineDigest1) throws IOException {
        ArrayNode runs = summaries.putArray("runs"), sh = summaries.putArray("shuffled");
        Path probe = Files.createTempDirectory("forseti-probe-");
        Map<String, Gen> table = new HashMap<>();
        long n = 0;
        for (String sc : Scenarios.all()) {
            if (!scenarios.contains(sc)) continue;
            n++;
            String stream = sc.equals(o.plcScenario()) ? "line2-plc" : "line2";
            String digest;
            JsonNode row;
            if (shared) {
                JsonNode p = register(dir.resolve(GateEvidence.STATE), stream);
                if (p.path("generation").asLong() != n) throw new IllegalStateException("expected generation " + n + ", got " + p);
                digest = p.path("registeredRowSha256").asText();
                row = row(dir.resolve(GateEvidence.STATE), n);
            } else {
                Path ps = probe.resolve(stream);
                if (!Files.exists(ps.resolve("generations.jsonl"))) register(ps, stream);
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
            runEntries.add(standInRun(dir, shard, GateEvidence.PIPELINE, table.get(sc), sc, CoreGate.DEFAULT_SEED, false, sc));
            runs.add(ShardsTest.summary(sc));
        }
        for (String sc : shuffled) for (long seed : Scenarios.SHUFFLE_SEEDS) {
            runEntries.add(standInRun(dir, shard, GateEvidence.PIPELINE, table.get(sc), sc, seed, true, sc + ".shuffle-" + seed));
            ObjectNode x = Json.object().put("name", sc);
            Json.putSafe(x, "seed", seed);
            x.setAll((ObjectNode) ShardsTest.summary(sc).without(List.of("name")));
            x.put("summary", sc + " seed " + seed + ": stand-in");
            x.putObject("ms");
            sh.add(x);
        }
        CoreGate.delete(probe);
    }

    static ObjectNode report(String layout, String shard) {
        ObjectNode r = common();
        Json.putSafe(r, "exit", 0L);
        r.put("kind", "gate").put("layout", layout).putNull("negativeControl").put("scope", "core").put("engine", "core");
        r.putArray("failed");
        r.putArray("notRun");
        r.putArray("intendedFailures");
        r.putNull("error");
        r.put("shard", shard);
        return r;
    }

    static void writeBound(Path dir, ObjectNode report, ObjectNode summaries) throws IOException {
        byte[] sb = Json.pretty(summaries);
        Files.write(dir.resolve(Shards.SUMMARIES), sb);
        report.put("summariesSha256", Canonical.digestOf(sb));
        Json.write(dir.resolve("report.json"), report);
    }

    // --- full --------------------------------------------------------------------------------------------------------------------

    /** A complete full-layout bundle in {@code dir}: every scenario of {@link Scenarios#all()} and every shuffled run, one shared
     *  pipeline store under {@code dir/state}, every manifest id held. */
    public static Bundle full(Path dir, Options o) throws IOException {
        Files.createDirectories(dir);
        ObjectNode report = report("full", null), summaries = Json.object();
        List<String> ids = ShardsTest.manifest();
        Json.putSafe(report, "assertionsExpected", ids.size());
        Json.putSafe(report, "assertionsRun", ids.size());
        report.putNull("shardPlanSha256");
        ArrayNode results = report.putArray("results");
        for (String id : ids) results.addObject().put("id", id).put("ok", true).put("evidence", "stand-in").putNull("shard");
        report.putArray("shards");
        ArrayNode gens = report.putArray("generations"), runs = report.putArray("runs");
        populate(dir, null, true, Scenarios.all(), Scenarios.SHUFFLED, o, gens, runs, summaries, new HashMap<>());
        sortRuns(runs);
        writeBound(dir, report, summaries);
        List<String> p = GateEvidence.validate(ROOT, dir);
        if (!p.isEmpty()) throw new IllegalStateException("the full bundle does not validate: " + p);
        return new Bundle(dir);
    }

    static void sortRuns(ArrayNode runs) {
        List<JsonNode> l = new ArrayList<>();
        runs.forEach(l::add);
        l.sort(Comparator.comparing((JsonNode r) -> r.path("store").asText()).thenComparing(r -> r.path("runRecord").asText()));
        runs.removeAll();
        l.forEach(runs::add);
    }

    // --- aggregate ---------------------------------------------------------------------------------------------------------------

    /** A complete aggregate report at {@code dir/aggregate/report.json} with its shard bundles under {@code dir/shards}: shard
     *  {@value #HAND_MADE} keeps its shared store, the others are isolated. */
    public static Aggregate aggregate(Path dir, Options o) throws IOException {
        Path shardsDir = Files.createDirectories(dir.resolve("shards"));
        Shards.Plan plan = Shards.load(ROOT);
        Map<String, String> digest1 = new HashMap<>();
        List<Shards.Shard> order = new ArrayList<>(plan.shards());
        order.sort(Comparator.comparing((Shards.Shard s) -> !s.name().equals(HAND_MADE)));   // the target's digest first, for a decoy
        for (Shards.Shard s : order) {
            Path d = Files.createDirectories(shardsDir.resolve(s.name()));
            boolean shared = s.name().equals(HAND_MADE);
            ObjectNode report = report("shard", s.name()), summaries = Json.object();
            Json.putSafe(report, "assertionsExpected", s.assertions().size());
            Json.putSafe(report, "assertionsRun", s.assertions().size());
            report.put("shardPlanSha256", plan.sha256());
            ArrayNode results = report.putArray("results");
            for (String id : s.assertions()) results.addObject().put("id", id).put("ok", true).put("evidence", "stand-in").put("shard", s.name());
            report.putArray("shards");
            ArrayNode gens = report.putArray("generations"), runs = report.putArray("runs");
            Map<String, String> mine = shared ? digest1 : new HashMap<>(Map.of("target", digest1.getOrDefault("target", "")));
            populate(d, s.name(), shared, s.scenarios(), s.shuffled(), o, gens, runs, summaries, mine);
            sortRuns(runs);
            writeBound(d, report, summaries);
        }
        ObjectNode agg = report("aggregate", null);
        GateBundle.Read read = GateBundle.readShards(ROOT, shardsDir, agg);
        if (!read.problems().isEmpty()) throw new IllegalStateException("the shard bundles do not merge: " + read.problems());
        List<String> ids = ShardsTest.manifest();
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
        return new Aggregate(report, shardsDir);
    }

    // --- edits -------------------------------------------------------------------------------------------------------------------

    /** The run entry of {@code scenario} (not shuffled) in {@code report}'s {@code runs[]} (of {@code shard} when non-null). */
    public static ObjectNode runEntry(JsonNode report, String shard, String scenario) {
        for (JsonNode r : report.get("runs"))
            if (r.path("scenario").asText().equals(scenario) && !r.path("shuffle").asBoolean() && GateEvidence.PIPELINE.equals(r.path("store").asText())
                && (shard == null || shard.equals(r.path("shard").asText()))) return (ObjectNode) r;
        throw new IllegalStateException("no run of " + scenario);
    }

    /** The pipeline generation entry of {@code scenario} (of {@code shard} when non-null). */
    public static ObjectNode generationEntry(JsonNode report, String shard, String scenario) {
        for (JsonNode g : report.get("generations"))
            if (g.path("scenario").asText().equals(scenario) && GateEvidence.PIPELINE.equals(g.path("store").asText())
                && (shard == null || shard.equals(g.path("shard").asText()))) return (ObjectNode) g;
        throw new IllegalStateException("no generation of " + scenario);
    }

    /** Rewrites a full bundle's summaries and rebinds their digest in its report. */
    public static void rewriteSummaries(Bundle b, ObjectNode summaries) throws IOException {
        ObjectNode r = b.reportJson();
        writeBound(b.dir(), r, summaries);
    }

    /** Rewrites one shard bundle's report or summaries and rebinds both digests in the aggregate report's {@code shards[]}. */
    public static void rebind(Aggregate a, String shard) throws IOException {
        ObjectNode agg = a.reportJson();
        for (JsonNode s : agg.get("shards"))
            if (s.path("name").asText().equals(shard)) {
                ((ObjectNode) s).put("reportSha256", Canonical.digestOf(Files.readAllBytes(a.shard(shard).report())))
                    .put("summariesSha256", Canonical.digestOf(Files.readAllBytes(a.shard(shard).summaries())));
            }
        Json.write(a.report(), agg);
    }

    /** Edits a JSON file of a bundle and returns its new raw digest. */
    public static String rewrite(Path file, java.util.function.Consumer<ObjectNode> change) throws IOException {
        ObjectNode n = (ObjectNode) Json.read(file);
        change.accept(n);
        byte[] b = Json.pretty(n);
        Files.write(file, b);
        return Canonical.digestOf(b);
    }

    /** The scenario whose generation is number 1 in a pipeline store that makes {@code scenarios} (the first in {@link Scenarios#all()} order). */
    public static String first(List<String> scenarios) {
        for (String sc : Scenarios.all()) if (scenarios.contains(sc)) return sc;
        throw new IllegalStateException();
    }

    public static List<String> allScenarios() { return List.copyOf(Scenarios.all()); }
    public static List<String> handMadeScenarios() { return Shards.load(ROOT).shard(HAND_MADE).scenarios(); }
    public static List<Long> shuffleSeeds() { return Scenarios.SHUFFLE_SEEDS; }
    public static List<String> shuffled() { return Scenarios.SHUFFLED; }
}
