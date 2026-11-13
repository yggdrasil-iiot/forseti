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
import yggdrasil.forseti.cli.gate.CoreGate.Check;
import yggdrasil.forseti.cli.gate.CoreGate.Result;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The gate's accounting and report. The assertions that launch the oracle run in the jar gate (CI's {@code forseti gate}); here the
 *  accounting runs on stand-in bodies, and the assertions that need no interpreter run for real against the repository's fixtures. */
class CoreGateTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    static final Path MANIFEST = ROOT.resolve(CoreGate.MANIFEST);
    static final String PYTHON = System.getProperty("forseti.python", "python");
    static final Set<String> NEEDS_PYTHON = Set.of("F-JCS-PY", "F-JCS-REFUSALS", "F-RULES-LOAD-OJ", "F-RULES-LOAD-PLC");
    /** The scenario assertions of pull request 4 build recordings, run the harness and the oracle: they run in the jar gate only. */
    static final Set<String> NEEDS_ORACLE = needsOracle();
    static Set<String> needsOracle() {
        Set<String> s = new TreeSet<>(NEEDS_PYTHON);
        for (Check c : Injections.checks()) s.add(c.id());
        return s;
    }
    static final List<String> REPORT_FIELDS = List.of("exit", "kind", "layout", "negativeControl", "scope", "engine", "manifestSha256", "assertionsExpected",
        "assertionsRun", "failed", "notRun", "intendedFailures", "pins", "seed", "fixtureDigests", "measurements", "startedAt", "finishedAt", "command", "error",
        "shard", "shardPlanSha256", "summariesSha256", "results", "shards", "generations", "runs", "control");

    @TempDir Path tmp;

    record Run(int exit, List<String> lines, JsonNode report) {
        String last() { return lines.get(lines.size() - 1); }
        List<String> list(String field) { List<String> l = new ArrayList<>(); report.path(field).forEach(n -> l.add(n.asText())); return l; }
    }

    Run run(Path root, Path manifest, List<Check> checks) { return run(new CoreGate.Options(root, manifest, tmp.resolve("out-" + UUID.randomUUID()), PYTHON, null,
        List.of(), CoreGate.DEFAULT_SEED, "forseti gate --engine core (test)"), checks); }
    Run run(CoreGate.Options o, List<Check> checks) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(), err = new ByteArrayOutputStream();
        int exit = CoreGate.run(o, checks, new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
        List<String> lines = new ArrayList<>(out.toString(StandardCharsets.UTF_8).lines().toList());
        lines.addAll(0, err.toString(StandardCharsets.UTF_8).lines().map(l -> "ERR " + l).toList());
        return new Run(exit, lines, Json.read(o.out().resolve("report.json")));
    }
    Run run(Path manifest, List<Check> checks) { return run(ROOT, manifest, checks); }

    static List<String> manifestIds(Path manifest) { return CoreGate.manifestIds(Json.read(manifest)); }
    static List<String> registeredIds() { return CoreGate.registered().stream().map(Check::id).toList(); }
    /** Stand-ins that hold, one per registered id (the accounting does not depend on what an assertion runs). */
    static List<Check> holding() { return registeredIds().stream().map(id -> new Check(id, c -> new Result(true, "stand-in"))).toList(); }

    Path manifestWith(java.util.function.Consumer<ArrayNode> edit) throws Exception {
        ObjectNode m = (ObjectNode) Json.read(MANIFEST);
        edit.accept((ArrayNode) m.path("assertions"));
        Path f = tmp.resolve("manifest-" + UUID.randomUUID() + ".json");
        Json.write(f, m);
        return f;
    }

    @Test void everyManifestIdHasARegisteredAssertion() {
        List<String> manifest = manifestIds(MANIFEST), registered = registeredIds();
        assertEquals(82, manifest.size());
        assertEquals(manifest.size(), new HashSet<>(manifest).size(), "no id twice in the manifest");
        assertEquals(registered.size(), new HashSet<>(registered).size(), "no id registered twice");
        assertEquals(new TreeSet<>(manifest), new TreeSet<>(registered), "every manifest id is registered and every registered id is in the manifest");
        for (JsonNode a : Json.read(MANIFEST).path("assertions")) {
            String id = a.path("id").asText();
            boolean pr5 = id.startsWith("F-GEN-") || id.equals("I-20") || id.equals("I-23"), pr6 = id.startsWith("G-SELF-") || id.equals("C-1");
            assertEquals(pr6 ? 6 : pr5 ? 5 : id.startsWith("F-") ? 1 : id.startsWith("S-") ? 2 : id.startsWith("T-") ? 3 : 4, a.path("pr").asInt(), a.toString());
            assertTrue(a.path("id").asText().matches("[FSTIJCG]-[A-Z0-9-]+[b-f]?"), a.toString());
            assertFalse(a.path("description").asText().isBlank(), a.toString());
        }
    }

    @Test void theAssertionsThatNeedNoInterpreterHoldOnTheFixtures() throws Exception {
        Path manifest = manifestWith(a -> { for (int i = a.size() - 1; i >= 0; i--) if (NEEDS_ORACLE.contains(a.get(i).path("id").asText())) a.remove(i); });
        List<Check> checks = CoreGate.registered().stream().filter(c -> !NEEDS_ORACLE.contains(c.id())).toList();
        Run r = run(manifest, checks);
        assertEquals(List.of(), r.list("failed"), String.join("\n", r.lines()));
        assertEquals(0, r.exit());
        assertEquals("assertions run 32 / expected 32", r.last());
        for (String id : List.of("S-SMOKE-VISIT", "S-SMOKE-PLC", "S-REVISIONS", "S-PROGRESS", "S-W1", "S-DETERMINISM",
                "T-PROGRESS-TRACKER", "T-SMOKE-INCIDENT", "T-CASCADE", "T-DELETE", "T-LEDGER-2", "F-GEN-NEW", "F-GEN-REFUSES-UNREGISTERED", "F-GEN-FLOOR",
                "F-GEN-LIFECYCLE", "F-GEN-PROMOTE-REQUIRES-REPORT"))
            assertTrue(r.lines().stream().anyMatch(l -> l.startsWith("PASS " + id + " ")), id + " in " + r.lines());
    }

    @Test void anAssertionNotRunFailsTheGate() throws Exception {
        Path manifest = manifestWith(a -> a.addObject().put("id", "F-NOT-REGISTERED").put("pr", 1).put("description", "listed, never registered"));
        Run r = run(manifest, holding());
        assertEquals(1, r.exit());
        assertEquals(List.of("F-NOT-REGISTERED"), r.list("notRun"));
        assertEquals(List.of(), r.list("failed"));
        assertEquals("assertions run 82 / expected 83", r.last());
        assertEquals(1, r.report().path("exit").asInt());
        assertEquals(83, r.report().path("assertionsExpected").asInt());
        assertEquals(82, r.report().path("assertionsRun").asInt());
    }

    @Test void anUnknownAssertionFailsTheGate() {
        List<Check> checks = new ArrayList<>(holding());
        checks.add(new Check("F-UNKNOWN", c -> new Result(true, "holds, but is not in the manifest")));
        Run r = run(MANIFEST, checks);
        assertEquals(1, r.exit());
        assertEquals(List.of("F-UNKNOWN"), r.list("failed"));
        assertEquals(List.of(), r.list("notRun"));
        assertEquals("assertions run 82 / expected 82", r.last());

        List<Check> twice = new ArrayList<>(holding());
        twice.add(new Check("F-JCS-JAVA", c -> new Result(true, "a second run of one id")));
        Run d = run(MANIFEST, twice);
        assertEquals(1, d.exit());
        assertEquals(List.of("F-JCS-JAVA"), d.list("failed"));
    }

    @Test void aFailingAssertionFailsTheGate() {
        List<Check> checks = new ArrayList<>(holding());
        checks.set(3, new Check(checks.get(3).id(), c -> new Result(false, "does not hold")));
        Run r = run(MANIFEST, checks);
        assertEquals(1, r.exit());
        assertEquals(List.of(checks.get(3).id()), r.list("failed"));
        assertEquals("assertions run 82 / expected 82", r.last());
    }

    @Test void aCrashStillWritesAFailureReport() throws Exception {
        List<Check> checks = new ArrayList<>(holding());
        checks.set(0, new Check(checks.get(0).id(), c -> { throw new IllegalStateException("boom"); }));
        Run crashed = run(MANIFEST, checks);
        assertEquals(1, crashed.exit());
        assertEquals(List.of("F-JCS-JAVA"), crashed.list("failed"));
        assertTrue(crashed.lines().stream().anyMatch(l -> l.startsWith("FAIL F-JCS-JAVA crashed: java.lang.IllegalStateException: boom")), crashed.lines().toString());

        Run noManifest = run(tmp.resolve("no-such-manifest.json"), holding());
        assertEquals(1, noManifest.exit());
        assertEquals(1, noManifest.report().path("exit").asInt());
        assertTrue(noManifest.report().path("error").asText().contains("no-such-manifest.json"), noManifest.report().toString());
        assertEquals("assertions run 0 / expected 0", noManifest.last());

        Path empty = Files.createDirectories(tmp.resolve("empty-root"));   // every fixture missing: every assertion fails, none is skipped
        Run missing = run(empty, MANIFEST, CoreGate.registered().stream().filter(c -> !NEEDS_ORACLE.contains(c.id())).toList());
        assertEquals(1, missing.exit());
        Set<String> wantFailed = new TreeSet<>(registeredIds());
        wantFailed.removeAll(NEEDS_ORACLE);
        wantFailed.remove("F-JOURNAL-REDUCERS");   // inline journals: the one assertion that reads no file
        assertEquals(wantFailed, new TreeSet<>(missing.list("failed")), missing.lines().toString());
        assertEquals(NEEDS_ORACLE, new TreeSet<>(missing.list("notRun")));
        assertEquals("assertions run 32 / expected 82", missing.last());
        assertTrue(missing.report().path("fixtureDigests").path("registry").isNull());
    }

    @Test void theReportRecordsPinsAndDigests() throws Exception {
        Run r = run(MANIFEST, holding());
        assertEquals(0, r.exit());
        assertEquals("assertions run 82 / expected 82", r.last());
        JsonNode rep = r.report();
        List<String> fields = new ArrayList<>();
        rep.fieldNames().forEachRemaining(fields::add);
        assertEquals(REPORT_FIELDS, fields);
        assertEquals("core", rep.path("scope").asText());
        assertEquals("core", rep.path("engine").asText());
        assertEquals(Canonical.digestOf(Files.readAllBytes(MANIFEST)), rep.path("manifestSha256").asText());
        assertEquals(82, rep.path("assertionsExpected").asInt());
        assertEquals(82, rep.path("assertionsRun").asInt());
        assertEquals(20261109L, rep.path("seed").asLong());
        assertEquals("io.github.erdtman:java-json-canonicalization:1.1", rep.path("pins").path("jcs").path("java").asText());
        assertTrue(rep.path("pins").has("python") && rep.path("pins").path("jcs").has("python"));
        assertTrue(rep.path("pins").path("jdk").asText().contains(System.getProperty("java.runtime.version")));
        assertEquals("unpacked", rep.path("pins").path("forseti").path("artifactSha256").asText(), "the tests run from classes, not the jar");
        for (String k : List.of("registry", "registryNext", "registryMismatch", "upstream", "vectors", "schemas", "scenarios"))
            assertTrue(rep.path("fixtureDigests").path(k).asText("").matches("[0-9a-f]{64}"), k);
        assertEquals(CoreGate.treeDigest(ROOT.resolve("fixtures/registry")), rep.path("fixtureDigests").path("registry").asText());
        assertNotEquals(rep.path("fixtureDigests").path("registry").asText(), rep.path("fixtureDigests").path("registryNext").asText());
        assertTrue(rep.path("startedAt").asText().compareTo(rep.path("finishedAt").asText()) <= 0);
        assertTrue(rep.path("error").isNull());
        assertEquals(List.of(), r.list("intendedFailures"));
    }

    @Test void anUnlistedNegativeControlAndAnotherEngineAreUsageErrors() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CoreGate.control(ROOT, "nope"));
        assertTrue(e.getMessage().contains("not in " + CoreGate.CONTROLS), e.getMessage());
        PrintStream sink = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        assertEquals(2, Main.run(new String[] {"gate", "--engine", "flink"}, sink, sink));
        assertEquals(2, Main.run(new String[] {"gate", "--engine", "core", "--negative-control", "nope", "--out", tmp.resolve("nc").toString()}, sink, sink));
        assertFalse(Files.exists(tmp.resolve("nc").resolve("report.json")), "a usage error runs nothing");
        assertEquals(2, Main.run(new String[] {"gate", "--engine", "core", "--unknown", "x"}, sink, sink));
    }

    // --- the gate on one store (plan Task 20, decisions 6-12) ----------------------------------------------------------------------

    static final Path SMOKE = ROOT.resolve("fixtures/recordings/smoke"), REGISTRY = ROOT.resolve("fixtures/registry");

    static PrintStream sink(ByteArrayOutputStream b) { return new PrintStream(b, true, StandardCharsets.UTF_8); }

    /** A context of {@code root} whose selection is {@code scenarios} (a one-shard plan), on its own temporary directories. */
    CoreGate.Context context(List<String> scenarios) throws Exception {
        Path base = Files.createDirectories(tmp.resolve("ctx-" + UUID.randomUUID()));
        CoreGate.Context c = new CoreGate.Context(ROOT, Files.createDirectories(base.resolve("tmp")), base.resolve("out").resolve("logs"), PYTHON);
        Files.createDirectories(c.logs);
        c.shard = new Shards.Shard("test", scenarios, List.of(), List.of());
        return c;
    }

    @Test void stateIsRefusedWithAggregateAndWithANegativeControl() throws Exception {
        Path out = tmp.resolve("agg"), shards = Files.createDirectories(tmp.resolve("shards"));
        ByteArrayOutputStream e1 = new ByteArrayOutputStream(), e2 = new ByteArrayOutputStream();
        PrintStream none = sink(new ByteArrayOutputStream());
        assertEquals(2, Main.run(new String[] {"gate", "--engine", "core", "--aggregate", shards.toString(), "--out", out.toString(), "--state", out.resolve("state").toString()}, none, sink(e1)));
        assertTrue(e1.toString(StandardCharsets.UTF_8).contains("--state is for a full or a --shard run, not --aggregate"), e1.toString(StandardCharsets.UTF_8));
        assertEquals(2, Main.run(new String[] {"gate", "--engine", "core", "--negative-control", "any", "--out", out.toString(), "--state", out.resolve("state").toString()}, none, sink(e2)));
        assertTrue(e2.toString(StandardCharsets.UTF_8).contains("--state is for a full or a --shard run, not --negative-control"), e2.toString(StandardCharsets.UTF_8));
        assertFalse(Files.exists(out.resolve("report.json")), "a usage error runs nothing");
        assertFalse(Files.exists(out.resolve("state")), "a usage error creates no store");
    }

    @Test void stateOutsideTheOutputDirectoryIsAUsageError() throws Exception {
        Path out = tmp.resolve("full");
        for (List<String> layout : List.of(List.<String>of(), List.of("--shard", "hand-made"))) {
            List<String> args = new ArrayList<>(List.of("gate", "--engine", "core", "--out", out.toString(), "--state", tmp.resolve("elsewhere").resolve("state").toString()));
            args.addAll(layout);
            ByteArrayOutputStream e = new ByteArrayOutputStream();
            assertEquals(2, Main.run(args.toArray(String[]::new), sink(new ByteArrayOutputStream()), sink(e)), layout.toString());
            assertTrue(e.toString(StandardCharsets.UTF_8).contains("the store of a gate run is <out>/state"), e.toString(StandardCharsets.UTF_8));
            assertFalse(Files.exists(out.resolve("report.json")), "a usage error runs nothing");
        }
        assertEquals(tmp.toRealPath().resolve("full").resolve("state"), CoreGate.real(out.resolve("x").resolve("..").resolve("state")), "compared by real path");
    }

    @Test void theReportNamesItsKindAndLayout() throws Exception {
        Run full = run(MANIFEST, holding());
        assertEquals(0, full.exit(), full.lines().toString());
        JsonNode f = full.report();
        assertEquals("gate", f.path("kind").asText());
        assertEquals("full", f.path("layout").asText());
        assertTrue(f.path("negativeControl").isNull());
        assertTrue(f.path("shard").isNull());
        assertTrue(f.path("summariesSha256").asText("").matches("[0-9a-f]{64}"));
        assertEquals(0, f.path("shards").size());
        for (JsonNode r : f.path("results")) assertTrue(r.has("shard") && r.path("shard").isNull(), r.toString());

        Path control = tmp.resolve("control");
        Run nc = run(new CoreGate.Options(ROOT, MANIFEST, control, PYTHON, "some-control", List.of("F-JCS-JAVA"), CoreGate.DEFAULT_SEED, "test"), holding());
        assertEquals("negative-control", nc.report().path("kind").asText());
        assertEquals("some-control", nc.report().path("negativeControl").asText());
        assertEquals("full", nc.report().path("layout").asText());

        // a shard run of a two-shard plan whose shard "some" makes nothing: layout shard, every result under its shard
        Path root = Files.createDirectories(tmp.resolve("root"));
        ObjectNode planJson = Json.object();
        ArrayNode shards = planJson.putArray("shards");
        ObjectNode all = shards.addObject().put("name", "everything");
        Scenarios.all().forEach(all.putArray("scenarios")::add);
        Scenarios.SHUFFLED.forEach(all.putArray("shuffled")::add);
        ArrayNode first = all.putArray("assertions");
        ArrayNode planned = all.putArray("generationRuns");
        Scenarios.GENERATION_RUNS.forEach(g -> planned.add(g.name()));
        ObjectNode some = shards.addObject().put("name", "some");
        some.putArray("scenarios");
        some.putArray("shuffled");
        some.putArray("generationRuns");
        ArrayNode ids = some.putArray("assertions");
        for (String id : manifestIds(MANIFEST)) if (!Injections.AGGREGATE.contains(id)) { if (first.isEmpty()) first.add(id); else ids.add(id); }
        Injections.AGGREGATE.forEach(planJson.putArray("aggregate")::add);
        Json.write(root.resolve(Shards.PLAN), planJson);
        Run shard = run(new CoreGate.Options(root, MANIFEST, tmp.resolve("shard-out"), PYTHON, null, List.of(), CoreGate.DEFAULT_SEED, "test", "some", null), holding());
        JsonNode s = shard.report();
        assertEquals(0, shard.exit(), shard.lines().toString());
        assertEquals("gate", s.path("kind").asText());
        assertEquals("shard", s.path("layout").asText());
        assertEquals("some", s.path("shard").asText());
        assertEquals(Canonical.digestOf(Files.readAllBytes(tmp.resolve("shard-out").resolve(Shards.SUMMARIES))), s.path("summariesSha256").asText());
        assertFalse(s.path("results").isEmpty());
        for (JsonNode r : s.path("results")) assertEquals("some", r.path("shard").asText(), r.toString());
    }

    @Test void theGateRegistersThroughTheVerbInAFixedOrder() throws Exception {
        List<String> selection = List.of("smoke-incident", "smoke", "smoke-plc");   // registered in Scenarios.all() order, not in this order
        Map<String, Long> first = new LinkedHashMap<>(), second = new LinkedHashMap<>();
        for (Map<String, Long> table : List.of(first, second)) {
            CoreGate.Context c = context(selection);
            for (Map.Entry<String, Scenarios.Run> e : Scenarios.table(c).entrySet()) {
                assertNull(e.getValue().failure, e.getValue().failure);
                table.put(e.getKey(), e.getValue().generation.generation());
            }
            assertEquals(List.of(), GateEvidence.validate(ROOT, c.out, bundle(c), new Summaries().toJson()));
            List<yggdrasil.forseti.core.generation.GenerationStore.Entry> rows = new yggdrasil.forseti.cli.generation.FileGenerationStore(c.pipeline.dir()).all();
            assertEquals(3, rows.size());
            for (var x : rows) assertEquals("registered", x.row().status(), "the pipeline store holds only registered rows");
            JsonNode gens = c.evidence.generations();
            assertEquals(3, gens.size());
            for (JsonNode g : gens) {
                assertEquals("pipeline", g.path("store").asText());
                assertEquals("online", g.path("variant").asText());
                assertTrue(g.path("assertion").isNull());
            }
        }
        assertEquals(Map.of("smoke", 1L, "smoke-plc", 2L, "smoke-incident", 3L), first);
        assertEquals(List.of("smoke", "smoke-plc", "smoke-incident"), new ArrayList<>(first.keySet()));
        assertEquals(first, second, "the numbers of a gate run are a function of its selection");
    }

    static ObjectNode bundle(CoreGate.Context c) {
        ObjectNode b = Json.object();
        b.set("pins", Json.object());
        b.set("generations", c.evidence.generations());
        b.set("runs", c.evidence.runs());
        return b;
    }

    @Test void aRunWithoutStateMarksEveryGenerationIsolated() throws Exception {
        List<Check> checks = new ArrayList<>(holding());
        int i = registeredIds().indexOf("F-GEN-NEW");
        checks.set(i, new Check("F-GEN-NEW", c -> {
            GateEvidence.register(c, c.pipeline, REGISTRY, SMOKE, "smoke", GateEvidence.ONLINE, null);
            GateEvidence.register(c, c.assertionStore(), REGISTRY, SMOKE, "smoke", GateEvidence.SELF_TEST, c.current);
            GateEvidence.register(c, c.assertionStore(), REGISTRY, SMOKE, "smoke", GateEvidence.SELF_TEST, c.current);
            return new Result(true, "registered");
        }));
        Run r = run(MANIFEST, checks);
        assertEquals(0, r.exit(), r.lines().toString());
        JsonNode gens = r.report().path("generations");
        assertEquals(3, gens.size());
        assertEquals(List.of("pipeline", "assertion:F-GEN-NEW", "assertion:F-GEN-NEW#2"), List.of(gens.get(0).path("store").asText(), gens.get(1).path("store").asText(), gens.get(2).path("store").asText()));
        for (JsonNode g : gens) {
            assertEquals("isolated", g.path("state").asText(), g.toString());
            assertEquals(1, g.path("generation").asLong(), "each store starts at generation 1");
            assertTrue(g.path("shard").isNull());
        }
        assertEquals(gens.get(1).path("registeredRowSha256"), gens.get(2).path("registeredRowSha256"), "identical registrations under the fixture clock");
        assertTrue(gens.get(1).path("assertion").asText().equals("F-GEN-NEW") && gens.get(0).path("assertion").isNull());
    }

    /** {@code smoke} as generation 1 of one store and as generation 3 of another: the summaries agree on every compared field. Runs the
     *  harness and the oracle ({@code -Dforseti.python}, default {@code python}, with the oracle installed). */
    @Test void aScenarioSummaryDoesNotDependOnItsGenerationNumber() throws Exception {
        CoreGate.Context a = context(List.of("smoke")), b = context(List.of("smoke"));
        for (int k = 0; k < 2; k++) {
            ByteArrayOutputStream so = new ByteArrayOutputStream();
            int exit = yggdrasil.forseti.cli.generation.GenerationCli.run(new String[] {"new", "--state", b.pipeline.dir().toString(), "--mappings", REGISTRY.resolve("mappings").toString(),
                    "--trusted-keys", ROOT.resolve("fixtures/trusted-keys.jsonl").toString(), "--stream", "line2", "--rule", "line2-attribution@1.0.0"}, sink(so), sink(new ByteArrayOutputStream()),
                java.time.Clock.fixed(java.time.Instant.ofEpochMilli(GateSigner.now(REGISTRY.resolve("mappings"), "line2")), java.time.ZoneOffset.UTC));
            assertEquals(0, exit, so.toString(StandardCharsets.UTF_8));
        }
        JsonNode sa = Summaries.fromPipeline(a).runs.get("smoke"), sb = Summaries.fromPipeline(b).runs.get("smoke");
        assertEquals(1, Scenarios.table(a).get("smoke").generation.generation());
        assertEquals(3, Scenarios.table(b).get("smoke").generation.generation());
        assertTrue(sa.path("ok").asBoolean(), sa.toString());
        assertTrue(sb.path("ok").asBoolean(), sb.toString());
        Set<String> fields = new TreeSet<>();
        sa.fieldNames().forEachRemaining(fields::add);
        sb.fieldNames().forEachRemaining(fields::add);
        fields.removeAll(Shards.NOT_COMPARED);
        for (String f : fields) assertTrue(Shards.same(sa.path(f), sb.path(f)), f + ": " + sa.path(f) + " vs " + sb.path(f));
        assertNotEquals(a.evidence.runs().get(0).path("generation"), b.evidence.runs().get(0).path("generation"));
    }

    @Test void aFullRunWritesItsSummariesAndBindsTheirDigest() throws Exception {
        Path out = tmp.resolve("full");
        Run r = run(new CoreGate.Options(ROOT, MANIFEST, out, PYTHON, null, List.of(), CoreGate.DEFAULT_SEED, "test"), holding());
        assertEquals(0, r.exit(), r.lines().toString());
        byte[] summaries = Files.readAllBytes(out.resolve(Shards.SUMMARIES));
        assertEquals(Canonical.digestOf(summaries), r.report().path("summariesSha256").asText());
        JsonNode s = Json.read(summaries);
        assertTrue(s.path("runs").isArray() && s.path("shuffled").isArray(), s.toString());
        assertEquals(List.of(), GateEvidence.validate(ROOT, out), "the full run's bundle passes the validator as written");
    }

    /** An aggregate run reads the shard bundles and evaluates the aggregate assertions: it registers no generation (no {@code state/},
     *  no {@code generation new}), keeps no run and no journals, and its {@code generations[]} and {@code runs[]} are the shards' merged,
     *  unchanged. The shard bundles state this test run's own pins, so the aggregate run passes. */
    @Test void theAggregateRegistersNothingAndRunsNothing() throws Exception {
        ObjectNode common = Json.object();
        common.put("manifestSha256", Canonical.digestOf(Files.readAllBytes(MANIFEST)));
        Json.putSafe(common, "seed", CoreGate.DEFAULT_SEED);
        common.set("pins", CoreGate.pins(context(List.of())));
        common.set("fixtureDigests", CoreGate.fixtureDigests(ROOT));
        GateBundles.Aggregate a = GateBundles.aggregate(tmp.resolve("agg-source"), GateBundles.Options.DEFAULT, common);
        Path out = tmp.resolve("agg-out");
        Run r = run(new CoreGate.Options(ROOT, MANIFEST, out, PYTHON, null, List.of(), CoreGate.DEFAULT_SEED, "test", null, a.shards()), holding());
        assertEquals(0, r.exit(), r.lines().toString());
        assertEquals("assertions run 82 / expected 82", r.last());
        JsonNode rep = r.report(), source = a.reportJson();
        assertEquals("aggregate", rep.path("layout").asText());
        assertTrue(Shards.same(source.get("generations"), rep.get("generations")), "generations[] are the shards' merged, unchanged");
        assertTrue(Shards.same(source.get("runs"), rep.get("runs")), "runs[] are the shards' merged, unchanged");
        assertTrue(Shards.same(source.get("shards"), rep.get("shards")));
        for (String d : List.of(GateEvidence.STATE, GateEvidence.RUNS, GateEvidence.JOURNALS)) assertFalse(Files.exists(out.resolve(d)), d + " in an aggregate run's output");
        try (java.util.stream.Stream<Path> logs = Files.exists(out.resolve("logs")) ? Files.list(out.resolve("logs")) : java.util.stream.Stream.empty()) {
            assertEquals(List.of(), logs.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("generation-new-") || n.startsWith("harness-")).toList());
        }
        Set<String> made = new TreeSet<>();
        for (JsonNode g : rep.get("generations")) made.add(g.path("shard").asText());
        assertEquals(new TreeSet<>(Shards.load(ROOT).names()), made, "every generation listed is a shard's");
    }

    /** The generations of {@code F-GEN-FLOOR} and of {@code I-20}'s rule change live in their own assertion stores, isolated, also when the
     *  pipeline store is a kept {@code --state} store; the pipeline store's floor stays 1 while theirs rise to 2. */
    @Test void floorAndRuleChangeGenerationsAreIsolated() throws Exception {
        CoreGate.Context c = context(List.of("smoke"));
        c.pipeline = new GateEvidence.Store(GateEvidence.PIPELINE, Files.createDirectories(c.out.resolve(GateEvidence.STATE)), true);
        GateEvidence.register(c, c.pipeline, REGISTRY, SMOKE, "smoke", GateEvidence.ONLINE, null);
        c.current = "F-GEN-FLOOR";
        Result floor = GenerationAssertions.floor(c);
        assertTrue(floor.ok(), floor.evidence());
        c.current = null;
        Scenarios.Run base = new Scenarios.Run();
        base.name = "smoke";
        base.recording = SMOKE;
        base.manifest = Json.read(SMOKE.resolve("manifest.json"));
        Scenarios.GenerationRun g = Scenarios.generationRun("baseline@rule-1.0.1");
        Scenarios.Run b = Scenarios.ruleChange(c, g, base, REGISTRY);
        assertNull(b.failure, b.failure);
        assertEquals("assertion:I-20", b.generation.store().name());
        assertEquals(2, b.generation.generation());
        assertEquals(1, b.previous.generation());
        assertEquals("1.0.1", b.rule.path("attributionRuleVersion").asText());
        JsonNode gens = c.evidence.generations();
        assertEquals(5, gens.size(), gens.toString());
        assertEquals("shared", gens.get(0).path("state").asText());
        for (int i = 1; i < 5; i++) {
            JsonNode e = gens.get(i);
            assertEquals("isolated", e.path("state").asText(), e.toString());
            assertTrue(e.path("store").asText().equals("assertion:" + e.path("assertion").asText()), e.toString());
        }
        assertEquals(List.of("F-GEN-FLOOR", "F-GEN-FLOOR", "I-20", "I-20"), List.of(gens.get(1).path("assertion").asText(), gens.get(2).path("assertion").asText(),
            gens.get(3).path("assertion").asText(), gens.get(4).path("assertion").asText()));
        assertEquals(List.of("self-test", "rule-change"), List.of(gens.get(3).path("variant").asText(), gens.get(4).path("variant").asText()));
        assertEquals(1L, new yggdrasil.forseti.core.release.FloorStore(c.pipeline.dir()).get("line2").map(yggdrasil.forseti.core.release.FloorStore.Floor::headVersion).orElse(null));
        assertEquals(2L, new yggdrasil.forseti.core.release.FloorStore(b.generation.store().dir()).get("line2").map(yggdrasil.forseti.core.release.FloorStore.Floor::headVersion).orElse(null));
        assertEquals(1, new yggdrasil.forseti.cli.generation.FileGenerationStore(c.pipeline.dir()).all().size(), "the pipeline store holds only its own registration");
        assertEquals(List.of(), GateEvidence.validate(ROOT, c.out, bundle(c), new Summaries().toJson()));
    }

    /** In a full bundle each generation run is listed once in {@code runs[]} with its variant and store, its generation with the same
     *  variant; a second entry for it, or the entry under another variant, is a coverage problem. */
    @Test void aGenerationRunIsListedOnceWithItsVariant() throws Exception {
        GateBundles.Bundle b = GateBundles.full(tmp.resolve("full"), GateBundles.Options.DEFAULT);
        ObjectNode rep = b.reportJson();
        JsonNode summaries = b.summariesJson();
        assertEquals(List.of(), GateEvidence.validate(ROOT, b.dir(), rep, summaries));
        for (Scenarios.GenerationRun g : Scenarios.GENERATION_RUNS) {
            List<JsonNode> entries = new ArrayList<>();
            for (JsonNode r : rep.get("runs")) if (r.path("scenario").asText().equals(g.scenario()) && r.path("variant").asText().equals(g.variant())) entries.add(r);
            assertEquals(1, entries.size(), g.name());
            assertEquals(g.store(), entries.get(0).path("store").asText(), g.name());
            JsonNode gen = null;
            for (JsonNode x : rep.get("generations"))
                if (x.path("store").asText().equals(g.store()) && x.path("registeredRowSha256").asText().equals(entries.get(0).path("registeredRowSha256").asText())) gen = x;
            assertNotNull(gen, g.name());
            assertEquals(g.variant(), gen.path("variant").asText(), g.name());
            assertTrue(summaries.path("generationRuns").has(g.name()), g.name());
            assertEquals(g.store().equals("pipeline") ? "shared" : "isolated", gen.path("state").asText(), g.name());
        }
        String replay = "horizon-planned-late@replay";
        ObjectNode twice = rep.deepCopy();
        ((ArrayNode) twice.get("runs")).add(GateBundles.generationRunEntry(twice, replay).deepCopy().put("runId", "a-second-entry"));
        assertTrue(GateEvidence.validate(ROOT, b.dir(), twice, summaries).contains(replay + ": two run entries for one summary"));
        ObjectNode other = rep.deepCopy();
        GateBundles.generationRunEntry(other, replay).put("variant", "online");
        List<String> p = GateEvidence.validate(ROOT, b.dir(), other, summaries);
        assertTrue(p.contains(replay + ": a summary without a generation-run run entry"), p.toString());
        assertTrue(p.contains("horizon-planned-late: two run entries for one summary"), p.toString());
    }

    /** Full layout: a shuffled run whose oracle comparison stops one cut short, and one whose comparison differs at one cut, are each a
     *  coverage problem and exit 1. The run is a real {@code --shuffle} harness run of {@code smoke} on a pipeline generation; only its
     *  summary is made by hand. */
    @Test void aFullRunWithAMissingOrFailedShuffledComparisonExitsOne() throws Exception {
        for (String fault : List.of("short", "differs")) {
            List<Check> checks = new ArrayList<>(holding());
            int i = registeredIds().indexOf("C-6-STAGE1");
            checks.set(i, new Check("C-6-STAGE1", c -> {
                GateEvidence.Registered g = GateEvidence.register(c, c.pipeline, REGISTRY, SMOKE, "smoke", GateEvidence.ONLINE, null);
                Path out = c.newDir("shuffle").resolve("out");
                ByteArrayOutputStream so = new ByteArrayOutputStream();
                int exit = Main.run(new String[] {"run", "--engine", "core", "--recording", SMOKE.toString(), "--registry", REGISTRY.toString(), "--state", c.pipeline.dir().toString(),
                    "--generation", Long.toString(g.generation()), "--out", out.toString(), "--seed", "7", "--shuffle"}, sink(so), sink(new ByteArrayOutputStream()));
                if (exit != 0) return new Result(false, "harness exit " + exit + ": " + so);
                GateEvidence.keep(c, g, out, SMOKE, "smoke", GateEvidence.ONLINE, 7, true, "smoke.shuffle-7");
                JsonNode cutLog = Json.read(out.resolve("cuts.json"));
                ObjectNode x = Json.object().put("name", "smoke");
                Json.putSafe(x, "seed", 7L);
                x.putNull("failure");
                x.put("summary", "smoke seed 7: by hand");
                x.put("recordingManifestSha256", Canonical.digestOf(Files.readAllBytes(SMOKE.resolve("manifest.json"))));
                ArrayNode planned = x.putArray("plannedCutIds"), logged = x.putArray("cutLogIds"), compared = x.putArray("comparedCutIds");
                for (JsonNode k : cutLog.path("cuts")) { planned.add(k.path("cutId").asLong()); logged.add(k.path("cutId").asLong()); compared.add(k.path("cutId").asLong()); }
                Json.putSafe(x, "cutLogCuts", cutLog.path("cuts").size());
                if (fault.equals("short")) compared.remove(compared.size() - 1);
                Json.putSafe(x, "cutsCompared", compared.size());
                x.put("equal", !fault.equals("differs"));
                x.put("ok", false);
                Summaries s = new Summaries();
                s.shuffled.put("smoke@7", x);
                c.memo(Summaries.MEMO, () -> s);
                return new Result(true, "a shuffled run and its summary");
            }));
            Run r = run(MANIFEST, checks);
            assertEquals(1, r.exit(), fault + ": " + r.lines());
            assertTrue(r.lines().stream().anyMatch(l -> l.startsWith("COVERAGE smoke seed 7")), fault + ": " + r.lines());
            if (fault.equals("short")) assertTrue(r.lines().stream().anyMatch(l -> l.startsWith("COVERAGE smoke seed 7: the gate compared cuts")), r.lines().toString());
            else assertTrue(r.lines().contains("COVERAGE smoke seed 7: its comparison with the oracle is not equal (equal false)"), r.lines().toString());
            assertTrue(r.report().path("error").asText().startsWith("java.lang.IllegalStateException: coverage: "), r.report().toString());
            assertEquals(1, r.report().path("runs").size());
        }
    }
}
