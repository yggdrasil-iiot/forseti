package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.CoreGate.Proc;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.rules.RuleSet;

/** Every scenario of the core gate made once per gate run, in parallel (plan Task 19): the recording is built from its committed
 *  scenario into {@code build/recordings/<name>/} and checked against its committed lock before anything runs (the three smoke
 *  recordings are checked in place under {@code fixtures/recordings/}); a generation is registered by hand in a fresh temporary state
 *  ({@link Stage1Assertions#register}); the harness runs as a child JVM ({@code forseti run --engine core}, its own heap); the oracle runs
 *  on the harness's cut log ({@code --python}); the journals are folded cut by cut and compared with the oracle at every cut
 *  ({@link Comparison}), the integrity checks recorded ({@link RunFold}, {@link Integrity}) and the ground truth asserted
 *  ({@link Expectations}). The run and oracle directories are deleted once folded; what the assertions read stays in memory.
 *
 *  <p>Parallelism: {@code -Dforseti.gate.parallel=<n>} hand-made scenarios at a time (default half the processors, at most 4), the
 *  five horizon recordings one at a time beside them (each harness gets {@code -Dforseti.gate.horizonXmx}, default 6g). {@code -Dforseti.gate.skipHorizon=true} (or
 *  {@code FORSETI_GATE_SKIP_HORIZON=true}) leaves the horizon scenarios out for local iteration; every assertion that needs one then
 *  fails as not run, so a gate run that skips them never passes. */
final class Scenarios {
    private Scenarios() {}

    static final List<String> SMOKE = List.of("smoke", "smoke-plc", "smoke-incident");
    static final List<String> HAND_MADE = List.of("baseline", "held-back-10min", "redelivery", "overlap", "bad-quality", "clock-plus-30", "clock-plus-90",
        "corrections", "corrections-10b-before", "corrections-10b-after", "corrections-10c", "session", "quiet-occupancy", "quiet-occupancy-control",
        "plc-stamped", "plc-nocheck", "plc-held-back", "stage2", "stage2-early", "cascade");
    static final List<String> HORIZON = List.of("horizon-no-departure", "horizon-reopened", "horizon-correction-beyond", "horizon-planned-late", "plc-horizon-no-end");
    /** C-6: the default run and three {@code --shuffle} runs with these seeds. */
    static final List<Long> SHUFFLE_SEEDS = List.of(CoreGate.DEFAULT_SEED, 7L, 20261110L);
    static final List<String> SHUFFLED = List.of("baseline", "stage2");

    static boolean skipHorizon() {
        return Boolean.getBoolean("forseti.gate.skipHorizon") || "true".equalsIgnoreCase(System.getenv("FORSETI_GATE_SKIP_HORIZON"));
    }
    static List<String> all() { List<String> l = new ArrayList<>(HAND_MADE); l.addAll(HORIZON); l.addAll(SMOKE); return l; }

    /** One scenario, run and compared. */
    static final class Run {
        String name, failure, lockLine;
        boolean synthetic;
        Path recording;
        JsonNode manifest, expected, cutLog, runJson;
        String rulesDigest;
        RuleSet rules;
        RunFold fold;
        Comparison.Tally tally;
        Integrity.Measures measures;
        Expectations.Outcome outcome;
        final Map<Long, Set<String>> open = new TreeMap<>();
        final Map<String, Long> ms = new LinkedHashMap<>();
        final Map<String, Integer> visitKeys = new HashMap<>();

        boolean ok() { return failure == null && tally.equal() && fold.findings.clean() && outcome.ok(); }
        /** The part of every injection assertion's evidence that says the scenario ran, matched the oracle, held integrity and ground truth. */
        String summary() {
            if (failure != null) return name + ": " + failure;
            return name + ": " + lockLine + "; " + tally.describe() + "; integrity " + (fold.findings.clean() ? "clean" : fold.findings.describe()) + "; " + outcome.describe();
        }
        List<Long> cutIds() { List<Long> l = new ArrayList<>(); for (JsonNode c : cutLog.path("cuts")) l.add(c.path("cutId").asLong()); return l; }
        long lastCut() { List<Long> l = cutIds(); return l.get(l.size() - 1); }
        JsonNode perStation(long cutId) {
            for (JsonNode c : cutLog.path("cuts")) if (c.path("cutId").asLong() == cutId) return c.path("perStation");
            return Json.object();
        }
        long w1(long cutId, String station) { return perStation(cutId).path(station).path("W1").asLong(); }
        long t0() { return manifest.path("T0").asLong(); }
        JsonNode visit(String ref) {
            for (JsonNode v : expected.path("visits")) if (v.path("visitRef").asText().equals(ref)) return v;
            throw new IllegalStateException(name + ": no ground-truth visit " + ref);
        }
        String key(String ref) { return RunFold.keyText(visit(ref).path("key")); }
        /** The ground-truth visit at {@code station} entering at {@code T0 + enterMs}. */
        JsonNode visitAt(String station, long enterMs) {
            for (JsonNode v : expected.path("visits")) if (v.path("station").asText().equals(station) && v.path("enter").asLong() == t0() + enterMs) return v;
            throw new IllegalStateException(name + ": no ground-truth visit at " + station + " entering at T0 + " + enterMs);
        }
        JsonNode transition(String kind, String ref) {
            for (JsonNode t : expected.path("transitions")) if (t.path("assertion").asText().equals(kind) && t.path("ref").asText().equals(ref)) return t;
            throw new IllegalStateException(name + ": no " + kind + " transition of " + ref);
        }
        /** The settled cut: the first completed cut at which every station that has telemetry reports W1 at or past settleTargetW. */
        Long settledCut() {
            long target = manifest.path("settleTargetW").asLong();
            long last = lastCut();
            Set<String> active = new HashSet<>();
            perStation(last).fieldNames().forEachRemaining(s -> { if (w1(last, s) > -Json.MAX_SAFE) active.add(s); });
            for (long c : cutIds()) {
                boolean all = true;
                for (String s : active) if (w1(c, s) < target) all = false;
                if (all) return c;
            }
            return null;
        }
        List<Long> deadlines() {
            List<Long> d = new ArrayList<>();
            for (JsonNode x : manifest.path("deadlines")) d.add(x.path("at").asLong());
            for (JsonNode x : expected.path("transitions")) d.add(x.path("at").asLong());
            return d;
        }
        Comparison.Side side(long cutId) { return new Comparison.Side(perStation(cutId), open.getOrDefault(cutId, Set.of())); }
    }

    /** A {@code --shuffle} run of a scenario, folded (not compared with the oracle: its cut log is checked equal to the default run's). */
    record Shuffled(String name, long seed, JsonNode cutLog, Map<String, Map<Long, String>> fingerprints, Map<String, Long> rows, RunFold.Findings findings,
                    String failure) {}

    static final class Pipeline {
        final Context c;
        final Path registry, diffs;
        final Map<String, Future<Run>> runs = new LinkedHashMap<>();
        final Map<String, Future<Shuffled>> shuffled = new LinkedHashMap<>();
        final ExecutorService pool, heavy;
        final AtomicInteger dirs = new AtomicInteger();
        final long started = System.nanoTime();

        Pipeline(Context c) throws IOException {
            this.c = c;
            registry = c.fixtures().resolve("registry");
            diffs = c.logs.getParent().resolve("diffs");
            CoreGate.delete(diffs);
            int n = Math.max(1, Integer.getInteger("forseti.gate.parallel", Math.min(4, Runtime.getRuntime().availableProcessors() / 2)));
            pool = Executors.newFixedThreadPool(n, daemon("forseti-gate-scenario"));
            heavy = Executors.newSingleThreadExecutor(daemon("forseti-gate-horizon"));
            for (String s : HAND_MADE) runs.put(s, pool.submit(() -> run(s)));
            if (!skipHorizon()) for (String s : HORIZON) runs.put(s, heavy.submit(() -> run(s)));
            for (String s : SMOKE) runs.put(s, pool.submit(() -> run(s)));
            for (String s : SHUFFLED) for (long seed : SHUFFLE_SEEDS) shuffled.put(s + "@" + seed, pool.submit(() -> shuffle(s, seed)));
            pool.shutdown();
            heavy.shutdown();
        }

        static ThreadFactory daemon(String name) {
            AtomicInteger i = new AtomicInteger();
            return r -> { Thread t = new Thread(r, name + "-" + i.incrementAndGet()); t.setDaemon(true); return t; };
        }

        Path dir(String name) throws IOException { return Files.createDirectories(c.tmp.resolve("scn-" + dirs.incrementAndGet() + "-" + name)); }

        Run get(String name) throws Exception {
            Future<Run> f = runs.get(name);
            if (f == null) {
                if (HORIZON.contains(name) && skipHorizon()) throw new IllegalStateException("not run: the horizon scenarios are skipped (-Dforseti.gate.skipHorizon=true)");
                throw new IllegalArgumentException("no scenario " + name);
            }
            return f.get();
        }
        Shuffled shuffled(String name, long seed) throws Exception { return shuffled.get(name + "@" + seed).get(); }

        private Run run(String name) {
            Run r = new Run();
            r.name = name;
            try {
                prepare(r);
                Path state = dir(name + "-state"), out = dir(name + "-run").resolve("out");
                Stage1Assertions.register(c, state, registry, r.recording);
                r.rulesDigest = Json.read(Files.readAllLines(state.resolve("generations.jsonl"), StandardCharsets.UTF_8).get(0).getBytes(StandardCharsets.UTF_8)).path("value").path("rulesDigest").asText();
                long t = System.nanoTime();
                harness(r, state, out, null);
                r.ms.put("harness", ms(t));
                Path oracle = dir(name + "-oracle").resolve("out");
                t = System.nanoTime();
                oracle(r, out, oracle);
                r.ms.put("oracle", ms(t));
                t = System.nanoTime();
                fold(r, out, oracle);
                r.ms.put("compare", ms(t));
                CoreGate.delete(oracle.getParent());
                CoreGate.delete(out.getParent());
                CoreGate.delete(state);
            } catch (Exception | Error e) {
                r.failure = "crashed: " + e;
            }
            return r;
        }

        static long ms(long t) { return (System.nanoTime() - t) / 1_000_000; }

        /** Builds (or, for a smoke recording, finds) the recording and checks it against the committed lock. */
        private void prepare(Run r) throws IOException {
            Path scenario = c.fixtures().resolve("scenarios").resolve(r.name + ".json"), lock = ScenarioBuilder.lockOf(scenario);
            r.synthetic = Json.read(scenario).path("synthetic").asBoolean(false);
            JsonNode committed = Json.read(lock);
            long t = System.nanoTime();
            if (SMOKE.contains(r.name)) r.recording = c.fixtures().resolve("recordings").resolve(r.name);
            else {
                r.recording = c.root.resolve("build").resolve("recordings").resolve(r.name);
                CoreGate.delete(r.recording);
                ScenarioBuilder.Built b = ScenarioBuilder.build(scenario, registry, r.recording);
                if (!Arrays.equals(Json.pretty(b.lock()), Files.readAllBytes(lock))) throw new IllegalStateException("the rebuilt lock differs from " + c.root.relativize(lock));
            }
            r.ms.put("build", ms(t));
            TreeMap<String, String> have = digests(r.recording), want = new TreeMap<>();
            committed.path("files").fields().forEachRemaining(e -> want.put(e.getKey(), e.getValue().asText()));
            if (!have.equals(want)) {
                List<String> d = new ArrayList<>();
                for (String k : want.keySet()) if (!want.get(k).equals(have.get(k))) d.add(k);
                for (String k : have.keySet()) if (!want.containsKey(k)) d.add("unexpected " + k);
                throw new IllegalStateException("the recording's bytes differ from " + c.root.relativize(lock) + ": " + d);
            }
            Recording rec = Recording.open(r.recording);
            r.manifest = rec.manifest();
            r.expected = rec.expected();
            JsonNode counts = committed.path("counts");
            r.lockLine = (SMOKE.contains(r.name) ? "committed recording" : "built") + " = lock (" + want.size() + " files; " + counts.path("messages").asLong() + " messages, "
                + counts.path("samples").asLong() + " samples, " + counts.path("cuts").asLong() + " cuts)";
        }

        static TreeMap<String, String> digests(Path root) throws IOException {
            TreeMap<String, String> m = new TreeMap<>();
            try (Stream<Path> s = Files.walk(root)) {
                for (Path p : s.filter(Files::isRegularFile).toList()) {
                    MessageDigest md;
                    try { md = MessageDigest.getInstance("SHA-256"); } catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
                    try (InputStream in = new DigestInputStream(Files.newInputStream(p), md)) { in.transferTo(java.io.OutputStream.nullOutputStream()); }
                    List<String> parts = new ArrayList<>();
                    for (Path x : root.relativize(p)) parts.add(x.toString());
                    m.put(String.join("/", parts), HexFormat.of().formatHex(md.digest()));
                }
            }
            return m;
        }

        /** {@code forseti run --engine core} in a child JVM on the gate's own classpath. */
        private void harness(Run r, Path state, Path out, Long seed) throws IOException, InterruptedException {
            String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            String cp = Arrays.stream(System.getProperty("java.class.path").split(java.io.File.pathSeparator)).map(p -> Path.of(p).toAbsolutePath().toString())
                .reduce((a, b) -> a + java.io.File.pathSeparator + b).orElseThrow();
            String xmx = r.synthetic ? System.getProperty("forseti.gate.horizonXmx", "6g") : System.getProperty("forseti.gate.harnessXmx", "2g");
            List<String> cmd = new ArrayList<>(List.of(javaBin, "-Xmx" + xmx, "-cp", cp, "yggdrasil.forseti.cli.Main", "run", "--engine", "core",
                "--recording", r.recording.toString(), "--registry", registry.toString(), "--state", state.toString(), "--generation", "1", "--out", out.toString()));
            if (seed != null) cmd.addAll(List.of("--seed", Long.toString(seed), "--shuffle"));
            Proc p = Proc.run(c.root, c.logs.resolve("harness-" + r.name + (seed == null ? "" : "-shuffle-" + seed) + ".log"), Duration.ofHours(2), cmd);
            if (p.exit() != 0) throw new IllegalStateException("forseti run exit " + p.exit() + ": " + p.lastLine());
            r.cutLog = Json.read(out.resolve("cuts.json"));
            r.runJson = Json.read(out.resolve("run.json"));
        }

        private void oracle(Run r, Path run, Path out) throws IOException, InterruptedException {
            JsonNode rule = r.manifest.path("rule");
            List<String> cmd = new ArrayList<>(List.of(c.python, "-I", "-m", "forseti_oracle.cli", "all", "--recording", r.recording.toString(),
                "--cuts", run.resolve("cuts.json").toString(), "--mappings", registry.resolve("mappings").toString(),
                "--release", Json.text(rule, "releaseId") + "@" + Json.text(rule, "version"),
                "--rule", Json.text(rule, "attributionRuleId") + "@" + Json.text(rule, "attributionRuleVersion"),
                "--definitions", r.recording.resolve(Recording.MANIFEST).toString(), "--out", out.toString()));
            r.runJson.path("subscribed").fields().forEachRemaining(e -> {
                List<String> ps = new ArrayList<>();
                e.getValue().forEach(p -> ps.add(p.asText()));
                cmd.add("--subscribe");
                cmd.add(e.getKey() + "=" + String.join(",", ps));
            });
            Proc p = Proc.run(c.root, c.logs.resolve("oracle-" + r.name + ".log"), Duration.ofHours(r.synthetic ? 6 : 1), cmd);
            if (p.exit() != 0) throw new IllegalStateException("oracle exit " + p.exit() + ": " + p.lastLine());
        }

        private void fold(Run r, Path out, Path oracle) throws IOException {
            r.rules = ruleSet(r);
            r.tally = new Comparison.Tally();
            Map<String, Integer> stations = Recording.open(r.recording).routing().stations();
            try (RunFold f = new RunFold(out, r.rulesDigest, 1, !r.synthetic)) {
                r.fold = f;
                for (JsonNode cut : r.cutLog.path("cuts")) {
                    long k = cut.path("cutId").asLong();
                    f.advance(k);
                    Comparison.compareCut(f, k, oracle.resolve("cut-" + k), r.name, diffs, cut, r.tally);
                    Set<String> open = new TreeSet<>();
                    for (Map.Entry<String, RunFold.Rev> e : f.live(RunFold.WR).entrySet()) {
                        JsonNode b = e.getValue().node();
                        if (b.path("interval").path("leave").isNull() && !"retracted".equals(b.path("status").asText())) open.add(b.path("readPoint").asText());
                    }
                    r.open.put(k, open);
                }
                f.finish();
                Map<String, List<Integer>> subscribed = new HashMap<>();
                r.runJson.path("subscribed").fields().forEachRemaining(e -> { List<Integer> l = new ArrayList<>(); e.getValue().forEach(x -> l.add(x.asInt())); subscribed.put(e.getKey(), l); });
                r.measures = Integrity.check(f, r.recording, r.cutLog, subscribed, r.rules, r.manifest.path("definitions"), stations);
                r.outcome = Expectations.check(f, r.expected, r::perStation);
                if (r.synthetic) f.compact();
            }
        }

        /** The rule set the recording names, from its release in the fixture registry (head verified at its signedAt + 60 s). */
        private RuleSet ruleSet(Run r) throws IOException {
            Path m = registry.resolve("mappings");
            JsonNode rule = r.manifest.get("rule");
            yggdrasil.forseti.core.release.HeadVerifier.Head h = yggdrasil.forseti.core.release.HeadVerifier.verify(m, Json.text(rule, "stream"), Foundation.fixtureKeys(c),
                new yggdrasil.forseti.core.release.FloorStore(dir(r.name + "-floors")), GateSigner.now(m, Json.text(rule, "stream")));
            return RuleSet.load(yggdrasil.forseti.core.release.ReleaseReader.read(m, h), Json.text(rule, "attributionRuleId"), Json.text(rule, "attributionRuleVersion"), r.manifest.get("definitions"));
        }

        private Shuffled shuffle(String name, long seed) {
            try {
                Run base = get(name);
                if (base.failure != null) return new Shuffled(name, seed, null, null, null, null, "the default run failed: " + base.failure);
                Run r = new Run();
                r.name = name; r.recording = base.recording; r.manifest = base.manifest; r.synthetic = false;
                Path state = dir(name + "-state-" + seed), out = dir(name + "-shuffle-" + seed).resolve("out");
                Stage1Assertions.register(c, state, registry, r.recording);
                harness(r, state, out, seed);
                Map<String, Map<Long, String>> fp;
                Map<String, Long> rows;
                RunFold.Findings findings;
                try (RunFold f = new RunFold(out, base.rulesDigest, 1, false)) {
                    for (JsonNode cut : r.cutLog.path("cuts")) f.advance(cut.path("cutId").asLong());
                    f.finish();
                    fp = f.fingerprints();
                    rows = f.rows();
                    findings = f.findings;
                }
                CoreGate.delete(out.getParent());
                CoreGate.delete(state);
                return new Shuffled(name, seed, r.cutLog, fp, rows, findings, null);
            } catch (Exception | Error e) {
                return new Shuffled(name, seed, null, null, null, null, "crashed: " + e);
            }
        }
    }

    static Pipeline pipeline(Context c) throws Exception { return c.memo("scenario-pipeline", () -> new Pipeline(c)); }
    static Run get(Context c, String name) throws Exception { return pipeline(c).get(name); }
}
