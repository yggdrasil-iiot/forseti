package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
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

/** Every scenario of the core gate made once per gate run, in parallel (plan Tasks 19 and 20): the recording is built from its committed
 *  scenario into {@code build/recordings/<name>/} and checked against its committed lock before anything runs (the three smoke
 *  recordings are checked in place under {@code fixtures/recordings/}); then, before any run starts, one generation per selected
 *  scenario is registered through {@code forseti generation new} in the gate run's pipeline store, one at a time in {@link #all()} order
 *  ({@link #table}), so the numbers depend on the selection and never on scheduling; the {@code --shuffle} runs read their scenario's
 *  generation and register none. The harness runs as a child JVM ({@code forseti run --engine core}, its own heap) on that generation;
 *  the oracle runs
 *  on the harness's cut log ({@code --python}) and hands its cuts over one at a time ({@code --stream}, {@link Comparison#consume}): the
 *  journals are folded cut by cut and compared with the oracle at every cut as it arrives ({@link Comparison}), so the oracle's output
 *  on disk stays at one cut; the integrity checks are recorded ({@link RunFold}, {@link Integrity}) and the ground truth asserted
 *  ({@link Expectations}). On a difference the oracle is rerun on the first differing cuts with {@code --snapshots} for the diff files.
 *  Each run's {@code run.json} and {@code cuts.json} are kept under {@code <out>/runs/} ({@link GateEvidence#keep}); the run and oracle
 *  directories are deleted once folded (the pipeline store is the gate run's and stays); what the assertions read stays in memory. A
 *  {@code --shuffle} run is compared with the oracle at every cut exactly like its scenario's run (plan Task 20 decision 12).
 *
 *  <p>Parallelism: {@code -Dforseti.gate.parallel=<n>} hand-made scenarios at a time (default half the processors, at most 4), the
 *  five horizon recordings one at a time beside them (each harness gets {@code -Dforseti.gate.horizonXmx}, default 6g). {@code -Dforseti.gate.skipHorizon=true} (or
 *  {@code FORSETI_GATE_SKIP_HORIZON=true}) leaves the horizon scenarios out for local iteration; every assertion that needs one then
 *  fails as not run, so a gate run that skips them never passes. {@code -Dforseti.gate.only=<name,name>} runs only the named scenarios
 *  (and their shuffled runs), with the same consequence for every other one; {@code -Dforseti.gate.oracleNoMemo=true} runs the oracle
 *  with {@code --no-memo}. */
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
    /** The scenarios {@code -Dforseti.gate.only} names, null when unset. */
    static Set<String> only() {
        String v = System.getProperty("forseti.gate.only", "");
        return v.isBlank() ? null : new TreeSet<>(Arrays.asList(v.split(",")));
    }
    static boolean selected(String name) { Set<String> o = only(); return o == null || o.contains(name); }
    /** Whether this gate run makes scenario {@code name}: in a shard run ({@code --shard}) the shard's scenarios, otherwise every
     *  scenario {@code -Dforseti.gate.only} does not leave out (the horizon scenarios also not when they are skipped). */
    static boolean selected(Context c, String name) { return notRun(c, name) == null; }
    /** Why scenario {@code name} is not made in this gate run, null when it is. */
    static String notRun(Context c, String name) {
        if (HORIZON.contains(name) && skipHorizon()) return "not run: the horizon scenarios are skipped (-Dforseti.gate.skipHorizon=true)";
        if (c.shard != null && !c.shard.scenarios().contains(name)) return "not run: not in shard " + c.shard.name() + " (" + Shards.PLAN + ")";
        if (!selected(name)) return "not run: -Dforseti.gate.only=" + System.getProperty("forseti.gate.only");
        return null;
    }
    /** Whether this gate run makes the {@code --shuffle} runs of {@code name}: a shard run those of its {@code shuffled} list. */
    static boolean shuffles(Context c, String name) {
        if (!SHUFFLED.contains(name) || !selected(c, name)) return false;
        return c.shard == null || c.shard.shuffled().contains(name);
    }
    static List<String> all() { List<String> l = new ArrayList<>(HAND_MADE); l.addAll(HORIZON); l.addAll(SMOKE); return l; }

    /** One scenario, run and compared. */
    static final class Run {
        String name, failure, lockLine;
        /** The name in logs, diffs and evidence text: the scenario, or {@code <scenario>.shuffle-<seed>}. */
        String label;
        /** The generation this run reads, from the registration table. */
        GateEvidence.Registered generation;
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
        String label() { return label != null ? label : name; }
        /** The part of every injection assertion's evidence that says the scenario ran, matched the oracle, held integrity and ground truth. */
        String summary() {
            if (failure != null) return label() + ": " + failure;
            return label() + ": " + lockLine + "; " + tally.describe() + "; integrity " + (fold.findings.clean() ? "clean" : fold.findings.describe()) + "; " + outcome.describe();
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

    /** A {@code --shuffle} run of a scenario, compared with the oracle at every cut on its own cut log ({@code run}); its fingerprints are
     *  also checked equal to the default run's ({@link Injections#determinism}). */
    record Shuffled(String name, long seed, JsonNode cutLog, Map<String, Map<Long, String>> fingerprints, Map<String, Long> rows, RunFold.Findings findings,
                    String failure, Run run) {}

    /** The registration table of this gate run (made once): every selected scenario's recording built and checked against its lock (in
     *  parallel, the horizon recordings one at a time), then one {@code generation new} per scenario with {@code --recording}, sequentially
     *  in {@link #all()} order, in the pipeline store. A scenario whose recording could not be made registers nothing and fails. */
    static Map<String, Run> table(Context c) throws Exception {
        return c.memo("generation-table", () -> {
            Path registry = c.fixtures().resolve("registry");
            int n = parallelism();
            ExecutorService builds = Executors.newFixedThreadPool(n, Pipeline.daemon("forseti-gate-build")), heavy = Executors.newSingleThreadExecutor(Pipeline.daemon("forseti-gate-build-horizon"));
            Map<String, Future<Run>> prepared = new LinkedHashMap<>();
            try {
                for (String s : all()) {
                    if (!selected(c, s)) continue;
                    prepared.put(s, (HORIZON.contains(s) ? heavy : builds).submit(() -> {
                        Run r = new Run();
                        r.name = s;
                        try { prepare(c, registry, r); } catch (Exception | Error e) { r.failure = "crashed: " + e; }
                        return r;
                    }));
                }
            } finally {
                builds.shutdown();
                heavy.shutdown();
            }
            Map<String, Run> table = new LinkedHashMap<>();
            for (Map.Entry<String, Future<Run>> e : prepared.entrySet()) {
                Run r = e.getValue().get();
                if (r.failure == null) {
                    try {
                        r.generation = GateEvidence.register(c, c.pipeline, registry, r.recording, r.name, GateEvidence.ONLINE, null);
                        r.rulesDigest = r.generation.rulesDigest();
                    } catch (Exception | Error x) { r.failure = "crashed: " + x; }
                }
                table.put(e.getKey(), r);
            }
            return table;
        });
    }

    static int parallelism() { return Math.max(1, Integer.getInteger("forseti.gate.parallel", Math.min(4, Runtime.getRuntime().availableProcessors() / 2))); }

    /** Builds (or, for a smoke recording, finds) the recording and checks it against the committed lock. */
    static void prepare(Context c, Path registry, Run r) throws IOException {
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
        r.ms.put("build", Pipeline.ms(t));
        TreeMap<String, String> have = Pipeline.digests(r.recording), want = new TreeMap<>();
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
            Map<String, Run> table;
            try { table = table(c); } catch (Exception e) { throw new IOException("the registration table: " + e, e); }
            pool = Executors.newFixedThreadPool(parallelism(), daemon("forseti-gate-scenario"));
            heavy = Executors.newSingleThreadExecutor(daemon("forseti-gate-horizon"));
            for (String s : HORIZON) if (table.containsKey(s)) runs.put(s, heavy.submit(() -> run(table.get(s))));
            for (String s : HAND_MADE) if (table.containsKey(s)) runs.put(s, pool.submit(() -> run(table.get(s))));
            for (String s : SMOKE) if (table.containsKey(s)) runs.put(s, pool.submit(() -> run(table.get(s))));
            for (String s : SHUFFLED) if (shuffles(c, s)) for (long seed : SHUFFLE_SEEDS) shuffled.put(s + "@" + seed, pool.submit(() -> shuffle(s, seed)));
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
                if (!all().contains(name)) throw new IllegalArgumentException("no scenario " + name);
                for (Future<Run> x : runs.values()) x.get();   // the scenarios this run makes finish (and log) before the gate reports
                throw new IllegalStateException(notRun(c, name));
            }
            return f.get();
        }
        Shuffled shuffled(String name, long seed) throws Exception {
            Future<Shuffled> f = shuffled.get(name + "@" + seed);
            if (f == null) throw new IllegalStateException(c.shard != null ? "not run: the shuffled runs of " + name + " are not in shard " + c.shard.name()
                : "not run: -Dforseti.gate.only=" + System.getProperty("forseti.gate.only"));
            return f.get();
        }

        /** The scenario's run on its registered generation: harness, fold and comparison, the run record and cut log kept. */
        private Run run(Run r) {
            String name = r.name;
            try {
                if (r.failure == null) {
                    Path out = dir(name + "-run").resolve("out");
                    long t = System.nanoTime();
                    harness(r, c.pipeline.dir(), out, null);
                    r.ms.put("harness", ms(t));
                    Path oracle = dir(name + "-oracle").resolve("out");
                    try { fold(r, out, oracle); }
                    finally { GateEvidence.keep(c, r.generation, out, r.recording, name, GateEvidence.ONLINE, CoreGate.DEFAULT_SEED, false, name); }
                    CoreGate.delete(oracle.getParent());
                    CoreGate.delete(out.getParent());
                }
            } catch (Exception | Error e) {
                r.failure = "crashed: " + e;
            }
            try {
                Files.writeString(c.logs.resolve("scenario-" + name + ".log"), r.summary() + "\n" + r.ms + "\n", StandardCharsets.UTF_8);
            } catch (Exception | Error e) { /* the log is for reading, not a gate result */ }
            return r;
        }

        static long ms(long t) { return (System.nanoTime() - t) / 1_000_000; }

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

        /** {@code forseti run --engine core} in a child JVM on the gate's own classpath, on the run's generation from the table. */
        private void harness(Run r, Path state, Path out, Long seed) throws IOException, InterruptedException {
            String xmx = r.synthetic ? System.getProperty("forseti.gate.horizonXmx", "6g") : System.getProperty("forseti.gate.harnessXmx", "2g");
            List<String> cmd = new ArrayList<>(CoreGate.java(xmx));
            cmd.addAll(List.of("run", "--engine", "core", "--recording", r.recording.toString(), "--registry", registry.toString(), "--state", state.toString(),
                "--generation", Long.toString(r.generation.generation()), "--out", out.toString()));
            if (seed != null) cmd.addAll(List.of("--seed", Long.toString(seed), "--shuffle"));
            Proc p = Proc.run(c.root, c.logs.resolve("harness-" + r.name + (seed == null ? "" : "-shuffle-" + seed) + ".log"), Duration.ofHours(2), cmd);
            if (p.exit() != 0) throw new IllegalStateException("forseti run exit " + p.exit() + ": " + p.lastLine());
            r.cutLog = Json.read(out.resolve("cuts.json"));
            r.runJson = Json.read(out.resolve("run.json"));
        }

        /** {@code python -I -m forseti_oracle.cli all} on the harness's cut log, the recording's rule and subscription. */
        private List<String> oracleCommand(Run r, Path run, Path out) {
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
            if (Boolean.getBoolean("forseti.gate.oracleNoMemo")) cmd.add("--no-memo");
            return cmd;
        }

        /** The oracle with {@code --stream} beside the fold: each cut it announces is folded, compared and deleted before it writes the
         *  next ({@link Comparison#consume}). Then the integrity checks and the ground truth; on a difference, the diff files from a
         *  {@code --snapshots} rerun of the first differing cuts. Timings: {@code oracle} (the oracle process, start to exit),
         *  {@code compare} (the gate's fold and comparison of the announced cuts); {@code oracleCutBytesPeak}, the largest cut on disk. */
        private void fold(Run r, Path out, Path oracle) throws IOException, InterruptedException {
            r.rules = ruleSet(r);
            r.tally = new Comparison.Tally();
            Map<String, Integer> stations = Recording.open(r.recording).routing().stations();
            Files.createDirectories(oracle);
            Path log = c.logs.resolve("oracle-" + r.label() + ".log");
            Files.createDirectories(log.getParent());
            List<String> cmd = oracleCommand(r, out, oracle);
            cmd.add("--stream");
            Duration timeout = Duration.ofHours(r.synthetic ? 6 : 1);
            Map<Long, JsonNode> entries = new HashMap<>();
            for (JsonNode cut : r.cutLog.path("cuts")) entries.put(cut.path("cutId").asLong(), cut);
            Comparison.OracleFold of = new Comparison.OracleFold();
            List<String> trailing = new ArrayList<>();
            long[] compareNs = {0};
            long started = System.nanoTime();
            Process p = new ProcessBuilder(cmd).directory(c.root.toFile()).redirectError(log.toFile()).start();
            Thread watchdog = new Thread(() -> {
                try { if (!p.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) p.destroyForcibly(); } catch (InterruptedException e) { p.destroyForcibly(); }
            }, "forseti-gate-oracle-watchdog-" + r.label());
            watchdog.setDaemon(true);
            watchdog.start();
            try (RunFold f = new RunFold(out, r.rulesDigest, r.generation.generation(), !r.synthetic)) {
                r.fold = f;
                String broken;
                try (BufferedReader announced = new BufferedReader(new java.io.InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
                     java.io.Writer acks = new java.io.OutputStreamWriter(p.getOutputStream(), StandardCharsets.UTF_8)) {
                    broken = Comparison.consume(announced, acks, oracle, r.cutIds(), trailing, (k, dir) -> {
                        long s = System.nanoTime();
                        f.advance(k);
                        Comparison.compareCut(f, k, dir, of, r.label(), entries.get(k), r.tally);
                        Set<String> open = new TreeSet<>();
                        for (Map.Entry<String, RunFold.Rev> e : f.live(RunFold.WR).entrySet()) {
                            JsonNode b = e.getValue().node();
                            if (b.path("interval").path("leave").isNull() && !"retracted".equals(b.path("status").asText())) open.add(b.path("readPoint").asText());
                        }
                        r.open.put(k, open);
                        compareNs[0] += System.nanoTime() - s;
                    });
                }
                if (!p.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) p.destroyForcibly();
                int exit = p.waitFor();
                r.ms.put("oracle", ms(started));
                r.ms.put("compare", compareNs[0] / 1_000_000);
                r.ms.put("oracleCutBytesPeak", of.diskPeak);
                Files.write(log, (String.join("\n", trailing) + "\noracle " + r.ms.get("oracle") + " ms, compare " + r.ms.get("compare") + " ms, largest cut on disk "
                    + of.diskPeak + " bytes, upserts " + of.upserts + ", deletes " + of.deletes + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
                if (broken != null || exit != 0) {
                    String[] lines = Files.readString(log, StandardCharsets.UTF_8).strip().split("\\R");
                    throw new IllegalStateException("oracle exit " + exit + (broken != null ? ", " + broken : "") + ": " + (lines.length > 1 ? lines[lines.length - 2] : ""));
                }
                f.finish();
                Map<String, List<Integer>> subscribed = new HashMap<>();
                r.runJson.path("subscribed").fields().forEachRemaining(e -> { List<Integer> l = new ArrayList<>(); e.getValue().forEach(x -> l.add(x.asInt())); subscribed.put(e.getKey(), l); });
                r.measures = Integrity.check(f, r.recording, r.cutLog, subscribed, r.rules, r.manifest.path("definitions"), stations);
                r.outcome = Expectations.check(f, r.expected, r::perStation);
                if (r.synthetic) f.compact();
            } finally {
                if (p.isAlive()) p.destroyForcibly();
            }
            if (!r.tally.pending.isEmpty()) diagnose(r, out);
        }

        /** The diff files of the first differing cuts, from an oracle rerun with {@code --snapshots --cut-ids} on the original inputs. */
        private void diagnose(Run r, Path run) throws IOException {
            Path snapshots = dir(r.label() + "-oracle-diag").resolve("out");
            String failure = null;
            try {
                List<String> cmd = oracleCommand(r, run, snapshots);
                cmd.addAll(List.of("--snapshots", "--cut-ids", String.join(",", r.tally.pendingCuts().stream().map(String::valueOf).toList())));
                Proc p = Proc.run(c.root, c.logs.resolve("oracle-" + r.label() + "-diagnostic.log"), Duration.ofHours(r.synthetic ? 6 : 1), cmd);
                if (p.exit() != 0) failure = "exit " + p.exit() + ": " + p.lastLine();
            } catch (Exception e) {
                failure = e.toString();
            }
            Comparison.writeDiffs(r.tally, diffs, r.label(), r.generation.generation(), failure == null ? snapshots : null, failure);
            CoreGate.delete(snapshots.getParent());
        }

        /** The rule set the recording names, from its release in the fixture registry (head verified at its signedAt + 60 s). */
        private RuleSet ruleSet(Run r) throws IOException {
            Path m = registry.resolve("mappings");
            JsonNode rule = r.manifest.get("rule");
            yggdrasil.forseti.core.release.HeadVerifier.Head h = yggdrasil.forseti.core.release.HeadVerifier.verify(m, Json.text(rule, "stream"), Foundation.fixtureKeys(c),
                new yggdrasil.forseti.core.release.FloorStore(dir(r.name + "-floors")), GateSigner.now(m, Json.text(rule, "stream")));
            return RuleSet.load(yggdrasil.forseti.core.release.ReleaseReader.read(m, h), Json.text(rule, "attributionRuleId"), Json.text(rule, "attributionRuleVersion"), r.manifest.get("definitions"));
        }

        /** A {@code --shuffle} run on its scenario's generation (it registers none), compared with the oracle at every cut of its own cut log
         *  through {@link #fold}, like its scenario's run; its run record and cut log are kept. */
        private Shuffled shuffle(String name, long seed) {
            Run r = new Run();
            r.name = name;
            r.label = name + ".shuffle-" + seed;
            try {
                Run base = get(name);
                if (base.failure != null) return new Shuffled(name, seed, null, null, null, null, "the default run failed: " + base.failure, null);
                r.recording = base.recording;
                r.manifest = Recording.open(r.recording).manifest();
                r.expected = base.expected;
                r.synthetic = false;
                r.lockLine = base.lockLine;
                r.generation = base.generation;
                r.rulesDigest = base.rulesDigest;
                Path out = dir(name + "-shuffle-" + seed).resolve("out");
                long t = System.nanoTime();
                harness(r, c.pipeline.dir(), out, seed);
                r.ms.put("harness", ms(t));
                Path oracle = dir(name + "-shuffle-" + seed + "-oracle").resolve("out");
                try { fold(r, out, oracle); }
                finally { GateEvidence.keep(c, r.generation, out, r.recording, name, GateEvidence.ONLINE, seed, true, name + ".shuffle-" + seed); }
                CoreGate.delete(oracle.getParent());
                CoreGate.delete(out.getParent());
                return new Shuffled(name, seed, r.cutLog, r.fold.fingerprints(), r.fold.rows(), r.fold.findings, null, r);
            } catch (Exception | Error e) {
                r.failure = "crashed: " + e;
                return new Shuffled(name, seed, r.cutLog, null, null, null, "crashed: " + e, r);
            }
        }
    }

    static Pipeline pipeline(Context c) throws Exception { return c.memo("scenario-pipeline", () -> new Pipeline(c)); }
    static Run get(Context c, String name) throws Exception { return pipeline(c).get(name); }
    /** Whether this gate run has made its scenario pipeline (an assertion asked for a scenario). */
    static boolean started(Context c) { return c.has("scenario-pipeline"); }
}
