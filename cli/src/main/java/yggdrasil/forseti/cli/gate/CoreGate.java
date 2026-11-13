package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.Args;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** {@code forseti gate --engine core}: loads the committed assertion manifest {@code gate/core-assertions.json}, runs every registered
 *  assertion, writes {@code <out>/report.json} and prints {@code assertions run N / expected N} as its last line. Exit 0 only when every
 *  manifest id ran and held and no id outside the manifest ran; 1 otherwise, including a crash or a missing artifact (the failure report
 *  is still written); 2 for a usage error, among them a {@code --negative-control} that {@code gate/core-negative-controls.json} does not
 *  list or that this gate does not implement. Paths are relative to the repository root, the directory the gate runs from. */
public final class CoreGate {
    private CoreGate() {}

    public static final long DEFAULT_SEED = 20261109L;
    public static final String MANIFEST = "gate/core-assertions.json", CONTROLS = "gate/core-negative-controls.json";
    static final String JCS_JAVA = "io.github.erdtman:java-json-canonicalization";

    /** One assertion: its manifest id and what it runs. Assertions register in {@link #registered()} (one list). */
    public record Check(String id, Body body) {}
    @FunctionalInterface public interface Body { Result run(Context c) throws Exception; }
    public record Result(boolean ok, String evidence) {}

    /** The negative controls this gate implements, each with its intended failures (spec Negative controls, plan Task 22);
     *  {@link #EVERY_INJECTION} stands for every {@code I-*} id of the manifest (the pins cross-check runs inside every injection). */
    static final String EVERY_INJECTION = "I-*";
    static final Map<String, List<String>> IMPLEMENTED_CONTROLS = implemented();
    private static Map<String, List<String>> implemented() {
        Map<String, List<String>> m = new LinkedHashMap<>();
        m.put("processing-time", List.of("I-1"));
        m.put("drop-late", List.of("J-DISPOSITION-COVERAGE"));
        m.put("no-cascade", List.of("I-18", "J-BASIS-CURRENT"));
        m.put("lenient-oracle", List.of("I-2", "I-6"));
        m.put("first-wins", List.of("I-4"));
        m.put("station-granularity", List.of("I-16"));
        m.put("ignore-session-gap", List.of("I-8"));
        m.put("wrong-rules-pin", List.of(EVERY_INJECTION));
        return Collections.unmodifiableMap(m);
    }

    /** The intended failures of implemented control {@code name} against the manifest ids ({@link #EVERY_INJECTION} expanded, in
     *  manifest order). */
    static List<String> intended(String name, List<String> manifestIds) {
        List<String> l = new ArrayList<>();
        for (String id : IMPLEMENTED_CONTROLS.getOrDefault(name, List.of()))
            if (id.equals(EVERY_INJECTION)) manifestIds.stream().filter(x -> x.startsWith("I-")).forEach(l::add); else l.add(id);
        return List.copyOf(l);
    }

    public static List<Check> registered() {
        List<Check> all = new ArrayList<>(Foundation.checks());
        all.addAll(Stage1Assertions.checks());
        all.addAll(Stage2Assertions.checks());
        all.addAll(Injections.checks());
        all.addAll(GenerationAssertions.checks());
        all.addAll(SelfTests.checks());
        return List.copyOf(all);
    }

    /** {@code shard}: run only that shard of {@code gate/core-shards.json}; {@code aggregate}: evaluate the shard outputs under that
     *  directory ({@link Shards}); both null for a full run. {@code state}: the pipeline store ({@code --state}, always {@code <out>/state}),
     *  null for a temporary one. */
    public record Options(Path root, Path manifest, Path out, String python, String negativeControl, List<String> intendedFailures,
                          long seed, String command, String shard, Path aggregate, Path state) {
        public Options(Path root, Path manifest, Path out, String python, String negativeControl, List<String> intendedFailures, long seed, String command) {
            this(root, manifest, out, python, negativeControl, intendedFailures, seed, command, null, null, null);
        }
        public Options(Path root, Path manifest, Path out, String python, String negativeControl, List<String> intendedFailures, long seed, String command,
                       String shard, Path aggregate) {
            this(root, manifest, out, python, negativeControl, intendedFailures, seed, command, shard, aggregate, null);
        }
    }

    /** What an assertion may use: the repository root, the oracle's interpreter, a private temporary directory, the log directory, the
     *  gate run's output directory, its pipeline store and the evidence it lists ({@link GateEvidence}). */
    public static final class Context {
        final Path root, tmp, logs;
        final String python;
        private int dirs;
        /** The gate run's output directory ({@code runs/} is kept under it). */
        Path out;
        /** The pipeline store: {@code --state} ({@code shared}) or a temporary store ({@code isolated}). */
        GateEvidence.Store pipeline;
        /** The assertion running now: the owner of the {@code assertion:} stores made while it runs. */
        volatile String current;
        final GateEvidence.Log evidence = new GateEvidence.Log();
        private final Map<String, Object> memo = new HashMap<>();
        /** The shard this gate run is ({@code --shard}), null otherwise. */
        Shards.Shard shard;
        /** The shards' merged summaries in an aggregate run ({@code --aggregate}), null otherwise. */
        Summaries aggregate;
        /** The negative control of this gate run ({@code --negative-control}), null otherwise: the harness applies it (or, for
         *  {@code lenient-oracle}, the oracle) on every run this gate run makes. */
        String control;
        /** What the assertions measure for {@code report.json} ({@code measurements}): item 2's reference values, revision counts. */
        final ObjectNode measurements = Json.object();
        Context(Path root, Path tmp, Path logs, String python) {
            this.root = root; this.tmp = tmp; this.logs = logs; this.python = python;
            this.out = logs.getParent();
            this.pipeline = new GateEvidence.Store(GateEvidence.PIPELINE, tmp.resolve("pipeline-state"), false);
        }
        public Path path(String relative) { return root.resolve(relative); }
        public Path fixtures() { return root.resolve("fixtures"); }
        /** A new empty directory under the gate's temporary directory. */
        public synchronized Path newDir(String name) throws IOException { return Files.createDirectories(tmp.resolve(name + "-" + dirs++)); }
        /** A new temporary control store of the running assertion, {@code assertion:<ID>} ({@code #2}, {@code #3}, ... for its further
         *  stores, in creation order); its registrations are {@code isolated}. */
        GateEvidence.Store assertionStore() throws IOException {
            String id = current;
            if (id == null) throw new IllegalStateException("an assertion store outside an assertion");
            return new GateEvidence.Store(evidence.storeName(id), newDir("store"), false);
        }
        /** The value {@code make} computes, made once per gate run and shared by the assertions that ask for {@code key} (a harness run,
         *  say); a computation that throws is not kept, so the next assertion that asks makes it again and reports its own failure. */
        @SuppressWarnings("unchecked")
        public <T> T memo(String key, java.util.concurrent.Callable<T> make) throws Exception {
            if (memo.containsKey(key)) return (T) memo.get(key);
            T v = make.call();
            memo.put(key, v);
            return v;
        }
        boolean has(String key) { return memo.containsKey(key); }
        void measure(String key, JsonNode value) { synchronized (measurements) { measurements.set(key, value); } }
        /** Runs {@code <python> -I <args...>} from the repository root; output goes to {@code <out>/logs/<log>.log}. */
        public Proc python(String log, String... args) throws IOException, InterruptedException {
            List<String> cmd = new ArrayList<>(List.of(python, "-I"));
            cmd.addAll(List.of(args));
            return Proc.run(root, logs.resolve(log + ".log"), Duration.ofMinutes(10), cmd);
        }
    }

    public record Proc(int exit, String output) {
        static Proc run(Path cwd, Path log, Duration timeout, List<String> cmd) throws IOException, InterruptedException {
            Files.createDirectories(log.getParent());
            ProcessBuilder pb = new ProcessBuilder(cmd).directory(cwd.toFile()).redirectErrorStream(true).redirectOutput(log.toFile());
            pb.redirectInput(ProcessBuilder.Redirect.PIPE);
            Process p = pb.start();
            p.getOutputStream().close();
            if (!p.waitFor(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)) {
                p.destroyForcibly();
                throw new IllegalStateException("timed out after " + timeout + ": " + String.join(" ", cmd));
            }
            return new Proc(p.exitValue(), new String(Files.readAllBytes(log), StandardCharsets.UTF_8));
        }
        public String lastLine() {
            String[] l = output.strip().split("\\R");
            return l.length == 0 ? "" : l[l.length - 1].strip();
        }
    }

    /** The command line: validates the flags (exit 2 on a usage error) and runs the gate from the current directory. */
    public static int main(Args flags, String command, PrintStream out, PrintStream err) {
        String engine, python, control, shard;
        Path outDir, aggregate, state;
        long seed;
        List<String> intended = List.of();
        try {
            flags.only(Set.of("engine", "out", "python", "negative-control", "seed", "shard", "aggregate", "state"));
            engine = flags.required("engine");
            if (!engine.equals("core")) throw new IllegalArgumentException("--engine " + engine + ": this gate runs --engine core only");
            outDir = Path.of(flags.get("out", "build/gate"));
            python = flags.get("python", "python");
            seed = Long.parseLong(flags.get("seed", Long.toString(DEFAULT_SEED)));
            control = flags.get("negative-control").orElse(null);
            shard = flags.get("shard").orElse(null);
            aggregate = flags.get("aggregate").map(Path::of).orElse(null);
            state = flags.get("state").map(Path::of).orElse(null);
            if (state != null && aggregate != null) throw new IllegalArgumentException("--state is for a full or a --shard run, not --aggregate (an aggregate run registers nothing)");
            if (state != null && control != null) throw new IllegalArgumentException("--state is for a full or a --shard run, not --negative-control");
            if (state != null && !real(state).equals(real(outDir.resolve("state"))))
                throw new IllegalArgumentException("--state " + state + ": the store of a gate run is <out>/state, " + outDir.resolve("state"));
            if (shard != null && aggregate != null) throw new IllegalArgumentException("--shard and --aggregate exclude each other");
            if (shard != null) Shards.load(Path.of("")).shard(shard);
            if (control != null) {
                // a control runs only on the shards that own or feed its intended ids (owner decision of 2026-11-12)
                ControlPlan.Control cc = control(Path.of(""), control);
                Shards.Plan plan = Shards.load(Path.of(""));
                if (shard != null && !cc.shards().contains(shard))
                    throw new IllegalArgumentException("--negative-control " + control + " runs on the shards " + cc.shards() + " (" + CONTROLS + "), not on " + shard);
                if (aggregate != null && cc.aggregateIds(plan).isEmpty())
                    throw new IllegalArgumentException("--negative-control " + control + " with --aggregate: it intends no aggregate id of " + Shards.PLAN + " (" + cc.intended() + ")");
                intended = cc.owned(plan, shard, aggregate != null);
            }
            if (aggregate != null && !Files.isDirectory(aggregate)) throw new IllegalArgumentException("--aggregate " + aggregate + ": not a directory");
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            err.println("usage: forseti gate --engine core [--out <dir>] [--python <interpreter>] [--negative-control <name>] [--seed <n>] [--shard <name> | --aggregate <dir>]");
            err.println("                               [--state <out>/state]");
            return 2;
        }
        Path root = Path.of("").toAbsolutePath();
        return run(new Options(root, root.resolve(MANIFEST), outDir.toAbsolutePath(), python, control, intended, seed, "forseti " + command, shard,
            aggregate == null ? null : aggregate.toAbsolutePath(), state == null ? null : state.toAbsolutePath()), registered(), out, err);
    }

    /** The real path of {@code p}: of its nearest existing ancestor, with the rest appended (nothing is created). */
    static Path real(Path p) {
        Path a = p.toAbsolutePath().normalize();
        try {
            if (Files.exists(a)) return a.toRealPath();
            Path parent = a.getParent();
            return parent == null ? a : real(parent).resolve(a.getFileName());
        } catch (IOException e) { return a; }
    }

    /** {@code java [-Xmx<xmx>] -cp <the gate's classpath> yggdrasil.forseti.cli.Main}: a child JVM on the gate's own classpath. */
    static List<String> java(String xmx) {
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String cp = Arrays.stream(System.getProperty("java.class.path").split(java.io.File.pathSeparator)).map(p -> Path.of(p).toAbsolutePath().toString())
            .reduce((a, b) -> a + java.io.File.pathSeparator + b).orElseThrow();
        List<String> cmd = new ArrayList<>(List.of(javaBin));
        if (xmx != null) cmd.add("-Xmx" + xmx);
        cmd.addAll(List.of("-cp", cp, "yggdrasil.forseti.cli.Main"));
        return cmd;
    }

    /** A control listed in {@code gate/core-negative-controls.json} and implemented here, the whole file validated against the shard plan
     *  and the manifest ({@link ControlPlan#validate}); a usage error otherwise. */
    static ControlPlan.Control control(Path root, String name) {
        ControlPlan file = ControlPlan.load(root);
        ControlPlan.Control c = file.control(name);
        if (!IMPLEMENTED_CONTROLS.containsKey(name)) throw new IllegalArgumentException("--negative-control " + name + ": listed but not implemented by this gate");
        List<String> ids;
        try { ids = manifestIds(Json.read(root.resolve(MANIFEST))); } catch (IllegalArgumentException e) { throw new IllegalArgumentException("cannot read " + MANIFEST + ": " + e.getMessage()); }
        List<String> wrong = file.validate(Shards.load(root), ids);
        if (!wrong.isEmpty()) throw new IllegalArgumentException(CONTROLS + ": " + String.join("; ", wrong));
        return c;
    }

    /** Runs {@code checks} against the manifest, writes the report, prints the accounting line last and returns the exit code. */
    public static int run(Options o, List<Check> checks, PrintStream out, PrintStream err) {
        String startedAt = Instant.now().toString();
        Assertions acc = null;
        String manifestSha = null, error = null, summariesSha = null;
        ObjectNode pins = Json.object(), digests = Json.object(), measurements = Json.object();
        ArrayNode shardsOut = Json.JSON.createArrayNode(), generations = Json.JSON.createArrayNode(), runs = Json.JSON.createArrayNode();
        Map<String, String> owner = new HashMap<>();
        String layout = o.shard() != null ? "shard" : o.aggregate() != null ? "aggregate" : "full";
        Shards.Plan plan = null;
        Context ctx = null;
        ControlPlan.Control control = null;
        Summaries evaluated = null;
        Set<String> crashed = new TreeSet<>();
        Path tmp = null;
        long started = System.nanoTime();
        try {
            byte[] manifestBytes = Files.readAllBytes(o.manifest());
            manifestSha = Canonical.digestOf(manifestBytes);
            List<String> ids = manifestIds(Json.read(manifestBytes));
            Shards.Shard shard = null;
            if (o.shard() != null || o.aggregate() != null) {
                plan = Shards.load(o.root());
                Set<String> aggregatable = new HashSet<>(Injections.AGGREGATE);
                List<String> wrong = Shards.validate(plan, ids, aggregatable);
                if (!wrong.isEmpty()) throw new IllegalStateException(Shards.PLAN + ": " + String.join("; ", wrong));
                if (o.shard() != null) shard = plan.shard(o.shard());
                for (Shards.Shard s : plan.shards()) for (String id : s.assertions()) owner.put(id, s.name());
                for (String id : plan.aggregate()) owner.put(id, "aggregate");
            }
            if (o.negativeControl() != null) {
                try { control = ControlPlan.load(o.root()).control(o.negativeControl()); } catch (IllegalArgumentException e) { control = null; }
                if (o.aggregate() != null && control == null) throw new IllegalStateException("--negative-control " + o.negativeControl() + " with --aggregate: not in " + CONTROLS);
            }
            final Shards.Shard only = shard;
            final Set<String> aggregateIds = plan == null ? Set.of() : new HashSet<>(plan.aggregate());
            List<String> expectedIds = shard != null ? shard.assertions() : ids;
            if (o.aggregate() != null && control != null) {
                // an aggregate control run evaluates its control's shards' assertions and the plan's aggregate ones (owner decision of 2026-11-12)
                Set<String> scope = new HashSet<>(plan.aggregate());
                for (Shards.Shard s : plan.shards()) if (control.shards().contains(s.name())) scope.addAll(s.assertions());
                expectedIds = ids.stream().filter(scope::contains).toList();
            }
            acc = new Assertions(expectedIds, out);
            Files.createDirectories(o.out());
            tmp = Files.createTempDirectory("forseti-gate-");
            ctx = new Context(o.root(), tmp, o.out().resolve("logs"), o.python());
            ctx.shard = shard;
            ctx.control = o.negativeControl();
            if (o.aggregate() == null) {   // the runs and journals this gate run keeps, none of an earlier one
                CoreGate.delete(o.out().resolve(GateEvidence.RUNS));
                CoreGate.delete(o.out().resolve(GateEvidence.JOURNALS));
            }
            if (o.state() != null) ctx.pipeline = new GateEvidence.Store(GateEvidence.PIPELINE, Files.createDirectories(o.state()), true);
            measurements = ctx.measurements;
            pins = pins(ctx);
            digests = fixtureDigests(o.root());
            List<Check> selected = checks.stream().filter(c -> only != null ? only.assertions().contains(c.id()) : o.aggregate() == null || aggregateIds.contains(c.id())).toList();
            List<String> coverage = new ArrayList<>();
            if (o.aggregate() != null) {
                ObjectNode expect = Json.object();
                expect.put("manifestSha256", manifestSha);
                Json.putSafe(expect, "seed", o.seed());
                expect.set("pins", pins);
                expect.set("fixtureDigests", digests);
                List<Shards.Recorded> recorded = new ArrayList<>();
                ObjectNode merged = Json.object();
                ctx.aggregate = Shards.read(o.root(), o.aggregate(), plan, expect, recorded, coverage, merged, control);
                evaluated = ctx.aggregate;
                shardsOut = (ArrayNode) merged.get("shards");
                generations = (ArrayNode) merged.get("generations");
                runs = (ArrayNode) merged.get("runs");
                for (Shards.Recorded r : recorded)
                    try { acc.check(r.id(), r.ok(), "[shard " + r.shard() + "] " + r.evidence()); } catch (Assertions.Refused ignored) { /* recorded by the accountant */ }
            }
            for (Check c : selected) {
                Result r;
                ctx.current = c.id();
                try { r = c.body().run(ctx); }
                catch (Exception | Error e) { r = new Result(false, "crashed: " + e); crashed.add(c.id()); }
                finally { ctx.current = null; }
                try { acc.check(c.id(), r.ok(), r.evidence()); } catch (Assertions.Refused ignored) { /* recorded by the accountant */ }
            }
            if (o.aggregate() == null) {
                // full and shard: the summaries beside the report, bound by digest, and the bundle checked by the one validator
                Summaries mine;
                if (shard != null) mine = Summaries.fromPipeline(ctx);
                else if (Scenarios.started(ctx) || ctx.has(Summaries.MEMO)) mine = Summaries.of(ctx);
                else mine = new Summaries();
                evaluated = mine;
                ObjectNode summaries = mine.toJson();
                byte[] bytes = Json.pretty(summaries);
                Files.write(o.out().resolve(Shards.SUMMARIES), bytes);
                summariesSha = Canonical.digestOf(bytes);
                generations = ctx.evidence.generations();
                runs = ctx.evidence.runs();
                if (shard == null && Scenarios.started(ctx) && !Scenarios.skipHorizon() && Scenarios.only() == null) coverage.addAll(mine.missing);
                ObjectNode bundle = Json.object();
                bundle.put("kind", o.negativeControl() != null ? GateEvidence.CONTROL_KIND : "gate");
                bundle.set("pins", pins);
                bundle.set("generations", generations);
                bundle.set("runs", runs);
                coverage.addAll(GateEvidence.validate(o.root(), o.out(), bundle, Json.read(bytes)));
            }
            for (String line : coverage) out.println("COVERAGE " + line);
            if (!coverage.isEmpty()) throw new IllegalStateException("coverage: " + coverage.size() + " problems, first: " + coverage.get(0));
        } catch (Exception | Error e) {
            error = e.toString();
            err.println("gate crashed: " + error);
            if (ctx != null && o.aggregate() == null && generations.isEmpty() && runs.isEmpty()) { generations = ctx.evidence.generations(); runs = ctx.evidence.runs(); }
        } finally {
            if (tmp != null) delete(tmp);
        }
        int exit = acc == null || error != null ? 1 : acc.exitCode();
        ObjectNode report = Json.object();
        Json.putSafe(report, "exit", exit);
        report.put("kind", o.negativeControl() != null ? "negative-control" : "gate").put("layout", layout).put("negativeControl", o.negativeControl());
        report.put("scope", "core").put("engine", "core").put("manifestSha256", manifestSha);
        Json.putSafe(report, "assertionsExpected", acc == null ? 0 : acc.expected().size());
        Json.putSafe(report, "assertionsRun", acc == null ? 0 : acc.ran().size());
        strings(report.putArray("failed"), acc == null ? List.of() : acc.failed());
        strings(report.putArray("notRun"), acc == null ? List.of() : acc.notRun());
        strings(report.putArray("intendedFailures"), o.intendedFailures());
        report.set("pins", pins);
        Json.putSafe(report, "seed", o.seed());
        report.set("fixtureDigests", digests);
        synchronized (measurements) { report.set("measurements", measurements.deepCopy()); }
        Json.putSafe((ObjectNode) report.get("measurements"), "gateWallTimeMs", (System.nanoTime() - started) / 1_000_000);
        report.put("startedAt", startedAt).put("finishedAt", Instant.now().toString()).put("command", o.command());
        report.put("error", error);
        report.put("shard", o.shard());
        report.put("shardPlanSha256", plan == null ? null : plan.sha256());
        report.put("summariesSha256", summariesSha);
        ArrayNode results = report.putArray("results");
        if (acc != null) for (var e : acc.results())
            results.addObject().put("id", e.getKey()).put("ok", e.getValue().getKey()).put("evidence", e.getValue().getValue())
                .put("shard", layout.equals("full") ? null : layout.equals("shard") ? o.shard() : owner.get(e.getKey()));
        report.set("shards", shardsOut);
        report.set("generations", generations);
        report.set("runs", runs);
        if (o.negativeControl() != null) report.set("control", verdict(o, control, plan, acc, error, exit, crashed, evaluated));
        else report.putNull("control");
        try { Json.write(o.out().resolve("report.json"), report); }
        catch (RuntimeException e) {
            // the gate's own failure report goes to stderr when the report directory cannot take it (G-SELF-MISSING-REPORT)
            exit = 1;
            report.put("exit", exit).put("error", "cannot write the report: " + e.getMessage());
            err.println("cannot write the report: " + e.getMessage() + "; the failure report follows");
            err.println(Canonical.utf8(Canonical.bytes(report)));
        }
        out.println(acc == null ? "assertions run 0 / expected 0" : acc.line());
        return exit;
    }

    /** {@code report.control} of a control run: whether it proved what it intends. {@code owned} are the intended ids this layout
     *  evaluates ({@code intendedFailures}); the control holds when every one of them ran, did not crash and failed, the run completed
     *  (no gate error, no coverage problem: every run finished and every cut was compared with the oracle), no scenario run an aggregate
     *  intended id rests on is missing, and the gate exited 1 when it owns an intended id. A crash or a missing run never counts as an
     *  intended failure (owner decision of 2026-11-12). */
    static ObjectNode verdict(Options o, ControlPlan.Control control, Shards.Plan plan, Assertions acc, String error, int exit, Set<String> crashed, Summaries evaluated) {
        ObjectNode v = Json.object().put("name", o.negativeControl());
        ArrayNode intended = v.putArray("intended"), shards = v.putArray("shards"), owned = v.putArray("owned"), failed = v.putArray("failedIntended"), problems = v.putArray("problems");
        if (control != null) { control.intended().forEach(intended::add); control.shards().forEach(shards::add); }
        else problems.add("not a control of " + CONTROLS);
        o.intendedFailures().forEach(owned::add);
        List<String> f = acc == null ? List.of() : acc.failed(), notRun = acc == null ? List.of() : acc.notRun(), ran = acc == null ? List.of() : acc.ran();
        if (error != null) problems.add("the run did not complete: " + error);
        for (String id : o.intendedFailures()) {
            if (!ran.contains(id) || notRun.contains(id)) problems.add(id + " did not run");
            else if (crashed.contains(id)) problems.add(id + " crashed");
            else if (!f.contains(id)) problems.add(id + " held: the control did not make it fail");
            else failed.add(id);
        }
        boolean aggregateOwned = plan != null && o.intendedFailures().stream().anyMatch(plan.aggregate()::contains)
            || plan == null && control != null && o.intendedFailures().stream().anyMatch(Injections.AGGREGATE::contains);
        if (aggregateOwned && evaluated != null && !evaluated.missing.isEmpty()) problems.add("runs an aggregate intended id rests on are missing: " + evaluated.missing);
        if (evaluated != null) {
            for (JsonNode r : evaluated.runs.values()) if (Summaries.failed(r)) problems.add(r.path("name").asText() + ": " + r.path("failure").asText());
            for (JsonNode r : evaluated.shuffled.values()) if (Summaries.failed(r)) problems.add(r.path("name").asText() + " seed " + r.path("seed").asLong() + ": " + r.path("failure").asText());
            for (JsonNode r : evaluated.generationRuns.values()) if (Summaries.failed(r)) problems.add(r.path("name").asText() + ": " + r.path("failure").asText());
        }
        if (!o.intendedFailures().isEmpty() && exit != 1) problems.add("exit " + exit + ", 1 expected");
        v.put("held", problems.isEmpty());
        return v;
    }

    static List<String> manifestIds(JsonNode m) {
        if (!"core".equals(m.path("engine").asText(null)) || !m.path("assertions").isArray()) throw new IllegalArgumentException("not a core assertion manifest");
        List<String> ids = new ArrayList<>();
        for (JsonNode a : m.path("assertions")) {
            Json.text(a, "description");
            if (!a.path("pr").canConvertToInt()) throw new IllegalArgumentException("assertion without pr: " + a);
            ids.add(Json.text(a, "id"));
        }
        return ids;
    }

    private static void strings(ArrayNode a, List<String> l) { l.forEach(a::add); }

    /** {jcs {java, python}, python, jdk, forseti {artifactSha256}}; a value that cannot be found is null (the assertions then fail). */
    static ObjectNode pins(Context ctx) {
        ObjectNode p = Json.object(), jcs = p.putObject("jcs");
        jcs.put("java", JCS_JAVA + ":" + jcsJavaVersion());
        String pyVersion = null, rfc = null;
        try {
            Proc v = ctx.python("pins", "-c", "import sys, importlib.metadata as m; print(sys.version.split()[0]); print(m.version('rfc8785'))");
            String[] lines = v.output().strip().split("\\R");
            if (v.exit() == 0 && lines.length == 2) { pyVersion = lines[0].strip(); rfc = "rfc8785==" + lines[1].strip(); }
        } catch (IOException | InterruptedException | RuntimeException ignored) { /* left null */ }
        jcs.put("python", rfc);
        p.put("python", pyVersion);
        p.put("jdk", System.getProperty("java.vendor") + " " + System.getProperty("java.runtime.version"));
        p.putObject("forseti").put("artifactSha256", artifactSha256());
        return p;
    }

    static String jcsJavaVersion() {
        try (InputStream in = CoreGate.class.getResourceAsStream("/META-INF/maven/io.github.erdtman/java-json-canonicalization/pom.properties")) {
            if (in != null) { Properties pr = new Properties(); pr.load(in); String v = pr.getProperty("version"); if (v != null) return v; }
        } catch (IOException ignored) { /* fall through */ }
        return "unknown";
    }

    /** SHA-256 of the running jar, or {@code unpacked} when the classes are run from a directory (the unit tests). */
    static String artifactSha256() {
        try {
            Path p = Path.of(CoreGate.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            return Files.isRegularFile(p) ? Canonical.digestOf(Files.readAllBytes(p)) : "unpacked";
        } catch (Exception e) { return "unpacked"; }
    }

    /** Each fixture tree's digest: JCS SHA-256 of {relative path: raw-byte SHA-256} over its files, null when the tree is missing. */
    static ObjectNode fixtureDigests(Path root) throws IOException {
        ObjectNode d = Json.object();
        String[][] trees = {{"registry", "fixtures/registry"}, {"registryNext", "fixtures/registry-next"}, {"registryMismatch", "fixtures/registry-mismatch"},
            {"upstream", "fixtures/upstream"}, {"vectors", "vectors"}, {"schemas", "schemas"}, {"scenarios", "fixtures/scenarios"}};
        for (String[] t : trees) d.put(t[0], treeDigest(root.resolve(t[1])));
        return d;
    }

    static String treeDigest(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) return null;
        ObjectNode files = Json.object();
        TreeMap<String, String> sorted = new TreeMap<>();
        try (Stream<Path> s = Files.walk(dir)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) {
                List<String> parts = new ArrayList<>();
                for (Path x : dir.relativize(p)) parts.add(x.toString());
                sorted.put(String.join("/", parts), Canonical.digestOf(Files.readAllBytes(p)));
            }
        }
        sorted.forEach(files::put);
        return Canonical.digest(files);
    }

    static void delete(Path root) {
        try (Stream<Path> w = Files.walk(root)) {
            for (Path p : w.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
        } catch (IOException e) { /* a leftover temporary directory is not a gate result */ }
    }
}
