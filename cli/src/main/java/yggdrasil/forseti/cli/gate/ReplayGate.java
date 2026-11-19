package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import yggdrasil.forseti.cli.gate.CoreGate.Check;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.CoreGate.Result;
import yggdrasil.forseti.cli.gate.Scenarios.Run;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.ReplayRecording;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.cli.replay.ReplayEngine;
import yggdrasil.forseti.cli.replay.Restarts;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/**
 * The engine-neutral replay gate ({@code forseti gate --engine <name>}, spec 705, 728; plan D10, D13): the engine recordings of
 * {@code engine-fixtures/} run on an external engine over Kafka ({@link ReplayEngine}), each compared with the unchanged oracle at every
 * cut by the core gate's own comparison ({@link Scenarios#fold}), against the engine's assertion manifest, shard plan and negative
 * controls ({@code gate/<engine>-assertions.json}, {@code gate/<engine>-shards.json}, {@code gate/<engine>-negative-controls.json}). The
 * injection assertions are the core gate's ({@link Injections}: the same {@code I-*} bodies, here on the engine's runs, with the same ids);
 * the journal-wide ones ({@code J-*}, {@code C-1}, {@code C-2-REFERENCE}, {@code J-AFTER-HORIZON-CUTS}) are evaluated on the run
 * summaries like the core aggregate; and the per-run replay assertions (spec 708-711, 715-718: {@code R-*} engine-independent,
 * {@link ReplayChecks}; the engine's own from {@link ReplayEngine#findings}) are recorded on every run and evaluated over every run
 * summary. Step 7 runs Kafka Streams through this same class.
 *
 * <p>Layouts as the core gate's: {@code --shard <name>} runs that shard's scenarios and generation runs and evaluates its own
 * assertions, writing {@code report.json} and {@code summaries.json}; {@code --aggregate <dir>} reads every shard's output (the same
 * manifest, plan and engine artifact, each shard exactly its own ids) and evaluates the plan's aggregate ids; neither runs every shard in
 * one process. {@code --negative-control <name>} runs one control of the engine's control file on a shard that owns or feeds its intended
 * ids (or, with {@code --aggregate}, over that control's shard runs); its report has {@code kind: negative-control} and a {@code control}
 * verdict that {@code scripts/check-control-report.py} checks: a crash or a missing run never counts as an intended failure.
 * Exit 0 only when every expected id ran and held; 1 otherwise; 2 for a usage error.
 */
public final class ReplayGate {
    private ReplayGate() {}

    /** The integrity findings of the kinds spec 700 names for control theta: duplicates, losses and revision gaps. */
    static final List<String> THETA_FINDINGS = List.of("J-REVISION-CONTINUITY", "J-NO-EQUAL-CONSECUTIVE", "J-PROGRESS-ORDER", "J-DISPOSITION-COVERAGE");

    /** The engine's gate files. */
    public record GateFiles(String manifest, String shards, String controls) {}

    /** {@code flags}: the parsed command line ({@code engine}, {@code out}, {@code python}, {@code shard}, {@code aggregate},
     *  {@code negative-control}, {@code registry}, {@code trusted-keys} and the engine's own, such as {@code kafka}); {@code open}: makes
     *  the engine from them (its broker started), called only when a run is made. */
    public static int main(String engine, Map<String, String> flags, String command, GateFiles files, Function<Map<String, String>, ReplayEngine> open,
                           PrintStream out, PrintStream err) {
        Path root = Path.of("").toAbsolutePath();
        Plan plan;
        List<String> ids;
        Control control = null;
        String shard = flags.get("shard"), aggregateDir = flags.get("aggregate"), controlName = flags.get("negative-control");
        try {
            ids = manifestIds(Json.read(root.resolve(files.manifest())), engine);
            plan = Plan.load(root.resolve(files.shards()), ids);
            if (shard != null && aggregateDir != null) throw new IllegalArgumentException("--shard and --aggregate exclude each other");
            if (shard != null) plan.shard(shard);
            if (controlName != null) {
                control = Control.load(root.resolve(files.controls()), plan, ids).get(controlName);
                if (control == null) throw new IllegalArgumentException("--negative-control " + controlName + ": not in " + files.controls());
                if (shard != null && !control.shards.contains(shard))
                    throw new IllegalArgumentException("--negative-control " + controlName + " runs on the shards " + control.shards + " (" + files.controls() + "), not on " + shard);
                if (aggregateDir != null && control.intended.stream().noneMatch(plan.aggregate::contains))
                    throw new IllegalArgumentException("--negative-control " + controlName + " with --aggregate: it intends no aggregate id (" + control.intended + ")");
                if (shard == null && aggregateDir == null) throw new IllegalArgumentException("--negative-control runs on a --shard or with --aggregate");
            }
            if (aggregateDir != null && !Files.isDirectory(Path.of(aggregateDir))) throw new IllegalArgumentException("--aggregate " + aggregateDir + ": not a directory");
        } catch (EmptyShard e) {
            err.println(e.getMessage());
            out.println("assertions run 0 / expected 0");
            return 1;
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            err.println("usage: forseti gate --engine " + engine + " [--out <dir>] [--python <interpreter>] [--shard <name> | --aggregate <dir>] [--negative-control <name>]");
            return 2;
        }
        Path outDir = Path.of(flags.getOrDefault("out", "build/" + engine)).toAbsolutePath();
        return new Gate(engine, root, outDir, flags, command, files, plan, ids, shard, aggregateDir == null ? null : Path.of(aggregateDir).toAbsolutePath(), control, open,
            out, err).run();
    }

    // --- the plan and the controls -------------------------------------------------------------------------------------------

    /** A shard plan with a shard that runs or asserts nothing: the gate fails (exit 1), not a usage error. */
    static final class EmptyShard extends IllegalStateException { EmptyShard(String m) { super(m); } }

    /** One shard: its scenarios, generation runs, restart runs ({@code <scenario>@restart-<kind>}, {@link Restarts}: each a run of a
     *  scenario the shard also runs, on generations of its own), its assertions and its engine settings. */
    record Shard(String name, List<String> scenarios, List<String> generationRuns, List<String> restartRuns, List<String> assertions, JsonNode settings) {}

    static final class Plan {
        final List<Shard> shards = new ArrayList<>();
        final List<String> aggregate = new ArrayList<>();
        String sha256;
        Shard shard(String name) {
            for (Shard s : shards) if (s.name.equals(name)) return s;
            throw new IllegalArgumentException("no shard " + name);
        }
        /** Every scenario and generation run of the plan, in plan order. */
        List<String> everything() {
            List<String> l = new ArrayList<>();
            for (Shard s : shards) {
                for (String x : s.scenarios) if (!l.contains(x)) l.add(x);
                for (String x : s.generationRuns) if (!l.contains(x)) l.add(x);
                for (String x : s.restartRuns) if (!l.contains(x)) l.add(x);
            }
            return l;
        }

        /** The plan, checked: every manifest id owned by exactly one shard or by the aggregate, the aggregate ids those the gate evaluates
         *  over run summaries, every generation run planned with its scenario. */
        static Plan load(Path file, List<String> ids) {
            byte[] bytes;
            try { bytes = Files.readAllBytes(file); } catch (IOException e) { throw new IllegalArgumentException("no shard plan " + file); }
            JsonNode n = Json.read(bytes);
            Plan p = new Plan();
            p.sha256 = Canonical.digestOf(bytes);
            Set<String> seen = new HashSet<>();
            for (JsonNode s : n.path("shards")) {
                Shard sh = new Shard(Json.text(s, "name"), Shards.texts(s, "scenarios"), Shards.texts(s, "generationRuns"),
                    s.has("restartRuns") ? Shards.texts(s, "restartRuns") : List.of(), Shards.texts(s, "assertions"), s.path("settings").isObject() ? s.get("settings") : Json.object());
                if (!seen.add(sh.name)) throw new IllegalArgumentException(file + ": shard " + sh.name + " twice");
                // spec 705: zero scenarios fail the gate; a shard that runs nothing or asserts nothing is refused (exit 1)
                if (sh.scenarios.isEmpty() || sh.assertions.isEmpty()) throw new EmptyShard(file + ": shard " + sh.name + " has " + sh.scenarios.size() + " scenarios and "
                    + sh.assertions.size() + " assertions; a shard runs at least one scenario and owns at least one assertion");
                for (String g : sh.generationRuns) {
                    Scenarios.GenerationRun gr = Scenarios.generationRun(g);
                    if (gr == null) throw new IllegalArgumentException(file + ": no generation run " + g);
                    if (!sh.scenarios.contains(gr.scenario())) throw new IllegalArgumentException(file + ": shard " + sh.name + " plans " + g + " without " + gr.scenario());
                }
                for (String g : sh.restartRuns) {
                    Restarts.RunName rr = Restarts.parse(g);
                    if (rr == null) throw new IllegalArgumentException(file + ": no restart run " + g + " (<scenario>@restart-<21|22i|22ii|22iii|22iv>)");
                    if (!sh.scenarios.contains(rr.scenario())) throw new IllegalArgumentException(file + ": shard " + sh.name + " plans " + g + " without " + rr.scenario());
                }
                p.shards.add(sh);
            }
            p.aggregate.addAll(Shards.texts(n, "aggregate"));
            Map<String, String> owner = new HashMap<>();
            List<String> wrong = new ArrayList<>();
            for (Shard s : p.shards) for (String id : s.assertions) if (owner.put(id, s.name) != null) wrong.add(id + " owned twice");
            for (String id : p.aggregate) if (owner.put(id, "aggregate") != null) wrong.add(id + " owned twice");
            for (String id : ids) if (!owner.containsKey(id)) wrong.add(id + " owned by no shard");
            for (String id : owner.keySet()) if (!ids.contains(id)) wrong.add(id + " is not a manifest id");
            for (String id : p.aggregate) if (id.startsWith("I-")) wrong.add(id + ": an injection is a shard's assertion");
            for (Shard s : p.shards) for (String id : s.assertions) if (!id.startsWith("I-")) wrong.add(id + " in shard " + s.name + ": only injections are shard assertions");
            if (!wrong.isEmpty()) throw new IllegalArgumentException(file + ": " + String.join("; ", wrong));
            return p;
        }
    }

    record Control(String name, List<String> intended, List<String> shards) {
        /** The intended ids a layout evaluates: a shard's own, or the aggregate's. */
        List<String> owned(Plan plan, String shard, boolean aggregate) {
            if (aggregate) return intended.stream().filter(plan.aggregate::contains).toList();
            Shard s = plan.shard(shard);
            return intended.stream().filter(s.assertions::contains).toList();
        }

        static Map<String, Control> load(Path file, Plan plan, List<String> ids) {
            JsonNode n;
            try { n = Json.read(Files.readAllBytes(file)); } catch (IOException e) { throw new IllegalArgumentException("no control file " + file); }
            Map<String, Control> m = new LinkedHashMap<>();
            for (JsonNode c : n.path("controls")) {
                Control x = new Control(Json.text(c, "name"), Shards.texts(c, "intended"), Shards.texts(c, "shards"));
                List<String> wrong = new ArrayList<>();
                if (!yggdrasil.forseti.cli.harness.Controls.ENGINE_NAMES.contains(x.name)) wrong.add("control " + x.name + " is not implemented");
                for (String id : x.intended) if (!ids.contains(id)) wrong.add(x.name + ": intended " + id + " is not a manifest id");
                Set<String> covered = new HashSet<>();
                for (String s : x.shards) {
                    Shard sh;
                    try { sh = plan.shard(s); } catch (IllegalArgumentException e) { wrong.add(x.name + ": no shard " + s); continue; }
                    covered.addAll(sh.assertions);
                    if (x.intended.stream().noneMatch(sh.assertions::contains) && x.intended.stream().noneMatch(plan.aggregate::contains))
                        wrong.add(x.name + ": shard " + s + " owns none of its intended ids and it intends no aggregate id");
                }
                for (String id : x.intended) if (!plan.aggregate.contains(id) && !covered.contains(id)) wrong.add(x.name + ": intended " + id + " is on none of its shards");
                if (!wrong.isEmpty()) throw new IllegalArgumentException(file + ": " + String.join("; ", wrong));
                m.put(x.name, x);
            }
            return m;
        }
    }

    static List<String> manifestIds(JsonNode m, String engine) {
        if (!engine.equals(m.path("engine").asText(null)) || !m.path("assertions").isArray()) throw new IllegalArgumentException("not a " + engine + " assertion manifest");
        List<String> ids = new ArrayList<>();
        for (JsonNode a : m.path("assertions")) {
            Json.text(a, "description");
            if (!a.path("pr").canConvertToInt()) throw new IllegalArgumentException("assertion without pr: " + a);
            String id = Json.text(a, "id");
            if (ids.contains(id)) throw new IllegalArgumentException("assertion " + id + " twice");
            ids.add(id);
        }
        return ids;
    }

    // --- one gate run --------------------------------------------------------------------------------------------------------

    /** The engine runs a gate run made, by scenario name ({@code dedicated/<name>} for the dedicated placement) or generation run name. */
    static final class EngineRuns implements Scenarios.RunSource {
        final Map<String, Run> runs = new LinkedHashMap<>();
        final Map<String, ReplayEngine.Run> engineRuns = new HashMap<>();
        final Set<String> planned;
        EngineRuns(Collection<String> planned) { this.planned = new LinkedHashSet<>(planned); }
        @Override public Run get(String name) {
            Run r = runs.get(name);
            if (r == null) throw new IllegalStateException(notRun(name));
            return r;
        }
        @Override public Run generationRun(String name) { return get(name); }
        @Override public Scenarios.Shuffled shuffled(String name, long seed) { throw new IllegalStateException("not run: a replay gate makes no --shuffle run (item 6 runs nightly, plan D13)"); }
        @Override public String notRun(String name) { return runs.containsKey(name) ? null : planned.contains(name) ? "not made: its run did not start" : "not run: not in this gate run's shards"; }
    }

    private static final class Gate {
        final String engine, command;
        final Path root, out;
        final Map<String, String> flags;
        final GateFiles files;
        final Plan plan;
        final List<String> ids;
        final String shard;
        final Path aggregate;
        final Control control;
        final Function<Map<String, String>, ReplayEngine> open;
        final PrintStream stdout, stderr;
        final Path registry, keys;
        final Set<String> crashed = new TreeSet<>();

        Gate(String engine, Path root, Path out, Map<String, String> flags, String command, GateFiles files, Plan plan, List<String> ids, String shard, Path aggregate,
             Control control, Function<Map<String, String>, ReplayEngine> open, PrintStream stdout, PrintStream stderr) {
            this.engine = engine; this.root = root; this.out = out; this.flags = flags; this.command = command; this.files = files; this.plan = plan; this.ids = ids;
            this.shard = shard; this.aggregate = aggregate; this.control = control; this.open = open; this.stdout = stdout; this.stderr = stderr;
            this.registry = root.resolve(flags.getOrDefault("registry", "fixtures/live/registry"));
            this.keys = root.resolve(flags.getOrDefault("trusted-keys", "fixtures/live/trusted-keys.jsonl"));
        }

        String layout() { return shard != null ? "shard" : aggregate != null ? "aggregate" : "full"; }

        int run() {
            String startedAt = Instant.now().toString(), error = null, summariesSha = null;
            long started = System.nanoTime();
            Assertions acc = null;
            ArrayNode generations = Json.JSON.createArrayNode(), runsOut = Json.JSON.createArrayNode(), shardsOut = Json.JSON.createArrayNode(), stores = Json.JSON.createArrayNode();
            ObjectNode measurements = Json.object(), engineSummary = Json.object();
            Summaries evaluated = null;
            Path tmp = null;
            byte[] manifestBytes;
            String manifestSha = null;
            List<String> intended = List.of();
            try {
                manifestBytes = Files.readAllBytes(root.resolve(files.manifest()));
                manifestSha = Canonical.digestOf(manifestBytes);
                List<String> expected;
                if (shard != null) expected = plan.shard(shard).assertions;
                else if (aggregate != null && control != null) {
                    // an aggregate control run: its control's shards' assertions and the aggregate ones
                    Set<String> scope = new HashSet<>(plan.aggregate);
                    for (Shard s : plan.shards) if (control.shards.contains(s.name)) scope.addAll(s.assertions);
                    expected = ids.stream().filter(scope::contains).toList();
                } else expected = ids;   // a full run, and the aggregate: every manifest id (the shards' own results merged)
                if (control != null) intended = control.owned(plan, shard, aggregate != null);
                acc = new Assertions(expected, stdout);
                Files.createDirectories(out);
                if (aggregate == null) { CoreGate.delete(out.resolve("runs")); CoreGate.delete(out.resolve("logs")); CoreGate.delete(out.resolve("diffs")); }
                tmp = Files.createTempDirectory("forseti-replay-gate-");
                Context c = new Context(root, tmp, out.resolve("logs"), flags.getOrDefault("python", "python"));
                c.control = control == null ? null : control.name;
                c.scenarios = root.resolve("engine-fixtures/scenarios/shared");
                c.trustedKeys = keys;
                measurements = c.measurements;
                Path state = control == null ? Files.createDirectories(out.resolve("state")) : c.newDir("state");
                if (control == null) CoreGate.delete(state);
                Files.createDirectories(state);
                c.pipeline = new GateEvidence.Store(GateEvidence.PIPELINE, state, control == null);
                List<Check> checks = new ArrayList<>(Injections.checks());
                checks.add(new Check("I-21", RestartAssertions::i21));
                checks.add(new Check("I-22", RestartAssertions::i22));
                for (String id : plan.aggregate) if (!Injections.AGGREGATE.contains(id)) checks.add(new Check(id, x -> perRun(x, id)));
                if (aggregate != null) {
                    ObjectNode merged = Json.object();
                    c.aggregate = readShards(manifestSha, acc, merged, expected);
                    evaluated = c.aggregate;
                    shardsOut = (ArrayNode) merged.get("shards");
                    generations = (ArrayNode) merged.get("generations");
                    runsOut = (ArrayNode) merged.get("runs");
                    for (Check k : checks) if (plan.aggregate.contains(k.id())) check(c, acc, k);
                } else {
                    List<String> names = new ArrayList<>();
                    if (shard != null) { Shard s = plan.shard(shard); names.addAll(s.scenarios); names.addAll(s.generationRuns); names.addAll(s.restartRuns); }
                    else names.addAll(plan.everything());
                    EngineRuns runs = new EngineRuns(names);
                    c.runs = runs;
                    JsonNode settings = shard != null ? plan.shard(shard).settings : Json.object();
                    Map<String, String> engineFlags = new HashMap<>(flags);
                    try (ReplayEngine e = open.apply(engineFlags)) {
                        c.w1Form = e.w1Form();
                        Map<String, String> na = new TreeMap<>();
                        e.notApplicable().forEach((kk, why) -> na.put(kk.spec(), why));
                        c.notApplicable = na;
                        List<Planned> planned = new ArrayList<>();
                        for (String n : names) planned.add(prepare(c, n));
                        ReplayEngine.ControlStore pipeline = e.controlStore(GateEvidence.PIPELINE);
                        stores.add(pipeline.toJson());
                        register(c, e, pipeline, planned, generations, stores);
                        Map<String, Planned> byName = new HashMap<>();
                        for (Planned p : planned) byName.put(p.name, p);
                        for (Planned p : planned) execute(c, e, runs, byName, p, settings, runsOut, engineSummary);
                    }
                    Summaries mine = new Summaries();
                    for (String n : names) {
                        Run r = runs.runs.get(n);
                        if (r == null) { mine.missing.add(n + " not made"); continue; }
                        ObjectNode s = Summaries.of(r);
                        s.put("name", n);
                        if (shard != null) s.put("shard", shard);
                        if (Scenarios.generationRun(n) != null || Restarts.parse(n) != null) mine.generationRuns.put(n, s); else mine.runs.put(n, s);
                    }
                    evaluated = mine;
                    c.aggregate = shard == null ? mine : null;
                    for (Check k : checks) if (expected.contains(k.id())) check(c, acc, k);
                    byte[] bytes = Json.pretty(mine.toJson());
                    Files.write(out.resolve(Shards.SUMMARIES), bytes);
                    summariesSha = Canonical.digestOf(bytes);
                }
            } catch (Exception | Error e) {
                error = e.toString();
                stderr.println("gate crashed: " + error);
                e.printStackTrace(stderr);
            } finally {
                if (tmp != null) CoreGate.delete(tmp);
            }
            int exit = acc == null || error != null ? 1 : acc.exitCode();
            ObjectNode report = Json.object();
            Json.putSafe(report, "exit", exit);
            report.put("kind", control != null ? "negative-control" : "gate").put("layout", layout()).put("negativeControl", control == null ? null : control.name);
            report.put("scope", "replay").put("engine", engine).put("manifest", files.manifest()).put("manifestSha256", manifestSha);
            Json.putSafe(report, "assertionsExpected", acc == null ? 0 : acc.expected().size());
            Json.putSafe(report, "assertionsRun", acc == null ? 0 : acc.ran().size());
            strings(report.putArray("failed"), acc == null ? List.of() : acc.failed());
            strings(report.putArray("notRun"), acc == null ? List.of() : acc.notRun());
            strings(report.putArray("intendedFailures"), intended);
            report.put("engineArtifactSha256", flags.get("engine-artifact"));
            report.put("shard", shard).put("shardPlanSha256", plan.sha256).put("summariesSha256", summariesSha);
            synchronized (measurements) { report.set("measurements", measurements.deepCopy()); }
            Json.putSafe((ObjectNode) report.get("measurements"), "gateWallTimeMs", (System.nanoTime() - started) / 1_000_000);
            report.set("engineRuns", engineSummary);
            report.put("startedAt", startedAt).put("finishedAt", Instant.now().toString()).put("command", command).put("error", error);
            ArrayNode results = report.putArray("results");
            if (acc != null) for (var e : acc.results()) results.addObject().put("id", e.getKey()).put("ok", e.getValue().getKey()).put("evidence", e.getValue().getValue());
            report.set("shards", shardsOut);
            report.set("controlStores", stores);
            report.set("generations", generations);
            report.set("runs", runsOut);
            if (control != null) report.set("control", verdict(intended, acc, error, exit, evaluated));
            else report.putNull("control");
            try { Files.createDirectories(out); Json.write(out.resolve("report.json"), report); }
            catch (IOException | RuntimeException e) { exit = 1; stderr.println("cannot write the report: " + e.getMessage()); stderr.println(Canonical.utf8(Canonical.bytes(report))); }
            stdout.println(acc == null ? "assertions run 0 / expected 0" : acc.line());
            return exit;
        }

        void check(Context c, Assertions acc, Check k) {
            Result r;
            c.current = k.id();
            try { r = k.body().run(c); }
            catch (Exception | Error e) { r = new Result(false, "crashed: " + e); crashed.add(k.id()); }
            finally { c.current = null; }
            try { acc.check(k.id(), r.ok(), r.evidence()); } catch (Assertions.Refused ignored) { /* recorded by the accountant */ }
        }

        /** One planned run of this gate run: the scenario run or generation run, its prepared Run, its directory, its registration. */
        static final class Planned {
            String name;
            Scenarios.GenerationRun g;
            Run r = new Run();
            Path work;
            ReplayEngine.Registered reg;
            /** A restart run: its name's parts, and the generations of its attempts (the first is {@link #reg}). */
            Restarts.RunName restart;
            final List<ReplayEngine.Registered> attempts = new ArrayList<>();
            /** The rule change's first generation (its store's generation 1, on the recording's rule). */
            ReplayEngine.Registered previous;
            GateEvidence.Store store;
        }

        /** Phase 1: every recording of the gate run built and checked against its lock (an event-time replay's materialized from its
         *  scenario's built recording and checked against the committed replay lock), before anything registers or runs. */
        Planned prepare(Context c, String name) {
            Planned p = new Planned();
            p.name = name;
            p.g = Scenarios.generationRun(name);
            p.restart = p.g == null ? Restarts.parse(name) : null;
            Run r = p.r;
            r.name = p.g != null ? p.g.scenario() : p.restart != null ? p.restart.scenario() : name;
            r.label = name.replace('/', '.').replace('@', '.');
            try {
                p.work = Files.createDirectories(out.resolve("runs").resolve(r.label));
                if (p.g == null && p.restart == null) prepare(c, r, scenarioFile(name), p.work.resolve("recording"));
            } catch (Exception | Error x) { r.failure = "crashed: " + x; }
            return p;
        }

        /** Phase 2 (spec 577): every registration of the gate run, through {@code generation new}, before any run starts, one at a time
         *  in a fixed order: every scenario of the run in its order on the pipeline store, then the generation runs in plan order (the
         *  event-time replay on the pipeline store, the rule change's two generations on its own assertion store). A run whose recording
         *  could not be made registers nothing and fails. */
        void register(Context c, ReplayEngine e, ReplayEngine.ControlStore pipeline, List<Planned> planned, ArrayNode generations, ArrayNode stores) throws Exception {
            Map<String, Planned> byName = new HashMap<>();
            for (Planned p : planned) byName.put(p.name, p);
            for (Planned p : planned) {
                if (p.g != null || p.restart != null || p.r.failure != null) continue;
                try {
                    p.reg = e.register(pipeline, c.pipeline.dir(), keys, p.r.manifest.get("rule"), new ReplayEngine.Registration(registry, null, GateEvidence.ONLINE), p.r.recording);
                    p.store = c.pipeline;
                    listed(generations, p, p.reg);
                } catch (Exception | Error x) { p.r.failure = "crashed: registration: " + x; }
            }
            for (Planned p : planned) {
                if (p.g == null) continue;
                Planned base = byName.get(p.g.scenario());
                try {
                    if (base == null || base.r.failure != null) throw new IllegalStateException("its scenario's recording failed or is not made: " + (base == null ? "absent" : base.r.failure));
                    if (GateEvidence.REPLAY.equals(p.g.variant())) {
                        p.reg = e.register(pipeline, c.pipeline.dir(), keys, base.r.manifest.get("rule"), new ReplayEngine.Registration(registry, null, p.g.variant()), base.r.recording);
                        p.store = c.pipeline;
                    } else {
                        Path next = root.resolve(flags.getOrDefault("registry-next", "fixtures/live/registry-next"));
                        ReplayEngine.ControlStore own = e.controlStore(p.g.store());
                        stores.add(own.toJson());
                        Path state = c.newDir("store-" + p.g.assertion());
                        p.store = new GateEvidence.Store(p.g.store(), state, false);
                        p.previous = e.register(own, state, keys, base.r.manifest.get("rule"), new ReplayEngine.Registration(registry, null, GateEvidence.SELF_TEST), base.r.recording);
                        listed(generations, p, p.previous);
                        p.reg = e.register(own, state, keys, base.r.manifest.get("rule"), new ReplayEngine.Registration(next, Scenarios.RULE_NEXT, p.g.variant()), base.r.recording);
                    }
                    listed(generations, p, p.reg);
                } catch (Exception | Error x) { p.r.failure = "crashed: registration: " + x; }
            }
            // the restart runs, after the generation runs in plan order: one generation per attempt it may take (plan D8: an invalid
            // attempt repeats on a fresh run), each on the pipeline store with its scenario's recording and rule, before any run starts
            for (Planned p : planned) {
                if (p.restart == null) continue;
                Planned base = byName.get(p.restart.scenario());
                try {
                    if (base == null || base.r.failure != null) throw new IllegalStateException("its scenario's recording failed or is not made: " + (base == null ? "absent" : base.r.failure));
                    p.store = c.pipeline;
                    for (int i = 0; i < Restarts.ATTEMPTS; i++) {
                        ReplayEngine.Registered x = e.register(pipeline, c.pipeline.dir(), keys, base.r.manifest.get("rule"), new ReplayEngine.Registration(registry, null, GateEvidence.ONLINE),
                            base.r.recording);
                        p.attempts.add(x);
                        listed(generations, p, x);
                    }
                    p.reg = p.attempts.get(0);
                } catch (Exception | Error x) { p.r.failure = "crashed: registration: " + x; }
            }
        }

        static void listed(ArrayNode generations, Planned p, ReplayEngine.Registered x) {
            ObjectNode o = generations.addObject().put("store", x.store().name()).put("state", p.store == null ? null : p.store.state()).put("scenario", p.r.name)
                .put("variant", x.variant()).put("run", p.name);
            o.set("controlStore", x.store().toJson());
            Json.putSafe(o, "generation", x.generation());
            o.put("registeredRowSha256", x.registeredRowSha256()).put("rulesDigest", x.rulesDigest()).put("scope", x.scope());
        }

        /** Phase 3: one run on its registered generation, the oracle comparison at every cut, the integrity checks, the ground truth and
         *  the per-run replay assertions. A failure is recorded on the run. */
        void execute(Context c, ReplayEngine e, EngineRuns runs, Map<String, Planned> byName, Planned p, JsonNode settings, ArrayNode runsOut, ObjectNode engineSummary) {
            Run r = p.r;
            String name = p.name;
            Scenarios.GenerationRun g = p.g;
            long t = System.nanoTime();
            stdout.println("--- " + name);
            try {
                if (r.failure != null) throw new IllegalStateException(r.failure);
                if (p.reg == null) throw new IllegalStateException("not registered");
                ReplayEngine.Request req = g != null ? generationRequest(c, runs, byName, p, settings)
                    : p.restart != null ? restartRequest(c, runs, p, settings)
                    : new ReplayEngine.Request(name, scenarioFile(name), r.recording, registry, p.reg, c.control, "input", p.work, settings);
                ReplayEngine.Run er = e.run(req);
                runs.engineRuns.put(name, er);
                // the generation the run's files belong to: a restart run's valid attempt
                ReplayEngine.Registered ran = er.generation();
                r.generation = new GateEvidence.Registered(p.store, ran.generation(), ran.scope(), ran.rulesDigest(), ran.registeredRowSha256());
                if (p.restart != null) r.restart = Json.read(er.out().resolve(ReplayEngine.INJECTIONS));
                if (p.previous != null) r.previous = new GateEvidence.Registered(p.store, p.previous.generation(), p.previous.scope(), p.previous.rulesDigest(), p.previous.registeredRowSha256());
                r.controlEvidence = ran.store().toJson();
                Json.putSafe(r.controlEvidence, "generation", ran.generation());
                Json.putSafe(r.controlEvidence, "declarationOffset", er.declarationOffset());
                Json.putSafe(r.controlEvidence, "controlEnd", er.controlEnd());
                r.rulesDigest = ran.rulesDigest();
                r.cutLog = Json.read(er.out().resolve("cuts.json"));
                r.runJson = Json.read(er.out().resolve("run.json"));
                r.ms.put("engine", (System.nanoTime() - t) / 1_000_000);
                boolean keep = g != null || name.equals(Injections.RULE_CHANGE_BASE) || keepsJournals(name);
                if (keep) r.journals = er.out();
                Path oracle = c.newDir(r.label + "-oracle").resolve("out");
                Scenarios.fold(c, r, er.out(), oracle, registry, out.resolve("diffs"));
                CoreGate.delete(oracle.getParent());
                if (g != null && GateEvidence.REPLAY.equals(g.variant())) {
                    r.brackets = ReplayRecording.checkRun(r.manifest, r.cutLog);
                    if (!r.brackets.isEmpty()) throw new IllegalStateException("the reported W1 does not bracket the replay deadlines: " + r.brackets);
                    e.redump(runs.engineRuns.get(g.scenario()));   // the online generation's journals, dumped again after the replay
                }
                if (keep) r.census = JournalCensus.of(er.out());
                Map<String, List<String>> findings = new TreeMap<>(ReplayChecks.findings(r, er, runJsonOf(er), e));
                Map<String, List<String>> own = e.findings(er, r.recording);
                for (String id : own.keySet()) if (findings.containsKey(id)) throw new IllegalStateException("the engine reports " + id + ", an engine-independent id of the gate");
                findings.putAll(own);
                r.replay = Json.object();
                findings.forEach((id, l) -> {
                    ObjectNode x = r.replay.putObject(id);
                    Json.putSafe(x, "count", l.size());
                    x.put("describe", l.isEmpty() ? "" : String.join("; ", l.subList(0, Math.min(5, l.size()))) + (l.size() > 5 ? "; ... " + (l.size() - 5) + " more" : ""));
                });
                engineSummary.set(name, er.summary());
                ObjectNode ro = runsOut.addObject().put("run", name).put("scenario", r.name).put("store", ran.store().name());
                ro.set("controlStore", r.controlEvidence);
                Json.putSafe(ro, "generation", ran.generation());
                ro.put("runId", r.runJson.path("runId").asText(null)).put("runRecordSha256", Canonical.digestOf(Files.readAllBytes(er.out().resolve("run.json"))));
                // the run directory keeps the run record, the cut log and the evidence; the journals only where an assertion reads them again
                if (!keep) {
                    CoreGate.delete(er.out().resolve("journals")); CoreGate.delete(er.out().resolve("quarantine"));
                    if (!restartBase(name)) CoreGate.delete(p.work.resolve("recording"));   // a restart run of this scenario reads it later
                }
            } catch (Exception | Error x) {
                if (r.failure == null || !r.failure.startsWith("crashed")) r.failure = "crashed: " + x;
                stderr.println(name + ": " + x);
                x.printStackTrace(stderr);
            }
            r.ms.put("total", (System.nanoTime() - t) / 1_000_000);
            runs.runs.put(name, r);
            try { Files.createDirectories(c.logs); Files.writeString(c.logs.resolve("run-" + r.label + ".log"), r.summary() + "\n" + r.ms + "\n" + (r.replay == null ? "" : r.replay.toString()) + "\n"); }
            catch (IOException | RuntimeException ignored) { /* for reading */ }
            stdout.println("    " + (r.failure != null ? "FAILED " + r.failure : (r.ok() ? "ok " : "NOT OK ") + r.summary()));
        }

        /** Whether a restart run of this gate run runs on scenario {@code name}'s recording. */
        boolean restartBase(String name) {
            List<String> l = shard != null ? plan.shard(shard).restartRuns : plan.everything();
            for (String x : l) { Restarts.RunName rr = Restarts.parse(x); if (rr != null && rr.scenario().equals(name)) return true; }
            return false;
        }

        /** The request of a restart run: its scenario's built and lock-checked recording, after the scenario's own uninterrupted run, on
         *  the generations of its attempts. */
        ReplayEngine.Request restartRequest(Context c, EngineRuns runs, Planned p, JsonNode settings) {
            Run r = p.r, base = runs.runs.get(p.restart.scenario());
            if (base == null || base.failure != null) throw new IllegalStateException("its scenario's run failed or is not made: " + (base == null ? "absent" : base.failure));
            r.recording = base.recording;
            r.registry = registry;
            r.manifest = base.manifest;
            r.expected = base.expected;
            r.manifestSha256 = base.manifestSha256;
            r.synthetic = base.synthetic;
            r.lockLine = base.lockLine;
            return new ReplayEngine.Request(p.name, scenarioFile(p.restart.scenario()), base.recording, registry, p.reg, c.control, "input", p.work, settings,
                new Restarts.Restart(p.restart.kind(), p.attempts));
        }

        boolean keepsJournals(String name) {
            for (Scenarios.GenerationRun g : Scenarios.GENERATION_RUNS) if (g.scenario().equals(name) && GateEvidence.REPLAY.equals(g.variant())) return true;
            return false;
        }

        static JsonNode runJsonOf(ReplayEngine.Run er) throws IOException { return Json.read(er.out().resolve("run.json")); }

        Path scenarioFile(String name) {
            if (name.startsWith("dedicated/")) return root.resolve("engine-fixtures/scenarios/dedicated").resolve(name.substring("dedicated/".length()) + ".json");
            return root.resolve("engine-fixtures/scenarios/shared").resolve(name + ".json");
        }

        /** Builds the recording of {@code scenario} into {@code dir} and checks it against the committed lock, byte for byte. */
        void prepare(Context c, Run r, Path scenario, Path dir) throws IOException {
            long t = System.nanoTime();
            CoreGate.delete(dir);
            ScenarioBuilder.Built b = ScenarioBuilder.build(scenario, registry, dir);
            Path lock = ScenarioBuilder.lockOf(scenario);
            if (!Arrays.equals(Json.pretty(b.lock()), Files.readAllBytes(lock))) throw new IllegalStateException("the rebuilt lock differs from " + root.relativize(lock));
            r.ms.put("build", (System.nanoTime() - t) / 1_000_000);
            Recording rec = Recording.open(dir);
            rec.verifyDigests();
            rec.verifyBatches();
            r.recording = dir;
            r.registry = registry;
            r.manifest = rec.manifest();
            r.expected = rec.expected();
            r.synthetic = Json.read(scenario).path("synthetic").asBoolean(false);
            r.manifestSha256 = Canonical.digestOf(Files.readAllBytes(dir.resolve(Recording.MANIFEST)));
            JsonNode counts = Json.read(lock).path("counts");
            r.lockLine = "built = lock (" + counts.path("messages").asLong() + " messages, " + counts.path("samples").asLong() + " samples, " + counts.path("cuts").asLong() + " cuts)";
        }

        /** The request of a generation run, on its registered generation: the rule change of injection 20 (generation 2 of the store
         *  {@code assertion:I-20}, on {@code fixtures/live/registry-next}'s next rule) or the event-time replay of injection 23 (the replay
         *  recording materialized from its scenario's built recording and checked against the committed replay lock, on its pipeline
         *  generation, after its base run). */
        ReplayEngine.Request generationRequest(Context c, EngineRuns runs, Map<String, Planned> byName, Planned p, JsonNode settings) throws Exception {
            Scenarios.GenerationRun g = p.g;
            Run r = p.r, base = runs.runs.get(g.scenario());
            if (base == null || base.failure != null) throw new IllegalStateException("its scenario's run failed or is not made: " + (base == null ? "absent" : base.failure));
            Path scenario = scenarioFile(g.scenario());
            r.synthetic = base.synthetic;
            r.lockLine = base.lockLine;
            r.genRun = g;
            r.variant = g.variant();
            if (GateEvidence.REPLAY.equals(g.variant())) {
                // the online journals' index, written before the replay starts (Injections.remains reads it after)
                Json.write(base.journals.resolveSibling(base.journals.getFileName() + ".index.json"), digestsOf(base.journals));
                Path dir = p.work.resolve("replay-recording");
                CoreGate.delete(dir);
                ReplayRecording.Result m = ReplayRecording.materialize(base.recording, base.rules.attribution().outOfOrdernessMs(), dir);
                List<String> d = ReplayRecording.compareLock(m.lock(), ReplayRecording.lockOf(scenario));
                if (!d.isEmpty()) throw new IllegalStateException("the materialized replay recording differs from " + root.relativize(ReplayRecording.lockOf(scenario)) + ": " + d);
                Recording rec = Recording.open(dir);
                rec.verifyDigests();
                rec.verifyBatches();
                r.original = base.recording;
                r.recording = dir;
                r.registry = registry;
                r.manifest = rec.manifest();
                r.expected = rec.expected();
                r.manifestSha256 = Canonical.digestOf(Files.readAllBytes(dir.resolve(Recording.MANIFEST)));
                r.lockLine = "materialized = replay lock (" + m.cutIds().size() + " cuts)";
                return new ReplayEngine.Request(g.name(), scenario, dir, registry, p.reg, c.control, "replayed", p.work, settings);
            }
            Path next = root.resolve(flags.getOrDefault("registry-next", "fixtures/live/registry-next"));
            r.recording = base.recording;
            r.manifest = base.manifest;
            r.expected = base.expected;
            r.manifestSha256 = base.manifestSha256;
            r.registry = next;
            String stream = Json.text(base.manifest.get("rule"), "stream");
            JsonNode head = Json.read(next.resolve("mappings").resolve("heads").resolve(stream + ".json"));
            ObjectNode rule = ((ObjectNode) base.manifest.get("rule")).deepCopy();
            rule.put("releaseId", Json.text(head, "id")).put("version", Json.text(head, "version"));
            int at = Scenarios.RULE_NEXT.lastIndexOf('@');
            rule.put("attributionRuleId", Scenarios.RULE_NEXT.substring(0, at)).put("attributionRuleVersion", Scenarios.RULE_NEXT.substring(at + 1));
            r.rule = rule;
            return new ReplayEngine.Request(g.name(), scenario, base.recording, next, p.reg, c.control, "input", p.work, settings);
        }

        static ObjectNode digestsOf(Path dir) throws IOException {
            ObjectNode o = Json.object();
            Scenarios.Pipeline.digests(dir).forEach(o::put);
            return o;
        }

        /** A per-run replay assertion over every run summary of this gate run (or, in an aggregate, of every execution of every shard):
         *  each run recorded it ({@link ReplayChecks}, {@link ReplayEngine#findings}) with no finding; a failed or missing run fails it. */
        static Result perRun(Context c, String id) throws Exception {
            Summaries all = Summaries.of(c);
            List<String> bad = new ArrayList<>(all.missing);
            long checked = 0;
            List<String> where = new ArrayList<>();
            for (JsonNode r : Injections.concat(all.runs.values(), all.generationRuns.values())) {
                String n = (r.path("shard").isTextual() ? r.path("shard").asText() + "/" : "") + r.path("name").asText();
                where.add(n);
                if (Summaries.failed(r)) { bad.add(n + ": " + r.path("failure").asText()); continue; }
                JsonNode f = r.path("replay").path(id);
                if (!f.isObject()) { bad.add(n + ": " + id + " was not checked on this run"); continue; }
                checked++;
                if (f.path("count").asLong() > 0) bad.add(n + ": " + f.path("count").asLong() + " findings: " + f.path("describe").asText());
            }
            if (checked == 0) bad.add("no run checked");
            if (id.equals("C-3-RESTART-SKIPS")) {
                // comparison item 3 (spec 726): the restart skips of every run, as (logical writer, cut) pairs, the reject collectors apart
                ObjectNode m = Json.object();
                long pairs = 0, rejects = 0;
                for (JsonNode r : Injections.concat(all.runs.values(), all.generationRuns.values())) {
                    JsonNode a = r.path("cutAccounting");
                    if (!a.isObject()) continue;
                    String n = (r.path("shard").isTextual() ? r.path("shard").asText() + "/" : "") + r.path("name").asText();
                    if (a.path("item3").asLong() > 0 || a.path("item3RejectCollectors").asLong() > 0) {
                        ObjectNode x = m.putObject(n);
                        x.set("restartSkips", a.path("restartSkips"));
                        x.set("rejectRestartSkips", a.path("rejectRestartSkips"));
                    }
                    pairs += a.path("item3").asLong();
                    rejects += a.path("item3RejectCollectors").asLong();
                }
                Json.putSafe(m, "restartSkipPairs", pairs);
                Json.putSafe(m, "rejectCollectorRestartSkips", rejects);
                c.measure("item3", m);
                where.add(pairs + " restart skip pairs, " + rejects + " of the reject collectors");
            }
            return new Result(bad.isEmpty(), (bad.isEmpty() ? "" : "PROBLEMS: " + String.join("; ", bad) + " || ") + id + " holds on " + checked + " executions " + where);
        }

        /** Every shard's output under {@link #aggregate}: the same manifest, plan and engine artifact, each shard's recorded results for
         *  its own ids, and every execution's summary kept under its own key {@code <shard>/<run>} (a scenario several shards run is
         *  evaluated once per execution). Each shard's coverage is checked against its own summaries. A shard that is missing, ran another
         *  manifest or plan, or recorded another id set is a problem that fails the aggregate (its ids then fail as not run). */
        Summaries readShards(String manifestSha, Assertions acc, ObjectNode merged, List<String> expected) throws IOException {
            Summaries s = new Summaries();
            ArrayNode shards = merged.putArray("shards"), generations = merged.putArray("generations"), runs = merged.putArray("runs");
            List<String> problems = new ArrayList<>();
            String artifact = null;
            for (Shard sh : plan.shards) {
                if (control != null && !control.shards.contains(sh.name)) continue;
                Path d = aggregate.resolve(sh.name);
                JsonNode report, summaries;
                byte[] rb, sb;
                try { rb = Files.readAllBytes(d.resolve("report.json")); sb = Files.readAllBytes(d.resolve(Shards.SUMMARIES)); report = Json.read(rb); summaries = Json.read(sb); }
                catch (IOException | RuntimeException e) { problems.add("shard " + sh.name + ": no readable output under " + d + " (" + e.getMessage() + ")"); for (String x : sh.scenarios) s.missing.add(sh.name + "/" + x + " (shard missing)"); continue; }
                ObjectNode entry = shards.addObject().put("name", sh.name);
                Json.putSafe(entry, "exit", report.path("exit").asLong(-1));
                entry.put("reportSha256", Canonical.digestOf(rb)).put("summariesSha256", Canonical.digestOf(sb));
                if (!manifestSha.equals(report.path("manifestSha256").asText(null))) problems.add("shard " + sh.name + ": manifest " + report.path("manifestSha256").asText());
                if (!plan.sha256.equals(report.path("shardPlanSha256").asText(null))) problems.add("shard " + sh.name + ": shard plan " + report.path("shardPlanSha256").asText());
                if (!sh.name.equals(report.path("shard").asText(null)) || !"shard".equals(report.path("layout").asText(null))) problems.add("shard " + sh.name + ": layout " + report.path("layout") + " shard " + report.path("shard"));
                if (!Canonical.digestOf(sb).equals(report.path("summariesSha256").asText(null))) problems.add("shard " + sh.name + ": its report binds other summaries");
                String a = report.path("engineArtifactSha256").asText(null);
                if (artifact == null) artifact = a; else if (!Objects.equals(artifact, a)) problems.add("shard " + sh.name + ": engine artifact " + a + ", another shard " + artifact);
                String want = control == null ? null : control.name;
                if (!Objects.equals(want, report.path("negativeControl").isTextual() ? report.get("negativeControl").textValue() : null))
                    problems.add("shard " + sh.name + ": negative control " + report.path("negativeControl") + ", expected " + want);
                Set<String> recorded = new TreeSet<>();
                for (JsonNode r : report.path("results")) {
                    String id = r.path("id").asText();
                    recorded.add(id);
                    if (expected.contains(id) && sh.assertions.contains(id))
                        try { acc.check(id, r.path("ok").asBoolean(false), "[shard " + sh.name + "] " + r.path("evidence").asText()); } catch (Assertions.Refused ignored) { /* recorded */ }
                }
                if (!recorded.equals(new TreeSet<>(sh.assertions))) problems.add("shard " + sh.name + ": recorded " + recorded + ", owns " + sh.assertions);
                report.path("generations").forEach(generations::add);
                report.path("runs").forEach(runs::add);
                // this shard's own coverage, from its own summaries
                Set<String> mine = new HashSet<>(), mineGen = new HashSet<>();
                for (JsonNode r : summaries.path("runs")) {
                    String n = r.path("name").asText();
                    if (!mine.add(n)) problems.add("shard " + sh.name + ": two summaries of " + n);
                    ObjectNode x = ((ObjectNode) r).deepCopy().put("shard", sh.name);
                    s.runs.put(sh.name + "/" + n, x);
                }
                summaries.path("generationRuns").fields().forEachRemaining(e -> {
                    mineGen.add(e.getKey());
                    s.generationRuns.put(sh.name + "/" + e.getKey(), ((ObjectNode) e.getValue()).deepCopy().put("shard", sh.name));
                });
                for (String x : sh.scenarios) if (!mine.contains(x)) s.missing.add(sh.name + "/" + x + " (no summary in its shard)");
                for (String x : sh.generationRuns) if (!mineGen.contains(x)) s.missing.add(sh.name + "/" + x + " (no summary in its shard)");
                for (String x : sh.restartRuns) if (!mineGen.contains(x)) s.missing.add(sh.name + "/" + x + " (no summary in its shard)");
                for (String x : mine) if (!sh.scenarios.contains(x)) problems.add("shard " + sh.name + ": a summary of " + x + ", which it does not plan");
            }
            if (!problems.isEmpty()) {
                for (String p : problems) stdout.println("COVERAGE " + p);
                s.missing.addAll(problems);
            }
            return s;
        }

        /** {@code report.control}: the control held when every intended id this layout owns ran, did not crash and failed, every run of
         *  the gate run completed and the gate exited 1 when it owns an intended id (a crash or a missing run never counts). */
        ObjectNode verdict(List<String> owned, Assertions acc, String error, int exit, Summaries evaluated) {
            ObjectNode v = Json.object().put("name", control.name);
            ArrayNode in = v.putArray("intended"), sh = v.putArray("shards"), ow = v.putArray("owned"), failed = v.putArray("failedIntended"), problems = v.putArray("problems");
            control.intended.forEach(in::add);
            control.shards.forEach(sh::add);
            owned.forEach(ow::add);
            List<String> f = acc == null ? List.of() : acc.failed(), notRun = acc == null ? List.of() : acc.notRun(), ran = acc == null ? List.of() : acc.ran();
            if (error != null) problems.add("the run did not complete: " + error);
            for (String id : owned) {
                if (!ran.contains(id) || notRun.contains(id)) problems.add(id + " did not run");
                else if (crashed.contains(id)) problems.add(id + " crashed");
                else if (!f.contains(id)) problems.add(id + " held: the control did not make it fail");
                else failed.add(id);
            }
            if (evaluated != null) {
                for (String m : evaluated.missing) problems.add("missing: " + m);
                for (JsonNode r : Injections.concat(evaluated.runs.values(), evaluated.generationRuns.values()))
                    if (Summaries.failed(r)) problems.add(r.path("name").asText() + ": " + r.path("failure").asText());
                    else if (!r.path("plannedCutIds").equals(r.path("comparedCutIds"))) problems.add(r.path("name").asText() + ": not every cut compared with the oracle");
            }
            if (control.name.equals(yggdrasil.forseti.cli.harness.Controls.NON_TRANSACTIONAL_OUTPUT) && evaluated != null) {
                // theta (spec 700): the intended failure must come from duplicates, losses or revision gaps the gate found in a restart
                // run (an integrity finding of that kind, or an oracle difference), never from any other failure of I-21 or I-22
                ArrayNode tf = v.putArray("thetaFindings");
                for (JsonNode r : evaluated.generationRuns.values()) {
                    String n = r.path("name").asText();
                    if (Restarts.parse(n) == null) continue;
                    List<String> kinds = new ArrayList<>();
                    for (String id : THETA_FINDINGS) if (r.path("findings").path(id).path("count").asLong() > 0) kinds.add(id + " " + r.path("findings").path(id).path("count").asLong());
                    if (r.has("equal") && !r.path("equal").asBoolean(true)) kinds.add("oracle differences");
                    tf.addObject().put("run", n).put("findings", String.join(", ", kinds));
                    if (kinds.isEmpty()) problems.add(n + ": failed without a duplicate, loss or revision-gap finding (spec 700)");
                }
                if (tf.isEmpty()) problems.add("theta: no restart run evaluated");
            }
            if (!owned.isEmpty() && exit != 1) problems.add("exit " + exit + ", 1 expected");
            v.put("held", problems.isEmpty());
            return v;
        }

        private static void strings(ArrayNode a, List<String> l) { l.forEach(a::add); }
    }
}
