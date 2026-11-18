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

    record Shard(String name, List<String> scenarios, List<String> generationRuns, List<String> assertions, JsonNode settings) {}

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
            for (Shard s : shards) { for (String x : s.scenarios) if (!l.contains(x)) l.add(x); for (String x : s.generationRuns) if (!l.contains(x)) l.add(x); }
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
                Shard sh = new Shard(Json.text(s, "name"), Shards.texts(s, "scenarios"), Shards.texts(s, "generationRuns"), Shards.texts(s, "assertions"),
                    s.path("settings").isObject() ? s.get("settings") : Json.object());
                if (!seen.add(sh.name)) throw new IllegalArgumentException(file + ": shard " + sh.name + " twice");
                for (String g : sh.generationRuns) {
                    Scenarios.GenerationRun gr = Scenarios.generationRun(g);
                    if (gr == null) throw new IllegalArgumentException(file + ": no generation run " + g);
                    if (!sh.scenarios.contains(gr.scenario())) throw new IllegalArgumentException(file + ": shard " + sh.name + " plans " + g + " without " + gr.scenario());
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
                if (!yggdrasil.forseti.cli.harness.Controls.NAMES.contains(x.name)) wrong.add("control " + x.name + " is not implemented");
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
            ArrayNode generations = Json.JSON.createArrayNode(), runsOut = Json.JSON.createArrayNode(), shardsOut = Json.JSON.createArrayNode();
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
                    if (shard != null) { Shard s = plan.shard(shard); names.addAll(s.scenarios); names.addAll(s.generationRuns); }
                    else names.addAll(plan.everything());
                    EngineRuns runs = new EngineRuns(names);
                    c.runs = runs;
                    JsonNode settings = shard != null ? plan.shard(shard).settings : Json.object();
                    Map<String, String> engineFlags = new HashMap<>(flags);
                    try (ReplayEngine e = open.apply(engineFlags)) {
                        for (String n : names) make(c, e, runs, n, settings, generations, runsOut, engineSummary);
                    }
                    Summaries mine = new Summaries();
                    for (String n : names) {
                        Run r = runs.runs.get(n);
                        if (r == null) { mine.missing.add(n + " not made"); continue; }
                        ObjectNode s = Summaries.of(r);
                        s.put("name", n);
                        if (Scenarios.generationRun(n) != null) mine.generationRuns.put(n, s); else mine.runs.put(n, s);
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

        /** One scenario or generation run: the recording built and checked against its lock, the engine run, the oracle comparison at
         *  every cut, the integrity checks, the ground truth and the per-run replay assertions. A failure is recorded on the run. */
        void make(Context c, ReplayEngine e, EngineRuns runs, String name, JsonNode settings, ArrayNode generations, ArrayNode runsOut, ObjectNode engineSummary) {
            Run r = new Run();
            Scenarios.GenerationRun g = Scenarios.generationRun(name);
            r.name = g == null ? name : g.scenario();
            r.label = name.replace('/', '.').replace('@', '.');
            long t = System.nanoTime();
            stdout.println("--- " + name);
            Path work = null;
            try {
                work = Files.createDirectories(out.resolve("runs").resolve(r.label));
                ReplayEngine.Request req;
                if (g == null) {
                    prepare(c, r, scenarioFile(name), work.resolve("recording"));
                    req = new ReplayEngine.Request(name, scenarioFile(name), r.recording, registry, keys, c.pipeline.dir(),
                        List.of(new ReplayEngine.Registration(registry, null, GateEvidence.ONLINE)), c.control, "input", null, work, settings);
                } else req = generationRequest(c, runs, g, r, work, settings);
                ReplayEngine.Run er = e.run(req);
                runs.engineRuns.put(name, er);
                List<ReplayEngine.Registered> regs = er.generations();
                GateEvidence.Store store = GateEvidence.PIPELINE.equals(g == null ? GateEvidence.PIPELINE : g.store()) ? c.pipeline
                    : new GateEvidence.Store(g.store(), req.state(), false);
                ReplayEngine.Registered mine = er.generation();
                r.generation = new GateEvidence.Registered(store, mine.generation(), mine.scope(), mine.rulesDigest(), mine.registeredRowSha256());
                if (regs.size() > 1) { ReplayEngine.Registered p = regs.get(0); r.previous = new GateEvidence.Registered(store, p.generation(), p.scope(), p.rulesDigest(), p.registeredRowSha256()); }
                for (ReplayEngine.Registered x : regs) {
                    ObjectNode o = generations.addObject().put("store", store.name()).put("state", store.state()).put("scenario", r.name)
                        .put("variant", g == null ? GateEvidence.ONLINE : g.variant()).put("run", name);
                    Json.putSafe(o, "generation", x.generation());
                    o.put("registeredRowSha256", x.registeredRowSha256()).put("rulesDigest", x.rulesDigest());
                }
                r.rulesDigest = mine.rulesDigest();
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
                Map<String, List<String>> findings = new TreeMap<>(ReplayChecks.findings(r, er, runJsonOf(er)));
                findings.putAll(e.findings(er, r.recording));
                r.replay = Json.object();
                findings.forEach((id, l) -> {
                    ObjectNode x = r.replay.putObject(id);
                    Json.putSafe(x, "count", l.size());
                    x.put("describe", l.isEmpty() ? "" : String.join("; ", l.subList(0, Math.min(5, l.size()))) + (l.size() > 5 ? "; ... " + (l.size() - 5) + " more" : ""));
                });
                engineSummary.set(name, er.summary());
                ObjectNode ro = runsOut.addObject().put("run", name).put("scenario", r.name).put("store", store.name());
                Json.putSafe(ro, "generation", mine.generation());
                ro.put("runId", r.runJson.path("runId").asText(null)).put("runRecordSha256", Canonical.digestOf(Files.readAllBytes(er.out().resolve("run.json"))));
                // the run directory keeps the run record, the cut log and the evidence; the journals only where an assertion reads them again
                if (!keep) { CoreGate.delete(er.out().resolve("journals")); CoreGate.delete(er.out().resolve("quarantine")); CoreGate.delete(work.resolve("recording")); }
            } catch (Exception | Error x) {
                r.failure = "crashed: " + x;
                stderr.println(name + ": " + x);
                x.printStackTrace(stderr);
            }
            r.ms.put("total", (System.nanoTime() - t) / 1_000_000);
            runs.runs.put(name, r);
            try { Files.writeString(c.logs.resolve("run-" + r.label + ".log"), r.summary() + "\n" + r.ms + "\n" + (r.replay == null ? "" : r.replay.toString()) + "\n"); }
            catch (IOException | RuntimeException ignored) { /* for reading */ }
            stdout.println("    " + (r.failure != null ? "FAILED " + r.failure : (r.ok() ? "ok " : "NOT OK ") + r.summary()));
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

        /** The request of a generation run: the rule change of injection 20 (two generations in the store {@code assertion:I-20}, the
         *  second on {@code fixtures/live/registry-next}'s next rule) or the event-time replay of injection 23 (the replay recording
         *  materialized from its scenario's built recording and checked against the committed replay lock; generation 2 of the
         *  pipeline store, on the online run's generation and recovery control topics). */
        ReplayEngine.Request generationRequest(Context c, EngineRuns runs, Scenarios.GenerationRun g, Run r, Path work, JsonNode settings) throws Exception {
            Run base = runs.runs.get(g.scenario());
            ReplayEngine.Run baseRun = runs.engineRuns.get(g.scenario());
            if (base == null || baseRun == null || base.failure != null) throw new IllegalStateException("its scenario's run failed or is not made: " + (base == null ? "absent" : base.failure));
            Path scenario = scenarioFile(g.scenario());
            r.synthetic = base.synthetic;
            r.lockLine = base.lockLine;
            r.genRun = g;
            r.variant = g.variant();
            if (GateEvidence.REPLAY.equals(g.variant())) {
                // the online journals' index, written before the replay starts (Injections.remains reads it after)
                Json.write(base.journals.resolveSibling(base.journals.getFileName() + ".index.json"), digestsOf(base.journals));
                Path dir = work.resolve("replay-recording");
                CoreGate.delete(dir);
                long oo = base.rules.attribution().outOfOrdernessMs();
                ReplayRecording.Result m = ReplayRecording.materialize(base.recording, oo, dir);
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
                return new ReplayEngine.Request(g.name(), scenario, dir, registry, keys, c.pipeline.dir(),
                    List.of(new ReplayEngine.Registration(registry, null, g.variant())), c.control, "replayed", baseRun, work, settings);
            }
            // the rule change: generation 1 on the recording's rule (self-test), generation 2 on registry-next's next rule
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
            Path state = c.newDir("store-" + g.assertion());
            return new ReplayEngine.Request(g.name(), scenario, base.recording, next, keys, state,
                List.of(new ReplayEngine.Registration(registry, null, GateEvidence.SELF_TEST), new ReplayEngine.Registration(next, Scenarios.RULE_NEXT, g.variant())),
                c.control, "input", null, work, settings);
        }

        static ObjectNode digestsOf(Path dir) throws IOException {
            ObjectNode o = Json.object();
            Scenarios.Pipeline.digests(dir).forEach(o::put);
            return o;
        }

        /** A per-run replay assertion over every run summary of this gate run (or, in an aggregate, of every shard): each run recorded
         *  it ({@link ReplayChecks}, {@link ReplayEngine#findings}) with no finding; a failed or missing run fails it. */
        static Result perRun(Context c, String id) throws Exception {
            Summaries all = Summaries.of(c);
            List<String> bad = new ArrayList<>(all.missing);
            long checked = 0;
            for (JsonNode r : Injections.concat(all.runs.values(), all.generationRuns.values())) {
                String n = r.path("name").asText();
                if (Summaries.failed(r)) { bad.add(n + ": " + r.path("failure").asText()); continue; }
                JsonNode f = r.path("replay").path(id);
                if (!f.isObject()) { bad.add(n + ": " + id + " was not checked on this run"); continue; }
                checked++;
                if (f.path("count").asLong() > 0) bad.add(n + ": " + f.path("count").asLong() + " findings: " + f.path("describe").asText());
            }
            if (checked == 0) bad.add("no run checked");
            return new Result(bad.isEmpty(), (bad.isEmpty() ? "" : "PROBLEMS: " + String.join("; ", bad) + " || ") + id + " holds on " + checked + " runs "
                + all.runs.keySet() + " and generation runs " + all.generationRuns.keySet());
        }

        /** Every shard's output under {@link #aggregate}: the same manifest and plan, each shard's recorded results for its own ids, the
         *  merged summaries. A shard that is missing, ran another manifest or plan, or recorded another id set is a problem that fails the
         *  aggregate (its ids then fail as not run). */
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
                catch (IOException | RuntimeException e) { problems.add("shard " + sh.name + ": no readable output under " + d + " (" + e.getMessage() + ")"); for (String x : sh.scenarios) s.missing.add(x + " (shard " + sh.name + " missing)"); continue; }
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
                for (JsonNode r : summaries.path("runs")) s.runs.put(r.path("name").asText(), r);
                summaries.path("generationRuns").fields().forEachRemaining(e -> s.generationRuns.put(e.getKey(), e.getValue()));
                for (String x : sh.scenarios) if (!s.runs.containsKey(x)) s.missing.add(x + " (no summary in shard " + sh.name + ")");
                for (String x : sh.generationRuns) if (!s.generationRuns.containsKey(x)) s.missing.add(x + " (no summary in shard " + sh.name + ")");
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
            if (!owned.isEmpty() && exit != 1) problems.add("exit " + exit + ", 1 expected");
            v.put("held", problems.isEmpty());
            return v;
        }

        private static void strings(ArrayNode a, List<String> l) { l.forEach(a::add); }
    }
}
