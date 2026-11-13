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
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.cli.gate.CoreGate.Check;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.CoreGate.Result;
import yggdrasil.forseti.cli.gate.Foundation.Problems;
import yggdrasil.forseti.cli.generation.FileGenerationStore;
import yggdrasil.forseti.cli.generation.GateReportReader;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationPolicy;
import yggdrasil.forseti.core.release.FloorStore;

/** The generation-control assertions ({@code F-GEN-*}): {@code forseti generation new} and the run preparation of {@code forseti run}
 *  (pull request 5a), the lifecycle through the CLI and {@code promote}'s refusals on fixture reports (5b), each in its own temporary store
 *  ({@code store: assertion:<ID>}, {@code isolated}, {@code variant: self-test}), never in the gate's pipeline store or its {@code --state}.
 *  The clock is the fixture clock, the head's signedAt + 60 s. No gate is launched inside the gate. */
final class GenerationAssertions {
    private GenerationAssertions() {}

    static final String SMOKE = Stage1Assertions.SMOKE, RULE = "line2-attribution@1.0.0", RULE_NEXT = "line2-attribution@1.0.1";

    static List<Check> checks() {
        return List.of(
            new Check("F-GEN-NEW", GenerationAssertions::created),
            new Check("F-GEN-REFUSES-UNREGISTERED", GenerationAssertions::unregistered),
            new Check("F-GEN-FLOOR", GenerationAssertions::floor),
            new Check("F-GEN-LIFECYCLE", GenerationAssertions::lifecycle),
            new Check("F-GEN-PROMOTE-REQUIRES-REPORT", GenerationAssertions::promoteRequiresReport));
    }

    /** {@code forseti generation <args>} in-process with the fixture clock of {@code line2} and the repository root as {@code promote}'s. */
    static Out cli(Context c, String... args) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        Path m = c.fixtures().resolve("registry").resolve("mappings");
        int exit = GenerationCli.run(args, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8),
            Clock.fixed(Instant.ofEpochMilli(GateSigner.now(m, "line2")), ZoneOffset.UTC), c.root);
        return new Out(exit, (so.toString(StandardCharsets.UTF_8).strip() + "\n" + se.toString(StandardCharsets.UTF_8).strip()).strip());
    }

    /** The {@code refusal} of the first JSON value a verb printed, null when it printed none. */
    static String refusal(Out o) {
        try (com.fasterxml.jackson.core.JsonParser parser = new com.fasterxml.jackson.core.JsonFactory().createParser(o.stdout())) {
            JsonNode n = new com.fasterxml.jackson.databind.ObjectMapper().readTree(parser);
            return n == null ? null : n.path("refusal").asText(null);
        } catch (IOException | RuntimeException e) { return null; }
    }

    /** {@code F-GEN-LIFECYCLE}: in its own store, through the CLI only: {@code new} writes one {@code registered} row; {@code run} on
     *  {@code smoke} adds no row and one declaration; {@code promote} without a report is a usage error and writes nothing;
     *  {@code retire} writes {@code retired}; a second {@code retire} and a {@code run} on the retired generation are refused and write
     *  nothing; and {@link GenerationPolicy} allows exactly the transitions of spec "Generations and revisions". No {@code replaying},
     *  {@code verified} or {@code current} row is written: the gate never promotes on a report it wrote (Step 21.7 does that). */
    static Result lifecycle(Context c) throws Exception {
        Problems p = new Problems();
        Path rec = c.path(SMOKE), registry = c.fixtures().resolve("registry");
        GateEvidence.Store store = c.assertionStore();
        Path state = store.dir(), file = state.resolve(FileGenerationStore.FILE), declarations = state.resolve(ReferenceEngine.RECOVERY_CONTROL);
        GateEvidence.Registered g = GateEvidence.register(c, store, registry, rec, "smoke", GateEvidence.SELF_TEST, c.current);
        p.equal(1, lines(file).size(), "rows after new");
        p.equal("registered", status(file, 0), "the row's status");
        Path out = c.newDir("gen-lifecycle-run").resolve("out");
        Out o = run(c, state, rec, g.generation(), out);
        p.equal(0, o.exit(), "run exit (" + o.stdout() + ")");
        if (o.exit() == 0) GateEvidence.keep(c, g, out, rec, "smoke", GateEvidence.SELF_TEST, ReferenceEngine.DEFAULT_SEED, false, GateEvidence.safe(store.name()) + ".smoke");
        p.equal(1, lines(file).size(), "rows after run");
        p.equal(1, lines(declarations).size(), "declarations after run");
        String n = Long.toString(g.generation()), keys = c.fixtures().resolve("trusted-keys.jsonl").toString(), m = registry.resolve("mappings").toString();
        byte[] before = Files.readAllBytes(file);
        Out promote = cli(c, "promote", "--state", state.toString(), "--mappings", m, "--trusted-keys", keys, "--generation", n, "--registered-row-sha256", g.registeredRowSha256(),
            "--run", out.resolve("run.json").toString());
        p.equal(2, promote.exit(), "promote without --gate-report (" + promote.stdout() + ")");
        p.require(Arrays.equals(before, Files.readAllBytes(file)), "promote without a report changed generations.jsonl");
        Out retire = cli(c, "retire", "--state", state.toString(), "--generation", n);
        p.equal(0, retire.exit(), "retire exit (" + retire.stdout() + ")");
        p.equal(2, lines(file).size(), "rows after retire");
        p.equal("retired", status(file, 1), "the second row's status");
        byte[] retired = Files.readAllBytes(file);
        Out again = cli(c, "retire", "--state", state.toString(), "--generation", n);
        p.equal(1, again.exit(), "a second retire exit (" + again.stdout() + ")");
        p.equal("generation-transition", refusal(again), "a second retire's refusal");
        Path out2 = c.newDir("gen-lifecycle-retired").resolve("out");
        Out ran = run(c, state, rec, g.generation(), out2);
        p.equal(1, ran.exit(), "run on the retired generation exit (" + ran.stdout() + ")");
        p.equal("generation-retired", refusal(ran), "run on the retired generation's refusal");
        p.require(!Files.exists(out2.resolve("run.json")), "a run record of the retired generation was written");
        p.require(Arrays.equals(retired, Files.readAllBytes(file)), "a refusal changed generations.jsonl");
        p.equal(1, lines(declarations).size(), "declarations after the refusals");
        Set<String> statuses = new TreeSet<>();
        for (String l : lines(file)) statuses.add(Json.read(l.getBytes(StandardCharsets.UTF_8)).path("value").path("status").asText());
        p.equal(Set.of("registered", "retired"), statuses, "statuses written");
        Set<String> allowed = Set.of("null>registered", "registered>replaying", "registered>retired", "replaying>verified", "verified>current", "current>retired");
        int pairs = 0;
        List<String> froms = new ArrayList<>(Generation.STATUSES);
        froms.add(null);
        for (String from : froms) for (String to : Generation.STATUSES) {
            pairs++;
            p.equal(allowed.contains(from + ">" + to), GenerationPolicy.allowed(from, to), "GenerationPolicy.allowed(" + from + ", " + to + ")");
        }
        return p.result("own store: new -> 1 registered row; run on smoke -> 1 row, 1 declaration; promote without --gate-report -> exit 2, no row; retire -> retired; "
            + "retire again -> generation-transition; run on the retired generation -> generation-retired, no run record; the store unchanged by every refusal; "
            + "GenerationPolicy allows exactly 6 of " + pairs + " status pairs; no replaying, verified or current row");
    }

    static String status(Path file, int line) throws IOException {
        List<String> l = lines(file);
        return line < l.size() ? Json.read(l.get(line).getBytes(StandardCharsets.UTF_8)).path("value").path("status").asText(null) : null;
    }

    /** One change of the fixture bundle (files edited, then restored) and the request arguments that go with it. */
    record Change(String what, String code, List<String> args, ChangeBody body) {}
    @FunctionalInterface interface ChangeBody { void apply() throws Exception; }

    /** {@code F-GEN-PROMOTE-REQUIRES-REPORT}: in its own store (the bundle's {@code state/}) with a registered generation and its
     *  {@code smoke} run: {@code promote} without a report or without a run is a usage error. A complete full fixture bundle around that
     *  store (every committed manifest id, every planned scenario, shuffled and generation run; the smoke run the real one, every other
     *  run a stand-in citing the same generation; {@code store: pipeline}, {@code state: shared}) passes {@link GateReportReader} with scope
     *  {@code core} accepted, checked without calling {@code promote}; then each single change is refused by {@code promote} with its code
     *  and leaves the store's bytes unchanged. No gate is launched inside the gate. */
    static Result promoteRequiresReport(Context c) throws Exception {
        Problems p = new Problems();
        Path rec = c.path(SMOKE), registry = c.fixtures().resolve("registry"), next = c.fixtures().resolve("registry-next");
        Path bundle = c.newDir("promote-bundle"), state = bundle.resolve(GateEvidence.STATE);
        GateEvidence.Store store = new GateEvidence.Store(c.evidence.storeName(c.current), state, false);
        GateEvidence.Registered g = GateEvidence.register(c, store, registry, rec, "smoke", GateEvidence.SELF_TEST, c.current);
        Path out = c.newDir("promote-smoke").resolve("out");
        Out o = run(c, state, rec, g.generation(), out);
        p.equal(0, o.exit(), "run exit (" + o.stdout() + ")");
        if (o.exit() != 0) return p.result("");
        GateEvidence.keep(c, g, out, rec, "smoke", GateEvidence.SELF_TEST, ReferenceEngine.DEFAULT_SEED, false, GateEvidence.safe(store.name()) + ".smoke");
        // the rule-change generation run of the bundle needs a rule-1.0.1 row: registered in the same store, listed self-test
        GateEvidence.Registered next2 = GateEvidence.register(c, store, next, "line2", Scenarios.RULE_NEXT, rec, "smoke", GateEvidence.SELF_TEST, c.current);
        Path file = state.resolve(FileGenerationStore.FILE);
        String keys = c.fixtures().resolve("trusted-keys.jsonl").toString(), m = registry.resolve("mappings").toString(), n = Long.toString(g.generation());
        byte[] stored = Files.readAllBytes(file);
        Out noReport = cli(c, "promote", "--state", state.toString(), "--mappings", m, "--trusted-keys", keys, "--generation", n, "--registered-row-sha256", g.registeredRowSha256(),
            "--run", out.resolve("run.json").toString());
        Out noRun = cli(c, "promote", "--state", state.toString(), "--mappings", m, "--trusted-keys", keys, "--generation", n, "--registered-row-sha256", g.registeredRowSha256(),
            "--gate-report", bundle.resolve("report.json").toString());
        p.equal(2, noReport.exit(), "promote without --gate-report (" + noReport.stdout() + ")");
        p.equal(2, noRun.exit(), "promote without --run (" + noRun.stdout() + ")");
        p.require(Arrays.equals(stored, Files.readAllBytes(file)), "a usage error changed generations.jsonl");

        Path report = fixtureBundle(c, bundle, g, next2, out);
        Path smokeRun = bundle.resolve(GateEvidence.RUNS).resolve("smoke.run.json");
        try {
            GateReportReader.read(new GateReportReader.Request(c.root, report, null, state, g.generation(), g.registeredRowSha256(), smokeRun, true), new FileGenerationStore(state));
        } catch (ReferenceEngine.Refused r) { p.list.add("the complete fixture bundle is refused: " + r.toJson()); return p.result(""); }

        List<String> base = List.of("promote", "--state", state.toString(), "--mappings", m, "--trusted-keys", keys);
        List<String> target = List.of("--generation", n, "--registered-row-sha256", g.registeredRowSha256(), "--run", smokeRun.toString(), "--gate-report", report.toString());
        List<String> accept = List.of("--accept-scope", "core");
        byte[] good = Files.readAllBytes(report), goodSummaries = Files.readAllBytes(bundle.resolve(Shards.SUMMARIES));
        List<String> ids = CoreGate.manifestIds(Json.read(c.root.resolve(CoreGate.MANIFEST)));
        Shards.Plan plan = Shards.load(c.root);
        java.util.function.Consumer<java.util.function.Consumer<ObjectNode>> edit = change -> {
            try { FixtureBundles.rewrite(report, change); } catch (IOException e) { throw new java.io.UncheckedIOException(e); }
        };
        Path changedRun = c.newDir("promote-changed").resolve("smoke.run.json");
        Files.write(changedRun, (Files.readString(smokeRun, StandardCharsets.UTF_8) + "\n").getBytes(StandardCharsets.UTF_8));
        JsonNode rr = Json.read(bundle.resolve(GateEvidence.RUNS).resolve("baseline.shuffle-7.run.json"));
        Scenarios.GenerationRun ruleChange = Scenarios.generationRun("baseline@rule-1.0.1");
        List<Change> changes = List.of(
            new Change("a shard report", GateReportReader.SHARD, cat(target, accept), () -> edit.accept(r -> r.put("layout", "shard").put("shard", FixtureBundles.HAND_MADE))),
            new Change("a control report", GateReportReader.NEGATIVE_CONTROL, cat(target, accept), () -> edit.accept(r -> r.put("kind", "negative-control"))),
            new Change("exit 1", GateReportReader.FAILED, cat(target, accept), () -> edit.accept(r -> Json.putSafe(r, "exit", 1L))),
            new Change("another manifest digest", GateReportReader.MANIFEST, cat(target, accept), () -> edit.accept(r -> r.put("manifestSha256", "0".repeat(64)))),
            new Change("0/0", GateReportReader.ASSERTIONS, cat(target, accept), () -> edit.accept(r -> { ((ArrayNode) r.get("results")).removeAll(); Json.putSafe(r, "assertionsExpected", 0L); Json.putSafe(r, "assertionsRun", 0L); })),
            new Change("a reduced k/k", GateReportReader.ASSERTIONS, cat(target, accept), () -> edit.accept(r -> {
                ArrayNode res = (ArrayNode) r.get("results");
                for (int i = 0; i < 5; i++) res.remove(res.size() - 1);
                Json.putSafe(r, "assertionsExpected", (long) ids.size() - 5);
                Json.putSafe(r, "assertionsRun", (long) ids.size() - 5);
            })),
            new Change("a count above N", GateReportReader.ASSERTIONS, cat(target, accept), () -> edit.accept(r -> { Json.putSafe(r, "assertionsExpected", (long) ids.size() + 1); Json.putSafe(r, "assertionsRun", (long) ids.size() + 1); })),
            new Change("N results with one id repeated", GateReportReader.ASSERTIONS, cat(target, accept), () -> edit.accept(r -> {
                ArrayNode res = (ArrayNode) r.get("results");
                ((ObjectNode) res.get(res.size() - 1)).put("id", res.get(0).path("id").asText());
            })),
            new Change("an aggregate report without one shard in shards[]", GateReportReader.LAYOUT, cat(target, accept), () -> edit.accept(r -> aggregate(r, plan, true, null, null))),
            new Change("another plan digest", GateReportReader.LAYOUT, cat(target, accept), () -> edit.accept(r -> aggregate(r, plan, false, "0".repeat(64), null))),
            new Change("a result under the wrong owner", GateReportReader.LAYOUT, cat(target, accept), () -> edit.accept(r -> aggregate(r, plan, false, null, "I-23"))),
            new Change("an aggregate report without --shards", GateReportReader.EVIDENCE, cat(target, accept), () -> edit.accept(r -> aggregate(r, plan, false, null, null))),
            new Change("an inserted run no summary covers", GateReportReader.EVIDENCE, cat(target, accept), () -> {
                Path runs = bundle.resolve(GateEvidence.RUNS);
                Files.copy(runs.resolve("smoke.cuts.json"), runs.resolve("smoke.extra.cuts.json"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                String d = FixtureBundles.rewrite(Files.copy(smokeRun, runs.resolve("smoke.extra.run.json"), java.nio.file.StandardCopyOption.REPLACE_EXISTING), x -> x.put("runId", "run-extra"));
                edit.accept(r -> {
                    ObjectNode e = FixtureBundles.runEntry(r, null, "smoke").deepCopy();
                    e.put("runId", "run-extra").put("runRecord", "runs/smoke.extra.run.json").put("runRecordSha256", d).put("cutLog", "runs/smoke.extra.cuts.json");
                    ((ArrayNode) r.get("runs")).add(e);
                });
            }),
            new Change("a summary with a compared cut removed", GateReportReader.EVIDENCE, cat(target, accept), () -> {
                ObjectNode s = (ObjectNode) Json.read(goodSummaries);
                for (JsonNode x : s.get("runs")) if (x.path("name").asText().equals("baseline")) { ArrayNode a = (ArrayNode) x.get("comparedCutIds"); a.remove(a.size() - 1); }
                byte[] sb = Json.pretty(s);
                Files.write(bundle.resolve(Shards.SUMMARIES), sb);
                edit.accept(r -> r.put("summariesSha256", Canonical.digestOf(sb)));
            }),
            new Change("another registeredRowSha256", GateReportReader.GENERATION, List.of("--generation", n, "--registered-row-sha256", "e".repeat(64), "--run", smokeRun.toString(),
                "--gate-report", report.toString(), "--accept-scope", "core"), () -> {}),
            new Change("the right digest under store " + ruleChange.store() + " only", GateReportReader.GENERATION, List.of("--generation", Long.toString(next2.generation()),
                "--registered-row-sha256", next2.registeredRowSha256(), "--run", bundle.resolve(GateEvidence.RUNS).resolve(ruleChange.label() + ".run.json").toString(),
                "--gate-report", report.toString(), "--accept-scope", "core"), () -> {}),
            new Change("state isolated", GateReportReader.ISOLATED, cat(target, accept), () -> edit.accept(r -> {
                for (JsonNode x : r.get("generations")) if (x.path("registeredRowSha256").asText().equals(g.registeredRowSha256())) ((ObjectNode) x).put("state", GateEvidence.ISOLATED);
                for (JsonNode x : r.get("runs")) if (x.path("registeredRowSha256").asText().equals(g.registeredRowSha256())) ((ObjectNode) x).put("state", GateEvidence.ISOLATED);
            })),
            new Change("another runRecordSha256", GateReportReader.RUN, List.of("--generation", n, "--registered-row-sha256", g.registeredRowSha256(), "--run", changedRun.toString(),
                "--gate-report", report.toString(), "--accept-scope", "core"), () -> {}),
            new Change("a shuffled run", GateReportReader.RUN, List.of("--generation", n, "--registered-row-sha256", g.registeredRowSha256(), "--run",
                bundle.resolve(GateEvidence.RUNS).resolve("baseline.shuffle-7.run.json").toString(), "--gate-report", report.toString(), "--accept-scope", "core"), () -> {}),
            new Change("scope core without --accept-scope", GateReportReader.SCOPE, target, () -> {}));
        List<String> refused = new ArrayList<>();
        for (Change ch : changes) {
            Files.write(report, good);
            Files.write(bundle.resolve(Shards.SUMMARIES), goodSummaries);
            ch.body().apply();
            byte[] before = Files.readAllBytes(file);
            Out r = cli(c, cat(base, ch.args()).toArray(String[]::new));
            p.equal(1, r.exit(), ch.what() + ": promote exit (" + r.stdout() + ")");
            p.equal(ch.code(), refusal(r), ch.what() + ": refusal");
            p.require(Arrays.equals(before, Files.readAllBytes(file)), ch.what() + ": generations.jsonl changed");
            if (r.exit() == 1 && ch.code().equals(refusal(r))) refused.add(ch.code());
        }
        Files.write(report, good);
        Files.write(bundle.resolve(Shards.SUMMARIES), goodSummaries);
        p.require(rr.path("shuffle").asBoolean(), "the shuffled stand-in run is not shuffled");
        return p.result("own store, generation " + g.generation() + " and its smoke run: promote without --gate-report or --run -> exit 2, no row; a complete full fixture bundle ("
            + ids.size() + " results, every planned scenario, shuffled and generation run, kept journals) accepted by GateReportReader with scope core; " + refused.size() + " of "
            + changes.size() + " single changes refused by promote with their code, the store's bytes unchanged each time: " + String.join(", ", refused));
    }

    static List<String> cat(List<String> a, List<String> b) { List<String> l = new ArrayList<>(a); l.addAll(b); return l; }

    /** Turns a full fixture report into an aggregate one whose layout fields are those of the committed plan ({@code dropShard}: one shard
     *  missing from {@code shards[]}; {@code planSha}: another plan digest; {@code wrongOwner}: that id's result under another shard). */
    static void aggregate(ObjectNode r, Shards.Plan plan, boolean dropShard, String planSha, String wrongOwner) {
        r.put("layout", "aggregate").putNull("summariesSha256").put("shardPlanSha256", planSha != null ? planSha : plan.sha256());
        Map<String, String> owner = new HashMap<>();
        for (Shards.Shard s : plan.shards()) for (String id : s.assertions()) owner.put(id, s.name());
        for (String id : plan.aggregate()) owner.put(id, "aggregate");
        for (JsonNode x : r.get("results")) {
            String id = x.path("id").asText(), o = owner.get(id);
            if (id.equals(wrongOwner)) o = FixtureBundles.HAND_MADE.equals(o) ? "aggregate" : FixtureBundles.HAND_MADE;
            ((ObjectNode) x).put("shard", o);
        }
        ArrayNode shards = r.putArray("shards");
        List<String> names = new ArrayList<>(plan.names());
        if (dropShard) names.remove(names.size() - 1);
        for (String s : names) Json.putSafe(shards.addObject().put("name", s), "exit", 0L).put("reportSha256", "a".repeat(64)).put("summariesSha256", "b".repeat(64));
    }

    /** The full fixture bundle around {@code bundle/state}: its {@code smoke} run is the real run in {@code smokeOut} on generation
     *  {@code g}; every other scenario's run, every shuffled run and the event-time replay are stand-ins citing {@code g}; the rule change
     *  is a stand-in in store {@code assertion:I-20} citing the rule-1.0.1 row {@code next}. The report's pins are the real run record's.
     *  Returns {@code bundle/report.json}. */
    static Path fixtureBundle(Context c, Path bundle, GateEvidence.Registered g, GateEvidence.Registered next, Path smokeOut) throws IOException {
        Path state = bundle.resolve(GateEvidence.STATE), runs = Files.createDirectories(bundle.resolve(GateEvidence.RUNS));
        JsonNode smokeRecord = Json.read(smokeOut.resolve("run.json")), smokeCuts = Json.read(smokeOut.resolve("cuts.json"));
        ObjectNode common = FixtureBundles.common(c.root);
        ((ObjectNode) common.path("pins").path("forseti")).set("artifactSha256", smokeRecord.path("artifactSha256"));
        ((ObjectNode) common.path("pins").path("jcs")).set("java", smokeRecord.path("jcs").path("java"));
        ObjectNode report = FixtureBundles.report(common, "full", null), summaries = Json.object();
        List<String> ids = CoreGate.manifestIds(Json.read(c.root.resolve(CoreGate.MANIFEST)));
        Json.putSafe(report, "assertionsExpected", ids.size());
        Json.putSafe(report, "assertionsRun", ids.size());
        report.putNull("shardPlanSha256");
        ArrayNode results = report.putArray("results");
        for (String id : ids) results.addObject().put("id", id).put("ok", true).put("evidence", "fixture").putNull("shard");
        report.putArray("shards");
        ArrayNode gens = report.putArray("generations"), entries = report.putArray("runs");
        ObjectNode gEntry = FixtureBundles.genEntry(null, GateEvidence.PIPELINE, g.generation(), g.registeredRowSha256(), g.scope(), g.rulesDigest(), GateEvidence.SHARED,
            "smoke", GateEvidence.ONLINE, null);
        gens.add(gEntry);
        FixtureBundles.Gen gen = new FixtureBundles.Gen(gEntry, g.generation(), g.registeredRowSha256(), g.scope(), FixtureBundles.row(state, g.generation()));
        ArrayNode runSummaries = summaries.putArray("runs"), shuffled = summaries.putArray("shuffled");
        for (String sc : Scenarios.all()) {
            if (!sc.equals("smoke")) {
                entries.add(FixtureBundles.standInRun(c.root, bundle, null, GateEvidence.PIPELINE, gen, sc, GateEvidence.ONLINE, CoreGate.DEFAULT_SEED, false, sc, common));
                runSummaries.add(FixtureBundles.summary(c.root, sc));
                continue;
            }
            byte[] rb = Files.readAllBytes(smokeOut.resolve("run.json")), cb = Files.readAllBytes(smokeOut.resolve("cuts.json"));
            Files.write(runs.resolve("smoke.run.json"), rb);
            Files.write(runs.resolve("smoke.cuts.json"), cb);
            ObjectNode e = Json.object().putNull("shard").put("store", GateEvidence.PIPELINE).put("scenario", "smoke").put("variant", GateEvidence.ONLINE);
            Json.putSafe(e, "seed", smokeRecord.path("seed").asLong());
            e.put("shuffle", false).put("runId", smokeRecord.path("runId").asText());
            Json.putSafe(e, "generation", g.generation());
            e.put("registeredRowSha256", g.registeredRowSha256()).put("state", GateEvidence.SHARED);
            e.put("runRecord", "runs/smoke.run.json").put("runRecordSha256", Canonical.digestOf(rb)).put("cutLog", "runs/smoke.cuts.json").put("cutLogSha256", Canonical.digestOf(cb));
            e.put("recordingManifestSha256", smokeRecord.path("fixtureDigests").path("recordingManifestSha256").asText()).putNull("originalRecordingManifestSha256");
            e.set("offsetRanges", smokeRecord.path("offsetRanges").deepCopy());
            e.putNull("journals").putNull("journalsIndexSha256");
            entries.add(e);
            ObjectNode s = FixtureBundles.summary(c.root, "smoke");
            ArrayNode planned = s.putArray("plannedCutIds"), logged = s.putArray("cutLogIds"), compared = s.putArray("comparedCutIds");
            for (JsonNode k : smokeCuts.path("cuts")) { planned.add(k.path("cutId").asLong()); logged.add(k.path("cutId").asLong()); compared.add(k.path("cutId").asLong()); }
            runSummaries.add(s);
        }
        for (String sc : Scenarios.SHUFFLED) for (long seed : Scenarios.SHUFFLE_SEEDS) {
            entries.add(FixtureBundles.standInRun(c.root, bundle, null, GateEvidence.PIPELINE, gen, sc, GateEvidence.ONLINE, seed, true, sc + ".shuffle-" + seed, common));
            ObjectNode x = Json.object().put("name", sc);
            Json.putSafe(x, "seed", seed);
            x.setAll((ObjectNode) FixtureBundles.summary(c.root, sc).without(List.of("name")));
            x.putObject("ms");
            shuffled.add(x);
        }
        ObjectNode generationRuns = summaries.putObject("generationRuns");
        JsonNode nextRow = FixtureBundles.row(state, next.generation());
        for (Scenarios.GenerationRun gr : Scenarios.GENERATION_RUNS) {
            ObjectNode e;
            if (GateEvidence.PIPELINE.equals(gr.store())) {
                e = FixtureBundles.standInRun(c.root, bundle, null, GateEvidence.PIPELINE, gen, gr.scenario(), gr.variant(), CoreGate.DEFAULT_SEED, false, gr.label(), common);
                FixtureBundles.journals(bundle, e, gr.scenario(), gr.variant());
                FixtureBundles.journals(bundle, FixtureBundles.runEntry(report, null, gr.scenario()), gr.scenario(), GateEvidence.ONLINE);
            } else {
                gens.add(FixtureBundles.genEntry(null, gr.store(), 1, Canonical.digest(Json.object().put("fixture", gr.store())), "line2", g.rulesDigest(), GateEvidence.ISOLATED,
                    gr.scenario(), GateEvidence.SELF_TEST, gr.assertion()));
                ObjectNode ge = FixtureBundles.genEntry(null, gr.store(), 2, next.registeredRowSha256(), next.scope(), next.rulesDigest(), GateEvidence.ISOLATED, gr.scenario(),
                    gr.variant(), gr.assertion());
                gens.add(ge);
                e = FixtureBundles.standInRun(c.root, bundle, null, gr.store(), new FixtureBundles.Gen(ge, 2, next.registeredRowSha256(), next.scope(), nextRow), gr.scenario(),
                    gr.variant(), CoreGate.DEFAULT_SEED, false, gr.label(), common);
            }
            entries.add(e);
            generationRuns.set(gr.name(), FixtureBundles.summary(gr.name(), FixtureBundles.lockOf(c.root, gr.scenario(), gr.variant())));
        }
        FixtureBundles.sortRuns(entries);
        FixtureBundles.writeBound(bundle, report, summaries);
        return bundle.resolve("report.json");
    }

    record Out(int exit, String stdout) {
        JsonNode json() { return Json.read(stdout.strip().getBytes(StandardCharsets.UTF_8)); }
    }

    static Out main(String... args) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = Main.run(args, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8));
        return new Out(exit, (so.toString(StandardCharsets.UTF_8).strip() + " " + se.toString(StandardCharsets.UTF_8).strip()).strip());
    }

    static Out run(Context c, Path state, Path recording, long generation, Path out) {
        return main("run", "--engine", "core", "--recording", recording.toString(), "--registry", c.fixtures().resolve("registry").toString(),
            "--state", state.toString(), "--generation", Long.toString(generation), "--out", out.toString());
    }

    static List<String> lines(Path f) throws IOException { return Files.exists(f) ? Files.readAllLines(f, StandardCharsets.UTF_8) : List.of(); }

    /** {@code F-GEN-NEW}: {@code new} with {@code --recording smoke} writes exactly one {@code registered} row with its pins and the head
     *  floor, and no recovery declaration; the printed {@code registeredRowSha256} is the digest of the stored line. The first
     *  {@code forseti run} writes exactly one declaration with the recording's routing; a second run adds neither a declaration nor a row. */
    static Result created(Context c) throws Exception {
        Problems p = new Problems();
        Path rec = c.path(SMOKE), registry = c.fixtures().resolve("registry");
        GateEvidence.Store store = c.assertionStore();
        Path state = store.dir();
        GateEvidence.Registered g = GateEvidence.register(c, store, registry, rec, "smoke", GateEvidence.SELF_TEST, c.current);
        Path file = state.resolve(FileGenerationStore.FILE), declarations = state.resolve(ReferenceEngine.RECOVERY_CONTROL);
        List<String> rows = lines(file);
        p.equal(1, rows.size(), "generation rows after new");
        if (rows.size() != 1) return p.result("");
        byte[] stored = Files.readAllBytes(file);
        JsonNode record = Json.read(rows.get(0).getBytes(StandardCharsets.UTF_8)), v = record.path("value");
        p.equal(1L, g.generation(), "printed generation");
        p.equal("registered", v.path("status").asText(null), "row status");
        p.equal("line2", record.path("key").asText(null), "record key (scope)");
        p.equal("1", record.path("headers").path(FileGenerationStore.HEADER).asText(null), "record header");
        p.require(v.path("run").isNull(), "run is not null on the registered row: " + v.path("run"));
        p.require(v.path("gateReportSha256").isNull(), "gateReportSha256 is not null on the registered row");
        p.equal(g.rulesDigest(), v.path("rulesDigest").asText(null), "printed rulesDigest");
        p.equal(g.rulesDigest(), Canonical.digest(v.path("releaseEvidence").path("pins")), "rulesDigest = digest of the pinned pins");
        JsonNode head = Json.read(registry.resolve("mappings/heads/line2.json"));
        p.equal(head.path("sha256").asText(), v.path("releaseEvidence").path("releaseSha256").asText(null), "pinned releaseSha256");
        p.equal(1, v.path("inputReleases").size(), "inputReleases");
        p.equal(Json.read(rec.resolve(Recording.MANIFEST)).path("definitions"), v.path("releaseEvidence").path("pins").path("definitions"), "pinned definitions");
        RoutingManifest routing = Recording.open(rec).routing();
        Set<String> want = new TreeSet<>(), pinned = new TreeSet<>();
        for (String t : RoutingManifest.TOPICS) for (int part : routing.subscribed(t)) want.add(t + "/" + part + "@0");
        v.path("replayFrom").fields().forEachRemaining(t -> t.getValue().fields().forEachRemaining(e -> pinned.add(t.getKey() + "/" + e.getKey() + "@" + e.getValue().asLong())));
        p.equal(want, pinned, "replayFrom: offset 0 of every subscribed partition");
        p.equal(Canonical.digestOf(Arrays.copyOf(stored, stored.length - 1)), g.registeredRowSha256(), "printed registeredRowSha256 = digest of the stored line");
        p.require(Files.isRegularFile(state.resolve("head-floors/line2.json")), "no head-floors/line2.json");
        p.equal(1L, new FloorStore(state).get("line2").map(FloorStore.Floor::headVersion).orElse(null), "floor");
        p.require(!Files.exists(declarations), "generation new wrote a recovery declaration");

        String routingDigest = routing.routingDigest();
        for (int i = 1; i <= 2; i++) {
            Path out = c.newDir("gen-new-run").resolve("out");
            Out o = run(c, state, rec, g.generation(), out);
            p.equal(0, o.exit(), "run " + i + " exit (" + o.stdout() + ")");
            if (o.exit() != 0) break;
            GateEvidence.keep(c, g, out, rec, "smoke", GateEvidence.SELF_TEST, ReferenceEngine.DEFAULT_SEED, false, GateEvidence.safe(store.name()) + ".smoke.run-" + i);
            List<String> d = lines(declarations);
            p.equal(1, d.size(), "recovery declarations after run " + i);
            if (d.size() == 1) {
                JsonNode decl = Json.read(d.get(0).getBytes(StandardCharsets.UTF_8));
                p.equal(1L, decl.path("generation").asLong(), "declared generation");
                p.equal(routingDigest, decl.path("routingDigest").asText(null), "declared routingDigest");
                p.equal(0L, decl.path("targetEpoch").asLong(-1), "declared targetEpoch");
            }
            p.require(Arrays.equals(stored, Files.readAllBytes(file)), "run " + i + " changed generations.jsonl");
        }
        return p.result("generation new (smoke, fixture clock): one registered row, run null, rulesDigest = digest of the pinned pins, replayFrom offset 0 on "
            + want.size() + " partitions, floor 1, no declaration, registeredRowSha256 = digest of the stored line; two forseti run: one declaration (routing "
            + routingDigest.substring(0, 12) + "...), still one generation row");
    }

    /** {@code F-GEN-REFUSES-UNREGISTERED}: in a store holding generation 1, the harness refuses generation 7 and writes nothing. */
    static Result unregistered(Context c) throws Exception {
        Problems p = new Problems();
        Path rec = c.path(SMOKE);
        GateEvidence.Store store = c.assertionStore();
        GateEvidence.register(c, store, c.fixtures().resolve("registry"), rec, "smoke", GateEvidence.SELF_TEST, c.current);
        Path file = store.dir().resolve(FileGenerationStore.FILE);
        byte[] before = Files.readAllBytes(file);
        Path out = c.newDir("gen-unregistered").resolve("out");
        Out o = run(c, store.dir(), rec, 7, out);
        p.equal(1, o.exit(), "forseti run --generation 7 exit (" + o.stdout() + ")");
        if (o.exit() == 1) p.equal("generation-unregistered", o.json().path("refusal").asText(null), "refusal");
        p.require(!Files.exists(store.dir().resolve(ReferenceEngine.RECOVERY_CONTROL)), "a recovery declaration was written");
        p.require(Arrays.equals(before, Files.readAllBytes(file)), "generations.jsonl changed");
        p.require(!Files.exists(out.resolve("run.json")), "a run record was written");
        return p.result("forseti run --generation 7 in a store holding generation 1: refused generation-unregistered (exit 1), no declaration, no run record, the store unchanged");
    }

    /** {@code F-GEN-FLOOR}: in its own store, {@code new} on {@code registry} (floor 1), on {@code registry-next} (floor 2), on
     *  {@code registry} again: {@code head-rollback}, no third row, the floor stays 2. */
    static Result floor(Context c) throws Exception {
        Problems p = new Problems();
        Path registry = c.fixtures().resolve("registry"), next = c.fixtures().resolve("registry-next");
        GateEvidence.Store store = c.assertionStore();
        Path state = store.dir();
        GateEvidence.Registered a = GateEvidence.register(c, store, registry, "line2", RULE, null, "registry", GateEvidence.SELF_TEST, c.current);
        p.equal(1L, a.generation(), "first generation");
        p.equal(1L, new FloorStore(state).get("line2").map(FloorStore.Floor::headVersion).orElse(null), "floor after registry");
        GateEvidence.Registered b = GateEvidence.register(c, store, next, "line2", RULE_NEXT, null, "registry-next", GateEvidence.SELF_TEST, c.current);
        p.equal(2L, b.generation(), "second generation");
        p.equal(2L, new FloorStore(state).get("line2").map(FloorStore.Floor::headVersion).orElse(null), "floor after registry-next");
        Path m = registry.resolve("mappings");
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = GenerationCli.run(new String[] {"new", "--state", state.toString(), "--mappings", m.toString(), "--trusted-keys", c.fixtures().resolve("trusted-keys.jsonl").toString(),
                "--stream", "line2", "--rule", RULE}, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8),
            Clock.fixed(Instant.ofEpochMilli(GateSigner.now(m, "line2")), ZoneOffset.UTC));
        String stdout = so.toString(StandardCharsets.UTF_8).strip();
        p.equal(1, exit, "new on registry after registry-next exit (" + stdout + " " + se.toString(StandardCharsets.UTF_8).strip() + ")");
        if (exit == 1) p.equal("head-rollback", Json.read(stdout.getBytes(StandardCharsets.UTF_8)).path("refusal").asText(null), "refusal");
        p.equal(2, lines(state.resolve(FileGenerationStore.FILE)).size(), "generation rows");
        p.equal(2L, new FloorStore(state).get("line2").map(FloorStore.Floor::headVersion).orElse(null), "floor after the refused rollback");
        return p.result("own store: new on registry -> generation 1, floor 1; on registry-next -> generation 2, floor 2; on registry again -> head-rollback, "
            + "2 rows, floor 2");
    }
}
