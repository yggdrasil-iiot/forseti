package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import yggdrasil.forseti.cli.Args;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.release.FloorStore;
import yggdrasil.forseti.core.release.HeadVerifier;
import yggdrasil.forseti.core.release.ReleaseReader;
import yggdrasil.forseti.core.release.TrustedKeys;
import yggdrasil.forseti.core.rules.RuleSet;

/** {@code forseti recordings compare <recording> [--registry <dir>] [--trusted-keys <file>] [--python <interpreter>] [--out <dir>]}: the
 *  core-against-oracle comparison the core gate makes for each of its scenarios, on one recording (a committed live capture, step-5 plan
 *  PR 8). The recording's generation is registered in a fresh state on the fixture clock (the head's {@code signedAt} + 60 s, as the
 *  gate registers), the reference harness runs on it in this process, and the oracle runs with {@code --stream} beside the gate's journal
 *  fold: at every cut of the cut log the folded journals are compared with the oracle's six tables of that cut ({@link Comparison}).
 *  Then the gate's integrity checks ({@link Integrity}: every delivered input has a disposition, the W1 formula, W2 = W1) and the
 *  recording's ground truth ({@link Expectations}). Nothing of the gate's assertions or manifest is involved. On a live capture, last,
 *  the acceptance of its visits against the injections it declares ({@link LiveAcceptance}): every visit complete except those an
 *  injection touches, which are insufficient with its reason.
 *
 *  <p>{@code --registry} defaults to {@code fixtures/live/registry}, {@code --trusted-keys} to {@code trusted-keys.jsonl} beside the
 *  registry. {@code --out} keeps the work directory ({@code state/}, {@code core/}, {@code diffs/}, {@code logs/}) and writes
 *  {@code compare-report.json} there; without it a temporary directory is used and removed. The report (also printed) carries no path
 *  and no time, so the same inputs give the same bytes; its {@code outcome} is {@code equal} when the bodies agree at every cut, every
 *  integrity check is clean and the ground truth holds, and its {@code accepted} is the visit acceptance (true when it does not apply).
 *  Exit 0 when both hold; 1 when either does not (or the harness refused); 2 on a usage or input error. */
public final class RecordingCompare {
    private RecordingCompare() {}

    public static final String REPORT = "compare-report.json";

    public static int main(String[] args, int from, PrintStream out, PrintStream err) {
        if (args.length <= from || args[from].startsWith("--")) { err.println("recordings compare needs a recording directory"); return 2; }
        Path recording = Path.of(args[from]);
        Args f;
        try { f = Args.parse(args, from + 1, Set.of()).only(Set.of("registry", "trusted-keys", "python", "out")); }
        catch (IllegalArgumentException e) { err.println(e.getMessage()); return 2; }
        Path registry = Path.of(f.get("registry", "fixtures/live/registry"));
        Path keys = f.get("trusted-keys").map(Path::of).orElse(registry.toAbsolutePath().getParent().resolve("trusted-keys.jsonl"));
        String python = f.get("python", "python");
        Path work = f.get("out").map(Path::of).orElse(null);
        boolean temporary = work == null;
        try {
            if (temporary) work = Files.createTempDirectory("forseti-compare-");
            else if (Files.exists(work) && !isEmptyDir(work)) { err.println("recordings compare: " + work + " exists and is not empty"); return 2; }
            Files.createDirectories(work);
            ObjectNode report = compare(recording, registry, keys, python, work, err);
            byte[] bytes = Json.pretty(report);
            if (!temporary) Files.write(work.resolve(REPORT), bytes);
            out.print(new String(bytes, StandardCharsets.UTF_8));
            return "equal".equals(report.path("outcome").asText()) && report.path("accepted").asBoolean(false) ? 0 : 1;
        } catch (ReferenceEngine.Refused e) {
            out.println(Canonical.utf8(Canonical.bytes(e.toJson())));
            return 1;
        } catch (IOException e) { err.println(e.getMessage()); return 2; }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); err.println("interrupted"); return 2; }
        catch (IllegalArgumentException | IllegalStateException | UncheckedIOException e) { err.println(e.getMessage()); return 2; }
        finally { if (temporary && work != null) CoreGate.delete(work); }
    }

    /** The comparison of one recording in {@code work}; the report. */
    static ObjectNode compare(Path recording, Path registry, Path keys, String python, Path work, PrintStream err) throws IOException, InterruptedException {
        long t0 = System.nanoTime();
        Recording rec = Recording.open(recording);
        JsonNode manifest = rec.manifest(), rule = manifest.get("rule");
        String name = manifest.path("name").asText(recording.getFileName().toString());
        if (rule == null) throw new IllegalArgumentException(recording + ": the manifest has no rule");
        String stream = Json.text(rule, "stream"), ruleRef = Json.text(rule, "attributionRuleId") + "@" + Json.text(rule, "attributionRuleVersion");
        Path mappings = registry.resolve("mappings"), state = work.resolve("state"), core = work.resolve("core"), logs = work.resolve("logs");
        Files.createDirectories(logs);
        long now = GateSigner.now(mappings, stream);

        // (1) the generation, registered as the gate registers it: in-process, on the fixture clock
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = GenerationCli.run(new String[] {"new", "--state", state.toString(), "--mappings", mappings.toString(), "--trusted-keys", keys.toString(),
            "--stream", stream, "--rule", ruleRef, "--recording", recording.toString()}, new PrintStream(so, true, StandardCharsets.UTF_8),
            new PrintStream(se, true, StandardCharsets.UTF_8), Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC));
        String printed = so.toString(StandardCharsets.UTF_8).strip();
        if (exit != 0) throw new IllegalStateException("generation new exit " + exit + ": " + printed + " " + se.toString(StandardCharsets.UTF_8).strip());
        JsonNode g = Json.read(printed.getBytes(StandardCharsets.UTF_8));
        long generation = g.get("generation").longValue();
        String rulesDigest = Json.text(g, "rulesDigest");

        // (2) the reference harness
        GenerationCli.prepareRun(state, generation, recording);
        ReferenceEngine.run(new ReferenceEngine.Options(recording, registry, state, generation, core, ReferenceEngine.DEFAULT_SEED, false, Map.of(), null,
            "forseti recordings compare"));
        JsonNode cutLog = Json.read(core.resolve("cuts.json")), runJson = Json.read(core.resolve("run.json"));
        long harnessMs = ms(t0);

        // (3) the oracle beside the fold, cut by cut
        RuleSet rules = RuleSet.load(ReleaseReader.read(mappings, HeadVerifier.verify(mappings, stream, TrustedKeys.load(keys),
            new FloorStore(work.resolve("floors")), now)), Json.text(rule, "attributionRuleId"), Json.text(rule, "attributionRuleVersion"), manifest.get("definitions"));
        List<String> cmd = oracle(python, recording, core, registry, rule, runJson, work.resolve("oracle"));
        cmd.add("--stream");
        List<Long> cutIds = new ArrayList<>();
        Map<Long, JsonNode> entries = new HashMap<>();
        for (JsonNode c : cutLog.path("cuts")) { cutIds.add(c.path("cutId").asLong()); entries.put(c.path("cutId").asLong(), c); }
        Comparison.Tally tally = new Comparison.Tally();
        Comparison.OracleFold of = new Comparison.OracleFold();
        Files.createDirectories(work.resolve("oracle"));
        Path log = logs.resolve("oracle.log");
        Map<String, List<Integer>> subscribed = new TreeMap<>();
        runJson.path("subscribed").fields().forEachRemaining(e -> { List<Integer> l = new ArrayList<>(); e.getValue().forEach(x -> l.add(x.asInt())); subscribed.put(e.getKey(), l); });
        Integrity.Measures measures;
        Expectations.Outcome outcome;
        RunFold.Findings findings;
        ObjectNode acceptance;
        long t1 = System.nanoTime();
        Process p = new ProcessBuilder(cmd).redirectError(log.toFile()).start();
        try (RunFold fold = new RunFold(core, rulesDigest, generation, true)) {
            String broken;
            List<String> trailing = new ArrayList<>();
            try (BufferedReader announced = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
                 Writer acks = new OutputStreamWriter(p.getOutputStream(), StandardCharsets.UTF_8)) {
                broken = Comparison.consume(announced, acks, work.resolve("oracle"), cutIds, trailing, (k, dir) -> {
                    fold.advance(k);
                    Comparison.compareCut(fold, k, dir, of, name, entries.get(k), tally);
                });
            }
            if (!p.waitFor(1, TimeUnit.HOURS)) p.destroyForcibly();
            int oracleExit = p.waitFor();
            if (broken != null || oracleExit != 0) {
                String[] lines = Files.readString(log, StandardCharsets.UTF_8).strip().split("\\R");
                throw new IllegalStateException("oracle exit " + oracleExit + (broken != null ? ", " + broken : "") + ": " + lines[lines.length - 1]);
            }
            fold.finish();
            measures = Integrity.check(fold, recording, cutLog, subscribed, rules, manifest.path("definitions"), rec.routing().stations());
            outcome = Expectations.check(fold, rec.expected(), k -> perStation(cutLog, k));
            findings = fold.findings;
            acceptance = LiveAcceptance.check(rec, fold.live(RunFold.WR));
        } finally {
            if (p.isAlive()) p.destroyForcibly();
        }
        long oracleMs = ms(t1);
        if (!tally.pending.isEmpty()) diagnose(python, recording, core, registry, rule, runJson, tally, work, generation, name);
        err.println("recordings compare " + name + ": harness " + harnessMs + " ms, oracle and fold " + oracleMs + " ms");
        return report(rec, recording, generation, rulesDigest, cutIds.size(), tally, findings, measures, outcome, acceptance);
    }

    /** {@code python -I -m forseti_oracle.cli all} on the harness's cut log, the recording's rule and the run's subscription. */
    private static List<String> oracle(String python, Path recording, Path core, Path registry, JsonNode rule, JsonNode runJson, Path out) {
        List<String> cmd = new ArrayList<>(List.of(python, "-I", "-m", "forseti_oracle.cli", "all", "--recording", recording.toString(),
            "--cuts", core.resolve("cuts.json").toString(), "--mappings", registry.resolve("mappings").toString(),
            "--release", Json.text(rule, "releaseId") + "@" + Json.text(rule, "version"),
            "--rule", Json.text(rule, "attributionRuleId") + "@" + Json.text(rule, "attributionRuleVersion"),
            "--definitions", recording.resolve(Recording.MANIFEST).toString(), "--out", out.toString()));
        runJson.path("subscribed").fields().forEachRemaining(e -> {
            List<String> ps = new ArrayList<>();
            e.getValue().forEach(x -> ps.add(x.asText()));
            cmd.add("--subscribe");
            cmd.add(e.getKey() + "=" + String.join(",", ps));
        });
        return cmd;
    }

    /** The diff files of the first differing cuts ({@code <work>/diffs}), from an oracle rerun with {@code --snapshots --cut-ids}. */
    private static void diagnose(String python, Path recording, Path core, Path registry, JsonNode rule, JsonNode runJson, Comparison.Tally tally, Path work,
                                 long generation, String name) {
        Path snapshots = work.resolve("oracle-diagnostic");
        String failure = null;
        try {
            List<String> cmd = oracle(python, recording, core, registry, rule, runJson, snapshots);
            cmd.addAll(List.of("--snapshots", "--cut-ids", String.join(",", tally.pendingCuts().stream().map(String::valueOf).toList())));
            // the working directory of this process: the command's paths are as given (relative ones to it), as for the stream run
            CoreGate.Proc p = CoreGate.Proc.run(Path.of("").toAbsolutePath(), work.resolve("logs").resolve("oracle-diagnostic.log"), Duration.ofHours(1), cmd);
            if (p.exit() != 0) failure = "exit " + p.exit() + ": " + p.lastLine();
        } catch (Exception e) { failure = e.toString(); }
        Comparison.writeDiffs(tally, work.resolve("diffs"), name, generation, failure == null ? snapshots : null, failure);
        CoreGate.delete(snapshots);
    }

    private static JsonNode perStation(JsonNode cutLog, long cutId) {
        for (JsonNode c : cutLog.path("cuts")) if (c.path("cutId").asLong() == cutId) return c.path("perStation");
        return Json.object();
    }

    private static ObjectNode report(Recording rec, Path recording, long generation, String rulesDigest, int cuts, Comparison.Tally t, RunFold.Findings findings,
                                     Integrity.Measures m, Expectations.Outcome o, ObjectNode acceptance) throws IOException {
        boolean equal = t.equal() && t.cuts == cuts && findings.clean() && m.inputsWithoutDisposition() == 0 && o.ok();
        ObjectNode r = Json.object().put("kind", "forseti-recording-compare").put("compareReportVersion", 1)
            .put("recording", rec.manifest().path("name").asText(recording.getFileName().toString()))
            .put("stream", Json.text(rec.manifest().get("rule"), "stream"))
            .put("manifestSha256", Canonical.digestOf(Files.readAllBytes(recording.resolve(Recording.MANIFEST))))
            .put("outcome", equal ? "equal" : "different")
            .put("accepted", acceptance.path("ok").asBoolean(false));
        Json.putSafe(r, "generation", generation);
        r.put("rulesDigest", rulesDigest);
        Json.putSafe(r, "cuts", cuts);
        Json.putSafe(r, "cutsCompared", t.cuts);
        Json.putSafe(r, "rowsCompared", t.rowsCompared);
        Json.putSafe(r, "dispositionsCompared", t.dispositionsCompared);
        Json.putSafe(r, "differences", t.differences);
        Json.putSafe(r, "oracleDigestMismatches", t.oracleDigestMismatches);
        ObjectNode byTopic = r.putObject("differencesByTopic");
        t.differencesByTopic.forEach((k, v) -> Json.putSafe(byTopic, k, v));
        ObjectNode keys = r.putObject("keysAtLastCut");
        t.keysAtLastCut.forEach((k, v) -> Json.putSafe(keys, k, (long) v));
        ArrayNode ex = r.putArray("differenceExamples");
        t.examples.forEach(ex::add);
        ObjectNode d = r.putObject("dispositionCoverage");
        Json.putSafe(d, "deliveries", m.deliveries());
        Json.putSafe(d, "inputsWithoutDisposition", m.inputsWithoutDisposition());
        ObjectNode lat = r.putObject("lateness");
        Json.putSafe(lat, "longestAcceptedMs", m.longestLatenessMs());
        Json.putSafe(lat, "acceptedLateSamples", m.acceptedLateSamples());
        ObjectNode fi = r.putObject("findings");
        for (String c : findings.checks()) {
            ObjectNode x = fi.putObject(c);
            Json.putSafe(x, "count", findings.count(c));
            x.put("examples", findings.describe(c));
        }
        ObjectNode gt = r.putObject("groundTruth");
        gt.put("scope", rec.expected().path("scope").asText());
        gt.put("ok", o.ok());
        Json.putSafe(gt, "visits", o.visits);
        Json.putSafe(gt, "samples", o.samples);
        Json.putSafe(gt, "identityParts", o.dispositions);
        Json.putSafe(gt, "incidentSets", o.incidents);
        Json.putSafe(gt, "transitions", o.transitions);
        Json.putSafe(gt, "problems", o.problems);
        ArrayNode ge = gt.putArray("problemExamples");
        o.examples.forEach(ge::add);
        r.set("acceptance", acceptance);
        return r;
    }

    private static boolean isEmptyDir(Path p) throws IOException {
        if (!Files.isDirectory(p)) return false;
        try (var s = Files.list(p)) { return s.findAny().isEmpty(); }
    }

    private static long ms(long t) { return (System.nanoTime() - t) / 1_000_000; }
}
