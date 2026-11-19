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
import yggdrasil.forseti.cli.gate.RequirementsAccount.Status;
import yggdrasil.forseti.cli.gate.Scenarios.Run;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.cli.replay.ReplayEngine;
import yggdrasil.forseti.cli.replay.StarvationObserver;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.cut.CutAssembler;

/**
 * {@code forseti requirements --engine <name> --gate-report <report> --out <dir>} (spec 705, item 5; plan D14), engine-neutral: the
 * station-isolation requirement H-isolation of an external engine. It runs injection 24 ({@code starved-station3}) on the dedicated and
 * the shared raw placement with its observation deadline ({@link StarvationObserver}), its control ({@code starved-station3-control}, the
 * same recording without the starvation), the baseline and the baseline at batch interval x2 and x1/2 on both placements, each compared
 * with the oracle at every cut exactly as the replay gate compares its runs
 * ({@link ReplayGate#runScenarios}); it records 5 (a), 5 (b) and the restart part of 5 (d) from the semantic gate's results for
 * injections 26, 7, 21 and 22 in {@code --gate-report}. Every requirement of {@code gate/<engine>-requirements.json} is recorded as
 * {@code pass}, {@code miss} or {@code error} with its evidence ({@link RequirementsAccount}); it prints one line per requirement,
 * {@code requirements recorded N / expected M} and the engine eligibility line, and writes {@code requirements.json} under {@code --out}.
 * Exit 0 when every requirement was run and recorded (a miss or an evaluated error is a recorded result; spec 705); 1 when one is not
 * recorded, when the runs crashed or a run was not executed (no Docker, a broker failure), or when the report cannot be written; 2 for a
 * usage error. A {@code --gate-report} that is missing or unreadable (an aggregate that never ran) is not a usage error: the runs still
 * execute and every requirement taken from the gate is recorded as an error with the reason.
 *
 * <p>Bodies are compared between two runs at their common cuts: the cuts whose replay input boundary is the same, that is the batches
 * after which the two recordings have delivered the same input records (the same multiset of (topic, partition, value) on every input
 * topic); at each common cut both runs completed every station, the cut is comparable under spec 624 (no ground-truth deadline between
 * the two runs' {@code W1} or {@code admissionW}, equal {@code W1} where a visit is open), and every output body partition
 * ({@code forseti.work-records}, {@code forseti.samples}, {@code forseti.affected}, {@code forseti.affected-sets}) is compared by its
 * body fingerprint, which holds no run id, path, checkpoint id or other diagnostic field. Every compared cut must show bodies on both
 * sides (live records, a work-records fingerprint).
 */
public final class ReplayRequirements {
    private ReplayRequirements() {}

    static final List<String> BODY_TOPICS = List.of(RunFold.WR, RunFold.SAMPLES, RunFold.AFFECTED, RunFold.SETS);
    static final String STARVED = "starved-station3", CONTROL = "starved-station3-control", BASELINE = "baseline", X2 = "baseline-batch-x2", HALF = "baseline-batch-half";
    static final String FILE = "requirements.json";

    /** The engine's requirements manifest: the requirement ids in order and the runs with their settings. */
    record Manifest(List<String> ids, Map<String, String> descriptions, List<String> runs, Map<String, JsonNode> settings, String sha256) {
        static Manifest load(Path file, String engine) {
            byte[] bytes;
            try { bytes = Files.readAllBytes(file); } catch (IOException e) { throw new IllegalArgumentException("no requirements manifest " + file); }
            JsonNode n = Json.read(bytes);
            if (!engine.equals(n.path("engine").asText(null))) throw new IllegalArgumentException(file + " is not a " + engine + " requirements manifest");
            List<String> ids = new ArrayList<>();
            Map<String, String> d = new LinkedHashMap<>();
            for (JsonNode r : n.path("requirements")) { String id = Json.text(r, "id"); if (d.put(id, Json.text(r, "description")) != null) throw new IllegalArgumentException(file + ": " + id + " twice"); ids.add(id); }
            List<String> runs = new ArrayList<>();
            Map<String, JsonNode> s = new LinkedHashMap<>();
            for (JsonNode r : n.path("runs")) { String name = Json.text(r, "name"); runs.add(name); s.put(name, r.path("settings").isObject() ? r.get("settings") : Json.object()); }
            if (!new TreeSet<>(ids).equals(new TreeSet<>(IDS))) throw new IllegalArgumentException(file + ": requirements " + ids + ", this command evaluates " + IDS);
            if (!new TreeSet<>(runs).equals(new TreeSet<>(RUNS))) throw new IllegalArgumentException(file + ": runs " + runs + ", this command needs " + RUNS);
            return new Manifest(List.copyOf(ids), d, List.copyOf(runs), s, Canonical.digestOf(bytes));
        }
    }

    /** The requirement ids this command evaluates and the runs it needs (the manifest must list exactly these). */
    static final List<String> IDS = List.of("R-5A", "R-5B", "R-24-DEDICATED", "R-24-SHARED", "R-5C-DEDICATED", "R-5C-SHARED", "R-5D-RESUME-DEDICATED",
        "R-5D-RESUME-SHARED", "R-5D-RESTART", "R-5D-SPEED-X2-DEDICATED", "R-5D-SPEED-HALF-DEDICATED", "R-5D-SPEED-X2-SHARED", "R-5D-SPEED-HALF-SHARED");
    static final List<String> RUNS = List.of("dedicated/" + BASELINE, "dedicated/" + STARVED, "dedicated/" + CONTROL, "dedicated/" + X2, "dedicated/" + HALF,
        BASELINE, STARVED, CONTROL, X2, HALF);

    /** The semantic gate as {@code --gate-report} records it. Usable (its results may decide a requirement) only when it is a {@code gate}
     *  report of this engine, scope {@code replay}, this checkout's assertion manifest and the engine artifact on the engine path (an
     *  unknown artifact is unusable), with at most one boolean result per id. Passed only when it is usable, a full or aggregate layout
     *  that exited 0 without an error, and its results hold exactly one successful result for every id of the current manifest and no
     *  other id; the report's own counters are not trusted. Its results by id. */
    record GateReport(boolean passed, String why, Map<String, Boolean> ok, Map<String, String> evidence, String sha256, boolean usable, String unusable) {
        /** A report that could not be read: nothing it would decide is decided. */
        static GateReport unreadable(String what) { return new GateReport(false, what, Map.of(), Map.of(), null, false, what); }

        static GateReport read(byte[] bytes, String engine, String manifestSha256, List<String> manifestIds, String engineArtifact) {
            JsonNode r;
            try { r = Json.read(bytes); } catch (RuntimeException e) { return new GateReport(false, "the gate report is not JSON", Map.of(), Map.of(), Canonical.digestOf(bytes), false, "not JSON: " + e.getMessage()); }
            List<String> wrong = new ArrayList<>(), unusable = new ArrayList<>();
            if (!engine.equals(r.path("engine").asText(null))) unusable.add("engine " + r.path("engine") + ", not " + engine);
            if (!"replay".equals(r.path("scope").asText(null))) unusable.add("scope " + r.path("scope"));
            if (!"gate".equals(r.path("kind").asText(null))) unusable.add("kind " + r.path("kind") + " (a negative-control report is not the semantic gate)");
            if (!Objects.equals(manifestSha256, r.path("manifestSha256").asText(null))) unusable.add("assertion manifest " + r.path("manifestSha256").asText(null) + ", this checkout's " + manifestSha256);
            if (engineArtifact == null) unusable.add("the engine path's artifact is unknown");
            else if (!engineArtifact.equals(r.path("engineArtifactSha256").asText(null)))
                unusable.add("engine artifact " + r.path("engineArtifactSha256").asText(null) + ", the engine path's " + engineArtifact);
            Map<String, Boolean> ok = new LinkedHashMap<>();
            Map<String, String> ev = new LinkedHashMap<>();
            Set<String> twice = new TreeSet<>();
            for (JsonNode x : r.path("results")) {
                String id = x.path("id").asText(null);
                if (id == null || !x.path("ok").isBoolean()) { unusable.add("a result without an id or a boolean ok: " + abbrev(x.toString(), 120)); continue; }
                if (ok.put(id, x.path("ok").booleanValue()) != null) twice.add(id);
                ev.put(id, x.path("evidence").asText(""));
            }
            if (!twice.isEmpty()) unusable.add("results recorded twice " + twice);
            String layout = r.path("layout").asText("");
            if (!layout.equals("full") && !layout.equals("aggregate")) wrong.add("layout " + layout + " (one shard is not the semantic gate)");
            if (r.path("exit").asLong(-1) != 0) wrong.add("exit " + r.path("exit"));
            if (!r.path("error").isNull() && !r.path("error").isMissingNode()) wrong.add("error " + r.path("error").asText());
            List<String> missing = new ArrayList<>(), failed = new ArrayList<>(), extra = new ArrayList<>();
            for (String id : manifestIds) { Boolean b = ok.get(id); if (b == null) missing.add(id); else if (!b) failed.add(id); }
            for (String id : ok.keySet()) if (!manifestIds.contains(id)) extra.add(id);
            if (!missing.isEmpty()) wrong.add(missing.size() + " manifest ids without a result " + abbrev(missing.toString(), 300));
            if (!failed.isEmpty()) wrong.add("failed " + failed);
            if (!extra.isEmpty()) wrong.add("results of ids not in the manifest " + extra);
            wrong.addAll(0, unusable);
            return new GateReport(wrong.isEmpty(), wrong.isEmpty() ? null : String.join("; ", wrong), ok, ev, Canonical.digestOf(bytes), unusable.isEmpty(),
                unusable.isEmpty() ? null : String.join("; ", unusable));
        }

        /** A requirement recorded from the gate's results for {@code ids}: pass when each ran and held, miss when one ran and failed,
         *  error when one is absent from the report or the report is not usable. */
        Entry from(List<String> ids) {
            if (!usable) return new Entry(Status.ERROR, "error: the gate report cannot decide it: " + unusable);
            List<String> absent = new ArrayList<>(), failed = new ArrayList<>(), held = new ArrayList<>();
            for (String id : ids) {
                Boolean b = ok.get(id);
                if (b == null) absent.add(id); else if (b) held.add(id); else failed.add(id);
            }
            StringBuilder e = new StringBuilder("gate report " + sha256.substring(0, 12) + ": ");
            for (String id : ids) if (ok.containsKey(id)) e.append(ok.get(id) ? "PASS " : "FAIL ").append(id).append(' ').append(abbrev(evidence.get(id), 400)).append(" || ");
            if (!absent.isEmpty()) return new Entry(Status.ERROR, "error: " + e + "not in the gate report: " + absent);
            if (!failed.isEmpty()) return new Entry(Status.MISS, e + "failed in the gate: " + failed);
            return new Entry(Status.PASS, e + "held in the gate: " + held);
        }
    }

    record Entry(Status status, String evidence) {}

    public static int main(String engine, Map<String, String> flags, String command, Function<Map<String, String>, ReplayEngine> open, PrintStream out, PrintStream err) {
        return main(Path.of("").toAbsolutePath(), engine, flags, command, open, out, err);
    }

    /** {@link #main} with the repository root {@code root}. */
    static int main(Path root, String engine, Map<String, String> flags, String command, Function<Map<String, String>, ReplayEngine> open, PrintStream out, PrintStream err) {
        Manifest manifest;
        String assertionSha, report = flags.get("gate-report");
        List<String> assertionIds;
        try {
            if (report == null) throw new IllegalArgumentException("--gate-report <report.json of forseti gate --engine " + engine + "> is required");
            manifest = Manifest.load(root.resolve("gate/" + engine + "-requirements.json"), engine);
            byte[] am = Files.readAllBytes(root.resolve("gate/" + engine + "-assertions.json"));
            assertionSha = Canonical.digestOf(am);
            assertionIds = ReplayGate.manifestIds(Json.read(am), engine);
        } catch (IllegalArgumentException | IOException e) {
            err.println(e.getMessage());
            err.println("usage: forseti requirements --engine " + engine + " --gate-report <report.json> [--out <dir>] [--python <interpreter>]");
            return 2;
        }
        Path outDir = Path.of(flags.getOrDefault("out", "build/requirements")).toAbsolutePath();
        String startedAt = Instant.now().toString(), error = null;
        // a missing or unreadable gate report (an aggregate that never ran) decides nothing; the runs still execute
        GateReport gate;
        try { gate = GateReport.read(Files.readAllBytes(Path.of(report)), engine, assertionSha, assertionIds, flags.get("engine-artifact")); }
        catch (IOException | RuntimeException e) { gate = GateReport.unreadable("no readable gate report at " + report + " (" + e + ")"); err.println("requirements: " + gate.why()); }
        RequirementsAccount acc = new RequirementsAccount(manifest.ids(), out);
        ReplayGate.Made made = null;
        ObjectNode observations = Json.object();
        List<String> notExecuted = new ArrayList<>();
        try {
            record(acc, "R-5A", gate.from(List.of("I-26")));
            record(acc, "R-5B", gate.from(List.of("I-7")));
            record(acc, "R-5D-RESTART", gate.from(List.of("I-21", "I-22")));
            made = ReplayGate.runScenarios(engine, flags, command, manifest.runs(), manifest.settings(), open, outDir, out, err);
            for (String n : manifest.runs()) if (!made.runs().engineRuns.containsKey(n)) notExecuted.add(n + ": " + (made.runs().runs.get(n) == null ? "not made" : made.runs().runs.get(n).failure));
            for (String placement : List.of("dedicated", "shared")) {
                String pre = placement.equals("dedicated") ? "dedicated/" : "", suf = "-" + placement.toUpperCase(Locale.ROOT);
                Run starved = made.runs().runs.get(pre + STARVED), control = made.runs().runs.get(pre + CONTROL), base = made.runs().runs.get(pre + BASELINE);
                ReplayEngine.Run engineRun = made.runs().engineRuns.get(pre + STARVED);
                JsonNode iso = isolation(engineRun);
                if (iso != null) observations.set(placement, iso);
                record(acc, "R-24" + suf, injection24(starved, engineRun == null ? null : engineRun.out(), control));
                record(acc, "R-5C" + suf, settles(starved, iso));
                record(acc, "R-5D-RESUME" + suf, resume(starved, control));
                record(acc, "R-5D-SPEED-X2" + suf, sameBodies(made.runs().runs.get(pre + X2), base, "batch interval x2"));
                record(acc, "R-5D-SPEED-HALF" + suf, sameBodies(made.runs().runs.get(pre + HALF), base, "batch interval x1/2"));
            }
        } catch (Exception | Error e) {
            error = e.toString();
            err.println("requirements crashed: " + error);
            e.printStackTrace(err);
            acc.errorUnrecorded("error: the requirements run did not complete: " + error);
        } finally {
            if (made != null) CoreGate.delete(made.tmp());
        }
        if (made == null) for (String n : manifest.runs()) notExecuted.add(n + ": not made");
        RequirementsAccount.Eligibility el = acc.eligibility(engine, gate.passed(), gate.why());
        int exit = exitCode(acc, error, notExecuted);
        ObjectNode rep = Json.object().put("kind", "requirements").put("engine", engine);
        Json.putSafe(rep, "exit", exit);
        rep.put("manifest", "gate/" + engine + "-requirements.json").put("manifestSha256", manifest.sha256());
        rep.put("gateReport", report).put("gateReportSha256", gate.sha256());
        rep.set("eligibility", el.toJson());
        Json.putSafe(rep, "requirementsExpected", acc.expected().size());
        Json.putSafe(rep, "requirementsRecorded", acc.entries().size());
        rep.set("results", acc.toJson());
        ArrayNode ne = rep.putArray("runsNotExecuted");
        notExecuted.forEach(ne::add);
        rep.set("isolation", observations);
        rep.put("engineArtifactSha256", flags.get("engine-artifact"));
        rep.put("command", command).put("startedAt", startedAt).put("finishedAt", Instant.now().toString()).put("error", error);
        if (made != null) { rep.set("generations", made.generations()); rep.set("controlStores", made.controlStores()); rep.set("runs", made.runsOut()); rep.set("engineRuns", made.engineRuns()); }
        try { Files.createDirectories(outDir); Json.write(outDir.resolve(FILE), rep); }
        catch (IOException | RuntimeException e) { exit = 1; err.println("cannot write the requirements report: " + e.getMessage()); }
        for (String n : notExecuted) out.println("NOT EXECUTED " + n);
        out.println(acc.line() + (exit == 1 && acc.exitCode() == 0 ? " (exit 1: " + (error != null ? "the runs crashed" : notExecuted.size() + " runs not executed") + ")" : ""));
        out.println(el.line());
        return exit;
    }

    /** Spec 705: 0 only when every requirement was run and recorded; a crash of the runs or a run not executed is 1 even though the
     *  requirements it would have decided are recorded as errors. */
    static int exitCode(RequirementsAccount acc, String error, List<String> notExecuted) {
        return error != null || !notExecuted.isEmpty() ? 1 : acc.exitCode();
    }

    private static void record(RequirementsAccount acc, String id, Entry e) { acc.record(id, e.status(), e.evidence()); }

    static JsonNode isolation(ReplayEngine.Run er) throws IOException {
        if (er == null) return null;
        Path f = er.out().resolve(StarvationObserver.FILE);
        return Files.isRegularFile(f) ? Json.read(f) : null;
    }

    // --- injection 24 and item 5 (c) -----------------------------------------------------------------------------------------

    /** The cuts of {@code r}'s recording whose batch declares {@code station} starved. */
    static TreeSet<Long> starvedCuts(Run r, String station) {
        TreeSet<Long> s = new TreeSet<>();
        for (JsonNode b : r.manifest.path("batches"))
            for (JsonNode i : b.path("idleInputs")) if (CutAssembler.STARVED.equals(i.path("kind").asText()) && station.equals(i.path("station").asText())) s.add(b.path("cutId").asLong());
        return s;
    }

    /** The stage-1 and stage-2 output topics a station's writers write at its partition. */
    static final List<String> STATION_TOPICS = List.of(RunFold.WR, RunFold.SAMPLES, "forseti.dispositions-stage1", RunFold.AFFECTED, RunFold.SETS, "forseti.dispositions-stage2");

    /** Injection 24's own expectation on one placement: the run equals the oracle at every cut (clean integrity, ground truth); in the
     *  committed journals ({@code runDir/journals}) none of the starved station's writers commits a progress of a starved cut, and the
     *  first progress each commits after the starvation merges exactly the starved cuts (the resume); every visit of the station keeps, at
     *  every starved cut, the body it had at the last cut before the starvation (a visit not settled then stays not settled), with at
     *  least one such unsettled visit; and every incident of the station ({@link #incidentEvidence}) accepted before the starvation with
     *  its affected set not settled keeps its set and assessment bodies at every starved cut and ends with the bodies of the control
     *  ({@code control}: the same recording without the starvation), with at least one such incident. A recording without an incident
     *  of the starved station, or a control that was not made, is an error. */
    static Entry injection24(Run r, Path runDir, Run control) throws IOException {
        if (r == null) return new Entry(Status.ERROR, "error: the run was not made");
        if (r.failure != null) return new Entry(Status.ERROR, "error: " + r.label() + ": " + r.failure);
        String starved = r.manifest.path("starvation").path("station").asText(null);
        if (starved == null) return new Entry(Status.ERROR, "error: " + r.label() + ": the recording carries no starvation");
        if (control == null) return new Entry(Status.ERROR, "error: the control run was not made");
        if (control.failure != null) return new Entry(Status.ERROR, "error: " + control.label() + ": " + control.failure);
        if (control.manifest.path("starvation").isObject()) return new Entry(Status.ERROR, "error: the control " + control.label() + " carries a starvation");
        List<String> bad = new ArrayList<>();
        if (!r.ok()) bad.add("the run does not hold: " + r.summary());
        TreeSet<Long> sc = starvedCuts(r, starved);
        List<Long> cuts = r.cutIds();
        Long before = null;
        for (long k : cuts) if (k < sc.first()) before = k;
        if (before == null) return new Entry(Status.ERROR, "error: no cut before the starvation (first starved cut " + sc.first() + ")");
        // the committed journals of the starved station's writers
        RoutingManifest routing = RoutingManifest.read(Json.read(r.recording.resolve(yggdrasil.forseti.cli.recording.Recording.ROUTING)));
        int part = routing.partitionOf(starved);
        for (Map.Entry<String, Integer> e : routing.stations().entrySet())
            if (!e.getKey().equals(starved) && e.getValue() == part) return new Entry(Status.ERROR, "error: the starved station shares its output partition " + part + " with " + e.getKey());
        if (runDir == null || !Files.isDirectory(runDir.resolve("journals"))) return new Entry(Status.ERROR, "error: " + r.label() + ": no committed journals kept");
        List<String> merges = new ArrayList<>();
        for (String t : STATION_TOPICS) {
            Path f = runDir.resolve("journals").resolve(t).resolve(part + ".jsonl");
            if (!Files.isRegularFile(f)) { bad.add("no journal " + t + "/" + part); continue; }
            JsonNode first = null;
            for (String line : Files.readAllLines(f, java.nio.charset.StandardCharsets.UTF_8)) {
                if (line.isEmpty()) continue;
                JsonNode v = RunFold.readLine(line).path("value");
                if (yggdrasil.forseti.cli.replay.OutputRecords.kind(v) != yggdrasil.forseti.cli.replay.OutputRecords.Kind.PROGRESS) continue;
                long c = v.path("cutId").asLong();
                if (sc.contains(c)) bad.add(t + "/" + part + " committed a progress of starved cut " + c);
                if (c > sc.last() && first == null) first = v;
            }
            if (first == null) { bad.add(t + "/" + part + ": no progress after the starvation"); continue; }
            TreeSet<Long> merged = new TreeSet<>();
            first.path("mergedCutIds").forEach(x -> merged.add(x.asLong()));
            if (!merged.equals(sc)) bad.add(t + "/" + part + ": the first progress after the starvation (cut " + first.path("cutId").asLong() + ") merges " + merged + ", the starved cuts are " + sc);
            else merges.add(t + " cut " + first.path("cutId").asLong());
        }
        for (long k : sc) if (r.perStation(k).has(starved)) bad.add("the starved station completed starved cut " + k);
        int unsettled = 0, visits = 0;
        for (JsonNode v : r.expected.path("visits")) {
            if (!starved.equals(v.path("station").asText())) continue;
            visits++;
            String key = RunFold.keyText(v.path("key"));
            JsonNode at0 = r.fold.at(RunFold.WR, key, before);
            if (at0 != null && !"complete".equals(at0.path("status").asText())) unsettled++;
            for (long k : sc) {
                JsonNode x = r.fold.at(RunFold.WR, key, k);
                if (!Objects.equals(at0, x)) { bad.add(v.path("visitRef").asText() + " changed at starved cut " + k + ": " + Injections.status(at0) + " -> " + Injections.status(x)); break; }
            }
        }
        if (bad.isEmpty() && unsettled == 0) return new Entry(Status.ERROR, "error: no visit of " + starved + " was unsettled at cut " + before + ", before the starvation: nothing to check");
        if (!control.ok()) bad.add("the control does not hold: " + control.summary());
        Map<String, Incident> mine = new TreeMap<>(), theirs = new TreeMap<>();
        for (Iterator<String> it = r.expected.path("incidents").fieldNames(); it.hasNext();) {
            String key = it.next();
            Incident x = Incident.of(r.fold, key, r.lastCut());
            if (x == null) bad.add("affected set " + key + ": none at the last cut " + r.lastCut());
            else if (starved.equals(x.station())) { mine.put(key, x); theirs.put(key, Incident.of(control.fold, key, control.lastCut())); }
        }
        if (mine.isEmpty() && bad.isEmpty()) return new Entry(Status.ERROR, "error: " + r.label() + " carries no incident of " + starved + ": nothing to check");
        IncidentEvidence inc = incidentEvidence(mine, theirs, before, sc);
        bad.addAll(inc.problems());
        if (bad.isEmpty() && inc.unsettled() == 0)
            return new Entry(Status.ERROR, "error: no incident of " + starved + " was accepted with its set unsettled at cut " + before + ", before the starvation: nothing to check");
        String text = r.label() + ": starved " + starved + " at cuts " + Injections.compact(new ArrayList<>(sc)) + "; " + unsettled + " of its " + visits
            + " visits unsettled at cut " + before + " (the last before the starvation) and unchanged at every starved cut; in the committed journals of partition " + part
            + " no progress of a starved cut, and the first progress after it merges exactly the starved cuts on " + merges.size() + " writers (" + String.join(", ", merges) + "); "
            + inc.unsettled() + " of its " + mine.size() + " incidents " + inc.summary() + " accepted with the affected set unsettled at cut " + before
            + ", its set and " + inc.assessments() + " assessment bodies unchanged and unsettled at every starved cut and equal to " + control.label()
            + "'s at the last cut (" + r.lastCut() + " / " + control.lastCut() + "); " + r.summary();
        return bad.isEmpty() ? new Entry(Status.PASS, text) : new Entry(Status.MISS, "PROBLEMS: " + abbrev(String.join("; ", bad), 1500) + " || " + text);
    }

    /** One incident of a run: its affected-set key, station ({@code incident.stationRef} of its set) and its set and assessment
     *  ({@code forseti.affected}) bodies as the fold saw them, by cut. */
    record Incident(String key, String id, String station, java.util.function.LongFunction<JsonNode> set, java.util.function.LongFunction<Map<String, JsonNode>> assessments,
                    long last) {
        /** The incident of {@code setKey} in {@code fold}, or null when it has no set at {@code last}. */
        static Incident of(RunFold fold, String setKey, long last) {
            JsonNode s = fold.at(RunFold.SETS, setKey, last);
            if (s == null) return null;
            JsonNode k = Json.read(setKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            List<String> units = new ArrayList<>();
            for (String u : fold.history(RunFold.AFFECTED).keySet()) {
                JsonNode uk = Json.read(u.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                if (uk.path("incidentId").equals(k.path("incidentId")) && uk.path("source").equals(k.path("source"))) units.add(u);
            }
            return new Incident(setKey, k.path("incidentId").asText(), s.path("incident").path("stationRef").asText(null), c -> fold.at(RunFold.SETS, setKey, c), c -> {
                Map<String, JsonNode> m = new TreeMap<>();
                for (String u : units) { JsonNode b = fold.at(RunFold.AFFECTED, u, c); if (b != null) m.put(u, b); }
                return m;
            }, last);
        }
    }

    /** What {@link #incidentEvidence} found: the incidents accepted with an unsettled set before the starvation and checked, their
     *  ids, the assessment bodies compared and the problems. */
    record IncidentEvidence(int unsettled, String summary, int assessments, List<String> problems) {}

    /** Injection 24's incident expectation on the starved station's incidents {@code mine} (set key -> incident of the starved run),
     *  against the control's ({@code theirs}, same keys): an incident whose set exists at cut {@code before} (accepted before the
     *  starvation) with {@code coverage.settled} false is checked; at every cut of {@code starved} its set body and every assessment body
     *  equal those at {@code before} (so the set stays unsettled), and at the last cut its set and assessment bodies equal the control's
     *  at its last cut (the resume converges to the run without the starvation). An incident settled or not yet delivered at
     *  {@code before} is not checked. */
    static IncidentEvidence incidentEvidence(Map<String, Incident> mine, Map<String, Incident> theirs, long before, SortedSet<Long> starved) {
        List<String> bad = new ArrayList<>(), ids = new ArrayList<>();
        int assessments = 0;
        for (Incident x : mine.values()) {
            JsonNode s0 = x.set().apply(before);
            if (s0 == null || s0.path("coverage").path("settled").asBoolean(true)) continue;
            ids.add(x.id());
            Map<String, JsonNode> u0 = x.assessments().apply(before);
            assessments += u0.size();
            for (long k : starved) {
                JsonNode sk = x.set().apply(k);
                if (!Objects.equals(s0, sk)) { bad.add("incident " + x.id() + ": its affected set changed at starved cut " + k + " (settled " + (sk == null ? null : sk.path("coverage").path("settled")) + ")"); break; }
                if (!u0.equals(x.assessments().apply(k))) { bad.add("incident " + x.id() + ": its assessments changed at starved cut " + k); break; }
            }
            Incident c = theirs.get(x.key());
            if (c == null) { bad.add("incident " + x.id() + ": the control has no affected set of it"); continue; }
            if (!Objects.equals(x.set().apply(x.last()), c.set().apply(c.last()))) bad.add("incident " + x.id() + ": its affected set at the last cut differs from the control's");
            if (!x.assessments().apply(x.last()).equals(c.assessments().apply(c.last()))) bad.add("incident " + x.id() + ": its assessments at the last cut differ from the control's");
        }
        return new IncidentEvidence(ids.size(), ids.toString(), assessments, bad);
    }

    /** Item 5 (c) on one placement, from the run's observation ({@link StarvationObserver#FILE}): pass when every checked deadline
     *  settled within the observation deadline and the run holds; miss when one did not (each listed with its counting cut and the
     *  W1/W2 last seen); error when the run failed, has no observation or checked nothing. */
    static Entry settles(Run r, JsonNode iso) {
        if (r == null) return new Entry(Status.ERROR, "error: the run was not made");
        if (r.failure != null) return new Entry(Status.ERROR, "error: " + r.label() + ": " + r.failure);
        if (iso == null) return new Entry(Status.ERROR, "error: " + r.label() + ": the run recorded no observation (" + StarvationObserver.FILE + ")");
        if (!iso.path("coverage").isObject()) return new Entry(Status.ERROR, "error: " + r.label() + ": the observation records no coverage");
        if (iso.path("coverage").path("uncovered").size() > 0)
            return new Entry(Status.ERROR, "error: " + r.label() + ": non-starved stations with a deadline in the starvation window and no check " + iso.path("coverage").path("uncovered")
                + " (" + iso.path("coverage").path("stations") + ")");
        int planned = iso.path("planned").size();
        if (planned == 0) return new Entry(Status.ERROR, "error: " + r.label() + ": no deadline of another station counts from a starved batch: nothing checked (" + iso.path("unchecked").size() + " unchecked)");
        if (iso.path("checks").size() != planned) return new Entry(Status.ERROR, "error: " + r.label() + ": " + iso.path("checks").size() + " of " + planned + " planned checks observed");
        List<String> missed = new ArrayList<>();
        long worst = 0;
        for (JsonNode c : iso.path("checks")) {
            String what = c.path("station").asText() + " " + c.path("kind").asText() + " " + c.path("ref").asText() + " (deadline " + c.path("deadline").asLong() + ", counting cut "
                + c.path("countingCut").asLong() + ")";
            if ("settled".equals(c.path("outcome").asText())) worst = Math.max(worst, c.path("settledAfterMs").asLong());
            else missed.add(what + " not settled after " + c.path("observedForMs").asLong() + " ms (W1 " + c.path("lastW1") + ", W2 " + c.path("lastW2") + ")");
        }
        String text = r.label() + ": " + planned + " checked deadlines of the other stations, observation deadline " + iso.path("deadlineMs").asLong() + " ms, slowest settled after " + worst
            + " ms; " + iso.path("unchecked").size() + " deadlines not counted from a starved batch";
        if (!missed.isEmpty()) return new Entry(Status.MISS, "MISSED: " + String.join("; ", missed) + " || " + text);
        if (!r.ok()) return new Entry(Status.MISS, "the run does not hold: " + r.summary() + " || " + text);
        return new Entry(Status.PASS, text);
    }

    // --- item 5 (d) ------------------------------------------------------------------------------------------------------------

    /** After injection 24's resume: the starved run's cuts from the first cut after the starvation, against its control (the same
     *  recording without the starvation, the same release). */
    static Entry resume(Run starved, Run control) {
        if (starved == null || control == null) return new Entry(Status.ERROR, "error: a run was not made");
        if (starved.failure != null || control.failure != null) return new Entry(Status.ERROR, "error: " + (starved.failure != null ? starved.label() + ": " + starved.failure : control.label() + ": " + control.failure));
        String station = starved.manifest.path("starvation").path("station").asText(null);
        if (station == null) return new Entry(Status.ERROR, "error: " + starved.label() + " carries no starvation");
        if (control.manifest.path("starvation").isObject()) return new Entry(Status.ERROR, "error: the control " + control.label() + " carries a starvation");
        long resumed = starvedCuts(starved, station).last() + 1;
        return compare(starved, control, resumed, "after the resume (cuts from " + resumed + ")");
    }

    /** The speed change: the run at another batch interval against the baseline, at every common cut. */
    static Entry sameBodies(Run x, Run base, String what) {
        if (x == null || base == null) return new Entry(Status.ERROR, "error: a run was not made");
        if (x.failure != null || base.failure != null) return new Entry(Status.ERROR, "error: " + (x.failure != null ? x.label() + ": " + x.failure : base.label() + ": " + base.failure));
        return compare(x, base, Long.MIN_VALUE, what);
    }

    /** Per cut of a recording, its replay input boundary: the count and an order-independent digest of the multiset of input records
     *  (topic, partition, canonical value bytes) its batch and every earlier one deliver. */
    static Map<Long, String> boundaries(Path recording) {
        yggdrasil.forseti.cli.recording.Recording rec = yggdrasil.forseti.cli.recording.Recording.open(recording);
        List<yggdrasil.forseti.cli.recording.Recording.Batch> batches = rec.batches();
        long[][] add = new long[batches.size()][3];
        for (String topic : RoutingManifest.TOPICS) for (int p : rec.routing().subscribed(topic)) {
            try (yggdrasil.forseti.cli.recording.Recording.Reader r = rec.reader(topic, p)) {
                int k = 0;
                for (yggdrasil.forseti.cli.recording.Recording.Line l; (l = r.next()) != null;) {
                    long off = l.record().offset();
                    while (k < batches.size() && batches.get(k).through(topic, p) < off) k++;
                    if (k >= batches.size()) break;
                    byte[] d = sha256((topic + "/" + p + "\u0000" + Canonical.utf8(Canonical.bytes(l.record().value()))).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(d);
                    add[k][0]++; add[k][1] += bb.getLong(); add[k][2] += bb.getLong();
                }
            }
        }
        Map<Long, String> m = new LinkedHashMap<>();
        long n = 0, x = 0, y = 0;
        for (int k = 0; k < batches.size(); k++) {
            n += add[k][0]; x += add[k][1]; y += add[k][2];
            m.put(batches.get(k).cutId(), n + ":" + Long.toHexString(x) + Long.toHexString(y));
        }
        return m;
    }

    static byte[] sha256(byte[] b) {
        try { return java.security.MessageDigest.getInstance("SHA-256").digest(b); } catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    /** The common cuts of two runs: (cut of a at or after {@code from}, cut of b) with the same replay input boundary, a boundary found at
     *  exactly one cut of each run, in a's cut order. */
    static List<long[]> commonCuts(Map<Long, String> a, Map<Long, String> b, long from) {
        Map<String, List<Long>> ia = new LinkedHashMap<>(), ib = new HashMap<>();
        a.forEach((k, v) -> ia.computeIfAbsent(v, z -> new ArrayList<>()).add(k));
        b.forEach((k, v) -> ib.computeIfAbsent(v, z -> new ArrayList<>()).add(k));
        List<long[]> l = new ArrayList<>();
        for (Map.Entry<String, List<Long>> e : ia.entrySet()) {
            List<Long> x = ib.get(e.getKey());
            if (e.getValue().size() == 1 && x != null && x.size() == 1 && e.getValue().get(0) >= from) l.add(new long[] {e.getValue().get(0), x.get(0)});
        }
        l.sort(Comparator.comparingLong(z -> z[0]));
        return l;
    }

    /** {@code a} against {@code b} at their common cuts (from {@code from} in {@code a}). Each pair at which both runs completed every
     *  station and that is comparable (spec 624) is compared on every body partition and must show bodies on both sides (live records
     *  and a work-records fingerprint). Pass: both runs hold, every compared pair equal with bodies, and the settled pair (the first at
     *  which both runs are settled) and the last common pair among the compared. Miss: a run does not hold or a compared pair differs.
     *  Error: a compared pair without bodies, or the settled or last common pair not compared. */
    static Entry compare(Run a, Run b, long from, String what) {
        List<String> bad = new ArrayList<>(), noBodies = new ArrayList<>();
        if (!a.ok()) bad.add("the run does not hold: " + a.summary());
        if (!b.ok()) bad.add("the run does not hold: " + b.summary());
        if (a.recording == null || b.recording == null || !Files.isRegularFile(a.recording.resolve("manifest.json")) || !Files.isRegularFile(b.recording.resolve("manifest.json")))
            return new Entry(Status.ERROR, "error: the recordings of " + a.label() + " and " + b.label() + " are not kept");
        List<long[]> common = commonCuts(boundaries(a.recording), boundaries(b.recording), from);
        List<Long> deadlines = new ArrayList<>(ReplayGate.comparisonDeadlines(a));
        deadlines.addAll(ReplayGate.comparisonDeadlines(b));
        Set<String> parts = new TreeSet<>();
        for (String t : BODY_TOPICS) { for (String k : a.fold.fingerprints().keySet()) if (k.startsWith(t + "/")) parts.add(k); for (String k : b.fold.fingerprints().keySet()) if (k.startsWith(t + "/")) parts.add(k); }
        Set<String> stations = new TreeSet<>();
        if (!a.cutIds().isEmpty()) a.perStation(a.lastCut()).fieldNames().forEachRemaining(stations::add);
        List<String> compared = new ArrayList<>(), skipped = new ArrayList<>();
        Set<String> comparedSet = new HashSet<>();
        long rows = 0;
        Long sa = a.settledCut(), sb = b.settledCut();
        String settledPair = null;
        for (long[] c : common) if (settledPair == null && sa != null && sb != null && c[0] >= sa && c[1] >= sb) settledPair = c[0] + "/" + c[1];
        String lastPair = common.isEmpty() ? null : common.get(common.size() - 1)[0] + "/" + common.get(common.size() - 1)[1];
        for (long[] c : common) {
            long ca = c[0], cb = c[1];
            String pair = ca + "/" + cb;
            boolean completed = !stations.isEmpty();
            for (String st : stations) if (!a.perStation(ca).has(st) || !b.perStation(cb).has(st)) completed = false;
            String why = completed ? Comparison.notComparable(a.side(ca), b.side(cb), deadlines) : "not completed by both";
            if (why != null) { skipped.add(pair); continue; }
            compared.add(pair);
            comparedSet.add(pair);
            long ra = 0, rb = 0;
            boolean wrA = false, wrB = false;
            for (String part : parts) {
                String fa = a.fold.fingerprints().getOrDefault(part, Map.of()).get(ca), fb = b.fold.fingerprints().getOrDefault(part, Map.of()).get(cb);
                if (!Objects.equals(fa, fb)) bad.add(a.label() + " cut " + ca + " vs " + b.label() + " cut " + cb + " " + part + " " + fa + " vs " + fb);
                ra += Math.max(0, Injections.count(fa)); rb += Math.max(0, Injections.count(fb));
                if (part.startsWith(RunFold.WR + "/")) { if (fa != null) wrA = true; if (fb != null) wrB = true; }
            }
            if (ra == 0 || rb == 0 || !wrA || !wrB) noBodies.add(pair + " (live " + ra + "/" + rb + ", work-records fingerprint " + wrA + "/" + wrB + ")");
            rows += ra;
        }
        String text = a.label() + " vs " + b.label() + " " + what + ": " + common.size() + " common cuts (equal replay input boundary), " + compared.size() + " compared "
            + abbrev(String.join(" ", compared), 300) + " on " + parts.size() + " body partitions (" + rows + " live bodies), " + skipped.size() + " not compared "
            + abbrev(String.join(" ", skipped), 200) + "; settled pair " + settledPair + ", last common pair " + lastPair;
        if (!bad.isEmpty()) return new Entry(Status.MISS, "PROBLEMS: " + abbrev(String.join("; ", bad), 1500) + " || " + text);
        List<String> errs = new ArrayList<>();
        if (!noBodies.isEmpty()) errs.add("compared pairs without bodies on both sides " + abbrev(noBodies.toString(), 300));
        if (settledPair == null || !comparedSet.contains(settledPair)) errs.add("the settled pair " + settledPair + " is not compared");
        if (lastPair == null || !comparedSet.contains(lastPair)) errs.add("the last common pair " + lastPair + " is not compared");
        if (!errs.isEmpty()) return new Entry(Status.ERROR, "error: " + String.join("; ", errs) + " || " + text);
        return new Entry(Status.PASS, text);
    }

    static String abbrev(String s, int n) { return s == null ? "" : s.length() <= n ? s : s.substring(0, n) + "..."; }
}
