package yggdrasil.forseti.cli.generation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.cli.gate.GateBundle;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.GenerationStore;

/** The reader of the gate report {@code generation promote} is given (plan Step 21.1; spec "Generations and revisions", "Run citation",
 *  "Gate registration"). Everything is checked against files the reader loads itself from the repository root it runs in
 *  ({@code gate/core-assertions.json}, {@code gate/core-shards.json}, the scenario locks); no flag and no report field points it at another
 *  manifest or plan. The evidence bundle is, for a {@code full} report, the report's own directory, and for an {@code aggregate} report
 *  each {@code <shards dir>/<shard>/}. Refusals are closed codes ({@link ReferenceEngine.Refused}, exit 1), checked in a fixed order:
 *
 *  <ol>
 *  <li>{@code gate-report-unreadable}: strict JSON (duplicate keys refused), every required field present with its type;</li>
 *  <li>{@code gate-report-negative-control}: {@code kind == gate}, {@code negativeControl} null, {@code intendedFailures} empty;</li>
 *  <li>{@code gate-report-shard}: {@code layout} is {@code full} or {@code aggregate}, {@code shard} null;</li>
 *  <li>{@code gate-report-failed}: exit 0, no error, nothing failed or not run, every result ok;</li>
 *  <li>{@code gate-report-manifest}: the committed manifest's raw digest;</li>
 *  <li>{@code gate-report-assertions}: the result-id set is the manifest's, each id once, and the counts are its size;</li>
 *  <li>{@code gate-report-layout}: the fields of the layout, the committed plan and the owner of every result;</li>
 *  <li>{@code gate-report-evidence}: the evidence files by the gate's own validator ({@link GateBundle#validate} on a full bundle,
 *      {@link GateBundle#readShards} on the shard directories), the summary sets of the plan, paths inside their bundle;</li>
 *  <li>{@code gate-report-generation}: the target selected by producer and store, then by {@code registeredRowSha256}, in the bundle's own
 *      store ({@code --state});</li>
 *  <li>{@code gate-report-generation-isolated}: the target is {@code shared};</li>
 *  <li>{@code gate-report-run}: the {@code --run} file is the one run entry of the target, not shuffled, and agrees with it;</li>
 *  <li>{@code gate-report-coverage}: the target run's summary holds and covers exactly its cut log, and its pins are the report's;</li>
 *  <li>{@code gate-report-scope}: {@code semantic}, or {@code core} with {@code --accept-scope core}.</li>
 *  </ol>
 *
 *  <p>Digests establish integrity between the files; the report itself is not authenticated (the operator-selected bundle or the CI run
 *  that produced it is what makes it trustworthy). {@code summaries.json} holds {@code generationRuns}, an object keyed by generation-run
 *  name; the plan's per-shard {@code generationRuns} is required; {@code runs[].journals} / {@code journalsIndexSha256} are null for a
 *  run whose journals are not kept, and the gate's validator requires them on the runs that keep them. */
public final class GateReportReader {
    private GateReportReader() {}

    public static final String UNREADABLE = "gate-report-unreadable", NEGATIVE_CONTROL = "gate-report-negative-control", SHARD = "gate-report-shard",
        FAILED = "gate-report-failed", MANIFEST = "gate-report-manifest", ASSERTIONS = "gate-report-assertions", LAYOUT = "gate-report-layout",
        EVIDENCE = "gate-report-evidence", GENERATION = "gate-report-generation", ISOLATED = "gate-report-generation-isolated", RUN = "gate-report-run",
        COVERAGE = "gate-report-coverage", SCOPE = "gate-report-scope";

    static final String FULL = "full", AGGREGATE = "aggregate", REPLAY = "event-time-replay";

    /** What {@code promote} asks: the repository root, {@code --gate-report}, {@code --shards} (null when absent), {@code --state},
     *  {@code --generation}, {@code --registered-row-sha256}, {@code --run} and whether {@code --accept-scope core} was given. */
    public record Request(Path root, Path report, Path shards, Path state, long generation, String registeredRowSha256, Path run, boolean acceptCore) {}

    /** An accepted report: its raw digest (what {@code verified} and {@code current} record), the run citation {@code {runId,
     *  runRecordSha256, offsetRanges}} the three rows of the promotion carry, and the generation and run entries it selected. */
    public record Accepted(String gateReportSha256, ObjectNode run, JsonNode generation, JsonNode runEntry) {}

    /** Reads and checks the report against {@code store}, the control store of {@code --state} as it is now (the caller holds its lock). */
    public static Accepted read(Request q, FileGenerationStore store) {
        // 1. strict parse, required fields
        byte[] bytes;
        try { bytes = Files.readAllBytes(q.report()); } catch (IOException e) { throw refused(UNREADABLE, "cannot read " + q.report() + ": " + e.getMessage()); }
        String reportSha = Canonical.digestOf(bytes);
        JsonNode report;
        try { report = Json.read(bytes); } catch (IllegalArgumentException e) { throw refused(UNREADABLE, "not strict JSON: " + e.getMessage()); }
        List<String> shape = shape(report);
        if (!shape.isEmpty()) throw refused(UNREADABLE, shape);

        // 2. kind
        if (!"gate".equals(report.get("kind").textValue()) || !report.get("negativeControl").isNull() || report.get("intendedFailures").size() != 0)
            throw refused(NEGATIVE_CONTROL, "kind " + report.get("kind") + ", negativeControl " + report.get("negativeControl") + ", intendedFailures "
                + report.get("intendedFailures"));

        // 3. layout
        String layout = report.get("layout").textValue();
        if (!(FULL.equals(layout) || AGGREGATE.equals(layout)) || !report.get("shard").isNull())
            throw refused(SHARD, "layout " + layout + ", shard " + report.get("shard") + ": promotion accepts a whole-gate report only (full or aggregate)");

        // 4. outcome
        List<String> failed = new ArrayList<>();
        if (report.get("exit").longValue() != 0) failed.add("exit " + report.get("exit"));
        if (!report.get("error").isNull()) failed.add("error " + report.get("error").textValue());
        if (report.get("failed").size() != 0) failed.add("failed " + report.get("failed"));
        if (report.get("notRun").size() != 0) failed.add("notRun " + report.get("notRun"));
        for (JsonNode r : report.get("results")) if (!r.get("ok").booleanValue()) failed.add(r.get("id").textValue() + " did not hold");
        if (!failed.isEmpty()) throw refused(FAILED, failed);

        // 5. the committed manifest
        Path manifestFile = q.root().resolve(GateBundle.MANIFEST);
        byte[] manifestBytes;
        try { manifestBytes = Files.readAllBytes(manifestFile); } catch (IOException e) { throw refused(MANIFEST, "cannot read the committed manifest: " + e.getMessage()); }
        String manifestSha = Canonical.digestOf(manifestBytes);
        if (!manifestSha.equals(report.get("manifestSha256").textValue()))
            throw refused(MANIFEST, "manifestSha256 " + report.get("manifestSha256").textValue() + ", the committed " + GateBundle.MANIFEST + " is " + manifestSha);
        List<String> ids = GateBundle.manifestIds(Json.read(manifestBytes));

        // 6. the result-id set, then the counts
        List<String> idProblems = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (JsonNode r : report.get("results")) if (!seen.add(r.get("id").textValue())) idProblems.add(r.get("id").textValue() + " is recorded twice");
        if (!seen.equals(new HashSet<>(ids))) {
            Set<String> missing = new TreeSet<>(ids), extra = new TreeSet<>(seen);
            missing.removeAll(seen);
            extra.removeAll(ids);
            idProblems.add("result ids differ from the manifest's: missing " + missing + ", not in the manifest " + extra);
        }
        long n = ids.size();
        if (report.get("assertionsExpected").longValue() != n || report.get("assertionsRun").longValue() != n || report.get("results").size() != n)
            idProblems.add("assertions run " + report.get("assertionsRun") + " / expected " + report.get("assertionsExpected") + " with " + report.get("results").size()
                + " results, the committed manifest holds " + n);
        if (!idProblems.isEmpty()) throw refused(ASSERTIONS, idProblems);

        // 7. the layout
        GateBundle.Plan plan = GateBundle.plan(q.root(), ids);
        List<String> lay = new ArrayList<>();
        if (FULL.equals(layout)) {
            if (!report.get("shardPlanSha256").isNull()) lay.add("a full report names shardPlanSha256 " + report.get("shardPlanSha256"));
            if (report.get("shards").size() != 0) lay.add("a full report lists shards " + report.get("shards"));
            for (JsonNode r : report.get("results")) if (!r.get("shard").isNull()) lay.add("result " + r.get("id").textValue() + " names shard " + r.get("shard"));
            if (!hex(report.get("summariesSha256"))) lay.add("summariesSha256 " + report.get("summariesSha256") + " is not a SHA-256");
        } else {
            if (!report.get("summariesSha256").isNull()) lay.add("an aggregate report names summariesSha256 " + report.get("summariesSha256"));
            if (!plan.sha256().equals(report.get("shardPlanSha256").textValue()))
                lay.add("shardPlanSha256 " + report.get("shardPlanSha256") + ", the committed " + GateBundle.PLAN + " is " + plan.sha256());
            for (String x : plan.problems()) lay.add(GateBundle.PLAN + ": " + x);
            List<String> names = new ArrayList<>();
            for (JsonNode s : report.get("shards")) {
                names.add(s.path("name").asText(null));
                if (!s.path("exit").isIntegralNumber() || s.path("exit").longValue() != 0) lay.add("shard " + s.path("name") + " exited " + s.path("exit"));
                if (!hex(s.path("reportSha256")) || !hex(s.path("summariesSha256"))) lay.add("shard " + s.path("name") + " has no two SHA-256 digests");
            }
            if (!names.equals(plan.shards())) lay.add("shards " + names + ", the committed plan's are " + plan.shards() + " (each once, in plan order)");
            for (JsonNode r : report.get("results")) {
                String id = r.get("id").textValue(), owner = plan.owner().get(id);
                if (owner == null || !owner.equals(r.get("shard").textValue())) lay.add("result " + id + " names shard " + r.get("shard") + ", its plan owner is " + owner);
            }
        }
        if (!lay.isEmpty()) throw refused(LAYOUT, lay);

        // 8. the evidence files, by the gate's own validator
        Path reportDir = q.report().toAbsolutePath().getParent();
        List<String> ev = new ArrayList<>();
        Map<String, JsonNode> bundleSummaries = new HashMap<>();   // the summaries.json of each bundle, by shard (null for full)
        if (FULL.equals(layout)) {
            if (q.shards() != null) throw refused(EVIDENCE, "--shards is for an aggregate report; a full report's evidence is its own directory");
            byte[] sb;
            try { sb = Files.readAllBytes(reportDir.resolve(GateBundle.SUMMARIES)); } catch (IOException e) { throw refused(EVIDENCE, "no " + GateBundle.SUMMARIES + " beside the report"); }
            if (!Canonical.digestOf(sb).equals(report.get("summariesSha256").textValue()))
                throw refused(EVIDENCE, GateBundle.SUMMARIES + " is " + Canonical.digestOf(sb) + ", the report binds " + report.get("summariesSha256").textValue());
            JsonNode summaries;
            try { summaries = Json.read(sb); } catch (IllegalArgumentException e) { throw refused(EVIDENCE, GateBundle.SUMMARIES + ": " + e.getMessage()); }
            bundleSummaries.put(null, summaries);
            ev.addAll(GateBundle.validate(q.root(), reportDir, report, summaries));
            keys(summaries.path("runs"), "name", GateBundle.scenarios(), "scenario summaries", ev);
            List<String> shuffled = new ArrayList<>();
            for (JsonNode s : summaries.path("shuffled")) shuffled.add(s.path("name").asText() + "@" + s.path("seed").asLong());
            sameSet(shuffled, GateBundle.shuffledKeys(), "shuffled summaries", ev);
            List<String> planned = new ArrayList<>();
            plan.generationRuns().values().forEach(planned::addAll);
            sameSet(generationRunKeys(summaries, ev), planned, "generation-run summaries", ev);
            for (JsonNode r : report.get("runs")) {
                if (!r.get("shard").isNull()) ev.add("run " + r.get("runId").textValue() + " names shard " + r.get("shard") + " in a full report");
                inside(reportDir, r, ev);
            }
        } else {
            if (q.shards() == null) throw refused(EVIDENCE, "an aggregate report needs --shards <dir>, the directory of its shard bundles");
            Path dir = q.shards().toAbsolutePath();
            Iterator<JsonNode> entries = report.get("shards").iterator();
            for (String name : plan.shards()) {
                JsonNode s = entries.next();
                Path d = dir.resolve(name);
                try {
                    byte[] rb = Files.readAllBytes(d.resolve("report.json")), sb = Files.readAllBytes(d.resolve(GateBundle.SUMMARIES));
                    if (!Canonical.digestOf(rb).equals(s.get("reportSha256").textValue())) ev.add("shard " + name + ": report.json is not the one the aggregate report binds");
                    if (!Canonical.digestOf(sb).equals(s.get("summariesSha256").textValue())) ev.add("shard " + name + ": " + GateBundle.SUMMARIES + " is not the one the aggregate report binds");
                    JsonNode summaries = Json.read(sb);
                    bundleSummaries.put(name, summaries);
                    sameSet(generationRunKeys(summaries, ev), plan.generationRuns().getOrDefault(name, List.of()), "shard " + name + ": generation-run summaries", ev);
                } catch (IOException | IllegalArgumentException e) { ev.add("shard " + name + ": " + e.getMessage()); }
            }
            if (ev.isEmpty()) {
                GateBundle.Read read = GateBundle.readShards(q.root(), dir, report);
                ev.addAll(read.problems());
                if (!GateBundle.same(read.merged().get("shards"), report.get("shards"))) ev.add("shards[] differ from the shard directories'");
                if (!GateBundle.same(read.merged().get("generations"), report.get("generations"))) ev.add("generations[] differ from the shards' merged generations");
                if (!GateBundle.same(read.merged().get("runs"), report.get("runs"))) ev.add("runs[] differ from the shards' merged runs");
                Map<String, JsonNode> results = new HashMap<>();
                for (JsonNode r : report.get("results")) results.put(r.get("id").textValue(), r);
                Set<String> recordedIds = new HashSet<>();
                for (GateBundle.Recorded r : read.recorded()) {
                    recordedIds.add(r.id());
                    JsonNode mine = results.get(r.id());
                    if (mine == null || mine.get("ok").booleanValue() != r.ok() || !r.shard().equals(mine.get("shard").textValue()))
                        ev.add("result " + r.id() + " differs from what shard " + r.shard() + " recorded");
                }
                for (JsonNode r : report.get("results"))
                    if (!"aggregate".equals(r.get("shard").textValue()) && !recordedIds.contains(r.get("id").textValue()))
                        ev.add("result " + r.get("id").textValue() + " is recorded by no shard");
            }
            for (JsonNode r : report.get("runs")) {
                String shard = r.get("shard").textValue();
                if (shard == null || !plan.shards().contains(shard)) { ev.add("run " + r.get("runId").textValue() + " names shard " + r.get("shard")); continue; }
                inside(dir.resolve(shard), r, ev);
            }
        }
        if (!ev.isEmpty()) throw refused(EVIDENCE, ev);

        // 9. the target: producer and store first, then the digest, in the bundle's own store
        String shardOfState = null;
        Path stateReal = real(q.state());
        if (FULL.equals(layout)) {
            if (stateReal == null || !stateReal.equals(real(reportDir.resolve(GateBundle.STATE))))
                throw refused(GENERATION, "--state " + q.state() + " is not the report's own store " + reportDir.resolve(GateBundle.STATE));
        } else {
            for (String name : plan.shards()) if (stateReal != null && stateReal.equals(real(q.shards().toAbsolutePath().resolve(name).resolve(GateBundle.STATE)))) shardOfState = name;
            if (shardOfState == null) throw refused(GENERATION, "--state " + q.state() + " is the store of no shard bundle under " + q.shards());
        }
        List<JsonNode> candidates = new ArrayList<>();
        for (JsonNode g : report.get("generations"))
            if (GateBundle.PIPELINE.equals(g.get("store").textValue()) && Objects.equals(shardOfState, g.get("shard").textValue())
                && q.registeredRowSha256().equals(g.get("registeredRowSha256").textValue())) candidates.add(g);
        if (candidates.size() != 1)
            throw refused(GENERATION, candidates.size() + " pipeline generations of " + (shardOfState == null ? "the report" : "shard " + shardOfState)
                + " have registeredRowSha256 " + q.registeredRowSha256() + ", exactly one required");
        JsonNode target = candidates.get(0);
        Optional<String> computed = store.registeredRowSha256(q.generation());
        Optional<GenerationStore.Entry> reg = store.registered(q.generation());
        if (computed.isEmpty() || reg.isEmpty()) throw refused(GENERATION, "generation " + q.generation() + " is not registered in --state");
        if (!computed.get().equals(q.registeredRowSha256()))
            throw refused(GENERATION, "the registered row of generation " + q.generation() + " in --state has digest " + computed.get() + ", not " + q.registeredRowSha256());
        if (target.get("generation").longValue() != q.generation() || !reg.get().scope().equals(target.get("scope").textValue())
            || !reg.get().row().rulesDigest().equals(target.get("rulesDigest").textValue()))
            throw refused(GENERATION, "the report's entry (generation " + target.get("generation") + ", scope " + target.get("scope") + ", rulesDigest "
                + target.get("rulesDigest") + ") is not the registered row of generation " + q.generation());

        // 10. shared
        if (!GateBundle.SHARED.equals(target.get("state").textValue()))
            throw refused(ISOLATED, "generation " + q.generation() + " is " + target.get("state").textValue() + ": only a generation of a kept pipeline store is promotable");

        // 11. the run
        byte[] runBytes;
        try { runBytes = Files.readAllBytes(q.run()); } catch (IOException e) { throw refused(RUN, "cannot read --run " + q.run()); }
        String runSha = Canonical.digestOf(runBytes);
        List<JsonNode> runs = new ArrayList<>();
        for (JsonNode r : report.get("runs"))
            if (Objects.equals(target.get("shard").textValue(), r.get("shard").textValue()) && target.get("store").textValue().equals(r.get("store").textValue())
                && q.registeredRowSha256().equals(r.get("registeredRowSha256").textValue()) && runSha.equals(r.get("runRecordSha256").textValue())) runs.add(r);
        if (runs.size() != 1) throw refused(RUN, runs.size() + " runs of the target generation have runRecordSha256 " + runSha + ", exactly one required");
        JsonNode entry = runs.get(0);
        Path bundle = FULL.equals(layout) ? reportDir : q.shards().toAbsolutePath().resolve(shardOfState);
        JsonNode rr;
        try { rr = Json.read(runBytes); } catch (IllegalArgumentException e) { throw refused(RUN, "--run is not JSON: " + e.getMessage()); }
        List<String> rp = new ArrayList<>();
        if (entry.get("shuffle").booleanValue()) rp.add("the run is a shuffled run");
        String variant = entry.get("variant").textValue();
        if (!GateBundle.ONLINE.equals(variant) && !REPLAY.equals(variant)) rp.add("variant " + variant + " is not a promotable run");
        if (rr.path("generation").asLong(-1) != q.generation()) rp.add("run record generation " + rr.path("generation"));
        if (!q.registeredRowSha256().equals(rr.path("registeredRowSha256").asText(null))) rp.add("run record registeredRowSha256 " + rr.path("registeredRowSha256"));
        if (!reg.get().scope().equals(rr.path("scope").asText(null))) rp.add("run record scope " + rr.path("scope") + ", the row's key " + reg.get().scope());
        if (!entry.get("runId").textValue().equals(rr.path("runId").asText(null))) rp.add("run record runId " + rr.path("runId") + ", the entry's " + entry.get("runId"));
        if (!reg.get().row().rulesDigest().equals(rr.path("fixtureDigests").path("rulesDigest").asText(null))) rp.add("run record rulesDigest " + rr.path("fixtureDigests").path("rulesDigest"));
        if (!entry.get("recordingManifestSha256").textValue().equals(rr.path("fixtureDigests").path("recordingManifestSha256").asText(null)))
            rp.add("run record recording manifest " + rr.path("fixtureDigests").path("recordingManifestSha256") + ", the entry's " + entry.get("recordingManifestSha256"));
        if (!rr.has("replay") || !rr.has("originalRecordingManifestSha256")) rp.add("the run record has no replay or originalRecordingManifestSha256 field");
        boolean replayed = REPLAY.equals(variant);
        if (replayed == rr.path("replay").isNull()) rp.add("run record replay " + rr.path("replay") + " for variant " + variant);
        if (!GateBundle.same(rr.path("originalRecordingManifestSha256"), entry.get("originalRecordingManifestSha256")))
            rp.add("run record originalRecordingManifestSha256 " + rr.path("originalRecordingManifestSha256") + ", the entry's " + entry.get("originalRecordingManifestSha256"));
        if (replayed == entry.get("originalRecordingManifestSha256").isNull()) rp.add("originalRecordingManifestSha256 " + entry.get("originalRecordingManifestSha256") + " for variant " + variant);
        if (!rr.path("offsetRanges").isObject() || !GateBundle.same(rr.path("offsetRanges"), entry.get("offsetRanges")))
            rp.add("run record offsetRanges " + rr.path("offsetRanges") + ", the entry's " + entry.get("offsetRanges"));
        byte[] cutBytes = null;
        try { cutBytes = Files.readAllBytes(bundle.resolve(entry.get("cutLog").textValue())); } catch (IOException e) { rp.add("no cut log " + entry.get("cutLog").textValue()); }
        if (cutBytes != null && !Canonical.digestOf(cutBytes).equals(entry.get("cutLogSha256").textValue())) rp.add("the cut log has another digest than cutLogSha256");
        journals(bundle, entry, rp);
        if (!rp.isEmpty()) throw refused(RUN, rp);

        // 12. its coverage
        List<String> cov = new ArrayList<>();
        JsonNode cutLog = Json.read(cutBytes);
        List<Long> cutIds = new ArrayList<>();
        for (JsonNode c : cutLog.path("cuts")) cutIds.add(c.path("cutId").asLong());
        String scenario = entry.get("scenario").textValue();
        JsonNode summaries = bundleSummaries.get(shardOfState), summary = null;
        String lockFile;
        if (replayed) {
            summary = summaries.path("generationRuns").get(scenario + "@replay");
            lockFile = scenario + ".replay.lock.json";
        } else {
            for (JsonNode s : summaries.path("runs")) if (scenario.equals(s.path("name").asText(null))) summary = s;
            lockFile = null;
        }
        if (summary == null) throw refused(COVERAGE, "no summary of the " + variant + " run of " + scenario);
        if (!summary.path("ok").asBoolean(false) || !(summary.path("failure").isNull() || summary.path("failure").isMissingNode()))
            cov.add("its summary did not hold: " + summary.path("summary").asText(""));
        String pinned;
        try {
            Path scenarios = q.root().resolve("fixtures").resolve("scenarios");
            JsonNode lock = Json.read(lockFile == null ? ScenarioBuilder.lockOf(scenarios.resolve(scenario + ".json")) : scenarios.resolve(lockFile));
            pinned = lock.path("files").path("manifest.json").asText(null);
        } catch (RuntimeException e) { pinned = null; cov.add("cannot read the committed lock of " + scenario + ": " + e.getMessage()); }
        if (pinned != null && (!pinned.equals(summary.path("recordingManifestSha256").asText(null)) || !pinned.equals(entry.get("recordingManifestSha256").textValue())))
            cov.add("its recording manifest " + summary.path("recordingManifestSha256") + " is not the one its committed lock pins, " + pinned);
        for (String f : List.of("plannedCutIds", "cutLogIds", "comparedCutIds")) {
            List<Long> l = new ArrayList<>();
            summary.path(f).forEach(x -> l.add(x.asLong()));
            if (!summary.path(f).isArray() || !l.equals(cutIds)) cov.add("its summary's " + f + " are not the cut ids of its cut log");
        }
        ObjectNode pins = rr.path("pins").isObject() ? ((ObjectNode) rr.path("pins")).deepCopy() : Json.object();
        pins.remove("job");
        if (!Canonical.digest(pins).equals(reg.get().row().rulesDigest())) cov.add("the run record's rule pins are not the registered rulesDigest");
        if (!reg.get().row().releaseEvidence().path("releaseSha256").asText("").equals(rr.path("fixtureDigests").path("releaseSha256").asText(null)))
            cov.add("the run record's release " + rr.path("fixtureDigests").path("releaseSha256") + " is not the pinned release");
        if (!GateBundle.same(rr.path("pins").path("job").path("routingDigest"), cutLog.path("routingDigest"))) cov.add("the run record's routing is not its cut log's");
        if (!GateBundle.same(rr.path("artifactSha256"), report.get("pins").path("forseti").path("artifactSha256"))) cov.add("the run record's artifactSha256 is not the report's");
        Set<String> subscribed = new TreeSet<>(), logged = new TreeSet<>();
        rr.path("subscribed").fields().forEachRemaining(e -> e.getValue().forEach(x -> subscribed.add(e.getKey() + "/" + x.asLong())));
        for (JsonNode c : cutLog.path("cuts")) c.path("offsetRanges").fields().forEachRemaining(t -> t.getValue().fieldNames().forEachRemaining(p -> logged.add(t.getKey() + "/" + Long.parseLong(p))));
        if (!subscribed.equals(logged)) cov.add("the run record subscribes " + subscribed + ", its cut log covers " + logged);
        if (!cov.isEmpty()) throw refused(COVERAGE, cov);

        // 13. scope
        String scope = report.get("scope").textValue();
        if (!("semantic".equals(scope) || ("core".equals(scope) && q.acceptCore())))
            throw refused(SCOPE, "the report's scope is " + scope + ("core".equals(scope) ? "; a core-scope report is accepted only with --accept-scope core" : ""));

        ObjectNode citation = Json.object().put("runId", entry.get("runId").textValue()).put("runRecordSha256", runSha);
        citation.set("offsetRanges", rr.get("offsetRanges").deepCopy());
        return new Accepted(reportSha, citation, target, entry);
    }

    // --- helpers -----------------------------------------------------------------------------------------------------------------

    private static ReferenceEngine.Refused refused(String code, String detail) { return refused(code, List.of(detail)); }

    private static ReferenceEngine.Refused refused(String code, List<String> problems) {
        ObjectNode ev = Json.object();
        ArrayNode a = ev.putArray("problems");
        problems.forEach(a::add);
        return new ReferenceEngine.Refused(code, ev);
    }

    static boolean hex(JsonNode n) { return n != null && n.isTextual() && n.textValue().matches("[0-9a-f]{64}"); }

    private static final String TEXT = "text", NULL_TEXT = "null|text", INT = "integer", BOOL = "boolean", OBJ = "object", ARR = "array";

    /** The required fields of a report and of its entries, each with its type. */
    static List<String> shape(JsonNode r) {
        List<String> p = new ArrayList<>();
        if (r == null || !r.isObject()) return List.of("the report is not a JSON object");
        fields(r, "report", p, "exit", INT, "kind", TEXT, "layout", TEXT, "negativeControl", NULL_TEXT, "scope", TEXT, "manifestSha256", NULL_TEXT,
            "assertionsExpected", INT, "assertionsRun", INT, "failed", ARR, "notRun", ARR, "intendedFailures", ARR, "pins", OBJ, "seed", INT,
            "fixtureDigests", OBJ, "error", NULL_TEXT, "shard", NULL_TEXT, "shardPlanSha256", NULL_TEXT, "summariesSha256", NULL_TEXT,
            "results", ARR, "shards", ARR, "generations", ARR, "runs", ARR);
        if (!p.isEmpty()) return p;
        if (r.get("manifestSha256").isNull()) p.add("report.manifestSha256 is null");
        for (String f : List.of("failed", "notRun", "intendedFailures")) for (JsonNode x : r.get(f)) if (!x.isTextual()) p.add("report." + f + " holds " + x);
        int i = 0;
        for (JsonNode x : r.get("results")) fields(x, "results[" + i++ + "]", p, "id", TEXT, "ok", BOOL, "evidence", TEXT, "shard", NULL_TEXT);
        i = 0;
        for (JsonNode x : r.get("shards")) fields(x, "shards[" + i++ + "]", p, "name", TEXT, "exit", INT, "reportSha256", TEXT, "summariesSha256", TEXT);
        i = 0;
        for (JsonNode x : r.get("generations"))
            fields(x, "generations[" + i++ + "]", p, "shard", NULL_TEXT, "store", TEXT, "generation", INT, "scope", TEXT, "rulesDigest", TEXT,
                "registeredRowSha256", TEXT, "state", TEXT, "scenario", NULL_TEXT, "variant", TEXT, "assertion", NULL_TEXT);
        i = 0;
        for (JsonNode x : r.get("runs")) {
            String w = "runs[" + i++ + "]";
            fields(x, w, p, "shard", NULL_TEXT, "store", TEXT, "scenario", TEXT, "variant", TEXT, "seed", INT, "shuffle", BOOL, "runId", TEXT, "generation", INT,
                "registeredRowSha256", TEXT, "state", TEXT, "runRecord", TEXT, "runRecordSha256", TEXT, "cutLog", TEXT, "cutLogSha256", TEXT,
                "recordingManifestSha256", TEXT, "originalRecordingManifestSha256", NULL_TEXT, "offsetRanges", OBJ);
            if (x.isObject()) fields(x, w, p, "journals", NULL_TEXT, "journalsIndexSha256", NULL_TEXT);
        }
        return p;
    }

    private static void fields(JsonNode n, String where, List<String> p, String... spec) {
        if (n == null || !n.isObject()) { p.add(where + " is not an object"); return; }
        for (int k = 0; k < spec.length; k += 2) {
            JsonNode v = n.get(spec[k]);
            boolean ok = v != null && switch (spec[k + 1]) {
                case TEXT -> v.isTextual();
                case NULL_TEXT -> v.isNull() || v.isTextual();
                case INT -> v.isIntegralNumber();
                case BOOL -> v.isBoolean();
                case OBJ -> v.isObject();
                default -> v.isArray();
            };
            if (!ok) p.add(where + "." + spec[k] + (v == null ? " is missing" : " is not " + spec[k + 1]));
        }
    }

    /** The names of the {@code generationRuns} summaries of a {@code summaries.json}: an object keyed by generation-run name, each
     *  summary's {@code name} its key. Anything else is a problem in {@code p}. */
    static List<String> generationRunKeys(JsonNode summaries, List<String> p) {
        List<String> l = new ArrayList<>();
        JsonNode g = summaries.get("generationRuns");
        if (g == null || !g.isObject()) { p.add(GateBundle.SUMMARIES + " has no generationRuns object keyed by name: " + g); return l; }
        g.fields().forEachRemaining(e -> {
            l.add(e.getKey());
            if (!e.getKey().equals(e.getValue().path("name").asText(null))) p.add("generation-run summary " + e.getKey() + " is named " + e.getValue().path("name"));
        });
        return l;
    }

    private static void keys(JsonNode array, String field, List<String> want, String what, List<String> p) {
        List<String> got = new ArrayList<>();
        array.forEach(x -> got.add(x.path(field).asText()));
        sameSet(got, want, what, p);
    }

    /** {@code got} is {@code want} as a set, each once: none missing, none extra, none twice. */
    private static void sameSet(List<String> got, List<String> want, String what, List<String> p) {
        Set<String> g = new TreeSet<>(got), w = new TreeSet<>(want);
        if (g.size() != got.size()) p.add(what + " name one key twice: " + got);
        if (!g.equals(w)) {
            Set<String> missing = new TreeSet<>(w), extra = new TreeSet<>(g);
            missing.removeAll(g);
            extra.removeAll(w);
            p.add(what + " are not the ones a full run of the committed plan makes: missing " + missing + ", extra " + extra);
        }
    }

    /** Every path a run entry names ({@code runRecord}, {@code cutLog}, {@code journals}) is relative, has no {@code ..} and resolves
     *  inside {@code bundle}. */
    static void inside(Path bundle, JsonNode r, List<String> p) {
        for (String f : List.of("runRecord", "cutLog", "journals")) {
            JsonNode v = r.get(f);
            if (v == null || v.isNull()) { if (!f.equals("journals")) p.add("run " + r.get("runId").textValue() + ": no " + f); continue; }
            if (!relativeInside(bundle, v.textValue())) p.add("run " + r.get("runId").textValue() + ": " + f + " " + v + " is not a relative path inside its bundle");
        }
    }

    static boolean relativeInside(Path bundle, String rel) {
        if (rel == null || rel.isEmpty() || rel.startsWith("/") || rel.contains("\\") || rel.contains(":") || Arrays.asList(rel.split("/")).contains("..")) return false;
        Path b = bundle.toAbsolutePath().normalize();
        return b.resolve(rel).normalize().startsWith(b);
    }

    /** When the entry names kept journals: the index {@code <journals>.index.json} has {@code journalsIndexSha256}, every file it lists
     *  ({@code {relative file: raw SHA-256}}, relative to the journals directory, inside it) has its digest, and no other file is there. */
    static void journals(Path bundle, JsonNode entry, List<String> p) {
        JsonNode j = entry.get("journals"), d = entry.get("journalsIndexSha256");
        if (j == null || j.isNull()) {
            if (d != null && !d.isNull()) p.add("journalsIndexSha256 without journals");
            return;
        }
        String rel = j.textValue().endsWith("/") ? j.textValue().substring(0, j.textValue().length() - 1) : j.textValue();
        if (!relativeInside(bundle, rel)) { p.add("journals " + j + " is not inside the bundle"); return; }
        Path dir = bundle.resolve(rel), index = bundle.resolve(rel + ".index.json");
        byte[] ib;
        try { ib = Files.readAllBytes(index); } catch (IOException e) { p.add("no journal index " + rel + ".index.json"); return; }
        if (d == null || !Canonical.digestOf(ib).equals(d.textValue())) { p.add("the journal index has another digest than journalsIndexSha256"); return; }
        JsonNode idx;
        try { idx = Json.read(ib); } catch (IllegalArgumentException e) { p.add("the journal index is not JSON"); return; }
        if (!idx.isObject()) { p.add("the journal index is not an object"); return; }
        Map<String, String> have;
        try { have = Files.isDirectory(dir) ? GateBundle.digests(dir) : Map.of(); } catch (IOException x) { p.add("journals " + rel + ": " + x.getMessage()); return; }
        idx.fields().forEachRemaining(e -> {
            if (!relativeInside(dir, e.getKey())) { p.add("journal " + e.getKey() + " is not inside " + rel); return; }
            if (!have.containsKey(e.getKey())) p.add("journal " + e.getKey() + " is missing");
            else if (!have.get(e.getKey()).equals(e.getValue().asText(null))) p.add("journal " + e.getKey() + " changed");
        });
        for (String f : have.keySet()) if (!idx.has(f)) p.add("journal " + f + " is not in the journal index");
    }

    /** The real path of {@code p}, null when it does not exist (8.3 short names and links resolved on both sides of a comparison). */
    static Path real(Path p) {
        try { return p.toAbsolutePath().toRealPath(); } catch (IOException e) { return null; }
    }
}
