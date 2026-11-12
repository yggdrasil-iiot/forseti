package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.Scenarios.Run;
import yggdrasil.forseti.cli.gate.Scenarios.Shuffled;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** What the assertions over every scenario run ({@code J-*}, {@code C-2-REFERENCE}) read: one summary per scenario run and per
 *  {@code --shuffle} run, made where the run was compared with the oracle. A gate run that makes every scenario takes them from its own
 *  pipeline; a shard run ({@code --shard}) writes the summaries of its scenarios to {@code summaries.json}, and the aggregate run
 *  ({@code --aggregate}) reads them back from every shard ({@link Shards}), so these assertions are evaluated on the same values either way.
 *
 *  <p>A run summary: {@code name, synthetic, failure, ok, summary} (the evidence text of {@link Run#summary()}), {@code plannedCutIds}
 *  (the recording's batches) and {@code recordingManifestSha256} (raw-byte digest of its {@code manifest.json}, pinned by the scenario
 *  lock), {@code cutLogCuts}/{@code cutLogIds} (cuts the harness completed), {@code cutsCompared}/{@code comparedCutIds} (cuts compared
 *  with the oracle, in order), {@code equal}, {@code findings {check: {count,
 *  describe}}}, {@code rows {topic: records}}, {@code measures}, {@code ms}, the rule's {@code horizonMs, allowedLatenessMs,
 *  outOfOrdernessMs}, {@code heldBackWantMs} ({@link Injections#heldBackLateness}) and, for a horizon recording, {@code afterHorizon}
 *  (what {@code J-AFTER-HORIZON-CUTS} checks). */
final class Summaries {
    /** Scenario name -> run summary, in {@link Scenarios#all()} order. */
    final Map<String, JsonNode> runs = new LinkedHashMap<>();
    /** {@code name@seed} -> shuffled-run summary. */
    final Map<String, JsonNode> shuffled = new LinkedHashMap<>();
    /** One line per scenario or shuffled run this gate run does not have, with the reason. */
    final List<String> missing = new ArrayList<>();

    /** The summaries this gate run evaluates its aggregate assertions on: the shards' in an aggregate run, else its own pipeline's. */
    static Summaries of(Context c) throws Exception {
        if (c.aggregate != null) return c.aggregate;
        return c.memo("scenario-summaries", () -> fromPipeline(c));
    }

    /** Every scenario and shuffled run of this gate run's pipeline (waiting for them), the others listed in {@link #missing}. */
    static Summaries fromPipeline(Context c) throws Exception {
        Summaries s = new Summaries();
        for (String name : Scenarios.all()) {
            String why = Scenarios.notRun(c, name);
            if (why != null) { s.missing.add(name + " " + why); continue; }
            s.runs.put(name, of(Scenarios.get(c, name)));
        }
        for (String name : Scenarios.SHUFFLED) for (long seed : Scenarios.SHUFFLE_SEEDS) {
            if (!Scenarios.shuffles(c, name)) { s.missing.add(name + " seed " + seed + " not run in this gate run"); continue; }
            s.shuffled.put(name + "@" + seed, of(Scenarios.pipeline(c).shuffled(name, seed)));
        }
        return s;
    }

    static ObjectNode of(Run r) {
        ObjectNode o = Json.object();
        o.put("name", r.name).put("synthetic", r.synthetic).put("failure", r.failure);
        boolean complete = r.failure == null && r.tally != null && r.fold != null && r.outcome != null;
        o.put("ok", complete && r.ok());
        String text;
        try { text = r.summary(); } catch (RuntimeException e) { text = r.name + ": " + (r.failure != null ? r.failure : "incomplete: " + e); }
        o.put("summary", text);
        if (r.manifest != null) {
            ArrayNode planned = o.putArray("plannedCutIds");
            for (JsonNode b : r.manifest.path("batches")) planned.add(b.path("cutId").asLong());
        }
        if (r.recording != null) {
            try { o.put("recordingManifestSha256", Canonical.digestOf(Files.readAllBytes(r.recording.resolve("manifest.json")))); }
            catch (IOException e) { o.putNull("recordingManifestSha256"); }
        }
        if (r.cutLog != null) {
            Json.putSafe(o, "cutLogCuts", r.cutLog.path("cuts").size());
            ArrayNode ids = o.putArray("cutLogIds");
            for (JsonNode c : r.cutLog.path("cuts")) ids.add(c.path("cutId").asLong());
        }
        if (r.tally != null) {
            Json.putSafe(o, "cutsCompared", r.tally.cuts);
            ArrayNode ids = o.putArray("comparedCutIds");
            r.tally.comparedIds.forEach(ids::add);
            o.put("equal", r.tally.equal());
        }
        if (r.fold != null) {
            o.set("findings", findings(r.fold.findings));
            ObjectNode rows = o.putObject("rows");
            r.fold.rows().forEach((k, v) -> Json.putSafe(rows, k, v));
        }
        if (r.measures != null) {
            ObjectNode m = o.putObject("measures");
            Json.putSafe(m, "longestLatenessMs", r.measures.longestLatenessMs());
            m.put("longestAt", r.measures.longestAt());
            Json.putSafe(m, "acceptedLateSamples", r.measures.acceptedLateSamples());
            Json.putSafe(m, "inputsWithoutDisposition", r.measures.inputsWithoutDisposition());
            Json.putSafe(m, "deliveries", r.measures.deliveries());
        }
        ObjectNode ms = o.putObject("ms");
        r.ms.forEach((k, v) -> Json.putSafe(ms, k, v));
        if (r.rules != null) {
            var a = r.rules.attribution();
            Json.putSafe(o, "horizonMs", a.horizonMs());
            Json.putSafe(o, "allowedLatenessMs", a.allowedLatenessMs());
            Json.putSafe(o, "outOfOrdernessMs", a.outOfOrdernessMs());
            Long want = r.manifest == null ? null : Injections.heldBackLateness(r.manifest, a.outOfOrdernessMs());
            if (want == null) o.putNull("heldBackWantMs"); else Json.putSafe(o, "heldBackWantMs", want);
        }
        if (r.synthetic && complete) o.set("afterHorizon", afterHorizon(r));
        return o;
    }

    /** {@code J-AFTER-HORIZON-CUTS}' values of a horizon recording, from its fold at the last cut. */
    static ObjectNode afterHorizon(Run r) {
        Injections.Need n = Injections.afterHorizonNeed(r.expected, r.rules.attribution().horizonMs(), r.rules.attribution().allowedLatenessMs());
        ObjectNode a = Json.object();
        Json.putSafe(a, "t0", r.t0());
        Json.putSafe(a, "need", n.at());
        a.put("station", n.station()).put("what", n.what());
        Json.putSafe(a, "lastW1", r.w1(r.lastCut(), n.station()));
        long maxOnline = 0;
        for (long k : r.cutIds()) maxOnline = Math.max(maxOnline, r.perStation(k).path(n.station()).path("onlineVisits").asLong());
        Json.putSafe(a, "maxOnlineVisits", maxOnline);
        Json.putSafe(a, "lastOnlineVisits", r.perStation(r.lastCut()).path(n.station()).path("onlineVisits").asLong(-1));
        Json.putSafe(a, "workRecords", r.fold.live(RunFold.WR).size());
        Json.putSafe(a, "visits", r.expected.path("visits").size());
        return a;
    }

    static ObjectNode of(Shuffled x) {
        ObjectNode o = Json.object();
        o.put("name", x.name());
        Json.putSafe(o, "seed", x.seed());
        o.put("failure", x.failure());
        if (x.findings() != null) o.set("findings", findings(x.findings()));
        if (x.rows() != null) { ObjectNode rows = o.putObject("rows"); x.rows().forEach((k, v) -> Json.putSafe(rows, k, v)); }
        return o;
    }

    static ObjectNode findings(RunFold.Findings f) {
        ObjectNode o = Json.object();
        for (String check : f.checks()) {
            ObjectNode e = o.putObject(check);
            Json.putSafe(e, "count", f.count(check));
            e.put("describe", f.describe(check));
        }
        return o;
    }

    /** {@code summaries.json}: {@code {runs: [...], shuffled: [...]}}. */
    ObjectNode toJson() {
        ObjectNode o = Json.object();
        ArrayNode a = o.putArray("runs"), b = o.putArray("shuffled");
        runs.values().forEach(a::add);
        shuffled.values().forEach(b::add);
        return o;
    }

    // --- what the aggregate assertions read ----------------------------------------------------------------------------------

    static boolean failed(JsonNode run) { return !run.path("failure").isNull() && !run.path("failure").isMissingNode(); }
    static long findingCount(JsonNode run, String check) { return run.path("findings").path(check).path("count").asLong(0); }
    static String findingText(JsonNode run, String check) { return run.path("findings").path(check).path("describe").asText(""); }
    static long rowSum(JsonNode run) {
        long n = 0;
        for (JsonNode v : run.path("rows")) n += v.asLong();
        return n;
    }
}
