package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.release.FloorStore;
import yggdrasil.forseti.core.release.HeadVerifier;
import yggdrasil.forseti.core.release.ReleaseReader;
import yggdrasil.forseti.core.release.TrustedKeys;
import yggdrasil.forseti.core.rules.RuleSet;

/** What the replay tests of the recording package read from the gate: the rule set a recording names (head verified as the gate verifies
 *  it), {@link Injections#afterHorizonNeed}, and the summary {@link Summaries#of(Scenarios.Run)} makes of a replay run, with the per-run
 *  check of {@code J-AFTER-HORIZON-CUTS}. */
public final class ReplayRuns {
    private ReplayRuns() {}

    public record Need(long at, String station, String what) {}

    /** The rule set {@code recording}'s manifest names, from the fixture registry (head verified at its signedAt + 60 s). */
    public static RuleSet rules(Path fixtures, Path recording, Path floors) throws IOException {
        Path m = fixtures.resolve("registry").resolve("mappings");
        JsonNode rule = Json.read(recording.resolve(Recording.MANIFEST)).get("rule");
        HeadVerifier.Head h = HeadVerifier.verify(m, Json.text(rule, "stream"), TrustedKeys.load(fixtures.resolve("trusted-keys.jsonl")),
            new FloorStore(Files.createDirectories(floors)), GateSigner.now(m, Json.text(rule, "stream")));
        return RuleSet.load(ReleaseReader.read(m, h), Json.text(rule, "attributionRuleId"), Json.text(rule, "attributionRuleVersion"),
            Json.read(recording.resolve(Recording.MANIFEST)).get("definitions"));
    }

    public static Need need(JsonNode expected, RuleSet rules) {
        Injections.Need n = Injections.afterHorizonNeed(expected, rules.attribution().horizonMs(), rules.attribution().allowedLatenessMs());
        return new Need(n.at(), n.station(), n.what());
    }

    /** The run summary of a run on {@code recording} (a replay recording) whose cut log is {@code cutLog}, folded from {@code journals}
     *  (a run directory with {@code journals/}), compared at every cut of the cut log, against {@code expected}. */
    public static ObjectNode summary(String name, Path recording, JsonNode expected, JsonNode cutLog, Path journals, String rulesDigest, RuleSet rules) throws IOException {
        Scenarios.Run r = new Scenarios.Run();
        r.name = name;
        r.synthetic = true;
        r.recording = recording;
        r.manifest = Json.read(recording.resolve(Recording.MANIFEST));
        r.expected = expected;
        r.cutLog = cutLog;
        r.rules = rules;
        r.rulesDigest = rulesDigest;
        r.lockLine = "replay recording";
        r.tally = new Comparison.Tally();
        try (RunFold f = new RunFold(journals, rulesDigest, 1, false)) {
            r.fold = f;
            for (long k : r.cutIds()) { f.advance(k); r.tally.cuts++; r.tally.comparedIds.add(k); }
            f.finish();
            r.outcome = Expectations.check(f, r.expected, r::perStation);
            return Summaries.of(r);
        }
    }

    /** {@code J-AFTER-HORIZON-CUTS} on one run summary: its problems, empty when it holds. */
    public static List<String> afterHorizonProblems(String name, JsonNode summary) {
        List<String> problems = new ArrayList<>();
        Injections.afterHorizon(name, summary, problems);
        if (!summary.path("ok").asBoolean(false)) problems.add(name + ": the summary is not ok: " + summary.path("summary").asText());
        return problems;
    }

    /** A cut log {@code {cuts: [{cutId, perStation {s: {W1, onlineVisits}}}]}} carrying {@code w1} (per station, in cut order), the online
     *  visits of a station at a cut being the visits of {@code expected} at it whose enter is at or below W1 and whose online expiry
     *  ({@code leave + H + L}, an open visit {@code enter + H + L}) lies above it. */
    public static ObjectNode cutLog(List<Long> cutIds, Map<String, List<Long>> w1, JsonNode expected, RuleSet rules) {
        long hl = rules.attribution().horizonMs() + rules.attribution().allowedLatenessMs();
        ObjectNode log = Json.object();
        ArrayNode cuts = log.putArray("cuts");
        for (int k = 0; k < cutIds.size(); k++) {
            ObjectNode c = Json.putSafe(Json.object(), "cutId", cutIds.get(k));
            ObjectNode per = c.putObject("perStation");
            for (Map.Entry<String, List<Long>> e : w1.entrySet()) {
                long w = e.getValue().get(k), online = 0;
                for (JsonNode v : expected.path("visits")) {
                    if (!v.path("station").asText().equals(e.getKey())) continue;
                    long enter = v.path("enter").asLong(), expiry = (v.path("leave").isNull() ? enter : v.path("leave").asLong()) + hl;
                    if (enter <= w && w < expiry) online++;
                }
                Json.putSafe(Json.putSafe(per.putObject(e.getKey()), "W1", w), "onlineVisits", online);
            }
            cuts.add(c);
        }
        return log;
    }

    /** A run directory whose {@code forseti.work-records/0} journal holds one row per visit of {@code expected} and the progress of every
     *  cut of {@code cutLog} at {@code station}'s W1. Returns the fixture rules digest its rows pin. */
    public static String journals(Path run, JsonNode expected, JsonNode cutLog, String station) throws IOException {
        Journals j = new Journals(run);
        long offset = 0;
        for (JsonNode v : expected.path("visits")) j.row(RunFold.WR, 0, v.path("key"), Journals.body("complete"), 1, offset++);
        for (JsonNode c : cutLog.path("cuts")) j.progress(RunFold.WR, 0, c.path("cutId").asLong(), c.path("perStation").path(station).path("W1").asLong(), offset);
        return Journals.RULES_DIGEST;
    }
}
