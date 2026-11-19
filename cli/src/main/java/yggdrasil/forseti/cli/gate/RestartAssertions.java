package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.CoreGate.Result;
import yggdrasil.forseti.cli.gate.Injections.P;
import yggdrasil.forseti.cli.gate.Scenarios.Run;
import yggdrasil.forseti.cli.replay.Restarts;
import yggdrasil.forseti.core.Json;

/**
 * The restart injections of the replay gate (spec 678-679, 685): {@code I-21} and {@code I-22}, each on its restart runs
 * ({@code <scenario>@restart-<kind>}, {@link Restarts}) beside the scenario's uninterrupted run in the same shard. A restart run holds
 * when it ran to its last cut on a valid attempt, was compared with the oracle at every planned cut and matched it with clean integrity
 * (no duplicate, loss or revision gap) and its ground truth; its injection manifest records what spec 685 asks (the jobs restarted,
 * {@code epochBefore}, {@code targetEpoch}, {@code lastWrittenCutId}, the declaration, the launches with the restored checkpoint, the
 * expected and the attributed sets, the writers whose {@code initEvidence} points to the declaration) and, per kind, the kill's own
 * evidence (22(i): the D8 transaction protocol and the post-restart outcome; 22(ii): stage 1 untouched, no epoch moved; 22(iv): the chained
 * second kill, the first injection's attributed set empty); its per-run restart assertions (719-723, 725, 726) found nothing; and its
 * committed outputs equal the uninterrupted run's at every cut both completed at every station and that is comparable (spec 609; reviewer
 * decision 7 of PR 5), the last cut among them. Injection 22(iii) is recorded as not applicable to Flink (spec 685), never as a skip.
 */
final class RestartAssertions {
    private RestartAssertions() {}

    /** The scenario the restart injections run on (spec 678: 21 during injection 13's scenario). */
    static final String SCENARIO = "stage2-early";
    static final List<String> PER_RUN = List.of("R-MERGED-EXACT", "R-MERGED-JUSTIFIED", "R-RECOVERY-WINDOW", "R-RECOVERED-EPOCH", "R-INIT-EVIDENCE", "R-BARRIER-REFS",
        "C-3-RESTART-SKIPS");

    static String run(Restarts.Kind k) { return SCENARIO + "@" + k.suffix; }

    static Result i21(Context c) throws Exception { return check(c, List.of(Restarts.Kind.I21), ""); }

    static Result i22(Context c) throws Exception {
        c.measure("injection22iii", Json.object().put("status", "not-applicable").put("why", Restarts.NOT_APPLICABLE_22III));
        return check(c, List.of(Restarts.Kind.I22_I, Restarts.Kind.I22_II, Restarts.Kind.I22_IV), Restarts.NOT_APPLICABLE_22III + "; ");
    }

    static Result check(Context c, List<Restarts.Kind> kinds, String prefix) throws Exception {
        Run base = Scenarios.get(c, SCENARIO);
        List<Run> runs = new ArrayList<>();
        for (Restarts.Kind k : kinds) runs.add(Scenarios.get(c, run(k)));
        P p = new P(base);
        for (Run r : runs) p.use(r);
        if (!p.sound()) return p.result(prefix);
        List<String> out = new ArrayList<>();
        ObjectNode m = Json.object();
        for (int i = 0; i < kinds.size(); i++) out.add(one(p, base, runs.get(i), kinds.get(i), m));
        c.measure("restarts-" + kinds.get(0).spec().substring(0, 2), m);
        return p.result(prefix + String.join("; ", out));
    }

    static String one(P p, Run base, Run r, Restarts.Kind kind, ObjectNode measures) {
        String n = r.label();
        JsonNode mf = r.restart;
        if (mf == null) { p.list.add(n + ": no injection manifest"); return n + ": no manifest"; }
        p.equal("valid", mf.path("status").asText(), n + " status");
        p.equal(kind.spec(), mf.path("kind").asText(), n + " kind");
        List<Long> planned = new ArrayList<>();
        for (JsonNode b : r.manifest.path("batches")) planned.add(b.path("cutId").asLong());
        p.equal(planned, r.tally.comparedIds, n + ": cuts compared with the oracle");
        JsonNode inj = mf.path("injections");
        p.equal(kind.injectionIds.size(), inj.size(), n + " injections");
        long k = mf.path("placedAt").asLong();
        List<String> stations = new ArrayList<>();
        mf.path("stationWriters").fieldNames().forEachRemaining(stations::add);
        List<String> writers = Restarts.stage1Writers(stations);
        for (int i = 0; i < inj.size(); i++) {
            JsonNode x = inj.get(i);
            String id = x.path("injectionId").asText();
            p.equal(kind.injectionIds.get(i), id, n + " injection " + i);
            p.equal(k, x.path("lastWrittenCutId").asLong(), n + " " + id + " lastWrittenCutId");
            p.equal(k + 1, x.path("firstCutAtTarget").asLong(), n + " " + id + " first cut at its epoch");
            if (kind.stage1) {
                p.equal((long) i, x.path("epochBefore").asLong(-1), n + " " + id + " epochBefore");
                p.equal((long) i + 1, x.path("targetEpoch").asLong(-1), n + " " + id + " targetEpoch");
                p.require(x.path("declaration").path("offset").isIntegralNumber(), n + " " + id + ": no committed declaration before the kill");
                JsonNode l1 = x.path("launches").path("stage1");
                p.require(l1.path("restoredCheckpointId").asLong(0) > 0, n + " " + id + ": stage 1 restored no checkpoint (" + l1 + ")");
                p.require(l1.path("launch").asInt(-1) == l1.path("launchBefore").asInt(-2) + 1, n + " " + id + ": the launch identity did not move (" + l1 + ")");
                p.equal(writers, texts(x.path("expected")), n + " " + id + " expected reinitialized set (every stage-1 progress writer)");
            }
            if (kind == Restarts.Kind.I21) {
                p.equal(List.of("stage1", "stage2"), texts(x.path("jobsRestarted")), n + " jobs restarted");
                p.require(x.path("launches").path("stage2").path("restoredCheckpointId").asLong(0) > 0, n + ": stage 2 restored no checkpoint");
            }
            if ((kind == Restarts.Kind.I22_I || kind == Restarts.Kind.I22_IV) && i == 0 && !mf.path("theta").asBoolean()) {
                JsonNode d8 = x.path("d8");
                p.require(d8.path("phase1").path("openRecords").asBoolean(false) && isOngoing(d8.path("phase1")), n + ": D8 phase 1 not an Ongoing transaction with open records: " + d8.path("phase1"));
                p.require(isOngoing(d8.path("phase2")) && d8.path("phase2").path("producerEpoch").equals(d8.path("phase1").path("producerEpoch"))
                    && d8.path("phase2").path("producerId").equals(d8.path("phase1").path("producerId")), n + ": D8 phase 2 not the same Ongoing transaction: " + d8.path("phase2"));
                String outcome = x.path("outcome").path("outcome").asText("");
                p.require(outcome.equals("aborted") || outcome.equals("committed by the restored committer"), n + ": post-restart outcome " + outcome);
            }
            if (kind == Restarts.Kind.I22_II) {
                p.equal(List.of("stage2"), texts(x.path("jobsRestarted")), n + " jobs restarted");
                p.require(x.path("declaration").isNull(), n + ": a stage-2 restart wrote a declaration");
                p.require(x.path("launches").path("stage2").path("restoredCheckpointId").asLong(0) > 0, n + ": stage 2 restored no checkpoint");
                p.equal(List.of(), texts(x.path("expected")), n + " expected reinitialized stage-1 writers");
            }
        }
        if (kind == Restarts.Kind.I22_IV && inj.size() == 2) {
            p.equal(List.of(), texts(inj.get(0).path("attributed")), n + " the first injection's attributed set (spec 416: empty for Flink)");
            p.equal(writers, texts(inj.get(1).path("attributed")), n + " the second injection's attributed set");
        } else if (kind.stage1 && inj.size() == 1) p.equal(writers, texts(inj.get(0).path("attributed")), n + " attributed set");
        // the epochs of the cuts: 0 through k, the last injection's after it; a stage-2 restart moves none
        long last = kind.stage1 ? kind.injectionIds.size() : 0;
        String lastRef = kind.stage1 ? kind.injectionIds.get(kind.injectionIds.size() - 1) : null;
        for (JsonNode ce : mf.path("cutEpochs")) {
            long cut = ce.path("cutId").asLong(), want = cut <= k ? 0 : last;
            String ref = cut <= k ? null : lastRef;
            if (ce.path("restartEpoch").asLong(-1) != want || !Objects.equals(ref, RestartChecks.text(ce.get("recoveryRef"))))
                p.list.add(n + " cut " + cut + ": barriers of epoch " + ce.path("restartEpoch") + "/" + ce.get("recoveryRef") + ", " + want + "/" + ref + " expected");
        }
        // the per-run restart assertions of this run
        for (String id : PER_RUN) {
            JsonNode f = r.replay == null ? null : r.replay.get(id);
            if (f == null) p.list.add(n + ": " + id + " not checked");
            else if (f.path("count").asLong() > 0) p.list.add(n + " " + id + ": " + f.path("describe").asText());
        }
        // restore equivalence with the uninterrupted run (reviewer decision 7): every cut both completed at every station, compared
        // directly where comparable, the last one required
        Set<Long> complete = new TreeSet<>();
        for (JsonNode x : r.accounting.path("completedCutIds")) complete.add(x.asLong());
        int equalCuts = 0;
        List<String> notComparable = new ArrayList<>();
        List<Long> cuts = r.cutIds();
        for (long cut : cuts) {
            if (!complete.contains(cut) || base.perStation(cut).size() != r.perStation(cut).size()) continue;
            List<Long> deadlines = new ArrayList<>(r.deadlines());
            String why = Comparison.notComparable(r.side(cut), base.side(cut), deadlines);
            if (why != null) { notComparable.add(cut + ": " + why); continue; }
            int before = p.list.size();
            Injections.direct(p, r, cut, base, cut, List.of(RunFold.WR, RunFold.SAMPLES, RunFold.AFFECTED, RunFold.SETS), Injections.partitions(r, RunFold.WR));
            if (p.list.size() == before) equalCuts++;
        }
        long lastCut = r.lastCut();
        if (!complete.contains(lastCut)) p.list.add(n + ": the last cut " + lastCut + " is not complete");
        else if (notComparable.stream().anyMatch(s -> s.startsWith(lastCut + ":"))) p.list.add(n + ": the last cut is not comparable with the uninterrupted run");
        JsonNode acc = r.accounting;
        ObjectNode mm = measures.putObject(kind.spec());
        Json.putSafe(mm, "placedAt", k);
        Json.putSafe(mm, "attempt", mf.path("attempt").asLong());
        Json.putSafe(mm, "invalidAttempts", mf.path("invalidAttempts").size());
        Json.putSafe(mm, "item3RestartSkips", acc.path("item3").asLong());
        Json.putSafe(mm, "item3RejectCollectorSkips", acc.path("item3RejectCollectors").asLong());
        Json.putSafe(mm, "completedCuts", complete.size());
        Json.putSafe(mm, "equalToUninterruptedCuts", equalCuts);
        ArrayNode rs = mm.putArray("restartMs");
        for (JsonNode x : inj) x.path("launches").fields().forEachRemaining(e -> rs.add(e.getKey() + " " + e.getValue().path("record").path("runningAfterLaunchMs").asLong(-1)));
        return n + ": " + kind.spec() + " at cut " + k + " (attempt " + mf.path("attempt").asLong() + ", " + mf.path("invalidAttempts").size() + " invalid before)"
            + ", restored checkpoints " + restored(inj) + ", restart skips " + acc.path("item3").asLong() + " (reject collectors " + acc.path("item3RejectCollectors").asLong() + ")"
            + ", " + complete.size() + " of " + cuts.size() + " cuts complete, equal to the uninterrupted run at " + equalCuts + " comparable complete cuts"
            + (notComparable.isEmpty() ? "" : " (" + notComparable.size() + " not comparable)")
            + (kind == Restarts.Kind.I22_I || kind == Restarts.Kind.I22_IV ? ", D8 outcome " + inj.path(0).path("outcome").path("outcome").asText(mf.path("theta").asBoolean() ? "n/a under theta" : "none") : "");
    }

    static boolean isOngoing(JsonNode d) { String s = d.path("state").asText(""); return s.equalsIgnoreCase("Ongoing"); }

    static String restored(JsonNode inj) {
        List<String> l = new ArrayList<>();
        for (JsonNode x : inj) x.path("launches").fields().forEachRemaining(e -> l.add(e.getKey() + " chk-" + e.getValue().path("restoredCheckpointId").asLong()));
        return l.toString();
    }

    static List<String> texts(JsonNode a) { List<String> l = new ArrayList<>(); a.forEach(x -> l.add(x.asText())); return l; }
}
