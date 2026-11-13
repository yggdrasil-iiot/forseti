package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.cli.gate.CoreGate.Check;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.CoreGate.Result;
import yggdrasil.forseti.cli.gate.Foundation.Problems;
import yggdrasil.forseti.cli.gate.Stage1Assertions.Run;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.*;

/** The stage-2 harness assertions of pull request 3 ({@code T-*}): {@code forseti run --engine core} on the committed
 *  {@code smoke-incident} recording and on its variants ({@link Stage2Variants}), each written into the gate's temporary directory and
 *  run under a generation registered through {@code generation new} in a temporary store of the assertion ({@link Stage1Assertions#harness});
 *  nothing is written under {@code fixtures/}. */
final class Stage2Assertions {
    private Stage2Assertions() {}

    static final String SMOKE_INCIDENT = "fixtures/recordings/smoke-incident";
    static final String WR = Stage1Assertions.WR, SAMPLES = Stage1Assertions.SAMPLES;
    static final String AFFECTED = "forseti.affected", SETS = "forseti.affected-sets", LEDGER2 = "forseti.dispositions-stage2", INC = "quality.incidents";
    static final String UNIT = "UNIT0000000000001";
    static final ObjectNode INC_KEY = Json.object().put("source", "qms-line2").put("incidentId", "inc-1");
    static final ObjectNode UNIT_KEY = INC_KEY.deepCopy().put("unitId", UNIT);
    static final ObjectNode VISIT = Json.object().put("mode", "occupancy-join").put("source", "mes-line2").put("arrivalEventId", "occ-1");

    static List<Check> checks() {
        return List.of(
            new Check("T-PROGRESS-TRACKER", Stage2Assertions::progressTracker),
            new Check("T-SMOKE-INCIDENT", Stage2Assertions::smokeIncident),
            new Check("T-CASCADE", Stage2Assertions::cascade),
            new Check("T-DELETE", Stage2Assertions::delete),
            new Check("T-LEDGER-2", Stage2Assertions::ledger2));
    }

    // --- runs ----------------------------------------------------------------------------------------------------------------

    static Run base(Context c) throws Exception { return Stage1Assertions.base(c, SMOKE_INCIDENT); }

    /** A variant of smoke-incident, written by {@code make} into a fresh gate directory and run once per gate. */
    interface Maker { Path make(Path src, Path dst); }
    static Run variant(Context c, String name, Maker make) throws Exception {
        return c.memo("variant:" + name, () -> {
            Path rec = make.make(c.path(SMOKE_INCIDENT), c.newDir("rec").resolve(name));
            return Stage1Assertions.harness(c, rec.toString(), null, false);
        });
    }

    // --- reading -------------------------------------------------------------------------------------------------------------

    static JsonNode keyOf(JsonNode line) {
        JsonNode del = line.path("value").path("delete");
        return del.isObject() ? del.get("logicalKey") : line.get("key");
    }

    /** A journal partition: its progress records and, per cut, the business lines before that cut's progress. */
    record Segmented(TreeMap<Long, JsonNode> progress, TreeMap<Long, List<JsonNode>> rows, int trailing) {}

    static Segmented segments(Path out, String topic, int p) throws IOException {
        TreeMap<Long, JsonNode> progress = new TreeMap<>();
        TreeMap<Long, List<JsonNode>> rows = new TreeMap<>();
        List<JsonNode> seg = new ArrayList<>();
        for (JsonNode n : Stage1Assertions.lines(out, topic, p)) {
            if (Stage1Assertions.isProgress(n)) {
                long cut = n.path("value").get("cutId").longValue();
                progress.put(cut, n.get("value"));
                rows.put(cut, seg);
                seg = new ArrayList<>();
            } else seg.add(n);
        }
        return new Segmented(progress, rows, seg.size());
    }

    /** The live body of {@code key} at each cut's progress (null when absent or deleted). */
    static Map<Long, JsonNode> perCut(Path out, String topic, int p, JsonNode key) throws IOException {
        Map<Long, JsonNode> m = new TreeMap<>();
        JsonNode live = null;
        for (JsonNode n : Stage1Assertions.lines(out, topic, p)) {
            if (Stage1Assertions.isProgress(n)) { m.put(n.path("value").get("cutId").longValue(), live); continue; }
            if (key.equals(keyOf(n))) live = n.path("value").path("delete").isObject() ? null : n.path("value").get("body");
        }
        return m;
    }

    static String statusOf(JsonNode body) { return body == null ? "absent" : body.path("status").asText() + " " + body.path("reason").asText(); }

    /** J-BASIS-CURRENT at every cut and station partition: every basis[].bodyDigest of every live assessment equals the folded work
     *  records' current digest of that visitRef at the same cut. Returns the (cut, assessment) pairs checked. */
    static int basisCurrent(Problems p, String label, Run r) throws IOException {
        Recording rec = Recording.open(r.recording());
        int checked = 0;
        for (int part : Stage1Assertions.stationPartitions(rec)) {
            List<JournalFold.Entry> wr = Stage1Assertions.entries(WR, Stage1Assertions.lines(r.out(), WR, part), p);
            List<JournalFold.Entry> af = Stage1Assertions.entries(AFFECTED, Stage1Assertions.lines(r.out(), AFFECTED, part), p);
            for (long cut : Stage1Assertions.cutIds(rec)) {
                Map<String, JournalFold.Entry> w = JournalFold.fold(wr, 1, cut).live(), a = JournalFold.fold(af, 1, cut).live();
                for (JournalFold.Entry e : a.values()) {
                    checked++;
                    for (JsonNode b : ((Row) e).body().path("basis")) {
                        JournalFold.Entry cur = w.get(Stage1Assertions.keyText(b.get("visitRef")));
                        if (cur == null) p.list.add(label + " cut " + cut + ": basis " + b.get("visitRef") + " has no work record");
                        else if (!((Row) cur).bodyDigest().equals(b.path("bodyDigest").asText()))
                            p.list.add(label + " cut " + cut + " partition " + part + ": stale basis digest of " + b.get("visitRef"));
                    }
                }
            }
        }
        return checked;
    }

    // --- T-PROGRESS-TRACKER --------------------------------------------------------------------------------------------------

    static Result progressTracker(Context c) throws Exception {
        Run r = base(c);
        Recording rec = Recording.open(r.recording());
        List<Long> cuts = Stage1Assertions.cutIds(rec);
        JsonNode cutLog = Json.read(r.out().resolve("cuts.json")).get("cuts");
        Problems p = new Problems();
        int records = 0;
        for (Map.Entry<String, Integer> st : rec.routing().stations().entrySet()) {
            int part = st.getValue();
            Segmented wr = segments(r.out(), WR, part), sm = segments(r.out(), SAMPLES, part);
            for (String topic : List.of(AFFECTED, SETS)) {
                String label = topic + "/" + part;
                Segmented s = segments(r.out(), topic, part);
                p.equal(0, s.trailing(), label + " lines after the last progress");
                p.equal(cuts, new ArrayList<>(s.progress().keySet()), label + " progress cut ids");
                for (int k = 0; k < cuts.size() && k < cutLog.size(); k++) {
                    long cut = cuts.get(k);
                    JsonNode pr = s.progress().get(cut);
                    if (pr == null) continue;
                    records++;
                    Recording.Batch b = rec.batches().get(k);
                    p.equal(st.getKey(), pr.path("station").asText(), label + " station");
                    // after both stage-1 progresses of the cut: the refs name them, and they exist on the evidence topics
                    List<String> refs = new ArrayList<>();
                    for (JsonNode x : pr.path("stage1ProgressRefs")) refs.add(x.path("topic").asText() + "/" + x.path("partition").asInt() + "@" + x.path("cutId").asLong());
                    p.equal(List.of(WR + "/" + part + "@" + cut, SAMPLES + "/" + part + "@" + cut), refs, label + " cut " + cut + " stage1ProgressRefs");
                    JsonNode w1 = wr.progress().get(cut), s1 = sm.progress().get(cut);
                    p.require(w1 != null && s1 != null, label + " cut " + cut + ": a stage-1 progress is missing");
                    // the six incident barriers of the cut, as the batch manifest has them
                    List<String> offs = new ArrayList<>(), want = new ArrayList<>();
                    for (JsonNode o : pr.path("incidentOffsets")) offs.add(o.path("cutId").asLong() + ":" + o.path("partition").asInt() + "@" + o.path("throughOffset").asLong());
                    for (int q = 0; q < 6; q++) want.add(cut + ":" + q + "@" + b.through(INC, q));
                    p.equal(want, offs, label + " cut " + cut + " incidentOffsets");
                    long w = pr.path("W2").asLong();
                    p.equal(cutLog.get(k).path("perStation").path(st.getKey()).path("W1").asLong(), w, label + " cut " + cut + " W2 == W1");
                    p.equal(cutLog.get(k).path("perStation").path(st.getKey()).path("W2").asLong(), w, label + " cut " + cut + " W2 in cuts.json");
                    if (w1 != null && s1 != null) p.equal(Math.min(w1.path("W1").asLong(), s1.path("W1").asLong()), w, label + " cut " + cut + " W2 = min of the stage-1 progress W1");
                    p.equal(0, pr.path("mergedCutIds").size(), label + " cut " + cut + " mergedCutIds");
                    // every row of the cut is stamped with its W2
                    for (JsonNode row : s.rows().get(cut))
                        if (!row.path("value").path("delete").isObject() && row.path("value").path("envelope").path("eventTimeAt").asLong() != w)
                            p.list.add(label + " cut " + cut + ": a row stamped " + row.path("value").path("envelope").path("eventTimeAt") + ", W2 " + w);
                }
            }
            // the stage-2 ledger progress per cut, its barriers the six incident partitions
            ledgerProgress(p, r, rec, part, st.getKey(), cuts);
        }
        ledgerProgress(p, r, rec, 6, null, cuts);
        return p.result(cuts.size() + " cuts x " + rec.routing().stations().size() + " stations x 2 topics = " + records
            + " stage-2 progress records after both stage-1 progresses, 6 incident barriers each, W2 == W1; stage-2 ledger progress on partitions 0-2 and 6 per cut");
    }

    static void ledgerProgress(Problems p, Run r, Recording rec, int part, String station, List<Long> cuts) throws IOException {
        String label = LEDGER2 + "/" + part;
        Segmented s = segments(r.out(), LEDGER2, part);
        p.equal(0, s.trailing(), label + " lines after the last progress");
        p.equal(cuts, new ArrayList<>(s.progress().keySet()), label + " progress cut ids");
        for (int k = 0; k < cuts.size(); k++) {
            JsonNode pr = s.progress().get(cuts.get(k));
            if (pr == null) continue;
            p.equal(2, pr.path("stage").asInt(), label + " stage");
            p.equal(station, pr.path("station").isNull() ? null : pr.path("station").asText(), label + " station");
            p.require(!pr.has("initEvidence"), label + " cut " + cuts.get(k) + ": initEvidence on a stage-2 ledger");
            List<String> refs = new ArrayList<>(), want = new ArrayList<>();
            for (JsonNode b : pr.path("barrierRefs")) refs.add(b.path("origin").path("topic").asText() + "/" + b.path("origin").path("partition").asInt() + "@" + b.path("throughOffset").asLong());
            for (int q = 0; q < 6; q++) want.add(INC + "/" + q + "@" + rec.batches().get(k).through(INC, q));
            p.equal(want, refs, label + " cut " + cuts.get(k) + " barrierRefs");
        }
        JournalFold.Result fold = JournalFold.fold(Stage1Assertions.entries(LEDGER2, Stage1Assertions.lines(r.out(), LEDGER2, part), p), 1, cuts.get(cuts.size() - 1));
        p.require(fold.violations().isEmpty(), label + ": " + fold.violations());
    }

    // --- T-SMOKE-INCIDENT ----------------------------------------------------------------------------------------------------

    static Result smokeIncident(Context c) throws Exception {
        Run r = base(c);
        Recording rec = Recording.open(r.recording());
        List<Long> cuts = Stage1Assertions.cutIds(rec);
        long last = cuts.get(cuts.size() - 1);
        Problems p = new Problems();
        Map<String, JsonNode> assessments = new TreeMap<>(), sets = new TreeMap<>();
        for (int part : Stage1Assertions.stationPartitions(rec)) {
            for (Map.Entry<String, JournalFold.Entry> e : JournalFold.fold(Stage1Assertions.entries(AFFECTED, Stage1Assertions.lines(r.out(), AFFECTED, part), p), 1, last).live().entrySet())
                assessments.put(part + " " + e.getKey(), ((Row) e.getValue()).body());
            for (Map.Entry<String, JournalFold.Entry> e : JournalFold.fold(Stage1Assertions.entries(SETS, Stage1Assertions.lines(r.out(), SETS, part), p), 1, last).live().entrySet())
                sets.put(part + " " + e.getKey(), ((Row) e.getValue()).body());
        }
        p.equal(Set.of("0 " + Stage1Assertions.keyText(UNIT_KEY)), assessments.keySet(), "live assessments at the last cut");
        p.equal(Set.of("0 " + Stage1Assertions.keyText(INC_KEY)), sets.keySet(), "live affected sets at the last cut");
        JsonNode a = assessments.get("0 " + Stage1Assertions.keyText(UNIT_KEY)), s = sets.get("0 " + Stage1Assertions.keyText(INC_KEY));
        p.equal("affected-candidate equipment-activity", statusOf(a), "assessment");
        JsonNode cov = s == null ? Json.object() : s.path("coverage");
        if (s != null) {
            p.equal(1, s.path("units").size(), "set units");
            p.equal(UNIT, s.path("units").path(0).path("unitId").asText(), "set unit");
            p.equal(true, cov.path("settled").asBoolean(), "coverage.settled");
            p.equal("complete", cov.path("status").asText(), "coverage.status");
        }
        return p.result("at cut " + last + ": " + statusOf(a) + " for " + UNIT + "; set units " + (s == null ? "-" : s.path("units").size())
            + ", resolution " + (s == null ? "-" : s.path("resolution")) + ", coverage " + Canonical.utf8(Canonical.bytes(cov)));
    }

    // --- T-CASCADE -----------------------------------------------------------------------------------------------------------

    static Result cascade(Context c) throws Exception {
        Run r = variant(c, "smoke-incident-cascade", Stage2Variants::cascade);
        Problems p = new Problems();
        Map<Long, JsonNode> wr = perCut(r.out(), WR, 0, VISIT), as = perCut(r.out(), AFFECTED, 0, UNIT_KEY);
        long retracted = -1, judged = -1;
        for (Map.Entry<Long, JsonNode> e : wr.entrySet()) if (retracted < 0 && e.getValue() != null && "retracted".equals(e.getValue().path("status").asText())) retracted = e.getKey();
        for (Map.Entry<Long, JsonNode> e : as.entrySet())
            if (judged < 0 && "not-affected-within-scope basis-retracted-error-declaration".equals(statusOf(e.getValue()))) judged = e.getKey();
        p.require(retracted > 0, "the work record is never retracted");
        p.equal(retracted, judged, "the cut of the re-judgement (the cut of the retraction)");
        if (retracted > 1) p.equal("affected-candidate equipment-activity", statusOf(as.get(retracted - 1)), "the assessment before the retraction");
        if (retracted > 0 && as.get(retracted) != null)
            p.equal(Canonical.digest(wr.get(retracted)), as.get(retracted).path("basis").path(0).path("bodyDigest").asText(), "the re-judged row cites the retracted digest");
        int base = basisCurrent(p, "smoke-incident", base(c)), variant = basisCurrent(p, "smoke-incident-cascade", r);
        return p.result("occ-1 retracted (did_not_occur, last batch) at cut " + retracted + "; the assessment becomes not-affected-within-scope "
            + "basis-retracted-error-declaration at cut " + judged + "; J-BASIS-CURRENT holds at every cut (" + base + " + " + variant + " live assessment-cuts checked)");
    }

    // --- T-DELETE ------------------------------------------------------------------------------------------------------------

    static final long NARROW_DEPART_AT = 30;

    static Result delete(Context c) throws Exception {
        Run r = variant(c, "smoke-incident-narrow", (s, d) -> Stage2Variants.narrow(s, d, NARROW_DEPART_AT, null));
        Recording rec = Recording.open(r.recording());
        List<Long> cuts = Stage1Assertions.cutIds(rec);
        Problems p = new Problems();
        List<String> chain = new ArrayList<>();
        long expected = 0;
        String lastDigest = null;
        JsonNode lastLine = null;
        Segmented s = segments(r.out(), AFFECTED, 0);
        long deleteCut = -1;
        for (Map.Entry<Long, List<JsonNode>> e : s.rows().entrySet()) for (JsonNode n : e.getValue()) {
            if (!UNIT_KEY.equals(keyOf(n))) continue;
            lastLine = n;
            JsonNode del = n.path("value").path("delete");
            long rev = del.isObject() ? del.path("revision").asLong() : n.path("value").path("envelope").path("revision").asLong();
            p.equal(++expected, rev, "revision of the unit row");
            if (del.isObject()) {
                p.equal(lastDigest, del.path("supersedesDigest").asText(null), "the delete supersedes the last digest");
                lastDigest = null;
                deleteCut = e.getKey();
                chain.add(rev + " delete@" + e.getKey());
            } else {
                lastDigest = n.path("value").path("bodyDigest").asText();
                chain.add(rev + " " + statusOf(n.path("value").path("body")) + "@" + e.getKey());
            }
        }
        p.require(lastLine != null && lastLine.path("value").path("delete").isObject(), "the unit row does not end with a delete: " + chain);
        p.equal(NARROW_DEPART_AT, deleteCut, "the cut of the delete (the late departure's)");
        JournalFold.Result fold = JournalFold.fold(Stage1Assertions.entries(AFFECTED, Stage1Assertions.lines(r.out(), AFFECTED, 0), p), 1, cuts.get(cuts.size() - 1));
        p.require(fold.violations().isEmpty(), "fold: " + fold.violations());
        p.require(!fold.live().containsKey(Stage1Assertions.keyText(UNIT_KEY)), "the unit is live after the delete");
        JsonNode set = perCut(r.out(), SETS, 0, INC_KEY).get(cuts.get(cuts.size() - 1));
        p.equal(0, set == null ? -1 : set.path("units").size(), "set units at the last cut");
        return p.result("open visit, incident narrowed to suspectFrom T+30 s, late departure at T+20 s in cut " + NARROW_DEPART_AT + ": " + String.join(", ", chain)
            + "; JournalFold violations empty; set units 0");
    }

    // --- T-LEDGER-2 ----------------------------------------------------------------------------------------------------------

    static Result ledger2(Context c) throws Exception {
        Problems p = new Problems();
        List<String> summary = new ArrayList<>();
        for (Map.Entry<String, Run> e : List.of(Map.entry("smoke-incident", base(c)),
                Map.entry("smoke-incident-station2", variant(c, "smoke-incident-station2", Stage2Variants::station2Incident)))) {
            Run r = e.getValue();
            Recording rec = Recording.open(r.recording());
            List<Long> cuts = Stage1Assertions.cutIds(rec);
            // one stage-2 disposition per incident delivery, on a station partition or partition 6
            Map<String, String> got = new TreeMap<>();
            Map<String, Integer> where = new TreeMap<>();
            List<Integer> partitions = new ArrayList<>(Stage1Assertions.stationPartitions(rec));
            partitions.add(6);
            for (int part : partitions) {
                for (JsonNode n : Stage1Assertions.lines(r.out(), LEDGER2, part)) {
                    if (Stage1Assertions.isProgress(n)) continue;
                    JsonNode k = n.get("key");
                    String id = k.path("inputTopic").asText() + "/" + k.path("inputPartition").asInt() + "/" + k.path("inputOffset").asLong() + " " + k.path("part").asText();
                    JsonNode body = n.path("value").path("body");
                    got.put(id, body.path("disposition").asText() + " " + body.path("reason").asText());
                    where.put(id, part);
                }
                ledgerProgress(p, r, rec, part, part == 6 ? null : rec.routing().stations().entrySet().stream().filter(x -> x.getValue() == part).findFirst().orElseThrow().getKey(), cuts);
            }
            Set<String> deliveries = new TreeSet<>();
            for (int q = 0; q < 6; q++) for (Recording.Record x : rec.records(INC, q)) deliveries.add(INC + "/" + q + "/" + x.offset() + " event");
            p.equal(deliveries, got.keySet(), e.getKey() + " stage-2 disposition keys");
            String inc1 = INC + "/" + Stage2Variants.INC1_PARTITION + "/0 event";
            p.equal("accepted accepted", got.get(inc1), e.getKey() + " inc-1");
            p.equal(0, where.get(inc1), e.getKey() + " inc-1 on station1's partition");
            if (e.getKey().endsWith("station2")) {
                String inc2 = INC + "/" + Stage2Variants.STATION2_PARTITION + "/0 event";
                p.equal("rejected outside-incident-scope", got.get(inc2), e.getKey() + " inc-2 on station2");
                p.equal(6, where.get(inc2), e.getKey() + " inc-2 on partition 6");
            }
            summary.add(e.getKey() + ": " + deliveries.size() + " deliveries, dispositions " + got + " (partitions " + where + ")");
        }
        return p.result(String.join("; ", summary) + "; stage-2 ledger progress per cut on partitions 0-2 and 6, barriers the six incident partitions");
    }
}
