package yggdrasil.forseti.cli.harness;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.gate.Stage2Variants;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.*;

/** Stage 2 in the reference harness (Task 15, rule sheet T2, T3, T10) on the committed {@code smoke-incident} recording and on its
 *  variants written into temporary directories by {@link Stage2Variants} (the fixtures are never written). Generations are registered
 *  by hand as in {@link ReferenceEngineTest}. */
class CascadeTest {
    static final Path SMOKE_INCIDENT = ReferenceEngineTest.FIXTURES.resolve("recordings/smoke-incident");
    static final String WR = "forseti.work-records", AFFECTED = "forseti.affected", SETS = "forseti.affected-sets", LEDGER2 = "forseti.dispositions-stage2";
    static final String UNIT = "UNIT0000000000001";
    static final ObjectNode INC_KEY = Json.object().put("source", "qms-line2").put("incidentId", "inc-1");
    static final ObjectNode UNIT_KEY = INC_KEY.deepCopy().put("unitId", UNIT);

    @TempDir static Path shared;
    @TempDir Path tmp;
    private static Path base;

    static synchronized Path base() throws IOException {
        if (base == null) base = run(SMOKE_INCIDENT, shared.resolve("base"));
        return base;
    }

    static Path run(Path recording, Path dir) throws IOException {
        Path state = Files.createDirectories(dir.resolve("state"));
        ReferenceEngineTest.register(state, 1, ReferenceEngineTest.REGISTRY, recording, null);
        Path out = dir.resolve("out");
        ReferenceEngine.run(ReferenceEngineTest.options(recording, ReferenceEngineTest.REGISTRY, state, 1, out));
        return out;
    }

    // ============================================================================================================ the tests

    @Test void aStage1RetractionReJudgesTheCitingRowInTheSameCut() throws IOException {
        Path rec = Stage2Variants.cascade(SMOKE_INCIDENT, tmp.resolve("cascade"));
        Path out = run(rec, tmp.resolve("run"));
        long last = Stage2Variants.lastCut(rec);
        ObjectNode visit = ReferenceEngineTest.visitRef().deepCopy();
        Map<Long, JsonNode> wr = perCut(out, WR, 0, visit), as = perCut(out, AFFECTED, 0, UNIT_KEY);
        long retractedAt = firstCut(wr, b -> "retracted".equals(b.path("status").asText()));
        assertEquals(last, retractedAt, "the declaration in the last batch retracts the visit at the last cut");
        JsonNode before = as.get(last - 1), after = as.get(last);
        assertEquals("affected-candidate equipment-activity", statusOf(before), "before the declaration");
        assertEquals("not-affected-within-scope basis-retracted-error-declaration", statusOf(after), "in the same cut as the retraction");
        assertEquals(Canonical.digest(wr.get(last)), after.path("basis").get(0).path("bodyDigest").asText(), "the row cites the retracted digest");
        // the re-judging row is caused by the work record, in the cut's segment of forseti.affected
        Segmented a = segments(out, AFFECTED, 0);
        List<JsonNode> rows = a.rows.get(last).stream().filter(n -> UNIT_KEY.equals(n.get("key"))).toList();
        assertEquals(1, rows.size(), "one assessment revision in the last cut");
        assertEquals("work-record", rows.get(0).path("value").path("envelope").path("cause").path("inputKind").asText());
        assertEquals(visit, rows.get(0).path("value").path("envelope").path("cause").path("identity"));
        // revision 1 basis-provisional at the delivering cut 27 (the visit settles at cut 28), 2 the candidate at 28, 3 the re-judgement
        List<String> chain = new ArrayList<>();
        for (List<JsonNode> seg : a.rows.values()) for (JsonNode n : seg) if (UNIT_KEY.equals(n.get("key")))
            chain.add(n.path("value").path("envelope").path("revision").longValue() + " " + statusOf(n.path("value").path("body")));
        assertEquals(List.of("1 insufficient-evidence basis-provisional", "2 affected-candidate equipment-activity",
            "3 not-affected-within-scope basis-retracted-error-declaration"), chain);
    }

    @Test void noLiveAssessmentCitesAStaleDigest() throws IOException {
        Path cascade = run(Stage2Variants.cascade(SMOKE_INCIDENT, tmp.resolve("cascade")), tmp.resolve("run-c"));
        Path two = run(Stage2Variants.twoVisits(SMOKE_INCIDENT, tmp.resolve("two"), true), tmp.resolve("run-t"));
        int checked = 0;
        for (Path out : List.of(base(), cascade, two)) checked += basisCurrent(out, Recording.open(SMOKE_INCIDENT).batches().size());
        assertTrue(checked >= 3 * 27, "assessments checked at every cut after the incident: " + checked);
    }

    @Test void aNarrowedOpenVisitEndsWithADeleteAndContinuesRevisions() throws IOException {
        Path rec = Stage2Variants.narrow(SMOKE_INCIDENT, tmp.resolve("narrow"), 30, 35L);
        Path out = run(rec, tmp.resolve("run"));
        List<JsonNode> chain = new ArrayList<>();
        Map<Long, Long> cutOf = new HashMap<>();
        Segmented s = segments(out, AFFECTED, 0);
        for (Map.Entry<Long, List<JsonNode>> e : s.rows.entrySet())
            for (JsonNode n : e.getValue()) if (UNIT_KEY.equals(keyOf(n))) { chain.add(n); cutOf.put((long) chain.size(), e.getKey()); }
        StringBuilder trace = new StringBuilder();
        long revision = 0;
        String lastDigest = null;
        int deleteAt = -1;
        for (int i = 0; i < chain.size(); i++) {
            JsonNode n = chain.get(i), del = n.path("value").path("delete");
            long r = del.isObject() ? del.get("revision").longValue() : n.path("value").path("envelope").path("revision").longValue();
            assertEquals(++revision, r, "revisions contiguous across the delete");
            if (del.isObject()) {
                assertEquals(lastDigest, del.get("supersedesDigest").asText(), "the delete supersedes the last digest");
                deleteAt = i;
                lastDigest = null;
                trace.append("delete@").append(cutOf.get((long) i + 1)).append(' ');
            } else {
                lastDigest = n.path("value").path("bodyDigest").asText();
                trace.append(statusOf(n.path("value").path("body"))).append('@').append(cutOf.get((long) i + 1)).append(' ');
            }
        }
        assertTrue(deleteAt > 0, "a delete: " + trace);
        assertEquals(30L, cutOf.get((long) deleteAt + 1), "deleted in the cut of the late departure: " + trace);
        assertTrue(deleteAt < chain.size() - 1, "the unit reappears after the departure is cancelled: " + trace);
        assertEquals(35L, cutOf.get((long) deleteAt + 2), trace.toString());
        assertEquals("insufficient-evidence basis-provisional", statusOf(chain.get(deleteAt + 1).path("value").path("body")), trace.toString());
        assertEquals("insufficient-evidence basis-provisional", statusOf(chain.get(deleteAt - 1).path("value").path("body")), trace.toString());
        JournalFold.Result fold = JournalFold.fold(entries(AFFECTED, ReferenceEngineTest.lines(out, AFFECTED, 0)), 1, Stage2Variants.lastCut(rec));
        assertTrue(fold.violations().isEmpty(), fold.violations().toString());
        // the set's units follow: empty after the delete, the unit again after the reappearance
        Map<Long, JsonNode> sets = perCut(out, SETS, 0, INC_KEY);
        assertEquals(0, sets.get(30L).path("units").size(), "set units after the delete");
        assertEquals(1, sets.get(35L).path("units").size(), "set units after the reappearance");
    }

    @Test void aUnitWithTwoVisitsKeepsPositiveEvidenceWhenOneIsCancelled() throws IOException {
        Path rec = Stage2Variants.twoVisits(SMOKE_INCIDENT, tmp.resolve("two"), true);
        Path out = run(rec, tmp.resolve("run"));
        long last = Stage2Variants.lastCut(rec);
        ObjectNode a = ReferenceEngineTest.visitRef().deepCopy(), b = a.deepCopy().put("arrivalEventId", "occ-3");
        Map<Long, JsonNode> wa = perCut(out, WR, 0, a), wb = perCut(out, WR, 0, b), as = perCut(out, AFFECTED, 0, UNIT_KEY);
        assertEquals("complete", wa.get(last - 1).path("status").asText());
        assertEquals("complete", wb.get(last - 1).path("status").asText(), "the second visit: " + wb.get(last - 1).path("insufficiencyReasons"));
        assertEquals("retracted", wa.get(last).path("status").asText());
        assertEquals("complete", wb.get(last).path("status").asText());
        JsonNode before = as.get(last - 1), after = as.get(last);
        assertEquals("affected-candidate equipment-activity", statusOf(before));
        assertEquals(2, before.path("basis").size(), "both visits are bases");
        assertEquals("affected-candidate equipment-activity", statusOf(after), "the second visit keeps the positive evidence");
        assertEquals(2, after.path("basis").size(), "the cancelled visit stays a basis");
        Map<String, String> cited = new HashMap<>();
        for (JsonNode x : after.path("basis")) cited.put(Canonical.utf8(Canonical.bytes(x.get("visitRef"))), x.get("bodyDigest").asText());
        assertEquals(Canonical.digest(wa.get(last)), cited.get(Canonical.utf8(Canonical.bytes(a))), "the cancelled visit's current digest");
        assertEquals(Canonical.digest(wb.get(last)), cited.get(Canonical.utf8(Canonical.bytes(b))));
        assertEquals(1, after.path("evidenceWarnings").size(), "the retracted basis is a warning: " + after.path("evidenceWarnings"));
        assertEquals(a, after.path("evidenceWarnings").get(0).get("visitRef"));
        // without the cancellation the same unit is a candidate as well
        Path plain = run(Stage2Variants.twoVisits(SMOKE_INCIDENT, tmp.resolve("two-plain"), false), tmp.resolve("run-plain"));
        assertEquals("affected-candidate equipment-activity", statusOf(perCut(plain, AFFECTED, 0, UNIT_KEY).get(last)));
    }

    @Test void progressFollowsEveryBusinessDeleteAndDispositionRow() throws IOException {
        Path rec = Stage2Variants.narrow(SMOKE_INCIDENT, tmp.resolve("narrow"), 30, null);
        Path out = run(rec, tmp.resolve("run"));
        List<Long> cuts = Recording.open(rec).batches().stream().map(Recording.Batch::cutId).toList();
        JsonNode cutLog = Json.read(out.resolve("cuts.json")).get("cuts");
        int rows = 0, deletes = 0, dispositions = 0;
        for (int p : List.of(0, 1, 2)) {
            String station = List.of(Stage2Variants.S1, Stage2Variants.S2, "synthetic-site:line2:station3").get(p);
            for (String topic : List.of(AFFECTED, SETS, LEDGER2)) {
                List<JsonNode> l = ReferenceEngineTest.lines(out, topic, p);
                assertTrue(ReferenceEngineTest.isProgress(l.get(l.size() - 1)), topic + "/" + p + " ends with progress");
                Segmented s = segments(out, topic, p);
                assertEquals(cuts, new ArrayList<>(s.progress.keySet()), topic + "/" + p + " progress per cut");
                for (Map.Entry<Long, List<JsonNode>> e : s.rows.entrySet()) for (JsonNode n : e.getValue()) {
                    if (n.path("value").path("delete").isObject()) { deletes++; continue; }
                    if (topic.equals(LEDGER2)) { dispositions++; continue; }
                    rows++;
                    assertEquals(s.progress.get(e.getKey()).get("W2").longValue(), n.path("value").path("envelope").path("eventTimeAt").longValue(), "stamped with its cut's W2");
                }
                JournalFold.Result fold = JournalFold.fold(entries(topic, l), 1, cuts.get(cuts.size() - 1));
                assertTrue(fold.reachedProgress());
                assertTrue(fold.violations().isEmpty(), topic + "/" + p + ": " + fold.violations());
                if (topic.equals(LEDGER2)) continue;
                for (int k = 0; k < cuts.size(); k++) {
                    JsonNode pr = s.progress.get(cuts.get(k));
                    assertEquals(station, pr.get("station").asText());
                    assertEquals(cutLog.get(k).path("perStation").path(station).get("W1").longValue(), pr.get("W2").longValue(), "W2 == W1");
                    assertEquals(cutLog.get(k).path("perStation").path(station).get("W2").longValue(), pr.get("W2").longValue(), "cuts.json W2");
                    assertEquals(6, pr.get("incidentOffsets").size());
                    assertEquals(2, pr.get("stage1ProgressRefs").size());
                    assertEquals(0, pr.get("mergedCutIds").size());
                }
            }
        }
        List<Long> six = new ArrayList<>(segments(out, LEDGER2, 6).progress.keySet());
        assertEquals(cuts, six, "partition-6 progress per cut");
        assertEquals(1, deletes, "the narrowing delete");
        assertEquals(1, dispositions, "one incident delivery, one disposition");
        assertTrue(rows > 0);
        // the delete precedes the progress of the departure's cut on forseti.affected/0
        Segmented a = segments(out, AFFECTED, 0);
        assertTrue(a.rows.get(30L).stream().anyMatch(n -> n.path("value").path("delete").isObject()), "the delete is in cut 30's segment");
    }

    @Test void aCutWithNoIncidentThenTheFirstIncidentAtOffsetZero() throws IOException {
        Path out = base();
        long delivering = Stage2Variants.deliveringCut(SMOKE_INCIDENT, Stage2Variants.INC, Stage2Variants.INC1_PARTITION, 0);
        assertEquals(27, delivering, "smoke-incident delivers inc-1 in cut 27");
        Segmented s = segments(out, AFFECTED, 0), d = segments(out, LEDGER2, 0);
        JsonNode before = s.progress.get(delivering - 1), at = s.progress.get(delivering), next = s.progress.get(delivering + 1);
        for (JsonNode o : before.get("incidentOffsets")) {
            assertEquals(delivering - 1, o.get("cutId").longValue());
            assertEquals(-1, o.get("throughOffset").longValue(), "before the incident: " + o);
        }
        for (long c = 1; c < delivering; c++) {
            assertTrue(s.rows.get(c).isEmpty(), "no assessment before the incident, cut " + c);
            assertTrue(d.rows.get(c).isEmpty(), "no stage-2 disposition before the incident, cut " + c);
        }
        for (JsonNode pr : List.of(at, next))
            for (JsonNode o : pr.get("incidentOffsets"))
                assertEquals(o.get("partition").intValue() == Stage2Variants.INC1_PARTITION ? 0 : -1, o.get("throughOffset").longValue(), "cut " + pr.get("cutId") + ": " + o);
        assertEquals(6, at.get("incidentOffsets").size());
        List<JsonNode> disp = d.rows.get(delivering);
        assertEquals(1, disp.size(), "the incident is applied once");
        assertEquals("quality.incidents", disp.get(0).path("key").path("inputTopic").asText());
        assertEquals(0, disp.get(0).path("key").path("inputOffset").longValue());
        assertEquals("accepted", disp.get(0).path("value").path("body").path("disposition").asText());
        for (long c = delivering + 1; c <= d.progress.lastKey(); c++) assertTrue(d.rows.get(c).isEmpty(), "nothing applied twice, cut " + c);
        JsonNode first = s.rows.get(delivering).stream().filter(n -> UNIT_KEY.equals(n.get("key"))).findFirst().orElseThrow();
        assertEquals("{\"offset\":0,\"partition\":1,\"topic\":\"quality.incidents\"}", Canonical.utf8(Canonical.bytes(first.path("value").path("envelope").path("trigger"))));
        // the base run: one candidate, one set of one unit
        Map<Long, JsonNode> as = perCut(out, AFFECTED, 0, UNIT_KEY), sets = perCut(out, SETS, 0, INC_KEY);
        long last = s.progress.lastKey();
        assertEquals("affected-candidate equipment-activity", statusOf(as.get(last)));
        assertEquals(1, sets.get(last).path("units").size());
    }

    // ============================================================================================================ helpers

    static String statusOf(JsonNode body) {
        assertNotNull(body, "no body");
        return body.path("status").asText() + " " + body.path("reason").asText();
    }

    static JsonNode keyOf(JsonNode line) {
        JsonNode del = line.path("value").path("delete");
        return del.isObject() ? del.get("logicalKey") : line.get("key");
    }

    /** A journal partition cut into its progress records and, per cut, the business lines before that cut's progress. */
    record Segmented(TreeMap<Long, JsonNode> progress, TreeMap<Long, List<JsonNode>> rows) {}

    static Segmented segments(Path out, String topic, int p) throws IOException {
        TreeMap<Long, JsonNode> progress = new TreeMap<>();
        TreeMap<Long, List<JsonNode>> rows = new TreeMap<>();
        List<JsonNode> seg = new ArrayList<>();
        for (JsonNode n : ReferenceEngineTest.lines(out, topic, p)) {
            if (ReferenceEngineTest.isProgress(n)) {
                long c = n.path("value").get("cutId").longValue();
                progress.put(c, n.get("value"));
                rows.put(c, seg);
                seg = new ArrayList<>();
            } else seg.add(n);
        }
        assertTrue(seg.isEmpty(), topic + "/" + p + ": lines after the last progress");
        return new Segmented(progress, rows);
    }

    /** The live body of {@code key} at each cut's progress (null after a delete). */
    static Map<Long, JsonNode> perCut(Path out, String topic, int p, JsonNode key) throws IOException {
        Map<Long, JsonNode> m = new TreeMap<>();
        JsonNode live = null;
        for (JsonNode n : ReferenceEngineTest.lines(out, topic, p)) {
            if (ReferenceEngineTest.isProgress(n)) { m.put(n.path("value").get("cutId").longValue(), live); continue; }
            if (!key.equals(keyOf(n))) continue;
            live = n.path("value").path("delete").isObject() ? null : n.path("value").get("body");
        }
        return m;
    }

    static long firstCut(Map<Long, JsonNode> perCut, java.util.function.Predicate<JsonNode> p) {
        for (Map.Entry<Long, JsonNode> e : perCut.entrySet()) if (e.getValue() != null && p.test(e.getValue())) return e.getKey();
        return -1;
    }

    /** J-BASIS-CURRENT (T10) at every cut: every basis[].bodyDigest of every live assessment of every station partition is the current
     *  digest of that visitRef in the folded work records of the same cut. Returns the number of (cut, assessment) pairs checked. */
    static int basisCurrent(Path out, int cuts) throws IOException {
        int checked = 0;
        for (int p = 0; p < 3; p++) {
            List<JournalFold.Entry> wr = entries(WR, ReferenceEngineTest.lines(out, WR, p)), af = entries(AFFECTED, ReferenceEngineTest.lines(out, AFFECTED, p));
            for (long c = 1; c <= cuts; c++) {
                Map<String, JournalFold.Entry> w = JournalFold.fold(wr, 1, c).live(), a = JournalFold.fold(af, 1, c).live();
                for (JournalFold.Entry e : a.values()) {
                    checked++;
                    for (JsonNode b : ((Row) e).body().path("basis")) {
                        JournalFold.Entry cur = w.get(Canonical.utf8(Canonical.bytes(b.get("visitRef"))));
                        assertNotNull(cur, "cut " + c + ": a basis without a work record " + b);
                        assertEquals(((Row) cur).bodyDigest(), b.get("bodyDigest").asText(), "cut " + c + " partition " + p + ": stale basis digest " + b.get("visitRef"));
                    }
                }
            }
        }
        return checked;
    }

    /** A journal partition's lines as fold entries: progress, deletes, stage-2 ledger dispositions, rows (bodyDigest recomputed). */
    static List<JournalFold.Entry> entries(String topic, List<JsonNode> l) {
        List<JournalFold.Entry> e = new ArrayList<>();
        for (JsonNode n : l) {
            JsonNode v = n.get("value");
            if (ReferenceEngineTest.isProgress(n)) e.add(Progress.fromJson(v));
            else if (v.path("delete").isObject()) e.add(Delete.fromJson(v.get("delete")));
            else if (topic.equals(LEDGER2)) {
                ObjectNode d = Json.object();
                d.set("key", n.get("key")); d.set("body", v.get("body")); d.set("revision", v.get("revision"));
                e.add(Disposition.fromJson(d));
            } else {
                assertEquals(Canonical.digest(v.get("body")), v.get("bodyDigest").asText(), "bodyDigest recomputed");
                e.add(new Row(topic, n.get("key"), v.get("body"), v.get("bodyDigest").asText(), Envelope.fromJson(v.get("envelope"))));
            }
        }
        return e;
    }
}
