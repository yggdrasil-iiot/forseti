package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** {@link Comparison}: the folded journals of a hand-written run against hand-written oracle cut streams (a snapshot, then deltas). */
class ComparisonTest {
    @TempDir Path tmp;

    static final ObjectNode K1 = Journals.key("occ-1"), K2 = Journals.key("occ-3");
    static final String T1 = text(K1), T2 = text(K2);

    static String text(JsonNode key) { return Canonical.utf8(Canonical.bytes(key)); }

    int runs;

    /** Cut 1: occ-1 provisional; cut 2: occ-1 complete, occ-3 provisional (a fresh run directory each call). */
    Journals run() throws Exception {
        return new Journals(tmp.resolve("run-" + runs++))
            .row(RunFold.WR, 0, K1, Journals.body("provisional"), 1, 0)
            .progress(RunFold.WR, 0, 1, 1000, 0)
            .row(RunFold.WR, 0, K1, Journals.body("complete"), 2, 1)
            .row(RunFold.WR, 0, K2, Journals.body("provisional"), 1, 1)
            .progress(RunFold.WR, 0, 2, 2000, 1);
    }

    static Map<String, Map<String, JsonNode>> wr(Map<String, JsonNode> rows) { return Map.of(RunFold.WR, rows); }
    static final Map<String, Map<String, JsonNode>> CUT1 = wr(Map.of(T1, Journals.body("provisional")));
    static final Map<String, Map<String, JsonNode>> CUT2 = wr(Map.of(T1, Journals.body("complete"), T2, Journals.body("provisional")));

    /** The agreeing stream: cut 1 a snapshot, cut 2 the delta. */
    void oracle(Path oracle, Map<String, Map<String, JsonNode>> cut2) throws Exception {
        Journals.oracleCut(oracle.resolve("cut-1"), 1, null, Map.of(), CUT1);
        Journals.oracleCut(oracle.resolve("cut-2"), 2, 1L, CUT1, cut2);
    }

    Comparison.Tally compare(Journals j, Path oracle) throws Exception {
        Comparison.Tally t = new Comparison.Tally();
        Comparison.OracleFold o = new Comparison.OracleFold();
        try (RunFold f = new RunFold(j.run, Journals.RULES_DIGEST, 1, true)) {
            for (long k = 1; k <= 2; k++) {
                f.advance(k);
                Comparison.compareCut(f, k, oracle.resolve("cut-" + k), o, "test", Json.object(), t);
            }
            f.finish();
            assertTrue(f.findings.clean(), f.findings.describe());
        }
        return t;
    }

    static boolean says(Comparison.Tally t, String text) { return t.examples.stream().anyMatch(x -> x.contains(text)); }

    /** Cut 2 of the agreeing stream, rewritten for the work-record table. */
    void cut2(Path oracle, Map<String, JsonNode> upserts, List<String> deletes, long count, String setHash, Map<String, String> reported) throws Exception {
        Journals.oracleTable(oracle.resolve("cut-2"), RunFold.WR, 2, 1L, upserts, deletes, count, setHash, reported);
    }

    @Test void agreeingOutputsPass() throws Exception {
        Path oracle = tmp.resolve("oracle"), diffs = tmp.resolve("diffs");
        oracle(oracle, CUT2);
        Comparison.Tally t = compare(run(), oracle);
        Comparison.writeDiffs(t, diffs, "test", 1, null, "not needed");
        assertTrue(t.equal(), t.describe());
        assertEquals(2, t.cuts);
        assertEquals(3, t.rowsCompared);
        assertFalse(Files.exists(diffs), "no difference, no diff directory");
        // cut 2 carries the two changed rows only, not the whole set
        JsonNode d = Json.read(oracle.resolve("cut-2").resolve(RunFold.WR + ".json"));
        assertEquals("delta", d.path("mode").asText());
        assertEquals(2, d.path("upserts").size());
        assertEquals(2, d.path("count").asInt());
    }

    @Test void aBodyChangedInTheOracleFailsWithADiffFile() throws Exception {
        Path oracle = tmp.resolve("oracle"), diffs = tmp.resolve("diffs");
        oracle(oracle, wr(Map.of(T1, Journals.body("insufficient"), T2, Journals.body("provisional"))));
        Comparison.Tally t = compare(run(), oracle);
        assertFalse(t.equal());
        assertEquals(1, t.differences, t.describe());
        Comparison.writeDiffs(t, diffs, "test", 1, null, "no rerun in this test");   // the body was in the cut's delta
        Path f = diffs.resolve("test").resolve("cut-2").resolve(RunFold.WR).resolve("1.json");
        assertTrue(Files.isRegularFile(f), t.describe());
        JsonNode d = Json.read(f);
        assertEquals(2, d.path("cut").asInt());
        assertEquals(1, d.path("generation").asInt());
        assertEquals(K1, d.path("key"));
        assertEquals("$.status", d.path("path").asText());
        assertEquals("insufficient", d.path("expected").asText());
        assertEquals("complete", d.path("actual").asText());
        assertTrue(d.has("inputOffsetRanges"));
    }

    /** A row the oracle did not change in the cut is not in that cut's delta: its diff file takes the oracle's body from the snapshot
     *  rerun, which must fold to the stream's set; a rerun that disagrees adds a difference and never removes one. */
    @Test void anUnchangedOracleRowTakesItsBodyFromTheSnapshotRerun() throws Exception {
        Path oracle = tmp.resolve("oracle"), diffs = tmp.resolve("diffs");
        Map<String, Map<String, JsonNode>> stale = wr(Map.of(T1, Journals.body("provisional"), T2, Journals.body("provisional")));
        oracle(oracle, stale);
        Comparison.Tally t = compare(run(), oracle);
        assertEquals(1, t.differences, t.describe());
        assertEquals(List.of(2L), t.pendingCuts());
        Path rerun = tmp.resolve("rerun");
        Journals.oracleCut(rerun.resolve("cut-2"), 2, null, Map.of(), stale);
        Comparison.writeDiffs(t, diffs, "test", 1, rerun, null);
        assertEquals(1, t.differences, t.describe());
        JsonNode d = Json.read(diffs.resolve("test").resolve("cut-2").resolve(RunFold.WR).resolve("1.json"));
        assertEquals("$.status", d.path("path").asText());
        assertEquals("provisional", d.path("expected").asText());
        assertEquals("complete", d.path("actual").asText());

        Path other = tmp.resolve("rerun-other");
        Journals.oracleCut(other.resolve("cut-2"), 2, null, Map.of(), CUT2);   // a rerun that would agree with the core
        Comparison.writeDiffs(t, tmp.resolve("diffs-other"), "test", 1, other, null);
        assertFalse(t.equal());
        assertTrue(says(t, "the snapshot rerun folds to") || t.differencesByTopic.containsKey("oracle-diagnostic"), t.describe());
        assertEquals(2, t.differences, t.describe());
    }

    @Test void aMissedUpsertFails() throws Exception {
        Path oracle = tmp.resolve("oracle");
        oracle(oracle, CUT2);
        // occ-3 left out of the delta, count and setHash those of the real set
        cut2(oracle, Map.of(T1, Journals.body("complete")), List.of(), 2, Journals.setHash(CUT2.get(RunFold.WR)), Map.of());
        Comparison.Tally t = compare(run(), oracle);
        assertFalse(t.equal());
        assertTrue(says(t, "the folded set has 1 keys"), t.describe());
    }

    @Test void aMissedDeleteFails() throws Exception {
        Path oracle = tmp.resolve("oracle");
        Map<String, Map<String, JsonNode>> cut1 = wr(Map.of(T1, Journals.body("provisional"), Journals.keyText("occ-9"), Journals.body("provisional")));
        Journals.oracleCut(oracle.resolve("cut-1"), 1, null, Map.of(), cut1);
        // occ-9 is gone at cut 2 but the delta does not delete it
        cut2(oracle, Map.of(T1, Journals.body("complete"), T2, Journals.body("provisional")), List.of(), 2, Journals.setHash(CUT2.get(RunFold.WR)), Map.of());
        for (String table : Comparison.TABLES) if (!table.equals(RunFold.WR)) Journals.oracleTable(oracle.resolve("cut-2"), table, 2, 1L, Map.of(), List.of(), 0, Journals.setHash(Map.of()), Map.of());
        Comparison.Tally t = compare(run(), oracle);
        assertFalse(t.equal());
        assertTrue(says(t, "cut 2 " + RunFold.WR + ": the folded set has 3 keys"), t.describe());
    }

    @Test void aTruncatedTableBreaksTheStream() throws Exception {
        Path oracle = tmp.resolve("oracle");
        oracle(oracle, CUT2);
        Path f = oracle.resolve("cut-2").resolve(RunFold.WR + ".json");
        byte[] b = Files.readAllBytes(f);
        Files.write(f, Arrays.copyOf(b, b.length / 2));
        Comparison.Tally t = compare(run(), oracle);
        assertFalse(t.equal());
        assertTrue(t.differencesByTopic.containsKey(Comparison.STREAM_TOPIC), t.describe());
    }

    @Test void aTruncatedStreamFails() throws Exception {
        Path out = Files.createDirectories(tmp.resolve("out"));
        Files.createDirectories(out.resolve("cut-1"));
        List<Long> seen = new ArrayList<>();
        StringWriter acks = new StringWriter();
        String why = Comparison.consume(new BufferedReader(new StringReader("cut 1\n")), acks, out, List.of(1L, 2L), new ArrayList<>(), (k, d) -> seen.add(k));
        assertEquals("the oracle's stream ended before cut 2", why);
        assertEquals(List.of(1L), seen);
        assertEquals("ok 1\n", acks.toString());
        assertFalse(Files.exists(out.resolve("cut-1")), "a consumed cut is deleted before it is acknowledged");
        // and the agreeing stream passes, the oracle's last line kept
        Files.createDirectories(out.resolve("cut-1"));
        Files.createDirectories(out.resolve("cut-2"));
        List<String> trailing = new ArrayList<>();
        assertNull(Comparison.consume(new BufferedReader(new StringReader("cut 1\ncut 2\ncuts 2, rows 3\n")), new StringWriter(), out, List.of(1L, 2L), trailing, (k, d) -> {}));
        assertEquals(List.of("cuts 2, rows 3"), trailing);
    }

    @Test void aMissingCutFails() throws Exception {
        // the stream announces cut 3 where the cut log has cut 2 next
        Path out = Files.createDirectories(tmp.resolve("out"));
        Files.createDirectories(out.resolve("cut-1"));
        String why = Comparison.consume(new BufferedReader(new StringReader("cut 1\ncut 3\n")), new StringWriter(), out, List.of(1L, 2L, 3L), new ArrayList<>(), (k, d) -> {});
        assertEquals("expected 'cut 2' from the oracle, read 'cut 3'", why);
        // a cut directory that is not there, and a delta that does not follow the cut folded last
        Path oracle = tmp.resolve("oracle");
        Journals.oracleCut(oracle.resolve("cut-1"), 1, null, Map.of(), CUT1);
        Comparison.Tally t = compare(run(), oracle);
        assertTrue(says(t, "cut 2 " + RunFold.WR + ": no cut-2"), t.describe());
        Path skipped = tmp.resolve("skipped");
        Journals.oracleCut(skipped.resolve("cut-1"), 1, null, Map.of(), CUT1);
        Journals.oracleCut(skipped.resolve("cut-2"), 2, 7L, CUT1, CUT2);
        t = compare(run(), skipped);
        assertFalse(t.equal());
        assertTrue(says(t, "a delta against cut 7 where the stream folded cut 1 last"), t.describe());
    }

    @Test void aCorruptedBodyDigestFails() throws Exception {
        Path oracle = tmp.resolve("oracle");
        oracle(oracle, CUT2);
        // the oracle's body equals the core's, but the digest it reports is not that body's
        cut2(oracle, Map.of(T1, Journals.body("complete"), T2, Journals.body("provisional")), List.of(), 2, Journals.setHash(CUT2.get(RunFold.WR)),
            Map.of(T1, Canonical.digest(Journals.body("provisional"))));
        Comparison.Tally t = compare(run(), oracle);
        assertFalse(t.equal());
        assertEquals(1, t.oracleDigestMismatches, t.describe());
        // and on the core's side: a journal row whose bodyDigest is not its body's is an integrity finding
        Journals j = new Journals(tmp.resolve("run-bad"))
            .row(RunFold.WR, 0, K1, Journals.body("complete"), Canonical.digest(Journals.body("provisional")), 1, 0, Journals.PINS)
            .progress(RunFold.WR, 0, 1, 1000, 0);
        try (RunFold f = j.fold(1)) { assertEquals(1, f.findings.count("J-NO-EQUAL-CONSECUTIVE"), f.findings.describe()); }
    }

    @Test void aWrongSetHashFails() throws Exception {
        Path oracle = tmp.resolve("oracle");
        oracle(oracle, CUT2);
        cut2(oracle, Map.of(T1, Journals.body("complete"), T2, Journals.body("provisional")), List.of(), 2, "0".repeat(64), Map.of());
        Comparison.Tally t = compare(run(), oracle);
        assertFalse(t.equal());
        assertEquals(1, t.differences, t.describe());
        assertTrue(says(t, "the oracle reports 2 and " + "0".repeat(64)), t.describe());
    }

    /** The order the oracle sorts keys in (Python's code-point order) is not Java's UTF-16 order above U+FFFF. */
    @Test void keysSortByCodePoint() {
        String bmp = "～", astral = new String(Character.toChars(0x1F600));
        assertTrue(bmp.compareTo(astral) > 0, "UTF-16 order puts the surrogate pair first");
        assertTrue(Comparison.codePointOrder(bmp, astral) < 0);
        assertTrue(Comparison.codePointOrder("ab", "abc") < 0);
        assertEquals(0, Comparison.codePointOrder("ab", "ab"));
    }

    /** A horizon recording's table has hundreds of thousands of keys; the stream reader does not intern them (Jackson's symbol table
     *  would overflow). */
    @Test void aCutFileWithManyKeysStreams() throws Exception {
        Path f = tmp.resolve("many.json");
        StringBuilder b = new StringBuilder("{");
        int n = 200_000;
        for (int i = 0; i < n; i++) b.append(i == 0 ? "" : ",").append("\"{\\\"seq\\\":").append(i).append("}\":{\"body\":{\"v\":").append(i).append("}}");
        Files.writeString(f, b.append("}").toString());
        int[] seen = {0};
        Comparison.stream(f, (k, v) -> seen[0]++);
        assertEquals(n, seen[0]);
        Path t = tmp.resolve("table.json");
        StringBuilder u = new StringBuilder("{\"cutId\":1,\"previousCutId\":null,\"table\":\"x\",\"mode\":\"snapshot\",\"count\":" + n + ",\"setHash\":\"" + "0".repeat(64) + "\",\"deletes\":[],\"upserts\":{");
        for (int i = 0; i < n; i++) u.append(i == 0 ? "" : ",").append("\"{\\\"seq\\\":").append(i).append("}\":{\"body\":{\"v\":").append(i).append("},\"bodyDigest\":\"x\"}");
        Files.writeString(t, u.append("}}").toString());
        int[] upserts = {0};
        assertNull(Comparison.parseTable(t, new Comparison.Head(), (k, v) -> upserts[0]++));
        assertEquals(n, upserts[0]);
    }

    /** JCS prints the double 2^53 as {@code 9007199254740992}, an integer literal outside the JCS integer domain: the journal reader takes
     *  it back as the double, so the digest recomputes to the one the harness wrote, and equals the oracle's for the same value. */
    @Test void aBodyDoubleOf2To53RecomputesItsDigest() throws Exception {
        ObjectNode body = Journals.body("complete");
        body.set("max", DoubleNode.valueOf(9007199254740992.0));
        String canonical = Canonical.utf8(Canonical.bytes(body)), digest = Canonical.digest(body);
        assertTrue(canonical.contains("\"max\":9007199254740992"), canonical);
        JsonNode strict = Json.read(canonical.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThrows(RuntimeException.class, () -> Canonical.digest(strict), "the strict reader alone cannot digest it");
        assertEquals(digest, Canonical.digest(RunFold.readLine(canonical)));
        Journals j = new Journals(tmp.resolve("run53")).row(RunFold.WR, 0, K1, body, 1, 0).progress(RunFold.WR, 0, 1, 1000, 0);
        Path oracle = tmp.resolve("oracle53");
        Journals.oracleCut(oracle.resolve("cut-1"), 1, null, Map.of(), Map.of());
        // the oracle (Python) prints the same double as 9007199254740992.0
        String set = Comparison.setHash(new TreeMap<>(Map.of(T1, digest)));
        Files.writeString(oracle.resolve("cut-1").resolve(RunFold.WR + ".json"), "{\"cutId\":1,\"previousCutId\":null,\"table\":\"" + RunFold.WR
            + "\",\"mode\":\"snapshot\",\"count\":1,\"setHash\":\"" + set + "\",\"deletes\":[],\"upserts\":{\n\"" + T1.replace("\"", "\\\"") + "\":{\"body\":"
            + canonical.replace("9007199254740992", "9007199254740992.0") + ",\"bodyDigest\":\"" + digest + "\"}\n}}\n");
        Comparison.Tally t = new Comparison.Tally();
        try (RunFold f = new RunFold(j.run, Journals.RULES_DIGEST, 1, true)) {
            f.advance(1);
            Comparison.compareCut(f, 1, oracle.resolve("cut-1"), new Comparison.OracleFold(), "test", Json.object(), t);
            f.finish();
            assertTrue(f.findings.clean(), f.findings.describe());
            assertEquals(digest, f.live(RunFold.WR).get(T1).digest());
        }
        assertTrue(t.equal(), t.describe());
    }
}
