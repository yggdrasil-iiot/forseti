package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** {@link Comparison}: the folded journals of a hand-written run against hand-written oracle cut files. */
class ComparisonTest {
    @TempDir Path tmp;

    static final ObjectNode K1 = Journals.key("occ-1"), K2 = Journals.key("occ-3");

    /** Cut 1: occ-1 provisional; cut 2: occ-1 complete, occ-3 provisional. */
    Journals run() throws Exception {
        return new Journals(tmp.resolve("run"))
            .row(RunFold.WR, 0, K1, Journals.body("provisional"), 1, 0)
            .progress(RunFold.WR, 0, 1, 1000, 0)
            .row(RunFold.WR, 0, K1, Journals.body("complete"), 2, 1)
            .row(RunFold.WR, 0, K2, Journals.body("provisional"), 1, 1)
            .progress(RunFold.WR, 0, 2, 2000, 1);
    }

    Comparison.Tally compare(Journals j, Path oracle, Path diffs) throws Exception {
        Comparison.Tally t = new Comparison.Tally();
        try (RunFold f = new RunFold(j.run, Journals.RULES_DIGEST, 1, true)) {
            for (long k = 1; k <= 2; k++) {
                f.advance(k);
                Comparison.compareCut(f, k, oracle.resolve("cut-" + k), "test", diffs, Json.object(), t);
            }
            f.finish();
            assertTrue(f.findings.clean(), f.findings.describe());
        }
        return t;
    }

    void oracle(Path oracle, JsonNode cut2Occ1, Map<JsonNode, String> digests) throws Exception {
        Journals.oracle(oracle.resolve("cut-1"), RunFold.WR, Map.of(K1, Journals.body("provisional")), Map.of());
        Journals.oracle(oracle.resolve("cut-2"), RunFold.WR, Map.of(K1, cut2Occ1, K2, Journals.body("provisional")), digests);
    }

    @Test void agreeingOutputsPass() throws Exception {
        Path oracle = tmp.resolve("oracle"), diffs = tmp.resolve("diffs");
        oracle(oracle, Journals.body("complete"), Map.of());
        Comparison.Tally t = compare(run(), oracle, diffs);
        assertTrue(t.equal(), t.describe());
        assertEquals(2, t.cuts);
        assertEquals(3, t.rowsCompared);
        assertFalse(Files.exists(diffs), "no difference, no diff directory");
    }

    @Test void aBodyChangedInTheOracleFailsWithADiffFile() throws Exception {
        Path oracle = tmp.resolve("oracle"), diffs = tmp.resolve("diffs");
        oracle(oracle, Journals.body("insufficient"), Map.of());
        Comparison.Tally t = compare(run(), oracle, diffs);
        assertFalse(t.equal());
        assertEquals(1, t.differences, t.describe());
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

    @Test void aReportedDigestThatIsNotItsBodysFails() throws Exception {
        Path oracle = tmp.resolve("oracle");
        // the oracle's body equals the core's, but the digest it reports is not that body's
        oracle(oracle, Journals.body("complete"), Map.of(K1, Canonical.digest(Journals.body("provisional"))));
        Comparison.Tally t = compare(run(), oracle, tmp.resolve("diffs"));
        assertFalse(t.equal());
        assertEquals(1, t.oracleDigestMismatches, t.describe());
        // and on the core's side: a journal row whose bodyDigest is not its body's is an integrity finding
        Journals j = new Journals(tmp.resolve("run-bad"))
            .row(RunFold.WR, 0, K1, Journals.body("complete"), Canonical.digest(Journals.body("provisional")), 1, 0, Journals.PINS)
            .progress(RunFold.WR, 0, 1, 1000, 0);
        try (RunFold f = j.fold(1)) { assertEquals(1, f.findings.count("J-NO-EQUAL-CONSECUTIVE"), f.findings.describe()); }
    }

    /** A horizon recording's cut file has hundreds of thousands of keys; the stream reader does not intern them (Jackson's symbol
     *  table would overflow). */
    @Test void aCutFileWithManyKeysStreams() throws Exception {
        Path f = tmp.resolve("many.json");
        StringBuilder b = new StringBuilder("{");
        int n = 200_000;
        for (int i = 0; i < n; i++) b.append(i == 0 ? "" : ",").append("\"{\\\"seq\\\":").append(i).append("}\":{\"body\":{\"v\":").append(i).append("}}");
        Files.writeString(f, b.append("}").toString());
        int[] seen = {0};
        Comparison.stream(f, (k, v) -> seen[0]++);
        assertEquals(n, seen[0]);
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
        // the oracle (Python) prints the same double as 9007199254740992.0
        Files.createDirectories(oracle.resolve("cut-1"));
        Files.writeString(oracle.resolve("cut-1").resolve(RunFold.WR + ".json"), "{\"" + Canonical.utf8(Canonical.bytes(K1)).replace("\"", "\\\"") + "\": {\"body\": "
            + canonical.replace("9007199254740992", "9007199254740992.0") + ", \"bodyDigest\": \"" + digest + "\"}}\n");
        Comparison.Tally t = new Comparison.Tally();
        try (RunFold f = new RunFold(j.run, Journals.RULES_DIGEST, 1, true)) {
            f.advance(1);
            Comparison.compareCut(f, 1, oracle.resolve("cut-1"), "test", tmp.resolve("diffs53"), Json.object(), t);
            f.finish();
            assertTrue(f.findings.clean(), f.findings.describe());
            assertEquals(digest, f.live(RunFold.WR).get(Canonical.utf8(Canonical.bytes(K1))).digest());
        }
        assertTrue(t.equal(), t.describe());
    }
}
