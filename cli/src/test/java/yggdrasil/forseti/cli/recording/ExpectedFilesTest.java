package yggdrasil.forseti.cli.recording;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;

/** {@code expected.json} version 2 (samples in chunk files) against version 1: the same truth read back, the chunk boundaries, and
 *  the refusals. */
class ExpectedFilesTest {
    @TempDir Path tmp;

    /** A version-1 truth with {@code n} samples, fields in the order {@link GroundTruth} writes them. */
    static ObjectNode truth(int n) {
        ObjectNode e = Json.object();
        Json.putSafe(e, "expectedVersion", 1);
        e.put("scope", "full").put("mode", "occupancy-join");
        e.putArray("visits").addObject().put("visitRef", "visit-1");
        ObjectNode s = e.putObject("samples");
        for (int i = 0; i < n; i++) {
            ObjectNode v = Json.object().put("attributedTo", i % 2 == 0 ? "visit-1" : null).put("state", "settled").put("reason", "accepted");
            s.set(ScenarioBuilder.sampleKey("edge-line2", "dev:" + (i % 3), 1000, "Torque", 0, 2000 - i, 5000 + i), v);
        }
        e.putObject("dispositions");
        e.putObject("messageMemberDispositions").put("topic", "ot.messages");
        e.putObject("incidents");
        e.putArray("transitions");
        Json.putSafe(e, "settleTargetW", 42);
        e.putNull("settledCut");
        return e;
    }

    /** {@link #truth} as a version-1 file of it reads back. */
    static JsonNode parsed(int n) { return Json.read(Json.pretty(truth(n))); }

    Path write(String name, Map<String, byte[]> files) throws Exception {
        Path dir = tmp.resolve(name);
        for (Map.Entry<String, byte[]> f : files.entrySet()) {
            Path p = dir.resolve(f.getKey());
            Files.createDirectories(p.getParent());
            Files.write(p, f.getValue());
        }
        return dir;
    }

    Path v2(String name, int samples, int per) throws Exception { return write(name, ExpectedFiles.split(truth(samples), per)); }

    static String refusal(Path dir) { return assertThrows(IllegalArgumentException.class, () -> ExpectedFiles.read(dir)).getMessage(); }

    @Test
    void versionTwoReadsBackAsTheVersionOneTruthByteForByte() throws Exception {
        ObjectNode v1 = truth(7);
        TreeMap<String, byte[]> files = ExpectedFiles.split(v1, 3);
        assertEquals(List.of("expected-samples/0000.json", "expected-samples/0001.json", "expected-samples/0002.json", "expected.json"), List.copyOf(files.keySet()));
        JsonNode head = Json.read(files.get("expected.json"));
        assertEquals(2, head.get("expectedVersion").intValue());
        assertFalse(head.has("samples"));
        assertEquals("[\"expected-samples/0000.json\",\"expected-samples/0001.json\",\"expected-samples/0002.json\"]", head.get("sampleFiles").toString());
        assertEquals(List.of("expectedVersion", "scope", "mode", "visits", "sampleFiles", "samplesPerFile", "sampleCount", "dispositions"),
            List.copyOf(fieldNames(head)).subList(0, 8));
        JsonNode back = ExpectedFiles.read(write("v2", files));
        assertEquals(parsed(7), back);
        assertArrayEquals(Json.pretty(v1), Json.pretty(back), "same fields in the same order");
        assertEquals(List.copyOf(fieldNames(v1.get("samples"))), List.copyOf(fieldNames(back.get("samples"))), "samples in the version-1 order");
        // deterministic: the same truth gives the same bytes
        TreeMap<String, byte[]> again = ExpectedFiles.split(truth(7), 3);
        for (String f : files.keySet()) assertArrayEquals(files.get(f), again.get(f), f);
        for (byte[] b : files.values()) { assertFalse(new String(b, java.nio.charset.StandardCharsets.UTF_8).contains("\r")); assertEquals('\n', b[b.length - 1]); }
    }

    @Test
    void versionOneIsReadAsWritten() throws Exception {
        ObjectNode v1 = truth(4);
        Path dir = write("v1", Map.of("expected.json", Json.pretty(v1)));
        assertEquals(parsed(4), ExpectedFiles.read(dir));
    }

    @Test
    void chunkBoundaries() throws Exception {
        for (int[] c : new int[][] {{0, 0}, {1, 1}, {5, 1}, {6, 2}, {10, 2}, {11, 3}}) {
            TreeMap<String, byte[]> files = ExpectedFiles.split(truth(c[0]), 5);
            assertEquals(c[1] + 1, files.size(), c[0] + " samples");
            Path dir = write("b" + c[0], files);
            assertEquals(parsed(c[0]), ExpectedFiles.read(dir), c[0] + " samples");
            assertEquals(c[0], ExpectedFiles.read(dir).get("samples").size());
            assertEquals(c[1] > 0, Files.exists(dir.resolve(ExpectedFiles.SAMPLE_DIR)));
        }
    }

    @Test
    void aMissingOrExtraChunkIsRefused() throws Exception {
        Path missing = v2("missing", 11, 5);
        Files.delete(missing.resolve("expected-samples/0001.json"));
        assertTrue(refusal(missing).contains("differ from sampleFiles"), refusal(missing));

        Path extra = v2("extra", 11, 5);
        Files.copy(extra.resolve("expected-samples/0002.json"), extra.resolve("expected-samples/0003.json"));
        assertTrue(refusal(extra).contains("differ from sampleFiles"), refusal(extra));

        Path v1With = write("v1-with", Map.of("expected.json", Json.pretty(truth(2)), "expected-samples/0000.json", "{}\n".getBytes()));
        assertTrue(refusal(v1With).contains("version-1 expected.json with expected-samples/"), refusal(v1With));
    }

    @Test
    void chunksOutOfOrderOverlappingOrMalformedAreRefused() throws Exception {
        Path swapped = v2("swapped", 10, 5);
        byte[] a = Files.readAllBytes(swapped.resolve("expected-samples/0000.json")), b = Files.readAllBytes(swapped.resolve("expected-samples/0001.json"));
        Files.write(swapped.resolve("expected-samples/0000.json"), b);
        Files.write(swapped.resolve("expected-samples/0001.json"), a);
        assertTrue(refusal(swapped).contains("out of order"), refusal(swapped));

        Path renamed = v2("renamed", 10, 5);
        ObjectNode head = (ObjectNode) Json.read(renamed.resolve("expected.json"));
        head.withArray("sampleFiles").set(0, Json.JSON.getNodeFactory().textNode("expected-samples/0001.json"));
        Files.write(renamed.resolve("expected.json"), Json.pretty(head));
        assertTrue(refusal(renamed).contains("sampleFiles[0]"), refusal(renamed));

        Path overlap = v2("overlap", 10, 5);
        ObjectNode c0 = (ObjectNode) Json.read(overlap.resolve("expected-samples/0000.json")), c1 = (ObjectNode) Json.read(overlap.resolve("expected-samples/0001.json"));
        String first = c0.get("samples").fieldNames().next(), dropped = c1.get("samples").fieldNames().next();
        ((ObjectNode) c1.get("samples")).remove(dropped);
        ((ObjectNode) c1.get("samples")).set(first, c0.get("samples").get(first));
        Files.write(overlap.resolve("expected-samples/0001.json"), Json.pretty(c1));
        assertTrue(refusal(overlap).contains("repeats sample"), refusal(overlap));

        Path shortChunk = v2("short", 10, 5);
        ObjectNode s0 = (ObjectNode) Json.read(shortChunk.resolve("expected-samples/0000.json"));
        ((ObjectNode) s0.get("samples")).remove(s0.get("samples").fieldNames().next());
        Files.write(shortChunk.resolve("expected-samples/0000.json"), Json.pretty(s0));
        assertTrue(refusal(shortChunk).contains("holds 4 samples"), refusal(shortChunk));

        Path notObject = v2("not-object", 3, 5);
        Files.write(notObject.resolve("expected-samples/0000.json"), "[]\n".getBytes());
        assertTrue(refusal(notObject).contains("is not {chunk, samples}"), refusal(notObject));

        Path count = v2("count", 3, 5);
        ObjectNode h = (ObjectNode) Json.read(count.resolve("expected.json"));
        Json.putSafe(h, "sampleCount", 4);
        Files.write(count.resolve("expected.json"), Json.pretty(h));
        assertTrue(refusal(count).contains("sampleCount 4"), refusal(count));

        Path inline = v2("inline", 3, 5);
        ObjectNode i = (ObjectNode) Json.read(inline.resolve("expected.json"));
        i.putObject("samples");
        Files.write(inline.resolve("expected.json"), Json.pretty(i));
        assertTrue(refusal(inline).contains("inline"), refusal(inline));

        Path v3 = v2("v3", 3, 5);
        ObjectNode x = (ObjectNode) Json.read(v3.resolve("expected.json"));
        Json.putSafe(x, "expectedVersion", 3);
        Files.write(v3.resolve("expected.json"), Json.pretty(x));
        assertTrue(refusal(v3).contains("expectedVersion 3"), refusal(v3));
    }

    @Test
    void onlyAVersionOneTruthWithInlineSamplesIsSplit() {
        ObjectNode runs = truth(1);
        runs.remove("samples");
        runs.put("samplesEncoding", ExpectedSamples.RUNS);
        assertThrows(IllegalArgumentException.class, () -> ExpectedFiles.split(runs, 5));
        assertThrows(IllegalArgumentException.class, () -> ExpectedFiles.split(truth(1), 0));
    }

    static List<String> fieldNames(JsonNode n) {
        List<String> l = new ArrayList<>();
        n.fieldNames().forEachRemaining(l::add);
        return l;
    }
}
