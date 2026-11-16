package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.cli.recording.CaptureTruth;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;

/** The recorder on a fake consumer (no broker): a committed step-3 smoke recording is replayed into the recorder's input model as if
 *  read from Kafka, with append times taken from the records' own delivery fields, and recorded back. */
class CaptureRecorderTest {
    static final Path FIXTURES = Path.of("..", "fixtures"), SMOKE = FIXTURES.resolve("recordings").resolve("smoke"), REGISTRY = FIXTURES.resolve("registry");
    static final String PYTHON = System.getProperty("forseti.python", "python");
    static final String PREFIX = "run-7.";
    static final String NR1 = "synthetic-site:line2:station1:nutrunner1";

    @TempDir Path tmp;

    /** A broker's view of a run: per topic name and partition its records, and optionally a log start that is not 0. */
    static final class FakeSource implements CaptureRecorder.Source {
        final Map<String, List<CaptureRecorder.Raw>> parts = new HashMap<>();
        final Map<String, Long> begin = new HashMap<>();
        List<CaptureRecorder.Raw> of(String topic, int p) { return parts.computeIfAbsent(topic + "/" + p, k -> new ArrayList<>()); }
        public long beginningOffset(String topic, int partition) { return begin.getOrDefault(topic + "/" + partition, 0L); }
        public long endOffset(String topic, int partition) { List<CaptureRecorder.Raw> l = parts.get(topic + "/" + partition); return l == null ? 0 : l.size(); }
        public List<CaptureRecorder.Raw> read(String topic, int partition, long end) { return List.copyOf(parts.getOrDefault(topic + "/" + partition, List.of())); }
    }

    /** The append time a broker would have given the record: its bridge receipt, record time or birth, never going back. */
    static long appendTime(String topic, JsonNode v) {
        return switch (topic) {
            case "ot.messages" -> v.get("bridgeReceivedAt").longValue();
            case "ot.sessions" -> v.get("birthPublishTs").longValue();
            case "mfg.occupancy" -> v.get("recordTime").longValue();
            default -> v.get("detectedAt").longValue();
        };
    }

    /** {@code recording} as the broker of a run with {@link #PREFIX} would hold it; {@code edit} rewrites value bytes. */
    static FakeSource source(Path recording, UnaryOperator<String> edit) {
        Recording rec = Recording.open(recording);
        FakeSource s = new FakeSource();
        for (String topic : RoutingManifest.TOPICS) for (int p : rec.routing().subscribed(topic)) {
            long last = 0;
            try (Recording.Reader r = rec.reader(topic, p)) {
                for (Recording.Line l; (l = r.next()) != null;) {
                    byte[] v = edit.apply(new String(Recording.valueBytes(l.bytes()), StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
                    last = Math.max(last, appendTime(topic, Json.read(v)));
                    s.of(LiveTopics.name(PREFIX, topic), p).add(new CaptureRecorder.Raw(l.record().offset(), last, "LogAppendTime", l.record().key(), Map.of(), v));
                }
            }
        }
        return s;
    }

    static CaptureRecorder.Inputs inputs(Path recording, List<JsonNode> sent) {
        Recording rec = Recording.open(recording);
        byte[] routing;
        try { routing = Files.readAllBytes(recording.resolve(Recording.ROUTING)); } catch (java.io.IOException e) { throw new IllegalStateException(e); }
        Map<String, CaptureTruth.Visit> facts = Map.of("occ-1", new CaptureTruth.Visit("visit-1", "occ-1", List.of(NR1)));
        return new CaptureRecorder.Inputs("smoke-capture", REGISTRY, rec.manifest().get("rule"), (ObjectNode) rec.manifest().get("definitions"), routing,
            rec.t0(), rec.scanMs(), 30_000, "mes-line2", facts, sent, Json.object().put("scenario", "smoke"));
    }

    static List<JsonNode> occupancy(Path recording) {
        Recording rec = Recording.open(recording);
        List<JsonNode> l = new ArrayList<>();
        for (int p : rec.routing().subscribed("mfg.occupancy")) for (Recording.Record r : rec.records("mfg.occupancy", p)) l.add(r.value());
        return l;
    }

    Path record(FakeSource src, CaptureRecorder.Inputs in, String name) {
        Path out = tmp.resolve(name);
        CaptureRecorder.record(src, PREFIX, in, out, CaptureRecorder.besides(out, CaptureRecorder.REPORT_SUFFIX), CaptureRecorder.besides(out, CaptureRecorder.DEAD_LETTER_SUFFIX));
        return out;
    }

    @Test
    void aSmokeRecordingRoundTripsThroughTheRecorder() throws Exception {
        Path out = record(source(SMOKE, v -> v), inputs(SMOKE, occupancy(SMOKE)), "smoke-capture");
        Recording orig = Recording.open(SMOKE), cap = Recording.open(out);
        cap.verifyDigests();
        cap.verifyBatches();
        assertEquals(List.of(), CaptureLock.check(out, REGISTRY));
        assertFalse(cap.manifest().get("synthetic").booleanValue());
        assertEquals(orig.manifest().get("definitions"), cap.manifest().get("definitions"));
        for (String topic : RoutingManifest.TOPICS) for (int p : orig.routing().subscribed(topic)) {
            List<byte[]> a = new ArrayList<>(), b = new ArrayList<>();
            try (Recording.Reader r = orig.reader(topic, p)) { for (Recording.Line l; (l = r.next()) != null;) a.add(Recording.valueBytes(l.bytes())); }
            List<Recording.Record> rs = cap.records(topic, p);
            try (Recording.Reader r = cap.reader(topic, p)) { for (Recording.Line l; (l = r.next()) != null;) b.add(Recording.valueBytes(l.bytes())); }
            assertEquals(a.size(), b.size(), topic + "/" + p);
            for (int i = 0; i < a.size(); i++) {
                assertArrayEquals(a.get(i), b.get(i), topic + "/" + p + " value " + i);
                assertEquals(i, rs.get(i).offset());
                assertEquals(orig.records(topic, p).get(i).key(), rs.get(i).key());
                assertNotNull(rs.get(i).timestamp());
            }
        }
        // the settlement of visit-1 (leave + L) is bracketed on consecutive cuts at station1
        assertEquals(1, cap.deadlines().size());
        Recording.Deadline d = cap.deadlines().get(0);
        assertEquals("visit-settlement", d.kind());
        assertEquals("visit-1", d.ref());
        assertEquals(orig.deadlines().get(0).at(), d.at());
        assertEquals(d.cutA() + 1, d.cutB());
        JsonNode expected = cap.expected();
        assertEquals("full", expected.get("scope").asText());
        assertEquals("visit-1", expected.get("visits").get(0).get("visitRef").asText());
        assertEquals("complete", expected.get("visits").get(0).get("finalStatus").asText());
        assertEquals(5684, expected.get("samples").size());
        assertEquals(1, expected.get("expectedVersion").intValue(), "read back in the version-1 form");
        JsonNode t = expected.get("transitions").get(0);
        assertTrue(t.get("w1A").asLong() < d.at() && d.at() <= t.get("w1B").asLong());
        // the side files sit next to the recording, not in it
        JsonNode report = Json.read(CaptureRecorder.besides(out, CaptureRecorder.REPORT_SUFFIX));
        assertEquals(4060, report.path("rates").path("messages").asLong());
        assertEquals(0, report.path("rates").path("incoherentSnapshots").asLong());
        assertEquals(4060, report.path("counts").path("ot.messages").asLong());
        assertEquals(0, Files.size(CaptureRecorder.besides(out, CaptureRecorder.DEAD_LETTER_SUFFIX)));
        oracleReads(out, 4060, 1);
    }

    @Test
    void aFiveEdgeRecordingPassesTheCoreAndTheOracleReader() throws Exception {
        UnaryOperator<String> fiveEdges = v -> {
            String device = Json.read(v.getBytes(StandardCharsets.UTF_8)).path("device").asText();
            return device.isEmpty() ? v : v.replace("\"edge\":\"edge-line2\"", "\"edge\":\"edge-" + device.substring(device.indexOf(':') + 1).replace(':', '-') + "\"");
        };
        Path out = record(source(SMOKE, fiveEdges), inputs(SMOKE, occupancy(SMOKE)), "five-edges");
        Recording cap = Recording.open(out);
        cap.verifyDigests();
        Set<String> edges = new TreeSet<>();
        for (String topic : List.of(InputParser.MESSAGES, InputParser.SESSIONS)) for (int p : cap.routing().subscribed(topic))
            try (Recording.Reader r = cap.reader(topic, p)) {
                for (Recording.Line l; (l = r.next()) != null;) {
                    InputParser.Read read = InputParser.read(new Inputs.Origin(topic, p, l.record().offset()), l.record().key(), Recording.valueBytes(l.bytes()));
                    assertFalse(read.rejected(), topic + "/" + p + " " + l.record().offset());
                    edges.add(read.record().parsed() instanceof Inputs.ProcessMessage m ? m.edge() : ((Inputs.SessionEvent) read.record().parsed()).edge());
                }
            }
        assertEquals(5, edges.size(), edges.toString());
        assertEquals(List.of(), CaptureLock.check(out, REGISTRY));
        assertTrue(cap.expected().get("samples").fieldNames().next().contains("\"edge\":\"edge-line2-"));
        oracleReads(out, 4060, 5);
    }

    /** A capture writes {@code expected.json} version 2: the samples in {@code expected-samples/}, each chunk in the manifest's digests
     *  and the lock, so a changed chunk byte is caught as a changed {@code expected.json} byte is. */
    @Test
    void aCaptureWritesExpectedVersionTwoAndItsLockCoversTheChunks() throws Exception {
        Path out = record(source(SMOKE, v -> v), inputs(SMOKE, occupancy(SMOKE)), "v2-capture");
        String chunk = "expected-samples/0000.json";
        JsonNode head = Json.read(out.resolve(Recording.EXPECTED));
        assertEquals(2, head.get("expectedVersion").intValue());
        assertFalse(head.has("samples"));
        assertEquals("[\"" + chunk + "\"]", head.get("sampleFiles").toString());
        assertEquals(5684, head.get("sampleCount").longValue());
        Recording cap = Recording.open(out);
        assertTrue(cap.digests().containsKey(chunk));
        assertEquals(5684, cap.expected().get("samples").size());
        assertEquals(List.of(), CaptureLock.check(out, REGISTRY));
        ObjectNode lock = CaptureLock.lock(out);
        assertTrue(lock.path("files").has(chunk));
        byte[] committed = Json.pretty(lock);

        // one byte of the chunk changed: the digest check fails; with the manifest's digest rewritten, the lock still differs
        Path file = out.resolve(chunk);
        byte[] original = Files.readAllBytes(file);
        String text = new String(original, StandardCharsets.UTF_8);
        int at = text.indexOf("\"visit-1\"");
        assertTrue(at > 0);
        byte[] tampered = original.clone();
        tampered[at + 7] = (byte) '2';
        Files.write(file, tampered);
        assertTrue(String.join("|", CaptureLock.check(out, REGISTRY)).contains("digest mismatch: " + chunk), CaptureLock.check(out, REGISTRY).toString());
        ObjectNode manifest = (ObjectNode) Json.read(out.resolve(Recording.MANIFEST));
        ((ObjectNode) manifest.get("digests")).put(chunk, Canonical.digestOf(tampered));
        Files.write(out.resolve(Recording.MANIFEST), Json.pretty(manifest));
        List<String> diff = CaptureLock.compare(CaptureLock.lock(out), committed);
        assertTrue(diff.stream().anyMatch(d -> d.startsWith("file " + chunk + ":")), diff.toString());

        // the chunk missing: refused by the digest check and by the reader
        Files.delete(file);
        assertTrue(String.join("|", CaptureLock.check(out, REGISTRY)).contains("differ from the manifest's digests"), CaptureLock.check(out, REGISTRY).toString());
        assertThrows(IllegalArgumentException.class, () -> Recording.open(out).expected());
    }

    /** The oracle's recording reader opens the capture, verifies its digests, reads every partition, and its input reader accepts
     *  every message and session event; prints the message count and the distinct edges. */
    void oracleReads(Path recording, long messages, int edges) throws Exception {
        Path script = tmp.resolve("read_capture.py");
        Files.writeString(script, String.join("\n",
            "import json, sys",
            "from forseti_oracle import canon, inputs, recording",
            "rec = recording.open_recording(sys.argv[1])",
            "rec.verify_digests()",
            "n, edges, bad = 0, set(), 0",
            "for topic in recording.TOPICS:",
            "    for p in rec.subscribed(topic):",
            "        raw = (rec.path / topic / f'{p}.jsonl').read_bytes()",
            "        for r in rec.records(topic, p):",
            "            pass",
            "        if topic in ('ot.messages', 'ot.sessions'):",
            "            for line in raw.split(b'\\n')[:-1]:",
            "                start = line.index(b'\"value\":') + len(b'\"value\":')",
            "                x = inputs.read(topic, line[start:-1])",
            "                bad += 1 if x.rejected else 0",
            "                if not x.rejected:",
            "                    edges.add(x.parsed.edge)",
            "                    n += 1 if topic == 'ot.messages' else 0",
            "json.dump({'messages': n, 'edges': len(edges), 'rejected': bad, 'expected': canon.digest(rec.expected())}, sys.stdout)", ""), StandardCharsets.UTF_8);
        Process p = new ProcessBuilder(PYTHON, "-I", script.toString(), recording.toAbsolutePath().toString()).start();
        byte[] stdout = p.getInputStream().readAllBytes();
        String stderr = new String(p.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(120, TimeUnit.SECONDS));
        assertEquals(0, p.exitValue(), stderr);
        JsonNode o = Json.read(stdout);
        assertEquals(messages, o.get("messages").asLong());
        assertEquals(edges, o.get("edges").asInt());
        assertEquals(0, o.get("rejected").asInt());
        assertEquals(Canonical.digest(Recording.open(recording).expected()), o.get("expected").asText(), "the oracle reads the same truth");
    }

    static int main(String... args) {
        ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
        return Main.run(args, new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8));
    }

    static void copy(Path from, Path to) throws Exception {
        try (var w = Files.walk(from)) {
            for (Path p : w.toList()) {
                Path q = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(q); else Files.copy(p, q);
            }
        }
    }

    /** Rewrites the manifest's digest of one file to the file's current bytes, as a tool that "repairs" a recording would. */
    static void redigest(Path dir, String file) throws Exception {
        ObjectNode m = (ObjectNode) Json.read(dir.resolve(Recording.MANIFEST));
        ((ObjectNode) m.get("digests")).put(file, yggdrasil.forseti.core.canon.Canonical.digestOf(Files.readAllBytes(dir.resolve(file))));
        Files.write(dir.resolve(Recording.MANIFEST), Json.pretty(m));
    }

    @Test
    void verifyCaptureLocksAFreshCaptureAndRefusesDamage() throws Exception {
        Path out = record(source(SMOKE, v -> v), inputs(SMOKE, occupancy(SMOKE)), "capture");
        Path lock = tmp.resolve("capture.lock.json");
        assertEquals(1, main("recordings", "verify-capture", out.toString(), "--lock", lock.toString(), "--registry", REGISTRY.toString()), "no lock yet");
        assertEquals(0, main("recordings", "verify-capture", out.toString(), "--lock", lock.toString(), "--registry", REGISTRY.toString(), "--write-lock"));
        assertEquals(0, main("recordings", "verify-capture", out.toString(), "--lock", lock.toString(), "--registry", REGISTRY.toString()));
        assertEquals(4060, Json.read(lock).path("counts").path("ot.messages").asLong());
        assertEquals(2, main("recordings", "verify-capture", "--lock", lock.toString(), "--registry", REGISTRY.toString()));
        assertEquals(2, main("recordings", "verify-capture", out.toString(), "--lock", lock.toString()), "--registry is required");
        assertEquals(2, main("recordings", "verify-capture", out.toString(), "--lock", lock.toString(), "--registry", REGISTRY.toString(), "--bogus", "x"));

        String part = "ot.messages/1.jsonl";
        byte[] bytes = Files.readAllBytes(out.resolve(part));
        int lastLf = lastIndexOf(bytes, (byte) '\n', bytes.length - 2);

        Path truncated = tmp.resolve("truncated");
        copy(out, truncated);
        Files.write(truncated.resolve(part), Arrays.copyOf(bytes, lastLf + 1));   // the last whole line gone
        assertEquals(1, main("recordings", "verify-capture", truncated.toString(), "--lock", lock.toString(), "--registry", REGISTRY.toString()));
        redigest(truncated, part);   // even with the manifest's digest rewritten: the lock pins the manifest and the record count
        assertEquals(1, main("recordings", "verify-capture", truncated.toString(), "--lock", lock.toString(), "--registry", REGISTRY.toString()));
        assertFalse(CaptureLock.compare(CaptureLock.lock(truncated), Files.readAllBytes(lock)).isEmpty());

        Path corrupt = tmp.resolve("corrupt");
        copy(out, corrupt);
        byte[] c = bytes.clone();
        c[lastLf + 20] = (byte) (c[lastLf + 20] == '1' ? '2' : '1');
        Files.write(corrupt.resolve(part), c);
        assertEquals(1, main("recordings", "verify-capture", corrupt.toString(), "--lock", lock.toString(), "--registry", REGISTRY.toString()));
        assertFalse(CaptureLock.check(corrupt, REGISTRY).isEmpty());

        Path missing = tmp.resolve("missing");
        copy(out, missing);
        Files.delete(missing.resolve("ot.sessions/2.jsonl"));
        assertEquals(1, main("recordings", "verify-capture", missing.toString(), "--lock", lock.toString(), "--registry", REGISTRY.toString()));
        assertTrue(CaptureLock.check(missing, REGISTRY).stream().anyMatch(p -> p.contains("ot.sessions/2")), CaptureLock.check(missing, REGISTRY).toString());

        Path tail = tmp.resolve("tail");
        copy(out, tail);
        Files.write(tail.resolve(part), Arrays.copyOf(bytes, bytes.length - 7));   // a partial last line, no LF
        redigest(tail, part);
        List<String> problems = CaptureLock.check(tail, REGISTRY);
        assertTrue(problems.stream().anyMatch(p -> p.contains("does not end with LF")), problems.toString());
        assertEquals(1, main("recordings", "verify-capture", tail.toString(), "--lock", lock.toString(), "--registry", REGISTRY.toString()));
    }

    static int lastIndexOf(byte[] b, byte x, int from) {
        for (int i = from; i >= 0; i--) if (b[i] == x) return i;
        return -1;
    }

    static void refused(String fragment, Runnable r) {
        CaptureRecorder.Refused e = assertThrows(CaptureRecorder.Refused.class, r::run);
        assertTrue(e.getMessage().contains(fragment), e.getMessage());
    }

    @Test
    void theRecorderRefusesWhatTheFormatCannotCarry() {
        CaptureRecorder.Inputs in = inputs(SMOKE, occupancy(SMOKE));
        String msg1 = LiveTopics.name(PREFIX, "ot.messages");

        FakeSource notFresh = source(SMOKE, v -> v);
        notFresh.begin.put(msg1 + "/1", 5L);
        refused("not a fresh topic", () -> record(notFresh, in, "a"));

        FakeSource gap = source(SMOKE, v -> v);
        List<CaptureRecorder.Raw> l = gap.of(msg1, 1);
        CaptureRecorder.Raw r = l.get(3);
        l.set(3, new CaptureRecorder.Raw(4, r.timestamp(), r.timestampType(), r.key(), r.headers(), r.value()));
        refused("offset 4 where 3 is due", () -> record(gap, in, "b"));

        FakeSource createTime = source(SMOKE, v -> v);
        r = createTime.of(msg1, 2).get(0);
        createTime.of(msg1, 2).set(0, new CaptureRecorder.Raw(0, r.timestamp(), "CreateTime", r.key(), r.headers(), r.value()));
        refused("not LogAppendTime", () -> record(createTime, in, "c"));

        FakeSource stray = source(SMOKE, v -> v);
        stray.of(msg1, 0).add(stray.of(msg1, 1).get(0));
        refused("does not subscribe", () -> record(stray, in, "d"));

        List<JsonNode> sent = new ArrayList<>(occupancy(SMOKE));
        sent.remove(1);
        refused("sent but not captured 0, captured but not sent 1", () -> record(source(SMOKE, v -> v), inputs(SMOKE, sent), "e"));

        // a capture whose telemetry stops before W1 reaches the settlement deadline cannot bracket it
        FakeSource shortTail = source(SMOKE, v -> v);
        long deadline = Recording.open(SMOKE).deadlines().get(0).at();
        for (int p : List.of(1, 2, 4, 5)) shortTail.of(msg1, p).removeIf(x -> Json.read(x.value()).get("members").get(0).get("sourceTs").longValue() > deadline);
        refused("is never reached", () -> record(shortTail, in, "f"));

        FakeSource headers = source(SMOKE, v -> v);
        r = headers.of(msg1, 4).get(0);
        headers.of(msg1, 4).set(0, new CaptureRecorder.Raw(0, r.timestamp(), r.timestampType(), r.key(), Map.of("x", new byte[] {1}), r.value()));
        refused("carries headers", () -> record(headers, in, "g"));
        assertFalse(Files.exists(tmp.resolve("a")) || Files.exists(tmp.resolve("f")), "a refused capture writes nothing");
    }

    static final String NR2 = "synthetic-site:line2:station1:nutrunner2", S1 = "synthetic-site:line2:station1";
    static final long T0 = 1794182400000L;

    /** The records of one partition kept by {@code keep} and rewritten by {@code edit}, renumbered from 0 (append times kept). */
    static void rewrite(FakeSource s, String topic, int p, java.util.function.Predicate<JsonNode> keep, UnaryOperator<ObjectNode> edit) {
        List<CaptureRecorder.Raw> l = s.of(topic, p), out = new ArrayList<>();
        for (CaptureRecorder.Raw r : l) {
            ObjectNode v = (ObjectNode) Json.read(r.value());
            if (!keep.test(v)) continue;
            byte[] b;
            try { b = Json.JSON.writeValueAsBytes(edit.apply(v)); } catch (Exception e) { throw new IllegalStateException(e); }
            out.add(new CaptureRecorder.Raw(out.size(), r.timestamp(), r.timestampType(), r.key(), r.headers(), b));
        }
        l.clear();
        l.addAll(out);
    }

    static long ts(JsonNode message) { return message.get("members").get(0).get("sourceTs").longValue(); }

    @Test
    void aStationWithoutRoutedTelemetryAtCutAIsRefusedByName() {
        FakeSource s = source(SMOKE, v -> v);
        String msg = LiveTopics.name(PREFIX, "ot.messages");
        // station1's devices deliver only their last scan: W1(station1) jumps from no telemetry to past the settlement deadline
        for (int p : List.of(1, 2, 4, 5)) rewrite(s, msg, p, v -> !v.get("device").asText().startsWith(S1) || ts(v) >= T0 + 1411000, v -> v);
        refused("station " + S1 + " has no routed telemetry at the cut before W1 reaches it", () -> record(s, inputs(SMOKE, occupancy(SMOKE)), "r1"));
    }

    @Test
    void aDeviceThatStopsEarlyIsRefusedAndAKilledAndRestartedOnePasses() {
        RoutingManifest routing = Recording.open(SMOKE).routing();
        int p2 = routing.rawPartitionOf(NR2);
        String msg = LiveTopics.name(PREFIX, "ot.messages"), ses = LiveTopics.name(PREFIX, "ot.sessions");

        FakeSource early = source(SMOKE, v -> v);
        rewrite(early, msg, p2, v -> !v.get("device").asText().equals(NR2) || ts(v) < T0 + 700000, v -> v);
        refused("the tail of device " + NR2 + " ends at " + (T0 + 699000), () -> record(early, inputs(SMOKE, occupancy(SMOKE)), "early"));

        // injection 8 as the live loop runs it: Muninn killed at T0 + 620000 and restarted with a new session at T0 + 650000 (the
        // session-up is a business event: a later restart would leave the smoke recording's tail short)
        FakeSource restarted = source(SMOKE, v -> v);
        long[] seq = {1};
        rewrite(restarted, msg, p2, v -> !v.get("device").asText().equals(NR2) || ts(v) < T0 + 620000 || ts(v) >= T0 + 650000, v -> {
            if (!v.get("device").asText().equals(NR2) || ts(v) < T0 + 650000) return v;
            v.put("birthPublishTs", T0 + 650000).put("bdSeq", 1).put("seq", ++seq[0]);
            return v;
        });
        List<CaptureRecorder.Raw> sl = restarted.of(ses, p2);
        String down = "{\"edge\":\"edge-line2\",\"device\":\"" + NR2 + "\",\"birthPublishTs\":" + T0 + ",\"bdSeq\":0,\"kind\":\"session-down\",\"atSeq\":21,"
            + "\"payload\":{\"from\":" + (T0 + 619000) + ",\"to\":null}}";
        String up = "{\"edge\":\"edge-line2\",\"device\":\"" + NR2 + "\",\"birthPublishTs\":" + (T0 + 650000) + ",\"bdSeq\":1,\"kind\":\"session-up\",\"atSeq\":1,"
            + "\"payload\":{\"at\":" + (T0 + 650000) + ",\"rebirth\":false}}";
        sl.add(new CaptureRecorder.Raw(sl.size(), T0 + 620120, "LogAppendTime", NR2, Map.of(), down.getBytes(StandardCharsets.UTF_8)));
        sl.add(new CaptureRecorder.Raw(sl.size(), T0 + 650120, "LogAppendTime", NR2, Map.of(), up.getBytes(StandardCharsets.UTF_8)));
        Path out = record(restarted, inputs(SMOKE, occupancy(SMOKE)), "restarted");
        assertEquals(List.of(), CaptureLock.check(out, REGISTRY));
        assertEquals(T0 + 1411000, Recording.open(out).evidenceRange().get(NR2).to());
    }

    @Test
    void verifyCaptureFailsATailShorterThanTheManifestsTailMs() throws Exception {
        Path out = record(source(SMOKE, v -> v), inputs(SMOKE, occupancy(SMOKE)), "tail-ms");
        ObjectNode m = (ObjectNode) Json.read(out.resolve(Recording.MANIFEST));
        Json.putSafe(m, "tailMs", 760_000);   // the manifest is not in its own digests
        Files.write(out.resolve(Recording.MANIFEST), Json.pretty(m));
        List<String> problems = CaptureLock.check(out, REGISTRY);
        assertTrue(problems.stream().anyMatch(p -> p.contains("the tail of raw partition") && p.contains("spec 628")), problems.toString());
        assertTrue(problems.stream().anyMatch(p -> p.contains("the tail of device " + NR2)), problems.toString());
    }

    @Test
    void inputsTheFixtureMustNotCarryAreRefusedWithTheirCountAndFirstOffset() {
        String msg = LiveTopics.name(PREFIX, "ot.messages"), ses = LiveTopics.name(PREFIX, "ot.sessions");
        CaptureRecorder.Inputs in = inputs(SMOKE, occupancy(SMOKE));
        int p = Recording.open(SMOKE).routing().rawPartitionOf(NR1);

        FakeSource rejected = source(SMOKE, v -> v);
        CaptureRecorder.Raw r = rejected.of(msg, p).get(5);
        rejected.of(msg, p).set(5, new CaptureRecorder.Raw(5, r.timestamp(), r.timestampType(), r.key(), r.headers(), "{\"edge\":\"x\"}".getBytes(StandardCharsets.UTF_8)));
        refused("1 message(s) the core's reader rejects, the first at ot.messages/" + p + " offset 5", () -> record(rejected, in, "rej"));

        FakeSource noTs = source(SMOKE, v -> v);
        rewrite(noTs, msg, p, v -> true, v -> {
            if (v.get("seq").asLong() >= 10 && v.get("seq").asLong() <= 11 && v.get("device").asText().equals(NR1)) ((ObjectNode) v.get("members").get(1)).remove("sourceTs");
            return v;
        });
        refused("2 kept member(s) without sourceTs, the first at ot.messages/" + p, () -> record(noTs, in, "nots"));

        FakeSource pin = source(SMOKE, v -> v);
        rewrite(pin, msg, p, v -> true, v -> v.get("seq").asLong() == 7 && v.get("device").asText().equals(NR1) ? v.put("definitionSha256", "0".repeat(64)) : v);
        refused("1 message(s) whose definitionSha256 is not the manifest's pin, the first at ot.messages/" + p, () -> record(pin, in, "pin"));

        FakeSource stranger = source(SMOKE, v -> v);
        String other = "synthetic-site:line2:station9";
        rewrite(stranger, msg, p, v -> true, v -> v.get("seq").asLong() == 9 && v.get("device").asText().equals(NR1) ? v.put("device", other) : v);
        refused("1 message(s) or session event(s) of a device outside the routing, the first at ot.messages/" + p, () -> record(stranger, in, "outside"));

        FakeSource strangerSession = source(SMOKE, v -> v);
        rewrite(strangerSession, ses, p, v -> true, v -> v.get("device").asText().equals(NR1) ? v.put("device", other) : v);
        refused("of a device outside the routing, the first at ot.sessions/" + p, () -> record(strangerSession, in, "outside-session"));
    }

    @Test
    void theDeadLetterTopicIsASideFileNotPartOfTheRecording() throws Exception {
        FakeSource src = source(SMOKE, v -> v);
        src.of(LiveTopics.name(PREFIX, LiveTopics.DEAD_LETTER), 3).add(new CaptureRecorder.Raw(0, 1794183000000L, "LogAppendTime", "spBv1.0/g/DDATA/e/d",
            Map.of("forseti-reason", "birthless".getBytes(StandardCharsets.UTF_8)), new byte[] {0, 1, 2, (byte) 0xff}));
        Path out = record(src, inputs(SMOKE, occupancy(SMOKE)), "dl");
        Recording.open(out).verifyDigests();
        List<String> lines = Files.readAllLines(CaptureRecorder.besides(out, CaptureRecorder.DEAD_LETTER_SUFFIX));
        assertEquals(1, lines.size());
        JsonNode d = Json.read(lines.get(0).getBytes(StandardCharsets.UTF_8));
        assertEquals(3, d.get("partition").asInt());
        assertEquals("AAEC/w==", d.get("valueBase64").asText());
        assertEquals("birthless", d.path("headers").path("forseti-reason").asText());
        assertEquals(1, Json.read(CaptureRecorder.besides(out, CaptureRecorder.REPORT_SUFFIX)).path("counts").path("ot.dead-letter").asLong());
    }

    /** The recorder plans its deadline brackets on the harness's W1, which only the telemetry of equipment carrying a rule member
     *  advances: the live plc-stamped rule names station1 and nutrunner1 but not nutrunner2, which is routed all the same (a live
     *  capture once placed a settlement bracket on a nutrunner2 sample that moved no W1, and its ground truth did not hold). */
    @Test
    void theW1TheRecorderPlansOnCountsOnlyEquipmentThatCarriesARuleMember() {
        Path live = FIXTURES.resolve("live").resolve("registry");
        String site = "site-071:line2:";
        for (String placement : List.of("dedicated", "shared")) {
            ObjectNode oj = Json.object().put("stream", "line2-" + placement).put("releaseId", "line2-rules-" + placement).put("version", "1.0.0")
                .put("attributionRuleId", "line2-attribution").put("attributionRuleVersion", "1.0.0");
            ObjectNode plc = oj.deepCopy().put("stream", "line2-" + placement + "-plc").put("releaseId", "line2-rules-" + placement + "-plc");
            String s = placement.equals("dedicated") ? site : "site-019:line2:";
            assertEquals(Set.of(s + "station1", s + "station1:nutrunner1", s + "station1:nutrunner2", s + "station2", s + "station3"),
                CaptureTruth.temporal(live, oj).memberEquipment(), placement);
            assertEquals(Set.of(s + "station1", s + "station1:nutrunner1"), CaptureTruth.temporal(live, plc).memberEquipment(), placement + " plc");
        }
    }

    /** The capture check recounts W1 with the recorder's rule ({@link CaptureRecorder#w1Step}): a routed device without a rule member
     *  does not advance its station's W1. On the plc-stamped smoke rule, station1's nutrunner2 carries no member; its sourceTs moved
     *  ahead of the station's members makes the unfiltered W1 overshoot the settlement deadline at {@code cutA}, as a live plc-stamped
     *  capture once did ("W1 ... at cut 20, ... at cut 21 do not bracket it"), and the check still finds the bracket the recorder
     *  planned. */
    @Test
    void theCaptureCheckCountsOnlyEquipmentThatCarriesARuleMemberTowardW1() throws Exception {
        Path smokePlc = FIXTURES.resolve("recordings").resolve("smoke-plc");
        long ahead = 60_000;
        UnaryOperator<String> nr2Ahead = v -> {
            ObjectNode o = (ObjectNode) Json.read(v.getBytes(StandardCharsets.UTF_8));
            if (!NR2.equals(o.path("device").asText()) || !o.has("members")) return v;
            for (JsonNode x : o.get("members")) if (x.has("sourceTs")) ((ObjectNode) x).put("sourceTs", x.get("sourceTs").longValue() + ahead);
            try { return Json.JSON.writeValueAsString(o); } catch (Exception e) { throw new IllegalStateException(e); }
        };
        CaptureRecorder.Inputs base = inputs(smokePlc, occupancy(smokePlc));
        CaptureRecorder.Inputs in = new CaptureRecorder.Inputs("smoke-plc-capture", base.registry(), base.rulePin(), base.definitions(), base.routing(), base.t0(),
            base.scanMs(), base.batchMs(), base.occupancySource(), Map.of("V1", new CaptureTruth.Visit("V1", null, List.of(NR1))), base.sent(), base.source());
        Path out = record(source(smokePlc, nr2Ahead), in, "smoke-plc-capture");
        Recording cap = Recording.open(out);
        CaptureTruth.Temporal temporal = CaptureTruth.temporal(REGISTRY, cap.manifest().get("rule"));
        assertFalse(temporal.memberEquipment().contains(NR2), temporal.memberEquipment().toString());
        assertTrue(temporal.memberEquipment().contains(NR1), temporal.memberEquipment().toString());

        assertEquals(1, cap.deadlines().size());
        Recording.Deadline d = cap.deadlines().get(0);
        JsonNode t = cap.expected().get("transitions").get(0);
        assertEquals(S1, t.get("station").asText());
        // W1 of station1 at cutA and cutB: with the rule's members only (the recorder's and the fixed check's) and with every routed device
        long[] member = new long[2], all = new long[2];
        long[] cuts = {d.cutA(), d.cutB()};
        for (int k = 0; k < 2; k++) {
            Recording.Batch b = cap.batches().get((int) cuts[k] - 1);
            long m = Long.MIN_VALUE, u = Long.MIN_VALUE;
            for (int p : cap.routing().subscribed(InputParser.MESSAGES)) try (Recording.Reader r = cap.reader(InputParser.MESSAGES, p)) {
                for (Recording.Line l; (l = r.next()) != null && l.record().offset() <= b.through(InputParser.MESSAGES, p);) {
                    byte[] v = Recording.valueBytes(l.bytes());
                    yggdrasil.forseti.cli.recording.ReplayRecording.Timing tm = yggdrasil.forseti.cli.recording.ReplayRecording.timing(InputParser.MESSAGES, p,
                        l.record().offset(), l.record().key(), v, cap.routing(), cap.manifest().get("definitions"));
                    if (!S1.equals(tm.station())) continue;
                    u = Math.max(u, tm.maxTs() - temporal.outOfOrdernessMs() - 1);
                    Long w = CaptureRecorder.w1Step(tm, v, temporal);
                    if (w != null) m = Math.max(m, w);
                }
            }
            member[k] = m;
            all[k] = u;
        }
        assertTrue(member[0] < d.at() && d.at() <= member[1], "the members' W1 brackets the deadline: " + Arrays.toString(member) + " " + d.at());
        assertEquals(t.get("w1A").asLong(), member[0]);
        assertEquals(t.get("w1B").asLong(), member[1]);
        assertTrue(all[0] >= d.at(), "the unfiltered W1 at cutA overshoots the deadline (the old check's failure): " + Arrays.toString(all) + " " + d.at());

        assertEquals(List.of(), CaptureLock.check(out, REGISTRY));
        Path lock = tmp.resolve("smoke-plc.lock.json");
        assertEquals(0, main("recordings", "verify-capture", out.toString(), "--lock", lock.toString(), "--registry", REGISTRY.toString(), "--write-lock"));
        assertEquals(0, main("recordings", "verify-capture", out.toString(), "--lock", lock.toString(), "--registry", REGISTRY.toString()));
        // a registry without the manifest's rule: a problem, not a pass
        List<String> problems = CaptureLock.check(out, tmp.resolve("no-registry"));
        assertTrue(problems.stream().anyMatch(p -> p.contains("does not resolve")), problems.toString());
    }
}
