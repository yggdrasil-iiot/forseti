package yggdrasil.forseti.cli.harness;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.cli.generation.FileGenerationStore;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.*;

/** The reference harness on the committed {@code smoke} recording and on copies of it edited in temporary directories (the fixtures are
 *  never written). Generations are registered through {@code generation new} on the fixed clock of the head's signedAt + 60 s (the
 *  verb verifies the head, never the harness), followed by the run preparation that writes the initial recovery declaration. */
class ReferenceEngineTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    static final Path FIXTURES = ROOT.resolve("fixtures"), SMOKE = FIXTURES.resolve("recordings/smoke"), REGISTRY = FIXTURES.resolve("registry");
    static final Path SMOKE_PLC = FIXTURES.resolve("recordings/smoke-plc");
    static final String S1 = "synthetic-site:line2:station1", NR1 = S1 + ":nutrunner1";
    static final long T = 1794183000000L, OUT_OF_ORDERNESS = 30000;
    static final String WR = "forseti.work-records", SAMPLES = "forseti.samples", LEDGER = "forseti.dispositions-stage1";
    static final List<String> STAGE1 = List.of("ot.messages", "ot.sessions", "mfg.occupancy");
    static final String ARTIFACT = "ab".repeat(32);

    @TempDir static Path shared;
    @TempDir Path tmp;
    private static Path base;

    // ============================================================================================================ the smoke run

    /** One run of {@code smoke} under generation 1, shared by the tests that only read it. */
    static synchronized Path base() throws IOException {
        if (base == null) {
            Path state = Files.createDirectories(shared.resolve("state"));
            register(state, 1, REGISTRY, null);
            base = shared.resolve("base");
            ReferenceEngine.run(options(SMOKE, REGISTRY, state, 1, base));
        }
        return base;
    }

    @Test void writesProgressAfterTheCutsRowsOnBothEvidenceTopics() throws IOException {
        Path out = base();
        List<Long> cutIds = cutIds();
        for (int p = 0; p < 3; p++) {
            List<Long> wr = progressCuts(lines(out, WR, p)), sm = progressCuts(lines(out, SAMPLES, p)), led = progressCuts(lines(out, LEDGER, p));
            assertEquals(cutIds, wr, "work-records/" + p);
            assertEquals(cutIds, sm, "samples/" + p);
            assertEquals(cutIds, led, "ledger/" + p);
            for (String topic : List.of(WR, SAMPLES, LEDGER)) {
                List<JsonNode> l = lines(out, topic, p);
                assertTrue(isProgress(l.get(l.size() - 1)), topic + "/" + p + " ends with the last cut's progress");
                JournalFold.Result fold = JournalFold.fold(entries(topic, l), 1, cutIds.get(cutIds.size() - 1));
                assertTrue(fold.reachedProgress());
                assertTrue(fold.violations().isEmpty(), topic + "/" + p + ": " + fold.violations());
            }
        }
        long rows = 0;
        for (int p = 0; p < 3; p++) rows += lines(out, SAMPLES, p).stream().filter(n -> !isProgress(n)).count();
        assertTrue(rows >= 5684, "every sample has at least one row: " + rows);
        assertTrue(lines(out, WR, 0).stream().anyMatch(n -> !isProgress(n)), "station1 carries the smoke visit");
    }

    @Test void W1FollowsThePerStationFormula() throws IOException {
        Path out = base();
        Recording rec = Recording.open(SMOKE);
        Map<String, Long> max = new HashMap<>(), expected = new HashMap<>();
        Map<String, Long> previous = new HashMap<>();
        JsonNode cuts = Json.read(out.resolve("cuts.json")).get("cuts");
        int k = 0;
        for (Recording.Batch b : rec.batches()) {
            for (int p : rec.routing().subscribed("ot.messages")) {
                long from = previous.getOrDefault("" + p, -1L) + 1, through = b.through("ot.messages", p);
                for (long off = from; off <= through; off++) {
                    JsonNode v = rec.records("ot.messages", p).get((int) off).value();
                    String station = rec.routing().stationOf(v.get("device").asText());
                    for (JsonNode m : v.get("members")) max.merge(station, m.get("sourceTs").longValue(), Math::max);
                }
                previous.put("" + p, through);
            }
            JsonNode c = cuts.get(k++);
            assertEquals(b.cutId(), c.get("cutId").longValue());
            for (String s : rec.routing().stations().keySet()) {
                long w = Math.max(max.get(s) - OUT_OF_ORDERNESS - 1, expected.getOrDefault(s, Long.MIN_VALUE));
                expected.put(s, w);
                assertEquals(w, c.path("perStation").path(s).get("W1").longValue(), "cut " + b.cutId() + " " + s);
            }
        }
        for (String s : rec.routing().stations().keySet()) {
            int p = rec.routing().partitionOf(s);
            List<Progress.Stage1> pr = stage1Progress(lines(out, WR, p));
            for (int i = 0; i < pr.size(); i++) assertEquals(cuts.get(i).path("perStation").path(s).get("W1").longValue(), pr.get(i).W1());
        }
    }

    @Test void admissionWIsThePreviousW1() throws IOException {
        Path out = base();
        JsonNode cuts = Json.read(out.resolve("cuts.json")).get("cuts");
        for (int p = 0; p < 3; p++) {
            for (String topic : List.of(WR, SAMPLES)) {
                List<Progress.Stage1> pr = stage1Progress(lines(out, topic, p));
                assertNull(pr.get(0).admissionW());
                for (int i = 1; i < pr.size(); i++) assertEquals(pr.get(i - 1).W1(), pr.get(i).admissionW(), topic + "/" + p + " cut " + pr.get(i).cutId());
            }
        }
        for (int i = 0; i < cuts.size(); i++)
            for (Iterator<Map.Entry<String, JsonNode>> it = cuts.get(i).get("perStation").fields(); it.hasNext();) {
                Map.Entry<String, JsonNode> e = it.next();
                if (i == 0) assertTrue(e.getValue().get("admissionW").isNull());
                else assertEquals(cuts.get(i - 1).get("perStation").get(e.getKey()).get("W1"), e.getValue().get("admissionW"));
                assertEquals(0, e.getValue().get("mergedCutIds").size());
            }
    }

    @Test void everyInputHasADisposition() throws IOException {
        Path out = base();
        Set<String> parts = new HashSet<>();
        for (int p : List.of(0, 1, 2, 6)) for (JsonNode n : lines(out, LEDGER, p)) if (!isProgress(n)) {
            JsonNode k = n.get("key");
            parts.add(k.get("inputTopic").asText() + "/" + k.get("inputPartition") + "/" + k.get("inputOffset") + "/" + k.get("part").asText());
        }
        Recording rec = Recording.open(SMOKE);
        int inputs = 0;
        for (String t : STAGE1) for (int p : rec.routing().subscribed(t)) for (Recording.Record r : rec.records(t, p)) {
            inputs++;
            String at = t + "/" + p + "/" + r.offset() + "/";
            if (t.equals("ot.messages")) for (JsonNode m : r.value().get("members")) assertTrue(parts.contains(at + "member:" + m.get("metric").asText()), at);
            else assertTrue(parts.contains(at + "event") || parts.contains(at + "errorDeclaration"), at);
        }
        assertEquals(4060 + 5 + 2, inputs);
    }

    @Test void theRejectCollectorWritesPartition6Progress() throws IOException {
        Path out = base();
        Recording rec = Recording.open(SMOKE);
        List<JsonNode> l = lines(out, LEDGER, 6);
        List<Progress.Ledger> pr = new ArrayList<>();
        for (JsonNode n : l) if (isProgress(n)) pr.add((Progress.Ledger) Progress.fromJson(n.get("value")));
        assertEquals(cutIds(), pr.stream().map(Progress::cutId).toList());
        for (int i = 0; i < pr.size(); i++) {
            Progress.Ledger g = pr.get(i);
            assertEquals(6, g.partition());
            assertEquals(1, g.stage());
            assertNull(g.station());
            assertEquals(i == 0, g.initEvidence() != null);
            Set<String> refs = new TreeSet<>(), want = new TreeSet<>();
            for (Progress.BarrierRef b : g.barrierRefs()) { refs.add(b.origin().topic() + "/" + b.origin().partition() + "@" + b.throughOffset()); assertEquals(g.cutId(), b.cutId()); }
            Recording.Batch b = rec.batches().get(i);
            for (String t : STAGE1) for (int p : rec.routing().subscribed(t)) want.add(t + "/" + p + "@" + b.through(t, p));
            assertEquals(want, refs, "cut " + g.cutId());
            assertEquals(11, refs.size());
        }
    }

    @Test void theRunRecordCarriesSeedPinsDigestsAndOffsetRanges() throws IOException {
        Path out = base();
        JsonNode run = Json.read(out.resolve("run.json"));
        assertEquals(ReferenceEngine.DEFAULT_SEED, run.get("seed").longValue());
        assertFalse(run.get("shuffle").booleanValue());
        assertEquals("core-harness", run.get("engine").asText());
        assertEquals(ARTIFACT, run.get("artifactSha256").asText());
        JsonNode job = run.path("pins").path("job");
        assertEquals("core-harness", job.get("engine").asText());
        assertEquals(ARTIFACT, job.get("artifactSha256").asText());
        assertEquals("payload", job.get("timestampSource").asText());
        assertEquals("off", job.get("idleness").asText());
        assertEquals(Recording.open(SMOKE).routing().routingDigest(), job.get("routingDigest").asText());
        assertEquals(run.get("runId").asText(), job.get("runId").asText());
        UUID.fromString(run.get("runId").asText());
        JsonNode row = lines(out, WR, 0).get(0);
        assertEquals(run.get("pins"), row.path("value").path("envelope").path("pins"), "every row carries the run's pins");
        assertEquals(Canonical.digestOf(Files.readAllBytes(SMOKE.resolve("manifest.json"))), run.path("fixtureDigests").get("recordingManifestSha256").asText());
        assertEquals(Json.read(SMOKE.resolve("manifest.json")).get("digests"), run.path("fixtureDigests").get("recordingFiles"));
        assertTrue(run.path("fixtureDigests").get("rulesDigest").asText().matches("[0-9a-f]{64}"));
        assertTrue(run.path("jcs").get("java").asText().startsWith("io.github.erdtman:java-json-canonicalization:"));
        Recording rec = Recording.open(SMOKE);
        for (String t : STAGE1) for (int p : rec.routing().subscribed(t)) {
            JsonNode r = run.path("offsetRanges").path(t).path(Integer.toString(p));
            assertEquals(0, r.get(0).longValue(), t + "/" + p);
            assertEquals(rec.records(t, p).size(), r.get(1).longValue(), t + "/" + p);
        }
        assertEquals(28, run.get("cuts").intValue());
        assertTrue(run.get("command").asText().startsWith("forseti run --engine core"));
        // the cut log's half-open ranges chain from 0 to the record count
        JsonNode cuts = Json.read(out.resolve("cuts.json")).get("cuts");
        for (int p : rec.routing().subscribed("ot.messages")) {
            long at = 0;
            for (JsonNode c : cuts) { JsonNode r = c.path("offsetRanges").path("ot.messages").path("" + p); assertEquals(at, r.get(0).longValue()); at = r.get(1).longValue(); }
            assertEquals(rec.records("ot.messages", p).size(), at);
        }
        assertEquals(6, cuts.get(0).get("incidentOffsets").size());
    }

    @Test void twoRunsEmitIdenticalBodiesKeysAndRevisions() throws IOException {
        Path a = base();
        Path state = Files.createDirectories(tmp.resolve("state"));
        register(state, 1, REGISTRY, null);
        Path b = tmp.resolve("shuffled");
        ReferenceEngine.Options o = options(SMOKE, REGISTRY, state, 1, b);
        ReferenceEngine.run(new ReferenceEngine.Options(o.recording(), o.registry(), o.state(), 1, b, 7, true, Map.of(), ARTIFACT, "test"));
        TreeMap<String, byte[]> fa = journalFiles(a), fb = journalFiles(b);
        assertEquals(fa.keySet(), fb.keySet());
        String runA = Json.read(a.resolve("run.json")).get("runId").asText(), runB = Json.read(b.resolve("run.json")).get("runId").asText();
        assertNotEquals(runA, runB);
        for (String f : fa.keySet()) {
            List<String> la = withoutRunId(fa.get(f)), lb = withoutRunId(fb.get(f));
            assertEquals(la.size(), lb.size(), f);
            for (int i = 0; i < la.size(); i++) assertEquals(la.get(i), lb.get(i), f + " line " + i);
        }
        assertArrayEquals(Files.readAllBytes(a.resolve("cuts.json")), Files.readAllBytes(b.resolve("cuts.json")));
    }

    // ============================================================================================================ start-up refusals

    @Test void anUnregisteredGenerationRefusesToStart() throws IOException {
        Path state = Files.createDirectories(tmp.resolve("state"));
        register(state, 1, REGISTRY, null);
        ByteArrayOutputStream out = new ByteArrayOutputStream(), err = new ByteArrayOutputStream();
        int exit = Main.run(new String[]{"run", "--engine", "core", "--recording", SMOKE.toString(), "--registry", REGISTRY.toString(),
            "--state", state.toString(), "--generation", "7", "--out", tmp.resolve("out").toString()},
            new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
        assertEquals(1, exit, err.toString(StandardCharsets.UTF_8));
        JsonNode r = Json.read(out.toString(StandardCharsets.UTF_8).strip().getBytes(StandardCharsets.UTF_8));
        assertEquals("generation-unregistered", r.get("refusal").asText());
        assertEquals(7, r.get("generation").intValue());
        assertFalse(Files.exists(tmp.resolve("out")), "a refused run writes nothing");
    }

    @Test void aDifferentRulesDigestRefusesToStart() throws IOException {
        Path state = Files.createDirectories(tmp.resolve("state"));
        Path definitions = Files.writeString(tmp.resolve("definitions.json"), "{}\n");   // the rules digest pins the definitions
        JsonNode printed = register(state, 1, REGISTRY, definitions);
        ReferenceEngine.Refused r = assertThrows(ReferenceEngine.Refused.class, () -> ReferenceEngine.run(options(SMOKE, REGISTRY, state, 1, tmp.resolve("out"))));
        assertEquals("generation-rules-mismatch", r.code);
        assertEquals(printed.get("rulesDigest").asText(), r.evidence.get("registered").asText());
        assertNotEquals(r.evidence.get("registered"), r.evidence.get("loaded"));
    }

    @Test void aRunOnAnotherRoutingThanTheDeclaredOneIsRefused() throws IOException {
        Path state = Files.createDirectories(tmp.resolve("state"));
        generationNew(state, REGISTRY, SMOKE, null);
        assertEquals(0, forsetiRun(SMOKE, state, 1, tmp.resolve("first")).getKey(), "the first run writes the declaration");
        byte[] declarations = Files.readAllBytes(state.resolve(ReferenceEngine.RECOVERY_CONTROL));
        // the same recording under another routing: an extra field changes the routing digest, recomputed so the file stays well-formed
        Path rec = copyTree(SMOKE, tmp.resolve("rec"));
        ObjectNode routing = (ObjectNode) Json.read(rec.resolve("routing.json"));
        routing.put("note", "another routing");
        routing.remove("routingDigest");
        routing.put("routingDigest", Canonical.digest(routing));
        Json.write(rec.resolve("routing.json"), routing);
        String other = Recording.open(rec).routing().routingDigest();
        assertNotEquals(Recording.open(SMOKE).routing().routingDigest(), other);
        Map.Entry<Integer, String> r = forsetiRun(rec, state, 1, tmp.resolve("second"));
        assertEquals(1, r.getKey(), r.getValue());
        JsonNode refusal = Json.read(r.getValue().getBytes(StandardCharsets.UTF_8));
        assertEquals("generation-routing-mismatch", refusal.get("refusal").asText());
        assertEquals(Recording.open(SMOKE).routing().routingDigest(), refusal.get("declared").asText());
        assertEquals(other, refusal.get("recording").asText());
        assertArrayEquals(declarations, Files.readAllBytes(state.resolve(ReferenceEngine.RECOVERY_CONTROL)), "nothing is declared");
        assertFalse(Files.exists(tmp.resolve("second")), "a refused run writes nothing");
    }

    @Test void anOnlineRunRecordsItsRegistrationAndNullReplayFields() throws IOException {
        Path state = Files.createDirectories(tmp.resolve("state"));
        JsonNode printed = generationNew(state, REGISTRY, SMOKE, null);
        Map.Entry<Integer, String> r = forsetiRun(SMOKE, state, 1, tmp.resolve("out"));
        assertEquals(0, r.getKey(), r.getValue());
        JsonNode run = Json.read(tmp.resolve("out").resolve("run.json"));
        String stored = Canonical.digestOf(Arrays.copyOf(Files.readAllBytes(state.resolve(FileGenerationStore.FILE)),
            Files.readAllBytes(state.resolve(FileGenerationStore.FILE)).length - 1));
        assertEquals(printed.get("registeredRowSha256").asText(), run.get("registeredRowSha256").asText());
        assertEquals(stored, run.get("registeredRowSha256").asText(), "the raw digest of the stored line");
        assertEquals("line2", run.get("scope").asText());
        assertEquals(1, run.get("generation").longValue());
        assertTrue(run.has("replay") && run.get("replay").isNull(), "replay is null for an online run");
        assertTrue(run.has("originalRecordingManifestSha256") && run.get("originalRecordingManifestSha256").isNull());
        assertEquals(printed.get("rulesDigest").asText(), run.path("fixtureDigests").get("rulesDigest").asText());
    }

    @Test void aRunUsesThePinnedSnapshotNotTheCurrentHead() throws IOException {
        Path registry = copyTree(REGISTRY, tmp.resolve("registry"));
        Path state = Files.createDirectories(tmp.resolve("state"));
        register(state, 1, registry, null);
        Path m = registry.resolve("mappings");
        Files.copy(m.resolve("heads/line2-plc.json"), m.resolve("heads/line2.json"), StandardCopyOption.REPLACE_EXISTING);
        Files.delete(m.resolve("head-attestations/line2.dsse.json"));
        ReferenceEngine.Result ok = ReferenceEngine.run(options(SMOKE, registry, state, 1, tmp.resolve("ok")));
        assertEquals(28, ok.cuts());
        Path payload = m.resolve("releases/line2-rules/1.0.0/payload/attribution-rules/0.json");
        byte[] b = Files.readAllBytes(payload);
        b[b.length - 2] = (byte) (b[b.length - 2] == ' ' ? '\t' : ' ');
        Files.write(payload, b);
        ReferenceEngine.Refused r = assertThrows(ReferenceEngine.Refused.class, () -> ReferenceEngine.run(options(SMOKE, registry, state, 1, tmp.resolve("bad"))));
        assertEquals("provenance-mismatch", r.code);
        assertEquals("release-file", r.evidence.get("rule").asText());
    }

    @Test void aRunWritesNoGenerationRow() throws IOException {
        Path state = Files.createDirectories(tmp.resolve("state"));
        register(state, 1, REGISTRY, null);
        TreeMap<String, byte[]> before = tree(state);
        ReferenceEngine.run(options(SMOKE, REGISTRY, state, 1, tmp.resolve("out")));
        TreeMap<String, byte[]> after = tree(state);
        assertEquals(before.keySet(), after.keySet());
        for (String k : before.keySet()) assertArrayEquals(before.get(k), after.get(k), k);
    }

    @Test void initEvidencePointsAtItsOwnDeclaration() throws IOException {
        Path state = Files.createDirectories(tmp.resolve("state"));
        register(state, 1, REGISTRY, null);
        register(state, 2, REGISTRY, null);
        for (long g : List.of(1L, 2L)) {
            Path out = tmp.resolve("g" + g);
            ReferenceEngine.run(options(SMOKE, REGISTRY, state, g, out));
            Map<String, Integer> writers = new LinkedHashMap<>();
            for (int p = 0; p < 3; p++) { writers.put(WR + "/" + p, p); writers.put(SAMPLES + "/" + p, p); writers.put(LEDGER + "/" + p, p); }
            writers.put(LEDGER + "/6", 6);
            for (Map.Entry<String, Integer> w : writers.entrySet()) {
                String topic = w.getKey().substring(0, w.getKey().indexOf('/'));
                List<JsonNode> progress = lines(out, topic, w.getValue()).stream().filter(ReferenceEngineTest::isProgress).toList();
                JsonNode init = progress.get(0).path("value").get("initEvidence");
                assertNotNull(init, w.getKey());
                assertEquals(g - 1, init.get("declarationOffset").longValue(), w.getKey());
                assertEquals(2, init.get("endOffset").longValue(), w.getKey());
                assertEquals("none", init.get("restore").asText());
                assertEquals(g, progress.get(0).path("value").get("generation").longValue());
                for (int i = 1; i < progress.size(); i++) assertFalse(progress.get(i).path("value").has("initEvidence"), w.getKey() + " progress " + i);
            }
        }
    }

    @Test void aFirstBatchWithoutTelemetryOnEverySubscribedPartitionIsRefused() throws IOException {
        Path rec = copyTree(SMOKE, tmp.resolve("rec"));
        ObjectNode manifest = (ObjectNode) Json.read(rec.resolve("manifest.json"));
        ((ObjectNode) manifest.path("batches").get(0).path("throughOffsets").path("ot.messages")).put("2", -1);
        Json.write(rec.resolve("manifest.json"), manifest);
        Path state = Files.createDirectories(tmp.resolve("state"));
        register(state, 1, REGISTRY, null);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ReferenceEngine.run(options(rec, REGISTRY, state, 1, tmp.resolve("out"))));
        assertTrue(e.getMessage().contains("first batch carries no telemetry on ot.messages/2"), e.getMessage());
    }

    // ============================================================================================================ door and number cases

    @Test void aNonCanonicalizableRecordIsRejectedAndTheRunContinues() throws IOException {
        Path rec = copyTree(SMOKE, tmp.resolve("rec"));
        String raw = "[9007199254740992]";
        editPartition(rec, "mfg.occupancy", 1, l -> List.of(line(0, "synthetic-site:line2:station2", raw)));
        throughFrom(rec, "mfg.occupancy", 1, 3, 0);
        Path out = runOn(rec);
        assertEquals(28, Json.read(out.resolve("run.json")).get("cuts").intValue());
        List<JsonNode> rejected = lines(out, LEDGER, 6).stream().filter(n -> !isProgress(n)).toList();
        assertEquals(1, rejected.size());
        JsonNode key = rejected.get(0).get("key"), body = rejected.get(0).path("value").get("body");
        assertEquals("mfg.occupancy", key.get("inputTopic").asText());
        assertEquals(1, key.get("inputPartition").intValue());
        assertEquals(0, key.get("inputOffset").intValue());
        assertEquals("event", key.get("part").asText());
        assertEquals("rejected", body.get("disposition").asText());
        assertEquals("schema-invalid", body.get("reason").asText());
        assertEquals("raw-bytes", body.path("refs").get("digestRule").asText());
        assertEquals(Canonical.digestOf(raw.getBytes(StandardCharsets.UTF_8)), body.get("payloadDigest").asText());
        Path b = base();
        for (String topic : List.of(WR, SAMPLES)) for (int p = 0; p < 3; p++)
            assertEquals(rowsWithoutRunId(lines(b, topic, p)), rowsWithoutRunId(lines(out, topic, p)), topic + "/" + p + " rows unchanged");
        for (int p = 0; p < 3; p++)
            assertEquals(rowsWithoutRunId(lines(b, LEDGER, p)), rowsWithoutRunId(lines(out, LEDGER, p)), "ledger " + p + " unchanged");
    }

    @Test void aMixedMessageKeepsItsGoodMembers() throws IOException {
        Path rec = copyTree(SMOKE, tmp.resolve("rec"));
        long at = T + 30000;
        int offset = torque(rec, at, "1e400");
        Path out = runOn(rec);
        JsonNode wr = lastWorkRecord(out);
        JsonNode torque = member(wr, "nutrunner1-torque");
        assertEquals(59, torque.get("observedPeriods").intValue());
        assertEquals(1, torque.get("missingPeriods").intValue());
        assertEquals(59, torque.get("count").intValue());
        assertEquals("complete", wr.get("status").asText());
        assertEquals("unattributed", sampleRow(out, at, "Torque").get("state").asText());
        assertEquals("non-finite", sampleRow(out, at, "Torque").get("reason").asText());
        assertEquals("attributed", sampleRow(out, at, "Running").get("state").asText());
        assertEquals(visitRef(), sampleRow(out, at, "Running").get("attributedTo"));
        for (String metric : List.of("Torque", "Running")) {
            JsonNode d = disposition(out, "ot.messages", 1, offset, "member:" + metric);
            assertEquals("accepted", d.get("disposition").asText(), metric);
            assertEquals("raw-bytes", d.path("refs").get("digestRule").asText(), metric);
        }
        JsonNode cutsA = Json.read(base().resolve("cuts.json")).get("cuts"), cutsB = Json.read(out.resolve("cuts.json")).get("cuts");
        for (int i = 0; i < cutsA.size(); i++) assertEquals(cutsA.get(i).get("perStation"), cutsB.get(i).get("perStation"), "the message advances event time as before");
    }

    @Test void overflowingIntegerAndExponentLiteralsAreNonFinite() throws IOException {
        Path rec = copyTree(SMOKE, tmp.resolve("rec"));
        String big = "1" + "0".repeat(400);
        Map<Long, String> literals = new LinkedHashMap<>();
        literals.put(T + 10000, "1e400");
        literals.put(T + 20000, "-1e400");
        literals.put(T + 30000, big);
        literals.put(T + 40000, "-" + big);
        Map<Long, Integer> offsets = new LinkedHashMap<>();
        for (Map.Entry<Long, String> e : literals.entrySet()) offsets.put(e.getKey(), torque(rec, e.getKey(), e.getValue()));
        Path out = runOn(rec);
        assertEquals(28, Json.read(out.resolve("run.json")).get("cuts").intValue());
        for (long at : literals.keySet()) {
            assertEquals("non-finite", sampleRow(out, at, "Torque").get("reason").asText(), literals.get(at).substring(0, 2));
            assertEquals("attributed", sampleRow(out, at, "Running").get("state").asText());
            for (String metric : List.of("Torque", "Running"))
                assertEquals("accepted", disposition(out, "ot.messages", 1, offsets.get(at), "member:" + metric).get("disposition").asText());
        }
        JsonNode torque = member(lastWorkRecord(out), "nutrunner1-torque");
        assertEquals(56, torque.get("observedPeriods").intValue());
        assertEquals(56, torque.get("count").intValue());
    }

    @Test void threeNotationsAreOneMeasurement() throws IOException {
        Path rec = copyTree(SMOKE, tmp.resolve("rec"));
        long at = T + 30000;
        int offset = torque(rec, at, "9007199254740992");
        List<String> l = partitionLines(rec, "ot.messages", 1);
        String original = l.get(offset);
        List<String> added = new ArrayList<>();
        int[] seqs = {900001, 900002};
        String[] notation = {"9007199254740992.0", "9.007199254740992e15"};
        for (int i = 0; i < 2; i++) added.add(withTorque(original.replaceFirst("\"seq\":[0-9]+,", "\"seq\":" + seqs[i] + ","), notation[i]));
        insertAfter(rec, "ot.messages", 1, offset, added);
        Path out = runOn(rec);
        JsonNode wr = lastWorkRecord(out);
        JsonNode torque = member(wr, "nutrunner1-torque"), baseTorque = member(lastWorkRecord(base()), "nutrunner1-torque");
        assertEquals(baseTorque.get("observedPeriods"), torque.get("observedPeriods"), "one observation");
        assertEquals(baseTorque.get("count"), torque.get("count"), "one observation in the aggregate");
        assertEquals(9007199254740992.0, torque.get("max").doubleValue());
        // the journal line is JCS text: the double 2^53 prints as 9007199254740992 whatever notation delivered it
        String last = null;
        for (String s : Files.readAllLines(JournalFiles.journal(out, WR, 0), StandardCharsets.UTF_8)) if (s.contains("\"arrivalEventId\":\"occ-1\"") && !s.contains("\"type\":\"progress\"")) last = s;
        assertNotNull(last);
        assertTrue(last.contains("\"max\":9007199254740992,") || last.contains("\"max\":9007199254740992}"), "printed as 9007199254740992");
        assertFalse(Files.readString(JournalFiles.journal(out, WR, 0)).contains("9.007199254740992"));
        for (int o = offset; o <= offset + 2; o++) assertEquals("accepted", disposition(out, "ot.messages", 1, o, "member:Torque").get("disposition").asText(), "offset " + o);
        for (int p : List.of(0, 1, 2, 6)) for (JsonNode n : lines(out, LEDGER, p))
            if (!isProgress(n)) assertNotEquals("measurement-conflict", n.path("value").path("body").path("reason").asText());
        int attributed = 0;
        for (JsonNode n : lines(out, SAMPLES, 0)) if (!isProgress(n) && n.path("key").path("device").asText().equals(NR1) && n.path("key").path("metric").asText().equals("Torque")
            && n.path("value").path("body").path("sourceTs").longValue() == at && n.path("value").path("body").path("state").asText().equals("attributed")) attributed++;
        assertTrue(attributed >= 3, "three sample rows, each attributed: " + attributed);
    }

    /** A plc-stamped rule with crossCheck none reads no occupancy: the harness does not subscribe the topic, so no occupancy input
     *  reaches the core and none is left without a disposition; subscribing it explicitly is refused. */
    @Test void aCrossCheckNoneRuleReadsNoOccupancy() throws IOException {
        Path rec = tmp.resolve("smoke-plc-nocheck");
        copyTree(SMOKE_PLC, rec);
        ObjectNode manifest = (ObjectNode) Json.read(rec.resolve("manifest.json"));
        ((ObjectNode) manifest.get("rule")).put("stream", "line2-plc-nocheck").put("releaseId", "line2-rules-plc-nocheck").put("version", "1.0.0").put("attributionRuleVersion", "1.0.1");
        Json.write(rec.resolve("manifest.json"), manifest);
        Path state = Files.createDirectories(tmp.resolve("state-nocheck"));
        register(state, 1, REGISTRY, rec, null);
        Path out = tmp.resolve("out-nocheck");
        ReferenceEngine.run(options(rec, REGISTRY, state, 1, out));
        int ledger = 0;
        for (int p : new int[] {0, 1, 2, 6}) for (JsonNode n : lines(out, "forseti.dispositions-stage1", p)) if (!isProgress(n)) {
            ledger++;
            assertNotEquals("mfg.occupancy", n.path("key").path("inputTopic").asText(), "no occupancy input was read");
        }
        assertTrue(ledger > 0);
        JsonNode run = Json.read(out.resolve("run.json"));
        JsonNode occ = run.path("offsetRanges").path("mfg.occupancy");
        for (JsonNode r : occ) assertEquals(r.get(0), r.get(1), "no occupancy offset was consumed: " + occ);
        assertEquals(0, run.path("subscribed").path("mfg.occupancy").size(), "occupancy is not subscribed");
        JsonNode last = null;
        for (JsonNode n : lines(out, WR, 0)) if (!isProgress(n)) last = n.path("value").get("body");
        assertNotNull(last);
        assertEquals("complete", last.get("status").asText());
        ReferenceEngine.Options explicit = new ReferenceEngine.Options(rec, REGISTRY, state, 1, tmp.resolve("out-nocheck-2"), 20261109L, false,
            Map.of("mfg.occupancy", List.of(0)), null, "test");
        assertThrows(IllegalArgumentException.class, () -> ReferenceEngine.run(explicit));
    }

    @Test void runsThePlcSmokeRecording() throws IOException {
        Path state = Files.createDirectories(tmp.resolve("state-plc"));
        register(state, 1, REGISTRY, SMOKE_PLC, null);
        Path out = tmp.resolve("out-plc");
        ReferenceEngine.run(options(SMOKE_PLC, REGISTRY, state, 1, out));
        ObjectNode ref = Json.object().put("mode", "plc-stamped").put("namespace", "urn:line2:visits").put("station", S1).put("visitId", "V1");
        Set<String> keys = new TreeSet<>();
        JsonNode last = null;
        for (int p = 0; p < 3; p++) for (JsonNode n : lines(out, WR, p)) if (!isProgress(n)) {
            keys.add(Canonical.utf8(Canonical.bytes(n.get("key"))));
            if (n.get("key").equals(ref)) { assertEquals(0, p, "the station1 work record partition"); last = n.path("value").get("body"); }
        }
        assertEquals(Set.of(Canonical.utf8(Canonical.bytes(ref))), keys, "one visit, keyed by the plc-stamped visitRef");
        assertNotNull(last);
        assertEquals("complete", last.get("status").asText());
        assertEquals("plc-stamped", last.get("attributionMode").asText());
        assertEquals("UNIT0000000000001", last.get("unitId").asText());
        assertEquals(S1, last.get("readPoint").asText());
        assertTrue(last.get("visitSeq").isNull());
        assertEquals(T, last.get("interval").get("enter").longValue());
        assertEquals(T + 60_000, last.get("interval").get("leave").longValue());
        assertEquals(0, last.get("insufficiencyReasons").size());
        JsonNode torque = member(last, "nutrunner1-torque");
        assertEquals(60, torque.get("observedPeriods").longValue());
        assertEquals(0, torque.get("missingPeriods").longValue());
        // crossCheck occupancy: the smoke occupant is the same unit, so no sample carries identity-mismatch; occupancy has its dispositions
        for (JsonNode n : lines(out, SAMPLES, 0)) if (!isProgress(n)) assertNotEquals("identity-mismatch", n.path("value").path("body").path("reason").asText());
        assertEquals("accepted", disposition(out, "mfg.occupancy", 0, 0, "event").get("disposition").asText());
        assertEquals("accepted", disposition(out, "mfg.occupancy", 0, 1, "event").get("disposition").asText());
    }

    // ============================================================================================================ helpers

    static ReferenceEngine.Options options(Path recording, Path registry, Path state, long generation, Path out) {
        return new ReferenceEngine.Options(recording, registry, state, generation, out, ReferenceEngine.DEFAULT_SEED, false, Map.of(), ARTIFACT,
            "forseti run --engine core --recording " + recording + " --generation " + generation);
    }

    Path runOn(Path recording) throws IOException {
        Path state = Files.createDirectories(tmp.resolve("state-" + UUID.randomUUID()));
        register(state, 1, REGISTRY, null);
        Path out = tmp.resolve("out-" + UUID.randomUUID());
        ReferenceEngine.run(options(recording, REGISTRY, state, 1, out));
        return out;
    }

    /** Registers generation {@code n} through {@code generation new} on the smoke recording (with {@code definitions} instead of the
     *  recording's when given) and runs the run preparation, as {@code forseti run} does before the engine. */
    static JsonNode register(Path state, long n, Path registry, Path definitions) throws IOException {
        return register(state, n, registry, SMOKE, definitions);
    }

    /** The same for the stream, rule and definitions of {@code recording}'s manifest: {@code generation new} in-process with the fixed
     *  clock of the head's signedAt + 60 s and the fixture keys; the number it allocates must be {@code n}. Returns what it printed. */
    static JsonNode register(Path state, long n, Path registry, Path recording, Path definitions) throws IOException {
        JsonNode printed = generationNew(state, registry, recording, definitions);
        assertEquals(n, printed.get("generation").longValue(), "the verb allocates the next number");
        GenerationCli.prepareRun(state, n, recording);
        return printed;
    }

    /** {@code forseti generation new} in-process on the fixed clock; exit 0 required. */
    static JsonNode generationNew(Path state, Path registry, Path recording, Path definitions) {
        JsonNode rule = Json.read(recording.resolve("manifest.json")).get("rule");
        String stream = rule.get("stream").asText();
        Path m = registry.resolve("mappings");
        List<String> args = new ArrayList<>(List.of("new", "--state", state.toString(), "--mappings", m.toString(),
            "--trusted-keys", FIXTURES.resolve("trusted-keys.jsonl").toString(), "--stream", stream,
            "--rule", rule.get("attributionRuleId").asText() + "@" + rule.get("attributionRuleVersion").asText(), "--recording", recording.toString()));
        if (definitions != null) args.addAll(List.of("--definitions", definitions.toString()));
        ByteArrayOutputStream out = new ByteArrayOutputStream(), err = new ByteArrayOutputStream();
        int exit = GenerationCli.run(args.toArray(String[]::new), new PrintStream(out, true, StandardCharsets.UTF_8),
            new PrintStream(err, true, StandardCharsets.UTF_8), clock(m, stream));
        assertEquals(0, exit, out.toString(StandardCharsets.UTF_8) + err.toString(StandardCharsets.UTF_8));
        return Json.read(out.toString(StandardCharsets.UTF_8).strip().getBytes(StandardCharsets.UTF_8));
    }

    /** The fixture clock: the head's signedAt + 60 s. */
    static Clock clock(Path mappings, String stream) {
        JsonNode statement = Json.read(Base64.getDecoder().decode(Json.read(mappings.resolve("head-attestations/" + stream + ".dsse.json")).get("payload").asText()));
        return Clock.fixed(Instant.parse(statement.path("predicate").path("signedAt").asText()).plusSeconds(60), ZoneOffset.UTC);
    }

    /** {@code forseti run --engine core} through {@code Main} (the run preparation, then the harness): exit code, stdout. */
    static Map.Entry<Integer, String> forsetiRun(Path recording, Path state, long generation, Path out) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = Main.run(new String[]{"run", "--engine", "core", "--recording", recording.toString(), "--registry", REGISTRY.toString(),
            "--state", state.toString(), "--generation", Long.toString(generation), "--out", out.toString(), "--artifact-sha256", ARTIFACT},
            new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8));
        return Map.entry(exit, so.toString(StandardCharsets.UTF_8).strip() + se.toString(StandardCharsets.UTF_8).strip());
    }

    static List<Long> cutIds() { return Recording.open(SMOKE).batches().stream().map(Recording.Batch::cutId).toList(); }

    static List<JsonNode> lines(Path out, String topic, int partition) throws IOException {
        List<JsonNode> l = new ArrayList<>();
        for (String s : Files.readAllLines(JournalFiles.journal(out, topic, partition), StandardCharsets.UTF_8)) l.add(Json.read(s.getBytes(StandardCharsets.UTF_8)));
        return l;
    }

    static boolean isProgress(JsonNode line) { return "progress".equals(line.path("value").path("type").textValue()); }

    static List<Long> progressCuts(List<JsonNode> l) {
        List<Long> c = new ArrayList<>();
        for (JsonNode n : l) if (isProgress(n)) { assertTrue(n.get("key").isNull()); c.add(n.path("value").get("cutId").longValue()); }
        return c;
    }

    static List<Progress.Stage1> stage1Progress(List<JsonNode> l) {
        List<Progress.Stage1> p = new ArrayList<>();
        for (JsonNode n : l) if (isProgress(n)) p.add((Progress.Stage1) Progress.fromJson(n.get("value")));
        return p;
    }

    static List<JournalFold.Entry> entries(String topic, List<JsonNode> l) {
        List<JournalFold.Entry> e = new ArrayList<>();
        for (JsonNode n : l) {
            JsonNode v = n.get("value");
            if (isProgress(n)) e.add(Progress.fromJson(v));
            else if (topic.equals(LEDGER)) {
                assertEquals(n.path("key").get("generation").asText(), n.path("headers").get("forseti-generation").asText());
                ObjectNode d = Json.object();
                d.set("key", n.get("key")); d.set("body", v.get("body")); d.set("revision", v.get("revision"));
                e.add(Disposition.fromJson(d));
            } else {
                assertEquals(Canonical.digest(v.get("body")), v.get("bodyDigest").asText(), "bodyDigest recomputed");
                assertEquals("1", n.path("headers").get("forseti-generation").asText());
                e.add(new Row(topic, n.get("key"), v.get("body"), v.get("bodyDigest").asText(), Envelope.fromJson(v.get("envelope"))));
            }
        }
        return e;
    }

    static JsonNode visitRef() { return Json.object().put("mode", "occupancy-join").put("source", "mes-line2").put("arrivalEventId", "occ-1"); }

    static JsonNode lastWorkRecord(Path out) throws IOException {
        JsonNode last = null;
        for (JsonNode n : lines(out, WR, 0)) if (!isProgress(n) && n.get("key").equals(visitRef())) last = n.path("value").get("body");
        assertNotNull(last);
        return last;
    }

    static JsonNode member(JsonNode workRecord, String mappingId) {
        for (JsonNode m : workRecord.get("members")) if (m.get("mappingId").asText().equals(mappingId)) return m;
        fail("no member " + mappingId);
        return null;
    }

    /** The latest sample row of nutrunner1's {@code metric} at {@code sourceTs}. */
    static JsonNode sampleRow(Path out, long sourceTs, String metric) throws IOException {
        JsonNode last = null;
        for (JsonNode n : lines(out, SAMPLES, 0))
            if (!isProgress(n) && n.path("key").path("device").asText().equals(NR1) && n.path("key").path("metric").asText().equals(metric)
                && n.path("value").path("body").path("sourceTs").longValue() == sourceTs) last = n.path("value").get("body");
        assertNotNull(last, metric + " at " + sourceTs);
        return last;
    }

    static JsonNode disposition(Path out, String topic, int partition, long offset, String part) throws IOException {
        JsonNode last = null;
        for (int p : List.of(0, 1, 2, 6)) for (JsonNode n : lines(out, LEDGER, p)) {
            if (isProgress(n)) continue;
            JsonNode k = n.get("key");
            if (k.get("inputTopic").asText().equals(topic) && k.get("inputPartition").intValue() == partition && k.get("inputOffset").longValue() == offset
                && k.get("part").asText().equals(part)) last = n.path("value").get("body");
        }
        assertNotNull(last, topic + "/" + partition + "/" + offset + " " + part);
        return last;
    }

    static List<String> rowsWithoutRunId(List<JsonNode> l) {
        List<String> out = new ArrayList<>();
        for (JsonNode n : l) if (!isProgress(n)) out.add(stripRunId(n));
        return out;
    }

    static String stripRunId(JsonNode n) {
        JsonNode c = n.deepCopy();
        JsonNode job = c.path("value").path("envelope").path("pins").path("job");
        if (job.isObject()) ((ObjectNode) job).remove("runId");
        return Canonical.utf8(Canonical.bytes(c));
    }

    static List<String> withoutRunId(byte[] file) {
        List<String> l = new ArrayList<>();
        for (String s : new String(file, StandardCharsets.UTF_8).split("\n")) if (!s.isEmpty()) l.add(stripRunId(Json.read(s.getBytes(StandardCharsets.UTF_8))));
        return l;
    }

    static TreeMap<String, byte[]> journalFiles(Path out) throws IOException {
        TreeMap<String, byte[]> m = tree(out);
        m.keySet().removeIf(k -> !(k.startsWith("journals/") || k.startsWith("quarantine/")));
        return m;
    }

    static TreeMap<String, byte[]> tree(Path root) throws IOException {
        TreeMap<String, byte[]> m = new TreeMap<>();
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) m.put(root.relativize(p).toString().replace('\\', '/'), Files.readAllBytes(p));
        }
        return m;
    }

    static Path copyTree(Path from, Path to) throws IOException {
        try (Stream<Path> s = Files.walk(from)) {
            for (Path p : s.toList()) {
                Path t = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
        return to;
    }

    // --- editing a copied recording: partition lines, offsets, batches and the manifest's digests ---

    static String line(long offset, String key, String rawValue) {
        return "{\"offset\":" + offset + ",\"timestamp\":null,\"key\":\"" + key + "\",\"headers\":{},\"value\":" + rawValue + "}";
    }

    static List<String> partitionLines(Path rec, String topic, int p) throws IOException {
        return new ArrayList<>(Files.readAllLines(rec.resolve(topic).resolve(p + ".jsonl"), StandardCharsets.UTF_8));
    }

    /** Rewrites one partition file through {@code edit}, renumbers its offsets from 0 and refreshes the manifest's digest of it. */
    static void editPartition(Path rec, String topic, int p, UnaryOperator<List<String>> edit) throws IOException {
        List<String> l = edit.apply(partitionLines(rec, topic, p));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < l.size(); i++) sb.append(l.get(i).replaceFirst("^\\{\"offset\":[0-9]+,", "{\"offset\":" + i + ",")).append('\n');
        Path f = rec.resolve(topic).resolve(p + ".jsonl");
        Files.write(f, sb.toString().getBytes(StandardCharsets.UTF_8));
        ObjectNode manifest = (ObjectNode) Json.read(rec.resolve("manifest.json"));
        ((ObjectNode) manifest.get("digests")).put(topic + "/" + p + ".jsonl", Canonical.digestOf(Files.readAllBytes(f)));
        Json.write(rec.resolve("manifest.json"), manifest);
    }

    /** Sets the through-offset of one partition to {@code through} in every batch from {@code fromCut} on. */
    static void throughFrom(Path rec, String topic, int p, long fromCut, long through) {
        ObjectNode manifest = (ObjectNode) Json.read(rec.resolve("manifest.json"));
        for (JsonNode b : manifest.get("batches"))
            if (b.get("cutId").longValue() >= fromCut) ((ObjectNode) b.path("throughOffsets").path(topic)).put(Integer.toString(p), through);
        Json.write(rec.resolve("manifest.json"), manifest);
    }

    /** Inserts lines after {@code offset}; every batch that already included {@code offset} includes them too. */
    static void insertAfter(Path rec, String topic, int p, int offset, List<String> added) throws IOException {
        editPartition(rec, topic, p, l -> { l.addAll(offset + 1, added); return l; });
        ObjectNode manifest = (ObjectNode) Json.read(rec.resolve("manifest.json"));
        for (JsonNode b : manifest.get("batches")) {
            ObjectNode t = (ObjectNode) b.path("throughOffsets").path(topic);
            long v = t.get(Integer.toString(p)).longValue();
            if (v >= offset) t.put(Integer.toString(p), v + added.size());
        }
        Json.write(rec.resolve("manifest.json"), manifest);
    }

    private static final Pattern TORQUE = Pattern.compile("(\"metric\":\"Torque\",\"value\":)([^,]+)");

    static String withTorque(String line, String literal) {
        Matcher m = TORQUE.matcher(line);
        assertTrue(m.find(), line);
        return line.substring(0, m.start(2)) + literal + line.substring(m.end(2));
    }

    /** Writes {@code literal} as the Torque value of nutrunner1's message at {@code sourceTs}; returns that line's offset. */
    static int torque(Path rec, long sourceTs, String literal) throws IOException {
        List<String> l = partitionLines(rec, "ot.messages", 1);
        int found = -1;
        for (int i = 0; i < l.size(); i++)
            if (l.get(i).contains("\"device\":\"" + NR1 + "\"") && l.get(i).contains("\"sourceTs\":" + sourceTs + ",")) { assertEquals(-1, found); found = i; }
        assertTrue(found >= 0, "no nutrunner1 message at " + sourceTs);
        int at = found;
        editPartition(rec, "ot.messages", 1, x -> { x.set(at, withTorque(x.get(at), literal)); return x; });
        return found;
    }
}
