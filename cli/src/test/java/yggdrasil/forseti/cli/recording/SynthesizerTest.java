package yggdrasil.forseti.cli.recording;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The synthesized horizon recordings (plan Task 17, Step 17.3). Partition files are read line by line, never whole: a horizon
 *  recording has about 450,000 messages. */
class SynthesizerTest {
    static final Path FIXTURES = Path.of("..", "fixtures"), SCENARIOS = FIXTURES.resolve("scenarios"), REGISTRY = FIXTURES.resolve("registry");
    static final List<String> HORIZON = List.of("horizon-no-departure", "horizon-reopened", "horizon-correction-beyond", "plc-horizon-no-end", "horizon-planned-late");
    static final long T0 = 1794182400000L, H = 86400000, O = 30000, HOLE_AT = 980000;
    static final String S1 = "synthetic-site:line2:station1", NR1 = S1 + ":nutrunner1";

    @TempDir Path tmp;

    /** Every record of one partition, in offset order, parsed one line at a time. */
    static void eachRecord(Path recording, String topic, int partition, Consumer<JsonNode> f) throws IOException {
        try (BufferedReader r = Files.newBufferedReader(recording.resolve(topic).resolve(partition + ".jsonl"), StandardCharsets.UTF_8)) {
            for (String line; (line = r.readLine()) != null;) f.accept(Json.read(line.getBytes(StandardCharsets.UTF_8)));
        }
    }

    static TreeMap<String, String> digests(Path root) throws IOException {
        TreeMap<String, String> m = new TreeMap<>();
        try (var s = Files.walk(root)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) m.put(Recording.relative(root, p), Canonical.digestOf(Files.readAllBytes(p)));
        }
        return m;
    }

    @Test void horizonNoDepartureBuildsInUnderSixtySecondsWithItsPlannedCountsAndNamesEverySampleOnce() throws IOException {
        Path out = tmp.resolve("hnd");
        long start = System.nanoTime();
        ScenarioBuilder.Built b = ScenarioBuilder.build(SCENARIOS.resolve("horizon-no-departure.json"), REGISTRY, out);
        long ms = (System.nanoTime() - start) / 1_000_000;
        assertTrue(ms < 60_000, "built in " + ms + " ms");
        // E = leave(visit 9) + tailMs = 88,560,000 + 751,000 = 89,311,000: 89,312 scans x 5 devices, x 7 members.
        assertEquals(446560, b.messages());
        assertEquals(625184, b.samples());
        Recording r = Recording.open(out);
        assertTrue(r.manifest().path("synthetic").booleanValue());
        assertEquals(Synthesizer.GENERATOR, r.manifest().path("source").path("generator").asText());
        r.verifyDigests();
        Set<String> delivered = new HashSet<>();
        long[] messages = {0};
        for (int p : r.routing().subscribed("ot.messages"))
            eachRecord(out, "ot.messages", p, line -> {
                messages[0]++;
                JsonNode v = line.path("value");
                for (JsonNode m : v.path("members"))
                    assertTrue(delivered.add(ScenarioBuilder.sampleKey(v.get("edge").asText(), v.get("device").asText(), v.get("birthPublishTs").longValue(),
                        m.get("metric").asText(), v.get("bdSeq").longValue(), v.get("seq").longValue(), v.get("publishTs").longValue())));
            });
        assertEquals(446560, messages[0]);
        JsonNode expected = r.expected();
        assertEquals(ExpectedSamples.RUNS, expected.path("samplesEncoding").asText());
        assertEquals(625184, ExpectedSamples.sampleCount(expected));
        Set<String> named = new HashSet<>();
        ExpectedSamples.forEachSample(expected, (k, v) -> assertTrue(named.add(k), "named twice: " + k));
        assertEquals(delivered, named, "expected.json names every delivered sampleId exactly once");
        // Injection 6: visit 8 provisional at cutA, insufficient missing-departure at cutB of enter + H; the later visit settles complete.
        JsonNode horizon = null;
        for (JsonNode t : expected.path("transitions")) if (t.path("assertion").asText().equals("visit-horizon")) horizon = t;
        assertNotNull(horizon);
        assertEquals(T0 + 1860000 + H, horizon.path("at").longValue());
        assertEquals(List.of("provisional", "insufficient"), List.of(horizon.path("atA").asText(), horizon.path("atB").asText()));
        assertEquals(T0 + 88260999, horizon.path("w1B").longValue());
    }

    @Test void twoBuildsAreByteIdentical() throws IOException {
        for (String n : List.of("horizon-no-departure", "horizon-planned-late")) {
            ScenarioBuilder.build(SCENARIOS.resolve(n + ".json"), REGISTRY, tmp.resolve(n + "-a"));
            ScenarioBuilder.build(SCENARIOS.resolve(n + ".json"), REGISTRY, tmp.resolve(n + "-b"));
            assertEquals(digests(tmp.resolve(n + "-a")), digests(tmp.resolve(n + "-b")), n);
        }
    }

    @Test void everyHorizonScenarioEqualsItsLockAndTheLockVerifies() throws IOException {
        assertEquals(5, HORIZON.size());
        for (String n : HORIZON) {
            ScenarioBuilder.Built b = ScenarioBuilder.build(SCENARIOS.resolve(n + ".json"), REGISTRY, tmp.resolve(n));
            assertArrayEquals(Files.readAllBytes(ScenarioBuilder.lockOf(SCENARIOS.resolve(n + ".json"))), Json.pretty(b.lock()), n + ": a fresh build differs from its lock");
            assertEquals(Synthesizer.GENERATOR, b.lock().path("generator").asText(), n);
        }
        PrintStream quiet = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        assertEquals(0, Main.run(new String[] {"recordings", "verify", "--scenario", SCENARIOS.resolve("horizon-correction-beyond.json").toString(),
            "--registry", REGISTRY.toString(), "--out", tmp.resolve("horizon-correction-beyond").toString()}, quiet, new PrintStream(err, true, StandardCharsets.UTF_8)),
            err.toString(StandardCharsets.UTF_8));
        // recordings synth refuses a hand-made scenario
        assertEquals(2, Main.run(new String[] {"recordings", "synth", "--scenario", SCENARIOS.resolve("baseline.json").toString(),
            "--registry", REGISTRY.toString(), "--out", tmp.resolve("not-synthetic").toString()}, quiet, quiet));
    }

    @Test void plannedLateManifestHasW1EqualToAAtThePlannedCut() throws IOException {
        Path out = tmp.resolve("hpl");
        ScenarioBuilder.build(SCENARIOS.resolve("horizon-planned-late.json"), REGISTRY, out);
        Recording r = Recording.open(out);
        Recording.Deadline planned = null;
        for (Recording.Deadline d : r.deadlines()) if (d.kind().equals("planned-late")) planned = d;
        assertNotNull(planned);
        long a = HOLE_AT + H - 1, t = a + O + 1;   // A = holeAt + H - 1 = 87,379,999; T = A + O + 1 = 87,410,000
        assertEquals(T0 + a, planned.at());
        Recording.Batch cut = r.batches().get((int) planned.cutB() - 1);
        // The per-station formula over everything delivered through the planned cut, and every raw partition's largest sourceTs.
        Map<String, Long> maxRouted = new TreeMap<>();
        for (int p : r.routing().subscribed("ot.messages")) {
            long through = cut.through("ot.messages", p);
            long[] max = {Long.MIN_VALUE};
            eachRecord(out, "ot.messages", p, line -> {
                if (line.path("offset").longValue() > through) return;
                String dev = line.path("key").asText();
                for (JsonNode m : line.path("value").path("members")) {
                    max[0] = Math.max(max[0], m.get("sourceTs").longValue());
                    maxRouted.merge(r.routing().stationOf(dev), m.get("sourceTs").longValue(), Math::max);
                }
            });
            assertEquals(T0 + t, max[0], "raw partition " + p + " at the planned cut");
        }
        assertEquals(T0 + a, maxRouted.get(S1) - O - 1, "W1 of station1 at the planned cut equals A");
        JsonNode tr = null;
        for (JsonNode x : r.expected().path("transitions")) if (x.path("assertion").asText().equals("planned-late")) tr = x;
        assertNotNull(tr);
        assertEquals(T0 + a, tr.path("w1B").longValue());
        assertEquals(planned.cutB(), tr.path("cutB").longValue());
    }

    @Test void theHoleIsAbsentOnTimeAndPresentAsTheFirstLateSample() throws IOException {
        Path out = tmp.resolve("hpl");
        ScenarioBuilder.build(SCENARIOS.resolve("horizon-planned-late.json"), REGISTRY, out);
        Recording r = Recording.open(out);
        int p = r.routing().rawPartitionOf(NR1);
        Recording.Deadline planned = null;
        for (Recording.Deadline d : r.deadlines()) if (d.kind().equals("planned-late")) planned = d;
        long through = r.batches().get((int) planned.cutB() - 1).through("ot.messages", p);
        List<JsonNode> onTimeAtHole = new ArrayList<>(), afterCut = new ArrayList<>();
        eachRecord(out, "ot.messages", p, line -> {
            JsonNode v = line.path("value");
            if (!v.path("device").asText().equals(NR1)) return;
            if (v.path("publishTs").longValue() == T0 + HOLE_AT + 50) onTimeAtHole.add(v);
            if (line.path("offset").longValue() > through && afterCut.size() < 4) afterCut.add(v);
        });
        assertEquals(1, onTimeAtHole.size());
        List<String> metrics = new ArrayList<>();
        onTimeAtHole.get(0).path("members").forEach(m -> metrics.add(m.path("metric").asText()));
        assertEquals(List.of("Running"), metrics, "the on-time scan at holeAt keeps Running and has no Torque");
        // The batch after the planned cut: the next scan's message, then the three late samples, seq continuing the session.
        JsonNode scan = afterCut.get(0);
        long[] lateTs = {T0 + HOLE_AT, T0 + HOLE_AT - 1, T0 + HOLE_AT - 2};
        for (int j = 0; j < 3; j++) {
            JsonNode late = afterCut.get(j + 1);
            assertEquals(scan.path("seq").longValue() + j + 1, late.path("seq").longValue());
            assertEquals(1, late.path("members").size());
            assertEquals("Torque", late.path("members").get(0).path("metric").asText());
            assertEquals(lateTs[j], late.path("members").get(0).path("sourceTs").longValue());
        }
        // The first late sample fills the hole; the expectation admits the first two and sends the third to replay.
        Map<String, JsonNode> exp = new HashMap<>();
        ExpectedSamples.forEachSample(r.expected(), exp::put);
        List<String> states = new ArrayList<>();
        for (int j = 1; j <= 3; j++) {
            JsonNode v = afterCut.get(j);
            JsonNode e = exp.get(ScenarioBuilder.sampleKey(v.get("edge").asText(), NR1, v.get("birthPublishTs").longValue(), "Torque", v.get("bdSeq").longValue(),
                v.get("seq").longValue(), v.get("publishTs").longValue()));
            states.add(e.path("state").asText() + " " + e.path("reason").asText() + " " + e.path("attributedTo").asText());
        }
        assertEquals(List.of("attributed null visit-3", "attributed null visit-3", "replay-required beyond-horizon null"), states);
        // S3: the third one's member disposition is replay-required beyond-horizon; the first two default to accepted
        Map<String, JsonNode> disp = new HashMap<>();
        ExpectedSamples.forEachDisposition(r.expected(), disp::put);
        List<String> parts = new ArrayList<>();
        for (int j = 1; j <= 3; j++) {
            JsonNode v = afterCut.get(j);
            JsonNode d = disp.get(ScenarioBuilder.sampleKey(v.get("edge").asText(), NR1, v.get("birthPublishTs").longValue(), "Torque", v.get("bdSeq").longValue(),
                v.get("seq").longValue(), v.get("publishTs").longValue()));
            parts.add(d == null ? "unlisted" : d.path("parts").path("member").path("disposition").asText() + " " + d.path("parts").path("member").path("reason").asText());
        }
        assertEquals(List.of("unlisted", "unlisted", "replay-required beyond-horizon"), parts);
        JsonNode visit3 = null;
        for (JsonNode v : r.expected().path("visits")) if (v.path("visitRef").asText().equals("visit-3")) visit3 = v;
        assertEquals(0, visit3.path("equipmentEvidence").path(NR1).path("members").path("nutrunner1-torque").path("missingPeriods").longValue());
    }
}
