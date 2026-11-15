package yggdrasil.forseti.cli.harness;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.recording.EngineScenarios;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.cli.recording.W1Form;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.journal.Progress;

/** The reference harness on engine recordings (governed placements, the Flink W1 form): the min-over-partitions form it reports, and
 *  injection 24, where the starved station completes no cut while it is starved and merges the cuts it skipped into the progress of the
 *  cut that carries its late burst, at both stages and on its ledger, while every other station completes every cut. */
class HarnessW1FormTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    static final Path LIVE = ROOT.resolve("fixtures/live/registry"), KEYS = ROOT.resolve("fixtures/live/trusted-keys.jsonl");
    static final Path ENGINE = ROOT.resolve("engine-fixtures/scenarios");
    static final long OO = 30_000;

    @TempDir Path tmp;

    Path run(Path scenario, W1Form form) throws IOException {
        Path rec = tmp.resolve("rec"), state = tmp.resolve("state"), out = tmp.resolve("run");
        ScenarioBuilder.build(scenario, LIVE, rec);
        JsonNode rule = Json.read(scenario).path("rule");
        String stream = rule.path("stream").asText();
        JsonNode statement = Json.read(Base64.getDecoder().decode(Json.read(LIVE.resolve("mappings/head-attestations/" + stream + ".dsse.json")).get("payload").asText()));
        Clock clock = Clock.fixed(Instant.parse(statement.path("predicate").path("signedAt").asText()).plusSeconds(60), ZoneOffset.UTC);
        ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
        int exit = GenerationCli.run(new String[] {"new", "--state", state.toString(), "--mappings", LIVE.resolve("mappings").toString(), "--trusted-keys", KEYS.toString(),
            "--stream", stream, "--rule", rule.path("attributionRuleId").asText() + "@" + rule.path("attributionRuleVersion").asText(), "--recording", rec.toString()},
            new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8), clock);
        assertEquals(0, exit, o.toString(StandardCharsets.UTF_8) + e.toString(StandardCharsets.UTF_8));
        GenerationCli.prepareRun(state, 1, rec);
        ReferenceEngine.run(new ReferenceEngine.Options(rec, LIVE, state, 1, out, ReferenceEngine.DEFAULT_SEED, false, Map.of(), "ab".repeat(32),
            "forseti run --engine core", null, null, null, form));
        return out;
    }

    @Test void theMinOverPartitionsFormReportsTheFlinkFormulaAtEveryStation() throws IOException {
        Path out = run(ENGINE.resolve("dedicated/smoke.json"), W1Form.MIN_OVER_PARTITIONS);
        Recording rec = Recording.open(tmp.resolve("rec"));
        JsonNode log = Json.read(out.resolve("cuts.json"));
        assertEquals("min-over-partitions", log.path("w1Form").asText());
        Map<Integer, Long> largest = new TreeMap<>();
        Map<Integer, Integer> next = new HashMap<>();
        Long previous = null;
        int k = 0;
        for (Recording.Batch b : rec.batches()) {
            for (int p : rec.routing().subscribed("ot.messages")) {
                List<Recording.Record> rs = rec.records("ot.messages", p);
                for (int off = next.getOrDefault(p, 0); off <= b.through("ot.messages", p); off++)
                    for (JsonNode m : rs.get(off).value().path("members")) largest.merge(p, m.get("sourceTs").longValue(), Math::max);
                next.put(p, (int) b.through("ot.messages", p) + 1);
            }
            long want = Collections.min(largest.values()) - OO - 1;
            JsonNode c = log.path("cuts").get(k++);
            for (String s : rec.routing().stations().keySet()) {
                assertEquals(want, c.path("perStation").path(s).path("W1").longValue(), "cut " + b.cutId() + " " + s);
                assertEquals(previous, c.path("perStation").path(s).path("admissionW").isNull() ? null : c.path("perStation").path(s).path("admissionW").longValue());
            }
            previous = want;
        }
    }

    @Test void aStarvedStationMergesTheCutsItSkippedIntoItsResumeProgress() throws IOException {
        Path out = run(ENGINE.resolve("shared/" + EngineScenarios.STARVED_SCENARIO + ".json"), W1Form.MIN_OVER_PARTITIONS);
        Recording rec = Recording.open(tmp.resolve("rec"));
        String starved = rec.manifest().path("starvation").path("station").asText();
        List<Long> skipped = new ArrayList<>();
        for (Recording.Batch b : rec.batches()) if (!b.idleInputs().isEmpty()) skipped.add(b.cutId());
        assertFalse(skipped.isEmpty());
        long resume = skipped.get(skipped.size() - 1) + 1;
        JsonNode log = Json.read(out.resolve("cuts.json"));
        for (JsonNode c : log.path("cuts")) {
            long cut = c.path("cutId").asLong();
            for (String s : rec.routing().stations().keySet()) {
                JsonNode v = c.path("perStation").path(s);
                if (s.equals(starved) && skipped.contains(cut)) { assertTrue(v.isMissingNode(), "cut " + cut + " lists the starved station"); continue; }
                assertFalse(v.isMissingNode(), "cut " + cut + " " + s);
                List<Long> merged = new ArrayList<>();
                v.path("mergedCutIds").forEach(x -> merged.add(x.asLong()));
                assertEquals(s.equals(starved) && cut == resume ? skipped : List.of(), merged, "cut " + cut + " " + s);
            }
        }
        int p = rec.routing().partitionOf(starved);
        for (String topic : List.of("forseti.work-records", "forseti.samples", "forseti.dispositions-stage1", "forseti.affected", "forseti.affected-sets", "forseti.dispositions-stage2")) {
            List<JsonNode> progress = new ArrayList<>();
            for (String line : Files.readAllLines(JournalFiles.journal(out, topic, p), StandardCharsets.UTF_8)) {
                JsonNode n = Json.read(line.getBytes(StandardCharsets.UTF_8));
                if ("progress".equals(n.path("value").path("type").textValue())) progress.add(n.get("value"));
            }
            List<Long> cuts = new ArrayList<>();
            progress.forEach(x -> cuts.add(x.path("cutId").asLong()));
            for (long c : skipped) assertFalse(cuts.contains(c), topic + " has a progress of the skipped cut " + c);
            JsonNode r = progress.stream().filter(x -> x.path("cutId").asLong() == resume).findFirst().orElseThrow();
            List<Long> merged = new ArrayList<>();
            r.path("mergedCutIds").forEach(x -> merged.add(x.asLong()));
            assertEquals(skipped, merged, topic);
            if (topic.equals("forseti.affected"))
                assertEquals(6 * (1 + skipped.size()), r.path("incidentOffsets").size(), "6 x (1 + number of mergedCutIds)");
            if (topic.equals("forseti.work-records")) {
                Progress.Stage1 s1 = (Progress.Stage1) Progress.fromJson(r);
                Progress.Stage1 before = (Progress.Stage1) Progress.fromJson(progress.get(cuts.indexOf(skipped.get(0) - 1)));
                assertEquals(before.W1(), s1.admissionW(), "the merged inputs are admitted against the last committed progress");
                for (Progress.BarrierRef b : s1.barrierRefs()) assertEquals(resume, b.cutId(), "no barrier of a starved cut was collected");
            }
        }
        // the reject collector counts every barrier, whatever the idle inputs say
        List<Long> six = new ArrayList<>();
        for (String line : Files.readAllLines(JournalFiles.journal(out, "forseti.dispositions-stage1", RejectCollector.PARTITION), StandardCharsets.UTF_8)) {
            JsonNode n = Json.read(line.getBytes(StandardCharsets.UTF_8)).path("value");
            if ("progress".equals(n.path("type").textValue())) { six.add(n.path("cutId").asLong()); assertEquals(0, n.path("mergedCutIds").size()); }
        }
        assertEquals(rec.batches().stream().map(Recording.Batch::cutId).toList(), six);
    }
}
