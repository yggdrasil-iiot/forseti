package yggdrasil.forseti.cli.recording;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;

/** The engine scenarios (step-6 plan D11) and the builder's W1-form parameter (spec 622), the injection-24 overlay (spec 681) and the
 *  tail rule (spec 628). The committed scenario files must equal their derivation; the two smoke recordings are rebuilt to their locks
 *  (every other lock is checked by {@code forseti recordings check-engine}). */
class EngineScenariosTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    static final Path FIXTURES = ROOT.resolve("fixtures"), STEP3 = FIXTURES.resolve("scenarios"), ENGINE = ROOT.resolve("engine-fixtures");
    static final Path LIVE = FIXTURES.resolve("live/registry");

    @TempDir Path tmp;

    @Test void theCommittedEngineScenariosEqualTheirDerivation() {
        assertEquals(List.of(), EngineScenarios.check(STEP3, ENGINE));
        List<EngineScenarios.Derived> d = EngineScenarios.derive(STEP3);
        long shared = d.stream().filter(x -> x.placement() == EngineScenarios.SHARED).count();
        assertEquals(EngineScenarios.STEP3.size() - EngineScenarios.NOT_REBUILT.size() + 3, shared, "every rebuilt step-3 scenario, injection 24 and the two speeds");
        assertEquals(EngineScenarios.DEDICATED_TOO.size() + 3, d.size() - shared);
    }

    @Test void aDerivedScenarioIsItsStep3ScenarioOnThePlacement() {
        for (EngineScenarios.Derived x : EngineScenarios.derive(STEP3)) {
            if (!x.kind().equals("semantic")) continue;
            ObjectNode s = x.scenario();
            String text = new String(Json.pretty(s), java.nio.charset.StandardCharsets.UTF_8);
            assertFalse(text.contains(EngineScenarios.STEP3_ROOT), x.file());
            s.path("devices").fieldNames().forEachRemaining(dev -> assertTrue(dev.startsWith(x.placement().site() + ":"), dev));
            assertEquals(EngineScenarios.W1_FORM, s.get("w1Form").asText());
            assertTrue(s.path("rule").path("stream").asText().startsWith("line2-" + x.placement().name()));
            // renamed back, without the pinned release and the form, it is the step-3 file
            ObjectNode back = (ObjectNode) Json.read(text.replace(x.placement().site() + ":", EngineScenarios.STEP3_ROOT + ":").getBytes(java.nio.charset.StandardCharsets.UTF_8));
            back.remove("w1Form");
            ObjectNode step3 = (ObjectNode) Json.read(STEP3.resolve(x.source() + ".json"));
            back.set("rule", step3.get("rule"));
            assertEquals(step3, back, x.file());
        }
    }

    @Test void theSmokeRecordingsRebuildToTheirLocks() throws IOException {
        for (EngineScenarios.Placement p : EngineScenarios.PLACEMENTS) {
            Path scenario = ENGINE.resolve("scenarios").resolve(p.name()).resolve("smoke.json");
            ScenarioBuilder.Built b = ScenarioBuilder.build(scenario, LIVE, tmp.resolve(p.name()));
            assertArrayEquals(Files.readAllBytes(ScenarioBuilder.lockOf(scenario)), Json.pretty(b.lock()), p.name());
            // the placement's routing is the governed one
            assertEquals(Json.read(FIXTURES.resolve("live/routing").resolve(p.name() + ".json")), Json.read(tmp.resolve(p.name()).resolve(Recording.ROUTING)));
        }
    }

    /** The largest delivered sourceTs of every subscribed raw partition through every batch, read back from a built recording. */
    static Map<Integer, long[]> partitionMax(Recording rec) {
        Map<Integer, long[]> m = new TreeMap<>();
        List<Recording.Batch> bs = rec.batches();
        for (int p : rec.routing().subscribed("ot.messages")) {
            long[] a = new long[bs.size()];
            long max = Long.MIN_VALUE;
            List<Recording.Record> records = rec.records("ot.messages", p);
            int off = 0;
            for (int k = 0; k < bs.size(); k++) {
                for (; off <= bs.get(k).through("ot.messages", p); off++)
                    for (JsonNode x : records.get(off).value().path("members")) max = Math.max(max, x.get("sourceTs").longValue());
                a[k] = max;
            }
            m.put(p, a);
        }
        return m;
    }

    @Test void bracketsArePlannedWithTheScenariosW1Form() throws IOException {
        // the dedicated baseline in both forms: every deadline is bracketed by the min-over-partitions W1 of the Flink form
        Path scenario = ENGINE.resolve("scenarios/dedicated/baseline.json");
        ScenarioBuilder.build(scenario, LIVE, tmp.resolve("min"));
        Recording rec = Recording.open(tmp.resolve("min"));
        assertEquals(EngineScenarios.W1_FORM, rec.manifest().path("w1Form").asText());
        Map<Integer, long[]> max = partitionMax(rec);
        long oo = 30_000;
        assertFalse(rec.deadlines().isEmpty());
        for (Recording.Deadline d : rec.deadlines()) {
            int a = (int) d.cutA() - 1, b = (int) d.cutB() - 1;
            long w1a = Long.MAX_VALUE, w1b = Long.MAX_VALUE;
            for (long[] x : max.values()) { w1a = Math.min(w1a, x[a] - oo - 1); w1b = Math.min(w1b, x[b] - oo - 1); }
            assertTrue(w1a < d.at() && d.at() <= w1b, d.ref() + " at " + d.at() + ": W1 " + w1a + ", " + w1b);
        }
        // the per-station form of the same scenario is a valid scenario too, without w1Form in its manifest
        ObjectNode s = (ObjectNode) Json.read(scenario);
        s.remove("w1Form");
        Path f = tmp.resolve("per-station.json");
        Json.write(f, s);
        ScenarioBuilder.build(f, LIVE, tmp.resolve("per"));
        assertTrue(Recording.open(tmp.resolve("per")).manifest().path("w1Form").isMissingNode());
        s.put("w1Form", "per-partition");
        Json.write(f, s);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> ScenarioBuilder.build(f, LIVE, tmp.resolve("bad"))).getMessage().contains("W1 form"));
    }

    @Test void injection24WithholdsTheStationAndDeclaresItStarved() throws IOException {
        for (EngineScenarios.Placement p : EngineScenarios.PLACEMENTS) {
            Path scenario = ENGINE.resolve("scenarios").resolve(p.name()).resolve(EngineScenarios.STARVED_SCENARIO + ".json");
            Path out = tmp.resolve("starved-" + p.name());
            ScenarioBuilder.Built built = ScenarioBuilder.build(scenario, LIVE, out);
            assertArrayEquals(Files.readAllBytes(ScenarioBuilder.lockOf(scenario)), Json.pretty(built.lock()), p.name());
            Recording rec = Recording.open(out);
            String station = p.site() + ":" + EngineScenarios.STARVED_STATION;
            long t0 = rec.t0(), from = t0 + EngineScenarios.STARVE_FROM, resume = t0 + EngineScenarios.STARVE_RESUME;
            int raw = rec.routing().rawPartitionOf(station);
            JsonNode starvation = rec.manifest().path("starvation");
            assertEquals(station, starvation.path("station").asText());
            assertEquals(List.of(raw), toInts(starvation.path("partitions")));
            List<Recording.Batch> bs = rec.batches();
            List<Recording.Record> records = rec.records("ot.messages", raw);
            int off = 0;
            boolean burst = false;
            for (int k = 0; k < bs.size(); k++) {
                boolean idle = !bs.get(k).idleInputs().isEmpty();
                long lastTs = Long.MIN_VALUE;
                boolean late = false;
                for (; off <= bs.get(k).through("ot.messages", raw); off++) {
                    JsonNode v = records.get(off).value();
                    if (!v.path("device").asText().equals(station)) continue;
                    long ts = v.path("members").get(0).get("sourceTs").longValue();
                    if (idle) fail(p.name() + ": cut " + bs.get(k).cutId() + " is starved but carries the station's telemetry at " + ts);
                    if (ts < lastTs) late = true;
                    if (ts >= from && ts < resume) burst = true;
                    lastTs = ts;
                }
                if (idle) {
                    JsonNode e = bs.get(k).idleInputs().get(0);
                    assertEquals(station, e.path("station").asText());
                    assertEquals("starved", e.path("kind").asText());
                    assertEquals(raw, e.path("partition").asInt());
                    assertEquals(starvation.path("frozenAt").asLong(), e.path("frozenAt").asLong());
                }
                assertFalse(late, "the late burst is appended in scan order");
            }
            assertTrue(burst, p.name() + ": the withheld telemetry is delivered as a late burst on resume");
            Set<String> held = new TreeSet<>();
            starvation.path("heldDeadlines").forEach(x -> held.add(x.path("station").asText()));
            // spec 681, Decision 18: on the shared placement the other station on the raw partition keeps W1 moving; on the dedicated one
            // the starved partition holds the min-over-partitions W1 of every station
            assertEquals(p == EngineScenarios.SHARED ? Set.of(station) : new TreeSet<>(rec.routing().stations().keySet()), held, p.name());
        }
    }

    @Test void aTailShorterThanLatenessPlusOutOfOrdernessPlusThePeriodIsRefused() throws IOException {
        ObjectNode s = (ObjectNode) Json.read(ENGINE.resolve("scenarios/shared/baseline.json"));
        Json.putSafe(s, "tailMs", 750_999);
        Path f = tmp.resolve("short-tail.json");
        Json.write(f, s);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> ScenarioBuilder.build(f, LIVE, tmp.resolve("x"))).getMessage().contains("tailMs 750999"));
    }

    static List<Integer> toInts(JsonNode a) { List<Integer> l = new ArrayList<>(); a.forEach(x -> l.add(x.asInt())); return l; }
}
