package yggdrasil.forseti.cli.replay;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.Stage2;

/** Injection 24's observation deadline (spec 681, item 5 (c)) on the committed starved-station3 recordings of both placements. */
class StarvationObserverTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    static final Path REGISTRY = ROOT.resolve("fixtures/live/registry");
    // the attribution rule of the governed placements: outOfOrdernessMs 30000, coverage.expectedPeriodMs 1000
    static final long O = 30000, P = 1000;

    @TempDir Path tmp;

    Recording built(String placement) {
        Path dir = tmp.resolve(placement);
        ScenarioBuilder.build(ROOT.resolve("engine-fixtures/scenarios/" + placement + "/starved-station3.json"), REGISTRY, dir);
        return Recording.open(dir);
    }

    @Test void aRecordingWithoutStarvationHasNoObserver() {
        Path dir = tmp.resolve("baseline");
        ScenarioBuilder.build(ROOT.resolve("engine-fixtures/scenarios/dedicated/baseline.json"), REGISTRY, dir);
        assertNull(StarvationObserver.of(Recording.open(dir), O, P));
    }

    @Test void onTheDedicatedPlacementEveryDeadlineOfTheOtherStationsInTheWindowCountsFromAStarvedBatch() {
        Recording rec = built("dedicated");
        StarvationObserver o = StarvationObserver.of(rec, O, P);
        assertEquals("site-071:line2:station3", o.starved());
        assertFalse(o.checks().isEmpty());
        for (StarvationObserver.Check c : o.checks()) {
            assertNotEquals(o.starved(), c.station());
            assertTrue(o.starvedCuts().contains(c.countingCut()), c.toString());
        }
        // every held deadline of another station (the starvation keeps W1 from bracketing it) is checked
        Set<String> checked = new TreeSet<>();
        for (StarvationObserver.Check c : o.checks()) checked.add(c.station() + " " + c.ref());
        for (JsonNode h : rec.manifest().path("starvation").path("heldDeadlines"))
            if (!h.path("station").asText().equals(o.starved())) assertTrue(checked.contains(h.path("station").asText() + " " + h.path("ref").asText()), h + " not checked: " + checked);
        // no deadline of the starved station is checked
        for (StarvationObserver.Unchecked u : o.unchecked()) assertNotEquals(o.starved(), u.station());
    }

    @Test void theCountingBatchIsTheFirstWhoseRoutedTelemetryReachesTheDeadlinePlusOPlusPOnEveryOtherStation() {
        Recording rec = built("shared");
        StarvationObserver o = StarvationObserver.of(rec, O, P);
        assertFalse(o.checks().isEmpty(), "the shared placement has deadlines of other stations inside the starvation window");
        List<Long> cuts = new ArrayList<>();
        for (Recording.Batch b : rec.batches()) cuts.add(b.cutId());
        Map<String, long[]> largest = StarvationObserver.largestRouted(rec, cuts);
        for (StarvationObserver.Check c : o.checks()) {
            int k = cuts.indexOf(c.countingCut());
            long need = c.deadline() + O + P;
            for (Map.Entry<String, long[]> e : largest.entrySet()) {
                if (e.getKey().equals(o.starved())) continue;
                assertTrue(e.getValue()[k] >= need, c + " " + e.getKey());
            }
            boolean before = true;
            for (Map.Entry<String, long[]> e : largest.entrySet()) if (!e.getKey().equals(o.starved()) && k > 0 && e.getValue()[k - 1] < need) before = false;
            assertFalse(k > 0 && before, c + ": an earlier batch already reached " + need);
        }
    }

    /** A completion reader stand-in: the latest progress per writer, set by the test. */
    static final class Progress implements StarvationObserver.ProgressSource {
        final Map<CompletionReader.Writer, JsonNode> latest = new HashMap<>();
        final AtomicLong clock;
        final long stepNs;
        Progress(AtomicLong clock, long stepNs) { this.clock = clock; this.stepNs = stepNs; }
        @Override public Map<CompletionReader.Writer, JsonNode> latest(Collection<CompletionReader.Writer> writers, Duration poll) { clock.addAndGet(stepNs); return latest; }
        void set(int partition, long w1, long w2) {
            latest.put(new CompletionReader.Writer(Stage1.WORK_RECORDS, partition), Json.object().put("W1", w1));
            latest.put(new CompletionReader.Writer(Stage2.AFFECTED, partition), Json.object().put("W2", w2));
        }
    }

    @Test void aCheckSettlesWhenBothStagesReachTheDeadlineInTimeAndIsAMissOtherwise() {
        Recording rec = built("dedicated");
        StarvationObserver o = StarvationObserver.of(rec, O, P);
        long cut = o.checks().get(0).countingCut();
        List<StarvationObserver.Check> mine = o.checks().stream().filter(c -> c.countingCut() == cut).toList();
        AtomicLong clock = new AtomicLong(1_000_000_000L);
        Progress p = new Progress(clock, Duration.ofSeconds(1).toNanos());
        o.appended(cut, clock.get());
        long max = mine.stream().mapToLong(StarvationObserver.Check::deadline).max().orElseThrow();
        // stage 1 at the deadline, stage 2 one short of it: not settled; the observation ends after 60 s with a miss
        for (StarvationObserver.Check c : mine) p.set(rec.routing().partitionOf(c.station()), max, max - 1);
        List<ObjectNode> r = o.observe(cut, p, clock::get);
        assertEquals(mine.size(), r.size());
        for (ObjectNode x : r) {
            assertEquals("miss", x.path("outcome").asText(), x.toString());
            assertTrue(x.path("observedForMs").asLong() >= StarvationObserver.DEADLINE.toMillis(), x.toString());
        }
        // the next counting batch: both stages past the deadline at once
        Optional<StarvationObserver.Check> next = o.checks().stream().filter(c -> c.countingCut() > cut).findFirst();
        assertTrue(next.isPresent());
        long cut2 = next.get().countingCut();
        o.appended(cut2, clock.get());
        long max2 = o.checks().stream().filter(c -> c.countingCut() == cut2).mapToLong(StarvationObserver.Check::deadline).max().orElseThrow();
        for (StarvationObserver.Check c : o.checks()) p.set(rec.routing().partitionOf(c.station()), max2, max2);
        for (ObjectNode x : o.observe(cut2, p, clock::get)) {
            assertEquals("settled", x.path("outcome").asText(), x.toString());
            assertEquals(1000, x.path("settledAfterMs").asLong(), x.toString());
        }
        JsonNode j = o.toJson();
        assertEquals(o.checks().size(), j.path("planned").size());
        assertTrue(j.path("checks").size() >= 2);
        // a batch without checks observes nothing
        assertEquals(List.of(), o.observe(-1, p, clock::get));
    }

    @Test void aSettlementAfterTheDeadlineIsStillAMiss() {
        Recording rec = built("dedicated");
        StarvationObserver o = StarvationObserver.of(rec, O, P);
        long cut = o.checks().get(0).countingCut();
        AtomicLong clock = new AtomicLong(0);
        o.appended(cut, 0);
        // the cut itself took 61 s to complete: the progress is there at the first poll, but after the deadline
        clock.set(Duration.ofSeconds(61).toNanos());
        Progress p = new Progress(clock, 1);
        for (StarvationObserver.Check c : o.checks()) p.set(rec.routing().partitionOf(c.station()), 4_000_000_000_000L, 4_000_000_000_000L);
        for (ObjectNode x : o.observe(cut, p, clock::get)) assertEquals("miss", x.path("outcome").asText(), x.toString());
    }

    @Test void everyNonStarvedStationWithADeadlineInTheWindowHasACheck() {
        for (String placement : List.of("dedicated", "shared")) {
            StarvationObserver o = StarvationObserver.of(built(placement), O, P);
            JsonNode cov = o.coverage();
            assertEquals(0, cov.path("uncovered").size(), placement + " " + cov);
            assertEquals(2, cov.path("stations").size(), placement + ": both non-starved stations count " + cov);
            cov.path("stations").fields().forEachRemaining(e -> {
                assertTrue(e.getValue().path("routedTelemetry").asBoolean(), placement + " " + e);
                assertTrue(e.getValue().path("checks").asLong() > 0, placement + " " + e);
            });
            assertEquals(cov, o.toJson().path("coverage"));
        }
    }

    @Test void aMissIsRecordedAtTheDeadlineWhileTheCutIsAwaitedSoTheHarnessCanAdvance() {
        Recording rec = built("dedicated");
        StarvationObserver o = StarvationObserver.of(rec, O, P);
        long cut = o.checks().get(0).countingCut();
        assertTrue(o.counts(cut));
        assertFalse(o.resolved(cut));
        AtomicLong clock = new AtomicLong(0);
        o.appended(cut, 0);
        Progress p = new Progress(clock, Duration.ofSeconds(10).toNanos());
        // nothing committed: five polls (50 s) leave the checks open, the sixth and seventh pass the 60 s deadline
        int polls = 0;
        while (!o.poll(cut, p, clock::get)) polls++;
        assertEquals(6, polls);
        assertTrue(o.resolved(cut));
        assertTrue(o.missed(cut));
        // a settled check is settled at its own writers' progress, whatever any other writer does
        long cut2 = o.checks().stream().filter(c -> c.countingCut() > cut).findFirst().orElseThrow().countingCut();
        o.appended(cut2, clock.get());
        for (StarvationObserver.Check c : o.checks()) p.set(rec.routing().partitionOf(c.station()), 4_000_000_000_000L, 4_000_000_000_000L);
        assertTrue(o.poll(cut2, p, clock::get));
        assertFalse(o.missed(cut2));
        // a cut without checks is resolved at once
        assertTrue(o.resolved(-1));
        assertFalse(o.counts(-1));
    }
}
