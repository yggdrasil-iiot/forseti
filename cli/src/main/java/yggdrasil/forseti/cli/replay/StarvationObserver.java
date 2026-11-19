package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.Stage2;

/**
 * The observation deadline of injection 24 (spec 681, item 5 (c), spec 418), engine-neutral: the harness side of a run of a recording
 * that carries {@code manifest.starvation} (the {@code starve-station} overlay). Every ground-truth deadline {@code d} of a station that
 * is not starved is checked when its counting batch is a starved batch (one whose {@code idleInputs} declare the starved station
 * {@code starved}): the counting batch is the first batch after whose append, for every non-starved station {@code s} that has routed
 * telemetry in the recording, the largest {@code sourceTs} of the telemetry routed to {@code s} is at least
 * {@code d + outOfOrdernessMs + expectedPeriodMs} (judged on the recording's routed telemetry, not on the engine's {@code W1}). Every
 * non-starved station counts: a station without routed telemetry is never dropped, so it leaves its deadlines unchecked and its
 * coverage short ({@link #coverage}). A check settles when the station's committed stage-1 progress on {@code forseti.work-records}
 * reports {@code W1 >= d} and its committed stage-2 progress on {@code forseti.affected} reports {@code W2 >= d} (read with
 * {@code read_committed}: the rows of the deadline are committed with that progress); it must settle within {@link #DEADLINE} of the
 * counting batch's first append (spec 681: the earliest broker append time of the batch's records, {@link BatchWriter.Written}),
 * else it is recorded as a miss. The harness observes the checks while it waits for the cut ({@link #poll}), so another writer's delay
 * never turns a settled check into a miss, and it writes the next batch once the cut is complete, or once every check of the batch has
 * settled or recorded its miss even if the cut is not complete (spec 418).
 *
 * <p>The deadlines are the ground truth's: the transitions of {@code expected.json} (each with its station) and the deadlines the
 * starvation keeps from being bracketed, {@code manifest.starvation.heldDeadlines}.
 */
public final class StarvationObserver {
    /** The observation deadline of H-isolation (spec 25, item 5 (c)). */
    public static final Duration DEADLINE = Duration.ofSeconds(60);
    /** The file a run writes the observation to, beside its run record. */
    public static final String FILE = "isolation.json";

    /** One checked deadline: its station, kind, ref and time, and the cut of its counting batch. */
    public record Check(String station, String kind, String ref, long deadline, long countingCut) {}

    /** A deadline of a non-starved station that is not checked, with the reason (its counting batch is not starved, or it has none). */
    public record Unchecked(String station, String kind, String ref, long deadline, String why) {}

    private final String starved;
    private final Set<Long> starvedCuts;
    private final List<Check> checks;
    private final List<Unchecked> unchecked;
    private final RoutingManifest routing;
    private final long oooMs, periodMs;
    private final ArrayNode results = Json.JSON.createArrayNode();
    /** Per counting cut, its batch's first append on the harness's {@link System#nanoTime} clock. */
    private final Map<Long, Long> firstAppendNanos = new TreeMap<>();
    private final Map<Check, Long> settledNanos = new HashMap<>();
    private final Map<Check, long[]> lastSeen = new HashMap<>();
    private final Set<Check> resolved = new HashSet<>();
    private final ObjectNode coverage;

    private StarvationObserver(String starved, Set<Long> starvedCuts, List<Check> checks, List<Unchecked> unchecked, RoutingManifest routing, long oooMs, long periodMs,
                               ObjectNode coverage) {
        this.starved = starved; this.starvedCuts = starvedCuts; this.checks = checks; this.unchecked = unchecked; this.routing = routing;
        this.oooMs = oooMs; this.periodMs = periodMs; this.coverage = coverage;
    }

    /** The observer of {@code rec}'s starvation, or null when the recording carries none. {@code oooMs} and {@code periodMs}: the run's
     *  attribution rule's {@code outOfOrdernessMs} and {@code coverage.expectedPeriodMs}. */
    public static StarvationObserver of(Recording rec, long oooMs, long periodMs) {
        JsonNode s = rec.manifest().path("starvation");
        if (!s.isObject()) return null;
        String starved = Json.text(s, "station");
        RoutingManifest routing = rec.routing();
        if (!routing.stations().containsKey(starved)) throw new IllegalArgumentException("malformed recording: starvation of an undeclared station " + starved);
        Set<Long> starvedCuts = new TreeSet<>();
        List<Long> cuts = new ArrayList<>();
        for (Recording.Batch b : rec.batches()) {
            cuts.add(b.cutId());
            for (CutAssembler.Idle i : EngineContract.idleInputs(b)) if (i.kind().equals(CutAssembler.STARVED) && i.station().equals(starved)) starvedCuts.add(b.cutId());
        }
        if (starvedCuts.isEmpty()) throw new IllegalArgumentException("malformed recording: a starvation of " + starved + " that no batch declares");
        // the largest routed sourceTs of every station after every batch
        Map<String, long[]> largest = largestRouted(rec, cuts);
        // every non-starved station of the routing counts, with or without routed telemetry
        Set<String> others = new TreeSet<>(routing.stations().keySet());
        others.remove(starved);
        // the ground-truth deadlines, each once
        Map<String, JsonNode> deadlines = new LinkedHashMap<>();
        for (JsonNode t : rec.expected().path("transitions"))
            deadlines.putIfAbsent(key(t.path("station").asText(), t.path("assertion").asText(), t.path("ref").asText(), t.path("at").asLong()),
                Json.object().put("station", t.path("station").asText()).put("kind", t.path("assertion").asText()).put("ref", t.path("ref").asText()).put("at", t.path("at").asLong()));
        for (JsonNode h : s.path("heldDeadlines"))
            deadlines.putIfAbsent(key(h.path("station").asText(), h.path("kind").asText(), h.path("ref").asText(), h.path("at").asLong()), h);
        List<Check> checks = new ArrayList<>();
        List<Unchecked> unchecked = new ArrayList<>();
        for (JsonNode d : deadlines.values()) {
            String station = d.path("station").asText(), kind = d.path("kind").asText(), ref = d.path("ref").asText();
            long at = d.path("at").asLong();
            if (station.equals(starved)) continue;
            if (!routing.stations().containsKey(station)) throw new IllegalArgumentException("malformed recording: deadline " + kind + " " + ref + " of an undeclared station " + station);
            Long counting = countingCut(at + oooMs + periodMs, others, largest, cuts);
            if (counting == null) unchecked.add(new Unchecked(station, kind, ref, at, "no batch carries routed telemetry at or past " + (at + oooMs + periodMs) + " on every non-starved station"));
            else if (!starvedCuts.contains(counting)) unchecked.add(new Unchecked(station, kind, ref, at, "its counting batch, cut " + counting + ", is not starved"));
            else checks.add(new Check(station, kind, ref, at, counting));
        }
        checks.sort(Comparator.comparingLong(Check::countingCut).thenComparingLong(Check::deadline).thenComparing(Check::station).thenComparing(Check::ref));
        // coverage: every non-starved station with a ground-truth deadline in the window the starvation can hold, from its start less
        // O + P to the resume, must have at least one check
        long from = s.path("from").asLong(), resume = s.path("resumeAt").asLong();
        ObjectNode cov = Json.object();
        Json.putSafe(cov, "windowFrom", from - oooMs - periodMs);
        Json.putSafe(cov, "windowTo", resume);
        ObjectNode per = cov.putObject("stations");
        ArrayNode short_ = cov.putArray("uncovered");
        for (String st : others) {
            long inWindow = 0, checked = 0;
            for (JsonNode d : deadlines.values()) if (st.equals(d.path("station").asText()) && d.path("at").asLong() >= from - oooMs - periodMs && d.path("at").asLong() < resume) inWindow++;
            for (Check c : checks) if (c.station().equals(st)) checked++;
            ObjectNode o = per.putObject(st);
            Json.putSafe(o, "deadlinesInWindow", inWindow);
            Json.putSafe(o, "checks", checked);
            o.put("routedTelemetry", largest.get(st)[largest.get(st).length - 1] != Long.MIN_VALUE);
            if (inWindow > 0 && checked == 0) short_.add(st);
        }
        return new StarvationObserver(starved, Collections.unmodifiableSet(starvedCuts), List.copyOf(checks), List.copyOf(unchecked), routing, oooMs, periodMs, cov);
    }

    private static String key(String station, String kind, String ref, long at) { return station + "\u0000" + kind + "\u0000" + ref + "\u0000" + at; }

    /** Per station with routed telemetry, the largest delivered {@code sourceTs} routed to it after each batch (index = batch). */
    static Map<String, long[]> largestRouted(Recording rec, List<Long> cuts) {
        RoutingManifest routing = rec.routing();
        Map<String, long[]> m = new TreeMap<>();
        for (String s : routing.stations().keySet()) { long[] a = new long[cuts.size()]; Arrays.fill(a, Long.MIN_VALUE); m.put(s, a); }
        List<Recording.Batch> batches = rec.batches();
        for (int p : routing.subscribed(InputParser.MESSAGES)) {
            try (Recording.Reader r = rec.reader(InputParser.MESSAGES, p)) {
                int k = 0;
                for (Recording.Line l; (l = r.next()) != null;) {
                    long off = l.record().offset();
                    while (k < batches.size() && batches.get(k).through(InputParser.MESSAGES, p) < off) k++;
                    if (k >= batches.size()) break;   // after the last batch: never delivered
                    String device = l.record().key();
                    if (device == null || !routing.devices().contains(device)) continue;
                    long[] a = m.get(routing.stationOf(device));
                    for (JsonNode x : l.record().value().path("members")) if (x.path("sourceTs").canConvertToLong()) a[k] = Math.max(a[k], x.path("sourceTs").asLong());
                }
            }
        }
        for (long[] a : m.values()) for (int k = 1; k < a.length; k++) a[k] = Math.max(a[k], a[k - 1]);
        return m;
    }

    /** The first cut after which every station of {@code others} has routed telemetry at or past {@code need}, null for none. */
    static Long countingCut(long need, Set<String> others, Map<String, long[]> largest, List<Long> cuts) {
        if (others.isEmpty()) return null;
        for (int k = 0; k < cuts.size(); k++) {
            boolean all = true;
            for (String s : others) if (largest.get(s)[k] < need) { all = false; break; }
            if (all) return cuts.get(k);
        }
        return null;
    }

    public String starved() { return starved; }
    public Set<Long> starvedCuts() { return starvedCuts; }
    public List<Check> checks() { return checks; }
    public List<Unchecked> unchecked() { return unchecked; }

    /** The coverage of the checks: per non-starved station, its ground-truth deadlines in the window the starvation can hold and its
     *  checks; {@code uncovered} lists every station with such a deadline and no check. */
    public ObjectNode coverage() { return coverage.deepCopy(); }

    /** Whether any check counts from the batch of {@code cutId}. */
    public boolean counts(long cutId) { for (Check c : checks) if (c.countingCut() == cutId) return true; return false; }

    /** The batch of {@code cutId} was written; {@code firstAppendNanos} is its first append (spec 681, {@link BatchWriter.Written}), the
     *  instant every check counting from it is timed from. */
    public void appended(long cutId, long firstAppendNanos) { this.firstAppendNanos.put(cutId, firstAppendNanos); }

    /** Reads the latest committed progress of the writers named; polls once for at most the given time. */
    public interface ProgressSource { Map<CompletionReader.Writer, JsonNode> latest(Collection<CompletionReader.Writer> writers, Duration poll); }

    /** Whether every check counting from the batch of {@code cutId} has settled or recorded its miss. */
    public boolean resolved(long cutId) {
        for (Check c : checks) if (c.countingCut() == cutId && !resolved.contains(c)) return false;
        return true;
    }

    /** Whether a check counting from the batch of {@code cutId} recorded a miss. */
    public boolean missed(long cutId) {
        for (JsonNode r : results) if (r.path("countingCut").asLong() == cutId && "miss".equals(r.path("outcome").asText())) return true;
        return false;
    }

    /** One observation step of the checks counting from the batch of {@code cutId}, while the harness waits for its cut: polls the
     *  committed progress once; a check settles at the first observation, within the deadline, that shows both stages at or past it, and
     *  is a miss once the deadline has passed without that. Returns {@link #resolved}. {@code clock}: monotonic nanoseconds. */
    public boolean poll(long cutId, ProgressSource progress, LongSupplier clock) {
        List<Check> mine = new ArrayList<>();
        for (Check c : checks) if (c.countingCut() == cutId && !resolved.contains(c)) mine.add(c);
        if (mine.isEmpty()) return true;
        Long from = firstAppendNanos.get(cutId);
        if (from == null) throw new IllegalStateException("cut " + cutId + ": its batch's first append was not recorded");
        long until = from + DEADLINE.toNanos();
        Set<CompletionReader.Writer> writers = new LinkedHashSet<>();
        for (Check c : mine) { int p = routing.partitionOf(c.station()); writers.add(new CompletionReader.Writer(Stage1.WORK_RECORDS, p)); writers.add(new CompletionReader.Writer(Stage2.AFFECTED, p)); }
        long now = clock.getAsLong();
        Map<CompletionReader.Writer, JsonNode> latest = progress.latest(writers, Duration.ofMillis(Math.max(1, Math.min(200, (until - now) / 1_000_000))));
        long at = clock.getAsLong();
        for (Check c : mine) {
            int p = routing.partitionOf(c.station());
            JsonNode wr = latest.get(new CompletionReader.Writer(Stage1.WORK_RECORDS, p)), af = latest.get(new CompletionReader.Writer(Stage2.AFFECTED, p));
            long w1 = wr == null || !wr.path("W1").canConvertToLong() ? Long.MIN_VALUE : wr.path("W1").asLong();
            long w2 = af == null || !af.path("W2").canConvertToLong() ? Long.MIN_VALUE : af.path("W2").asLong();
            lastSeen.put(c, new long[] {w1, w2});
            if (w1 >= c.deadline() && w2 >= c.deadline() && at <= until) { settledNanos.put(c, at); resolve(c, from, at); }
            else if (at > until) resolve(c, from, at);
        }
        return resolved(cutId);
    }

    private void resolve(Check c, long from, long at) {
        resolved.add(c);
        ObjectNode o = Json.object().put("station", c.station()).put("kind", c.kind()).put("ref", c.ref());
        Json.putSafe(o, "deadline", c.deadline());
        Json.putSafe(o, "countingCut", c.countingCut());
        Json.putSafe(o, "deadlineMs", DEADLINE.toMillis());
        long[] w = lastSeen.getOrDefault(c, new long[] {Long.MIN_VALUE, Long.MIN_VALUE});
        if (w[0] == Long.MIN_VALUE) o.putNull("lastW1"); else Json.putSafe(o, "lastW1", w[0]);
        if (w[1] == Long.MIN_VALUE) o.putNull("lastW2"); else Json.putSafe(o, "lastW2", w[1]);
        Long s = settledNanos.get(c);
        if (s != null) { o.put("outcome", "settled"); Json.putSafe(o, "settledAfterMs", (s - from) / 1_000_000); }
        else { o.put("outcome", "miss"); o.putNull("settledAfterMs"); Json.putSafe(o, "observedForMs", (at - from) / 1_000_000); }
        results.add(o);
    }

    /** Observes the checks of {@code cutId} until each settled or missed (a test's and a single-threaded caller's loop). */
    public List<ObjectNode> observe(long cutId, ProgressSource progress, LongSupplier clock) {
        int before = results.size();
        while (!poll(cutId, progress, clock)) { /* poll again */ }
        List<ObjectNode> out = new ArrayList<>();
        for (int i = before; i < results.size(); i++) out.add((ObjectNode) results.get(i));
        return out;
    }

    /** The observation of the run ({@link #FILE}): the starvation, the rule values the counting used, every check with its outcome,
     *  every unchecked deadline with its reason. */
    public ObjectNode toJson() {
        ObjectNode n = Json.object().put("starved", starved);
        ArrayNode sc = n.putArray("starvedCuts");
        starvedCuts.forEach(sc::add);
        Json.putSafe(n, "outOfOrdernessMs", oooMs);
        Json.putSafe(n, "expectedPeriodMs", periodMs);
        Json.putSafe(n, "deadlineMs", DEADLINE.toMillis());
        ArrayNode planned = n.putArray("planned");
        for (Check c : checks) {
            ObjectNode o = planned.addObject().put("station", c.station()).put("kind", c.kind()).put("ref", c.ref());
            Json.putSafe(o, "deadline", c.deadline());
            Json.putSafe(o, "countingCut", c.countingCut());
        }
        n.set("checks", results.deepCopy());
        n.set("coverage", coverage.deepCopy());
        ArrayNode u = n.putArray("unchecked");
        for (Unchecked x : unchecked) {
            ObjectNode o = u.addObject().put("station", x.station()).put("kind", x.kind()).put("ref", x.ref());
            Json.putSafe(o, "deadline", x.deadline());
            o.put("why", x.why());
        }
        return n;
    }
}
