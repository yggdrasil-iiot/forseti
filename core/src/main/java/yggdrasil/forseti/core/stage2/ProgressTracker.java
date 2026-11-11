package yggdrasil.forseti.core.stage2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.stage1.DeadlineIndex;
import yggdrasil.forseti.core.stage1.Stage1;

/** Rule sheet T2 (spec 511, 426-435): stage 2's only notion of event time. It holds the last stage-1 progress record per evidence topic
 *  and station, computes {@code W2(s) = min(W1 of the last forseti.work-records progress, W1 of the last forseti.samples progress)}, and
 *  collects the six {@code quality.incidents} barriers per cut. A cut is ready when both evidence topics carry its progress with equal
 *  {@code mergedCutIds} and the incident barriers of the cut and of every merged cut were collected; processed barriers are kept as
 *  evidence and never applied twice. Incidents and barriers never advance W2. Timers are the station's {@link DeadlineIndex}
 *  (the stage-2 state's, bound with {@link #track}): {@link #fire} returns those with {@code deadline <= W2(s)}, in index order. */
public final class ProgressTracker {
    public static final String WORK_RECORDS = Stage1.WORK_RECORDS, SAMPLES = Stage1.SAMPLES;
    public static final int INCIDENT_PARTITIONS = 6;
    public static final int VERSION = 1;

    private static final class Track {
        Progress.Stage1 workRecords, samples;
        final TreeMap<Long, TreeMap<Integer, Long>> barriers = new TreeMap<>();
        final TreeSet<Long> processed = new TreeSet<>();
        DeadlineIndex index;
    }
    private final TreeMap<String, Track> stations = new TreeMap<>();

    private Track track(String station) { return stations.computeIfAbsent(Objects.requireNonNull(station), k -> new Track()); }

    /** One stage-1 progress record of {@code topic} at {@code station}; a progress whose cutId or W1 decreases is refused. */
    public void observe(String topic, String station, Progress.Stage1 p) {
        if (!WORK_RECORDS.equals(topic) && !SAMPLES.equals(topic)) throw new IllegalArgumentException("not a stage-1 evidence topic: " + topic);
        if (!station.equals(p.station())) throw new IllegalArgumentException("progress of " + p.station() + " observed for " + station);
        Track t = track(station);
        Progress.Stage1 last = WORK_RECORDS.equals(topic) ? t.workRecords : t.samples;
        if (last != null && (p.cutId() < last.cutId() || p.W1() < last.W1()))
            throw new IllegalArgumentException("decreasing progress on " + topic + " at " + station + ": cut " + last.cutId() + " W1 " + last.W1() + " -> cut " + p.cutId() + " W1 " + p.W1());
        if (WORK_RECORDS.equals(topic)) t.workRecords = p; else t.samples = p;
    }

    /** One incident routing barrier of {@code cutId} on incident partition {@code partition}; false when the cut was processed already
     *  (the barrier is not applied twice) or the same barrier was collected before. */
    public boolean observeIncidentBarrier(String station, long cutId, int partition, long throughOffset) {
        if (partition < 0 || partition >= INCIDENT_PARTITIONS) throw new IllegalArgumentException("incident partition " + partition);
        if (throughOffset < -1 || throughOffset > Json.MAX_SAFE) throw new IllegalArgumentException("throughOffset " + throughOffset);
        Track t = track(station);
        if (t.processed.contains(cutId)) return false;
        Long old = t.barriers.computeIfAbsent(cutId, k -> new TreeMap<>()).putIfAbsent(partition, throughOffset);
        if (old != null && old != throughOffset) throw new IllegalStateException("two barriers of cut " + cutId + " on incident partition " + partition);
        return old == null;
    }

    public Progress.Stage1 lastProgress(String topic, String station) {
        Track t = stations.get(station);
        if (t == null) return null;
        return WORK_RECORDS.equals(topic) ? t.workRecords : SAMPLES.equals(topic) ? t.samples : null;
    }

    public OptionalLong w2(String station) {
        Track t = stations.get(station);
        if (t == null || t.workRecords == null || t.samples == null) return OptionalLong.empty();
        return OptionalLong.of(Math.min(t.workRecords.W1(), t.samples.W1()));
    }

    /** Both evidence topics carry progress {@code cutId} with equal mergedCutIds (else IllegalStateException), and the six incident
     *  barriers of the cut and of every merged cut were collected. */
    public boolean ready(String station, long cutId) {
        Track t = stations.get(station);
        if (t == null || t.workRecords == null || t.samples == null) return false;
        if (t.workRecords.cutId() != cutId || t.samples.cutId() != cutId) return false;
        if (!t.workRecords.mergedCutIds().equals(t.samples.mergedCutIds()))
            throw new IllegalStateException("cut " + cutId + " at " + station + ": mergedCutIds " + t.workRecords.mergedCutIds() + " != " + t.samples.mergedCutIds());
        for (long c : cuts(t, cutId)) { Map<Integer, Long> b = t.barriers.get(c); if (b == null || b.size() < INCIDENT_PARTITIONS) return false; }
        return true;
    }

    private static List<Long> cuts(Track t, long cutId) {
        TreeSet<Long> s = new TreeSet<>(t.workRecords == null ? List.of() : t.workRecords.mergedCutIds());
        s.add(cutId);
        return List.copyOf(s);
    }

    /** {@code {cutId, partition, throughOffset}} of the cut and every merged cut, 6 x (1 + mergedCutIds.size()), by cut then partition. */
    public List<Progress.IncidentOffset> incidentOffsets(String station, long cutId) {
        Track t = stations.get(station);
        if (t == null) throw new IllegalStateException("no progress at " + station);
        List<Progress.IncidentOffset> l = new ArrayList<>();
        for (long c : cuts(t, cutId)) {
            Map<Integer, Long> b = t.barriers.get(c);
            for (int q = 0; q < INCIDENT_PARTITIONS; q++) {
                Long off = b == null ? null : b.get(q);
                if (off == null) throw new IllegalStateException("cut " + c + " at " + station + ": no barrier of incident partition " + q);
                l.add(new Progress.IncidentOffset(c, q, off));
            }
        }
        return l;
    }

    /** The stage-1 progress records the cut's stage-2 progress processed: work records, then samples, at the station's partition. */
    public List<Progress.Stage1ProgressRef> stage1ProgressRefs(String station, long cutId, int partition) {
        return List.of(new Progress.Stage1ProgressRef(WORK_RECORDS, partition, cutId), new Progress.Stage1ProgressRef(SAMPLES, partition, cutId));
    }

    /** The cut (and its merged cuts) was applied: later barriers of them are ignored. */
    public void markProcessed(String station, long cutId) {
        Track t = track(station);
        t.processed.addAll(cuts(t, cutId));
    }
    public boolean processed(String station, long cutId) { Track t = stations.get(station); return t != null && t.processed.contains(cutId); }

    /** Binds the station's deadline index (the stage-2 state's {@code deadlines()}). */
    public void track(String station, DeadlineIndex index) { track(station).index = index; }

    /** The station's registrations with {@code deadline <= W2(s)}, in index order; nothing is removed (the decision function removes a
     *  fired registration). Empty before W2 exists. */
    public List<DeadlineIndex.Deadline> fire(String station) {
        Track t = stations.get(station);
        OptionalLong w = w2(station);
        if (t == null || t.index == null || w.isEmpty()) return List.of();
        return t.index.due(w.getAsLong());
    }

    /** The tracker's own state (the bound deadline index belongs to the stage-2 state and is not written). */
    public ObjectNode toJson() {
        ObjectNode n = Json.object().put("trackerVersion", VERSION);
        ObjectNode st = n.putObject("stations");
        stations.forEach((s, t) -> {
            ObjectNode e = st.putObject(s);
            if (t.workRecords == null) e.putNull("workRecords"); else e.set("workRecords", t.workRecords.toJson());
            if (t.samples == null) e.putNull("samples"); else e.set("samples", t.samples.toJson());
            ArrayNode b = e.putArray("barriers");
            t.barriers.forEach((c, m) -> m.forEach((q, off) -> {
                ObjectNode x = Json.putSafe(b.addObject(), "cutId", c);
                x.put("partition", q);
                Json.putSafe(x, "throughOffset", off);
            }));
            ArrayNode p = e.putArray("processed");
            t.processed.forEach(p::add);
        });
        return n;
    }

    public static ProgressTracker fromJson(JsonNode n) {
        if (n.path("trackerVersion").intValue() != VERSION) throw new IllegalArgumentException("progress tracker version " + n.path("trackerVersion") + " is not " + VERSION);
        ProgressTracker pt = new ProgressTracker();
        n.get("stations").fields().forEachRemaining(e -> {
            Track t = pt.track(e.getKey());
            t.workRecords = stage1(e.getValue().get("workRecords"));
            t.samples = stage1(e.getValue().get("samples"));
            for (JsonNode b : e.getValue().get("barriers"))
                t.barriers.computeIfAbsent(Json.safeLong(b.get("cutId"), "cutId", Json.NON_NEGATIVE), k -> new TreeMap<>())
                    .put((int) Json.safeLong(b.get("partition"), "partition", 0, INCIDENT_PARTITIONS - 1), Json.safeLong(b.get("throughOffset"), "throughOffset", Json.THROUGH));
            for (JsonNode c : e.getValue().get("processed")) t.processed.add(Json.safeLong(c, "processed", Json.NON_NEGATIVE));
        });
        return pt;
    }

    private static Progress.Stage1 stage1(JsonNode n) {
        if (n == null || n.isNull()) return null;
        if (!(Progress.fromJson(n) instanceof Progress.Stage1 p)) throw new IllegalArgumentException("not a stage-1 progress record");
        return p;
    }
}
