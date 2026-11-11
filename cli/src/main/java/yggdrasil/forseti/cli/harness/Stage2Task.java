package yggdrasil.forseti.cli.harness;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.Ids;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Delete;
import yggdrasil.forseti.core.journal.Pins;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.stage1.DeadlineIndex;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.Events2;
import yggdrasil.forseti.core.stage2.ProgressTracker;
import yggdrasil.forseti.core.stage2.SetCoverage;
import yggdrasil.forseti.core.stage2.Stage2;
import yggdrasil.forseti.core.stage2.Stage2State;

/** One station's stage-2 writer (the single owner of partition {@code p(s)} of {@code forseti.affected}, {@code forseti.affected-sets}
 *  and {@code forseti.dispositions-stage2}), rule sheet T2. Per cut it receives what stage 1 wrote to {@code p(s)} of the two evidence
 *  topics, in written order, and the incidents routed to the station. The cut's stage-1 progress (written on both evidence topics) and
 *  the six incident barriers go through the {@link ProgressTracker} first; when the tracker says the cut is ready, the rows and deletes are
 *  applied as written, then the incidents in {@code (identity bytes, payloadDigest)} order, then the timers due at {@code W2(s)} in index
 *  order, then the close. Every row is stamped with the pins and {@code eventTimeAt = W2}. */
final class Stage2Task {
    /** Incidents of a cut: {@code (identity bytes, payloadDigest)}, then the origin (a byte-identical redelivery in one order). */
    static final Comparator<Inputs.InputRecord> INCIDENT_ORDER = Comparator
        .<Inputs.InputRecord, byte[]>comparing(r -> Canonical.bytes(Ids.incidentId((Inputs.IncidentEvent) r.parsed())), Arrays::compareUnsigned)
        .thenComparing(r -> r.payloadDigest().sha256())
        .thenComparing(ContractOrder.ORIGIN);

    /** One cut's outputs in emission order and the W2 they ran with. */
    record CutOutputs(List<Object> outputs, long w2) {}

    final String station;
    final int partition;
    private final Stage2 stage2;
    private final ProgressTracker tracker;
    private final Pins pins;
    private Stage2State state;
    private final List<Inputs.InputRecord> incidents = new ArrayList<>();

    Stage2Task(Stage2 stage2, ProgressTracker tracker, long generation, String station, int partition, Pins pins,
               Map<String, SetCoverage.EvidenceRange> evidenceRanges) {
        this.stage2 = stage2;
        this.tracker = tracker;
        this.station = station;
        this.partition = partition;
        this.pins = pins;
        this.state = stage2.newState(generation, station);
        if (state.partition() != partition) throw new IllegalStateException("station " + station + " partition " + state.partition() + " != routing " + partition);
        state.evidenceRanges(evidenceRanges);
        tracker.track(station, state.deadlines());
    }

    void addIncident(Inputs.InputRecord r) { incidents.add(r); }

    /** {@code stage1}: the station's stage-1 outputs of the cut as written; {@code incidentThrough}: the cut's barrier per incident
     *  partition (all six). */
    CutOutputs cut(long cutId, List<Object> stage1, Map<Integer, Long> incidentThrough) {
        Progress.Stage1 progress = null;
        for (Object o : stage1) if (o instanceof Progress.Stage1 p) progress = p;
        if (progress == null || progress.cutId() != cutId) throw new IllegalStateException("station " + station + ": no stage-1 progress of cut " + cutId);
        tracker.observe(Stage1.WORK_RECORDS, station, progress);
        tracker.observe(Stage1.SAMPLES, station, progress);
        for (Map.Entry<Integer, Long> b : incidentThrough.entrySet()) tracker.observeIncidentBarrier(station, cutId, b.getKey(), b.getValue());
        if (!tracker.ready(station, cutId)) throw new IllegalStateException("station " + station + ": cut " + cutId + " is not ready at stage 2");
        OptionalLong ow = tracker.w2(station);
        long w2 = ow.getAsLong();
        List<Object> raw = new ArrayList<>();
        for (Object o : stage1) {
            if (o instanceof Row r && Stage1.WORK_RECORDS.equals(r.topic())) apply(new Events2.Basis(r), w2, raw);
            else if (o instanceof Row r && Stage1.SAMPLES.equals(r.topic())) apply(new Events2.Sample(r), w2, raw);
            else if (o instanceof Delete d) apply(new Events2.Basis(d), w2, raw);
        }
        incidents.sort(INCIDENT_ORDER);
        for (Inputs.InputRecord r : incidents) apply(new Events2.Incident(r), w2, raw);
        incidents.clear();
        while (true) {
            List<DeadlineIndex.Deadline> due = tracker.fire(station);
            if (due.isEmpty()) break;
            DeadlineIndex.Deadline d = due.get(0);
            apply(new Events2.Timer(d.deadline(), d.key(), d.purpose()), w2, raw);
        }
        apply(new Events2.CutClose(cutId, w2, tracker.stage1ProgressRefs(station, cutId, partition), tracker.incidentOffsets(station, cutId),
            progress.mergedCutIds()), w2, raw);
        tracker.markProcessed(station, cutId);
        List<Object> out = new ArrayList<>(raw.size());
        for (Object o : raw) out.add(o instanceof Row row ? Stage1.stamp(row, pins, w2) : o);
        return new CutOutputs(out, w2);
    }

    private void apply(Events2.Event e, long w2, List<Object> out) {
        Stage2.Result res = stage2.decide(state, e, w2);
        state = res.state();
        out.addAll(res.outputs());
    }

    Stage2State state() { return state; }
}
