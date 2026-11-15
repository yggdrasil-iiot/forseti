package yggdrasil.forseti.cli.harness;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import yggdrasil.forseti.cli.recording.W1Form;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Pins;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.stage1.DeadlineIndex;
import yggdrasil.forseti.core.stage1.Events;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage1.Stage1State;

/** One station's stage-1 writer (the single owner of its output partition {@code p(s)}): it keeps the station's state and its
 *  {@link CutAssembler}, which holds the routed inputs and collects the required barriers. When the assembler closes a cut, the
 *  task applies the cut's inputs (those of every merged cut included) in contract order against the assembler's {@code admissionW},
 *  reports its native time as {@code W1}, fires the due timers in index order and closes the cut. The native time is the harness's
 *  W1 form: {@link W1Form#PER_STATION} (S10: the largest telemetry {@code sourceTs} routed to the station, minus {@code outOfOrdernessMs},
 *  minus 1) or {@link W1Form#MIN_OVER_PARTITIONS} (the {@link PartitionClock} of the subscribed raw telemetry partitions). Rows come back
 *  stamped with the pins and {@code eventTimeAt = W1}. */
final class StationTask {
    /** {@code W1} of a station that has received no telemetry yet: the bottom of the JCS integer domain. */
    static final long NO_TELEMETRY = -Json.MAX_SAFE;

    /** One closed cut's outputs in emission order, the W1 and the admissionW it ran with, and the cuts it merged. */
    record CutOutputs(long cutId, List<Object> outputs, long w1, Long admissionW, List<Long> mergedCutIds) {}

    final String station;
    final int partition;
    private final Stage1 stage1;
    private final long outOfOrdernessMs;
    private final Pins pins;
    private final W1Form form;
    private final PartitionClock clock;
    private final CutAssembler<Inputs.InputRecord> cuts;
    private Stage1State state;
    private Long maxTelemetryTs;
    /** Inputs delivered to this station so far: the delivery sequence number of the {@code processing-time} control. */
    private long delivered;

    StationTask(Stage1 stage1, long generation, String station, int partition, long outOfOrdernessMs, Pins pins, Progress.InitEvidence init,
                List<Progress.Origin> required, W1Form form, PartitionClock clock) {
        this.stage1 = stage1;
        this.station = station;
        this.partition = partition;
        this.outOfOrdernessMs = outOfOrdernessMs;
        this.pins = pins;
        this.form = form;
        this.clock = clock;
        if (form == W1Form.MIN_OVER_PARTITIONS && clock == null) throw new IllegalArgumentException("the min-over-partitions form reads a partition clock");
        this.cuts = new CutAssembler<>(station, required, true);
        this.state = stage1.newState(generation, station);
        if (state.partition() != partition) throw new IllegalStateException("station " + station + " partition " + state.partition() + " != routing " + partition);
        state.initialize(init);
    }

    void add(Inputs.InputRecord r) {
        if (stage1.controls().processingTime()) r = Stage1.processingTime(r, ++delivered);   // the processing-time control, before decide
        cuts.input(new Progress.Origin(r.origin().topic(), r.origin().partition()), r);
        if (InputParser.MESSAGES.equals(r.origin().topic())) {
            Long t = Events.eventTime(r);
            if (t != null && (maxTelemetryTs == null || t > maxTelemetryTs)) maxTelemetryTs = t;
        }
    }

    /** One barrier of a required channel (the reference harness has no restart injection: the control is never read). */
    void barrier(CutAssembler.Barrier b) {
        cuts.barrier(b, () -> { throw new IllegalStateException("the reference harness writes no recovery declaration after the initial one"); });
    }

    /** The station's native event time: the W1 it reports when a cut closes now. */
    long nativeTime() {
        if (form == W1Form.MIN_OVER_PARTITIONS) return clock.w1(outOfOrdernessMs);
        return maxTelemetryTs == null ? NO_TELEMETRY : maxTelemetryTs - outOfOrdernessMs - 1;
    }

    /** The cut the assembler closes now, if any, applied. */
    Optional<CutOutputs> close() {
        long w = nativeTime();
        Optional<CutAssembler.Closed<Inputs.InputRecord>> c = cuts.poll(w);
        if (c.isEmpty()) return Optional.empty();
        CutAssembler.Closed<Inputs.InputRecord> closed = c.get();
        Long admissionW = closed.admissionW();
        if (!Objects.equals(admissionW, state.admissionW()))
            throw new IllegalStateException("station " + station + ": the assembler's admissionW " + admissionW + " differs from the state's " + state.admissionW());
        List<Object> raw = new ArrayList<>();
        for (Inputs.InputRecord r : ContractOrder.sort(new ArrayList<>(closed.inputs()))) apply(new Events.Admit(r, Events.kindRank(r.origin().topic())), admissionW, raw);
        while (true) {
            List<DeadlineIndex.Deadline> due = state.deadlines().due(w);
            if (due.isEmpty()) break;
            DeadlineIndex.Deadline d = due.get(0);
            apply(new Events.Timer(d.deadline(), d.key(), d.purpose()), w, raw);
        }
        List<JsonNode> refs = new ArrayList<>();
        for (Progress.BarrierRef b : closed.barrierRefs()) refs.add(b.toJson());
        apply(new Events.CutClose(closed.cutId(), w, refs, closed.restartEpoch(), closed.recoveryRef(), closed.mergedCutIds()), w, raw);
        List<Object> out = new ArrayList<>(raw.size());
        for (Object o : raw) out.add(o instanceof Row row ? Stage1.stamp(row, pins, w) : o);
        return Optional.of(new CutOutputs(closed.cutId(), out, w, admissionW, closed.mergedCutIds()));
    }

    private void apply(Events.Event e, Long watermark, List<Object> out) {
        Stage1.Result res = stage1.decide(state, e, watermark);
        state = res.state();
        out.addAll(res.outputs());
    }

    int onlineVisits() { return state.onlineVisits(); }
}
