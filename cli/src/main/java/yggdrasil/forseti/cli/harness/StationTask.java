package yggdrasil.forseti.cli.harness;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Pins;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.stage1.DeadlineIndex;
import yggdrasil.forseti.core.stage1.Events;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage1.Stage1State;

/** One station's stage-1 writer (the single owner of its output partition {@code p(s)}): it keeps the station's state, collects the
 *  cut's routed inputs, and at the cut applies them in contract order against the {@code admissionW} captured before the cut, computes
 *  {@code W1(s)} (S10: the largest telemetry {@code sourceTs} routed to the station through the cut, minus {@code outOfOrdernessMs}, minus
 *  1, never decreasing), fires the due timers in index order and closes the cut. Rows come back stamped with the pins and
 *  {@code eventTimeAt = W1}. */
final class StationTask {
    /** {@code W1} of a station that has received no telemetry yet: the bottom of the JCS integer domain. */
    static final long NO_TELEMETRY = -Json.MAX_SAFE;

    /** One cut's outputs in emission order, the W1 and the admissionW it ran with. */
    record CutOutputs(List<Object> outputs, long w1, Long admissionW) {}

    final String station;
    final int partition;
    private final Stage1 stage1;
    private final long outOfOrdernessMs;
    private final Pins pins;
    private Stage1State state;
    private final List<Inputs.InputRecord> pending = new ArrayList<>();
    private Long maxTelemetryTs;
    private Long w1;

    StationTask(Stage1 stage1, long generation, String station, int partition, long outOfOrdernessMs, Pins pins, Progress.InitEvidence init) {
        this.stage1 = stage1;
        this.station = station;
        this.partition = partition;
        this.outOfOrdernessMs = outOfOrdernessMs;
        this.pins = pins;
        this.state = stage1.newState(generation, station);
        if (state.partition() != partition) throw new IllegalStateException("station " + station + " partition " + state.partition() + " != routing " + partition);
        state.initialize(init);
    }

    void add(Inputs.InputRecord r) {
        pending.add(r);
        if (InputParser.MESSAGES.equals(r.origin().topic())) {
            Long t = Events.eventTime(r);
            if (t != null && (maxTelemetryTs == null || t > maxTelemetryTs)) maxTelemetryTs = t;
        }
    }

    CutOutputs cut(long cutId, List<JsonNode> barrierRefs) {
        List<Object> raw = new ArrayList<>();
        Long admissionW = state.admissionW();
        for (Inputs.InputRecord r : ContractOrder.sort(pending)) apply(new Events.Admit(r, Events.kindRank(r.origin().topic())), admissionW, raw);
        pending.clear();
        long w = maxTelemetryTs == null ? NO_TELEMETRY : maxTelemetryTs - outOfOrdernessMs - 1;
        if (w1 != null && w < w1) w = w1;
        w1 = w;
        while (true) {
            List<DeadlineIndex.Deadline> due = state.deadlines().due(w);
            if (due.isEmpty()) break;
            DeadlineIndex.Deadline d = due.get(0);
            apply(new Events.Timer(d.deadline(), d.key(), d.purpose()), w, raw);
        }
        apply(new Events.CutClose(cutId, w, barrierRefs), w, raw);
        List<Object> out = new ArrayList<>(raw.size());
        for (Object o : raw) out.add(o instanceof Row row ? Stage1.stamp(row, pins, w) : o);
        return new CutOutputs(out, w, admissionW);
    }

    private void apply(Events.Event e, Long watermark, List<Object> out) {
        Stage1.Result res = stage1.decide(state, e, watermark);
        state = res.state();
        out.addAll(res.outputs());
    }

    int onlineVisits() { return state.onlineVisits(); }
}
