package yggdrasil.forseti.cli.harness;

import java.util.List;
import java.util.TreeMap;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.stage1.Events;

/** The native time of the {@link yggdrasil.forseti.cli.recording.W1Form#MIN_OVER_PARTITIONS} form, as Flink's operator watermark
 *  computes it (spec 620): the largest event time delivered on every subscribed raw telemetry partition, over everything delivered in
 *  the generation, whatever station or reject collector the record then goes to; {@code W1} is the minimum over those partitions minus
 *  {@code outOfOrdernessMs} minus 1 (no bound, {@link StationTask#NO_TELEMETRY}, while a partition has delivered none). A record
 *  without an event time (a schema-invalid value, a message without any member {@code sourceTs}) advances nothing. */
final class PartitionClock {
    private final TreeMap<Integer, Long> largest = new TreeMap<>();
    private final List<Integer> partitions;

    PartitionClock(List<Integer> partitions) {
        if (partitions.isEmpty()) throw new IllegalArgumentException("no subscribed raw telemetry partition");
        this.partitions = List.copyOf(partitions);
    }

    /** One delivered raw telemetry record (before routing); {@code parsed} false for a record that did not parse. */
    void delivered(Inputs.InputRecord r, boolean parsed) {
        if (!parsed) return;
        Long t = Events.eventTime(r);
        if (t == null) return;
        largest.merge(r.origin().partition(), t, Math::max);
    }

    long w1(long outOfOrdernessMs) {
        long min = Long.MAX_VALUE;
        for (int p : partitions) {
            Long t = largest.get(p);
            if (t == null) return StationTask.NO_TELEMETRY;
            min = Math.min(min, t);
        }
        return min - outOfOrdernessMs - 1;
    }
}
