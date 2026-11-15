package yggdrasil.forseti.cli.recording;

/** The two forms of a station's reported {@code W1} (spec 620, 622): {@link #PER_STATION}, the step-3 reference harness's and Kafka
 *  Streams' (the largest telemetry {@code sourceTs} routed to the station over everything delivered through the cut, minus
 *  {@code outOfOrdernessMs}, minus 1), and {@link #MIN_OVER_PARTITIONS}, Flink's (the minimum over the subscribed raw telemetry
 *  partitions of the largest {@code sourceTs} delivered on that partition, minus {@code outOfOrdernessMs}, minus 1; every station
 *  reports the same value). The scenario builder plans brackets with one of them and the reference harness reports one of them. */
public enum W1Form {
    PER_STATION("per-station"), MIN_OVER_PARTITIONS("min-over-partitions");

    public final String wire;
    W1Form(String wire) { this.wire = wire; }

    /** The form named {@code wire}; null is {@link #PER_STATION}. */
    public static W1Form of(String wire) {
        if (wire == null) return PER_STATION;
        for (W1Form f : values()) if (f.wire.equals(wire)) return f;
        throw new IllegalArgumentException("W1 form " + wire + ": not per-station or min-over-partitions");
    }
}
