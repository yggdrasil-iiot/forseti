package yggdrasil.forseti.engine.flink.job;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * The telemetry watermark of spec line 77 (plan D3, docs/ENGINE-ASSUMPTIONS.md F3): Flink's own {@link BoundedOutOfOrdernessWatermarks}
 * ({@code largest timestamp - outOfOrdernessMs - 1}), emitted in {@code onEvent} for every record by this thin delegating generator,
 * because the built-in generator emits only in {@code onPeriodicEmit} and the jobs run with the automatic watermark interval 0. A record
 * without an event time (a barrier, a record that does not parse, a message without a valid member {@code sourceTs}) is offered with
 * {@code Long.MIN_VALUE}, which never raises the bound. No idleness is configured: a telemetry split is never idle.
 */
public final class PerRecordWatermarks implements WatermarkGenerator<Msg> {
  /** The wire name of the emission mode, recorded in the job evidence. */
  public static final String MODE = "per-record-delegating-bounded-out-of-orderness";
  private final BoundedOutOfOrdernessWatermarks<Msg> bound;

  PerRecordWatermarks(long outOfOrdernessMs) { this.bound = new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(outOfOrdernessMs)); }

  @Override public void onEvent(Msg event, long eventTimestamp, WatermarkOutput output) {
    bound.onEvent(event, eventTimestamp, output);
    bound.onPeriodicEmit(output);
  }

  @Override public void onPeriodicEmit(WatermarkOutput output) {}

  public static WatermarkStrategy<Msg> strategy(long outOfOrdernessMs) {
    return WatermarkStrategy.<Msg>forGenerator(c -> new PerRecordWatermarks(outOfOrdernessMs))
        .withTimestampAssigner((m, t) -> m.eventTime == null ? Long.MIN_VALUE : m.eventTime);
  }
}
