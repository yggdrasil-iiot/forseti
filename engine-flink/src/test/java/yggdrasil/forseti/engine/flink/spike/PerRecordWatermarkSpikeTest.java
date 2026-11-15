package yggdrasil.forseti.engine.flink.spike;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Spec line 762, the {@code -1} of {@code BoundedOutOfOrdernessWatermarks} and the delegating generator's per-record emission
 * (docs/ENGINE-ASSUMPTIONS.md, F3): the built-in generator emits {@code maxTimestamp - outOfOrderness - 1} and only in
 * {@code onPeriodicEmit}; with the automatic watermark interval 0 the periodic emitter never runs, so the built-in generator alone
 * never advances the operator watermark, while the thin delegating generator of spec line 77 makes the watermark after every record
 * equal {@code max(ts so far) - outOfOrderness - 1} at the next record.
 */
class PerRecordWatermarkSpikeTest {
  /** Out of order on purpose: 1200 and 2500, 2600 do not raise the maximum. */
  private static final long[] TS = {1000, 1500, 1200, 3000, 2500, 2600, 4000};
  private static MiniCluster mc;

  @BeforeAll
  static void start() throws Exception {
    mc = Spikes.miniCluster(2);
  }

  @AfterAll
  static void stop() throws Exception {
    mc.close();
  }

  @Test
  void theBuiltInBoundIsMaxMinusOutOfOrdernessMinusOneAndOnlyPeriodic() {
    BoundedOutOfOrdernessWatermarks<Object> bound = new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(Spikes.OUT_OF_ORDERNESS_MS));
    List<Long> emitted = new ArrayList<>();
    WatermarkOutput out = new WatermarkOutput() {
      @Override
      public void emitWatermark(Watermark watermark) {
        emitted.add(watermark.getTimestamp());
      }

      @Override
      public void markIdle() {
        throw new AssertionError("markIdle");
      }

      @Override
      public void markActive() {
        throw new AssertionError("markActive");
      }
    };
    bound.onPeriodicEmit(out);
    assertEquals(List.of(Long.MIN_VALUE), emitted, "before any record the bound is Long.MIN_VALUE");
    emitted.clear();
    bound.onEvent("r", 1000, out);
    assertTrue(emitted.isEmpty(), "onEvent emits nothing");
    bound.onPeriodicEmit(out);
    bound.onEvent("r", 900, out);
    bound.onPeriodicEmit(out);
    assertEquals(List.of(1000 - Spikes.OUT_OF_ORDERNESS_MS - 1, 1000 - Spikes.OUT_OF_ORDERNESS_MS - 1), emitted);
  }

  @Test
  void theDelegatingGeneratorEmitsAfterEveryRecordWithIntervalZero() throws Exception {
    String name = "per-record-delegating";
    List<Probe.Event> elems = run(name, Spikes.delegating(), true);
    long max = Long.MIN_VALUE;
    List<Long> expectedWms = new ArrayList<>();
    for (int i = 0; i < TS.length; i++) {
      long expected = i == 0 ? Long.MIN_VALUE : max - Spikes.OUT_OF_ORDERNESS_MS - 1;
      assertEquals(expected, elems.get(i).value(), "operator watermark at record " + i);
      if (TS[i] > max) {
        max = TS[i];
        expectedWms.add(max - Spikes.OUT_OF_ORDERNESS_MS - 1);
      }
    }
    List<Long> wms = Probe.events(name, "WM").stream().map(Probe.Event::value).toList();
    assertEquals(expectedWms, wms, "one watermark for every record that raised the maximum, none periodic");
  }

  @Test
  void theBuiltInGeneratorAloneNeverAdvancesWithIntervalZero() throws Exception {
    String name = "per-record-built-in";
    List<Probe.Event> elems = run(name, Spikes.builtIn(), false);
    for (int i = 0; i < TS.length; i++) {
      assertEquals(Long.MIN_VALUE, elems.get(i).value(), "operator watermark at record " + i);
    }
    assertTrue(Probe.events(name, "WM").isEmpty(), "no watermark: " + Probe.events(name, "WM"));
  }

  private static List<Probe.Event> run(String name, org.apache.flink.api.common.eventtime.WatermarkStrategy<Rec> strategy,
      boolean awaitLastWatermark) throws Exception {
    Probe.reset(name);
    SpikeSource.reset(name);
    JobGraph job = Spikes.probeJob(Spikes.env(false), new SpikeSource(name, new long[][] {TS}, -1), strategy, 1,
        new WatermarkProbe(name, 0));
    mc.submitJob(job).get();
    Spikes.await(mc, job.getJobID(), () -> Probe.events(name, "ELEM").size() == TS.length, TS.length + " records at the probe");
    if (awaitLastWatermark) {
      long last = 4000 - Spikes.OUT_OF_ORDERNESS_MS - 1;
      Spikes.await(mc, job.getJobID(), () -> Probe.events(name, "WM").stream().anyMatch(e -> e.value() == last), "watermark " + last);
    }
    Spikes.cancel(mc, job.getJobID());
    return Probe.events(name, "ELEM");
  }
}
