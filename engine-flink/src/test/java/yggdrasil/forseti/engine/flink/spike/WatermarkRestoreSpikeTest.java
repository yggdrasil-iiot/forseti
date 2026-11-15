package yggdrasil.forseti.engine.flink.spike;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Spec line 762, Flink does not restore watermarks after a restore (docs/ENGINE-ASSUMPTIONS.md, F4): the job runs with
 * {@code EXACTLY_ONCE} checkpoints, the source stops at position 4 until a checkpoint taken there completes and then fails once; the
 * restored attempt resumes at position 4 from that checkpoint. Before the failure the keyed operator's watermark had advanced and it had
 * registered an event-time timer; after the restore the timer is back (timers are keyed state), but the operator watermark is
 * Long.MIN_VALUE at the first record read after the restore and advances only from records read after it.
 */
class WatermarkRestoreSpikeTest {
  private static final long[] TS = {1000, 2000, 3000, 4000, 5000, 6000};
  private static final int FAIL_AT = 4;
  private static final long TIMER_AT = 1_000_000;
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
  void theOperatorWatermarkStartsAtMinValueAfterARestore() throws Exception {
    String name = "restore";
    Probe.reset(name);
    SpikeSource.reset(name);
    StreamExecutionEnvironment env = Spikes.env(true);
    env.enableCheckpointing(50, CheckpointingMode.EXACTLY_ONCE);
    JobGraph job = Spikes.probeJob(env, new SpikeSource(name, new long[][] {TS}, FAIL_AT), Spikes.delegating(), 1,
        new WatermarkProbe(name, TIMER_AT));
    mc.submitJob(job).get();
    Spikes.await(mc, job.getJobID(),
        () -> Probe.events(name, "ELEM").stream().filter(e -> e.attempt() == 1).count() == TS.length - FAIL_AT, "the restored records");
    Spikes.cancel(mc, job.getJobID());

    List<Probe.Event> inits = Probe.events(name, "INIT");
    assertEquals(2, inits.size(), inits.toString());
    assertEquals(0, inits.get(0).attempt());
    assertTrue(!inits.get(0).restored() && inits.get(0).restoredCheckpointId() == -1, inits.get(0).toString());
    assertEquals(1, inits.get(1).attempt());
    assertTrue(inits.get(1).restored() && inits.get(1).restoredCheckpointId() > 0, "attempt 1 restored from a checkpoint: " + inits.get(1));

    List<Probe.Event> timers = Probe.events(name, "TIMERS");
    assertEquals(List.of(0L, 1L), timers.stream().map(Probe.Event::value).toList(), "the event-time timer is restored");

    List<Probe.Event> before = Probe.events(name, "ELEM").stream().filter(e -> e.attempt() == 0).toList();
    List<Probe.Event> after = Probe.events(name, "ELEM").stream().filter(e -> e.attempt() == 1).toList();
    assertEquals(FAIL_AT, before.size());
    assertEquals(TS[FAIL_AT - 2] - Spikes.OUT_OF_ORDERNESS_MS - 1, before.get(FAIL_AT - 1).value(), "advanced before the failure");
    assertEquals(FAIL_AT, after.get(0).seq(), "the restored attempt resumes at the checkpointed position");
    assertEquals(Long.MIN_VALUE, after.get(0).value(), "operator watermark at the first record after the restore");
    for (int i = 1; i < after.size(); i++) {
      assertEquals(TS[FAIL_AT + i - 1] - Spikes.OUT_OF_ORDERNESS_MS - 1, after.get(i).value(), "only post-restore records advance it");
    }
    List<Probe.Event> wmAfter = Probe.events(name, "WM").stream().filter(e -> e.attempt() == 1).toList();
    assertTrue(!wmAfter.isEmpty() && wmAfter.get(0).value() == TS[FAIL_AT] - Spikes.OUT_OF_ORDERNESS_MS - 1,
        "the first watermark after the restore comes from the first post-restore record: " + wmAfter);
  }
}
