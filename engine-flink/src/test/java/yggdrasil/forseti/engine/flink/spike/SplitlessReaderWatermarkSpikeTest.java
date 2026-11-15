package yggdrasil.forseti.engine.flink.spike;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Spec line 762, split-less readers (docs/ENGINE-ASSUMPTIONS.md, F2): with the gate's settings (automatic watermark interval 0, no
 * idleness) a source subtask that owns no split emits no watermark and no idle status, so the keyed operator downstream of a source at
 * parallelism n+1 over n splits stays at Long.MIN_VALUE although every split has advanced; at parallelism n over n splits the same
 * records advance it to the minimum over the splits. This is why line 717 requires exactly one split per source subtask.
 *
 * <p>Each split ends with a sentinel record carrying its last timestamp: once the probe has seen every sentinel, it has processed every
 * watermark the sources emitted before them (records and watermarks of one channel arrive in order), so the final value is exact.
 */
class SplitlessReaderWatermarkSpikeTest {
  private static final long[][] SPLITS = {{1000, 3000, 3000}, {500, 2000, 2000}};
  private static MiniCluster mc;

  @BeforeAll
  static void start() throws Exception {
    mc = Spikes.miniCluster(4);
  }

  @AfterAll
  static void stop() throws Exception {
    mc.close();
  }

  private static List<Probe.Event> run(String name, int sourceParallelism) throws Exception {
    Probe.reset(name);
    SpikeSource.reset(name);
    JobGraph job = Spikes.probeJob(Spikes.env(false), new SpikeSource(name, SPLITS, -1), Spikes.delegating(), sourceParallelism,
        new WatermarkProbe(name, 0));
    mc.submitJob(job).get();
    int records = SPLITS[0].length + SPLITS[1].length;
    Spikes.await(mc, job.getJobID(), () -> Probe.events(name, "ELEM").size() == records, records + " records at the probe");
    Spikes.cancel(mc, job.getJobID());
    return Probe.events(name);
  }

  @Test
  void nOverNSplitsAdvancesToTheMinimumOverTheSplits() throws Exception {
    String name = "splitless-n-over-n";
    run(name, 2);
    Set<String> owners = new TreeSet<>();
    Probe.events(name, "SPLIT").forEach(e -> owners.add(e.subtask() + ":" + e.split()));
    assertEquals(Set.of("0:0", "1:1"), owners, "each source subtask owns exactly one split");
    List<Probe.Event> elems = Probe.events(name, "ELEM");
    assertEquals(Math.min(3000, 2000) - Spikes.OUT_OF_ORDERNESS_MS - 1, elems.get(elems.size() - 1).value());
  }

  @Test
  void aSplitLessReaderHoldsTheDownstreamWatermarkAtMinValue() throws Exception {
    String name = "splitless-n-plus-1";
    run(name, 3);
    Set<String> owners = new TreeSet<>();
    Probe.events(name, "SPLIT").forEach(e -> owners.add(e.subtask() + ":" + e.split()));
    assertEquals(Set.of("0:0", "1:1"), owners, "subtask 2 owns no split");
    for (Probe.Event e : Probe.events(name, "ELEM")) {
      assertEquals(Long.MIN_VALUE, e.value(), "operator watermark at split " + e.split() + " seq " + e.seq());
    }
    assertTrue(Probe.events(name, "WM").isEmpty(), "no watermark reached the probe: " + Probe.events(name, "WM"));
  }
}
