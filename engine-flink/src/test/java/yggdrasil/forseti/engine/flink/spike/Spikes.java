package yggdrasil.forseti.engine.flink.spike;

import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;

/** Shared pieces of the Flink spikes: a MiniCluster, the job environment the gate pins, the two watermark strategies and waiting. */
final class Spikes {
  static final long OUT_OF_ORDERNESS_MS = 100;
  static final Duration TIMEOUT = Duration.ofSeconds(60);

  private Spikes() {}

  static MiniCluster miniCluster(int slots) throws Exception {
    Configuration conf = new Configuration();
    // any free port: the default 8081 may be taken by another process on the build host
    conf.set(RestOptions.BIND_PORT, "0");
    MiniCluster mc = new MiniCluster(new MiniClusterConfiguration.Builder()
        .setConfiguration(conf).setNumTaskManagers(1).setNumSlotsPerTaskManager(slots).build());
    mc.start();
    return mc;
  }

  /** The gate's watermark settings (spec lines 77, 590): automatic watermark interval 0, no idleness; no restart unless asked. */
  static StreamExecutionEnvironment env(boolean restartOnce) {
    Configuration conf = new Configuration();
    conf.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ZERO);
    if (restartOnce) {
      conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
      conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
      conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
    } else {
      conf.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
    }
    return new StreamExecutionEnvironment(conf);
  }

  /** The thin delegating generator of spec line 77: the built-in bound, emitted in onEvent for every record. */
  static final class Delegating<T> implements WatermarkGenerator<T> {
    private final BoundedOutOfOrdernessWatermarks<T> bound = new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(OUT_OF_ORDERNESS_MS));

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
      bound.onEvent(event, eventTimestamp, output);
      bound.onPeriodicEmit(output);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {}
  }

  static WatermarkStrategy<Rec> delegating() {
    return WatermarkStrategy.<Rec>forGenerator(c -> new Delegating<>()).withTimestampAssigner((r, t) -> r.ts);
  }

  static WatermarkStrategy<Rec> builtIn() {
    return WatermarkStrategy.<Rec>forBoundedOutOfOrderness(Duration.ofMillis(OUT_OF_ORDERNESS_MS)).withTimestampAssigner((r, t) -> r.ts);
  }

  /** source (parallelism {@code sourceParallelism}) -> keyBy(constant) -> probe (parallelism 1). */
  static JobGraph probeJob(StreamExecutionEnvironment env, SpikeSource source, WatermarkStrategy<Rec> strategy, int sourceParallelism,
      WatermarkProbe probe) {
    env.fromSource(source, strategy, "spike-source").setParallelism(sourceParallelism)
        .keyBy(r -> 0, Types.INT)
        .transform("probe", Types.VOID, probe).setParallelism(1);
    return env.getStreamGraph().getJobGraph();
  }

  /** Waits until {@code done} holds, failing early with the job's failure when the job ends first. */
  static void await(MiniCluster mc, JobID job, BooleanSupplier done, String what) throws Exception {
    long until = System.nanoTime() + TIMEOUT.toNanos();
    while (!done.getAsBoolean()) {
      JobStatus status = mc.getJobStatus(job).get();
      if (status.isGloballyTerminalState()) {
        Throwable cause = mc.requestJobResult(job).get().getSerializedThrowable().map(t -> (Throwable) t).orElse(null);
        fail("job ended " + status + " before: " + what, cause);
      }
      if (System.nanoTime() > until) {
        fail("timed out waiting for: " + what);
      }
      Thread.sleep(10);
    }
  }

  static void cancel(MiniCluster mc, JobID job) throws Exception {
    if (!mc.getJobStatus(job).get().isGloballyTerminalState()) {
      mc.cancelJob(job).get();
    }
    mc.requestJobResult(job).get();
  }
}
