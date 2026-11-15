package yggdrasil.forseti.engine.flink.spike;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;

/**
 * Spec line 762, Flink runs {@code initializeState} before the task reports RUNNING (docs/ENGINE-ASSUMPTIONS.md, F5): an operator whose
 * {@code initializeState} blocks keeps its task INITIALIZING for as long as it blocks, while the unblocked source task of the same job
 * reaches RUNNING; once released, the task reports RUNNING. Stage 1 reads {@code forseti.recovery-control} in {@code initializeState}
 * (plan D5), so no stage-1 task is RUNNING before its epoch is fixed.
 */
class InitializeStateBeforeRunningSpikeTest {
  private static final CountDownLatch ENTERED = new CountDownLatch(1);
  private static final CountDownLatch RELEASE = new CountDownLatch(1);

  /** Blocks in initializeState until the test releases it. */
  static final class BlockingInit extends AbstractStreamOperator<Void> implements OneInputStreamOperator<Rec, Void> {
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
      super.initializeState(context);
      ENTERED.countDown();
      if (!RELEASE.await(60, TimeUnit.SECONDS)) {
        throw new IllegalStateException("spike: never released");
      }
    }

    @Override
    public void processElement(StreamRecord<Rec> element) {}
  }

  @Test
  void aBlockedInitializeStateNeverShowsRunning() throws Exception {
    String name = "initialize-state";
    SpikeSource.reset(name);
    try (MiniCluster mc = Spikes.miniCluster(2)) {
      StreamExecutionEnvironment env = Spikes.env(false);
      env.disableOperatorChaining();
      env.fromSource(new SpikeSource(name, new long[][] {{1000}}, -1), Spikes.delegating(), "spike-source").setParallelism(1)
          .transform("blocked", Types.VOID, new BlockingInit()).setParallelism(1);
      JobGraph job = env.getStreamGraph().getJobGraph();
      mc.submitJob(job).get();
      assertTrue(ENTERED.await(60, TimeUnit.SECONDS), "initializeState entered");
      // Poll for 1 s while initializeState blocks: the blocked task is INITIALIZING every time; the source task reaches RUNNING.
      boolean sourceRan = false;
      for (int i = 0; i < 50; i++) {
        Map<String, ExecutionState> states = states(mc, job.getJobID());
        assertEquals(ExecutionState.INITIALIZING, states.get("blocked"), "while initializeState blocks: " + states);
        sourceRan |= states.get("spike-source") == ExecutionState.RUNNING;
        Thread.sleep(20);
      }
      assertTrue(sourceRan, "the source task, which does not block, reached RUNNING meanwhile");
      RELEASE.countDown();
      Spikes.await(mc, job.getJobID(), () -> {
        try {
          return states(mc, job.getJobID()).get("blocked") == ExecutionState.RUNNING;
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }, "the released task to report RUNNING");
      Spikes.cancel(mc, job.getJobID());
    }
  }

  private static Map<String, ExecutionState> states(MiniCluster mc, JobID job) throws Exception {
    Map<String, ExecutionState> states = new TreeMap<>();
    for (AccessExecutionVertex v : mc.getExecutionGraph(job).get().getAllExecutionVertices()) {
      String task = v.getTaskNameWithSubtaskIndex();
      states.put(task.contains("blocked") ? "blocked" : "spike-source", v.getExecutionState());
    }
    return states;
  }
}
