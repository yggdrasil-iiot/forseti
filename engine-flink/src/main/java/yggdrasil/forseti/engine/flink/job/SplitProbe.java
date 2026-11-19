package yggdrasil.forseti.engine.flink.job;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.TreeSet;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import yggdrasil.forseti.core.Json;

/** A pass-through operator chained to each Kafka source at the source's parallelism (spec 717, plan D6): it records, per source
 *  subtask, the partitions whose records (business records and barriers alike: every subscribed partition carries a barrier at every
 *  cut) reached it, as the evidence file {@code split-<source>-<subtask>-l<launch>-a<attempt>.json}, written whenever a new partition appears.
 *  The gate asserts from these records that every source subtask owns exactly one split and every subscribed partition one subtask. */
public final class SplitProbe extends ProcessFunction<Msg, Msg> {
  private static final long serialVersionUID = 1L;
  private final JobSettings settings;
  private final String source;
  private transient TreeSet<Integer> partitions;

  public SplitProbe(JobSettings settings, String source) { this.settings = settings; this.source = source; }

  @Override public void processElement(Msg m, Context ctx, Collector<Msg> out) {
    if (partitions == null) partitions = new TreeSet<>();
    if (partitions.add(m.partition)) {
      var info = getRuntimeContext().getTaskInfo();
      ObjectNode n = Json.object().put("source", source).put("topic", m.topic).put("subtask", info.getIndexOfThisSubtask())
          .put("parallelism", info.getNumberOfParallelSubtasks()).put("attemptNumber", info.getAttemptNumber())
          .put("launch", settings.launch());
      ArrayNode a = n.putArray("partitions");
      partitions.forEach(a::add);
      Writers.evidence(settings, "split-" + source + "-" + info.getIndexOfThisSubtask() + "-l" + settings.launch() + "-a" + info.getAttemptNumber(), n);
    }
    out.collect(m);
  }
}
