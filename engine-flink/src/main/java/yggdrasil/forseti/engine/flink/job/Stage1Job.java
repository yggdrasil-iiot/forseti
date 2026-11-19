package yggdrasil.forseti.engine.flink.job;

import java.nio.file.Path;
import java.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;

/**
 * The stage-1 job (plan D3, D6): one Kafka source per subscribed raw topic, each followed by its chained {@link Stage1Router}; routed
 * telemetry (input 1) and routed sessions and occupancy (input 2) keyed by station into {@link Stage1Stations} at parallelism 6 with
 * the pinned max parallelism, chained to its transactional sink; every refused input and every barrier into the stage-1
 * {@link RejectCollectorFunction} at parallelism 1, chained to its own transactional sink. Operator UIDs are stable. The only argument
 * is the harness's job spec file.
 */
public final class Stage1Job {
  private Stage1Job() {}

  public static final String NAME = "forseti-stage1";

  public static void main(String[] args) throws Exception {
    if (args.length != 1) throw new IllegalArgumentException("usage: Stage1Job <job spec file>");
    var spec = Json.read(Path.of(args[0]));
    JobSettings s = JobSettings.prepare(spec);
    if (s.stage() != 1) throw new IllegalArgumentException("not a stage-1 job spec");
    Configuration conf = Topology.configuration(Json.text(spec, "checkpointDir"));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    env.configure(conf);
    long oo = s.runtime().outOfOrdernessMs;
    TypeInformation<Msg> msg = TypeInformation.of(Msg.class);
    Map<String, Integer> parallelism = new LinkedHashMap<>();
    Map<String, Properties> sources = new LinkedHashMap<>(), sinks = new LinkedHashMap<>();
    Map<String, SingleOutputStreamOperator<Msg>> routed = new LinkedHashMap<>();
    for (String t : List.of(InputParser.MESSAGES, InputParser.SESSIONS, InputParser.OCCUPANCY)) {
      List<Integer> ps = s.subscribed(t);
      if (ps.isEmpty()) continue;
      String id = "stage1-source-" + t;
      sources.put(t, Topology.sourceProperties(s, id));
      parallelism.put(t, ps.size());
      WatermarkStrategy<Msg> ws = t.equals(InputParser.MESSAGES) ? PerRecordWatermarks.strategy(oo) : WatermarkStrategy.noWatermarks();
      DataStream<Msg> raw = env.fromSource(Topology.source(s, t, ps, null, id), ws, id).setParallelism(ps.size()).uid("forseti-" + id)
          .process(new SplitProbe(s, id), msg).setParallelism(ps.size()).name("split-probe-" + id).uid("forseti-split-probe-" + id);
      routed.put(t, raw.process(new Stage1Router(s), msg).setParallelism(ps.size()).name("stage1-router-" + t).uid("forseti-stage1-router-" + t));
    }
    if (!routed.containsKey(InputParser.MESSAGES)) throw new IllegalStateException("no subscribed raw telemetry partition");
    DataStream<Msg> telemetry = routed.get(InputParser.MESSAGES), others = null;
    List<DataStream<Msg>> rejects = new ArrayList<>();
    for (Map.Entry<String, SingleOutputStreamOperator<Msg>> e : routed.entrySet()) {
      rejects.add(e.getValue().getSideOutput(Stage1Router.REJECTS));
      if (e.getKey().equals(InputParser.MESSAGES)) continue;
      others = others == null ? e.getValue() : others.union(e.getValue());
    }
    if (others == null) throw new IllegalStateException("stage 1 subscribes no session or occupancy partition");
    SingleOutputStreamOperator<Out> stations = telemetry.connect(others).keyBy(m -> m.station, m -> m.station, Types.STRING)
        .transform("stage1-stations", TypeInformation.of(Out.class), new Stage1Stations(s))
        .setParallelism(JobSettings.KEYED_PARALLELISM).setMaxParallelism(JobSettings.MAX_PARALLELISM).uid("forseti-stage1-stations");
    sinks.put("stage1-stations", Topology.sinkProperties(s, "stage1-stations"));
    stations.sinkTo(Topology.sink(s, "stage1-stations")).setParallelism(JobSettings.KEYED_PARALLELISM).name("stage1-stations-sink").uid("forseti-stage1-stations-sink");
    DataStream<Msg> rejected = rejects.get(0);
    for (int i = 1; i < rejects.size(); i++) rejected = rejected.union(rejects.get(i));
    SingleOutputStreamOperator<Out> collector = rejected.process(new RejectCollectorFunction(s, 1), TypeInformation.of(Out.class))
        .setParallelism(1).setMaxParallelism(1).name("stage1-rejects").uid("forseti-stage1-rejects");
    sinks.put("stage1-rejects", Topology.sinkProperties(s, "stage1-rejects"));
    collector.sinkTo(Topology.sink(s, "stage1-rejects")).setParallelism(1).name("stage1-rejects-sink").uid("forseti-stage1-rejects-sink");
    Writers.evidence(s, "stage1-pins-l" + s.launch(), Topology.pins(s, conf, parallelism, sources, sinks));
    env.execute(NAME);
  }
}
