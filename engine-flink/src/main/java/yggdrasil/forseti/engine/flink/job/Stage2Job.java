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
import yggdrasil.forseti.core.stage1.Stage1;

/**
 * The stage-2 job (plan D4): Kafka sources over the stations' partitions of {@code forseti.work-records} and {@code forseti.samples}
 * (read_committed, so only what stage 1 committed) and the six {@code quality.incidents} partitions, none with a watermark; the
 * {@link IncidentRouter} at parallelism 1; the evidence and the routed incidents keyed by station into {@link Stage2Stations} at
 * parallelism 6, chained to its transactional sink; the stage-2 {@link RejectCollectorFunction} at parallelism 1 with its own sink.
 */
public final class Stage2Job {
  private Stage2Job() {}

  public static final String NAME = "forseti-stage2";

  public static void main(String[] args) throws Exception {
    if (args.length != 1) throw new IllegalArgumentException("usage: Stage2Job <job spec file>");
    var spec = Json.read(Path.of(args[0]));
    JobSettings s = JobSettings.prepare(spec);
    if (s.stage() != 2) throw new IllegalArgumentException("not a stage-2 job spec");
    Configuration conf = Topology.configuration(Json.text(spec, "checkpointDir"));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    env.configure(conf);
    JobSettings.Runtime rt = s.runtime();
    TypeInformation<Msg> msg = TypeInformation.of(Msg.class);
    Map<String, Integer> parallelism = new LinkedHashMap<>();
    Map<String, Properties> sources = new LinkedHashMap<>(), sinks = new LinkedHashMap<>();
    Map<Integer, String> stationOf = new HashMap<>();
    for (String st : s.stations()) stationOf.put(rt.partitionOf(st), st);
    DataStream<Msg> evidence = null;
    for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES)) {
      List<Integer> ps = s.subscribed(t);
      String id = "stage2-source-" + t;
      sources.put(t, Topology.sourceProperties(s, id));
      parallelism.put(t, ps.size());
      DataStream<Msg> d = env.fromSource(Topology.source(s, t, ps, stationOf, id), WatermarkStrategy.noWatermarks(), id).setParallelism(ps.size()).uid("forseti-" + id)
          .process(new SplitProbe(s, id), msg).setParallelism(ps.size()).name("split-probe-" + id).uid("forseti-split-probe-" + id);
      evidence = evidence == null ? d : evidence.union(d);
    }
    List<Integer> incidents = s.subscribed(InputParser.INCIDENTS);
    String incId = "stage2-source-" + InputParser.INCIDENTS;
    sources.put(InputParser.INCIDENTS, Topology.sourceProperties(s, incId));
    parallelism.put(InputParser.INCIDENTS, incidents.size());
    DataStream<Msg> raw = env.fromSource(Topology.source(s, InputParser.INCIDENTS, incidents, null, incId), WatermarkStrategy.noWatermarks(), incId)
        .setParallelism(incidents.size()).uid("forseti-" + incId)
        .process(new SplitProbe(s, incId), msg).setParallelism(incidents.size()).name("split-probe-" + incId).uid("forseti-split-probe-" + incId);
    SingleOutputStreamOperator<Msg> routed = raw.process(new IncidentRouter(s), msg).setParallelism(1).setMaxParallelism(1)
        .name("incident-router").uid("forseti-incident-router");
    SingleOutputStreamOperator<Out> stations = evidence.union(routed).keyBy(m -> m.station, Types.STRING)
        .transform("stage2-stations", TypeInformation.of(Out.class), new Stage2Stations(s))
        .setParallelism(JobSettings.KEYED_PARALLELISM).setMaxParallelism(JobSettings.MAX_PARALLELISM).uid("forseti-stage2-stations");
    sinks.put("stage2-stations", Topology.sinkProperties(s, "stage2-stations"));
    stations.sinkTo(Topology.sink(s, "stage2-stations")).setParallelism(JobSettings.KEYED_PARALLELISM).name("stage2-stations-sink").uid("forseti-stage2-stations-sink");
    SingleOutputStreamOperator<Out> collector = routed.getSideOutput(IncidentRouter.REJECTS).process(new RejectCollectorFunction(s, 2), TypeInformation.of(Out.class))
        .setParallelism(1).setMaxParallelism(1).name("stage2-rejects").uid("forseti-stage2-rejects");
    sinks.put("stage2-rejects", Topology.sinkProperties(s, "stage2-rejects"));
    collector.sinkTo(Topology.sink(s, "stage2-rejects")).setParallelism(1).name("stage2-rejects-sink").uid("forseti-stage2-rejects-sink");
    Writers.evidence(s, "stage2-pins", Topology.pins(s, conf, parallelism, sources, sinks));
    env.execute(NAME);
  }
}
