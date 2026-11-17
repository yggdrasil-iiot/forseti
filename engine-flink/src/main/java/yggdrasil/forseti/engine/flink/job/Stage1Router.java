package yggdrasil.forseti.engine.flink.job;

import java.util.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import yggdrasil.forseti.cli.replay.CutBarrierRecord;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.stage1.Routing;

/**
 * The stage-1 router of one raw input source (plan D6), chained to its source subtask: it parses every business record with the
 * core's {@code InputParser} and routes it with the core's {@link Routing}: a routed record goes to its station (the key of the keyed
 * stage-1 operator), a refused one to the reject collector ({@link #REJECTS}). A barrier is checked against the partition it was
 * consumed from ({@link CutBarrierRecord#consumed}: origin, generation, routing digest) and fanned out (spec 412) to every station whose
 * required barrier set holds its origin, except the stations its {@code idleInputs} declare {@code starved}; for a Flink station that set
 * is every subscribed raw telemetry partition plus the session and occupancy partitions that feed it, so a telemetry barrier reaches
 * every active station, including those its partition does not feed. Every barrier also goes to the reject collector, which counts every
 * barrier it receives. Records and barriers leave in the order they were consumed, after all earlier outputs.
 */
public final class Stage1Router extends ProcessFunction<Msg, Msg> {
  private static final long serialVersionUID = 1L;
  public static final OutputTag<Msg> REJECTS = new OutputTag<>("stage1-rejects", TypeInformation.of(Msg.class));
  private final JobSettings settings;
  private transient JobSettings.Runtime rt;

  public Stage1Router(JobSettings settings) { this.settings = settings; }

  @Override public void processElement(Msg m, Context ctx, Collector<Msg> out) {
    if (rt == null) rt = settings.runtime();
    if (m.kind == Msg.BARRIER) {
      CutBarrierRecord br = CutBarrierRecord.consumed(m.topic, m.partition, m.value, settings.generation(), settings.routingDigest());
      CutAssembler.Barrier b = br.barrier();
      Set<String> starved = new TreeSet<>();
      for (CutAssembler.Idle i : b.idleInputs()) if (i.kind().equals(CutAssembler.STARVED)) starved.add(i.station());
      Progress.Origin origin = b.origin();
      for (Map.Entry<String, List<Progress.Origin>> e : rt.required.entrySet())
        if (!starved.contains(e.getKey()) && e.getValue().contains(origin)) out.collect(m.to(e.getKey()));
      ctx.output(REJECTS, m.to(null));
      return;
    }
    if (m.kind != Msg.RECORD) throw new IllegalStateException("stage-1 router: element kind " + m.kind);
    InputParser.Read read = InputParser.read(new Inputs.Origin(m.topic, m.partition, m.offset), m.keyText(), m.value);
    Routing.Outcome o = rt.router.route(read);
    if (o instanceof Routing.Routed r) out.collect(m.to(r.station()));
    else ctx.output(REJECTS, m.to(null));
  }
}
