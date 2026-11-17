package yggdrasil.forseti.engine.flink.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import yggdrasil.forseti.cli.harness.ContractOrder;
import yggdrasil.forseti.cli.replay.CutBarrierRecord;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.cut.InputCodec;
import yggdrasil.forseti.core.input.Ids;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.stage2.Incidents;

/**
 * The incident router {@code incident-router} of the stage-2 topology (spec 497), at parallelism 1 over the six
 * {@code quality.incidents} source subtasks (rule sheet T1, as the reference harness runs it). Every incident goes through the core's
 * door ({@link Incidents#door}); a refused one, and every incident barrier as it arrives, go to the stage-2 reject collector
 * ({@link #REJECTS}). The routed incidents of a cut are held until the cut's barriers of all six partitions arrived (each barrier checked
 * against the partition it was consumed from); then they are sent to their stations in {@code (identity bytes, payloadDigest, origin)}
 * order, an incident whose {@code (source, incidentId)} passed the door before going to the station of its first routed revision (so a
 * revision naming another station reaches the decision function and is refused {@code station-change} there), followed by one
 * {@link Msg#INCIDENT_CUT} to every active station, starved ones included, carrying the six through-offsets of the cut. State: one
 * versioned entry in operator state.
 */
public final class IncidentRouter extends ProcessFunction<Msg, Msg> implements CheckpointedFunction {
  private static final long serialVersionUID = 1L;
  public static final int STATE_VERSION = 1;
  public static final OutputTag<Msg> REJECTS = new OutputTag<>("stage2-rejects", TypeInformation.of(Msg.class));
  /** {@code (identity bytes, payloadDigest)}, then the origin: the order the reference harness routes and applies a cut's incidents in. */
  public static final Comparator<Inputs.InputRecord> INCIDENT_ORDER = Comparator
      .<Inputs.InputRecord, byte[]>comparing(r -> Canonical.bytes(Ids.incidentId((Inputs.IncidentEvent) r.parsed())), Arrays::compareUnsigned)
      .thenComparing(r -> r.payloadDigest().sha256())
      .thenComparing(ContractOrder.ORIGIN);

  private final JobSettings settings;
  private transient JobSettings.Runtime rt;
  private transient ListState<byte[]> stored;
  private transient TreeMap<Integer, List<Inputs.InputRecord>> pending;
  private transient TreeMap<Long, List<Inputs.InputRecord>> cutIncidents;
  private transient TreeMap<Long, TreeMap<Integer, Long>> barriers;
  private transient TreeMap<String, String> stationOf;

  public IncidentRouter(JobSettings settings) { this.settings = settings; }

  @Override public void initializeState(FunctionInitializationContext context) throws Exception {
    rt = settings.runtime();
    stored = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("forseti-incident-router", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    pending = new TreeMap<>();
    cutIncidents = new TreeMap<>();
    barriers = new TreeMap<>();
    stationOf = new TreeMap<>();
    if (!context.isRestored()) return;
    int entries = 0;
    for (byte[] b : stored.get()) {
      if (entries++ > 0) throw new IllegalStateException("incident router: two state entries");
      JsonNode n = Json.read(b);
      if (n.path("incidentRouterStateVersion").intValue() != STATE_VERSION) throw new IllegalArgumentException("incident router state version " + n.path("incidentRouterStateVersion"));
      n.path("pending").fields().forEachRemaining(e -> pending.put(Integer.parseInt(e.getKey()), records(e.getValue())));
      n.path("cutIncidents").fields().forEachRemaining(e -> cutIncidents.put(Long.parseLong(e.getKey()), records(e.getValue())));
      n.path("barriers").fields().forEachRemaining(e -> {
        TreeMap<Integer, Long> m = new TreeMap<>();
        e.getValue().fields().forEachRemaining(x -> m.put(Integer.parseInt(x.getKey()), x.getValue().longValue()));
        barriers.put(Long.parseLong(e.getKey()), m);
      });
      n.path("stationOf").fields().forEachRemaining(e -> stationOf.put(e.getKey(), e.getValue().asText()));
    }
  }

  private static List<Inputs.InputRecord> records(JsonNode a) {
    List<Inputs.InputRecord> l = new ArrayList<>();
    a.forEach(x -> l.add(InputCodec.decode(x)));
    return l;
  }

  @Override public void snapshotState(FunctionSnapshotContext context) throws Exception {
    ObjectNode n = Json.object().put("incidentRouterStateVersion", STATE_VERSION);
    ObjectNode p = n.putObject("pending");
    pending.forEach((k, v) -> { ArrayNode a = p.putArray(Integer.toString(k)); v.forEach(r -> a.add(InputCodec.encode(r))); });
    ObjectNode c = n.putObject("cutIncidents");
    cutIncidents.forEach((k, v) -> { ArrayNode a = c.putArray(Long.toString(k)); v.forEach(r -> a.add(InputCodec.encode(r))); });
    ObjectNode b = n.putObject("barriers");
    barriers.forEach((k, v) -> { ObjectNode o = b.putObject(Long.toString(k)); v.forEach((q, t) -> Json.putSafe(o, Integer.toString(q), t)); });
    ObjectNode s = n.putObject("stationOf");
    stationOf.forEach(s::put);
    stored.update(List.of(Canonical.bytes(n)));
  }

  @Override public void processElement(Msg m, Context ctx, Collector<Msg> out) {
    if (!InputParser.INCIDENTS.equals(m.topic)) throw new IllegalStateException("incident router: a record of " + m.topic);
    if (m.kind == Msg.RECORD) {
      InputParser.Read read = InputParser.read(new Inputs.Origin(m.topic, m.partition, m.offset), m.keyText(), m.value);
      if (Incidents.door(rt.rules, read) instanceof Incidents.Rejected) ctx.output(REJECTS, m.to(null));
      else pending.computeIfAbsent(m.partition, x -> new ArrayList<>()).add(read.record());
      return;
    }
    if (m.kind != Msg.BARRIER) throw new IllegalStateException("incident router: element kind " + m.kind);
    CutAssembler.Barrier b = CutBarrierRecord.consumed(m.topic, m.partition, m.value, settings.generation(), settings.routingDigest()).barrier();
    long cut = b.cutId();
    List<Inputs.InputRecord> held = pending.remove(m.partition);
    if (held != null) cutIncidents.computeIfAbsent(cut, x -> new ArrayList<>()).addAll(held);
    TreeMap<Integer, Long> got = barriers.computeIfAbsent(cut, x -> new TreeMap<>());
    if (got.put(m.partition, b.throughOffset()) != null) throw new IllegalStateException("two incident barriers of cut " + cut + " on partition " + m.partition);
    ctx.output(REJECTS, m.to(null));
    if (got.size() < rt.incidentOrigins.size()) return;
    // every incident barrier of the cut: the cut's incidents in the harness's order, then the barriers to every station
    List<Inputs.InputRecord> routed = cutIncidents.remove(cut);
    if (routed == null) routed = new ArrayList<>();
    routed.sort(INCIDENT_ORDER);
    for (Inputs.InputRecord r : routed) {
      Inputs.IncidentEvent i = (Inputs.IncidentEvent) r.parsed();
      String station = stationOf.computeIfAbsent(Canonical.utf8(Canonical.bytes(Incidents.setKey(i.source(), i.incidentId()))), y -> i.stationRef());
      Msg x = Msg.record(r.origin().topic(), r.origin().partition(), r.origin().offset(), r.key() == null ? null : r.key().getBytes(StandardCharsets.UTF_8), r.rawValue());
      out.collect(x.to(station));
    }
    ObjectNode through = Json.object();
    got.forEach((q, t) -> Json.putSafe(through, Integer.toString(q), t));
    byte[] v = Canonical.bytes(through);
    for (String s : settings.stations()) {
      Msg c = new Msg();
      c.kind = Msg.INCIDENT_CUT;
      c.station = s;
      c.topic = InputParser.INCIDENTS;
      c.cutId = cut;
      c.value = v;
      out.collect(c);
    }
    barriers.remove(cut);
  }
}
