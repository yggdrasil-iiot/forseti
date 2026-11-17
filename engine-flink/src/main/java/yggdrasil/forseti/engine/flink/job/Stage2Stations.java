package yggdrasil.forseti.engine.flink.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.cut.InputCodec;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Delete;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.stage1.DeadlineIndex;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.Events2;
import yggdrasil.forseti.core.stage2.ProgressTracker;
import yggdrasil.forseti.core.stage2.Stage2;
import yggdrasil.forseti.core.stage2.Stage2State;

/**
 * The stage-2 station writers (rule sheet T2, plan D4): a keyed operator over what stage 1 committed to {@code p(s)} of
 * {@code forseti.work-records} and {@code forseti.samples} and the incidents and incident barriers the incident router sends. Stage 2
 * uses no engine watermark and no native timer: a persisted core {@link ProgressTracker} per station computes
 * {@code W2(s) = min(W1 of the two evidence topics' last progress)}, says when a cut is ready (its stage-1 progress on both topics with
 * equal {@code mergedCutIds}, the six incident barriers of the cut and of every cut it merges) and fires the station's deadlines up to
 * {@code W2}. Each evidence topic's records are kept in segments closed by their progress record; a ready cut applies the work-record
 * segment, then the sample segment (rows and deletes as written), then the incidents of the cut and of its merged cuts in incident order,
 * then the due deadlines, then the close, and writes every row, delete, disposition and progress to {@code p(s)} of
 * {@code forseti.affected}, {@code forseti.affected-sets} and {@code forseti.dispositions-stage2}. State: per station, versioned
 * canonical bytes in keyed {@link ValueState} (RocksDB), written for each changed station at every checkpoint.
 */
public final class Stage2Stations extends AbstractStreamOperator<Out> implements OneInputStreamOperator<Msg, Out> {
  private static final long serialVersionUID = 1L;
  public static final int STATE_VERSION = 1;
  public static final String STATE_NAME = "forseti-stage2-station";

  private final JobSettings settings;
  private transient JobSettings.Runtime rt;
  private transient ValueState<byte[]> stored;
  private transient Map<String, Station> stations;
  private transient ObjectNode initRecord;
  private transient long appliedCuts;

  public Stage2Stations(JobSettings settings) { this.settings = settings; }

  /** The lines of one evidence partition up to (and closed by) a stage-1 progress record. */
  private record Segment(JsonNode progressLine, List<JsonNode> lines) {
    Progress.Stage1 progress() {
      if (!(Progress.fromJson(progressLine.get("value")) instanceof Progress.Stage1 p)) throw new IllegalStateException("not a stage-1 progress");
      return p;
    }
  }

  private final class Station {
    final String name;
    final int partition;
    Stage2State state;
    ProgressTracker tracker;
    final Map<String, ArrayDeque<Segment>> segments = new TreeMap<>();
    final Map<String, List<JsonNode>> open = new TreeMap<>();
    final TreeMap<Long, List<Inputs.InputRecord>> incidents = new TreeMap<>();
    final List<Inputs.InputRecord> pendingIncidents = new ArrayList<>();
    boolean dirty = true;

    Station(String name) {
      this.name = name;
      this.partition = rt.partitionOf(name);
      this.state = rt.stage2.newState(settings.generation(), name);
      if (state.partition() != partition) throw new IllegalStateException("station " + name + " partition " + state.partition() + " != routing " + partition);
      state.evidenceRanges(rt.evidenceRanges);
      this.tracker = new ProgressTracker();
      for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES)) { segments.put(t, new ArrayDeque<>()); open.put(t, new ArrayList<>()); }
      tracker.track(name, state.deadlines());
    }

    Station(JsonNode n) {
      if (n.path("stationStateVersion").intValue() != STATE_VERSION) throw new IllegalArgumentException("stage-2 station state version " + n.path("stationStateVersion"));
      this.name = Json.text(n, "station");
      this.partition = rt.partitionOf(name);
      this.state = Stage2State.fromJson(n.get("state"));
      this.tracker = ProgressTracker.fromJson(n.get("tracker"));
      for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES)) {
        ArrayDeque<Segment> q = new ArrayDeque<>();
        for (JsonNode s : n.path("segments").path(t)) { List<JsonNode> l = new ArrayList<>(); s.path("lines").forEach(l::add); q.add(new Segment(s.get("progress"), l)); }
        segments.put(t, q);
        List<JsonNode> o = new ArrayList<>();
        n.path("open").path(t).forEach(o::add);
        open.put(t, o);
      }
      n.path("incidents").fields().forEachRemaining(e -> {
        List<Inputs.InputRecord> l = new ArrayList<>();
        e.getValue().forEach(x -> l.add(InputCodec.decode(x)));
        incidents.put(Long.parseLong(e.getKey()), l);
      });
      n.path("pendingIncidents").forEach(x -> pendingIncidents.add(InputCodec.decode(x)));
      tracker.track(name, state.deadlines());
    }

    byte[] bytes() {
      ObjectNode n = Json.object().put("stationStateVersion", STATE_VERSION).put("station", name);
      n.set("state", state.toJson());
      n.set("tracker", tracker.toJson());
      ObjectNode sg = n.putObject("segments"), op = n.putObject("open");
      segments.forEach((t, q) -> {
        ArrayNode a = sg.putArray(t);
        for (Segment s : q) { ObjectNode x = a.addObject(); x.set("progress", s.progressLine()); s.lines().forEach(x.putArray("lines")::add); }
      });
      open.forEach((t, l) -> l.forEach(op.withArray(t)::add));
      for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES)) if (!op.has(t)) op.putArray(t);
      ObjectNode inc = n.putObject("incidents");
      incidents.forEach((c, l) -> { ArrayNode a = inc.putArray(Long.toString(c)); l.forEach(r -> a.add(InputCodec.encode(r))); });
      ArrayNode p = n.putArray("pendingIncidents");
      pendingIncidents.forEach(r -> p.add(InputCodec.encode(r)));
      return Canonical.bytes(n);
    }
  }

  @Override public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    rt = settings.runtime();
    stored = context.getKeyedStateStore().getState(new ValueStateDescriptor<>(STATE_NAME, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    var info = getContainingTask().getEnvironment().getTaskInfo();
    KeyGroupRange range = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(info.getMaxNumberOfParallelSubtasks(), info.getNumberOfParallelSubtasks(),
        info.getIndexOfThisSubtask());
    stations = new TreeMap<>();
    if (context.isRestored()) {
      List<String> keys = new ArrayList<>();
      try (java.util.stream.Stream<Object> s = getKeyedStateBackend().<VoidNamespace>getKeys(STATE_NAME, VoidNamespace.INSTANCE)) {
        s.forEach(k -> keys.add((String) k));
      }
      for (String k : keys) {
        setCurrentKey(k);
        byte[] b = stored.value();
        if (b != null) { Station st = new Station(Json.read(b)); stations.put(st.name, st); }
      }
    }
    ObjectNode keyGroups = Json.object();
    for (String s : settings.stations()) {
      int kg = KeyGroupRangeAssignment.assignToKeyGroup(s, info.getMaxNumberOfParallelSubtasks());
      keyGroups.put(s, kg);
      if (range.contains(kg)) stations.computeIfAbsent(s, Station::new);
    }
    initRecord = Json.object().put("operator", "stage2-stations").put("subtask", info.getIndexOfThisSubtask()).put("parallelism", info.getNumberOfParallelSubtasks())
        .put("maxParallelism", info.getMaxNumberOfParallelSubtasks()).put("attemptNumber", info.getAttemptNumber()).put("isRestored", context.isRestored());
    initRecord.putArray("keyGroupRange").add(range.getStartKeyGroup()).add(range.getEndKeyGroup());
    initRecord.set("stationKeyGroups", keyGroups);
    ArrayNode owned = initRecord.putArray("stations");
    stations.keySet().forEach(owned::add);
    writeEvidence();
  }

  @Override public void processElement(StreamRecord<Msg> element) {
    Msg m = element.getValue();
    Station st = stations.get(m.station);
    if (st == null) throw new IllegalStateException("station " + m.station + " is not a writer of this subtask");
    if (m.kind == Msg.INCIDENT_CUT) {
      st.incidents.computeIfAbsent(m.cutId, x -> new ArrayList<>()).addAll(st.pendingIncidents);
      st.pendingIncidents.clear();
      Json.read(m.value).fields().forEachRemaining(e -> st.tracker.observeIncidentBarrier(st.name, m.cutId, Integer.parseInt(e.getKey()), e.getValue().longValue()));
    } else if (m.kind == Msg.RECORD && InputParser.INCIDENTS.equals(m.topic)) {
      st.pendingIncidents.add(InputParser.read(new Inputs.Origin(m.topic, m.partition, m.offset), m.keyText(), m.value).record());
    } else if (m.kind == Msg.RECORD && (Stage1.WORK_RECORDS.equals(m.topic) || Stage1.SAMPLES.equals(m.topic))) {
      if (m.partition != st.partition) throw new IllegalStateException("station " + st.name + ": evidence of partition " + m.partition);
      Map<String, String> h = new TreeMap<>();
      if (m.generationHeader != null) h.put(OutputRecords.GENERATION_HEADER, m.generationHeader);
      ObjectNode line = OutputRecords.line(new OutputRecords.Transport(m.key, h, m.value), settings.generation());
      if (OutputRecords.kind(line.get("value")) == OutputRecords.Kind.PROGRESS) {
        st.segments.get(m.topic).add(new Segment(line, st.open.get(m.topic)));
        st.open.put(m.topic, new ArrayList<>());
      } else st.open.get(m.topic).add(line);
    } else throw new IllegalStateException("stage-2 station: element " + m);
    st.dirty = true;
    tryCuts(st);
  }

  /** Stage 2 has no engine watermark (spec 412): watermarks are neither used nor forwarded. */
  @Override public void processWatermark(Watermark mark) {}

  private void tryCuts(Station st) {
    while (true) {
      Segment wr = st.segments.get(Stage1.WORK_RECORDS).peek(), sm = st.segments.get(Stage1.SAMPLES).peek();
      if (wr == null || sm == null) return;
      Progress.Stage1 pw = wr.progress(), ps = sm.progress();
      if (pw.cutId() != ps.cutId() || !pw.mergedCutIds().equals(ps.mergedCutIds()))
        throw new IllegalStateException(st.name + ": the evidence topics disagree on cut " + pw.cutId() + "/" + ps.cutId() + " merged " + pw.mergedCutIds() + "/" + ps.mergedCutIds());
      long cutId = pw.cutId();
      st.tracker.observe(Stage1.WORK_RECORDS, st.name, pw);
      st.tracker.observe(Stage1.SAMPLES, st.name, ps);
      if (!st.tracker.ready(st.name, cutId)) return;
      apply(st, cutId, wr, sm, pw.mergedCutIds());
      st.segments.get(Stage1.WORK_RECORDS).poll();
      st.segments.get(Stage1.SAMPLES).poll();
    }
  }

  private void apply(Station st, long cutId, Segment wr, Segment sm, List<Long> merged) {
    long w2 = st.tracker.w2(st.name).getAsLong();
    List<Object> raw = new ArrayList<>();
    for (JsonNode line : wr.lines()) apply(st, evidence(Stage1.WORK_RECORDS, line), w2, raw);
    for (JsonNode line : sm.lines()) apply(st, evidence(Stage1.SAMPLES, line), w2, raw);
    List<Inputs.InputRecord> inc = new ArrayList<>();
    SortedMap<Long, List<Inputs.InputRecord>> through = st.incidents.headMap(cutId, true);
    through.values().forEach(inc::addAll);
    through.clear();
    inc.sort(IncidentRouter.INCIDENT_ORDER);
    for (Inputs.InputRecord r : inc) apply(st, new Events2.Incident(r), w2, raw);
    while (true) {
      List<DeadlineIndex.Deadline> due = st.tracker.fire(st.name);
      if (due.isEmpty()) break;
      DeadlineIndex.Deadline d = due.get(0);
      apply(st, new Events2.Timer(d.deadline(), d.key(), d.purpose()), w2, raw);
    }
    apply(st, new Events2.CutClose(cutId, w2, st.tracker.stage1ProgressRefs(st.name, cutId, st.partition), st.tracker.incidentOffsets(st.name, cutId), merged), w2, raw);
    st.tracker.markProcessed(st.name, cutId);
    for (Object o : raw) emit(st, o instanceof Row row ? Stage1.stamp(row, rt.pins, w2) : o);
    appliedCuts++;
  }

  /** A stage-1 evidence line as the stage-2 event it is: a work record or a delete is a basis, a sample row a sample. */
  private static Events2.Event evidence(String topic, JsonNode line) {
    JsonNode v = line.get("value");
    if (OutputRecords.kind(v) == OutputRecords.Kind.DELETE) return new Events2.Basis(Delete.fromJson(v.get("delete")));
    ObjectNode n = Json.object().put("topic", topic);
    n.set("logicalKey", line.get("key"));
    n.set("body", v.get("body"));
    n.set("bodyDigest", v.get("bodyDigest"));
    n.set("envelope", v.get("envelope"));
    Row r = Row.fromJson(n);
    return Stage1.WORK_RECORDS.equals(topic) ? new Events2.Basis(r) : new Events2.Sample(r);
  }

  private void apply(Station st, Events2.Event e, long w2, List<Object> out) {
    Stage2.Result res = rt.stage2.decide(st.state, e, w2);
    st.state = res.state();
    st.tracker.track(st.name, st.state.deadlines());
    out.addAll(res.outputs());
  }

  private void emit(Station st, Object x) {
    int p = st.partition;
    if (x instanceof Row r) out(r.topic(), p, OutputRecords.rowLine(r));
    else if (x instanceof Delete d) out(Stage2.AFFECTED, p, OutputRecords.deleteLine(d));
    else if (x instanceof Disposition d) out(ReplayTopics.DISPOSITIONS2, p, OutputRecords.dispositionLine(d));
    else if (x instanceof Progress.Stage2 s) { out(Stage2.AFFECTED, p, OutputRecords.progressLine(s)); out(Stage2.AFFECTED_SETS, p, OutputRecords.progressLine(s)); }
    else if (x instanceof Progress.Ledger l) out(ReplayTopics.DISPOSITIONS2, p, OutputRecords.progressLine(l));
    else throw new IllegalStateException("not a stage-2 output: " + x);
  }

  private void out(String topic, int partition, ObjectNode line) {
    output.collect(new StreamRecord<>(Out.of(settings.runName(topic), partition, OutputRecords.transport(line))));
  }

  @Override public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    for (Station st : stations.values()) {
      if (!st.dirty) continue;
      setCurrentKey(st.name);
      stored.update(st.bytes());
      st.dirty = false;
    }
    writeEvidence();
  }

  private void writeEvidence() {
    ObjectNode n = initRecord.deepCopy();
    Json.putSafe(n, "appliedCuts", appliedCuts);
    Writers.evidence(settings, "stage2-stations-" + n.path("subtask").intValue() + "-attempt-" + n.path("attemptNumber").intValue(), n);
  }
}
