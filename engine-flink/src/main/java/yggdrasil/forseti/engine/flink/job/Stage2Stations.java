package yggdrasil.forseti.engine.flink.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
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
import yggdrasil.forseti.core.segment.Segments;

/**
 * The stage-2 station writers (rule sheet T2, plan D4): a keyed operator over what stage 1 committed to {@code p(s)} of
 * {@code forseti.work-records} and {@code forseti.samples} and the incidents and incident barriers the incident router sends. Stage 2
 * uses no engine watermark and no native timer: a persisted core {@link ProgressTracker} per station computes
 * {@code W2(s) = min(W1 of the two evidence topics' last progress)}, says when a cut is ready (its stage-1 progress on both topics with
 * equal {@code mergedCutIds}, the six incident barriers of the cut and of every cut it merges) and fires the station's deadlines up to
 * {@code W2}. Each evidence topic's records are kept in segments closed by their progress record; a ready cut applies the work-record
 * segment, then the sample segment (rows and deletes as written), then the incidents of the cut and of its merged cuts in incident order,
 * then the due deadlines, then the close, and writes every row, delete, disposition and progress to {@code p(s)} of
 * {@code forseti.affected}, {@code forseti.affected-sets} and {@code forseti.dispositions-stage2}. State: per station a keyed
 * {@link MapState} (RocksDB) of segment id to canonical bytes, as stage 1 keeps it: {@value #ENGINE} (the tracker, the incidents and
 * the queue of evidence segments, {@code stationStateVersion} {@value #STATE_VERSION}), {@code evidence/<topic>/<seq>} for each closed
 * evidence segment (written once, removed when its cut is applied), {@code open/<topic>} for the lines after the last progress, and
 * {@code state/<id>} for every segment of the core {@link Stage2State}; a checkpoint writes only what changed since the previous one.
 */
public final class Stage2Stations extends AbstractStreamOperator<Out> implements OneInputStreamOperator<Msg, Out> {
  private static final long serialVersionUID = 1L;
  public static final int STATE_VERSION = 2;
  public static final String STATE_NAME = "forseti-stage2-station-segments", ENGINE = "engine", STATE_PREFIX = "state/", EVIDENCE_PREFIX = "evidence/",
      OPEN_PREFIX = "open/";

  private final JobSettings settings;
  private transient JobSettings.Runtime rt;
  private transient MapState<String, byte[]> stored;
  private transient Map<String, Station> stations;
  private transient ObjectNode initRecord;
  private transient String incarnation;
  private transient long appliedCuts, snapshots, segmentsWritten, segmentBytes, segmentBytesMax, segmentsRemoved, headBytesMax;

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
    final Map<String, ArrayDeque<Long>> queued = new TreeMap<>();
    final Map<Long, Segment> closedSegments = new HashMap<>();
    final Map<String, List<JsonNode>> open = new TreeMap<>();
    final TreeMap<Long, List<Inputs.InputRecord>> incidents = new TreeMap<>();
    final List<Inputs.InputRecord> pendingIncidents = new ArrayList<>();
    long nextSeq;
    /** What changed since the last checkpoint: the head, the open lines per topic, closed segments to write and to remove. */
    boolean headDirty = true;
    final Set<String> openDirty = new TreeSet<>();
    final Set<Long> toWrite = new TreeSet<>(), toRemove = new TreeSet<>();
    final Map<Long, String> topicOfSeq = new HashMap<>();

    Station(String name) {
      this.name = name;
      this.partition = rt.partitionOf(name);
      this.state = rt.stage2.newState(settings.generation(), name);
      if (state.partition() != partition) throw new IllegalStateException("station " + name + " partition " + state.partition() + " != routing " + partition);
      state.evidenceRanges(rt.evidenceRanges);
      this.tracker = new ProgressTracker();
      for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES)) { queued.put(t, new ArrayDeque<>()); open.put(t, new ArrayList<>()); openDirty.add(t); }
      tracker.track(name, state.deadlines());
    }

    /** The station a stored segment map holds. */
    Station(String name, Map<String, byte[]> stored) {
      this.name = name;
      this.partition = rt.partitionOf(name);
      JsonNode n = Segments.read(ENGINE, Objects.requireNonNull(stored.get(ENGINE), "station " + name + " without an engine head"));
      if (n.path("stationStateVersion").intValue() != STATE_VERSION) throw new IllegalArgumentException("stage-2 station state version " + n.path("stationStateVersion"));
      if (!name.equals(n.path("station").asText())) throw new IllegalStateException("station " + name + " stored as " + n.path("station"));
      Map<String, byte[]> core = new TreeMap<>();
      stored.forEach((k, v) -> { if (k.startsWith(STATE_PREFIX)) core.put(k.substring(STATE_PREFIX.length()), v); });
      this.state = Stage2State.fromSegments(core);
      this.tracker = ProgressTracker.fromJson(n.get("tracker"));
      this.nextSeq = n.path("nextSeq").longValue();
      for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES)) {
        ArrayDeque<Long> q = new ArrayDeque<>();
        for (JsonNode x : n.path("queued").path(t)) {
          long seq = x.longValue();
          String id = EVIDENCE_PREFIX + t + "/" + seq;
          JsonNode sg = Segments.read(id, Objects.requireNonNull(stored.get(id), "station " + name + " without " + id));
          List<JsonNode> l = new ArrayList<>();
          sg.path("lines").forEach(l::add);
          closedSegments.put(seq, new Segment(sg.get("progress"), l));
          topicOfSeq.put(seq, t);
          q.add(seq);
        }
        queued.put(t, q);
        String oid = OPEN_PREFIX + t;
        List<JsonNode> o = new ArrayList<>();
        if (stored.containsKey(oid)) Segments.read(oid, stored.get(oid)).path("lines").forEach(o::add);
        open.put(t, o);
      }
      n.path("incidents").fields().forEachRemaining(e -> {
        List<Inputs.InputRecord> l = new ArrayList<>();
        e.getValue().forEach(x -> l.add(InputCodec.decode(x)));
        incidents.put(Long.parseLong(e.getKey()), l);
      });
      n.path("pendingIncidents").forEach(x -> pendingIncidents.add(InputCodec.decode(x)));
      tracker.track(name, state.deadlines());
      headDirty = false;
    }

    Segment peek(String topic) { Long s = queued.get(topic).peek(); return s == null ? null : closedSegments.get(s); }

    void poll(String topic) {
      Long s = queued.get(topic).poll();
      closedSegments.remove(s);
      if (toWrite.remove(s)) topicOfSeq.remove(s); else toRemove.add(s);
      headDirty = true;
    }

    void close(String topic, JsonNode progressLine) {
      long seq = nextSeq++;
      closedSegments.put(seq, new Segment(progressLine, open.get(topic)));
      topicOfSeq.put(seq, topic);
      queued.get(topic).add(seq);
      open.put(topic, new ArrayList<>());
      toWrite.add(seq);
      openDirty.add(topic);
      headDirty = true;
    }

    byte[] head() {
      ObjectNode n = Segments.header(ENGINE).put("stationStateVersion", STATE_VERSION).put("station", name);
      n.set("tracker", tracker.toJson());
      Json.putSafe(n, "nextSeq", nextSeq);
      ObjectNode q = n.putObject("queued");
      queued.forEach((t, d) -> { ArrayNode a = q.putArray(t); d.forEach(a::add); });
      ObjectNode inc = n.putObject("incidents");
      incidents.forEach((c, l) -> { ArrayNode a = inc.putArray(Long.toString(c)); l.forEach(r -> a.add(InputCodec.encode(r))); });
      ArrayNode p = n.putArray("pendingIncidents");
      pendingIncidents.forEach(r -> p.add(InputCodec.encode(r)));
      return Segments.bytes(n);
    }

    byte[] closedSegment(long seq) {
      Segment sg = closedSegments.get(seq);
      ObjectNode n = Segments.header(EVIDENCE_PREFIX + topicOfSeq.get(seq) + "/" + seq);
      n.set("progress", sg.progressLine());
      ArrayNode a = n.putArray("lines");
      sg.lines().forEach(a::add);
      return Segments.bytes(n);
    }

    byte[] openSegment(String topic) {
      ObjectNode n = Segments.header(OPEN_PREFIX + topic);
      ArrayNode a = n.putArray("lines");
      open.get(topic).forEach(a::add);
      return Segments.bytes(n);
    }
  }

  @Override public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    rt = settings.runtime();
    stored = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>(STATE_NAME, BasicTypeInfo.STRING_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
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
        Map<String, byte[]> segs = new TreeMap<>();
        for (Map.Entry<String, byte[]> e : stored.entries()) segs.put(e.getKey(), e.getValue());
        if (!segs.isEmpty()) stations.put(k, new Station(k, segs));
      }
    }
    ObjectNode keyGroups = Json.object();
    for (String s : settings.stations()) {
      int kg = KeyGroupRangeAssignment.assignToKeyGroup(s, info.getMaxNumberOfParallelSubtasks());
      keyGroups.put(s, kg);
      if (range.contains(kg)) stations.computeIfAbsent(s, Station::new);
    }
    initRecord = Json.object().put("operator", "stage2-stations").put("subtask", info.getIndexOfThisSubtask()).put("parallelism", info.getNumberOfParallelSubtasks())
        .put("maxParallelism", info.getMaxNumberOfParallelSubtasks()).put("attemptNumber", info.getAttemptNumber()).put("isRestored", context.isRestored())
        .put("launch", settings.launch());
    // stage 2 never reads the recovery control and carries no initEvidence; its incarnation names its evidence file all the same
    incarnation = RestoreEvidence.of(settings.launch(), context.isRestored(), context.getRestoredCheckpointId(), info.getAttemptNumber()).incarnation();
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
      st.headDirty = true;
      Json.read(m.value).fields().forEachRemaining(e -> st.tracker.observeIncidentBarrier(st.name, m.cutId, Integer.parseInt(e.getKey()), e.getValue().longValue()));
    } else if (m.kind == Msg.RECORD && InputParser.INCIDENTS.equals(m.topic)) {
      st.pendingIncidents.add(InputParser.read(new Inputs.Origin(m.topic, m.partition, m.offset), m.keyText(), m.value).record());
      st.headDirty = true;
    } else if (m.kind == Msg.RECORD && (Stage1.WORK_RECORDS.equals(m.topic) || Stage1.SAMPLES.equals(m.topic))) {
      if (m.partition != st.partition) throw new IllegalStateException("station " + st.name + ": evidence of partition " + m.partition);
      Map<String, String> h = new TreeMap<>();
      if (m.generationHeader != null) h.put(OutputRecords.GENERATION_HEADER, m.generationHeader);
      ObjectNode line = OutputRecords.line(new OutputRecords.Transport(m.key, h, m.value), settings.generation());
      if (OutputRecords.kind(line.get("value")) == OutputRecords.Kind.PROGRESS) st.close(m.topic, line);
      else { st.open.get(m.topic).add(line); st.openDirty.add(m.topic); }
    } else throw new IllegalStateException("stage-2 station: element " + m);
    tryCuts(st);
  }

  /** Stage 2 has no engine watermark (spec 412): watermarks are neither used nor forwarded. */
  @Override public void processWatermark(Watermark mark) {}

  private void tryCuts(Station st) {
    while (true) {
      Segment wr = st.peek(Stage1.WORK_RECORDS), sm = st.peek(Stage1.SAMPLES);
      if (wr == null || sm == null) return;
      Progress.Stage1 pw = wr.progress(), ps = sm.progress();
      if (pw.cutId() != ps.cutId() || !pw.mergedCutIds().equals(ps.mergedCutIds()))
        throw new IllegalStateException(st.name + ": the evidence topics disagree on cut " + pw.cutId() + "/" + ps.cutId() + " merged " + pw.mergedCutIds() + "/" + ps.mergedCutIds());
      long cutId = pw.cutId();
      st.tracker.observe(Stage1.WORK_RECORDS, st.name, pw);
      st.tracker.observe(Stage1.SAMPLES, st.name, ps);
      st.headDirty = true;   // the tracker observed
      if (!st.tracker.ready(st.name, cutId)) return;
      apply(st, cutId, wr, sm, pw.mergedCutIds());
      st.poll(Stage1.WORK_RECORDS);
      st.poll(Stage1.SAMPLES);
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
    for (Object o : raw) emit(st, o instanceof Row row ? Stage1.stamp(row, rt.stamped, w2) : o);
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
    snapshots++;
    for (Station st : stations.values()) {
      setCurrentKey(st.name);
      Segments.Drain d = st.state.drainChanged();
      if (d.full()) {
        List<String> old = new ArrayList<>();
        for (String k : stored.keys()) if (k.startsWith(STATE_PREFIX)) old.add(k);
        for (String k : old) stored.remove(k);
      }
      for (Map.Entry<String, byte[]> e : d.written().entrySet()) put(STATE_PREFIX + e.getKey(), e.getValue());
      for (String id : d.removed()) { stored.remove(STATE_PREFIX + id); segmentsRemoved++; }
      for (long seq : st.toWrite) put(EVIDENCE_PREFIX + st.topicOfSeq.get(seq) + "/" + seq, st.closedSegment(seq));
      for (long seq : st.toRemove) { stored.remove(EVIDENCE_PREFIX + st.topicOfSeq.remove(seq) + "/" + seq); segmentsRemoved++; }
      st.toWrite.clear();
      st.toRemove.clear();
      for (String t : st.openDirty) put(OPEN_PREFIX + t, st.openSegment(t));
      st.openDirty.clear();
      if (st.headDirty) { byte[] h = st.head(); headBytesMax = Math.max(headBytesMax, h.length); put(ENGINE, h); st.headDirty = false; }
    }
    writeEvidence();
  }

  private void put(String id, byte[] b) throws Exception {
    stored.put(id, b);
    segmentsWritten++;
    segmentBytes += b.length;
    segmentBytesMax = Math.max(segmentBytesMax, b.length);
  }

  /** A station of this subtask as the restore test compares it: the core state, the engine head, the open and queued evidence
   *  (package-private). */
  ObjectNode inspect(String station) {
    Station st = stations.get(station);
    if (st == null) return null;
    ObjectNode n = Json.object();
    n.set("state", st.state.toJson());
    n.set("head", Segments.read(ENGINE, st.head()));
    ObjectNode open = n.putObject("open"), queued = n.putObject("queued");
    for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES)) {
      open.set(t, Segments.read(OPEN_PREFIX + t, st.openSegment(t)));
      ArrayNode q = queued.putArray(t);
      for (long seq : st.queued.get(t)) q.add(Segments.read(EVIDENCE_PREFIX + t + "/" + seq, st.closedSegment(seq)));
    }
    return n;
  }

  private void writeEvidence() {
    ObjectNode n = initRecord.deepCopy();
    Json.putSafe(n, "appliedCuts", appliedCuts);
    ObjectNode seg = n.putObject("segments");
    Json.putSafe(seg, "snapshots", snapshots);
    Json.putSafe(seg, "written", segmentsWritten);
    Json.putSafe(seg, "writtenBytes", segmentBytes);
    Json.putSafe(seg, "largestBytes", segmentBytesMax);
    Json.putSafe(seg, "largestHeadBytes", headBytesMax);
    Json.putSafe(seg, "removed", segmentsRemoved);
    Writers.evidence(settings, "stage2-stations-" + n.path("subtask").intValue() + "-" + incarnation, n);
  }
}
