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
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import yggdrasil.forseti.cli.harness.ContractOrder;
import yggdrasil.forseti.cli.replay.CutBarrierRecord;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.cli.replay.RecoveryControlStore;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.cut.InputCodec;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.stage1.DeadlineIndex;
import yggdrasil.forseti.core.stage1.Events;
import yggdrasil.forseti.core.stage1.QuarantineRecord;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage1.Stage1State;

/**
 * The stage-1 station writers (spec 412-418, plan D3-D5): a custom two-input keyed operator, keyed by station, with routed telemetry
 * and its barriers on input 1 and routed sessions and occupancy and their barriers on input 2. Each station is one progress writer: its
 * core {@link CutAssembler} holds the routed inputs and collects the required barriers; when it closes a cut, the cut's inputs (those of
 * every merged cut included) are applied in the contract order against the assembler's {@code admissionW}, the due deadlines are fired
 * at {@code W1}, and the cut's rows, dispositions, quarantine records and progress go to the station's partition {@code p(s)}.
 *
 * <p>Time. {@code W1} is the operator watermark itself (spec 620, the Flink form): input 1's watermark drives the event-time timer
 * service directly ({@link #processWatermark1}), input 2's watermark and stream status are ignored ({@link #processWatermark2},
 * {@link #processWatermarkStatus(WatermarkStatus, int)}), so the operator watermark is the minimum over the telemetry channels, that is
 * over the subscribed raw telemetry partitions (one split per source subtask). Before any telemetry it is {@code Long.MIN_VALUE}, which
 * the writer reports as the no-telemetry bound {@code -(2^53 - 1)}.
 *
 * <p>Timers (plan D4). The core {@link DeadlineIndex} of the station state is authoritative. Each distinct deadline of the index has
 * one Flink event-time timer; when it fires it only marks the deadline held. At a cut close the due deadlines are drained in the index's
 * order, so the firing order of Flink's timer queue never decides anything; a held mark is cleared once its deadline left the index.
 *
 * <p>State (plan D5). The writers of a subtask live in memory; at every checkpoint each changed station is written to keyed
 * {@link ValueState} (RocksDB) as versioned canonical bytes ({@value #STATE_VERSION}: the assembler with its held inputs, the core
 * state, the registered and the held deadlines). {@code initializeState} reads {@code forseti.recovery-control} up to its end offset H
 * with {@code read_committed}, fixes the epoch E of the latest declaration of the generation, and gives every writer its
 * {@code initEvidence} (the declaration's offset, H and the Flink restore evidence, {@link RestoreEvidence}); a restored or re-attempted
 * writer is reinitialized ({@link CutAssembler#reinitialize}). Nothing is committed from {@code snapshotState}: the transactional sink
 * commits on checkpoint completion.
 */
public final class Stage1Stations extends AbstractStreamOperator<Out>
    implements TwoInputStreamOperator<Msg, Msg, Out>, Triggerable<String, VoidNamespace> {
  private static final long serialVersionUID = 1L;
  public static final int STATE_VERSION = 1;
  public static final String STATE_NAME = "forseti-stage1-station", TIMERS = "forseti-stage1-deadlines";
  /** {@code W1} of a station before any telemetry: the bottom of the JCS integer domain (the reference harness's bound). */
  public static final long NO_TELEMETRY = -Json.MAX_SAFE;

  private final JobSettings settings;
  private transient JobSettings.Runtime rt;
  private transient ValueState<byte[]> stored;
  private transient InternalTimerService<VoidNamespace> timers;
  private transient Map<String, Station> stations;
  private transient long firedTimers, heldDrained, closedCuts;
  private transient ObjectNode initRecord;

  public Stage1Stations(JobSettings settings) { this.settings = settings; }

  /** One station writer. */
  private final class Station {
    final String name;
    final int partition;
    CutAssembler<Inputs.InputRecord> cuts;
    Stage1State state;
    final TreeSet<Long> timerAt = new TreeSet<>(), held = new TreeSet<>();
    boolean dirty = true;

    Station(String name) {
      this.name = name;
      this.partition = rt.partitionOf(name);
      this.cuts = new CutAssembler<>(name, rt.required.get(name), true);
      this.state = rt.stage1.newState(settings.generation(), name);
      if (state.partition() != partition) throw new IllegalStateException("station " + name + " partition " + state.partition() + " != routing " + partition);
    }

    Station(JsonNode n) {
      if (n.path("stationStateVersion").intValue() != STATE_VERSION) throw new IllegalArgumentException("stage-1 station state version " + n.path("stationStateVersion"));
      this.name = Json.text(n, "station");
      this.partition = rt.partitionOf(name);
      this.cuts = CutAssembler.fromJson(n.get("assembler"), InputCodec::decode);
      if (!cuts.required().equals(rt.required.get(name))) throw new IllegalStateException("station " + name + ": restored required barriers " + cuts.required() + " are not the routing's");
      this.state = Stage1State.fromJson(n.get("state"));
      n.path("timerAt").forEach(x -> timerAt.add(x.longValue()));
      n.path("held").forEach(x -> held.add(x.longValue()));
    }

    byte[] bytes() {
      ObjectNode n = Json.object().put("stationStateVersion", STATE_VERSION).put("station", name);
      n.set("assembler", cuts.toJson(InputCodec::encode));
      n.set("state", state.toJson());
      ArrayNode t = n.putArray("timerAt");
      timerAt.forEach(t::add);
      ArrayNode h = n.putArray("held");
      held.forEach(h::add);
      return Canonical.bytes(n);
    }
  }

  // --- initialization --------------------------------------------------------------------------------------------------------

  @Override public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    rt = settings.runtime();
    stored = context.getKeyedStateStore().getState(new ValueStateDescriptor<>(STATE_NAME, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    int attempt = getContainingTask().getEnvironment().getTaskInfo().getAttemptNumber();
    int subtask = getContainingTask().getEnvironment().getTaskInfo().getIndexOfThisSubtask();
    int parallelism = getContainingTask().getEnvironment().getTaskInfo().getNumberOfParallelSubtasks();
    int maxParallelism = getContainingTask().getEnvironment().getTaskInfo().getMaxNumberOfParallelSubtasks();
    RestoreEvidence restore = RestoreEvidence.of(context.isRestored(), context.getRestoredCheckpointId(), attempt);

    // the writer's initialization reads the recovery control up to H, before any business input (spec 416, F5)
    RecoveryControlStore.Snapshot control = Writers.readControl(settings);
    CutAssembler.Declaration declaration = control.assemblerDeclaration(settings.generation());
    if (declaration == null) throw new IllegalStateException("no recovery declaration of generation " + settings.generation());
    Progress.InitEvidence init = control.initEvidence(settings.generation(), restore.encode());

    // the stations of this subtask's key groups (spec 715: the actual assignment is recorded)
    KeyGroupRange range = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, subtask);
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
      int kg = KeyGroupRangeAssignment.assignToKeyGroup(s, maxParallelism);
      keyGroups.put(s, kg);
      if (range.contains(kg)) stations.computeIfAbsent(s, Station::new);
      else if (stations.containsKey(s)) throw new IllegalStateException("restored station " + s + " of key group " + kg + " outside " + range);
    }
    for (Station st : stations.values()) {
      if (restore.reinitialized()) st.cuts.reinitialize(declaration);
      st.state.initialize(init);
      st.dirty = true;
    }
    initRecord = Json.object().put("operator", "stage1-stations").put("subtask", subtask).put("parallelism", parallelism).put("maxParallelism", maxParallelism)
        .put("attemptNumber", attempt);
    initRecord.putArray("keyGroupRange").add(range.getStartKeyGroup()).add(range.getEndKeyGroup());
    initRecord.set("stationKeyGroups", keyGroups);
    ArrayNode owned = initRecord.putArray("stations");
    stations.keySet().forEach(owned::add);
    initRecord.put("restore", restore.encode());
    initRecord.set("initEvidence", Writers.initJson(init));
    Json.putSafe(initRecord, "epoch", declaration.targetEpoch());
    writeEvidence(subtask);
  }

  @Override public void open() throws Exception {
    super.open();
    timers = getInternalTimerService(TIMERS, VoidNamespaceSerializer.INSTANCE, this);
  }

  // --- inputs ---------------------------------------------------------------------------------------------------------------

  @Override public void processElement1(StreamRecord<Msg> element) { handle(element.getValue()); }

  @Override public void processElement2(StreamRecord<Msg> element) { handle(element.getValue()); }

  private void handle(Msg m) {
    Station st = stations.get(m.station);
    if (st == null) throw new IllegalStateException("station " + m.station + " is not a writer of this subtask: key groups " + initRecord.get("keyGroupRange"));
    Progress.Origin origin = new Progress.Origin(m.topic, m.partition);
    if (m.kind == Msg.RECORD) {
      st.cuts.input(origin, InputParser.read(new Inputs.Origin(m.topic, m.partition, m.offset), m.keyText(), m.value).record());
    } else if (m.kind == Msg.BARRIER) {
      CutAssembler.Barrier b = CutBarrierRecord.fromJson(Json.read(m.value)).barrier();
      if (!b.origin().equals(origin)) throw new IllegalStateException("barrier-origin: " + b.origin() + " delivered as " + origin);
      st.cuts.barrier(b, Writers.control(settings));
    } else throw new IllegalStateException("stage-1 station: element kind " + m.kind);
    st.dirty = true;
    close(st);
  }

  /** Input 1 (telemetry): the watermark drives the timer service directly, bypassing the two-input combination. */
  @Override public void processWatermark1(Watermark mark) throws Exception {
    super.processWatermark(mark);
    for (Station st : stations.values()) {
      setCurrentKey(st.name);
      close(st);
    }
  }

  /** Input 2 (sessions, occupancy): its watermark neither advances nor holds stage-1 event time (spec 77, plan D3). */
  @Override public void processWatermark2(Watermark mark) {}

  /** Stream status: input 2's is ignored; telemetry is never idle (no idleness), so an idle status on input 1 is a violated
   *  assumption and fails the job. */
  @Override protected void processWatermarkStatus(WatermarkStatus status, int index) {
    if (index == 0 && status.isIdle()) throw new IllegalStateException("the telemetry input of stage 1 became idle; gate runs set no idleness (spec 590)");
  }

  @Override public void onEventTime(InternalTimer<String, VoidNamespace> timer) {
    Station st = stations.get(timer.getKey());
    if (st == null) throw new IllegalStateException("a timer of station " + timer.getKey() + " outside this subtask");
    if (st.timerAt.remove(timer.getTimestamp())) {
      st.held.add(timer.getTimestamp());
      st.dirty = true;
      firedTimers++;
    }
  }

  @Override public void onProcessingTime(InternalTimer<String, VoidNamespace> timer) {
    throw new IllegalStateException("stage 1 registers no processing-time timer");
  }

  // --- cut close --------------------------------------------------------------------------------------------------------------

  /** The engine's own event time: the operator watermark, the no-telemetry bound before the first one. */
  private long nativeTime() {
    long w = timers.currentWatermark();
    return w < NO_TELEMETRY ? NO_TELEMETRY : w;
  }

  private void close(Station st) {
    while (true) {
      long w = nativeTime();
      Optional<CutAssembler.Closed<Inputs.InputRecord>> c = st.cuts.poll(w);
      if (c.isEmpty()) return;
      CutAssembler.Closed<Inputs.InputRecord> closed = c.get();
      Long admissionW = closed.admissionW();
      if (!Objects.equals(admissionW, st.state.admissionW()))
        throw new IllegalStateException("station " + st.name + ": the assembler's admissionW " + admissionW + " differs from the state's " + st.state.admissionW());
      List<Object> raw = new ArrayList<>();
      for (Inputs.InputRecord r : ContractOrder.sort(new ArrayList<>(closed.inputs())))
        apply(st, new Events.Admit(r, Events.kindRank(r.origin().topic())), admissionW, raw);
      // the due deadlines, drained in the index's order (plan D4); a held mark only says that Flink's timer fired
      while (true) {
        List<DeadlineIndex.Deadline> due = st.state.deadlines().due(w);
        if (due.isEmpty()) break;
        DeadlineIndex.Deadline d = due.get(0);
        if (st.held.contains(d.deadline())) heldDrained++;
        apply(st, new Events.Timer(d.deadline(), d.key(), d.purpose()), w, raw);
      }
      List<JsonNode> refs = new ArrayList<>();
      for (Progress.BarrierRef b : closed.barrierRefs()) refs.add(b.toJson());
      apply(st, new Events.CutClose(closed.cutId(), w, refs, closed.restartEpoch(), closed.recoveryRef(), closed.mergedCutIds()), w, raw);
      for (Object o : raw) emit(st, o instanceof Row row ? Stage1.stamp(row, rt.pins, w) : o);
      syncTimers(st);
      st.dirty = true;
      closedCuts++;
    }
  }

  private void apply(Station st, Events.Event e, Long watermark, List<Object> out) {
    Stage1.Result res = rt.stage1.decide(st.state, e, watermark);
    st.state = res.state();
    out.addAll(res.outputs());
  }

  /** One Flink event-time timer per distinct deadline of the index that has not fired yet; timers of deadlines that left it removed. */
  private void syncTimers(Station st) {
    TreeSet<Long> current = new TreeSet<>();
    for (DeadlineIndex.Deadline d : st.state.deadlines().all()) current.add(d.deadline());
    st.held.retainAll(current);
    for (Long d : new ArrayList<>(st.timerAt)) if (!current.contains(d)) { timers.deleteEventTimeTimer(VoidNamespace.INSTANCE, d); st.timerAt.remove(d); }
    for (Long d : current) if (!st.held.contains(d) && st.timerAt.add(d)) timers.registerEventTimeTimer(VoidNamespace.INSTANCE, d);
  }

  private void emit(Station st, Object x) {
    int p = st.partition;
    if (x instanceof Row r) out(r.topic(), p, OutputRecords.rowLine(r));
    else if (x instanceof Disposition d) out(ReplayTopics.DISPOSITIONS1, p, OutputRecords.dispositionLine(d));
    else if (x instanceof QuarantineRecord q) out(ReplayTopics.QUARANTINE, p, OutputRecords.quarantineLine(q));
    else if (x instanceof Progress.Stage1 s) { out(Stage1.WORK_RECORDS, p, OutputRecords.progressLine(s)); out(Stage1.SAMPLES, p, OutputRecords.progressLine(s)); }
    else if (x instanceof Progress.Ledger l) out(ReplayTopics.DISPOSITIONS1, p, OutputRecords.progressLine(l));
    else throw new IllegalStateException("not a stage-1 output: " + x);
  }

  private void out(String topic, int partition, ObjectNode line) {
    output.collect(new StreamRecord<>(Out.of(settings.runName(topic), partition, OutputRecords.transport(line))));
  }

  // --- checkpoint -------------------------------------------------------------------------------------------------------------

  @Override public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    for (Station st : stations.values()) {
      if (!st.dirty) continue;
      setCurrentKey(st.name);
      stored.update(st.bytes());
      st.dirty = false;
    }
    writeEvidence(getContainingTask().getEnvironment().getTaskInfo().getIndexOfThisSubtask());
  }

  private void writeEvidence(int subtask) {
    ObjectNode n = initRecord.deepCopy();
    Json.putSafe(n, "closedCuts", closedCuts);
    Json.putSafe(n, "firedTimers", firedTimers);
    Json.putSafe(n, "heldDeadlinesDrained", heldDrained);
    Writers.evidence(settings, "stage1-stations-" + subtask + "-attempt-" + n.path("attemptNumber").intValue(), n);
  }
}
