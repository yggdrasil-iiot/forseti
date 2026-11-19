package yggdrasil.forseti.engine.flink.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
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
import yggdrasil.forseti.core.journal.Revisions;
import yggdrasil.forseti.core.stage1.Routing;
import yggdrasil.forseti.core.stage2.Incidents;

/**
 * A reject collector (plan D6): the single writer of partition 6 of {@code forseti.dispositions-stage1} (stage 1) or
 * {@code forseti.dispositions-stage2} (stage 2), run at parallelism 1. It receives the inputs its router refused and every barrier its
 * router consumed, holds the inputs in its core {@link CutAssembler} (required set: every subscribed raw telemetry, session and occupancy
 * partition at stage 1, the six incident partitions at stage 2; every barrier counts, whatever its {@code idleInputs}) and, when a cut
 * closes, writes the disposition rows of the cut's refused inputs in origin order (one revision per body change) and its ledger
 * progress. The refusal is derived again from the held input's bytes with the same core door the router used ({@link Routing} at stage 1,
 * {@link Incidents#door} at stage 2), so only the record is held. Stage 1 reads {@code forseti.recovery-control} at its initialization
 * and its first progress carries {@code initEvidence}; stage 2 never skips and carries none. State: one versioned entry in operator state.
 */
public final class RejectCollectorFunction extends ProcessFunction<Msg, Out> implements CheckpointedFunction {
  private static final long serialVersionUID = 1L;
  public static final int STATE_VERSION = 1, PARTITION = 6;
  private final JobSettings settings;
  private final int stage;
  private transient JobSettings.Runtime rt;
  private transient ListState<byte[]> stored;
  private transient CutAssembler<Inputs.InputRecord> cuts;
  private transient Revisions revisions;
  private transient Progress.InitEvidence init;

  public RejectCollectorFunction(JobSettings settings, int stage) {
    if (stage != 1 && stage != 2) throw new IllegalArgumentException("stage " + stage);
    this.settings = settings;
    this.stage = stage;
  }

  private String ledger() { return stage == 1 ? ReplayTopics.DISPOSITIONS1 : ReplayTopics.DISPOSITIONS2; }

  @Override public void initializeState(FunctionInitializationContext context) throws Exception {
    rt = settings.runtime();
    stored = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("forseti-stage" + stage + "-rejects", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    List<Progress.Origin> required = stage == 1 ? rt.rejectsRequired : rt.incidentOrigins;
    cuts = null;
    if (context.isRestored()) for (byte[] b : stored.get()) {
      if (cuts != null) throw new IllegalStateException("reject collector " + stage + ": two state entries");
      JsonNode n = Json.read(b);
      if (n.path("rejectCollectorStateVersion").intValue() != STATE_VERSION) throw new IllegalArgumentException("reject collector state version " + n.path("rejectCollectorStateVersion"));
      cuts = CutAssembler.fromJson(n.get("assembler"), InputCodec::decode);
      revisions = Revisions.fromJson(n.get("revisions"));
    }
    if (cuts == null) { cuts = new CutAssembler<>(null, required, false); revisions = new Revisions(); }
    if (!cuts.required().equals(required)) throw new IllegalStateException("reject collector " + stage + ": restored required barriers " + cuts.required());
    if (stage == 2) { init = null; return; }
    int attempt = getRuntimeContext().getTaskInfo().getAttemptNumber();
    RestoreEvidence restore = RestoreEvidence.of(settings.launch(), context.isRestored(), context.getRestoredCheckpointId(), attempt);
    RecoveryControlStore.Snapshot control = Writers.readControl(settings);
    CutAssembler.Declaration declaration = control.assemblerDeclaration(settings.generation());
    if (declaration == null) throw new IllegalStateException("no recovery declaration of generation " + settings.generation());
    init = control.initEvidence(settings.generation(), restore.encode());
    if (restore.reinitialized()) cuts.reinitialize(declaration);
    ObjectNode e = Json.object().put("operator", "stage1-rejects").put("attemptNumber", attempt).put("launch", settings.launch()).put("restore", restore.encode());
    e.set("initEvidence", Writers.initJson(init));
    e.set("required", Writers.origins(required).get("origins"));
    Writers.evidence(settings, "stage1-rejects-" + restore.incarnation(), e);
  }

  @Override public void snapshotState(FunctionSnapshotContext context) throws Exception {
    ObjectNode n = Json.object().put("rejectCollectorStateVersion", STATE_VERSION).put("stage", stage);
    n.set("assembler", cuts.toJson(InputCodec::encode));
    n.set("revisions", revisions.toJson());
    stored.update(List.of(Canonical.bytes(n)));
  }

  @Override public void processElement(Msg m, Context ctx, Collector<Out> out) {
    Progress.Origin origin = new Progress.Origin(m.topic, m.partition);
    if (m.kind == Msg.RECORD) cuts.input(origin, InputParser.read(new Inputs.Origin(m.topic, m.partition, m.offset), m.keyText(), m.value).record());
    else if (m.kind == Msg.BARRIER) {
      CutAssembler.Barrier b = CutBarrierRecord.fromJson(Json.read(m.value)).barrier();
      if (!b.origin().equals(origin)) throw new IllegalStateException("barrier-origin: " + b.origin() + " delivered as " + origin);
      cuts.barrier(b, Writers.control(settings));
    } else throw new IllegalStateException("reject collector: element kind " + m.kind);
    while (true) {
      Optional<CutAssembler.Closed<Inputs.InputRecord>> c = cuts.poll(null);
      if (c.isEmpty()) return;
      close(c.get(), out);
    }
  }

  private void close(CutAssembler.Closed<Inputs.InputRecord> c, Collector<Out> out) {
    if (stage == 2 && !c.mergedCutIds().isEmpty()) throw new IllegalStateException("the stage-2 reject collector never skips: cut " + c.cutId() + " merges " + c.mergedCutIds());
    long g = settings.generation();
    List<Inputs.InputRecord> pending = new ArrayList<>(c.inputs());
    pending.sort(ContractOrder.ORIGIN);
    for (Inputs.InputRecord r : pending) {
      InputParser.Read read = InputParser.read(r.origin(), r.key(), r.rawValue());
      String part;
      Disposition.Body body;
      if (stage == 1) {
        if (!(rt.router.route(read) instanceof Routing.Rejected x)) throw new IllegalStateException("a held input the router does not refuse: " + r.origin());
        part = x.part();
        body = x.body();
      } else {
        if (!(Incidents.door(rt.rules, read) instanceof Incidents.Rejected x)) throw new IllegalStateException("a held incident the door does not refuse: " + r.origin());
        part = Disposition.Part.EVENT;
        body = x.body();
      }
      Disposition.Key key = new Disposition.Key(g, r.origin().topic(), r.origin().partition(), r.origin().offset(), part);
      Optional<Revisions.Emission> em = revisions.next(g, key.toJson(), body.digest());
      if (em.isPresent()) emit(out, OutputRecords.dispositionLine(new Disposition(key, body, em.get().revision())));
    }
    emit(out, OutputRecords.progressLine(new Progress.Ledger(g, stage, PARTITION, null, c.cutId(), 0, c.barrierRefs(), c.mergedCutIds(), c.restartEpoch(),
        c.recoveryRef(), init)));
    init = null;
  }

  private void emit(Collector<Out> out, ObjectNode line) { out.collect(Out.of(settings.runName(ledger()), PARTITION, OutputRecords.transport(line))); }
}
