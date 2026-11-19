package yggdrasil.forseti.engine.flink.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.common.TopicPartition;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.cli.replay.CompletionReader;
import yggdrasil.forseti.cli.replay.ProcessSupervisor;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.cli.replay.Restarts;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.Stage2;
import yggdrasil.forseti.engine.flink.job.RestoreEvidence;
import yggdrasil.forseti.engine.flink.job.Topology;
import yggdrasil.forseti.replay.kafka.KafkaCompletionReader;
import yggdrasil.forseti.replay.kafka.KafkaProgressLog;
import yggdrasil.forseti.replay.kafka.KafkaRecoveryControl;
import yggdrasil.forseti.replay.kafka.KafkaTransactions;

/**
 * The restart injections of one Flink run (spec 416-418, 678-679, 685; plan D7, D8, D9), placed at cut k ({@link Restarts#place}): the
 * harness commits the first stage-1 injection's recovery declaration ({@code targetEpoch = epochBefore + 1}, {@code lastWrittenCutId} k)
 * right before batch k, so confirming it adds nothing between batch k and the kill (the running writers are not recovering and never
 * read it), writes batch k, then
 * <ul><li><b>21</b>: SIGKILL of the JobManager and TaskManager JVMs of both jobs together (on a POSIX host both TaskManagers SIGSTOPped
 * first, the instant the batch is written);</li>
 * <li><b>22(i)</b> (plan D8): the stage-1 station sink's transaction of cut k's output identified while it is
 * {@code Ongoing}; SIGSTOP of the stage-1 TaskManager; {@code describeTransactions} of that transaction (transactional id, producer id,
 * epoch) asserting {@code Ongoing} with its {@code read_uncommitted} end above the last stable offset; SIGKILL of the TaskManager; a second
 * {@code describeTransactions} before any replacement starts, asserting the same transaction still {@code Ongoing}; then the JobManager
 * stopped (SIGKILL, it holds nothing the restore needs); missing or ambiguous evidence makes the attempt invalid;</li>
 * <li><b>22(iv)</b>: 22(i), then, once every job is RUNNING again and before batch k+1, a second declaration and a second SIGKILL of
 * the stage-1 JVMs (chained: no cut carries the first injection's epoch);</li>
 * <li><b>22(ii)</b>: stage 2 held (SIGSTOP of its TaskManager on a POSIX host) before batch k is written, the stage-1 output of cut k
 * committed at every stage-1 writer, stage 2's committed progress still below k, then SIGKILL of the stage-2 JVMs (a process kill, not a
 * pause: the hold only fixes the instant); no declaration, the stage-1 epoch unchanged.</li></ul>
 * Each killed job is relaunched as a new launch of its cluster (the next durable launch identity, recorded before the start) restoring
 * its latest completed checkpoint, and the harness waits until every job and all its tasks are RUNNING. Under control theta
 * (non-transactional sinks, plan D9) the D8 transaction protocol has no transaction to describe: the harness writes batch k right after a
 * checkpoint completed and kills right after the cut's output is published and before the next checkpoint completes (an attempt in which
 * a checkpoint completed in between is invalid). Every attempt's records go into the run's injection manifest ({@code injections.json}).
 */
final class RestartInjector {
  static final Duration WAIT = Duration.ofSeconds(60), RUNNING = FlinkRun.RUNNING_TIMEOUT;

  /** An injection attempt whose evidence is missing or ambiguous (plan D8): the run is abandoned and repeated on a fresh one. */
  static final class Invalid extends RuntimeException {
    /** The attempt's injection records up to the moment it was abandoned (plan D8: every attempt is recorded). */
    final JsonNode records;
    Invalid(String m, JsonNode records) { super(m); this.records = records; }
  }

  /** Abandons the attempt: its records so far go with the failure, which the run persists before the next attempt starts. */
  private Invalid invalid(String why) { return new Invalid(why, injections.deepCopy()); }

  /** What a relaunch needs: the job spec of the next launch of a stage. */
  interface Specs { Path forLaunch(int stage, int launch); }

  final Restarts.Kind kind;
  final long k;
  final boolean theta, posix;
  final String bootstrap, prefix;
  final long generation;
  final String routingDigest;
  final RoutingManifest routing;
  final Map<String, String> names;
  final FlinkCluster s1, s2;
  final KafkaRecoveryControl rc;
  final Specs specs;
  final ArrayNode injections = Json.JSON.createArrayNode();
  /** Per stage-1 writer partition {@code <topic>/<p>} (the three stage-1 progress topics and the reject collector's ledger partition). */
  final Map<String, TopicPartition> stage1Partitions = new TreeMap<>(), representative = new TreeMap<>();
  final Map<String, TopicPartition> stage2Partitions = new TreeMap<>();
  private Long checkpointBeforeBatch;
  private String heldStage2;
  private KafkaTransactions.Described identified;
  private ObjectNode beforeSnapshot, first;
  private long batchNanos;
  private Map<TopicPartition, Long> outputEnds;

  RestartInjector(Restarts.Kind kind, long k, boolean theta, boolean posix, String bootstrap, String prefix, long generation, String routingDigest,
                  RoutingManifest routing, Map<String, String> names, FlinkCluster s1, FlinkCluster s2, KafkaRecoveryControl rc, Specs specs) {
    this.kind = kind; this.k = k; this.theta = theta; this.posix = posix; this.bootstrap = bootstrap; this.prefix = prefix; this.generation = generation;
    this.routingDigest = routingDigest; this.routing = routing; this.names = names; this.s1 = s1; this.s2 = s2; this.rc = rc; this.specs = specs;
    for (Map.Entry<String, Integer> s : new TreeMap<>(routing.stations()).entrySet()) {
      for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES, ReplayTopics.DISPOSITIONS1)) stage1Partitions.put(t + "/" + s.getValue(), tp(t, s.getValue()));
      representative.put(s.getKey(), tp(Stage1.WORK_RECORDS, s.getValue()));
      for (String t : List.of(Stage2.AFFECTED, Stage2.AFFECTED_SETS, ReplayTopics.DISPOSITIONS2)) stage2Partitions.put(t + "/" + s.getValue(), tp(t, s.getValue()));
    }
    stage1Partitions.put(ReplayTopics.DISPOSITIONS1 + "/" + CompletionReader.REJECTS, tp(ReplayTopics.DISPOSITIONS1, CompletionReader.REJECTS));
    representative.put(Restarts.REJECTS, tp(ReplayTopics.DISPOSITIONS1, CompletionReader.REJECTS));
  }

  private TopicPartition tp(String contract, int p) { return new TopicPartition(names.get(contract), p); }

  /** Every stage-1 progress writer: the stations and the stage-1 reject collector (spec 416). */
  List<String> stage1Writers() { return Restarts.stage1Writers(routing.stations().keySet()); }

  // --- before batch k ------------------------------------------------------------------------------------------------------------

  /** Before batch k is written: for a stage-1 kind the records before the injection, a fresh checkpoint of stage 1 (the widest window
   *  before the next one: cut k's output is then in an open transaction, or under theta published and not yet in a checkpoint) and the
   *  log ends of the station output partitions; on a host without POSIX signals the first declaration (spec 416 only asks it committed
   *  before the kill; with a SIGSTOP hold it is written after the hold instead); for 22(ii), stage 2 held. */
  void beforeBatch() {
    if (kind.stage1) {
      first = newInjection(kind.injectionIds.get(0), true, kind == Restarts.Kind.I21 ? List.of("stage1", "stage2") : List.of("stage1"));
      try (KafkaProgressLog log = new KafkaProgressLog(bootstrap, generation)) { beforeSnapshot = snapshot(log); }
      if (!posix) {
        declare(first, new Epoch(0, null), kind.injectionIds.get(0));
        event(first, "declaration committed (before batch " + k + ": no POSIX hold on this host)");
      }
      checkpointBeforeBatch = freshCheckpoint(s1);
      try (KafkaProgressLog log = new KafkaProgressLog(bootstrap, generation)) {
        // the log ends of the station output partitions before batch k: the first record above one is cut k's output in the sink
        outputEnds = new LinkedHashMap<>();
        for (int p : new TreeSet<>(routing.stations().values())) for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES)) outputEnds.put(tp(t, p), log.highWatermark(tp(t, p)));
      }
    }
    if (kind == Restarts.Kind.I22_II) {
      if (posix) { s2.taskManager().signal(ProcessSupervisor.Signal.STOP); heldStage2 = "SIGSTOP of the stage-2 TaskManager before batch " + k + " (released by its SIGKILL)"; }
      else heldStage2 = "none: this host has no POSIX signals (the replay gate runs on Linux, plan D12); the kill is valid only if stage 2 committed no progress of cut " + k;
    }
    batchNanos = 0;
  }

  /** The instant batch k was written (the reference of the injection's timeline). */
  void batchWritten() { batchNanos = System.nanoTime(); }

  /** One event of the injection's timeline, in milliseconds since batch k was written (negative before it). */
  private void event(ObjectNode inj, String what) {
    ArrayNode t = inj.has("timeline") ? (ArrayNode) inj.get("timeline") : inj.putArray("timeline");
    ObjectNode e = t.addObject().put("event", what);
    if (batchNanos != 0) Json.putSafe(e, "msAfterBatch", (System.nanoTime() - batchNanos) / 1_000_000); else e.put("msAfterBatch", "before batch " + k);
  }

  /** Waits for the next checkpoint of {@code c} that the JobManager reports completed and returns its id. */
  private Long freshCheckpoint(FlinkCluster c) {
    Long first = c.reportedCompletedCheckpoint();
    long until = System.nanoTime() + WAIT.toNanos();
    while (System.nanoTime() < until) {
      Long now = c.reportedCompletedCheckpoint();
      if (now != null && !now.equals(first)) return now;
      sleep(20);
    }
    throw invalid(c.name + ": no checkpoint completed within " + WAIT);
  }

  // --- after batch k -------------------------------------------------------------------------------------------------------------

  /** The epoch state of the harness after the injection: the epoch and recovery reference of the next batches. */
  record Epoch(long epoch, String ref) {}

  /** The injection after batch k, with the epoch {@code epoch} batch k carried; returns the epoch of the batches after it. */
  Epoch inject(Epoch epoch, KafkaCompletionReader completion, Map<String, Long> timings) throws Exception {
    switch (kind) {
      case I21 -> { return stage1Restart(epoch, kind.injectionIds.get(0), completion, true); }
      case I22_I -> { return stage1Restart(epoch, kind.injectionIds.get(0), completion, false); }
      case I22_IV -> {
        Epoch first = stage1Restart(epoch, kind.injectionIds.get(0), completion, false);
        // the second kill: after every job is RUNNING again and before batch k+1, so no cut carries the first injection's epoch
        return stage1Kill(first, kind.injectionIds.get(1), "the second stage-1 kill of the chained injection (spec 685)");
      }
      case I22_II -> { stage2Restart(completion); return epoch; }
      default -> throw new IllegalStateException(kind.toString());
    }
  }

  /** The declaration of a stage-1 restart: {@code targetEpoch = epochBefore + 1}, committed before anything is killed (spec 416). */
  private ObjectNode declare(ObjectNode inj, Epoch e, String id) {
    ObjectNode d = Json.putSafe(Json.object(), "generation", generation).put("injectionId", id).put("stage", 1);
    Json.putSafe(d, "epochBefore", e.epoch());
    Json.putSafe(d, "targetEpoch", e.epoch() + 1);
    Json.putSafe(d, "lastWrittenCutId", k);
    d.put("routingDigest", routingDigest);
    long t0 = System.nanoTime();
    long offset = rc.append(d);   // returns once the declaration is committed and readable with read_committed
    Json.putSafe(inj, "declarationMs", (System.nanoTime() - t0) / 1_000_000);
    ObjectNode x = inj.putObject("declaration");
    Json.putSafe(x, "offset", offset);
    x.set("value", d);
    // H after the declaration: the end offset every writer initialized after it reads (nothing else writes the control topic of the run)
    Json.putSafe(x, "controlEndAfter", rc.read().endOffset());
    Json.putSafe(inj, "epochBefore", e.epoch());
    Json.putSafe(inj, "targetEpoch", e.epoch() + 1);
    return d;
  }

  private ObjectNode newInjection(String id, boolean stage1Restart, List<String> jobs) {
    ObjectNode inj = injections.addObject().put("injectionId", id).put("spec", kind.spec()).put("stage1Restart", stage1Restart);
    ArrayNode j = inj.putArray("jobsRestarted");
    jobs.forEach(j::add);
    Json.putSafe(inj, "lastWrittenCutId", k);
    Json.putSafe(inj, "firstCutAtTarget", k + 1);
    return inj;
  }

  private Epoch stage1Restart(Epoch e, String id, KafkaCompletionReader completion, boolean both) throws Exception {
    ObjectNode inj = first;
    if (e.epoch() != 0) throw new IllegalStateException("the first stage-1 injection at epoch " + e.epoch());
    inj.set("before", beforeSnapshot);
    int launchBefore = s1.launchId(), launch2Before = s2.launchId();
    killDuringCut(inj, id, both);
    ObjectNode launches = inj.putObject("launches");
    relaunch(s1, 1, launchBefore, launches.putObject("stage1"));
    if (both) relaunch(s2, 2, launch2Before, launches.putObject("stage2"));
    awaitAllRunning(inj);
    outcome(inj);
    expectedSets(inj);
    return new Epoch(e.epoch() + 1, id);
  }

  /** The kill of the first stage-1 injection (21, 22(i), the first of 22(iv)), at one point for the run and for control theta: after
   *  cut k's output appears in the stage-1 sink and before it commits (plan D8, D9). Without theta the proof is the transaction's: an
   *  {@code Ongoing} station-sink transaction with records above the last stable offset is identified, the TaskManager(s) held (SIGSTOP,
   *  on a POSIX host; 22(i) requires it), the declaration committed, the transaction described again ({@code Ongoing}, open records, the
   *  same producer id and epoch), SIGKILL, and described a third time before any replacement starts (the same {@code Ongoing}
   *  transaction with open records). Under theta there is no transaction: the kill is valid when no checkpoint completed between batch k
   *  and the kill. Either way at least one stage-1 writer must not have committed cut k at the kill (the kill hits uncommitted output);
   *  otherwise the attempt is invalid. */
  private void killDuringCut(ObjectNode inj, String id, boolean both) {
    ObjectNode kp = inj.putObject(theta ? "theta" : "d8");
    Json.putSafe(kp, "checkpointBeforeBatch", checkpointBeforeBatch);
    String txPrefix = Topology.transactionalPrefix(prefix, generation, "stage1-stations");
    if (!theta) kp.put("transactionalIdPrefix", txPrefix);
    if (!posix && !theta && !both) throw new UnsupportedOperationException("injection 22(i) holds the stage-1 TaskManager with SIGSTOP (plan D8): it needs a POSIX host (the replay gate runs on Linux, plan D12)");
    try (KafkaTransactions tx = new KafkaTransactions(bootstrap)) {
      kp.put("published", published());
      event(inj, "cut " + k + "'s output in the stage-1 sink: " + kp.path("published").asText());
      KafkaTransactions.Described pick = null;
      if (!theta) {
        ArrayNode seen = kp.putArray("candidates");
        long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (pick == null && System.nanoTime() < until) {
          for (String t : tx.ongoing(txPrefix)) {
            Optional<KafkaTransactions.Described> d = tx.describe(t);
            if (d.isPresent()) seen.add(d.get().toJson());
            if (d.isPresent() && d.get().ongoing() && d.get().openRecords()) { pick = d.get(); break; }
          }
          if (pick == null) sleep(10);
        }
        if (pick == null) { s1.kill(true, true, "injection " + id + " abandoned: no Ongoing transaction"); if (both) s2.kill(true, true, "injection " + id + " abandoned"); throw invalid(kind.spec() + ": no Ongoing stage-1 station transaction with open records within 30 s"); }
        kp.set("identified", pick.toJson());
        event(inj, "identified the Ongoing transaction " + pick.transactionalId());
      }
      if (posix) {
        s1.taskManager().signal(ProcessSupervisor.Signal.STOP);
        if (both) s2.taskManager().signal(ProcessSupervisor.Signal.STOP);
        kp.put("hold", "SIGSTOP of the stage-1" + (both ? " and stage-2" : "") + " TaskManager" + (both ? "s" : "") + " (released by SIGKILL)");
        event(inj, "hold: SIGSTOP");
        // the letter of spec 416: the declaration written and confirmed committed after batch k and before the kill; the hold makes the
        // time it takes free
        declare(inj, new Epoch(0, null), id);
        event(inj, "declaration committed (after the hold, before the kill)");
      } else kp.put("hold", "none: this host has no POSIX signals (plan D12)");
      KafkaTransactions.Described one = null;
      if (!theta) {
        one = tx.describe(pick.transactionalId()).orElse(null);
        kp.set("phase1", one == null ? null : one.toJson());
        event(inj, "phase 1 describeTransactions" + (posix ? " under SIGSTOP" : ""));
        if (one == null || !one.ongoing() || !one.openRecords() || one.producerId() != pick.producerId() || one.producerEpoch() != pick.producerEpoch()) {
          s1.kill(true, true, "injection " + id + " abandoned: phase 1"); if (both) s2.kill(true, true, "injection " + id + " abandoned");
          throw invalid(kind.spec() + " phase 1: the identified transaction " + pick.transactionalId() + " is not the same Ongoing one with open records: " + (one == null ? "unknown" : one.toJson()));
        }
      }
      s1.kill(false, true, "injection " + id + (theta ? " (control theta)" : ""));
      if (both) s2.kill(false, true, "injection " + id + (theta ? " (control theta)" : ""));
      event(inj, "SIGKILL of the TaskManager" + (both ? "s" : ""));
      if (!theta) {
        KafkaTransactions.Described two = tx.describe(pick.transactionalId()).orElse(null);
        kp.set("phase2", two == null ? null : two.toJson());
        if (two == null || !two.ongoing() || !two.openRecords() || two.producerId() != one.producerId() || two.producerEpoch() != one.producerEpoch()) {
          s1.kill(true, false, "injection " + id + " abandoned: phase 2"); if (both) s2.kill(true, false, "injection " + id + " abandoned");
          throw invalid(kind.spec() + " phase 2: after SIGKILL the transaction " + pick.transactionalId() + " is not the same Ongoing one with open records: " + (two == null ? "unknown" : two.toJson()));
        }
        identified = two;
      }
      s1.kill(true, false, "after injection " + id + ": the JobManager stopped; the restart relaunches the cluster from the latest completed checkpoint");
      if (both) s2.kill(true, false, "after injection " + id + ": the JobManager stopped");
    }
    Long onDisk = s1.latestCompletedCheckpoint();
    Json.putSafe(kp, "checkpointOnDiskAtKill", onDisk);
    try (KafkaProgressLog log = new KafkaProgressLog(bootstrap, generation)) { inj.set("atKill", snapshot(log)); }
    if (theta) {
      if (!Objects.equals(onDisk, checkpointBeforeBatch))
        throw invalid("theta: a checkpoint completed between batch " + k + " and the kill (before " + checkpointBeforeBatch + ", on disk " + onDisk + ")");
      kp.put("kill", "after cut " + k + "'s output was published and before the next checkpoint");
      return;
    }
    // the kill hits uncommitted output: at least one stage-1 writer had not committed cut k (read_committed, at the kill)
    List<String> open = new ArrayList<>();
    inj.path("atKill").path("lastProgressCutId").fields().forEachRemaining(x -> { if (x.getValue().asLong() < k) open.add(x.getKey()); });
    ArrayNode u = kp.putArray("uncommittedAtKill");
    open.forEach(u::add);
    if (open.isEmpty()) throw invalid(kind.spec() + ": every stage-1 writer had committed cut " + k + " at the kill: the kill hit no uncommitted output");
  }

  /** Plan D8: the post-restart outcome of the identified transaction, read right after every job is RUNNING again, by its state and
   *  producer epoch, and cross-checked with the restored checkpoint: a restore of the checkpoint completed before batch k means the
   *  transaction's checkpoint never completed, so it must have been aborted. */
  private void outcome(ObjectNode inj) {
    if (identified == null) return;
    ObjectNode o = inj.putObject("outcome");
    long restored = inj.path("launches").path("stage1").path("restoredCheckpointId").asLong(-1);
    boolean mustAbort = checkpointBeforeBatch != null && restored == checkpointBeforeBatch;
    o.put("restoredBeforeBatchCheckpoint", mustAbort);
    try (KafkaTransactions tx = new KafkaTransactions(bootstrap)) {
      KafkaTransactions.Described after = null;
      long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (System.nanoTime() < until) {
        after = tx.describe(identified.transactionalId()).orElse(null);
        if (after == null || !(after.ongoing() && after.producerEpoch() == identified.producerEpoch())) break;
        sleep(50);
      }
      o.set("after", after == null ? null : after.toJson());
      String st = after == null ? "unknown" : after.state();
      boolean sameEpoch = after != null && after.producerId() == identified.producerId() && after.producerEpoch() == identified.producerEpoch();
      boolean abortish = st.equals("CompleteAbort") || st.equals("PrepareAbort") || after != null && after.producerEpoch() > identified.producerEpoch() && !st.equals("CompleteCommit") && !st.equals("PrepareCommit");
      boolean commitish = (st.equals("CompleteCommit") || st.equals("PrepareCommit")) && sameEpoch;
      String out;
      if (after == null) out = "unresolved: the transactional id is unknown after the restart";
      else if (after.ongoing() && sameEpoch) out = "unresolved: still the same Ongoing transaction after every job is RUNNING";
      else if (mustAbort) out = abortish || !commitish ? "aborted" : "inconsistent: committed though its checkpoint never completed (restored checkpoint " + restored + ")";
      else out = commitish ? "committed by the restored committer" : abortish ? "aborted" : "unresolved: " + st + " epoch " + after.producerEpoch();
      o.put("outcome", out);
    }
  }

  /** The second kill of 22(iv): a declaration and SIGKILL of both stage-1 JVMs once every job is RUNNING, before batch k+1. */
  private Epoch stage1Kill(Epoch e, String id, String why) throws Exception {
    ObjectNode inj = newInjection(id, true, List.of("stage1"));
    try (KafkaProgressLog log = new KafkaProgressLog(bootstrap, generation)) { inj.set("before", snapshot(log)); }
    declare(inj, e, id);
    event(inj, "declaration committed (every job RUNNING, before batch " + (k + 1) + ")");
    int launchBefore = s1.launchId();
    s1.kill(true, true, why);
    event(inj, "SIGKILL of the stage-1 JobManager and TaskManager");
    try (KafkaProgressLog log = new KafkaProgressLog(bootstrap, generation)) { inj.set("atKill", snapshot(log)); }
    relaunch(s1, 1, launchBefore, inj.putObject("launches").putObject("stage1"));
    awaitAllRunning(inj);
    expectedSets(inj);
    return new Epoch(e.epoch() + 1, id);
  }

  /** Waits until a record of cut k's stage-1 output is appended (under theta published: no transaction) on a station partition. */
  private String published() {
    long until = System.nanoTime() + WAIT.toNanos();
    try (KafkaProgressLog log = new KafkaProgressLog(bootstrap, generation)) {
      while (System.nanoTime() < until) {
        for (Map.Entry<TopicPartition, Long> e : outputEnds.entrySet()) {
          long now = log.highWatermark(e.getKey());
          if (now > e.getValue()) return e.getKey() + " " + e.getValue() + " -> " + now;
        }
        sleep(2);
      }
    }
    throw invalid("no output of cut " + k + " published within " + WAIT);
  }

  /** 22(ii): stage-1 output of cut k committed at every stage-1 writer, stage 2 committed no progress of k, then SIGKILL of stage 2. */
  private void stage2Restart(KafkaCompletionReader completion) throws Exception {
    ObjectNode inj = newInjection(kind.injectionIds.get(0), false, List.of("stage2"));
    inj.put("hold", heldStage2);
    inj.putNull("declaration");
    List<CompletionReader.Writer> s1w = new ArrayList<>();
    for (int p : new TreeSet<>(routing.stations().values())) for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES, ReplayTopics.DISPOSITIONS1)) s1w.add(new CompletionReader.Writer(t, p));
    s1w.add(new CompletionReader.Writer(ReplayTopics.DISPOSITIONS1, CompletionReader.REJECTS));
    completion.await(k, s1w, FlinkRun.CUT_TIMEOUT);
    inj.put("stage1Committed", "every stage-1 writer committed the progress closing cut " + k);
    int launchBefore = s2.launchId();
    s2.kill(true, true, "injection " + kind.injectionIds.get(0) + " (after stage-1 output of cut " + k + " committed, before stage-2 processing)");
    try (KafkaProgressLog log = new KafkaProgressLog(bootstrap, generation)) {
      ObjectNode at = Json.object();
      for (Map.Entry<String, TopicPartition> e : stage2Partitions.entrySet()) {
        List<KafkaProgressLog.Entry> p = log.progress(e.getValue());
        Json.putSafe(at.putObject(e.getKey()), "lastProgressCutId", p.isEmpty() ? 0 : p.get(p.size() - 1).value().path("cutId").asLong());
      }
      inj.set("stage2AtKill", at);
      for (Map.Entry<String, JsonNode> e : (Iterable<Map.Entry<String, JsonNode>>) at::fields)
        if (e.getValue().path("lastProgressCutId").asLong() >= k) throw invalid("22(ii): stage 2 had committed progress " + e.getValue().path("lastProgressCutId") + " on " + e.getKey() + " at the kill");
    }
    relaunch(s2, 2, launchBefore, inj.putObject("launches").putObject("stage2"));
    awaitAllRunning(inj);
    inj.putArray("expected");
    inj.putArray("attributed");
  }

  /** A relaunch of {@code c} restoring its latest completed checkpoint, recorded (the harness's records of the restart, spec 416). */
  private void relaunch(FlinkCluster c, int stage, int launchBefore, ObjectNode rec) {
    Long cp = c.latestCompletedCheckpoint();
    if (cp == null) throw invalid(c.name + ": no completed checkpoint to restore (a restart restores the latest completed checkpoint, plan D7)");
    int launch = launchBefore + 1;
    Path spec = specs.forLaunch(stage, launch);
    Json.putSafe(rec, "launchBefore", launchBefore);
    Json.putSafe(rec, "launch", launch);
    Json.putSafe(rec, "restoredCheckpointId", cp);
    // the restore evidence every reinitialized writer of this launch must report (no restart strategy: attempt 0)
    rec.put("restore", new RestoreEvidence(launch, true, cp, 0).encode());
    c.relaunch(spec, cp);
    rec.set("record", c.currentLaunch());
  }

  private void awaitAllRunning(ObjectNode inj) {
    if (inj.path("declaration").isObject()) Json.putSafe((ObjectNode) inj.get("declaration"), "controlEndAfter", rc.read().endOffset());
    s1.awaitRunning(RUNNING);
    s2.awaitRunning(RUNNING);
    inj.put("running", "every job and all its tasks RUNNING");
  }

  /** The expected reinitialized set of a stage-1 restart from the harness's records (spec 416): the stage-1 job's launch changed, so
   *  every stage-1 progress writer; the attributed set equals it until a later chained injection is known ({@link #finish}). */
  private void expectedSets(ObjectNode inj) {
    ArrayNode e = inj.putArray("expected"), a = inj.putArray("attributed");
    ObjectNode r = inj.putObject("restore");
    // per writer, engine-neutral: the restore evidence each reinitialized writer must report (for Flink every stage-1 writer of the
    // relaunched job reports its launch's; an engine that moves only some tasks lists only those)
    String restore = inj.path("launches").path("stage1").path("restore").asText(null);
    for (String w : stage1Writers()) { e.add(w); a.add(w); r.put(w, restore); }
    inj.put("expectedFrom", "the stage-1 job's durable launch identity before and after the injection (" + inj.path("launches").path("stage1").path("launchBefore")
        + " -> " + inj.path("launches").path("stage1").path("launch") + ")");
  }

  /** Every stage-1 writer partition's high watermark and every representative partition's last committed progress. */
  private ObjectNode snapshot(KafkaProgressLog log) {
    ObjectNode n = Json.object();
    ObjectNode hw = n.putObject("highWatermark");
    stage1Partitions.forEach((k2, tp) -> Json.putSafe(hw, k2, log.highWatermark(tp)));
    ObjectNode last = n.putObject("lastProgressCutId");
    representative.forEach((w, tp) -> {
      List<KafkaProgressLog.Entry> p = log.progress(tp);
      Json.putSafe(last, w, p.isEmpty() ? 0 : p.get(p.size() - 1).value().path("cutId").asLong());
    });
    return n;
  }

  // --- after the run ---------------------------------------------------------------------------------------------------------------

  /** After every cut completed: the attributed sets of chained injections, every
   *  committed stage-1 progress with its offset, and the writers whose initEvidence points to each declaration. */
  ObjectNode finish() throws Exception {
    ObjectNode m = Json.object();
    // the attributed set of an injection followed by a chained one: minus the writers the later one reinitialized before they wrote
    // any progress after the first (spec 416), from the records the harness took at the later kill, never from initEvidence
    for (int i = 0; i + 1 < injections.size(); i++) {
      ObjectNode a = (ObjectNode) injections.get(i), b = (ObjectNode) injections.get(i + 1);
      if (!a.path("stage1Restart").asBoolean() || !b.path("stage1Restart").asBoolean()) continue;
      ArrayNode attributed = a.putArray("attributed");
      for (JsonNode w : a.path("expected")) {
        long last = b.path("atKill").path("lastProgressCutId").path(w.asText()).asLong(-1);
        boolean reinitializedFirst = containsText(b.path("expected"), w.asText()) && last <= a.path("lastWrittenCutId").asLong();
        if (!reinitializedFirst) attributed.add(w);
      }
      a.put("attributedFrom", "the writers' last committed progress at the chained injection " + b.path("injectionId").asText() + " (none after cut "
          + a.path("lastWrittenCutId") + ": reinitialized again before writing any progress)");
    }
    ObjectNode progress = m.putObject("progress");
    try (KafkaProgressLog log = new KafkaProgressLog(bootstrap, generation)) {
      for (Map.Entry<String, TopicPartition> e : stage1Partitions.entrySet()) {
        ArrayNode a = progress.putArray(e.getKey());
        for (KafkaProgressLog.Entry x : log.progress(e.getValue())) {
          ObjectNode o = Json.putSafe(Json.object(), "offset", x.offset());
          Json.putSafe(o, "cutId", x.value().path("cutId").asLong());
          o.set("initEvidence", x.value().has("initEvidence") ? x.value().get("initEvidence") : null);
          a.add(o);
        }
      }
    }
    for (JsonNode inj : injections) {
      if (!inj.path("stage1Restart").asBoolean()) { ((ObjectNode) inj).putArray("initEvidenceWriters"); continue; }
      long decl = inj.path("declaration").path("offset").asLong(-1);
      TreeSet<String> pointing = new TreeSet<>();
      for (String w : stage1Writers()) {
        String part = w.equals(Restarts.REJECTS) ? ReplayTopics.DISPOSITIONS1 + "/" + CompletionReader.REJECTS : Stage1.WORK_RECORDS + "/" + routing.partitionOf(w);
        for (JsonNode x : progress.path(part)) if (x.path("initEvidence").path("declarationOffset").asLong(-2) == decl) pointing.add(w);
      }
      ArrayNode a = ((ObjectNode) inj).putArray("initEvidenceWriters");
      pointing.forEach(a::add);
    }
    m.set("injections", injections);
    return m;
  }

  private static boolean containsText(JsonNode a, String s) { for (JsonNode x : a) if (x.asText().equals(s)) return true; return false; }

  private static void sleep(long ms) {
    try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
  }
}
