package yggdrasil.forseti.engine.flink.gate;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.cli.replay.EngineContract;
import yggdrasil.forseti.cli.replay.ReplayEngine;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.engine.flink.job.JobSettings;
import yggdrasil.forseti.engine.flink.job.PerRecordWatermarks;
import yggdrasil.forseti.replay.kafka.DockerBroker;
import yggdrasil.forseti.replay.kafka.KafkaConfig;
import yggdrasil.forseti.replay.kafka.KafkaTopicManager;
import yggdrasil.forseti.engine.flink.job.RestoreEvidence;

/** The Flink engine of the replay gate ({@link ReplayEngine}, plan D1, D7): each run on its own topics of one broker (the pinned Docker
 *  image the engine starts and removes, or a running one), each job in its own application-mode cluster ({@link FlinkRun}), and the
 *  Flink-specific per-run assertions read from the run's evidence:
 *  <ul><li>{@code FL-KEYGROUP-MAP} (spec 715): the recorded key-group and subtask assignment of both stations operators (from their
 *  init records: the subtask's key-group range and the stations it owns) gives every station exactly one owner subtask, at the key group
 *  Flink assigns it under the pinned max parallelism, inside that subtask's range, so every station output partition has exactly one
 *  owner; and the {@code routingDigest} of the run record, the cut log and the recording are one;</li>
 *  <li>{@code FL-WATERMARK-PER-RECORD} (716): the stage-1 job emits watermarks per record (the delegating generator), with the
 *  automatic watermark interval 0 and idleness off, as the job settings record them from the cluster;</li>
 *  <li>{@code FL-ONE-SPLIT} (717): every source subtask of both jobs received records of exactly one partition and every subscribed
 *  partition reached exactly one subtask ({@code SplitProbe}), at a source parallelism equal to the subscribed partitions;</li>
 *  <li>{@code FL-JOB-SETTINGS} (588-590): the observed job settings are the engine pins: {@code EXACTLY_ONCE} checkpoints every
 *  1000 ms, one at a time, RocksDB with incremental checkpoints, no restart strategy, keyed parallelism 6 and max parallelism 128 as the
 *  operators saw them, transactional {@code EXACTLY_ONCE} sinks;</li>
 *  <li>{@code FL-TIMER-INVARIANT} (reviewer decision 4): the native timers are reconciled with the authoritative deadline index at
 *  every cut close: every distinct deadline of the index has exactly one Flink registration or a held mark, Flink's own timer queue holds
 *  exactly the registered ones, every held mark is at or below the watermark, and every deadline registered before the close and at or
 *  below {@code W1} is held; on a run whose stations had deadlines, timers were registered, fired and drained as held (no vacuous pass),
 *  and a horizon run (a synthetic recording) had deadlines and fired timers (reviewer decision 6 of PR 5).</li></ul>
 *  A restart run has several incarnations of each restarted job (one per launch, reviewer decision 2 of PR 5): the key-group map holds in
 *  every one and is the same in all, every incarnation's source subtasks own one split each (the first and the last launch on every
 *  subscribed partition), and the timer evidence of every incarnation is summed.
 *  The engine-independent init and cut evidence are the gate's {@code R-INIT-EVIDENCE} and {@code R-CUT-EVIDENCE}.
 */
final class FlinkEngine implements ReplayEngine {
  static final List<String> IDS = List.of("FL-KEYGROUP-MAP", "FL-WATERMARK-PER-RECORD", "FL-ONE-SPLIT", "FL-JOB-SETTINGS", "FL-TIMER-INVARIANT");

  private final Path root;
  private final DockerBroker broker;
  private final String bootstrap;

  private FlinkEngine(Path root, DockerBroker broker, String bootstrap) { this.root = root; this.broker = broker; this.bootstrap = bootstrap; }

  static FlinkEngine open(Path root, String kafka, PrintStream out) {
    if (kafka.equals("docker")) {
      DockerBroker b = DockerBroker.start(root);
      out.println("forseti gate --engine flink: broker " + b.bootstrap() + " (" + b.image() + ", container " + b.container() + ")");
      return new FlinkEngine(root, b, b.bootstrap());
    }
    out.println("forseti gate --engine flink: broker " + kafka);
    return new FlinkEngine(root, null, kafka);
  }

  @Override public String name() { return JobSettings.ENGINE; }

  @Override public String w1Form() { return "min-over-partitions"; }

  @Override public ControlStore controlStore(String name) throws Exception {
    String prefix = "flink-store-" + name.replaceAll("[^a-z0-9.-]", "-") + "-" + java.util.UUID.randomUUID().toString().substring(0, 8) + ".";
    List<ReplayTopics.Spec> specs = new ArrayList<>();
    for (ReplayTopics.Spec s : ReplayTopics.plan(prefix))
      if (s.base().equals(ReplayTopics.GENERATIONS) || s.base().equals(ReplayTopics.RECOVERY_CONTROL)) specs.add(s);
    try (KafkaTopicManager admin = new KafkaTopicManager(bootstrap, "forseti-harness-admin")) { admin.create(specs); }
    String cluster;
    try (org.apache.kafka.clients.admin.Admin a = org.apache.kafka.clients.admin.Admin.create(KafkaConfig.admin(bootstrap, "forseti-harness-cluster-id"))) {
      cluster = "kafka:" + a.describeCluster().clusterId().get(30, java.util.concurrent.TimeUnit.SECONDS);
    }
    return new ControlStore(name, cluster, ReplayTopics.nameOf(specs, ReplayTopics.GENERATIONS), ReplayTopics.nameOf(specs, ReplayTopics.RECOVERY_CONTROL));
  }

  @Override public Registered register(ControlStore store, Path state, Path trustedKeys, JsonNode rule, Registration registration, Path recording) throws Exception {
    return FlinkRun.register(bootstrap, store, state, trustedKeys, rule, registration, recording);
  }

  @Override public String firstStartRestore() { return RestoreEvidence.firstStart().encode(); }

  /** From the split probes of both jobs: the partitions each source consumed (the stage-1 raw topics and {@code quality.incidents}). */
  @Override public Map<String, List<Integer>> observedSubscription(Run run) {
    Map<String, List<Integer>> m = new LinkedHashMap<>();
    for (String t : EngineContract.STAGE1_TOPICS_AND_INCIDENTS) {
      String source = (t.equals("quality.incidents") ? "stage2-source-" : "stage1-source-") + t;
      TreeSet<Integer> ps = new TreeSet<>();
      run.evidence().path("flink").path("jobs").fields().forEachRemaining(e -> {
        if (e.getKey().startsWith("split-" + source + "-") && source.equals(e.getValue().path("source").asText())) e.getValue().path("partitions").forEach(x -> ps.add(x.intValue()));
      });
      m.put(t, new ArrayList<>(ps));
    }
    return m;
  }

  @Override public Run run(Request r) throws Exception {
    return FlinkRun.run(bootstrap, root, r, FlinkCluster.Memory.forStage(1, r.settings()), FlinkCluster.Memory.forStage(2, r.settings()));
  }

  @Override public void redump(Run base) { FlinkRun.redump((FlinkRun.Handle) base.handle(), base.out()); }

  @Override public void close() { if (broker != null) broker.close(); }

  @Override public Map<String, List<String>> findings(Run run, Path recording) throws Exception {
    Map<String, List<String>> f = new TreeMap<>();
    for (String id : IDS) f.put(id, new ArrayList<>());
    JsonNode ev = run.evidence(), jobs = ev.path("flink").path("jobs");
    JsonNode runJson = Json.read(run.out().resolve("run.json")), cutLog = Json.read(run.out().resolve("cuts.json")), settings = runJson.path("jobSettings");
    RoutingManifest routing = Recording.open(recording).routing();
    List<String> stations = new ArrayList<>(new TreeMap<>(routing.stations()).keySet());

    // 715: the key-group and subtask map of both station operators, and the routing digest: in every incarnation of the job (each
    // launch its own init records, reviewer decision 2 of PR 5), the same map
    List<String> km = f.get("FL-KEYGROUP-MAP");
    for (String op : List.of("stage1-stations", "stage2-stations")) {
     Map<Integer, List<JsonNode>> byLaunch = new TreeMap<>();
     for (Iterator<Map.Entry<String, JsonNode>> it = jobs.fields(); it.hasNext();) {
       Map.Entry<String, JsonNode> e = it.next();
       if (e.getKey().startsWith(op + "-")) byLaunch.computeIfAbsent(e.getValue().path("launch").asInt(0), x -> new ArrayList<>()).add(e.getValue());
     }
     if (byLaunch.isEmpty()) byLaunch.put(0, List.of());
     Map<String, Integer> firstOwner = null;
     for (Map.Entry<Integer, List<JsonNode>> inc : byLaunch.entrySet()) {
      String where = op + (byLaunch.size() > 1 ? " (launch " + inc.getKey() + ")" : "");
      Map<String, Integer> owner = new TreeMap<>();
      int subtasks = 0;
      for (JsonNode n : inc.getValue()) {
        subtasks++;
        int sub = n.path("subtask").intValue(), max = n.path("maxParallelism").intValue(), par = n.path("parallelism").intValue();
        KeyGroupRange want = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(max, par, sub);
        JsonNode range = n.path("keyGroupRange");
        if (range.path(0).intValue() != want.getStartKeyGroup() || range.path(1).intValue() != want.getEndKeyGroup())
          km.add(op + " subtask " + sub + ": key groups " + range + ", Flink's " + want);
        for (JsonNode s : n.path("stations")) {
          String st = s.asText();
          Integer before = owner.put(st, sub);
          if (before != null) km.add(op + ": station " + st + " owned by subtasks " + before + " and " + sub);
          int kg = n.path("stationKeyGroups").path(st).intValue(), flink = KeyGroupRangeAssignment.assignToKeyGroup(st, max);
          if (kg != flink) km.add(op + ": station " + st + " key group " + kg + ", Flink's " + flink);
          if (!want.contains(kg)) km.add(op + ": station " + st + " key group " + kg + " outside subtask " + sub + "'s " + want);
          if (max != JobSettings.MAX_PARALLELISM) km.add(op + " subtask " + sub + ": max parallelism " + max);
        }
      }
      if (subtasks != JobSettings.KEYED_PARALLELISM) km.add(where + ": " + subtasks + " init records, " + JobSettings.KEYED_PARALLELISM + " subtasks expected");
      if (!owner.keySet().equals(new TreeSet<>(stations))) km.add(where + ": owned stations " + owner.keySet() + ", the routing's " + stations);
      // each station output partition: exactly one owner
      Map<Integer, Set<Integer>> partitionOwners = new TreeMap<>();
      owner.forEach((st, sub) -> partitionOwners.computeIfAbsent(routing.partitionOf(st), x -> new TreeSet<>()).add(sub));
      partitionOwners.forEach((p, subs) -> { if (subs.size() != 1) km.add(where + ": output partition " + p + " owned by subtasks " + subs); });
      if (firstOwner == null) firstOwner = owner;
      else if (!firstOwner.equals(owner)) km.add(where + ": station owners " + owner + " differ from the first launch's " + firstOwner);
     }
    }
    String rd = routing.routingDigest();
    if (!rd.equals(runJson.path("pins").path("job").path("routingDigest").asText(null))) km.add("run.json routingDigest " + runJson.path("pins").path("job").path("routingDigest") + ", the recording's " + rd);
    if (!rd.equals(cutLog.path("routingDigest").asText(null))) km.add("cuts.json routingDigest " + cutLog.path("routingDigest") + ", the recording's " + rd);

    // 716: per-record watermark emission
    List<String> wm = f.get("FL-WATERMARK-PER-RECORD");
    JsonNode s1 = settings.path("stage1");
    if (!PerRecordWatermarks.MODE.equals(s1.path("watermarkEmission").asText(null))) wm.add("stage-1 watermark emission " + s1.path("watermarkEmission"));
    if (!"0 ms".equals(s1.path("autoWatermarkInterval").asText(null))) wm.add("stage-1 automatic watermark interval " + s1.path("autoWatermarkInterval") + " (observed)");
    if (!"off".equals(s1.path("idleness").asText(null))) wm.add("stage-1 idleness " + s1.path("idleness"));
    if (!"off".equals(runJson.path("pins").path("job").path("idleness").asText(null))) wm.add("pins.job.idleness " + runJson.path("pins").path("job").path("idleness"));

    // 717: one split per source subtask
    List<String> sp = f.get("FL-ONE-SPLIT");
    Map<String, List<Integer>> want = new LinkedHashMap<>();
    runJson.path("subscribed").fields().forEachRemaining(e -> {
      List<Integer> l = new ArrayList<>();
      e.getValue().forEach(x -> l.add(x.intValue()));
      if (l.isEmpty()) return;
      want.put((e.getKey().equals("quality.incidents") ? "stage2-source-" : "stage1-source-") + e.getKey(), l);
    });
    List<Integer> stationPartitions = new ArrayList<>(new TreeSet<>(routing.stations().values()));
    for (String t : List.of("forseti.work-records", "forseti.samples")) want.put("stage2-source-" + t, stationPartitions);
    for (Map.Entry<String, List<Integer>> w : want.entrySet()) {
      // per incarnation (launch) of the job: every source subtask one split; the first and the last launch received records on every
      // subscribed partition (a launch killed again before batch k+1 may have received nothing, injection 22(iv))
      Map<Integer, Map<Integer, Set<Integer>>> launches = new TreeMap<>();
      Map<Integer, Set<Long>> parallelisms = new TreeMap<>();
      for (Iterator<Map.Entry<String, JsonNode>> it = jobs.fields(); it.hasNext();) {
        Map.Entry<String, JsonNode> e = it.next();
        if (!e.getKey().startsWith("split-" + w.getKey() + "-")) continue;
        JsonNode n = e.getValue();
        if (!w.getKey().equals(n.path("source").asText())) continue;
        int launch = n.path("launch").asInt(0);
        parallelisms.computeIfAbsent(launch, x -> new TreeSet<>()).add(n.path("parallelism").asLong());
        Set<Integer> ps = launches.computeIfAbsent(launch, x -> new TreeMap<>()).computeIfAbsent(n.path("subtask").intValue(), x -> new TreeSet<>());
        n.path("partitions").forEach(x -> ps.add(x.intValue()));
      }
      if (!launches.containsKey(0)) launches.put(0, new TreeMap<>());
      int lastLaunch = Collections.max(launches.keySet());
      for (Map.Entry<Integer, Map<Integer, Set<Integer>>> l : launches.entrySet()) {
        String where = w.getKey() + (launches.size() > 1 ? " (launch " + l.getKey() + ")" : "");
        Map<Integer, Set<Integer>> bySubtask = l.getValue();
        Set<Long> parallelism = parallelisms.getOrDefault(l.getKey(), Set.of());
        boolean full = l.getKey() == 0 || l.getKey() == lastLaunch;
        if (full && !parallelism.equals(Set.of((long) w.getValue().size())) || !full && !parallelism.isEmpty() && !parallelism.equals(Set.of((long) w.getValue().size())))
          sp.add(where + ": source parallelism " + parallelism + ", subscribed partitions " + w.getValue());
        if (full && bySubtask.size() != w.getValue().size()) sp.add(where + ": " + bySubtask.size() + " subtasks received records, " + w.getValue().size() + " expected " + bySubtask);
        Map<Integer, Integer> ownerOf = new TreeMap<>();
        bySubtask.forEach((sub, ps) -> {
          if (ps.size() != 1) sp.add(where + " subtask " + sub + " owns " + ps.size() + " splits " + ps);
          for (int p : ps) { Integer o = ownerOf.put(p, sub); if (o != null) sp.add(where + ": partition " + p + " on subtasks " + o + " and " + sub); }
        });
        if (full && !ownerOf.keySet().equals(new TreeSet<>(w.getValue()))) sp.add(where + ": partitions " + ownerOf.keySet() + ", subscribed " + w.getValue());
        if (!full && !new TreeSet<>(w.getValue()).containsAll(ownerOf.keySet())) sp.add(where + ": partitions " + ownerOf.keySet() + " outside " + w.getValue());
      }
    }

    // 588-590: the observed job settings are the engine pins
    List<String> js = f.get("FL-JOB-SETTINGS");
    for (String stage : List.of("stage1", "stage2")) {
      JsonNode s = settings.path(stage), cp = s.path("checkpointing");
      if (!"EXACTLY_ONCE".equalsIgnoreCase(cp.path("mode").asText(""))) js.add(stage + " checkpoint mode " + cp.path("mode"));
      if (cp.path("intervalMs").asLong(-1) != 1000) js.add(stage + " checkpoint interval " + cp.path("intervalMs"));
      if (cp.path("maxConcurrent").asLong(-1) != 1) js.add(stage + " max concurrent checkpoints " + cp.path("maxConcurrent"));
      if (!"true".equals(cp.path("incremental").asText(null))) js.add(stage + " incremental checkpoints " + cp.path("incremental"));
      if (!"rocksdb".equalsIgnoreCase(cp.path("stateBackendType").asText(""))) js.add(stage + " state backend " + cp.path("stateBackendType"));
      if (!"none".equals(s.path("restartStrategy").asText(null))) js.add(stage + " restart strategy " + s.path("restartStrategy"));
      if (s.path("keyedParallelism").asLong(-1) != JobSettings.KEYED_PARALLELISM) js.add(stage + " keyed parallelism " + s.path("keyedParallelism"));
      if (s.path("maxParallelism").asLong(-1) != JobSettings.MAX_PARALLELISM) js.add(stage + " max parallelism " + s.path("maxParallelism"));
      s.path("sinks").fields().forEachRemaining(e -> {
        if (!"EXACTLY_ONCE".equals(e.getValue().path("deliveryGuarantee").asText(null))) js.add(stage + " sink " + e.getKey() + " " + e.getValue().path("deliveryGuarantee"));
      });
      if (s.path("sinks").size() != 2) js.add(stage + ": " + s.path("sinks").size() + " sinks recorded, 2 expected");
      if (!s.path("sourceParallelism").isObject() || s.path("sourceParallelism").isEmpty()) js.add(stage + ": no source parallelism recorded");
      if (!s.path("readers").path("generations").isObject()) js.add(stage + ": the job's forseti.generations reader is not recorded");
      if (stage.equals("stage1") && !s.path("readers").path("recoveryControl").isObject()) js.add(stage + ": the writers' forseti.recovery-control reader is not recorded");
      s.path("sourceParallelism").fields().forEachRemaining(e -> {
        List<Integer> w = want.get(stage + "-source-" + e.getKey());
        if (w == null || e.getValue().asLong(-1) != w.size()) js.add(stage + " source " + e.getKey() + " parallelism " + e.getValue() + " (observed), subscribed " + w);
      });
      if (!engineMatches(s)) js.add(stage + " engine " + s.path("engine") + " " + s.path("engineVersion"));
    }

    // reviewer decision 4: native registrations and held marks reconciled with the deadline index, with coverage
    List<String> ti = f.get("FL-TIMER-INVARIANT");
    long closes = 0, deadlines = 0, registrations = 0, fired = 0, drained = 0;
    for (Iterator<Map.Entry<String, JsonNode>> it = jobs.fields(); it.hasNext();) {
      Map.Entry<String, JsonNode> e = it.next();
      if (!e.getKey().startsWith("stage1-stations-")) continue;
      JsonNode n = e.getValue();
      if (!n.has("timerRegistrations")) { ti.add(e.getKey() + ": no timer evidence"); continue; }
      closes += n.path("timerInvariantCloses").asLong();
      deadlines += n.path("deadlinesSeen").asLong();
      registrations += n.path("timerRegistrations").asLong();
      fired += n.path("firedTimers").asLong();
      drained += n.path("heldDeadlinesDrained").asLong();
      long v = n.path("timerInvariantViolations").asLong(-1);
      if (v != 0) ti.add(e.getKey() + ": " + v + " timer invariant violations " + n.path("timerInvariantExamples"));
    }
    if (closes == 0) ti.add("no cut close checked");
    // reviewer decision 6 of PR 5: a horizon run (a synthetic recording) has deadlines, and its timers must have fired
    boolean horizon = Recording.open(recording).manifest().path("synthetic").asBoolean(false);
    if (horizon && deadlines == 0) ti.add("a horizon run with no deadline in any index");
    if (horizon && fired == 0) ti.add("a horizon run on which no Flink timer fired (" + registrations + " registered)");
    if (deadlines > 0) {
      if (registrations == 0) ti.add(deadlines + " deadlines in the index, no Flink timer registered");
      if (fired == 0) ti.add(registrations + " timers registered, none fired");
      if (drained == 0) ti.add(fired + " timers fired, no held deadline drained at a close");
    }
    return f;
  }

  private static boolean engineMatches(JsonNode s) {
    return JobSettings.ENGINE.equals(s.path("engine").asText(null)) && JobSettings.ENGINE_VERSION.equals(s.path("engineVersion").asText(null));
  }

  static boolean exists(Path p) { return Files.exists(p); }
}
