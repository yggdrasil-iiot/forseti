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
import yggdrasil.forseti.cli.replay.ReplayEngine;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.engine.flink.job.JobSettings;
import yggdrasil.forseti.engine.flink.job.PerRecordWatermarks;
import yggdrasil.forseti.replay.kafka.DockerBroker;

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
 *  <li>{@code FL-TIMER-INVARIANT} (reviewer decision 4): at every cut close, evaluated at the operator watermark W1, every deadline
 *  registered before it and at or below {@code W1} was held (its timer had fired);</li>
 *  <li>{@code FL-INIT-EVIDENCE}: the first progress of every stage-1 progress writer carries the run's {@code initEvidence} (its
 *  declaration, the control end offset H and Flink's restore evidence of a first start) and no other progress does;</li>
 *  <li>{@code FL-CUT-EVIDENCE}: every station's entry of every cut in {@code cuts.json} carries the {@code onlineVisits} its operator
 *  recorded at that close.</li></ul> */
final class FlinkEngine implements ReplayEngine {
  static final List<String> IDS = List.of("FL-KEYGROUP-MAP", "FL-WATERMARK-PER-RECORD", "FL-ONE-SPLIT", "FL-JOB-SETTINGS", "FL-TIMER-INVARIANT", "FL-INIT-EVIDENCE",
      "FL-CUT-EVIDENCE");

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

    // 715: the key-group and subtask map of both station operators, and the routing digest
    List<String> km = f.get("FL-KEYGROUP-MAP");
    for (String op : List.of("stage1-stations", "stage2-stations")) {
      Map<String, Integer> owner = new TreeMap<>();
      int subtasks = 0;
      for (Iterator<Map.Entry<String, JsonNode>> it = jobs.fields(); it.hasNext();) {
        Map.Entry<String, JsonNode> e = it.next();
        if (!e.getKey().startsWith(op + "-")) continue;
        JsonNode n = e.getValue();
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
      if (subtasks != JobSettings.KEYED_PARALLELISM) km.add(op + ": " + subtasks + " init records, " + JobSettings.KEYED_PARALLELISM + " subtasks expected");
      if (!owner.keySet().equals(new TreeSet<>(stations))) km.add(op + ": owned stations " + owner.keySet() + ", the routing's " + stations);
      // each station output partition: exactly one owner
      Map<Integer, Set<Integer>> partitionOwners = new TreeMap<>();
      owner.forEach((st, sub) -> partitionOwners.computeIfAbsent(routing.partitionOf(st), x -> new TreeSet<>()).add(sub));
      partitionOwners.forEach((p, subs) -> { if (subs.size() != 1) km.add(op + ": output partition " + p + " owned by subtasks " + subs); });
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
      Map<Integer, Set<Integer>> bySubtask = new TreeMap<>();
      Set<Long> parallelism = new TreeSet<>();
      for (Iterator<Map.Entry<String, JsonNode>> it = jobs.fields(); it.hasNext();) {
        Map.Entry<String, JsonNode> e = it.next();
        if (!e.getKey().startsWith("split-" + w.getKey() + "-")) continue;
        JsonNode n = e.getValue();
        if (!w.getKey().equals(n.path("source").asText())) continue;
        parallelism.add(n.path("parallelism").asLong());
        Set<Integer> ps = bySubtask.computeIfAbsent(n.path("subtask").intValue(), x -> new TreeSet<>());
        n.path("partitions").forEach(x -> ps.add(x.intValue()));
      }
      if (!parallelism.equals(Set.of((long) w.getValue().size()))) sp.add(w.getKey() + ": source parallelism " + parallelism + ", subscribed partitions " + w.getValue());
      if (bySubtask.size() != w.getValue().size()) sp.add(w.getKey() + ": " + bySubtask.size() + " subtasks received records, " + w.getValue().size() + " expected " + bySubtask);
      Map<Integer, Integer> ownerOf = new TreeMap<>();
      bySubtask.forEach((sub, ps) -> {
        if (ps.size() != 1) sp.add(w.getKey() + " subtask " + sub + " owns " + ps.size() + " splits " + ps);
        for (int p : ps) { Integer o = ownerOf.put(p, sub); if (o != null) sp.add(w.getKey() + ": partition " + p + " on subtasks " + o + " and " + sub); }
      });
      if (!ownerOf.keySet().equals(new TreeSet<>(w.getValue()))) sp.add(w.getKey() + ": partitions " + ownerOf.keySet() + ", subscribed " + w.getValue());
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
      s.path("sourceParallelism").fields().forEachRemaining(e -> {
        List<Integer> w = want.get(stage + "-source-" + e.getKey());
        if (w == null || e.getValue().asLong(-1) != w.size()) js.add(stage + " source " + e.getKey() + " parallelism " + e.getValue() + " (observed), subscribed " + w);
      });
      if (!engineMatches(s)) js.add(stage + " engine " + s.path("engine") + " " + s.path("engineVersion"));
    }

    // reviewer decision 4: the timer invariant at watermark-driven closes
    List<String> ti = f.get("FL-TIMER-INVARIANT");
    long closes = 0;
    for (Iterator<Map.Entry<String, JsonNode>> it = jobs.fields(); it.hasNext();) {
      Map.Entry<String, JsonNode> e = it.next();
      if (!e.getKey().startsWith("stage1-stations-")) continue;
      closes += e.getValue().path("timerInvariantCloses").asLong();
      long v = e.getValue().path("timerInvariantViolations").asLong(-1);
      if (v != 0) ti.add(e.getKey() + ": " + v + " deadlines at or below W1 not held at a cut close " + e.getValue().path("timerInvariantExamples"));
    }
    if (closes == 0) ti.add("no cut close checked");

    // initEvidence
    JsonNode prep = ev.path("flink").path("preparation");
    f.get("FL-INIT-EVIDENCE").addAll(FlinkGate.initEvidence(run.out(), prep.path("declarationOffset").asLong(-1), prep.path("controlEnd").asLong(-1)));

    // onlineVisits in the cut log
    List<String> ce = f.get("FL-CUT-EVIDENCE");
    for (JsonNode c : cutLog.path("cuts"))
      c.path("perStation").fields().forEachRemaining(e -> {
        if (!e.getValue().path("onlineVisits").isIntegralNumber() && ce.size() < 20) ce.add("cut " + c.path("cutId") + " " + e.getKey() + ": no onlineVisits");
      });
    return f;
  }

  private static boolean engineMatches(JsonNode s) {
    return JobSettings.ENGINE.equals(s.path("engine").asText(null)) && JobSettings.ENGINE_VERSION.equals(s.path("engineVersion").asText(null));
  }

  static boolean exists(Path p) { return Files.exists(p); }
}
