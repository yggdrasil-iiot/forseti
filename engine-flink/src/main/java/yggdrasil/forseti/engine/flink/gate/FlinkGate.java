package yggdrasil.forseti.engine.flink.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.gate.Assertions;
import yggdrasil.forseti.cli.gate.EngineCheck;
import yggdrasil.forseti.cli.gate.Reconciliation;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.Stage2;
import yggdrasil.forseti.engine.flink.job.RestoreEvidence;
import yggdrasil.forseti.replay.kafka.DockerBroker;

/**
 * {@code forseti gate --engine flink} as step 6 PR 4 delivers it: the smoke shard, that is the smoke engine recording on both governed
 * placements ({@code engine-fixtures/scenarios/dedicated/smoke.json} on site-071 and {@code shared/smoke.json} on site-019), each run on
 * Flink over Kafka ({@link FlinkRun}) and compared with the unchanged oracle at every cut, against the assertion manifest
 * {@code gate/flink-assertions.json}. Like every gate it prints one line per assertion, then {@code assertions run N / expected M}, and
 * exits 0 only when every manifest id ran and held (1 otherwise, 2 for a usage error). The broker is the pinned Docker image
 * ({@code --kafka docker}, the default, started and removed by the gate) or a running one ({@code --kafka <host:port>}). The report is
 * {@code <out>/report.json}; each run's files are under {@code <out>/<label>/}.
 *
 * <p>Flags: {@code --out <dir>} (build/flink), {@code --python <interpreter>} (python), {@code --kafka docker|<host:port>},
 * {@code --registry <dir>} (fixtures/live/registry), {@code --trusted-keys <file>} (fixtures/live/trusted-keys.jsonl),
 * {@code --placement dedicated|shared} (one placement only: the other's assertions do not run, so the gate exits 1). The repository root
 * is the working directory.
 */
public final class FlinkGate {
  private FlinkGate() {}

  public static final String MANIFEST = "gate/flink-assertions.json";
  /** The smoke shard: label, placement and recording. */
  static final List<String[]> SMOKE = List.of(new String[] {"smoke-dedicated", "DEDICATED", "engine-fixtures/scenarios/dedicated/smoke.json"},
      new String[] {"smoke-shared", "SHARED", "engine-fixtures/scenarios/shared/smoke.json"});
  static final Set<String> FLAGS = Set.of("engine", "out", "python", "kafka", "registry", "trusted-keys", "placement");

  public static int main(String[] args, PrintStream out, PrintStream err) {
    Map<String, String> f = new LinkedHashMap<>();
    for (int i = 1; i < args.length; i++) {
      String a = args[i];
      if (!a.startsWith("--") || i + 1 >= args.length || !FLAGS.contains(a.substring(2))) {
        err.println("forseti gate --engine flink: unknown or incomplete flag " + a + " (flags: --out, --python, --kafka, --registry, --trusted-keys, --placement)");
        return 2;
      }
      if (f.put(a.substring(2), args[++i]) != null) { err.println("flag " + a + " given twice"); return 2; }
    }
    if (!"flink".equals(f.get("engine"))) { err.println("--engine flink"); return 2; }
    // the harness's own Kafka clients log through the engine path's slf4j-simple: warnings only
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", System.getProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn"));
    Path root = Path.of("").toAbsolutePath();
    Path outDir = Path.of(f.getOrDefault("out", "build/flink")).toAbsolutePath();
    Path registry = Path.of(f.getOrDefault("registry", "fixtures/live/registry")).toAbsolutePath();
    Path keys = Path.of(f.getOrDefault("trusted-keys", "fixtures/live/trusted-keys.jsonl")).toAbsolutePath();
    String python = f.getOrDefault("python", "python"), kafka = f.getOrDefault("kafka", "docker");
    List<String> ids = new ArrayList<>();
    try {
      for (JsonNode a : Json.read(root.resolve(MANIFEST)).path("assertions")) ids.add(Json.text(a, "id"));
    } catch (RuntimeException e) { err.println("no assertion manifest " + root.resolve(MANIFEST) + ": " + e.getMessage()); return 2; }
    Assertions as = new Assertions(ids, out);
    ObjectNode report = Json.object().put("engine", "flink").put("command", "forseti " + String.join(" ", args)).put("manifest", MANIFEST);
    ArrayNode runs = report.putArray("runs");
    long t0 = System.nanoTime();
    DockerBroker broker = null;
    try {
      FlinkRun.delete(outDir);
      Files.createDirectories(outDir);
      String bootstrap;
      if (kafka.equals("docker")) {
        broker = DockerBroker.start(root);
        bootstrap = broker.bootstrap();
        report.put("kafka", broker.image()).put("kafkaContainer", broker.container());
      } else { bootstrap = kafka; report.put("kafka", kafka); }
      out.println("forseti gate --engine flink: broker " + bootstrap + ", out " + outDir);
      for (String[] s : SMOKE) if (!f.containsKey("placement") || s[0].equals("smoke-" + f.get("placement"))) runs.add(scenario(as, s[0], s[1], root.resolve(s[2]), registry, keys, python, outDir.resolve(s[0]), bootstrap, root, out));
    } catch (Exception e) {
      out.println("ERROR the Flink gate stopped: " + e);
      report.put("error", e.toString());
    } finally {
      if (broker != null) broker.close();
    }
    Json.putSafe(report, "wallMs", (System.nanoTime() - t0) / 1_000_000);
    ArrayNode results = report.putArray("assertions");
    for (var r : as.results()) results.addObject().put("id", r.getKey()).put("held", r.getValue().getKey()).put("evidence", r.getValue().getValue());
    as.failed().forEach(report.putArray("failed")::add);
    as.notRun().forEach(report.putArray("notRun")::add);
    report.put("line", as.line());
    int exit = as.exitCode();
    report.put("exit", exit);
    try { Files.createDirectories(outDir); Json.write(outDir.resolve("report.json"), report); } catch (IOException e) { err.println("report: " + e); exit = 1; }
    out.println(as.line());
    return exit;
  }

  /** One smoke run and its five assertions; a run that fails before its comparison leaves the later ids not run (the gate then fails). */
  private static ObjectNode scenario(Assertions as, String label, String placement, Path scenario, Path registry, Path keys, String python, Path work,
                                     String bootstrap, Path root, PrintStream out) throws Exception {
    String p = "FL-SMOKE-" + placement + "-";
    out.println("--- " + label + ": " + root.relativize(scenario));
    FlinkRun.Result r;
    ObjectNode summary = Json.object().put("label", label).put("scenario", root.relativize(scenario).toString().replace('\\', '/'));
    try {
      r = FlinkRun.run(bootstrap, root, label, scenario, registry, keys, work, FlinkCluster.Memory.defaults());
    } catch (RuntimeException | IOException e) {
      as.check(p + "RUN", false, "the run failed: " + e.getMessage());
      summary.put("error", e.toString());
      return summary;
    }
    JsonNode clusters = r.evidence().path("flink").path("clusters");
    boolean stopped = true;
    for (JsonNode c : clusters) stopped &= c.path("jobManagerExit").asText().matches("-?[0-9]+");
    boolean runOk = r.lock().equals("equal") && r.topicProblems().isEmpty() && r.cuts() == r.batches() && stopped;
    as.check(p + "RUN", runOk, "lock " + r.lock() + ", topic problems " + r.topicProblems() + ", cuts " + r.cuts() + " of " + r.batches() + " batches, both jobs RUNNING before batch 1 ("
        + clusters.path("stage1").path("runningAfterMs").asLong() + " ms, " + clusters.path("stage2").path("runningAfterMs").asLong() + " ms), JobManager exits "
        + clusters.path("stage1").path("jobManagerExit").asText() + "/" + clusters.path("stage2").path("jobManagerExit").asText());

    // the output journals: every record carried the run's forseti-generation header and RFC 8785 bytes (the dump refuses anything
    // else), every writer's journal is there, and every consumer of the run read with read_committed
    List<String> missing = new ArrayList<>();
    for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES, Stage2.AFFECTED, Stage2.AFFECTED_SETS, ReplayTopics.DISPOSITIONS1, ReplayTopics.DISPOSITIONS2)) {
      boolean any = false;
      for (String k : r.dumped().keySet()) any |= k.startsWith(t + "/");
      if (!any) missing.add(t);
    }
    List<String> notCommitted = readCommitted(r.evidence());
    as.check(p + "OUTPUT", missing.isEmpty() && notCommitted.isEmpty(), "dumped " + r.dumped().values().stream().mapToLong(Long::longValue).sum() + " records "
        + r.dumped() + ", journals missing " + missing + ", consumers not read_committed " + notCommitted);

    // initEvidence: the first progress of every stage-1 progress writer, and only that one
    List<String> init = initEvidence(r.run(), r.declarationOffset(), r.controlEnd());
    as.check(p + "INIT", init.isEmpty(), init.isEmpty() ? "every stage-1 writer's first progress carries initEvidence {declarationOffset " + r.declarationOffset()
        + ", endOffset " + r.controlEnd() + ", restore " + new RestoreEvidence(false, null, 0).encode() + "}, no other progress does" : String.join("; ", init));

    // the oracle at every cut, unchanged (plan D10), then the shared reconciliation at every cut (decision F5)
    EngineCheck.Compared c = EngineCheck.compare(r.recording(), r.run(), Json.read(scenario), registry, python, root, work.resolve("oracle"), work.resolve("oracle.log"),
        r.rulesDigest());
    boolean oracleOk = c.equal() && c.cutsCompared() == r.cuts() && c.rowsCompared() > 0 && c.dispositionsCompared() > 0;
    as.check(p + "ORACLE", oracleOk, "cuts " + r.cuts() + ", compared " + c.cutsCompared() + ", rows " + c.rowsCompared() + ", dispositions " + c.dispositionsCompared()
        + ", differences " + c.differences() + ", oracle " + (c.broken() != null ? c.broken() : "exit " + c.exit()) + ", findings " + c.findings() + ": " + c.describe());
    List<String> problems = new ArrayList<>();
    Reconciliation rc = Reconciliation.open(r.recording(), r.run());
    for (JsonNode x : Json.read(r.run().resolve("cuts.json")).path("cuts")) problems.addAll(rc.cut(x.path("cutId").asLong()));
    problems.addAll(rc.finish());
    as.check(p + "RECONCILE", problems.isEmpty(), r.cuts() + " cuts reconciled, " + problems.size() + " problems" + (problems.isEmpty() ? "" : ": " + problems.get(0)));

    summary.put("lock", r.lock());
    Json.putSafe(summary, "batches", r.batches());
    Json.putSafe(summary, "cuts", r.cuts());
    Json.putSafe(summary, "compared", c.cutsCompared());
    Json.putSafe(summary, "differences", c.differences());
    Json.putSafe(summary, "businessRecords", r.businessRecords());
    Json.putSafe(summary, "barriers", r.barriers());
    Json.putSafe(summary, "wallMs", r.wallMs());
    Json.putSafe(summary, "firstBatchAfterMs", r.firstBatchAfterMs());
    summary.set("memory", r.memory());
    summary.set("clusters", clusters);
    return summary;
  }

  /** Every consumer configuration recorded for the run (the harness's clients, the jobs' sources) without {@code read_committed}. */
  static List<String> readCommitted(JsonNode evidence) {
    List<String> bad = new ArrayList<>();
    evidence.path("clients").fields().forEachRemaining(e -> {
      if (e.getValue().has("isolation.level") && !"read_committed".equals(e.getValue().path("isolation.level").asText())) bad.add(e.getKey());
    });
    int sources = 0;
    for (String stage : List.of("stage1-pins", "stage2-pins")) {
      JsonNode s = evidence.path("flink").path("jobs").path(stage).path("sources");
      if (s.isMissingNode()) bad.add(stage + " (no job pins recorded)");
      for (Iterator<Map.Entry<String, JsonNode>> it = s.fields(); it.hasNext();) {
        Map.Entry<String, JsonNode> e = it.next();
        sources++;
        if (!"read_committed".equals(e.getValue().path("isolation.level").asText())) bad.add(stage + "/" + e.getKey());
      }
    }
    if (sources == 0) bad.add("no job source recorded");
    return bad;
  }

  /** The stage-1 progress writers' {@code initEvidence} in the dumped journals: every first progress of {@code forseti.work-records},
   *  {@code forseti.samples} and {@code forseti.dispositions-stage1} (station partitions and the reject collector's) carries it, naming the
   *  run's declaration, the control end offset H and Flink's restore evidence of a first start; no later progress and no stage-2 progress
   *  carries one. Returns the problems. */
  static List<String> initEvidence(Path run, long declaration, long end) throws IOException {
    List<String> problems = new ArrayList<>();
    String expected = new RestoreEvidence(false, null, 0).encode();
    Path journals = run.resolve("journals");
    for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES, ReplayTopics.DISPOSITIONS1, Stage2.AFFECTED, Stage2.AFFECTED_SETS, ReplayTopics.DISPOSITIONS2)) {
      boolean stage1 = t.equals(Stage1.WORK_RECORDS) || t.equals(Stage1.SAMPLES) || t.equals(ReplayTopics.DISPOSITIONS1);
      Path dir = journals.resolve(t);
      if (!Files.isDirectory(dir)) { problems.add(t + ": no journal"); continue; }
      List<Path> files;
      try (Stream<Path> s = Files.list(dir)) { files = s.sorted().toList(); }
      for (Path file : files) {
        int n = 0;
        for (String l : Files.readAllLines(file, StandardCharsets.UTF_8)) {
          JsonNode v = Json.read(l.getBytes(StandardCharsets.UTF_8)).path("value");
          if (!"progress".equals(v.path("type").asText())) continue;
          JsonNode ie = v.get("initEvidence");
          String w = t + "/" + file.getFileName() + " progress " + v.path("cutId").asLong();
          if (!stage1 || n > 0) { if (ie != null) problems.add(w + " carries initEvidence"); n++; continue; }
          n++;
          if (ie == null) { problems.add(w + ": the first progress carries no initEvidence"); continue; }
          if (ie.path("declarationOffset").asLong(-1) != declaration) problems.add(w + ": declarationOffset " + ie.path("declarationOffset"));
          if (ie.path("endOffset").asLong(-1) != end) problems.add(w + ": endOffset " + ie.path("endOffset") + ", H " + end);
          try {
            RestoreEvidence re = RestoreEvidence.decode(ie.path("restore").asText());
            if (!re.encode().equals(expected)) problems.add(w + ": restore " + re.encode());
          } catch (IllegalArgumentException e) { problems.add(w + ": restore " + ie.path("restore") + ": " + e.getMessage()); }
        }
        if (stage1 && n == 0) problems.add(t + "/" + file.getFileName() + ": no progress");
      }
    }
    return problems;
  }
}
