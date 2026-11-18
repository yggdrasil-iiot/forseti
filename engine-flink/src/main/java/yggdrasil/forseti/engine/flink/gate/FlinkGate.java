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
import yggdrasil.forseti.cli.gate.ReplayGate;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.Stage2;
import yggdrasil.forseti.engine.flink.job.RestoreEvidence;
import yggdrasil.forseti.engine.flink.job.JobSettings;

/**
 * {@code forseti gate --engine flink} (spec 705, 728; step 6 PR 5): the engine-neutral replay gate ({@link ReplayGate}) on the Flink
 * engine ({@link FlinkEngine}), against {@code gate/flink-assertions.json}, {@code gate/flink-shards.json} and
 * {@code gate/flink-negative-controls.json}. Like every gate it prints one line per assertion, then {@code assertions run N / expected M},
 * and exits 0 only when every expected id ran and held (1 otherwise, 2 for a usage error). The broker is the pinned Docker image
 * ({@code --kafka docker}, the default, started and removed by the gate) or a running one ({@code --kafka <host:port>}).
 *
 * <p>Flags: {@code --out <dir>} (build/flink), {@code --python <interpreter>} (python), {@code --kafka docker|<host:port>},
 * {@code --shard <name>}, {@code --aggregate <dir>}, {@code --negative-control <name>}, {@code --registry <dir>}
 * (fixtures/live/registry), {@code --registry-next <dir>} (fixtures/live/registry-next), {@code --trusted-keys <file>}
 * (fixtures/live/trusted-keys.jsonl). The repository root is the working directory.
 */
public final class FlinkGate {
  private FlinkGate() {}

  public static final ReplayGate.GateFiles FILES = new ReplayGate.GateFiles("gate/flink-assertions.json", "gate/flink-shards.json", "gate/flink-negative-controls.json");
  static final Set<String> FLAGS = Set.of("engine", "out", "python", "kafka", "registry", "registry-next", "trusted-keys", "shard", "aggregate", "negative-control");

  public static int main(String[] args, PrintStream out, PrintStream err) {
    Map<String, String> f = new LinkedHashMap<>();
    for (int i = 1; i < args.length; i++) {
      String a = args[i];
      if (!a.startsWith("--") || i + 1 >= args.length || !FLAGS.contains(a.substring(2))) {
        err.println("forseti gate --engine flink: unknown or incomplete flag " + a + " (flags: " + new TreeSet<>(FLAGS) + ")");
        return 2;
      }
      if (f.put(a.substring(2), args[++i]) != null) { err.println("flag " + a + " given twice"); return 2; }
    }
    if (!"flink".equals(f.get("engine"))) { err.println("--engine flink"); return 2; }
    // the harness's own Kafka clients log through the engine path's slf4j-simple: warnings only
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", System.getProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn"));
    f.put("engine-artifact", JobSettings.engineArtifact());
    Path root = Path.of("").toAbsolutePath();
    return ReplayGate.main("flink", f, "forseti " + String.join(" ", args), FILES, flags -> FlinkEngine.open(root, flags.getOrDefault("kafka", "docker"), out), out, err);
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
