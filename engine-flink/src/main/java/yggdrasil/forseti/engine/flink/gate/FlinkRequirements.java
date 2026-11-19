package yggdrasil.forseti.engine.flink.gate;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.cli.gate.ReplayRequirements;
import yggdrasil.forseti.engine.flink.job.JobSettings;

/**
 * {@code forseti requirements --engine flink --gate-report <report> [--out <dir>]} (spec 705, plan D14): the engine-neutral
 * requirements run ({@link ReplayRequirements}) on the Flink engine ({@link FlinkEngine}), against {@code gate/flink-requirements.json}.
 * Spec Decision 18 predicts a miss of item 5 (c) on the dedicated placement (Flink's stage-1 watermark is the minimum over every
 * subscribed raw telemetry partition); a miss is recorded, makes Flink ineligible, and does not fail the command.
 *
 * <p>Flags: {@code --gate-report <file>} (required: the semantic gate's {@code report.json}), {@code --out <dir>} (build/requirements),
 * {@code --python <interpreter>} (python), {@code --kafka docker|<host:port>}, {@code --registry <dir>} (fixtures/live/registry),
 * {@code --trusted-keys <file>} (fixtures/live/trusted-keys.jsonl). The repository root is the working directory.
 */
public final class FlinkRequirements {
  private FlinkRequirements() {}

  static final Set<String> FLAGS = Set.of("engine", "gate-report", "out", "python", "kafka", "registry", "trusted-keys");

  public static int main(String[] args, PrintStream out, PrintStream err) {
    Map<String, String> f = new LinkedHashMap<>();
    for (int i = 1; i < args.length; i++) {
      String a = args[i];
      if (!a.startsWith("--") || i + 1 >= args.length || !FLAGS.contains(a.substring(2))) {
        err.println("forseti requirements --engine flink: unknown or incomplete flag " + a + " (flags: " + new TreeSet<>(FLAGS) + ")");
        return 2;
      }
      if (f.put(a.substring(2), args[++i]) != null) { err.println("flag " + a + " given twice"); return 2; }
    }
    if (!"flink".equals(f.get("engine"))) { err.println("--engine flink"); return 2; }
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", System.getProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn"));
    f.put("engine-artifact", JobSettings.engineArtifact());
    Path root = Path.of("").toAbsolutePath();
    return ReplayRequirements.main("flink", f, "forseti " + String.join(" ", args), flags -> FlinkEngine.open(root, flags.getOrDefault("kafka", "docker"), out), out, err);
  }
}
