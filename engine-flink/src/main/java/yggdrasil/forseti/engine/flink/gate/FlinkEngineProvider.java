package yggdrasil.forseti.engine.flink.gate;

import java.io.PrintStream;
import yggdrasil.forseti.cli.replay.EngineProvider;

/** The {@code flink} engine of {@code forseti gate --engine flink} (spec 705, plan D1), found by {@link java.util.ServiceLoader} on the
 *  engine path ({@code engine-flink/target/engine}). The gate runs the replay gate's shards, aggregate and controls ({@link FlinkGate}); {@code requirements}
 *  runs injection 24 and comparison item 5 ({@link FlinkRequirements}); a single {@code run} is not offered and answers with a usage
 *  error. */
public final class FlinkEngineProvider implements EngineProvider {
  @Override public String name() { return "flink"; }

  @Override public int gate(String[] args, PrintStream out, PrintStream err) { return FlinkGate.main(args, out, err); }

  @Override public int run(String[] args, PrintStream out, PrintStream err) {
    err.println("forseti run --engine flink is not offered: the Flink engine runs under forseti gate --engine flink");
    return 2;
  }

  @Override public int requirements(String[] args, PrintStream out, PrintStream err) { return FlinkRequirements.main(args, out, err); }
}
