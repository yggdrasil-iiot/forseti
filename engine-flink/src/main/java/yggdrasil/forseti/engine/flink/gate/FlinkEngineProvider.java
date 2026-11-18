package yggdrasil.forseti.engine.flink.gate;

import java.io.PrintStream;
import yggdrasil.forseti.cli.replay.EngineProvider;

/** The {@code flink} engine of {@code forseti gate --engine flink} (spec 705, plan D1), found by {@link java.util.ServiceLoader} on the
 *  engine path ({@code engine-flink/target/engine}). The gate runs the replay gate's shards, aggregate and controls ({@link FlinkGate}); a single
 *  {@code run} and {@code requirements} (PR 7) are not offered yet and answer with a usage error. */
public final class FlinkEngineProvider implements EngineProvider {
  @Override public String name() { return "flink"; }

  @Override public int gate(String[] args, PrintStream out, PrintStream err) { return FlinkGate.main(args, out, err); }

  @Override public int run(String[] args, PrintStream out, PrintStream err) {
    err.println("forseti run --engine flink is not offered: the Flink engine runs under forseti gate --engine flink");
    return 2;
  }

  @Override public int requirements(String[] args, PrintStream out, PrintStream err) {
    err.println("forseti requirements --engine flink comes with step 6 PR 7 (spec 705)");
    return 2;
  }
}
