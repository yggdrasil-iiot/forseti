package yggdrasil.forseti.cli.replay;

import java.io.PrintStream;

/** An external engine of {@code forseti gate|run|requirements --engine <name>} (plan D1), found by {@link java.util.ServiceLoader} on
 *  the engine path ({@link EngineProviders}). {@code --engine core} is never a provider: the reference harness runs in-process. A
 *  provider receives the command's whole argument list (the command name first) and returns its exit code under the launcher's rule: 0
 *  done, 1 a check failed or a refusal, 2 usage or input error. Every call runs with the provider's loader as the thread context class
 *  loader. */
public interface EngineProvider {
    /** The name {@code --engine} selects; never {@code core}. */
    String name();

    /** {@code forseti gate --engine <name> ...}. */
    int gate(String[] args, PrintStream out, PrintStream err);

    /** {@code forseti run --engine <name> ...}. */
    int run(String[] args, PrintStream out, PrintStream err);

    /** {@code forseti requirements --engine <name> ...} (spec 705): injection 24 and comparison item 5, each requirement recorded as
     *  pass, miss or error; exit 0 when every requirement is recorded, a miss included (plan D14). */
    int requirements(String[] args, PrintStream out, PrintStream err);
}
