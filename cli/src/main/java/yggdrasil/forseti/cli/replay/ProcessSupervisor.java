package yggdrasil.forseti.cli.replay;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/** Process supervision of a replay run (plan D7): the harness starts each engine JVM itself, signals it at the instant an injection
 *  names (stop, continue, terminate, kill) and waits for it, without Docker images. Signals other than a kill are POSIX signals: the
 *  replay gate runs on Linux (plan D12), and a host without them refuses them ({@link UnsupportedOperationException}). */
public interface ProcessSupervisor extends AutoCloseable {
    enum Signal { STOP, CONT, TERM, KILL }

    /** One supervised process. */
    interface Supervised {
        String name();
        long pid();
        boolean alive();
        void signal(Signal s);
        /** The exit code once the process ended within {@code timeout}, empty otherwise. */
        OptionalInt waitFor(Duration timeout);
        /** The file its standard output and error go to. */
        Path log();
    }

    /** Starts {@code command} with {@code env} added to the supervisor's environment, its output appended to {@code log}. */
    Supervised start(String name, List<String> command, Map<String, String> env, Path log);

    /** The processes started and still alive. */
    List<Supervised> running();

    /** Kills every process still alive and waits for it. */
    @Override void close();
}
