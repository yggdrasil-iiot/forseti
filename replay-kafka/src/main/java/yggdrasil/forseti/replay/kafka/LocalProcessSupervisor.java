package yggdrasil.forseti.replay.kafka;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import yggdrasil.forseti.cli.replay.ProcessSupervisor;

/** {@link ProcessSupervisor} over local operating-system processes ({@link ProcessBuilder}): each engine JVM is a child of the harness,
 *  its output appended to its log file. A kill is {@link Process#destroyForcibly} (SIGKILL on POSIX); stop, continue and terminate are
 *  sent with {@code kill -STOP|-CONT|-TERM <pid>}, which only a POSIX host has: elsewhere they are refused
 *  ({@link UnsupportedOperationException}, plan D12: the replay gate runs on Linux). {@link #close} kills what is still alive. */
public final class LocalProcessSupervisor implements ProcessSupervisor {
    private final List<Child> started = new ArrayList<>();
    private final boolean posix;

    public LocalProcessSupervisor() { this(!System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows")); }

    LocalProcessSupervisor(boolean posix) { this.posix = posix; }

    public boolean posix() { return posix; }

    private final class Child implements Supervised {
        final String name; final Process process; final Path log;
        Child(String name, Process process, Path log) { this.name = name; this.process = process; this.log = log; }
        @Override public String name() { return name; }
        @Override public long pid() { return process.pid(); }
        @Override public boolean alive() { return process.isAlive(); }
        @Override public Path log() { return log; }

        @Override public void signal(Signal s) {
            if (s == Signal.KILL) { process.destroyForcibly(); return; }
            if (!posix) throw new UnsupportedOperationException("signal " + s + " needs a POSIX host (the replay gate runs on Linux)");
            String sig = switch (s) { case STOP -> "-STOP"; case CONT -> "-CONT"; case TERM -> "-TERM"; default -> throw new IllegalStateException(); };
            try {
                Process k = new ProcessBuilder("kill", sig, Long.toString(process.pid())).redirectErrorStream(true).start();
                if (!k.waitFor(10, TimeUnit.SECONDS) || k.exitValue() != 0) throw new IllegalStateException("kill " + sig + " " + process.pid() + " failed");
            } catch (IOException e) { throw new UncheckedIOException(e); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
        }

        @Override public OptionalInt waitFor(Duration timeout) {
            try { return process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS) ? OptionalInt.of(process.exitValue()) : OptionalInt.empty(); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
        }
    }

    @Override public synchronized Supervised start(String name, List<String> command, Map<String, String> env, Path log) {
        try {
            Files.createDirectories(log.toAbsolutePath().getParent());
            ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()));
            pb.environment().putAll(env);
            Child c = new Child(name, pb.start(), log);
            started.add(c);
            return c;
        } catch (IOException e) { throw new UncheckedIOException("start " + name + ": " + e.getMessage(), e); }
    }

    @Override public synchronized List<Supervised> running() {
        List<Supervised> l = new ArrayList<>();
        for (Child c : started) if (c.alive()) l.add(c);
        return l;
    }

    @Override public synchronized void close() {
        for (Child c : started) if (c.alive()) {
            if (posix) try { c.signal(Signal.CONT); } catch (RuntimeException ignored) { /* a stopped child must not outlive the kill */ }
            c.process.destroyForcibly();
        }
        for (Child c : started) c.waitFor(Duration.ofSeconds(30));
    }
}
