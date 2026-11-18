package yggdrasil.forseti.engine.flink.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.replay.ProcessSupervisor;

/** The start of a cluster whose JobManager could not bind a port it was given (CI 2026-11-18: {@code Could not start rest endpoint on
 *  any port in port range 40731}, a free port taken by another process before the JobManager bound it): the cluster starts again on
 *  fresh ports, the same job id, each attempt recorded; any other early exit still fails the run, and so do too many bind failures. */
class FlinkClusterStartTest {
    @TempDir Path tmp;

    /** A supervisor whose JobManager JVMs follow a script: {@code true} runs, {@code false} exits at once with {@code exitLog}. */
    static final class Scripted implements ProcessSupervisor {
        final Deque<Boolean> jobManagers;
        final String exitLog;
        final List<String> started = new ArrayList<>();
        Scripted(String exitLog, Boolean... jobManagers) { this.exitLog = exitLog; this.jobManagers = new ArrayDeque<>(List.of(jobManagers)); }

        @Override public Supervised start(String name, List<String> command, Map<String, String> env, Path log) {
            started.add(name);
            boolean runs = !name.endsWith("-jobmanager") || jobManagers.removeFirst();
            try { Files.writeString(log, runs ? "started\n" : exitLog, StandardCharsets.UTF_8); } catch (Exception e) { throw new IllegalStateException(e); }
            return new Supervised() {
                boolean alive = runs;
                public String name() { return name; }
                public long pid() { return 1; }
                public boolean alive() { return alive; }
                public void signal(Signal s) { alive = false; }
                public OptionalInt waitFor(Duration timeout) { return alive ? OptionalInt.empty() : OptionalInt.of(runs ? 0 : 1); }
                public Path log() { return log; }
            };
        }
        @Override public List<Supervised> running() { return List.of(); }
        @Override public void close() {}
    }

    static final String BIND = "Caused by: java.net.BindException: Could not start rest endpoint on any port in port range 40731\n";

    /** A REST endpoint that reports every job RUNNING. */
    static HttpServer running() throws Exception {
        HttpServer s = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        s.createContext("/jobs/", x -> {
            byte[] b = "{\"state\":\"RUNNING\",\"vertices\":[{\"name\":\"v\",\"status\":\"RUNNING\",\"parallelism\":1}]}".getBytes(StandardCharsets.UTF_8);
            x.sendResponseHeaders(200, b.length);
            x.getResponseBody().write(b);
            x.close();
        });
        s.start();
        return s;
    }

    FlinkCluster start(Scripted sup, int... ports) throws Exception {
        Path dir = Files.createDirectories(tmp.resolve("stage1")), spec = Files.writeString(dir.resolve("job-spec.json"), "{}");
        Iterator<Integer> it = Arrays.stream(ports).iterator();
        return FlinkCluster.start(sup, "stage1", dir, "x.Job", spec, tmp.resolve("cp").toUri().toString(), FlinkCluster.Memory.defaults(), it::next);
    }

    @Test void aJobManagerThatCouldNotBindItsPortIsStartedAgainOnFreshPortsAndTheAttemptIsRecorded() throws Exception {
        HttpServer rest = running();
        try {
            Scripted sup = new Scripted(BIND, false, true);
            int taken = FlinkCluster.freePort();
            try (FlinkCluster c = start(sup, taken, 40001, rest.getAddress().getPort(), 40002)) {
                String job = c.jobId.toHexString();
                c.awaitRunning(Duration.ofSeconds(20));
                assertEquals(rest.getAddress().getPort(), c.restPort());
                assertEquals(job, c.jobId.toHexString(), "the same job: a retried start is not another run");
                JsonNode r = c.record;
                assertEquals(2, r.path("starts").asInt());
                assertEquals(1, r.path("startAttempts").size());
                JsonNode a = r.path("startAttempts").get(0);
                assertEquals(taken, a.path("restPort").asInt());
                assertEquals(40001, a.path("rpcPort").asInt());
                assertTrue(a.path("cause").asText().contains("Could not start rest endpoint"), a.toString());
                assertTrue(Files.exists(tmp.resolve("stage1").resolve(a.path("jobManagerLog").asText())), "the failed start's log is kept");
                assertEquals(List.of("stage1-jobmanager", "stage1-taskmanager", "stage1-jobmanager", "stage1-taskmanager"), sup.started);
            }
        } finally { rest.stop(0); }
    }

    @Test void anyOtherEarlyExitFailsTheRunAndSoDoTooManyBindFailures() throws Exception {
        try (FlinkCluster c = start(new Scripted("java.lang.OutOfMemoryError: Java heap space\n", false), 1, 2)) {
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> c.awaitRunning(Duration.ofSeconds(10)));
            assertTrue(e.getMessage().contains("ended before RUNNING (start 1)"), e.getMessage());
            assertEquals(0, c.record.path("startAttempts").size());
        }
        Files.walk(tmp.resolve("stage1")).sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        try (FlinkCluster c = start(new Scripted(BIND, false, false, false), 1, 2, 3, 4, 5, 6)) {
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> c.awaitRunning(Duration.ofSeconds(10)));
            assertTrue(e.getMessage().contains("ended before RUNNING (start " + FlinkCluster.MAX_STARTS + ")"), e.getMessage());
            assertEquals(FlinkCluster.MAX_STARTS - 1, c.record.path("startAttempts").size());
        }
    }
}
