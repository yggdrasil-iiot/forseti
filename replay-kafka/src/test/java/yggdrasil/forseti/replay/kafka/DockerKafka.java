package yggdrasil.forseti.replay.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Assumptions;
import yggdrasil.forseti.core.Json;

/** The broker of the Kafka tests (plan D12): Docker-managed Apache Kafka 4.2.2, the image pinned by digest in {@code live/stack-lock.json}
 *  (the step-5 pin), single-node KRaft with the settings of {@code live/docker-compose.yml}; no embedded broker, no Testcontainers.
 *  {@code -Dforseti.kafka=docker} starts one container for the test JVM on a free port and removes it when the JVM exits;
 *  {@code -Dforseti.kafka=<host:port>} uses a running broker. Without the property the Kafka tests do not run (the Windows and unit
 *  builds of plan D12); the replay gate's CI job sets it. */
public final class DockerKafka {
    private DockerKafka() {}

    public static final String PROPERTY = "forseti.kafka";
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    private static String bootstrap, container;

    public static synchronized String bootstrap() {
        if (bootstrap != null) return bootstrap;
        String v = System.getProperty(PROPERTY, "");
        Assumptions.assumeTrue(!v.isEmpty(), "no Kafka for this test: -D" + PROPERTY + "=docker (the pinned image of live/stack-lock.json) or -D" + PROPERTY + "=<host:port>");
        if (!v.equals("docker")) return bootstrap = v;
        JsonNode k = Json.read(ROOT.resolve("live/stack-lock.json")).path("images").path("kafka");
        String image = Json.text(k, "image") + "@" + Json.text(k, "digest");
        int port = freePort();
        String name = "forseti-replay-kafka-" + ProcessHandle.current().pid() + "-" + port;
        List<String> cmd = new ArrayList<>(List.of("docker", "run", "-d", "--rm", "--name", name, "-p", port + ":" + port));
        Map<String, String> env = new LinkedHashMap<>();
        env.put("KAFKA_NODE_ID", "1");
        env.put("KAFKA_PROCESS_ROLES", "broker,controller");
        env.put("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093");
        env.put("KAFKA_LISTENERS", "PLAINTEXT://:" + port + ",CONTROLLER://:9093");
        env.put("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:" + port);
        env.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
        env.put("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
        env.put("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT");
        env.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        env.put("KAFKA_NUM_PARTITIONS", "6");
        env.put("KAFKA_DEFAULT_REPLICATION_FACTOR", "1");
        env.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        env.put("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
        env.put("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        env.put("KAFKA_MIN_INSYNC_REPLICAS", "1");
        env.put("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
        env.put("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs");
        env.forEach((a, b) -> { cmd.add("-e"); cmd.add(a + "=" + b); });
        cmd.add(image);
        String out = exec(cmd, 300);
        container = name;
        Runtime.getRuntime().addShutdownHook(new Thread(DockerKafka::remove, "forseti-kafka-remove"));
        String b = "localhost:" + port;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (true) {
            try (Admin a = Admin.create(KafkaConfig.admin(b, "forseti-test-probe"))) {
                a.describeCluster().nodes().get(5, TimeUnit.SECONDS);
                break;
            } catch (Exception e) {
                if (System.nanoTime() > deadline) { remove(); throw new IllegalStateException("the Kafka container " + name + " (" + out.trim() + ") did not become ready", e); }
                try { Thread.sleep(1000); } catch (InterruptedException x) { Thread.currentThread().interrupt(); throw new IllegalStateException(x); }
            }
        }
        return bootstrap = b;
    }

    /** The container this JVM started, if any. */
    public static synchronized Optional<String> container() { return Optional.ofNullable(container); }

    static synchronized void remove() {
        if (container == null) return;
        try { exec(List.of("docker", "rm", "-f", container), 60); } catch (RuntimeException ignored) { /* best effort at exit */ }
        container = null;
        bootstrap = null;
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); } catch (IOException e) { throw new IllegalStateException(e); }
    }

    private static String exec(List<String> cmd, int seconds) {
        try {
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            byte[] out = p.getInputStream().readAllBytes();
            if (!p.waitFor(seconds, TimeUnit.SECONDS)) { p.destroyForcibly(); throw new IllegalStateException(cmd + " timed out"); }
            String s = new String(out, StandardCharsets.UTF_8);
            if (p.exitValue() != 0) throw new IllegalStateException(cmd + " exit " + p.exitValue() + ": " + s);
            return s;
        } catch (IOException e) { throw new IllegalStateException(cmd + ": " + e.getMessage(), e); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
}
