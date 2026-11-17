package yggdrasil.forseti.replay.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import yggdrasil.forseti.core.Json;

/** The broker of a replay gate run (plan D12): Docker-managed Apache Kafka 4.2.2, the image pinned by digest in
 *  {@code live/stack-lock.json} (the step-5 pin), single-node KRaft with the settings of {@code live/docker-compose.yml}; no embedded
 *  broker, no Testcontainers. {@link #start} runs one container named {@code forseti-replay-gate-<pid>-<port>} on a free host port and
 *  waits until it answers; {@link #close} removes exactly that container. The test helper {@code DockerKafka} starts the same broker. */
public final class DockerBroker implements AutoCloseable {
    public static final String NAME_PREFIX = "forseti-replay-gate-";
    private final String bootstrap, container, image;

    private DockerBroker(String bootstrap, String container, String image) { this.bootstrap = bootstrap; this.container = container; this.image = image; }

    public String bootstrap() { return bootstrap; }
    public String container() { return container; }
    /** {@code image@digest} as pinned. */
    public String image() { return image; }

    /** Starts the pinned broker; {@code root} is the repository root holding {@code live/stack-lock.json}. */
    public static DockerBroker start(Path root) {
        JsonNode k = Json.read(root.resolve("live/stack-lock.json")).path("images").path("kafka");
        String image = Json.text(k, "image") + "@" + Json.text(k, "digest");
        int port = freePort();
        String name = NAME_PREFIX + ProcessHandle.current().pid() + "-" + port;
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
        DockerBroker broker = new DockerBroker("localhost:" + port, name, image);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (true) {
            try (Admin a = Admin.create(KafkaConfig.admin(broker.bootstrap, "forseti-gate-probe"))) {
                a.describeCluster().nodes().get(5, TimeUnit.SECONDS);
                return broker;
            } catch (Exception e) {
                if (System.nanoTime() > deadline) { broker.close(); throw new IllegalStateException("the Kafka container " + name + " (" + out.trim() + ") did not become ready", e); }
                try { Thread.sleep(1000); } catch (InterruptedException x) { Thread.currentThread().interrupt(); broker.close(); throw new IllegalStateException(x); }
            }
        }
    }

    @Override public void close() {
        try { exec(List.of("docker", "rm", "-f", container), 60); } catch (RuntimeException ignored) { /* best effort: the container runs with --rm */ }
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
