package yggdrasil.forseti.bridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The bridge process: MQTT in, Kafka out, until SIGTERM. On the stop signal it stops taking frames, writes what is already queued,
 * releases what a hold stage still holds, waits for every Kafka acknowledgement and writes the run record; any failure on the way
 * exits 1 with the failure in the run record.
 *
 * <pre>
 * forseti-bridge --mqtt tcp://host:1883 --kafka host:9092 [--client-id id] [--pins pins.json] [--routing routing.json]
 *   [--messages-topic ot.messages] [--sessions-topic ot.sessions] [--dead-letter-topic ot.dead-letter]
 *   [--hold-ms N [--hold-start-ms M]] [--source-offset-ms N] [--raw-log frames.jsonl] [--run-record run.json]
 *   [--queue 10000] [--buffer-bytes 33554432]
 * </pre>
 * {@code pins.json} is a JSON object from device to the pinned {@code definitionSha256}. With {@code --routing} (the routing manifest,
 * which preflight always passes) every routed device must have a pin, or the bridge exits 2 naming the devices; without it the run
 * record lists the devices that came up unpinned. Without {@code --hold-ms} and
 * {@code --source-offset-ms} no D3 stage runs.
 */
public final class BridgeMain {
    private BridgeMain() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> a = new TreeMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) usage("unexpected argument " + args[i]);
            a.put(args[i].substring(2), args[++i]);
        }
        for (String k : a.keySet())
            if (!List.of("mqtt", "kafka", "client-id", "pins", "routing", "messages-topic", "sessions-topic", "dead-letter-topic", "hold-ms", "hold-start-ms",
                "source-offset-ms", "raw-log", "run-record", "queue", "buffer-bytes").contains(k)) usage("unknown option --" + k);
        if (!a.containsKey("mqtt") || !a.containsKey("kafka")) usage("--mqtt and --kafka are required");
        if (a.containsKey("hold-start-ms") && !a.containsKey("hold-ms")) usage("--hold-start-ms needs --hold-ms");
        String clientId = a.getOrDefault("client-id", "forseti-bridge");
        Records.Topics topics = new Records.Topics(a.getOrDefault("messages-topic", Records.Topics.DEFAULT.messages()),
            a.getOrDefault("sessions-topic", Records.Topics.DEFAULT.sessions()), a.getOrDefault("dead-letter-topic", Records.Topics.DEFAULT.deadLetter()));
        List<Stage> stages = new ArrayList<>();
        if (a.containsKey("source-offset-ms")) stages.add(new SourceOffsetStage(Long.parseLong(a.get("source-offset-ms"))));
        if (a.containsKey("hold-ms")) stages.add(new HoldStage(Long.parseLong(a.getOrDefault("hold-start-ms", "0")), Long.parseLong(a.get("hold-ms"))));
        Path runRecord = a.containsKey("run-record") ? Path.of(a.get("run-record")) : null;
        Map<String, String> pins = pins(a.get("pins"));
        if (a.containsKey("routing")) {
            List<String> missing = unpinned(routingDevices(Path.of(a.get("routing"))), pins);
            if (!missing.isEmpty()) usage("devices of the routing manifest without a pin: " + String.join(", ", missing));
        }
        BridgeCore core = new BridgeCore(pins);

        RunRecord run = new RunRecord();
        FrameQueue queue = new FrameQueue(Integer.parseInt(a.getOrDefault("queue", "10000")), 10_000);
        CountDownLatch done = new CountDownLatch(1);
        Thread main = Thread.currentThread();
        boolean[] stop = {false};
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            synchronized (stop) { stop[0] = true; }
            try { done.await(120, TimeUnit.SECONDS); } catch (InterruptedException e) { main.interrupt(); }
        }));
        int exit = 0;
        try (KafkaSink sink = KafkaSink.connect(a.get("kafka"), clientId, Long.parseLong(a.getOrDefault("buffer-bytes", "33554432")));
             RawFrameLog log = a.containsKey("raw-log") ? new RawFrameLog(Path.of(a.get("raw-log"))) : null) {
            Pipeline pipeline = new Pipeline(core, stages, sink, topics, run, log);
            try (MqttSource source = new MqttSource(a.get("mqtt"), clientId, queue, System::currentTimeMillis)) {
                while (true) {
                    synchronized (stop) { if (stop[0]) break; }
                    Frame f = queue.poll(100);
                    long now = System.currentTimeMillis();
                    if (f == null) pipeline.tick(now); else pipeline.accept(f, now);
                }
            }
            for (Frame f = queue.pollNow(); f != null; f = queue.pollNow()) pipeline.accept(f, System.currentTimeMillis());
            pipeline.drain(System.currentTimeMillis());
            run.end("stopped", null);
        } catch (Exception e) {
            run.end("failed", e.toString());
            System.err.println("[BRIDGE] failed: " + e);
            exit = 1;
        } finally {
            run.pins(a.containsKey("routing"), core.unpinnedSeen());
            if (runRecord != null) run.write(runRecord, stages);
            done.countDown();
        }
        if (exit != 0) Runtime.getRuntime().halt(exit);
    }

    /** The device paths of a routing manifest ({@code devices {path -> {station, rawPartition}}}, the file preflight checks). */
    static List<String> routingDevices(Path file) throws java.io.IOException {
        JsonNode n = new ObjectMapper().readTree(Files.readAllBytes(file)).path("devices");
        if (!n.isObject() || n.isEmpty()) throw new IllegalArgumentException("routing: no devices object in " + file);
        List<String> l = new ArrayList<>();
        n.fieldNames().forEachRemaining(l::add);
        return l;
    }

    /** The routed devices without a pin, sorted; with a routing manifest each must be pinned, or the bridge refuses to start. */
    static List<String> unpinned(List<String> routed, Map<String, String> pins) {
        List<String> l = new ArrayList<>();
        for (String d : new java.util.TreeSet<>(routed)) if (!pins.containsKey(d)) l.add(d);
        return l;
    }

    /** The generation pin per device; a device without one is not compared. */
    static Map<String, String> pins(String file) throws java.io.IOException {
        if (file == null) return Map.of();
        JsonNode n = new ObjectMapper().readTree(Files.readAllBytes(Path.of(file)));
        if (!n.isObject()) throw new IllegalArgumentException("pins: a JSON object from device to definitionSha256 expected");
        Map<String, String> m = new TreeMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> it = n.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> x = it.next();
            if (!x.getValue().isTextual() || !x.getValue().textValue().matches("[0-9a-f]{64}")) throw new IllegalArgumentException("pins: " + x.getKey() + " is not a SHA-256");
            m.put(x.getKey(), x.getValue().textValue());
        }
        return m;
    }

    private static void usage(String why) {
        System.err.println("forseti-bridge: " + why);
        System.exit(2);
    }
}
