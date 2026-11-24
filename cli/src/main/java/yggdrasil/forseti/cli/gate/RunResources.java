package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import yggdrasil.forseti.core.Json;

/** The resources a replay gate run holds after each of its runs, recorded for diagnosis only (never a gate criterion): the harness
 *  heap after a full collection, the harness's child processes still alive (every engine JVM of a finished run must be gone), and the
 *  engine's own shared infrastructure ({@link yggdrasil.forseti.cli.replay.ReplayEngine#resources}: the broker container's memory and
 *  data, its topics). On only when the system property {@value #PROPERTY} names a file: each run appends one JSON line there and the
 *  gate prints one line per run. The nightly unsharded gate sets it. */
final class RunResources {
    private RunResources() {}

    static final String PROPERTY = "forseti.gate.memoryLog";

    static Path file() {
        String f = System.getProperty(PROPERTY);
        return f == null || f.isBlank() ? null : Path.of(f).toAbsolutePath();
    }

    /** The heap after an explicit full collection, and the harness's live child processes. */
    static ObjectNode harness() {
        System.gc();
        MemoryUsage h = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        ObjectNode n = Json.object();
        Json.putSafe(n, "heapAfterGcBytes", h.getUsed());
        Json.putSafe(n, "heapCommittedBytes", h.getCommitted());
        Json.putSafe(n, "heapMaxBytes", h.getMax());
        Json.putSafe(n, "rssBytes", rss(ProcessHandle.current().pid()));
        ArrayNode children = n.putArray("liveChildren");
        ProcessHandle.current().descendants().filter(ProcessHandle::isAlive).forEach(p -> {
            ObjectNode c = children.addObject();
            Json.putSafe(c, "pid", p.pid());
            c.put("command", p.info().command().orElse("?"));
            Json.putSafe(c, "rssBytes", rss(p.pid()));
        });
        return n;
    }

    /** One run's record: its name, the harness's resources, the engine's, and the run's own cluster memory (its summary's
     *  {@code memory}, when the engine reports one). */
    static ObjectNode record(String run, ObjectNode harness, ObjectNode engine, JsonNode runSummary, JsonNode released) {
        ObjectNode n = Json.object().put("run", run).put("at", java.time.Instant.now().toString());
        n.set("harness", harness);
        n.set("engine", engine);
        n.set("runMemory", runSummary == null ? null : runSummary.path("memory").isObject() ? runSummary.get("memory") : null);
        n.set("releasedTopics", released);
        return n;
    }

    /** The line the gate prints for {@code record}. */
    static String line(ObjectNode r) {
        JsonNode h = r.path("harness"), e = r.path("engine"), m = r.path("runMemory");
        StringBuilder b = new StringBuilder("    memory ").append(r.path("run").asText()).append(": harness heap after GC ").append(mib(h.path("heapAfterGcBytes")))
            .append(" (committed ").append(mib(h.path("heapCommittedBytes"))).append(", RSS ").append(mib(h.path("rssBytes"))).append("), live children ")
            .append(h.path("liveChildren").size());
        if (e.has("brokerMemoryBytes")) b.append(", broker container ").append(mib(e.path("brokerMemoryBytes")))
            .append(" (anon ").append(mib(e.path("brokerAnonBytes"))).append(", file ").append(mib(e.path("brokerFileBytes"))).append(")");
        if (e.has("brokerDataBytes")) b.append(", broker data ").append(mib(e.path("brokerDataBytes")));
        if (e.has("topics")) b.append(", topics ").append(e.path("topics").asLong()).append(" / partitions ").append(e.path("partitions").asLong());
        if (r.path("releasedTopics").has("deleted")) b.append(" (").append(r.path("releasedTopics").path("deleted").asLong()).append(" deleted after the run)");
        if (m.isObject()) m.fields().forEachRemaining(c -> b.append(", ").append(c.getKey()).append(" TM RSS peak ").append(mib(c.getValue().path("taskManagerRssPeakBytes"))));
        return b.toString();
    }

    static String mib(JsonNode bytes) { return bytes.isNumber() ? (bytes.asLong() >> 20) + " MiB" : "?"; }

    static void append(Path file, ObjectNode r) throws IOException {
        Files.createDirectories(file.getParent());
        Files.write(file, (Json.JSON.writeValueAsString(r) + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /** The resident set of {@code pid} from {@code /proc} on Linux; -1 where it cannot be read. */
    static long rss(long pid) {
        Path status = Path.of("/proc/" + pid + "/status");
        if (!Files.isReadable(status)) return -1;
        try {
            for (String l : Files.readAllLines(status)) if (l.startsWith("VmRSS:")) return Long.parseLong(l.replaceAll("[^0-9]", "")) * 1024;
        } catch (IOException | NumberFormatException ignored) { /* the process ended */ }
        return -1;
    }

    /** Records and prints the resources after run {@code run}; a failure to measure is printed, never a gate failure. */
    static void after(String run, yggdrasil.forseti.cli.replay.ReplayEngine e, JsonNode runSummary, JsonNode released, PrintStream out) {
        Path f = file();
        if (f == null) return;
        try {
            ObjectNode r = record(run, harness(), e.resources(), runSummary, released);
            append(f, r);
            out.println(line(r));
        } catch (Exception | Error x) { out.println("    memory " + run + ": not measured: " + x); }
    }
}
