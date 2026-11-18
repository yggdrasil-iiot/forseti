package yggdrasil.forseti.engine.flink.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.jobmanager.JobManagerProcessSpec;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import yggdrasil.forseti.cli.replay.ProcessSupervisor;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.engine.flink.job.ApplicationEntryPoint;
import yggdrasil.forseti.engine.flink.job.JobSettings;
import yggdrasil.forseti.engine.flink.job.Topology;

/**
 * One job's application-mode cluster (plan D7): one JobManager JVM ({@link ApplicationEntryPoint}, which runs the job's {@code main})
 * and one TaskManager JVM (Flink's {@code TaskManagerRunner}), both children of the harness through its {@link ProcessSupervisor}, on a
 * configuration directory of their own ({@code config.yaml}), with the class path of the engine, no Flink distribution and no Docker
 * image. The JVM memory flags and the TaskManager's dynamic memory properties are derived with Flink's own process-memory utilities, as
 * the distribution's scripts derive them. The harness talks to the cluster over its REST endpoint only: the job (its id fixed) and all its
 * tasks RUNNING, the job's exceptions, heap metrics, and the cancel that stops it; the TaskManager is stopped after the JobManager has
 * exited. Every process's resident set is read from the operating system ({@code /proc/<pid>/status} on Linux, the process's working
 * set on Windows).
 */
final class FlinkCluster implements AutoCloseable {
  static final String TASK_MANAGER = "org.apache.flink.runtime.taskexecutor.TaskManagerRunner";
  /** The JVM options the Flink distribution gives every JVM on Java 17 ({@code env.java.opts.all}). */
  static final List<String> JAVA_17_OPTS = List.of("-XX:+IgnoreUnrecognizedVMOptions", "--add-exports=java.base/sun.net.util=ALL-UNNAMED",
      "--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED", "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.net=ALL-UNNAMED", "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.text=ALL-UNNAMED", "--add-opens=java.base/java.time=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED", "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED");
  /** The JVMs log at INFO (Kafka clients at WARN) to their log file through slf4j-simple. */
  static final List<String> LOGGING = List.of("-Dorg.slf4j.simpleLogger.defaultLogLevel=info", "-Dorg.slf4j.simpleLogger.showDateTime=true",
      "-Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS", "-Dorg.slf4j.simpleLogger.log.org.apache.kafka=warn");

  /** The memory of one cluster: explicit components, so the JVM flags never depend on a fraction of a host default. A component is
   *  {@code forseti.flink.<key>} (a system property), overridden per stage by {@code forseti.flink.stage<N>.<key>} and then by the
   *  shard's engine settings ({@code "stage<N>": {"<key>": "5g"}} in {@code gate/flink-shards.json}: the horizon shards' stage-1 task
   *  heap, measured on CI). */
  record Memory(String jmHeap, String jmOffHeap, String jmMetaspace, String jmOverhead, String tmFrameworkHeap, String tmTaskHeap, String tmFrameworkOffHeap,
                String tmManaged, String tmNetwork, String tmMetaspace, String tmOverhead) {
    static Memory defaults() { return forStage(0, null); }

    static Memory forStage(int stage, JsonNode settings) {
      JsonNode s = settings == null ? null : settings.path("stage" + stage);
      return new Memory(prop(stage, s, "jm.heap", "192m"), prop(stage, s, "jm.offheap", "64m"), prop(stage, s, "jm.metaspace", "160m"), prop(stage, s, "jm.overhead", "96m"),
          prop(stage, s, "tm.framework.heap", "64m"), prop(stage, s, "tm.task.heap", "224m"), prop(stage, s, "tm.framework.offheap", "64m"),
          prop(stage, s, "tm.managed", "128m"), prop(stage, s, "tm.network", "64m"), prop(stage, s, "tm.metaspace", "192m"), prop(stage, s, "tm.overhead", "128m"));
    }

    private static String prop(int stage, JsonNode shard, String k, String d) {
      String v = System.getProperty("forseti.flink." + k, d);
      v = System.getProperty("forseti.flink.stage" + stage + "." + k, v);
      if (shard != null && shard.path(k).isTextual()) v = shard.get(k).textValue();
      return v;
    }

    ObjectNode toJson() {
      return Json.object().put("jm.heap", jmHeap).put("jm.offheap", jmOffHeap).put("jm.metaspace", jmMetaspace).put("jm.overhead", jmOverhead)
          .put("tm.framework.heap", tmFrameworkHeap).put("tm.task.heap", tmTaskHeap).put("tm.framework.offheap", tmFrameworkOffHeap).put("tm.managed", tmManaged)
          .put("tm.network", tmNetwork).put("tm.metaspace", tmMetaspace).put("tm.overhead", tmOverhead);
    }
  }

  /** How often a cluster is started when its JobManager could not bind a port it was given: the ports are taken free and handed to
   *  the JobManager's configuration, so another process can take one in between; such a start never reached the job (the REST
   *  endpoint and the RPC service come up before the dispatcher), and it is started again on fresh ports, each attempt recorded. */
  static final int MAX_STARTS = 3;

  final String name;
  final Path dir;
  final JobID jobId;
  private final ProcessSupervisor sup;
  private final String jobClass, checkpointUri;
  private final Path jobSpec;
  private final Memory mem;
  private final java.util.function.IntSupplier ports;
  private volatile int restPort;
  private volatile ProcessSupervisor.Supervised jm, tm;
  private int starts;
  private boolean jobSeen;
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  final ObjectNode record = Json.object();
  private final long startedNanos = System.nanoTime();
  long runningAfterMs = -1;
  private final Map<String, Long> heapPeak = new TreeMap<>();

  private final Path tmp;

  private FlinkCluster(ProcessSupervisor sup, String name, Path dir, String jobClass, Path jobSpec, String checkpointUri, Memory mem,
                       java.util.function.IntSupplier ports, Path tmp) {
    this.sup = sup; this.name = name; this.dir = dir; this.jobClass = jobClass; this.jobSpec = jobSpec; this.checkpointUri = checkpointUri;
    this.mem = mem; this.ports = ports; this.tmp = tmp; this.jobId = JobID.generate();
  }

  int restPort() { return restPort; }

  /** Starts the cluster of {@code jobClass} on {@code jobSpec} under {@code dir}; {@code checkpointDir} as a file URI. */
  static FlinkCluster start(ProcessSupervisor sup, String name, Path dir, String jobClass, Path jobSpec, String checkpointUri, Memory mem) {
    return start(sup, name, dir, jobClass, jobSpec, checkpointUri, mem, FlinkCluster::freePort);
  }

  /** {@link #start}, the ports taken from {@code ports}. */
  static FlinkCluster start(ProcessSupervisor sup, String name, Path dir, String jobClass, Path jobSpec, String checkpointUri, Memory mem,
                            java.util.function.IntSupplier ports) {
    // the working and RocksDB directories in a short temporary directory: RocksDB refuses paths over 247 characters on Windows, and
    // the per-operator directory names alone take about 150
    Path tmp;
    try {
      Files.createDirectories(dir.resolve("conf"));
      tmp = Files.createTempDirectory("fst-" + name + "-");
    } catch (IOException e) { throw new UncheckedIOException(e); }
    FlinkCluster f = new FlinkCluster(sup, name, dir, jobClass, jobSpec, checkpointUri, mem, ports, tmp);
    f.record.put("jobId", f.jobId.toHexString()).put("jobClass", jobClass);
    f.record.putArray("startAttempts");
    f.launch();
    return f;
  }

  /** One start of both JVMs on fresh REST and RPC ports, the same job id, job spec and checkpoint directory. */
  private void launch() {
    starts++;
    int rest = ports.getAsInt(), rpc = ports.getAsInt();
    Configuration c = Topology.configuration(checkpointUri);
    c.setString("jobmanager.rpc.address", "localhost");
    c.setString("jobmanager.bind-host", "localhost");
    c.setString("jobmanager.rpc.port", Integer.toString(rpc));
    c.set(RestOptions.ADDRESS, "localhost");
    c.set(RestOptions.BIND_ADDRESS, "localhost");
    c.set(RestOptions.PORT, rest);
    c.setString("taskmanager.host", "localhost");
    c.setString("taskmanager.bind-host", "localhost");
    c.setString("taskmanager.rpc.port", "0");
    // a fixed resource id: the default one carries host:port, which is not a directory name on Windows
    c.setString("taskmanager.resource-id", name + "-taskmanager");
    c.setString("taskmanager.data.port", "0");
    c.setString("blob.server.port", "0");
    c.set(TaskManagerOptions.NUM_TASK_SLOTS, JobSettings.KEYED_PARALLELISM);
    c.setString("parallelism.default", "1");
    c.set(CoreOptions.TMP_DIRS, tmp.toAbsolutePath().toString());
    c.setString("high-availability.type", "NONE");
    c.setString("web.submit.enable", "false");
    c.setString("web.cancel.enable", "true");
    c.setString("execution.shutdown-on-application-finish", "true");
    c.set(JobManagerOptions.JVM_HEAP_MEMORY, MemorySize.parse(mem.jmHeap()));
    c.set(JobManagerOptions.OFF_HEAP_MEMORY, MemorySize.parse(mem.jmOffHeap()));
    c.set(JobManagerOptions.JVM_METASPACE, MemorySize.parse(mem.jmMetaspace()));
    c.set(JobManagerOptions.JVM_OVERHEAD_MIN, MemorySize.parse(mem.jmOverhead()));
    c.set(JobManagerOptions.JVM_OVERHEAD_MAX, MemorySize.parse(mem.jmOverhead()));
    c.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.parse(mem.tmFrameworkHeap()));
    c.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.parse(mem.tmTaskHeap()));
    c.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.parse(mem.tmFrameworkOffHeap()));
    c.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ZERO);
    c.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse(mem.tmManaged()));
    c.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse(mem.tmNetwork()));
    c.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse(mem.tmNetwork()));
    c.set(TaskManagerOptions.JVM_METASPACE, MemorySize.parse(mem.tmMetaspace()));
    c.set(TaskManagerOptions.JVM_OVERHEAD_MIN, MemorySize.parse(mem.tmOverhead()));
    c.set(TaskManagerOptions.JVM_OVERHEAD_MAX, MemorySize.parse(mem.tmOverhead()));
    writeConfig(dir.resolve("conf/config.yaml"), c);

    JobManagerProcessSpec jmSpec = JobManagerProcessUtils.processSpecFromConfig(c);
    TaskExecutorProcessSpec tmSpec = TaskExecutorProcessUtils.processSpecFromConfig(c);
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString(), cp = classPath();
    String conf = dir.resolve("conf").toAbsolutePath().toString();
    List<String> jmCmd = new ArrayList<>(List.of(java));
    jmCmd.addAll(split(JobManagerProcessUtils.generateJvmParametersStr(jmSpec, c)));
    jmCmd.addAll(JAVA_17_OPTS);
    jmCmd.addAll(LOGGING);
    jmCmd.addAll(List.of("-cp", cp, ApplicationEntryPoint.class.getName(), conf, jobId.toHexString(), jobClass, jobSpec.toAbsolutePath().toString()));
    List<String> tmCmd = new ArrayList<>(List.of(java));
    tmCmd.addAll(split(ProcessMemoryUtils.generateJvmParametersStr(tmSpec)));
    tmCmd.addAll(JAVA_17_OPTS);
    tmCmd.addAll(LOGGING);
    tmCmd.addAll(List.of("-cp", cp, TASK_MANAGER, "--configDir", conf));
    tmCmd.addAll(split(TaskExecutorProcessUtils.generateDynamicConfigsStr(tmSpec)));
    restPort = rest;
    jm = sup.start(name + "-jobmanager", jmCmd, Map.of(), dir.resolve("jobmanager.log"));
    tm = sup.start(name + "-taskmanager", tmCmd, Map.of(), dir.resolve("taskmanager.log"));
    record.put("restPort", rest).put("rpcPort", rpc).put("starts", starts);
    Json.putSafe(record, "jobManagerPid", jm.pid());
    Json.putSafe(record, "taskManagerPid", tm.pid());
    ArrayNode jc = record.putArray("jobManagerCommand"), tc = record.putArray("taskManagerCommand");
    jmCmd.forEach(x -> jc.add(x.equals(cp) ? "<engine class path>" : x));
    tmCmd.forEach(x -> tc.add(x.equals(cp) ? "<engine class path>" : x));
  }

  /** The line of the JobManager's log that says it could not bind a port, null when it says no such thing. */
  static String bindFailure(Path jobManagerLog) {
    try (java.util.stream.Stream<String> l = Files.lines(jobManagerLog, StandardCharsets.UTF_8)) {
      return l.filter(x -> x.contains("java.net.BindException")).findFirst().map(String::trim).orElse(null);
    } catch (IOException | UncheckedIOException e) { return null; }
  }

  /** After a start whose JobManager exited on a port it could not bind, before the job was ever reported: the attempt is recorded
   *  (its ports, the bind failure, its logs kept as {@code <jvm>.start-<n>.log}) and both JVMs are started again on fresh ports. The
   *  same cluster, job id and run: nothing of the job ran. Returns false when the exit was anything else, or the starts are used up. */
  private boolean restartOnBindFailure() {
    if (jobSeen || jm.alive() || starts >= MAX_STARTS) return false;
    String bind = bindFailure(jm.log());
    if (bind == null) return false;
    if (tm.alive()) { tm.signal(ProcessSupervisor.Signal.KILL); tm.waitFor(Duration.ofSeconds(30)); }
    jm.waitFor(Duration.ofSeconds(30));
    ObjectNode a = ((ArrayNode) record.get("startAttempts")).addObject().put("start", starts).put("restPort", restPort).put("rpcPort", record.path("rpcPort").asInt())
        .put("cause", bind);
    for (ProcessSupervisor.Supervised p : List.of(jm, tm)) {
      Path kept = p.log().resolveSibling(p.log().getFileName().toString().replace(".log", ".start-" + starts + ".log"));
      try { Files.move(p.log(), kept, java.nio.file.StandardCopyOption.REPLACE_EXISTING); a.put(p == jm ? "jobManagerLog" : "taskManagerLog", kept.getFileName().toString()); }
      catch (IOException e) { a.put(p == jm ? "jobManagerLog" : "taskManagerLog", "not kept: " + e.getMessage()); }
    }
    launch();
    return true;
  }

  private static List<String> split(String s) {
    List<String> l = new ArrayList<>();
    for (String x : s.trim().split("\\s+")) if (!x.isEmpty()) l.add(x);
    return l;
  }

  /** The class path of the engine: the jars of the engine loader this class came from, else this JVM's own class path. */
  static String classPath() {
    ClassLoader l = FlinkCluster.class.getClassLoader();
    if (l instanceof URLClassLoader u && u.getURLs().length > 0) {
      List<String> parts = new ArrayList<>();
      for (URL url : u.getURLs()) {
        try { parts.add(Path.of(url.toURI()).toString()); } catch (Exception e) { throw new IllegalStateException(url + ": " + e.getMessage(), e); }
      }
      return String.join(File.pathSeparator, parts);
    }
    return System.getProperty("java.class.path");
  }

  private static void writeConfig(Path f, Configuration c) {
    StringBuilder b = new StringBuilder("# written by the forseti replay harness for one application-mode cluster (plan D7)\n");
    for (Map.Entry<String, String> e : new TreeMap<>(c.toMap()).entrySet())
      b.append(e.getKey()).append(": \"").append(e.getValue().replace("\\", "\\\\").replace("\"", "\\\"")).append("\"\n");
    try { Files.writeString(f, b.toString(), StandardCharsets.UTF_8); } catch (IOException e) { throw new UncheckedIOException(e); }
  }

  static int freePort() {
    try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); } catch (IOException e) { throw new IllegalStateException(e); }
  }

  // --- REST --------------------------------------------------------------------------------------------------------------------

  private JsonNode get(String path) {
    try {
      HttpResponse<byte[]> r = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + restPort + path)).timeout(Duration.ofSeconds(10)).GET().build(),
          HttpResponse.BodyHandlers.ofByteArray());
      if (r.statusCode() / 100 != 2) return null;
      return Json.read(r.body());
    } catch (IOException e) { return null; }
    catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
  }

  /** The job's state and whether every vertex is RUNNING; null while the REST endpoint does not answer or the job is not submitted. */
  record Status(String state, boolean allRunning, String detail) {}

  Status status() {
    JsonNode j = get("/jobs/" + jobId.toHexString());
    if (j == null) return null;
    boolean all = true;
    StringBuilder d = new StringBuilder();
    for (JsonNode v : j.path("vertices")) {
      if (!"RUNNING".equals(v.path("status").asText())) all = false;
      d.append(v.path("name").asText()).append('=').append(v.path("status").asText()).append(' ');
    }
    return new Status(j.path("state").asText(), all && j.path("vertices").size() > 0, d.toString().trim());
  }

  /** Throws with the job's failure when either JVM ended or the job left RUNNING. */
  void requireAlive() {
    if (!jm.alive() || !tm.alive()) throw new IllegalStateException(name + ": " + (jm.alive() ? "" : "JobManager exited ") + (tm.alive() ? "" : "TaskManager exited ") + failure());
    Status s = status();
    if (s != null && !s.state().equals("RUNNING")) throw new IllegalStateException(name + ": job " + s.state() + " " + failure());
  }

  /** Waits until the job and every one of its tasks report RUNNING (spec 418: the harness writes a generation's first batch only then). */
  void awaitRunning(Duration timeout) {
    long until = System.nanoTime() + timeout.toNanos();
    Status last = null;
    while (System.nanoTime() < until) {
      if (!jm.alive() || !tm.alive()) {
        if (restartOnBindFailure()) continue;
        throw new IllegalStateException(name + " ended before RUNNING (start " + starts + "): " + tail());
      }
      last = status();
      if (last != null) jobSeen = true;
      if (last != null && (last.state().equals("FAILED") || last.state().equals("CANCELED") || last.state().equals("FINISHED")))
        throw new IllegalStateException(name + ": job " + last.state() + " before RUNNING " + failure());
      if (last != null && last.state().equals("RUNNING") && last.allRunning()) {
        runningAfterMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
        Json.putSafe(record, "runningAfterMs", runningAfterMs);
        return;
      }
      sleep(200);
    }
    throw new IllegalStateException(name + ": not RUNNING within " + timeout + " (last " + last + ") " + tail());
  }

  String failure() {
    JsonNode e = get("/jobs/" + jobId.toHexString() + "/exceptions");
    String root = e == null ? "" : e.path("rootException").asText("");
    if (root.isEmpty() && e != null) root = e.path("exceptionHistory").path("entries").path(0).path("stacktrace").asText("");
    return root.isEmpty() ? tail() : root.lines().limit(40).reduce((a, b) -> a + "\n" + b).orElse("");
  }

  private String tail() {
    StringBuilder b = new StringBuilder();
    for (Path p : List.of(jm.log(), tm.log())) {
      try {
        List<String> l = Files.readAllLines(p, StandardCharsets.UTF_8);
        b.append("\n--- ").append(p.getFileName()).append('\n').append(String.join("\n", l.subList(Math.max(0, l.size() - 30), l.size())));
      } catch (IOException | UncheckedIOException ignored) { /* no log yet */ }
    }
    return b.toString();
  }

  /** One sample of the heap metrics of the JobManager and every TaskManager; the peaks are kept. */
  void sampleHeap() {
    heap("jobManager", get("/jobmanager/metrics?get=Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max"));
    JsonNode tms = get("/taskmanagers");
    if (tms != null) for (JsonNode t : tms.path("taskmanagers"))
      heap("taskManager", get("/taskmanagers/" + t.path("id").asText() + "/metrics?get=Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max"));
  }

  private void heap(String who, JsonNode metrics) {
    if (metrics == null) return;
    for (JsonNode m : metrics) {
      String id = m.path("id").asText();
      long v;
      try { v = (long) Double.parseDouble(m.path("value").asText()); } catch (NumberFormatException e) { continue; }
      heapPeak.merge(who + (id.endsWith("Used") ? "HeapUsedPeakBytes" : "HeapMaxBytes"), v, Math::max);
    }
  }

  /** The keys of the JobManager's effective configuration the job settings record (run-invariant engine pins, spec 588-590). */
  static final List<String> OBSERVED_KEYS = List.of("execution.checkpointing.interval", "execution.checkpointing.mode", "execution.checkpointing.max-concurrent-checkpoints",
      "execution.checkpointing.incremental", "state.backend.type", "pipeline.auto-watermark-interval", "pipeline.max-parallelism", "restart-strategy.type");

  /** The job's settings as the cluster reports them over REST (reviewer decision 1 of 2026-11-18: observed, not only submitted): the
   *  checkpoint configuration, the selected keys of the JobManager's effective configuration and every vertex's parallelism. */
  ObjectNode observedSettings() {
    ObjectNode n = Json.object();
    JsonNode cp = get("/jobs/" + jobId.toHexString() + "/checkpoints/config");
    n.set("checkpointConfig", cp == null ? com.fasterxml.jackson.databind.node.NullNode.getInstance() : cp);
    ObjectNode conf = n.putObject("configuration");
    JsonNode jc = get("/jobs/" + jobId.toHexString() + "/jobmanager/config");
    if (jc != null) for (JsonNode e : jc) if (OBSERVED_KEYS.contains(e.path("key").asText())) conf.put(e.path("key").asText(), e.path("value").asText());
    ObjectNode v = n.putObject("vertices");
    JsonNode j = get("/jobs/" + jobId.toHexString());
    if (j != null) for (JsonNode x : j.path("vertices")) Json.putSafe(v, x.path("name").asText(), x.path("parallelism").asLong());
    return n;
  }

  /** The job's checkpoint statistics over REST (comparison item 8): counts, and the checkpointed size, state size and end-to-end
   *  duration summaries. */
  ObjectNode checkpointStatistics() {
    ObjectNode n = Json.object();
    JsonNode c = get("/jobs/" + jobId.toHexString() + "/checkpoints");
    if (c == null) return n.put("error", "no checkpoint statistics");
    n.set("counts", c.path("counts"));
    ObjectNode s = n.putObject("summary");
    for (String k : List.of("checkpointed_size", "state_size", "end_to_end_duration")) s.set(k, c.path("summary").path(k));
    return n;
  }

  /** The resident set of both JVMs now and at their peak, as the operating system reports it. */
  ObjectNode memory() {
    ObjectNode n = Json.object();
    heapPeak.forEach((k, v) -> Json.putSafe(n, k, v));
    rss(n, "jobManager", jm.pid());
    rss(n, "taskManager", tm.pid());
    return n;
  }

  private static void rss(ObjectNode n, String who, long pid) {
    Path status = Path.of("/proc/" + pid + "/status");
    if (Files.isReadable(status)) {
      try {
        for (String l : Files.readAllLines(status)) {
          if (l.startsWith("VmRSS:")) Json.putSafe(n, who + "RssBytes", kb(l));
          if (l.startsWith("VmHWM:")) Json.putSafe(n, who + "RssPeakBytes", kb(l));
        }
      } catch (IOException ignored) { /* the process ended */ }
      return;
    }
    if (!System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows")) return;
    try {
      Process p = new ProcessBuilder("powershell", "-NoProfile", "-NonInteractive", "-Command",
          "$p = Get-Process -Id " + pid + "; $p.WorkingSet64; $p.PeakWorkingSet64").redirectErrorStream(true).start();
      String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
      p.waitFor(30, TimeUnit.SECONDS);
      String[] parts = out.split("\\s+");
      if (parts.length == 2) {
        Json.putSafe(n, who + "RssBytes", Long.parseLong(parts[0]));
        Json.putSafe(n, who + "RssPeakBytes", Long.parseLong(parts[1]));
      }
    } catch (IOException | NumberFormatException ignored) { /* not measured */ }
    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
  }

  private static long kb(String line) { return Long.parseLong(line.replaceAll("[^0-9]", "")) * 1024; }

  /** The clean stop: cancel over REST, wait for the JobManager to exit (the application cluster shuts down with its job), then stop
   *  the TaskManager (SIGTERM on a POSIX host, a kill elsewhere) and wait for it. */
  void stop(Duration timeout) {
    try {
      http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + restPort + "/jobs/" + jobId.toHexString() + "?mode=cancel"))
          .timeout(Duration.ofSeconds(10)).method("PATCH", HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding());
    } catch (IOException ignored) { /* the JobManager is gone already */ }
    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    OptionalInt jmExit = jm.waitFor(timeout);
    record.put("jobManagerExit", jmExit.isPresent() ? Integer.toString(jmExit.getAsInt()) : "killed after " + timeout);
    if (jmExit.isEmpty()) jm.signal(ProcessSupervisor.Signal.KILL);
    try { tm.signal(ProcessSupervisor.Signal.TERM); } catch (UnsupportedOperationException e) { tm.signal(ProcessSupervisor.Signal.KILL); }
    OptionalInt tmExit = tm.waitFor(timeout);
    if (tmExit.isEmpty()) { tm.signal(ProcessSupervisor.Signal.KILL); tm.waitFor(timeout); }
    record.put("taskManagerExit", tmExit.isPresent() ? Integer.toString(tmExit.getAsInt()) : "killed after " + timeout);
  }

  private static void sleep(long ms) {
    try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
  }

  @Override public void close() {
    if (jm.alive()) { jm.signal(ProcessSupervisor.Signal.KILL); jm.waitFor(Duration.ofSeconds(30)); }
    if (tm.alive()) { tm.signal(ProcessSupervisor.Signal.KILL); tm.waitFor(Duration.ofSeconds(30)); }
    try (java.util.stream.Stream<Path> w = Files.walk(tmp)) {
      for (Path p : w.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
    } catch (IOException | UncheckedIOException ignored) { /* a temporary directory the operating system cleans up */ }
  }
}
