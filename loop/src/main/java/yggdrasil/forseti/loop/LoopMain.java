package yggdrasil.forseti.loop;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.cli.live.*;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;

/** {@code forseti-loop}: the live loop's commands around the Kafka-free rules of {@code cli/live}.
 *
 *  <pre>
 *  forseti-loop topics    --kafka host:9092 [--prefix run-1.] [--client-id forseti-loop-admin]
 *  forseti-loop preflight --kafka host:9092 [--prefix run-1.] --registry fixtures/live/registry --definitions fixtures/live/definitions
 *                         --stream line2-dedicated --routing fixtures/live/routing/dedicated.json --pins pins.json
 *                         --muninn-period-ms 500 --scan-ms 250 --opcua opc.tcp://localhost:48400
 *                         --edge edge-s1=line2:station1 ... (one per routed device)
 *                         [--bridge-client-id forseti-bridge] [--driver-client-id forseti-driver]
 *  forseti-loop drive     --kafka host:9092 [--prefix run-1.] --registry ... --definitions ... --stream line2-dedicated
 *                         --routing fixtures/live/routing/dedicated.json --scenario fixtures/live-scenarios/baseline.json
 *                         --opcua opc.tcp://localhost:48400 [--driver-client-id forseti-driver] [--log drive.jsonl]
 *                         [--poll-ms 10] [--timeout-ms 10000]
 *  forseti-loop record    --kafka host:9092 [--prefix run-1.] --registry ... --definitions ... --stream line2-dedicated
 *                         --routing fixtures/live/routing/dedicated.json --scenario fixtures/live-scenarios/baseline.json
 *                         --drive-log drive.jsonl --out build/run-1/recording [--name baseline] [--batch-ms 60000]
 *                         [--report <out>.capture-report.json] [--dead-letter <out>.dead-letter.jsonl]
 *                         [--client-id forseti-recorder] [--timeout-ms 60000] [--injection kind=spec ...]
 *  forseti-loop deployment --registry ... --definitions ... --stream line2-dedicated --routing fixtures/live/routing/dedicated.json
 *                         --pins-out pins.json
 *  forseti-loop stack-check --lock live/stack-lock.json [--compose live/docker-compose.yml] [--jar name=file ...]
 *                         [--image service=repoDigest[,repoDigest] ...]
 *  </pre>
 *  {@code deployment} writes the bridge's pins and prints what the runner starts, one Muninn per routed device ({@link LiveDeployment});
 *  {@code stack-check} compares the compose file, the given jars and the observed images with the stack lock ({@link StackLock}) and
 *  exits 1 naming every difference.
 *  {@code record} runs after the drive, once the publishers have stopped and the bridge has drained: it reads the run's input topics
 *  at their end offsets and writes the recording, the capture report and the dead-letter side file (exit 1 when the capture is
 *  refused); each {@code --injection} is one of the run's injection options ({@link LiveInjections}), declared in the manifest.
 *  {@code topics} creates the run's five topics and checks them as preflight does; {@code preflight} prints every problem and exits 1
 *  if there is one, 0 when clean; {@code drive} runs a live scenario and writes its drive log. A usage error exits 2. An edge's device
 *  is a full path or a path below the site ({@code line2:station1}); {@code --muninn-period-ms} is Muninn's {@code --session} period
 *  and {@code --scan-ms} the sim's {@code bifrost.sim.scanMs}, both as the run starts them (the scenario's {@code muninnPeriodMs} and
 *  {@code scanMs}), checked against the rule's {@code expectedPeriodMs} ({@link Preflight#periodProblem}). {@code preflight} and
 *  {@code drive} also take {@code [--deployment fixtures/live/deployment.json]}, the device -> definition binding, by default the file
 *  next to {@code --definitions}. */
public final class LoopMain {
    private LoopMain() {}

    static final class Usage extends RuntimeException { Usage(String m) { super(m); } }

    public static void main(String[] args) throws Exception {
        int code;
        try { code = run(args, System.out); }
        catch (Usage u) { System.err.println("forseti-loop: " + u.getMessage()); code = 2; }
        System.exit(code);
    }

    static int run(String[] args, PrintStream out) throws Exception {
        if (args.length == 0) throw new Usage("a command is required: topics, preflight, drive, record, deployment or stack-check");
        Map<String, List<String>> a = options(Arrays.copyOfRange(args, 1, args.length));
        switch (args[0]) {
            case "topics" -> {
                allowed(a, "kafka", "prefix", "client-id");
                List<LiveTopics.Spec> plan = LiveTopics.plan(opt(a, "prefix", ""));
                try (TopicAdmin admin = new KafkaTopicAdmin(req(a, "kafka"), opt(a, "client-id", "forseti-loop-admin"))) { return topics(admin, plan, out); }
            }
            case "preflight" -> {
                allowed(a, "kafka", "prefix", "registry", "definitions", "deployment", "stream", "routing", "pins", "muninn-period-ms", "scan-ms", "opcua", "edge",
                    "bridge-client-id", "driver-client-id");
                Config c = config(a);
                try (TopicAdmin admin = new KafkaTopicAdmin(req(a, "kafka"), c.driverClientId + "-preflight");
                     SimPort sim = MiloSimPort.connect(req(a, "opcua"), c.driverClientId + "-preflight")) {
                    List<String> problems = Preflight.problems(facts(c, admin, sim));
                    return report(problems, out);
                }
            }
            case "drive" -> {
                allowed(a, "kafka", "prefix", "registry", "definitions", "deployment", "stream", "routing", "scenario", "opcua", "driver-client-id", "log", "poll-ms", "timeout-ms");
                Path registry = Path.of(req(a, "registry")), definitions = Path.of(req(a, "definitions"));
                LiveEquipment.LiveStream stream = LiveEquipment.read(registry, definitions, deployment(a, definitions), req(a, "stream"));
                LiveScenario scenario = LiveScenario.read(Json.read(Path.of(req(a, "scenario"))), stream);
                String periods = Preflight.periodProblem(scenario.muninnPeriodMs(), scenario.scanMs(), stream.expectedPeriodMs());
                if (periods != null) throw new Usage("scenario " + scenario.name() + ": " + periods);
                RoutingManifest routing = RoutingManifest.read(Json.read(Path.of(req(a, "routing"))));
                List<DrivePlan.Step> steps = DrivePlan.plan(scenario, stream);
                String clientId = opt(a, "driver-client-id", "forseti-driver");
                Path logFile = a.containsKey("log") ? Path.of(req(a, "log")) : null;
                try (InputProducer producer = new InputProducer(InputProducer.kafka(req(a, "kafka"), clientId), routing, stream, LiveTopics.plan(opt(a, "prefix", "")));
                     SimPort sim = MiloSimPort.connect(req(a, "opcua"), clientId);
                     BufferedWriter log = logFile == null ? null : Files.newBufferedWriter(logFile, StandardCharsets.UTF_8)) {
                    Driver d = new Driver(sim, producer, Driver.Clock.SYSTEM, scenario, stream, number(a, "poll-ms", 10), number(a, "timeout-ms", 10_000), line -> {
                        if (log == null) return;
                        try { log.write(new String(InputProducer.bytes(line), StandardCharsets.UTF_8)); log.write('\n'); log.flush(); }
                        catch (IOException e) { throw new IllegalStateException("drive log: " + e.getMessage(), e); }
                    });
                    try { d.run(steps); }
                    catch (Driver.DriveFailure f) { out.println("drive failed: " + f.getMessage()); return 1; }
                    out.println("drive: " + steps.size() + " steps of " + scenario.name() + " on " + stream.stream());
                    return 0;
                }
            }
            case "record" -> {
                allowed(a, "kafka", "prefix", "registry", "definitions", "deployment", "stream", "routing", "scenario", "drive-log", "out", "name", "batch-ms",
                    "report", "dead-letter", "client-id", "timeout-ms", "injection");
                Path definitions = Path.of(req(a, "definitions")), out1 = Path.of(req(a, "out"));
                CaptureRecorder.Inputs in = LiveCapture.inputs(Path.of(req(a, "registry")), definitions, deployment(a, definitions), req(a, "stream"),
                    Path.of(req(a, "routing")), Path.of(req(a, "scenario")), Path.of(req(a, "drive-log")), a.containsKey("name") ? req(a, "name") : null,
                    number(a, "batch-ms", CaptureRecorder.DEFAULT_BATCH_MS), a.getOrDefault("injection", List.of()));
                try (KafkaCaptureSource src = new KafkaCaptureSource(KafkaCaptureSource.kafka(req(a, "kafka"), opt(a, "client-id", "forseti-recorder")), number(a, "timeout-ms", 60_000))) {
                    return record(src, opt(a, "prefix", ""), in, out1, a.containsKey("report") ? Path.of(req(a, "report")) : CaptureRecorder.besides(out1, CaptureRecorder.REPORT_SUFFIX),
                        a.containsKey("dead-letter") ? Path.of(req(a, "dead-letter")) : CaptureRecorder.besides(out1, CaptureRecorder.DEAD_LETTER_SUFFIX), out);
                }
            }
            case "deployment" -> {
                allowed(a, "registry", "definitions", "deployment", "stream", "routing", "pins-out");
                Path definitions = Path.of(req(a, "definitions"));
                LiveDeployment.Deployment d = LiveDeployment.of(Path.of(req(a, "registry")), definitions, deployment(a, definitions), req(a, "stream"),
                    Path.of(req(a, "routing")));
                Json.write(Path.of(req(a, "pins-out")).toAbsolutePath(), d.pins());
                for (String l : LiveDeployment.lines(d)) out.println(l);
                return 0;
            }
            case "stack-check" -> {
                allowed(a, "lock", "compose", "jar", "image");
                return stackCheck(StackLock.read(Json.read(Path.of(req(a, "lock")))), a.containsKey("compose") ? StackLock.read(Path.of(req(a, "compose"))) : null,
                    pairs(a, "jar"), pairs(a, "image"), out);
            }
            default -> throw new Usage("unknown command " + args[0]);
        }
    }

    /** Records a run's input topics ({@link CaptureRecorder}); a refused capture prints its reason and exits 1. */
    static int record(CaptureRecorder.Source src, String prefix, CaptureRecorder.Inputs in, Path out, Path report, Path deadLetter, PrintStream ps) {
        try {
            CaptureRecorder.Result r = CaptureRecorder.record(src, prefix, in, out, report, deadLetter);
            ps.println("recorded " + in.name() + ": cuts " + r.manifest().path("batches").size() + ", deadlines " + r.manifest().path("deadlines").size()
                + ", manifest " + r.manifestSha256() + ", report " + report);
            return 0;
        } catch (CaptureRecorder.Refused e) { ps.println(e.getMessage()); return 1; }
    }

    /** The stack lock against the compose file (when given), the jars (when any is given) and the observed images (when any is given). */
    static int stackCheck(StackLock.Lock lock, String compose, Map<String, String> jars, Map<String, String> images, PrintStream out) {
        List<String> problems = new ArrayList<>();
        if (compose != null) problems.addAll(StackLock.composeProblems(lock, compose));
        if (!jars.isEmpty()) {
            Map<String, Path> files = new TreeMap<>();
            jars.forEach((k, v) -> files.put(k, Path.of(v)));
            problems.addAll(StackLock.jarProblems(lock, files));
        }
        if (!images.isEmpty()) {
            Map<String, List<String>> observed = new TreeMap<>();
            images.forEach((k, v) -> observed.put(k, v.isEmpty() ? List.of() : List.of(v.split(","))));
            problems.addAll(StackLock.imageProblems(lock, observed));
        }
        if (problems.isEmpty()) {
            out.println("stack-check: clean (" + (compose != null ? "compose, " : "") + jars.size() + " jars, " + images.size() + " images)");
            return 0;
        }
        for (String p : problems) out.println("problem: " + p);
        return 1;
    }

    /** {@code --<key> name=value}, repeatable; a name given twice is a usage error. */
    static Map<String, String> pairs(Map<String, List<String>> a, String key) {
        Map<String, String> m = new TreeMap<>();
        for (String v : a.getOrDefault(key, List.of())) {
            int eq = v.indexOf('=');
            if (eq <= 0) throw new Usage("--" + key + " takes name=value, not " + v);
            if (m.put(v.substring(0, eq), v.substring(eq + 1)) != null) throw new Usage("--" + key + " names " + v.substring(0, eq) + " twice");
        }
        return m;
    }

    /** Creates the planned topics, then checks the broker's view of them with the preflight's topic rules. */
    static int topics(TopicAdmin admin, List<LiveTopics.Spec> plan, PrintStream out) {
        admin.create(plan);
        List<String> problems = LiveTopics.problems(plan, admin.describe(plan.stream().map(LiveTopics.Spec::name).toList()));
        if (problems.isEmpty()) for (LiveTopics.Spec s : plan) out.println("created " + s.name() + " (" + s.partitions() + " partitions, " + s.configs() + ")");
        return report(problems, out);
    }

    static int report(List<String> problems, PrintStream out) {
        if (problems.isEmpty()) { out.println("preflight: clean"); return 0; }
        for (String p : problems) out.println("problem: " + p);
        return 1;
    }

    /** The configuration of a preflight. */
    record Config(Path registry, Path definitions, Path deployment, String stream, Path routing, Path pins, long muninnPeriodMs, long scanMs, String prefix,
                  String bridgeClientId, String driverClientId, List<String[]> edges) {}

    static Config config(Map<String, List<String>> a) {
        List<String[]> edges = new ArrayList<>();
        for (String e : a.getOrDefault("edge", List.of())) {
            int eq = e.indexOf('=');
            if (eq < 0) throw new Usage("--edge takes edge=device, not " + e);
            edges.add(new String[] {e.substring(0, eq), e.substring(eq + 1)});
        }
        return new Config(Path.of(req(a, "registry")), Path.of(req(a, "definitions")), deployment(a, Path.of(req(a, "definitions"))), req(a, "stream"), Path.of(req(a, "routing")), Path.of(req(a, "pins")),
            number(a, "muninn-period-ms", -1), number(a, "scan-ms", -1), opt(a, "prefix", ""), opt(a, "bridge-client-id", "forseti-bridge"),
            opt(a, "driver-client-id", "forseti-driver"), edges);
    }

    /** Gathers the facts of a preflight from the files, the broker and the sim. */
    static Preflight.Facts facts(Config c, TopicAdmin admin, SimPort sim) {
        LiveEquipment.LiveStream stream = LiveEquipment.read(c.registry(), c.definitions(), c.deployment(), c.stream());
        String kind = Preflight.placementOf(c.stream());
        JsonNode routing = readOrNull(c.routing());
        JsonNode recomputed = null;
        String recomputeError = null;
        try { recomputed = Placement.fixture(c.registry(), kind); } catch (RuntimeException e) { recomputeError = e.getMessage(); }
        String site = siteOf(stream);
        List<Preflight.Edge> edges = new ArrayList<>();
        for (String[] e : c.edges()) edges.add(new Preflight.Edge(e[0], e[1].startsWith(site + ":") ? e[1] : site + ":" + e[1]));
        List<LiveTopics.Spec> plan = LiveTopics.plan(c.prefix());
        Map<String, LiveTopics.Observed> observed = admin.describe(plan.stream().map(LiveTopics.Spec::name).toList());
        Set<String> present = new TreeSet<>();
        Map<String, List<String>> byNamespace = new TreeMap<>();
        for (LiveEquipment.Equipment e : stream.equipment().values())
            byNamespace.computeIfAbsent(e.namespaceUri(), k -> new ArrayList<>()).addAll(Preflight.requiredNodes(stream, List.of(e.device())));
        for (Map.Entry<String, List<String>> ns : byNamespace.entrySet()) {
            List<SimPort.Value> vs = sim.read(ns.getKey(), ns.getValue());
            for (int i = 0; i < vs.size(); i++) if (vs.get(i).exists()) present.add(ns.getValue().get(i));
        }
        Map<String, String> pins = new TreeMap<>();
        JsonNode p = readOrNull(c.pins());
        if (p != null && p.isObject()) p.fields().forEachRemaining(e -> pins.put(e.getKey(), e.getValue().asText()));
        return new Preflight.Facts(stream, kind, routing, recomputed, recomputeError, c.bridgeClientId(), c.driverClientId(), edges,
            c.muninnPeriodMs(), c.scanMs(), plan, observed, present, pins);
    }

    private static String siteOf(LiveEquipment.LiveStream s) {
        String d = s.equipment().firstKey();
        return d.substring(0, d.indexOf(':'));
    }

    /** {@code --deployment}, or the binding committed next to the definitions ({@code fixtures/live/deployment.json}). */
    private static Path deployment(Map<String, List<String>> a, Path definitions) {
        return a.containsKey("deployment") ? Path.of(a.get("deployment").get(0)) : LiveEquipment.deploymentOf(definitions);
    }

    private static JsonNode readOrNull(Path p) {
        try { return Json.read(p); } catch (RuntimeException e) { return null; }
    }

    private static final Set<String> REPEATABLE = Set.of("edge", "jar", "image", "injection");

    static Map<String, List<String>> options(String[] args) {
        Map<String, List<String>> m = new TreeMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) throw new Usage("unexpected argument " + args[i]);
            String k = args[i].substring(2);
            m.computeIfAbsent(k, x -> new ArrayList<>()).add(args[++i]);
            if (!REPEATABLE.contains(k) && m.get(k).size() > 1) throw new Usage("--" + k + " given twice");
        }
        return m;
    }

    private static void allowed(Map<String, List<String>> a, String... keys) {
        Set<String> ok = Set.of(keys);
        for (String k : a.keySet()) if (!ok.contains(k)) throw new Usage("unknown option --" + k);
    }

    private static String req(Map<String, List<String>> a, String k) {
        List<String> v = a.get(k);
        if (v == null) throw new Usage("--" + k + " is required");
        return v.get(0);
    }

    private static String opt(Map<String, List<String>> a, String k, String dflt) { return a.containsKey(k) ? a.get(k).get(0) : dflt; }

    private static long number(Map<String, List<String>> a, String k, long dflt) {
        if (!a.containsKey(k)) { if (dflt < 0) throw new Usage("--" + k + " is required"); return dflt; }
        try { return Long.parseLong(a.get(k).get(0)); } catch (NumberFormatException e) { throw new Usage("--" + k + " is not a number"); }
    }
}
