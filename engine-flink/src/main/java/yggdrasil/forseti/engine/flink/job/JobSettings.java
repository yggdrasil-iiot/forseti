package yggdrasil.forseti.engine.flink.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import yggdrasil.forseti.cli.harness.Controls;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.cli.replay.EngineContract;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.NegativeControls;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.journal.Envelope;
import yggdrasil.forseti.core.journal.Pins;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.release.HeadVerifier;
import yggdrasil.forseti.core.release.Refusal;
import yggdrasil.forseti.core.release.ReleaseReader;
import yggdrasil.forseti.core.rules.RuleSet;
import yggdrasil.forseti.core.stage1.Routing;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.SetCoverage;
import yggdrasil.forseti.core.stage2.Stage2;
import yggdrasil.forseti.replay.kafka.KafkaGenerationStore;

/**
 * What every operator of a job needs, fixed once by the job's main on the JobManager ({@link #prepare}) and shipped to the operators as
 * one canonical JSON text, the only serialized field. Each TaskManager rebuilds its {@link Runtime} from that text once per JVM
 * ({@link #runtime}): the rule set read again from the pinned release (the release reader verifies every file against the digests the
 * generation pinned), the routing manifest, the pins, the decision functions and the required barrier sets.
 *
 * <p>The job refuses to start, as spec 575 requires of every job, on a generation that {@code forseti.generations} does not hold as
 * registered ({@code generation-unregistered}), on a retired one ({@code generation-retired}), and on a rule set whose digest is not
 * the registered one ({@code generation-rules-mismatch}).
 */
public final class JobSettings implements Serializable {
  private static final long serialVersionUID = 1L;
  public static final String ENGINE = "flink", ENGINE_VERSION = "2.2.1";
  /** Keyed operators and their sinks run at the partition count of the station topics (spec 588). */
  public static final int KEYED_PARALLELISM = ReplayTopics.PARTITIONS;
  /** The pinned max parallelism of the keyed operators: the key-group count, fixed so a station's key group never moves (plan D6). */
  public static final int MAX_PARALLELISM = 128;

  private final String json;
  private transient JsonNode node;

  private JobSettings(String json) { this.json = json; }

  public static JobSettings of(JsonNode n) { return new JobSettings(Canonical.utf8(Canonical.bytes(n))); }

  public JsonNode node() {
    if (node == null) node = Json.read(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return node;
  }

  public String json() { return json; }

  public int stage() { return node().path("stage").intValue(); }
  public String bootstrap() { return Json.text(node(), "bootstrap"); }
  public long generation() { return node().path("generation").longValue(); }
  public String routingDigest() { return Json.text(node(), "routingDigest"); }
  public String runName(String contract) { return Json.text(node().path("names"), contract); }
  public String prefix() { return Json.text(node(), "prefix"); }
  public Path evidenceDir() { return Path.of(Json.text(node(), "evidenceDir")); }
  public long transactionTimeoutMs() { return node().path("transactionTimeoutMs").longValue(); }
  public List<String> stations() { List<String> l = new ArrayList<>(); node().path("stations").forEach(s -> l.add(s.asText())); return l; }

  /** The subscribed partitions of {@code topic} (contract name): the stage-1 raw topics as the routing manifest and the rule fix them,
   *  {@code quality.incidents} all six, a station topic the stations' partitions. */
  public List<Integer> subscribed(String topic) {
    List<Integer> l = new ArrayList<>();
    node().path("subscribed").path(topic).forEach(p -> l.add(p.intValue()));
    return l;
  }

  public Map<String, String> contractOfRunName() {
    Map<String, String> m = new HashMap<>();
    node().path("names").fields().forEachRemaining(e -> m.put(e.getValue().asText(), e.getKey()));
    return m;
  }

  // --- the job main: from the harness's job spec to the settings --------------------------------------------------------------

  /** The settings of the job the harness's {@code spec} describes: {@code {stage, bootstrap, prefix, topics?, generation, mappings,
   *  definitions, routing, evidenceRanges, runId, artifactSha256, evidenceDir, checkpointDir, transactionTimeoutMs, negativeControl, causeKind,
   *  launch}}.
   *  The spec carries no recording path: the definitions, the routing manifest and the evidence ranges come in the spec itself
   *  (reviewer decision 3 of 2026-11-18), so nothing here opens the recording directory. {@code topics}, when present, names the run
   *  topic of a contract topic in place of {@code <prefix><contract>} (an event-time replay shares its base run's generation and recovery
   *  control topics). The job refuses to run when the engine path it runs from does not have the digest {@code artifactSha256}
   *  ({@link EngineContract#engineArtifactSha256}, reviewer decision 2), and on an unregistered or retired generation or a rule set whose
   *  digest is not the registered one (spec 575). */
  public static JobSettings prepare(JsonNode spec) {
    int stage = spec.path("stage").intValue();
    if (stage != 1 && stage != 2) throw new IllegalArgumentException("job spec stage " + spec.path("stage"));
    if (spec.has("recording")) throw new IllegalArgumentException("a job spec carries no recording path (reviewer decision 3)");
    String bootstrap = Json.text(spec, "bootstrap"), prefix = spec.path("prefix").asText("");
    long generation = Json.safeLong(spec.get("generation"), "generation", 1, Json.MAX_SAFE);
    String artifact = Json.text(spec, "artifactSha256"), own = engineArtifact();
    if (!artifact.equals(own)) throw new IllegalStateException("engine-artifact-mismatch: the job spec names " + artifact + ", the engine path digests to " + own);
    String control = spec.path("negativeControl").isTextual() ? spec.get("negativeControl").textValue() : null;
    if (control != null && !Controls.engine(control)) throw new IllegalArgumentException("negative control " + control + " is not one the jobs apply");
    if (!spec.path("launch").canConvertToInt() || spec.path("launch").intValue() < 0) throw new IllegalArgumentException("job spec launch " + spec.path("launch"));
    List<ReplayTopics.Spec> plan = ReplayTopics.plan(prefix);
    ObjectNode names = Json.object();
    for (ReplayTopics.Spec s : plan) names.put(s.base(), spec.path("topics").path(s.base()).asText(s.name()));

    // the registered generation, read from forseti.generations (spec 575: a job refuses an unregistered generation)
    GenerationStore.Entry registered;
    boolean retired;
    ObjectNode generationsReader = Json.object();
    try (KafkaGenerationStore store = new KafkaGenerationStore(bootstrap, Json.text(names, ReplayTopics.GENERATIONS), false)) {
      new TreeMap<>(store.readerConfig()).forEach((k, v) -> generationsReader.put(k.toString(), v.toString()));
      registered = store.registered(generation).orElseThrow(() -> new IllegalStateException("generation-unregistered: generation " + generation));
      retired = Generation.RETIRED.equals(store.latestStatus(generation).orElse(null));
    }
    if (retired) throw new IllegalStateException("generation-retired: generation " + generation);
    return build(spec, registered.row(), generationsReader);
  }

  /** The settings of {@code spec} on the registered generation {@code g} ({@link #prepare} after its reads; the operator restore test
   *  calls it with a generation of a file store). {@code generationsReader}: the configuration of the job's own generations reader. */
  static JobSettings build(JsonNode spec, Generation g, ObjectNode generationsReader) {
    int stage = spec.path("stage").intValue();
    String bootstrap = Json.text(spec, "bootstrap"), prefix = spec.path("prefix").asText("");
    long generation = Json.safeLong(spec.get("generation"), "generation", 1, Json.MAX_SAFE);
    if (g.generation() != generation) throw new IllegalArgumentException("job spec generation " + generation + ", the row's " + g.generation());
    String artifact = Json.text(spec, "artifactSha256");
    String control = spec.path("negativeControl").isTextual() ? spec.get("negativeControl").textValue() : null;
    ObjectNode names = Json.object();
    for (ReplayTopics.Spec s : ReplayTopics.plan(prefix)) names.put(s.base(), spec.path("topics").path(s.base()).asText(s.name()));
    Path mappings = Path.of(Json.text(spec, "mappings"));
    JsonNode definitions = spec.get("definitions");
    if (definitions == null || !definitions.isObject()) throw new IllegalArgumentException("job spec without definitions");
    RuleSet rules = rules(mappings, g.releaseEvidence(), definitions);
    if (!rules.rulesDigest().equals(g.rulesDigest()))
      throw new IllegalStateException("generation-rules-mismatch: registered " + g.rulesDigest() + ", loaded " + rules.rulesDigest());
    RoutingManifest routing = RoutingManifest.read(spec.get("routing"));
    List<String> stations = new ArrayList<>(rules.attribution().stations());
    Collections.sort(stations);

    // subscriptions, as the reference harness and the step-6 test driver fix them
    boolean occupancyRead = !(rules.attribution().mode().equals("plc-stamped") && rules.attribution().crossCheck().equals("none"));
    ObjectNode subscribed = Json.object();
    for (String t : EngineContract.STAGE1_TOPICS) {
      ArrayNode a = subscribed.putArray(t);
      if (!(t.equals(InputParser.OCCUPANCY) && !occupancyRead)) routing.subscribed(t).forEach(a::add);
    }
    ArrayNode inc = subscribed.putArray(InputParser.INCIDENTS);
    routing.subscribed(InputParser.INCIDENTS).forEach(inc::add);
    for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES)) {
      ArrayNode a = subscribed.putArray(t);
      for (String s : stations) a.add(routing.partitionOf(s));
    }

    Pins pins = EngineContract.pins(rules, definitions, new Pins.Job(ENGINE, ENGINE_VERSION, artifact, Json.text(spec, "runId"),
        "payload", TextNode.valueOf("off"), routing.routingDigest()));
    // the wrong-rules-pin control: the rows' envelope pins name another release's attribution rule; the run record keeps the true pins
    Pins stamped = Controls.WRONG_RULES_PIN.equals(control) ? EngineContract.wrongRulesPin(pins, mappings, EngineContract.head(g.releaseEvidence())) : pins;
    ObjectNode n = Json.object().put("stage", stage).put("bootstrap", bootstrap).put("prefix", prefix);
    n.set("names", names);
    Json.putSafe(n, "generation", generation);
    n.put("routingDigest", routing.routingDigest());
    n.set("routing", routing.node());
    n.set("definitions", definitions);
    n.put("mappings", mappings.toAbsolutePath().toString());
    n.set("releaseEvidence", g.releaseEvidence());
    n.put("rulesDigest", g.rulesDigest());
    n.put("runId", Json.text(spec, "runId"));
    n.put("artifactSha256", artifact);
    n.put("evidenceDir", Json.text(spec, "evidenceDir"));
    n.set("subscribed", subscribed);
    ArrayNode st = n.putArray("stations");
    stations.forEach(st::add);
    n.set("pins", pins.toJson());
    n.set("stampedPins", stamped.toJson());
    if (control == null) n.putNull("negativeControl"); else n.put("negativeControl", control);
    ObjectNode er = n.putObject("evidenceRanges");
    spec.path("evidenceRanges").fields().forEachRemaining(e -> {
      if (!e.getValue().isArray() || e.getValue().size() != 2) throw new IllegalArgumentException("job spec evidenceRanges." + e.getKey());
      er.putArray(e.getKey()).add(e.getValue().get(0).longValue()).add(e.getValue().get(1).longValue());
    });
    Json.putSafe(n, "transactionTimeoutMs", spec.path("transactionTimeoutMs").longValue());
    // the durable launch identity of this launch of the job's cluster (reviewer decision 3 of PR 5): the harness writes it into the job
    // spec, and records it, before it starts the cluster; 0 at the first launch, plus one at every relaunch of a restart injection
    n.put("launch", spec.path("launch").asInt(0));
    // the cause kind of every row (spec 616): input for an online generation, replayed for an event-time replay
    n.set("generationsReader", generationsReader);   // the job's own forseti.generations reader (spec 708), recorded in the job settings
    n.put("causeKind", Envelope.causeKind(spec.path("causeKind").asText(Envelope.ONLINE)));
    return of(n);
  }

  /** The digest of the engine path this class runs from ({@link EngineContract#engineArtifactSha256} of the directory of its jar),
   *  {@code unpacked} when it runs from a classes directory (the unit tests). */
  public static String engineArtifact() {
    try {
      Path p = Path.of(JobSettings.class.getProtectionDomain().getCodeSource().getLocation().toURI());
      return Files.isRegularFile(p) ? EngineContract.engineArtifactSha256(p.getParent()) : "unpacked";
    } catch (java.net.URISyntaxException | RuntimeException e) { return "unpacked"; }
  }

  /** The durable launch identity of this launch of the job (0 at the first). */
  public int launch() { return node().path("launch").asInt(0); }

  /** Control theta (spec 700, plan D9): the sinks are non-transactional at-least-once. */
  public boolean nonTransactional() { return Controls.NON_TRANSACTIONAL_OUTPUT.equals(negativeControl()); }

  /** The negative control the jobs apply, null for none. */
  public String negativeControl() { return node().path("negativeControl").isTextual() ? node().get("negativeControl").textValue() : null; }

  static RuleSet rules(Path mappings, JsonNode releaseEvidence, JsonNode definitions) {
    HeadVerifier.Head head = EngineContract.head(releaseEvidence);
    try {
      ReleaseReader.Release release = ReleaseReader.read(mappings, head);
      JsonNode r = releaseEvidence.path("pins").path("rules").path("attribution");
      return RuleSet.load(release, Json.text(r, "id"), Json.text(r, "version"), definitions);
    } catch (Refusal r) { throw new IllegalStateException("release refused: " + r.reason); }
  }

  // --- the operators' runtime ----------------------------------------------------------------------------------------------

  /** The rebuilt, non-serializable side of the settings. */
  public static final class Runtime {
    public final JobSettings settings;
    public final RuleSet rules;
    public final RoutingManifest routing;
    public final Pins pins;
    /** The pins the rows are stamped with: {@link #pins}, or under {@code wrong-rules-pin} another release's attribution pin. */
    public final Pins stamped;
    public final NegativeControls controls;
    public final Stage1 stage1;
    public final Stage2 stage2;
    public final Routing router;
    public final long outOfOrdernessMs;
    public final Map<String, List<Progress.Origin>> required = new TreeMap<>();
    public final List<Progress.Origin> rejectsRequired, incidentOrigins;
    public final Map<String, SetCoverage.EvidenceRange> evidenceRanges = new TreeMap<>();

    Runtime(JobSettings s) {
      JsonNode n = s.node();
      this.settings = s;
      this.rules = JobSettings.rules(Path.of(Json.text(n, "mappings")), n.get("releaseEvidence"), n.get("definitions"));
      if (!rules.rulesDigest().equals(Json.text(n, "rulesDigest"))) throw new IllegalStateException("generation-rules-mismatch on the TaskManager");
      this.routing = RoutingManifest.read(n.get("routing"));
      this.pins = Pins.fromJson(n.get("pins"));
      this.stamped = Pins.fromJson(n.get("stampedPins"));
      // the negative control of the run, through the job settings (lenient-oracle is the oracle's and never reaches a job)
      this.controls = Controls.of(s.negativeControl());
      String cause = n.path("causeKind").asText(Envelope.ONLINE);
      this.stage1 = new Stage1(rules, controls, cause);
      this.stage2 = new Stage2(rules, controls, cause);
      this.router = new Routing(rules);
      this.outOfOrdernessMs = rules.attribution().outOfOrdernessMs();
      Map<String, List<Integer>> stage1Subscribed = new LinkedHashMap<>();
      for (String t : EngineContract.STAGE1_TOPICS) stage1Subscribed.put(t, s.subscribed(t));
      for (String st : s.stations()) required.put(st, EngineContract.stationRequired(st, routing, stage1Subscribed));
      this.rejectsRequired = EngineContract.required(stage1Subscribed, null);
      List<Progress.Origin> io = new ArrayList<>();
      for (int p : s.subscribed(InputParser.INCIDENTS)) io.add(new Progress.Origin(InputParser.INCIDENTS, p));
      this.incidentOrigins = List.copyOf(io);
      n.path("evidenceRanges").fields().forEachRemaining(e -> evidenceRanges.put(e.getKey(),
          new SetCoverage.EvidenceRange(e.getValue().get(0).longValue(), e.getValue().get(1).longValue())));
    }

    public int partitionOf(String station) { return routing.partitionOf(station); }
  }

  private static final Map<String, Runtime> RUNTIMES = new ConcurrentHashMap<>();

  public Runtime runtime() { return RUNTIMES.computeIfAbsent(json, k -> new Runtime(this)); }
}
