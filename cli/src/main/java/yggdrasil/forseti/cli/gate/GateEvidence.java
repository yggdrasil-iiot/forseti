package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.CoreGate.Proc;
import yggdrasil.forseti.cli.generation.FileGenerationStore;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;

/** What a gate run registered and ran, and the one validator of that evidence (plan Task 20, decisions 6, 9 and 10).
 *
 *  <p>Registration: every generation a gate run causes is written by {@code forseti generation new}. In the pipeline store with
 *  {@code --state} the verb runs as a child JVM on the gate's own classpath with the system clock ({@code shared}); in a temporary store
 *  it runs in-process ({@link GenerationCli#run}) with the fixture clock, the head's signedAt + 60 s ({@link GateSigner#now},
 *  {@code isolated}). Each registration is listed in {@code generations[]}; each harness run in {@code runs[]}, with its {@code run.json}
 *  and {@code cuts.json} kept under {@code <out>/runs/}.
 *
 *  <p>{@link #validate}: a bundle is one output directory ({@code report.json}, {@code summaries.json}, {@code runs/}, and {@code state/}
 *  when the run had {@code --state}). It binds every run to its generation inside its store, every pipeline run to exactly one
 *  successful summary and every summary to exactly one run, each summary to its committed lock and exact cut ids, each run record to
 *  its entry, its cut log and the report, and every {@code shared} generation to the row in {@code state/generations.jsonl}. Every
 *  problem it returns is a coverage problem. */
final class GateEvidence {
    private GateEvidence() {}

    static final String PIPELINE = "pipeline", RUNS = "runs", STATE = "state", SHARED = "shared", ISOLATED = "isolated";
    static final String ONLINE = "online", SELF_TEST = "self-test";

    /** A control store of this gate run: its bundle-local name ({@code pipeline}, {@code assertion:<ID>}, {@code assertion:<ID>#2}, ...),
     *  its directory, and whether its registrations are {@code shared} (the executable verb, the system clock, kept). */
    record Store(String name, Path dir, boolean shared) {
        String state() { return shared ? SHARED : ISOLATED; }
    }

    /** One registration: the store it lives in and what {@code generation new} printed. */
    record Registered(Store store, long generation, String scope, String rulesDigest, String registeredRowSha256) {}

    /** {@code generations[]} and {@code runs[]} of one gate run. Generations are listed in registration order; runs are sorted by store
     *  and kept file name when reported, so a report does not depend on scheduling. */
    static final class Log {
        private final List<ObjectNode> generations = new ArrayList<>(), runs = new ArrayList<>();
        private final Map<String, Integer> stores = new HashMap<>();
        private final Set<String> kept = new HashSet<>();

        synchronized void generation(ObjectNode e) { generations.add(e); }
        synchronized void run(ObjectNode e) { runs.add(e); }
        synchronized String storeName(String assertion) {
            int n = stores.merge(assertion, 1, Integer::sum);
            return "assertion:" + assertion + (n == 1 ? "" : "#" + n);
        }
        /** {@code base}, or {@code base.2}, {@code base.3}, ... when a run of this gate run already kept files under that name. */
        synchronized String keptName(String base) {
            String name = base;
            for (int i = 2; !kept.add(name); i++) name = base + "." + i;
            return name;
        }
        synchronized ArrayNode generations() {
            ArrayNode a = Json.JSON.createArrayNode();
            generations.forEach(g -> a.add(g.deepCopy()));
            return a;
        }
        synchronized ArrayNode runs() {
            List<ObjectNode> l = new ArrayList<>(runs);
            l.sort(Comparator.comparing((ObjectNode r) -> r.path("store").asText()).thenComparing(r -> r.path("runRecord").asText()));
            ArrayNode a = Json.JSON.createArrayNode();
            l.forEach(r -> a.add(r.deepCopy()));
            return a;
        }
    }

    // --- registration ----------------------------------------------------------------------------------------------------------

    /** Registers the generation of {@code recording}'s rule (its manifest's {@code rule.stream}, {@code attributionRuleId@attributionRuleVersion})
     *  from {@code registry}, with {@code --recording}. */
    static Registered register(Context c, Store store, Path registry, Path recording, String scenario, String variant, String assertion) throws IOException, InterruptedException {
        JsonNode rule = Json.read(recording.resolve(Recording.MANIFEST)).get("rule");
        return register(c, store, registry, Json.text(rule, "stream"), Json.text(rule, "attributionRuleId") + "@" + Json.text(rule, "attributionRuleVersion"),
            recording, scenario, variant, assertion);
    }

    /** {@code forseti generation new} on {@code store}: a child JVM with the system clock for a shared store, in-process with the fixture
     *  clock otherwise. Exit 0 is required; the registration is listed in {@code generations[]}. */
    static Registered register(Context c, Store store, Path registry, String stream, String rule, Path recording, String scenario, String variant,
                               String assertion) throws IOException, InterruptedException {
        Path mappings = registry.resolve("mappings");
        List<String> args = new ArrayList<>(List.of("new", "--state", store.dir().toString(), "--mappings", mappings.toString(),
            "--trusted-keys", c.fixtures().resolve("trusted-keys.jsonl").toString(), "--stream", stream, "--rule", rule));
        if (recording != null) args.addAll(List.of("--recording", recording.toString()));
        Files.createDirectories(store.dir());
        int exit;
        String printed;
        if (store.shared()) {
            List<String> cmd = new ArrayList<>(CoreGate.java(null));
            cmd.add("generation");
            cmd.addAll(args);
            Proc p = Proc.run(c.root, c.logs.resolve("generation-new-" + safe(store.name()) + "-" + safe(scenario) + ".log"), Duration.ofMinutes(10), cmd);
            exit = p.exit();
            printed = p.lastLine();
        } else {
            ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
            Clock clock = Clock.fixed(Instant.ofEpochMilli(GateSigner.now(mappings, stream)), ZoneOffset.UTC);
            exit = GenerationCli.run(args.toArray(String[]::new), new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8), clock);
            printed = (so.toString(StandardCharsets.UTF_8).strip() + " " + se.toString(StandardCharsets.UTF_8).strip()).strip();
        }
        if (exit != 0) throw new IllegalStateException("generation new on " + store.name() + " (" + scenario + ") exit " + exit + ": " + printed);
        JsonNode p = Json.read(printed.getBytes(StandardCharsets.UTF_8));
        Registered g = new Registered(store, p.get("generation").longValue(), Json.text(p, "scope"), Json.text(p, "rulesDigest"), Json.text(p, "registeredRowSha256"));
        ObjectNode e = Json.object();
        e.put("shard", c.shard == null ? null : c.shard.name()).put("store", store.name());
        Json.putSafe(e, "generation", g.generation());
        e.put("scope", g.scope()).put("rulesDigest", g.rulesDigest()).put("registeredRowSha256", g.registeredRowSha256()).put("state", store.state())
            .put("scenario", scenario).put("variant", variant).put("assertion", assertion);
        c.evidence.generation(e);
        return g;
    }

    static String safe(String s) { return s.replaceAll("[^A-Za-z0-9.-]", "-"); }

    // --- kept runs -------------------------------------------------------------------------------------------------------------

    /** Copies the run's {@code run.json} and {@code cuts.json} to {@code <out>/runs/<name>.run.json} and {@code .cuts.json} and lists the
     *  run in {@code runs[]}. */
    static ObjectNode keep(Context c, Registered g, Path runOut, Path recording, String scenario, String variant, long seed, boolean shuffle, String base) throws IOException {
        Path dir = Files.createDirectories(c.out.resolve(RUNS));
        String name = c.evidence.keptName(base);
        byte[] record = Files.readAllBytes(runOut.resolve("run.json")), cuts = Files.readAllBytes(runOut.resolve("cuts.json"));
        Files.write(dir.resolve(name + ".run.json"), record);
        Files.write(dir.resolve(name + ".cuts.json"), cuts);
        JsonNode rr = Json.read(record);
        ObjectNode e = Json.object();
        e.put("shard", c.shard == null ? null : c.shard.name()).put("store", g.store().name()).put("scenario", scenario).put("variant", variant);
        Json.putSafe(e, "seed", seed);
        e.put("shuffle", shuffle).put("runId", rr.path("runId").asText(null));
        Json.putSafe(e, "generation", g.generation());
        e.put("registeredRowSha256", g.registeredRowSha256()).put("state", g.store().state());
        e.put("runRecord", RUNS + "/" + name + ".run.json").put("runRecordSha256", Canonical.digestOf(record));
        e.put("cutLog", RUNS + "/" + name + ".cuts.json").put("cutLogSha256", Canonical.digestOf(cuts));
        e.put("recordingManifestSha256", Canonical.digestOf(Files.readAllBytes(recording.resolve(Recording.MANIFEST))));
        e.putNull("originalRecordingManifestSha256");
        e.set("offsetRanges", rr.path("offsetRanges").deepCopy());
        c.evidence.run(e);
        return e;
    }

    // --- the validator -----------------------------------------------------------------------------------------------------------

    /** {@link #validate(Path, Path, JsonNode, JsonNode)} on the files of {@code bundle}. */
    static List<String> validate(Path root, Path bundle) {
        JsonNode report, summaries;
        try { report = Json.read(bundle.resolve("report.json")); summaries = Json.read(bundle.resolve(Shards.SUMMARIES)); }
        catch (RuntimeException e) { return List.of(bundle.getFileName() + ": " + e.getMessage()); }
        return validate(root, bundle, report, summaries);
    }

    /** The problems of one bundle: {@code report} and {@code summaries} as read (or about to be written), the kept files under
     *  {@code bundle}. */
    static List<String> validate(Path root, Path bundle, JsonNode report, JsonNode summaries) {
        List<String> p = new ArrayList<>();
        JsonNode gens = report.path("generations"), runs = report.path("runs");
        if (!gens.isArray() || !runs.isArray()) { p.add("the report has no generations[] or runs[]"); return p; }

        // identity: (store, generation) and (store, registeredRowSha256) are unique
        Map<String, JsonNode> byNumber = new HashMap<>(), byDigest = new HashMap<>();
        for (JsonNode g : gens) {
            String store = g.path("store").asText(null), state = g.path("state").asText(null);
            if (store == null || !(store.equals(PIPELINE) || store.startsWith("assertion:"))) p.add("a generation entry names store " + g.path("store"));
            if (!SHARED.equals(state) && !ISOLATED.equals(state)) p.add("generation " + g.path("generation") + " of store " + store + " has state " + g.path("state"));
            if (SHARED.equals(state) && !PIPELINE.equals(store)) p.add("generation " + g.path("generation") + " of store " + store + " is shared outside the pipeline store");
            if (byNumber.put(store + " " + g.path("generation").asLong(-1), g) != null) p.add("generation " + g.path("generation") + " of store " + store + " is listed twice");
            if (byDigest.put(store + " " + g.path("registeredRowSha256").asText(null), g) != null)
                p.add("registeredRowSha256 " + g.path("registeredRowSha256").asText() + " is listed twice in store " + store);
        }

        // the summaries, by key, and what each must hold
        Map<String, JsonNode> summaryByKey = new LinkedHashMap<>();
        for (JsonNode s : summaries.path("runs")) if (summaryByKey.put(s.path("name").asText(), s) != null) p.add(s.path("name").asText() + " is summarised twice");
        for (JsonNode s : summaries.path("shuffled")) {
            String key = s.path("name").asText() + "@" + s.path("seed").asLong();
            if (summaryByKey.put(key, s) != null) p.add("shuffled run " + key + " is summarised twice");
        }
        Map<String, List<Long>> summaryCuts = new HashMap<>();
        for (Map.Entry<String, JsonNode> e : summaryByKey.entrySet()) summaryCuts.put(e.getKey(), summary(root, e.getKey(), e.getValue(), p));

        // the shared store
        List<GenerationStore.Entry> stored = null;
        Map<Long, String> storedDigests = new HashMap<>();
        boolean anyShared = false;
        for (JsonNode g : gens) anyShared |= SHARED.equals(g.path("state").asText());
        if (anyShared) {
            Path state = bundle.resolve(STATE);
            if (!Files.isRegularFile(state.resolve(FileGenerationStore.FILE))) p.add("shared generations, but no " + STATE + "/" + FileGenerationStore.FILE + " in the bundle");
            else try {
                FileGenerationStore fs = new FileGenerationStore(state);
                stored = fs.all();
                for (GenerationStore.Entry x : stored)
                    if (Generation.REGISTERED.equals(x.row().status())) storedDigests.put(x.row().generation(), fs.registeredRowSha256(x.row().generation()).orElse(null));
            } catch (RuntimeException e) { p.add(STATE + "/" + FileGenerationStore.FILE + ": " + e.getMessage()); }
        }
        if (stored != null) for (JsonNode g : gens) {
            if (!SHARED.equals(g.path("state").asText())) continue;
            long n = g.path("generation").asLong(-1);
            String label = "shared generation " + n;
            List<GenerationStore.Entry> reg = new ArrayList<>();
            for (GenerationStore.Entry x : stored) if (x.row().generation() == n && Generation.REGISTERED.equals(x.row().status())) reg.add(x);
            if (reg.size() != 1) { p.add(label + ": " + reg.size() + " registered rows in " + STATE + "/" + FileGenerationStore.FILE); continue; }
            Generation row = reg.get(0).row();
            if (!g.path("registeredRowSha256").asText("").equals(storedDigests.get(n)))
                p.add(label + ": registeredRowSha256 " + g.path("registeredRowSha256").asText() + ", the stored row's " + storedDigests.get(n));
            if (!g.path("rulesDigest").asText("").equals(row.rulesDigest())) p.add(label + ": rulesDigest " + g.path("rulesDigest").asText() + ", the stored row's " + row.rulesDigest());
            if (!g.path("scope").asText("").equals(reg.get(0).scope())) p.add(label + ": scope " + g.path("scope").asText() + ", the stored row's key " + reg.get(0).scope());
        }

        // every run: its generation, its files, its record, its summary
        Set<String> runIds = new HashSet<>();
        Map<String, Integer> covered = new HashMap<>();
        for (JsonNode r : runs) {
            String store = r.path("store").asText(null), runId = r.path("runId").asText(null);
            String label = "run " + runId + " (" + store + ", " + r.path("scenario").asText() + (r.path("shuffle").asBoolean() ? " shuffled seed " + r.path("seed").asLong() : "") + ")";
            if (runId == null || !runIds.add(runId)) p.add(label + ": runId listed twice or missing");
            JsonNode g = byDigest.get(store + " " + r.path("registeredRowSha256").asText(null));
            if (g == null) p.add(label + ": registeredRowSha256 " + r.path("registeredRowSha256").asText() + " names no generation of store " + store);
            else {
                if (g.path("generation").asLong(-1) != r.path("generation").asLong(-2)) p.add(label + ": generation " + r.path("generation") + ", its registration's " + g.path("generation"));
                if (!g.path("state").asText("").equals(r.path("state").asText(null))) p.add(label + ": state " + r.path("state") + ", its registration's " + g.path("state"));
            }
            JsonNode rr = kept(bundle, r, "runRecord", p, label), cl = kept(bundle, r, "cutLog", p, label);
            if (rr != null && cl != null) runRecord(r, g, rr, cl, report, stored, p, label);
            if (rr != null && !r.path("recordingManifestSha256").asText("").equals(rr.path("fixtureDigests").path("recordingManifestSha256").asText(null)))
                p.add(label + ": recording manifest " + r.path("recordingManifestSha256").asText() + ", its run record's " + rr.path("fixtureDigests").path("recordingManifestSha256"));
            if (!PIPELINE.equals(store)) continue;
            String key;
            if (r.path("shuffle").asBoolean()) key = r.path("scenario").asText() + "@" + r.path("seed").asLong();
            else if (ONLINE.equals(r.path("variant").asText())) key = r.path("scenario").asText();
            else { p.add(label + ": variant " + r.path("variant") + " is not a pipeline run of this gate"); continue; }
            JsonNode s = summaryByKey.get(key);
            if (s == null) { p.add(label + ": no summary " + key + " in " + Shards.SUMMARIES); continue; }
            if (covered.merge(key, 1, Integer::sum) > 1) { p.add(key + ": two run entries for one summary"); continue; }
            if (!r.path("recordingManifestSha256").asText("").equals(s.path("recordingManifestSha256").asText(null)))
                p.add(label + ": recording manifest " + r.path("recordingManifestSha256").asText() + ", its summary's " + s.path("recordingManifestSha256"));
            if (cl != null) {
                List<Long> logged = new ArrayList<>();
                for (JsonNode k : cl.path("cuts")) logged.add(k.path("cutId").asLong());
                List<Long> want = summaryCuts.get(key);
                if (want != null && !logged.equals(want)) p.add(label + ": its kept cut log holds cuts " + Shards.firstDifference(logged, want) + " other than its summary's");
            }
        }
        for (String key : summaryByKey.keySet()) if (!covered.containsKey(key)) p.add(key + ": a summary without a pipeline run entry");
        return p;
    }

    /** One summary against its committed lock (the rules {@code Shards.coverage} held before): it held, the recording it read is the one its
     *  lock pins, and its planned, completed and compared cut ids are one strictly increasing list of the lock's length. Returns the list. */
    static List<Long> summary(Path root, String key, JsonNode r, List<String> p) {
        String name = r.path("name").asText();
        boolean shuffled = key.contains("@");
        String label = shuffled ? name + " seed " + r.path("seed").asLong() : name;
        JsonNode lock;
        try { lock = Json.read(ScenarioBuilder.lockOf(root.resolve("fixtures").resolve("scenarios").resolve(name + ".json"))); }
        catch (RuntimeException e) { p.add(label + ": cannot read its lock: " + e.getMessage()); return null; }
        long lockCuts = lock.path("counts").path("cuts").asLong(-1);
        if (shuffled) {
            if (Summaries.failed(r)) p.add(label + ": " + r.path("failure").asText());
            if (!r.path("equal").asBoolean(false)) p.add(label + ": its comparison with the oracle is not equal (equal " + r.path("equal") + ")");
            if (!r.path("comparedCutIds").isArray()) p.add(label + ": no comparedCutIds of its own oracle comparison");
        }
        if (!r.path("ok").asBoolean(false)) p.add(label + " did not hold: " + r.path("summary").asText());
        long compared = r.path("cutsCompared").asLong(-1), completed = r.path("cutLogCuts").asLong(-1);
        if (compared != lockCuts || completed != lockCuts) p.add(label + ": " + compared + " cuts compared with the oracle and " + completed + " completed, its lock plans " + lockCuts);
        String pinned = lock.path("files").path("manifest.json").asText(null), read = r.path("recordingManifestSha256").asText(null);
        if (pinned == null || !pinned.equals(read)) p.add(label + ": its run read recording manifest " + read + ", its lock pins " + pinned);
        List<Long> planned = Shards.longs(r.path("plannedCutIds")), logged = Shards.longs(r.path("cutLogIds")), checked = Shards.longs(r.path("comparedCutIds"));
        if (planned.size() != lockCuts) p.add(label + ": its recording plans " + planned.size() + " cuts, its lock plans " + lockCuts);
        for (int i = 1; i < planned.size(); i++)
            if (planned.get(i) <= planned.get(i - 1)) { p.add(label + ": planned cut ids are not strictly increasing at " + planned.get(i)); break; }
        if (!logged.equals(planned)) p.add(label + ": the harness completed cuts " + Shards.firstDifference(logged, planned) + " other than the recording plans");
        if (!checked.equals(planned)) p.add(label + ": the gate compared cuts " + Shards.firstDifference(checked, planned) + " other than the recording plans");
        return planned;
    }

    /** The kept file an entry names in {@code field} ({@code runs/...}, relative, without {@code ..}), whose raw digest is
     *  {@code <field>Sha256}; null (and a problem) when it is missing, elsewhere or different. */
    static JsonNode kept(Path bundle, JsonNode r, String field, List<String> p, String label) {
        String rel = r.path(field).asText(null);
        if (rel == null || rel.startsWith("/") || rel.contains("\\") || rel.contains(":") || !rel.startsWith(RUNS + "/") || Arrays.asList(rel.split("/")).contains("..")) {
            p.add(label + ": " + field + " " + r.path(field) + " is not a path under " + RUNS + "/");
            return null;
        }
        Path f = bundle.resolve(rel);
        byte[] b;
        try { b = Files.readAllBytes(f); } catch (IOException e) { p.add(label + ": " + rel + " is not in the bundle"); return null; }
        if (!Canonical.digestOf(b).equals(r.path(field + "Sha256").asText(null))) { p.add(label + ": " + rel + " has another digest than " + field + "Sha256"); return null; }
        try { return Json.read(b); } catch (RuntimeException e) { p.add(label + ": " + rel + ": " + e.getMessage()); return null; }
    }

    /** The run record against its entry, its registration, its cut log, the report's pins and (for a shared generation) the stored row. */
    static void runRecord(JsonNode r, JsonNode g, JsonNode rr, JsonNode cl, JsonNode report, List<GenerationStore.Entry> stored, List<String> p, String label) {
        for (String f : List.of("runId", "generation", "registeredRowSha256", "seed", "shuffle", "offsetRanges"))
            if (!Shards.same(r.path(f), rr.path(f))) p.add(label + ": run record " + f + " " + rr.path(f) + ", its entry's " + r.path(f));
        if (g != null && !Shards.same(g.path("scope"), rr.path("scope"))) p.add(label + ": run record scope " + rr.path("scope") + ", its registration's " + g.path("scope"));
        if (rr.path("cuts").asLong(-1) != cl.path("cuts").size()) p.add(label + ": run record cuts " + rr.path("cuts") + ", its cut log holds " + cl.path("cuts").size());
        if (cl.path("generation").asLong(-1) != r.path("generation").asLong(-2)) p.add(label + ": cut log generation " + cl.path("generation") + ", its entry's " + r.path("generation"));
        // subscriptions and offset ranges: the run record's are the cut log's, from offset 0, contiguous
        Set<String> subscribed = new TreeSet<>(), logged = new TreeSet<>();
        rr.path("subscribed").fields().forEachRemaining(e -> e.getValue().forEach(x -> subscribed.add(e.getKey() + "/" + x.asLong())));
        Map<String, long[]> union = new TreeMap<>();
        for (JsonNode cut : cl.path("cuts"))
            cut.path("offsetRanges").fields().forEachRemaining(t -> t.getValue().fields().forEachRemaining(e -> {
                String k = t.getKey() + "/" + e.getKey();
                logged.add(t.getKey() + "/" + Long.parseLong(e.getKey()));
                long from = e.getValue().path(0).asLong(-1), to = e.getValue().path(1).asLong(-1);
                long[] u = union.computeIfAbsent(k, x -> new long[] {0, 0});
                if (from != u[1] || to < from) p.add(label + ": cut " + cut.path("cutId") + " " + k + " range [" + from + ", " + to + ") does not continue at " + u[1]);
                u[1] = Math.max(u[1], to);
            }));
        if (!subscribed.equals(logged)) p.add(label + ": run record subscribes " + subscribed + ", its cut log's offset ranges cover " + logged);
        ObjectNode want = Json.object();
        union.forEach((k, u) -> {
            int slash = k.lastIndexOf('/');
            String t = k.substring(0, slash), part = Long.toString(Long.parseLong(k.substring(slash + 1)));
            ObjectNode tn = want.has(t) ? (ObjectNode) want.get(t) : want.putObject(t);
            tn.putArray(part).add(u[0]).add(u[1]);
        });
        if (!Shards.same(want, rr.path("offsetRanges"))) p.add(label + ": run record offsetRanges " + rr.path("offsetRanges") + ", the union of its cut log's " + want);
        // rules, routing, artifact, JCS
        ObjectNode pins = rr.path("pins").isObject() ? ((ObjectNode) rr.path("pins")).deepCopy() : Json.object();
        pins.remove("job");
        String pinsDigest = Canonical.digest(pins);
        if (g != null) {
            String rules = g.path("rulesDigest").asText("");
            if (!rules.equals(rr.path("fixtureDigests").path("rulesDigest").asText(null))) p.add(label + ": run record rulesDigest " + rr.path("fixtureDigests").path("rulesDigest") + ", its registration's " + rules);
            if (!rules.equals(pinsDigest)) p.add(label + ": run record pins digest " + pinsDigest + ", its registration's rulesDigest " + rules);
        }
        if (!Shards.same(rr.path("pins").path("job").path("routingDigest"), cl.path("routingDigest")))
            p.add(label + ": run record routing " + rr.path("pins").path("job").path("routingDigest") + ", its cut log's " + cl.path("routingDigest"));
        if (!Shards.same(rr.path("artifactSha256"), report.path("pins").path("forseti").path("artifactSha256")))
            p.add(label + ": run record artifactSha256 " + rr.path("artifactSha256") + ", the report's " + report.path("pins").path("forseti").path("artifactSha256"));
        if (!Shards.same(rr.path("jcs").path("java"), report.path("pins").path("jcs").path("java")))
            p.add(label + ": run record jcs.java " + rr.path("jcs").path("java") + ", the report's " + report.path("pins").path("jcs").path("java"));
        if (stored != null && g != null && SHARED.equals(g.path("state").asText())) {
            for (GenerationStore.Entry x : stored)
                if (x.row().generation() == g.path("generation").asLong(-1) && Generation.REGISTERED.equals(x.row().status())
                    && !x.row().releaseEvidence().path("releaseSha256").asText("").equals(rr.path("fixtureDigests").path("releaseSha256").asText(null)))
                    p.add(label + ": run record releaseSha256 " + rr.path("fixtureDigests").path("releaseSha256") + ", the stored row pins " + x.row().releaseEvidence().path("releaseSha256"));
        }
    }
}
