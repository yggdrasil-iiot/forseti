package yggdrasil.forseti.cli.recording;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.core.Json;

/** The engine recordings' scenarios (step-6 plan D11): every step-3 semantic scenario rebuilt by the step-3 builder on the shared
 *  governed placement ({@code site-019}, raw partition 0 carrying two stations, spec 491), baseline and smoke also on the dedicated one
 *  ({@code site-071}), injection 24 on both (spec 681) and the baseline at batch interval x2 and x1/2 on both (item 5 (d)), all in the
 *  Flink W1 form ({@code w1Form: min-over-partitions}, spec 620, 622). A scenario is derived from its step-3 file by renaming the
 *  {@code synthetic-site} hierarchy root to the placement's site in every key and value, pinning the placement's published release
 *  ({@code fixtures/live/registry}: the step-3 release {@code line2-rules}, {@code line2-rules-plc} or {@code line2-rules-plc-nocheck} maps
 *  to the placement's release of the same rule) and setting the W1 form; nothing else changes, so the visits, overlays, timing and seeds
 *  are the step-3 scenario's.
 *
 *  <p>Injection 24 ({@link #STARVED_SCENARIO}) and its control ({@link #STARVED_CONTROL}, the same recording without the starvation) pin
 *  the placement's starvation release ({@code line2-rules-<placement>-starvation}: the placement's rule with the starved station in the
 *  impact rule's {@code incidentScope}) and carry one closed incident on the starved station ({@link #STARVED_INCIDENT}): its suspect
 *  interval is the station's first visit, so its settlement deadline ({@code suspectTo + allowedLatenessMs}) is that visit's, and it is
 *  delivered before the starvation, whose window holds the deadline.
 *
 *  <p>The derived files live under {@code engine-fixtures/scenarios/<placement>/}, outside the paths of the core-input key (the core gate
 *  never replays them), each beside its lock in the step-3 lock format ({@link ScenarioBuilder#lockOf}); {@code
 *  engine-fixtures/engine-scenarios.json} lists them with their source and the step-3 scenarios that are not rebuilt and why. The
 *  builder that turns them into recordings stays in the core key. {@link #write} writes every file; {@link #check} compares the
 *  committed scenario files and index with a fresh derivation (the locks are checked by rebuilding, {@code forseti recordings
 *  check-engine}).
 *
 *  <p>Replay locks: a scenario of {@link #REPLAYED} (injection 23's event-time replay, spec 626) also carries the lock of its replay
 *  recording, {@code <name>.replay.lock.json} ({@link ReplayRecording#lockOf}), a companion of its base lock: {@link #check} requires it
 *  for exactly those scenarios and checks that it belongs to its base (same scenario and scenario digest, the original's cut count, the
 *  replay generator, the lock's canonical bytes); {@code check-engine} rematerializes the replay and compares its bytes. */
public final class EngineScenarios {
    private EngineScenarios() {}

    public static final String W1_FORM = W1Form.MIN_OVER_PARTITIONS.wire;
    static final String STEP3_ROOT = "synthetic-site";
    public static final String INDEX = "engine-scenarios.json";

    /** A governed placement: its site id and the published streams of its rules (occupancy-join, plc-stamped with crossCheck occupancy,
     *  plc-stamped with crossCheck none). */
    public record Placement(String name, String site, String stream, String release, String plcStream, String plcRelease,
                            String plcNocheckStream, String plcNocheckRelease, String starvationStream, String starvationRelease) {}
    public static final Placement SHARED = new Placement("shared", "site-019", "line2-shared", "line2-rules-shared", "line2-shared-plc", "line2-rules-shared-plc",
        "line2-shared-plc-nocheck", "line2-rules-shared-plc-nocheck", "line2-shared-starvation", "line2-rules-shared-starvation");
    public static final Placement DEDICATED = new Placement("dedicated", "site-071", "line2-dedicated", "line2-rules-dedicated", "line2-dedicated-plc", "line2-rules-dedicated-plc",
        "line2-dedicated-plc-nocheck", "line2-rules-dedicated-plc-nocheck", "line2-dedicated-starvation", "line2-rules-dedicated-starvation");
    /** The step-3 releases, by rule. */
    static final String STEP3_RELEASE = "line2-rules", STEP3_PLC_RELEASE = "line2-rules-plc", STEP3_PLC_NOCHECK_RELEASE = "line2-rules-plc-nocheck";
    public static final List<Placement> PLACEMENTS = List.of(SHARED, DEDICATED);

    /** The step-3 semantic scenarios, in the core gate's order (hand-made, horizon, smoke). */
    public static final List<String> STEP3 = List.of("baseline", "held-back-10min", "redelivery", "overlap", "bad-quality", "clock-plus-30", "clock-plus-90",
        "corrections", "corrections-10b-before", "corrections-10b-after", "corrections-10c", "session", "quiet-occupancy", "quiet-occupancy-control",
        "plc-stamped", "plc-nocheck", "plc-held-back", "stage2", "stage2-early", "cascade",
        "horizon-no-departure", "horizon-reopened", "horizon-correction-beyond", "horizon-planned-late", "plc-horizon-no-end",
        "smoke", "smoke-plc", "smoke-incident");
    /** Step-3 scenarios not rebuilt, with the reason (none: every step-3 scenario's rule has a release on the governed placements). */
    public static final Map<String, String> NOT_REBUILT = Collections.unmodifiableMap(new LinkedHashMap<>());
    /** The scenarios (on {@link #SHARED}) whose event-time replay recording is locked beside them: injection 23 (spec 626). */
    public static final List<String> REPLAYED = List.of("horizon-planned-late");
    /** The scenarios also built on the dedicated placement. */
    public static final List<String> DEDICATED_TOO = List.of("baseline", "smoke");

    /** Injection 24: the station starved, and the window (milliseconds from T0) in which its telemetry is withheld. */
    public static final String STARVED_SCENARIO = "starved-station3", STARVED_STATION = "line2:station3";
    public static final long STARVE_FROM = 1_500_000, STARVE_RESUME = 2_100_000;
    /** Injection 24's control: the starved scenario without the starvation (the same telemetry, occupancy and incident). */
    public static final String STARVED_CONTROL = "starved-station3-control";
    /** The starved station's incident: closed {@code tool-check-failed} on the station itself, suspect over its first visit
     *  ([{@link #STARVED_INCIDENT_FROM}, {@link #STARVED_INCIDENT_TO}), the baseline's station3 visit 1), delivered at
     *  {@link #STARVED_INCIDENT_DELIVER}, before the starvation; its deadline {@code suspectTo + allowedLatenessMs} is that visit's
     *  settlement deadline, inside the starvation window. */
    public static final String STARVED_INCIDENT = "inc-24";
    public static final long STARVED_INCIDENT_FROM = 780_000, STARVED_INCIDENT_TO = 840_000, STARVED_INCIDENT_DELIVER = 900_000;
    /** Item 5 (d): the baseline at batch interval x2 and x1/2. */
    public static final String BASELINE_X2 = "baseline-batch-x2", BASELINE_HALF = "baseline-batch-half";

    /** One derived scenario: its placement, name, step-3 source and kind. */
    public record Derived(Placement placement, String name, String source, String kind, ObjectNode scenario) {
        public String file() { return placement.name() + "/" + name + ".json"; }
        /** Whether the scenario's event-time replay recording is locked beside it ({@link #REPLAYED}). */
        public boolean replayed() { return placement == SHARED && kind.equals("semantic") && REPLAYED.contains(name); }
    }

    /** Every engine scenario derived from the step-3 scenarios under {@code fixtures/scenarios}. */
    public static List<Derived> derive(Path step3) {
        List<Derived> l = new ArrayList<>();
        for (Placement p : PLACEMENTS) {
            for (String n : STEP3) {
                if (NOT_REBUILT.containsKey(n)) continue;
                if (p == DEDICATED && !DEDICATED_TOO.contains(n)) continue;
                l.add(new Derived(p, n, n, "semantic", onPlacement(read(step3, n), p)));
            }
            ObjectNode starved = starvationRun(read(step3, "baseline"), p);
            starved.put("name", STARVED_SCENARIO);
            ObjectNode inj = ((ArrayNode) starved.get("injections")).addObject().put("type", "starve-station").put("station", p.site() + ":" + STARVED_STATION);
            Json.putSafe(inj, "from", STARVE_FROM);
            Json.putSafe(inj, "resumeAt", STARVE_RESUME);
            l.add(new Derived(p, STARVED_SCENARIO, "baseline", "injection-24", starved));
            ObjectNode control = starvationRun(read(step3, "baseline"), p);
            control.put("name", STARVED_CONTROL);
            l.add(new Derived(p, STARVED_CONTROL, "baseline", "injection-24-control", control));
            for (String n : List.of(BASELINE_X2, BASELINE_HALF)) {
                ObjectNode x = onPlacement(read(step3, "baseline"), p);
                x.put("name", n);
                long batch = x.get("batchMs").longValue();
                Json.putSafe(x, "batchMs", n.equals(BASELINE_X2) ? batch * 2 : batch / 2);
                l.add(new Derived(p, n, "baseline", "speed-change", x));
            }
        }
        return l;
    }

    /** The step-3 scenario on {@code p}: the hierarchy root renamed, the placement's release pinned, the Flink W1 form. */
    static ObjectNode onPlacement(JsonNode step3, Placement p) {
        ObjectNode s = (ObjectNode) rename(step3, p.site());
        ObjectNode rule = (ObjectNode) s.get("rule");
        String release = Json.text(rule, "releaseId");
        switch (release) {
            case STEP3_RELEASE -> { rule.put("stream", p.stream()); rule.put("releaseId", p.release()); }
            case STEP3_PLC_RELEASE -> { rule.put("stream", p.plcStream()); rule.put("releaseId", p.plcRelease()); }
            case STEP3_PLC_NOCHECK_RELEASE -> { rule.put("stream", p.plcNocheckStream()); rule.put("releaseId", p.plcNocheckRelease()); }
            default -> throw new IllegalArgumentException("scenario " + Json.text(s, "name") + ": no governed release for " + release);
        }
        s.put("w1Form", W1_FORM);
        return s;
    }

    /** The step-3 baseline on {@code p} with the placement's starvation release pinned and the starved station's incident
     *  ({@link #STARVED_INCIDENT}): injection 24's run and its control before the starvation overlay. */
    static ObjectNode starvationRun(JsonNode baseline, Placement p) {
        ObjectNode s = onPlacement(baseline, p);
        ObjectNode rule = (ObjectNode) s.get("rule");
        if (!p.release().equals(Json.text(rule, "releaseId"))) throw new IllegalArgumentException("the starvation run derives from the occupancy-join baseline");
        rule.put("stream", p.starvationStream()).put("releaseId", p.starvationRelease());
        String station = p.site() + ":" + STARVED_STATION;
        boolean overlaps = false;
        for (JsonNode v : s.path("visits"))
            if (station.equals(Json.text(v, "station")) && v.path("enter").longValue() == STARVED_INCIDENT_FROM && v.path("leave").longValue() == STARVED_INCIDENT_TO) overlaps = true;
        if (!overlaps) throw new IllegalArgumentException("no visit of " + station + " over [" + STARVED_INCIDENT_FROM + ", " + STARVED_INCIDENT_TO + ")");
        if (!s.path("incidents").isEmpty()) throw new IllegalArgumentException("the baseline carries incidents");
        ObjectNode f = ((ArrayNode) s.get("incidents")).addObject().put("incidentId", STARVED_INCIDENT).putArray("revisions").addObject()
            .put("revision", 1).put("deliverAt", STARVED_INCIDENT_DELIVER).putObject("fields");
        f.put("stationRef", station).put("equipmentRef", station).put("kind", "tool-check-failed");
        Json.putSafe(f, "detectedAt", STARVED_INCIDENT_TO);
        Json.putSafe(f, "revisionEventTime", STARVED_INCIDENT_TO);
        Json.putSafe(f, "suspectFrom", STARVED_INCIDENT_FROM);
        Json.putSafe(f, "suspectTo", STARVED_INCIDENT_TO);
        f.put("status", "closed").put("evidenceRef", "urn:line2:qms:" + STARVED_INCIDENT).put("source", "qms-line2");
        return s;
    }

    /** {@code n} with every key and string value under the step-3 root moved to {@code site}. */
    static JsonNode rename(JsonNode n, String site) {
        if (n.isTextual()) return TextNode.valueOf(renamed(n.textValue(), site));
        if (n.isObject()) {
            ObjectNode o = Json.object();
            n.fields().forEachRemaining(e -> o.set(renamed(e.getKey(), site), rename(e.getValue(), site)));
            return o;
        }
        if (n.isArray()) {
            ArrayNode a = Json.JSON.createArrayNode();
            n.forEach(x -> a.add(rename(x, site)));
            return a;
        }
        return n.deepCopy();
    }

    private static String renamed(String s, String site) {
        if (s.startsWith(STEP3_ROOT + ":")) return site + s.substring(STEP3_ROOT.length());
        if (s.contains(STEP3_ROOT)) throw new IllegalArgumentException("the step-3 root inside a value: " + s);
        return s;
    }

    /** {@code engine-scenarios.json}: the placements, every engine scenario and the step-3 scenarios not rebuilt. */
    public static ObjectNode index(List<Derived> derived) {
        ObjectNode n = Json.object().put("indexVersion", 1).put("w1Form", W1_FORM);
        ObjectNode ps = n.putObject("placements");
        for (Placement p : PLACEMENTS) ps.putObject(p.name()).put("site", p.site()).put("stream", p.stream()).put("release", p.release())
            .put("plcStream", p.plcStream()).put("plcRelease", p.plcRelease()).put("plcNocheckStream", p.plcNocheckStream()).put("plcNocheckRelease", p.plcNocheckRelease())
            .put("starvationStream", p.starvationStream()).put("starvationRelease", p.starvationRelease());
        n.put("registry", "fixtures/live/registry").put("trustedKeys", "fixtures/live/trusted-keys.jsonl");
        ArrayNode sc = n.putArray("scenarios");
        for (Derived d : derived) {
            ObjectNode e = sc.addObject().put("name", d.name()).put("placement", d.placement().name()).put("file", "scenarios/" + d.file())
                .put("lock", "scenarios/" + d.placement().name() + "/" + d.name() + ".lock.json");
            if (d.replayed()) e.put("replayLock", "scenarios/" + d.placement().name() + "/" + d.name() + ".replay.lock.json");
            e.put("source", "fixtures/scenarios/" + d.source() + ".json").put("kind", d.kind()).put("synthetic", d.scenario().path("synthetic").asBoolean(false));
        }
        ObjectNode nr = n.putObject("notRebuilt");
        NOT_REBUILT.forEach(nr::put);
        return n;
    }

    /** Writes every derived scenario and the index under {@code out} ({@code engine-fixtures}). */
    public static List<Derived> write(Path step3, Path out) {
        List<Derived> d = derive(step3);
        for (Derived x : d) Json.write(out.resolve("scenarios").resolve(x.placement().name()).resolve(x.name() + ".json"), x.scenario());
        Json.write(out.resolve(INDEX), index(d));
        return d;
    }

    /** The differences between the committed engine scenarios and index under {@code out} and a fresh derivation (empty when equal). */
    public static List<String> check(Path step3, Path out) {
        List<String> problems = new ArrayList<>();
        List<Derived> d = derive(step3);
        Set<String> want = new TreeSet<>();
        for (Derived x : d) {
            Path f = out.resolve("scenarios").resolve(x.placement().name()).resolve(x.name() + ".json");
            want.add(x.placement().name() + "/" + x.name() + ".json");
            if (!Files.isRegularFile(f)) { problems.add("missing " + f); continue; }
            if (!Arrays.equals(bytes(f), Json.pretty(x.scenario()))) problems.add("differs from its derivation: " + f);
            if (!Files.isRegularFile(ScenarioBuilder.lockOf(f))) problems.add("no lock beside " + f);
            else if (x.replayed()) {
                want.add(x.placement().name() + "/" + x.name() + REPLAY_LOCK_SUFFIX);
                problems.addAll(replayCompanion(f));
            }
        }
        for (Placement p : PLACEMENTS) {
            Path dir = out.resolve("scenarios").resolve(p.name());
            if (!Files.isDirectory(dir)) continue;
            try (var s = Files.list(dir)) {
                for (Path f : s.toList()) {
                    String n = f.getFileName().toString();
                    if (n.endsWith(REPLAY_LOCK_SUFFIX)) {
                        // a replay lock is a companion of a base lock of REPLAYED; any other is refused
                        if (!want.contains(p.name() + "/" + n)) problems.add("not the replay lock of an engine scenario of REPLAYED: " + f);
                        continue;
                    }
                    String scen = n.endsWith(".lock.json") ? n.substring(0, n.length() - ".lock.json".length()) + ".json" : n;
                    if (!want.contains(p.name() + "/" + scen)) problems.add("not an engine scenario: " + f);
                }
            } catch (IOException e) { throw new UncheckedIOException(e); }
        }
        Path idx = out.resolve(INDEX);
        if (!Files.isRegularFile(idx) || !Arrays.equals(bytes(idx), Json.pretty(index(d)))) problems.add("differs from its derivation: " + idx);
        return problems;
    }

    static final String REPLAY_LOCK_SUFFIX = ".replay.lock.json";

    /** The replay-companion rule: the replay lock beside {@code scenario} exists and belongs to its base lock, the same scenario name and
     *  scenario digest, the original's cut count (a replay keeps the original's cut ids), the replay generator, a non-empty file table,
     *  and the lock's canonical bytes. Its file digests are checked by rematerializing the replay ({@code check-engine}). */
    static List<String> replayCompanion(Path scenario) {
        List<String> problems = new ArrayList<>();
        Path base = ScenarioBuilder.lockOf(scenario), replay = ReplayRecording.lockOf(scenario);
        if (!Files.isRegularFile(replay)) { problems.add("no replay lock beside " + scenario); return problems; }
        byte[] bytes = bytes(replay);
        JsonNode r, b = Json.read(bytes(base));
        try { r = Json.read(bytes); } catch (RuntimeException e) { problems.add("not a lock: " + replay + ": " + e.getMessage()); return problems; }
        if (!r.isObject() || !Arrays.equals(bytes, Json.pretty(r))) { problems.add("not in the lock's canonical form: " + replay); return problems; }
        if (r.path("lockVersion").asInt(-1) != b.path("lockVersion").asInt(-2)) problems.add("lockVersion differs from its base lock: " + replay);
        for (String field : List.of("scenario", "scenarioSha256"))
            if (!r.path(field).isTextual() || !r.get(field).equals(b.get(field))) problems.add(field + " differs from its base lock " + base.getFileName() + ": " + replay);
        if (!r.path("counts").path("cuts").isIntegralNumber() || r.path("counts").path("cuts").longValue() != b.path("counts").path("cuts").longValue())
            problems.add("counts.cuts differs from its base lock " + base.getFileName() + ": " + replay);
        if (!ReplayRecording.GENERATOR.equals(r.path("generator").textValue())) problems.add("generator is not " + ReplayRecording.GENERATOR + ": " + replay);
        if (!r.path("outOfOrdernessMs").isIntegralNumber()) problems.add("no outOfOrdernessMs: " + replay);
        if (!r.path("files").isObject() || r.path("files").isEmpty()) problems.add("no files: " + replay);
        return problems;
    }

    private static JsonNode read(Path step3, String name) { return Json.read(step3.resolve(name + ".json")); }
    private static byte[] bytes(Path p) { try { return Files.readAllBytes(p); } catch (IOException e) { throw new UncheckedIOException(e); } }
}
