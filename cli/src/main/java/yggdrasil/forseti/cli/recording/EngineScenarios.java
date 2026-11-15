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
 *  ({@code fixtures/live/registry}) and setting the W1 form; nothing else changes, so the visits, overlays, timing and seeds are the
 *  step-3 scenario's.
 *
 *  <p>The derived files live under {@code engine-fixtures/scenarios/<placement>/}, outside the paths of the core-input key (the core gate
 *  never replays them), each beside its lock in the step-3 lock format ({@link ScenarioBuilder#lockOf}); {@code
 *  engine-fixtures/engine-scenarios.json} lists them with their source and the step-3 scenarios that are not rebuilt and why. The
 *  builder that turns them into recordings stays in the core key. {@link #write} writes every file; {@link #check} compares the
 *  committed scenario files and index with a fresh derivation (the locks are checked by rebuilding, {@code forseti recordings
 *  verify-engine}). */
public final class EngineScenarios {
    private EngineScenarios() {}

    public static final String W1_FORM = W1Form.MIN_OVER_PARTITIONS.wire;
    static final String STEP3_ROOT = "synthetic-site";
    public static final String INDEX = "engine-scenarios.json";

    /** A governed placement: its site id and the published streams of its rules (occupancy-join and plc-stamped). */
    public record Placement(String name, String site, String stream, String release, String plcStream, String plcRelease) {}
    public static final Placement SHARED = new Placement("shared", "site-019", "line2-shared", "line2-rules-shared", "line2-shared-plc", "line2-rules-shared-plc");
    public static final Placement DEDICATED = new Placement("dedicated", "site-071", "line2-dedicated", "line2-rules-dedicated", "line2-dedicated-plc", "line2-rules-dedicated-plc");
    public static final List<Placement> PLACEMENTS = List.of(SHARED, DEDICATED);

    /** The step-3 semantic scenarios, in the core gate's order (hand-made, horizon, smoke). */
    public static final List<String> STEP3 = List.of("baseline", "held-back-10min", "redelivery", "overlap", "bad-quality", "clock-plus-30", "clock-plus-90",
        "corrections", "corrections-10b-before", "corrections-10b-after", "corrections-10c", "session", "quiet-occupancy", "quiet-occupancy-control",
        "plc-stamped", "plc-nocheck", "plc-held-back", "stage2", "stage2-early", "cascade",
        "horizon-no-departure", "horizon-reopened", "horizon-correction-beyond", "horizon-planned-late", "plc-horizon-no-end",
        "smoke", "smoke-plc", "smoke-incident");
    /** Step-3 scenarios not rebuilt, with the reason. */
    public static final Map<String, String> NOT_REBUILT = new LinkedHashMap<>();
    static {
        String noRelease = "its rule is the plc-stamped rule with crossCheck none (line2-rules-plc-nocheck 1.0.0); no release of that rule is published for the governed placements";
        NOT_REBUILT.put("plc-nocheck", noRelease);
        NOT_REBUILT.put("plc-held-back", noRelease);
    }
    /** The scenarios also built on the dedicated placement. */
    public static final List<String> DEDICATED_TOO = List.of("baseline", "smoke");

    /** Injection 24: the station starved, and the window (milliseconds from T0) in which its telemetry is withheld. */
    public static final String STARVED_SCENARIO = "starved-station3", STARVED_STATION = "line2:station3";
    public static final long STARVE_FROM = 1_500_000, STARVE_RESUME = 2_100_000;
    /** Item 5 (d): the baseline at batch interval x2 and x1/2. */
    public static final String BASELINE_X2 = "baseline-batch-x2", BASELINE_HALF = "baseline-batch-half";

    /** One derived scenario: its placement, name, step-3 source and kind. */
    public record Derived(Placement placement, String name, String source, String kind, ObjectNode scenario) {
        public String file() { return placement.name() + "/" + name + ".json"; }
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
            ObjectNode starved = onPlacement(read(step3, "baseline"), p);
            starved.put("name", STARVED_SCENARIO);
            ObjectNode inj = ((ArrayNode) starved.get("injections")).addObject().put("type", "starve-station").put("station", p.site() + ":" + STARVED_STATION);
            Json.putSafe(inj, "from", STARVE_FROM);
            Json.putSafe(inj, "resumeAt", STARVE_RESUME);
            l.add(new Derived(p, STARVED_SCENARIO, "baseline", "injection-24", starved));
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
        boolean plc = Json.text(rule, "releaseId").equals("line2-rules-plc");
        if (!plc && !Json.text(rule, "releaseId").equals("line2-rules")) throw new IllegalArgumentException("scenario " + Json.text(s, "name") + ": no governed release for " + Json.text(rule, "releaseId"));
        rule.put("stream", plc ? p.plcStream() : p.stream());
        rule.put("releaseId", plc ? p.plcRelease() : p.release());
        s.put("w1Form", W1_FORM);
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
            .put("plcStream", p.plcStream()).put("plcRelease", p.plcRelease());
        n.put("registry", "fixtures/live/registry").put("trustedKeys", "fixtures/live/trusted-keys.jsonl");
        ArrayNode sc = n.putArray("scenarios");
        for (Derived d : derived) sc.addObject().put("name", d.name()).put("placement", d.placement().name()).put("file", "scenarios/" + d.file())
            .put("lock", "scenarios/" + d.placement().name() + "/" + d.name() + ".lock.json").put("source", "fixtures/scenarios/" + d.source() + ".json").put("kind", d.kind())
            .put("synthetic", d.scenario().path("synthetic").asBoolean(false));
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
        }
        for (Placement p : PLACEMENTS) {
            Path dir = out.resolve("scenarios").resolve(p.name());
            if (!Files.isDirectory(dir)) continue;
            try (var s = Files.list(dir)) {
                for (Path f : s.toList()) {
                    String n = f.getFileName().toString();
                    String scen = n.endsWith(".lock.json") ? n.substring(0, n.length() - ".lock.json".length()) + ".json" : n;
                    if (!want.contains(p.name() + "/" + scen)) problems.add("not an engine scenario: " + f);
                }
            } catch (IOException e) { throw new UncheckedIOException(e); }
        }
        Path idx = out.resolve(INDEX);
        if (!Files.isRegularFile(idx) || !Arrays.equals(bytes(idx), Json.pretty(index(d)))) problems.add("differs from its derivation: " + idx);
        return problems;
    }

    private static JsonNode read(Path step3, String name) { return Json.read(step3.resolve(name + ".json")); }
    private static byte[] bytes(Path p) { try { return Files.readAllBytes(p); } catch (IOException e) { throw new UncheckedIOException(e); } }
}
