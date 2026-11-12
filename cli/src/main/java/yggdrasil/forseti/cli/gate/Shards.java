package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The core gate split across CI jobs ({@code gate/core-shards.json}). Each shard names the scenarios it makes, the scenarios whose
 *  {@code --shuffle} runs it makes, and the manifest assertions it evaluates; {@code aggregate} names the assertions evaluated over the
 *  summaries of every shard ({@link Summaries}). The plan partitions the manifest: every manifest id is in exactly one shard or in
 *  {@code aggregate}, and every scenario of {@link Scenarios#all()} is made by at least one shard.
 *
 *  <p>{@code forseti gate --engine core --shard <name>} runs one shard: its assertions, every scenario it names (each compared with the
 *  oracle at every cut, as in a full run), and writes {@code report.json} and {@code summaries.json}. {@code --aggregate <dir>} reads
 *  {@code <dir>/<shard>/} of every shard and passes only when every shard ran against the same manifest, plan, fixtures, seed and pins,
 *  every shard's assertions are recorded, every scenario was compared with the oracle at every cut its lock plans and held integrity
 *  and ground truth, the summaries of a scenario made by two shards agree, and the aggregate assertions hold. */
final class Shards {
    private Shards() {}

    static final String PLAN = "gate/core-shards.json", SUMMARIES = "summaries.json";

    record Shard(String name, List<String> scenarios, List<String> shuffled, List<String> assertions) {}
    record Plan(List<Shard> shards, List<String> aggregate, String sha256) {
        Shard shard(String name) {
            for (Shard s : shards) if (s.name().equals(name)) return s;
            throw new IllegalArgumentException("--shard " + name + ": not in " + PLAN + " " + names());
        }
        List<String> names() { return shards.stream().map(Shard::name).toList(); }
    }

    static Plan load(Path root) {
        Path f = root.resolve(PLAN);
        byte[] bytes;
        try { bytes = Files.readAllBytes(f); } catch (IOException e) { throw new IllegalArgumentException("cannot read " + PLAN + ": " + e); }
        JsonNode n = Json.read(bytes);
        List<Shard> shards = new ArrayList<>();
        for (JsonNode s : n.path("shards")) shards.add(new Shard(Json.text(s, "name"), texts(s, "scenarios"), texts(s, "shuffled"), texts(s, "assertions")));
        return new Plan(List.copyOf(shards), texts(n, "aggregate"), Canonical.digestOf(bytes));
    }

    static List<String> texts(JsonNode n, String field) {
        if (!n.path(field).isArray()) throw new IllegalArgumentException(PLAN + ": " + field + " is not an array: " + n);
        List<String> l = new ArrayList<>();
        n.path(field).forEach(x -> l.add(x.asText()));
        return List.copyOf(l);
    }

    /** What is wrong with {@code plan} against the manifest ids, the aggregate assertions this gate implements and the scenario lists. */
    static List<String> validate(Plan plan, List<String> manifestIds, Set<String> aggregatable) {
        List<String> p = new ArrayList<>();
        Map<String, String> owner = new HashMap<>();
        Set<String> names = new HashSet<>(), made = new HashSet<>(), shuffledBy = new HashSet<>();
        for (Shard s : plan.shards()) {
            if (!s.name().matches("[a-z0-9][a-z0-9-]*")) p.add("shard name " + s.name() + " is not [a-z0-9-]");
            if (!names.add(s.name())) p.add("shard " + s.name() + " listed twice");
            if (new HashSet<>(s.scenarios()).size() != s.scenarios().size()) p.add("shard " + s.name() + " lists a scenario twice");
            for (String sc : s.scenarios()) { if (!Scenarios.all().contains(sc)) p.add("shard " + s.name() + ": no scenario " + sc); made.add(sc); }
            for (String sc : s.shuffled()) {
                if (!Scenarios.SHUFFLED.contains(sc) || !s.scenarios().contains(sc)) p.add("shard " + s.name() + ": shuffled " + sc + " is not a shuffled scenario it makes");
                if (!shuffledBy.add(sc)) p.add("the shuffled runs of " + sc + " are made by two shards");
            }
            for (String id : s.assertions()) if (owner.put(id, s.name()) != null) p.add(id + " is in two places of the plan");
        }
        for (String id : plan.aggregate()) {
            if (owner.put(id, "aggregate") != null) p.add(id + " is in two places of the plan");
            if (!aggregatable.contains(id)) p.add(id + " is not an assertion the aggregate run can evaluate");
        }
        for (String id : manifestIds) if (!owner.containsKey(id)) p.add(id + " is in no shard and not in aggregate");
        for (String id : owner.keySet()) if (!manifestIds.contains(id)) p.add(id + " (" + owner.get(id) + ") is not in the manifest");
        for (String sc : Scenarios.all()) if (!made.contains(sc)) p.add("no shard makes scenario " + sc);
        for (String sc : Scenarios.SHUFFLED) if (!shuffledBy.contains(sc)) p.add("no shard makes the shuffled runs of " + sc);
        return p;
    }

    /** One assertion result a shard recorded. */
    record Recorded(String shard, String id, boolean ok, String evidence) {}

    /** The shard outputs under {@code dir}: every shard's recorded results (plan order) and the merged summaries, with every problem
     *  found in {@code problems}. {@code expect} holds what every shard report must state: {@code manifestSha256, seed, pins,
     *  fixtureDigests}. */
    static Summaries read(Path root, Path dir, Plan plan, ObjectNode expect, List<Recorded> recorded, List<String> problems) {
        Summaries merged = new Summaries();
        Map<String, List<JsonNode>> byScenario = new LinkedHashMap<>();
        for (Shard s : plan.shards()) {
            Path d = dir.resolve(s.name());
            JsonNode report, summaries;
            try { report = Json.read(d.resolve("report.json")); summaries = Json.read(d.resolve(SUMMARIES)); }
            catch (RuntimeException e) { problems.add("shard " + s.name() + ": " + e.getMessage()); continue; }
            if (!s.name().equals(report.path("shard").asText(null))) problems.add("shard " + s.name() + ": its report names shard " + report.path("shard"));
            if (!plan.sha256().equals(report.path("shardPlanSha256").asText(null))) problems.add("shard " + s.name() + ": ran against another " + PLAN);
            for (String f : List.of("manifestSha256", "seed", "pins", "fixtureDigests"))
                if (!same(expect.path(f), report.path(f))) problems.add("shard " + s.name() + ": " + f + " " + report.path(f) + ", the aggregate run's " + expect.path(f));
            if (!report.path("error").isNull()) problems.add("shard " + s.name() + ": " + report.path("error").asText());
            Set<String> seen = new HashSet<>();
            for (JsonNode r : report.path("results")) {
                String id = r.path("id").asText();
                if (!s.assertions().contains(id)) { problems.add("shard " + s.name() + " recorded " + id + ", which the plan does not give it"); continue; }
                seen.add(id);
                recorded.add(new Recorded(s.name(), id, r.path("ok").asBoolean(false), r.path("evidence").asText("")));
            }
            for (String id : s.assertions()) if (!seen.contains(id)) problems.add("shard " + s.name() + " did not record " + id);
            Set<String> runs = new HashSet<>();
            for (JsonNode r : summaries.path("runs")) {
                String name = r.path("name").asText();
                if (!s.scenarios().contains(name)) { problems.add("shard " + s.name() + " summarised " + name + ", which the plan does not give it"); continue; }
                runs.add(name);
                byScenario.computeIfAbsent(name, k -> new ArrayList<>()).add(r);
            }
            for (String sc : s.scenarios()) if (!runs.contains(sc)) problems.add("shard " + s.name() + " has no summary of " + sc);
            Set<String> shuffled = new HashSet<>();
            for (JsonNode x : summaries.path("shuffled")) {
                String key = x.path("name").asText() + "@" + x.path("seed").asLong();
                if (!s.shuffled().contains(x.path("name").asText())) { problems.add("shard " + s.name() + " summarised shuffled run " + key + ", which the plan does not give it"); continue; }
                shuffled.add(key);
                merged.shuffled.put(key, x);
            }
            for (String sc : s.shuffled()) for (long seed : Scenarios.SHUFFLE_SEEDS)
                if (!shuffled.contains(sc + "@" + seed)) problems.add("shard " + s.name() + " has no summary of shuffled run " + sc + "@" + seed);
        }
        for (String sc : Scenarios.all()) {
            List<JsonNode> l = byScenario.get(sc);
            if (l == null) { merged.missing.add(sc + " not run: no shard summarised it"); continue; }
            merged.runs.put(sc, l.get(0));
            for (JsonNode other : l.subList(1, l.size()))
                for (String f : List.of("ok", "cutLogCuts", "cutsCompared", "equal", "findings", "rows"))
                    if (!same(l.get(0).path(f), other.path(f))) problems.add(sc + ": two shards disagree on " + f + " (" + l.get(0).path(f) + " vs " + other.path(f) + ")");
        }
        for (String sc : Scenarios.SHUFFLED) for (long seed : Scenarios.SHUFFLE_SEEDS)
            if (!merged.shuffled.containsKey(sc + "@" + seed)) merged.missing.add(sc + " seed " + seed + " not run: no shard summarised it");
        problems.addAll(coverage(root, merged));
        return merged;
    }

    /** Equal as JSON values (an integer read back from a file is equal to the long it was written from). */
    static boolean same(JsonNode a, JsonNode b) {
        if (a.isMissingNode() || b.isMissingNode()) return a.isMissingNode() && b.isMissingNode();
        ObjectNode x = Json.object(), y = Json.object();
        x.set("v", a);
        y.set("v", b);
        return Canonical.digest(x).equals(Canonical.digest(y));
    }

    /** Every summarised scenario was compared with the oracle at every cut its committed lock plans, and held. */
    static List<String> coverage(Path root, Summaries s) {
        List<String> p = new ArrayList<>(s.missing);
        for (JsonNode r : s.runs.values()) {
            String name = r.path("name").asText();
            long lockCuts;
            try { lockCuts = Json.read(ScenarioBuilder.lockOf(root.resolve("fixtures").resolve("scenarios").resolve(name + ".json"))).path("counts").path("cuts").asLong(-1); }
            catch (RuntimeException e) { p.add(name + ": cannot read its lock: " + e.getMessage()); continue; }
            if (!r.path("ok").asBoolean(false)) p.add(name + " did not hold: " + r.path("summary").asText());
            long compared = r.path("cutsCompared").asLong(-1), completed = r.path("cutLogCuts").asLong(-1);
            if (compared != lockCuts || completed != lockCuts) p.add(name + ": " + compared + " cuts compared with the oracle and " + completed + " completed, its lock plans " + lockCuts);
        }
        for (JsonNode x : s.shuffled.values()) if (Summaries.failed(x)) p.add(x.path("name").asText() + " seed " + x.path("seed").asLong() + ": " + x.path("failure").asText());
        return p;
    }
}
