package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The core gate split across CI jobs ({@code gate/core-shards.json}). Each shard names the scenarios it makes, the scenarios whose
 *  {@code --shuffle} runs it makes, the manifest assertions it evaluates and the generation runs it makes ({@code generationRuns},
 *  required); {@code aggregate} names the assertions evaluated over the summaries of every shard ({@link Summaries}). The plan partitions
 *  the manifest: every manifest id is in exactly one shard or in {@code aggregate}, every shard owns at least one assertion, every
 *  scenario of {@link Scenarios#all()} is made by at least one shard, and every generation run of {@link Scenarios#GENERATION_RUNS} by
 *  exactly one shard that also makes its scenario.
 *
 *  <p>{@code forseti gate --engine core --shard <name>} runs one shard: its assertions, every scenario it names (each compared with the
 *  oracle at every cut, as in a full run), and writes {@code report.json} and {@code summaries.json}. {@code --aggregate <dir>} reads
 *  {@code <dir>/<shard>/} of every shard and passes only when every shard ran against the same manifest, plan, fixtures, seed and pins,
 *  every shard's assertions are recorded, every scenario was compared with the oracle at every cut its lock plans and held integrity
 *  and ground truth, the summaries of a scenario made by two shards agree, and the aggregate assertions hold. Each shard directory is
 *  one bundle checked by {@link GateEvidence#validate} (its run records, cut logs and, for a shared store, {@code state/}); the
 *  aggregate run binds every shard's {@code report.json} and {@code summaries.json} by the raw digest of the bytes it parsed
 *  ({@code shards[]}) and merges the shards' {@code generations[]} and {@code runs[]} under their shard. */
final class Shards {
    private Shards() {}

    static final String PLAN = "gate/core-shards.json", SUMMARIES = "summaries.json";
    /** Summary fields two shards that made the same scenario may differ on: timings and the evidence text that embeds them. */
    static final Set<String> NOT_COMPARED = Set.of("ms", "summary");

    /** One shard: its scenarios, the scenarios whose {@code --shuffle} runs it makes, its assertions and the generation runs it makes
     *  ({@link Scenarios#GENERATION_RUNS}, by name). */
    record Shard(String name, List<String> scenarios, List<String> shuffled, List<String> assertions, List<String> generationRuns) {
        Shard(String name, List<String> scenarios, List<String> shuffled, List<String> assertions) { this(name, scenarios, shuffled, assertions, List.of()); }
    }
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
        for (JsonNode s : n.path("shards")) shards.add(new Shard(Json.text(s, "name"), texts(s, "scenarios"), texts(s, "shuffled"), texts(s, "assertions"), texts(s, "generationRuns")));
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
        Map<String, String> generationRunBy = new HashMap<>();
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
            if (s.assertions().isEmpty()) p.add("shard " + s.name() + " owns no assertion");
            for (String g : s.generationRuns()) {
                Scenarios.GenerationRun run = Scenarios.generationRun(g);
                if (run == null) { p.add("shard " + s.name() + ": no generation run " + g); continue; }
                if (!s.scenarios().contains(run.scenario())) p.add("shard " + s.name() + ": generation run " + g + " needs its scenario " + run.scenario() + " in the shard");
                if (generationRunBy.put(g, s.name()) != null) p.add("generation run " + g + " is made by two shards");
            }
        }
        for (String id : plan.aggregate()) {
            if (owner.put(id, "aggregate") != null) p.add(id + " is in two places of the plan");
            if (!aggregatable.contains(id)) p.add(id + " is not an assertion the aggregate run can evaluate");
        }
        for (String id : manifestIds) if (!owner.containsKey(id)) p.add(id + " is in no shard and not in aggregate");
        for (String id : owner.keySet()) if (!manifestIds.contains(id)) p.add(id + " (" + owner.get(id) + ") is not in the manifest");
        for (String sc : Scenarios.all()) if (!made.contains(sc)) p.add("no shard makes scenario " + sc);
        for (String sc : Scenarios.SHUFFLED) if (!shuffledBy.contains(sc)) p.add("no shard makes the shuffled runs of " + sc);
        for (Scenarios.GenerationRun g : Scenarios.GENERATION_RUNS) if (!generationRunBy.containsKey(g.name())) p.add("no shard makes generation run " + g.name());
        return p;
    }

    /** One assertion result a shard recorded. */
    record Recorded(String shard, String id, boolean ok, String evidence) {}

    /** The shard outputs under {@code dir}: every shard's recorded results (plan order) and the merged summaries, with every problem
     *  found in {@code problems}. {@code expect} holds what every shard report must state: {@code manifestSha256, seed, pins,
     *  fixtureDigests}. {@code out} receives {@code shards[]} ({@code {name, exit, reportSha256, summariesSha256}} per shard of the plan,
     *  in plan order) and the shards' {@code generations[]} and {@code runs[]} concatenated in plan order, unchanged. */
    static Summaries read(Path root, Path dir, Plan plan, ObjectNode expect, List<Recorded> recorded, List<String> problems, ObjectNode out) {
        Summaries merged = new Summaries();
        ArrayNode shardsOut = out.putArray("shards"), gensOut = out.putArray("generations"), runsOut = out.putArray("runs");
        Map<String, List<JsonNode>> byScenario = new LinkedHashMap<>();
        for (Shard s : plan.shards()) {
            Path d = dir.resolve(s.name());
            JsonNode report, summaries;
            byte[] reportBytes, summariesBytes;
            try {
                reportBytes = Files.readAllBytes(d.resolve("report.json"));
                summariesBytes = Files.readAllBytes(d.resolve(SUMMARIES));
                report = Json.read(reportBytes);
                summaries = Json.read(summariesBytes);
            } catch (IOException | RuntimeException e) { problems.add("shard " + s.name() + ": " + e); continue; }
            String reportSha = Canonical.digestOf(reportBytes), summariesSha = Canonical.digestOf(summariesBytes);
            ObjectNode entry = shardsOut.addObject().put("name", s.name());
            entry.set("exit", report.path("exit").isMissingNode() ? null : report.path("exit"));
            entry.put("reportSha256", reportSha).put("summariesSha256", summariesSha);
            if (!"gate".equals(report.path("kind").asText(null))) problems.add("shard " + s.name() + ": its report is of kind " + report.path("kind") + ", not gate");
            if (!"shard".equals(report.path("layout").asText(null))) problems.add("shard " + s.name() + ": its report has layout " + report.path("layout") + ", not shard");
            if (!summariesSha.equals(report.path("summariesSha256").asText(null)))
                problems.add("shard " + s.name() + ": its report binds summaries " + report.path("summariesSha256") + ", " + SUMMARIES + " is " + summariesSha);
            for (String line : GateEvidence.validate(root, d, report, summaries)) problems.add("shard " + s.name() + ": " + line);
            Map<String, Integer> online = new HashMap<>();
            for (JsonNode g : report.path("generations")) {
                if (!s.name().equals(g.path("shard").asText(null))) problems.add("shard " + s.name() + ": a generation entry names shard " + g.path("shard"));
                gensOut.add(g);
            }
            for (JsonNode r : report.path("runs")) {
                if (!s.name().equals(r.path("shard").asText(null))) problems.add("shard " + s.name() + ": a run entry names shard " + r.path("shard"));
                if (GateEvidence.PIPELINE.equals(r.path("store").asText()) && !r.path("shuffle").asBoolean() && GateEvidence.ONLINE.equals(r.path("variant").asText()))
                    online.merge(r.path("scenario").asText(), 1, Integer::sum);
                runsOut.add(r);
            }
            for (String sc : s.scenarios())
                if (online.getOrDefault(sc, 0) != 1) problems.add("shard " + s.name() + ": " + online.getOrDefault(sc, 0) + " online pipeline runs of " + sc + ", one required");
            if (!s.name().equals(report.path("shard").asText(null))) problems.add("shard " + s.name() + ": its report names shard " + report.path("shard"));
            if (!plan.sha256().equals(report.path("shardPlanSha256").asText(null))) problems.add("shard " + s.name() + ": ran against another " + PLAN);
            for (String f : List.of("manifestSha256", "seed", "pins", "fixtureDigests"))
                if (!same(expect.path(f), report.path(f))) problems.add("shard " + s.name() + ": " + f + " " + report.path(f) + ", the aggregate run's " + expect.path(f));
            if (!report.path("error").isNull()) problems.add("shard " + s.name() + ": " + report.path("error").asText());
            if (report.path("exit").asLong(-1) != 0) problems.add("shard " + s.name() + ": exited " + report.path("exit"));
            Set<String> seen = new HashSet<>(), notOk = new TreeSet<>();
            for (JsonNode r : report.path("results")) {
                String id = r.path("id").asText();
                if (!s.assertions().contains(id)) { problems.add("shard " + s.name() + " recorded " + id + ", which the plan does not give it"); continue; }
                if (!seen.add(id)) { problems.add("shard " + s.name() + " recorded " + id + " twice"); continue; }
                boolean ok = r.path("ok").asBoolean(false);
                if (!ok) notOk.add(id);
                recorded.add(new Recorded(s.name(), id, ok, r.path("evidence").asText("")));
            }
            for (String id : s.assertions()) if (!seen.contains(id)) problems.add("shard " + s.name() + " did not record " + id);
            // the report's own accounting must agree with its results: a shard never passes on counts its results do not back
            Set<String> failed = new TreeSet<>(), notRun = new TreeSet<>();
            report.path("failed").forEach(x -> failed.add(x.asText()));
            report.path("notRun").forEach(x -> notRun.add(x.asText()));
            if (!failed.equals(notOk)) problems.add("shard " + s.name() + ": its report lists failed " + failed + ", its results fail " + notOk);
            if (!notRun.isEmpty()) problems.add("shard " + s.name() + ": not run " + notRun);
            long expected = report.path("assertionsExpected").asLong(-1), ran = report.path("assertionsRun").asLong(-1);
            if (expected != s.assertions().size() || ran != s.assertions().size() || seen.size() != s.assertions().size())
                problems.add("shard " + s.name() + ": assertions run " + ran + " / expected " + expected + " with " + seen.size() + " results, its plan gives it " + s.assertions().size());
            Set<String> runs = new HashSet<>();
            for (JsonNode r : summaries.path("runs")) {
                String name = r.path("name").asText();
                if (!s.scenarios().contains(name)) { problems.add("shard " + s.name() + " summarised " + name + ", which the plan does not give it"); continue; }
                if (!runs.add(name)) { problems.add("shard " + s.name() + " summarised " + name + " twice"); continue; }
                byScenario.computeIfAbsent(name, k -> new ArrayList<>()).add(r);
            }
            for (String sc : s.scenarios()) if (!runs.contains(sc)) problems.add("shard " + s.name() + " has no summary of " + sc);
            Set<String> shuffled = new HashSet<>();
            for (JsonNode x : summaries.path("shuffled")) {
                String key = x.path("name").asText() + "@" + x.path("seed").asLong();
                if (!s.shuffled().contains(x.path("name").asText()) || !Scenarios.SHUFFLE_SEEDS.contains(x.path("seed").asLong())) { problems.add("shard " + s.name() + " summarised shuffled run " + key + ", which the plan does not give it"); continue; }
                if (!shuffled.add(key)) { problems.add("shard " + s.name() + " summarised shuffled run " + key + " twice"); continue; }
                merged.shuffled.put(key, x);
            }
            for (String sc : s.shuffled()) for (long seed : Scenarios.SHUFFLE_SEEDS)
                if (!shuffled.contains(sc + "@" + seed)) problems.add("shard " + s.name() + " has no summary of shuffled run " + sc + "@" + seed);
            JsonNode gr = summaries.path("generationRuns");
            if (!gr.isObject()) problems.add("shard " + s.name() + ": " + SUMMARIES + " has no generationRuns object keyed by name");
            else gr.fields().forEachRemaining(e -> {
                if (!s.generationRuns().contains(e.getKey())) { problems.add("shard " + s.name() + " summarised generation run " + e.getKey() + ", which the plan does not give it"); return; }
                merged.generationRuns.put(e.getKey(), e.getValue());
            });
            for (String g : s.generationRuns()) if (!gr.has(g)) problems.add("shard " + s.name() + " has no summary of generation run " + g);
        }
        for (String sc : Scenarios.all()) {
            List<JsonNode> l = byScenario.get(sc);
            if (l == null) { merged.missing.add(sc + " not run: no shard summarised it"); continue; }
            merged.runs.put(sc, l.get(0));
            for (JsonNode other : l.subList(1, l.size())) {
                Set<String> fields = new TreeSet<>();
                l.get(0).fieldNames().forEachRemaining(fields::add);
                other.fieldNames().forEachRemaining(fields::add);
                fields.removeAll(NOT_COMPARED);
                for (String f : fields)
                    if (!same(l.get(0).path(f), other.path(f))) problems.add(sc + ": two shards disagree on " + f + " (" + l.get(0).path(f) + " vs " + other.path(f) + ")");
            }
        }
        for (String sc : Scenarios.SHUFFLED) for (long seed : Scenarios.SHUFFLE_SEEDS)
            if (!merged.shuffled.containsKey(sc + "@" + seed)) merged.missing.add(sc + " seed " + seed + " not run: no shard summarised it");
        for (Scenarios.GenerationRun g : Scenarios.GENERATION_RUNS)
            if (!merged.generationRuns.containsKey(g.name())) merged.missing.add(g.name() + " not run: no shard summarised it");
        problems.addAll(merged.missing);
        return merged;
    }

    static List<Long> longs(JsonNode a) {
        List<Long> l = new ArrayList<>();
        a.forEach(x -> l.add(x.asLong()));
        return l;
    }

    /** Where {@code got} departs from {@code want}: the position, the two values there, and the two lengths. */
    static String firstDifference(List<Long> got, List<Long> want) {
        int i = 0;
        while (i < got.size() && i < want.size() && got.get(i).equals(want.get(i))) i++;
        return "(at position " + i + ": " + (i < got.size() ? got.get(i) : "none") + " vs " + (i < want.size() ? want.get(i) : "none")
            + "; " + got.size() + " vs " + want.size() + " cuts)";
    }

    /** Equal as JSON values (an integer read back from a file is equal to the long it was written from). */
    static boolean same(JsonNode a, JsonNode b) {
        if (a.isMissingNode() || b.isMissingNode()) return a.isMissingNode() && b.isMissingNode();
        ObjectNode x = Json.object(), y = Json.object();
        x.set("v", a);
        y.set("v", b);
        return Canonical.digest(x).equals(Canonical.digest(y));
    }
}
