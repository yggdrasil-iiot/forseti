package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.core.Json;

/**
 * {@code forseti determinism --runs <gate out>,<gate out>,<gate out> [--out <dir>]}: comparison item 6 for an external engine (spec 598,
 * plan D13: nightly), over three executions of the same replay gate run (the same shard, or the whole gate, run three times). Each
 * execution's report must be a {@code gate} report that exited 0, and the three must share their layout, shard, engine, engine artifact
 * and assertion manifest; each must hold the same runs ({@code runs/<label>/comparison.json}, written by {@link ReplayGate}). For every
 * run, every pair of executions (1-2, 1-3, 2-3) is compared at every cut both completed that is comparable (spec 624: the ground-truth
 * deadlines, {@code W1}, {@code admissionW} and open visits of both): every output body partition's fingerprint is equal. A restart run's
 * executions may complete different cuts (its skipped cuts); their cut lists need not be equal, and each pair needs at least one
 * comparable cut; the counts are reported. The fingerprints are of the bodies only, so run ids, topic names, paths, checkpoint ids and every
 * other envelope or diagnostic field never enter the comparison. Revision counts (records per output topic) of every execution are
 * reported from its {@code summaries.json}. Exit 0 when every run held with at least one comparable cut against each other execution, 1
 * otherwise, 2 for a usage error.
 */
public final class Determinism {
    private Determinism() {}

    public static final String FILE = "determinism.json";

    public static int main(List<Path> runs, Path out, PrintStream stdout, PrintStream stderr) {
        if (runs.size() != 3) { stderr.println("forseti determinism: --runs names three gate output directories, not " + runs.size()); return 2; }
        for (Path r : runs) if (!Files.isDirectory(r)) { stderr.println("forseti determinism: " + r + " is not a directory"); return 2; }
        List<String> problems = new ArrayList<>();
        ObjectNode report = Json.object().put("kind", "determinism");
        ArrayNode ex = report.putArray("executions");
        List<Map<String, JsonNode>> byRun = new ArrayList<>();
        List<JsonNode> summaries = new ArrayList<>();
        for (Path r : runs) {
            ObjectNode e = ex.addObject().put("dir", r.toAbsolutePath().toString());
            try {
                JsonNode rep = Json.read(r.resolve("report.json"));
                e.put("layout", rep.path("layout").asText(null)).put("shard", rep.path("shard").asText(null)).put("engine", rep.path("engine").asText(null))
                    .put("engineArtifactSha256", rep.path("engineArtifactSha256").asText(null)).put("manifestSha256", rep.path("manifestSha256").asText(null));
                Json.putSafe(e, "exit", rep.path("exit").asLong(-1));
                if (!"gate".equals(rep.path("kind").asText(null)) || rep.path("exit").asLong(-1) != 0) problems.add(r + ": report kind " + rep.path("kind") + " exit " + rep.path("exit") + " (a gate run that exited 0 is required)");
                summaries.add(Files.isRegularFile(r.resolve(Shards.SUMMARIES)) ? Json.read(r.resolve(Shards.SUMMARIES)) : Json.object());
            } catch (RuntimeException x) { problems.add(r + ": no readable report.json (" + x.getMessage() + ")"); summaries.add(Json.object()); }
            byRun.add(comparisons(r, problems));
        }
        // the three executions are of the same gate run: layout, shard, engine, engine artifact and assertion manifest
        for (String f : List.of("layout", "shard", "engine", "engineArtifactSha256", "manifestSha256")) {
            Set<String> vals = new LinkedHashSet<>();
            for (JsonNode e : ex) vals.add(e.path(f).isMissingNode() || e.path(f).isNull() ? "null" : e.path(f).asText());
            if (vals.size() != 1) problems.add("the executions differ in " + f + ": " + vals);
            else if (!f.equals("shard") && vals.contains("null")) problems.add("the executions record no " + f);
        }
        if (byRun.size() == 3 && !byRun.get(1).keySet().equals(byRun.get(2).keySet())) problems.add("executions 2 and 3 hold different runs: " + byRun.get(1).keySet() + " / " + byRun.get(2).keySet());
        if (byRun.size() == 3 && !byRun.get(0).keySet().equals(byRun.get(1).keySet())) problems.add("executions 1 and 2 hold different runs: " + byRun.get(0).keySet() + " / " + byRun.get(1).keySet());
        if (byRun.size() == 3 && !byRun.get(0).keySet().equals(byRun.get(2).keySet())) problems.add("executions 1 and 3 hold different runs: " + byRun.get(0).keySet() + " / " + byRun.get(2).keySet());
        if (byRun.get(0).isEmpty()) problems.add("no run with " + ReplayGate.COMPARISON + " in " + runs.get(0));
        ObjectNode per = report.putObject("runs");
        long comparedTotal = 0;
        for (String label : byRun.get(0).keySet()) {
            JsonNode a = byRun.get(0).get(label);
            ObjectNode o = per.putObject(label);
            List<String> mine = new ArrayList<>();
            ArrayNode pairs = o.putArray("pairs");
            for (int[] ij : new int[][] {{0, 1}, {0, 2}, {1, 2}}) {
                JsonNode x = byRun.get(ij[0]).get(label), y = byRun.get(ij[1]).get(label);
                String name = (ij[0] + 1) + "-" + (ij[1] + 1);
                if (x == null || y == null) { mine.add("executions " + name + ": the run is missing from one"); continue; }
                Pair p = compare(x, y);
                comparedTotal += p.compared;
                ObjectNode po = pairs.addObject().put("executions", name);
                Json.putSafe(po, "cutsFirst", ids(x).size());
                Json.putSafe(po, "cutsSecond", ids(y).size());
                Json.putSafe(po, "compared", p.compared);
                Json.putSafe(po, "notComparable", p.skipped);
                if (p.compared == 0) mine.add("executions " + name + ": no comparable cut");
                for (String d : p.problems) mine.add("executions " + name + ": " + d);
            }
            ObjectNode rows = o.putObject("revisionCounts");
            for (int i = 0; i < 3; i++) rows.set("execution-" + (i + 1), rowsOf(summaries.get(i), a.path("name").asText(), label));
            o.put("held", mine.isEmpty());
            ArrayNode pr = o.putArray("problems");
            mine.forEach(pr::add);
            stdout.println((mine.isEmpty() ? "PASS " : "FAIL ") + label + ": " + a.path("cuts").size() + " cuts; compared " + pairs.findValuesAsText("compared") + " comparable cuts for the execution pairs 1-2, 1-3, 2-3"
                + (mine.isEmpty() ? "" : " || " + String.join("; ", mine.subList(0, Math.min(5, mine.size())))));
            for (String m : mine) problems.add(label + ": " + m);
        }
        ArrayNode pa = report.putArray("problems");
        problems.forEach(pa::add);
        report.put("held", problems.isEmpty());
        Json.putSafe(report, "comparedCuts", comparedTotal);
        try { Files.createDirectories(out); Json.write(out.resolve(FILE), report); }
        catch (IOException | RuntimeException e) { stderr.println("cannot write " + out.resolve(FILE) + ": " + e.getMessage()); return 1; }
        for (String p : problems) if (!p.contains(": executions ")) stdout.println("PROBLEM " + p);
        stdout.println("determinism: " + byRun.get(0).size() + " runs x 3 executions, 3 pairs each, " + comparedTotal + " comparable cuts compared, " + (problems.isEmpty() ? "every body equal" : problems.size() + " problems"));
        return problems.isEmpty() ? 0 : 1;
    }

    /** {@code runs/<label>/comparison.json} of one execution, by label. */
    static Map<String, JsonNode> comparisons(Path dir, List<String> problems) {
        Map<String, JsonNode> m = new TreeMap<>();
        Path runs = dir.resolve("runs");
        if (!Files.isDirectory(runs)) { problems.add(dir + ": no runs/"); return m; }
        try (Stream<Path> s = Files.list(runs)) {
            for (Path p : s.sorted().toList()) {
                Path f = p.resolve(ReplayGate.COMPARISON);
                if (Files.isRegularFile(f)) m.put(p.getFileName().toString(), Json.read(f));
            }
        } catch (IOException e) { problems.add(dir + ": " + e.getMessage()); }
        return m;
    }

    record Pair(long compared, long skipped, List<String> problems) {}

    /** Execution {@code a} against {@code b} of one run: every cut both list that is comparable is equal on every body partition (the
     *  cut lists themselves may differ: a restart run's skipped cuts). */
    static Pair compare(JsonNode a, JsonNode b) {
        List<String> problems = new ArrayList<>();
        List<Long> deadlines = new ArrayList<>();
        for (JsonNode d : a.path("deadlines")) deadlines.add(d.asLong());
        for (JsonNode d : b.path("deadlines")) deadlines.add(d.asLong());
        Map<Long, JsonNode> cb = new HashMap<>();
        for (JsonNode c : b.path("cuts")) cb.put(c.path("cutId").asLong(), c);
        long compared = 0, skipped = 0;
        for (JsonNode x : a.path("cuts")) {
            long k = x.path("cutId").asLong();
            JsonNode y = cb.get(k);
            if (y == null) continue;
            String why = Comparison.notComparable(side(x), side(y), deadlines);
            if (why != null) { skipped++; continue; }
            compared++;
            Set<String> parts = new TreeSet<>();
            x.path("fingerprints").fieldNames().forEachRemaining(parts::add);
            y.path("fingerprints").fieldNames().forEachRemaining(parts::add);
            for (String part : parts) {
                String fa = x.path("fingerprints").path(part).asText(null), fb = y.path("fingerprints").path(part).asText(null);
                if (!Objects.equals(fa, fb)) problems.add("cut " + k + " " + part + " " + fa + " vs " + fb);
            }
        }
        return new Pair(compared, skipped, problems);
    }

    static Comparison.Side side(JsonNode cut) {
        Set<String> open = new TreeSet<>();
        cut.path("open").forEach(x -> open.add(x.asText()));
        return new Comparison.Side(cut.path("perStation"), open);
    }

    static List<Long> ids(JsonNode run) { List<Long> l = new ArrayList<>(); for (JsonNode c : run.path("cuts")) l.add(c.path("cutId").asLong()); return l; }

    /** The run's records per output topic in one execution's summaries (scenario runs by name, generation and restart runs by label). */
    static JsonNode rowsOf(JsonNode summaries, String name, String label) {
        for (JsonNode r : summaries.path("runs")) if (name.equals(r.path("name").asText()) && label.equals(name.replace('/', '.').replace('@', '.'))) return r.path("rows");
        for (Iterator<Map.Entry<String, JsonNode>> it = summaries.path("generationRuns").fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> e = it.next();
            if (e.getKey().replace('/', '.').replace('@', '.').equals(label)) return e.getValue().path("rows");
        }
        return Json.object();
    }
}
