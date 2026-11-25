package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.core.Json;

/** The stage-1 progress history of the two evidence journals stage 2 reads together, partition by partition: {@code forseti.work-records}
 *  and {@code forseti.samples} carry one progress record per cut of each station partition, both written by the same writer, so their
 *  sequences of {@code (cutId, mergedCutIds)} must be equal. Read from a run's dumped journals ({@code <run>/journals/<topic>/<p>.jsonl}),
 *  the generation's records only. Three kinds of evidence, each with a few examples and a count:
 *  <ul>
 *  <li>{@code remerged}: a progress merging a cut that already has a progress of its own on the same partition (a cut announced twice);
 *  <li>{@code disagreements}: the first position where the two sequences of a partition differ, both present;
 *  <li>{@code truncated}: one sequence a strict prefix of the other (a journal that ends earlier). This alone shows no fault: a run that
 *      stopped leaves its journals at different lengths.
 *  </ul>
 *  A report of what a run committed, read by no assertion. */
final class ProgressAgreement {
    private ProgressAgreement() {}

    static final int EXAMPLES = 5;

    /** One progress record: its cut and the cuts it merged, in journal order. */
    record Mark(long cutId, List<Long> merged) {
        @Override public String toString() { return cutId + " " + merged; }
    }

    static ObjectNode check(Path run, long generation) throws IOException {
        Map<Integer, List<Mark>> wr = read(run, RunFold.WR, generation), samples = read(run, RunFold.SAMPLES, generation);
        ObjectNode o = Json.object();
        Kind remerged = new Kind(), disagreements = new Kind(), truncated = new Kind();
        for (String topic : List.of(RunFold.WR, RunFold.SAMPLES))
            (topic.equals(RunFold.WR) ? wr : samples).forEach((p, marks) -> {
                Set<Long> own = new HashSet<>();
                for (Mark m : marks) {
                    for (long c : m.merged()) if (own.contains(c)) remerged.add(topic + "/" + p + ": progress of cut " + m.cutId() + " merges " + c + ", which has its own progress");
                    own.add(m.cutId());
                }
            });
        SortedSet<Integer> partitions = new TreeSet<>(wr.keySet());
        partitions.addAll(samples.keySet());
        for (int p : partitions) {
            List<Mark> a = wr.getOrDefault(p, List.of()), b = samples.getOrDefault(p, List.of());
            int n = Math.min(a.size(), b.size()), i = 0;
            while (i < n && a.get(i).equals(b.get(i))) i++;
            if (i < n) disagreements.add("partition " + p + " at progress " + (i + 1) + ": " + RunFold.WR + " " + a.get(i) + ", " + RunFold.SAMPLES + " " + b.get(i));
            else if (a.size() != b.size())
                truncated.add("partition " + p + ": " + RunFold.WR + " " + a.size() + " progress records, " + RunFold.SAMPLES + " " + b.size()
                    + " (last " + (a.size() > b.size() ? RunFold.WR + " " + a.get(a.size() - 1) : RunFold.SAMPLES + " " + b.get(b.size() - 1)) + ")");
        }
        Json.putSafe(o, "partitions", partitions.size());
        o.set("remerged", remerged.toJson());
        o.set("disagreements", disagreements.toJson());
        o.set("truncated", truncated.toJson());
        return o;
    }

    /** Partition -> its progress records of {@code generation}, in journal order; no file, no partition. */
    static Map<Integer, List<Mark>> read(Path run, String topic, long generation) throws IOException {
        Map<Integer, List<Mark>> m = new TreeMap<>();
        Path dir = run.resolve("journals").resolve(topic);
        if (!Files.isDirectory(dir)) return m;
        List<Path> files;
        try (var s = Files.list(dir)) { files = s.filter(f -> f.getFileName().toString().endsWith(".jsonl")).sorted().toList(); }
        for (Path f : files) {
            String n = f.getFileName().toString();
            int p = Integer.parseInt(n.substring(0, n.length() - ".jsonl".length()));
            List<Mark> l = new ArrayList<>();
            try (BufferedReader in = Files.newBufferedReader(f, StandardCharsets.UTF_8)) {
                for (String s; (s = in.readLine()) != null;) {
                    if (s.isEmpty()) continue;
                    JsonNode v = RunFold.readLine(s).path("value");
                    if (!"progress".equals(v.path("type").textValue()) || v.path("generation").asLong(generation) != generation) continue;
                    List<Long> merged = new ArrayList<>();
                    for (JsonNode x : v.path("mergedCutIds")) merged.add(x.asLong());
                    l.add(new Mark(v.path("cutId").asLong(), List.copyOf(merged)));
                }
            }
            m.put(p, l);
        }
        return m;
    }

    private static final class Kind {
        long count;
        final List<String> examples = new ArrayList<>();
        void add(String s) { count++; if (examples.size() < EXAMPLES) examples.add(s); }
        ObjectNode toJson() {
            ObjectNode o = Json.object();
            Json.putSafe(o, "count", count);
            ArrayNode a = o.putArray("examples");
            examples.forEach(a::add);
            return o;
        }
    }
}
