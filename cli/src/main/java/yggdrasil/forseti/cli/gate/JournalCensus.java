package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.Envelope;

/** What every record of a run's journals carries, read from the files themselves (not from a fold, which keeps only the rows of its own
 *  generation): the generation of every row, delete, disposition and progress record; for every business row its envelope pins digest
 *  (without {@code job}), its attribution rule version and its cause kind; and the timer-emitted rows, by the shape of their cause.
 *  Injection 20 reads it for "every row of that generation carries the new rulesDigest"; injection 23 for "every row carries the replay
 *  generation's number" and "every timer-emitted row carries the replayed timer cause". */
final class JournalCensus {
    /** Generation numbers seen, by record kind ({@code row}, {@code delete}, {@code disposition}, {@code progress}). */
    final Map<String, Set<Long>> generations = new TreeMap<>();
    /** Business rows by envelope pins digest (without {@code job}), by {@code pins.rules.attribution.version}, by {@code cause.kind}. */
    final Map<String, Long> pinsDigests = new TreeMap<>(), attributionVersions = new TreeMap<>(), causeKinds = new TreeMap<>();
    long rows, records;
    /** Timer-emitted rows: {@code {"kind": "timer", "deadline"}}; {@code {"kind": "replayed", "inputKind": "timer", "identity": {"deadline"}}}
     *  with a null trigger; and replayed timer rows of any other shape (a trigger, no deadline). */
    long onlineTimerRows, replayedTimerRows, malformedReplayedTimerRows;
    final List<String> examples = new ArrayList<>();

    /** Every {@code journals/<topic>/<partition>.jsonl} under {@code run}. */
    static JournalCensus of(Path run) {
        JournalCensus c = new JournalCensus();
        Path journals = run.resolve("journals");
        if (!Files.isDirectory(journals)) return c;
        Map<JsonNode, String> digests = new HashMap<>();
        List<Path> files;
        try (Stream<Path> w = Files.walk(journals)) { files = w.filter(p -> p.getFileName().toString().endsWith(".jsonl")).sorted().toList(); }
        catch (IOException e) { throw new UncheckedIOException(e); }
        for (Path f : files) {
            try (BufferedReader in = Files.newBufferedReader(f, StandardCharsets.UTF_8)) {
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.isEmpty()) continue;
                    c.record(journals.relativize(f).toString().replace('\\', '/'), RunFold.readLine(line), digests);
                }
            } catch (IOException e) { throw new UncheckedIOException(e); }
        }
        return c;
    }

    private void record(String file, JsonNode n, Map<JsonNode, String> digests) {
        records++;
        JsonNode v = n.path("value");
        if ("progress".equals(v.path("type").textValue())) { gen("progress", v.path("generation")); return; }
        if (v.path("delete").isObject()) { gen("delete", v.path("delete").path("generation")); return; }
        JsonNode env = v.path("envelope");
        if (env.isObject()) {
            rows++;
            gen("row", env.path("evaluationGeneration"));
            JsonNode pins = env.path("pins");
            String d = digests.get(pins);
            if (d == null) {
                ObjectNode without = pins.isObject() ? ((ObjectNode) pins).deepCopy() : Json.object();
                without.remove("job");
                d = Canonical.digest(without);
                digests.put(pins, d);
            }
            pinsDigests.merge(d, 1L, Long::sum);
            attributionVersions.merge(pins.path("rules").path("attribution").path("version").asText("none"), 1L, Long::sum);
            JsonNode cause = env.path("cause");
            String kind = cause.path("kind").asText("none");
            causeKinds.merge(kind, 1L, Long::sum);
            if ("timer".equals(kind)) onlineTimerRows++;
            else if (Envelope.REPLAYED.equals(kind) && "timer".equals(cause.path("inputKind").textValue())) {
                if (Envelope.timerDeadline(cause) != null && cause.path("identity").path("deadline").isIntegralNumber() && (env.path("trigger").isNull() || env.path("trigger").isMissingNode()))
                    replayedTimerRows++;
                else { malformedReplayedTimerRows++; example(file + ": a replayed timer row of another shape: cause " + cause + ", trigger " + env.path("trigger")); }
            }
            return;
        }
        if (n.path("key").path("generation").isIntegralNumber()) { gen("disposition", n.path("key").path("generation")); return; }
        example(file + ": a record of no known kind");
    }

    private void gen(String kind, JsonNode g) {
        generations.computeIfAbsent(kind, k -> new TreeSet<>()).add(g.isIntegralNumber() ? g.longValue() : -1L);
    }

    private void example(String s) { if (examples.size() < 5) examples.add(s); }

    /** Every generation number seen, of every kind. */
    Set<Long> allGenerations() {
        Set<Long> s = new TreeSet<>();
        generations.values().forEach(s::addAll);
        return s;
    }

    String describe() {
        return records + " records (" + rows + " rows), generations " + generations + ", pins digests " + pinsDigests.size() + ", attribution versions "
            + attributionVersions.keySet() + ", causes " + causeKinds + ", timer rows " + onlineTimerRows + " online / " + replayedTimerRows + " replayed"
            + (malformedReplayedTimerRows > 0 ? " / " + malformedReplayedTimerRows + " malformed " + examples : "");
    }
}
