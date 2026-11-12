package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** Hand-written journal files for {@link ComparisonTest} and {@link IntegrityTest}: one run directory, its lines appended by topic and
 *  partition in the order the test writes them. */
final class Journals {
    static final ObjectNode PINS = pins();
    static final String RULES_DIGEST = rulesDigest(PINS);

    final Path run;
    Journals(Path run) { this.run = run; }

    static ObjectNode pins() {
        ObjectNode p = Json.object();
        p.putObject("rules").put("attribution", "a".repeat(64));
        p.putObject("hierarchy").put("sha256", "b".repeat(64));
        p.putObject("bindings");
        p.putObject("definitions");
        p.putObject("job").put("runId", "run-1");
        return p;
    }
    static String rulesDigest(ObjectNode pins) { ObjectNode w = pins.deepCopy(); w.remove("job"); return Canonical.digest(w); }

    static ObjectNode key(String id) { return Json.object().put("arrivalEventId", id).put("mode", "occupancy-join").put("source", "mes-line2"); }
    static String keyText(String id) { return Canonical.utf8(Canonical.bytes(key(id))); }
    static ObjectNode body(String status) { return Json.object().put("status", status).put("rulesDigest", RULES_DIGEST); }

    /** A row of {@code topic} triggered by {@code ot.messages/1/<offset>}. */
    Journals row(String topic, int partition, JsonNode key, JsonNode body, long revision, long offset) throws IOException {
        return row(topic, partition, key, body, Canonical.digest(body), revision, offset, PINS);
    }
    Journals row(String topic, int partition, JsonNode key, JsonNode body, String digest, long revision, long offset, JsonNode pins) throws IOException {
        ObjectNode line = Json.object();
        line.putObject("headers").put("forseti-generation", "1");
        line.set("key", key);
        ObjectNode v = line.putObject("value");
        v.set("body", body);
        v.put("bodyDigest", digest);
        ObjectNode env = v.putObject("envelope");
        env.put("evaluationGeneration", 1).put("revision", revision);
        env.putObject("cause").put("kind", "input");
        env.putObject("trigger").put("topic", "ot.messages").put("partition", 1).put("offset", offset);
        env.put("eventTimeAt", 0);
        env.set("pins", pins);
        return line(topic, partition, Canonical.utf8(Canonical.bytes(line)));
    }
    /** A disposition of {@code ot.messages/1/<offset>}, part {@code member:Torque}. */
    Journals disposition(int partition, long offset, String disposition, long revision) throws IOException {
        ObjectNode line = Json.object();
        line.set("key", Json.object().put("generation", 1).put("inputOffset", offset).put("inputPartition", 1).put("inputTopic", "mfg.occupancy").put("part", "event"));
        ObjectNode v = line.putObject("value");
        v.putObject("body").put("disposition", disposition).put("reason", disposition).set("identity", Json.object().put("eventId", "occ-" + offset).put("source", "mes-line2"));
        v.put("revision", revision);
        return line(RunFold.LEDGER1, partition, Canonical.utf8(Canonical.bytes(line)));
    }
    /** The progress record of {@code cutId}, its barrier at {@code ot.messages/1} through {@code through}. */
    Journals progress(String topic, int partition, long cutId, long w1, long through) throws IOException {
        ObjectNode v = Json.object().put("type", "progress").put("cutId", cutId).put("generation", 1).put("W1", w1);
        ArrayNode b = v.putArray("barrierRefs");
        ObjectNode r = b.addObject().put("cutId", cutId).put("throughOffset", through);
        r.putObject("origin").put("topic", "ot.messages").put("partition", 1);
        ObjectNode line = Json.object();
        line.putNull("key");
        line.set("value", v);
        return line(topic, partition, Canonical.utf8(Canonical.bytes(line)));
    }
    Journals line(String topic, int partition, String text) throws IOException {
        Path f = run.resolve("journals").resolve(topic).resolve(partition + ".jsonl");
        Files.createDirectories(f.getParent());
        Files.write(f, (text + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        return this;
    }

    /** Folds cuts {@code 1..cuts} with the fixture's rulesDigest. */
    RunFold fold(long cuts) throws IOException {
        RunFold f = new RunFold(run, RULES_DIGEST, 1, true);
        for (long k = 1; k <= cuts; k++) f.advance(k);
        f.finish();
        return f;
    }

    /** One oracle table of one cut in the oracle's encoding ({@code table_text}): {@code upserts} with their {@code reported} digest
     *  (the body's own when absent), {@code deletes}, and the whole set's {@code count} and {@code setHash} as given. */
    static void oracleTable(Path cutDir, String table, long cutId, Long previous, Map<String, JsonNode> upserts, List<String> deletes, long count, String setHash,
                            Map<String, String> reported) throws IOException {
        ObjectNode o = Json.object();
        Json.putSafe(o, "cutId", cutId);
        if (previous == null) o.putNull("previousCutId"); else Json.putSafe(o, "previousCutId", previous);
        o.put("table", table).put("mode", previous == null ? "snapshot" : "delta");
        Json.putSafe(o, "count", count);
        o.put("setHash", setHash);
        ArrayNode d = o.putArray("deletes");
        deletes.forEach(d::add);
        ObjectNode u = o.putObject("upserts");
        for (Map.Entry<String, JsonNode> e : upserts.entrySet())
            u.putObject(e.getKey()).<ObjectNode>set("body", e.getValue()).put("bodyDigest", reported.getOrDefault(e.getKey(), Canonical.digest(e.getValue())));
        Json.write(cutDir.resolve(table + ".json"), o);
    }

    /** The setHash of a full set {@code key -> body}. */
    static String setHash(Map<String, JsonNode> rows) {
        TreeMap<String, String> m = new TreeMap<>(Comparison::codePointOrder);
        rows.forEach((k, v) -> m.put(k, Canonical.digest(v)));
        return Comparison.setHash(m);
    }

    /** A whole oracle cut directory: every table the snapshot of {@code now} ({@code previous} null) or its delta against
     *  {@code before}, with the correct count and setHash; a table absent from a map is empty. */
    static void oracleCut(Path cutDir, long cutId, Long previous, Map<String, Map<String, JsonNode>> before, Map<String, Map<String, JsonNode>> now) throws IOException {
        for (String table : Comparison.TABLES) {
            Map<String, JsonNode> b = previous == null ? Map.of() : before.getOrDefault(table, Map.of()), n = now.getOrDefault(table, Map.of());
            Map<String, JsonNode> up = new TreeMap<>();
            n.forEach((k, v) -> { if (!b.containsKey(k) || !Canonical.digest(b.get(k)).equals(Canonical.digest(v))) up.put(k, v); });
            List<String> del = new ArrayList<>();
            for (String k : b.keySet()) if (!n.containsKey(k)) del.add(k);
            oracleTable(cutDir, table, cutId, previous, up, del, n.size(), setHash(n), Map.of());
        }
    }
}
