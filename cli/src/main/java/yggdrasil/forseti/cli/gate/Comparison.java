package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.*;
import java.util.function.BiConsumer;
import yggdrasil.forseti.cli.gate.RunFold.Disp;
import yggdrasil.forseti.cli.gate.RunFold.Rev;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The core against the oracle, cut by cut (plan Task 19, spec 590-610): at every completed cut the harness journals folded through
 *  that cut's progress ({@link RunFold}) against the oracle's {@code cut-<k>} files. Key sets and recomputed body digests are compared
 *  per key for the four evidence journals: the gate recomputes {@code Canonical.digest(body)} on both sides and a reported digest that
 *  is not its body's is itself a difference. Dispositions are compared per identity and part: the main judgement ({@code disposition},
 *  {@code reason}) and the multiset of every delivery's judgement, never which delivery is the duplicate (spec 452). Every difference is
 *  counted; the first ones of every cut and journal are written to {@code <diffs>/<scenario>/cut-<k>/<topic>/<n>.json}
 *  {generation, cut, topic, key, path, expected, actual, inputOffsetRanges} (expected is the oracle's, actual the harness's). The
 *  comparable-cut predicate of spec 609 decides which cuts two runs are compared at directly. */
final class Comparison {
    private Comparison() {}

    static final int DIFF_FILES_PER_TOPIC = 20;
    static final List<String> ORACLE_DISPOSITIONS = List.of("dispositions-stage1", "dispositions-stage2");

    /** The result of comparing every cut of one run. */
    static final class Tally {
        long cuts, rowsCompared, dispositionsCompared, differences, oracleDigestMismatches;
        final Map<String, Long> differencesByTopic = new TreeMap<>();
        final Map<String, Integer> keysAtLastCut = new TreeMap<>();
        final List<String> examples = new ArrayList<>();
        final Map<String, Integer> filesWritten = new HashMap<>();
        Path diffDir;
        boolean equal() { return differences == 0 && oracleDigestMismatches == 0 && cuts > 0; }
        void difference(String topic, String example) {
            differences++;
            differencesByTopic.merge(topic, 1L, Long::sum);
            if (examples.size() < 6) examples.add(example);
        }
        String describe() {
            String keys = keysAtLastCut.entrySet().stream().map(e -> e.getKey().replace("forseti.", "").replace("dispositions-", "disp-") + " " + e.getValue())
                .reduce((a, b) -> a + ", " + b).orElse("");
            if (equal()) return cuts + " cuts equal the oracle (" + keys + " at the last cut)";
            return cuts + " cuts compared: " + differences + " differences " + differencesByTopic + (oracleDigestMismatches > 0 ? ", " + oracleDigestMismatches + " oracle digests not their bodies'" : "")
                + (diffDir != null ? ", diffs in " + diffDir : "") + ": " + String.join(" | ", examples);
        }
    }

    /** Compares the fold (just advanced to {@code cutId}) with the oracle's directory {@code oracleCut}. */
    static void compareCut(RunFold fold, long cutId, Path oracleCut, String scenario, Path diffs, JsonNode cutEntry, Tally t) throws IOException {
        t.cuts++;
        for (String topic : RunFold.ROWS) {
            Path f = oracleCut.resolve(topic + ".json");
            Map<String, Rev> harness = fold.live(topic);
            Set<String> seen = new HashSet<>();
            if (Files.isRegularFile(f)) {
                stream(f, (key, value) -> {
                    seen.add(key);
                    t.rowsCompared++;
                    JsonNode body = RunFold.doublesBeyondSafe(value.path("body"));
                    String digest = Canonical.digest(body);
                    if (!digest.equals(value.path("bodyDigest").asText())) {
                        t.oracleDigestMismatches++;
                        t.difference(topic, "cut " + cutId + " " + topic + " " + key + ": the oracle's bodyDigest " + value.path("bodyDigest").asText() + " is not its body's " + digest);
                        write(t, diffs, scenario, cutId, topic, key, "$ (oracle bodyDigest)", value.path("bodyDigest"), Json.JSON.getNodeFactory().textNode(digest), fold, cutEntry);
                    }
                    Rev h = harness.get(key);
                    if (h == null) {
                        t.difference(topic, "cut " + cutId + " " + topic + " " + key + ": only in the oracle");
                        write(t, diffs, scenario, cutId, topic, key, "$", body, null, fold, cutEntry);
                    } else if (!h.digest().equals(digest)) {
                        JsonNode hb = h.node();
                        String[] d = firstDiff(body, hb, "$");
                        t.difference(topic, "cut " + cutId + " " + topic + " " + key + " " + (d == null ? "$" : d[0] + ": oracle " + d[1] + ", core " + d[2]));
                        write(t, diffs, scenario, cutId, topic, key, d == null ? "$" : d[0], at(body, d), at(hb, d), fold, cutEntry);
                    }
                });
            } else if (!harness.isEmpty()) t.difference(topic, "cut " + cutId + ": the oracle wrote no " + topic + ".json");
            for (Map.Entry<String, Rev> e : harness.entrySet())
                if (!seen.contains(e.getKey())) {
                    t.difference(topic, "cut " + cutId + " " + topic + " " + e.getKey() + ": only in the core");
                    write(t, diffs, scenario, cutId, topic, e.getKey(), "$", null, e.getValue().node(), fold, cutEntry);
                }
            t.keysAtLastCut.put(topic, harness.size());
        }
        // dispositions per (input topic, identity, part)
        Map<String, Map<String, List<String[]>>> harness = new HashMap<>();
        for (Disp d : fold.dispositions())
            harness.computeIfAbsent(d.ledger(), k -> new HashMap<>()).computeIfAbsent(groupKey(d.topic(), d.identity(), d.part()), k -> new ArrayList<>())
                .add(new String[] {d.disposition(), d.reason()});
        for (int stage = 1; stage <= 2; stage++) {
            String ledger = stage == 1 ? RunFold.LEDGER1 : RunFold.LEDGER2, name = ORACLE_DISPOSITIONS.get(stage - 1);
            Map<String, List<String[]>> h = harness.getOrDefault(ledger, Map.of());
            Path f = oracleCut.resolve(name + ".json");
            Set<String> seen = new HashSet<>();
            if (Files.isRegularFile(f)) {
                stream(f, (key, value) -> {
                    t.dispositionsCompared++;
                    JsonNode k = Json.read(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    String g = groupKey(k.path("inputKind").asText(), RunFold.keyText(k.path("identity")), k.path("part").asText());
                    seen.add(g);
                    List<String[]> mine = h.get(g);
                    List<String> want = new ArrayList<>();
                    for (JsonNode x : value.path("deliveries")) want.add(x.get(0).asText() + " " + x.get(1).asText());
                    Collections.sort(want);
                    String wantMain = value.path("disposition").asText() + " " + value.path("reason").asText();
                    if (mine == null) {
                        t.difference(ledger, "cut " + cutId + " " + ledger + " " + g + ": only in the oracle (" + want + ")");
                        write(t, diffs, scenario, cutId, ledger, g, "$", value, null, fold, cutEntry);
                        return;
                    }
                    List<String> have = new ArrayList<>();
                    String haveMain = null;
                    for (String[] x : mine) {
                        have.add(x[0] + " " + x[1]);
                        if (haveMain == null && !(x[0] + " " + x[1]).equals("duplicate identical-redelivery")) haveMain = x[0] + " " + x[1];
                    }
                    if (haveMain == null) haveMain = mine.get(0)[0] + " " + mine.get(0)[1];
                    Collections.sort(have);
                    if (!have.equals(want) || !haveMain.equals(wantMain)) {
                        t.difference(ledger, "cut " + cutId + " " + ledger + " " + g + ": oracle " + wantMain + " " + want + ", core " + haveMain + " " + have);
                        ObjectNode a = Json.object().put("disposition", haveMain);
                        ArrayNode dl = a.putArray("deliveries");
                        have.forEach(dl::add);
                        write(t, diffs, scenario, cutId, ledger, g, "$", value, a, fold, cutEntry);
                    }
                });
            } else if (!h.isEmpty()) t.difference(ledger, "cut " + cutId + ": the oracle wrote no " + name + ".json");
            for (String g : h.keySet())
                if (!seen.contains(g)) {
                    t.difference(ledger, "cut " + cutId + " " + ledger + " " + g + ": only in the core");
                    write(t, diffs, scenario, cutId, ledger, g, "$", null, Json.object().put("deliveries", h.get(g).size()), fold, cutEntry);
                }
            t.keysAtLastCut.put(ledger, h.size());
        }
    }

    static String groupKey(String topic, String identity, String part) { return "{\"identity\":" + identity + ",\"inputKind\":\"" + topic + "\",\"part\":\"" + part + "\"}"; }

    /** The strict mapper, reading one value at a time out of a larger document (the trailing-token check would refuse the rest). Field
     *  names are not interned: an oracle cut file of a horizon recording has hundreds of thousands of distinct keys, which overflow
     *  Jackson's symbol table. */
    static final com.fasterxml.jackson.databind.ObjectMapper STREAM = stream();
    private static com.fasterxml.jackson.databind.ObjectMapper stream() {
        com.fasterxml.jackson.databind.ObjectMapper m = Json.JSON.copy().disable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        m.getFactory().disable(com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
            .disable(com.fasterxml.jackson.core.JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW);
        return m;
    }

    /** Reads a {@code {key -> value}} file entry by entry (an oracle cut file of a horizon recording holds hundreds of thousands). */
    static void stream(Path f, BiConsumer<String, JsonNode> each) throws IOException {
        try (InputStream in = Files.newInputStream(f); JsonParser p = STREAM.getFactory().createParser(in)) {
            p.setCodec(STREAM);
            if (p.nextToken() != JsonToken.START_OBJECT) throw new IllegalArgumentException(f + ": not an object");
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String key = p.currentName();
                p.nextToken();
                each.accept(key, p.readValueAsTree());
            }
        }
    }

    private static JsonNode at(JsonNode root, String[] d) {
        if (d == null || root == null) return root;
        JsonNode n = root;
        String path = d[0];
        if (path.endsWith("[len]")) path = path.substring(0, path.length() - 5);
        for (String seg : path.substring(1).split("(?=[.\\[])")) {
            if (seg.isEmpty()) continue;
            if (seg.startsWith(".")) n = n.path(seg.substring(1));
            else n = n.path(Integer.parseInt(seg.substring(1, seg.length() - 1)));
        }
        return n.isMissingNode() ? null : n;
    }

    /** The JSON path of the first difference of {@code a} and {@code b} (members in sorted order), with both values as text; null when
     *  the two are canonically equal. */
    static String[] firstDiff(JsonNode a, JsonNode b, String path) {
        if (a != null && b != null && a.isObject() && b.isObject()) {
            TreeSet<String> names = new TreeSet<>();
            a.fieldNames().forEachRemaining(names::add);
            b.fieldNames().forEachRemaining(names::add);
            for (String k : names) {
                if (!a.has(k) || !b.has(k)) return new String[] {path + "." + k, a.has(k) ? text(a.get(k)) : "<absent>", b.has(k) ? text(b.get(k)) : "<absent>"};
                String[] r = firstDiff(a.get(k), b.get(k), path + "." + k);
                if (r != null) return r;
            }
            return null;
        }
        if (a != null && b != null && a.isArray() && b.isArray()) {
            if (a.size() != b.size()) return new String[] {path + "[len]", Integer.toString(a.size()), Integer.toString(b.size())};
            for (int i = 0; i < a.size(); i++) { String[] r = firstDiff(a.get(i), b.get(i), path + "[" + i + "]"); if (r != null) return r; }
            return null;
        }
        return text(a).equals(text(b)) ? null : new String[] {path, text(a), text(b)};
    }

    /** The canonical text of any value (JCS here canonicalizes documents, so a scalar is canonicalized inside a one-member object). */
    static String text(JsonNode n) {
        if (n == null) return "<absent>";
        String w = Canonical.utf8(Canonical.bytes(Json.object().set("v", n)));
        return w.substring("{\"v\":".length(), w.length() - 1);
    }

    private static void write(Tally t, Path diffs, String scenario, long cutId, String topic, String key, String path, JsonNode expected, JsonNode actual,
                              RunFold fold, JsonNode cutEntry) {
        if (diffs == null) return;
        String slot = cutId + "/" + topic;
        int n = t.filesWritten.merge(slot, 1, Integer::sum);
        if (n > DIFF_FILES_PER_TOPIC) return;
        Path dir = diffs.resolve(scenario).resolve("cut-" + cutId).resolve(topic);
        ObjectNode d = Json.object();
        Json.putSafe(d, "generation", fold.generation);
        Json.putSafe(d, "cut", cutId);
        d.put("topic", topic);
        try { d.set("key", Json.read(key.getBytes(java.nio.charset.StandardCharsets.UTF_8))); } catch (IllegalArgumentException e) { d.put("key", key); }
        d.put("path", path);
        d.set("expected", expected == null ? Json.JSON.getNodeFactory().nullNode() : expected);
        d.set("actual", actual == null ? Json.JSON.getNodeFactory().nullNode() : actual);
        d.set("inputOffsetRanges", cutEntry == null ? Json.object() : cutEntry.path("offsetRanges"));
        Json.write(dir.resolve(n + ".json"), d);
        t.diffDir = diffs.resolve(scenario);
    }

    // --- comparable cuts (spec 609) ------------------------------------------------------------------------------------------

    /** One side of a direct comparison: a cut's {@code perStation} entry of {@code cuts.json} and the stations with an open visit or an
     *  open session gap at that cut. */
    record Side(JsonNode perStation, Set<String> open) {}

    /** Whether two runs' cuts are comparable: at every station, no ground-truth deadline lies in {@code (min, max]} of the two runs'
     *  {@code W1} or of their {@code admissionW}, and where either run has an open visit or session gap the two {@code W1} are equal.
     *  Returns null when comparable, the reason otherwise. */
    static String notComparable(Side a, Side b, Collection<Long> deadlines) {
        TreeSet<String> stations = new TreeSet<>();
        a.perStation().fieldNames().forEachRemaining(stations::add);
        b.perStation().fieldNames().forEachRemaining(stations::add);
        for (String s : stations) {
            JsonNode x = a.perStation().path(s), y = b.perStation().path(s);
            if (x.isMissingNode() || y.isMissingNode()) return s + " is missing from one run";
            for (String field : List.of("W1", "admissionW")) {
                if (x.path(field).isNull() != y.path(field).isNull()) return s + " " + field + " " + x.path(field) + " vs " + y.path(field);
                if (x.path(field).isNull()) continue;
                long lo = Math.min(x.path(field).asLong(), y.path(field).asLong()), hi = Math.max(x.path(field).asLong(), y.path(field).asLong());
                for (long d : deadlines) if (lo < d && d <= hi) return s + " deadline " + d + " between the two runs' " + field + " " + lo + " and " + hi;
            }
            if ((a.open().contains(s) || b.open().contains(s)) && x.path("W1").asLong() != y.path("W1").asLong())
                return s + " has an open visit or session gap and W1 " + x.path("W1") + " vs " + y.path("W1");
        }
        return null;
    }
}
