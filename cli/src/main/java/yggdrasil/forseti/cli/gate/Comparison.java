package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.gate.RunFold.Disp;
import yggdrasil.forseti.cli.gate.RunFold.Rev;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The core against the oracle, cut by cut (plan Task 19, spec 590-610): at every completed cut the harness journals folded through
 *  that cut's progress ({@link RunFold}) against the oracle's six tables of that cut, folded from its stream ({@link OracleFold}).
 *
 *  <p>The oracle recomputes every cut from scratch and writes the first one as a snapshot, every later one as the delta against the
 *  previous cut ({@code upserts}, {@code deletes}) with the whole set's {@code count} and {@code setHash} (the oracle's
 *  {@code table_text}). The gate folds the deltas, recomputes {@code Canonical.digest(body)} of every body it receives (a reported
 *  digest that is not its body's is a difference) and recomputes the folded set's count and setHash from the keys and the digests it
 *  computed itself (a missed upsert or delete, or a wrong setHash, is a difference). A malformed table, a delta that does not follow the
 *  cut folded last, or a delete of a key the folded set does not hold breaks the stream: every later cut is a difference.
 *
 *  <p>The folded set is compared per key with the folded journals for the four evidence journals (digests on both sides recomputed),
 *  and per identity and part for the dispositions: the main judgement ({@code disposition}, {@code reason}) and the multiset of every
 *  delivery's judgement, never which delivery is the duplicate (spec 452). Every difference is counted. The first differences of the
 *  first {@link #DIFF_CUTS} cuts that have any are kept and written by {@link #writeDiffs} to
 *  {@code <diffs>/<scenario>/cut-<k>/<topic>/<n>.json} {generation, cut, topic, key, path, expected, actual, inputOffsetRanges}
 *  (expected is the oracle's, actual the harness's) from a snapshot rerun of the oracle on those cuts; the rerun only adds
 *  differences. The comparable-cut predicate of spec 609 decides which cuts two runs are compared at directly. */
final class Comparison {
    private Comparison() {}

    static final int DIFF_FILES_PER_TOPIC = 20, DIFF_CUTS = 5;
    static final String DISP1 = "dispositions-stage1", DISP2 = "dispositions-stage2", STREAM_TOPIC = "oracle-stream";
    static final List<String> ORACLE_DISPOSITIONS = List.of(DISP1, DISP2);
    /** The oracle's tables of a cut, in the order it writes them. */
    static final List<String> TABLES = List.of(RunFold.WR, RunFold.SAMPLES, DISP1, RunFold.AFFECTED, RunFold.SETS, DISP2);

    /** A difference kept for its diff file: the oracle's body when it was at hand, otherwise read from the snapshot rerun. */
    record Pending(long cut, String topic, String table, String key, boolean oracleHas, JsonNode expected, JsonNode actual, JsonNode offsetRanges) {}

    /** The result of comparing every cut of one run. */
    static final class Tally {
        long cuts, rowsCompared, dispositionsCompared, differences, oracleDigestMismatches;
        final Map<String, Long> differencesByTopic = new TreeMap<>();
        final Map<String, Integer> keysAtLastCut = new TreeMap<>();
        final List<String> examples = new ArrayList<>();
        final Map<String, Integer> filesWritten = new HashMap<>();
        final List<Pending> pending = new ArrayList<>();
        /** The cuts compared, in the order compared. */
        final List<Long> comparedIds = new ArrayList<>();
        /** Per cut with pending differences: table -> "count setHash" of the folded stream, recomputed by the gate. */
        final Map<Long, Map<String, String>> summaries = new TreeMap<>();
        Path diffDir;
        boolean equal() { return differences == 0 && oracleDigestMismatches == 0 && cuts > 0; }
        void difference(String topic, String example) {
            differences++;
            differencesByTopic.merge(topic, 1L, Long::sum);
            if (examples.size() < 6) examples.add(example);
        }
        /** Whether a diff file of (cut, topic) is still kept. */
        boolean keeps(long cut, String topic) {
            if (!summaries.containsKey(cut) && summaries.size() >= DIFF_CUTS) return false;
            return filesWritten.getOrDefault(cut + "/" + topic, 0) < DIFF_FILES_PER_TOPIC;
        }
        void keep(OracleFold o, Pending p) {
            if (!keeps(p.cut(), p.topic())) return;
            summaries.computeIfAbsent(p.cut(), k -> new TreeMap<>(o.summary));
            filesWritten.merge(p.cut() + "/" + p.topic(), 1, Integer::sum);
            pending.add(p);
        }
        List<Long> pendingCuts() { return new ArrayList<>(summaries.keySet()); }
        String describe() {
            String keys = keysAtLastCut.entrySet().stream().map(e -> e.getKey().replace("forseti.", "").replace("dispositions-", "disp-") + " " + e.getValue())
                .reduce((a, b) -> a + ", " + b).orElse("");
            if (equal()) return cuts + " cuts equal the oracle (" + keys + " at the last cut)";
            return cuts + " cuts compared: " + differences + " differences " + differencesByTopic + (oracleDigestMismatches > 0 ? ", " + oracleDigestMismatches + " oracle digests not their bodies'" : "")
                + (diffDir != null ? ", diffs in " + diffDir : "") + ": " + String.join(" | ", examples);
        }
    }

    /** One key of a folded oracle table: the digest the gate computed from the body, and for a disposition its comparison group and
     *  the judgement it is compared by ({@code main|delivery;delivery}, deliveries sorted). */
    record Entry(String digest, String group, String judgement) {}

    /** The oracle's tables folded from its cut stream. */
    static final class OracleFold {
        final Map<String, TreeMap<String, Entry>> tables = new HashMap<>();
        /** table -> "count setHash" of the cut folded last, as the gate recomputed it. */
        final Map<String, String> summary = new TreeMap<>();
        private final Map<String, String> judgements = new HashMap<>();
        Long lastCut;
        String broken;
        long diskPeak, upserts, deletes;
        TreeMap<String, Entry> table(String name) { return tables.computeIfAbsent(name, k -> new TreeMap<>(Comparison::codePointOrder)); }
    }

    /** The order the oracle sorts keys in: by code point, which is the order of their UTF-8 bytes. */
    static int codePointOrder(String a, String b) {
        int i = 0, j = 0;
        while (i < a.length() && j < b.length()) {
            int x = a.codePointAt(i), y = b.codePointAt(j);
            if (x != y) return Integer.compare(x, y);
            i += Character.charCount(x);
            j += Character.charCount(y);
        }
        return Boolean.compare(i < a.length(), j < b.length());
    }

    /** SHA-256 over {@code key LF digest LF} per entry, in the map's (code-point) order: the oracle's {@code set_hash}. */
    static String setHash(SortedMap<String, String> digests) {
        MessageDigest md = sha256();
        for (Map.Entry<String, String> e : digests.entrySet()) line(md, e.getKey(), e.getValue());
        return HexFormat.of().formatHex(md.digest());
    }
    static String setHashOf(TreeMap<String, Entry> m) {
        MessageDigest md = sha256();
        for (Map.Entry<String, Entry> e : m.entrySet()) line(md, e.getKey(), e.getValue().digest());
        return HexFormat.of().formatHex(md.digest());
    }
    private static void line(MessageDigest md, String key, String digest) {
        md.update(key.getBytes(StandardCharsets.UTF_8));
        md.update((byte) '\n');
        md.update(digest.getBytes(StandardCharsets.US_ASCII));
        md.update((byte) '\n');
    }
    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); } catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    // --- the stream -----------------------------------------------------------------------------------------------------------

    /** What the gate does with one announced cut directory (before it is deleted). */
    interface CutSink { void accept(long cutId, Path dir) throws IOException; }

    /** The gate's side of the oracle's {@code --stream}: for every cut of the cut log in order, reads {@code cut <cutId>} from the
     *  oracle, hands {@code <out>/cut-<cutId>} to {@code sink}, deletes it and answers {@code ok <cutId>}; the oracle writes the next cut
     *  only after that answer, so the disk holds one cut. Then reads the rest of the oracle's output into {@code trailing}. Returns null
     *  when every cut came in order and none beyond, the reason otherwise. */
    static String consume(BufferedReader announced, Writer acks, Path out, List<Long> cutIds, List<String> trailing, CutSink sink) throws IOException {
        for (long k : cutIds) {
            String line = announced.readLine();
            if (line == null) return "the oracle's stream ended before cut " + k;
            if (!line.equals("cut " + k)) return "expected 'cut " + k + "' from the oracle, read '" + line + "'";
            Path dir = out.resolve("cut-" + k);
            if (!Files.isDirectory(dir)) return "the oracle announced cut " + k + " without " + dir;
            sink.accept(k, dir);
            CoreGate.delete(dir);
            if (Files.exists(dir)) return dir + " could not be deleted";
            try {
                acks.write("ok " + k + "\n");
                acks.flush();
            } catch (IOException e) {
                return "the oracle stopped reading at cut " + k + " (" + e.getMessage() + ")";
            }
        }
        for (String line; (line = announced.readLine()) != null; ) {
            if (line.startsWith("cut ")) return "the oracle announced a cut beyond the cut log: '" + line + "'";
            trailing.add(line);
        }
        return null;
    }

    /** The header of one table file. */
    static final class Head {
        Long cutId, previousCutId, count;
        boolean previousSeen;
        String table, mode, setHash;
        final List<String> deletes = new ArrayList<>();
    }

    interface Upsert { void accept(String key, JsonNode value); }

    /** Reads one table file member by member, every upsert handed to {@code each} as it is read; null when the file is a complete
     *  object of the expected members, the reason otherwise (a truncated file is not complete). */
    static String parseTable(Path f, Head h, Upsert each) {
        if (!Files.isRegularFile(f)) return "no " + f.getFileName();
        try (InputStream in = Files.newInputStream(f); JsonParser p = STREAM.getFactory().createParser(in)) {
            p.setCodec(STREAM);
            if (p.nextToken() != JsonToken.START_OBJECT) return "not an object";
            JsonToken t;
            while ((t = p.nextToken()) == JsonToken.FIELD_NAME) {
                String field = p.currentName();
                JsonToken v = p.nextToken();
                switch (field) {
                    case "cutId", "count" -> {
                        if (v != JsonToken.VALUE_NUMBER_INT) return field + " is not an integer";
                        if (field.equals("cutId")) h.cutId = p.getLongValue(); else h.count = p.getLongValue();
                    }
                    case "previousCutId" -> {
                        if (v != JsonToken.VALUE_NULL && v != JsonToken.VALUE_NUMBER_INT) return "previousCutId is neither null nor an integer";
                        h.previousSeen = true;
                        h.previousCutId = v == JsonToken.VALUE_NULL ? null : p.getLongValue();
                    }
                    case "table", "mode", "setHash" -> {
                        if (v != JsonToken.VALUE_STRING) return field + " is not a string";
                        if (field.equals("table")) h.table = p.getText(); else if (field.equals("mode")) h.mode = p.getText(); else h.setHash = p.getText();
                    }
                    case "deletes" -> {
                        if (v != JsonToken.START_ARRAY) return "deletes is not an array";
                        JsonToken x;
                        while ((x = p.nextToken()) == JsonToken.VALUE_STRING) h.deletes.add(p.getText());
                        if (x != JsonToken.END_ARRAY) return "deletes holds a value that is not a key";
                    }
                    case "upserts" -> {
                        if (v != JsonToken.START_OBJECT) return "upserts is not an object";
                        JsonToken x;
                        while ((x = p.nextToken()) == JsonToken.FIELD_NAME) {
                            String key = p.currentName();
                            p.nextToken();
                            each.accept(key, p.readValueAsTree());
                        }
                        if (x != JsonToken.END_OBJECT) return "upserts is not closed";
                    }
                    default -> { return "unexpected member " + field; }
                }
            }
            if (t != JsonToken.END_OBJECT) return "not closed";
            if (p.nextToken() != null) return "trailing data";
        } catch (IOException | RuntimeException e) {
            return "unreadable (" + e.getMessage().lines().findFirst().orElse("") + ")";
        }
        if (h.cutId == null || h.count == null || !h.previousSeen || h.table == null || h.mode == null || h.setHash == null) return "a header member is missing";
        if (!h.setHash.matches("[0-9a-f]{64}")) return "setHash is not a SHA-256";
        if (!h.mode.equals("snapshot") && !h.mode.equals("delta")) return "mode " + h.mode;
        if (h.mode.equals("snapshot") != (h.previousCutId == null)) return "mode " + h.mode + " with previousCutId " + h.previousCutId;
        if (h.mode.equals("snapshot") && !h.deletes.isEmpty()) return "a snapshot with deletes";
        return null;
    }

    private static String judgement(String main, List<String> deliveries) { return main + "|" + String.join(";", deliveries); }

    /** Folds one table of cut {@code cutId} into {@code o}; null when it folded, the reason the stream broke otherwise. Digest, count and
     *  setHash disagreements are differences, not breaks. {@code hand}: the bodies received in this cut. */
    private static String foldTable(Path f, String table, long cutId, OracleFold o, Map<String, JsonNode> hand, Tally t) {
        Head h = new Head();
        Map<String, Entry> received = new LinkedHashMap<>();
        String[] bad = {null};
        String why = parseTable(f, h, (key, value) -> {
            if (bad[0] != null) return;
            JsonNode body = value.get("body");
            if (body == null || !value.path("bodyDigest").isTextual() || value.size() != 2) { bad[0] = "upsert " + key + " is not {body, bodyDigest}"; return; }
            body = RunFold.doublesBeyondSafe(body);
            String digest = Canonical.digest(body);
            if (!digest.equals(value.path("bodyDigest").asText())) {
                t.oracleDigestMismatches++;
                t.difference(table, "cut " + cutId + " " + table + " " + key + ": the oracle's bodyDigest " + value.path("bodyDigest").asText() + " is not its body's " + digest);
            }
            Entry e;
            if (ORACLE_DISPOSITIONS.contains(table)) {
                JsonNode k = Json.read(key.getBytes(StandardCharsets.UTF_8));
                String g = groupKey(k.path("inputKind").asText(), RunFold.keyText(k.path("identity")), k.path("part").asText());
                List<String> want = new ArrayList<>();
                for (JsonNode x : body.path("deliveries")) want.add(x.path(0).asText() + " " + x.path(1).asText());
                Collections.sort(want);
                String j = judgement(body.path("disposition").asText() + " " + body.path("reason").asText(), want);
                e = new Entry(digest, g.equals(key) ? key : g, o.judgements.computeIfAbsent(j, x -> x));
            } else e = new Entry(digest, null, null);
            if (received.put(key, e) != null) { bad[0] = "upsert " + key + " twice"; return; }
            hand.put(key, body);
        });
        if (why == null) why = bad[0];
        if (why != null) return why;
        if (h.cutId != cutId) return "cutId " + h.cutId + " where cut " + cutId + " was expected";
        if (!h.table.equals(table)) return "table " + h.table + " in " + f.getFileName();
        TreeMap<String, Entry> m = o.table(table);
        if (h.mode.equals("snapshot")) m.clear();
        else if (!h.previousCutId.equals(o.lastCut)) return "a delta against cut " + h.previousCutId + " where the stream folded cut " + o.lastCut + " last";
        for (String key : h.deletes) {
            if (received.containsKey(key)) return key + " both deleted and upserted";
            if (m.remove(key) == null) return "a delete of " + key + ", which the folded set does not hold";
        }
        m.putAll(received);
        o.upserts += received.size();
        o.deletes += h.deletes.size();
        String hash = setHashOf(m);
        if (m.size() != h.count || !hash.equals(h.setHash))
            t.difference(table, "cut " + cutId + " " + table + ": the folded set has " + m.size() + " keys and setHash " + hash + ", the oracle reports " + h.count + " and " + h.setHash);
        o.summary.put(table, m.size() + " " + hash);
        return null;
    }

    static long size(Path dir) throws IOException {
        try (Stream<Path> s = Files.walk(dir)) {
            long n = 0;
            for (Path p : s.filter(Files::isRegularFile).toList()) n += Files.size(p);
            return n;
        }
    }

    /** Folds the oracle's cut directory {@code oracleCut} into {@code o} and compares it with the fold (just advanced to {@code cutId}). */
    static void compareCut(RunFold fold, long cutId, Path oracleCut, OracleFold o, String scenario, JsonNode cutEntry, Tally t) throws IOException {
        t.cuts++;
        t.comparedIds.add(cutId);
        if (o.broken != null) {
            t.difference(STREAM_TOPIC, "cut " + cutId + ": not compared, the oracle stream broke at " + o.broken);
            return;
        }
        Map<String, Map<String, JsonNode>> hand = new HashMap<>();
        if (Files.isDirectory(oracleCut)) o.diskPeak = Math.max(o.diskPeak, size(oracleCut));
        for (String table : TABLES) {
            String why = Files.isDirectory(oracleCut) ? foldTable(oracleCut.resolve(table + ".json"), table, cutId, o, hand.computeIfAbsent(table, k -> new HashMap<>()), t)
                : "no " + oracleCut.getFileName();
            if (why != null) {
                o.broken = "cut " + cutId + " " + table + ": " + why;
                t.difference(STREAM_TOPIC, o.broken);
                return;
            }
        }
        o.lastCut = cutId;
        JsonNode ranges = cutEntry == null ? Json.object() : cutEntry.path("offsetRanges");
        for (String topic : RunFold.ROWS) {
            TreeMap<String, Entry> om = o.table(topic);
            Map<String, JsonNode> inHand = hand.getOrDefault(topic, Map.of());
            Map<String, Rev> harness = fold.live(topic);
            for (Map.Entry<String, Entry> e : om.entrySet()) {
                t.rowsCompared++;
                String key = e.getKey();
                Rev h = harness.get(key);
                if (h == null) {
                    t.difference(topic, "cut " + cutId + " " + topic + " " + key + ": only in the oracle");
                    t.keep(o, new Pending(cutId, topic, topic, key, true, inHand.get(key), null, ranges));
                } else if (!h.digest().equals(e.getValue().digest())) {
                    boolean detail = t.examples.size() < 6 || t.keeps(cutId, topic);
                    JsonNode ob = inHand.get(key), hb = detail ? h.node() : null;
                    String[] d = ob == null || hb == null ? null : firstDiff(ob, hb, "$");
                    t.difference(topic, "cut " + cutId + " " + topic + " " + key + " " + (d != null ? d[0] + ": oracle " + d[1] + ", core " + d[2]
                        : "oracle digest " + e.getValue().digest() + ", core " + h.digest()));
                    if (detail) t.keep(o, new Pending(cutId, topic, topic, key, true, ob, hb, ranges));
                }
            }
            for (Map.Entry<String, Rev> e : harness.entrySet())
                if (!om.containsKey(e.getKey())) {
                    t.difference(topic, "cut " + cutId + " " + topic + " " + e.getKey() + ": only in the core");
                    if (t.keeps(cutId, topic)) t.keep(o, new Pending(cutId, topic, topic, e.getKey(), false, null, e.getValue().node(), ranges));
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
            Map<String, JsonNode> inHand = hand.getOrDefault(name, Map.of());
            Set<String> seen = new HashSet<>();
            for (Map.Entry<String, Entry> e : o.table(name).entrySet()) {
                t.dispositionsCompared++;
                String g = e.getValue().group();
                seen.add(g);
                List<String[]> mine = h.get(g);
                if (mine == null) {
                    t.difference(ledger, "cut " + cutId + " " + ledger + " " + g + ": only in the oracle (" + e.getValue().judgement() + ")");
                    t.keep(o, new Pending(cutId, ledger, name, e.getKey(), true, inHand.get(e.getKey()), null, ranges));
                    continue;
                }
                List<String> have = new ArrayList<>();
                String haveMain = null;
                for (String[] x : mine) {
                    have.add(x[0] + " " + x[1]);
                    if (haveMain == null && !(x[0] + " " + x[1]).equals("duplicate identical-redelivery")) haveMain = x[0] + " " + x[1];
                }
                if (haveMain == null) haveMain = mine.get(0)[0] + " " + mine.get(0)[1];
                Collections.sort(have);
                String j = judgement(haveMain, have);
                if (!j.equals(e.getValue().judgement())) {
                    t.difference(ledger, "cut " + cutId + " " + ledger + " " + g + ": oracle " + e.getValue().judgement() + ", core " + j);
                    ObjectNode a = Json.object().put("disposition", haveMain);
                    ArrayNode dl = a.putArray("deliveries");
                    have.forEach(dl::add);
                    t.keep(o, new Pending(cutId, ledger, name, e.getKey(), true, inHand.get(e.getKey()), a, ranges));
                }
            }
            for (String g : h.keySet())
                if (!seen.contains(g)) {
                    t.difference(ledger, "cut " + cutId + " " + ledger + " " + g + ": only in the core");
                    t.keep(o, new Pending(cutId, ledger, name, g, false, null, Json.object().put("deliveries", h.get(g).size()), ranges));
                }
            t.keysAtLastCut.put(ledger, h.size());
        }
    }

    static String groupKey(String topic, String identity, String part) { return "{\"identity\":" + identity + ",\"inputKind\":\"" + topic + "\",\"part\":\"" + part + "\"}"; }

    // --- diff files ----------------------------------------------------------------------------------------------------------

    /** Writes the diff files of the kept differences. {@code snapshots}: the oracle's {@code --snapshots --cut-ids} rerun of
     *  {@link Tally#pendingCuts} from the original inputs (null when it could not run, {@code rerunFailure} saying why); each of its
     *  tables must fold to the count and setHash the stream had at that cut, and holds the oracle's bodies the stream did not carry in
     *  that cut. A disagreement is one more difference: the rerun never removes one. */
    static void writeDiffs(Tally t, Path diffs, String scenario, long generation, Path snapshots, String rerunFailure) {
        if (diffs == null || t.pending.isEmpty()) return;
        Map<String, JsonNode> bodies = new HashMap<>();   // cut/table/key -> the oracle's body from the rerun
        if (snapshots == null) t.difference("oracle-diagnostic", "the snapshot rerun did not run: " + rerunFailure);
        else for (long cut : t.pendingCuts()) {
            for (String table : TABLES) {
                Set<String> wanted = new HashSet<>();
                for (Pending p : t.pending) if (p.cut() == cut && p.table().equals(table) && p.oracleHas() && p.expected() == null) wanted.add(p.key());
                Head h = new Head();
                TreeMap<String, String> digests = new TreeMap<>(Comparison::codePointOrder);
                String why = parseTable(snapshots.resolve("cut-" + cut).resolve(table + ".json"), h, (key, value) -> {
                    JsonNode body = RunFold.doublesBeyondSafe(value.path("body"));
                    digests.put(key, Canonical.digest(body));
                    if (wanted.contains(key)) bodies.put(cut + "/" + table + "/" + key, body);
                });
                String stream = t.summaries.get(cut).get(table), rerun = digests.size() + " " + setHash(digests);
                if (why == null && (!"snapshot".equals(h.mode) || h.cutId != cut)) why = "not the snapshot of cut " + cut;
                if (why != null) t.difference("oracle-diagnostic", "cut " + cut + " " + table + ": the snapshot rerun is unreadable: " + why);
                else if (!rerun.equals(stream)) t.difference("oracle-diagnostic", "cut " + cut + " " + table + ": the snapshot rerun folds to " + rerun + ", the stream to " + stream);
            }
        }
        for (Pending p : t.pending) {
            JsonNode expected = p.expected() != null ? p.expected() : bodies.get(p.cut() + "/" + p.table() + "/" + p.key());
            if (p.oracleHas() && expected == null && snapshots != null)
                t.difference("oracle-diagnostic", "cut " + p.cut() + " " + p.table() + " " + p.key() + ": not in the snapshot rerun");
            String[] d = expected != null && p.actual() != null && RunFold.ROWS.contains(p.topic()) ? firstDiff(expected, p.actual(), "$") : null;
            write(t, diffs, scenario, generation, p.cut(), p.topic(), p.key(), d == null ? "$" : d[0], at(expected, d), at(p.actual(), d), p.offsetRanges());
        }
    }

    /** The strict mapper, reading one value at a time out of a larger document (the trailing-token check would refuse the rest). Field
     *  names are not interned: an oracle table of a horizon recording has hundreds of thousands of distinct keys, which overflow
     *  Jackson's symbol table. */
    static final com.fasterxml.jackson.databind.ObjectMapper STREAM = stream();
    private static com.fasterxml.jackson.databind.ObjectMapper stream() {
        com.fasterxml.jackson.databind.ObjectMapper m = Json.JSON.copy().disable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        m.getFactory().disable(com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
            .disable(com.fasterxml.jackson.core.JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW);
        return m;
    }

    /** Reads a {@code {key -> value}} file entry by entry. */
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

    private static void write(Tally t, Path diffs, String scenario, long generation, long cutId, String topic, String key, String path, JsonNode expected,
                              JsonNode actual, JsonNode offsetRanges) {
        Path dir = diffs.resolve(scenario).resolve("cut-" + cutId).resolve(topic);
        int n = 1;
        while (Files.exists(dir.resolve(n + ".json"))) n++;
        ObjectNode d = Json.object();
        Json.putSafe(d, "generation", generation);
        Json.putSafe(d, "cut", cutId);
        d.put("topic", topic);
        try { d.set("key", Json.read(key.getBytes(StandardCharsets.UTF_8))); } catch (IllegalArgumentException e) { d.put("key", key); }
        d.put("path", path);
        d.set("expected", expected == null ? Json.JSON.getNodeFactory().nullNode() : expected);
        d.set("actual", actual == null ? Json.JSON.getNodeFactory().nullNode() : actual);
        d.set("inputOffsetRanges", offsetRanges == null ? Json.object() : offsetRanges);
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
