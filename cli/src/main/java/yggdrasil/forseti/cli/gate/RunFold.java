package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.Envelope;

/** A harness run's journals folded cut by cut in one streaming pass ({@link #advance}): every partition of every journal is read up to
 *  and including its progress record of the cut, the current generation's rows and deletes are applied (highest revision per key, a
 *  delete removes the key), and the integrity checks of the plan (Task 19) are recorded on the way into {@link #findings}: revisions
 *  contiguous from 1 per key, no equal consecutive digests, every reported {@code bodyDigest} recomputed, progress {@code cutId}
 *  increasing and event time non-decreasing per partition, no business row of a cut after that cut's progress, the envelope pins (without
 *  {@code job}) digesting to the generation's {@code rulesDigest} and the body {@code rulesDigest} of work records and assessments equal
 *  to it, and every live assessment's {@code basis[].bodyDigest} current at every cut. The fold keeps, per key, the revisions it saw
 *  with the cut they belong to (samples only when asked, a horizon recording has hundreds of thousands), and an order-independent
 *  fingerprint of every partition at every cut (for the run-to-run comparisons). Nothing here trusts a reported digest. */
final class RunFold implements Closeable {
    static final String WR = "forseti.work-records", SAMPLES = "forseti.samples", AFFECTED = "forseti.affected", SETS = "forseti.affected-sets",
        LEDGER1 = "forseti.dispositions-stage1", LEDGER2 = "forseti.dispositions-stage2";
    static final List<String> ROWS = List.of(WR, SAMPLES, AFFECTED, SETS), LEDGERS = List.of(LEDGER1, LEDGER2);
    static final BigInteger MAX_SAFE = BigInteger.valueOf(Json.MAX_SAFE);

    /** One revision of a key: the cut it belongs to, its revision, its recomputed digest, its canonical body text (null for a delete). */
    record Rev(long cut, long revision, String digest, String body, int partition) {
        boolean deleted() { return body == null; }
        JsonNode node() { return body == null ? null : Json.read(body.getBytes(StandardCharsets.UTF_8)); }
    }

    /** The latest disposition revision of one delivery (one input record and part). */
    record Disp(String ledger, String topic, int partition, long offset, String part, String identity, String disposition, String reason,
                long revision, long cut, String digest) {}

    /** One progress record as read: its cut, its event time (W1 or W2; null for a ledger) and, for stage 2, its incident offsets. */
    record Mark(long cutId, Long w, Long admissionW, int incidentOffsets) {}

    final Path run;
    final String rulesDigest;
    final long generation;
    final Findings findings = new Findings();
    private final boolean sampleHistory;
    private final Map<String, Map<String, Rev>> live = new HashMap<>();
    private final Map<String, Map<String, List<Rev>>> history = new HashMap<>();
    private final Map<String, Disp> ledger = new HashMap<>();
    private final Map<String, Part> parts = new TreeMap<>();
    private final Map<JsonNode, String> pinsDigests = new HashMap<>();
    private final Map<String, Map<Long, String>> fingerprints = new TreeMap<>();
    private final Map<String, Long> rows = new TreeMap<>();
    private long cut = 0;

    RunFold(Path run, String rulesDigest, long generation, boolean sampleHistory) throws IOException {
        this.run = run; this.rulesDigest = rulesDigest; this.generation = generation; this.sampleHistory = sampleHistory;
        for (String t : ROWS) { live.put(t, new HashMap<>()); history.put(t, new HashMap<>()); }
        for (String t : concat(ROWS, LEDGERS)) {
            Path dir = run.resolve("journals").resolve(t);
            if (!Files.isDirectory(dir)) continue;
            try (Stream<Path> s = Files.list(dir)) {
                for (Path f : s.filter(p -> p.getFileName().toString().endsWith(".jsonl")).sorted().toList()) {
                    String n = f.getFileName().toString();
                    int p = Integer.parseInt(n.substring(0, n.length() - ".jsonl".length()));
                    parts.put(t + "/" + p, new Part(t, p, Files.newBufferedReader(f, StandardCharsets.UTF_8)));
                }
            }
        }
    }

    static List<String> concat(List<String> a, List<String> b) { List<String> l = new ArrayList<>(a); l.addAll(b); return l; }

    // --- reading -------------------------------------------------------------------------------------------------------------

    /** A journal line. JCS prints a body double equal to or beyond 2^53 as an integer literal ({@code 9007199254740992}); the strict
     *  mapper reads that back as an integer outside the JCS integer domain, which {@link Canonical} refuses. Every integer literal beyond
     *  +-(2^53 - 1) is therefore read back as the double it was (the oracle's reader does the same), so the digest recomputes equal;
     *  every integer the journals otherwise carry (times, counts, revisions, offsets) lies inside the safe range. */
    static JsonNode readLine(String line) { return doublesBeyondSafe(Json.read(line.getBytes(StandardCharsets.UTF_8))); }

    static JsonNode doublesBeyondSafe(JsonNode n) {
        if (n instanceof ObjectNode o) {
            Iterator<Map.Entry<String, JsonNode>> it = o.fields();
            List<Map.Entry<String, JsonNode>> replace = null;
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                JsonNode v = e.getValue(), w = doublesBeyondSafe(v);
                if (w != v) { if (replace == null) replace = new ArrayList<>(); replace.add(Map.entry(e.getKey(), w)); }
            }
            if (replace != null) for (Map.Entry<String, JsonNode> e : replace) o.set(e.getKey(), e.getValue());
            return o;
        }
        if (n instanceof ArrayNode a) {
            for (int i = 0; i < a.size(); i++) { JsonNode v = a.get(i), w = doublesBeyondSafe(v); if (w != v) a.set(i, w); }
            return a;
        }
        if (n != null && n.isIntegralNumber() && n.bigIntegerValue().abs().compareTo(MAX_SAFE) > 0) return DoubleNode.valueOf(n.bigIntegerValue().doubleValue());
        return n;
    }

    /** One journal partition, read line by line; a line read past a progress record of a later cut is held back for the next cut. */
    private final class Part {
        final String topic; final int partition; final BufferedReader in;
        final boolean ledger, stage2;
        final List<Mark> marks = new ArrayList<>();
        JsonNode lastProgress, held;
        long fpA, fpB; int count;   // the order-independent fingerprint of the live rows of this partition
        boolean done;
        Part(String topic, int partition, BufferedReader in) {
            this.topic = topic; this.partition = partition; this.in = in;
            ledger = LEDGERS.contains(topic); stage2 = topic.equals(AFFECTED) || topic.equals(SETS);
        }
        String label() { return topic + "/" + partition; }

        void advance(long cutId) throws IOException {
            while (true) {
                JsonNode n;
                if (held != null) { n = held; held = null; }
                else {
                    if (done) { findings.add("J-PROGRESS-ORDER", label() + ": no progress record of cut " + cutId); return; }
                    String s = in.readLine();
                    if (s == null) { done = true; in.close(); findings.add("J-PROGRESS-ORDER", label() + ": no progress record of cut " + cutId); return; }
                    if (s.isEmpty()) continue;
                    n = readLine(s);
                }
                JsonNode v = n.path("value");
                if ("progress".equals(v.path("type").textValue())) {
                    if (v.path("generation").asLong(generation) != generation) continue;
                    long c = v.path("cutId").asLong();
                    if (c > cutId) { held = n; findings.add("J-PROGRESS-ORDER", label() + ": no progress record of cut " + cutId + " (next is " + c + ")"); return; }
                    progress(v, c);
                    if (c == cutId) return;
                    continue;
                }
                if (ledger) disposition(this, n, cutId);
                else row(this, n, cutId);
            }
        }

        void progress(JsonNode v, long c) {
            Mark prev = marks.isEmpty() ? null : marks.get(marks.size() - 1);
            Long w = v.has("W1") ? Long.valueOf(v.get("W1").asLong()) : v.has("W2") ? Long.valueOf(v.get("W2").asLong()) : null;
            Long adm = v.path("admissionW").isIntegralNumber() ? Long.valueOf(v.get("admissionW").asLong()) : null;
            Mark m = new Mark(c, w, adm, v.path("incidentOffsets").size());
            if (prev != null && c <= prev.cutId()) findings.add("J-PROGRESS-ORDER", label() + ": progress cutId " + c + " after " + prev.cutId());
            if (prev != null && w != null && prev.w() != null && w < prev.w()) findings.add("J-PROGRESS-ORDER", label() + " cut " + c + ": event time " + w + " below " + prev.w());
            marks.add(m);
            lastProgress = v;
        }

        void close() { try { in.close(); } catch (IOException ignored) { /* nothing to report */ } }
    }

    /** Folds every partition through its progress of {@code cutId} (the next cut of the run), then checks the basis digests and takes
     *  the fingerprints of the cut. */
    void advance(long cutId) throws IOException {
        if (cutId <= cut) throw new IllegalArgumentException("cut " + cutId + " after " + cut);
        cut = cutId;
        for (Part p : parts.values()) p.advance(cutId);
        // J-BASIS-CURRENT: every basis digest of every live assessment is the current work record digest
        Map<String, Rev> wr = live.get(WR);
        for (Map.Entry<String, Rev> e : live.get(AFFECTED).entrySet()) {
            if (e.getValue().deleted()) continue;
            for (JsonNode b : e.getValue().node().path("basis")) {
                Rev cur = wr.get(keyText(b.path("visitRef")));
                if (cur == null || cur.deleted() || !cur.digest().equals(b.path("bodyDigest").asText()))
                    findings.add("J-BASIS-CURRENT", "cut " + cutId + " " + e.getKey() + ": basis " + keyText(b.path("visitRef")) + " cites " + b.path("bodyDigest").asText()
                        + ", current " + (cur == null || cur.deleted() ? "none" : cur.digest()));
            }
        }
        for (Part p : parts.values()) if (!p.ledger) fingerprints.computeIfAbsent(p.label(), k -> new TreeMap<>()).put(cutId, Long.toHexString(p.fpA) + Long.toHexString(p.fpB) + ":" + p.count);
    }

    private void row(Part p, JsonNode n, long cutId) {
        JsonNode v = n.path("value");
        String label = p.label();
        if (v.path("delete").isObject()) {
            JsonNode d = v.get("delete");
            if (d.path("generation").asLong(generation) != generation) return;
            String key = keyText(d.path("logicalKey"));
            Rev last = live.get(p.topic).get(key);
            long rev = d.path("revision").asLong();
            long due = last == null ? 1 : last.revision() + 1;
            if (rev != due) findings.add("J-REVISION-CONTINUITY", label + " " + key + ": delete revision " + rev + " where " + due + " was due");
            if (last == null || last.deleted() || !last.digest().equals(d.path("supersedesDigest").asText()))
                findings.add("J-REVISION-CONTINUITY", label + " " + key + ": delete supersedes " + d.path("supersedesDigest").asText() + ", last digest " + (last == null ? null : last.digest()));
            put(p, key, new Rev(cutId, rev, null, null, p.partition), last);
            return;
        }
        JsonNode env = v.path("envelope");
        if (env.path("evaluationGeneration").asLong(generation) != generation) return;
        String key = keyText(n.path("key"));
        JsonNode body = v.path("body");
        String canonical = Canonical.utf8(Canonical.bytes(body)), digest = Canonical.digestOf(canonical.getBytes(StandardCharsets.UTF_8));
        if (!digest.equals(v.path("bodyDigest").asText())) findings.add("J-NO-EQUAL-CONSECUTIVE", label + " " + key + ": reported bodyDigest " + v.path("bodyDigest").asText() + " is not the body's " + digest);
        Rev last = live.get(p.topic).get(key);
        long rev = env.path("revision").asLong();
        long due = last == null ? 1 : last.revision() + 1;
        if (rev != due) findings.add("J-REVISION-CONTINUITY", label + " " + key + ": revision " + rev + " where " + due + " was due");
        if (last != null && !last.deleted() && last.digest().equals(digest)) findings.add("J-NO-EQUAL-CONSECUTIVE", label + " " + key + ": revision " + rev + " repeats digest " + digest);
        if (p.lastProgress != null && belongsToEarlier(env, p.lastProgress)) findings.add("J-PROGRESS-ORDER", label + " " + key + " revision " + rev + ": follows the progress of cut " + p.lastProgress.path("cutId").asLong() + " it belongs to");
        pins(label + " " + key, env.path("pins"), body, p.topic);
        put(p, key, new Rev(cutId, rev, digest, canonical, p.partition), last);
    }

    private void put(Part p, String key, Rev r, Rev last) {
        rows.merge(p.topic, 1L, Long::sum);
        live.get(p.topic).put(key, r);
        if (sampleHistory || !p.topic.equals(SAMPLES)) history.get(p.topic).computeIfAbsent(key, k -> new ArrayList<>(2)).add(r);
        if (last != null && !last.deleted()) { long[] h = hash(key, last.digest()); p.fpA -= h[0]; p.fpB -= h[1]; p.count--; }
        if (!r.deleted()) { long[] h = hash(key, r.digest()); p.fpA += h[0]; p.fpB += h[1]; p.count++; }
    }

    private static long[] hash(String key, String digest) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest((key + "\u0000" + digest).getBytes(StandardCharsets.UTF_8));
            long a = 0, b = 0;
            for (int i = 0; i < 8; i++) { a = (a << 8) | (d[i] & 0xff); b = (b << 8) | (d[8 + i] & 0xff); }
            return new long[] {a, b};
        } catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    /** The pins cross-check: {@code Canonical.digest(pins without job)} is the generation's rulesDigest; the body's rulesDigest too, for
     *  the two bodies that carry one (work record, impact assessment). */
    private void pins(String label, JsonNode pins, JsonNode body, String topic) {
        if (rulesDigest == null) return;
        String d = pinsDigests.get(pins);
        if (d == null) {
            ObjectNode without = pins.isObject() ? ((ObjectNode) pins).deepCopy() : Json.object();
            without.remove("job");
            d = Canonical.digest(without);
            pinsDigests.put(pins, d);
        }
        if (!d.equals(rulesDigest)) findings.add("PINS", label + ": envelope pins digest " + d + ", the generation's rulesDigest " + rulesDigest);
        if ((topic.equals(WR) || topic.equals(AFFECTED)) && !rulesDigest.equals(body.path("rulesDigest").asText(null)))
            findings.add("PINS", label + ": body rulesDigest " + body.path("rulesDigest").asText(null) + ", the generation's " + rulesDigest);
    }

    private void disposition(Part p, JsonNode n, long cutId) {
        JsonNode k = n.path("key"), v = n.path("value"), body = v.path("body");
        if (k.path("generation").asLong(generation) != generation) return;
        String key = keyText(k);
        Disp last = ledger.get(key);
        long rev = v.path("revision").asLong();
        long due = last == null ? 1 : last.revision() + 1;
        String label = p.label();
        if (rev != due) findings.add("J-REVISION-CONTINUITY", label + " " + key + ": disposition revision " + rev + " where " + due + " was due");
        String digest = Canonical.digest(body);
        if (last != null && last.digest().equals(digest)) findings.add("J-NO-EQUAL-CONSECUTIVE", label + " " + key + ": disposition revision " + rev + " repeats its body");
        if (rev == 1 && p.lastProgress != null && covered(p.lastProgress, k.path("inputTopic").asText(), k.path("inputPartition").asInt(), k.path("inputOffset").asLong()))
            findings.add("J-PROGRESS-ORDER", label + " " + key + ": a disposition after the progress of cut " + p.lastProgress.path("cutId").asLong() + " it belongs to");
        rows.merge(p.topic, 1L, Long::sum);
        ledger.put(key, new Disp(p.topic, k.path("inputTopic").asText(), k.path("inputPartition").asInt(), k.path("inputOffset").asLong(), k.path("part").asText(),
            keyText(body.path("identity")), body.path("disposition").asText(null), body.path("reason").asText(null), rev, cutId, digest));
    }

    /** Whether a business row belongs to the cut that progress {@code p} closed or an earlier one (then it must precede {@code p}): its
     *  timer deadline is at or below p's event time, or its triggering input is at or below a through-offset of p. */
    static boolean belongsToEarlier(JsonNode env, JsonNode p) {
        Long deadline = Envelope.timerDeadline(env.path("cause"));   // the online and the replayed timer cause alike (spec 583)
        if (deadline != null) {
            JsonNode w = p.has("W1") ? p.get("W1") : p.get("W2");
            return w != null && deadline <= w.asLong();
        }
        JsonNode t = env.path("trigger");
        return t.isObject() && covered(p, t.path("topic").asText(), t.path("partition").asInt(), t.path("offset").asLong());
    }

    static boolean covered(JsonNode p, String topic, int partition, long offset) {
        for (JsonNode b : p.path("barrierRefs"))
            if (b.path("origin").path("topic").asText().equals(topic) && b.path("origin").path("partition").asInt() == partition && offset <= b.path("throughOffset").asLong()) return true;
        if ("quality.incidents".equals(topic))
            for (JsonNode i : p.path("incidentOffsets")) if (i.path("partition").asInt() == partition && offset <= i.path("throughOffset").asLong()) return true;
        return false;
    }

    static String keyText(JsonNode key) { return Canonical.utf8(Canonical.bytes(key)); }

    /** After the last cut: a row after the last progress of a partition is a row after its cut's progress. */
    void finish() throws IOException {
        for (Part p : parts.values()) {
            if (p.held != null) { findings.add("J-PROGRESS-ORDER", p.label() + ": records after the progress of the last cut " + cut); continue; }
            if (p.done) continue;
            String s;
            int trailing = 0;
            while ((s = p.in.readLine()) != null) if (!s.isEmpty()) trailing++;
            p.done = true;
            p.in.close();
            if (trailing > 0) findings.add("J-PROGRESS-ORDER", p.label() + ": " + trailing + " records after the progress of the last cut " + cut);
        }
    }

    @Override public void close() { for (Part p : parts.values()) p.close(); }

    /** Drops what the assertions of a horizon run never read once its checks are made: the sample rows and the dispositions of
     *  telemetry messages (hundreds of thousands each). */
    void compact() {
        live.get(SAMPLES).clear();
        history.get(SAMPLES).clear();
        ledger.values().removeIf(d -> d.topic().equals("ot.messages"));
    }

    /** What two folds of the same journals must agree on (injection 23 folds the kept online journals again after the replay): the
     *  records per topic ({@link #rows}), per topic the SHA-256 of every key's final revision and digest (deleted keys included, in key
     *  order), and the SHA-256 of every partition's fingerprint at every cut. Taken before {@link #compact}. */
    ObjectNode digest() {
        ObjectNode o = Json.object(), r = o.putObject("rows"), finals = o.putObject("finalRevisions");
        rows.forEach((k, v) -> Json.putSafe(r, k, v));
        for (String t : ROWS) {
            MessageDigest md = sha256();
            List<String> keys = new ArrayList<>(live.get(t).keySet());
            Collections.sort(keys);
            for (String k : keys) {
                Rev x = live.get(t).get(k);
                md.update((k + "\u0000" + x.revision() + "\u0000" + (x.deleted() ? "deleted" : x.digest()) + "\n").getBytes(StandardCharsets.UTF_8));
            }
            finals.put(t, HexFormat.of().formatHex(md.digest()));
        }
        MessageDigest md = sha256();
        fingerprints.forEach((p, m) -> m.forEach((k, f) -> md.update((p + "\u0000" + k + "\u0000" + f + "\n").getBytes(StandardCharsets.UTF_8))));
        o.put("fingerprints", HexFormat.of().formatHex(md.digest()));
        return o;
    }

    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); } catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    // --- views ---------------------------------------------------------------------------------------------------------------

    long cut() { return cut; }

    /** Journal records written per topic (rows, deletes and dispositions with their revisions; progress records excluded). */
    Map<String, Long> rows() { return Collections.unmodifiableMap(rows); }

    /** The live (not deleted) rows of {@code topic}: key text -> revision. */
    Map<String, Rev> live(String topic) {
        Map<String, Rev> m = new TreeMap<>();
        live.get(topic).forEach((k, r) -> { if (!r.deleted()) m.put(k, r); });
        return m;
    }
    /** The rows of {@code topic} on {@code partition}, live. */
    Map<String, Rev> live(String topic, int partition) {
        Map<String, Rev> m = new TreeMap<>();
        live.get(topic).forEach((k, r) -> { if (!r.deleted() && r.partition() == partition) m.put(k, r); });
        return m;
    }
    /** Every revision of {@code key} seen so far (samples only when the fold keeps their history). */
    List<Rev> history(String topic, String key) { return history.get(topic).getOrDefault(key, List.of()); }
    Map<String, List<Rev>> history(String topic) { return Collections.unmodifiableMap(history.get(topic)); }
    /** The body of {@code key} at cut {@code cutId}: its last revision of a cut at or before it; null when absent or deleted. */
    JsonNode at(String topic, String key, long cutId) {
        Rev r = null;
        for (Rev x : history(topic, key)) if (x.cut() <= cutId) r = x;
        return r == null || r.deleted() ? null : r.node();
    }
    /** The latest revision of every delivery seen so far. */
    Collection<Disp> dispositions() { return Collections.unmodifiableCollection(ledger.values()); }
    /** Per {@code topic/partition}: the progress records read, in order. */
    Map<String, List<Mark>> marks() {
        Map<String, List<Mark>> m = new TreeMap<>();
        parts.forEach((k, p) -> m.put(k, List.copyOf(p.marks)));
        return m;
    }
    /** Per {@code topic/partition} of the four evidence journals: cutId -> fingerprint of the live rows ({@code <hash>:<count>}). */
    Map<String, Map<Long, String>> fingerprints() { return fingerprints; }

    /** The problems one run's integrity checks found, by check; a few examples each, every one counted. */
    static final class Findings {
        static final int EXAMPLES = 5;
        private final Map<String, Long> counts = new TreeMap<>();
        private final Map<String, List<String>> examples = new TreeMap<>();
        synchronized void add(String check, String problem) {
            counts.merge(check, 1L, Long::sum);
            List<String> l = examples.computeIfAbsent(check, k -> new ArrayList<>());
            if (l.size() < EXAMPLES) l.add(problem);
        }
        synchronized long count(String check) { return counts.getOrDefault(check, 0L); }
        synchronized boolean clean() { return counts.isEmpty(); }
        synchronized Set<String> checks() { return new TreeSet<>(counts.keySet()); }
        synchronized String describe(String check) { return counts.getOrDefault(check, 0L) + " (" + String.join("; ", examples.getOrDefault(check, List.of())) + ")"; }
        synchronized String describe() {
            StringBuilder b = new StringBuilder();
            for (String c : counts.keySet()) b.append(b.length() == 0 ? "" : "; ").append(c).append(' ').append(describe(c));
            return b.toString();
        }
    }
}
