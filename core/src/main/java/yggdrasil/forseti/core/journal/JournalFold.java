package yggdrasil.forseti.core.journal;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import yggdrasil.forseti.core.Json;

/** Folds one generation's records of one journal partition, in partition order, up to and including the progress record of a cut:
 *  the highest revision per key is live, deleted keys are dropped, progress records are excluded, records of other generations are
 *  ignored. It reports four integrity violations: a revision gap (each key's chain runs 1, 2, 3, ... with deletes in the chain), two
 *  consecutive equal digests of one key, a delete whose supersedesDigest is not the key's last digest, and a business row that follows
 *  the progress record of the cut it belongs to. */
public final class JournalFold {
    private JournalFold() {}

    /** A journal partition's records. */
    public sealed interface Entry permits Row, Delete, Disposition, Progress {
        long generation();
    }

    public record Violations(List<String> revisionGaps, List<String> equalConsecutiveDigests, List<String> deleteSupersedesMismatches,
                             List<String> rowsAfterProgress) {
        public boolean isEmpty() {
            return revisionGaps.isEmpty() && equalConsecutiveDigests.isEmpty() && deleteSupersedesMismatches.isEmpty() && rowsAfterProgress.isEmpty();
        }
    }

    /** {@code live}: canonical key text -> the live Row or Disposition; {@code reachedProgress}: the cut's progress was found. */
    public record Result(Map<String, Entry> live, Violations violations, boolean reachedProgress) {}

    private record Chain(long lastRevision, String lastDigest) {}

    public static Result fold(List<? extends Entry> records, long generation, long throughCutId) {
        Map<String, Entry> live = new TreeMap<>();
        Map<String, Chain> chains = new HashMap<>();
        List<String> gaps = new ArrayList<>(), equal = new ArrayList<>(), deletes = new ArrayList<>(), after = new ArrayList<>();
        Progress last = null;
        boolean reached = false;
        for (Entry e : records) {
            if (e.generation() != generation) continue;
            if (e instanceof Progress p) {
                last = p;
                if (p.cutId() == throughCutId) { reached = true; break; }
                continue;
            }
            String key;
            long revision;
            String digest;   // null for a delete
            if (e instanceof Row r) { key = Revisions.keyOf(r.logicalKey()); revision = r.envelope().revision(); digest = r.bodyDigest(); }
            else if (e instanceof Disposition d) { key = Revisions.keyOf(d.key().toJson()); revision = d.revision(); digest = d.body().digest(); }
            else { Delete x = (Delete) e; key = Revisions.keyOf(x.logicalKey()); revision = x.revision(); digest = null; }

            Chain c = chains.get(key);
            long expected = c == null ? 1 : c.lastRevision() + 1;
            if (revision != expected) gaps.add(key + " revision " + revision + " where " + expected + " was due");
            if (digest == null) {
                Delete x = (Delete) e;
                String lastDigest = c == null ? null : c.lastDigest();
                if (lastDigest == null || !lastDigest.equals(x.supersedesDigest()))
                    deletes.add(key + " revision " + revision + " supersedes " + x.supersedesDigest() + " but the last digest is " + lastDigest);
                live.remove(key);
            } else {
                if (c != null && digest.equals(c.lastDigest())) equal.add(key + " revision " + revision + " repeats digest " + digest);
                live.put(key, e);
                if (last != null && belongsTo(e, last)) after.add(key + " revision " + revision + " follows the progress of cut " + last.cutId());
            }
            chains.put(key, new Chain(revision, digest));
        }
        return new Result(Collections.unmodifiableMap(live),
            new Violations(List.copyOf(gaps), List.copyOf(equal), List.copyOf(deletes), List.copyOf(after)), reached);
    }

    /** Whether a business record belongs to the cut that progress {@code p} closed (or an earlier one): its triggering input is at or
     *  below a through-offset of {@code p}, or its timer deadline is at or below p's event time. A later disposition revision of an old
     *  input (a conflict found later) is legitimate, so only a first disposition revision is judged. */
    private static boolean belongsTo(Entry e, Progress p) {
        if (e instanceof Disposition d) {
            if (d.revision() != 1) return false;
            return covered(p, d.key().inputTopic(), d.key().inputPartition(), d.key().inputOffset());
        }
        Row r = (Row) e;
        JsonNode cause = r.envelope().cause();
        if ("timer".equals(cause.path("kind").textValue()) && cause.path("deadline").isIntegralNumber()) {
            Long w = p instanceof Progress.Stage1 s1 ? Long.valueOf(s1.W1()) : p instanceof Progress.Stage2 s2 ? Long.valueOf(s2.W2()) : null;
            return w != null && cause.path("deadline").longValue() <= w;
        }
        Envelope.Trigger t = r.envelope().trigger();
        return t != null && covered(p, t.topic(), t.partition(), t.offset());
    }

    private static boolean covered(Progress p, String topic, int partition, long offset) {
        List<Progress.BarrierRef> refs = p instanceof Progress.Stage1 s1 ? s1.barrierRefs() : p instanceof Progress.Ledger l ? l.barrierRefs() : List.of();
        for (Progress.BarrierRef b : refs) if (b.origin().topic().equals(topic) && b.origin().partition() == partition && offset <= b.throughOffset()) return true;
        if (p instanceof Progress.Stage2 s2 && "quality.incidents".equals(topic))
            for (Progress.IncidentOffset i : s2.incidentOffsets()) if (i.partition() == partition && offset <= i.throughOffset()) return true;
        return false;
    }

    // --- reading helpers shared by the journal types ---
    static JsonNode required(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null) throw new IllegalArgumentException("missing field " + field);
        return v;
    }
    static String textOrNull(JsonNode n, String field) {
        JsonNode v = required(n, field);
        if (v.isNull()) return null;
        if (!v.isTextual()) throw new IllegalArgumentException("not text: " + field);
        return v.textValue();
    }
    static int partition(JsonNode n, String field) { return (int) Json.safeLong(n, field, 0, Integer.MAX_VALUE); }
}
