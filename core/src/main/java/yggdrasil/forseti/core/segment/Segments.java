package yggdrasil.forseti.core.segment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The segmented form of a writer's state (plan step 6): the state as a map of stable segment ids to canonical bytes (RFC 8785), so an
 *  engine stores and rewrites only the parts a cut changed instead of the whole state at every checkpoint. A segment is the JCS form of
 *  {@code {"segmentVersion": 1, "segment": id, ...}}; its id is ASCII ({@code [a-z0-9./-]}, a letter or digit first) and usable as a
 *  store key. The layout and bucket sizes are the states' ({@code Stage1State}, {@code Stage2State}); the helpers here are shared. */
public final class Segments {
    public static final int VERSION = 1;
    public static final String HEAD = "head";
    /** Time buckets of samples, measurements, snapshots, sample revision chains and stage-2 activity rows: 10 s of sourceTs (publishTs
     *  for a sample revision key). A bucket holds what one device sent in 10 s, whatever the horizon. */
    public static final long TIME_WINDOW_MS = 10_000;
    /** Offset buckets of raw messages, quarantined origins, sample buckets without sourceTs and disposition revision chains: 64 offsets
     *  of one input partition. Offsets grow with the input, so a bucket closes like a time bucket. */
    public static final long OFFSET_WINDOW = 64;
    /** Time buckets of what there is about one per visit: occupancy deliveries (the first delivery's eventTime), expired-visit summaries
     *  (enter) and deadline registrations (the deadline): 10 min. */
    public static final long VISIT_WINDOW_MS = 600_000;
    /** Hash buckets of keys without a time or offset (admission visits, session events, the work-record, assessment and set revision
     *  chains, stage-2 bases and incidents). */
    public static final int HASH_BUCKETS = 64;

    private Segments() {}

    /** What changed since the last drain. {@code full}: the first drain of a state that was not built by {@code fromSegments}; it holds
     *  every segment and no removal, and replaces whatever the store held for the writer. */
    public record Drain(boolean full, SortedMap<String, byte[]> written, SortedSet<String> removed) {
        public boolean isEmpty() { return written.isEmpty() && removed.isEmpty(); }
        public long writtenBytes() { long n = 0; for (byte[] b : written.values()) n += b.length; return n; }
    }

    /** A stable 8-hex-digit token of a string ({@link String#hashCode} is fixed by the language), for device and partition buckets.
     *  Two strings with one token share a bucket, which only makes it larger. */
    public static String token(String s) { return String.format("%08x", s.hashCode()); }
    public static String hashBucket(String s) { return "h" + Math.floorMod(s.hashCode(), HASH_BUCKETS); }
    public static long window(long v, long size) { return Math.floorDiv(v, size); }
    public static String timeBucket(long t) { return "t" + window(t, TIME_WINDOW_MS); }
    public static String visitBucket(long t) { return "t" + window(t, VISIT_WINDOW_MS); }
    /** The deadline window of a {@code deadlines/t<n>} id. */
    public static long visitWindowOf(String id) { return Long.parseLong(id.substring(id.lastIndexOf("/t") + 2)); }
    /** {@code <token(topic/partition)>/o<offset window>}. */
    public static String originBucket(String topic, int partition, long offset) { return token(topic + "/" + partition) + "/o" + window(offset, OFFSET_WINDOW); }
    /** The origin bucket of an origin key {@code topic/partition/offset}. */
    public static String originKeyBucket(String originKey) {
        int i = originKey.lastIndexOf('/');
        try { return token(originKey.substring(0, i)) + "/o" + window(Long.parseLong(originKey.substring(i + 1)), OFFSET_WINDOW); }
        catch (RuntimeException e) { return hashBucket(originKey); }
    }

    /** The kind of a segment id: its text before the first {@code /}. */
    public static String kind(String id) { int i = id.indexOf('/'); return i < 0 ? id : id.substring(0, i); }

    public static ObjectNode header(String id) { return Json.object().put("segmentVersion", VERSION).put("segment", id); }
    public static byte[] bytes(ObjectNode segment) { return Canonical.bytes(segment); }

    /** A stored segment, checked against its id and the version. */
    public static JsonNode read(String id, byte[] b) {
        JsonNode n = Json.read(b);
        if (!n.path("segmentVersion").isIntegralNumber() || n.get("segmentVersion").intValue() != VERSION)
            throw new IllegalArgumentException("segment " + id + ": version " + n.path("segmentVersion") + " is not " + VERSION);
        if (!id.equals(n.path("segment").asText(null))) throw new IllegalArgumentException("segment " + id + " is stored as " + n.path("segment"));
        return n;
    }

    /** The entries of one bucket as {@code {"segmentVersion", "segment", "entries": {key: entry}}}; null when the bucket is empty. */
    public static <V> ObjectNode entries(String id, SortedMap<String, V> m, java.util.function.Function<V, JsonNode> entry) {
        if (m.isEmpty()) return null;
        ObjectNode n = header(id);
        ObjectNode e = n.putObject("entries");
        m.forEach((k, v) -> e.set(k, entry.apply(v)));
        return n;
    }

    /** Drains {@code dirty}: every segment when it was never drained, else the marked ids, each written (its bytes) or removed. */
    public static Drain drain(Dirty dirty, java.util.function.Function<String, byte[]> segment, java.util.function.Supplier<SortedMap<String, byte[]>> all) {
        if (dirty.all) {
            dirty.all = false;
            dirty.ids.clear();
            return new Drain(true, Collections.unmodifiableSortedMap(all.get()), Collections.emptySortedSet());
        }
        TreeMap<String, byte[]> written = new TreeMap<>();
        TreeSet<String> removed = new TreeSet<>();
        for (String id : dirty.ids) { byte[] b = segment.apply(id); if (b == null) removed.add(id); else written.put(id, b); }
        dirty.ids.clear();
        return new Drain(false, Collections.unmodifiableSortedMap(written), Collections.unmodifiableSortedSet(removed));
    }

    /** The segment ids changed since the last drain. A state that was never drained (fresh, or built by {@code fromJson}) has
     *  changed as a whole ({@code all}); then nothing is recorded per id. */
    public static final class Dirty {
        boolean all = true;
        final TreeSet<String> ids = new TreeSet<>();
        public void mark(String id) { if (!all) ids.add(id); }
        /** After {@code fromSegments}: the store holds exactly this state. */
        public void clean() { all = false; ids.clear(); }
        public boolean all() { return all; }
        public SortedSet<String> ids() { return Collections.unmodifiableSortedSet(ids); }
    }
}
