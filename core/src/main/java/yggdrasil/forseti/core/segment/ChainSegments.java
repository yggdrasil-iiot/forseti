package yggdrasil.forseti.core.segment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.journal.Revisions;

/** The segments of one revision chain ({@link Revisions}): {@code <prefix>/<generation>/<bucket>}, the bucket a function of the
 *  logical key. A chain only grows (a delete keeps the key with its revision), so a key never leaves its segment. A segment is marked
 *  when {@link Revisions#next} emits or {@link Revisions#delete} deletes one of its keys, never when a body is unchanged. */
public final class ChainSegments {
    /** The bucket of a logical key, from its canonical text and its JSON (null when only the text is at hand; parse it if needed). */
    @FunctionalInterface public interface Bucket { String of(String text, JsonNode key); }

    private final String prefix;
    private final Bucket bucket;
    private final Segments.Dirty dirty;
    private final HashMap<String, TreeSet<String>> bySegment = new HashMap<>();
    private Revisions chain;

    public ChainSegments(String prefix, Bucket bucket, Segments.Dirty dirty) { this.prefix = prefix; this.bucket = bucket; this.dirty = dirty; }

    /** Indexes every key of {@code chain} and follows its changes from now on; returns it. */
    public Revisions bind(Revisions chain) {
        bySegment.clear();
        this.chain = chain;
        chain.forEachKey((g, k) -> add(g, k, null));
        chain.listen(this::add);
        return chain;
    }

    public static JsonNode parse(String text, JsonNode key) { return key != null ? key : Json.read(text.getBytes(StandardCharsets.UTF_8)); }

    private void add(long generation, String key, JsonNode logicalKey) {
        String s = prefix + "/" + generation + "/" + bucket.of(key, logicalKey);
        bySegment.computeIfAbsent(s, x -> new TreeSet<>()).add(key);
        dirty.mark(s);
    }

    public boolean owns(String id) { return id.startsWith(prefix + "/"); }
    public Set<String> segmentIds() { return Collections.unmodifiableSet(bySegment.keySet()); }

    /** {@code {"segmentVersion", "segment", "entries": {"<generation>": {key: {revision, digest}}}}}; null when the segment is empty. */
    public byte[] segment(String id) {
        TreeSet<String> keys = bySegment.get(id);
        if (keys == null) return null;
        String g = id.substring(prefix.length() + 1, id.indexOf('/', prefix.length() + 1));
        ObjectNode n = Segments.header(id);
        ObjectNode e = n.putObject("entries").putObject(g);
        long generation = Long.parseLong(g);
        for (String k : keys) e.set(k, chain.entryJson(generation, k));
        return Segments.bytes(n);
    }

    /** Adds a segment's entries to a chain in {@link Revisions#toJson} form. */
    public static void merge(ObjectNode chainJson, JsonNode segment) {
        segment.get("entries").fields().forEachRemaining(g -> {
            JsonNode existing = chainJson.get(g.getKey());
            ObjectNode target = existing instanceof ObjectNode o ? o : chainJson.putObject(g.getKey());
            g.getValue().fields().forEachRemaining(k -> {
                if (target.has(k.getKey())) throw new IllegalArgumentException("revision key " + k.getKey() + " in two segments");
                target.set(k.getKey(), k.getValue());
            });
        });
    }
}
