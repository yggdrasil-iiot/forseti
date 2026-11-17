package yggdrasil.forseti.core.journal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** Revision emission per (generation, logical key): a row is emitted only when its body digest differs from the last emitted one;
 *  revisions count from 1 per generation per key; a delete takes the next revision and supersedes the last digest; a row reappearing
 *  after a delete continues the numbering. Run ids and envelopes never enter the decision (only the body digest does). */
public final class Revisions {
    public record Emission(long revision, String supersedesDigest) {}
    private record State(long lastRevision, String lastDigest) {}   // lastDigest null: nothing live (never emitted, or deleted)

    private final Map<Long, Map<String, State>> byGeneration = new HashMap<>();

    /** Told of every key whose state changed (an emission or a delete), with its canonical text and its logical key; never of an
     *  unchanged body. The segmented state store follows a chain through it. */
    @FunctionalInterface public interface Listener { void changed(long generation, String key, JsonNode logicalKey); }
    private Listener listener;

    /** Sets the one listener of this chain (null: none). */
    public void listen(Listener l) { this.listener = l; }
    private void changed(long generation, String key, JsonNode logicalKey) { if (listener != null) listener.changed(generation, key, logicalKey); }

    private Map<String, State> generation(long g) { return byGeneration.computeIfAbsent(g, k -> new HashMap<>()); }
    static String keyOf(JsonNode logicalKey) { return Canonical.utf8(Canonical.bytes(logicalKey)); }

    /** The revision to emit for this body, or empty when it equals the last emitted body of the key. */
    public Optional<Emission> next(long generation, JsonNode logicalKey, String bodyDigest) {
        if (bodyDigest == null) throw new IllegalArgumentException("bodyDigest");
        Map<String, State> g = generation(generation);
        String k = keyOf(logicalKey);
        State s = g.get(k);
        if (s != null && bodyDigest.equals(s.lastDigest())) return Optional.empty();
        long revision = s == null ? 1 : s.lastRevision() + 1;
        g.put(k, new State(revision, bodyDigest));
        changed(generation, k, logicalKey);
        return Optional.of(new Emission(revision, s == null ? null : s.lastDigest()));
    }

    /** The delete control record's revision and supersedesDigest; the key must have a live row. */
    public Emission delete(long generation, JsonNode logicalKey) {
        Map<String, State> g = generation(generation);
        String k = keyOf(logicalKey);
        State s = g.get(k);
        if (s == null || s.lastDigest() == null) throw new IllegalStateException("no live row to delete: " + k);
        g.put(k, new State(s.lastRevision() + 1, null));
        changed(generation, k, logicalKey);
        return new Emission(s.lastRevision() + 1, s.lastDigest());
    }

    /** The emission state as JSON, for the adapters' state stores: {@code {"<generation>": {"<canonical key>": {revision, digest}}}}
     *  with generations and keys in sorted order, so equal states serialize to equal bytes. */
    public ObjectNode toJson() {
        ObjectNode n = Json.object();
        new TreeMap<>(byGeneration).forEach((g, keys) -> {
            ObjectNode gn = n.putObject(Long.toString(g));
            new TreeMap<>(keys).forEach((k, st) -> gn.set(k, entryJson(st)));
        });
        return n;
    }

    private static ObjectNode entryJson(State st) {
        ObjectNode e = Json.putSafe(Json.object(), "revision", st.lastRevision());
        return e.put("digest", st.lastDigest());
    }
    /** One key's entry as {@link #toJson} writes it: {@code {revision, digest}}. */
    public ObjectNode entryJson(long generation, String key) {
        State st = byGeneration.getOrDefault(generation, Map.of()).get(key);
        if (st == null) throw new IllegalArgumentException("no revision state of " + key + " in generation " + generation);
        return entryJson(st);
    }
    /** Every (generation, canonical key) with a state, in no particular order. */
    public void forEachKey(java.util.function.BiConsumer<Long, String> f) { byGeneration.forEach((g, keys) -> keys.keySet().forEach(k -> f.accept(g, k))); }

    public static Revisions fromJson(JsonNode n) {
        Revisions r = new Revisions();
        for (Iterator<Map.Entry<String, JsonNode>> gs = n.fields(); gs.hasNext();) {
            Map.Entry<String, JsonNode> g = gs.next();
            Map<String, State> keys = r.generation(Long.parseLong(g.getKey()));
            for (Iterator<Map.Entry<String, JsonNode>> ks = g.getValue().fields(); ks.hasNext();) {
                Map.Entry<String, JsonNode> k = ks.next();
                JsonNode d = k.getValue().get("digest");
                keys.put(k.getKey(), new State(Json.safeLong(k.getValue().get("revision"), "revision", Json.NON_NEGATIVE), d == null || d.isNull() ? null : d.textValue()));
            }
        }
        return r;
    }
}
