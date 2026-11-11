package yggdrasil.forseti.core.journal;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import yggdrasil.forseti.core.canon.Canonical;

/** Revision emission per (generation, logical key): a row is emitted only when its body digest differs from the last emitted one;
 *  revisions count from 1 per generation per key; a delete takes the next revision and supersedes the last digest; a row reappearing
 *  after a delete continues the numbering. Run ids and envelopes never enter the decision (only the body digest does). */
public final class Revisions {
    public record Emission(long revision, String supersedesDigest) {}
    private record State(long lastRevision, String lastDigest) {}   // lastDigest null: nothing live (never emitted, or deleted)

    private final Map<Long, Map<String, State>> byGeneration = new HashMap<>();

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
        return Optional.of(new Emission(revision, s == null ? null : s.lastDigest()));
    }

    /** The delete control record's revision and supersedesDigest; the key must have a live row. */
    public Emission delete(long generation, JsonNode logicalKey) {
        Map<String, State> g = generation(generation);
        String k = keyOf(logicalKey);
        State s = g.get(k);
        if (s == null || s.lastDigest() == null) throw new IllegalStateException("no live row to delete: " + k);
        g.put(k, new State(s.lastRevision() + 1, null));
        return new Emission(s.lastRevision() + 1, s.lastDigest());
    }
}
