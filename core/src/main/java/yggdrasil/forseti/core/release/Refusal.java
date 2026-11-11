package yggdrasil.forseti.core.release;

import java.util.*;

/** The reader's closed refusal set: Vor's four names for the same failures plus the spec's start-up refusal. A refusal names
 *  its rule and evidence, never carries secrets, and is never a disposition reason. */
public final class Refusal extends RuntimeException {
    public static final List<String> REASONS = List.of("head-unverified", "head-rollback", "head-expired", "provenance-mismatch", "binding.schema-mismatch");
    public final String reason; public final Map<String, Object> evidence;
    public Refusal(String reason, Object... kv) {
        super(reason); if (!REASONS.contains(reason)) throw new IllegalArgumentException("not a refusal reason: " + reason);
        this.reason = reason; Map<String, Object> e = new LinkedHashMap<>(); for (int i = 0; i + 1 < kv.length; i += 2) e.put(String.valueOf(kv[i]), kv[i + 1]); this.evidence = Collections.unmodifiableMap(e);
    }
}
