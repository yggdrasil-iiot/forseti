package yggdrasil.forseti.core.journal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** One disposition ledger row (spec "the disposition ledgers"): the delivery key, the body and the disposition revision under that key.
 *  Dispositions and reasons form the spec's closed vocabulary; a reason belongs to exactly one disposition. */
public record Disposition(Key key, Body body, long revision) implements JournalFold.Entry {
    public Disposition { if (revision < 1) throw new IllegalArgumentException("revision >= 1"); }
    @Override public long generation() { return key.generation(); }

    public enum Kind {
        ACCEPTED("accepted"), DUPLICATE("duplicate"), CONFLICT("conflict"), REJECTED("rejected"), REPLAY_REQUIRED("replay-required");
        private final String wire;
        Kind(String wire) { this.wire = wire; }
        public String wire() { return wire; }
        public static Kind of(String wire) {
            for (Kind k : values()) if (k.wire.equals(wire)) return k;
            throw new IllegalArgumentException("not a disposition: " + wire);
        }
    }

    public enum Reason {
        ACCEPTED("accepted", Kind.ACCEPTED), LATE_AFTER_SETTLEMENT("late-after-settlement", Kind.ACCEPTED),
        IDENTICAL_REDELIVERY("identical-redelivery", Kind.DUPLICATE), SUPERSEDED("superseded", Kind.DUPLICATE),
        IDENTITY_CONFLICT("identity-conflict", Kind.CONFLICT), MEASUREMENT_CONFLICT("measurement-conflict", Kind.CONFLICT),
        CORRECTION_CONFLICT("correction-conflict", Kind.CONFLICT),
        SCHEMA_INVALID("schema-invalid", Kind.REJECTED), UNKNOWN_DEVICE("unknown-device", Kind.REJECTED), UNKNOWN_READPOINT("unknown-readpoint", Kind.REJECTED),
        DEFINITION_MISMATCH("definition-mismatch", Kind.REJECTED), OUTSIDE_INCIDENT_SCOPE("outside-incident-scope", Kind.REJECTED),
        STATION_CHANGE("station-change", Kind.REJECTED), NO_SOURCE_TIMESTAMP("no-source-timestamp", Kind.REJECTED),
        BEYOND_HORIZON("beyond-horizon", Kind.REPLAY_REQUIRED), CORRECTION_BEYOND_HORIZON("correction-beyond-horizon", Kind.REPLAY_REQUIRED);
        private final String wire;
        private final Kind kind;
        Reason(String wire, Kind kind) { this.wire = wire; this.kind = kind; }
        public String wire() { return wire; }
        public Kind kind() { return kind; }
        public static Reason of(String wire) {
            for (Reason r : values()) if (r.wire.equals(wire)) return r;
            throw new IllegalArgumentException("not a disposition reason: " + wire);
        }
    }

    /** The {@code refs.check} values of a profile check failing at the door ({@code rejected schema-invalid}). A schema or integer-range
     *  failure of the input itself names the input field instead. */
    public enum ProfileCheck {
        SOURCES("sources"), IDENTITY_PATTERN("identity.pattern"), BIZ_STEP("bizStep"), CORRECTION_REASONS("correction.reasons"), KINDS("kinds"),
        REVISION_EVENT_TIME("revisionEventTime");
        private final String wire;
        ProfileCheck(String wire) { this.wire = wire; }
        public String wire() { return wire; }
    }

    /** {@code part}: {@code event} | {@code errorDeclaration} for occupancy, {@code message} | {@code member:<metric>} for ot.messages. */
    public static final class Part {
        public static final String EVENT = "event", ERROR_DECLARATION = "errorDeclaration", MESSAGE = "message";
        private static final String MEMBER = "member:";
        private Part() {}
        public static String member(String metric) { return check(MEMBER + metric); }
        static String check(String part) {
            if (EVENT.equals(part) || ERROR_DECLARATION.equals(part) || MESSAGE.equals(part)) return part;
            if (part != null && part.startsWith(MEMBER) && part.length() > MEMBER.length()) return part;
            throw new IllegalArgumentException("not a disposition part: " + part);
        }
    }

    public record Key(long generation, String inputTopic, int inputPartition, long inputOffset, String part) {
        public Key { Part.check(part); if (generation < 1 || inputPartition < 0 || inputOffset < 0) throw new IllegalArgumentException("disposition key out of range"); }
        public ObjectNode toJson() {
            ObjectNode n = Json.putSafe(Json.object(), "generation", generation).put("inputTopic", inputTopic);
            Json.putSafe(n, "inputPartition", inputPartition);
            Json.putSafe(n, "inputOffset", inputOffset);
            return n.put("part", part);
        }
        public static Key fromJson(JsonNode n) {
            return new Key(Json.safeLong(n.get("generation"), "generation", Json.NON_NEGATIVE), Json.text(n, "inputTopic"),
                JournalFold.partition(n.get("inputPartition"), "inputPartition"), Json.safeLong(n.get("inputOffset"), "inputOffset", Json.NON_NEGATIVE),
                Json.text(n, "part"));
        }
    }

    public record Body(String inputKind, JsonNode identity, String payloadDigest, Kind disposition, Reason reason, ObjectNode refs) {
        public Body {
            if (reason.kind() != disposition) throw new IllegalArgumentException("reason " + reason.wire() + " is not a reason of " + disposition.wire());
            if (refs == null) throw new IllegalArgumentException("refs");
        }
        public ObjectNode toJson() {
            ObjectNode n = Json.object().put("inputKind", inputKind);
            n.set("identity", identity);
            n.put("payloadDigest", payloadDigest).put("disposition", disposition.wire()).put("reason", reason.wire());
            n.set("refs", refs);
            return n;
        }
        /** The digest a new disposition revision is decided on (one revision per body change). */
        public String digest() { return Canonical.digest(toJson()); }
        public static Body fromJson(JsonNode n) {
            JsonNode refs = JournalFold.required(n, "refs");
            if (!refs.isObject()) throw new IllegalArgumentException("refs is not an object");
            return new Body(Json.text(n, "inputKind"), JournalFold.required(n, "identity"), Json.text(n, "payloadDigest"),
                Kind.of(Json.text(n, "disposition")), Reason.of(Json.text(n, "reason")), (ObjectNode) refs);
        }
    }

    public ObjectNode toJson() {
        ObjectNode n = Json.object();
        n.set("key", key.toJson());
        n.set("body", body.toJson());
        return Json.putSafe(n, "revision", revision);
    }
    public static Disposition fromJson(JsonNode n) {
        return new Disposition(Key.fromJson(JournalFold.required(n, "key")), Body.fromJson(JournalFold.required(n, "body")),
            Json.safeLong(n.get("revision"), "revision", Json.NON_NEGATIVE));
    }
}
