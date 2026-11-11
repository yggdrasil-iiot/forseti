package yggdrasil.forseti.core.journal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Set;
import yggdrasil.forseti.core.Json;

/** The common envelope of the four output journals (spec Output rows). Execution paths and triggering inputs live here, never in the
 *  body. {@code cause} is {@code {"kind": "input"|"timer"|"replayed", ...}}; {@code trigger} is null for a row no input record
 *  triggered (a timer); {@code eventTimeAt} is the emitting engine's own event-time progress (diagnostic). */
public record Envelope(long evaluationGeneration, long revision, String supersedesDigest, JsonNode cause, Long eventTimeAt, Trigger trigger,
                       JsonNode basisRevisions, Pins pins) {
    public static final Set<String> CAUSE_KINDS = Set.of("input", "timer", "replayed");

    public record Trigger(String topic, int partition, long offset) {
        ObjectNode toJson() {
            ObjectNode n = Json.object().put("topic", topic);
            Json.putSafe(n, "partition", partition);
            return Json.putSafe(n, "offset", offset);
        }
        static Trigger fromJson(JsonNode n) {
            return new Trigger(Json.text(n, "topic"), JournalFold.partition(n.get("partition"), "trigger.partition"),
                Json.safeLong(n.get("offset"), "trigger.offset", Json.NON_NEGATIVE));
        }
    }

    public Envelope {
        if (evaluationGeneration < 1) throw new IllegalArgumentException("evaluationGeneration >= 1");
        if (revision < 1) throw new IllegalArgumentException("revision >= 1");
        if (cause == null || !cause.isObject() || !CAUSE_KINDS.contains(cause.path("kind").asText(null)))
            throw new IllegalArgumentException("cause.kind is one of " + CAUSE_KINDS);
        if (basisRevisions == null || !basisRevisions.isArray()) throw new IllegalArgumentException("basisRevisions is an array");
    }

    public ObjectNode toJson() {
        ObjectNode n = Json.object();
        Json.putSafe(n, "evaluationGeneration", evaluationGeneration);
        Json.putSafe(n, "revision", revision);
        n.put("supersedesDigest", supersedesDigest);
        n.set("cause", cause);
        if (eventTimeAt == null) n.putNull("eventTimeAt"); else Json.putSafe(n, "eventTimeAt", eventTimeAt);
        if (trigger == null) n.putNull("trigger"); else n.set("trigger", trigger.toJson());
        n.set("basisRevisions", basisRevisions);
        n.set("pins", pins.toJson());
        return n;
    }

    public static Envelope fromJson(JsonNode n) {
        JsonNode t = JournalFold.required(n, "trigger"), at = JournalFold.required(n, "eventTimeAt");
        return new Envelope(Json.safeLong(n.get("evaluationGeneration"), "evaluationGeneration", Json.NON_NEGATIVE),
            Json.safeLong(n.get("revision"), "revision", Json.NON_NEGATIVE), JournalFold.textOrNull(n, "supersedesDigest"),
            JournalFold.required(n, "cause"), at.isNull() ? null : Json.safeLong(at, "eventTimeAt", Json.ANY),
            t.isNull() ? null : Trigger.fromJson(t), JournalFold.required(n, "basisRevisions"), Pins.fromJson(JournalFold.required(n, "pins")));
    }
}
