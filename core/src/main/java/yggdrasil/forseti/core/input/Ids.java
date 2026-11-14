package yggdrasil.forseti.core.input;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** Semantic identities (spec Input events). Each is an ObjectNode in contract field order; {@link #bytes} is its JCS form, used for
 *  ordering (contract order inside a cut) and as the ledger {@code identity}. {@link #payloadDigest} is the JCS SHA-256 of the record
 *  value without the diagnostic fields recordTime and bridgeReceivedAt and without each member's lossy {@code quality}. */
public final class Ids {
    private Ids() {}
    public static ObjectNode sampleId(Inputs.ProcessMessage m, String metric) {
        ObjectNode n = Json.object().put("edge", m.edge()).put("device", m.device());
        Json.putSafe(n, "birthPublishTs", m.birthPublishTs()).put("metric", metric);
        Json.putSafe(n, "bdSeq", m.bdSeq()); Json.putSafe(n, "seq", m.seq()); Json.putSafe(n, "publishTs", m.publishTs());
        return n;
    }
    /** The whole-message delivery identity: sampleId without metric. */
    public static ObjectNode messageId(Inputs.ProcessMessage m) {
        ObjectNode n = Json.object().put("edge", m.edge()).put("device", m.device());
        Json.putSafe(n, "birthPublishTs", m.birthPublishTs());
        Json.putSafe(n, "bdSeq", m.bdSeq()); Json.putSafe(n, "seq", m.seq()); Json.putSafe(n, "publishTs", m.publishTs());
        return n;
    }
    public static ObjectNode measurementId(String device, String metric, long sourceTs, JsonNode value, Long uaStatusCode) {
        ObjectNode n = Json.object().put("device", device).put("metric", metric);
        Json.putSafe(n, "sourceTs", sourceTs);
        n.set("value", value);
        if (uaStatusCode == null) n.putNull("uaStatusCode"); else Json.putSafe(n, "uaStatusCode", uaStatusCode);
        return n;
    }
    /** {@code (edge, device, birthPublishTs, bdSeq, kind, atSeq)}; the birthless {@code session-ambiguous} variant (plan D4) has a null
     *  birthPublishTs, possibly a null atSeq, and adds the death's {@code rawPublishTs} and {@code payloadDigest} from its payload, so two
     *  birthless deaths with equal counters stay two events. An event with a birth keeps the six fields, byte for byte as before. */
    public static ObjectNode sessionEventId(Inputs.SessionEvent s) {
        ObjectNode n = Json.object().put("edge", s.edge()).put("device", s.device());
        nullable(n, "birthPublishTs", s.birthPublishTs()); Json.putSafe(n, "bdSeq", s.bdSeq()).put("kind", s.kind());
        nullable(n, "atSeq", s.atSeq());
        if (s.birthless()) {
            Json.putSafe(n, "rawPublishTs", s.payload().get("rawPublishTs").longValue());
            n.put("payloadDigest", s.payload().get("payloadDigest").textValue());
        }
        return n;
    }
    private static void nullable(ObjectNode n, String field, Long v) { if (v == null) n.putNull(field); else Json.putSafe(n, field, v); }
    public static ObjectNode occupancyId(Inputs.OccupancyEvent o) { return Json.object().put("source", o.source()).put("eventId", o.eventId()); }
    public static ObjectNode incidentId(Inputs.IncidentEvent i) {
        return Json.putSafe(Json.object().put("source", i.source()).put("incidentId", i.incidentId()), "revision", i.revision());
    }
    public static byte[] bytes(JsonNode identity) { return Canonical.bytes(identity); }

    /** The ledger's payloadDigest with the rule that produced it: "jcs" for a canonicalizable object value, "raw-bytes" for an input
     *  that cannot be canonicalized (spec, the payloadDigest bullet of the ledger body), which still gets its ledger row. */
    public record Digest(String sha256, String rule) {}

    public static Digest payloadDigest(JsonNode value, byte[] rawValue) {
        if (value != null && value.isObject()) {
            ObjectNode v = value.deepCopy();
            v.remove("recordTime");
            v.remove("bridgeReceivedAt");
            if (v.path("members").isArray()) for (JsonNode m : v.path("members")) if (m.isObject()) ((ObjectNode) m).remove("quality");
            try { return new Digest(Canonical.digest(v), "jcs"); }
            catch (IllegalArgumentException e) { /* integer-domain, non-finite, lone surrogate: fall through to the raw bytes */ }
        }
        return new Digest(Canonical.digestOf(rawValue), "raw-bytes");
    }
}
