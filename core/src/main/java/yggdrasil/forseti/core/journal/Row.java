package yggdrasil.forseti.core.journal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** One journal row: logical key, body, {@code bodyDigest} (JCS SHA-256 of the body alone) and the envelope. */
public record Row(String topic, JsonNode logicalKey, JsonNode body, String bodyDigest, Envelope envelope) implements JournalFold.Entry {
    /** A row whose digest is computed from its body; the envelope never enters it. */
    public static Row of(String topic, JsonNode logicalKey, JsonNode body, Envelope envelope) {
        return new Row(topic, logicalKey, body, Canonical.digest(body), envelope);
    }
    @Override public long generation() { return envelope.evaluationGeneration(); }

    public ObjectNode toJson() {
        ObjectNode n = Json.object().put("topic", topic);
        n.set("logicalKey", logicalKey);
        n.set("body", body);
        n.put("bodyDigest", bodyDigest);
        n.set("envelope", envelope.toJson());
        return n;
    }
    public static Row fromJson(JsonNode n) {
        return new Row(Json.text(n, "topic"), JournalFold.required(n, "logicalKey"), JournalFold.required(n, "body"), Json.text(n, "bodyDigest"),
            Envelope.fromJson(JournalFold.required(n, "envelope")));
    }
}
