package yggdrasil.forseti.core.journal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import yggdrasil.forseti.core.Json;

/** The {@code delete} control record (spec Output rows): a row's basis for existing disappeared. It takes the next revision of its key
 *  and carries no business body. */
public record Delete(long generation, JsonNode logicalKey, long revision, String supersedesDigest) implements JournalFold.Entry {
    public ObjectNode toJson() {
        ObjectNode n = Json.putSafe(Json.object(), "generation", generation);
        n.set("logicalKey", logicalKey);
        Json.putSafe(n, "revision", revision);
        return n.put("supersedesDigest", supersedesDigest);
    }
    public static Delete fromJson(JsonNode n) {
        return new Delete(Json.safeLong(n.get("generation"), "generation", Json.NON_NEGATIVE), JournalFold.required(n, "logicalKey"),
            Json.safeLong(n.get("revision"), "revision", Json.NON_NEGATIVE), JournalFold.textOrNull(n, "supersedesDigest"));
    }
}
