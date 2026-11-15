package yggdrasil.forseti.core.cut;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Base64;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;

/** The held-input codec of a {@link CutAssembler} of input records: {@code {topic, partition, offset, key, value}} with the record's
 *  value bytes as delivered (base64, never re-serialized: a raw-bytes digest and an overflowing literal depend on them); decoding parses
 *  the bytes again with {@link InputParser#read}, so a restored input is the record that was received. */
public final class InputCodec {
    private InputCodec() {}

    public static JsonNode encode(Inputs.InputRecord r) {
        ObjectNode n = Json.object().put("topic", r.origin().topic()).put("partition", r.origin().partition());
        Json.putSafe(n, "offset", r.origin().offset());
        n.put("key", r.key());
        return n.put("value", Base64.getEncoder().encodeToString(r.rawValue()));
    }

    public static Inputs.InputRecord decode(JsonNode n) {
        Inputs.Origin o = new Inputs.Origin(Json.text(n, "topic"), (int) Json.safeLong(n.get("partition"), "partition", 0, Integer.MAX_VALUE),
            Json.safeLong(n.get("offset"), "offset", Json.NON_NEGATIVE));
        String key = n.path("key").isNull() ? null : Json.text(n, "key");
        return InputParser.read(o, key, Base64.getDecoder().decode(Json.text(n, "value"))).record();
    }
}
