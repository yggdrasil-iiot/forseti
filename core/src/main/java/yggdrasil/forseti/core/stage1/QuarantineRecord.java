package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Base64;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.Inputs;

/** The raw bytes of a conflicting input for {@code forseti.quarantine} (spec 235, 461): a side store with no judgement state of its own,
 *  linked from the disposition's {@code refs.quarantine} by the input's origin. */
public record QuarantineRecord(long generation, Inputs.Origin origin, String reason, byte[] rawValue) {
    public ObjectNode toJson() {
        ObjectNode n = Json.putSafe(Json.object(), "generation", generation);
        n.setAll(ref(origin));
        return n.put("reason", reason).put("rawValueBase64", Base64.getEncoder().encodeToString(rawValue));
    }
    /** The link a disposition carries in {@code refs.quarantine}. */
    public static ObjectNode ref(Inputs.Origin o) {
        ObjectNode n = Json.object().put("inputTopic", o.topic());
        Json.putSafe(n, "inputPartition", o.partition());
        return Json.putSafe(n, "inputOffset", o.offset());
    }
}
