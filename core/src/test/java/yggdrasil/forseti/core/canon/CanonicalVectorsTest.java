package yggdrasil.forseti.core.canon;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import yggdrasil.forseti.core.Json;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

/** The shared JCS vector file (vectors/jcs-vectors.json) run by the Java side; the oracle runs the same file. */
class CanonicalVectorsTest {
    static final Path VECTORS = Path.of("..", "vectors", "jcs-vectors.json");
    static JsonNode file;
    @BeforeAll static void load() { file = Json.read(VECTORS); assertEquals(1, file.path("vectorsVersion").asInt()); assertEquals(9, file.path("vectors").size()); assertEquals(8, file.path("refusals").size()); }

    @Test void everyVectorCanonicalizesAndDigestsAsPinned() {
        for (JsonNode v : file.path("vectors")) {
            JsonNode in = Json.read(Json.text(v, "inputText").getBytes(StandardCharsets.UTF_8));
            byte[] c = Canonical.bytes(in);
            assertEquals(Json.text(v, "canonical"), Canonical.utf8(c), Json.text(v, "id"));
            assertEquals(Json.text(v, "sha256"), Canonical.digestOf(c), Json.text(v, "id"));
        }
    }
    @Test void everyRefusalVectorIsRefusedWithItsReason() {
        for (JsonNode r : file.path("refusals")) {
            String reason = Json.text(r, "reason"), text = Json.text(r, "inputText");
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Canonical.bytes(Json.read(text.getBytes(StandardCharsets.UTF_8))), Json.text(r, "id"));
            // NaN/Infinity are not JSON: Jackson refuses them at parse time (not-json), which is also forseti's refusal; the domain cases reach the pre-check.
            assertTrue(e.getMessage().startsWith(reason) || reason.equals("non-finite") && e.getMessage().startsWith("not-json"), Json.text(r, "id") + ": " + e.getMessage());
        }
    }
    @Test void aNaNPlacedInATreeIsRefusedBeforeTheLibrary() {
        assertEquals("non-finite", assertThrows(IllegalArgumentException.class, () -> Canonical.bytes(new DoubleNode(Double.NaN))).getMessage());
        assertEquals("non-finite", assertThrows(IllegalArgumentException.class, () -> Canonical.bytes(new DoubleNode(Double.POSITIVE_INFINITY))).getMessage());
    }
    @Test void digestIsSha256OfTheCanonicalUtf8Bytes() {
        assertEquals("44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a", Canonical.digest(Json.object()));   // sha256("{}")
    }
}
