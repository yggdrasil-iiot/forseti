package yggdrasil.forseti.core;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Json.pretty writes the bytes Python's json.dumps(indent=2, ensure_ascii=False) + "\n" writes (Tyr's shape), empty
 *  arrays and objects included. The expected text was printed by Python 3.13. */
class JsonPrettyTest {
    @Test void prettyIsTyrsShapeIncludingEmptyContainers() {
        String in = "{\"a\":[],\"b\":{},\"c\":[1,{\"d\":[]}],\"e\":\"x\",\"f\":1.5}";
        String expected = "{\n  \"a\": [],\n  \"b\": {},\n  \"c\": [\n    1,\n    {\n      \"d\": []\n    }\n  ],\n  \"e\": \"x\",\n  \"f\": 1.5\n}\n";
        assertEquals(expected, new String(Json.pretty(Json.read(in.getBytes(StandardCharsets.UTF_8))), StandardCharsets.UTF_8));
    }
    @Test void topLevelEmptyContainers() {
        assertEquals("[]\n", new String(Json.pretty(Json.JSON.createArrayNode()), StandardCharsets.UTF_8));
        assertEquals("{}\n", new String(Json.pretty(Json.object()), StandardCharsets.UTF_8));
    }
}
