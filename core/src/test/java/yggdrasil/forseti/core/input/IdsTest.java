package yggdrasil.forseti.core.input;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

class IdsTest {
    /** The first occupancy record of the smoke recording (plan Step 8.1). */
    static final String SMOKE_OCC_1 = "{\"eventId\":\"occ-1\",\"eventTime\":1794183000000,\"recordTime\":1794183000200,\"unitId\":\"UNIT0000000000001\","
        + "\"action\":\"OBSERVE\",\"bizStep\":\"urn:epcglobal:cbv:bizstep:arriving\",\"readPoint\":\"synthetic-site:line2:station1\",\"visitSeq\":1,\"source\":\"mes-line2\"}";
    /** Pinned literal; the oracle's test_payload_digest_equals_java_pin carries the same one. */
    static final String SMOKE_OCC_1_DIGEST = "7ae229772ad8b8d77467029c0a07f0e46bbe63f24a7b92afd4c6daa1ce9c44e5";

    static byte[] utf8(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static Ids.Digest digest(String s) { byte[] b = utf8(s); return Ids.payloadDigest(Json.read(b), b); }
    static String message(String recv, String quality, String value) {
        return "{\"edge\":\"edge1\",\"device\":\"d1\",\"birthPublishTs\":1,\"bdSeq\":0,\"seq\":2,\"publishTs\":10," + recv + "\"definitionSha256\":\"x\","
            + "\"members\":[{\"metric\":\"Torque\",\"value\":" + value + ",\"uaStatusCode\":0," + quality + "\"sourceTs\":9}]}";
    }

    @Test void payloadDigestIgnoresRecordTimeBridgeReceivedAtAndMemberQualityAndNothingElse() {
        Ids.Digest pin = digest(SMOKE_OCC_1);
        assertEquals(new Ids.Digest(SMOKE_OCC_1_DIGEST, "jcs"), pin);
        assertEquals(pin, digest(SMOKE_OCC_1.replace("\"recordTime\":1794183000200", "\"recordTime\":1794183999999")));
        assertNotEquals(pin, digest(SMOKE_OCC_1.replace("\"eventTime\":1794183000000", "\"eventTime\":1794183000001")));

        Ids.Digest m = digest(message("\"bridgeReceivedAt\":11,", "\"quality\":192,", "1.5"));
        assertEquals("jcs", m.rule());
        assertEquals(m, digest(message("\"bridgeReceivedAt\":99,", "\"quality\":0,", "1.5")));
        assertEquals(m, digest(message("", "", "1.5")));
        assertNotEquals(m, digest(message("\"bridgeReceivedAt\":11,", "\"quality\":192,", "1.6")));
    }

    @Test void aNonCanonicalizableValueGetsARawBytesDigest() {
        for (String raw : new String[] {"[9007199254740992]", SMOKE_OCC_1.replace("1794183000000", "9007199254740992"), "\"text\""}) {
            Ids.Digest d = digest(raw);
            assertEquals("raw-bytes", d.rule(), raw);
            assertEquals(Canonical.digestOf(utf8(raw)), d.sha256(), raw);
        }
        assertEquals("raw-bytes", Ids.payloadDigest(null, utf8("not json")).rule());
    }

    @Test void sampleIdsOfTwoMembersDifferOnlyInMetric() {
        Inputs.ProcessMessage m = new Inputs.ProcessMessage("edge1", "d1", 1L, 0L, 2L, 10L, null, "x", java.util.List.of(), java.util.List.of());
        ObjectNode a = Ids.sampleId(m, "Torque"), b = Ids.sampleId(m, "Running");
        assertNotEquals(a, b);
        ObjectNode a2 = a.deepCopy(), b2 = b.deepCopy();
        a2.remove("metric"); b2.remove("metric");
        assertEquals(a2, b2);
        assertEquals(Ids.messageId(m), a2);
        assertEquals(java.util.List.of("edge", "device", "birthPublishTs", "metric", "bdSeq", "seq", "publishTs"), fields(a));
    }

    @Test void identityBytesOrderLexicographicallyByCanonicalForm() {
        JsonNode x = Json.read(utf8("{\"a\":2}")), y = Json.read(utf8("{\"b\":1}"));
        assertTrue(Arrays.compareUnsigned(Ids.bytes(x), Ids.bytes(y)) < 0);
        assertArrayEquals(utf8("{\"edge\":\"e\",\"source\":\"s\"}"), Ids.bytes(Json.read(utf8("{\"source\":\"s\",\"edge\":\"e\"}"))));
    }

    static java.util.List<String> fields(JsonNode n) { java.util.List<String> l = new java.util.ArrayList<>(); n.fieldNames().forEachRemaining(l::add); return l; }

    static Inputs.SessionEvent session(String raw) { return (Inputs.SessionEvent) InputParser.parse(InputParser.SESSIONS, Json.read(utf8(raw))); }
    static String birthless(long rawPublishTs, String payloadDigest) {
        return "{\"edge\":\"e\",\"device\":\"d\",\"birthPublishTs\":null,\"bdSeq\":3,\"kind\":\"session-ambiguous\",\"atSeq\":null,"
            + "\"payload\":{\"detectedEventTime\":null,\"rawPublishTs\":" + rawPublishTs + ",\"bdSeq\":3,\"payloadDigest\":\"" + payloadDigest + "\"}}";
    }

    @Test void aSessionEventWithABirthKeepsItsSixFieldIdentity() {
        Inputs.SessionEvent s = session("{\"edge\":\"e\",\"device\":\"d\",\"birthPublishTs\":1,\"bdSeq\":0,\"kind\":\"session-ambiguous\",\"atSeq\":5,"
            + "\"payload\":{\"detectedEventTime\":9,\"rawPublishTs\":8,\"bdSeq\":0,\"payloadDigest\":\"ab\"}}");
        assertEquals("{\"atSeq\":5,\"bdSeq\":0,\"birthPublishTs\":1,\"device\":\"d\",\"edge\":\"e\",\"kind\":\"session-ambiguous\"}",
            Canonical.utf8(Ids.bytes(Ids.sessionEventId(s))));
    }

    @Test void twoBirthlessDeathsWithEqualCountersStayDistinct() {
        JsonNode a = Ids.sessionEventId(session(birthless(100, "aa"))), b = Ids.sessionEventId(session(birthless(101, "aa")));
        JsonNode c = Ids.sessionEventId(session(birthless(100, "bb")));
        assertEquals("{\"atSeq\":null,\"bdSeq\":3,\"birthPublishTs\":null,\"device\":\"d\",\"edge\":\"e\",\"kind\":\"session-ambiguous\","
            + "\"payloadDigest\":\"aa\",\"rawPublishTs\":100}", Canonical.utf8(Ids.bytes(a)));
        assertFalse(Arrays.equals(Ids.bytes(a), Ids.bytes(b)), "a different raw publish timestamp is a different death");
        assertFalse(Arrays.equals(Ids.bytes(a), Ids.bytes(c)), "different raw NDEATH bytes are a different death");
        assertArrayEquals(Ids.bytes(a), Ids.bytes(Ids.sessionEventId(session(birthless(100, "aa")))));
    }
}
