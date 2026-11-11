package yggdrasil.forseti.core.input;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

class InputParserTest {
    static final String ARRIVAL = "{\"eventId\":\"occ-1\",\"eventTime\":1794183000000,\"recordTime\":1794183000200,\"unitId\":\"UNIT0000000000001\","
        + "\"action\":\"OBSERVE\",\"bizStep\":\"urn:epcglobal:cbv:bizstep:arriving\",\"readPoint\":\"synthetic-site:line2:station1\",\"visitSeq\":1,\"source\":\"mes-line2\"}";
    static final String DEPARTURE = "{\"eventId\":\"occ-2\",\"eventTime\":1794183060000,\"recordTime\":1794183060200,\"unitId\":\"UNIT0000000000001\","
        + "\"action\":\"OBSERVE\",\"bizStep\":\"urn:epcglobal:cbv:bizstep:departing\",\"readPoint\":\"synthetic-site:line2:station1\",\"visitSeq\":1,"
        + "\"arrivalEventId\":\"occ-1\",\"source\":\"mes-line2\"}";
    static final String INCIDENT = "{\"incidentId\":\"inc-1\",\"revision\":1,\"stationRef\":\"synthetic-site:line2:station1\","
        + "\"equipmentRef\":\"synthetic-site:line2:station1:nutrunner1\",\"kind\":\"tool-check-failed\",\"detectedAt\":1794183100000,"
        + "\"revisionEventTime\":1794183100000,\"suspectFrom\":1794183000000,\"suspectTo\":null,\"status\":\"open\",\"evidenceRef\":\"ev-1\",\"source\":\"qms\"}";
    static final String MESSAGE = "{\"edge\":\"edge1\",\"device\":\"synthetic-site:line2:station1:nutrunner1\",\"birthPublishTs\":1794182400000,"
        + "\"bdSeq\":0,\"seq\":2,\"publishTs\":1794183000100,\"bridgeReceivedAt\":1794183000150,\"definitionSha256\":\"" + "ab".repeat(32) + "\","
        + "\"members\":[{\"metric\":\"Running\",\"value\":true,\"uaStatusCode\":0,\"quality\":192,\"sourceTs\":1794183000000,\"serverTs\":1794183000001}]}";
    static final String SESSION = "{\"edge\":\"edge1\",\"device\":\"synthetic-site:line2:station1:nutrunner1\",\"birthPublishTs\":1794182400000,"
        + "\"bdSeq\":0,\"kind\":\"seq-gap\",\"atSeq\":5,\"payload\":{\"from\":3,\"to\":5,\"missing\":2}}";

    static JsonNode json(String s) { return Json.read(s.getBytes(StandardCharsets.UTF_8)); }
    static String with(String base, String field, String literal) {   // replaces one top-level field's value, keeping the raw literal (big integers stay big)
        return base.replaceFirst("\"" + field + "\":(\"[^\"]*\"|[^,}]*)", "\"" + field + "\":" + java.util.regex.Matcher.quoteReplacement(literal));
    }
    static InputParser.Read read(String topic, String raw, long offset) {
        return InputParser.read(new Inputs.Origin(topic, 0, offset), null, raw.getBytes(StandardCharsets.UTF_8));
    }
    static String invalid(String topic, String raw) {
        InputParser.Rejected e = assertThrows(InputParser.Rejected.class, () -> InputParser.parse(topic, json(raw)));
        assertEquals("schema-invalid", e.getMessage());
        return e.check();
    }

    @Test void aValidArrivalWithoutArrivalEventIdAndWithoutErrorDeclarationParses() {
        Inputs.OccupancyEvent o = (Inputs.OccupancyEvent) InputParser.parse(InputParser.OCCUPANCY, json(ARRIVAL));
        assertTrue(o.arriving());
        assertEquals("occ-1", o.eventId());
        assertEquals(1794183000000L, o.eventTime());
        assertNull(o.arrivalEventId());
        assertNull(o.errorDeclaration());
        Inputs.OccupancyEvent d = (Inputs.OccupancyEvent) InputParser.parse(InputParser.OCCUPANCY, json(DEPARTURE));
        assertFalse(d.arriving());
        assertEquals("occ-1", d.arrivalEventId());
    }

    @Test void visitSeqAboveInt32IsKeptAsALong() {
        Inputs.OccupancyEvent o = (Inputs.OccupancyEvent) InputParser.parse(InputParser.OCCUPANCY, json(with(ARRIVAL, "visitSeq", "2147483648")));
        assertEquals(Long.valueOf(2147483648L), o.visitSeq());
    }

    @Test void integerRangeBoundariesPerInputKind() {
        String[][] kinds = {{InputParser.OCCUPANCY, ARRIVAL, "visitSeq"}, {InputParser.INCIDENTS, INCIDENT, "revision"},
                            {InputParser.MESSAGES, MESSAGE, "seq"}, {InputParser.SESSIONS, SESSION, "atSeq"}};
        for (String[] k : kinds) {
            List<String> stream = new ArrayList<>();
            stream.add(with(k[1], k[2], "9007199254740991"));
            stream.add(with(k[1], k[2], "9007199254740992"));
            stream.add(with(k[1], k[2], "18446744073709551617"));
            stream.add(k[1]);
            List<InputParser.Read> out = new ArrayList<>();
            for (int i = 0; i < stream.size(); i++) out.add(read(k[0], stream.get(i), i));   // no exception escapes
            assertFalse(out.get(0).rejected(), k[2] + " = 2^53-1 is accepted");
            for (int i = 1; i <= 2; i++) {
                InputParser.Read r = out.get(i);
                assertTrue(r.rejected(), k[2] + " beyond 2^53-1 is rejected");
                assertEquals(k[2], r.check());
                assertNull(r.record().parsed());
                assertEquals("raw-bytes", r.record().payloadDigest().rule());   // the value itself is not canonicalizable either
                assertEquals(Canonical.digestOf(stream.get(i).getBytes(StandardCharsets.UTF_8)), r.record().payloadDigest().sha256());
            }
            assertFalse(out.get(3).rejected(), "the next record of " + k[0] + " is processed");
        }
    }

    @Test void anArrivalWithoutRecordTimeOrWithoutVisitSeqIsSchemaInvalid() {
        invalid(InputParser.OCCUPANCY, ARRIVAL.replace("\"recordTime\":1794183000200,", ""));
        invalid(InputParser.OCCUPANCY, ARRIVAL.replace("\"visitSeq\":1,", ""));
    }

    @Test void aDepartureWithoutArrivalEventIdIsSchemaInvalid() {
        invalid(InputParser.OCCUPANCY, DEPARTURE.replace("\"arrivalEventId\":\"occ-1\",", ""));
    }

    @Test void anArrivalCarryingArrivalEventIdIsSchemaInvalid() {
        invalid(InputParser.OCCUPANCY, ARRIVAL.replace("\"source\"", "\"arrivalEventId\":\"occ-0\",\"source\""));
    }

    @Test void aClosedIncidentWithNullSuspectToIsSchemaInvalid() {
        assertNotNull(InputParser.parse(InputParser.INCIDENTS, json(INCIDENT)));
        invalid(InputParser.INCIDENTS, with(INCIDENT, "status", "\"closed\""));
        Inputs.IncidentEvent closed = (Inputs.IncidentEvent) InputParser.parse(InputParser.INCIDENTS,
            json(with(with(INCIDENT, "status", "\"closed\""), "suspectTo", "1794183200000")));
        assertEquals(Long.valueOf(1794183200000L), closed.suspectTo());
    }

    @Test void anUnknownExtraFieldIsSchemaInvalid() {
        invalid(InputParser.OCCUPANCY, ARRIVAL.replace("\"source\"", "\"shift\":\"A\",\"source\""));
        invalid(InputParser.INCIDENTS, INCIDENT.replace("\"source\"", "\"severity\":3,\"source\""));
    }

    @Test void schemaSha256IsTheRawDigestOfThePublishedSchemaFile() throws Exception {
        assertEquals(Canonical.digestOf(Files.readAllBytes(Path.of("..", "schemas", "occupancy-1.0.0.schema.json"))),
            InputParser.schemaSha256("urn:forseti:schema:occupancy:1.0.0"));
        assertEquals("e97cd7c97b1ab5e2d63d5b06e6057415945b82cff6d2ca0345106d2daf9d29dc", InputParser.schemaSha256("urn:forseti:schema:occupancy:1.0.0"));
        assertEquals("7141c3115d90812f16b22049c66bfaa64942ebacf854b343fb99b3dce168fb34", InputParser.schemaSha256("urn:forseti:schema:incident:1.0.0"));
        assertEquals(List.of("tool-check-failed", "gauge-drift"), InputParser.incidentKinds());
    }
}
