package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.cli.recording.Murmur2;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;

/** The occupancy and incident records the live producers send: the core's InputParser reads them back as the events they were built
 *  from, they carry the step-3 recordings' field order, and what the schema or the binding profile refuses is refused before a send. */
class LiveRecordsTest {
    static final String S1 = "site-071:line2:station1", T1 = S1 + ":nutrunner1";
    static final Path SMOKE_INCIDENT = Path.of("..", "fixtures", "recordings", "smoke-incident");

    static Inputs.InputRecord read(String topic, ObjectNode v) throws Exception {
        InputParser.Read r = InputParser.read(new Inputs.Origin(topic, 0, 0), "k", Json.JSON.writeValueAsBytes(v));
        assertFalse(r.rejected(), "rejected " + r.check() + ": " + v);
        return r.record();
    }

    static List<String> fieldNames(JsonNode n) {
        List<String> l = new ArrayList<>();
        n.fieldNames().forEachRemaining(l::add);
        return l;
    }

    static JsonNode firstRecorded(String topic, int partition, int line) throws Exception {
        return Json.read(Files.readAllLines(SMOKE_INCIDENT.resolve(topic).resolve(partition + ".jsonl")).get(line).getBytes()).path("value");
    }

    @Test
    void anArrivalADepartureAndTheirErrorDeclarationRoundTripThroughTheCoreReader() throws Exception {
        LiveEquipment.OccupancyProfile p = LiveEquipmentTest.dedicated().occupancy();
        ObjectNode a = OccupancyRecords.checked(OccupancyRecords.arrival("S1-V1-arrive", 1794183000123L, 1794183000200L, "UNIT0000000000001", S1, 1, "mes-line2"), p);
        ObjectNode d = OccupancyRecords.checked(OccupancyRecords.departure("S1-V1-depart", 1794183060123L, 1794183060200L, "UNIT0000000000001", S1, 1, "S1-V1-arrive", "mes-line2"), p);
        Inputs.OccupancyEvent ra = (Inputs.OccupancyEvent) read(InputParser.OCCUPANCY, a).parsed();
        assertEquals(new Inputs.OccupancyEvent("S1-V1-arrive", 1794183000123L, 1794183000200L, "UNIT0000000000001", OccupancyRecords.ARRIVING, S1, 1L, null, "mes-line2", null), ra);
        Inputs.OccupancyEvent rd = (Inputs.OccupancyEvent) read(InputParser.OCCUPANCY, d).parsed();
        assertEquals("S1-V1-arrive", rd.arrivalEventId());
        assertEquals(OccupancyRecords.DEPARTING, rd.bizStep());
        assertEquals(1794183060123L, rd.eventTime());

        ObjectNode c = OccupancyRecords.checked(OccupancyRecords.errorDeclaration(a, 1794183100000L, 1794183100050L, "urn:epcglobal:cbv:er:did_not_occur", List.of()), p);
        Inputs.OccupancyEvent rc = (Inputs.OccupancyEvent) read(InputParser.OCCUPANCY, c).parsed();
        assertEquals(1794183100000L, rc.errorDeclaration().path("declarationTime").longValue());
        // Decision 17: everything but errorDeclaration and recordTime equals the original
        ObjectNode stripped = c.deepCopy();
        stripped.remove("errorDeclaration");
        stripped.remove("recordTime");
        ObjectNode original = a.deepCopy();
        original.remove("recordTime");
        assertEquals(original, stripped);
        assertThrows(IllegalArgumentException.class, () -> OccupancyRecords.errorDeclaration(c, 1, 1, "urn:epcglobal:cbv:er:did_not_occur", List.of()));
    }

    @Test
    void occupancyAndIncidentsCarryTheFieldOrderOfTheStep3Recordings() throws Exception {
        ObjectNode a = OccupancyRecords.arrival("e", 1, 2, "UNIT0000000000001", S1, 1, "mes-line2");
        ObjectNode d = OccupancyRecords.departure("f", 1, 2, "UNIT0000000000001", S1, 1, "e", "mes-line2");
        assertEquals(fieldNames(firstRecorded("mfg.occupancy", 0, 0)), fieldNames(a));
        assertEquals(fieldNames(firstRecorded("mfg.occupancy", 0, 1)), fieldNames(d));
        JsonNode recorded = Json.read(Files.readAllLines(SMOKE_INCIDENT.resolve("quality.incidents").resolve(Murmur2.partition("synthetic-site:line2:station1:nutrunner1", 6) + ".jsonl")).get(0).getBytes()).path("value");
        ObjectNode i = IncidentRecords.revision("inc-1", 1, S1, T1, "tool-check-failed", 10, 10, 5, 10L, "closed", "urn:x", "qms-line2");
        assertEquals(fieldNames(recorded), fieldNames(i));
    }

    @Test
    void theBindingProfileAndTheSchemaRefuseBeforeASend() {
        LiveEquipment.OccupancyProfile p = LiveEquipmentTest.dedicated().occupancy();
        Map<String, ObjectNode> refused = new LinkedHashMap<>();
        refused.put("sources", OccupancyRecords.arrival("e", 1, 2, "UNIT0000000000001", S1, 1, "mes-line9"));
        refused.put("identity.pattern", OccupancyRecords.arrival("e", 1, 2, "unit-1", S1, 1, "mes-line2"));
        ObjectNode wrongReason = OccupancyRecords.errorDeclaration(OccupancyRecords.arrival("e", 1, 2, "UNIT0000000000001", S1, 1, "mes-line2"), 3, 3,
            "urn:epcglobal:cbv:er:other", List.of());
        refused.put("schema-invalid", wrongReason);   // the schema's enum already refuses it, before the profile's reasons
        ObjectNode readPoint = OccupancyRecords.arrival("e", 1, 2, "UNIT0000000000001", "Site:Line2", 1, "mes-line2");
        refused.put("schema-invalid (readPoint pattern)", readPoint);
        for (Map.Entry<String, ObjectNode> r : refused.entrySet()) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> OccupancyRecords.checked(r.getValue(), p), r.getKey());
            assertTrue(e.getMessage().contains(r.getKey().split(" ")[0]), e.getMessage());
        }
        LiveEquipment.OccupancyProfile narrow = new LiveEquipment.OccupancyProfile(p.schemaSha256(), p.sources(), p.pattern(), p.enter(), p.leave(),
            List.of("urn:epcglobal:cbv:er:incorrect_data"));
        ObjectNode didNotOccur = OccupancyRecords.errorDeclaration(OccupancyRecords.arrival("e", 1, 2, "UNIT0000000000001", S1, 1, "mes-line2"), 3, 3,
            "urn:epcglobal:cbv:er:did_not_occur", List.of());
        assertTrue(assertThrows(IllegalArgumentException.class, () -> OccupancyRecords.checked(didNotOccur, narrow)).getMessage().contains("correction.reasons"));
        assertThrows(IllegalArgumentException.class, () -> OccupancyRecords.departure("f", 1, 2, "UNIT0000000000001", S1, 1, null, "mes-line2"));
    }

    @Test
    void anIncidentRoundTripsAndItsRulesRefuse() throws Exception {
        LiveEquipment.IncidentProfile p = LiveEquipmentTest.dedicated().incidents();
        ObjectNode open = IncidentRecords.checked(IncidentRecords.revision("inc-1", 1, S1, T1, "gauge-drift", 1000, 1000, 500, null, "open", "urn:x", "qms-line2"), p);
        Inputs.IncidentEvent r = (Inputs.IncidentEvent) read(InputParser.INCIDENTS, open).parsed();
        assertEquals(new Inputs.IncidentEvent("inc-1", 1, S1, T1, "gauge-drift", 1000, 1000, 500, null, "open", "urn:x", "qms-line2"), r);
        ObjectNode later = IncidentRecords.checked(IncidentRecords.revision("inc-1", 2, S1, T1, "gauge-drift", 1000, 3000, 500, 2000L, "closed", "urn:x", "qms-line2"), p);
        assertEquals(3000, ((Inputs.IncidentEvent) read(InputParser.INCIDENTS, later).parsed()).revisionEventTime());

        assertTrue(assertThrows(IllegalArgumentException.class, () -> IncidentRecords.checked(
            IncidentRecords.revision("inc-1", 1, S1, T1, "gauge-drift", 1000, 1001, 500, null, "open", "urn:x", "qms-line2"), p)).getMessage().contains("revisionEventTime"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> IncidentRecords.checked(
            IncidentRecords.revision("inc-1", 1, "site-071:line2:station2", T1, "gauge-drift", 1000, 1000, 500, null, "open", "urn:x", "qms-line2"), p)).getMessage().contains("is not"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> IncidentRecords.checked(
            IncidentRecords.revision("inc-1", 1, S1, T1, "gauge-drift", 1000, 1000, 500, null, "closed", "urn:x", "qms-line2"), p)).getMessage().contains("schema-invalid"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> IncidentRecords.checked(
            IncidentRecords.revision("inc-1", 1, S1, T1, "gauge-drift", 1000, 1000, 500, null, "open", "urn:x", "qms-line2"),
            new LiveEquipment.IncidentProfile(p.schemaSha256(), List.of("tool-check-failed")))).getMessage().contains("kinds"));
    }

    @Test
    void occupancyGoesToTheStationPartitionAndIncidentsToTheDefaultPartitionerOfTheirEquipment() {
        RoutingManifest r = RoutingManifest.read(Json.read(LiveEquipmentTest.LIVE.resolve("routing/dedicated.json")));
        assertEquals(0, OccupancyRecords.partition(r, OccupancyRecords.arrival("e", 1, 2, "UNIT0000000000001", S1, 1, "mes-line2")));
        assertEquals(2, OccupancyRecords.partition(r, OccupancyRecords.arrival("e", 1, 2, "UNIT0000000000001", "site-071:line2:station3", 1, "mes-line2")));
        assertEquals(S1, OccupancyRecords.key(OccupancyRecords.arrival("e", 1, 2, "UNIT0000000000001", S1, 1, "mes-line2")));
        ObjectNode i = IncidentRecords.revision("inc-1", 1, S1, T1, "gauge-drift", 1, 1, 0, null, "open", "urn:x", "q");
        assertEquals(T1, IncidentRecords.key(i));
        assertEquals(r.rawPartitionOf(T1), IncidentRecords.partition(i, 6));   // the same default partitioner that placed the device
    }
}
