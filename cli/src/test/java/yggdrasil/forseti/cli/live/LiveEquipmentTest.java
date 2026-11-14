package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;

/** What the loop reads from the committed live fixtures (step-5 PR 3 bytes): each device's sim node, its definition and pin, the
 *  rule's period and mode, and the binding profiles. */
class LiveEquipmentTest {
    static final Path LIVE = Path.of("..", "fixtures", "live"), REGISTRY = LIVE.resolve("registry"), DEFINITIONS = LIVE.resolve("definitions");
    static final String STATION_SHA = "4d40df048267814f409ff93e16cb838cb0e5be710fbde83a39001f96c39efbac";
    static final String TOOL_SHA = "276bf45c3f834818d359442395c40f2932af7436c81a4b72d20a8ff218c9a4dc";

    static LiveEquipment.LiveStream dedicated() { return LiveEquipment.read(REGISTRY, DEFINITIONS, "line2-dedicated"); }

    @Test
    void everyDeviceOfTheDedicatedStreamHasItsSimNodeDefinitionAndPin() {
        LiveEquipment.LiveStream s = dedicated();
        assertEquals(List.of("site-071:line2:station1", "site-071:line2:station1:nutrunner1", "site-071:line2:station1:nutrunner2",
            "site-071:line2:station2", "site-071:line2:station3"), new ArrayList<>(s.equipment().keySet()));
        LiveEquipment.Equipment st1 = s.device("site-071:line2:station1"), t1 = s.device("site-071:line2:station1:nutrunner1");
        assertEquals("urn:bifrost:opcua:sim", st1.namespaceUri());
        assertEquals("Line2/Station1", st1.node());
        assertEquals("Line2/Station1/Nutrunner1", t1.node());
        assertEquals("Line2/Station3", s.device("site-071:line2:station3").node());
        assertTrue(st1.isStation());
        assertFalse(t1.isStation());
        assertEquals("site-071:line2:station1", t1.station());
        assertEquals(List.of("Running", "UnitId", "VisitId", "VisitPresent"), st1.members().stream().map(LiveEquipment.Member::name).toList());
        assertEquals(List.of("Torque", "Running", "UnitId", "VisitId", "VisitPresent"), t1.members().stream().map(LiveEquipment.Member::name).toList());
        assertEquals("Double", t1.member("Torque").type());
        assertEquals("Sim/Line2/Station1/Nutrunner1.Torque.Value", t1.valueControl("Torque"));
        assertEquals("Sim/Line2/Station1/Nutrunner1.Torque.StatusCode", t1.statusControl("Torque"));
        assertEquals("Line2/Station1/Nutrunner1.Torque", t1.live("Torque"));
        assertEquals(STATION_SHA, s.pins().get("site-071:line2:station2"));
        assertEquals(TOOL_SHA, s.pins().get("site-071:line2:station1:nutrunner2"));
        assertEquals(5, s.pins().size());
    }

    @Test
    void theRuleGivesThePeriodModeAndTheBindingProfiles() {
        LiveEquipment.LiveStream s = dedicated();
        assertEquals(1000, s.expectedPeriodMs());
        assertEquals("occupancy-join", s.mode());
        assertEquals(List.of("site-071:line2:station1", "site-071:line2:station2", "site-071:line2:station3"), s.stations());
        assertEquals(List.of("mes-line2"), s.occupancy().sources());
        assertEquals("[A-Z0-9]{17}", s.occupancy().pattern());
        assertEquals(OccupancyRecords.ARRIVING, s.occupancy().enter());
        assertEquals(OccupancyRecords.DEPARTING, s.occupancy().leave());
        assertEquals(List.of("urn:epcglobal:cbv:er:did_not_occur", "urn:epcglobal:cbv:er:incorrect_data"), s.occupancy().reasons());
        assertEquals(List.of("tool-check-failed", "gauge-drift"), s.incidents().kinds());
        assertEquals("site-019:line2:station3", LiveEquipment.read(REGISTRY, DEFINITIONS, "line2-shared").device("site-019:line2:station3").device());
    }

    /** The plc streams' targets name {@code station-stamped-model} and {@code tool-stamped-model}; the deployment binding, not the target,
     *  says what each device runs, so both regimes and both placements pin the published station-model and tool-model. */
    @Test
    void everyStreamOfBothRegimesAndPlacementsPinsThePublishedDefinitionsThroughTheBinding() {
        JsonNode binding = Json.read(LIVE.resolve("deployment.json"));
        Set<String> bound = new TreeSet<>();
        binding.fieldNames().forEachRemaining(bound::add);
        Set<String> routed = new TreeSet<>();
        for (String stream : List.of("line2-dedicated", "line2-dedicated-plc", "line2-shared", "line2-shared-plc")) {
            LiveEquipment.LiveStream s = LiveEquipment.read(REGISTRY, DEFINITIONS, stream);
            for (LiveEquipment.Equipment e : s.equipment().values()) {
                assertEquals(e.isStation() ? "station-model" : "tool-model", e.definitionRef(), stream + " " + e.device());
                assertEquals(e.isStation() ? STATION_SHA : TOOL_SHA, e.contentSha256());
                assertEquals(e.isStation() ? 4 : 5, e.members().size());
                routed.add(e.device());
            }
        }
        assertEquals(routed, bound, "exactly the devices of the four live streams are bound");
        LiveEquipment.LiveStream plc = LiveEquipment.read(REGISTRY, DEFINITIONS, "line2-shared-plc");
        assertEquals("plc-stamped", plc.mode());
        assertEquals("occupancy", plc.crossCheck());
        assertEquals(LiveEquipment.read(REGISTRY, DEFINITIONS, "line2-shared").pins(), plc.pins(), "both regimes pin the same definitions");
        var lock = Json.read(LIVE.resolve("lock.json"));
        assertEquals(sha256(LIVE.resolve("deployment.json")), lock.path("deployment").path("deployment.json").asText(), "the lock pins the binding");
    }

    static String sha256(Path p) {
        try { return yggdrasil.forseti.core.canon.Canonical.digestOf(Files.readAllBytes(p)); } catch (IOException e) { throw new java.io.UncheckedIOException(e); }
    }

    static Path copyDefinitions(Path tmp) throws IOException {
        Path defs = tmp.resolve("definitions");
        try (Stream<Path> w = Files.walk(DEFINITIONS)) {
            for (Path p : w.toList()) {
                Path q = defs.resolve(DEFINITIONS.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(q); else Files.copy(p, q);
            }
        }
        Files.copy(LIVE.resolve("deployment.json"), tmp.resolve("deployment.json"));
        return defs;
    }

    @Test
    void aDefinitionWhoseBytesAreNotItsContentSha256IsRefused(@TempDir Path tmp) throws IOException {
        Path defs = copyDefinitions(tmp);
        LiveEquipment.read(REGISTRY, defs, "line2-dedicated");   // the copy reads
        Path yaml = defs.resolve("recipe/tool-model/1.0.0/recipe-setpoints.yaml");
        Files.writeString(yaml, Files.readString(yaml, StandardCharsets.UTF_8).replace("\"Torque\"", "\"Torq\""), StandardCharsets.UTF_8);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> LiveEquipment.read(REGISTRY, defs, "line2-dedicated"));
        assertTrue(e.getMessage().contains("contentSha256"), e.getMessage());
    }

    @Test
    void aDeviceWithoutABindingOrBoundToAnUnpublishedDefinitionIsRefused(@TempDir Path tmp) throws IOException {
        Path defs = copyDefinitions(tmp), binding = tmp.resolve("deployment.json");
        com.fasterxml.jackson.databind.node.ObjectNode b = (com.fasterxml.jackson.databind.node.ObjectNode) Json.read(binding);
        b.remove("site-071:line2:station2");
        Json.write(binding, b);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> LiveEquipment.read(REGISTRY, defs, "line2-dedicated")).getMessage()
            .contains("site-071:line2:station2 has no entry in the deployment binding"));
        b.putObject("site-071:line2:station2").put("templateRef", "station-stamped-model").put("version", "1.0.0");
        Json.write(binding, b);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> LiveEquipment.read(REGISTRY, defs, binding, "line2-dedicated")).getMessage()
            .contains("is bound to station-stamped-model 1.0.0, which"));
    }

    @Test
    void theBindingProfilesNameTheSchemaDigestsTheLockPins() {
        var lock = Json.read(LIVE.resolve("lock.json"));
        assertEquals(lock.path("occupancySchemaSha256").asText(), dedicated().occupancy().schemaSha256());
        assertEquals(lock.path("incidentSchemaSha256").asText(), dedicated().incidents().schemaSha256());
    }
}
