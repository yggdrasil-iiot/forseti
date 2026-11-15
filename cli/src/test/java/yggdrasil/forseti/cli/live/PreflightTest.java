package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;

/** Each preflight refusal (step-5 PR 5), on facts that are clean except for the one thing a test breaks. */
class PreflightTest {
    static final String S1 = "site-071:line2:station1", T1 = S1 + ":nutrunner1", T2 = S1 + ":nutrunner2", S2 = "site-071:line2:station2", S3 = "site-071:line2:station3";

    /** Clean facts for the dedicated placement; each field may be changed before {@link #build}. */
    static final class F {
        LiveEquipment.LiveStream stream = LiveEquipmentTest.dedicated();
        String kind = "dedicated";
        JsonNode routing = Json.read(LiveEquipmentTest.LIVE.resolve("routing/dedicated.json"));
        JsonNode recomputed = Placement.fixture(LiveEquipmentTest.REGISTRY, "dedicated");
        String recomputeError = null;
        String bridge = "forseti-bridge", driver = "forseti-driver";
        List<Preflight.Edge> edges = new ArrayList<>(List.of(new Preflight.Edge("s1", S1), new Preflight.Edge("t1", T1), new Preflight.Edge("t2", T2),
            new Preflight.Edge("s2", S2), new Preflight.Edge("s3", S3)));
        long period = 500, scan = 250;
        List<LiveTopics.Spec> topics = LiveTopics.plan("run-1.");
        Map<String, LiveTopics.Observed> observed = LiveTopicsTest.asPlanned(topics);
        Set<String> present = new TreeSet<>(Preflight.requiredNodes(stream, stream.equipment().keySet()));
        Map<String, String> pins = new TreeMap<>(stream.pins());

        Preflight.Facts build() {
            return new Preflight.Facts(stream, kind, routing, recomputed, recomputeError, bridge, driver, edges, period, scan, topics, observed, present, pins);
        }
        List<String> problems() { return Preflight.problems(build()); }
    }

    static void hasOnly(List<String> problems, String... expected) {
        assertEquals(List.of(expected), problems);
    }

    @Test
    void cleanFactsHaveNoProblem() {
        assertEquals(List.of(), new F().problems());
        assertEquals(66, Preflight.requiredNodes(new F().stream, new F().stream.equipment().keySet()).size());   // (3 x 4 + 2 x 5) members, each with two controls
    }

    @Test
    void aDuplicateMqttClientIdIsRefusedThroughMuninnsDerivedId() {
        F f = new F();
        f.bridge = "muninn-edge-s2";
        hasOnly(f.problems(), "MQTT client id muninn-edge-s2 is used by bridge, muninn s2 (" + S2 + ")");
        f = new F();
        f.edges.set(1, new Preflight.Edge("s1", T1));
        assertTrue(f.problems().contains("MQTT client id muninn-edge-s1 is used by muninn s1 (" + S1 + "), muninn s1 (" + T1 + ")"), f.problems().toString());
        f = new F();
        f.driver = f.bridge;
        hasOnly(f.problems(), "MQTT client id forseti-bridge is used by bridge, driver");
    }

    @Test
    void everyRoutedDeviceHasExactlyOneEdge() {
        F f = new F();
        f.edges.remove(4);
        f.edges.add(new Preflight.Edge("s1b", S1));
        f.edges.add(new Preflight.Edge("x", "site-071:line2:station9"));
        List<String> p = f.problems();
        assertTrue(p.contains("routed device " + S1 + " has 2 Muninn edges [s1, s1b], not one"), p.toString());
        assertTrue(p.contains("routed device " + S3 + " has 0 Muninn edges [], not one"), p.toString());
        assertTrue(p.contains("edge [x] serves site-071:line2:station9, which the routing manifest does not route"), p.toString());
        assertEquals(3, p.size(), p.toString());
    }

    @Test
    void aTopicWithAnotherPartitionCountOrTimestampTypeIsRefused() {
        F f = new F();
        f.observed.put("run-1.ot.messages", new LiveTopics.Observed("run-1.ot.messages", 5, f.topics.get(0).configs()));
        Map<String, String> c = new TreeMap<>(f.topics.get(3).configs());
        c.put("message.timestamp.type", "CreateTime");
        f.observed.put("run-1.quality.incidents", new LiveTopics.Observed("run-1.quality.incidents", 6, c));
        hasOnly(f.problems(), "topic run-1.ot.messages has 5 partitions, not 6",
            "topic run-1.quality.incidents has message.timestamp.type=CreateTime, not LogAppendTime");
    }

    @Test
    void theMuninnPeriodPlusTheScanPlusTheJitterMarginMustFitTheRulePeriod() {
        String bad = "Muninn period %d ms + sim scan %d ms + jitter margin 100 ms exceeds the rule's expectedPeriodMs %d";
        assertEquals(100, Preflight.JITTER_MARGIN_MS);
        F f = new F();
        assertEquals(List.of(), f.problems(), "the live values 500, 250 and 1000");
        f.period = 250; f.scan = 500;
        assertEquals(List.of(), f.problems(), "the fallback 250 and 500");
        f.period = 600; f.scan = 300;
        assertEquals(List.of(), f.problems(), "exactly at the bound");
        f.period = 601;
        hasOnly(f.problems(), String.format(bad, 601, 300, 1000));
        f = new F();
        f.period = 1000; f.scan = 1000;   // the old equality no longer passes
        hasOnly(f.problems(), String.format(bad, 1000, 1000, 1000));
        f = new F();
        LiveEquipment.LiveStream s = f.stream;
        f.stream = new LiveEquipment.LiveStream(s.stream(), s.mode(), s.crossCheck(), 800, s.stations(), s.equipment(), s.occupancy(), s.incidents());
        hasOnly(f.problems(), String.format(bad, 500, 250, 800));
        f = new F();
        f.scan = 0;
        hasOnly(f.problems(), "Muninn period 500 ms and sim scan 0 ms must be positive");
        assertNull(Preflight.periodProblem(500, 250, 1000));
    }

    @Test
    void aRoutingManifestWhoseDigestDoesNotHoldIsRefused() {
        F f = new F();
        ObjectNode r = (ObjectNode) f.routing.deepCopy();
        ((ObjectNode) r.path("devices").path(S3)).put("rawPartition", 0);
        f.routing = r;
        List<String> p = f.problems();
        assertEquals(2, p.size(), p.toString());
        assertTrue(p.get(0).startsWith("routing: malformed recording: routingDigest"), p.toString());
        assertEquals("routing manifest differs from the one recomputed from the registry (dedicated)", p.get(1));
    }

    @Test
    void aValidRoutingOfTheOtherPlacementFailsItsRulesAndDiffersFromTheRecomputedOne() {
        F f = new F();
        f.routing = Json.read(LiveEquipmentTest.LIVE.resolve("routing/shared.json"));
        List<String> p = f.problems();
        assertTrue(p.stream().anyMatch(x -> x.startsWith("routing: dedicated:")), p.toString());
        assertTrue(p.contains("routing manifest differs from the one recomputed from the registry (dedicated)"), p.toString());
        // the shared manifest routes the site-019 devices, so the dedicated edges and pins do not match either
        assertTrue(p.stream().anyMatch(x -> x.contains("is not a device of stream line2-dedicated")), p.toString());
    }

    @Test
    void aRoutingThatCannotBeRecomputedIsRefused() {
        F f = new F();
        f.recomputed = null;
        f.recomputeError = "line2-dedicated-plc places differently from line2-dedicated";
        hasOnly(f.problems(), "routing could not be recomputed from the registry: line2-dedicated-plc places differently from line2-dedicated");
        f = new F();
        f.routing = null;
        assertTrue(f.problems().contains("routing manifest is not JSON"), f.problems().toString());
    }

    @Test
    void aMissingSimControlIsRefused() {
        F f = new F();
        f.present.remove("Sim/Line2/Station2.Running.Value");
        f.present.remove("Sim/Line2/Station1/Nutrunner2.Torque.StatusCode");
        f.present.remove("Line2/Station3.VisitPresent");
        hasOnly(f.problems(), "sim node Line2/Station3.VisitPresent is missing", "sim node Sim/Line2/Station1/Nutrunner2.Torque.StatusCode is missing",
            "sim node Sim/Line2/Station2.Running.Value is missing");
    }

    @Test
    void pinsThatAreNotTheDefinitionsContentSha256AreRefused() {
        F f = new F();
        f.pins.put(S2, LiveEquipmentTest.TOOL_SHA);
        f.pins.remove(T1);
        f.pins.put("site-071:line2:station4", LiveEquipmentTest.STATION_SHA);
        hasOnly(f.problems(), "no pin for " + T1 + " (expected " + LiveEquipmentTest.TOOL_SHA + ")",
            "pin of " + S2 + " is " + LiveEquipmentTest.TOOL_SHA + ", the definition's contentSha256 is " + LiveEquipmentTest.STATION_SHA,
            "pin for site-071:line2:station4, which is not routed");
    }

    @Test
    void aBindingProfileOnAnotherSchemaIsRefused() {
        F f = new F();
        LiveEquipment.LiveStream s = f.stream;
        LiveEquipment.OccupancyProfile o = s.occupancy();
        f.stream = new LiveEquipment.LiveStream(s.stream(), s.mode(), s.crossCheck(), s.expectedPeriodMs(), s.stations(), s.equipment(),
            new LiveEquipment.OccupancyProfile("0".repeat(64), o.sources(), o.pattern(), o.enter(), o.leave(), o.reasons()),
            new LiveEquipment.IncidentProfile("1".repeat(64), s.incidents().kinds()));
        hasOnly(f.problems(), "occupancy binding schemaSha256 " + "0".repeat(64) + " is not the built-in schema 1.0.0",
            "incident binding schemaSha256 " + "1".repeat(64) + " is not the built-in schema 1.0.0");
    }

    @Test
    void theSharedPlacementIsCleanWithItsOwnRouting() {
        F f = new F();
        f.stream = LiveEquipment.read(LiveEquipmentTest.REGISTRY, LiveEquipmentTest.DEFINITIONS, "line2-shared-plc");
        f.kind = Preflight.placementOf("line2-shared-plc");
        f.routing = Json.read(LiveEquipmentTest.LIVE.resolve("routing/shared.json"));
        f.recomputed = Placement.fixture(LiveEquipmentTest.REGISTRY, f.kind);
        f.edges.clear();
        for (String d : f.stream.equipment().keySet()) f.edges.add(new Preflight.Edge(d.substring(d.indexOf(':') + 1).replace(':', '-'), d));
        f.present = new TreeSet<>(Preflight.requiredNodes(f.stream, f.stream.equipment().keySet()));
        f.pins = new TreeMap<>(f.stream.pins());
        assertEquals("shared", f.kind);
        assertEquals(List.of(), f.problems());
        assertThrows(IllegalArgumentException.class, () -> Preflight.placementOf("line2"));
    }
}
