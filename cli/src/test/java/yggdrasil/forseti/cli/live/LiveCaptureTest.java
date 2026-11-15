package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;

/** The capture's inputs from the committed live fixtures, the baseline live scenario and a drive log. */
class LiveCaptureTest {
    static final Path LIVE = LiveEquipmentTest.LIVE, ROUTING = LIVE.resolve("routing");
    static final String S1 = DrivePlanTest.S1, T1 = DrivePlanTest.T1;

    @TempDir Path tmp;

    Path driveLog(String... lines) throws Exception {
        Path f = tmp.resolve("drive.jsonl");
        Files.writeString(f, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
        return f;
    }

    @Test
    void pinsDefinitionsOriginAndVisitFactsComeFromTheFixturesTheScenarioAndTheDriveLog() throws Exception {
        Path log = driveLog(
            "{\"at\":0,\"commits\":{\"" + S1 + "\":1794700001000,\"" + T1 + "\":1794700000000},\"eventTime\":1794700001000,\"sent\":[{\"eventId\":\"V1-arrive\"}]}",
            "{\"at\":60000,\"commits\":{\"" + S1 + "\":1794700061000},\"eventTime\":1794700061000,\"sent\":[{\"eventId\":\"V1-depart\"}]}");
        CaptureRecorder.Inputs in = LiveCapture.inputs(LiveEquipmentTest.REGISTRY, LiveEquipmentTest.DEFINITIONS, LIVE.resolve("deployment.json"), "line2-dedicated",
            ROUTING.resolve("dedicated.json"), DrivePlanTest.BASELINE, log, null, 60_000);
        assertEquals("baseline", in.name());
        assertEquals(1794700000000L, in.t0(), "the earliest commit of the step at offset 0");
        assertEquals(250, in.scanMs());
        assertEquals(500, in.source().get("muninnPeriodMs").asLong());
        assertEquals(0, in.source().get("injections").size(), "the baseline declares no injection");
        assertEquals("line2-dedicated", in.rulePin().get("stream").asText());
        JsonNode head = Json.read(LiveEquipmentTest.REGISTRY.resolve("mappings/heads/line2-dedicated.json"));
        assertEquals(head.get("id").asText(), in.rulePin().get("releaseId").asText());
        assertEquals(head.get("version").asText(), in.rulePin().get("version").asText());
        LiveEquipment.LiveStream s = LiveEquipment.read(LiveEquipmentTest.REGISTRY, LiveEquipmentTest.DEFINITIONS, "line2-dedicated");
        assertEquals(5, in.definitions().size());
        assertEquals(s.device(T1).contentSha256(), in.definitions().get(T1).get("sha256").asText());
        assertEquals("tool-model", in.definitions().get(T1).get("templateRef").asText());
        assertEquals(2, in.sent().size());
        LiveScenario sc = DrivePlanTest.baseline(s);
        assertEquals(sc.visits().size(), in.facts().size());
        LiveScenario.Visit v1 = sc.visits().get(0);
        assertEquals(v1.visitId(), in.facts().get(v1.arrivalId()).visitRef());
        assertEquals(v1.tools(), in.facts().get(v1.arrivalId()).tools());
        assertEquals(64, in.source().get("driveLogSha256").asText().length());
    }

    @Test
    void aDriveLogWithoutTheFirstStepHasNoOrigin() throws Exception {
        Path log = driveLog("{\"at\":1000,\"commits\":{\"" + S1 + "\":1794700001000},\"eventTime\":1794700001000,\"sent\":[]}");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> LiveCapture.inputs(LiveEquipmentTest.REGISTRY, LiveEquipmentTest.DEFINITIONS,
            LIVE.resolve("deployment.json"), "line2-dedicated", ROUTING.resolve("dedicated.json"), DrivePlanTest.BASELINE, log, "x", 60_000));
        assertTrue(e.getMessage().contains("no drive origin"), e.getMessage());
    }

    @Test
    void theRunnersInjectionsAreDeclaredAndAPeriodThatDoesNotFitTheRuleIsRefused() throws Exception {
        Path log = driveLog("{\"at\":0,\"commits\":{\"" + S1 + "\":1794700001000},\"eventTime\":1794700001000,\"sent\":[]}");
        CaptureRecorder.Inputs in = LiveCapture.inputs(LiveEquipmentTest.REGISTRY, LiveEquipmentTest.DEFINITIONS, LIVE.resolve("deployment.json"), "line2-dedicated",
            ROUTING.resolve("dedicated.json"), DrivePlanTest.BASELINE, log, null, 60_000, java.util.List.of("kill-muninn=line2:station1@440000+20000"));
        assertEquals("kill-muninn", in.source().get("injections").get(0).get("kind").asText());
        com.fasterxml.jackson.databind.node.ObjectNode slow = (com.fasterxml.jackson.databind.node.ObjectNode) Json.read(DrivePlanTest.BASELINE);
        slow.put("scanMs", 1000).put("muninnPeriodMs", 1000);
        Path f = tmp.resolve("slow.json");
        Json.write(f, slow);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> LiveCapture.inputs(LiveEquipmentTest.REGISTRY, LiveEquipmentTest.DEFINITIONS,
            LIVE.resolve("deployment.json"), "line2-dedicated", ROUTING.resolve("dedicated.json"), f, log, null, 60_000));
        assertTrue(e.getMessage().contains("exceeds the rule's expectedPeriodMs 1000"), e.getMessage());
    }
}
