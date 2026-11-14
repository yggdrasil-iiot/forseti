package yggdrasil.forseti.loop;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.live.LiveEquipment;
import yggdrasil.forseti.core.Json;

/** What the runner starts for each committed stream: five Muninns with unique edge ids, pins exactly the definitions' digests of the
 *  routed devices (what preflight requires), and the rule's tail. */
class LiveDeploymentTest {
    static final Path LIVE = DriverTest.LIVE;

    static LiveDeployment.Deployment of(String stream, String placement) {
        return LiveDeployment.of(LIVE.resolve("registry"), LIVE.resolve("definitions"), LIVE.resolve("deployment.json"), stream,
            LIVE.resolve("routing").resolve(placement + ".json"));
    }

    @Test
    void edgeIdsArePathsBelowTheSite() {
        assertEquals("line2-station1-nutrunner1", LiveDeployment.edgeOf("site-071:line2:station1:nutrunner1"));
        assertEquals("line2-station2", LiveDeployment.edgeOf("site-019:line2:station2"));
        assertThrows(IllegalArgumentException.class, () -> LiveDeployment.edgeOf("site-071"));
        assertThrows(IllegalArgumentException.class, () -> LiveDeployment.edgeOf("site-071:line2/station1"));
    }

    @Test
    void everyCommittedStreamGivesFiveEdgesPinsAndTheTail() {
        for (String[] s : List.of(new String[] {"line2-dedicated", "dedicated"}, new String[] {"line2-shared", "shared"},
                                  new String[] {"line2-dedicated-plc", "dedicated"}, new String[] {"line2-shared-plc", "shared"})) {
            LiveDeployment.Deployment d = of(s[0], s[1]);
            assertEquals(List.of("line2-station1", "line2-station1-nutrunner1", "line2-station1-nutrunner2", "line2-station2", "line2-station3"),
                d.edges().stream().map(LiveDeployment.Edge::edge).toList(), s[0]);
            assertEquals(720_000 + 30_000 + 1_000, d.tailMs(), s[0]);
            assertEquals(1_000, d.expectedPeriodMs());
            LiveEquipment.LiveStream st = LiveEquipment.read(LIVE.resolve("registry"), LIVE.resolve("definitions"), s[0]);
            SortedMap<String, String> pins = new TreeMap<>();
            d.pins().fields().forEachRemaining(e -> pins.put(e.getKey(), e.getValue().asText()));
            assertEquals(st.pins(), pins, s[0]);
            LiveDeployment.Edge tool = d.edges().get(1);
            assertEquals("tool-model", tool.templateRef());
            assertEquals("nutrunner1", tool.instanceId());
            assertEquals("Line2/Station1/Nutrunner1", tool.node(), "the sim node Muninn samples is the device's source node");
            assertEquals("host-line2-station1-nutrunner1", tool.host());
        }
    }

    @Test
    void theCommandWritesThePinsAndPrintsOneLinePerEdge(@TempDir Path dir) throws Exception {
        Path pins = dir.resolve("pins.json");
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        int code = LoopMain.run(new String[] {"deployment", "--registry", LIVE.resolve("registry").toString(), "--definitions", LIVE.resolve("definitions").toString(),
            "--stream", "line2-dedicated", "--routing", LIVE.resolve("routing").resolve("dedicated.json").toString(), "--pins-out", pins.toString()},
            new PrintStream(b, true, StandardCharsets.UTF_8));
        assertEquals(0, code);
        List<String> lines = b.toString(StandardCharsets.UTF_8).lines().toList();
        assertEquals("tail-ms\t751000", lines.get(0));
        assertEquals("period-ms\t1000", lines.get(1));
        assertEquals(7, lines.size());
        assertEquals("edge\tline2-station1\tsite-071:line2:station1\tstation-model\t1.0.0\turn:bifrost:opcua:sim\tLine2/Station1\tstation1", lines.get(2));
        JsonNode p = Json.read(pins);
        assertEquals(5, p.size());
        assertEquals(of("line2-dedicated", "dedicated").pins(), p);
    }
}
