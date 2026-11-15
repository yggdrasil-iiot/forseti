package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;

/** The injections a live capture declares in its manifest: the scenario's bad statuses and the runner's options, with the device's
 *  station and the window in the scenario's clock. */
class LiveInjectionsTest {
    static final String S1 = DrivePlanTest.S1, T1 = DrivePlanTest.T1;

    @Test
    void theScenarioBadStatusAndTheRunnerOptionsAreDeclaredInOrder() {
        LiveEquipment.LiveStream s = LiveEquipmentTest.dedicated();
        LiveScenario bad = LiveScenario.read(Json.read(DrivePlanTest.BASELINE.resolveSibling("bad-status.json")), s);
        JsonNode d = LiveInjections.declare(bad, s, List.of("kill-muninn=line2:station1@440000+20000", "host-offline=" + T1 + "@200000+15000",
            "bridge-hold=60000:600000", "bridge-source-offset=-30000"));
        assertEquals(5, d.size());
        assertEquals(Json.read(("{\"injection\":\"3\",\"kind\":\"bad-status\",\"equipment\":\"" + T1 + "\",\"station\":\"" + S1
            + "\",\"member\":\"Torque\",\"from\":70000,\"to\":100000}").getBytes()).toString(), d.get(0).toString());
        assertEquals(Json.read(("{\"injection\":\"8\",\"kind\":\"kill-muninn\",\"equipment\":\"" + S1 + "\",\"station\":\"" + S1
            + "\",\"from\":440000,\"to\":460000}").getBytes()).toString(), d.get(1).toString());
        assertEquals(Json.read(("{\"injection\":\"25(a)\",\"kind\":\"host-offline\",\"equipment\":\"" + T1 + "\",\"station\":\"" + S1
            + "\",\"from\":200000,\"to\":215000}").getBytes()).toString(), d.get(2).toString(), "a full path is kept");
        assertEquals(Json.read("{\"injection\":\"1\",\"kind\":\"bridge-hold\",\"startMs\":60000,\"holdMs\":600000}".getBytes()).toString(), d.get(3).toString());
        assertEquals(Json.read("{\"injection\":\"9\",\"kind\":\"bridge-source-offset\",\"offsetMs\":-30000}".getBytes()).toString(), d.get(4).toString());
        assertEquals(0, LiveInjections.declare(DrivePlanTest.baseline(s), s, List.of()).size(), "the baseline declares none");
    }

    @Test
    void aMalformedOrRepeatedOptionIsRefused() {
        LiveEquipment.LiveStream s = LiveEquipmentTest.dedicated();
        LiveScenario base = DrivePlanTest.baseline(s);
        for (List<String> o : List.of(List.of("bridge-hold"), List.of("bridge-hold=1:0"), List.of("bridge-source-offset=0"), List.of("kill-muninn=line2:station9@1+1"),
            List.of("kill-muninn=line2:station1@1"), List.of("host-offline=line2:station1@1+1", "host-offline=line2:station2@1+1"), List.of("reboot=1")))
            assertThrows(IllegalArgumentException.class, () -> LiveInjections.declare(base, s, o), o.toString());
    }
}
