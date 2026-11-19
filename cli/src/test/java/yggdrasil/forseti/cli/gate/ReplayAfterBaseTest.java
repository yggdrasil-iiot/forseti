package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;

/** Injection 23's base and replay generations identified by the gate run's registrations, not by position: in a shard nothing is
 *  registered between them (base + 1); in the unsharded gate every scenario registers first, so other runs' generations lie between. */
class ReplayAfterBaseTest {
    static ObjectNode store() { return Json.object().put("cluster", "kafka:c").put("generationsTopic", "g").put("recoveryControlTopic", "r"); }

    static void reg(ArrayNode a, long generation, String run, String row) {
        ObjectNode o = a.addObject().put("store", "pipeline").put("run", run).put("registeredRowSha256", row);
        o.set("controlStore", store());
        Json.putSafe(o, "generation", generation);
    }

    static ObjectNode evidence(long generation) { return Json.putSafe(store(), "generation", generation); }

    static List<String> check(ArrayNode regs, long base, long replay) {
        Injections.P p = new Injections.P();
        Injections.replayAfterBase(p, regs, evidence(base), evidence(replay), "rowB", "rowR");
        return p.list;
    }

    @Test void inAShardTheReplayIsTheNextGeneration() {
        ArrayNode a = Json.JSON.createArrayNode();
        reg(a, 1, "horizon-planned-late", "rowB");
        reg(a, 2, "horizon-planned-late@replay", "rowR");
        assertEquals(List.of(), check(a, 1, 2));
    }

    @Test void inTheUnshardedGateOtherRunsGenerationsMayLieBetween() {
        ArrayNode a = Json.JSON.createArrayNode();
        reg(a, 1, "baseline", "x1");
        reg(a, 2, "horizon-planned-late", "rowB");
        reg(a, 3, "plc-horizon-no-end", "x3");
        reg(a, 4, "smoke", "x4");
        reg(a, 5, "horizon-planned-late@replay", "rowR");
        assertEquals(List.of(), check(a, 2, 5));
    }

    @Test void aGapAnotherRegistrationOfTheScenarioOrAWrongRowFails() {
        ArrayNode gap = Json.JSON.createArrayNode();
        reg(gap, 1, "horizon-planned-late", "rowB");
        reg(gap, 3, "horizon-planned-late@replay", "rowR");
        assertFalse(check(gap, 1, 3).isEmpty(), "generation 2 is not a registration of this gate run");
        ArrayNode ours = Json.JSON.createArrayNode();
        reg(ours, 1, "horizon-planned-late", "rowB");
        reg(ours, 2, "horizon-planned-late@rule-x", "x");
        reg(ours, 3, "horizon-planned-late@replay", "rowR");
        assertFalse(check(ours, 1, 3).isEmpty(), "another generation of the scenario between the base and the replay");
        ArrayNode row = Json.JSON.createArrayNode();
        reg(row, 1, "horizon-planned-late", "other");
        reg(row, 2, "horizon-planned-late@replay", "rowR");
        assertFalse(check(row, 1, 2).isEmpty(), "the base generation's row is not the base run's");
        ArrayNode backwards = Json.JSON.createArrayNode();
        reg(backwards, 2, "horizon-planned-late", "rowB");
        reg(backwards, 1, "horizon-planned-late@replay", "rowR");
        assertFalse(check(backwards, 2, 1).isEmpty(), "the replay before the base");
        Injections.P p = new Injections.P();
        Injections.replayAfterBase(p, null, evidence(1), evidence(2), "rowB", "rowR");
        assertFalse(p.list.isEmpty(), "no registrations");
    }
}
