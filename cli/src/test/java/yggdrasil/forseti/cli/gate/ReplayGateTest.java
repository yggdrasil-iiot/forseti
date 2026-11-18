package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.replay.EngineContract;
import yggdrasil.forseti.core.Json;

/** The replay gate's files and accounting without an engine: the committed Flink manifest, shard plan and control file are consistent
 *  (every id owned once, every injection and its scenarios in one shard, every control on the shards of its intended ids), a broken
 *  plan or control file is a usage error, and the engine artifact digest is the sorted (name, sha256) list of the engine path. */
class ReplayGateTest {
    static final Path ROOT = CoreGateTest.ROOT;
    @TempDir Path tmp;

    static List<String> ids() { return ReplayGate.manifestIds(Json.read(ROOT.resolve("gate/flink-assertions.json")), "flink"); }

    @Test void theCommittedFlinkGateFilesAreConsistent() {
        List<String> ids = ids();
        ReplayGate.Plan plan = ReplayGate.Plan.load(ROOT.resolve("gate/flink-shards.json"), ids);
        Map<String, ReplayGate.Control> controls = ReplayGate.Control.load(ROOT.resolve("gate/flink-negative-controls.json"), plan, ids);
        // the eight engine-independent controls of spec 689-700 (theta comes with PR 6)
        assertEquals(List.of("processing-time", "drop-late", "no-cascade", "lenient-oracle", "first-wins", "station-granularity", "ignore-session-gap", "wrong-rules-pin"),
            new ArrayList<>(controls.keySet()));
        // every engine-independent injection of spec 705 but 21, 22 and 24
        Set<String> inj = new TreeSet<>();
        for (String id : ids) if (id.startsWith("I-")) inj.add(id);
        assertEquals(34, inj.size(), inj.toString());
        assertFalse(inj.contains("I-21") || inj.contains("I-22") || inj.contains("I-24"));
        // each horizon recording runs in a shard of its own
        for (String h : Scenarios.HORIZON) {
            long n = plan.shards.stream().filter(s -> s.scenarios().contains(h)).count();
            assertEquals(1, n, h);
            ReplayGate.Shard s = plan.shards.stream().filter(x -> x.scenarios().contains(h)).findFirst().orElseThrow();
            for (String o : Scenarios.HORIZON) if (!o.equals(h)) assertFalse(s.scenarios().contains(o), h + " shares a shard with " + o);
        }
        for (String a : List.of("R-READ-COMMITTED", "FL-KEYGROUP-MAP", "FL-ONE-SPLIT", "FL-WATERMARK-PER-RECORD", "J-W1-FORMULA")) assertTrue(plan.aggregate.contains(a), a);
    }

    @Test void aPlanThatLeavesAnIdUnownedOrOwnsOneTwiceIsRefused() throws Exception {
        List<String> ids = ids();
        ObjectNode plan = (ObjectNode) Json.read(ROOT.resolve("gate/flink-shards.json"));
        ((ArrayNode) plan.get("aggregate")).remove(0);
        Path f = tmp.resolve("plan.json");
        Json.write(f, plan);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ReplayGate.Plan.load(f, ids));
        assertTrue(e.getMessage().contains("owned by no shard"), e.getMessage());
        plan = (ObjectNode) Json.read(ROOT.resolve("gate/flink-shards.json"));
        ((ArrayNode) plan.get("shards").get(0).get("assertions")).add("I-2");
        Json.write(f, plan);
        e = assertThrows(IllegalArgumentException.class, () -> ReplayGate.Plan.load(f, ids));
        assertTrue(e.getMessage().contains("owned twice"), e.getMessage());
    }

    @Test void aControlOnAShardThatOwnsNoneOfItsIdsIsRefused() throws Exception {
        List<String> ids = ids();
        ReplayGate.Plan plan = ReplayGate.Plan.load(ROOT.resolve("gate/flink-shards.json"), ids);
        ObjectNode c = (ObjectNode) Json.read(ROOT.resolve("gate/flink-negative-controls.json"));
        ((ArrayNode) c.get("controls").get(0).get("shards")).removeAll().add("inj-stage1");
        Path f = tmp.resolve("controls.json");
        Json.write(f, c);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ReplayGate.Control.load(f, plan, ids));
        assertTrue(e.getMessage().contains("owns none of its intended ids"), e.getMessage());
    }

    @Test void aControlWithoutAShardOrAnAggregateIsAUsageError() {
        ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
        Map<String, String> flags = new HashMap<>(Map.of("engine", "flink", "negative-control", "processing-time"));
        int exit = ReplayGate.main("flink", flags, "test", new ReplayGate.GateFiles(ROOT.resolve("gate/flink-assertions.json").toString(),
                ROOT.resolve("gate/flink-shards.json").toString(), ROOT.resolve("gate/flink-negative-controls.json").toString()),
            x -> { throw new AssertionError("no engine is made for a usage error"); },
            new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8));
        assertEquals(2, exit, e.toString(StandardCharsets.UTF_8));
        exit = ReplayGate.main("flink", new HashMap<>(Map.of("engine", "flink", "negative-control", "processing-time", "shard", "inj-stage2")), "test",
            new ReplayGate.GateFiles(ROOT.resolve("gate/flink-assertions.json").toString(), ROOT.resolve("gate/flink-shards.json").toString(),
                ROOT.resolve("gate/flink-negative-controls.json").toString()),
            x -> { throw new AssertionError("no engine"); }, new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8));
        assertEquals(2, exit, "processing-time runs on inj-baseline only");
    }

    @Test void theEngineArtifactDigestIsTheSortedNameAndDigestListOfTheEnginePath() throws Exception {
        Path d = Files.createDirectories(tmp.resolve("engine"));
        Files.writeString(d.resolve("b.jar"), "B");
        Files.writeString(d.resolve("a.jar"), "A");
        Files.createDirectories(d.resolve("sub"));
        ArrayNode want = Json.JSON.createArrayNode();
        want.addArray().add("a.jar").add(yggdrasil.forseti.core.canon.Canonical.digestOf("A".getBytes(StandardCharsets.UTF_8)));
        want.addArray().add("b.jar").add(yggdrasil.forseti.core.canon.Canonical.digestOf("B".getBytes(StandardCharsets.UTF_8)));
        String first = EngineContract.engineArtifactSha256(d);
        assertEquals(yggdrasil.forseti.core.canon.Canonical.digest(want), first);
        Files.writeString(d.resolve("a.jar"), "A2");
        assertNotEquals(first, EngineContract.engineArtifactSha256(d), "a changed jar changes the digest");
        assertThrows(IllegalArgumentException.class, () -> EngineContract.engineArtifactSha256(tmp.resolve("absent")));
    }
}
