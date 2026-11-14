package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.release.*;
import yggdrasil.forseti.core.rules.RuleSet;

/** The live fixtures of step 5 (plan PR 3): the placement each published stream gives its five devices, the routing files committed
 *  for both placements, the placement rules (distinct modulo the subscription count, n distinct split owners), and the lock that pins
 *  every imported byte to its upstream commit. */
class PlacementTest {
    static final Path FIXTURES = Path.of("..", "fixtures"), LIVE = FIXTURES.resolve("live"), REGISTRY = LIVE.resolve("registry");
    static final String[] STREAMS = {"line2-dedicated", "line2-dedicated-plc", "line2-shared", "line2-shared-plc"};

    static Map<String, Integer> placed(String site, int station1, int nutrunner1, int nutrunner2, int station2, int station3) {
        Map<String, Integer> m = new TreeMap<>();
        m.put(site + ":line2:station1", station1);
        m.put(site + ":line2:station1:nutrunner1", nutrunner1);
        m.put(site + ":line2:station1:nutrunner2", nutrunner2);
        m.put(site + ":line2:station2", station2);
        m.put(site + ":line2:station3", station3);
        return m;
    }

    static Map<String, Integer> rawPartitions(RoutingManifest r) {
        Map<String, Integer> m = new TreeMap<>();
        for (String d : r.devices()) m.put(d, r.rawPartitionOf(d));
        return m;
    }

    static RoutingManifest routing(String stream) { return RoutingManifest.read(Placement.routing(Placement.read(REGISTRY, stream))); }

    @Test void dedicatedPutsEveryDeviceOnItsOwnPartitionDistinctModuloFive() {
        for (String s : new String[] {"line2-dedicated", "line2-dedicated-plc"}) {
            RoutingManifest r = routing(s);
            assertEquals(placed("site-071", 5, 1, 3, 4, 2), rawPartitions(r), s);
            assertEquals(List.of(1, 2, 3, 4, 5), r.subscribed("ot.messages"), s);
            assertEquals(List.of(1, 2, 3, 4, 5), r.subscribed("ot.sessions"), s);
            Set<Integer> residues = new TreeSet<>();
            for (int p : r.subscribed("ot.messages")) residues.add(p % 5);
            assertEquals(Set.of(0, 1, 2, 3, 4), residues, s);
            assertEquals(List.of(), Placement.problems(r, "dedicated"), s);
            assertFalse(Placement.problems(r, "shared").isEmpty(), s + " is no shared placement");
        }
    }

    @Test void sharedPutsStation2AndStation3AloneOnOnePartitionDistinctModuloFour() {
        for (String s : new String[] {"line2-shared", "line2-shared-plc"}) {
            RoutingManifest r = routing(s);
            assertEquals(placed("site-019", 2, 3, 5, 0, 0), rawPartitions(r), s);
            assertEquals(List.of(0, 2, 3, 5), r.subscribed("ot.messages"), s);
            Set<Integer> residues = new TreeSet<>();
            for (int p : r.subscribed("ot.messages")) residues.add(p % 4);
            assertEquals(Set.of(0, 1, 2, 3), residues, s);
            assertEquals(List.of(), Placement.problems(r, "shared"), s);
            assertFalse(Placement.problems(r, "dedicated").isEmpty(), s + " is no dedicated placement");
        }
    }

    @Test void splitOwnersAreNDistinctOnBothRawTopics() {
        for (String s : STREAMS) {
            RoutingManifest r = routing(s);
            int n = r.subscribed("ot.messages").size();
            for (String t : Placement.RAW_TOPICS) {
                Set<Integer> owners = new TreeSet<>();
                for (int p : r.subscribed(t)) owners.add((((t.hashCode() * 31) & 0x7FFFFFFF) % n + p) % n);
                assertEquals(n, owners.size(), s + " " + t);
                for (int p : r.subscribed(t)) assertEquals((((t.hashCode() * 31) & 0x7FFFFFFF) % n + p) % n, Placement.owner(t, p, n));
            }
        }
    }

    @Test void theSmokePlacementFailsTheModuloRule() {
        RoutingManifest smoke = RoutingManifest.read(Json.read(FIXTURES.resolve("recordings/smoke/routing.json")));
        assertEquals(List.of(1, 2, 4, 5), smoke.subscribed("ot.messages"));
        List<String> p = Placement.problems(smoke, "any");
        assertTrue(p.stream().anyMatch(x -> x.equals("partitions 1 and 5 share residue 1 modulo 4")), p.toString());
        assertTrue(p.stream().anyMatch(x -> x.startsWith("ot.messages: split owners")), p.toString());
        assertTrue(p.stream().anyMatch(x -> x.startsWith("ot.sessions: split owners")), p.toString());
    }

    @Test void theOwnerFormulaClearsTheSignBitOfTheProduct() {
        String t = "ot.messages";
        int h = t.hashCode() * 31;
        assertEquals(((h & 0x7FFFFFFF) % 5 + 3) % 5, Placement.owner(t, 3, 5));
        assertThrows(IllegalArgumentException.class, () -> Placement.owner(t, 0, 0));
        // A topic whose product is negative: Math.abs would give another owner when the sign bit is not simply cleared.
        String neg = Stream.iterate(0, i -> i + 1).map(i -> "t" + i).filter(x -> x.hashCode() * 31 < 0).findFirst().orElseThrow();
        assertTrue(Placement.owner(neg, 0, 7) >= 0);
    }

    @Test void theCommittedRoutingFilesAreTheComputedOnes() throws IOException {
        for (String name : Placement.FIXTURES.keySet()) {
            ObjectNode computed = Placement.fixture(REGISTRY, name);
            Path file = LIVE.resolve("routing").resolve(name + ".json");
            assertArrayEquals(Json.pretty(computed), Files.readAllBytes(file), file + " differs from the placement the registry gives");
            RoutingManifest r = RoutingManifest.read(Json.read(file));
            assertEquals(List.of(), Placement.problems(r, name), name);
            assertEquals(List.of(0, 1, 2), r.subscribed("mfg.occupancy"), name);
            assertEquals(List.of(0, 1, 2, 3, 4, 5), r.subscribed("quality.incidents"), name);
        }
    }

    /** A routing file in the recording format with these raw partitions per device (stations a, b, station3; tools under a). */
    static RoutingManifest manual(Map<String, Integer> raw) {
        ObjectNode n = Json.object();
        n.put("partitions", 6);
        ObjectNode st = n.putObject("stations");
        st.put("a", 0); st.put("b", 1); st.put("station3", 2);
        ObjectNode dv = n.putObject("devices");
        TreeSet<Integer> parts = new TreeSet<>(raw.values());
        new TreeMap<>(raw).forEach((d, p) -> { ObjectNode o = dv.putObject(d); o.put("station", d.split(":")[0]); o.put("rawPartition", p); });
        ObjectNode sub = n.putObject("subscribed");
        parts.forEach(p -> { sub.withArray("ot.messages").add(p); sub.withArray("ot.sessions").add(p); });
        n.put("routingDigest", yggdrasil.forseti.core.canon.Canonical.digest(n.deepCopy()));
        return RoutingManifest.read(n);
    }

    @Test void theRulesRefuseCollisionsToolsOnTheSharedPartitionAndUnknownFixtures() {
        assertThrows(IllegalArgumentException.class, () -> Placement.fixture(REGISTRY, "smoke"));
        assertThrows(IllegalArgumentException.class, () -> Placement.problems(manual(Map.of("a", 0)), "other"));
        // Two stations on one partition is the shared rule, never the dedicated one.
        RoutingManifest shared = manual(Map.of("a", 1, "a:t", 2, "b", 0, "station3", 0));
        assertEquals(List.of(), Placement.problems(shared, "shared"));
        assertTrue(Placement.problems(shared, "dedicated").get(0).startsWith("dedicated: 4 devices on 3 partitions"));
        // A tool sharing with its station, or three devices on one partition, is no shared placement.
        RoutingManifest tool = manual(Map.of("a", 1, "a:t", 1, "b", 0, "station3", 2));
        assertTrue(Placement.problems(tool, "shared").contains("shared: a:t on the shared partition is not a station"), Placement.problems(tool, "shared").toString());
        RoutingManifest three = manual(Map.of("a", 1, "b", 1, "station3", 1));
        assertTrue(Placement.problems(three, "shared").get(0).startsWith("shared: expected exactly one partition carrying two devices"));
        // Residues collide modulo n although the partitions differ.
        RoutingManifest residue = manual(Map.of("a", 1, "b", 4, "station3", 5));
        assertTrue(Placement.problems(residue, "dedicated").contains("partitions 1 and 4 share residue 1 modulo 3"), Placement.problems(residue, "dedicated").toString());
    }

    @Test void station3IsTheStarvedStationInBothFixtures() {
        assertEquals("station3", Placement.STARVED);
        // shared: station3 alone (the shared partition holds station a and station b) is refused, as is station3 with a tool.
        RoutingManifest notStarved = manual(Map.of("a", 0, "b", 0, "station3", 2));
        assertTrue(Placement.problems(notStarved, "shared").contains("shared: station3 must share its partition with exactly one other station, found []"),
            Placement.problems(notStarved, "shared").toString());
        RoutingManifest withTool = manual(Map.of("a", 0, "a:t", 2, "b", 1, "station3", 2));
        assertTrue(Placement.problems(withTool, "shared").contains("shared: station3 must share its partition with exactly one other station, found [a:t]"),
            Placement.problems(withTool, "shared").toString());
        // dedicated: station3 sharing its partition is refused.
        RoutingManifest sharedInDedicated = manual(Map.of("a", 0, "b", 2, "station3", 2));
        assertTrue(Placement.problems(sharedInDedicated, "dedicated").contains("dedicated: station3 shares its partition with [b]"),
            Placement.problems(sharedInDedicated, "dedicated").toString());
        // A placement without a station3 meets neither kind.
        RoutingManifest none = RoutingManifest.read(RoutingManifest.build(6, List.of("p", "q"), new TreeMap<>(Map.of("p", "p", "q", "q"))));
        assertTrue(Placement.problems(none, "dedicated").contains("dedicated: no station station3"), Placement.problems(none, "dedicated").toString());
        // The committed fixtures meet it.
        assertEquals(List.of(), Placement.problems(routing("line2-dedicated"), "dedicated"));
        assertEquals(List.of(), Placement.problems(routing("line2-shared"), "shared"));
    }

    @Test void theSearchFindsTyrsSiteIds() {
        assertEquals("site-071", Placement.search("dedicated"));
        assertEquals("site-019", Placement.search("shared"));
        assertThrows(IllegalArgumentException.class, () -> Placement.search("any"));
        // The searched layout is the published one: same devices, same routing.
        assertEquals(Placement.routing(Placement.read(REGISTRY, "line2-dedicated")), Placement.routing(Placement.line2("site-071")));
        assertEquals(Placement.routing(Placement.read(REGISTRY, "line2-shared")), Placement.routing(Placement.line2("site-019")));
    }

    @Test void mainWritesTheTwoRoutingFiles(@TempDir Path out) throws IOException {
        Placement.main(new String[] {REGISTRY.toString(), out.toString()});
        for (String name : Placement.FIXTURES.keySet())
            assertArrayEquals(Files.readAllBytes(LIVE.resolve("routing").resolve(name + ".json")), Files.readAllBytes(out.resolve(name + ".json")), name);
    }

    // ------------------------------------------------------------------------------------------------ the governed bytes and their pins

    static String sha(byte[] b) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b));
    }

    static Map<String, String> inventory(Path root) throws Exception {
        Map<String, String> m = new TreeMap<>();
        try (Stream<Path> s = Files.walk(root)) {
            for (Path f : s.filter(Files::isRegularFile).sorted().toList()) m.put(root.relativize(f).toString().replace('\\', '/'), sha(Files.readAllBytes(f)));
        }
        return m;
    }

    static Map<String, String> pinned(JsonNode lock, String field) {
        Map<String, String> m = new TreeMap<>();
        lock.path(field).fields().forEachRemaining(e -> m.put(e.getKey(), e.getValue().asText()));
        return m;
    }

    @Test void theLockPinsTheUpstreamCommitsAndEveryImportedByte() throws Exception {
        JsonNode lock = Json.read(LIVE.resolve("lock.json"));
        assertEquals("a113ef8f104004aae53de943a4b035e6870448db", Json.text(lock, "tyr"));
        assertEquals("a405cb60ff032f5d870b35106fe2dbbca78c1552", Json.text(lock, "bifrost"));
        JsonNode step3 = Json.read(FIXTURES.resolve("registry.lock.json"));
        assertEquals(Json.text(step3, "occupancySchemaSha256"), Json.text(lock, "occupancySchemaSha256"));
        assertEquals(Json.text(step3, "incidentSchemaSha256"), Json.text(lock, "incidentSchemaSha256"));
        assertEquals(sha(Files.readAllBytes(Path.of("..", "schemas", "occupancy-1.0.0.schema.json"))), Json.text(lock, "occupancySchemaSha256"));
        for (String[] f : new String[][] {{"upstream", "216"}, {"definitions", "4"}, {"registry", "242"}}) {
            Map<String, String> want = pinned(lock, f[0]);
            assertEquals(Integer.parseInt(f[1]), want.size(), f[0] + " entries");
            assertEquals(want, inventory(LIVE.resolve(f[0])), f[0] + " bytes vs lock.json");
        }
    }

    @Test void theDefinitionsAreBifrostsPublishedPairs() throws Exception {
        Map<String, String> content = Map.of("station-model", "4d40df048267814f409ff93e16cb838cb0e5be710fbde83a39001f96c39efbac",
                                             "tool-model", "276bf45c3f834818d359442395c40f2932af7436c81a4b72d20a8ff218c9a4dc");
        for (var e : content.entrySet()) {
            Path pair = LIVE.resolve("definitions/recipe").resolve(e.getKey()).resolve("1.0.0");
            JsonNode m = Json.read(pair.resolve("manifest.json"));
            assertEquals(List.of("recipe-setpoints", e.getKey(), "1.0.0", e.getValue()),
                List.of(Json.text(m, "kind"), Json.text(m, "ref"), Json.text(m, "version"), Json.text(m, "contentSha256")));
            assertEquals(e.getValue(), sha(Files.readAllBytes(pair.resolve("recipe-setpoints.yaml"))), e.getKey());
        }
    }

    @Test void everyReleasePublishesTyrsBundleByteForByte() throws Exception {
        Map<String, String> upstream = pinned(Json.read(LIVE.resolve("lock.json")), "upstream");
        Map<String, String> variant = Map.of("line2-dedicated", "dedicated", "line2-dedicated-plc", "dedicated-plc", "line2-shared", "shared", "line2-shared-plc", "shared-plc");
        for (String s : STREAMS) {
            JsonNode head = Json.read(REGISTRY.resolve("mappings/heads").resolve(s + ".json"));
            assertEquals("line2-rules-" + s.substring("line2-".length()), Json.text(head, "id"), s);
            assertEquals("1.0.0", Json.text(head, "version"), s);
            Path dir = REGISTRY.resolve("mappings/releases").resolve(Json.text(head, "id")).resolve("1.0.0");
            Set<String> payload = new TreeSet<>();
            for (JsonNode f : Json.read(dir.resolve("release.json")).path("files")) {
                String path = Json.text(f, "path"), rel = path.substring("payload/".length()), key = variant.get(s) + "/" + rel;
                assertTrue(path.startsWith("payload/"), path);
                payload.add(key);
                assertEquals(upstream.get(key), Json.text(f, "sha256"), s + " " + path + " vs Tyr's");
                assertEquals(Json.text(f, "sha256"), sha(Files.readAllBytes(dir.resolve(path))), s + " " + path + " bytes");
            }
            Set<String> bundle = new TreeSet<>();
            for (String k : upstream.keySet()) if (k.startsWith(variant.get(s) + "/")) bundle.add(k);
            assertEquals(bundle, payload, s + " payload set vs Tyr's bundle");
        }
    }

    @Test void everyHeadVerifiesUnderTheLiveKeysAndItsRuleLoads(@TempDir Path state) throws Exception {
        Path m = REGISTRY.resolve("mappings");
        TrustedKeys keys = TrustedKeys.load(LIVE.resolve("trusted-keys.jsonl"));
        for (String s : STREAMS) {
            JsonNode env = Json.read(m.resolve("head-attestations").resolve(s + ".dsse.json"));
            JsonNode statement = Json.read(Base64.getDecoder().decode(env.path("payload").asText()));
            long now = Instant.parse(statement.path("predicate").path("signedAt").asText()).toEpochMilli() + 60_000;
            HeadVerifier.Head h = HeadVerifier.verify(m, s, keys, new FloorStore(state.resolve(s)), now);
            RuleSet rules = RuleSet.load(ReleaseReader.read(m, h), "line2-attribution", "1.0.0", Json.object());
            assertNotNull(rules, s);
            // The step-3 keys do not vouch for the live registry: a registry cannot borrow another's trust.
            TrustedKeys step3 = TrustedKeys.load(FIXTURES.resolve("trusted-keys.jsonl"));
            assertThrows(Refusal.class, () -> HeadVerifier.verify(m, s, step3, new FloorStore(state.resolve(s + "-step3")), now), s);
        }
    }
}
