package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.release.*;
import yggdrasil.forseti.core.rules.RuleSet;

/** Injection 24's releases appended to {@code fixtures/live/registry} (step 6, spec 681; {@code make_registry_fixture.py
 *  --live-starvation}): Tyr's two starvation bundles, the dedicated and the shared placement bundle with the starved station3 in the
 *  impact rule's {@code incidentScope}, published byte for byte on their own streams under a third key of each principal. The placement
 *  is the placement bundle's; every earlier release is untouched ({@link LiveNextTest}). */
class LiveStarvationTest {
    static final Path LIVE = PlacementTest.LIVE, REGISTRY = PlacementTest.REGISTRY;
    /** Stream -> {Tyr variant, the placement variant it derives from, the site}. */
    static final Map<String, String[]> STREAMS = new TreeMap<>(Map.of(
        "line2-dedicated-starvation", new String[] {"dedicated-starvation", "dedicated", "site-071"},
        "line2-shared-starvation", new String[] {"shared-starvation", "shared", "site-019"}));

    /** Whether a registry path belongs to a starvation stream or release. */
    static boolean ofStarvation(String path) { return path.contains("-starvation"); }

    @Test void everyStarvationReleasePublishesTyrsBundleByteForByte() throws Exception {
        Map<String, String> upstream = PlacementTest.pinned(Json.read(LIVE.resolve("lock.json")), "upstream");
        for (var e : STREAMS.entrySet()) {
            String s = e.getKey(), variant = e.getValue()[0];
            JsonNode head = Json.read(REGISTRY.resolve("mappings/heads").resolve(s + ".json"));
            assertEquals("line2-rules-" + s.substring("line2-".length()), Json.text(head, "id"), s);
            assertEquals("1.0.0", Json.text(head, "version"), s);
            Path dir = REGISTRY.resolve("mappings/releases").resolve(Json.text(head, "id")).resolve("1.0.0");
            Set<String> payload = new TreeSet<>(), bundle = new TreeSet<>();
            for (JsonNode f : Json.read(dir.resolve("release.json")).path("files")) {
                String path = Json.text(f, "path"), key = variant + "/" + path.substring("payload/".length());
                payload.add(key);
                assertEquals(upstream.get(key), Json.text(f, "sha256"), s + " " + path + " vs Tyr's");
                assertEquals(Json.text(f, "sha256"), PlacementTest.sha(Files.readAllBytes(dir.resolve(path))), s + " " + path + " bytes");
            }
            for (String k : upstream.keySet()) if (k.startsWith(variant + "/")) bundle.add(k);
            assertEquals(bundle, payload, s + " payload set vs Tyr's bundle");
        }
    }

    @Test void theStarvationBundleIsThePlacementBundleWithStation3InTheIncidentScope() throws Exception {
        Path up = LIVE.resolve("upstream");
        for (var e : STREAMS.entrySet()) {
            String[] v = e.getValue();
            Map<String, String> starved = PlacementTest.inventory(up.resolve(v[0])), placed = PlacementTest.inventory(up.resolve(v[1]));
            assertEquals(placed.keySet(), starved.keySet(), e.getKey());
            Set<String> differ = new TreeSet<>();
            for (String k : placed.keySet()) if (!placed.get(k).equals(starved.get(k))) differ.add(k);
            assertEquals(Set.of("impact-rules/0.json", "proposal.json", "submission.json"), differ, e.getKey());
            JsonNode a = Json.read(up.resolve(v[1]).resolve("impact-rules/0.json")), b = Json.read(up.resolve(v[0]).resolve("impact-rules/0.json"));
            List<String> scope = new ArrayList<>();
            b.path("incidentScope").forEach(x -> scope.add(x.asText()));
            assertEquals(List.of(v[2] + ":line2:station1:nutrunner1", v[2] + ":line2:station1:nutrunner2", v[2] + ":line2:station3"), scope, e.getKey());
            ((com.fasterxml.jackson.databind.node.ObjectNode) b).set("incidentScope", a.get("incidentScope"));
            assertEquals(a, b, e.getKey() + ": nothing but the incident scope");
            assertEquals(e.getKey(), Json.text(Json.read(up.resolve(v[0]).resolve("submission.json")), "streamId"));
            // the placement is the placement stream's
            RoutingManifest r = PlacementTest.routing(e.getKey()), base = PlacementTest.routing("line2-" + v[1]);
            assertEquals(PlacementTest.rawPartitions(base), PlacementTest.rawPartitions(r), e.getKey());
            assertArrayEquals(Files.readAllBytes(LIVE.resolve("routing").resolve(v[1] + ".json")), Json.pretty(Placement.routing(Placement.read(REGISTRY, e.getKey()))),
                e.getKey() + ": the committed routing of the placement");
        }
    }

    @Test void everyStarvationHeadVerifiesUnderTheAppendedKeysOnlyAndItsRuleLoads(@TempDir Path tmp) throws Exception {
        List<String> lines = Files.readAllLines(LIVE.resolve("trusted-keys.jsonl"));
        assertEquals(6, lines.size());
        TrustedKeys all = TrustedKeys.load(LIVE.resolve("trusted-keys.jsonl")), earlier = LiveNextTest.keys(tmp, "earlier.jsonl", lines.subList(0, 4)),
            added = LiveNextTest.keys(tmp, "added.jsonl", lines.subList(4, 6));
        Path m = REGISTRY.resolve("mappings");
        int n = 0;
        for (String s : STREAMS.keySet()) {
            long now = LiveNextTest.signedAt(m, s) + 60_000;
            HeadVerifier.Head h = HeadVerifier.verify(m, s, all, new FloorStore(tmp.resolve("f" + n++)), now);
            assertEquals(1, h.headVersion(), s);
            RuleSet rules = RuleSet.load(ReleaseReader.read(m, h), "line2-attribution", "1.0.0", Json.object());
            assertNotNull(rules, s);
            HeadVerifier.verify(m, s, added, new FloorStore(tmp.resolve("f" + n++)), now);
            assertThrows(Refusal.class, () -> HeadVerifier.verify(m, s, earlier, new FloorStore(tmp.resolve("x" + s)), now), s);
        }
        // and every earlier head still verifies under the earlier keys alone (LiveNextTest)
        for (String s : PlacementTest.STREAMS) HeadVerifier.verify(m, s, earlier, new FloorStore(tmp.resolve("e" + s)), LiveNextTest.signedAt(m, s) + 60_000);
    }

    @Test void theLockPinsOneTyrCommitAndTheAppendedFiles() throws Exception {
        JsonNode lock = Json.read(LIVE.resolve("lock.json"));
        Map<String, String> upstream = PlacementTest.pinned(lock, "upstream"), registry = PlacementTest.pinned(lock, "registry");
        assertEquals(88, upstream.keySet().stream().filter(k -> k.startsWith("dedicated-starvation/") || k.startsWith("shared-starvation/")).count());
        assertEquals(100, registry.keySet().stream().filter(LiveStarvationTest::ofStarvation).count());
        assertTrue(Json.text(lock, "tyr").matches("[0-9a-f]{40}"));
        assertEquals(new String(Files.readAllBytes(LIVE.resolve("trusted-keys.jsonl")), StandardCharsets.UTF_8), String.join("\n", Files.readAllLines(REGISTRY.resolve("identity/authorized-keys.jsonl"))) + "\n");
    }
}
