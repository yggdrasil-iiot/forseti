package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.generation.FileGenerationStore;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.release.*;
import yggdrasil.forseti.core.rules.RuleSet;

/** The live fixtures appended for step 6: the plc-stamped rule with {@code crossCheck none} on its own stream per placement in
 *  {@code fixtures/live/registry}, and {@code fixtures/live/registry-next}, that registry plus rule 1.0.1 ({@code maxMissingPeriods}
 *  4) on {@code line2-shared} and {@code line2-dedicated} at head version 2. They are published under a second key of each
 *  principal; the step-5 releases, heads and attestations stay byte-identical and verify under the step-5 keys. */
class LiveNextTest {
    static final Path LIVE = PlacementTest.LIVE, REGISTRY = PlacementTest.REGISTRY, NEXT = LIVE.resolve("registry-next");
    static final String RULE = "line2-attribution";
    /** The step-5 heads (release sha256) as forseti main cf23449 committed them. */
    static final Map<String, String> STEP5_HEADS = Map.of(
        "line2-dedicated", "ceac77f9fc45d6e2647f4cf0ca98928b4af427af8b59c03d1687882f04bcc607",
        "line2-dedicated-plc", "c3d1784325791c1fed8c113779e787900280133bd7f6eb63645e9c1ea5a847dc",
        "line2-shared", "627f3c4f268561ce882398fddf7bfe0e0333a25aa81bfcc79208710e4cca3c77",
        "line2-shared-plc", "ec161cfc67a6693aaa18d4cc0b7eabbdd91ae3c9dbabf667c48d39ce3b4e83d3");
    /** Stream -> the Tyr placement bundle its release derives from. */
    static final Map<String, String> NOCHECK = Map.of("line2-dedicated-plc-nocheck", "dedicated-plc", "line2-shared-plc-nocheck", "shared-plc");
    static final Map<String, String> BUMPED = Map.of("line2-dedicated", "dedicated", "line2-shared", "shared");
    static final Set<String> DERIVED_FILES = Set.of("attribution-rules/0.json", "impact-rules/0.json", "proposal.json", "submission.json");
    /** The streams appended after registry-next was written (injection 24's starvation releases, {@link LiveStarvationTest}): only in
     *  registry, published under a third key of each principal. */
    static final Set<String> STARVATION = LiveStarvationTest.STREAMS.keySet();
    static final Set<String> IDENTITY = Set.of("identity/activation-policy.json", "identity/authorized-keys.jsonl");

    static long signedAt(Path mappings, String stream) {
        JsonNode env = Json.read(mappings.resolve("head-attestations").resolve(stream + ".dsse.json"));
        JsonNode statement = Json.read(Base64.getDecoder().decode(env.path("payload").asText()));
        return Instant.parse(statement.path("predicate").path("signedAt").asText()).toEpochMilli();
    }

    static List<String> streams(Path registry) throws Exception {
        List<String> s = new ArrayList<>();
        try (DirectoryStream<Path> d = Files.newDirectoryStream(registry.resolve("mappings/heads"), "*.json")) {
            for (Path f : d) s.add(f.getFileName().toString().replaceFirst("\\.json$", ""));
        }
        Collections.sort(s);
        return s;
    }

    static TrustedKeys keys(Path dir, String name, List<String> lines) throws Exception {
        Path f = dir.resolve(name);
        Files.write(f, (String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8));
        return TrustedKeys.load(f);
    }

    @Test void theStep5HeadsAreUnchangedAndRegistryNextOnlyAdvancesTheBumpedStreams() throws Exception {
        for (var e : STEP5_HEADS.entrySet())
            assertEquals(e.getValue(), Json.text(Json.read(REGISTRY.resolve("mappings/heads").resolve(e.getKey() + ".json")), "sha256"), e.getKey());
        List<String> step6 = List.of("line2-dedicated", "line2-dedicated-plc", "line2-dedicated-plc-nocheck", "line2-shared", "line2-shared-plc", "line2-shared-plc-nocheck");
        assertEquals(step6, streams(NEXT));
        List<String> all = new ArrayList<>(step6);
        all.addAll(STARVATION);
        Collections.sort(all);
        assertEquals(all, streams(REGISTRY));
        // Every registry file is in registry-next byte for byte, but the bumped streams' heads, head attestations and anchors (their
        // anchors only grow), the starvation streams' files (registry only) and the two identity files (registry-next's plus the
        // starvation streams' rules and keys, appended).
        Map<String, String> reg = PlacementTest.inventory(REGISTRY), next = PlacementTest.inventory(NEXT);
        Set<String> moved = new TreeSet<>();
        for (String s : BUMPED.keySet())
            moved.addAll(List.of("mappings/heads/" + s + ".json", "mappings/head-attestations/" + s + ".dsse.json", "anchor/mapping-head-" + s + ".anchor.jsonl"));
        for (var e : reg.entrySet())
            if (LiveStarvationTest.ofStarvation(e.getKey())) assertNull(next.get(e.getKey()), e.getKey());
            else if (moved.contains(e.getKey()) || IDENTITY.contains(e.getKey())) assertNotEquals(e.getValue(), next.get(e.getKey()), e.getKey());
            else assertEquals(e.getValue(), next.get(e.getKey()), e.getKey());
        String keys = Files.readString(REGISTRY.resolve("identity/authorized-keys.jsonl")), nextKeys = Files.readString(NEXT.resolve("identity/authorized-keys.jsonl"));
        assertTrue(keys.startsWith(nextKeys) && keys.length() > nextKeys.length(), "the authorized keys are appended to");
        JsonNode policy = Json.read(REGISTRY.resolve("identity/activation-policy.json")), nextPolicy = Json.read(NEXT.resolve("identity/activation-policy.json"));
        for (int i = 0; i < nextPolicy.path("rules").size(); i++) assertEquals(nextPolicy.path("rules").get(i), policy.path("rules").get(i), "policy rule " + i);
        assertEquals(nextPolicy.path("rules").size() + 2 * STARVATION.size(), policy.path("rules").size());
        for (String s : BUMPED.keySet()) {
            String a = Files.readString(REGISTRY.resolve("anchor/mapping-head-" + s + ".anchor.jsonl")), b = Files.readString(NEXT.resolve("anchor/mapping-head-" + s + ".anchor.jsonl"));
            assertTrue(b.startsWith(a) && b.length() > a.length(), s + " anchor is appended to");
            JsonNode head = Json.read(NEXT.resolve("mappings/heads").resolve(s + ".json"));
            assertEquals(List.of("line2-rules-" + s.substring("line2-".length()), "1.0.1"), List.of(Json.text(head, "id"), Json.text(head, "version")), s);
        }
    }

    @Test void theDerivedReleasesDifferFromTyrsBundleInTheRuleAndItsPinsOnly() throws Exception {
        Path up = LIVE.resolve("upstream");
        Map<String, Path> payloads = new TreeMap<>();
        NOCHECK.forEach((s, v) -> payloads.put(s, REGISTRY.resolve("mappings/releases/line2-rules-" + s.substring("line2-".length()) + "/1.0.0/payload")));
        BUMPED.forEach((s, v) -> payloads.put(s, NEXT.resolve("mappings/releases/line2-rules-" + s.substring("line2-".length()) + "/1.0.1/payload")));
        for (var e : payloads.entrySet()) {
            String variant = NOCHECK.containsKey(e.getKey()) ? NOCHECK.get(e.getKey()) : BUMPED.get(e.getKey());
            Map<String, String> derived = PlacementTest.inventory(e.getValue()), tyr = PlacementTest.inventory(up.resolve(variant));
            assertEquals(tyr.keySet(), derived.keySet(), e.getKey());
            Set<String> differ = new TreeSet<>();
            for (String k : tyr.keySet()) if (!tyr.get(k).equals(derived.get(k))) differ.add(k);
            assertEquals(DERIVED_FILES, differ, e.getKey());
            JsonNode rule = Json.read(e.getValue().resolve("attribution-rules/0.json")), base = Json.read(up.resolve(variant).resolve("attribution-rules/0.json"));
            JsonNode impact = Json.read(e.getValue().resolve("impact-rules/0.json")), submission = Json.read(e.getValue().resolve("submission.json"));
            assertEquals("1.0.0", Json.text(base, "version"), e.getKey());
            assertEquals("1.0.1", Json.text(rule, "version"), e.getKey());
            assertEquals("1.0.1", Json.text(impact.path("attributionRuleRef"), "version"), e.getKey());
            assertEquals(e.getKey(), Json.text(submission, "streamId"));
            if (NOCHECK.containsKey(e.getKey())) {
                assertEquals("plc-stamped", Json.text(rule.path("identity"), "mode"), e.getKey());
                assertEquals("occupancy", Json.text(base.path("identity"), "crossCheck"), e.getKey());
                assertEquals("none", Json.text(rule.path("identity"), "crossCheck"), e.getKey());
                assertEquals(base.path("coverage"), rule.path("coverage"), e.getKey());
                assertTrue(submission.path("baseRelease").isNull(), e.getKey());
            } else {
                assertEquals(3, base.path("coverage").path("maxMissingPeriods").asInt(), e.getKey());
                assertEquals(4, rule.path("coverage").path("maxMissingPeriods").asInt(), e.getKey());
                assertEquals(base.path("identity"), rule.path("identity"), e.getKey());
                // The base is the exact head the release advanced from.
                JsonNode head = Json.read(REGISTRY.resolve("mappings/heads").resolve(e.getKey() + ".json"));
                assertEquals(head, submission.path("baseRelease"), e.getKey());
                assertEquals(head, Json.read(e.getValue().resolve("proposal.json")).path("baseRelease"), e.getKey());
            }
        }
    }

    @Test void everyHeadVerifiesAtItsOwnClockUnderTheKeysThatSignedIt(@TempDir Path tmp) throws Exception {
        List<String> lines = Files.readAllLines(LIVE.resolve("trusted-keys.jsonl"));
        assertEquals(6, lines.size(), "the step-5, step-6 and starvation keys");
        assertEquals(lines, Files.readAllLines(REGISTRY.resolve("identity/authorized-keys.jsonl")));
        TrustedKeys all = TrustedKeys.load(LIVE.resolve("trusted-keys.jsonl")), old = keys(tmp, "step5.jsonl", lines.subList(0, 2)),
            added = keys(tmp, "added.jsonl", lines.subList(2, 4));
        int n = 0;
        for (Path registry : List.of(REGISTRY, NEXT)) {
            Path m = registry.resolve("mappings");
            for (String s : streams(registry)) {
                if (STARVATION.contains(s)) continue;   // LiveStarvationTest
                long now = signedAt(m, s) + 60_000;
                boolean bumped = registry == NEXT && BUMPED.containsKey(s);
                HeadVerifier.Head h = HeadVerifier.verify(m, s, all, new FloorStore(tmp.resolve("f" + n++)), now);
                assertEquals(bumped ? 2 : 1, h.headVersion(), registry + " " + s);
                String version = bumped || NOCHECK.containsKey(s) ? "1.0.1" : "1.0.0";
                assertNotNull(RuleSet.load(ReleaseReader.read(m, h), RULE, version, Json.object()), registry + " " + s);
                // A step-5 head verifies under the step-5 keys alone; an appended head only under the added keys.
                boolean step5 = STEP5_HEADS.containsKey(s) && !bumped;
                HeadVerifier.verify(m, s, step5 ? old : added, new FloorStore(tmp.resolve("f" + n++)), now);
                assertThrows(Refusal.class, () -> HeadVerifier.verify(m, s, step5 ? added : old, new FloorStore(tmp.resolve("x" + s + registry.getFileName())), now), registry + " " + s);
            }
        }
    }

    @Test void oneClockForSeveralStreamsIsTheLatestSignedAtPlus60Seconds(@TempDir Path tmp) throws Exception {
        TrustedKeys all = TrustedKeys.load(LIVE.resolve("trusted-keys.jsonl"));
        Path m = NEXT.resolve("mappings");
        long latest = 0;
        for (String s : streams(NEXT)) latest = Math.max(latest, signedAt(m, s));   // registry-next has no starvation stream
        int n = 0;
        for (String s : streams(NEXT)) HeadVerifier.verify(m, s, all, new FloorStore(tmp.resolve("f" + n++)), latest + 60_000);
        // A step-5 head's clock is before every appended head: one stream's signedAt + 60 s would refuse them as signed in the future.
        long step5 = signedAt(m, "line2-shared-plc") + 60_000;
        Refusal r = assertThrows(Refusal.class, () -> HeadVerifier.verify(m, "line2-shared", all, new FloorStore(tmp.resolve("future")), step5));
        assertEquals("head-unverified", r.reason);
        assertEquals("signed-in-future", r.evidence.get("rule"));
    }

    record Out(int exit, String stdout, String stderr) {}

    static Out generation(Path state, Path registry, String stream, String rule, long nowMs) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = GenerationCli.run(new String[] {"new", "--state", state.toString(), "--mappings", registry.resolve("mappings").toString(),
                "--trusted-keys", LIVE.resolve("trusted-keys.jsonl").toString(), "--stream", stream, "--rule", rule},
            new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8), Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC));
        return new Out(exit, so.toString(StandardCharsets.UTF_8).strip(), se.toString(StandardCharsets.UTF_8).strip());
    }

    /** The live analog of F-GEN-FLOOR on each placement: {@code new} on registry (generation 1, floor 1), on registry-next
     *  (generation 2, floor 2), on registry again: {@code head-rollback}, no third row, the floor stays 2. */
    @Test void theRuleChangeGenerationAdvancesTheFloorAndARollbackIsRefused(@TempDir Path tmp) throws Exception {
        for (String s : BUMPED.keySet()) {
            Path state = Files.createDirectories(tmp.resolve(s));
            long v1 = signedAt(REGISTRY.resolve("mappings"), s) + 60_000, v2 = signedAt(NEXT.resolve("mappings"), s) + 60_000;
            Out a = generation(state, REGISTRY, s, RULE + "@1.0.0", v1);
            assertEquals(0, a.exit(), s + " " + a.stdout() + " " + a.stderr());
            assertEquals(1L, Json.read(a.stdout().getBytes(StandardCharsets.UTF_8)).path("generation").asLong(), s + " " + a.stdout());
            assertEquals(1L, new FloorStore(state).get(s).map(FloorStore.Floor::headVersion).orElse(null), s);
            Out b = generation(state, NEXT, s, RULE + "@1.0.1", v2);
            assertEquals(0, b.exit(), s + " " + b.stdout());
            assertEquals(2L, Json.read(b.stdout().getBytes(StandardCharsets.UTF_8)).path("generation").asLong(), s + " " + b.stdout());
            assertEquals(2L, new FloorStore(state).get(s).map(FloorStore.Floor::headVersion).orElse(null), s);
            Out c = generation(state, REGISTRY, s, RULE + "@1.0.0", v1);
            assertEquals(1, c.exit(), s + " " + c.stdout());
            assertEquals("head-rollback", Json.read(c.stdout().getBytes(StandardCharsets.UTF_8)).path("refusal").asText(null), s);
            assertEquals(2, Files.readAllLines(state.resolve(FileGenerationStore.FILE)).size(), s + " generation rows");
            assertEquals(2L, new FloorStore(state).get(s).map(FloorStore.Floor::headVersion).orElse(null), s);
        }
    }
}
