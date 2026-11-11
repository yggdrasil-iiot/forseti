package yggdrasil.forseti.cli.recording;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.release.*;
import yggdrasil.forseti.core.rules.MemberCatalog;
import yggdrasil.forseti.core.rules.RuleSet;

class RecordingTest {
    static final Path FIXTURES = Path.of("..", "fixtures");
    static final List<String> SMOKES = List.of("smoke", "smoke-plc", "smoke-incident");
    static final long T0 = 1794182400000L;
    static final String NR1 = "synthetic-site:line2:station1:nutrunner1";

    @TempDir Path tmp;

    static Recording open(String name) { return Recording.open(FIXTURES.resolve("recordings").resolve(name)); }
    static Path scenario(String name) { return FIXTURES.resolve("scenarios").resolve(name + ".json"); }

    static List<Recording.Record> all(Recording r, String topic) {
        List<Recording.Record> l = new ArrayList<>();
        for (int p : r.routing().subscribed(topic)) l.addAll(r.records(topic, p));
        return l;
    }
    static long samples(Recording r) { return all(r, "ot.messages").stream().mapToLong(x -> x.value().path("members").size()).sum(); }

    /** The per-station W1 of the reference harness at a cut: the largest member sourceTs routed to the station through the cut,
     *  minus outOfOrdernessMs, minus 1. */
    static long w1(Recording r, String station, Recording.Batch b, long ooo) {
        long max = Long.MIN_VALUE;
        for (int p : r.routing().subscribed("ot.messages"))
            for (Recording.Record x : r.records("ot.messages", p)) {
                if (x.offset() > b.through("ot.messages", p)) break;
                if (!r.routing().stationOf(x.key()).equals(station)) continue;
                for (JsonNode m : x.value().path("members")) max = Math.max(max, Json.safeLong(m.get("sourceTs"), "sourceTs", Json.NON_NEGATIVE));
            }
        return max - ooo - 1;
    }

    @Test void smokeHasItsCountsContiguousOffsetsAndBracketingCuts() throws IOException {
        Recording r = open("smoke");
        r.verifyDigests();
        r.verifyBatches();
        List<Recording.Record> messages = all(r, "ot.messages");
        assertEquals(4060, messages.size());
        Map<String, Long> perDevice = new TreeMap<>();
        messages.forEach(m -> perDevice.merge(m.key(), 1L, Long::sum));
        assertEquals(5, perDevice.size());
        perDevice.values().forEach(n -> assertEquals(812L, n));
        assertEquals(5684, samples(r));
        assertEquals(5, all(r, "ot.sessions").size());
        assertEquals(2, all(r, "mfg.occupancy").size());
        assertEquals(0, all(r, "quality.incidents").size());
        for (String topic : RoutingManifest.TOPICS)
            for (int p : r.routing().subscribed(topic)) {
                List<Recording.Record> l = r.records(topic, p);
                for (int i = 0; i < l.size(); i++) { assertEquals(i, l.get(i).offset()); assertNull(l.get(i).timestamp()); assertEquals(Json.object(), l.get(i).headers()); }
            }
        assertEquals(T0, r.t0());
        assertEquals(1000, r.scanMs());
        assertEquals(751000, r.tailMs());
        assertEquals(28, r.batches().size());
        for (Recording.Batch b : r.batches()) for (int p = 0; p < 6; p++) assertEquals(-1, b.through("quality.incidents", p));
        // the first process message and the first occupancy line in the shape of plan Step 8.1
        JsonNode first = r.records("ot.messages", r.routing().rawPartitionOf(NR1)).get(0).value();
        assertEquals(2, first.get("seq").longValue());
        assertEquals(T0 + 600000, first.path("members").get(0).get("sourceTs").longValue());
        assertEquals(r.manifest().path("definitions").path(NR1).path("sha256").asText(), first.get("definitionSha256").asText());
        assertEquals("{\"offset\":0,\"timestamp\":null,\"key\":\"synthetic-site:line2:station1\",\"headers\":{},\"value\":{\"eventId\":\"occ-1\",\"eventTime\":1794183000000,"
            + "\"recordTime\":1794183000200,\"unitId\":\"UNIT0000000000001\",\"action\":\"OBSERVE\",\"bizStep\":\"urn:epcglobal:cbv:bizstep:arriving\","
            + "\"readPoint\":\"synthetic-site:line2:station1\",\"visitSeq\":1,\"source\":\"mes-line2\"}}", firstLine(r.dir().resolve("mfg.occupancy").resolve("0.jsonl")));
        // one settlement deadline, bracketed by the batch ending at scan 1410000 and the next at 1411000
        assertEquals(1, r.deadlines().size());
        Recording.Deadline d = r.deadlines().get(0);
        assertEquals(new Recording.Deadline("visit-settlement", "visit-1", T0 + 1380000, 27, 28), d);
        assertEquals(T0 + 1380000, r.settleTargetW());
        long ooo = 30000;
        for (String station : r.routing().stations().keySet()) {
            long a = w1(r, station, r.batches().get(26), ooo), b = w1(r, station, r.batches().get(27), ooo);
            assertEquals(T0 + 1379999, a);
            assertEquals(T0 + 1380999, b);
            assertTrue(a < d.at() && d.at() <= b);
        }
        r.evidenceRange().values().forEach(e -> assertEquals(new Recording.Range(T0 + 600000, T0 + 1411000), e));
    }

    @Test void digestsVerifyAndEqualTheLocks() throws IOException {
        Map<String, long[]> counts = Map.of("smoke", new long[] {4060, 5684, 28}, "smoke-plc", new long[] {4060, 10556, 28}, "smoke-incident", new long[] {7760, 10864, 53});
        for (String name : SMOKES) {
            Recording r = open(name);
            r.verifyDigests();
            r.verifyBatches();
            JsonNode lock = Json.read(ScenarioBuilder.lockOf(scenario(name)));
            Map<String, String> fromLock = new TreeMap<>();
            lock.path("files").fields().forEachRemaining(e -> fromLock.put(e.getKey(), e.getValue().asText()));
            Map<String, String> fromManifest = new TreeMap<>(r.digests());
            fromManifest.put(Recording.MANIFEST, Canonical.digestOf(Files.readAllBytes(r.dir().resolve(Recording.MANIFEST))));
            assertEquals(fromLock, fromManifest, name);
            assertEquals(Canonical.digestOf(Files.readAllBytes(scenario(name))), r.manifest().path("source").path("scenarioSha256").asText(), name);
            long[] c = counts.get(name);
            assertEquals(c[0], all(r, "ot.messages").size(), name);
            assertEquals(c[1], samples(r), name);
            assertEquals(c[2], r.batches().size(), name);
            assertEquals(c[0], lock.path("counts").path("messages").longValue(), name);
            assertEquals(c[1], lock.path("counts").path("samples").longValue(), name);
            assertEquals(c[2], lock.path("counts").path("cuts").longValue(), name);
        }
    }

    @Test void routingDigestIsRecomputedAndPlacementIsKafkasDefault() throws IOException {
        for (String name : SMOKES) {
            Recording r = open(name);
            ObjectNode file = (ObjectNode) Json.read(r.dir().resolve(Recording.ROUTING));
            String declared = file.remove("routingDigest").asText();
            assertEquals(Canonical.digest(file), declared, name);
            assertEquals(declared, r.routing().routingDigest(), name);
            assertEquals(List.of("synthetic-site:line2:station1", "synthetic-site:line2:station2", "synthetic-site:line2:station3"), new ArrayList<>(r.routing().stations().keySet()));
            for (String d : r.routing().devices()) assertEquals(Murmur2.partition(d, 6), r.routing().rawPartitionOf(d), d);
            assertEquals(List.of(0, 1, 2), r.routing().subscribed("mfg.occupancy"));
            assertEquals(List.of(0, 1, 2, 3, 4, 5), r.routing().subscribed("quality.incidents"));
        }
        Path copy = copy("smoke");
        ObjectNode routing = (ObjectNode) Json.read(copy.resolve(Recording.ROUTING));
        ((ObjectNode) routing.path("devices").path(NR1)).put("rawPartition", 0);
        Files.write(copy.resolve(Recording.ROUTING), Json.pretty(routing));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Recording.open(copy));
        assertTrue(e.getMessage().contains("routingDigest"), e.getMessage());
    }

    @Test void aWrongDigestAndOutOfRangeOffsetsAreRefused() throws IOException {
        Path wrong = copy("smoke");
        ObjectNode m = (ObjectNode) Json.read(wrong.resolve(Recording.MANIFEST));
        ((ObjectNode) m.path("digests")).put("ot.messages/1.jsonl", "0".repeat(64));
        Files.write(wrong.resolve(Recording.MANIFEST), Json.pretty(m));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Recording.open(wrong).verifyDigests());
        assertTrue(e.getMessage().startsWith("digest mismatch: ot.messages/1.jsonl"), e.getMessage());

        for (String value : List.of("9007199254740992", "-2")) {
            Path bad = copy("smoke");
            String text = Files.readString(bad.resolve(Recording.MANIFEST), StandardCharsets.UTF_8);
            String replaced = text.replaceFirst("\"0\": -1", "\"0\": " + value);   // the first quality.incidents through-offset of cut 1
            assertNotEquals(text, replaced);
            Files.writeString(bad.resolve(Recording.MANIFEST), replaced, StandardCharsets.UTF_8);
            IllegalArgumentException x = assertThrows(IllegalArgumentException.class, () -> Recording.open(bad), value);
            assertTrue(x.getMessage().startsWith("schema-invalid: throughOffsets.quality.incidents.0"), x.getMessage());
        }

        Path badOffset = copy("smoke");
        Path part = badOffset.resolve("mfg.occupancy").resolve("0.jsonl");
        Files.writeString(part, Files.readString(part, StandardCharsets.UTF_8).replaceFirst("\"offset\":0,", "\"offset\":9007199254740992,"), StandardCharsets.UTF_8);
        IllegalArgumentException y = assertThrows(IllegalArgumentException.class, () -> Recording.open(badOffset).records("mfg.occupancy", 0));
        assertEquals("schema-invalid: offset", y.getMessage());

        Path gap = copy("smoke");
        Path part2 = gap.resolve("mfg.occupancy").resolve("0.jsonl");
        Files.writeString(part2, Files.readString(part2, StandardCharsets.UTF_8).replaceFirst("\"offset\":1,", "\"offset\":2,"), StandardCharsets.UTF_8);
        IllegalArgumentException z = assertThrows(IllegalArgumentException.class, () -> Recording.open(gap).records("mfg.occupancy", 0));
        assertTrue(z.getMessage().contains("offset 2 where 1 is due"), z.getMessage());
    }

    @Test void recordingsVerifyRebuildsByteIdenticallyAndFailsOnADifference() throws IOException {
        for (String name : SMOKES) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(), err = new ByteArrayOutputStream();
            int code = Main.run(new String[] {"recordings", "verify", "--scenario", scenario(name).toString(), "--out", FIXTURES.resolve("recordings").resolve(name).toString()},
                new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
            assertEquals(0, code, name + ": " + err.toString(StandardCharsets.UTF_8));
        }
        // two builds into fresh directories are byte-identical to the committed files
        Path a = tmp.resolve("a"), b = tmp.resolve("b");
        ScenarioBuilder.build(scenario("smoke"), a);
        ScenarioBuilder.build(scenario("smoke"), b);
        assertEquals(files(a), files(b));
        assertEquals(files(a), files(FIXTURES.resolve("recordings").resolve("smoke")));

        Path tampered = copy("smoke");
        Path part = tampered.resolve("ot.sessions").resolve("1.jsonl");
        Files.writeString(part, Files.readString(part, StandardCharsets.UTF_8).replace("\"rebirth\":false", "\"rebirth\":true"), StandardCharsets.UTF_8);
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        assertEquals(1, Main.run(new String[] {"recordings", "verify", "--scenario", scenario("smoke").toString(), "--out", tampered.toString()},
            new PrintStream(new ByteArrayOutputStream()), new PrintStream(err, true, StandardCharsets.UTF_8)));
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("bytes differ: ot.sessions/1.jsonl"));

        Path scenarios = tmp.resolve("scenarios");
        Files.createDirectories(scenarios);
        Files.copy(scenario("smoke"), scenarios.resolve("smoke.json"));
        Files.writeString(scenarios.resolve("smoke.lock.json"), Files.readString(ScenarioBuilder.lockOf(scenario("smoke")), StandardCharsets.UTF_8)
            .replace("\"messages\": 4060", "\"messages\": 4061"), StandardCharsets.UTF_8);
        ByteArrayOutputStream err2 = new ByteArrayOutputStream();
        assertEquals(1, Main.run(new String[] {"recordings", "verify", "--scenario", scenarios.resolve("smoke.json").toString(), "--out", FIXTURES.resolve("recordings").resolve("smoke").toString()},
            new PrintStream(new ByteArrayOutputStream()), new PrintStream(err2, true, StandardCharsets.UTF_8)));
        assertTrue(err2.toString(StandardCharsets.UTF_8).contains("lock differs"));
    }

    @Test void incidentsSitInTheMurmur2PartitionOfTheirEquipmentRef() {
        Recording r = open("smoke-incident");
        assertEquals(7760, all(r, "ot.messages").size());
        assertEquals(53, r.batches().size());
        int expected = Murmur2.partition(NR1, 6);
        int station = r.routing().partitionOf("synthetic-site:line2:station1");
        assertNotEquals(station, expected, "the fixture must tell the equipment partition from the station partition");
        for (int p = 0; p < 6; p++) {
            List<Recording.Record> l = r.records("quality.incidents", p);
            if (p != expected) { assertEquals(List.of(), l, "partition " + p); continue; }
            assertEquals(1, l.size());
            assertEquals(NR1, l.get(0).key());
            assertEquals(NR1, l.get(0).value().get("equipmentRef").asText());
            assertEquals("inc-1", l.get(0).value().get("incidentId").asText());
        }
        // -1 until the batch that delivers the incident (deliverAt 1400000, the batch ending at scan 1410000), offset 0 from then on
        for (Recording.Batch b : r.batches()) {
            long want = b.cutId() < 27 ? -1 : 0;
            assertEquals(want, b.through("quality.incidents", expected), "cut " + b.cutId());
            for (int p = 0; p < 6; p++) if (p != expected) assertEquals(-1, b.through("quality.incidents", p));
        }
        assertEquals(List.of(new Recording.Deadline("visit-settlement", "visit-1", T0 + 1380000, 27, 28),
                             new Recording.Deadline("incident-settlement", "inc-1", T0 + 1380000, 27, 28)), r.deadlines());
    }

    @Test void scenariosMatchTheReleasedRules() throws IOException {
        TrustedKeys keys = TrustedKeys.load(FIXTURES.resolve("trusted-keys.jsonl"));
        for (String name : SMOKES) {
            Recording r = open(name);
            JsonNode rule = r.manifest().path("rule");
            String stream = rule.path("stream").asText();
            Path mappings = FIXTURES.resolve("registry").resolve("mappings");
            HeadVerifier.Head head = HeadVerifier.verify(mappings, stream, keys, new FloorStore(tmp.resolve("floors-" + name)), signedAt(mappings, stream) + 60_000);
            assertEquals(rule.path("releaseId").asText(), head.releaseId());
            assertEquals(rule.path("version").asText(), head.version());
            RuleSet rs = RuleSet.load(ReleaseReader.read(mappings, head), rule.path("attributionRuleId").asText(), rule.path("attributionRuleVersion").asText(), r.manifest().path("definitions"));
            assertEquals(rs.attribution().stations(), new ArrayList<>(r.routing().stations().keySet()), name);
            JsonNode s = Json.read(scenario(name));
            assertEquals(rs.attribution().mode(), s.path("mode").asText(), name);
            assertEquals(rs.attribution().outOfOrdernessMs(), s.path("temporal").path("outOfOrdernessMs").longValue(), name);
            assertEquals(rs.attribution().allowedLatenessMs(), s.path("temporal").path("allowedLatenessMs").longValue(), name);
            assertEquals(rs.attribution().expectedPeriodMs(), r.scanMs(), name);
            for (MemberCatalog.Resolved m : rs.members().byMappingId().values()) {
                JsonNode declared = s.path("devices").path(m.equipmentPath()).path("members").path(m.metric());
                assertEquals(m.valueType(), declared.asText(null), name + " " + m.mappingId());
            }
            // every member the recording carries on a device the rule catalogues is one of the rule's members
            JsonNode first = r.records("ot.messages", r.routing().rawPartitionOf(NR1)).get(0).value();
            for (JsonNode mem : first.path("members")) assertTrue(rs.members().byEquipmentAndMetric(NR1, mem.get("metric").asText()).isPresent(), name + " " + mem);
        }
    }

    static long signedAt(Path mappings, String stream) {
        JsonNode env = Json.read(mappings.resolve("head-attestations").resolve(stream + ".dsse.json"));
        JsonNode statement = Json.read(Base64.getDecoder().decode(env.path("payload").asText()));
        return Instant.parse(statement.path("predicate").path("signedAt").asText()).toEpochMilli();
    }

    static String firstLine(Path p) throws IOException {
        String s = Files.readString(p, StandardCharsets.UTF_8);
        return s.substring(0, s.indexOf('\n'));
    }

    Path copy(String name) throws IOException {
        Path src = FIXTURES.resolve("recordings").resolve(name), dst = Files.createTempDirectory(tmp, name + "-");
        try (Stream<Path> s = Files.walk(src)) {
            for (Path p : s.toList()) {
                Path t = dst.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
        return dst;
    }

    static Map<String, String> files(Path root) throws IOException {
        Map<String, String> m = new TreeMap<>();
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) m.put(Recording.relative(root, p), Canonical.digestOf(Files.readAllBytes(p)));
        }
        return m;
    }
}
