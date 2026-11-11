package yggdrasil.forseti.cli.recording;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

class ScenarioBuilderTest {
    static final Path FIXTURES = Path.of("..", "fixtures"), SCENARIOS = FIXTURES.resolve("scenarios"), REGISTRY = FIXTURES.resolve("registry");
    static final long T0 = 1794182400000L, OOO = 30000;
    static final String S1 = "synthetic-site:line2:station1", NR1 = S1 + ":nutrunner1", NR2 = S1 + ":nutrunner2";

    @TempDir Path tmp;

    static TreeMap<String, byte[]> files(Path root) throws IOException {
        TreeMap<String, byte[]> m = new TreeMap<>();
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) m.put(Recording.relative(root, p), Files.readAllBytes(p));
        }
        return m;
    }
    static void assertSameFiles(Path want, Path have, String what) throws IOException {
        TreeMap<String, byte[]> a = files(want), b = files(have);
        assertEquals(a.keySet(), b.keySet(), what);
        for (String k : a.keySet()) assertArrayEquals(a.get(k), b.get(k), what + " " + k);
    }
    static List<Recording.Record> all(Recording r, String topic) {
        List<Recording.Record> l = new ArrayList<>();
        for (int p : r.routing().subscribed(topic)) l.addAll(r.records(topic, p));
        return l;
    }
    static List<String> handMade() throws IOException {
        try (Stream<Path> s = Files.list(SCENARIOS)) {
            return s.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".json") && !n.endsWith(".lock.json") && !n.startsWith("smoke"))
                .filter(n -> !Json.read(SCENARIOS.resolve(n)).path("synthetic").asBoolean(false))   // the horizon scenarios are SynthesizerTest's
                .map(n -> n.substring(0, n.length() - 5)).sorted().toList();
        }
    }
    /** W1 of {@code station} at batch {@code b}: largest sourceTs routed to it through the batch, minus O, minus 1 (rule catalog devices). */
    static long w1(Recording r, String station, Recording.Batch b, Set<String> routed) {
        long max = Long.MIN_VALUE;
        for (int p : r.routing().subscribed("ot.messages"))
            for (Recording.Record x : r.records("ot.messages", p)) {
                if (x.offset() > b.through("ot.messages", p)) break;
                if (!routed.contains(x.key()) || !r.routing().stationOf(x.key()).equals(station)) continue;
                for (JsonNode m : x.value().path("members")) max = Math.max(max, m.get("sourceTs").longValue());
            }
        return max - OOO - 1;
    }

    /** A one-visit scenario of the smoke shape that reads its rule from the registry, for the overlay cases. */
    static ObjectNode small(boolean plc, ArrayNode injections) {
        ObjectNode s = (ObjectNode) Json.read(SCENARIOS.resolve(plc ? "smoke-plc.json" : "smoke.json"));
        s.remove("temporal");
        s.put("name", "small");
        s.set("injections", injections);
        return s;
    }
    Recording build(ObjectNode s, String dir) { ScenarioBuilder.build(s, "0".repeat(64), REGISTRY, tmp.resolve(dir)); return Recording.open(tmp.resolve(dir)); }
    static ArrayNode inj(String json) { return (ArrayNode) Json.read(json.getBytes(StandardCharsets.UTF_8)); }
    static JsonNode message(Recording r, String device, long t) {
        for (Recording.Record x : r.records("ot.messages", r.routing().rawPartitionOf(device)))
            if (x.value().path("publishTs").longValue() == T0 + t + 50) return x.value();
        return null;
    }
    static JsonNode member(JsonNode message, String metric) {
        for (JsonNode m : message.path("members")) if (m.path("metric").asText().equals(metric)) return m;
        return null;
    }
    static JsonNode visit(JsonNode expected, String ref) {
        for (JsonNode v : expected.path("visits")) if (v.path("visitRef").asText().equals(ref)) return v;
        return null;
    }

    @Test void baselineBuildsDeterministicallyAndEveryHandMadeScenarioEqualsItsLock() throws IOException {
        Path a = tmp.resolve("a"), b = tmp.resolve("b");
        ScenarioBuilder.build(SCENARIOS.resolve("baseline.json"), REGISTRY, a);
        ScenarioBuilder.build(SCENARIOS.resolve("baseline.json"), REGISTRY, b);
        assertSameFiles(a, b, "two builds of baseline");
        List<String> names = handMade();
        assertEquals(20, names.size(), names.toString());
        for (String n : names) {
            Path lock = ScenarioBuilder.lockOf(SCENARIOS.resolve(n + ".json"));
            if (!Files.isRegularFile(lock)) continue;   // a scenario is checked once its lock is written (Step 16.6)
            ScenarioBuilder.Built built = ScenarioBuilder.build(SCENARIOS.resolve(n + ".json"), REGISTRY, tmp.resolve("h-" + n));
            assertArrayEquals(Files.readAllBytes(lock), Json.pretty(built.lock()), n + ": a fresh build differs from its lock");
        }
    }

    @Test void freshSmokeBuildsEqualTheCommittedRecordings() throws IOException {
        for (String n : List.of("smoke", "smoke-plc", "smoke-incident")) {
            ScenarioBuilder.Built b = ScenarioBuilder.build(SCENARIOS.resolve(n + ".json"), REGISTRY, tmp.resolve(n));
            assertSameFiles(FIXTURES.resolve("recordings").resolve(n), tmp.resolve(n), n);
            assertArrayEquals(Files.readAllBytes(ScenarioBuilder.lockOf(SCENARIOS.resolve(n + ".json"))), Json.pretty(b.lock()), n + " lock");
        }
    }

    @Test void baselineHasItsPlannedCounts() {
        ScenarioBuilder.Built b = ScenarioBuilder.build(SCENARIOS.resolve("baseline.json"), REGISTRY, tmp.resolve("baseline"));
        assertEquals(15760, b.messages());
        assertEquals(22064, b.samples());
        Recording r = Recording.open(tmp.resolve("baseline"));
        assertEquals(15760, all(r, "ot.messages").size());
        assertEquals(32, all(r, "mfg.occupancy").size());
        assertEquals(16, r.expected().path("visits").size());
        assertEquals(T0 + 1920000 + 720000, r.settleTargetW());
    }

    @Test void everyDeviceHasATailOfAtLeastTailMs() {
        for (String n : List.of("baseline", "stage2", "corrections-10c")) {
            ScenarioBuilder.build(SCENARIOS.resolve(n + ".json"), REGISTRY, tmp.resolve(n));
            Recording r = Recording.open(tmp.resolve(n));
            long lastBusiness = 0;
            for (Recording.Record o : all(r, "mfg.occupancy")) lastBusiness = Math.max(lastBusiness, o.value().get("eventTime").longValue());
            for (Recording.Record o : all(r, "quality.incidents")) lastBusiness = Math.max(lastBusiness, o.value().get("revisionEventTime").longValue());
            JsonNode injections = Json.read(SCENARIOS.resolve(n + ".json")).path("injections");
            for (JsonNode i : injections) if (i.has("deliverAt")) lastBusiness = Math.max(lastBusiness, T0 + i.get("deliverAt").longValue());
            JsonNode scenario = Json.read(SCENARIOS.resolve(n + ".json"));
            for (JsonNode inc : scenario.path("incidents")) for (JsonNode rv : inc.path("revisions")) lastBusiness = Math.max(lastBusiness, T0 + rv.get("deliverAt").longValue());
            Map<String, Long> last = new TreeMap<>();
            for (Recording.Record m : all(r, "ot.messages")) for (JsonNode x : m.value().path("members")) last.merge(m.key(), x.get("sourceTs").longValue(), Math::max);
            assertEquals(5, last.size(), n);
            for (Map.Entry<String, Long> e : last.entrySet()) assertTrue(e.getValue() - lastBusiness >= r.tailMs(), n + " " + e.getKey());
        }
    }

    @Test void everyDeadlineIsBracketedUnderThePerStationFormula() {
        for (String n : List.of("baseline", "clock-plus-90", "held-back-10min", "plc-stamped", "stage2")) {
            ScenarioBuilder.build(SCENARIOS.resolve(n + ".json"), REGISTRY, tmp.resolve(n));
            Recording r = Recording.open(tmp.resolve(n));
            Set<String> routed = new TreeSet<>(r.routing().devices());
            if (n.startsWith("plc")) routed = Set.of(S1, NR1);
            assertFalse(r.deadlines().isEmpty(), n);
            Map<String, String> stationOfRef = new HashMap<>();
            for (JsonNode t : r.expected().path("transitions")) stationOfRef.put(t.get("ref").asText() + "@" + t.get("at").asText(), t.get("station").asText());
            for (Recording.Deadline d : r.deadlines()) {
                String station = stationOfRef.get(d.ref() + "@" + d.at());
                assertNotNull(station, n + " " + d);
                long a = w1(r, station, r.batches().get((int) d.cutA() - 1), routed), b = w1(r, station, r.batches().get((int) d.cutB() - 1), routed);
                assertTrue(a < d.at() && d.at() <= b, n + " " + d + " W1 " + a + ", " + b);
                assertEquals(d.cutA() + 1, d.cutB());
            }
        }
    }

    @Test void expectedNamesEverySampleIdOnce() throws IOException {
        ScenarioBuilder.build(SCENARIOS.resolve("session.json"), REGISTRY, tmp.resolve("session"));
        Recording r = Recording.open(tmp.resolve("session"));
        List<String> ids = new ArrayList<>();
        for (Recording.Record m : all(r, "ot.messages")) {
            JsonNode v = m.value();
            for (JsonNode x : v.path("members")) {
                ObjectNode id = Json.object().put("edge", v.get("edge").asText()).put("device", v.get("device").asText());
                Json.putSafe(id, "birthPublishTs", v.get("birthPublishTs").longValue()).put("metric", x.get("metric").asText());
                Json.putSafe(id, "bdSeq", v.get("bdSeq").longValue()); Json.putSafe(id, "seq", v.get("seq").longValue()); Json.putSafe(id, "publishTs", v.get("publishTs").longValue());
                ids.add(Canonical.utf8(Canonical.bytes(id)));
            }
        }
        assertEquals(ids.size(), new HashSet<>(ids).size(), "sampleIds are unique in the recording");
        Set<String> keys = new HashSet<>();
        r.expected().path("samples").fieldNames().forEachRemaining(keys::add);
        assertEquals(new HashSet<>(ids), keys);
        // the expected file has no duplicate key: its sample count equals the distinct count
        String text = Files.readString(tmp.resolve("session").resolve(Recording.EXPECTED), StandardCharsets.UTF_8);
        assertEquals(ids.size(), r.expected().path("samples").size());
        assertTrue(text.endsWith("}\n"));
    }

    @Test void eachOverlayTypeProducesItsDocumentedEffect() {
        // hold-back: the held scans move to a later batch, offsets renumbered in delivery order; counts unchanged
        Recording on = build(small(false, inj("[]")), "on");
        Recording held = build(small(false, inj("[{\"type\":\"hold-back\",\"topic\":\"ot.messages\",\"devices\":[\"" + NR1 + "\"],\"from\":620000,\"to\":630000,\"delayMs\":30000}]")), "held");
        assertEquals(all(on, "ot.messages").size(), all(held, "ot.messages").size());
        List<Recording.Record> nr1 = held.records("ot.messages", held.routing().rawPartitionOf(NR1));
        long previous = Long.MIN_VALUE; boolean outOfOrder = false;
        for (Recording.Record x : nr1) { if (!x.key().equals(NR1)) continue; long t = member(x.value(), "Torque").get("sourceTs").longValue(); if (t < previous) outOfOrder = true; previous = t; }
        assertTrue(outOfOrder, "held scans are delivered after later ones");
        // redeliver: an occupancy event appended again, identical except recordTime; its disposition counts one duplicate
        Recording re = build(small(false, inj("[{\"type\":\"redeliver\",\"topic\":\"mfg.occupancy\",\"identity\":{\"eventId\":\"occ-1\"},\"deliverAt\":640000}]")), "re");
        List<Recording.Record> occ = all(re, "mfg.occupancy");
        assertEquals(3, occ.size());
        ObjectNode x0 = occ.get(0).value().deepCopy(), x2 = occ.get(1).value().deepCopy();
        assertEquals(x0.get("eventId"), x2.get("eventId"));
        assertNotEquals(x0.get("recordTime"), x2.get("recordTime"));
        x0.remove("recordTime"); x2.remove("recordTime");
        assertEquals(x0, x2);
        assertEquals(1, re.expected().path("dispositions").path("{\"eventId\":\"occ-1\",\"source\":\"mes-line2\"}").path("duplicates").intValue());
        // shift-source-ts: sourceTs moved, publishTs unchanged
        Recording sh = build(small(false, inj("[{\"type\":\"shift-source-ts\",\"devices\":[\"" + NR1 + "\"],\"deltaMs\":30000}]")), "sh");
        assertEquals(T0 + 630000, member(message(sh, NR1, 600000), "Torque").get("sourceTs").longValue());
        // bad-status: 7 Bad torque samples, unattributed bad-quality, visit insufficient bad-share-exceeded
        Recording bad = build(small(false, inj("[{\"type\":\"bad-status\",\"device\":\"" + NR1 + "\",\"metric\":\"Torque\",\"from\":610000,\"to\":617000,\"code\":2147483648}]")), "bad");
        JsonNode m = member(message(bad, NR1, 610000), "Torque");
        assertEquals(2147483648L, m.get("uaStatusCode").longValue());
        assertEquals(0, m.get("quality").intValue());
        long badRows = 0;
        for (JsonNode sx : bad.expected().path("samples")) if ("bad-quality".equals(sx.path("reason").asText())) badRows++;
        assertEquals(7, badRows);
        assertEquals("insufficient", visit(bad.expected(), "visit-1").path("finalStatus").asText());
        assertEquals("[\"bad-share-exceeded\"]", visit(bad.expected(), "visit-1").path("insufficiencyReasons").toString());
        // overlap-visit: both closed visits retracted overlap
        Recording ov = build(small(false, inj("[{\"type\":\"overlap-visit\",\"unit\":\"UNIT0000000000009\",\"station\":\"" + S1 + "\",\"enter\":640000,\"leave\":700000,\"tools\":[],\"visitSeq\":9}]")), "ov");
        assertEquals("overlap", visit(ov.expected(), "visit-1").path("retraction").path("reason").asText());
        assertEquals("overlap", visit(ov.expected(), "visit-2").path("retraction").path("reason").asText());
        // delay-occupancy: the departure delivered after its samples; still complete at the end, provisional before
        Recording dl = build(small(false, inj("[{\"type\":\"delay-occupancy\",\"eventId\":\"occ-2\",\"deliverAt\":720000}]")), "dl");
        assertEquals("complete", visit(dl.expected(), "visit-1").path("finalStatus").asText());
        assertEquals("occ-1", all(dl, "mfg.occupancy").get(0).value().get("eventId").asText());
        // late-correction: declaration and corrective departure both accepted late-after-settlement; the visit closes at the new leave
        Recording lc = build(small(false, inj("[{\"type\":\"late-correction\",\"departureEventId\":\"occ-2\",\"declarationTime\":1500000,\"newDepartureEventId\":\"occ-2-c1\",\"newLeave\":665000,\"deliverAt\":1500000}]")), "lc");
        JsonNode d = lc.expected().path("dispositions");
        assertEquals("late-after-settlement", d.path("{\"eventId\":\"occ-2\",\"source\":\"mes-line2\"}").path("parts").path("errorDeclaration").path("reason").asText());
        assertEquals("late-after-settlement", d.path("{\"eventId\":\"occ-2-c1\",\"source\":\"mes-line2\"}").path("parts").path("event").path("reason").asText());
        assertEquals(T0 + 665000, visit(lc.expected(), "visit-1").path("leave").longValue());
        // error-declaration: did_not_occur on the arrival retracts the visit
        Recording ed = build(small(false, inj("[{\"type\":\"error-declaration\",\"eventId\":\"occ-1\",\"reason\":\"did_not_occur\",\"declarationTime\":700000,\"deliverAt\":700000}]")), "ed");
        assertEquals("error-declaration", visit(ed.expected(), "visit-1").path("retraction").path("reason").asText());
        assertEquals(Overlays.DID_NOT_OCCUR, visit(ed.expected(), "visit-1").path("retraction").path("errorReason").asText());
        // resend-different: both deliveries conflict identity-conflict, the visit retracted input-conflict
        Recording rd = build(small(false, inj("[{\"type\":\"resend-different\",\"eventId\":\"occ-1\",\"field\":\"unitId\",\"value\":\"UNIT0000000000099\",\"deliverAt\":700000}]")), "rd");
        assertEquals("identity-conflict", rd.expected().path("dispositions").path("{\"eventId\":\"occ-1\",\"source\":\"mes-line2\"}").path("parts").path("event").path("reason").asText());
        assertEquals("input-conflict", visit(rd.expected(), "visit-1").path("retraction").path("reason").asText());
        // activity-without-visit: Running true and torque outside any visit, samples unattributed no-occupancy
        Recording aw = build(small(false, inj("[{\"type\":\"activity-without-visit\",\"device\":\"" + NR2 + "\",\"from\":800000,\"to\":810000}]")), "aw");
        assertTrue(member(message(aw, NR2, 800000), "Running").get("value").booleanValue());
        assertFalse(member(message(aw, NR2, 810000), "Running").get("value").booleanValue());
        // session-gap: messages with seq strictly between removed, one seq-gap event, the visit insufficient session-gap
        Recording sg = build(small(false, inj("[{\"type\":\"session-gap\",\"device\":\"" + NR1 + "\",\"fromSeq\":20,\"toSeq\":24}]")), "sg");
        assertEquals(all(on, "ot.messages").size() - 3, all(sg, "ot.messages").size());
        assertTrue(all(sg, "ot.sessions").stream().anyMatch(x -> x.value().get("kind").asText().equals("seq-gap")));
        assertEquals("[\"session-gap\"]", visit(sg.expected(), "visit-1").path("insufficiencyReasons").toString());
        // session-down: messages removed, session-down then a session-up with a new birth and bdSeq + 1
        Recording sd = build(small(false, inj("[{\"type\":\"session-down\",\"device\":\"" + NR2 + "\",\"at\":620000,\"upAt\":625000}]")), "sd");
        assertEquals(all(on, "ot.messages").size() - 5, all(sd, "ot.messages").size());
        assertEquals(1, message(sd, NR2, 625000).get("bdSeq").longValue());
        assertEquals(T0 + 625000, message(sd, NR2, 625000).get("birthPublishTs").longValue());
        // stale-death: one event, nothing else changes
        Recording st = build(small(false, inj("[{\"type\":\"stale-death\",\"device\":\"" + NR2 + "\",\"detectedEventTime\":620000,\"rawPublishTs\":0,\"bdSeq\":0}]")), "st");
        assertEquals(all(on, "ot.messages").size(), all(st, "ot.messages").size());
        assertEquals("complete", visit(st.expected(), "visit-1").path("finalStatus").asText());
        // session-ambiguous: unknown interval, the visit insufficient session-ambiguous
        Recording sa = build(small(false, inj("[{\"type\":\"session-ambiguous\",\"device\":\"" + NR2 + "\",\"detectedEventTime\":620000,\"rawPublishTs\":619000,\"bdSeq\":1,\"upAt\":622000}]")), "sa");
        assertEquals("[\"session-ambiguous\"]", visit(sa.expected(), "visit-1").path("insufficiencyReasons").toString());
        // no-departure: the departing event removed
        Recording nd = build(small(false, inj("[{\"type\":\"no-departure\",\"eventId\":\"occ-2\"}]")), "nd");
        assertEquals(1, all(nd, "mfg.occupancy").size());
        assertEquals("provisional", visit(nd.expected(), "visit-1").path("finalStatus").asText());
        // plc overlays on the smoke-plc shape (plc-identity, cross-check mismatch, mixed snapshot, id change, no visit end)
        String id = "{\"type\":\"plc-identity\",\"station\":\"" + S1 + "\",\"equipment\":[\"" + S1 + "\",\"" + NR1 + "\"],\"visitId\":\"V1\",\"unit\":\"UNIT0000000000001\",\"present\":{\"from\":600000,\"to\":660000}}";
        Recording pm = build(small(true, inj("[" + id + ",{\"type\":\"plc-cross-check-mismatch\",\"station\":\"" + S1 + "\",\"from\":600000,\"to\":660000,\"otherUnit\":\"UNIT0000000000099\"},"
            + "{\"type\":\"mixed-snapshot\",\"device\":\"" + NR1 + "\",\"at\":610000}]")), "pm");
        assertEquals("V1", member(message(pm, NR1, 620000), "VisitId").get("value").asText());
        assertEquals("UNIT0000000000099", all(pm, "mfg.occupancy").get(0).value().get("unitId").asText());
        assertEquals(T0 + 609000, member(message(pm, NR1, 610000), "VisitId").get("sourceTs").longValue());
        long mismatch = 0, incoherent = 0;
        for (JsonNode sx : pm.expected().path("samples")) { if ("identity-mismatch".equals(sx.path("reason").asText())) mismatch++; if ("incoherent-snapshot".equals(sx.path("reason").asText())) incoherent++; }
        assertTrue(mismatch > 0);
        assertEquals(5, incoherent);
        Recording ic = build(small(true, inj("[" + id + ",{\"type\":\"id-change-without-false\",\"station\":\"" + S1 + "\",\"at\":630000,\"newVisitId\":\"V2\",\"newUnit\":\"UNIT0000000000002\"}]")), "ic");
        assertEquals("[\"missing-visit-close\"]", visit(ic.expected(), "V1").path("insufficiencyReasons").toString());
        assertTrue(member(message(ic, NR1, 630000), "VisitPresent").get("value").booleanValue());
        Recording ne = build(small(true, inj("[" + id + ",{\"type\":\"no-visit-end\",\"station\":\"" + S1 + "\",\"visitId\":\"V1\"}]")), "ne");
        assertTrue(member(message(ne, NR1, 700000), "VisitPresent").get("value").booleanValue());
        assertTrue(visit(ne.expected(), "V1").path("leave").isNull());
        // incidents (scenario incidents[]): delivered on the partition of their equipmentRef, judged in expected.incidents
        ObjectNode si = (ObjectNode) Json.read(SCENARIOS.resolve("smoke-incident.json"));
        si.remove("temporal");
        Recording inc = build(si, "inc");
        assertEquals(1, all(inc, "quality.incidents").size());
        assertEquals("affected-candidate", inc.expected().path("incidents").path("{\"incidentId\":\"inc-1\",\"source\":\"qms-line2\"}").path("units").path("UNIT0000000000001").path("status").asText());
        // an unknown overlay type is refused
        assertThrows(IllegalArgumentException.class, () -> build(small(false, inj("[{\"type\":\"teleport\"}]")), "x"));
    }

    @Test void aScenarioWithoutBracketingCutsIsRefused() {
        // every device's telemetry around the settlement bracket (leave + L + O = 1410000) held back past it: W1 cannot reach d at cutB
        ObjectNode s = small(false, inj("[{\"type\":\"hold-back\",\"topic\":\"ot.messages\",\"devices\":[],\"from\":1300000,\"to\":1412000,\"delayMs\":100000}]"));
        s.put("tailMs", 900000);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> build(s, "refused"));
        assertTrue(e.getMessage().contains("without bracketing cuts"), e.getMessage());
        // and a scenario that needs the registry is refused without it
        assertThrows(IllegalArgumentException.class, () -> ScenarioBuilder.build(small(false, inj("[]")), "0".repeat(64), null, tmp.resolve("noreg")));
    }

    @Test void verifyFailsOnAChangedLockDigest() throws IOException {
        Path scenarios = Files.createDirectories(tmp.resolve("scenarios"));
        Files.copy(SCENARIOS.resolve("baseline.json"), scenarios.resolve("baseline.json"));
        Path out = tmp.resolve("baseline");
        PrintStream quiet = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        assertEquals(0, Main.run(new String[] {"recordings", "build", "--scenario", scenarios.resolve("baseline.json").toString(), "--registry", REGISTRY.toString(),
            "--out", out.toString(), "--lock"}, quiet, quiet));
        assertEquals(0, Main.run(new String[] {"recordings", "verify", "--scenario", scenarios.resolve("baseline.json").toString(), "--registry", REGISTRY.toString(),
            "--out", out.toString()}, quiet, quiet));
        Path lock = scenarios.resolve("baseline.lock.json");
        String text = Files.readString(lock, StandardCharsets.UTF_8);
        String digest = Json.read(lock).path("files").path("expected.json").asText();
        Files.writeString(lock, text.replace(digest, "0".repeat(64)), StandardCharsets.UTF_8);
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        assertEquals(1, Main.run(new String[] {"recordings", "verify", "--scenario", scenarios.resolve("baseline.json").toString(), "--registry", REGISTRY.toString(),
            "--out", out.toString()}, quiet, new PrintStream(err, true, StandardCharsets.UTF_8)));
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("lock differs"), err.toString(StandardCharsets.UTF_8));
    }
}
