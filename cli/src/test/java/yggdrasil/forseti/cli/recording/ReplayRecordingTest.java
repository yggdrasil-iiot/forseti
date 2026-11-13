package yggdrasil.forseti.cli.recording;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.gate.ReplayRuns;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.rules.RuleSet;

/** The event-time replay materializer ({@link ReplayRecording}) on the committed smoke recordings, on copies of {@code smoke} edited in
 *  temporary directories (the fixtures are never written), and on the real {@code horizon-planned-late} recording built as the gate
 *  builds it. Smoke's telemetry: one message per device and second from T0 + 600 s, 30 s per batch, so with the rule's
 *  {@code outOfOrdernessMs} O = 30 s the target of batch k is {@code T_k = T0 + 600000 + 30000 k} and {@code W1_k = T_k - O - 1}. */
class ReplayRecordingTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    static final Path FIXTURES = ROOT.resolve("fixtures"), SMOKE = FIXTURES.resolve("recordings/smoke"), SCENARIOS = FIXTURES.resolve("scenarios");
    static final long T0 = 1794182400000L;
    static final String S1 = "synthetic-site:line2:station1", S2 = "synthetic-site:line2:station2", S3 = "synthetic-site:line2:station3";
    static final String NR1 = S1 + ":nutrunner1";
    static final List<String> TOPICS = RoutingManifest.TOPICS;

    @TempDir static Path shared;
    @TempDir Path tmp;
    private static RuleSet rules;
    private static long oo;

    static synchronized long O() throws IOException {
        if (rules == null) { rules = ReplayRuns.rules(FIXTURES, SMOKE, shared.resolve("floors-smoke")); oo = rules.attribution().outOfOrdernessMs(); }
        return oo;
    }

    static long w1(int k) throws IOException { return T0 + 600000 + 30000L * k - O() - 1; }

    // ============================================================================================================ order and bytes

    @Test void everyRecordOfTheOriginalAppearsOnceWithItsBytes() throws IOException {
        Path rec = reordered();
        ReplayRecording.Result r = ReplayRecording.materialize(rec, O(), tmp.resolve("replay"));
        Recording replay = Recording.open(r.out()), original = Recording.open(rec);
        replay.verifyDigests();
        replay.verifyBatches();
        int moved = 0;
        for (String t : TOPICS) for (int p : original.routing().subscribed(t)) {
            List<String> have = lines(r.out(), t, p), want = lines(rec, t, p);
            List<Long> map = offsetMap(r.out(), t, p);
            assertEquals(want.size(), have.size(), t + "/" + p);
            assertEquals(new TreeSet<>(range(want.size())), new TreeSet<>(map), t + "/" + p + ": the offset map is a permutation");
            for (int i = 0; i < have.size(); i++) {
                String w = want.get((int) (long) map.get(i));
                assertEquals(Json.read(have.get(i).getBytes(StandardCharsets.UTF_8)).get("offset").longValue(), i);
                assertEquals(after(w, "{\"offset\":" + map.get(i) + ","), after(have.get(i), "{\"offset\":" + i + ","), t + "/" + p + " replay offset " + i);
                if (map.get(i) != i) moved++;
            }
        }
        assertTrue(moved > 0, "the copy reorders records");
        assertEquals(Canonical.digestOf(Files.readAllBytes(rec.resolve("manifest.json"))), r.manifest().path("source").get("replayOf").asText());
        assertEquals(O(), r.manifest().path("source").get("outOfOrdernessMs").longValue());
        assertArrayEquals(Files.readAllBytes(rec.resolve("routing.json")), Files.readAllBytes(r.out().resolve("routing.json")), "routing unchanged");
        for (JsonNode b : r.manifest().get("batches")) assertEquals(0, b.get("idleInputs").size());
        for (String t : TOPICS) for (int p : original.routing().subscribed(t)) assertTrue(replay.digests().containsKey("offset-map/" + t + "/" + p + ".jsonl"));
    }

    @Test void eachPartitionIsOrderedByOrderingKeyThenOriginalOffset() throws IOException {
        Path rec = reordered();
        ReplayRecording.Result r = ReplayRecording.materialize(rec, O(), tmp.resolve("replay"));
        for (String t : List.of("ot.messages", "ot.sessions", "mfg.occupancy")) for (int p : Recording.open(rec).routing().subscribed(t)) {
            List<String> original = lines(rec, t, p);
            Long[] keys = keys(t, original);
            List<Long> map = offsetMap(r.out(), t, p);
            for (int i = 1; i < map.size(); i++) {
                long a = map.get(i - 1), b = map.get(i);
                Long ka = keys[(int) a], kb = keys[(int) b];
                if (ka == null) { assertTrue(kb == null ? a < b : true, t + "/" + p + ": the untimed head stays first, in order"); continue; }
                assertNotNull(kb, t + "/" + p + ": an untimed head record after a keyed one at " + i);
                assertTrue(ka < kb || (ka.equals(kb) && a < b), t + "/" + p + " at replay offset " + i + ": (" + ka + ", " + a + ") then (" + kb + ", " + b + ")");
            }
        }
    }

    @Test void aRecordWithoutEventTimeTravelsWithItsPredecessor() throws IOException {
        Path rec = reordered();
        ReplayRecording.Result r = ReplayRecording.materialize(rec, O(), tmp.resolve("replay"));
        int x = offsetOf(rec, "ot.messages", 1, NR1, T0 + 911000), u = x + 1;
        assertFalse(lines(rec, "ot.messages", 1).get(u).contains("sourceTs"), "the record after it carries no sourceTs");
        List<Long> map = offsetMap(r.out(), "ot.messages", 1);
        int px = map.indexOf((long) x), pu = map.indexOf((long) u);
        assertEquals(px + 1, pu, "the untimed record follows the record it inherits its key from");
        assertTrue(px < x, "both moved forward with the mixed message");
        assertEquals(batchOf(r.manifest(), "ot.messages", 1, px), batchOf(r.manifest(), "ot.messages", 1, pu));
        assertEquals(10, batchOf(r.manifest(), "ot.messages", 1, pu), "cut 10, whose target T0 + 900000 reaches the inherited key T0 + 890000");
    }

    @Test void aPartitionWithoutEventTimesKeepsItsOrderAndBatches() throws IOException {
        Path rec = FIXTURES.resolve("recordings/smoke-incident");
        ReplayRecording.Result r = ReplayRecording.materialize(rec, O(), tmp.resolve("replay"));
        Recording original = Recording.open(rec);
        int incidents = 0;
        for (int p : original.routing().subscribed("quality.incidents")) {
            assertArrayEquals(Files.readAllBytes(rec.resolve("quality.incidents/" + p + ".jsonl")), Files.readAllBytes(r.out().resolve("quality.incidents/" + p + ".jsonl")));
            assertEquals(range(original.size("quality.incidents", p)), offsetMap(r.out(), "quality.incidents", p));
            for (int k = 0; k < original.batches().size(); k++)
                assertEquals(original.batches().get(k).through("quality.incidents", p), r.manifest().get("batches").get(k).path("throughOffsets").path("quality.incidents").get("" + p).longValue());
            incidents += (int) original.size("quality.incidents", p);
        }
        assertTrue(incidents > 0, "smoke-incident carries incidents");
    }

    @Test void materializingTwiceGivesTheSameBytes() throws IOException {
        Path rec = reordered();
        ReplayRecording.Result a = ReplayRecording.materialize(rec, O(), tmp.resolve("a")), b = ReplayRecording.materialize(rec, O(), tmp.resolve("b"));
        TreeMap<String, byte[]> fa = tree(a.out()), fb = tree(b.out());
        assertEquals(fa.keySet(), fb.keySet());
        for (String f : fa.keySet()) assertArrayEquals(fa.get(f), fb.get(f), f);
        assertArrayEquals(Json.pretty(a.lock()), Json.pretty(b.lock()));
    }

    @Test void theSmokeReplayEqualsItsCommittedLock() throws IOException {
        ReplayRecording.Result r = ReplayRecording.materialize(SMOKE, O(), tmp.resolve("replay"));
        Path lock = ReplayRecording.lockOf(SCENARIOS.resolve("smoke.json"));
        assertEquals(SCENARIOS.resolve("smoke.replay.lock.json"), lock);
        assertEquals(List.of(), ReplayRecording.compareLock(r.lock(), lock));
        assertArrayEquals(Files.readAllBytes(lock), Json.pretty(r.lock()));
        JsonNode l = Json.read(lock);
        assertEquals(Json.read(SCENARIOS.resolve("smoke.lock.json")).get("scenarioSha256"), l.get("scenarioSha256"));
        assertEquals(28, l.path("counts").get("cuts").longValue());
        assertEquals(O(), l.get("outOfOrdernessMs").longValue());
        // smoke is delivered in event-time order: every partition keeps its bytes and every batch its through-offsets
        for (String t : TOPICS) for (int p : Recording.open(SMOKE).routing().subscribed(t))
            assertArrayEquals(Files.readAllBytes(SMOKE.resolve(t + "/" + p + ".jsonl")), Files.readAllBytes(r.out().resolve(t + "/" + p + ".jsonl")), t + "/" + p);
        for (int k = 0; k < 28; k++)
            assertEquals(c(Json.read(SMOKE.resolve("manifest.json")).get("batches").get(k).get("throughOffsets")), c(r.manifest().get("batches").get(k).get("throughOffsets")));
        // the lock catches a change of any byte
        ObjectNode other = r.lock().deepCopy();
        ((ObjectNode) other.get("files")).put("ot.messages/1.jsonl", "0".repeat(64));
        Path bad = Files.write(tmp.resolve("bad.replay.lock.json"), Json.pretty(other));
        assertEquals(1, ReplayRecording.compareLock(r.lock(), bad).size());
    }

    // ============================================================================================================ batches

    @Test void aFutureDatedOccupancyOrSessionEventSetsNoCutTarget() throws IOException {
        Path rec = copy();
        long future = T0 + 100_000_000L;   // beyond every telemetry timestamp of the recording
        insertAfter(rec, "mfg.occupancy", 0, 0, List.of(lines(rec, "mfg.occupancy", 0).get(0).replace("\"occ-1\"", "\"occ-future\"")
            .replace("\"eventTime\":" + (T0 + 600000), "\"eventTime\":" + future)));
        insertAfter(rec, "ot.sessions", 2, 0, List.of(lines(rec, "ot.sessions", 2).get(0).replace("\"at\":" + T0, "\"at\":" + future)));
        assertTrue(lines(rec, "mfg.occupancy", 0).get(1).contains("\"eventTime\":" + future) && lines(rec, "ot.sessions", 2).get(1).contains("\"at\":" + future));
        ReplayRecording.Result r = ReplayRecording.materialize(rec, O(), tmp.resolve("replay"));
        ReplayRecording.Result plain = ReplayRecording.materialize(SMOKE, O(), tmp.resolve("plain"));
        JsonNode original = Json.read(rec.resolve("manifest.json"));
        int n = r.cutIds().size();
        for (int k = 0; k < n; k++) {
            JsonNode b = r.manifest().get("batches").get(k).get("throughOffsets");
            assertEquals(c(original.get("batches").get(k).get("throughOffsets").get("ot.messages")), c(b.get("ot.messages")), "no telemetry is pulled into cut " + (k + 1));
            assertEquals(k == n - 1 ? 2 : k == 0 ? 0 : 1, b.path("mfg.occupancy").get("0").longValue(), "cut " + (k + 1));
            assertEquals(k == n - 1 ? 1 : 0, b.path("ot.sessions").get("2").longValue(), "cut " + (k + 1));
        }
        assertEquals(1L, (long) offsetMap(r.out(), "mfg.occupancy", 0).get(2), "the future occupancy is last");
        assertEquals(plain.w1Replay(), r.w1Replay(), "W1' at every cut is as without the two events");
        assertEquals(r.w1(), r.w1Replay());
    }

    @Test void partitionsWithUnequalProgressFollowTheirOwnTargets() throws IOException {
        Path rec = copy();
        // raw partition 2 (station2) ahead: batch 1 includes it up to T0 + 720000; partition 1 (nutrunner1, station3) stays at T0 + 630000
        ObjectNode m = (ObjectNode) Json.read(rec.resolve("manifest.json"));
        for (JsonNode b : m.get("batches")) {
            ObjectNode t = (ObjectNode) b.path("throughOffsets").path("ot.messages");
            t.put("2", Math.max(t.get("2").longValue(), 120));
        }
        Json.write(rec.resolve("manifest.json"), m);
        assertTrue(lines(rec, "ot.messages", 2).get(120).contains("\"sourceTs\":" + (T0 + 720000)));
        // a session event at T0 + 700000 (above T_1 of partition 1, at or below C_1) delivered in batch 2
        editPartition(rec, "ot.sessions", 1, l -> { l.add(l.get(1).replace("\"at\":" + T0, "\"at\":" + (T0 + 700000))); return l; });
        throughFrom(rec, "ot.sessions", 1, 2, 2);
        ReplayRecording.Result r = ReplayRecording.materialize(rec, O(), tmp.resolve("replay"));
        JsonNode b1 = r.manifest().get("batches").get(0).get("throughOffsets");
        assertEquals(120, b1.path("ot.messages").get("2").longValue());
        assertEquals(61, b1.path("ot.messages").get("1").longValue(), "partition 1 is cut at its own target, not at C_1");
        assertEquals(30, b1.path("ot.messages").get("4").longValue());
        assertEquals(2, b1.path("ot.sessions").get("1").longValue(), "a session event up to C_1 is included");
        assertEquals(T0 + 720000 - O() - 1, (long) r.w1Replay().get(S2).get(0));
        assertEquals(w1(1), (long) r.w1Replay().get(S3).get(0), "station3, fed by the slower partition, does not advance");
        assertEquals(w1(1), (long) r.w1Replay().get(S1).get(0));
        assertEquals(r.w1(), r.w1Replay());
    }

    @Test void aMixedMemberMessageIsOrderedByItsSmallestAndAdvancesByItsLargestTimestamp() throws IOException {
        Path rec = copy();
        int x = mix(rec, 1, NR1, T0 + 911000, T0 + 890000);
        probe(rec, T0 + 890000, 10);
        ReplayRecording.Result r = ReplayRecording.materialize(rec, O(), tmp.resolve("replay"));
        int pos = offsetMap(r.out(), "ot.messages", 1).indexOf((long) x);
        assertEquals(10, batchOf(r.manifest(), "ot.messages", 1, pos), "it enters cut 10: T0 + 890000 <= T_10 = T0 + 900000");
        assertEquals(11, batchOf(Json.read(rec.resolve("manifest.json")), "ot.messages", 1, x), "delivered in cut 11");
        long lifted = T0 + 911000 - O() - 1;
        assertEquals(lifted, (long) r.w1Replay().get(S1).get(9), "W1' of cut 10 from its largest sourceTs");
        assertEquals(w1(10), (long) r.w1().get(S1).get(9));
        assertEquals(w1(10), (long) r.w1Replay().get(S2).get(9));
        JsonNode proof = proof(r.manifest(), "probe");
        assertEquals(10, proof.get("cutA").longValue());
        assertEquals(lifted, proof.path("w1A").get(S1).longValue(), "the manifest records the larger watermark");
        assertEquals(w1(10), proof.path("w1A").get(S2).longValue());
        for (int k = 0; k < r.cutIds().size(); k++) for (String s : List.of(S1, S2, S3))
            assertTrue(r.w1Replay().get(s).get(k) >= r.w1().get(s).get(k), "W1' is never below W1");
    }

    // ============================================================================================================ deadlines

    @Test void aDeadlineWhoseBracketNoLongerHoldsIsReplanned() throws IOException {
        Path rec = copy();
        // every station's mixed message lifts W1'_10 to T0 + 880999, at or above the deadline T0 + 880000 bracketed by cuts 10 and 11
        mix(rec, 1, NR1, T0 + 911000, T0 + 890000);
        mix(rec, 1, S3, T0 + 911000, T0 + 890000);
        mix(rec, 2, S2, T0 + 911000, T0 + 890000);
        probe(rec, T0 + 880000, 10);
        ReplayRecording.Result r = ReplayRecording.materialize(rec, O(), tmp.resolve("replay"));
        JsonNode proof = proof(r.manifest(), "probe");
        assertEquals(List.of(S1, S2, S3), texts(proof.get("stations")));
        assertEquals(10, proof.get("originalCutA").longValue());
        assertEquals(11, proof.get("originalCutB").longValue());
        assertEquals(9, proof.get("cutA").longValue());
        assertEquals(10, proof.get("cutB").longValue());
        for (String s : List.of(S1, S2, S3)) {
            assertEquals(w1(9), proof.path("w1A").get(s).longValue());
            assertEquals(T0 + 911000 - O() - 1, proof.path("w1B").get(s).longValue());
        }
        JsonNode d = null;
        for (JsonNode x : r.manifest().get("deadlines")) if (x.get("ref").asText().equals("probe")) d = x;
        assertEquals(9, d.get("cutA").longValue());
        assertEquals(10, d.get("cutB").longValue());
        Recording.open(r.out());   // the manifest's deadlines read like any recording's
        JsonNode visit1 = proof(r.manifest(), "visit-1");
        assertEquals(visit1.get("originalCutA"), visit1.get("cutA"), "the other deadline keeps its cut ids");
    }

    @Test void aDeadlineWithNoBracketRefusesTheMaterialization() throws IOException {
        Path rec = copy();
        mix(rec, 1, NR1, T0 + 911000, T0 + 890000);   // station1 alone: it needs cuts 9 and 10, station2 and station3 cuts 10 and 11
        probe(rec, T0 + 880000, 10);
        Path out = tmp.resolve("replay");
        ReplayRecording.Refused e = assertThrows(ReplayRecording.Refused.class, () -> ReplayRecording.materialize(rec, O(), out));
        assertEquals("replay-deadline-unbracketed", e.code);
        assertEquals("probe", e.evidence.path("deadline").get("ref").asText());
        assertEquals(List.of(S1, S2, S3), texts(e.evidence.get("stations")));
        assertEquals(28, e.evidence.path("w1Replay").get(S1).size());
        assertEquals(T0 + 911000 - O() - 1, e.evidence.path("w1Replay").get(S1).get(9).longValue());
        assertFalse(Files.exists(out), "nothing is written");
        // the same through the CLI: exit 1 with the refusal
        java.io.ByteArrayOutputStream so = new java.io.ByteArrayOutputStream(), se = new java.io.ByteArrayOutputStream();
        int exit = yggdrasil.forseti.cli.Main.run(new String[]{"recordings", "replay", "--recording", rec.toString(), "--out-of-orderness-ms", Long.toString(O()),
            "--out", out.toString()}, new java.io.PrintStream(so, true, StandardCharsets.UTF_8), new java.io.PrintStream(se, true, StandardCharsets.UTF_8));
        assertEquals(1, exit, se.toString(StandardCharsets.UTF_8));
        assertEquals("replay-deadline-unbracketed", Json.read(so.toString(StandardCharsets.UTF_8).strip().getBytes(StandardCharsets.UTF_8)).get("refusal").asText());
        assertFalse(Files.exists(out));
        // an unconsumed record and declared idle inputs are refused before anything else
        Path unconsumed = copy();
        throughFrom(unconsumed, "ot.messages", 1, 28, lines(unconsumed, "ot.messages", 1).size() - 2);
        assertEquals("replay-input-unconsumed", assertThrows(ReplayRecording.Refused.class, () -> ReplayRecording.materialize(unconsumed, O(), out)).code);
        Path idle = copy();
        ObjectNode m = (ObjectNode) Json.read(idle.resolve("manifest.json"));
        ((ArrayNode) m.get("batches").get(3).get("idleInputs")).addObject().put("station", S1).put("topic", "ot.messages").put("partition", 1).put("frozenAt", T0);
        Json.write(idle.resolve("manifest.json"), m);
        assertEquals("replay-idle-inputs", assertThrows(ReplayRecording.Refused.class, () -> ReplayRecording.materialize(idle, O(), out)).code);
        Path unproved = copy();
        probe(unproved, T0 + 950000, 10);   // W1_10 < W1_11 = T0 + 899999 < at: no station is bracketed by cuts 10 and 11
        assertEquals("replay-deadline-unproved", assertThrows(ReplayRecording.Refused.class, () -> ReplayRecording.materialize(unproved, O(), out)).code);
        assertFalse(Files.exists(out));
    }

    // ============================================================================================================ the untimed head

    @Test void theUntimedHeadKeepsItsOriginalBatches() throws IOException {
        Path rec = copy();
        // occupancy partition 1: two records without an event time (schema-invalid), then a timed one; batch 1 includes the first
        // head record, batch 3 on everything
        String timed = lines(rec, "mfg.occupancy", 0).get(0).replace("\"occ-1\"", "\"occ-p1\"");
        editPartition(rec, "mfg.occupancy", 1, l -> {
            l.add(line(0, "x", "{\"bad\":1}"));
            l.add(line(1, "y", "{\"bad\":2}"));
            l.add(timed);
            return l;
        });
        ObjectNode m = (ObjectNode) Json.read(rec.resolve("manifest.json"));
        for (JsonNode b : m.get("batches")) ((ObjectNode) b.path("throughOffsets").path("mfg.occupancy")).put("1", b.get("cutId").longValue() < 3 ? 0 : 2);
        Json.write(rec.resolve("manifest.json"), m);
        ReplayRecording.Result r = ReplayRecording.materialize(rec, O(), tmp.resolve("replay"));
        assertEquals(range(3), offsetMap(r.out(), "mfg.occupancy", 1));
        for (int k = 0; k < r.cutIds().size(); k++)
            assertEquals(m.get("batches").get(k).path("throughOffsets").path("mfg.occupancy").get("1").longValue(),
                r.manifest().get("batches").get(k).path("throughOffsets").path("mfg.occupancy").get("1").longValue(), "cut " + (k + 1));
    }

    @Test void aBatchPrefixWithoutTimestampedTelemetryIncludesOnlyTheUntimedHead() throws IOException {
        Path rec = copy();
        // every raw partition begins with a message without any sourceTs, and batch 1 includes only that one: no target is defined
        for (int p : List.of(1, 2, 4, 5)) {
            editPartition(rec, "ot.messages", p, l -> { l.add(0, l.get(0).replaceAll("\"sourceTs\":[0-9]+,", "")); return l; });
            ObjectNode m = (ObjectNode) Json.read(rec.resolve("manifest.json"));
            for (JsonNode b : m.get("batches")) {
                ObjectNode t = (ObjectNode) b.path("throughOffsets").path("ot.messages");
                t.put("" + p, b.get("cutId").longValue() == 1 ? 0 : t.get("" + p).longValue() + 1);
            }
            Json.write(rec.resolve("manifest.json"), m);
        }
        ReplayRecording.Result r = ReplayRecording.materialize(rec, O(), tmp.resolve("replay"));
        JsonNode b1 = r.manifest().get("batches").get(0).get("throughOffsets"), b2 = r.manifest().get("batches").get(1).get("throughOffsets");
        for (int p : List.of(1, 2, 4, 5)) assertEquals(0, b1.path("ot.messages").get("" + p).longValue(), "only the head of " + p);
        for (int p : List.of(1, 2, 4, 5)) assertEquals(-1, b1.path("ot.sessions").get("" + p).longValue(), "C_1 is undefined: no session event");
        assertEquals(-1, b1.path("mfg.occupancy").get("0").longValue(), "C_1 is undefined: no occupancy event, though batch 1 delivered it");
        JsonNode o2 = Json.read(rec.resolve("manifest.json")).get("batches").get(1).get("throughOffsets");
        assertEquals(c(o2.get("ot.sessions")), c(b2.get("ot.sessions")));
        assertEquals(c(o2.get("mfg.occupancy")), c(b2.get("mfg.occupancy")));
        for (String s : List.of(S1, S2, S3)) assertEquals(ReplayRecording.NO_TELEMETRY, (long) r.w1Replay().get(s).get(0));
    }

    // ============================================================================================================ horizon-planned-late

    private static Path plannedLate;
    private static ReplayRecording.Result plannedLateReplay;
    private static RuleSet plannedLateRules;

    /** {@code horizon-planned-late} built as the gate builds it (the committed lock checked), and its replay, made once. */
    static synchronized ReplayRecording.Result plannedLate() throws IOException {
        if (plannedLateReplay == null) {
            Path scenario = SCENARIOS.resolve("horizon-planned-late.json");
            plannedLate = shared.resolve("horizon-planned-late");
            ScenarioBuilder.Built b = ScenarioBuilder.build(scenario, FIXTURES.resolve("registry"), plannedLate);
            assertArrayEquals(Files.readAllBytes(ScenarioBuilder.lockOf(scenario)), Json.pretty(b.lock()), "the rebuilt recording is the locked one");
            plannedLateRules = ReplayRuns.rules(FIXTURES, plannedLate, shared.resolve("floors-hpl"));
            plannedLateReplay = ReplayRecording.materialize(plannedLate, plannedLateRules.attribution().outOfOrdernessMs(), shared.resolve("horizon-planned-late.replay"));
        }
        return plannedLateReplay;
    }

    @Test void thePlannedLateFixtureKeepsEveryBracketAndItsPlannedA() throws IOException {
        ReplayRecording.Result r = plannedLate();
        assertEquals(List.of(), ReplayRecording.compareLock(r.lock(), ReplayRecording.lockOf(SCENARIOS.resolve("horizon-planned-late.json"))));
        assertEquals(r.w1(), r.w1Replay(), "W1' = W1 at every cut and station");
        JsonNode proofs = r.manifest().path("source").get("deadlines");
        assertEquals(Json.read(plannedLate.resolve("manifest.json")).get("deadlines").size(), proofs.size());
        JsonNode planned = null;
        for (JsonNode d : proofs) {
            assertEquals(d.get("originalCutA"), d.get("cutA"), d.toString());
            assertEquals(d.get("originalCutB"), d.get("cutB"), d.toString());
            if (d.get("kind").asText().equals("planned-late")) planned = d;
        }
        assertNotNull(planned);
        long a = planned.get("at").longValue(), h = plannedLateRules.attribution().horizonMs();
        int cutB = r.cutIds().indexOf(planned.get("cutB").longValue());
        assertEquals(a, (long) r.w1Replay().get(S1).get(cutB), "the planned cut's W1' is A");
        assertEquals(a, planned.path("w1B").get(S1).longValue());
        // the three late samples (A - H + 1, A - H, A - H - 1), delivered after the planned cut, sit at their event-time positions
        Recording original = Recording.open(plannedLate), replay = Recording.open(r.out());
        int p = original.routing().rawPartitionOf(NR1);
        long through = original.batches().get(cutB).through("ot.messages", p);
        List<Long> map = offsetMap(r.out(), "ot.messages", p);
        List<Long> lateTs = new ArrayList<>();
        List<Recording.Record> records = original.records("ot.messages", p);
        long[] target = new long[original.batches().size()];
        long max = Long.MIN_VALUE;
        for (int k = 0, i = 0; k < target.length; k++) {
            for (; i <= original.batches().get(k).through("ot.messages", p); i++) for (JsonNode m : records.get(i).value().path("members")) max = Math.max(max, m.path("sourceTs").asLong());
            target[k] = max;
        }
        for (Recording.Record x : records) {
            if (x.offset() <= through) continue;
            long min = Long.MAX_VALUE;
            for (JsonNode m : x.value().path("members")) min = Math.min(min, m.path("sourceTs").asLong());
            if (min > a - h + 1) continue;
            lateTs.add(min);
            int pos = map.indexOf(x.offset());
            long prev = minTs(replay.records("ot.messages", p).get(pos - 1)), next = minTs(replay.records("ot.messages", p).get(pos + 1));
            assertTrue(prev <= min && min <= next, "late sample at " + min + " between " + prev + " and " + next);
            int k = batchOf(r.manifest(), "ot.messages", p, pos);
            int want = 0;
            while (target[want] < min) want++;
            assertEquals(r.cutIds().get(want).intValue(), k, "the first cut whose target reaches " + min);
            assertTrue(k < planned.get("cutA").longValue(), "replayed long before the planned cut");
        }
        Collections.sort(lateTs);
        assertEquals(List.of(a - h - 1, a - h, a - h + 1), lateTs);
    }

    @Test void thePlannedLateReplayKeepsTheVisitFactsItsAfterHorizonNeedIsComputedFrom() throws IOException {
        ReplayRecording.Result r = plannedLate();
        JsonNode replayExpected = Json.read(r.out().resolve("expected.json")), originalExpected = Json.read(plannedLate.resolve("expected.json"));
        assertEquals("replay", replayExpected.get("scope").asText());
        assertEquals(originalExpected.get("visits").size(), replayExpected.get("visits").size());
        for (int i = 0; i < replayExpected.get("visits").size(); i++) {
            JsonNode v = replayExpected.get("visits").get(i), o = originalExpected.get("visits").get(i);
            for (String f : List.of("visitRef", "key", "station", "enter", "leave")) assertEquals(o.get(f), v.get(f), f);
            for (String f : List.of("finalStatus", "insufficiencyReasons", "retraction", "equipmentEvidence")) assertFalse(v.has(f), f);
        }
        for (String f : List.of("samples", "sampleRuns", "dispositions", "messageDispositionRuns", "incidents")) assertFalse(replayExpected.has(f), f);
        ReplayRuns.Need need = ReplayRuns.need(replayExpected, plannedLateRules), was = ReplayRuns.need(originalExpected, plannedLateRules);
        assertEquals(was, need);
        List<Long> w = r.w1Replay().get(need.station());
        assertTrue(need.at() <= w.get(w.size() - 1), "need " + need + " <= last W1' " + w.get(w.size() - 1));

        // the summary of a replay run of the recording, its cut log carrying the manifest's W1'
        JsonNode cutLog = ReplayRuns.cutLog(r.cutIds(), r.w1Replay(), replayExpected, plannedLateRules);
        assertEquals(List.of(), ReplayRecording.checkRun(r.manifest(), cutLog), "the gate's bracket check on the run's W1");
        Path run = tmp.resolve("run");
        String digest = ReplayRuns.journals(run, replayExpected, cutLog, need.station());
        JsonNode summary = ReplayRuns.summary("horizon-planned-late@replay", r.out(), replayExpected, cutLog, run, digest, plannedLateRules);
        JsonNode after = summary.path("afterHorizon");
        for (String f : List.of("t0", "need", "station", "what", "lastW1", "maxOnlineVisits", "lastOnlineVisits", "workRecords", "visits")) assertTrue(after.has(f), f);
        assertEquals(need.at(), after.get("need").longValue());
        assertEquals(replayExpected.get("visits").size(), after.get("visits").intValue());
        assertEquals(List.of(), ReplayRuns.afterHorizonProblems("horizon-planned-late@replay", summary));
        // a W1 the run did not report as the manifest proves it is caught
        ObjectNode shifted = cutLog.deepCopy();
        ((ObjectNode) shifted.get("cuts").get(r.cutIds().size() - 2).path("perStation").path(S1)).put("W1", 0);
        assertFalse(ReplayRecording.checkRun(r.manifest(), shifted).isEmpty());
        // without visit facts the summary fails by name, and so does J-AFTER-HORIZON-CUTS for that run
        ObjectNode bare = replayExpected.deepCopy();
        bare.putArray("visits");
        Path bareRun = tmp.resolve("bare");
        JsonNode failed = ReplayRuns.summary("horizon-planned-late@replay", r.out(), bare, cutLog, bareRun, ReplayRuns.journals(bareRun, bare, cutLog, need.station()), plannedLateRules);
        assertTrue(failed.get("failure").asText().contains("no visit facts"), failed.toString());
        List<String> problems = ReplayRuns.afterHorizonProblems("horizon-planned-late@replay", failed);
        assertTrue(problems.get(0).startsWith("horizon-planned-late@replay: afterHorizon: no visit facts"), problems.toString());
    }

    // ============================================================================================================ helpers

    /** The canonical text of a node: a value read from a file and the same value built in memory compare equal. */
    static String c(JsonNode n) { return Canonical.utf8(Canonical.bytes(n)); }

    static long minTs(Recording.Record x) {
        long min = Long.MAX_VALUE;
        for (JsonNode m : x.value().path("members")) if (m.has("sourceTs")) min = Math.min(min, m.get("sourceTs").asLong());
        return min;
    }

    static List<Long> range(long n) { List<Long> l = new ArrayList<>(); for (long i = 0; i < n; i++) l.add(i); return l; }

    static List<String> texts(JsonNode a) { List<String> l = new ArrayList<>(); a.forEach(x -> l.add(x.asText())); return l; }

    static String after(String line, String prefix) {
        assertTrue(line.startsWith(prefix), line);
        return line.substring(prefix.length());
    }

    static JsonNode proof(JsonNode manifest, String ref) {
        for (JsonNode d : manifest.path("source").path("deadlines")) if (d.get("ref").asText().equals(ref)) return d;
        throw new AssertionError("no deadline proof of " + ref);
    }

    /** The cut id of the batch that first includes {@code offset} of {@code topic/p}. */
    static int batchOf(JsonNode manifest, String topic, int p, long offset) {
        for (JsonNode b : manifest.get("batches")) if (b.path("throughOffsets").path(topic).get("" + p).longValue() >= offset) return b.get("cutId").intValue();
        throw new AssertionError("no batch includes " + topic + "/" + p + "/" + offset);
    }

    /** The ordering keys of one partition's original lines, computed here from the values (every member of these copies is valid). */
    static Long[] keys(String topic, List<String> lines) {
        Long[] k = new Long[lines.size()];
        for (int i = 0; i < k.length; i++) {
            JsonNode v = Json.read(lines.get(i).getBytes(StandardCharsets.UTF_8)).get("value");
            Long key = null;
            if (topic.equals("ot.messages")) { for (JsonNode m : v.path("members")) if (m.has("sourceTs")) key = key == null ? m.get("sourceTs").asLong() : Math.min(key, m.get("sourceTs").asLong()); }
            else if (topic.equals("ot.sessions")) key = v.path("payload").path("at").isIntegralNumber() ? v.path("payload").get("at").asLong() : null;
            else if (v.path("eventTime").isIntegralNumber() && v.has("unitId")) key = v.get("eventTime").asLong();
            k[i] = key != null ? key : i > 0 ? k[i - 1] : null;
        }
        return k;
    }

    /** smoke with a reordering: nutrunner1's message at T0 + 911000 (cut 11) carries a member at T0 + 890000, and a message without any
     *  sourceTs follows it. */
    Path reordered() throws IOException {
        Path rec = copy();
        int x = mix(rec, 1, NR1, T0 + 911000, T0 + 890000);
        insertAfter(rec, "ot.messages", 1, x, List.of(lines(rec, "ot.messages", 1).get(x).replaceAll("\"sourceTs\":[0-9]+,", "")));
        return rec;
    }

    Path copy() throws IOException { return copyTree(SMOKE, tmp.resolve("rec-" + UUID.randomUUID())); }

    /** Adds the deadline {@code probe} at {@code at}, bracketed by {@code cutA} and the next cut. */
    static void probe(Path rec, long at, long cutA) {
        ObjectNode m = (ObjectNode) Json.read(rec.resolve("manifest.json"));
        ObjectNode d = ((ArrayNode) m.get("deadlines")).addObject().put("kind", "probe").put("ref", "probe");
        Json.putSafe(Json.putSafe(Json.putSafe(d, "at", at), "cutA", cutA), "cutB", cutA + 1);
        Json.write(rec.resolve("manifest.json"), m);
    }

    /** {@code device}'s message at {@code sourceTs} on raw partition p: its first member moves to {@code smallest}, and a member at the
     *  original {@code sourceTs} is added. Returns the line's offset. */
    static int mix(Path rec, int p, String device, long sourceTs, long smallest) throws IOException {
        int at = offsetOf(rec, "ot.messages", p, device, sourceTs);
        editPartition(rec, "ot.messages", p, l -> {
            ObjectNode line = (ObjectNode) Json.read(l.get(at).getBytes(StandardCharsets.UTF_8));
            ArrayNode members = (ArrayNode) line.path("value").get("members");
            ObjectNode lifted = ((ObjectNode) members.get(0).deepCopy()).put("metric", "Lifted");
            ((ObjectNode) members.get(0)).put("sourceTs", smallest);
            members.add(lifted);
            try { l.set(at, Json.JSON.writeValueAsString(line)); } catch (IOException e) { throw new java.io.UncheckedIOException(e); }
            return l;
        });
        return at;
    }

    static int offsetOf(Path rec, String topic, int p, String device, long sourceTs) throws IOException {
        List<String> l = lines(rec, topic, p);
        for (int i = 0; i < l.size(); i++) if (l.get(i).contains("\"device\":\"" + device + "\"") && l.get(i).contains("\"sourceTs\":" + sourceTs + ",")) return i;
        throw new AssertionError("no " + device + " message at " + sourceTs);
    }

    static List<String> lines(Path rec, String topic, int p) throws IOException {
        return new ArrayList<>(Files.readAllLines(rec.resolve(topic).resolve(p + ".jsonl"), StandardCharsets.UTF_8));
    }

    static List<Long> offsetMap(Path replay, String topic, int p) throws IOException {
        List<Long> l = new ArrayList<>();
        int i = 0;
        for (String s : lines(replay.resolve(ReplayRecording.OFFSET_MAP), topic, p)) {
            JsonNode n = Json.read(s.getBytes(StandardCharsets.UTF_8));
            assertEquals(i++, n.get("offset").longValue());
            l.add(n.get("originalOffset").longValue());
        }
        return l;
    }

    static String line(long offset, String key, String rawValue) {
        return "{\"offset\":" + offset + ",\"timestamp\":null,\"key\":\"" + key + "\",\"headers\":{},\"value\":" + rawValue + "}";
    }

    /** Rewrites one partition file through {@code edit}, renumbers its offsets from 0 and refreshes the manifest's digest of it. */
    static void editPartition(Path rec, String topic, int p, UnaryOperator<List<String>> edit) throws IOException {
        List<String> l = edit.apply(lines(rec, topic, p));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < l.size(); i++) sb.append(l.get(i).replaceFirst("^\\{\"offset\":[0-9]+,", "{\"offset\":" + i + ",")).append('\n');
        Path f = rec.resolve(topic).resolve(p + ".jsonl");
        Files.write(f, sb.toString().getBytes(StandardCharsets.UTF_8));
        ObjectNode manifest = (ObjectNode) Json.read(rec.resolve("manifest.json"));
        ((ObjectNode) manifest.get("digests")).put(topic + "/" + p + ".jsonl", Canonical.digestOf(Files.readAllBytes(f)));
        Json.write(rec.resolve("manifest.json"), manifest);
    }

    /** Sets the through-offset of one partition to {@code through} in every batch from {@code fromCut} on. */
    static void throughFrom(Path rec, String topic, int p, long fromCut, long through) {
        ObjectNode manifest = (ObjectNode) Json.read(rec.resolve("manifest.json"));
        for (JsonNode b : manifest.get("batches"))
            if (b.get("cutId").longValue() >= fromCut) ((ObjectNode) b.path("throughOffsets").path(topic)).put(Integer.toString(p), through);
        Json.write(rec.resolve("manifest.json"), manifest);
    }

    /** Inserts lines after {@code offset}; every batch that already included {@code offset} includes them too. */
    static void insertAfter(Path rec, String topic, int p, int offset, List<String> added) throws IOException {
        editPartition(rec, topic, p, l -> { l.addAll(offset + 1, added); return l; });
        ObjectNode manifest = (ObjectNode) Json.read(rec.resolve("manifest.json"));
        for (JsonNode b : manifest.get("batches")) {
            ObjectNode t = (ObjectNode) b.path("throughOffsets").path(topic);
            long v = t.get(Integer.toString(p)).longValue();
            if (v >= offset) t.put(Integer.toString(p), v + added.size());
        }
        Json.write(rec.resolve("manifest.json"), manifest);
    }

    static Path copyTree(Path from, Path to) throws IOException {
        try (Stream<Path> s = Files.walk(from)) {
            for (Path p : s.toList()) {
                Path t = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
        return to;
    }

    static TreeMap<String, byte[]> tree(Path root) throws IOException {
        TreeMap<String, byte[]> m = new TreeMap<>();
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) m.put(root.relativize(p).toString().replace('\\', '/'), Files.readAllBytes(p));
        }
        return m;
    }
}
