package yggdrasil.forseti.bridge;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.bridge.SessionTableTest.*;
import static yggdrasil.forseti.bridge.Sp.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.bridge.StagesTest.ListSink;

/** Injection 25(a) on recorded frames: the host-offline path of Muninn (stop NDEATH of connection bdSeq 0, then the NBIRTH of bdSeq 1)
 *  with the declared input stage that hands the death to the core only after the newer birth. The death becomes a {@code stale-death}
 *  of the earlier session for every device, no {@code session-down} is written for it, and the newer session stays open. */
class DeathAfterBirthStageTest {
    static final String E2 = "edge-station2", D3 = "site:line2:station2";

    /** Muninn's host-offline path: connection 0 (two devices), its stop NDEATH, then connection 1 with both devices again. */
    static List<Frame> hostOffline() {
        List<Frame> l = new ArrayList<>(List.of(nbirth(E, 0, B1), dbirth(E, D, 1, B1 + 1000), dbirth(E, D2, 2, B1 + 1000),
            ddata(E, D, 3, B1 + 2000), ddata(E, D2, 4, B1 + 2000), ndeath(E, 0, B1 + 2500),
            nbirth(E, 1, B2), dbirth(E, D, 1, B2 + 1000), dbirth(E, D2, 2, B2 + 1000), ddata(E, D, 3, B2 + 2000), ddata(E, D2, 4, B2 + 2000)));
        return l;
    }

    static Pipeline pipeline(ListSink sink, RawFrameLog log, InputStage... inputs) {
        return new Pipeline(new BridgeCore(Map.of()), List.of(inputs), List.of(), sink, Records.Topics.DEFAULT, new RunRecord(), log, null);
    }

    static List<JsonNode> sessions(ListSink sink) {
        List<JsonNode> l = new ArrayList<>();
        for (Records.Out r : sink.sent) if (r.topic().equals("ot.sessions")) l.add(StagesTest.json(r.value()));
        return l;
    }

    static String ev(JsonNode n) { return n.get("kind").textValue() + " " + n.get("device").textValue() + " " + n.get("birthPublishTs").asLong() + " " + n.get("bdSeq").asLong(); }

    @Test
    void withoutTheStageTheStopDeathIsTheEarlierSessionsDown() {
        ListSink sink = new ListSink();
        Pipeline p = pipeline(sink, null);
        for (Frame f : hostOffline()) p.accept(f, f.receivedAt());
        List<String> kinds = sessions(sink).stream().map(DeathAfterBirthStageTest::ev).toList();
        assertTrue(kinds.contains("session-down " + D + " " + B1 + " 0"), kinds.toString());
        assertFalse(kinds.stream().anyMatch(k -> k.startsWith("stale-death")), kinds.toString());
    }

    @Test
    void theHeldDeathIsAStaleDeathPerDeviceAndTheNewerSessionStaysOpen(@TempDir Path tmp) throws Exception {
        ListSink sink = new ListSink();
        DeathAfterBirthStage stage = new DeathAfterBirthStage(E);
        List<Frame> frames = hostOffline();
        Path logFile = tmp.resolve("frames.jsonl");
        try (RawFrameLog log = new RawFrameLog(logFile)) {
            Pipeline p = pipeline(sink, log, stage);
            for (Frame f : frames) p.accept(f, f.receivedAt());
            // the newer session is open: more data of it is written as process messages of that session
            Frame later = ddata(E, D, 5, B2 + 3000);
            p.accept(later, later.receivedAt());
            p.drain(B2 + 10_000);
        }
        List<String> evs = sessions(sink).stream().map(DeathAfterBirthStageTest::ev).toList();
        assertEquals(List.of("session-up " + D + " " + B1 + " 0", "session-up " + D2 + " " + B1 + " 0",
            "stale-death " + D + " " + B1 + " 0", "stale-death " + D2 + " " + B1 + " 0",
            "session-up " + D + " " + B2 + " 1", "session-up " + D2 + " " + B2 + " 1"), evs, "no session-down, one stale-death per device of the earlier session");
        JsonNode stale = sessions(sink).get(2);
        assertEquals(B1 + 2500, stale.path("payload").path("rawPublishTs").asLong(), stale.toString());
        Records.Out last = sink.sent.get(sink.sent.size() - 1);
        assertEquals("ot.messages", last.topic());
        JsonNode m = StagesTest.json(last.value());
        assertEquals(List.of(B2, 1L, 5L), List.of(m.get("birthPublishTs").asLong(), m.get("bdSeq").asLong(), m.get("seq").asLong()), "telemetry continues in the newer session");

        Map<String, Object> rec = stage.record();
        assertEquals("death-after-birth", rec.get("stage"));
        assertEquals("25(a)", rec.get("injection"));
        assertEquals(true, rec.get("completed"));
        assertEquals("next-birth", rec.get("releasedBy"));
        @SuppressWarnings("unchecked") Map<String, Object> held = (Map<String, Object>) rec.get("held"), after = (Map<String, Object>) rec.get("releasedAfter");
        assertEquals(List.of(E, 0L, B1 + 2500), List.of(held.get("edge"), held.get("bdSeq"), held.get("timestamp")));
        assertEquals(List.of("spBv1.0/line2/NBIRTH/" + E, E, 1L, B2), List.of(after.get("topic"), after.get("edge"), after.get("bdSeq"), after.get("timestamp")));

        // the raw-frame log keeps the wire order: the death before the newer birth
        List<String> logged = new ArrayList<>();
        for (String l : Files.readAllLines(logFile, StandardCharsets.UTF_8)) logged.add(StagesTest.JSON.readTree(l).get("topic").textValue());
        assertEquals(frames.get(5).topic(), logged.get(5));
        assertEquals(frames.get(6).topic(), logged.get(6));
        assertTrue(logged.get(5).contains("/NDEATH/") && logged.get(6).contains("/NBIRTH/"));
    }

    @Test
    void framesOfOtherEdgesPassInOrderWhileTheDeathIsHeld() {
        ListSink sink = new ListSink();
        Pipeline p = pipeline(sink, null, new DeathAfterBirthStage(E));
        List<Frame> frames = new ArrayList<>(hostOffline());
        frames.add(6, nbirth(E2, 0, B1 + 2600));
        frames.add(7, dbirth(E2, D3, 1, B1 + 2700));
        frames.add(8, ndeath(E2, 0, B1 + 2800));
        for (Frame f : frames) p.accept(f, f.receivedAt());
        List<String> evs = sessions(sink).stream().map(DeathAfterBirthStageTest::ev).toList();
        assertTrue(evs.contains("session-down " + D3 + " " + (B1 + 2600) + " 0"), "another edge's death is not held: " + evs);
        assertTrue(evs.indexOf("session-down " + D3 + " " + (B1 + 2600) + " 0") < evs.indexOf("stale-death " + D + " " + B1 + " 0"), evs.toString());
    }

    @Test
    void aRunWithoutASuccessorBirthReleasesTheDeathAtTheDrainAndIsNotCompleted() {
        ListSink sink = new ListSink();
        DeathAfterBirthStage stage = new DeathAfterBirthStage(E);
        Pipeline p = pipeline(sink, null, stage);
        for (Frame f : hostOffline().subList(0, 6)) p.accept(f, f.receivedAt());
        assertFalse(sessions(sink).stream().anyMatch(n -> n.get("kind").textValue().equals("session-down")), "the death is held");
        p.drain(B1 + 60_000);
        List<String> evs = sessions(sink).stream().map(DeathAfterBirthStageTest::ev).toList();
        assertTrue(evs.contains("session-down " + D + " " + B1 + " 0"), "released at the drain, never dropped: " + evs);
        assertEquals(false, stage.record().get("completed"));
        assertEquals("drain", stage.record().get("releasedBy"));
        assertNull(stage.record().get("releasedAfter"));
        assertEquals(1, sink.flushes);
    }

    @Test
    void aDeathBeforeAnyBirthIsNotHeldAndTheStageRunsOnce() {
        ListSink sink = new ListSink();
        DeathAfterBirthStage stage = new DeathAfterBirthStage(E);
        Pipeline p = pipeline(sink, null, stage);
        List<Frame> frames = new ArrayList<>(List.of(ndeath(E, 7, B1 - 100)));
        frames.addAll(hostOffline());
        frames.add(ndeath(E, 1, B2 + 2500));
        for (Frame f : frames) p.accept(f, f.receivedAt());
        List<String> evs = sessions(sink).stream().map(DeathAfterBirthStageTest::ev).toList();
        assertTrue(evs.contains("session-down " + D + " " + B2 + " 1"), "the second death passes: " + evs);
        assertEquals(true, stage.record().get("completed"));
        @SuppressWarnings("unchecked") Map<String, Object> held = (Map<String, Object>) stage.record().get("held");
        assertEquals(0L, held.get("bdSeq"));
    }

    @Test
    void theRunRecordNamesTheInputStage(@TempDir Path tmp) throws Exception {
        ListSink sink = new ListSink();
        Pipeline p = pipeline(sink, null, new DeathAfterBirthStage(E));
        for (Frame f : hostOffline()) p.accept(f, f.receivedAt());
        p.drain(B2 + 10_000);
        p.run().end("stopped", null);
        Path out = tmp.resolve("run.json");
        p.run().write(out, p.inputs(), p.stages());
        JsonNode r = StagesTest.JSON.readTree(Files.readAllBytes(out));
        assertEquals("death-after-birth", r.get("inputStages").get(0).get("stage").textValue());
        assertTrue(r.get("inputStages").get(0).get("completed").booleanValue());
        assertEquals(0, r.get("stages").size());
        assertEquals(hostOffline().size(), r.get("frames").intValue(), "every wire frame counted once");
        assertThrows(IllegalArgumentException.class, () -> new DeathAfterBirthStage(""));
        assertEquals(0, new RunRecord().toMap(List.of()).get("inputStages") instanceof List<?> l ? l.size() : -1, "default off");
    }
}
