package yggdrasil.forseti.bridge;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.bridge.SessionTableTest.*;
import static yggdrasil.forseti.bridge.Sp.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The two D3 stages (injections 1 and 9) and the pipeline around them: default off, outside the session tracker, logged in the run
 *  record, never dropping a record, with the wire evidence kept in the raw-frame log. */
class StagesTest {
    static final ObjectMapper JSON = new ObjectMapper();

    /** A sink that keeps what it is sent, in order. */
    static final class ListSink implements Sink {
        final List<Records.Out> sent = new ArrayList<>();
        int flushes;
        @Override public void send(Records.Out r) { sent.add(r); }
        @Override public void flush() { flushes++; }
        @Override public void close() {}
        List<String> summary() {
            List<String> l = new ArrayList<>();
            for (Records.Out r : sent) {
                JsonNode v = r.topic().equals("ot.dead-letter") ? null : json(r.value());
                l.add(r.topic().equals("ot.messages") ? "m" + v.get("seq").longValue() : r.topic().equals("ot.sessions") ? v.get("kind").textValue() : "dead");
            }
            return l;
        }
    }
    static JsonNode json(byte[] b) { try { return JSON.readTree(b); } catch (Exception e) { throw new IllegalStateException(e); } }

    static Pipeline pipeline(ListSink sink, Stage... stages) {
        return new Pipeline(new BridgeCore(Map.of()), List.of(stages), sink, Records.Topics.DEFAULT, new RunRecord(), null);
    }

    @Test
    void withNoStageEveryRecordGoesStraightThroughUnchanged() {
        ListSink plain = new ListSink();
        Pipeline p = pipeline(plain);
        for (Frame f : session(0, B1, 4)) p.accept(f, f.receivedAt());
        p.drain(B1 + 10_000);
        assertEquals(List.of("session-up", "m2", "m3", "m4"), plain.summary());
        assertEquals(1, plain.flushes);
        assertEquals(List.of(), p.run().toMap(p.stages()).get("stages"));
    }

    @Test
    void theSourceOffsetMovesEveryMemberSourceTsAndNothingElse() {
        ListSink plain = new ListSink(), shifted = new ListSink();
        Pipeline a = pipeline(plain), b = pipeline(shifted, new SourceOffsetStage(30_000));
        List<Frame> frames = cat(session(0, B1, 3), List.of(device("DDATA", E, D, 4, B1 + 4050, scan(null)), ddata(E, D, 7, B1 + 7000)));
        for (Frame f : frames) { a.accept(f, f.receivedAt()); b.accept(f, f.receivedAt()); }
        assertEquals(plain.summary(), shifted.summary());
        for (int i = 0; i < plain.sent.size(); i++) {
            JsonNode x = json(plain.sent.get(i).value()), y = json(shifted.sent.get(i).value());
            if (!plain.sent.get(i).topic().equals("ot.messages")) { assertEquals(x, y, "session events keep the source clock"); continue; }
            for (int k = 0; k < x.get("members").size(); k++) {
                JsonNode mx = x.get("members").get(k), my = y.get("members").get(k);
                if (mx.has("sourceTs")) assertEquals(mx.get("sourceTs").longValue() + 30_000, my.get("sourceTs").longValue());
                else assertFalse(my.has("sourceTs"), "a missing sourceTs stays missing");
                ((com.fasterxml.jackson.databind.node.ObjectNode) mx).remove("sourceTs");
                ((com.fasterxml.jackson.databind.node.ObjectNode) my).remove("sourceTs");
            }
            assertEquals(x, y, "only sourceTs moved");
        }
        Map<String, Object> rec = b.stages().get(0).record();
        assertEquals("source-offset", rec.get("stage"));
        assertEquals(30_000L, rec.get("offsetMs"));
        assertEquals(4L, rec.get("messages"));
        assertEquals(6L, rec.get("membersShifted"));
    }

    @Test
    void theHoldStageHoldsTelemetryInItsWindowAndReleasesItInOrder() {
        ListSink sink = new ListSink();
        HoldStage hold = new HoldStage(1_000, 3_000);
        Pipeline p = pipeline(sink, hold);
        // DDATA seq s is received at B1 + 1000 s + 57; the first message (seq 2) opens the window at B1 + 3057, closed at B1 + 6057.
        for (Frame f : session(0, B1, 5)) p.accept(f, f.receivedAt());
        assertEquals(List.of("session-up", "m2"), sink.summary(), "seq 3, 4 and 5 are held");
        p.accept(ndeath(E, 0, B1 + 5500), B1 + 5505);
        assertEquals(List.of("session-up", "m2", "session-down"), sink.summary(), "session events are not held");
        p.tick(B1 + 6056);
        assertEquals(3, sink.sent.size());
        p.tick(B1 + 6057);
        assertEquals(List.of("session-up", "m2", "session-down", "m3", "m4", "m5"), sink.summary());
        Map<String, Object> rec = hold.record();
        assertEquals(List.of("hold-and-release", B1 + 3057, B1 + 6057, 3L, B1 + 6057, "window-end"),
            List.of(rec.get("stage"), rec.get("windowStart"), rec.get("windowEnd"), rec.get("held"), rec.get("releasedAt"), rec.get("releasedBy")));
    }

    @Test
    void aRecordArrivingAfterTheWindowIsWrittenAfterTheReleasedOnes() {
        ListSink sink = new ListSink();
        HoldStage hold = new HoldStage(0, 1_500);
        Pipeline p = pipeline(sink, hold);
        // The window is [B1 + 2057, B1 + 3557): seq 2 and 3 are held; seq 4, received at B1 + 4057, first releases them.
        for (Frame f : session(0, B1, 3)) p.accept(f, f.receivedAt());
        assertEquals(List.of("session-up"), sink.summary());
        Frame m4 = ddata(E, D, 4, B1 + 4000);
        p.accept(m4, m4.receivedAt());
        assertEquals(List.of("session-up", "m2", "m3", "m4"), sink.summary());
        assertEquals(2L, hold.record().get("held"));
    }

    @Test
    void theEndOfTheRunReleasesWhatIsStillHeld() {
        ListSink sink = new ListSink();
        HoldStage hold = new HoldStage(0, 600_000);
        Pipeline p = pipeline(sink, new SourceOffsetStage(1), hold);
        for (Frame f : session(0, B1, 3)) p.accept(f, f.receivedAt());
        assertEquals(List.of("session-up"), sink.summary());
        p.drain(B1 + 4000);
        assertEquals(List.of("session-up", "m2", "m3"), sink.summary());
        assertEquals("drain", hold.record().get("releasedBy"));
        assertEquals(1, sink.flushes);
    }

    @Test
    void c8AHoldReleasedAtItsWindowEndIsCompletedAndADrainInsideTheWindowIsNot() {
        HoldStage ran = new HoldStage(0, 1_000), cut = new HoldStage(0, 600_000);
        Pipeline a = pipeline(new ListSink(), ran), b = pipeline(new ListSink(), cut);
        for (Frame f : session(0, B1, 3)) { a.accept(f, f.receivedAt()); b.accept(f, f.receivedAt()); }
        a.tick(B1 + 10_000);
        a.drain(B1 + 10_000);
        b.drain(B1 + 10_000);
        assertEquals(true, ran.record().get("completed"));
        assertEquals(false, cut.record().get("completed"));
        assertEquals("drain", cut.record().get("releasedBy"));
        assertEquals(false, new HoldStage(0, 1_000).record().get("completed"), "a window that never opened did not run");
    }

    @Test
    void c6WithARoutingManifestEveryRoutedDeviceMustBePinned(@TempDir Path tmp) throws Exception {
        Path routing = tmp.resolve("routing.json");
        Files.writeString(routing, "{\"partitions\":6,\"devices\":{\"" + D + "\":{\"station\":\"" + D + "\",\"rawPartition\":1},\""
            + D2 + "\":{\"station\":\"" + D + "\",\"rawPartition\":2}}}", StandardCharsets.UTF_8);
        List<String> routed = BridgeMain.routingDevices(routing);
        assertEquals(List.of(D2), BridgeMain.unpinned(routed, Map.of(D, DEF_SHA)));
        assertEquals(List.of(), BridgeMain.unpinned(routed, Map.of(D, DEF_SHA, D2, DEF_SHA)));
        Files.writeString(routing, "{\"devices\":{}}", StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class, () -> BridgeMain.routingDevices(routing));
    }

    @Test
    void c6WithoutARoutingManifestTheRunRecordListsTheUnpinnedDevices() {
        RunRecord run = new RunRecord();
        run.pins(false, java.util.Set.of(D2, D));
        Map<String, Object> m = run.toMap(List.of());
        assertEquals(false, m.get("routing"));
        assertEquals(List.of(D, D2), m.get("unpinnedDevices"));
    }

    @Test
    void theStagesAreDeclaredOffAndRefuseAMeaninglessSetting() {
        assertThrows(IllegalArgumentException.class, () -> new SourceOffsetStage(0));
        assertThrows(IllegalArgumentException.class, () -> new HoldStage(0, 0));
        assertThrows(IllegalArgumentException.class, () -> new HoldStage(-1, 10));
    }

    @Test
    void theRunRecordNamesEveryStageAndCountsWhatWasWritten(@TempDir Path tmp) throws Exception {
        ListSink sink = new ListSink();
        Pipeline p = pipeline(sink, new SourceOffsetStage(30_000), new HoldStage(0, 1));
        for (Frame f : cat(List.of(ddata(E, D, 9, B1)), session(0, B1, 2))) p.accept(f, f.receivedAt());
        p.drain(B1 + 10_000);
        p.run().end("stopped", null);
        Path out = tmp.resolve("run.json");
        p.run().write(out, p.stages());
        byte[] bytes = Files.readAllBytes(out);
        assertFalse(new String(bytes, StandardCharsets.UTF_8).contains("\r"), "LF only");
        JsonNode r = JSON.readTree(bytes);
        assertEquals(List.of("source-offset", "hold-and-release"), List.of(r.get("stages").get(0).get("stage").textValue(), r.get("stages").get(1).get("stage").textValue()));
        assertEquals(4, r.get("frames").intValue());
        assertEquals(1, r.get("written").get("ot.messages").intValue());
        assertEquals(1, r.get("written").get("ot.sessions").intValue());
        assertEquals(1, r.get("deadLetters").get("birthless").intValue());
        assertEquals("stopped", r.get("outcome").textValue());
    }

    @Test
    void theRawFrameLogKeepsEveryFrameAsReceived(@TempDir Path tmp) throws Exception {
        Path log = tmp.resolve("frames.jsonl");
        List<Frame> frames = session(0, B1, 3);
        try (RawFrameLog l = new RawFrameLog(log)) {
            Pipeline p = new Pipeline(new BridgeCore(Map.of()), List.of(new SourceOffsetStage(30_000)), new ListSink(), Records.Topics.DEFAULT, new RunRecord(), l);
            for (Frame f : frames) p.accept(f, f.receivedAt());
        }
        List<String> lines = Files.readAllLines(log, StandardCharsets.UTF_8);
        assertEquals(frames.size(), lines.size());
        for (int i = 0; i < lines.size(); i++) {
            JsonNode n = JSON.readTree(lines.get(i));
            assertEquals(frames.get(i).topic(), n.get("topic").textValue());
            assertEquals(frames.get(i).receivedAt(), n.get("receivedAt").longValue());
            assertArrayEquals(frames.get(i).payload(), Base64.getDecoder().decode(n.get("payload").textValue()));
        }
    }
}
