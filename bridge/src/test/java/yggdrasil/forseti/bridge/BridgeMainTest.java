package yggdrasil.forseti.bridge;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.bridge.SessionTableTest.*;
import static yggdrasil.forseti.bridge.Sp.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** What the full-loop runner reads from the bridge process: the stop file, and one line per session event written. */
class BridgeMainTest {
    @Test
    void theStopFileStopsTheBridgeOnlyOnceItExists(@TempDir Path dir) throws Exception {
        Path stop = dir.resolve("bridge.stop");
        assertFalse(BridgeMain.stopRequested(null), "no stop file, no stop");
        assertFalse(BridgeMain.stopRequested(stop));
        Files.createFile(stop);
        assertTrue(BridgeMain.stopRequested(stop));
    }

    @Test
    void everySessionEventWrittenIsReportedOnceAfterItsSend() {
        StagesTest.ListSink sink = new StagesTest.ListSink();
        List<String> lines = new ArrayList<>();
        Pipeline p = new Pipeline(new BridgeCore(Map.of()), List.of(), sink, Records.Topics.DEFAULT, new RunRecord(), null, e -> {
            assertFalse(sink.sent.isEmpty(), "reported after the send");
            lines.add(BridgeMain.sessionLine(e));
        });
        for (Frame f : session(0, B1, 3)) p.accept(f, f.receivedAt());
        assertEquals(List.of("[BRIDGE] session session-up " + E + " " + D), lines);
        assertEquals(1, sink.summary().stream().filter(s -> s.equals("session-up")).count());
    }
}
