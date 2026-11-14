package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;

/** The D8 rates of the capture report on crafted messages. */
class CaptureRatesTest {
    static final String S1 = "site-071:line2:station1", NR1 = "site-071:line2:station1:nutrunner1";

    static byte[] message(String device, long seq, long tsRunning, long tsOther) {
        String member = "{\"metric\":\"%s\",\"value\":%s,\"uaStatusCode\":0,\"quality\":192,\"sourceTs\":%d,\"serverTs\":%d}";
        String m = "{\"edge\":\"edge-x\",\"device\":\"" + device + "\",\"birthPublishTs\":1794182400000,\"bdSeq\":0,\"seq\":" + seq + ",\"publishTs\":" + (tsRunning + 50)
            + ",\"bridgeReceivedAt\":" + (tsRunning + 120) + ",\"definitionSha256\":\"" + "a".repeat(64) + "\",\"members\":["
            + String.format(member, "Running", "false", tsRunning, tsRunning + 10) + "," + String.format(member, "VisitPresent", "false", tsOther, tsOther + 10) + "]}";
        return m.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void incoherentSnapshotsAndRepeatedSourceTimestampsAreCountedPerDevice() {
        long t = 1794183000000L;
        List<byte[]> v = new ArrayList<>();
        // station1: 4 messages, one straddling two scans (t+1000 / t+2000), one repeating the scan t+2000
        v.add(message(S1, 2, t, t));
        v.add(message(S1, 3, t + 2000, t + 1000));
        v.add(message(S1, 4, t + 2000, t + 2000));
        v.add(message(S1, 5, t + 3000, t + 3000));
        // nutrunner1: 2 clean messages, and one the reader rejects
        v.add(message(NR1, 2, t, t));
        v.add(message(NR1, 3, t + 1000, t + 1000));
        v.add("{\"not\":\"a message\"}".getBytes(StandardCharsets.UTF_8));
        JsonNode r = CaptureRates.measure(v);
        assertEquals(6, r.get("messages").asLong());
        assertEquals(1, r.get("incoherentSnapshots").asLong());
        assertEquals("0.166667", r.get("incoherentSnapshotRate").decimalValue().toPlainString());
        assertEquals(1, r.get("repeatedSourceTs").asLong());
        assertEquals(1, r.get("rejected").asLong());
        JsonNode s = r.get("perDevice").get(S1);
        assertEquals(4, s.get("messages").asLong());
        assertEquals("0.250000", s.get("incoherentSnapshotRate").decimalValue().toPlainString());
        assertEquals("0.250000", s.get("repeatedSourceTsRate").decimalValue().toPlainString());
        JsonNode n = r.get("perDevice").get(NR1);
        assertEquals("0.000000", n.get("incoherentSnapshotRate").decimalValue().toPlainString());
        assertEquals(0, n.get("repeatedSourceTs").asLong());
    }
}
