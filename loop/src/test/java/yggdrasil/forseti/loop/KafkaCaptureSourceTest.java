package yggdrasil.forseti.loop;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.cli.live.CaptureRecorder;

/** The recorder's Kafka reader on the MockConsumer kafka-clients ships: a fixed prefix up to the end offset, append times and their
 *  type carried over, nothing past the end offset kept. */
class KafkaCaptureSourceTest {
    static ConsumerRecord<byte[], byte[]> rec(String topic, int p, long offset, long ts) {
        return new ConsumerRecord<>(topic, p, offset, ts, TimestampType.LOG_APPEND_TIME, 1, 2, ("k" + offset).getBytes(StandardCharsets.UTF_8),
            ("{\"o\":" + offset + "}").getBytes(StandardCharsets.UTF_8), new RecordHeaders(), Optional.empty());
    }

    @Test
    void readsUpToTheTerminalOffsetOnly() {
        MockConsumer<byte[], byte[]> c = new MockConsumer<>("earliest");
        TopicPartition tp = new TopicPartition("run-1.ot.messages", 2);
        c.updateBeginningOffsets(Map.of(tp, 0L));
        c.updateEndOffsets(Map.of(tp, 3L));
        c.schedulePollTask(() -> { for (long o = 0; o < 4; o++) c.addRecord(rec(tp.topic(), 2, o, 1000 + o)); });
        try (KafkaCaptureSource s = new KafkaCaptureSource(c, 10_000)) {
            assertEquals(0, s.beginningOffset(tp.topic(), 2));
            assertEquals(3, s.endOffset(tp.topic(), 2));
            List<CaptureRecorder.Raw> l = s.read(tp.topic(), 2, 3);
            assertEquals(3, l.size());
            for (int i = 0; i < 3; i++) {
                assertEquals(i, l.get(i).offset());
                assertEquals(1000 + i, l.get(i).timestamp());
                assertEquals("LogAppendTime", l.get(i).timestampType());
                assertEquals("k" + i, l.get(i).key());
                assertEquals("{\"o\":" + i + "}", new String(l.get(i).value(), StandardCharsets.UTF_8));
            }
            assertEquals(List.of(), s.read("run-1.ot.sessions", 0, 0));
        }
    }
}
