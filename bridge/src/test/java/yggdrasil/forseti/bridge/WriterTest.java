package yggdrasil.forseti.bridge;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/** The Kafka writer and the input queue, without a broker: the producer settings, keys and headers, and that a failed send or a full
 *  queue fails the run instead of dropping a record. */
class WriterTest {
    static MockProducer<byte[], byte[]> mock(boolean autoComplete) {
        return new MockProducer<>(autoComplete, null, new ByteArraySerializer(), new ByteArraySerializer());
    }
    static Records.Out out(String key) { return new Records.Out("ot.messages", key, "{}".getBytes(StandardCharsets.UTF_8), Map.of()); }

    @Test
    void theProducerIsIdempotentWithAllAcksAndABoundedBuffer() {
        Properties p = KafkaSink.config("localhost:9092", "forseti-bridge", 1_048_576);
        assertEquals("true", p.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", p.get(ProducerConfig.ACKS_CONFIG));
        assertEquals("5", p.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        assertEquals("1048576", p.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
        assertEquals("60000", p.get(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

    @Test
    void recordsAreKeyedByDeviceAndTheDeadLetterKeepsItsHeaders() {
        MockProducer<byte[], byte[]> m = mock(true);
        try (KafkaSink sink = new KafkaSink(m)) {
            sink.send(out("site:line2:station1"));
            sink.send(Records.encode(new Emitted.DeadLetter("spBv1.0/g/DDATA/e/d", new byte[] {9, 8}, 42, "birthless", "e", "d"), Records.Topics.DEFAULT));
            sink.flush();
        }
        ProducerRecord<byte[], byte[]> a = m.history().get(0), b = m.history().get(1);
        assertEquals("site:line2:station1", new String(a.key(), StandardCharsets.UTF_8));
        assertNull(a.timestamp(), "the broker stamps the append time");
        assertEquals("ot.dead-letter", b.topic());
        assertArrayEquals(new byte[] {9, 8}, b.value());
        assertEquals("birthless", new String(b.headers().lastHeader(Records.H_REASON).value(), StandardCharsets.UTF_8));
        assertEquals("42", new String(b.headers().lastHeader(Records.H_RECEIVED_AT).value(), StandardCharsets.UTF_8));
    }

    @Test
    void aFailedSendFailsTheRun() {
        MockProducer<byte[], byte[]> m = mock(false);
        KafkaSink sink = new KafkaSink(m);
        sink.send(out("a"));
        assertTrue(m.errorNext(new RuntimeException("broker said no")));
        BridgeFailure f = assertThrows(BridgeFailure.class, () -> sink.send(out("b")));
        assertTrue(f.getMessage().contains("broker said no"));
        assertThrows(BridgeFailure.class, sink::flush, "the failure stays");
    }

    @Test
    void aFailureSeenOnlyAtFlushFailsTheDrain() {
        MockProducer<byte[], byte[]> m = mock(false);
        KafkaSink sink = new KafkaSink(m);
        sink.send(out("a"));
        m.errorNext(new RuntimeException("not acknowledged"));
        assertThrows(BridgeFailure.class, sink::flush);
    }

    @Test
    void aFullInputQueueFailsTheRunInsteadOfDroppingTheFrame() throws Exception {
        FrameQueue q = new FrameQueue(1, 0);
        q.offer(new Frame("t", new byte[0], 1));
        q.offer(new Frame("t", new byte[0], 2));
        assertThrows(BridgeFailure.class, () -> q.poll(0));
    }

    @Test
    void aLostConnectionFailsTheRun() {
        FrameQueue q = new FrameQueue(4, 0);
        q.offer(new Frame("t", new byte[0], 1));
        q.fail(new BridgeFailure("MQTT connection lost"));
        assertThrows(BridgeFailure.class, q::pollNow);
    }

    @Test
    void framesLeaveTheQueueInArrivalOrder() throws Exception {
        FrameQueue q = new FrameQueue(4, 0);
        for (long i = 1; i <= 3; i++) q.offer(new Frame("t", new byte[0], i));
        assertEquals(1, q.poll(0).receivedAt());
        assertEquals(2, q.pollNow().receivedAt());
        assertEquals(3, q.pollNow().receivedAt());
        assertNull(q.pollNow());
    }
}
