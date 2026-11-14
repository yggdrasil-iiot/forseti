package yggdrasil.forseti.bridge;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** The Kafka writer: an idempotent producer with {@code acks=all}, so a retried batch is never written twice and a record counts as
 *  written only once every in-sync replica has it, and a bounded buffer ({@code buffer.memory}, {@code max.block.ms}), so a stalled
 *  broker stops the bridge instead of growing it. The first failed send is kept and thrown from the next {@link #send} or
 *  {@link #flush}: the run fails and nothing is dropped silently. Record timestamps are left to the broker (the input topics use
 *  {@code LogAppendTime}). */
public final class KafkaSink implements Sink {
    private final Producer<byte[], byte[]> producer;
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    public KafkaSink(Producer<byte[], byte[]> producer) { this.producer = producer; }

    /** The producer settings of the bridge; {@code bufferBytes} bounds the queue of unsent records. */
    public static Properties config(String bootstrap, String clientId, long bufferBytes) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.toString(bufferBytes));
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return p;
    }

    public static KafkaSink connect(String bootstrap, String clientId, long bufferBytes) {
        return new KafkaSink(new KafkaProducer<>(config(bootstrap, clientId, bufferBytes)));
    }

    @Override public void send(Records.Out r) {
        check();
        ProducerRecord<byte[], byte[]> pr = new ProducerRecord<>(r.topic(), null, null, r.key().getBytes(StandardCharsets.UTF_8), r.value());
        for (Map.Entry<String, String> h : r.headers().entrySet()) pr.headers().add(h.getKey(), h.getValue().getBytes(StandardCharsets.UTF_8));
        try {
            producer.send(pr, (meta, ex) -> { if (ex != null) failure.compareAndSet(null, ex); });
        } catch (RuntimeException ex) {
            failure.compareAndSet(null, ex);
        }
        check();
    }

    @Override public void flush() {
        try { producer.flush(); } catch (RuntimeException ex) { failure.compareAndSet(null, ex); }
        check();
    }

    private void check() {
        Exception ex = failure.get();
        if (ex != null) throw new BridgeFailure("a Kafka send failed: " + ex, ex);
    }

    @Override public void close() { producer.close(); }
}
