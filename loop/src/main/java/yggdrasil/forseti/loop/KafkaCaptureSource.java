package yggdrasil.forseti.loop;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import yggdrasil.forseti.cli.live.CaptureRecorder;

/** The recorder's Kafka reader (step-5 plan D6): one partition at a time, assigned (no group, nothing committed), from offset 0 to the
 *  end offset read once with {@code isolation.level=read_committed}, so the capture is a fixed prefix of the log taken after the
 *  publishers stopped and the bridge drained. Records at or past the end offset are not kept. A partition that does not reach its end
 *  offset within {@code timeoutMs} fails the capture. */
public final class KafkaCaptureSource implements CaptureRecorder.Source, AutoCloseable {
    private final Consumer<byte[], byte[]> consumer;
    private final long timeoutMs;

    public KafkaCaptureSource(Consumer<byte[], byte[]> consumer, long timeoutMs) { this.consumer = consumer; this.timeoutMs = timeoutMs; }

    public static Consumer<byte[], byte[]> kafka(String bootstrap, String clientId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(p);
    }

    @Override public long beginningOffset(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return consumer.beginningOffsets(List.of(tp)).get(tp);
    }

    @Override public long endOffset(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return consumer.endOffsets(List.of(tp)).get(tp);
    }

    @Override public List<CaptureRecorder.Raw> read(String topic, int partition, long endOffset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        List<CaptureRecorder.Raw> out = new ArrayList<>();
        if (endOffset == 0) return out;
        consumer.assign(List.of(tp));
        consumer.seek(tp, 0);
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (consumer.position(tp) < endOffset) {
            if (System.currentTimeMillis() > deadline) throw new IllegalStateException(topic + "/" + partition + " did not reach its end offset " + endOffset + " within " + timeoutMs + " ms");
            for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(200)).records(tp)) {
                if (r.offset() >= endOffset) continue;
                Map<String, byte[]> headers = new LinkedHashMap<>();
                for (Header h : r.headers()) headers.put(h.key(), h.value());
                out.add(new CaptureRecorder.Raw(r.offset(), r.timestamp(), r.timestampType().name, r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8),
                    headers, r.value()));
            }
        }
        consumer.unsubscribe();
        return out;
    }

    @Override public void close() { consumer.close(); }
}
