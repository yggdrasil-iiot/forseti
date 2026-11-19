package yggdrasil.forseti.replay.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.core.Json;

/** The harness's records of the progress partitions around a restart injection (spec 416, 685: the expected and the attributed sets are
 *  computed from records the harness takes before and after the injection, never from {@code initEvidence}). {@link #snapshot} reads,
 *  for each progress partition, the log end with every record counted (the high watermark, {@code read_uncommitted}): every record a
 *  later incarnation of a writer appends lies at or above it; and, with {@code read_committed}, the cut of its last committed progress.
 *  {@link #progress} reads every committed progress record of the partitions with its Kafka offset, so the gate can tell which progress
 *  records were appended after an injection. Both are fresh reads, never a cached value. */
public final class KafkaProgressLog implements AutoCloseable {
    static final Duration TIMEOUT = Duration.ofSeconds(60);
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Admin admin;
    private final Properties config;
    private final long generation;

    public KafkaProgressLog(String bootstrap, long generation) {
        this.generation = generation;
        this.config = KafkaConfig.consumer(bootstrap, "forseti-harness-progress-log");
        this.consumer = new KafkaConsumer<>(config);
        this.admin = Admin.create(KafkaConfig.admin(bootstrap, "forseti-harness-progress-log-offsets"));
    }

    public Properties config() { return config; }

    /** One committed progress record of a partition: its Kafka offset and its value. */
    public record Entry(long offset, JsonNode value) {}

    /** The high watermark of {@code tp}: the log end, open and aborted transactions included. */
    public long highWatermark(TopicPartition tp) {
        try {
            return admin.listOffsets(Map.of(tp, OffsetSpec.latest()), new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)).partitionResult(tp)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).offset();
        } catch (Exception e) { throw new IllegalStateException(tp + ": high watermark: " + e.getMessage(), e); }
    }

    /** Every committed progress record of {@code tp} below its committed end taken now, in offset order. */
    public List<Entry> progress(TopicPartition tp) {
        List<Entry> out = new ArrayList<>();
        consumer.assign(List.of(tp));
        long end = consumer.endOffsets(List.of(tp), TIMEOUT).get(tp);
        if (end == 0) { consumer.assign(List.of()); return out; }
        consumer.seekToBeginning(List.of(tp));
        long position = consumer.position(tp, TIMEOUT), moved = System.nanoTime();
        while (position < end) {
            for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(200))) {
                if (r.offset() >= end) continue;
                Map<String, String> h = new TreeMap<>();
                for (Header x : r.headers()) h.put(x.key(), new String(x.value(), StandardCharsets.UTF_8));
                JsonNode v;
                try { v = OutputRecords.line(new OutputRecords.Transport(r.key(), h, r.value()), generation).get("value"); }
                catch (IllegalArgumentException e) { throw new IllegalStateException(tp + " offset " + r.offset() + ": " + e.getMessage(), e); }
                if (OutputRecords.kind(v) == OutputRecords.Kind.PROGRESS) out.add(new Entry(r.offset(), v));
            }
            long now = consumer.position(tp, TIMEOUT);
            if (now > position) { position = now; moved = System.nanoTime(); }
            else if (System.nanoTime() - moved > TIMEOUT.toNanos()) throw new IllegalStateException(tp + ": the progress read stopped at " + position + " below " + end);
        }
        consumer.assign(List.of());
        return out;
    }

    /** For each partition: {@code highWatermark}, {@code lastProgressCutId} (the cut of its last committed progress, 0 for none) and
     *  {@code committedProgress} (how many committed progress records it holds). Keys are {@code <topic>/<partition>}. */
    public ObjectNode snapshot(Map<String, TopicPartition> partitions) {
        ObjectNode n = Json.object();
        for (Map.Entry<String, TopicPartition> e : partitions.entrySet()) {
            long high = highWatermark(e.getValue());
            List<Entry> p = progress(e.getValue());
            ObjectNode x = n.putObject(e.getKey());
            Json.putSafe(x, "highWatermark", high);
            Json.putSafe(x, "lastProgressCutId", p.isEmpty() ? 0 : p.get(p.size() - 1).value().path("cutId").asLong());
            Json.putSafe(x, "committedProgress", p.size());
        }
        return n;
    }

    @Override public void close() { consumer.close(); admin.close(); }
}
