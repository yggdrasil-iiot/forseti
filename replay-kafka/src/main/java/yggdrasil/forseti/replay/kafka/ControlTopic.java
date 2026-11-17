package yggdrasil.forseti.replay.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/** One single-partition control topic ({@code forseti.generations}, {@code forseti.recovery-control}; spec 467, 485-486, 748): an
 *  append-only log with one writer. The writer is a transactional producer whose {@code transactional.id} is fixed by the topic
 *  ({@link #transactionalId}); initializing it fences every earlier producer of that id, so of two writers only the latest can commit,
 *  and the fenced one fails without writing ({@code control-log-fenced}). Every append is its own transaction and returns only after the
 *  commit and after a {@code read_committed} read sees the record. Reads take the end offset first (with {@code read_committed}, the
 *  last stable offset), then every committed record below it. */
final class ControlTopic implements AutoCloseable {
    static final String FENCED = "control-log-fenced";
    static final Duration TIMEOUT = Duration.ofSeconds(60);

    /** One committed record. */
    record Rec(long offset, byte[] key, Map<String, String> headers, byte[] value) {}

    /** One read: the records below {@code endOffset}. */
    record Read(List<Rec> records, long endOffset) {}

    private final String bootstrap, topic;
    private final TopicPartition tp;
    private final KafkaProducer<byte[], byte[]> producer;
    private final Properties consumerConfig;
    private boolean fenced;

    static String transactionalId(String topic) { return topic + ".writer"; }

    /** {@code writer}: this instance may append (and fences every earlier writer of the topic now); a reader never initializes one. */
    ControlTopic(String bootstrap, String topic, boolean writer) {
        this.bootstrap = bootstrap;
        this.topic = topic;
        this.tp = new TopicPartition(topic, 0);
        this.consumerConfig = KafkaConfig.consumer(bootstrap, topic + ".reader");
        if (writer) {
            KafkaProducer<byte[], byte[]> p = new KafkaProducer<>(KafkaConfig.transactionalProducer(bootstrap, topic + ".writer", transactionalId(topic)));
            try { p.initTransactions(); } catch (KafkaException e) { p.close(Duration.ZERO); throw new IllegalStateException(topic + ": initTransactions: " + e, e); }
            this.producer = p;
        } else this.producer = null;
    }

    Properties consumerConfig() { return consumerConfig; }

    /** One record to append: key, headers, value. */
    record Out(byte[] key, Map<String, String> headers, byte[] value) {}

    long append(byte[] key, Map<String, String> headers, byte[] value) { return appendAll(List.of(new Out(key, headers, value))); }

    /** Appends {@code records} in one transaction and returns the offset of the last, once it is readable with {@code read_committed}. */
    long appendAll(List<Out> records) {
        if (producer == null) throw new IllegalStateException(topic + ": this store instance is a reader");
        if (fenced) throw new IllegalStateException(FENCED + ": " + topic + ": this writer was fenced by a later one");
        if (records.isEmpty()) throw new IllegalArgumentException("an empty append");
        long offset;
        try {
            producer.beginTransaction();
            List<java.util.concurrent.Future<RecordMetadata>> fs = new ArrayList<>();
            for (Out o : records) {
                List<Header> hs = new ArrayList<>();
                o.headers().forEach((k, v) -> hs.add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8))));
                fs.add(producer.send(new ProducerRecord<>(topic, 0, null, o.key(), o.value(), hs)));
            }
            long last = -1;
            for (java.util.concurrent.Future<RecordMetadata> f : fs) last = f.get().offset();
            producer.commitTransaction();
            offset = last;
        } catch (ProducerFencedException | InvalidProducerEpochException e) {
            fenced = true;
            throw new IllegalStateException(FENCED + ": " + topic + ": " + e.getMessage(), e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ProducerFencedException || e.getCause() instanceof InvalidProducerEpochException) {
                fenced = true;
                throw new IllegalStateException(FENCED + ": " + topic + ": " + e.getCause().getMessage(), e.getCause());
            }
            try { producer.abortTransaction(); } catch (KafkaException ignored) { /* the append failed either way */ }
            throw new IllegalStateException(topic + ": append: " + e.getCause(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(topic + ": append interrupted", e);
        } catch (KafkaException e) {
            if (e.getCause() instanceof ProducerFencedException || e.getCause() instanceof InvalidProducerEpochException) { fenced = true; throw new IllegalStateException(FENCED + ": " + topic + ": " + e.getMessage(), e); }
            throw new IllegalStateException(topic + ": append: " + e, e);
        }
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (true) {
            Read r = read();
            for (Rec x : r.records()) if (x.offset() == offset) return offset;
            if (System.nanoTime() > deadline) throw new IllegalStateException(topic + ": the committed record at " + offset + " is not readable with read_committed");
        }
    }

    Read read() {
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(consumerConfig)) {
            c.assign(List.of(tp));
            long end = c.endOffsets(List.of(tp), TIMEOUT).get(tp);
            c.seekToBeginning(List.of(tp));
            List<Rec> out = new ArrayList<>();
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (c.position(tp, TIMEOUT) < end) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(200))) {
                    if (r.offset() >= end) continue;
                    Map<String, String> h = new TreeMap<>();
                    for (Header x : r.headers()) h.put(x.key(), new String(x.value(), StandardCharsets.UTF_8));
                    out.add(new Rec(r.offset(), r.key(), h, r.value()));
                }
                if (System.nanoTime() > deadline) throw new IllegalStateException(topic + ": read below end offset " + end + " did not finish");
            }
            return new Read(List.copyOf(out), end);
        }
    }

    String topic() { return topic; }
    String bootstrap() { return bootstrap; }

    @Override public void close() { if (producer != null) producer.close(Duration.ofSeconds(5)); }
}
