package yggdrasil.forseti.replay.kafka;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import yggdrasil.forseti.cli.replay.JournalDump;
import yggdrasil.forseti.cli.replay.JournalTranslation;
import yggdrasil.forseti.cli.replay.OffsetMap;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.canon.Canonical;

/** {@link JournalDump} with one consumer, {@code isolation.level=read_committed} set explicitly (plan D10): for every output journal of
 *  the plan, every partition is read from offset 0 up to its end offset taken before the read (the last stable offset, so an aborted or
 *  open transaction contributes nothing), each record refused unless its key and value are RFC 8785 bytes and it carries the run's
 *  {@code forseti-generation} (progress and delete records included), turned into its step-3 journal line ({@link OutputRecords#line}), translated to
 *  recording offsets ({@link JournalTranslation}) and written as RFC 8785 bytes and an LF. A partition without a record gets no file, as
 *  the step-3 journals have none. */
public final class KafkaJournalDump implements JournalDump {
    static final Duration TIMEOUT = Duration.ofSeconds(60);
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Properties config;
    private final List<ReplayTopics.Spec> outputs;
    private final long generation;

    /** {@code generation}: the run's; every record must carry it ({@link OutputRecords#line(OutputRecords.Transport, long)}). */
    public KafkaJournalDump(String bootstrap, List<ReplayTopics.Spec> plan, long generation) {
        this.generation = generation;
        this.config = KafkaConfig.consumer(bootstrap, "forseti-harness-journal-dump");
        this.consumer = new KafkaConsumer<>(config);
        List<ReplayTopics.Spec> l = new ArrayList<>();
        for (ReplayTopics.Spec s : plan) if (s.role() == ReplayTopics.Role.OUTPUT) l.add(s);
        this.outputs = List.copyOf(l);
    }

    public Properties config() { return config; }

    /** {@code <out>/journals/<topic>/<p>.jsonl}, the quarantine at {@code <out>/quarantine/<p>.jsonl}. */
    static Path file(Path out, String topic, int partition) {
        return topic.equals(ReplayTopics.QUARANTINE) ? out.resolve("quarantine").resolve(partition + ".jsonl")
            : out.resolve("journals").resolve(topic).resolve(partition + ".jsonl");
    }

    @Override public Map<String, Long> dump(Path out, OffsetMap offsets) {
        Map<String, Long> counts = new TreeMap<>();
        for (ReplayTopics.Spec s : outputs) for (int p = 0; p < s.partitions(); p++) {
            TopicPartition tp = new TopicPartition(s.name(), p);
            consumer.assign(List.of(tp));
            long end = consumer.endOffsets(List.of(tp), TIMEOUT).get(tp);
            if (end == 0) continue;
            consumer.seekToBeginning(List.of(tp));
            long n = 0;
            OutputStream o = null;
            try {
                long deadline = System.nanoTime() + TIMEOUT.toNanos();
                while (consumer.position(tp, TIMEOUT) < end) {
                    for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(200))) {
                        if (r.offset() >= end) continue;
                        ObjectNode line;
                        Map<String, String> h = new TreeMap<>();
                        for (Header x : r.headers()) h.put(x.key(), new String(x.value(), StandardCharsets.UTF_8));
                        OutputRecords.Transport t = new OutputRecords.Transport(r.key(), h, r.value());
                        try {
                            OutputRecords.requireCanonical(t);
                            line = OutputRecords.line(t, generation);
                        } catch (IllegalArgumentException e) { throw new IllegalStateException(tp + " offset " + r.offset() + ": " + e.getMessage(), e); }
                        JournalTranslation.toRecording(s.base(), line, offsets);
                        if (o == null) {
                            Path f = file(out, s.base(), p);
                            Files.createDirectories(f.getParent());
                            o = new BufferedOutputStream(Files.newOutputStream(f), 1 << 16);
                        }
                        o.write(Canonical.bytes(line));
                        o.write('\n');
                        n++;
                    }
                    if (System.nanoTime() > deadline) throw new IllegalStateException("dump of " + tp + " below " + end + " did not finish");
                }
            } catch (IOException e) { throw new UncheckedIOException(e); }
            finally { if (o != null) try { o.close(); } catch (IOException e) { throw new UncheckedIOException(e); } }
            if (n > 0) counts.put(s.base() + "/" + p, n);
        }
        consumer.assign(List.of());
        return counts;
    }

    @Override public void close() { consumer.close(); }
}
