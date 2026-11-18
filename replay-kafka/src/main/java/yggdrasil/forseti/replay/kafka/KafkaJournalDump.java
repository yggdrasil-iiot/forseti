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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import yggdrasil.forseti.cli.replay.JournalDump;
import yggdrasil.forseti.cli.replay.JournalTranslation;
import yggdrasil.forseti.cli.replay.OffsetMap;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** {@link JournalDump} with one consumer, {@code isolation.level=read_committed} set explicitly (plan D10): for every output journal of
 *  the plan, every partition is read from offset 0 up to its end offset taken before the read (the last stable offset, so an aborted or
 *  open transaction contributes nothing), each record refused unless its key and value are RFC 8785 bytes and it carries the run's
 *  {@code forseti-generation} (progress and delete records included), turned into its step-3 journal line ({@link OutputRecords#line}), translated to
 *  recording offsets ({@link JournalTranslation}) and written as RFC 8785 bytes and an LF. A partition without a record gets no file, as
 *  the step-3 journals have none. */
public final class KafkaJournalDump implements JournalDump {
    static final Duration TIMEOUT = Duration.ofSeconds(60);
    /** How long a partition's read may go without its position advancing. The read itself has no total budget: a horizon partition
     *  holds close to a million records (2.1 GB of samples), read, checked canonical and translated one by one, and how long that takes
     *  depends on the host; what fails the dump is a read that stops moving below its target. */
    static final Duration STALL = Duration.ofSeconds(60);
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Admin admin;
    private final Properties config;
    private final List<ReplayTopics.Spec> outputs;
    private final long generation;
    private final Duration stall;
    private final Runnable perRecord;
    private final com.fasterxml.jackson.databind.node.ArrayNode stats = Json.JSON.createArrayNode();

    /** {@code generation}: the run's; every record must carry it ({@link OutputRecords#line(OutputRecords.Transport, long)}). */
    public KafkaJournalDump(String bootstrap, List<ReplayTopics.Spec> plan, long generation) { this(bootstrap, plan, generation, STALL, () -> {}); }

    /** {@code stall}: the longest read without progress; {@code perRecord}: run after every record dumped (a test's slow host). */
    KafkaJournalDump(String bootstrap, List<ReplayTopics.Spec> plan, long generation, Duration stall, Runnable perRecord) {
        this.generation = generation;
        this.stall = stall;
        this.perRecord = perRecord;
        this.config = KafkaConfig.consumer(bootstrap, "forseti-harness-journal-dump");
        this.consumer = new KafkaConsumer<>(config);
        this.admin = Admin.create(KafkaConfig.admin(bootstrap, "forseti-harness-journal-dump-offsets"));
        List<ReplayTopics.Spec> l = new ArrayList<>();
        for (ReplayTopics.Spec s : plan) if (s.role() == ReplayTopics.Role.OUTPUT) l.add(s);
        this.outputs = List.copyOf(l);
    }

    public Properties config() { return config; }

    /** Per partition read: {@code topic}, {@code partition}, {@code end} (the committed end the read went to), {@code highWatermark}
     *  (the log end, open and aborted transactions included, when the read started), {@code records} and {@code elapsedMs}. */
    public com.fasterxml.jackson.databind.node.ArrayNode stats() { return stats; }

    /** {@code <out>/journals/<topic>/<p>.jsonl}, the quarantine at {@code <out>/quarantine/<p>.jsonl}. */
    static Path file(Path out, String topic, int partition) {
        return topic.equals(ReplayTopics.QUARANTINE) ? out.resolve("quarantine").resolve(partition + ".jsonl")
            : out.resolve("journals").resolve(topic).resolve(partition + ".jsonl");
    }

    /** The log end of {@code tp} with every record counted (the high watermark): never read, only listed by the admin client. */
    private long highWatermark(TopicPartition tp) {
        try {
            return admin.listOffsets(Map.of(tp, OffsetSpec.latest()), new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)).partitionResult(tp)
                .get(TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS).offset();
        } catch (Exception e) { throw new IllegalStateException(tp + ": high watermark: " + e.getMessage(), e); }
    }

    @Override public Map<String, Long> dump(Path out, OffsetMap offsets) {
        Map<String, Long> counts = new TreeMap<>();
        for (ReplayTopics.Spec s : outputs) for (int p = 0; p < s.partitions(); p++) {
            TopicPartition tp = new TopicPartition(s.name(), p);
            consumer.assign(List.of(tp));
            long end = consumer.endOffsets(List.of(tp), TIMEOUT).get(tp);
            long high = highWatermark(tp);
            if (end == 0) continue;
            consumer.seekToBeginning(List.of(tp));
            long n = 0, started = System.nanoTime();
            OutputStream o = null;
            try {
                long position = consumer.position(tp, TIMEOUT), moved = System.nanoTime();
                while (position < end) {
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
                        perRecord.run();
                    }
                    long now = consumer.position(tp, TIMEOUT);
                    if (now > position) { position = now; moved = System.nanoTime(); }
                    else if (System.nanoTime() - moved > stall.toNanos()) throw stalled(tp, end, high, position, n, started);
                }
            } catch (IOException e) { throw new UncheckedIOException(e); }
            finally { if (o != null) try { o.close(); } catch (IOException e) { throw new UncheckedIOException(e); } }
            ObjectNode st = stats.addObject().put("topic", s.base()).put("partition", p);
            Json.putSafe(st, "end", end);
            Json.putSafe(st, "highWatermark", high);
            Json.putSafe(st, "records", n);
            Json.putSafe(st, "elapsedMs", (System.nanoTime() - started) / 1_000_000);
            if (n > 0) counts.put(s.base() + "/" + p, n);
        }
        consumer.assign(List.of());
        return counts;
    }

    /** The failure of a read that stopped moving below its target, with which of the causes it was. The target is the committed end
     *  (the last stable offset) taken before the read, so an open transaction lies above it and cannot hold the read; a transaction
     *  marker or an aborted record below it is stepped over by a {@code read_committed} consumer, which advances its position past them
     *  without delivering them; and the read has no total time budget, only this one for a position that does not advance. */
    private IllegalStateException stalled(TopicPartition tp, long end, long highAtStart, long position, long records, long started) {
        long committedNow = consumer.endOffsets(List.of(tp), TIMEOUT).get(tp), highNow = highWatermark(tp);
        String cause;
        if (committedNow < end) cause = "the committed end fell from " + end + " to " + committedNow + " (the partition was truncated or deleted)";
        else cause = "the broker delivers nothing at position " + position + " below the committed end " + end
            + " (offsets read_committed never delivers and the consumer does not step over: transaction markers or aborted records)";
        String open = highAtStart > end ? "; " + (highAtStart - end) + " offsets above the target were not committed when the read started (open transactions of the jobs)" : "";
        return new IllegalStateException("dump of " + tp + " below " + end + " did not finish: no progress for " + stall + " at position " + position + " after "
            + records + " records in " + (System.nanoTime() - started) / 1_000_000 + " ms; " + cause + "; committed end now " + committedNow + ", high watermark now " + highNow
            + open + "; not a time budget, the read has none");
    }

    @Override public void close() { consumer.close(); admin.close(); }
}
