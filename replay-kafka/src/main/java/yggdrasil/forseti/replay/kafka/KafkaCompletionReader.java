package yggdrasil.forseti.replay.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import yggdrasil.forseti.cli.replay.CompletionReader;
import yggdrasil.forseti.cli.replay.OutputRecords;

/** {@link CompletionReader} over one {@code read_committed} consumer assigned to every progress partition the run can wait for, from
 *  offset 0: every committed progress record is kept per partition, so a wait for a cut sees the progress that closed it however long
 *  ago it was committed. Every record read must carry the run's {@code forseti-generation} header (progress and delete included). */
public final class KafkaCompletionReader implements CompletionReader {
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Properties config;
    private final Map<String, String> contractOf = new HashMap<>();
    private final Map<Writer, List<JsonNode>> progress = new HashMap<>();
    private final long generation;

    /** {@code names}: the run's topic name of each output topic's contract name; {@code writers}: every partition to read;
     *  {@code generation}: the run's, which every record read must carry. */
    public KafkaCompletionReader(String bootstrap, Map<String, String> names, Collection<Writer> writers, long generation) {
        this.generation = generation;
        this.config = KafkaConfig.consumer(bootstrap, "forseti-harness-completion");
        this.consumer = new KafkaConsumer<>(config);
        names.forEach((c, n) -> contractOf.put(n, c));
        List<TopicPartition> tps = new ArrayList<>();
        for (Writer w : new LinkedHashSet<>(writers)) {
            String n = names.get(w.topic());
            if (n == null) throw new IllegalArgumentException("no topic name for " + w.topic());
            tps.add(new TopicPartition(n, w.partition()));
            progress.put(w, new ArrayList<>());
        }
        consumer.assign(tps);
        consumer.seekToBeginning(tps);
    }

    public Properties config() { return config; }

    @Override public Map<Writer, JsonNode> await(long cutId, List<Writer> expected, Duration timeout) {
        for (Writer w : expected) if (!progress.containsKey(w)) throw new IllegalArgumentException("the completion reader does not read " + w);
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            Map<Writer, JsonNode> found = new LinkedHashMap<>();
            List<Writer> missing = new ArrayList<>();
            for (Writer w : expected) {
                JsonNode p = closing(progress.get(w), cutId);
                if (p == null) missing.add(w); else found.put(w, p);
            }
            if (missing.isEmpty()) return found;
            if (System.nanoTime() > deadline) throw new IllegalStateException("cut " + cutId + " did not complete within " + timeout + ": no progress on " + missing);
            poll(Duration.ofMillis(100));
        }
    }

    @Override public Map<Writer, JsonNode> latest(Collection<Writer> writers, Duration poll) {
        for (Writer w : writers) if (!progress.containsKey(w)) throw new IllegalArgumentException("the completion reader does not read " + w);
        poll(poll);
        Map<Writer, JsonNode> m = new LinkedHashMap<>();
        for (Writer w : writers) { List<JsonNode> l = progress.get(w); if (!l.isEmpty()) m.put(w, l.get(l.size() - 1)); }
        return m;
    }

    /** One poll: every committed progress record read is kept under its writer. */
    private void poll(Duration d) {
        for (ConsumerRecord<byte[], byte[]> r : consumer.poll(d)) {
            Map<String, String> h = new TreeMap<>();
            for (org.apache.kafka.common.header.Header x : r.headers()) h.put(x.key(), new String(x.value(), java.nio.charset.StandardCharsets.UTF_8));
            JsonNode v;
            try { v = OutputRecords.line(new OutputRecords.Transport(r.key(), h, r.value()), generation).get("value"); }
            catch (IllegalArgumentException e) { throw new IllegalStateException(r.topic() + "/" + r.partition() + " offset " + r.offset() + ": " + e.getMessage(), e); }
            if (OutputRecords.kind(v) != OutputRecords.Kind.PROGRESS) continue;
            progress.get(new Writer(contractOf.get(r.topic()), r.partition())).add(v);
        }
    }

    /** The progress that closes {@code cutId}: its own, or a later one that merged it. */
    static JsonNode closing(List<JsonNode> seen, long cutId) {
        for (JsonNode p : seen) {
            long c = p.path("cutId").asLong();
            if (c == cutId) return p;
            if (c > cutId) for (JsonNode m : p.path("mergedCutIds")) if (m.asLong() == cutId) return p;
        }
        return null;
    }

    @Override public void close() { consumer.close(); }
}
