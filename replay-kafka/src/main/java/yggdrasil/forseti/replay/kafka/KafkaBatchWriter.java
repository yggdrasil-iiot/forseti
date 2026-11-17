package yggdrasil.forseti.replay.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.cli.replay.BatchWriter;
import yggdrasil.forseti.cli.replay.CutBarrierRecord;
import yggdrasil.forseti.cli.replay.OffsetMap;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.journal.Progress;

/** {@link BatchWriter} over one idempotent producer with one request in flight, so each partition receives its records in the order
 *  they are sent. A batch is written in two acknowledged rounds: first the business records of every partition, in recording order, with
 *  their recorded key (UTF-8), headers (each a UTF-8 string) and value bytes as recorded (never re-serialized) to the recording's own
 *  partition, the broker offset of each added to the {@link OffsetMap}; then, on every partition, the cut's {@link CutBarrierRecord} whose
 *  {@code throughOffset} is the broker offset of the last business record the cut includes there. Record timestamps are the broker's
 *  ({@code LogAppendTime}); the recorded timestamps are not sent. A recorded record that carries the barrier header is refused. */
public final class KafkaBatchWriter implements BatchWriter {
    private final KafkaProducer<byte[], byte[]> producer;
    private final Properties config;
    private final Recording rec;
    private final long generation;
    private final String routingDigest;
    private final Map<String, String> names;
    private final Map<String, List<Integer>> partitions;
    private final Map<String, Recording.Reader> readers = new HashMap<>();
    private final Map<String, Long> previous = new HashMap<>();
    private final OffsetMap offsets = new OffsetMap();

    /** {@code names}: the run's topic name of each input topic's contract name; {@code partitions}: the partitions written per input topic. */
    public KafkaBatchWriter(String bootstrap, Map<String, String> names, Recording rec, long generation, Map<String, List<Integer>> partitions) {
        this.config = KafkaConfig.inputProducer(bootstrap, "forseti-harness-batch-writer");
        this.producer = new KafkaProducer<>(config);
        this.rec = rec;
        this.generation = generation;
        this.routingDigest = rec.routing().routingDigest();
        this.names = Map.copyOf(names);
        Map<String, List<Integer>> p = new LinkedHashMap<>();
        for (String t : RoutingManifest.TOPICS) if (partitions.containsKey(t)) p.put(t, List.copyOf(new TreeSet<>(partitions.get(t))));
        this.partitions = Collections.unmodifiableMap(p);
        for (Map.Entry<String, List<Integer>> e : this.partitions.entrySet()) {
            if (!names.containsKey(e.getKey())) throw new IllegalArgumentException("no topic name for " + e.getKey());
            for (int x : e.getValue()) offsets.declare(e.getKey(), x);
        }
    }

    public Properties config() { return config; }

    @Override public Written write(Recording.Batch b, long restartEpoch, String recoveryRef) {
        List<CutAssembler.Idle> idle = new ArrayList<>();
        for (JsonNode x : b.idleInputs()) idle.add(CutAssembler.Idle.fromJson(x));
        // (1) the business records
        record Sent(String topic, int partition, long recordingOffset, Future<RecordMetadata> f) {}
        List<Sent> sent = new ArrayList<>();
        Map<String, Map<Integer, Long>> through = new LinkedHashMap<>();
        for (Map.Entry<String, List<Integer>> e : partitions.entrySet()) {
            String t = e.getKey();
            for (int p : e.getValue()) {
                String k = t + "/" + p;
                long from = previous.getOrDefault(k, -1L) + 1, to = b.through(t, p);
                Recording.Reader reader = readers.computeIfAbsent(k, x -> rec.reader(t, p));
                for (long off = from; off <= to; off++) {
                    Recording.Line l = reader.next();
                    if (l == null || l.record().offset() != off) throw new IllegalStateException("recording reader of " + k + " is not at offset " + off);
                    sent.add(new Sent(t, p, off, producer.send(record(names.get(t), p, l))));
                }
                previous.put(k, to);
                through.computeIfAbsent(t, x -> new TreeMap<>()).put(p, to);
            }
        }
        producer.flush();
        for (Sent s : sent) offsets.put(s.topic(), s.partition(), s.recordingOffset(), get(s.f()).offset());
        // (2) the barriers
        Map<String, Map<Integer, Long>> brokerThrough = new LinkedHashMap<>();
        List<Future<RecordMetadata>> barriers = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Long>> e : through.entrySet())
            for (Map.Entry<Integer, Long> x : e.getValue().entrySet()) {
                long bt = offsets.throughBroker(e.getKey(), x.getKey(), x.getValue());
                brokerThrough.computeIfAbsent(e.getKey(), y -> new TreeMap<>()).put(x.getKey(), bt);
                CutBarrierRecord br = new CutBarrierRecord(generation, routingDigest,
                    new CutAssembler.Barrier(b.cutId(), new Progress.Origin(e.getKey(), x.getKey()), bt, restartEpoch, recoveryRef, idle));
                List<Header> hs = List.of(new RecordHeader(CutBarrierRecord.TYPE_HEADER, CutBarrierRecord.TYPE.getBytes(StandardCharsets.UTF_8)),
                    new RecordHeader(CutBarrierRecord.GENERATION_HEADER, Long.toString(generation).getBytes(StandardCharsets.UTF_8)));
                barriers.add(producer.send(new ProducerRecord<>(names.get(e.getKey()), x.getKey(), null, null, Canonical.bytes(br.toJson()), hs)));
            }
        producer.flush();
        for (Future<RecordMetadata> f : barriers) get(f);
        return new Written(b.cutId(), sent.size(), barriers.size(), brokerThrough);
    }

    private static ProducerRecord<byte[], byte[]> record(String topic, int partition, Recording.Line l) {
        List<Header> hs = new ArrayList<>();
        JsonNode h = l.record().headers();
        if (h != null) h.fields().forEachRemaining(e -> {
            if (e.getKey().equals(CutBarrierRecord.TYPE_HEADER)) throw new IllegalArgumentException("a recorded input carries the barrier header " + CutBarrierRecord.TYPE_HEADER);
            if (!e.getValue().isTextual()) throw new IllegalArgumentException("a recorded header is not a string: " + e.getKey());
            hs.add(new RecordHeader(e.getKey(), e.getValue().textValue().getBytes(StandardCharsets.UTF_8)));
        });
        byte[] key = l.record().key() == null ? null : l.record().key().getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topic, partition, null, key, Recording.valueBytes(l.bytes()), hs);
    }

    private static RecordMetadata get(Future<RecordMetadata> f) {
        try { return f.get(); }
        catch (ExecutionException e) { throw new IllegalStateException("produce: " + e.getCause(), e.getCause()); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("produce interrupted", e); }
    }

    @Override public OffsetMap offsets() { return offsets; }

    /** The input topics' run names for {@code plan}. */
    public static Map<String, String> inputNames(List<ReplayTopics.Spec> plan) {
        Map<String, String> m = new LinkedHashMap<>();
        for (String t : ReplayTopics.INPUTS) m.put(t, ReplayTopics.nameOf(plan, t));
        return m;
    }

    @Override public void close() {
        for (Recording.Reader r : readers.values()) r.close();
        producer.close(Duration.ofSeconds(5));
    }
}
