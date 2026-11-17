package yggdrasil.forseti.replay.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import yggdrasil.forseti.cli.generation.FileGenerationStore;
import yggdrasil.forseti.cli.generation.OperatorStore;
import yggdrasil.forseti.core.release.LockedFile;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationPolicy;
import yggdrasil.forseti.core.generation.GenerationStore;

/** {@code forseti.generations} as a single-partition Kafka topic (spec 485, 557-575, 748): the row shape of the step-3 file store
 *  unchanged. One Kafka record per row: the key is the scope (UTF-8), the header {@code forseti-generation} the generation number, the
 *  value the RFC 8785 bytes of the row. {@code registeredRowSha256} is the digest of the same transport record
 *  {@code {topic: "forseti.generations", key, headers, value}} the file store digests ({@link FileGenerationStore#record}), so a
 *  generation has the same registration digest in both stores whatever the run's topic prefix. Every read validates the whole history
 *  ({@link GenerationPolicy#validate}); a value that is not the canonical bytes of a row, a header that disagrees with its row, or an
 *  invalid history fails closed ({@code control-log-corrupt}). An append validates the history with the new row first; the single
 *  writer is the topic's fixed {@code transactional.id} ({@link ControlTopic}): only the operator CLI holds a writer, jobs and the
 *  harness open readers. */
public final class KafkaGenerationStore implements OperatorStore {
    private final ControlTopic log;

    /** {@code writer}: this instance appends (and fences every earlier writer of {@code topic}). */
    public KafkaGenerationStore(String bootstrap, String topic, boolean writer) { this.log = new ControlTopic(bootstrap, topic, writer); }

    @Override public void append(String scope, Generation row) {
        List<Entry> next = new ArrayList<>(all());
        next.add(new Entry(scope, row));
        try { GenerationPolicy.validate(next); }
        catch (IllegalStateException e) { throw new IllegalArgumentException("the row would make the history invalid: " + e.getMessage(), e); }
        log.append(scope.getBytes(StandardCharsets.UTF_8), Map.of(FileGenerationStore.HEADER, Long.toString(row.generation())), Canonical.bytes(row.toJson()));
    }

    /** A promotion batch: the history validated with every row, then every row in one transaction of the fenced writer, so a reader
     *  sees all of them or none. */
    @Override public void appendBatch(List<Entry> rows) {
        if (rows.isEmpty()) throw new IllegalArgumentException("an empty batch");
        List<Entry> next = new ArrayList<>(all());
        next.addAll(rows);
        try { GenerationPolicy.validate(next); }
        catch (IllegalStateException e) { throw new IllegalArgumentException("the batch would make the history invalid: " + e.getMessage(), e); }
        List<ControlTopic.Out> out = new ArrayList<>();
        for (Entry e : rows) out.add(new ControlTopic.Out(e.scope().getBytes(StandardCharsets.UTF_8), Map.of(FileGenerationStore.HEADER, Long.toString(e.row().generation())),
            Canonical.bytes(e.row().toJson())));
        log.appendAll(out);
    }

    /** The single fenced writer is the hold: the body runs as is. */
    @Override public <T> T locked(LockedFile.Body<T> body) {
        try { return body.run(); } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
    }

    /** The transport records {@code {topic: "forseti.generations", key, headers, value}} of every row, as the file store stores them. */
    @Override public byte[] bytes() {
        java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
        for (Entry e : all()) { b.writeBytes(Canonical.bytes(FileGenerationStore.record(e.scope(), e.row()))); b.write(10); }
        return b.toByteArray();
    }

    @Override public List<Entry> all() { return entries(log.read()); }

    /** The digest of the transport record of the {@code registered} row of {@code generation}, if registered. */
    @Override public Optional<String> registeredRowSha256(long generation) {
        return registered(generation).map(e -> Canonical.digest(FileGenerationStore.record(e.scope(), e.row())));
    }

    /** The effective configuration of this store's {@code read_committed} reader. */
    public Properties readerConfig() { return log.consumerConfig(); }

    private List<Entry> entries(ControlTopic.Read r) {
        List<Entry> out = new ArrayList<>();
        for (ControlTopic.Rec x : r.records()) {
            if (x.key() == null) throw corrupt("offset " + x.offset() + " has no scope key");
            if (x.value() == null) throw corrupt("offset " + x.offset() + " has no value");
            JsonNode v;
            try { v = Json.read(x.value()); } catch (IllegalArgumentException e) { throw corrupt("offset " + x.offset() + " is not JSON"); }
            if (!Arrays.equals(x.value(), Canonical.bytes(v))) throw corrupt("offset " + x.offset() + " is not the canonical bytes of its row");
            Generation g;
            try { g = Generation.fromJson(v); } catch (IllegalArgumentException e) { throw corrupt("offset " + x.offset() + ": " + e.getMessage()); }
            if (x.headers().size() != 1 || !Long.toString(g.generation()).equals(x.headers().get(FileGenerationStore.HEADER)))
                throw corrupt("offset " + x.offset() + " header " + FileGenerationStore.HEADER);
            out.add(new Entry(new String(x.key(), StandardCharsets.UTF_8), g));
        }
        try { GenerationPolicy.validate(out); }
        catch (IllegalStateException e) { throw corrupt(e.getMessage().substring(GenerationPolicy.CORRUPT.length() + 2)); }
        return List.copyOf(out);
    }

    private IllegalStateException corrupt(String what) { return new IllegalStateException(GenerationPolicy.CORRUPT + ": " + log.topic() + ": " + what); }

    @Override public void close() { log.close(); }
}
