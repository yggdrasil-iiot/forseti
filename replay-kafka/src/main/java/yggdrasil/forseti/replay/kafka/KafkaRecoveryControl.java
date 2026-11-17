package yggdrasil.forseti.replay.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.cli.replay.RecoveryControlStore;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.GenerationPolicy;

/** {@code forseti.recovery-control} as a single-partition Kafka topic (spec 416, 486, 748), the declaration shape of the step-3 file
 *  unchanged: one record per declaration, the key and the header {@code forseti-generation} the generation number, the value the RFC 8785
 *  bytes of the declaration. Only the harness holds a writer (its fixed {@code transactional.id}, {@link ControlTopic}); every stage-1
 *  progress writer reads at its initialization ({@link #read}: the end offset H, then every committed declaration below it). A value
 *  that is not a canonical JSON object, or a header that disagrees with its generation, fails closed ({@code control-log-corrupt}). */
public final class KafkaRecoveryControl implements RecoveryControlStore {
    private static final Set<String> FIELDS = Set.of("generation", "injectionId", "stage", "epochBefore", "targetEpoch", "lastWrittenCutId", "routingDigest");
    private final ControlTopic log;

    public KafkaRecoveryControl(String bootstrap, String topic, boolean writer) { this.log = new ControlTopic(bootstrap, topic, writer); }

    @Override public long append(JsonNode declaration) {
        check(declaration, "the declaration");
        long g = Json.safeLong(declaration.get("generation"), "generation", 1, Json.MAX_SAFE);
        byte[] gen = Long.toString(g).getBytes(StandardCharsets.UTF_8);
        return log.append(gen, Map.of(OutputRecords.GENERATION_HEADER, Long.toString(g)), Canonical.bytes(declaration));
    }

    @Override public Snapshot read() {
        ControlTopic.Read r = log.read();
        List<Declaration> out = new ArrayList<>();
        for (ControlTopic.Rec x : r.records()) {
            JsonNode v;
            try { v = Json.read(x.value()); } catch (IllegalArgumentException | NullPointerException e) { throw corrupt("offset " + x.offset() + " is not JSON"); }
            if (!Arrays.equals(x.value(), Canonical.bytes(v))) throw corrupt("offset " + x.offset() + " is not canonical bytes");
            try { check(v, "offset " + x.offset()); } catch (IllegalArgumentException e) { throw corrupt(e.getMessage()); }
            Declaration d = new Declaration(x.offset(), v);
            if (!Long.toString(d.generation()).equals(x.headers().get(OutputRecords.GENERATION_HEADER))) throw corrupt("offset " + x.offset() + " header " + OutputRecords.GENERATION_HEADER);
            out.add(d);
        }
        return new Snapshot(out, r.endOffset());
    }

    /** A declaration has exactly the step-3 fields, an integral generation and target epoch. */
    private static void check(JsonNode d, String what) {
        if (d == null || !d.isObject() || d.size() != FIELDS.size()) throw new IllegalArgumentException(what + " is not a declaration " + FIELDS);
        d.fieldNames().forEachRemaining(f -> { if (!FIELDS.contains(f)) throw new IllegalArgumentException(what + ": field " + f); });
        Json.safeLong(d.get("generation"), "generation", 1, Json.MAX_SAFE);
        Json.safeLong(d.get("targetEpoch"), "targetEpoch", Json.NON_NEGATIVE);
    }

    public Properties readerConfig() { return log.consumerConfig(); }

    private IllegalStateException corrupt(String what) { return new IllegalStateException(GenerationPolicy.CORRUPT + ": " + log.topic() + ": " + what); }

    @Override public void close() { log.close(); }
}
