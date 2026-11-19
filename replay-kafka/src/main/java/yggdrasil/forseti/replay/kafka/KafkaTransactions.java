package yggdrasil.forseti.replay.kafka;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import yggdrasil.forseti.core.Json;

/** The Kafka Admin evidence of injection 22(i) (plan D8): the sink transactions of a writer chain as the transaction coordinators report
 *  them ({@code listTransactions}, {@code describeTransactions}: transactional id, producer id, producer epoch, state, partitions) and,
 *  per partition of a transaction, the {@code read_uncommitted} end offset against the last stable offset (the {@code read_committed}
 *  end): an open transaction with records on a partition holds the last stable offset below the log end. Reads only, never a write. */
public final class KafkaTransactions implements AutoCloseable {
    static final Duration TIMEOUT = Duration.ofSeconds(30);
    private final Admin admin;

    public KafkaTransactions(String bootstrap) { this.admin = Admin.create(KafkaConfig.admin(bootstrap, "forseti-harness-transactions")); }

    /** One transaction as described, with its partitions' offsets. */
    public record Described(String transactionalId, long producerId, int producerEpoch, String state, List<TopicPartition> partitions,
                            Map<TopicPartition, Long> uncommittedEnd, Map<TopicPartition, Long> lastStable) {
        public boolean ongoing() { return TransactionState.ONGOING.toString().equals(state) || TransactionState.ONGOING.name().equals(state); }
        /** Whether some partition of the transaction has its {@code read_uncommitted} end above its last stable offset. */
        public boolean openRecords() {
            for (TopicPartition tp : partitions) if (uncommittedEnd.getOrDefault(tp, -1L) > lastStable.getOrDefault(tp, Long.MAX_VALUE)) return true;
            return false;
        }
        public ObjectNode toJson() {
            ObjectNode n = Json.object().put("transactionalId", transactionalId);
            Json.putSafe(n, "producerId", producerId);
            n.put("producerEpoch", producerEpoch).put("state", state);
            ArrayNode a = n.putArray("partitions");
            for (TopicPartition tp : partitions) {
                ObjectNode x = a.addObject().put("topic", tp.topic()).put("partition", tp.partition());
                if (uncommittedEnd.containsKey(tp)) Json.putSafe(x, "readUncommittedEnd", uncommittedEnd.get(tp));
                if (lastStable.containsKey(tp)) Json.putSafe(x, "lastStableOffset", lastStable.get(tp));
            }
            n.put("openRecords", openRecords());
            return n;
        }
    }

    /** The transactional ids starting with {@code prefix} that the coordinators list as {@code Ongoing} now. */
    public List<String> ongoing(String prefix) {
        try {
            Collection<TransactionListing> l = admin.listTransactions(new ListTransactionsOptions().filterStates(List.of(TransactionState.ONGOING)))
                .all().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            List<String> ids = new ArrayList<>();
            for (TransactionListing t : l) if (t.transactionalId().startsWith(prefix)) ids.add(t.transactionalId());
            Collections.sort(ids);
            return ids;
        } catch (Exception e) { throw new IllegalStateException("listTransactions: " + e.getMessage(), e); }
    }

    /** {@code describeTransactions} of {@code id} and the offsets of its partitions; empty when the coordinator knows no such id. */
    public Optional<Described> describe(String id) {
        TransactionDescription d;
        try { d = admin.describeTransactions(List.of(id)).description(id).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); }
        catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TransactionalIdNotFoundException) return Optional.empty();
            throw new IllegalStateException("describeTransactions " + id + ": " + e.getCause(), e);
        } catch (Exception e) { throw new IllegalStateException("describeTransactions " + id + ": " + e, e); }
        List<TopicPartition> tps = new ArrayList<>(d.topicPartitions());
        tps.sort(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
        Map<TopicPartition, Long> uncommitted = new HashMap<>(), stable = new HashMap<>();
        if (!tps.isEmpty()) {
            Map<TopicPartition, OffsetSpec> spec = new HashMap<>();
            for (TopicPartition tp : tps) spec.put(tp, OffsetSpec.latest());
            try {
                var u = admin.listOffsets(spec, new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)).all().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                var c = admin.listOffsets(spec, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)).all().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                u.forEach((tp, r) -> uncommitted.put(tp, r.offset()));
                c.forEach((tp, r) -> stable.put(tp, r.offset()));
            } catch (Exception e) { throw new IllegalStateException("listOffsets of " + tps + ": " + e, e); }
        }
        return Optional.of(new Described(id, d.producerId(), d.producerEpoch(), d.state().toString(), tps, uncommitted, stable));
    }

    @Override public void close() { admin.close(); }
}
