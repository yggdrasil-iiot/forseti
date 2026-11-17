package yggdrasil.forseti.replay.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** The client configurations of the replay harness, every one of them explicit (spec 409: every consumer reads with
 *  {@code isolation.level=read_committed}, set explicitly, and the gate asserts it from the recorded configuration). Bytes in, bytes
 *  out: the harness never lets a client serializer touch a value. */
public final class KafkaConfig {
    private KafkaConfig() {}

    public static final String READ_COMMITTED = "read_committed";

    /** A consumer that only ever {@code assign}s partitions: no group, no offset commits, {@code read_committed}. */
    public static Properties consumer(String bootstrap, String clientId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000");
        return p;
    }

    /** The idempotent, non-transactional producer of the input topics: one in-flight request, so a partition's records land in the
     *  order they were sent. */
    public static Properties inputProducer(String bootstrap, String clientId) {
        Properties p = producer(bootstrap, clientId);
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        return p;
    }

    /** A transactional producer with a fixed {@code transactional.id}: the single writer of what it writes. Initializing another
     *  producer with the same id fences this one (its next send or commit fails), which is how a control topic keeps one writer. */
    public static Properties transactionalProducer(String bootstrap, String clientId, String transactionalId) {
        Properties p = producer(bootstrap, clientId);
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        p.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000");
        return p;
    }

    private static Properties producer(String bootstrap, String clientId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return p;
    }

    public static Properties admin(String bootstrap, String clientId) {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        return p;
    }

    /** The configuration as recorded evidence: every key and value as text, sorted. */
    public static Map<String, String> evidence(Properties p) {
        Map<String, String> m = new TreeMap<>();
        p.forEach((k, v) -> m.put(k.toString(), v.toString()));
        return m;
    }
}
