package yggdrasil.forseti.replay.kafka;

import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.cli.replay.TopicManager;

/** {@link TopicManager} over the Kafka Admin client. Replication factor 1: the PoC broker is a single KRaft node (spec 467). */
public final class KafkaTopicManager implements TopicManager {
    private final Admin admin;

    public KafkaTopicManager(String bootstrap, String clientId) { this.admin = Admin.create(KafkaConfig.admin(bootstrap, clientId)); }

    @Override public void create(List<ReplayTopics.Spec> topics) {
        List<NewTopic> l = new ArrayList<>();
        for (ReplayTopics.Spec s : topics) l.add(new NewTopic(s.name(), s.partitions(), (short) 1).configs(new HashMap<>(s.configs())));
        get(admin.createTopics(l).all(), "create topics");
    }

    @Override public Map<String, ReplayTopics.Observed> describe(Collection<String> names) {
        Map<String, ReplayTopics.Observed> out = new TreeMap<>();
        Map<String, KafkaFuture<TopicDescription>> d = admin.describeTopics(names).topicNameValues();
        for (String n : names) {
            TopicDescription td;
            try { td = d.get(n).get(); }
            catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) continue;
                throw new IllegalStateException("describe " + n + ": " + e.getCause(), e.getCause());
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
            ConfigResource res = new ConfigResource(ConfigResource.Type.TOPIC, n);
            Config c = get(admin.describeConfigs(List.of(res)).all(), "describe configs of " + n).get(res);
            Map<String, String> configs = new TreeMap<>();
            c.entries().forEach(e -> { if (e.value() != null) configs.put(e.name(), e.value()); });
            out.put(n, new ReplayTopics.Observed(n, td.partitions().size(), configs));
        }
        return out;
    }

    static <T> T get(KafkaFuture<T> f, String what) {
        try { return f.get(); }
        catch (ExecutionException e) { throw new IllegalStateException(what + ": " + e.getCause().getMessage(), e.getCause()); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(what + " interrupted", e); }
    }

    @Override public void close() { admin.close(); }
}
