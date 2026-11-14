package yggdrasil.forseti.loop;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import yggdrasil.forseti.cli.live.LiveTopics;

/** The two broker calls the loop makes about topics. {@link KafkaTopicAdmin} implements them with the Kafka Admin client; the tests
 *  fake them (kafka-clients 4.2.2 publishes no test jar with a MockAdminClient). */
public interface TopicAdmin extends AutoCloseable {
    /** Creates every topic; an existing topic is an error, because a run's topics must be fresh (plan D6). */
    void create(List<LiveTopics.Spec> topics);

    /** The topics of {@code names} that exist, with their partition counts and effective configs; a missing topic is absent. */
    Map<String, LiveTopics.Observed> describe(Collection<String> names);

    @Override void close();
}
