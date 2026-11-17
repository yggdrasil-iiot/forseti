package yggdrasil.forseti.cli.replay;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Topic management of a replay run: the two broker calls the harness makes about topics ({@code forseti-replay-kafka} implements
 *  them with the Kafka Admin client). */
public interface TopicManager extends AutoCloseable {
    /** Creates every topic of {@code topics}; an existing topic is an error, because a run's topics are fresh. */
    void create(List<ReplayTopics.Spec> topics);

    /** The topics of {@code names} that exist, with their partition counts and effective configs; a missing topic is absent. */
    Map<String, ReplayTopics.Observed> describe(Collection<String> names);

    @Override void close();
}
