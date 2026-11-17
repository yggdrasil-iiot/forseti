package yggdrasil.forseti.replay.kafka;

import java.util.Map;
import yggdrasil.forseti.cli.generation.GenerationStoreProvider;
import yggdrasil.forseti.cli.generation.OperatorStore;

/** {@code forseti generation --store kafka --bootstrap <host:port> --generations-topic <topic>}: the operator CLI's store on the
 *  single-partition {@code forseti.generations} topic ({@link KafkaGenerationStore}). In {@code writer} mode the CLI holds the topic's
 *  fixed {@code transactional.id}, fencing every earlier writer; {@code list} opens a reader. */
public final class KafkaGenerationStoreProvider implements GenerationStoreProvider {
    public static final String NAME = "kafka";

    @Override public String name() { return NAME; }

    @Override public OperatorStore open(Map<String, String> options) {
        String b = options.get("bootstrap"), t = options.get("topic"), mode = options.getOrDefault("mode", "writer");
        if (b == null || b.isEmpty() || t == null || t.isEmpty()) throw new IllegalArgumentException("--store kafka needs --bootstrap and --generations-topic");
        if (!mode.equals("writer") && !mode.equals("reader")) throw new IllegalArgumentException("store mode " + mode);
        return new KafkaGenerationStore(b, t, mode.equals("writer"));
    }
}
