package yggdrasil.forseti.replay.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.cli.replay.EvidenceCollector;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.cli.replay.TopicManager;
import yggdrasil.forseti.core.Json;

/** {@link EvidenceCollector} of a Kafka replay run: named JSON entries, plus the two kinds the Kafka side records itself, a client's
 *  effective configuration ({@link #client}: the gate reads {@code isolation.level} from the consumers' entries) and the topic settings
 *  the broker reports for the run's plan ({@link #topics}, with the problems {@link ReplayTopics#problems} finds). */
public final class KafkaEvidenceCollector implements EvidenceCollector {
    private final ObjectNode evidence = Json.object();

    @Override public void collect(String name, JsonNode e) {
        if (evidence.has(name)) throw new IllegalArgumentException("evidence " + name + " collected twice");
        evidence.set(name, e.deepCopy());
    }

    /** {@code clients.<name>}: one client's configuration as it was given to the client. */
    public void client(String name, Properties config) {
        ObjectNode c = Json.object();
        KafkaConfig.evidence(config).forEach(c::put);
        ObjectNode clients = evidence.has("clients") ? (ObjectNode) evidence.get("clients") : evidence.putObject("clients");
        if (clients.has(name)) throw new IllegalArgumentException("client " + name + " collected twice");
        clients.set(name, c);
    }

    /** {@code topics}: the broker's view of every planned topic and the settings problems; returns the problems. */
    public List<String> topics(TopicManager admin, List<ReplayTopics.Spec> plan) {
        List<String> names = new ArrayList<>();
        for (ReplayTopics.Spec s : plan) names.add(s.name());
        Map<String, ReplayTopics.Observed> seen = admin.describe(names);
        List<String> problems = ReplayTopics.problems(plan, seen);
        ObjectNode t = Json.object();
        for (ReplayTopics.Spec s : plan) {
            ReplayTopics.Observed o = seen.get(s.name());
            ObjectNode e = t.putObject(s.name()).put("base", s.base());
            if (o == null) { e.put("exists", false); continue; }
            e.put("partitions", o.partitions());
            for (String k : s.configs().keySet()) e.put(k, o.configs().get(k));
        }
        ObjectNode n = Json.object();
        n.set("observed", t);
        problems.forEach(n.putArray("problems")::add);
        collect("topics", n);
        return problems;
    }

    @Override public ObjectNode evidence() { return evidence.deepCopy(); }
}
