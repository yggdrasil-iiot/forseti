package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

/** The run's topic plan (plan D6): five topics under a fresh prefix, six partitions, LogAppendTime, 30 d; and the topic rules. */
class LiveTopicsTest {
    static Map<String, LiveTopics.Observed> asPlanned(List<LiveTopics.Spec> plan) {
        Map<String, LiveTopics.Observed> m = new TreeMap<>();
        for (LiveTopics.Spec s : plan) {
            Map<String, String> c = new TreeMap<>(s.configs());
            c.put("cleanup.policy", "delete");   // a broker reports every effective config, not only the ones set
            m.put(s.name(), new LiveTopics.Observed(s.name(), s.partitions(), c));
        }
        return m;
    }

    @Test
    void thePlanIsTheFiveTopicsUnderTheRunPrefix() {
        List<LiveTopics.Spec> plan = LiveTopics.plan("run-7.");
        assertEquals(List.of("run-7.ot.messages", "run-7.ot.sessions", "run-7.mfg.occupancy", "run-7.quality.incidents", "run-7.ot.dead-letter"),
            plan.stream().map(LiveTopics.Spec::name).toList());
        for (LiveTopics.Spec s : plan) {
            assertEquals(6, s.partitions());
            assertEquals("LogAppendTime", s.configs().get("message.timestamp.type"));
            assertEquals("2592000000", s.configs().get("retention.ms"));
        }
        assertEquals("run-7.mfg.occupancy", LiveTopics.nameOf(plan, "mfg.occupancy"));
        assertEquals("ot.messages", LiveTopics.plan("").get(0).name());
    }

    @Test
    void aPrefixKafkaCannotNameIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> LiveTopics.plan("run 7/"));
        assertThrows(IllegalArgumentException.class, () -> LiveTopics.plan("x".repeat(240)));
    }

    @Test
    void topicsAsPlannedHaveNoProblem() {
        List<LiveTopics.Spec> plan = LiveTopics.plan("r.");
        assertEquals(List.of(), LiveTopics.problems(plan, asPlanned(plan)));
    }

    @Test
    void aMissingTopicAnotherPartitionCountTimestampTypeOrRetentionIsAProblem() {
        List<LiveTopics.Spec> plan = LiveTopics.plan("r.");
        Map<String, LiveTopics.Observed> o = asPlanned(plan);
        o.remove("r.ot.dead-letter");
        o.put("r.ot.messages", new LiveTopics.Observed("r.ot.messages", 3, plan.get(0).configs()));
        Map<String, String> create = new TreeMap<>(plan.get(1).configs());
        create.put("message.timestamp.type", "CreateTime");
        o.put("r.ot.sessions", new LiveTopics.Observed("r.ot.sessions", 6, create));
        Map<String, String> week = new TreeMap<>(plan.get(2).configs());
        week.put("retention.ms", "604800000");
        o.put("r.mfg.occupancy", new LiveTopics.Observed("r.mfg.occupancy", 6, week));
        List<String> p = LiveTopics.problems(plan, o);
        assertEquals(4, p.size(), p.toString());
        assertTrue(p.contains("topic r.ot.messages has 3 partitions, not 6"), p.toString());
        assertTrue(p.contains("topic r.ot.sessions has message.timestamp.type=CreateTime, not LogAppendTime"), p.toString());
        assertTrue(p.contains("topic r.mfg.occupancy has retention.ms=604800000, not 2592000000"), p.toString());
        assertTrue(p.contains("topic r.ot.dead-letter does not exist"), p.toString());
    }
}
