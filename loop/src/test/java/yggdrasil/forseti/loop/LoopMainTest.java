package yggdrasil.forseti.loop;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.live.*;
import yggdrasil.forseti.core.Json;

/** The forseti-loop shells on fakes of the broker and the sim: topic creation checked as preflight checks it, and a preflight that
 *  gathers its facts from the committed fixtures, the broker and the sim. */
class LoopMainTest {
    static final Path LIVE = DriverTest.LIVE;

    /** A broker that keeps what it was asked to create, optionally with another partition count or timestamp type. */
    static final class FakeAdmin implements TopicAdmin {
        final Map<String, LiveTopics.Observed> topics = new TreeMap<>();
        int partitions = -1;
        String timestampType;
        public void create(List<LiveTopics.Spec> specs) {
            for (LiveTopics.Spec s : specs) {
                if (topics.containsKey(s.name())) throw new IllegalStateException("topic exists: " + s.name());
                Map<String, String> c = new TreeMap<>(s.configs());
                if (timestampType != null) c.put(LiveTopics.TIMESTAMP_TYPE_KEY, timestampType);
                topics.put(s.name(), new LiveTopics.Observed(s.name(), partitions > 0 ? partitions : s.partitions(), c));
            }
        }
        public Map<String, LiveTopics.Observed> describe(Collection<String> names) {
            Map<String, LiveTopics.Observed> m = new TreeMap<>();
            for (String n : names) if (topics.containsKey(n)) m.put(n, topics.get(n));
            return m;
        }
        public void close() {}
    }

    static String run(java.util.function.ToIntFunction<PrintStream> f, int expected) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        assertEquals(expected, f.applyAsInt(new PrintStream(b, true, StandardCharsets.UTF_8)), b.toString(StandardCharsets.UTF_8));
        return b.toString(StandardCharsets.UTF_8);
    }

    @Test
    void topicsCreatesTheRunTopicsAndRefusesABrokerThatChangedThem() {
        FakeAdmin admin = new FakeAdmin();
        String out = run(o -> LoopMain.topics(admin, LiveTopics.plan("run-9."), o), 0);
        assertTrue(out.contains("created run-9.ot.dead-letter (6 partitions"), out);
        assertEquals(5, admin.topics.size());
        assertThrows(IllegalStateException.class, () -> LoopMain.topics(admin, LiveTopics.plan("run-9."), System.out), "a run's topics are fresh");
        FakeAdmin odd = new FakeAdmin();
        odd.timestampType = "CreateTime";
        out = run(o -> LoopMain.topics(odd, LiveTopics.plan("run-9."), o), 1);
        assertTrue(out.contains("problem: topic run-9.ot.messages has message.timestamp.type=CreateTime, not LogAppendTime"), out);
    }

    static LoopMain.Config config(Path pins, String... edges) {
        Map<String, List<String>> a = LoopMain.options(new String[] {"--registry", LIVE.resolve("registry").toString(), "--definitions", LIVE.resolve("definitions").toString(),
            "--stream", "line2-dedicated", "--routing", LIVE.resolve("routing/dedicated.json").toString(), "--pins", pins.toString(),
            "--muninn-period-ms", "250", "--scan-ms", "500", "--prefix", "run-9."});
        List<String> e = new ArrayList<>();
        for (String x : edges) e.add(x);
        a.put("edge", e);
        return LoopMain.config(a);
    }

    static final String[] EDGES = {"s1=line2:station1", "t1=line2:station1:nutrunner1", "t2=line2:station1:nutrunner2", "s2=line2:station2", "s3=site-071:line2:station3"};

    @Test
    void aPreflightOnTheCommittedFixturesAFreshBrokerAndTheSimIsClean(@TempDir Path tmp) throws Exception {
        Path pins = tmp.resolve("pins.json");
        Json.write(pins, Json.JSON.valueToTree(DriverTest.stream().pins()));
        FakeAdmin admin = new FakeAdmin();
        admin.create(LiveTopics.plan("run-9."));
        FakeSim sim = new FakeSim(DriverTest.stream(), 1000, 0);
        Preflight.Facts f = LoopMain.facts(config(pins, EDGES), admin, sim);
        assertEquals(List.of(), Preflight.problems(f));
        assertEquals("site-071:line2:station3", f.edges().get(4).device(), "a full device path is kept");
        assertEquals("site-071:line2:station1", f.edges().get(0).device(), "a path below the site is completed");
        assertEquals(66, f.presentNodes().size());
    }

    @Test
    void aPreflightListsWhatTheBrokerTheSimAndThePinsGetWrong(@TempDir Path tmp) throws Exception {
        Path pins = tmp.resolve("pins.json");
        Map<String, String> p = new TreeMap<>(DriverTest.stream().pins());
        p.remove("site-071:line2:station2");
        Json.write(pins, Json.JSON.valueToTree(p));
        FakeAdmin admin = new FakeAdmin();
        admin.partitions = 4;
        admin.create(LiveTopics.plan("run-9."));
        FakeSim sim = new FakeSim(DriverTest.stream(), 1000, 0);
        sim.missing.add("Sim/Line2/Station1/Nutrunner1.Torque.Value");
        List<String> problems = Preflight.problems(LoopMain.facts(config(pins, "s1=line2:station1", "x=line2:station1:nutrunner1", "t2=line2:station1:nutrunner2",
            "s2=line2:station2", "forseti-bridge-x=line2:station3"), admin, sim));
        assertTrue(problems.contains("topic run-9.ot.messages has 4 partitions, not 6"), problems.toString());
        assertTrue(problems.contains("sim node Sim/Line2/Station1/Nutrunner1.Torque.Value is missing"), problems.toString());
        assertTrue(problems.stream().anyMatch(x -> x.startsWith("no pin for site-071:line2:station2")), problems.toString());
        assertEquals(5 + 1 + 1, problems.size(), problems.toString());
    }

    @Test
    void usageErrorsAreRefused() {
        assertThrows(LoopMain.Usage.class, () -> LoopMain.run(new String[] {}, System.out));
        assertThrows(LoopMain.Usage.class, () -> LoopMain.run(new String[] {"replay"}, System.out));
        assertThrows(LoopMain.Usage.class, () -> LoopMain.run(new String[] {"topics", "--bogus", "1"}, System.out));
        assertThrows(LoopMain.Usage.class, () -> LoopMain.options(new String[] {"--kafka", "a", "--kafka", "b"}));
        assertEquals(List.of("a=b", "c=d"), LoopMain.options(new String[] {"--edge", "a=b", "--edge", "c=d"}).get("edge"));
        assertThrows(LoopMain.Usage.class, () -> LoopMain.config(LoopMain.options(new String[] {"--edge", "nodevice"})));
    }

    @Test
    void aDriveWhoseAcquisitionCadenceDoesNotFitTheRulePeriodIsRefusedBeforeItConnects(@TempDir Path tmp) {
        com.fasterxml.jackson.databind.node.ObjectNode slow = (com.fasterxml.jackson.databind.node.ObjectNode) Json.read(DriverTest.BASELINE);
        slow.put("muninnPeriodMs", 1000);
        Path f = tmp.resolve("slow.json");
        Json.write(f, slow);
        LoopMain.Usage u = assertThrows(LoopMain.Usage.class, () -> LoopMain.run(new String[] {"drive", "--kafka", "localhost:1", "--registry", LIVE.resolve("registry").toString(),
            "--definitions", LIVE.resolve("definitions").toString(), "--stream", "line2-dedicated", "--routing", LIVE.resolve("routing/dedicated.json").toString(),
            "--scenario", f.toString(), "--opcua", "opc.tcp://localhost:1"}, System.out));
        assertTrue(u.getMessage().contains("Muninn period 1000 ms + sim scan 500 ms + jitter margin 100 ms exceeds the rule's expectedPeriodMs 1000"), u.getMessage());
    }
}
