package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.harness.KafkaReferenceDriver;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.W1Form;
import yggdrasil.forseti.cli.replay.OffsetMap;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.replay.kafka.DockerKafka;
import yggdrasil.forseti.replay.kafka.KafkaConfig;

/** Step-6 PR 3 verification: the reference tasks run over Kafka ({@link KafkaReferenceDriver}) on the smoke engine recording, in the
 *  Flink W1 form, and the oracle's cut stream equals the dumped journals at every cut ({@link EngineCheck#compare}, exactly the core
 *  gate's comparison). Needs a broker ({@code -Dforseti.kafka}, {@link DockerKafka}) and the oracle ({@code -Dforseti.python}). */
class KafkaSmokeOracleTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    static final Path LIVE = ROOT.resolve("fixtures/live/registry"), KEYS = ROOT.resolve("fixtures/live/trusted-keys.jsonl");
    static final Path SMOKE = ROOT.resolve("engine-fixtures/scenarios/dedicated/smoke.json");
    static final String PYTHON = System.getProperty("forseti.python", "python");

    @TempDir Path tmp;

    @Test void theReferenceTasksOverKafkaEqualTheOracleAtEveryCutOfSmoke() throws Exception {
        String bootstrap = DockerKafka.bootstrap();
        KafkaReferenceDriver.Result r = KafkaReferenceDriver.run(bootstrap, "smoke-" + UUID.randomUUID() + ".", SMOKE, LIVE, KEYS, tmp, W1Form.MIN_OVER_PARTITIONS);
        Recording rec = Recording.open(r.recording());
        assertEquals(rec.batches().size(), r.cuts(), "a cut per batch");
        assertEquals(List.of(), r.topicProblems(), "the broker's topics are the plan");
        assertEquals(r.registeredRowSha256Cli(), r.registeredRowSha256Kafka(), "the CLI's printed registration digest is the Kafka store's");
        assertEquals(0, r.declarationOffset(), "the run preparation wrote the initial declaration first");
        assertEquals(0, r.firstInit().declarationOffset());
        assertEquals(2, r.firstInit().endOffset(), "H past the declaration and its commit marker");
        for (JsonNode c : r.evidence().path("clients")) if (c.has("isolation.level")) assertEquals(KafkaConfig.READ_COMMITTED, c.get("isolation.level").asText());
        assertEquals(KafkaConfig.READ_COMMITTED, r.evidence().path("clients").path("journal-dump").path("isolation.level").asText());

        // the barriers moved the broker offsets away from the recording's, so the dump's translation was exercised
        OffsetMap map = OffsetMap.fromJson(Json.read(r.run().resolve("offset-map.json")));
        int p = rec.routing().subscribed("ot.messages").get(0);
        long last = rec.size("ot.messages", p) - 1;
        assertTrue(map.broker("ot.messages", p, last) > last, "barriers interleave the business records on the broker");

        EngineCheck.Compared c = EngineCheck.compare(r.recording(), r.run(), Json.read(SMOKE), LIVE, PYTHON, ROOT, tmp.resolve("oracle"), tmp.resolve("oracle.log"), r.rulesDigest());
        String summary = "kafka smoke: cuts " + r.cuts() + ", compared " + c.tally().cuts + ", rows " + c.tally().rowsCompared + ", dispositions "
            + c.tally().dispositionsCompared + ", differences " + c.tally().differences + ", oracle " + (c.broken() != null ? c.broken() : "exit " + c.exit())
            + ", findings " + c.findings() + ", business records " + r.businessRecords() + ", barriers " + r.barriers() + ", dumped " + r.dumped();
        System.out.println(summary);
        Files.writeString(ROOT.resolve("replay-kafka/target/kafka-smoke-summary.txt"), summary + System.lineSeparator() + c.tally().describe() + System.lineSeparator());
        assertNull(c.broken(), summary + " " + Files.readString(tmp.resolve("oracle.log")));
        assertEquals(0, c.exit(), summary + " " + Files.readString(tmp.resolve("oracle.log")));
        assertNull(c.findings(), summary);
        assertEquals(r.cuts(), c.tally().cuts, summary);
        assertEquals(0, c.tally().differences, summary + " " + c.tally().describe());
        assertTrue(c.equal(), summary + " " + c.tally().describe());
        assertTrue(c.tally().rowsCompared > 0 && c.tally().dispositionsCompared > 0, summary);

        // the oracle comparison reconciles dispositions by identity, not by offset; the shared reconciliation binds every disposition and
        // trigger to its recorded input at every completed cut (decision F5)
        List<Long> cutIds = new ArrayList<>();
        for (JsonNode x : Json.read(r.run().resolve("cuts.json")).path("cuts")) cutIds.add(x.path("cutId").asLong());
        assertEquals(r.cuts(), cutIds.size());
        Reconciliation rc = Reconciliation.open(r.recording(), r.run());
        for (long k : cutIds) assertEquals(List.of(), rc.cut(k), "cut " + k);
        assertEquals(List.of(), rc.finish());
        System.out.println("kafka smoke reconciliation: " + cutIds.size() + " cuts reconciled, 0 problems");

        // two dispositions of one ledger partition with their input offsets swapped no longer reconcile
        Path copy = tmp.resolve("swapped");
        try (java.util.stream.Stream<Path> w = Files.walk(r.run())) {
            for (Path f : w.toList()) {
                Path to = copy.resolve(r.run().relativize(f).toString());
                if (Files.isDirectory(f)) Files.createDirectories(to); else Files.copy(f, to);
            }
        }
        Path ledger = copy.resolve("journals/forseti.dispositions-stage1/0.jsonl");
        List<String> lines = new ArrayList<>(Files.readAllLines(ledger));
        int a = -1, b = -1;
        for (int i = 0; i < lines.size() && b < 0; i++) {
            if (lines.get(i).startsWith("{\"key\":null")) continue;
            JsonNode k = Json.read(lines.get(i).getBytes(java.nio.charset.StandardCharsets.UTF_8)).path("key");
            if (!k.path("inputTopic").asText().equals("ot.messages")) continue;
            if (a < 0) { a = i; continue; }
            JsonNode ka = Json.read(lines.get(a).getBytes(java.nio.charset.StandardCharsets.UTF_8)).path("key");
            if (ka.path("inputOffset").asLong() != k.path("inputOffset").asLong()) b = i;
        }
        assertTrue(a >= 0 && b >= 0);
        com.fasterxml.jackson.databind.node.ObjectNode la = (com.fasterxml.jackson.databind.node.ObjectNode) Json.read(lines.get(a).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        com.fasterxml.jackson.databind.node.ObjectNode lb = (com.fasterxml.jackson.databind.node.ObjectNode) Json.read(lines.get(b).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        JsonNode oa = la.path("key").get("inputOffset"), ob = lb.path("key").get("inputOffset");
        ((com.fasterxml.jackson.databind.node.ObjectNode) la.get("key")).set("inputOffset", ob);
        ((com.fasterxml.jackson.databind.node.ObjectNode) lb.get("key")).set("inputOffset", oa);
        lines.set(a, yggdrasil.forseti.core.canon.Canonical.utf8(yggdrasil.forseti.core.canon.Canonical.bytes(la)));
        lines.set(b, yggdrasil.forseti.core.canon.Canonical.utf8(yggdrasil.forseti.core.canon.Canonical.bytes(lb)));
        Files.writeString(ledger, String.join("\n", lines) + "\n");
        Reconciliation swapped = Reconciliation.open(r.recording(), copy);
        List<String> problems = new ArrayList<>();
        for (long k : cutIds) problems.addAll(swapped.cut(k));
        problems.addAll(swapped.finish());
        assertFalse(problems.isEmpty(), "two swapped offsets must not reconcile");
        System.out.println("kafka smoke, two swapped disposition offsets: " + problems.size() + " problems, first " + problems.get(0));
    }
}
