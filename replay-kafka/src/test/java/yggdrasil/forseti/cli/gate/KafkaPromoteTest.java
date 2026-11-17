package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.generation.FileGenerationStore;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;
import yggdrasil.forseti.replay.kafka.DockerKafka;
import yggdrasil.forseti.replay.kafka.KafkaGenerationStore;
import yggdrasil.forseti.replay.kafka.KafkaTopicManager;

/** {@code generation promote --store kafka} (decision F4): the gate report is validated against the Kafka store, and the promotion's
 *  rows are appended in one transaction; {@code list --store kafka} prints the same transport records as the file store holding the
 *  same rows (the row shapes and the registration digest unchanged, spec 557, 571). Needs a broker ({@link DockerKafka}). */
class KafkaPromoteTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    static final Path REGISTRY = ROOT.resolve("fixtures/registry"), NEXT = ROOT.resolve("fixtures/registry-next"), KEYS = ROOT.resolve("fixtures/trusted-keys.jsonl");

    @TempDir Path tmp;

    record Out(int exit, String stdout, String stderr, byte[] bytes) {}

    static Out cli(Clock clock, String... args) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = GenerationCli.run(args, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8), clock, ROOT);
        return new Out(exit, so.toString(StandardCharsets.UTF_8), se.toString(StandardCharsets.UTF_8), so.toByteArray());
    }

    @Test void promoteOverKafkaAppendsTheBatchAndListEqualsTheFileStore() throws Exception {
        String bootstrap = DockerKafka.bootstrap();
        Path bundle = FixtureBundles.full(ROOT, tmp.resolve("bundle"), new FixtureBundles.Options(null, null), FixtureBundles.common(ROOT));
        Path state = bundle.resolve(GateEvidence.STATE), report = bundle.resolve("report.json");
        List<ReplayTopics.Spec> plan = ReplayTopics.plan("promote-" + UUID.randomUUID() + ".");
        try (KafkaTopicManager admin = new KafkaTopicManager(bootstrap, "test-admin")) { admin.create(plan); }
        String topic = ReplayTopics.nameOf(plan, ReplayTopics.GENERATIONS);
        // the fixture: the bundle's own store, its rows as one batch on the topic
        FileGenerationStore file = new FileGenerationStore(state);
        try (KafkaGenerationStore seed = new KafkaGenerationStore(bootstrap, topic, true)) { seed.appendBatch(file.all()); }
        String[] kafka = {"--store", "kafka", "--bootstrap", bootstrap, "--generations-topic", topic};

        Out listFile = cli(Clock.systemUTC(), "list", "--state", state.toString());
        Out listKafka = cli(Clock.systemUTC(), concat(new String[] {"list", "--state", state.toString()}, kafka));
        assertEquals(0, listKafka.exit(), listKafka.stderr());
        assertArrayEquals(listFile.bytes(), listKafka.bytes(), "the same transport records, byte for byte");

        JsonNode r = Json.read(report);
        JsonNode g = FixtureBundles.generationEntry(r, null, "smoke"), e = FixtureBundles.runEntry(r, null, "smoke");
        long n = g.get("generation").longValue();
        long at = Math.max(Math.max(GateSigner.signedAtMs(REGISTRY.resolve("mappings"), "line2"), GateSigner.signedAtMs(REGISTRY.resolve("mappings"), "line2-plc")),
            GateSigner.signedAtMs(NEXT.resolve("mappings"), "line2")) + 120_000;
        Clock clock = Clock.fixed(Instant.ofEpochMilli(at), ZoneOffset.UTC);
        String[] promote = concat(new String[] {"promote", "--state", state.toString(), "--mappings", REGISTRY.resolve("mappings").toString(), "--trusted-keys", KEYS.toString(),
            "--generation", Long.toString(n), "--registered-row-sha256", g.get("registeredRowSha256").asText(),
            "--run", bundle.resolve(e.get("runRecord").asText()).toString(), "--gate-report", report.toString(), "--accept-scope", "core"}, kafka);
        byte[] fileBefore = Files.readAllBytes(state.resolve(FileGenerationStore.FILE));
        Out o = cli(clock, promote);
        assertEquals(0, o.exit(), o.stdout() + o.stderr());
        assertEquals("current", Json.read(o.bytes()).get("status").asText());
        try (KafkaGenerationStore k = new KafkaGenerationStore(bootstrap, topic, false)) {
            assertEquals(List.of(Generation.REGISTERED, Generation.REPLAYING, Generation.VERIFIED, Generation.CURRENT),
                k.byNumber(n).stream().map(x -> x.row().status()).toList());
            assertEquals(n, k.current("line2").orElseThrow().row().generation());
            assertEquals(file.all().size() + 3, k.all().size());
        }
        assertArrayEquals(fileBefore, Files.readAllBytes(state.resolve(FileGenerationStore.FILE)), "the file store is not the selected store");
        Out again = cli(clock, promote);
        assertEquals(1, again.exit(), again.stdout() + again.stderr());
        assertTrue(again.stdout().contains("generation-transition"), again.stdout());
        try (KafkaGenerationStore k = new KafkaGenerationStore(bootstrap, topic, false)) { assertEquals(file.all().size() + 3, k.all().size(), "a refusal appends nothing"); }
        // a batch that would make the history invalid is refused whole
        try (KafkaGenerationStore w = new KafkaGenerationStore(bootstrap, topic, true)) {
            GenerationStore.Entry reg = w.registered(n).orElseThrow();
            assertThrows(IllegalArgumentException.class, () -> w.appendBatch(List.of(reg)));
            assertEquals(file.all().size() + 3, w.all().size());
        }
    }

    static String[] concat(String[] a, String[] b) {
        String[] c = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }
}
