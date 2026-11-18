package yggdrasil.forseti.replay.kafka;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.replay.OffsetMap;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The journal dump on a real broker ({@link DockerKafka}): it reads every partition to its committed end, stepping over transaction
 *  markers and aborted records and leaving an open transaction above the target alone, and it fails only a read that stops moving,
 *  never one that is merely long (the CI failure of 2026-11-18: a 793,150-record samples partition against a 60 s budget for the
 *  whole read). */
class KafkaJournalDumpTest {
    static final long GENERATION = 7;
    @TempDir Path tmp;

    static ProducerRecord<byte[], byte[]> record(String topic, int i) {
        ProducerRecord<byte[], byte[]> r = new ProducerRecord<>(topic, 0, Canonical.bytes(Json.object().put("k", i)),
            Canonical.bytes(Json.object().<com.fasterxml.jackson.databind.node.ObjectNode>set("body", Json.object().put("i", i))));
        r.headers().add("forseti-generation", Long.toString(GENERATION).getBytes(StandardCharsets.UTF_8));
        return r;
    }

    static List<ReplayTopics.Spec> topics(String bootstrap) {
        List<ReplayTopics.Spec> plan = ReplayTopics.plan("dump-" + UUID.randomUUID() + ".");
        try (KafkaTopicManager admin = new KafkaTopicManager(bootstrap, "test-admin")) { admin.create(plan); }
        return plan;
    }

    static long lines(Path f) throws Exception { try (var l = Files.lines(f)) { return l.count(); } }

    @Test void theDumpReadsToTheCommittedEndOverMarkersAndAbortedRecordsAndLeavesAnOpenTransactionAlone() throws Exception {
        String b = DockerKafka.bootstrap();
        List<ReplayTopics.Spec> plan = topics(b);
        String samples = ReplayTopics.nameOf(plan, "forseti.samples");
        int committed = 0;
        try (KafkaProducer<byte[], byte[]> p = new KafkaProducer<>(KafkaConfig.transactionalProducer(b, "t", "dump-test-" + UUID.randomUUID()));
             KafkaProducer<byte[], byte[]> open = new KafkaProducer<>(KafkaConfig.transactionalProducer(b, "o", "dump-open-" + UUID.randomUUID()))) {
            p.initTransactions();
            open.initTransactions();
            for (int t = 0; t < 20; t++) {
                p.beginTransaction();
                for (int i = 0; i < 50; i++) p.send(record(samples, t * 50 + i));
                if (t % 3 == 1) p.abortTransaction(); else { p.commitTransaction(); committed += 50; }
            }
            open.beginTransaction();
            for (int i = 0; i < 30; i++) open.send(record(samples, 10_000 + i));
            open.flush();
            // a transaction left open while the dump reads: its records lie above the committed end the read goes to
            try (KafkaJournalDump d = new KafkaJournalDump(b, plan, GENERATION)) {
                Map<String, Long> counts = d.dump(tmp, new OffsetMap());
                assertEquals(Map.of("forseti.samples/0", (long) committed), counts);
                assertEquals(committed, lines(tmp.resolve("journals/forseti.samples/0.jsonl")));
                JsonNode st = d.stats().get(0);
                assertEquals("forseti.samples", st.path("topic").asText());
                assertTrue(st.path("highWatermark").asLong() > st.path("end").asLong(), "the open transaction is above the target: " + st);
                assertEquals(committed, st.path("records").asLong());
            }
            open.abortTransaction();
        }
    }

    @Test void aLongReadThatKeepsMovingIsNoFailureOnlyOneThatStops() throws Exception {
        String b = DockerKafka.bootstrap();
        List<ReplayTopics.Spec> plan = topics(b);
        String samples = ReplayTopics.nameOf(plan, "forseti.samples");
        try (KafkaProducer<byte[], byte[]> p = new KafkaProducer<>(KafkaConfig.transactionalProducer(b, "t", "dump-slow-" + UUID.randomUUID()))) {
            p.initTransactions();
            p.beginTransaction();
            for (int i = 0; i < 600; i++) p.send(record(samples, i));
            p.commitTransaction();
        }
        Duration stall = Duration.ofMillis(800);
        // a slow host: 5 ms a record, three seconds for the partition, well past the stall window; the old dump, a single budget for
        // the whole read, failed exactly here
        try (KafkaJournalDump d = new KafkaJournalDump(b, plan, GENERATION, stall, () -> {
            try { Thread.sleep(5); } catch (InterruptedException e) { throw new IllegalStateException(e); }
        })) {
            assertEquals(Map.of("forseti.samples/0", 600L), d.dump(tmp, new OffsetMap()));
            assertTrue(d.stats().get(0).path("elapsedMs").asLong() > 3 * stall.toMillis(), d.stats().toString());
        }
    }
}
