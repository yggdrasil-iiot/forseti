package yggdrasil.forseti.replay.kafka;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.cli.generation.FileGenerationStore;
import yggdrasil.forseti.cli.replay.RecoveryControlStore;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.cli.replay.RunPreparation;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;

/** The two control topics on a real broker (plan PR 3, spec 748): single partition, the step-3 row shapes unchanged, one writer by a
 *  fixed {@code transactional.id}; and the topic plan's settings as the broker reports them. Needs a broker ({@link DockerKafka}). */
class KafkaStoresTest {
    static Generation row(long n, String status) {
        return new Generation(n, status, "e".repeat(64), Json.JSON.createArrayNode(), Json.object(), 1, Json.object(), null, null, 1794182400000L + n);
    }

    static List<ReplayTopics.Spec> topics(String bootstrap) {
        List<ReplayTopics.Spec> plan = ReplayTopics.plan("stores-" + UUID.randomUUID() + ".");
        try (KafkaTopicManager admin = new KafkaTopicManager(bootstrap, "test-admin")) {
            admin.create(plan);
            KafkaEvidenceCollector ev = new KafkaEvidenceCollector();
            assertEquals(List.of(), ev.topics(admin, plan), "the broker reports the planned partitions and settings");
            assertThrows(IllegalStateException.class, () -> admin.create(List.of(plan.get(0))), "a run's topics are fresh");
        }
        return plan;
    }

    @Test void theGenerationTopicKeepsTheRowsAndTheirRegistrationDigest() {
        String b = DockerKafka.bootstrap();
        String topic = ReplayTopics.nameOf(topics(b), ReplayTopics.GENERATIONS);
        try (KafkaGenerationStore w = new KafkaGenerationStore(b, topic, true); KafkaGenerationStore r = new KafkaGenerationStore(b, topic, false)) {
            assertEquals(List.of(), r.all());
            w.append("line2", row(1, Generation.REGISTERED));
            w.append("line2-plc", row(2, Generation.REGISTERED));
            assertThrows(IllegalArgumentException.class, () -> w.append("line2", row(2, Generation.REGISTERED)), "a number is registered once");
            assertEquals(List.of(new GenerationStore.Entry("line2", row(1, Generation.REGISTERED)), new GenerationStore.Entry("line2-plc", row(2, Generation.REGISTERED))), r.all());
            assertEquals(Optional.of(Canonical.digest(FileGenerationStore.record("line2", row(1, Generation.REGISTERED)))), r.registeredRowSha256(1));
            assertEquals(2, r.highest());
            assertThrows(IllegalStateException.class, () -> r.append("line2", row(3, Generation.REGISTERED)), "a reader is no writer");
        }
    }

    @Test void aSecondWriterFencesTheFirst() {
        String b = DockerKafka.bootstrap();
        List<ReplayTopics.Spec> plan = topics(b);
        String gen = ReplayTopics.nameOf(plan, ReplayTopics.GENERATIONS), ctl = ReplayTopics.nameOf(plan, ReplayTopics.RECOVERY_CONTROL);
        try (KafkaGenerationStore first = new KafkaGenerationStore(b, gen, true)) {
            first.append("line2", row(1, Generation.REGISTERED));
            try (KafkaGenerationStore second = new KafkaGenerationStore(b, gen, true)) {
                IllegalStateException e = assertThrows(IllegalStateException.class, () -> first.append("line2", row(2, Generation.REGISTERED)));
                assertTrue(e.getMessage().startsWith(ControlTopic.FENCED), e.getMessage());
                second.append("line2", row(2, Generation.REGISTERED));
                assertEquals(2, second.all().size(), "the fenced writer wrote nothing");
            }
        }
        try (KafkaRecoveryControl first = new KafkaRecoveryControl(b, ctl, true)) {
            first.append(RunPreparation.initialDeclaration(1, "a".repeat(64)));
            try (KafkaRecoveryControl second = new KafkaRecoveryControl(b, ctl, true)) {
                IllegalStateException e = assertThrows(IllegalStateException.class, () -> first.append(RunPreparation.initialDeclaration(2, "a".repeat(64))));
                assertTrue(e.getMessage().startsWith(ControlTopic.FENCED), e.getMessage());
                assertEquals(1, second.read().declarations().size());
            }
        }
    }

    @Test void theRecoveryControlIsReadUpToItsEndOffset() {
        String b = DockerKafka.bootstrap();
        List<ReplayTopics.Spec> plan = topics(b);
        String gen = ReplayTopics.nameOf(plan, ReplayTopics.GENERATIONS), ctl = ReplayTopics.nameOf(plan, ReplayTopics.RECOVERY_CONTROL);
        try (KafkaGenerationStore cli = new KafkaGenerationStore(b, gen, true); KafkaRecoveryControl harness = new KafkaRecoveryControl(b, ctl, true);
             KafkaRecoveryControl writer = new KafkaRecoveryControl(b, ctl, false)) {
            cli.append("line2", row(1, Generation.REGISTERED));
            assertEquals(0, RunPreparation.prepare(cli, harness, 1, "a".repeat(64)));
            assertEquals(0, RunPreparation.prepare(cli, harness, 1, "a".repeat(64)), "a later run writes nothing");
            RecoveryControlStore.Snapshot s = writer.read();
            assertEquals(1, s.declarations().size());
            assertEquals(2, s.endOffset(), "H counts the commit marker of the declaration's transaction");
            JsonNode d = Json.read(("{\"generation\":1,\"injectionId\":\"inj-21\",\"stage\":1,\"epochBefore\":0,\"targetEpoch\":1,\"lastWrittenCutId\":3,\"routingDigest\":\"" + "a".repeat(64) + "\"}").getBytes(StandardCharsets.UTF_8));
            long at = harness.append(d);
            assertEquals(2, at);
            s = writer.read();
            assertEquals(4, s.endOffset());
            assertEquals(new yggdrasil.forseti.core.journal.Progress.InitEvidence(2, 4, "none"), s.initEvidence(1, "none"));
            assertEquals(1, s.assemblerDeclaration(1).targetEpoch());
            assertThrows(IllegalArgumentException.class, () -> harness.append(Json.object().put("generation", 1)), "only the step-3 declaration shape");
            assertThrows(yggdrasil.forseti.cli.harness.ReferenceEngine.Refused.class, () -> RunPreparation.prepare(cli, harness, 1, "b".repeat(64)));
        }
    }

    @Test void aForeignRecordOnAControlTopicFailsClosed() {
        String b = DockerKafka.bootstrap();
        String ctl = ReplayTopics.nameOf(topics(b), ReplayTopics.RECOVERY_CONTROL);
        try (KafkaProducer<byte[], byte[]> p = new KafkaProducer<>(KafkaConfig.inputProducer(b, "test-foreign"))) {
            p.send(new ProducerRecord<>(ctl, 0, "1".getBytes(StandardCharsets.UTF_8), "{ \"generation\": 1 }".getBytes(StandardCharsets.UTF_8))).get();
        } catch (Exception e) { throw new IllegalStateException(e); }
        try (KafkaRecoveryControl r = new KafkaRecoveryControl(b, ctl, false)) {
            IllegalStateException e = assertThrows(IllegalStateException.class, r::read);
            assertTrue(e.getMessage().startsWith("control-log-corrupt"), e.getMessage());
        }
    }
}
