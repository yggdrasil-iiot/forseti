package yggdrasil.forseti.loop;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.cli.live.*;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;

/** The producers on Kafka's MockProducer: run topic names, explicit partitions, keys, compact JSON the core reads back, and a refused
 *  record or a failed acknowledgement never passing silently. */
class InputProducerTest {
    static final String S1 = DriverTest.S1, S3 = "site-071:line2:station3", T1 = DriverTest.T1;

    static MockProducer<String, byte[]> mock() { return new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer()); }

    static InputProducer producer(MockProducer<String, byte[]> m) {
        RoutingManifest r = RoutingManifest.read(Json.read(DriverTest.LIVE.resolve("routing/dedicated.json")));
        return new InputProducer(m, r, DriverTest.stream(), LiveTopics.plan("run-3."));
    }

    @Test
    void occupancyGoesToTheRunTopicOnItsStationPartitionAndReadsBackThroughTheCore() {
        MockProducer<String, byte[]> m = mock();
        InputProducer p = producer(m);
        ObjectNode a = OccupancyRecords.arrival("S3-V1-arrive", 1790000240437L, 7, "UNIT0000000000001", S3, 1, "mes-line2");
        p.occupancy(a);
        ProducerRecord<String, byte[]> r = m.history().get(0);
        assertEquals("run-3.mfg.occupancy", r.topic());
        assertEquals(2, r.partition());
        assertEquals(S3, r.key());
        assertFalse(new String(r.value()).contains("\n"), "compact JSON");
        InputParser.Read back = InputParser.read(new Inputs.Origin(InputParser.OCCUPANCY, 2, 0), r.key(), r.value());
        assertFalse(back.rejected());
        assertEquals(1790000240437L, ((Inputs.OccupancyEvent) back.record().parsed()).eventTime());
    }

    @Test
    void incidentsGoToTheDefaultPartitionerPartitionOfTheirEquipment() {
        MockProducer<String, byte[]> m = mock();
        producer(m).incident(IncidentRecords.revision("inc-1", 1, S1, T1, "gauge-drift", 5, 5, 1, null, "open", "urn:x", "qms-line2"));
        ProducerRecord<String, byte[]> r = m.history().get(0);
        assertEquals("run-3.quality.incidents", r.topic());
        assertEquals(1, r.partition());   // nutrunner1's raw partition in the dedicated routing: the same partitioner on the same key
        assertEquals(T1, r.key());
        assertFalse(InputParser.read(new Inputs.Origin(InputParser.INCIDENTS, 1, 0), r.key(), r.value()).rejected());
    }

    @Test
    void aRefusedRecordIsNeverSentAndAFailedSendFailsTheCall() {
        MockProducer<String, byte[]> m = mock();
        InputProducer p = producer(m);
        assertThrows(IllegalArgumentException.class, () -> p.occupancy(OccupancyRecords.arrival("e", 1, 2, "unit-1", S1, 1, "mes-line2")));
        assertThrows(IllegalArgumentException.class, () -> p.incident(IncidentRecords.revision("inc-1", 1, S1, T1, "gauge-drift", 5, 6, 1, null, "open", "urn:x", "q")));
        assertEquals(List.of(), m.history());
        MockProducer<String, byte[]> failing = new MockProducer<>(false, null, new StringSerializer(), new ByteArraySerializer());
        InputProducer q = producer(failing);
        Thread t = new Thread(() -> { while (!failing.errorNext(new RuntimeException("broker down"))) Thread.onSpinWait(); });
        t.start();
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> q.occupancy(OccupancyRecords.arrival("e", 1, 2, "UNIT0000000000001", S1, 1, "mes-line2")));
        assertTrue(e.getMessage().contains("broker down"), e.getMessage());
    }
}
