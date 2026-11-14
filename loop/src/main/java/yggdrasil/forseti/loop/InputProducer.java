package yggdrasil.forseti.loop;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import yggdrasil.forseti.cli.live.IncidentRecords;
import yggdrasil.forseti.cli.live.LiveEquipment;
import yggdrasil.forseti.cli.live.LiveTopics;
import yggdrasil.forseti.cli.live.OccupancyRecords;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;

/** The occupancy and incident producers of the live loop (spec "Topics and partitioning"): occupancy to {@code mfg.occupancy}, keyed
 *  by {@code readPoint} on the station partition of the routing manifest; incidents to {@code quality.incidents}, keyed by
 *  {@code equipmentRef} on the default-partitioner partition of that key. The partition is always given explicitly, computed by the
 *  Kafka-free rules in cli, so what the producer writes is what the recorder and the core expect.
 *
 *  <p>Every record is checked first ({@link OccupancyRecords#checked}, {@link IncidentRecords#checked}: the core's reader, schema 1.0.0
 *  and the binding profile) and refused instead of sent. The value is compact JSON, the bytes the step-3 recordings carry. Each send
 *  waits for its acknowledgement (idempotent producer, {@code acks=all}), so a failed delivery fails the drive. */
public final class InputProducer implements EventSink, AutoCloseable {
    private final Producer<String, byte[]> producer;
    private final RoutingManifest routing;
    private final LiveEquipment.LiveStream stream;
    private final String occupancyTopic, incidentTopic;

    public InputProducer(Producer<String, byte[]> producer, RoutingManifest routing, LiveEquipment.LiveStream stream, List<LiveTopics.Spec> topics) {
        this.producer = producer;
        this.routing = routing;
        this.stream = stream;
        this.occupancyTopic = LiveTopics.nameOf(topics, "mfg.occupancy");
        this.incidentTopic = LiveTopics.nameOf(topics, "quality.incidents");
    }

    /** The Kafka producer the loop uses: idempotent, every acknowledgement from all in-sync replicas. */
    public static Producer<String, byte[]> kafka(String bootstrap, String clientId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(p);
    }

    @Override
    public void occupancy(ObjectNode event) {
        OccupancyRecords.checked(event, stream.occupancy());
        send(new ProducerRecord<>(occupancyTopic, OccupancyRecords.partition(routing, event), OccupancyRecords.key(event), bytes(event)));
    }

    @Override
    public void incident(ObjectNode revision) {
        IncidentRecords.checked(revision, stream.incidents());
        send(new ProducerRecord<>(incidentTopic, IncidentRecords.partition(revision, routing.partitions()), IncidentRecords.key(revision), bytes(revision)));
    }

    private void send(ProducerRecord<String, byte[]> r) {
        try { producer.send(r).get(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("interrupted sending to " + r.topic(), e); }
        catch (ExecutionException e) { throw new IllegalStateException("send to " + r.topic() + " failed: " + e.getCause(), e.getCause()); }
    }

    static byte[] bytes(ObjectNode n) {
        try { return Json.JSON.writeValueAsBytes(n); } catch (JsonProcessingException e) { throw new IllegalStateException(e); }
    }

    @Override
    public void close() { producer.close(); }
}
