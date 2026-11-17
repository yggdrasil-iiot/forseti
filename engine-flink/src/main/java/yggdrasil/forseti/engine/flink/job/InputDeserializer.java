package yggdrasil.forseti.engine.flink.job;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import yggdrasil.forseti.cli.replay.CutBarrierRecord;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.stage1.Events;

/**
 * The deserializer of every Kafka source of both jobs: one consumed record becomes one {@link Msg} carrying its origin (the contract
 * topic name, the partition and the broker offset it was consumed at) and its key and value bytes unchanged. A record whose
 * {@code forseti-record-type} header is {@code cut-barrier} is a barrier (spec 412); any other value of that header is refused. On
 * {@code ot.messages} the event time the watermark advances with is computed here, from the record as the core parses it: the largest
 * valid member {@code sourceTs} ({@link Events#eventTime}), none for a barrier or a record that does not parse. On the stage-2 evidence
 * topics the {@code forseti-generation} header is kept and the record is addressed to the station that owns its partition.
 */
public final class InputDeserializer implements KafkaRecordDeserializationSchema<Msg> {
  private static final long serialVersionUID = 1L;
  private final Map<String, String> contractOf;
  private final Map<Integer, String> stationOfPartition;

  /** {@code contractOf}: run topic name to contract name; {@code stationOfPartition}: for the stage-2 evidence topics, null otherwise. */
  public InputDeserializer(Map<String, String> contractOf, Map<Integer, String> stationOfPartition) {
    this.contractOf = new HashMap<>(contractOf);
    this.stationOfPartition = stationOfPartition == null ? null : new HashMap<>(stationOfPartition);
  }

  @Override public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<Msg> out) {
    String topic = contractOf.get(r.topic());
    if (topic == null) throw new IllegalStateException("a record of an unplanned topic " + r.topic());
    Msg m = Msg.record(topic, r.partition(), r.offset(), r.key(), r.value());
    Header type = r.headers().lastHeader(CutBarrierRecord.TYPE_HEADER);
    if (type != null) {
      if (!CutBarrierRecord.TYPE.equals(new String(type.value(), StandardCharsets.UTF_8)))
        throw new IllegalStateException(topic + "/" + r.partition() + "@" + r.offset() + ": unknown record type " + new String(type.value(), StandardCharsets.UTF_8));
      m.kind = Msg.BARRIER;
    }
    Header gen = r.headers().lastHeader(OutputRecords.GENERATION_HEADER);
    if (gen != null) m.generationHeader = new String(gen.value(), StandardCharsets.UTF_8);
    if (stationOfPartition != null) {
      m.station = stationOfPartition.get(r.partition());
      if (m.station == null) throw new IllegalStateException(topic + "/" + r.partition() + " is no station's partition");
    }
    if (m.kind == Msg.RECORD && InputParser.MESSAGES.equals(topic)) {
      InputParser.Read read = InputParser.read(new Inputs.Origin(topic, r.partition(), r.offset()), m.keyText(), r.value());
      if (!read.rejected()) m.eventTime = Events.eventTime(read.record());
    }
    out.collect(m);
  }

  @Override public TypeInformation<Msg> getProducedType() { return TypeInformation.of(Msg.class); }
}
