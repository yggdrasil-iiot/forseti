package yggdrasil.forseti.engine.flink.job;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * One element between the operators of both jobs: a Flink POJO (public no-argument constructor, public fields of basic types), so the
 * records cross the network with Flink's own serializer and never through Kryo. A business record keeps its origin ({@code topic} by its
 * contract name, {@code partition}, {@code offset} on the broker) and its key and value bytes exactly as consumed: every operator that
 * needs the parsed input parses the bytes again ({@code InputParser.read}), the same way the cut assembler's held-input codec restores
 * them. {@code station} is the key of the keyed operators (null before routing).
 */
public final class Msg {
  /** A business input record. */
  public static final int RECORD = 0;
  /** A cut barrier; {@code value} holds the barrier record's bytes as consumed. */
  public static final int BARRIER = 1;
  /** Stage 2: the six incident barriers of cut {@code cutId} for one station; {@code value} is {@code {"<partition>": throughOffset}}. */
  public static final int INCIDENT_CUT = 2;

  public int kind;
  public String station;
  public String topic;
  public int partition;
  public long offset;
  public byte[] key;
  public byte[] value;
  /** The {@code forseti-generation} header of a stage-2 evidence record (null on a raw input). */
  public String generationHeader;
  /** The event time a telemetry record advances the watermark with (null: none). */
  public Long eventTime;
  public long cutId;

  public Msg() {}

  public static Msg record(String topic, int partition, long offset, byte[] key, byte[] value) {
    Msg m = new Msg();
    m.kind = RECORD;
    m.topic = topic;
    m.partition = partition;
    m.offset = offset;
    m.key = key;
    m.value = value;
    return m;
  }

  /** A copy addressed to {@code station}. */
  public Msg to(String station) {
    Msg m = new Msg();
    m.kind = kind;
    m.station = station;
    m.topic = topic;
    m.partition = partition;
    m.offset = offset;
    m.key = key;
    m.value = value;
    m.generationHeader = generationHeader;
    m.eventTime = eventTime;
    m.cutId = cutId;
    return m;
  }

  /** The key as the UTF-8 text a raw input topic carries (null for none). */
  public String keyText() { return key == null ? null : new String(key, StandardCharsets.UTF_8); }

  @Override public boolean equals(Object o) {
    if (!(o instanceof Msg m)) return false;
    return kind == m.kind && partition == m.partition && offset == m.offset && cutId == m.cutId && Objects.equals(station, m.station)
        && Objects.equals(topic, m.topic) && Arrays.equals(key, m.key) && Arrays.equals(value, m.value)
        && Objects.equals(generationHeader, m.generationHeader) && Objects.equals(eventTime, m.eventTime);
  }

  @Override public int hashCode() { return Objects.hash(kind, station, topic, partition, offset, cutId); }

  @Override public String toString() { return "Msg{" + kind + " " + station + " " + topic + "/" + partition + "@" + offset + "}"; }
}
