package yggdrasil.forseti.engine.flink.job;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.core.Json;

/**
 * The engine pins of both jobs (spec 588-590, plan D5, D6) and their sources and sinks. Checkpoints: {@code EXACTLY_ONCE} every 1000 ms,
 * one at a time, RocksDB with incremental checkpoints; automatic watermark interval 0; max parallelism pinned; no restart strategy (a
 * failure fails the run; restarts are injected by the harness, plan D7). Sources: {@link KafkaSource} with the subscribed partitions set
 * statically ({@code setPartitions}), partition discovery off, {@code read_committed}, from the earliest offset, no offset commits, at
 * a parallelism equal to the number of subscribed partitions. Sinks: one transactional {@link KafkaSink} per writer chain
 * ({@code EXACTLY_ONCE}, a transactional-id prefix unique to the run and the writer, {@code transaction.timeout.ms} above the longest
 * restart), writing each record to the partition its writer chose.
 */
public final class Topology {
  private Topology() {}

  public static final Duration CHECKPOINT_INTERVAL = Duration.ofMillis(1000);
  public static final int MAX_CONCURRENT_CHECKPOINTS = 1;

  /** The job configuration the jobs set on their environment; the harness writes the same values into the cluster configuration. */
  public static Configuration configuration(String checkpointDir) {
    Configuration c = new Configuration();
    c.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
    c.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
    c.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
    c.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, CHECKPOINT_INTERVAL);
    c.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE);
    c.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, MAX_CONCURRENT_CHECKPOINTS);
    c.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ZERO);
    c.set(PipelineOptions.MAX_PARALLELISM, JobSettings.MAX_PARALLELISM);
    c.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
    return c;
  }

  /** The source consumer properties beyond the builder's own: every one explicit (the gate asserts {@code read_committed}). */
  static Properties sourceProperties(JobSettings s, String clientIdPrefix) {
    Properties p = new Properties();
    p.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    p.setProperty("partition.discovery.interval.ms", "-1");
    p.setProperty("commit.offsets.on.checkpoint", "false");
    p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    p.setProperty("client.id.prefix", s.prefix() + clientIdPrefix);
    p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, s.prefix() + clientIdPrefix);
    return p;
  }

  static KafkaSource<Msg> source(JobSettings s, String contract, List<Integer> partitions, Map<Integer, String> stationOfPartition, String clientIdPrefix) {
    Set<TopicPartition> tps = new LinkedHashSet<>();
    for (int p : partitions) tps.add(new TopicPartition(s.runName(contract), p));
    return KafkaSource.<Msg>builder().setBootstrapServers(s.bootstrap()).setPartitions(tps).setStartingOffsets(OffsetsInitializer.earliest())
        .setDeserializer(new InputDeserializer(s.contractOfRunName(), stationOfPartition)).setProperties(sourceProperties(s, clientIdPrefix)).build();
  }

  static Properties sinkProperties(JobSettings s, String writer) {
    Properties p = new Properties();
    p.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, Long.toString(s.transactionTimeoutMs()));
    p.setProperty(ProducerConfig.CLIENT_ID_CONFIG, s.prefix() + writer);
    p.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    p.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    return p;
  }

  public static String transactionalPrefix(JobSettings s, String writer) { return s.prefix() + "forseti-flink-g" + s.generation() + "-" + writer; }

  static KafkaSink<Out> sink(JobSettings s, String writer) {
    return KafkaSink.<Out>builder().setBootstrapServers(s.bootstrap()).setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix(transactionalPrefix(s, writer)).setKafkaProducerConfig(sinkProperties(s, writer)).setRecordSerializer(new OutSerializer()).build();
  }

  /** Each output record to its run topic and its writer's explicit partition, with the {@code forseti-generation} header. */
  static final class OutSerializer implements KafkaRecordSerializationSchema<Out> {
    private static final long serialVersionUID = 1L;

    @Override public ProducerRecord<byte[], byte[]> serialize(Out o, KafkaSinkContext context, Long timestamp) {
      return new ProducerRecord<>(o.topic, o.partition, null, o.key, o.value,
          List.of(new RecordHeader(OutputRecords.GENERATION_HEADER, o.generationHeader.getBytes(StandardCharsets.UTF_8))));
    }
  }

  /** The job's engine pins as recorded evidence (spec 588-590, plan D10): the job configuration, the watermark emission mode, the
   *  source parallelism, the effective source consumer properties and the sink producer properties. */
  static ObjectNode pins(JobSettings s, Configuration conf, Map<String, Integer> sourceParallelism, Map<String, Properties> sources, Map<String, Properties> sinks) {
    ObjectNode n = Json.object().put("engine", JobSettings.ENGINE).put("engineVersion", JobSettings.ENGINE_VERSION).put("stage", s.stage());
    ObjectNode c = n.putObject("configuration");
    new TreeMap<>(conf.toMap()).forEach(c::put);
    n.put("watermarkEmission", PerRecordWatermarks.MODE).put("idleness", "off");
    ObjectNode sp = n.putObject("sourceParallelism");
    sourceParallelism.forEach(sp::put);
    n.put("keyedParallelism", JobSettings.KEYED_PARALLELISM).put("maxParallelism", JobSettings.MAX_PARALLELISM);
    ObjectNode so = n.putObject("sources");
    sources.forEach((k, p) -> { ObjectNode x = so.putObject(k); new TreeMap<>(p).forEach((a, b) -> x.put(a.toString(), b.toString())); });
    ObjectNode si = n.putObject("sinks");
    sinks.forEach((k, p) -> {
      ObjectNode x = si.putObject(k);
      new TreeMap<>(p).forEach((a, b) -> x.put(a.toString(), b.toString()));
      x.put("deliveryGuarantee", DeliveryGuarantee.EXACTLY_ONCE.name()).put("transactionalIdPrefix", transactionalPrefix(s, k));
    });
    return n;
  }
}
