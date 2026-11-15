package yggdrasil.forseti.engine.flink.spike;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Spec line 762, Flink Kafka source split ownership (docs/ENGINE-ASSUMPTIONS.md, F1): for both governed raw placements
 * ({@code fixtures/live/routing/dedicated.json}, {@code shared.json}) and every topic each subscribes, the pinned
 * {@code KafkaSourceEnumerator} of flink-connector-kafka 5.0.0-2.2 gives every subscribed partition its own reader when the source
 * parallelism equals the number of subscribed partitions (spec lines 77, 588, 717), for the topic names the jobs use.
 *
 * <p>No broker: the enumerator is built with its public constructor (partition discovery off, no owner selector, as {@code KafkaSource}
 * builds it), the subscribed splits are handed to the step it runs after discovery ({@code addPartitionSplitChangeToPendingAssignments})
 * and every reader registers through {@code addReader}; the splits it then assigns through the context are the ones a job would get.
 */
class KafkaSplitOwnerSpikeTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @ParameterizedTest
  @ValueSource(strings = {"dedicated", "shared"})
  void everySubscribedPartitionOfAGovernedPlacementHasItsOwnReader(String placement) throws Exception {
    JsonNode routing = JSON.readTree(Files.readString(Path.of("..", "fixtures", "live", "routing", placement + ".json")));
    // The raw partitions the declared devices land on are exactly the subscribed raw partitions (dedicated 5,1,3,4,2; shared 2,3,5,0,0).
    Set<Integer> devicePartitions = new TreeSet<>();
    routing.get("devices").forEach(d -> devicePartitions.add(d.get("rawPartition").asInt()));
    for (String raw : List.of("ot.messages", "ot.sessions")) {
      Set<Integer> subscribed = new TreeSet<>();
      routing.get("subscribed").get(raw).forEach(p -> subscribed.add(p.asInt()));
      assertEquals(devicePartitions, subscribed, placement + " " + raw);
    }
    var topics = routing.get("subscribed").fields();
    while (topics.hasNext()) {
      var topic = topics.next();
      List<Integer> partitions = new ArrayList<>();
      topic.getValue().forEach(p -> partitions.add(p.asInt()));
      int parallelism = partitions.size();
      Map<Integer, List<Integer>> owned = assign(topic.getKey(), partitions, parallelism);
      for (int reader = 0; reader < parallelism; reader++) {
        assertEquals(1, owned.getOrDefault(reader, List.of()).size(),
            placement + " " + topic.getKey() + " at parallelism " + parallelism + ": reader " + reader + " owns " + owned);
      }
      // The static owner computation the default selector calls gives the same owners.
      Method owner = KafkaSourceEnumerator.class.getDeclaredMethod("getSplitOwner", TopicPartition.class, int.class);
      owner.setAccessible(true);
      for (var e : owned.entrySet()) {
        int p = e.getValue().get(0);
        assertEquals(e.getKey(), owner.invoke(null, new TopicPartition(topic.getKey(), p), parallelism));
      }
    }
  }

  /** The step-3 placement {1,2,4,5} at parallelism 4 (plan D11) gives one reader two splits and leaves one without a split. */
  @ParameterizedTest
  @ValueSource(strings = {"ot.messages", "ot.sessions"})
  void theStep3PlacementCannotGiveEveryReaderOneSplit(String topic) throws Exception {
    Map<Integer, List<Integer>> owned = assign(topic, List.of(1, 2, 4, 5), 4);
    int withOne = 0;
    for (int reader = 0; reader < 4; reader++) {
      withOne += owned.getOrDefault(reader, List.of()).size() == 1 ? 1 : 0;
    }
    assertEquals(2, withOne, topic + ": " + owned);
  }

  /** Reader -> partitions the pinned enumerator assigns to it for {@code topic} at {@code parallelism}. */
  private static Map<Integer, List<Integer>> assign(String topic, List<Integer> partitions, int parallelism) throws Exception {
    Map<Integer, List<Integer>> owned = new TreeMap<>();
    Map<Integer, ReaderInfo> registered = new HashMap<>();
    @SuppressWarnings("unchecked")
    SplitEnumeratorContext<KafkaPartitionSplit> context = (SplitEnumeratorContext<KafkaPartitionSplit>) Proxy.newProxyInstance(
        KafkaSplitOwnerSpikeTest.class.getClassLoader(), new Class<?>[] {SplitEnumeratorContext.class}, (proxy, m, args) -> {
          switch (m.getName()) {
            case "currentParallelism": return parallelism;
            case "registeredReaders": return registered;
            case "assignSplits": {
              @SuppressWarnings("unchecked")
              SplitsAssignment<KafkaPartitionSplit> a = (SplitsAssignment<KafkaPartitionSplit>) args[0];
              a.assignment().forEach((reader, splits) -> splits.forEach(s -> {
                assertEquals(topic, s.getTopic());
                owned.computeIfAbsent(reader, k -> new ArrayList<>()).add(s.getPartition());
              }));
              return null;
            }
            case "toString": return "spike context";
            default: throw new UnsupportedOperationException(m.getName());
          }
        });
    Set<TopicPartition> tps = new HashSet<>();
    partitions.forEach(p -> tps.add(new TopicPartition(topic, p)));
    Properties props = new Properties();
    props.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "-1");
    props.setProperty("group.id", "spike");
    KafkaSourceEnumerator enumerator = new KafkaSourceEnumerator(subscriber(tps), OffsetsInitializer.earliest(),
        OffsetsInitializer.latest(), props, context, Boundedness.CONTINUOUS_UNBOUNDED);
    Collection<KafkaPartitionSplit> splits = new ArrayList<>();
    tps.forEach(tp -> splits.add(new KafkaPartitionSplit(tp, KafkaPartitionSplit.EARLIEST_OFFSET)));
    Method pending = KafkaSourceEnumerator.class.getDeclaredMethod("addPartitionSplitChangeToPendingAssignments", Collection.class);
    pending.setAccessible(true);
    pending.invoke(enumerator, splits);
    for (int reader = 0; reader < parallelism; reader++) {
      registered.put(reader, new ReaderInfo(reader, "localhost"));
      enumerator.addReader(reader);
    }
    return owned;
  }

  private static KafkaSubscriber subscriber(Set<TopicPartition> tps) throws Exception {
    Method m = KafkaSubscriber.class.getDeclaredMethod("getPartitionSetSubscriber", Set.class);
    m.setAccessible(true);
    return (KafkaSubscriber) m.invoke(null, tps);
  }
}
