package yggdrasil.forseti.engine.flink.spike;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * What the spike jobs observe, collected in this JVM: the MiniCluster runs every task in-process, so operators append to a list named
 * by the test and the test reads it. Each test uses its own name.
 */
final class Probe {
  /** One observation. {@code kind} is INIT (operator state initialized), ELEM (a record, with the operator watermark at that point),
   *  WM (a watermark the operator processed), SPLIT (a reader received a split) or TIMERS (event-time timers held after initialization). */
  record Event(String kind, int attempt, int subtask, int split, int seq, long value, boolean restored, long restoredCheckpointId) {}

  private static final Map<String, List<Event>> EVENTS = new ConcurrentHashMap<>();

  private Probe() {}

  static void reset(String name) {
    EVENTS.put(name, new CopyOnWriteArrayList<>());
  }

  static void add(String name, Event e) {
    EVENTS.computeIfAbsent(name, k -> new CopyOnWriteArrayList<>()).add(e);
  }

  static List<Event> events(String name) {
    return EVENTS.getOrDefault(name, List.of());
  }

  static List<Event> events(String name, String kind) {
    return events(name).stream().filter(e -> e.kind().equals(kind)).toList();
  }
}
