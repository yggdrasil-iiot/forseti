package yggdrasil.forseti.engine.flink.spike;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * An unbounded FLIP-27 source shaped like the Kafka source where the spikes need it, without a broker: split {@code i} is a fixed list
 * of event timestamps and goes to reader {@code i} only; a reader with an index of at least the number of splits never gets one, and,
 * as the Kafka enumerator does for an unbounded source with partition discovery off, the enumerator never signals "no more splits".
 * Records are emitted through the split-local output ({@code ReaderOutput.createOutputForSplit}), the path the Kafka reader's base class
 * takes, so each split has its own watermark generator under the reader's watermark multiplexer. A drained split stays open.
 *
 * <p>With {@code failAt >= 0} the reader of split 0 stops before position {@code failAt}, waits until a checkpoint taken at that
 * position completes, and then fails once (per {@code name}); the restored attempt continues from the checkpointed position.
 */
final class SpikeSource implements Source<Rec, SpikeSource.Split, Set<Integer>> {
  private static final Set<String> FAILED = ConcurrentHashMap.newKeySet();

  private final String name;
  private final long[][] timestamps;
  private final int failAt;

  SpikeSource(String name, long[][] timestamps, int failAt) {
    this.name = name;
    this.timestamps = timestamps;
    this.failAt = failAt;
  }

  static void reset(String name) {
    FAILED.remove(name);
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<Rec, Split> createReader(SourceReaderContext context) {
    return new Reader(context.getIndexOfSubtask());
  }

  @Override
  public SplitEnumerator<Split, Set<Integer>> createEnumerator(SplitEnumeratorContext<Split> context) {
    return new Enumerator(context, new HashSet<>());
  }

  @Override
  public SplitEnumerator<Split, Set<Integer>> restoreEnumerator(SplitEnumeratorContext<Split> context, Set<Integer> assigned) {
    return new Enumerator(context, new HashSet<>(assigned));
  }

  @Override
  public SimpleVersionedSerializer<Split> getSplitSerializer() {
    return new SplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<Set<Integer>> getEnumeratorCheckpointSerializer() {
    return new AssignedSerializer();
  }

  /** Split {@code index}, read up to (excluding) {@code position}. */
  static final class Split implements SourceSplit {
    final int index;
    int position;

    Split(int index, int position) {
      this.index = index;
      this.position = position;
    }

    @Override
    public String splitId() {
      return "split-" + index;
    }
  }

  private final class Enumerator implements SplitEnumerator<Split, Set<Integer>> {
    private final SplitEnumeratorContext<Split> context;
    private final Set<Integer> assigned;
    private final Map<Integer, List<Split>> returned = new HashMap<>();

    Enumerator(SplitEnumeratorContext<Split> context, Set<Integer> assigned) {
      this.context = context;
      this.assigned = assigned;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {}

    @Override
    public void addSplitsBack(List<Split> splits, int subtaskId) {
      for (Split s : splits) {
        assigned.remove(s.index);
      }
      returned.computeIfAbsent(subtaskId, k -> new ArrayList<>()).addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
      List<Split> back = returned.remove(subtaskId);
      if (back != null) {
        back.forEach(s -> {
          assigned.add(s.index);
          context.assignSplit(s, subtaskId);
        });
      } else if (subtaskId < timestamps.length && assigned.add(subtaskId)) {
        context.assignSplit(new Split(subtaskId, 0), subtaskId);
      }
    }

    @Override
    public Set<Integer> snapshotState(long checkpointId) {
      return new HashSet<>(assigned);
    }

    @Override
    public void close() {}
  }

  private final class Reader implements SourceReader<Rec, Split> {
    private final int subtask;
    private final List<Split> splits = new ArrayList<>();
    private final Set<Long> pausedCheckpoints = new HashSet<>();
    private boolean failNow;
    private CompletableFuture<Void> available = new CompletableFuture<>();

    Reader(int subtask) {
      this.subtask = subtask;
    }

    @Override
    public void start() {}

    private boolean paused(Split s) {
      return failAt >= 0 && s.index == 0 && s.position == failAt && !FAILED.contains(name);
    }

    private boolean hasWork() {
      for (Split s : splits) {
        if (s.position < timestamps[s.index].length && (!paused(s) || failNow)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Rec> output) {
      for (Split s : splits) {
        if (s.position >= timestamps[s.index].length) {
          continue;
        }
        if (paused(s)) {
          if (!failNow) {
            continue;
          }
          FAILED.add(name);
          throw new IllegalStateException("spike: injected failure after a checkpoint at position " + s.position);
        }
        output.createOutputForSplit(s.splitId()).collect(new Rec(s.index, s.position, timestamps[s.index][s.position]));
        s.position++;
        return InputStatus.MORE_AVAILABLE;
      }
      return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<Split> snapshotState(long checkpointId) {
      List<Split> copy = new ArrayList<>();
      for (Split s : splits) {
        if (paused(s)) {
          pausedCheckpoints.add(checkpointId);
        }
        copy.add(new Split(s.index, s.position));
      }
      return copy;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      if (pausedCheckpoints.contains(checkpointId)) {
        failNow = true;
        wake();
      }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      return hasWork() ? CompletableFuture.completedFuture(null) : available;
    }

    private void wake() {
      CompletableFuture<Void> f = available;
      available = new CompletableFuture<>();
      f.complete(null);
    }

    @Override
    public void addSplits(List<Split> added) {
      for (Split s : added) {
        Probe.add(name, new Probe.Event("SPLIT", -1, subtask, s.index, s.position, 0, false, -1));
      }
      splits.addAll(added);
      wake();
    }

    @Override
    public void notifyNoMoreSplits() {
      throw new IllegalStateException("spike: the enumerator of an unbounded source never signals no more splits");
    }

    @Override
    public void close() {}
  }

  private static final class SplitSerializer implements SimpleVersionedSerializer<Split> {
    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(Split s) throws IOException {
      ByteArrayOutputStream b = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(b)) {
        out.writeInt(s.index);
        out.writeInt(s.position);
      }
      return b.toByteArray();
    }

    @Override
    public Split deserialize(int version, byte[] bytes) throws IOException {
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
        return new Split(in.readInt(), in.readInt());
      }
    }
  }

  private static final class AssignedSerializer implements SimpleVersionedSerializer<Set<Integer>> {
    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(Set<Integer> assigned) throws IOException {
      ByteArrayOutputStream b = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(b)) {
        out.writeInt(assigned.size());
        for (int i : assigned) {
          out.writeInt(i);
        }
      }
      return b.toByteArray();
    }

    @Override
    public Set<Integer> deserialize(int version, byte[] bytes) throws IOException {
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
        Set<Integer> s = new HashSet<>();
        for (int n = in.readInt(); n > 0; n--) {
          s.add(in.readInt());
        }
        return s;
      }
    }
  }
}
