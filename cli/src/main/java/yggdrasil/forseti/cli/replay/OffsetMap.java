package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;

/** The recording-to-broker offset mapping of one replay run (plan D1): for every input partition the run wrote, the broker offset of
 *  every recording offset, as the broker assigned them. The harness writes a batch's business records and then its cut barrier to the
 *  same partition (spec 412), so broker offsets run ahead of recording offsets by the barriers (and any transaction markers) written
 *  before; the map is observed from the broker's acknowledgements, never computed from that rule. It is strictly increasing per
 *  partition, so the order of records within a partition is the same in both offset spaces.
 *
 *  <p>Engines read the broker and carry broker offsets in what they write (spec 412: a routed record carries its origin offset on its
 *  partition). The recording, the batch manifest and the oracle carry recording offsets; the map is how the harness compares the two
 *  ({@link JournalTranslation}). */
public final class OffsetMap {
    public static final int VERSION = 1;
    private final TreeMap<String, long[]> brokerOf = new TreeMap<>();
    private final TreeMap<String, Integer> sizes = new TreeMap<>();

    static String key(String topic, int partition) { return topic + "/" + partition; }

    /** Records that the record at {@code recordingOffset} (the next of its partition) was written at {@code brokerOffset}. */
    public void put(String topic, int partition, long recordingOffset, long brokerOffset) {
        String k = key(topic, partition);
        int n = sizes.getOrDefault(k, 0);
        if (recordingOffset != n) throw new IllegalArgumentException(k + ": recording offset " + recordingOffset + " where " + n + " is due");
        long[] a = brokerOf.computeIfAbsent(k, x -> new long[16]);
        if (n > 0 && brokerOffset <= a[n - 1]) throw new IllegalArgumentException(k + ": broker offset " + brokerOffset + " after " + a[n - 1]);
        if (brokerOffset < 0) throw new IllegalArgumentException(k + ": broker offset " + brokerOffset);
        if (n == a.length) { a = Arrays.copyOf(a, n * 2); brokerOf.put(k, a); }
        a[n] = brokerOffset;
        sizes.put(k, n + 1);
    }

    /** Declares a partition the run subscribes to, even when it writes no business record to it. */
    public void declare(String topic, int partition) { String k = key(topic, partition); brokerOf.putIfAbsent(k, new long[16]); sizes.putIfAbsent(k, 0); }

    public boolean knows(String topic, int partition) { return sizes.containsKey(key(topic, partition)); }

    public int size(String topic, int partition) { return sizes.getOrDefault(key(topic, partition), 0); }

    public long broker(String topic, int partition, long recordingOffset) {
        String k = key(topic, partition);
        int n = need(k);
        if (recordingOffset < 0 || recordingOffset >= n) throw new IllegalArgumentException(k + ": no recording offset " + recordingOffset + " was written");
        return brokerOf.get(k)[(int) recordingOffset];
    }

    /** The recording offset of a business record's broker offset; an offset that is not one is an {@link IllegalArgumentException}. */
    public long recording(String topic, int partition, long brokerOffset) {
        String k = key(topic, partition);
        int n = need(k);
        int i = Arrays.binarySearch(brokerOf.get(k), 0, n, brokerOffset);
        if (i < 0) throw new IllegalArgumentException(k + ": broker offset " + brokerOffset + " is not a business record the run wrote");
        return i;
    }

    /** An inclusive through-offset ({@code -1}: nothing included) from the recording to the broker. */
    public long throughBroker(String topic, int partition, long recordingThrough) { return recordingThrough < 0 ? -1 : broker(topic, partition, recordingThrough); }

    /** An inclusive through-offset from the broker back to the recording. */
    public long throughRecording(String topic, int partition, long brokerThrough) { return brokerThrough < 0 ? -1 : recording(topic, partition, brokerThrough); }

    private int need(String k) {
        Integer n = sizes.get(k);
        if (n == null) throw new IllegalArgumentException("the offset map has no partition " + k);
        return n;
    }

    /** {@code {offsetMapVersion, partitions: {"<topic>/<p>": [[recordingFrom, brokerFrom, length], ...]}}}: every run of consecutive
     *  offsets in both spaces as one segment. */
    public ObjectNode toJson() {
        ObjectNode n = Json.object().put("offsetMapVersion", VERSION);
        ObjectNode ps = n.putObject("partitions");
        for (Map.Entry<String, Integer> e : sizes.entrySet()) {
            ArrayNode segs = ps.putArray(e.getKey());
            long[] a = brokerOf.get(e.getKey());
            int i = 0;
            while (i < e.getValue()) {
                int j = i;
                while (j + 1 < e.getValue() && a[j + 1] == a[j] + 1) j++;
                ArrayNode s = segs.addArray();
                s.add(i).add(a[i]).add(j - i + 1);
                i = j + 1;
            }
        }
        return n;
    }

    public static OffsetMap fromJson(JsonNode n) {
        if (!n.path("offsetMapVersion").isIntegralNumber() || n.path("offsetMapVersion").longValue() != VERSION) throw new IllegalArgumentException("offset map version");
        OffsetMap m = new OffsetMap();
        n.path("partitions").fields().forEachRemaining(e -> {
            int slash = e.getKey().lastIndexOf('/');
            String t = e.getKey().substring(0, slash);
            int p = Integer.parseInt(e.getKey().substring(slash + 1));
            m.declare(t, p);
            for (JsonNode s : e.getValue()) {
                long rec = Json.safeLong(s.get(0), "recordingFrom", Json.NON_NEGATIVE), br = Json.safeLong(s.get(1), "brokerFrom", Json.NON_NEGATIVE),
                    len = Json.safeLong(s.get(2), "length", 1, Json.MAX_SAFE);
                for (long i = 0; i < len; i++) m.put(t, p, rec + i, br + i);
            }
        });
        return m;
    }
}
