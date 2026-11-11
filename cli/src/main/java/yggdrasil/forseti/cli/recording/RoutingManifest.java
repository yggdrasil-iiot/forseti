package yggdrasil.forseti.cli.recording;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** {@code routing.json}: {@code partitions}; {@code stations {path -> partition}} (sorted {@code stations[]} of the pinned rule);
 *  {@code devices {path -> {station, rawPartition}}} (Kafka's default partitioner on the device key); {@code subscribed {topic ->
 *  [partitions]}}; {@code routingDigest}, the JCS SHA-256 of the file without that field. {@link #read} recomputes the digest and
 *  refuses a file whose declared digest differs. */
public final class RoutingManifest {
    public static final List<String> TOPICS = List.of("ot.messages", "ot.sessions", "mfg.occupancy", "quality.incidents");

    private final ObjectNode node;
    private final int partitions;
    private final Map<String, Integer> stations = new LinkedHashMap<>();
    private final Map<String, String> stationOfDevice = new LinkedHashMap<>();
    private final Map<String, Integer> rawPartition = new LinkedHashMap<>();
    private final Map<String, List<Integer>> subscribed = new LinkedHashMap<>();

    private RoutingManifest(ObjectNode node) {
        this.node = node;
        long p = Json.safeLong(node.get("partitions"), "partitions", 1, 1024);
        this.partitions = (int) p;
        JsonNode st = node.path("stations");
        if (!st.isObject()) throw malformed("stations");
        st.fields().forEachRemaining(e -> stations.put(e.getKey(), partition(e.getValue(), "stations." + e.getKey())));
        JsonNode dv = node.path("devices");
        if (!dv.isObject()) throw malformed("devices");
        dv.fields().forEachRemaining(e -> {
            String station = e.getValue().path("station").asText(null);
            if (station == null || !stations.containsKey(station)) throw malformed("devices." + e.getKey() + ".station");
            stationOfDevice.put(e.getKey(), station);
            rawPartition.put(e.getKey(), partition(e.getValue().get("rawPartition"), "devices." + e.getKey() + ".rawPartition"));
        });
        JsonNode sub = node.path("subscribed");
        if (!sub.isObject()) throw malformed("subscribed");
        sub.fields().forEachRemaining(e -> {
            if (!TOPICS.contains(e.getKey()) || !e.getValue().isArray()) throw malformed("subscribed." + e.getKey());
            List<Integer> l = new ArrayList<>();
            for (JsonNode x : e.getValue()) {
                int v = partition(x, "subscribed." + e.getKey());
                if (!l.isEmpty() && v <= l.get(l.size() - 1)) throw malformed("subscribed." + e.getKey() + " not strictly increasing");
                l.add(v);
            }
            subscribed.put(e.getKey(), List.copyOf(l));
        });
        String declared = node.path("routingDigest").asText(null);
        if (declared == null) throw malformed("routingDigest");
        String actual = routingDigest();
        if (!actual.equals(declared)) throw new IllegalArgumentException("malformed recording: routingDigest " + declared + " != recomputed " + actual);
    }

    private int partition(JsonNode n, String field) {
        long v = Json.safeLong(n, field, Json.NON_NEGATIVE);
        if (v >= partitions) throw malformed(field + " = " + v + " outside " + partitions + " partitions");
        return (int) v;
    }
    private static IllegalArgumentException malformed(String what) { return new IllegalArgumentException("malformed recording: routing " + what); }

    public static RoutingManifest read(JsonNode n) {
        if (n == null || !n.isObject()) throw malformed("is not an object");
        return new RoutingManifest((ObjectNode) n);
    }

    /** The routing file for these stations and devices: stations by sorted path, devices by the default partitioner, raw topics
     *  subscribed on the partitions that carry a declared device, occupancy on the assigned station partitions, incidents on all. */
    public static ObjectNode build(int partitions, List<String> stations, Map<String, String> stationOfDevice) {
        List<String> sorted = new ArrayList<>(stations);
        Collections.sort(sorted);
        if (sorted.size() > partitions) throw new IllegalArgumentException("more stations than partitions");
        ObjectNode n = Json.object();
        Json.putSafe(n, "partitions", partitions);
        ObjectNode st = n.putObject("stations");
        for (int i = 0; i < sorted.size(); i++) Json.putSafe(st, sorted.get(i), i);
        ObjectNode dv = n.putObject("devices");
        TreeSet<Integer> raw = new TreeSet<>();
        for (String d : new TreeSet<>(stationOfDevice.keySet())) {
            int p = Murmur2.partition(d, partitions);
            raw.add(p);
            ObjectNode o = dv.putObject(d);
            o.put("station", stationOfDevice.get(d));
            Json.putSafe(o, "rawPartition", p);
        }
        ObjectNode sub = n.putObject("subscribed");
        ArrayNode m = sub.putArray("ot.messages"), s = sub.putArray("ot.sessions"), o = sub.putArray("mfg.occupancy"), q = sub.putArray("quality.incidents");
        raw.forEach(p -> { m.add(p); s.add(p); });
        for (int i = 0; i < sorted.size(); i++) o.add(i);
        for (int i = 0; i < partitions; i++) q.add(i);
        n.put("routingDigest", digestWithout(n));
        return n;
    }

    static String digestWithout(ObjectNode n) {
        ObjectNode copy = n.deepCopy();
        copy.remove("routingDigest");
        return Canonical.digest(copy);
    }

    /** Recomputed from the file's content, never read from it. */
    public String routingDigest() { return digestWithout(node); }
    public int partitions() { return partitions; }
    public Map<String, Integer> stations() { return Collections.unmodifiableMap(stations); }
    public Set<String> devices() { return Collections.unmodifiableSet(stationOfDevice.keySet()); }
    public String stationOf(String device) {
        String s = stationOfDevice.get(device);
        if (s == null) throw new IllegalArgumentException("unknown device " + device);
        return s;
    }
    public int partitionOf(String station) {
        Integer p = stations.get(station);
        if (p == null) throw new IllegalArgumentException("unknown station " + station);
        return p;
    }
    public int rawPartitionOf(String device) {
        Integer p = rawPartition.get(device);
        if (p == null) throw new IllegalArgumentException("unknown device " + device);
        return p;
    }
    public List<Integer> subscribed(String topic) { return subscribed.getOrDefault(topic, List.of()); }
    public ObjectNode node() { return node.deepCopy(); }
}
