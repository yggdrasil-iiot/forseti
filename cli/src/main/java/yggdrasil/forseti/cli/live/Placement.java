package yggdrasil.forseti.cli.live;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.recording.Murmur2;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;

/** The device placement of a live registry stream, without Kafka (step-5 plan D1, "Placement configuration"). The stream's head names
 *  its release; the release payload's hierarchy gives every equipment path (equipmentInstanceIds from the root, joined by ':'), its
 *  mapping sets name the devices, and its attribution rule's {@code stations[]} the stations. {@link RoutingManifest#build} places each
 *  device with Kafka's default partitioner ({@link Murmur2}) on six partitions; no partition number is ever written by hand.
 *
 *  <p>The rules a placement must meet ({@link #problems}): the raw topics {@code ot.messages} and {@code ot.sessions} are subscribed on
 *  the same partitions, those partitions are distinct modulo their count n (spec row 5), and the split-owner formula
 *  {@code (((topic.hashCode() * 31) & 0x7FFFFFFF) % n + partition) % n} gives n distinct owners on each raw topic. {@code dedicated}
 *  puts every device on its own raw partition; {@code shared} puts exactly two stations on one partition no other device uses.
 *
 *  <p>{@code main(registry, outDir)} writes {@code dedicated.json} (stream {@code line2-dedicated}) and {@code shared.json} (stream
 *  {@code line2-shared}) in the routing format of {@code fixtures/recordings/smoke/routing.json}; each plc twin stream must place
 *  identically, and every routing must meet its rules, or nothing is written. */
public final class Placement {
    public static final int PARTITIONS = 6;
    public static final List<String> RAW_TOPICS = List.of("ot.messages", "ot.sessions");
    /** Fixture name -> {stream, its plc twin}. */
    public static final Map<String, List<String>> FIXTURES = new LinkedHashMap<>();
    static {
        FIXTURES.put("dedicated", List.of("line2-dedicated", "line2-dedicated-plc"));
        FIXTURES.put("shared", List.of("line2-shared", "line2-shared-plc"));
    }

    /** The station the plan starves in both fixtures ("Placement configuration": the same station is starved in both): in
     *  {@code dedicated} its raw partition carries no other device, in {@code shared} exactly one other device, a station. */
    public static final String STARVED = "station3";
    /** The search space of {@link #search}: {@code site-001} .. {@code site-999}; the other hierarchy ids are Tyr's Line2 ids. */
    public static final int SEARCH_LIMIT = 999;
    /** Line2's devices below the site (path suffix -> station suffix), as Tyr's placement bundles declare them. */
    static final Map<String, String> LINE2 = new LinkedHashMap<>();
    static {
        LINE2.put("line2:station1", "line2:station1");
        LINE2.put("line2:station1:nutrunner1", "line2:station1");
        LINE2.put("line2:station1:nutrunner2", "line2:station1");
        LINE2.put("line2:station2", "line2:station2");
        LINE2.put("line2:station3", "line2:station3");
    }

    private Placement() {}

    /** The Line2 layout under {@code site}: every hierarchy id but the site's kept. */
    public static Layout line2(String site) {
        SortedMap<String, String> stationOf = new TreeMap<>();
        LINE2.forEach((d, st) -> stationOf.put(site + ":" + d, site + ":" + st));
        List<String> stations = new TreeSet<>(stationOf.values()).stream().toList();
        return new Layout(site, stations, Collections.unmodifiableSortedMap(stationOf));
    }

    /** The first site id {@code site-%03d} (001 .. 999) whose Line2 placement meets every rule of {@code kind}; pure, no Kafka.
     *  This is the deterministic search that chose Tyr's placement site ids. */
    public static String search(String kind) {
        if (!FIXTURES.containsKey(kind)) throw new IllegalArgumentException("unknown placement fixture " + kind);
        for (int i = 1; i <= SEARCH_LIMIT; i++) {
            String site = String.format(Locale.ROOT, "site-%03d", i);
            if (problems(RoutingManifest.read(routing(line2(site))), kind).isEmpty()) return site;
        }
        throw new IllegalStateException("no site id in site-001..site-" + SEARCH_LIMIT + " meets the " + kind + " rules");
    }

    /** A stream's stations (sorted) and devices (path -> station path), read from its published release. */
    public record Layout(String stream, List<String> stations, SortedMap<String, String> stationOfDevice) {}

    public static Layout read(Path registry, String stream) {
        Path m = registry.resolve("mappings");
        JsonNode head = Json.read(m.resolve("heads").resolve(stream + ".json"));
        Path payload = m.resolve("releases").resolve(Json.text(head, "id")).resolve(Json.text(head, "version")).resolve("payload");
        Map<String, String> parent = new HashMap<>();
        for (JsonNode h : jsons(payload.resolve("hierarchy")))
            for (JsonNode n : h.path("nodes"))
                parent.put(Json.text(n, "equipmentInstanceId"), n.path("parentEquipmentInstanceId").isTextual() ? n.path("parentEquipmentInstanceId").asText() : null);
        List<JsonNode> rules = jsons(payload.resolve("attribution-rules"));
        if (rules.size() != 1) throw new IllegalArgumentException(stream + ": expected one attribution rule, found " + rules.size());
        List<String> stations = new ArrayList<>();
        rules.get(0).path("stations").forEach(s -> stations.add(s.asText()));
        Collections.sort(stations);
        SortedMap<String, String> stationOf = new TreeMap<>();
        for (JsonNode set : jsons(payload.resolve("mappings"))) {
            String device = path(parent, Json.text(set, "equipmentInstanceId"));
            String station = null;
            for (String s : stations) if (device.equals(s) || device.startsWith(s + ":")) station = s;
            if (station == null) throw new IllegalArgumentException(stream + ": device " + device + " sits under no station of " + stations);
            stationOf.put(device, station);
        }
        return new Layout(stream, List.copyOf(stations), Collections.unmodifiableSortedMap(stationOf));
    }

    private static String path(Map<String, String> parent, String id) {
        if (!parent.containsKey(id)) throw new IllegalArgumentException("equipment " + id + " is not in the hierarchy");
        Deque<String> parts = new ArrayDeque<>();
        Set<String> seen = new HashSet<>();
        for (String n = id; n != null; n = parent.get(n)) {
            if (!seen.add(n)) throw new IllegalArgumentException("hierarchy cycle at " + n);
            if (!parent.containsKey(n)) throw new IllegalArgumentException("parent " + n + " is not in the hierarchy");
            parts.addFirst(n);
        }
        return String.join(":", parts);
    }

    private static List<JsonNode> jsons(Path dir) {
        if (!Files.isDirectory(dir)) return List.of();
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().map(Json::read).toList();
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    public static ObjectNode routing(Layout l) { return RoutingManifest.build(PARTITIONS, l.stations(), l.stationOfDevice()); }

    /** The split owner of {@code partition} of {@code topic} among {@code n} subscriptions. */
    public static int owner(String topic, int partition, int n) {
        if (n <= 0) throw new IllegalArgumentException("n must be positive");
        return (((topic.hashCode() * 31) & 0x7FFFFFFF) % n + partition) % n;
    }

    /** Everything that keeps {@code r} from being a valid {@code kind} placement ({@code dedicated}, {@code shared} or {@code any} for
     *  the shared rules alone); empty when it is one. */
    public static List<String> problems(RoutingManifest r, String kind) {
        List<String> p = new ArrayList<>();
        List<Integer> raw = r.subscribed(RAW_TOPICS.get(0));
        for (String t : RAW_TOPICS) if (!r.subscribed(t).equals(raw)) p.add(t + " subscribed " + r.subscribed(t) + " differs from " + raw);
        TreeSet<Integer> carried = new TreeSet<>();
        for (String d : r.devices()) carried.add(r.rawPartitionOf(d));
        if (!new ArrayList<>(carried).equals(raw)) p.add("raw subscription " + raw + " is not the device partitions " + carried);
        int n = raw.size();
        if (n == 0) { p.add("no raw partition subscribed"); return p; }
        Map<Integer, Integer> residues = new TreeMap<>();
        for (int q : raw) {
            Integer earlier = residues.putIfAbsent(q % n, q);
            if (earlier != null) p.add("partitions " + earlier + " and " + q + " share residue " + q % n + " modulo " + n);
        }
        for (String t : RAW_TOPICS) {
            Set<Integer> owners = new TreeSet<>();
            for (int q : r.subscribed(t)) owners.add(owner(t, q, n));
            if (owners.size() != n) p.add(t + ": split owners " + owners + " are not " + n + " distinct");
        }
        Map<Integer, List<String>> on = new TreeMap<>();
        for (String d : r.devices()) on.computeIfAbsent(r.rawPartitionOf(d), k -> new ArrayList<>()).add(d);
        String starved = null;
        for (String st : r.stations().keySet()) if (st.equals(STARVED) || st.endsWith(":" + STARVED)) starved = st;
        List<String> withStarved = new ArrayList<>();
        if (starved != null) for (String d : on.getOrDefault(r.rawPartitionOf(starved), List.of())) if (!d.equals(starved)) withStarved.add(d);
        switch (kind) {
            case "dedicated" -> {
                if (carried.size() != r.devices().size()) p.add("dedicated: " + r.devices().size() + " devices on " + carried.size() + " partitions " + on);
                if (starved == null) p.add("dedicated: no station " + STARVED);
                else if (!withStarved.isEmpty()) p.add("dedicated: " + STARVED + " shares its partition with " + withStarved);
            }
            case "shared" -> {
                List<List<String>> sharedBy = on.values().stream().filter(l -> l.size() > 1).toList();
                if (sharedBy.size() != 1 || sharedBy.get(0).size() != 2) p.add("shared: expected exactly one partition carrying two devices, found " + on);
                else for (String d : sharedBy.get(0)) if (!r.stations().containsKey(d)) p.add("shared: " + d + " on the shared partition is not a station");
                if (starved == null) p.add("shared: no station " + STARVED);
                else if (withStarved.size() != 1 || !r.stations().containsKey(withStarved.get(0)))
                    p.add("shared: " + STARVED + " must share its partition with exactly one other station, found " + withStarved);
            }
            case "any" -> {}
            default -> throw new IllegalArgumentException("unknown placement kind " + kind);
        }
        return p;
    }

    /** The routing of fixture {@code name} from {@code registry}: both streams place identically and meet the rules, or it throws. */
    public static ObjectNode fixture(Path registry, String name) {
        List<String> streams = FIXTURES.get(name);
        if (streams == null) throw new IllegalArgumentException("unknown placement fixture " + name);
        ObjectNode first = null;
        for (String s : streams) {
            ObjectNode n = routing(read(registry, s));
            List<String> p = problems(RoutingManifest.read(n), name);
            if (!p.isEmpty()) throw new IllegalStateException(s + ": " + String.join("; ", p));
            if (first == null) first = n;
            else if (!first.equals(n)) throw new IllegalStateException(s + " places differently from " + streams.get(0));
        }
        return first;
    }

    public static void main(String[] args) {
        if (args.length != 2) { System.err.println("usage: Placement <live registry dir> <routing out dir>"); System.exit(2); }
        Path registry = Path.of(args[0]), out = Path.of(args[1]);
        Map<String, ObjectNode> all = new LinkedHashMap<>();
        for (String name : FIXTURES.keySet()) all.put(name, fixture(registry, name));
        for (var e : all.entrySet()) {
            Json.write(out.resolve(e.getKey() + ".json"), e.getValue());
            System.out.println(e.getKey() + ": " + e.getValue().path("devices") + " subscribed " + e.getValue().path("subscribed").path("ot.messages"));
        }
    }
}
