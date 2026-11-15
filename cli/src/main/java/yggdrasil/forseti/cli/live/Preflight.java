package yggdrasil.forseti.cli.live;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;

/** The preflight rules of a live run, without Kafka, MQTT or OPC UA (step-5 plan D1, PR 5, "Period configuration"): the loop's shell
 *  gathers the {@link Facts} from the broker, the sim and the run's configuration, and {@link #problems} says what is wrong. A run
 *  starts only on an empty list.
 *
 *  <p>Why each rule:
 *  <ul>
 *  <li>MQTT client ids are unique. A broker drops the older connection of a duplicate client id, so a bridge and a Muninn, or two
 *  Muninns, sharing one would make Sparkplug sessions die and reborn without any fault being injected. Muninn derives its id as
 *  {@code muninn-edge-<edge>}, so the edges are checked through that derived id; every routed device has exactly one edge (one Muninn
 *  per device, plan "Goal").</li>
 *  <li>Topics exist with six partitions, {@code LogAppendTime} and 30 d retention ({@link LiveTopics#problems}): the routing manifest
 *  and the recorder assume each.</li>
 *  <li>The acquisition cadence fits inside the rule's period: {@code M + S + JITTER_MARGIN_MS <= coverage.expectedPeriodMs}, with
 *  {@code M} Muninn's {@code --session} period and {@code S} the sim's {@code bifrost.sim.scanMs} ({@link #periodProblem}). Muninn
 *  polls free-running and publishes the last scan it read, which is at most {@code S} old, so two consecutive distinct source
 *  timestamps it delivers lie less than {@code M + S} plus jitter apart; under the bound every slot of {@code expectedPeriodMs} holds
 *  an observation wherever a visit's {@code enter} falls, and coverage counts only real absences. And {@code 2 M <= S}: neither the
 *  sim's scan nor Muninn's read of a device's members is atomic (the sim sets its nodes one by one, Muninn reads them one by one), so a
 *  read that straddles a scan boundary mixes two scans and is an incoherent snapshot, which plc-stamped attribution leaves
 *  unattributed; with at least two reads per scan a straddling read is not a scan's only reading. The source timestamps stay the sim's
 *  real acquisition times: none is moved onto a grid.</li>
 *  <li>The routing manifest meets the placement rules of its fixture ({@link Placement#problems}) and is byte-for-byte the routing
 *  recomputed from the registry: a hand-edited partition number would be visible.</li>
 *  <li>Every routed device's live members and their {@code Sim/<member>.Value} and {@code .StatusCode} controls exist in the sim,
 *  because the driver writes each snapshot as one request and a missing node would fail it midway.</li>
 *  <li>The bridge's pins are exactly the definitions' {@code contentSha256} for the routed devices, so an NBIRTH carrying another
 *  definition is caught as {@code definition-mismatch} and not taken as the governed one.</li>
 *  <li>The occupancy and incident binding profiles name the schema digests of the core's built-in schemas 1.0.0, which the producers
 *  validate every record against.</li>
 *  </ul> */
public final class Preflight {
    /** The declared jitter margin of {@link #periodProblem}. In the live captures at a 1000 ms scan, consecutive distinct source
     *  timestamps of a device lay 928 to 1072 ms apart (at most 72 ms off the scan period, typically 16 ms); 100 ms covers the largest
     *  deviation seen with room for a slower host. */
    public static final long JITTER_MARGIN_MS = 100;

    private Preflight() {}

    /** One Muninn: its edge id and the device it serves. */
    public record Edge(String edge, String device) {
        public String clientId() { return "muninn-edge-" + edge; }
    }

    /** What the shell observed and was configured with. {@code routing} is the manifest file as read (null when it is not JSON);
     *  {@code recomputed} is {@code Placement.fixture(registry, kind)}, or null with {@code recomputeError} when that failed;
     *  {@code presentNodes} are the sim node identifiers (in the stream's namespace) the shell found. */
    public record Facts(LiveEquipment.LiveStream stream, String placementKind, JsonNode routing, JsonNode recomputed, String recomputeError,
                        String bridgeClientId, String driverClientId, List<Edge> edges,
                        long muninnPeriodMs, long scanMs,
                        List<LiveTopics.Spec> topics, Map<String, LiveTopics.Observed> observedTopics,
                        Set<String> presentNodes, Map<String, String> pins) {}

    /** The placement fixture a stream belongs to ({@code dedicated} or {@code shared}). */
    public static String placementOf(String stream) {
        for (Map.Entry<String, List<String>> e : Placement.FIXTURES.entrySet()) if (e.getValue().contains(stream)) return e.getKey();
        throw new IllegalArgumentException("stream " + stream + " is in no placement fixture " + Placement.FIXTURES);
    }

    /** The routed devices: those of the manifest (all of the stream's when the manifest cannot be read). */
    static Set<String> routed(Facts f) {
        try { return new TreeSet<>(RoutingManifest.read(f.routing()).devices()); }
        catch (RuntimeException e) { return new TreeSet<>(f.stream().equipment().keySet()); }
    }

    /** Every sim node the drive of {@code devices} reads or writes: each member, its value control and its status control. */
    public static SortedSet<String> requiredNodes(LiveEquipment.LiveStream stream, Collection<String> devices) {
        SortedSet<String> s = new TreeSet<>();
        for (String d : devices) {
            LiveEquipment.Equipment e = stream.device(d);
            for (LiveEquipment.Member m : e.members()) { s.add(e.live(m.name())); s.add(e.valueControl(m.name())); s.add(e.statusControl(m.name())); }
        }
        return s;
    }

    /** Null when Muninn's period {@code muninnPeriodMs} (M) and the sim's scan {@code scanMs} (S) are positive,
     *  {@code M + S + JITTER_MARGIN_MS <= expectedPeriodMs} and {@code 2 M <= S}; otherwise the problem. The live values are 250, 500
     *  and 1000. */
    public static String periodProblem(long muninnPeriodMs, long scanMs, long expectedPeriodMs) {
        if (muninnPeriodMs <= 0 || scanMs <= 0) return "Muninn period " + muninnPeriodMs + " ms and sim scan " + scanMs + " ms must be positive";
        if (muninnPeriodMs + scanMs + JITTER_MARGIN_MS > expectedPeriodMs)
            return "Muninn period " + muninnPeriodMs + " ms + sim scan " + scanMs + " ms + jitter margin " + JITTER_MARGIN_MS + " ms exceeds the rule's expectedPeriodMs "
                + expectedPeriodMs;
        if (2 * muninnPeriodMs > scanMs)
            return "Muninn period " + muninnPeriodMs + " ms is more than half the sim scan " + scanMs
                + " ms: each scan must be read at least twice, so that a read straddling a scan boundary is not its only reading";
        return null;
    }

    public static List<String> problems(Facts f) {
        List<String> p = new ArrayList<>();
        Set<String> routed = routed(f);

        // MQTT client ids and edges.
        Map<String, List<String>> holders = new TreeMap<>();
        holders.computeIfAbsent(Objects.toString(f.bridgeClientId(), ""), k -> new ArrayList<>()).add("bridge");
        holders.computeIfAbsent(Objects.toString(f.driverClientId(), ""), k -> new ArrayList<>()).add("driver");
        Map<String, List<String>> edgesOf = new TreeMap<>();
        for (Edge e : f.edges()) {
            if (e.edge().isEmpty()) p.add("an edge id is empty (device " + e.device() + ")");
            holders.computeIfAbsent(e.clientId(), k -> new ArrayList<>()).add("muninn " + e.edge() + " (" + e.device() + ")");
            edgesOf.computeIfAbsent(e.device(), k -> new ArrayList<>()).add(e.edge());
        }
        for (Map.Entry<String, List<String>> h : holders.entrySet()) {
            if (h.getKey().isEmpty()) p.add("empty MQTT client id for " + h.getValue());
            else if (h.getValue().size() > 1) p.add("MQTT client id " + h.getKey() + " is used by " + String.join(", ", h.getValue()));
        }
        for (String d : routed) {
            List<String> e = edgesOf.getOrDefault(d, List.of());
            if (e.size() != 1) p.add("routed device " + d + " has " + e.size() + " Muninn edges " + e + ", not one");
        }
        for (String d : edgesOf.keySet()) if (!routed.contains(d)) p.add("edge " + edgesOf.get(d) + " serves " + d + ", which the routing manifest does not route");

        // Topics.
        p.addAll(LiveTopics.problems(f.topics(), f.observedTopics()));

        // Periods.
        String periods = periodProblem(f.muninnPeriodMs(), f.scanMs(), f.stream().expectedPeriodMs());
        if (periods != null) p.add(periods);

        // Routing.
        if (f.routing() == null) p.add("routing manifest is not JSON");
        else {
            try {
                RoutingManifest r = RoutingManifest.read(f.routing());
                for (String q : Placement.problems(r, f.placementKind())) p.add("routing: " + q);
            } catch (RuntimeException e) { p.add("routing: " + e.getMessage()); }
            if (f.recomputed() == null) p.add("routing could not be recomputed from the registry: " + f.recomputeError());
            // JCS digests, not JsonNode.equals: a file read back holds BigInteger nodes where the builder put longs, the same JSON.
            else if (!Canonical.digest(f.recomputed()).equals(Canonical.digest(f.routing()))) p.add("routing manifest differs from the one recomputed from the registry (" + f.placementKind() + ")");
        }

        // Sim controls.
        for (String d : routed) if (!f.stream().equipment().containsKey(d)) p.add("routed device " + d + " is not a device of stream " + f.stream().stream());
        List<String> devices = routed.stream().filter(f.stream().equipment()::containsKey).toList();
        for (String n : requiredNodes(f.stream(), devices)) if (!f.presentNodes().contains(n)) p.add("sim node " + n + " is missing");

        // Pins.
        SortedMap<String, String> expected = new TreeMap<>();
        for (String d : devices) expected.put(d, f.stream().device(d).contentSha256());
        for (Map.Entry<String, String> e : expected.entrySet()) {
            String pin = f.pins().get(e.getKey());
            if (pin == null) p.add("no pin for " + e.getKey() + " (expected " + e.getValue() + ")");
            else if (!pin.equals(e.getValue())) p.add("pin of " + e.getKey() + " is " + pin + ", the definition's contentSha256 is " + e.getValue());
        }
        for (String d : f.pins().keySet()) if (!expected.containsKey(d)) p.add("pin for " + d + ", which is not routed");

        // Schemas.
        if (!f.stream().occupancy().schemaSha256().equals(InputParser.schemaSha256(InputParser.OCCUPANCY_SCHEMA_ID)))
            p.add("occupancy binding schemaSha256 " + f.stream().occupancy().schemaSha256() + " is not the built-in schema 1.0.0");
        if (!f.stream().incidents().schemaSha256().equals(InputParser.schemaSha256(InputParser.INCIDENT_SCHEMA_ID)))
            p.add("incident binding schemaSha256 " + f.stream().incidents().schemaSha256() + " is not the built-in schema 1.0.0");
        return p;
    }
}
