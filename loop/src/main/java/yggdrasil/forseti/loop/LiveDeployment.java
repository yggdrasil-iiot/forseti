package yggdrasil.forseti.loop;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.live.LiveEquipment;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;

/** What the full-loop runner starts for one stream, from the committed bytes only: one Muninn per routed device, each with its own
 *  edge id, the definition it births, the sim node it samples and its place in the hierarchy; the bridge's pins; and the telemetry
 *  tail the capture needs after the last business event.
 *
 *  <p>Why one edge per device and these names: the full-loop gate runs one Muninn per device with unique edge ids (spec row 5), and
 *  Muninn connects as {@code muninn-edge-<edge>} and waits for {@code spBv1.0/STATE/host-<edge>}, so the edge id must be a valid
 *  Sparkplug topic token and unique across the run. It is the device's path below the site with {@code ':'} replaced by {@code '-'}
 *  ({@code site-071:line2:station1:nutrunner1} -> {@code line2-station1-nutrunner1}): readable, fixed by the path, and the same in
 *  both placements, which run the same equipment under two site ids.
 *
 *  <p>The tail is {@code allowedLatenessMs + outOfOrdernessMs + expectedPeriodMs} of the stream's attribution rule (spec 628, the
 *  recorder's {@code tailMs}). */
public final class LiveDeployment {
    private LiveDeployment() {}

    /** One Muninn: {@code feed <definitions> <templateRef> <version> <opcua> <namespaceUri> <node> <mqtt> <group> <edge>
     *  --hierarchy <mappings> <stream> <instanceId>}. */
    public record Edge(String edge, String device, String templateRef, String version, String namespaceUri, String node, String instanceId, String pin) {
        public String host() { return "host-" + edge; }
    }

    public record Deployment(String stream, long tailMs, long expectedPeriodMs, List<Edge> edges) {
        /** The bridge's and the preflight's {@code --pins}: routed device -> definition contentSha256. */
        public ObjectNode pins() {
            ObjectNode o = Json.object();
            for (Edge e : edges) o.put(e.device(), e.pin());
            return o;
        }
    }

    /** The edge id of a device path ({@code <site>:<a>:<b>...} -> {@code <a>-<b>...}). */
    public static String edgeOf(String device) {
        int c = device.indexOf(':');
        if (c <= 0 || c == device.length() - 1) throw new IllegalArgumentException("device " + device + " has no path below its site");
        String e = device.substring(c + 1).replace(':', '-');
        if (!e.matches("[A-Za-z0-9_-]+")) throw new IllegalArgumentException("device " + device + " gives edge id " + e + ", not a Sparkplug topic token");
        return e;
    }

    public static Deployment of(Path registry, Path definitions, Path deployment, String stream, Path routing) {
        LiveEquipment.LiveStream s = LiveEquipment.read(registry, definitions, deployment, stream);
        RoutingManifest r = RoutingManifest.read(Json.read(routing));
        List<Edge> edges = new ArrayList<>();
        Set<String> ids = new TreeSet<>();
        for (String d : new TreeSet<>(r.devices())) {
            LiveEquipment.Equipment e = s.device(d);
            String edge = edgeOf(d);
            if (!ids.add(edge)) throw new IllegalArgumentException("two routed devices give edge id " + edge);
            edges.add(new Edge(edge, d, e.definitionRef(), e.definitionVersion(), e.namespaceUri(), e.node(), d.substring(d.lastIndexOf(':') + 1), e.contentSha256()));
        }
        if (edges.isEmpty()) throw new IllegalArgumentException(routing + ": no routed devices");
        JsonNode temporal = rule(registry, stream).path("temporal");
        long tail = Json.safeLong(temporal.get("allowedLatenessMs"), "temporal.allowedLatenessMs", 0, Json.MAX_SAFE)
            + Json.safeLong(temporal.get("outOfOrdernessMs"), "temporal.outOfOrdernessMs", 0, Json.MAX_SAFE) + s.expectedPeriodMs();
        return new Deployment(stream, tail, s.expectedPeriodMs(), List.copyOf(edges));
    }

    /** The lines the runner reads: {@code tail-ms <n>}, {@code rule-period-ms <n>} (the rule's {@code coverage.expectedPeriodMs}, not
     *  Muninn's poll period, which the runner takes from the live scenario), then per edge
     *  {@code edge <edge> <device> <templateRef> <version> <namespaceUri> <node> <instanceId>}, tab-separated. */
    public static List<String> lines(Deployment d) {
        List<String> l = new ArrayList<>();
        l.add("tail-ms\t" + d.tailMs());
        l.add("rule-period-ms\t" + d.expectedPeriodMs());
        for (Edge e : d.edges())
            l.add(String.join("\t", "edge", e.edge(), e.device(), e.templateRef(), e.version(), e.namespaceUri(), e.node(), e.instanceId()));
        return l;
    }

    /** The stream's one attribution rule, through its head and release. */
    static JsonNode rule(Path registry, String stream) {
        Path m = registry.resolve("mappings");
        JsonNode head = Json.read(m.resolve("heads").resolve(stream + ".json"));
        Path dir = m.resolve("releases").resolve(Json.text(head, "id")).resolve(Json.text(head, "version")).resolve("payload").resolve("attribution-rules");
        List<Path> files;
        try (Stream<Path> st = Files.list(dir)) { files = st.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().toList(); }
        catch (IOException e) { throw new UncheckedIOException(e); }
        if (files.size() != 1) throw new IllegalArgumentException(stream + ": expected one attribution rule, found " + files.size());
        return Json.read(files.get(0));
    }
}
