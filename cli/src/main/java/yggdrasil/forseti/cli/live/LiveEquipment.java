package yggdrasil.forseti.cli.live;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** What the live loop needs to know about one published registry stream, read from the committed bytes only (step-5 plan D9, PR 3):
 *  every device with its station, the OPC UA node of the sim it is read from, its governed definition and that definition's members;
 *  the attribution rule's mode and {@code coverage.expectedPeriodMs}; and the occupancy and incident binding profiles.
 *
 *  <p>Why read it from the release and not from a configuration of the driver: the sim node of a device is what its source definitions
 *  say Muninn reads, and the members the driver writes are the members of the definition Muninn publishes in its NBIRTH. Taking both
 *  from the release the bridge and the core pin keeps the driver, the preflight and the pins on one source of truth.
 *
 *  <p>A device's node is the common prefix of its sources' OPC UA identifiers before the member ({@code Line2/Station1.Running} ->
 *  {@code Line2/Station1}); its sources must agree on it and on the namespace.
 *
 *  <p>The definition a device runs comes from the explicit deployment binding ({@code fixtures/live/deployment.json}, device path ->
 *  {@code {templateRef, version}}), not from the Tyr target's {@code semanticDefinitionRef}: a Muninn device definition and a target's
 *  semantic definition are independent identities joined only by the sha256 pin, and nothing in the core, the bridge or the rule set
 *  compares their names. The bound pair {@code definitions/recipe/<templateRef>/<version>/} must have a {@code manifest.json} whose
 *  {@code contentSha256} is the SHA-256 of {@code recipe-setpoints.yaml}: that digest is the pin. Muninn publishes the members the
 *  instance and the definition share, so the driver writes the definition's members. */
public final class LiveEquipment {
    private LiveEquipment() {}

    /** One member of a definition: its name and its OPC UA built-in type ({@code Boolean}, {@code String} or {@code Double}). */
    public record Member(String name, String type) {}

    /** One device of the stream. */
    public record Equipment(String device, String station, String namespaceUri, String node, String definitionRef, String definitionVersion,
                            String contentSha256, List<Member> members) {
        public boolean isStation() { return device.equals(station); }
        /** The live member the sim re-stamps every scan, e.g. {@code Line2/Station1.Running}. */
        public String live(String member) { return node + "." + member; }
        /** {@code Sim/<member>.Value}. */
        public String valueControl(String member) { return "Sim/" + live(member) + ".Value"; }
        /** {@code Sim/<member>.StatusCode}. */
        public String statusControl(String member) { return "Sim/" + live(member) + ".StatusCode"; }
        public Member member(String name) {
            for (Member m : members) if (m.name().equals(name)) return m;
            throw new IllegalArgumentException(device + " has no member " + name);
        }
    }

    /** The occupancy binding profile: what an occupancy producer may send (spec line 460: anything else is {@code schema-invalid}). */
    public record OccupancyProfile(String schemaSha256, List<String> sources, String pattern, String enter, String leave, List<String> reasons) {}

    /** The incident binding profile. */
    public record IncidentProfile(String schemaSha256, List<String> kinds) {}

    /** One stream: its rule's facts and its devices by path. */
    public record LiveStream(String stream, String mode, String crossCheck, long expectedPeriodMs, List<String> stations,
                             SortedMap<String, Equipment> equipment, OccupancyProfile occupancy, IncidentProfile incidents) {
        public Equipment device(String path) {
            Equipment e = equipment.get(path);
            if (e == null) throw new IllegalArgumentException("stream " + stream + " has no device " + path);
            return e;
        }
        /** The generation pin of every device: the definition digest its NBIRTH must carry (the bridge's {@code --pins}). */
        public SortedMap<String, String> pins() {
            SortedMap<String, String> m = new TreeMap<>();
            equipment.forEach((d, e) -> m.put(d, e.contentSha256()));
            return Collections.unmodifiableSortedMap(m);
        }
    }

    /** The committed binding next to the definitions: {@code fixtures/live/deployment.json} for {@code fixtures/live/definitions}. */
    public static Path deploymentOf(Path definitions) { return definitions.toAbsolutePath().normalize().getParent().resolve("deployment.json"); }

    /** {@link #read(Path, Path, Path, String)} with the binding committed next to {@code definitions}. */
    public static LiveStream read(Path registry, Path definitions, String stream) { return read(registry, definitions, deploymentOf(definitions), stream); }

    public static LiveStream read(Path registry, Path definitions, Path deployment, String stream) {
        Placement.Layout layout = Placement.read(registry, stream);
        Path payload = payload(registry, stream);
        Map<String, JsonNode> sets = byField(jsons(payload.resolve("mappings")), "equipmentInstanceId");
        Map<String, JsonNode> sources = byField(jsons(payload.resolve("sources")), "id");
        JsonNode bound = Json.read(deployment);
        if (!bound.isObject()) throw new IllegalArgumentException(deployment + ": a JSON object from device path to {templateRef, version} expected");
        List<JsonNode> rules = jsons(payload.resolve("attribution-rules"));
        if (rules.size() != 1) throw new IllegalArgumentException(stream + ": expected one attribution rule, found " + rules.size());
        JsonNode rule = rules.get(0);

        SortedMap<String, Equipment> equipment = new TreeMap<>();
        for (Map.Entry<String, String> d : layout.stationOfDevice().entrySet()) {
            String path = d.getKey(), id = path.substring(path.lastIndexOf(':') + 1);
            JsonNode set = sets.get(id);
            if (set == null) throw new IllegalArgumentException(stream + ": no mapping set for " + path);
            String ns = null, node = null;
            for (JsonNode m : set.path("mappings")) {
                JsonNode ref = m.path("valueRef").path("sourceRef");
                JsonNode src = sources.get(ref.path("id").asText());
                if (src == null) throw new IllegalArgumentException(stream + ": " + path + " maps unknown source " + ref.path("id").asText());
                JsonNode access = src.path("access");
                if (!"opcua-node".equals(access.path("kind").asText()) || !"string".equals(access.path("identifierType").asText()))
                    throw new IllegalArgumentException(stream + ": source " + src.path("id").asText() + " is not a string OPC UA node");
                String ident = Json.text(access, "identifier"), uri = Json.text(access, "namespaceUri");
                int dot = ident.lastIndexOf('.');
                if (dot <= 0) throw new IllegalArgumentException(stream + ": source identifier " + ident + " names no member");
                String n = ident.substring(0, dot);
                if (node != null && (!node.equals(n) || !ns.equals(uri)))
                    throw new IllegalArgumentException(stream + ": the sources of " + path + " name two nodes, " + ns + ";" + node + " and " + uri + ";" + n);
                node = n; ns = uri;
            }
            if (node == null) throw new IllegalArgumentException(stream + ": " + path + " maps no source");
            JsonNode def = bound.get(path);
            if (def == null || !def.isObject()) throw new IllegalArgumentException(stream + ": device " + path + " has no entry in the deployment binding " + deployment);
            String ref = Json.text(def, "templateRef"), version = Json.text(def, "version");
            Path dir = definitions.resolve("recipe").resolve(ref).resolve(version);
            if (!Files.isRegularFile(dir.resolve("manifest.json")))
                throw new IllegalArgumentException(stream + ": device " + path + " is bound to " + ref + " " + version + ", which " + definitions + " does not publish");
            JsonNode manifest = Json.read(dir.resolve("manifest.json"));
            if (!ref.equals(manifest.path("ref").asText()) || !version.equals(manifest.path("version").asText()))
                throw new IllegalArgumentException(dir + ": manifest names " + manifest.path("ref").asText() + " " + manifest.path("version").asText());
            byte[] content = bytes(dir.resolve("recipe-setpoints.yaml"));
            String sha = Json.text(manifest, "contentSha256"), actual = Canonical.digestOf(content);
            if (!sha.equals(actual)) throw new IllegalArgumentException(dir + ": contentSha256 " + sha + " is not the content's SHA-256 " + actual);
            List<Member> members = new ArrayList<>();
            for (JsonNode m : Json.read(content).path("members")) {
                String type = Json.text(m, "type");
                if (!List.of("Boolean", "String", "Double").contains(type)) throw new IllegalArgumentException(dir + ": member type " + type + " is not driven");
                members.add(new Member(Json.text(m, "name"), type));
            }
            if (members.isEmpty()) throw new IllegalArgumentException(dir + ": no members");
            equipment.put(path, new Equipment(path, d.getValue(), ns, node, ref, version, sha, List.copyOf(members)));
        }

        JsonNode occ = binding(payload, "mfg.occupancy"), inc = binding(payload, "quality.incidents");
        OccupancyProfile op = new OccupancyProfile(Json.text(occ.path("format"), "schemaSha256"), texts(occ.path("sources")),
            Json.text(occ.path("identity"), "pattern"), Json.text(occ.path("enter"), "bizStep"), Json.text(occ.path("leave"), "bizStep"),
            texts(occ.path("correction").path("reasons")));
        IncidentProfile ip = new IncidentProfile(Json.text(inc.path("format"), "schemaSha256"), texts(inc.path("kinds")));
        long period = Json.safeLong(rule.path("coverage").get("expectedPeriodMs"), "coverage.expectedPeriodMs", 1, Json.MAX_SAFE);
        return new LiveStream(stream, Json.text(rule.path("identity"), "mode"), rule.path("identity").path("crossCheck").asText("none"), period,
            layout.stations(), Collections.unmodifiableSortedMap(equipment), op, ip);
    }

    /** The release payload directory the stream's head names. */
    static Path payload(Path registry, String stream) {
        Path m = registry.resolve("mappings");
        JsonNode head = Json.read(m.resolve("heads").resolve(stream + ".json"));
        return m.resolve("releases").resolve(Json.text(head, "id")).resolve(Json.text(head, "version")).resolve("payload");
    }

    private static JsonNode binding(Path payload, String topic) {
        JsonNode found = null;
        for (JsonNode n : jsons(payload.resolve("data"))) if (topic.equals(n.path("topic").asText())) {
            if (found != null) throw new IllegalArgumentException(payload + ": two " + topic + " bindings");
            found = n;
        }
        if (found == null) throw new IllegalArgumentException(payload + ": no " + topic + " binding");
        return found;
    }

    private static List<String> texts(JsonNode a) {
        List<String> l = new ArrayList<>();
        a.forEach(x -> l.add(x.asText()));
        return List.copyOf(l);
    }

    private static Map<String, JsonNode> byField(List<JsonNode> l, String field) {
        Map<String, JsonNode> m = new HashMap<>();
        for (JsonNode n : l) if (m.put(Json.text(n, field), n) != null) throw new IllegalArgumentException("two documents with " + field + " " + n.path(field).asText());
        return m;
    }

    private static byte[] bytes(Path p) {
        try { return Files.readAllBytes(p); } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    private static List<JsonNode> jsons(Path dir) {
        if (!Files.isDirectory(dir)) return List.of();
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().map(Json::read).toList();
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }
}
