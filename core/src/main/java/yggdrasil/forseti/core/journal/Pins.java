package yggdrasil.forseti.core.journal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import yggdrasil.forseti.core.Json;

/** The envelope's pin set (spec Output rows): {@code rules}, {@code hierarchy}, {@code bindings}, {@code definitions} (devicePath ->
 *  {templateRef, version, sha256}) and {@code job}. {@code rules}, {@code hierarchy} and {@code bindings} are the objects the rule set
 *  pins (Task 7); they are carried as given. */
public record Pins(JsonNode rules, JsonNode hierarchy, JsonNode bindings, Map<String, Definition> definitions, Job job) {
    public record Definition(String templateRef, String version, String sha256) {
        ObjectNode toJson() { return Json.object().put("templateRef", templateRef).put("version", version).put("sha256", sha256); }
        static Definition fromJson(JsonNode n) { return new Definition(Json.text(n, "templateRef"), Json.text(n, "version"), Json.text(n, "sha256")); }
    }
    /** {@code idleness} is recorded as the engine states it (a gate run records it off), so it is any JSON value. */
    public record Job(String engine, String engineVersion, String artifactSha256, String runId, String timestampSource, JsonNode idleness, String routingDigest) {
        ObjectNode toJson() {
            ObjectNode n = Json.object().put("engine", engine).put("engineVersion", engineVersion).put("artifactSha256", artifactSha256)
                .put("runId", runId).put("timestampSource", timestampSource);
            n.set("idleness", idleness);
            return n.put("routingDigest", routingDigest);
        }
        static Job fromJson(JsonNode n) {
            return new Job(Json.text(n, "engine"), Json.text(n, "engineVersion"), Json.text(n, "artifactSha256"), Json.text(n, "runId"),
                Json.text(n, "timestampSource"), JournalFold.required(n, "idleness"), Json.text(n, "routingDigest"));
        }
    }

    public Pins {
        definitions = java.util.Collections.unmodifiableMap(new TreeMap<>(definitions));
    }

    public ObjectNode toJson() {
        ObjectNode n = Json.object();
        n.set("rules", rules);
        n.set("hierarchy", hierarchy);
        n.set("bindings", bindings);
        ObjectNode defs = n.putObject("definitions");
        definitions.forEach((path, d) -> defs.set(path, d.toJson()));
        n.set("job", job.toJson());
        return n;
    }

    public static Pins fromJson(JsonNode n) {
        Map<String, Definition> defs = new TreeMap<>();
        JsonNode d = JournalFold.required(n, "definitions");
        if (!d.isObject()) throw new IllegalArgumentException("definitions is not an object");
        for (Iterator<Map.Entry<String, JsonNode>> it = d.fields(); it.hasNext();) { Map.Entry<String, JsonNode> e = it.next(); defs.put(e.getKey(), Definition.fromJson(e.getValue())); }
        return new Pins(JournalFold.required(n, "rules"), JournalFold.required(n, "hierarchy"), JournalFold.required(n, "bindings"), defs,
            Job.fromJson(JournalFold.required(n, "job")));
    }
}
