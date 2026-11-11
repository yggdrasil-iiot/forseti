package yggdrasil.forseti.core.rules;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import java.util.*;

/** The incident binding profile 0.1.0 (spec Binding profiles): the carrying evidence artifact (id, version, the release's raw-byte
 *  digest of the artifact file, for the pins) and the profile file. */
public final class IncidentBinding {
    private final String id, version, sha256;
    private final JsonNode profile;
    private final List<String> kinds;

    public IncidentBinding(String id, String version, String sha256, JsonNode profile) {
        this.id = id;
        this.version = version;
        this.sha256 = sha256;
        this.profile = profile;
        List<String> k = new ArrayList<>();
        profile.path("kinds").forEach(x -> k.add(x.textValue()));
        this.kinds = List.copyOf(k);
    }
    public String id() { return id; }
    public String version() { return version; }
    public String sha256() { return sha256; }
    public JsonNode profile() { return profile; }
    public String topic() { return Json.text(profile, "topic"); }
    public String schemaId() { return Json.text(profile.path("format"), "schemaId"); }
    public String schemaSha256() { return Json.text(profile.path("format"), "schemaSha256"); }
    public List<String> kinds() { return kinds; }
}
