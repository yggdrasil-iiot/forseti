package yggdrasil.forseti.core.rules;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import java.util.*;
import java.util.regex.Pattern;

/** The occupancy binding profile 0.1.0 (spec Binding profiles): the evidence artifact that carries it (id, version, the release's
 *  raw-byte digest of the artifact file, kept for the pins) and the profile file it names. {@code identity.pattern} is compiled once
 *  and matched against the whole unit id. */
public final class OccupancyBinding {
    private final String id, version, sha256;
    private final JsonNode profile;
    private final Pattern pattern;
    private final List<String> correctionReasons, sources;

    public OccupancyBinding(String id, String version, String sha256, JsonNode profile) {
        this.id = id;
        this.version = version;
        this.sha256 = sha256;
        this.profile = profile;
        this.pattern = Pattern.compile(Json.text(profile.path("identity"), "pattern"));
        List<String> r = new ArrayList<>();
        profile.path("correction").path("reasons").forEach(x -> r.add(x.textValue()));
        List<String> s = new ArrayList<>();
        profile.path("sources").forEach(x -> s.add(x.textValue()));
        this.correctionReasons = List.copyOf(r);
        this.sources = List.copyOf(s);
    }
    public String id() { return id; }
    public String version() { return version; }
    public String sha256() { return sha256; }
    public JsonNode profile() { return profile; }
    public String topic() { return Json.text(profile, "topic"); }
    public String schemaId() { return Json.text(profile.path("format"), "schemaId"); }
    public String schemaSha256() { return Json.text(profile.path("format"), "schemaSha256"); }
    public Pattern pattern() { return pattern; }
    public boolean matches(String unitId) { return pattern.matcher(unitId).matches(); }
    public String enterBizStep() { return Json.text(profile.path("enter"), "bizStep"); }
    public String leaveBizStep() { return Json.text(profile.path("leave"), "bizStep"); }
    public List<String> correctionReasons() { return correctionReasons; }
    public List<String> sources() { return sources; }
}
