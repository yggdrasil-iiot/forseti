package yggdrasil.forseti.core.rules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.release.Refusal;
import yggdrasil.forseti.core.release.ReleaseReader;
import java.util.*;

/** What the job needs from a verified release: the attribution rule, its hierarchy, the one impact rule that names it, both binding
 *  profiles and the resolved members. Bifrost approved the bundle, so the {@code attribution.*} checks are not re-run; the job refuses
 *  only what the spec assigns to it at start-up, {@code binding.schema-mismatch}: a profile whose {@code format.schemaSha256} is not
 *  the built-in schema's raw-byte SHA-256, or an incident kind the built-in incident schema does not support. {@link #pins()} names
 *  every document by the release's {@code files[]} digest; {@link #rulesDigest()} is its JCS SHA-256 (spec 358). */
public final class RuleSet {
    private final AttributionRule attribution;
    private final ImpactRule impact;
    private final Hierarchy hierarchy;
    private final OccupancyBinding occupancy;
    private final IncidentBinding incident;
    private final MemberCatalog members;
    private final ObjectNode definitions;

    private RuleSet(AttributionRule a, ImpactRule i, Hierarchy h, OccupancyBinding o, IncidentBinding n, MemberCatalog m, ObjectNode definitions) {
        this.attribution = a; this.impact = i; this.hierarchy = h; this.occupancy = o; this.incident = n; this.members = m; this.definitions = definitions;
    }

    /** {@code definitions}: devicePath -> {templateRef, version, sha256}, pinned as given. */
    public static RuleSet load(ReleaseReader.Release release, String attributionRuleId, String version, JsonNode definitions) {
        if (definitions == null || !definitions.isObject()) throw new IllegalArgumentException("definitions is not an object");
        ReleaseReader.Doc ruleDoc = release.document("attribution-rule", new ReleaseReader.Ref(attributionRuleId, version));
        AttributionRule rule = new AttributionRule(ruleDoc.node(), digest(release, ruleDoc));
        ReleaseReader.Doc hDoc = release.document("equipment-hierarchy", new ReleaseReader.Ref(rule.equipmentRefId(), rule.equipmentRefVersion()));
        Hierarchy hierarchy = Hierarchy.from(hDoc.node(), digest(release, hDoc));

        List<ReleaseReader.Doc> impacts = release.documentsOfKind("impact-rule").stream()
            .filter(d -> attributionRuleId.equals(d.node().path("attributionRuleRef").path("id").asText(null))
                && version.equals(d.node().path("attributionRuleRef").path("version").asText(null))).toList();
        if (impacts.size() != 1) throw new IllegalStateException(impacts.size() + " impact rules name attribution rule " + attributionRuleId + " " + version);
        ImpactRule impact = new ImpactRule(impacts.get(0).node(), digest(release, impacts.get(0)));

        ReleaseReader.Doc occArt = release.document("evidence-artifact", new ReleaseReader.Ref(rule.occupancyBindingId(), rule.occupancyBindingVersion()));
        OccupancyBinding occupancy = new OccupancyBinding(occArt.id(), occArt.version(), digest(release, occArt), profile(release, occArt));
        ReleaseReader.Doc incArt = release.document("evidence-artifact", new ReleaseReader.Ref(impact.incidentBindingId(), impact.incidentBindingVersion()));
        IncidentBinding incident = new IncidentBinding(incArt.id(), incArt.version(), digest(release, incArt), profile(release, incArt));

        sameSchema(occupancy.id(), InputParser.OCCUPANCY_SCHEMA_ID, occupancy.schemaSha256());
        sameSchema(incident.id(), InputParser.INCIDENT_SCHEMA_ID, incident.schemaSha256());
        List<String> supported = InputParser.incidentKinds();
        for (String k : incident.kinds())
            if (!supported.contains(k)) throw new Refusal("binding.schema-mismatch", "rule", "kind-unsupported", "profile", incident.id(), "kind", k, "supported", supported);

        MemberCatalog members = MemberCatalog.resolve(rule, hierarchy, release);
        return new RuleSet(rule, impact, hierarchy, occupancy, incident, members, definitions.deepCopy());
    }

    private static void sameSchema(String profile, String schemaId, String declared) {
        String builtIn = InputParser.schemaSha256(schemaId);
        if (!builtIn.equals(declared)) throw new Refusal("binding.schema-mismatch", "profile", profile, "expected", builtIn, "approved", declared);
    }

    /** The profile file the evidence artifact names, by the release's verified bytes; its digest must be the one the artifact pins. */
    private static JsonNode profile(ReleaseReader.Release release, ReleaseReader.Doc artifact) {
        JsonNode file = artifact.node().path("file");
        String path = Json.text(file, "path");
        byte[] bytes = release.payload(path);
        String actual = Canonical.digestOf(bytes);
        if (!actual.equals(Json.text(file, "sha256")))
            throw new Refusal("provenance-mismatch", "rule", "profile-file", "profile", artifact.id(), "path", path, "expected", Json.text(file, "sha256"), "actual", actual);
        return Json.read(bytes);
    }

    private static String digest(ReleaseReader.Release release, ReleaseReader.Doc d) {
        String sha = release.files().get("payload/" + d.path());
        if (sha == null) throw new Refusal("provenance-mismatch", "rule", "unlisted", "path", d.path());
        return sha;
    }

    public AttributionRule attribution() { return attribution; }
    public ImpactRule impact() { return impact; }
    public Hierarchy hierarchy() { return hierarchy; }
    public OccupancyBinding occupancy() { return occupancy; }
    public IncidentBinding incident() { return incident; }
    public MemberCatalog members() { return members; }

    /** {@code {rules: {attribution, impact}, hierarchy, bindings: {occupancy, incident}, definitions}}, each document as
     *  {@code {id, version, sha256}} with the release's {@code files[]} digest. The envelope's Pins is this object plus {@code job}. */
    public ObjectNode pins() {
        ObjectNode n = Json.object();
        ObjectNode rules = n.putObject("rules");
        rules.set("attribution", pin(attribution.id(), attribution.version(), attribution.sha256()));
        rules.set("impact", pin(impact.id(), impact.version(), impact.sha256()));
        n.set("hierarchy", pin(hierarchy.id(), hierarchy.version(), hierarchy.sha256()));
        ObjectNode bindings = n.putObject("bindings");
        bindings.set("occupancy", pin(occupancy.id(), occupancy.version(), occupancy.sha256()));
        bindings.set("incident", pin(incident.id(), incident.version(), incident.sha256()));
        n.set("definitions", definitions.deepCopy());
        return n;
    }

    public String rulesDigest() { return Canonical.digest(pins()); }

    private static ObjectNode pin(String id, String version, String sha256) { return Json.object().put("id", id).put("version", version).put("sha256", sha256); }
}
