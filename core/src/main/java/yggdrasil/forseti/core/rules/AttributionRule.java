package yggdrasil.forseti.core.rules;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import java.util.*;

/** A typed view of a released attribution rule (contract 2.27.0, spec Attribution rule). Bifrost approved it, so the view reads and
 *  does not re-validate; the node and the release's raw-byte digest of its file are kept for the pins. */
public final class AttributionRule {
    public record IdentityMember(String equipmentPath, String unitMemberRef, String visitMemberRef, String presentMemberRef) {}
    public record Activity(String equipmentPath, String memberRef) {}
    public record Member(String mappingSetId, String mappingSetVersion, String mappingId, String semanticId, String metric) {}

    static final long MS_MAX = 86_400_000L;
    private final JsonNode node;
    private final String sha256;
    private final List<String> stations;
    private final List<Member> members;
    private final List<Activity> activity;
    private final List<IdentityMember> identityMembers;

    public AttributionRule(JsonNode node, String sha256) {
        this.node = node;
        this.sha256 = sha256;
        List<String> st = new ArrayList<>();
        node.path("stations").forEach(s -> st.add(s.textValue()));
        this.stations = List.copyOf(st);
        List<Member> ms = new ArrayList<>();
        for (JsonNode m : node.path("members"))
            ms.add(new Member(Json.text(m.path("mappingSetRef"), "id"), Json.text(m.path("mappingSetRef"), "version"), Json.text(m, "mappingId"),
                Json.text(m, "semanticId"), Json.text(m, "metric")));
        this.members = List.copyOf(ms);
        List<Activity> as = new ArrayList<>();
        for (JsonNode a : node.path("activity")) as.add(new Activity(Json.text(a, "equipmentPath"), Json.text(a, "memberRef")));
        this.activity = List.copyOf(as);
        List<IdentityMember> im = new ArrayList<>();
        for (JsonNode i : node.path("identity").path("members"))
            im.add(new IdentityMember(Json.text(i, "equipmentPath"), Json.text(i, "unitMemberRef"), Json.text(i, "visitMemberRef"), Json.text(i, "presentMemberRef")));
        this.identityMembers = List.copyOf(im);
    }

    public JsonNode node() { return node; }
    public String sha256() { return sha256; }
    public String id() { return Json.text(node, "id"); }
    public String version() { return Json.text(node, "version"); }
    public String equipmentRefId() { return Json.text(node.path("equipmentRef"), "id"); }
    public String equipmentRefVersion() { return Json.text(node.path("equipmentRef"), "version"); }
    public String occupancyBindingId() { return Json.text(node.path("occupancyBindingRef"), "id"); }
    public String occupancyBindingVersion() { return Json.text(node.path("occupancyBindingRef"), "version"); }
    public List<String> stations() { return stations; }
    public List<Member> members() { return members; }
    public List<Activity> activity() { return activity; }

    public long outOfOrdernessMs() { return temporal("outOfOrdernessMs"); }
    public long allowedLatenessMs() { return temporal("allowedLatenessMs"); }
    public long horizonMs() { return temporal("horizonMs"); }
    public long enterToleranceMs() { return temporal("enterToleranceMs"); }
    public long leaveToleranceMs() { return temporal("leaveToleranceMs"); }
    private long temporal(String f) { return Json.safeLong(node.path("temporal").path(f), "temporal." + f, 0, MS_MAX); }

    public String uncertainInput() { return Json.text(node.path("quality"), "uncertainInput"); }
    public double maxBadShare() {
        JsonNode v = node.path("quality").path("maxBadShare");
        if (!v.isNumber()) throw new IllegalArgumentException("schema-invalid: quality.maxBadShare");
        double d = v.doubleValue();
        if (!(d >= 0 && d <= 1)) throw new IllegalArgumentException("schema-invalid: quality.maxBadShare");
        return d;
    }
    public long expectedPeriodMs() { return Json.safeLong(node.path("coverage").path("expectedPeriodMs"), "coverage.expectedPeriodMs", 1, MS_MAX); }
    public long maxMissingPeriods() { return Json.safeLong(node.path("coverage").path("maxMissingPeriods"), "coverage.maxMissingPeriods", Json.NON_NEGATIVE); }

    public String mode() { return Json.text(node.path("identity"), "mode"); }
    public Optional<String> namespace() {
        JsonNode n = node.path("identity").path("namespace");
        return n.isTextual() ? Optional.of(n.asText()) : Optional.empty();
    }
    public String crossCheck() { return Json.text(node.path("identity"), "crossCheck"); }
    public List<IdentityMember> identityMembers() { return identityMembers; }
}
