package yggdrasil.forseti.core.rules;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.release.ReleaseReader;
import java.util.*;

/** The rule's members resolved through the release (spec Attribution rule, {@code members[]}): {@code mappingSetRef + mappingId} ->
 *  the mapping of the set -> its one {@code member} step -> the leaf member of the semantic definition of the set's target instance
 *  ({@code name} = {@code metric}, {@code valueType}, {@code semanticId}); the set's {@code equipmentInstanceId} -> hierarchy path;
 *  the role by reference (activity, identity unit/visit/present, otherwise measurement). Bifrost approved the rule, so an
 *  inconsistency met here is an IllegalStateException, never a refusal of the job. */
public final class MemberCatalog {
    public record Resolved(String mappingId, String equipmentPath, String station, String metric, String valueType, String semanticId, Role role) {}
    public enum Role { MEASUREMENT, ACTIVITY, IDENTITY_UNIT, IDENTITY_VISIT, IDENTITY_PRESENT }

    private final Map<String, Resolved> byMappingId;
    private final Map<String, Resolved> activityByEquipment;

    private MemberCatalog(Map<String, Resolved> byMappingId, Map<String, Resolved> activityByEquipment) {
        this.byMappingId = Collections.unmodifiableMap(byMappingId);
        this.activityByEquipment = Collections.unmodifiableMap(activityByEquipment);
    }

    public static MemberCatalog resolve(AttributionRule rule, Hierarchy hierarchy, ReleaseReader.Release release) {
        Map<String, Role> roles = new HashMap<>();
        for (AttributionRule.Activity a : rule.activity()) role(roles, a.memberRef(), Role.ACTIVITY);
        for (AttributionRule.IdentityMember i : rule.identityMembers()) {
            role(roles, i.unitMemberRef(), Role.IDENTITY_UNIT);
            role(roles, i.visitMemberRef(), Role.IDENTITY_VISIT);
            role(roles, i.presentMemberRef(), Role.IDENTITY_PRESENT);
        }
        Map<String, Resolved> all = new TreeMap<>();
        for (AttributionRule.Member m : rule.members()) {
            JsonNode set = release.document("mapping-set", new ReleaseReader.Ref(m.mappingSetId(), m.mappingSetVersion())).node();
            JsonNode mapping = null;
            for (JsonNode x : set.path("mappings")) if (m.mappingId().equals(x.path("mappingId").asText(null))) mapping = x;
            if (mapping == null) throw new IllegalStateException("member " + m.mappingId() + ": not in mapping set " + m.mappingSetId());
            JsonNode steps = mapping.path("memberPath");
            if (steps.size() != 1 || !"member".equals(steps.get(0).path("kind").asText(null)))
                throw new IllegalStateException("member " + m.mappingId() + ": memberPath is not one member step");
            String memberId = Json.text(steps.get(0), "memberId");
            JsonNode targetRef = set.path("targetInstanceRef");
            JsonNode target = release.document("target-instance", new ReleaseReader.Ref(Json.text(targetRef, "id"), Json.text(targetRef, "version"))).node();
            JsonNode defRef = target.path("semanticDefinitionRef");
            JsonNode def = release.document("semantic-definition", new ReleaseReader.Ref(Json.text(defRef, "id"), Json.text(defRef, "version"))).node();
            JsonNode leaf = null;
            for (JsonNode x : def.path("members")) if (memberId.equals(x.path("memberId").asText(null))) leaf = x;
            if (leaf == null) throw new IllegalStateException("member " + m.mappingId() + ": no member " + memberId + " in " + Json.text(def, "id"));
            String name = Json.text(leaf, "name"), semanticId = Json.text(leaf, "semanticId");
            if (!name.equals(m.metric()) || !semanticId.equals(m.semanticId()))
                throw new IllegalStateException("member " + m.mappingId() + ": metric or semanticId differs from the semantic definition");
            String path = hierarchy.path(Json.text(set, "equipmentInstanceId"));
            String station = hierarchy.stationOf(rule.stations(), path)
                .orElseThrow(() -> new IllegalStateException("member " + m.mappingId() + ": " + path + " is under no station"));
            Resolved r = new Resolved(m.mappingId(), path, station, name, Json.text(leaf, "valueType"), semanticId, roles.getOrDefault(m.mappingId(), Role.MEASUREMENT));
            if (all.put(m.mappingId(), r) != null) throw new IllegalStateException("member " + m.mappingId() + " listed twice");
        }
        for (String ref : roles.keySet()) if (!all.containsKey(ref)) throw new IllegalStateException("reference " + ref + " names no member");
        Map<String, Resolved> activity = new TreeMap<>();
        for (AttributionRule.Activity a : rule.activity()) {
            Resolved r = all.get(a.memberRef());
            if (!r.equipmentPath().equals(a.equipmentPath())) throw new IllegalStateException("activity " + a.memberRef() + ": equipment differs");
            if (activity.put(a.equipmentPath(), r) != null) throw new IllegalStateException("two activity members for " + a.equipmentPath());
        }
        return new MemberCatalog(new LinkedHashMap<>(all), activity);
    }

    private static void role(Map<String, Role> roles, String ref, Role role) {
        if (roles.put(ref, role) != null) throw new IllegalStateException("member " + ref + " has more than one role");
    }

    /** Every resolved member by {@code mappingId}, in {@code mappingId} order. */
    public Map<String, Resolved> byMappingId() { return byMappingId; }
    public Optional<Resolved> byEquipmentAndMetric(String path, String metric) {
        return byMappingId.values().stream().filter(r -> r.equipmentPath().equals(path) && r.metric().equals(metric)).findFirst();
    }
    public Optional<Resolved> activityOf(String equipmentPath) { return Optional.ofNullable(activityByEquipment.get(equipmentPath)); }
    /** The equipment paths that carry at least one member. */
    public Set<String> equipment() {
        Set<String> s = new TreeSet<>();
        byMappingId.values().forEach(r -> s.add(r.equipmentPath()));
        return Collections.unmodifiableSet(s);
    }
    /** The measurement members of one equipment, by {@code mappingId}. */
    public List<Resolved> measurementsOf(String equipmentPath) {
        return byMappingId.values().stream().filter(r -> r.role() == Role.MEASUREMENT && r.equipmentPath().equals(equipmentPath)).toList();
    }
    /** Every measurement member of the station and of its tools (spec coverage: the required members), by {@code mappingId}. */
    public List<Resolved> requiredMembersOf(String station) {
        return byMappingId.values().stream().filter(r -> r.role() == Role.MEASUREMENT && r.station().equals(station)).toList();
    }
}
