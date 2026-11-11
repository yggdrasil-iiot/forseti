package yggdrasil.forseti.core.rules;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.release.*;

class RuleSetTest {
    /** The rulesDigest of each fixture rule with {@code definitions = {}}; oracle/tests/test_rules.py pins the same literals. */
    static final String DIGEST_LINE2 = "eebc7b50e48d67bd50a16fd8dcb5071d9b5262c67535b260ee78d9c68889b540";
    static final String DIGEST_LINE2_PLC = "83b158de38f98cfc0d51782b5a711e173ecc712c0cc5210347cd0f921b8f3381";
    static final String DIGEST_LINE2_NEXT = "7b7a2e369dd70829a71f7830a82725bd0700c7fed02b86cc483abe915ac3cb76";
    static final String DIGEST_LINE2_PLC_NOCHECK = "7dbcb9e79435daed9c879726ceca48701c9b52052426959a5dad025e8e6deaed";

    static final String S1 = "synthetic-site:line2:station1", S2 = "synthetic-site:line2:station2", S3 = "synthetic-site:line2:station3";
    static final String NR1 = S1 + ":nutrunner1", NR2 = S1 + ":nutrunner2";
    static final String OCCUPANCY_SHA = "e97cd7c97b1ab5e2d63d5b06e6057415945b82cff6d2ca0345106d2daf9d29dc";

    @TempDir Path tmp;
    int copies;

    ReleaseReader.Release release(String registry, String stream, TrustedKeys keys) {
        Path m = TestKeys.copy(registry, tmp.resolve("copy-" + copies++));
        return release(m, stream, keys);
    }
    ReleaseReader.Release release(Path m, String stream, TrustedKeys keys) {
        HeadVerifier.Head h = HeadVerifier.verify(m, stream, keys, new FloorStore(tmp.resolve("state-" + copies++)), TestKeys.signedAtMs(m, stream) + 60_000);
        return ReleaseReader.read(m, h);
    }
    RuleSet load(String registry, String stream, String version) {
        return RuleSet.load(release(registry, stream, TestKeys.fixtureKeys()), "line2-attribution", version, Json.object());
    }
    static Map<MemberCatalog.Role, Long> roles(RuleSet r) {
        Map<MemberCatalog.Role, Long> m = new EnumMap<>(MemberCatalog.Role.class);
        r.members().byMappingId().values().forEach(x -> m.merge(x.role(), 1L, Long::sum));
        return m;
    }
    static List<String> ids(List<MemberCatalog.Resolved> l) { return l.stream().map(MemberCatalog.Resolved::mappingId).toList(); }

    @Test void loadsTheOccupancyJoinRule() {
        RuleSet r = load("registry", "line2", "1.0.0");
        AttributionRule a = r.attribution();
        assertEquals("line2-attribution", a.id());
        assertEquals("1.0.0", a.version());
        assertEquals(List.of(S1, S2, S3), a.stations());
        assertEquals("occupancy-join", a.mode());
        assertEquals("none", a.crossCheck());
        assertTrue(a.namespace().isEmpty());
        assertEquals(List.of(), a.identityMembers());
        assertEquals(30000, a.outOfOrdernessMs());
        assertEquals(720000, a.allowedLatenessMs());
        assertEquals(86400000, a.horizonMs());
        assertEquals(45000, a.enterToleranceMs());
        assertEquals(45000, a.leaveToleranceMs());
        assertEquals("reject", a.uncertainInput());
        assertEquals(0.1, a.maxBadShare());
        assertEquals(1000, a.expectedPeriodMs());
        assertEquals(3, a.maxMissingPeriods());
        assertEquals(7, r.members().byMappingId().size());
        assertEquals(Map.of(MemberCatalog.Role.MEASUREMENT, 2L, MemberCatalog.Role.ACTIVITY, 5L), roles(r));
        assertEquals(MemberCatalog.Role.ACTIVITY, r.members().byMappingId().get("station3-running").role());
        assertEquals(List.of("nutrunner1-torque", "nutrunner2-torque"), ids(r.members().requiredMembersOf(S1)));
        assertEquals(List.of(), ids(r.members().requiredMembersOf(S2)));
        MemberCatalog.Resolved act = r.members().activityOf(NR1).orElseThrow();
        assertEquals("nutrunner1-running", act.mappingId());
        assertEquals("Boolean", act.valueType());
        assertTrue(r.members().activityOf("synthetic-site:line2").isEmpty());
        assertEquals(Set.of(S1, S2, S3, NR1, NR2), r.members().equipment());
    }

    @Test void loadsThePlcStampedRule() {
        RuleSet r = load("registry", "line2-plc", "1.0.0");
        AttributionRule a = r.attribution();
        assertEquals(List.of(S1, S2, S3), a.stations());
        assertEquals("plc-stamped", a.mode());
        assertEquals("occupancy", a.crossCheck());
        assertEquals(Optional.of("urn:line2:visits"), a.namespace());
        assertEquals(List.of(new AttributionRule.IdentityMember(S1, "station1-unit", "station1-visit", "station1-present"),
                             new AttributionRule.IdentityMember(NR1, "nutrunner1-unit", "nutrunner1-visit", "nutrunner1-present")), a.identityMembers());
        assertEquals(9, r.members().byMappingId().size());
        assertEquals(Map.of(MemberCatalog.Role.MEASUREMENT, 1L, MemberCatalog.Role.ACTIVITY, 2L,
                            MemberCatalog.Role.IDENTITY_UNIT, 2L, MemberCatalog.Role.IDENTITY_VISIT, 2L, MemberCatalog.Role.IDENTITY_PRESENT, 2L), roles(r));
        assertEquals(List.of("nutrunner1-torque"), ids(r.members().requiredMembersOf(S1)));
        assertEquals("nutrunner1-running", r.members().activityOf(NR1).orElseThrow().mappingId());
        MemberCatalog.Resolved visit = r.members().byMappingId().get("station1-visit");
        assertEquals("VisitId", visit.metric());
        assertEquals("String", visit.valueType());
        assertEquals(MemberCatalog.Role.IDENTITY_VISIT, visit.role());
        assertEquals(MemberCatalog.Role.IDENTITY_PRESENT, r.members().byMappingId().get("nutrunner1-present").role());
        assertEquals("Boolean", r.members().byMappingId().get("nutrunner1-present").valueType());
    }

    @Test void resolvesMembersThroughMappingTargetAndDefinition() {
        RuleSet r = load("registry", "line2", "1.0.0");
        MemberCatalog.Resolved t = r.members().byMappingId().get("nutrunner2-torque");
        assertEquals(new MemberCatalog.Resolved("nutrunner2-torque", NR2, S1, "Torque", "Double", "urn:bifrost:sem:Tool/Torque", MemberCatalog.Role.MEASUREMENT), t);
        assertEquals(Optional.of(t), r.members().byEquipmentAndMetric(NR2, "Torque"));
        assertTrue(r.members().byEquipmentAndMetric(NR2, "Speed").isEmpty());
        assertEquals(List.of("nutrunner1-torque"), ids(r.members().measurementsOf(NR1)));
        assertEquals(List.of(), ids(r.members().measurementsOf(S1)));
        MemberCatalog.Resolved s3 = r.members().byMappingId().get("station3-running");
        assertEquals(S3, s3.equipmentPath());
        assertEquals(S3, s3.station());
        assertEquals("urn:bifrost:sem:Station/Running", s3.semanticId());
    }

    @Test void hierarchyPathsAndStations() {
        RuleSet r = load("registry", "line2", "1.0.0");
        Hierarchy h = r.hierarchy();
        assertEquals("line2-plant", h.id());
        assertEquals(NR1, h.path("nutrunner1"));
        assertEquals("synthetic-site", h.path("synthetic-site"));
        assertEquals(Optional.of(S1), h.stationOf(r.attribution().stations(), NR2));
        assertEquals(Optional.of(S3), h.stationOf(r.attribution().stations(), S3));
        assertTrue(h.stationOf(r.attribution().stations(), "synthetic-site:line2").isEmpty());
        assertTrue(h.stationOf(r.attribution().stations(), S1 + "x").isEmpty());
        assertTrue(h.isDescendantOrSelf(S1, NR1));
        assertTrue(h.isDescendantOrSelf(S1, S1));
        assertFalse(h.isDescendantOrSelf(S1, S2));
        assertFalse(h.isDescendantOrSelf(NR1, S1));
        assertThrows(IllegalArgumentException.class, () -> h.path("nowhere"));
        ObjectNode cyclic = Json.object().put("id", "h").put("version", "1.0.0");
        ArrayNode nodes = cyclic.putArray("nodes");
        nodes.addObject().put("equipmentInstanceId", "a").put("parentEquipmentInstanceId", "b");
        nodes.addObject().put("equipmentInstanceId", "b").put("parentEquipmentInstanceId", "a");
        assertThrows(IllegalArgumentException.class, () -> Hierarchy.from(cyclic, "0".repeat(64)).path("a"));
    }

    @Test void impactRuleAndBindings() {
        RuleSet r = load("registry", "line2", "1.0.0");
        ImpactRule i = r.impact();
        assertEquals("line2-impact", i.id());
        assertEquals(List.of(NR1, NR2), i.incidentScope());
        assertEquals("any-overlap", i.overlap());
        assertEquals("equipment-evidence", i.match());
        assertEquals("by-cause", i.onBasisRetracted());
        assertEquals("not-affected", i.onWithdrawn());
        assertEquals(604800000, i.workRecordsMs());
        OccupancyBinding o = r.occupancy();
        assertEquals("mfg.occupancy", o.topic());
        assertEquals(InputParser.OCCUPANCY_SCHEMA_ID, o.schemaId());
        assertEquals(OCCUPANCY_SHA, o.schemaSha256());
        assertTrue(o.matches("UNIT0000000000001"));
        assertFalse(o.matches("unit-1"));
        assertFalse(o.matches("UNIT00000000000012"), "the pattern matches the whole unit id");
        assertEquals("urn:epcglobal:cbv:bizstep:arriving", o.enterBizStep());
        assertEquals("urn:epcglobal:cbv:bizstep:departing", o.leaveBizStep());
        assertEquals(List.of("urn:epcglobal:cbv:er:did_not_occur", "urn:epcglobal:cbv:er:incorrect_data"), o.correctionReasons());
        assertEquals(List.of("mes-line2"), o.sources());
        IncidentBinding n = r.incident();
        assertEquals("quality.incidents", n.topic());
        assertEquals(InputParser.INCIDENT_SCHEMA_ID, n.schemaId());
        assertEquals(InputParser.schemaSha256(InputParser.INCIDENT_SCHEMA_ID), n.schemaSha256());
        assertEquals(List.of("tool-check-failed", "gauge-drift"), n.kinds());
    }

    @Test void pinsNameEachDocumentByItsReleaseDigest() {
        ReleaseReader.Release rel = release("registry", "line2", TestKeys.fixtureKeys());
        RuleSet r = RuleSet.load(rel, "line2-attribution", "1.0.0", Json.object());
        JsonNode p = r.pins();
        assertEquals(List.of("bindings", "definitions", "hierarchy", "rules"), sortedFields(p));
        assertEquals(pin("line2-attribution", "1.0.0", rel.files().get("payload/attribution-rules/0.json")), p.path("rules").path("attribution"));
        assertEquals(pin("line2-impact", "1.0.0", rel.files().get("payload/impact-rules/0.json")), p.path("rules").path("impact"));
        assertEquals(pin("line2-plant", "1.0.0", rel.files().get("payload/hierarchy/0.json")), p.path("hierarchy"));
        assertEquals(pin("occupancy-binding-line2", "1.0.0", rel.files().get("payload/evidence/occupancy-binding-0.json")), p.path("bindings").path("occupancy"));
        assertEquals(pin("incident-binding-line2", "1.0.0", rel.files().get("payload/evidence/incident-binding-0.json")), p.path("bindings").path("incident"));
        assertEquals(Json.object(), p.path("definitions"));
        assertEquals(Canonical.digest(p), r.rulesDigest());
    }

    @Test void rulesDigestIsPinnedAndChangesWithDefinitions() {
        RuleSet line2 = load("registry", "line2", "1.0.0"), plc = load("registry", "line2-plc", "1.0.0");
        assertTrue(line2.rulesDigest().matches("[0-9a-f]{64}"));
        assertEquals(DIGEST_LINE2, line2.rulesDigest());
        assertEquals(DIGEST_LINE2_PLC, plc.rulesDigest());
        ObjectNode defs = Json.object();
        defs.putObject(NR1).put("templateRef", "tool-stamped-model").put("version", "1.0.0").put("sha256", "ab".repeat(32));
        RuleSet withDefs = RuleSet.load(release("registry", "line2", TestKeys.fixtureKeys()), "line2-attribution", "1.0.0", defs);
        assertNotEquals(line2.rulesDigest(), withDefs.rulesDigest());
        assertEquals(defs, withDefs.pins().path("definitions"));
    }

    @Test void loadsRule101FromRegistryNext() {
        RuleSet r = load("registry-next", "line2", "1.0.1");
        assertEquals("1.0.1", r.attribution().version());
        assertEquals(4, r.attribution().maxMissingPeriods());
        assertEquals("1.0.1", r.impact().version());
        assertEquals(DIGEST_LINE2_NEXT, r.rulesDigest());
        assertNotEquals(DIGEST_LINE2, r.rulesDigest());
        assertThrows(Refusal.class, () -> RuleSet.load(release("registry-next", "line2", TestKeys.fixtureKeys()), "line2-attribution", "1.0.0", Json.object()),
            "the 1.0.1 release does not carry rule 1.0.0");
    }

    @Test void loadsThePlcNocheckRule() {
        RuleSet r = load("registry", "line2-plc-nocheck", "1.0.1");
        assertEquals("1.0.1", r.attribution().version());
        assertEquals("none", r.attribution().crossCheck());
        assertEquals("plc-stamped", r.attribution().mode());
        assertEquals("1.0.1", r.impact().version());
        assertEquals(DIGEST_LINE2_PLC_NOCHECK, r.rulesDigest());
    }

    @Test void aProfileWithAnotherDigestIsASchemaMismatch() {
        ReleaseReader.Release rel = release("registry-mismatch", "line2", TestKeys.fixtureKeys());   // head and release verify
        Refusal e = assertThrows(Refusal.class, () -> RuleSet.load(rel, "line2-attribution", "1.0.0", Json.object()));
        assertEquals("binding.schema-mismatch", e.reason);
        assertEquals("occupancy-binding-line2", e.evidence.get("profile"));
        assertEquals("0".repeat(63) + "1", e.evidence.get("approved"));
        assertEquals(OCCUPANCY_SHA, e.evidence.get("expected"));
        assertEquals(InputParser.schemaSha256(InputParser.OCCUPANCY_SCHEMA_ID), e.evidence.get("expected"));
    }

    @Test void anUnsupportedKindIsASchemaMismatch() {
        Path m = TestKeys.copy("registry", tmp);
        String data = "data/incident-binding-0.json", artifact = "evidence/incident-binding-0.json";
        Path dir = m.resolve("releases/line2-rules/1.0.0/payload");
        ObjectNode profile = (ObjectNode) Json.read(dir.resolve(data));
        ((ArrayNode) profile.path("kinds")).add("spindle-overheat");
        byte[] profileBytes = Json.pretty(profile);
        TestKeys.rewritePayload(m, "line2-rules", "1.0.0", data, profileBytes);
        ObjectNode ev = (ObjectNode) Json.read(dir.resolve(artifact));
        ((ObjectNode) ev.path("file")).put("sha256", Canonical.digestOf(profileBytes));
        TestKeys.rewritePayload(m, "line2-rules", "1.0.0", artifact, Json.pretty(ev));
        KeyPair p = TestKeys.generate(), a = TestKeys.generate();
        TestKeys.republish(m, "line2", new TestKeys.Signer("proposer", p.getPrivate()), new TestKeys.Signer("approver", a.getPrivate()));
        ReleaseReader.Release rel = release(m, "line2", TestKeys.trusted(tmp, "proposer", p.getPublic(), "approver", a.getPublic()));
        Refusal e = assertThrows(Refusal.class, () -> RuleSet.load(rel, "line2-attribution", "1.0.0", Json.object()));
        assertEquals("binding.schema-mismatch", e.reason);
        assertEquals("kind-unsupported", e.evidence.get("rule"));
        assertEquals("spindle-overheat", e.evidence.get("kind"));
        assertEquals("incident-binding-line2", e.evidence.get("profile"));
    }

    static ObjectNode pin(String id, String version, String sha) { return Json.object().put("id", id).put("version", version).put("sha256", sha); }
    static List<String> sortedFields(JsonNode n) { List<String> l = new ArrayList<>(); n.fieldNames().forEachRemaining(l::add); Collections.sort(l); return l; }
}
