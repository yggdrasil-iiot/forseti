package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.NegativeControls;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.release.*;
import yggdrasil.forseti.core.rules.RuleSet;
import yggdrasil.forseti.core.stage1.Events;
import yggdrasil.forseti.core.stage1.Routing;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage1.Stage1State;

/** The binding puts the full station-model on every station, so under an occupancy-join rule a station's DDATA also carries UnitId,
 *  VisitId and VisitPresent, members the rule does not name. This runs the core's own stage-1 decision (Routing S1, then
 *  {@link Stage1#decide}) on the live dedicated rule set, pinned with the binding's digests, for one visit at Station1: an arrival and
 *  one scan, then a cut close. What it shows: the message parses and routes; every member, surplus ones included, gets an accepted
 *  disposition and an attributed sample row (the samples journal is per member, not per catalog metric); the work record, which
 *  carries the attribution, lists only the rule's mapping ids and gives station1 one piece of evidence, from {@code station1-running} alone. */
class SurplusMembersTest {
    static final String S1 = "site-071:line2:station1";
    static final long T = 1794183000000L;

    static RuleSet dedicated(Path state) throws java.io.IOException {
        Path m = LiveEquipmentTest.REGISTRY.resolve("mappings");
        TrustedKeys keys = TrustedKeys.load(LiveEquipmentTest.LIVE.resolve("trusted-keys.jsonl"));
        JsonNode env = Json.read(m.resolve("head-attestations").resolve("line2-dedicated.dsse.json"));
        JsonNode statement = Json.read(Base64.getDecoder().decode(env.path("payload").asText()));
        long now = Instant.parse(statement.path("predicate").path("signedAt").asText()).toEpochMilli() + 60_000;
        HeadVerifier.Head h = HeadVerifier.verify(m, "line2-dedicated", keys, new FloorStore(state), now);
        ObjectNode definitions = Json.object();
        for (LiveEquipment.Equipment e : LiveEquipmentTest.dedicated().equipment().values())
            definitions.putObject(e.device()).put("templateRef", e.definitionRef()).put("version", e.definitionVersion()).put("sha256", e.contentSha256());
        return RuleSet.load(ReleaseReader.read(m, h), "line2-attribution", "1.0.0", definitions);
    }

    static ObjectNode member(String metric, JsonNode value, long ts) {
        ObjectNode n = Json.object().put("metric", metric);
        n.set("value", value);
        Json.putSafe(n, "uaStatusCode", 0).put("quality", 192);
        Json.putSafe(n, "sourceTs", ts);
        Json.putSafe(n, "serverTs", ts + 10);
        return n;
    }

    @Test
    void surplusIdentityMembersParseAndOnlyTheRulesMetricsReachTheWorkRecord(@TempDir Path tmp) throws Exception {
        RuleSet rules = dedicated(tmp.resolve("floor"));
        Stage1 stage1 = new Stage1(rules, NegativeControls.NONE);
        Routing routing = new Routing(rules);
        Stage1State state = stage1.newState(1, S1);
        List<Object> out = new ArrayList<>();

        ObjectNode msg = Json.object().put("edge", "line2-station1").put("device", S1);
        Json.putSafe(msg, "birthPublishTs", T - 60_000).put("bdSeq", 0);
        Json.putSafe(msg, "seq", 2);
        Json.putSafe(msg, "publishTs", T + 1050);
        Json.putSafe(msg, "bridgeReceivedAt", T + 1100);
        msg.put("definitionSha256", LiveEquipmentTest.STATION_SHA);
        msg.putArray("members").add(member("Running", Json.JSON.getNodeFactory().booleanNode(true), T + 1000))
            .add(member("UnitId", Json.JSON.getNodeFactory().textNode("UNIT0000000000001"), T + 1000))
            .add(member("VisitId", Json.JSON.getNodeFactory().textNode("S1-V1"), T + 1000))
            .add(member("VisitPresent", Json.JSON.getNodeFactory().booleanNode(true), T + 1000));
        ObjectNode arrival = OccupancyRecords.arrival("S1-V1-arrive", T, T + 5, "UNIT0000000000001", S1, 1, "mes-line2");

        InputParser.Read m = InputParser.read(new Inputs.Origin(InputParser.MESSAGES, 5, 0), S1, Json.JSON.writeValueAsBytes(msg));
        InputParser.Read a = InputParser.read(new Inputs.Origin(InputParser.OCCUPANCY, 0, 0), S1, Json.JSON.writeValueAsBytes(arrival));
        assertFalse(m.rejected(), "the message with surplus members parses");
        Inputs.ProcessMessage pm = (Inputs.ProcessMessage) m.record().parsed();
        assertEquals(List.of("Running", "UnitId", "VisitId", "VisitPresent"), pm.members().stream().map(Inputs.Member::metric).toList());
        assertEquals(List.of(), pm.rejectedMembers());
        Routing.Outcome routed = routing.route(m);
        assertInstanceOf(Routing.Routed.class, routed, "routed to its station: the binding's pin is the generation pin");
        assertEquals(S1, ((Routing.Routed) routed).station());
        assertInstanceOf(Routing.Routed.class, routing.route(a));

        List<Inputs.InputRecord> cut = new ArrayList<>(List.of(a.record(), m.record()));
        cut.sort(Events.CONTRACT_ORDER);
        Long admission = state.admissionW();
        JsonNode admitted = null;   // the work record revision the message produced (the cut close below re-states it at a W1 before the scan)
        for (Inputs.InputRecord r : cut) {
            Stage1.Result res = stage1.decide(state, new Events.Admit(r, Events.kindRank(r.origin().topic())), admission);
            state = res.state(); out.addAll(res.outputs());
            if (r == m.record()) for (Object o : res.outputs()) if (o instanceof Row row && row.topic().equals(Stage1.WORK_RECORDS)) admitted = row.body();
        }
        long w = T + 1000 - rules.attribution().outOfOrdernessMs() - 1;
        Stage1.Result res = stage1.decide(state, new Events.CutClose(1, w, List.of()), w);
        out.addAll(res.outputs());

        // Every member, surplus ones included, is accepted and journaled as a sample of the visit: the core does not drop a member the
        // rule does not name (Stage1 keeps each parsed member as a sample).
        Map<String, String> memberDisposition = new TreeMap<>();
        Map<String, String> sampleState = new TreeMap<>();
        JsonNode workRecord = admitted;
        for (Object o : out) {
            if (o instanceof Disposition d && d.key().part().startsWith("member:")) memberDisposition.put(d.key().part().substring(7), d.body().reason().wire());
            if (o instanceof Row r && r.topic().equals(Stage1.SAMPLES)) sampleState.put(r.logicalKey().path("metric").asText(), r.body().path("state").asText());
        }
        Map<String, String> accepted = new TreeMap<>(), attributed = new TreeMap<>();
        for (String metric : List.of("Running", "UnitId", "VisitId", "VisitPresent")) { accepted.put(metric, "accepted"); attributed.put(metric, "attributed"); }
        assertEquals(accepted, memberDisposition);
        assertEquals(attributed, sampleState);
        for (Object o : out) if (o instanceof Disposition d) assertEquals("accepted", d.body().reason().wire(), Stage1.describe(o));

        // The work record is built from the rule's catalog only: its members are the rule's mapping ids, station1's evidence names only
        // station1-running, and the one scan counts once for it, whatever else the message carried.
        assertNotNull(workRecord);
        Set<String> mappingIds = new TreeSet<>();
        for (JsonNode x : workRecord.get("members")) mappingIds.add(x.get("mappingId").asText());
        Set<String> ruleMappings = new TreeSet<>(rules.members().byMappingId().keySet());
        ruleMappings.removeIf(id -> !S1.equals(rules.members().byMappingId().get(id).station())
            || rules.members().byMappingId().get(id).role() != yggdrasil.forseti.core.rules.MemberCatalog.Role.MEASUREMENT);
        assertEquals(ruleMappings, mappingIds, "the work record's members are the rule's measurements at the station");
        assertFalse(mappingIds.stream().anyMatch(x -> x.contains("unit") || x.contains("visit") || x.contains("present")), mappingIds.toString());
        JsonNode evidence = workRecord.get("equipmentEvidence").get(0);
        assertEquals(S1, evidence.get("equipmentPath").asText());
        assertEquals(List.of("station1-running"), Json.JSON.convertValue(evidence.get("memberRefs"), List.class));
        assertEquals(1, workRecord.get("equipmentEvidence").size(), "only station1 has evidence; the surplus members add none");
        assertEquals(1, evidence.get("activityIntervals").size(), "one scan, one activity interval from Running");
        assertEquals(1, evidence.get("coverage").get("observedPeriods").asInt());
    }
}
