package yggdrasil.forseti.cli.live;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.cli.recording.Telemetry;
import yggdrasil.forseti.core.Json;

/** The live scenario format and its schedule: the committed baseline drives both placements, holds the four Station1 tool patterns
 *  and visits at Station2 and Station3, and its plan writes whole-equipment snapshots with the step-3 telemetry rules. */
class DrivePlanTest {
    static final Path BASELINE = Path.of("..", "fixtures", "live-scenarios", "baseline.json");
    static final String S1 = "site-071:line2:station1", T1 = S1 + ":nutrunner1", T2 = S1 + ":nutrunner2", S2 = "site-071:line2:station2", S3 = "site-071:line2:station3";

    static LiveScenario baseline(LiveEquipment.LiveStream s) { return LiveScenario.read(Json.read(BASELINE), s); }

    static DrivePlan.Step stepAt(List<DrivePlan.Step> steps, long at) {
        for (DrivePlan.Step s : steps) if (s.at() == at) return s;
        throw new AssertionError("no step at " + at);
    }

    static DrivePlan.Snapshot snapshotOf(DrivePlan.Step step, String device) {
        for (DrivePlan.Snapshot s : step.snapshots()) if (s.device().equals(device)) return s;
        return null;
    }

    @Test
    void theBaselineHoldsTheFourStation1ToolPatternsAndVisitsAtEveryStation() {
        for (String stream : List.of("line2-dedicated", "line2-shared", "line2-dedicated-plc", "line2-shared-plc")) {
            LiveEquipment.LiveStream s = LiveEquipment.read(LiveEquipmentTest.REGISTRY, LiveEquipmentTest.DEFINITIONS, stream);
            LiveScenario sc = baseline(s);
            String site = LiveScenario.site(s);
            Set<Set<String>> patterns = new HashSet<>();
            Set<String> stations = new TreeSet<>();
            for (LiveScenario.Visit v : sc.visits()) {
                stations.add(v.station());
                if (v.station().equals(site + ":line2:station1")) {
                    Set<String> t = new TreeSet<>();
                    for (String d : v.tools()) t.add(d.substring(d.lastIndexOf(':') + 1));
                    patterns.add(t);
                    assertEquals(60000, v.leave() - v.enter());
                }
            }
            assertEquals(Set.of(Set.of("nutrunner1"), Set.of("nutrunner2"), Set.of("nutrunner1", "nutrunner2"), Set.of()), patterns, stream);
            assertEquals(Set.copyOf(s.stations()), stations, stream);
        }
        LiveScenario sc = baseline(LiveEquipmentTest.dedicated());
        List<LiveScenario.Visit> s1 = sc.visits().stream().filter(v -> v.station().equals(S1)).toList();
        for (int i = 1; i < s1.size(); i++) assertEquals(120000, s1.get(i).enter() - s1.get(i - 1).leave(), "120 s between adjacent Station1 visits");
    }

    @Test
    void theFirstStepWritesEveryDeviceAndEachSnapshotIsTheWholeEquipment() {
        LiveEquipment.LiveStream s = LiveEquipmentTest.dedicated();
        List<DrivePlan.Step> steps = DrivePlan.plan(baseline(s), s);
        DrivePlan.Step first = steps.get(0);
        assertEquals(0, first.at());
        assertEquals(List.of(S1, S2, S3, T1, T2), first.snapshots().stream().map(DrivePlan.Snapshot::device).toList(), "stations first");
        DrivePlan.Snapshot t1 = snapshotOf(first, T1);
        List<DrivePlan.Write> w = t1.writes();
        assertEquals(10, w.size());
        assertEquals(new DrivePlan.Write("Sim/Line2/Station1/Nutrunner1.Torque.Value", "Double", 0.0), w.get(0));
        assertEquals(new DrivePlan.Write("Sim/Line2/Station1/Nutrunner1.Torque.StatusCode", "UInt32", 0L), w.get(1));
        assertEquals(new DrivePlan.Write("Sim/Line2/Station1/Nutrunner1.UnitId.Value", "String", ""), w.get(4));
        assertEquals(8, snapshotOf(first, S2).writes().size());
        assertEquals(List.of("Line2/Station2.Running", "Line2/Station2.UnitId", "Line2/Station2.VisitId", "Line2/Station2.VisitPresent"), snapshotOf(first, S2).liveNodes());
        assertTrue(first.events().isEmpty());
    }

    @Test
    void aVisitArrivesWithItsStationAndStampsEveryToolButRunsOnlyTheToolsItUses() {
        LiveEquipment.LiveStream s = LiveEquipmentTest.dedicated();
        LiveScenario sc = baseline(s);
        List<DrivePlan.Step> steps = DrivePlan.plan(sc, s);
        DrivePlan.Step enter = stepAt(steps, 60000);   // S1-V1, nutrunner1 only
        DrivePlan.Snapshot st = snapshotOf(enter, S1), t1 = snapshotOf(enter, T1), t2 = snapshotOf(enter, T2);
        assertEquals(List.of(true, "UNIT0000000000001", "S1-V1", true), new ArrayList<>(st.values().values()));
        assertEquals(List.of(0.0, false, "UNIT0000000000001", "S1-V1", true), new ArrayList<>(t2.values().values()), "unused tool: VisitPresent, not Running");
        assertEquals(List.of(0.0, false, "UNIT0000000000001", "S1-V1", true), new ArrayList<>(t1.values().values()), "the used tool starts 5 s after enter");
        assertEquals(List.of(new DrivePlan.Arrival(sc.visit("S1-V1"))), enter.events());

        DrivePlan.Snapshot run = snapshotOf(stepAt(steps, 65000), T1);
        double torque = Telemetry.torque(20261109, T1, 65, new BigDecimal("10.0"), new BigDecimal("45.0")).doubleValue();
        assertEquals(List.of(torque, true, "UNIT0000000000001", "S1-V1", true), new ArrayList<>(run.values().values()));
        assertNull(snapshotOf(stepAt(steps, 65000), T2), "an unchanged tool is not written");
        assertTrue(snapshotOf(stepAt(steps, 119000), T1).values().get("Running").equals(true), "the last measurement 1 s before leave");
        assertEquals(false, snapshotOf(stepAt(steps, 120000), T1).values().get("Running"));

        DrivePlan.Step leave = stepAt(steps, 120000);
        assertEquals(List.of(false, "UNIT0000000000001", "S1-V1", false), new ArrayList<>(snapshotOf(leave, S1).values().values()), "after the visit the ids stay");
        assertEquals(List.of(new DrivePlan.Departure(sc.visit("S1-V1"))), leave.events());

        DrivePlan.Step both = stepAt(steps, 425000);   // S1-V3 runs both tools
        assertEquals(true, snapshotOf(both, T1).values().get("Running"));
        assertEquals(true, snapshotOf(both, T2).values().get("Running"));
        assertEquals(List.of(false, "UNIT0000000000004", "S1-V4", true), new ArrayList<>(snapshotOf(stepAt(steps, 600000), T1).values().values()).subList(1, 5));
        assertTrue(steps.stream().noneMatch(x -> x.at() > 600000 && x.at() < 660000), "nothing changes during a no-tool visit");
    }

    static ObjectNode withInjections(ObjectNode base) {
        ObjectNode n = base.deepCopy();
        ArrayNode bad = n.putArray("badStatus");
        bad.addObject().put("equipment", "line2:station1:nutrunner1").put("member", "Torque").put("statusCode", 2147483648L).put("from", 70000).put("to", 80000);
        ArrayNode corr = n.putArray("corrections");
        corr.addObject().put("event", "S2-V1-arrive").put("at", 230000).put("reason", "urn:epcglobal:cbv:er:did_not_occur").putArray("correctiveEventIDs");
        ArrayNode inc = n.putArray("incidents");
        ObjectNode i = inc.addObject().put("incidentId", "inc-1").put("equipment", "line2:station1:nutrunner1").put("kind", "tool-check-failed")
            .put("evidenceRef", "urn:line2:qms:inc-1").put("source", "qms-line2");
        ObjectNode r1 = i.putArray("revisions").addObject().put("revision", 1).put("at", 121000).put("status", "closed").put("suspectTo", "detected");
        r1.putObject("suspectFrom").put("visit", "S1-V1").put("at", "enter");
        ObjectNode r2 = ((ArrayNode) i.get("revisions")).addObject().put("revision", 2).put("at", 500000).put("status", "withdrawn").put("suspectTo", "detected");
        r2.putObject("suspectFrom").put("visit", "S1-V1").put("at", "enter");
        return n;
    }

    @Test
    void badStatusCorrectionsAndIncidentsBecomeSnapshotsAndEventsOfTheirSteps() {
        LiveEquipment.LiveStream s = LiveEquipmentTest.dedicated();
        LiveScenario sc = LiveScenario.read(withInjections((ObjectNode) Json.read(BASELINE)), s);
        List<DrivePlan.Step> steps = DrivePlan.plan(sc, s);
        DrivePlan.Snapshot bad = snapshotOf(stepAt(steps, 70000), T1);
        assertEquals(2147483648L, bad.statuses().get("Torque"));
        assertEquals(0L, bad.statuses().get("Running"));
        assertEquals(new DrivePlan.Write("Sim/Line2/Station1/Nutrunner1.Torque.StatusCode", "UInt32", 2147483648L), bad.writes().get(1));
        assertEquals(0L, snapshotOf(stepAt(steps, 80000), T1).statuses().get("Torque"));
        DrivePlan.Step corr = stepAt(steps, 230000);
        assertTrue(corr.snapshots().isEmpty());
        assertEquals(List.of(new DrivePlan.Correction(sc.corrections().get(0), sc.visit("S2-V1"))), corr.events());
        DrivePlan.Step inc = stepAt(steps, 121000);
        assertEquals(1, inc.events().size());
        assertEquals(S1, inc.events().get(0).station());
        assertEquals(2, ((DrivePlan.IncidentRevision) stepAt(steps, 500000).events().get(0)).revision().revision());
        assertEquals(660000, steps.get(steps.size() - 1).at());
    }

    @Test
    void whatTheDriveCouldNotCarryOutIsRefused() {
        LiveEquipment.LiveStream s = LiveEquipmentTest.dedicated();
        ObjectNode base = (ObjectNode) Json.read(BASELINE);
        Map<String, java.util.function.Consumer<ObjectNode>> cases = new LinkedHashMap<>();
        cases.put("overlap", n -> ((ObjectNode) n.withArray("visits").get(1)).put("enter", 100000));
        cases.put("is not a tool of", n -> ((ArrayNode) n.withArray("visits").get(4).get("tools")).add("nutrunner1"));
        cases.put("multiple of scanMs", n -> ((ObjectNode) n.withArray("visits").get(0)).put("leave", 120500));
        cases.put("identity pattern", n -> ((ObjectNode) n.withArray("visits").get(0)).put("unit", "u1"));
        cases.put("field extra", n -> n.put("extra", 1));
        cases.put("has no device", n -> ((ObjectNode) n.withArray("visits").get(0)).put("station", "line3:station1"));
        cases.put("not a Bad status code", n -> n.putArray("badStatus").addObject().put("equipment", "line2:station2").put("member", "Running")
            .put("statusCode", 1073741824L).put("from", 0).put("to", 1000));
        cases.put("names no occupancy event", n -> n.putArray("corrections").addObject().put("event", "S9-arrive").put("at", 1000)
            .put("reason", "urn:epcglobal:cbv:er:did_not_occur"));
        cases.put("not after the event", n -> n.putArray("corrections").addObject().put("event", "S1-V1-depart").put("at", 120000)
            .put("reason", "urn:epcglobal:cbv:er:did_not_occur"));
        cases.put("occupancy.source", n -> n.with("occupancy").put("source", "mes-line9"));
        cases.put("after the revision is sent", n -> {
            ObjectNode i = n.putArray("incidents").addObject().put("incidentId", "i").put("equipment", "line2:station1").put("kind", "gauge-drift")
                .put("evidenceRef", "e").put("source", "q");
            ObjectNode r = i.putArray("revisions").addObject().put("revision", 1).put("at", 100000).put("status", "open").putNull("suspectTo");
            r.putObject("suspectFrom").put("visit", "S1-V1").put("at", "leave");
        });
        for (var c : cases.entrySet()) {
            ObjectNode n = base.deepCopy();
            c.getValue().accept(n);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> LiveScenario.read(n, s), c.getKey());
            assertTrue(e.getMessage().contains(c.getKey().equals("field extra") ? "field extra" : c.getKey()), c.getKey() + ": " + e.getMessage());
        }
    }

    @Test
    void aScenarioWithoutOccupancyStillStampsTheIdsTheirPlcRegimeReads() {
        LiveEquipment.LiveStream s = LiveEquipment.read(LiveEquipmentTest.REGISTRY, LiveEquipmentTest.DEFINITIONS, "line2-dedicated-plc");
        ObjectNode n = (ObjectNode) Json.read(BASELINE);
        n.with("occupancy").put("emit", false);
        List<DrivePlan.Step> steps = DrivePlan.plan(LiveScenario.read(n, s), s);
        assertTrue(steps.stream().allMatch(x -> x.events().isEmpty()));
        DrivePlan.Step enter = stepAt(steps, 60000);
        assertEquals("S1-V1", snapshotOf(enter, S1).values().get("VisitId"));
        assertEquals(true, snapshotOf(enter, S1).values().get("VisitPresent"));
        assertEquals("S1-V1", snapshotOf(enter, T2).values().get("VisitId"));
    }
}
