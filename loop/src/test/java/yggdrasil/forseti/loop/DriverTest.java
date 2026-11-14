package yggdrasil.forseti.loop;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.cli.live.*;
import yggdrasil.forseti.core.Json;

/** The driver against a fake sim and clock: one Write request per equipment snapshot, event times read back from the committing scan's
 *  source timestamp (never the client clock), and the cases where the committing scan cannot be identified. */
class DriverTest {
    static final Path LIVE = Path.of("..", "fixtures", "live");
    static final Path BASELINE = Path.of("..", "fixtures", "live-scenarios", "baseline.json");
    static final String S1 = "site-071:line2:station1", T1 = S1 + ":nutrunner1", T2 = S1 + ":nutrunner2";

    static LiveEquipment.LiveStream stream() { return LiveEquipment.read(LIVE.resolve("registry"), LIVE.resolve("definitions"), "line2-dedicated"); }

    /** Collects what the driver sends. */
    static final class Sink implements EventSink {
        final List<ObjectNode> occupancy = new ArrayList<>(), incidents = new ArrayList<>();
        public void occupancy(ObjectNode e) { occupancy.add(e); }
        public void incident(ObjectNode r) { incidents.add(r); }
        ObjectNode event(String id) {
            ObjectNode found = null;
            for (ObjectNode o : occupancy) if (o.path("eventId").asText().equals(id) && !o.has("errorDeclaration")) found = o;
            return Objects.requireNonNull(found, id);
        }
    }

    record Run(FakeSim sim, Sink sink, List<Driver.StepResult> results, List<DrivePlan.Step> steps, List<ObjectNode> log) {}

    static Run drive(ObjectNode scenario, long pollMs, java.util.function.Consumer<FakeSim> setup) throws InterruptedException {
        LiveEquipment.LiveStream s = stream();
        LiveScenario sc = LiveScenario.read(scenario, s);
        FakeSim sim = new FakeSim(s, 1000, 437);
        setup.accept(sim);
        Sink sink = new Sink();
        List<ObjectNode> log = new ArrayList<>();
        List<DrivePlan.Step> steps = DrivePlan.plan(sc, s);
        List<Driver.StepResult> results = new Driver(sim, sink, sim, sc, s, pollMs, 10_000, log::add).run(steps);
        return new Run(sim, sink, results, steps, log);
    }

    static ObjectNode baseline() { return (ObjectNode) Json.read(BASELINE); }

    static long committedTs(Run r, String node) {
        for (FakeSim.Request q : r.sim().requests) if (q.items().get(0).nodeId().startsWith("Sim/" + node + ".")) return q.committedAt()[0];
        throw new AssertionError("no write of " + node);
    }

    @Test
    void everySnapshotIsOneWriteRequestOfTheWholeEquipment() throws Exception {
        Run r = drive(baseline(), 10, s -> {});
        int snapshots = r.steps().stream().mapToInt(x -> x.snapshots().size()).sum();
        assertEquals(snapshots, r.sim().requests.size());
        for (FakeSim.Request q : r.sim().requests) {
            String first = q.items().get(0).nodeId();
            String node = first.substring(4, first.indexOf('.'));
            Set<String> nodes = new TreeSet<>();
            for (DrivePlan.Write w : q.items()) { assertTrue(w.nodeId().startsWith("Sim/" + node + "."), w.nodeId()); nodes.add(w.nodeId()); }
            int members = node.contains("Nutrunner") ? 5 : 4;
            assertEquals(2 * members, nodes.size(), "every member's .Value and .StatusCode: " + node);
            assertTrue(q.committedAt()[0] > 0, "committed by a scan");
        }
        Set<Long> scans = new HashSet<>();
        for (Driver.StepResult s : r.results()) for (long ts : s.commits().values()) assertTrue(ts >= FakeSim.SKEW);
        for (FakeSim.Request q : r.sim().requests) scans.add(q.committedAt()[0]);
        assertTrue(scans.size() > 1);
    }

    @Test
    void occupancyEventsCarryTheSourceTimestampOfTheScanThatCommittedTheirStation() throws Exception {
        Run r = drive(baseline(), 10, s -> {});
        assertEquals(16, r.sink().occupancy.size());
        for (DrivePlan.Step step : r.steps()) for (DrivePlan.Event e : step.events()) {
            LiveScenario.Visit v = e instanceof DrivePlan.Arrival a ? a.visit() : ((DrivePlan.Departure) e).visit();
            ObjectNode o = r.sink().event(e instanceof DrivePlan.Arrival ? v.arrivalId() : v.departureId());
            Driver.StepResult res = r.results().stream().filter(x -> x.at() == step.at()).findFirst().orElseThrow();
            assertEquals(res.commits().get(v.station()).longValue(), o.get("eventTime").longValue(), v.visitId());
            assertTrue(o.get("eventTime").longValue() >= FakeSim.SKEW, "a sim time");
            assertTrue(o.get("recordTime").longValue() < FakeSim.SKEW, "recordTime is the driver's clock");
            assertEquals(v.station(), o.get("readPoint").asText());
        }
        ObjectNode arrive = r.sink().event("S1-V1-arrive"), depart = r.sink().event("S1-V1-depart");
        assertEquals("S1-V1-arrive", depart.get("arrivalEventId").asText());
        // the first scan in which the station shows Running=true is the arrival's time
        FakeSim.Request enter = r.sim().requests.stream().filter(q -> q.items().get(0).nodeId().startsWith("Sim/Line2/Station1.")
            && Boolean.TRUE.equals(q.items().get(0).value())).findFirst().orElseThrow();
        assertEquals(enter.committedAt()[0], arrive.get("eventTime").longValue());
        assertEquals(60000, depart.get("eventTime").longValue() - arrive.get("eventTime").longValue(), "a 60 s visit on a steady scan");
        assertEquals(r.results().size(), r.log().size());
        assertEquals(arrive, r.log().stream().filter(l -> l.get("at").longValue() == 60000).findFirst().orElseThrow().get("sent").get(0));
    }

    @Test
    void anUnusedToolIsWrittenWithTheVisitIdsPresentAndNotRunning() throws Exception {
        Run r = drive(baseline(), 10, s -> {});
        // after the driver wrote the S1-V2 enter (nutrunner2 only), nutrunner1 holds the visit's ids, VisitPresent=true, Running=false
        FakeSim.Request q = r.sim().requests.stream().filter(x -> x.items().get(0).nodeId().startsWith("Sim/Line2/Station1/Nutrunner1.")
            && x.items().stream().anyMatch(w -> "S1-V2".equals(w.value()))).findFirst().orElseThrow();
        Map<String, Object> v = new HashMap<>();
        for (DrivePlan.Write w : q.items()) v.put(w.nodeId(), w.value());
        assertEquals(true, v.get("Sim/Line2/Station1/Nutrunner1.VisitPresent.Value"));
        assertEquals(false, v.get("Sim/Line2/Station1/Nutrunner1.Running.Value"));
        assertEquals("UNIT0000000000002", v.get("Sim/Line2/Station1/Nutrunner1.UnitId.Value"));
        assertEquals(0.0, v.get("Sim/Line2/Station1/Nutrunner1.Torque.Value"));
    }

    static ObjectNode injections() {
        ObjectNode n = baseline();
        n.putArray("badStatus").addObject().put("equipment", "line2:station1:nutrunner1").put("member", "Torque").put("statusCode", 2147483648L)
            .put("from", 70000).put("to", 80000);
        n.putArray("corrections").addObject().put("event", "S2-V1-arrive").put("at", 230000).put("reason", "urn:epcglobal:cbv:er:did_not_occur").putArray("correctiveEventIDs");
        ObjectNode i = n.putArray("incidents").addObject().put("incidentId", "inc-1").put("equipment", "line2:station1:nutrunner1").put("kind", "tool-check-failed")
            .put("evidenceRef", "urn:line2:qms:inc-1").put("source", "qms-line2");
        ArrayNode revs = i.putArray("revisions");
        ObjectNode r1 = revs.addObject().put("revision", 1).put("at", 121000).put("status", "closed").put("suspectTo", "detected");
        r1.putObject("suspectFrom").put("visit", "S1-V1").put("at", "enter");
        ObjectNode r2 = revs.addObject().put("revision", 2).put("at", 500000).put("status", "withdrawn");
        r2.putObject("suspectFrom").put("visit", "S1-V1").put("at", "enter");
        r2.putObject("suspectTo").put("visit", "S1-V1").put("at", "leave");
        return n;
    }

    @Test
    void injection3CorrectionsAndIncidentsAreDatedBySimSourceTimestamps() throws Exception {
        Run r = drive(injections(), 10, s -> {});
        FakeSim.Request bad = r.sim().requests.stream().filter(q -> q.items().contains(new DrivePlan.Write("Sim/Line2/Station1/Nutrunner1.Torque.StatusCode", "UInt32", 2147483648L)))
            .findFirst().orElseThrow();
        assertTrue(bad.committedAt()[0] > 0, "the Bad status snapshot was committed and read back with its status");
        ObjectNode correction = r.sink().occupancy.stream().filter(o -> o.has("errorDeclaration")).findFirst().orElseThrow();
        assertEquals("S2-V1-arrive", correction.get("eventId").asText());
        long declared = correction.get("errorDeclaration").get("declarationTime").longValue();
        assertTrue(declared >= FakeSim.SKEW && declared > r.sink().event("S2-V1-depart").get("eventTime").longValue());
        assertEquals(r.sink().event("S2-V1-arrive").get("eventTime"), correction.get("eventTime"));

        assertEquals(2, r.sink().incidents.size());
        ObjectNode i1 = r.sink().incidents.get(0), i2 = r.sink().incidents.get(1);
        long detected = i1.get("detectedAt").longValue();
        assertTrue(detected >= FakeSim.SKEW);
        assertTrue(detected > r.sink().event("S1-V1-depart").get("eventTime").longValue(), "a step without snapshots takes the next newer scan");
        assertEquals(detected, i1.get("revisionEventTime").longValue());
        assertEquals(detected, i1.get("suspectTo").longValue());
        assertEquals(r.sink().event("S1-V1-arrive").get("eventTime"), i1.get("suspectFrom"));
        assertEquals(S1, i1.get("stationRef").asText());
        assertEquals(T1, i1.get("equipmentRef").asText());
        assertEquals(detected, i2.get("detectedAt").longValue());
        assertTrue(i2.get("revisionEventTime").longValue() > detected);
        assertEquals(r.sink().event("S1-V1-depart").get("eventTime"), i2.get("suspectTo"));
    }

    @Test
    void aStraddlingReadIsRepeatedAndAScanBetweenReadAndWriteIsNotTakenForTheCommit() throws Exception {
        LiveEquipment.LiveStream s = stream();
        LiveScenario sc = LiveScenario.read(baseline(), s);
        FakeSim sim = new FakeSim(s, 1000, 437);
        Driver d = new Driver(sim, new Sink(), sim, sc, s, 10, 10_000, l -> {});
        DrivePlan.Snapshot st1 = DrivePlan.stateAt(sc, s.device(S1), 60000);

        sim.straddleNextRead = true;
        int before = sim.reads;
        long ts = d.writeAndAwait(st1);
        assertEquals(sim.requests.get(0).committedAt()[0], ts);
        assertTrue(sim.reads - before >= 3, "the straddling read was repeated");

        sim.sleep(5000);
        DrivePlan.Snapshot back = DrivePlan.stateAt(sc, s.device(S1), 120000);
        sim.scanBeforeNextWrite = true;   // a scan lands after the driver's first read but before its write
        long ts2 = d.writeAndAwait(back);
        assertEquals(sim.requests.get(1).committedAt()[0], ts2);
        assertTrue(ts2 - sim.requests.get(1).at() - FakeSim.SKEW > 0, "the commit is the scan after the write, not the one before it");
    }

    @Test
    void aCommitThatMayHaveSkippedAScanOrARefusedItemFailsTheDrive() throws Exception {
        Driver.DriveFailure f = assertThrows(Driver.DriveFailure.class, () -> drive(baseline(), 2500, s -> {}));
        assertTrue(f.getMessage().contains("a scan may have been missed"), f.getMessage());
        f = assertThrows(Driver.DriveFailure.class, () -> drive(baseline(), 10, s -> s.refuse = "Sim/Line2/Station2.VisitId.Value"));
        assertTrue(f.getMessage().contains("the sim refused Sim/Line2/Station2.VisitId.Value (0x80740000)"), f.getMessage());
        f = assertThrows(Driver.DriveFailure.class, () -> drive(baseline(), 10, s -> s.missing.add("Line2/Station3.Running")));
        assertTrue(f.getMessage().contains("does not exist"), f.getMessage());
    }
}
