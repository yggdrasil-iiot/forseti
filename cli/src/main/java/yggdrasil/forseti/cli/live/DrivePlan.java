package yggdrasil.forseti.cli.live;

import java.util.*;
import yggdrasil.forseti.cli.recording.Telemetry;

/** The schedule of a live scenario as steps on the scan grid, without OPC UA or Kafka (step-5 plan D1): at each step, the equipment
 *  snapshots to write and the events to send once the sim has committed them.
 *
 *  <p>The state of a device at offset t follows the step-3 builder's telemetry rules ({@code ScenarioBuilder}), so a live run and a
 *  synthetic one describe a visit the same way. A station is {@code Running} and {@code VisitPresent} over {@code [enter, leave)} with
 *  the visit's {@code UnitId} and {@code VisitId}; after a visit it keeps the last ids with {@code VisitPresent=false} and
 *  {@code Running=false}; before any visit the ids are empty. A tool under the station reports the same ids and presence; it is
 *  {@code Running} only when the visit uses it, from {@code enter + startAfterEnter} to {@code leave - endBeforeLeave} inclusive (the
 *  last measurement 1 s before leave), with {@code Torque = Telemetry.torque(seed, device, t / scanMs, min, max)} while running and
 *  {@code 0.0} otherwise (an unused tool: {@code VisitPresent=true}, {@code Running=false}, spec line 641). Every StatusCode is 0 (Good)
 *  except where a {@code badStatus} covers {@code [from, to)}.
 *
 *  <p>A step at t writes a snapshot of every device whose state at t differs from its state at {@code t - scanMs} (every device at
 *  t = 0), and carries the events whose time is t: departures, arrivals, corrections, incident revisions, in that order. A snapshot is
 *  the whole equipment, every member's {@code .Value} and {@code .StatusCode}, because the sim commits one Write request in one scan
 *  and the driver sends one request per snapshot (Bifrost step-4 plan, "Note for step 5"). Stations come before tools. */
public final class DrivePlan {
    private DrivePlan() {}

    /** One item of a Write request: a control node, the OPC UA built-in type to encode, and the value. */
    public record Write(String nodeId, String type, Object value) {}

    /** The whole state of one device at a step: values in definition order and their status codes. */
    public record Snapshot(String device, String namespaceUri, String node, LinkedHashMap<String, Object> values, LinkedHashMap<String, Long> statuses,
                           List<LiveEquipment.Member> members) {
        /** The Write request: per member its {@code .Value}, then its {@code .StatusCode} (UInt32). */
        public List<Write> writes() {
            List<Write> l = new ArrayList<>();
            for (LiveEquipment.Member m : members) {
                l.add(new Write("Sim/" + node + "." + m.name() + ".Value", m.type(), values.get(m.name())));
                l.add(new Write("Sim/" + node + "." + m.name() + ".StatusCode", "UInt32", statuses.get(m.name())));
            }
            return List.copyOf(l);
        }
        /** The live members the sim re-stamps, in definition order. */
        public List<String> liveNodes() {
            List<String> l = new ArrayList<>();
            for (LiveEquipment.Member m : members) l.add(node + "." + m.name());
            return List.copyOf(l);
        }
        boolean sameState(Snapshot o) { return o != null && values.equals(o.values) && statuses.equals(o.statuses); }
    }

    /** An event a step sends after its snapshots are committed. */
    public sealed interface Event permits Arrival, Departure, Correction, IncidentRevision {
        /** The station whose committed snapshot dates the event, or null for the step's time. */
        String station();
    }
    public record Arrival(LiveScenario.Visit visit) implements Event { public String station() { return visit.station(); } }
    public record Departure(LiveScenario.Visit visit) implements Event { public String station() { return visit.station(); } }
    public record Correction(LiveScenario.Correction correction, LiveScenario.Visit visit) implements Event { public String station() { return visit.station(); } }
    public record IncidentRevision(LiveScenario.Incident incident, LiveScenario.Revision revision) implements Event { public String station() { return incident.station(); } }

    /** One step: its scheduled offset from the drive's start, the snapshots to write and the events to send. */
    public record Step(long at, List<Snapshot> snapshots, List<Event> events) {}

    public static List<Step> plan(LiveScenario s, LiveEquipment.LiveStream stream) {
        long last = 0;
        for (LiveScenario.Visit v : s.visits()) last = Math.max(last, v.leave());
        for (LiveScenario.BadStatus b : s.badStatus()) last = Math.max(last, b.to());
        for (LiveScenario.Correction c : s.corrections()) last = Math.max(last, c.at());
        for (LiveScenario.Incident i : s.incidents()) for (LiveScenario.Revision r : i.revisions()) last = Math.max(last, r.at());
        List<LiveEquipment.Equipment> order = new ArrayList<>();
        for (LiveEquipment.Equipment e : stream.equipment().values()) if (e.isStation()) order.add(e);
        for (LiveEquipment.Equipment e : stream.equipment().values()) if (!e.isStation()) order.add(e);

        Map<String, Snapshot> previous = new HashMap<>();
        List<Step> steps = new ArrayList<>();
        for (long t = 0; t <= last; t += s.scanMs()) {
            List<Snapshot> snaps = new ArrayList<>();
            for (LiveEquipment.Equipment e : order) {
                Snapshot now = stateAt(s, e, t);
                if (!now.sameState(previous.get(e.device()))) snaps.add(now);
                previous.put(e.device(), now);
            }
            List<Event> events = new ArrayList<>();
            if (s.emitOccupancy()) {
                for (LiveScenario.Visit v : s.visits()) if (v.leave() == t) events.add(new Departure(v));
                for (LiveScenario.Visit v : s.visits()) if (v.enter() == t) events.add(new Arrival(v));
                for (LiveScenario.Correction c : s.corrections()) if (c.at() == t) events.add(new Correction(c, visitOf(s, c.event())));
            }
            for (LiveScenario.Incident i : s.incidents()) for (LiveScenario.Revision r : i.revisions()) if (r.at() == t) events.add(new IncidentRevision(i, r));
            if (!snaps.isEmpty() || !events.isEmpty()) steps.add(new Step(t, List.copyOf(snaps), List.copyOf(events)));
        }
        return List.copyOf(steps);
    }

    static LiveScenario.Visit visitOf(LiveScenario s, String eventId) {
        for (LiveScenario.Visit v : s.visits()) if (eventId.equals(v.arrivalId()) || eventId.equals(v.departureId())) return v;
        throw new IllegalArgumentException("no visit for event " + eventId);
    }

    /** The state of {@code e} at offset {@code t}. */
    public static Snapshot stateAt(LiveScenario s, LiveEquipment.Equipment e, long t) {
        LiveScenario.Visit current = null, lastStarted = null;
        for (LiveScenario.Visit v : s.visits()) if (v.station().equals(e.station())) {
            if (v.enter() <= t && t < v.leave()) current = v;
            if (v.enter() <= t && (lastStarted == null || v.enter() > lastStarted.enter())) lastStarted = v;
        }
        LiveScenario.ToolActivity ta = s.toolActivity();
        boolean running = e.isStation() ? current != null
            : current != null && current.tools().contains(e.device()) && current.enter() + ta.startAfterEnter() <= t && t <= current.leave() - ta.endBeforeLeave();
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        LinkedHashMap<String, Long> statuses = new LinkedHashMap<>();
        for (LiveEquipment.Member m : e.members()) {
            Object v = switch (m.name() + ":" + m.type()) {
                case "Running:Boolean" -> running;
                case "VisitPresent:Boolean" -> current != null;
                case "UnitId:String" -> lastStarted == null ? "" : lastStarted.unit();
                case "VisitId:String" -> lastStarted == null ? "" : lastStarted.visitId();
                case "Torque:Double" -> {
                    if (e.isStation()) throw new IllegalArgumentException("Torque on station " + e.device());
                    yield running ? Telemetry.torque(s.seed(), e.device(), t / s.scanMs(), ta.min(), ta.max()).doubleValue() : 0.0;
                }
                default -> throw new IllegalArgumentException(e.device() + " member " + m.name() + " " + m.type() + " is not driven");
            };
            values.put(m.name(), v);
            long code = 0;
            for (LiveScenario.BadStatus b : s.badStatus()) if (b.equipment().equals(e.device()) && b.member().equals(m.name()) && b.from() <= t && t < b.to()) code = b.statusCode();
            statuses.put(m.name(), code);
        }
        return new Snapshot(e.device(), e.namespaceUri(), e.node(), values, statuses, e.members());
    }
}
