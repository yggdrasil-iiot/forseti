package yggdrasil.forseti.cli.live;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Pattern;
import yggdrasil.forseti.core.Json;

/** A live scenario: what the step-5 driver does to the Bifrost sim and which occupancy and incident events it sends, on a wall-clock
 *  schedule relative to the drive's start. The files live in {@code fixtures/live-scenarios/}, next to the synthetic
 *  {@code fixtures/scenarios/}.
 *
 *  <pre>
 *  {
 *    "liveScenarioVersion": 1,
 *    "name": "baseline",
 *    "scanMs": 250,                        // the sim's bifrost.sim.scanMs (S); every time below is a multiple of it
 *    "muninnPeriodMs": 500,                // Muninn's --session period (M); M + S + Preflight.JITTER_MARGIN_MS <= expectedPeriodMs
 *    "seed": 20261109,                     // Telemetry.torque seed
 *    "occupancy": {"emit": true, "source": "mes-line2"},
 *    "toolActivity": {"startAfterEnter": 5000, "endBeforeLeave": 1000, "torque": {"min": 10.0, "max": 45.0}},
 *    "visits": [
 *      {"visitId": "V1", "unit": "UNIT0000000000001", "station": "line2:station1", "enter": 0, "leave": 60000,
 *       "tools": ["nutrunner1"], "visitSeq": 1}
 *    ],
 *    "badStatus": [{"equipment": "line2:station1:nutrunner1", "member": "Torque", "statusCode": 2147483648, "from": 10000, "to": 20000}],
 *    "corrections": [{"event": "V1-arrive", "at": 70000, "reason": "urn:epcglobal:cbv:er:did_not_occur", "correctiveEventIDs": []}],
 *    "incidents": [
 *      {"incidentId": "inc-1", "equipment": "line2:station1:nutrunner1", "kind": "tool-check-failed",
 *       "evidenceRef": "urn:line2:qms:inc-1", "source": "qms-line2",
 *       "revisions": [{"revision": 1, "at": 61000, "status": "closed", "suspectFrom": {"visit": "V1", "at": "enter"}, "suspectTo": "detected"}]}
 *    ]
 *  }
 *  </pre>
 *
 *  <p>Equipment is named below the site ({@code line2:station1}), so one scenario drives both placements: the stream's devices are the
 *  site's id plus that path. A visit's {@code tools} are the names of tool devices under its station; a tool not listed still reports
 *  the visit's {@code UnitId} and {@code VisitId} with {@code VisitPresent=true} and {@code Running=false} (spec line 641). Visits at
 *  one station may not overlap (the sim shows one occupant per station). A visit's occupancy events are {@code <visitId>-arrive} and
 *  {@code <visitId>-depart}; {@code corrections} name one of them and re-send it as an EPCIS error declaration at {@code at}
 *  (Decision 17). {@code badStatus} writes a Bad StatusCode for one member over {@code [from, to)} (injection 3). An incident
 *  revision's times are the sim source timestamps the drive observes: {@code detectedAt} at revision 1's {@code at},
 *  {@code revisionEventTime} at each revision's own {@code at}; {@code suspectFrom} and {@code suspectTo} name a visit's enter or leave,
 *  {@code "detected"} (the detection time) or, for {@code suspectTo}, null (open). Unknown fields are refused. */
public record LiveScenario(String name, long scanMs, long muninnPeriodMs, long seed, boolean emitOccupancy, String occupancySource, ToolActivity toolActivity,
                           List<Visit> visits, List<BadStatus> badStatus, List<Correction> corrections, List<Incident> incidents) {

    public record ToolActivity(long startAfterEnter, long endBeforeLeave, BigDecimal min, BigDecimal max) {}
    /** One visit, with its station and tools resolved to the stream's device paths. */
    public record Visit(String visitId, String unit, String station, long enter, long leave, List<String> tools, long visitSeq) {
        public String arrivalId() { return visitId + "-arrive"; }
        public String departureId() { return visitId + "-depart"; }
    }
    public record BadStatus(String equipment, String member, long statusCode, long from, long to) {}
    public record Correction(String event, long at, String reason, List<String> correctiveEventIds) {}
    /** A time that a drive observes: the enter or leave of a visit, or (visit null) the incident's detection time. */
    public record Anchor(String visit, String at) {
        public static final Anchor DETECTED = new Anchor(null, "detected");
        public boolean detected() { return visit == null; }
    }
    public record Revision(long revision, long at, String status, Anchor suspectFrom, Anchor suspectTo) {}
    public record Incident(String incidentId, String equipment, String station, String kind, String evidenceRef, String source, List<Revision> revisions) {}

    private static final Set<String> TOP = Set.of("liveScenarioVersion", "name", "scanMs", "muninnPeriodMs", "seed", "occupancy", "toolActivity", "visits", "badStatus", "corrections", "incidents");

    /** Reads and resolves a scenario against the stream it drives; refuses anything the drive could not carry out exactly. */
    public static LiveScenario read(JsonNode s, LiveEquipment.LiveStream stream) {
        fields(s, TOP, "scenario");
        if (Json.safeLong(s.get("liveScenarioVersion"), "liveScenarioVersion", 1, 1) != 1) throw refuse("liveScenarioVersion");
        String name = Json.text(s, "name");
        long scanMs = Json.safeLong(s.get("scanMs"), "scanMs", 1, 86_400_000);
        long muninnPeriodMs = Json.safeLong(s.get("muninnPeriodMs"), "muninnPeriodMs", 1, 86_400_000);
        long seed = Json.safeLong(s.get("seed"), "seed", Json.NON_NEGATIVE);
        JsonNode occ = s.path("occupancy");
        fields(occ, Set.of("emit", "source"), "occupancy");
        if (!occ.path("emit").isBoolean()) throw refuse("occupancy.emit");
        String occSource = Json.text(occ, "source");
        if (!stream.occupancy().sources().contains(occSource)) throw refuse("occupancy.source " + occSource + " is not in the binding's sources " + stream.occupancy().sources());
        JsonNode ta = s.path("toolActivity");
        fields(ta, Set.of("startAfterEnter", "endBeforeLeave", "torque"), "toolActivity");
        fields(ta.path("torque"), Set.of("min", "max"), "toolActivity.torque");
        ToolActivity tool = new ToolActivity(grid(ta, "startAfterEnter", scanMs), grid(ta, "endBeforeLeave", scanMs),
            decimal(ta.path("torque").get("min"), "torque.min"), decimal(ta.path("torque").get("max"), "torque.max"));
        if (tool.min().compareTo(tool.max()) > 0) throw refuse("torque.min > torque.max");

        String site = site(stream);
        Pattern unitPattern = Pattern.compile(stream.occupancy().pattern());
        List<Visit> visits = new ArrayList<>();
        Map<String, Visit> byId = new HashMap<>();
        for (JsonNode v : array(s, "visits")) {
            fields(v, Set.of("visitId", "unit", "station", "enter", "leave", "tools", "visitSeq"), "visit");
            String id = Json.text(v, "visitId"), unit = Json.text(v, "unit");
            if (!unitPattern.matcher(unit).matches()) throw refuse("visit " + id + " unit " + unit + " does not match the identity pattern " + stream.occupancy().pattern());
            String station = device(stream, site, Json.text(v, "station"));
            if (!stream.device(station).isStation()) throw refuse("visit " + id + " station " + station + " is not a station");
            long enter = grid(v, "enter", scanMs), leave = grid(v, "leave", scanMs);
            if (leave <= enter) throw refuse("visit " + id + " leaves before it enters");
            List<String> tools = new ArrayList<>();
            for (JsonNode t : v.path("tools")) {
                String d = station + ":" + t.asText();
                if (!stream.equipment().containsKey(d) || stream.device(d).isStation() || !stream.device(d).station().equals(station))
                    throw refuse("visit " + id + " tool " + t.asText() + " is not a tool of " + station);
                if (tools.contains(d)) throw refuse("visit " + id + " names tool " + t.asText() + " twice");
                tools.add(d);
            }
            Visit vv = new Visit(id, unit, station, enter, leave, List.copyOf(tools), Json.safeLong(v.get("visitSeq"), "visitSeq", Json.NON_NEGATIVE));
            if (byId.put(id, vv) != null) throw refuse("two visits " + id);
            for (Visit o : visits) if (o.station().equals(station) && o.enter() < leave && enter < o.leave()) throw refuse("visits " + o.visitId() + " and " + id + " overlap at " + station);
            visits.add(vv);
        }
        if (visits.isEmpty()) throw refuse("no visits");

        List<BadStatus> bad = new ArrayList<>();
        for (JsonNode b : array(s, "badStatus")) {
            fields(b, Set.of("equipment", "member", "statusCode", "from", "to"), "badStatus");
            String d = device(stream, site, Json.text(b, "equipment"));
            String member = Json.text(b, "member");
            stream.device(d).member(member);
            long code = Json.safeLong(b.get("statusCode"), "statusCode", 0, 4294967295L);
            if ((code & 0xC0000000L) != 0x80000000L) throw refuse("badStatus statusCode " + code + " is not a Bad status code");
            long from = grid(b, "from", scanMs), to = grid(b, "to", scanMs);
            if (to <= from) throw refuse("badStatus to <= from");
            for (BadStatus o : bad) if (o.equipment().equals(d) && o.member().equals(member) && o.from() < to && from < o.to()) throw refuse("overlapping badStatus on " + d + " " + member);
            bad.add(new BadStatus(d, member, code, from, to));
        }

        List<Correction> corrections = new ArrayList<>();
        for (JsonNode c : array(s, "corrections")) {
            fields(c, Set.of("event", "at", "reason", "correctiveEventIDs"), "correction");
            String event = Json.text(c, "event");
            long at = grid(c, "at", scanMs);
            Long original = null;
            for (Visit v : visits) {
                if (event.equals(v.arrivalId())) original = v.enter();
                if (event.equals(v.departureId())) original = v.leave();
            }
            if (original == null) throw refuse("correction names no occupancy event " + event);
            if (at <= original) throw refuse("correction of " + event + " at " + at + " is not after the event");
            if (!occ.path("emit").booleanValue()) throw refuse("correction of " + event + " in a scenario that emits no occupancy");
            String reason = Json.text(c, "reason");
            if (!stream.occupancy().reasons().contains(reason)) throw refuse("correction reason " + reason + " is not in the binding's reasons");
            List<String> corrective = new ArrayList<>();
            c.path("correctiveEventIDs").forEach(x -> corrective.add(x.asText()));
            for (Correction o : corrections) if (o.event().equals(event)) throw refuse("two corrections of " + event);
            corrections.add(new Correction(event, at, reason, List.copyOf(corrective)));
        }

        List<Incident> incidents = new ArrayList<>();
        Set<String> incidentIds = new HashSet<>();
        for (JsonNode i : array(s, "incidents")) {
            fields(i, Set.of("incidentId", "equipment", "kind", "evidenceRef", "source", "revisions"), "incident");
            String id = Json.text(i, "incidentId");
            if (!incidentIds.add(id)) throw refuse("two incidents " + id);
            String eq = device(stream, site, Json.text(i, "equipment"));
            String kind = Json.text(i, "kind");
            if (!stream.incidents().kinds().contains(kind)) throw refuse("incident " + id + " kind " + kind + " is not in the binding's kinds");
            List<Revision> revs = new ArrayList<>();
            for (JsonNode r : array(i, "revisions")) {
                fields(r, Set.of("revision", "at", "status", "suspectFrom", "suspectTo"), "revision");
                long n = Json.safeLong(r.get("revision"), "revision", 1, Json.MAX_SAFE);
                if (n != revs.size() + 1) throw refuse("incident " + id + " revisions must be 1, 2, ... in order");
                long at = grid(r, "at", scanMs);
                if (!revs.isEmpty() && at <= revs.get(revs.size() - 1).at()) throw refuse("incident " + id + " revision " + n + " is not after the previous one");
                String status = Json.text(r, "status");
                if (!List.of("open", "closed", "withdrawn").contains(status)) throw refuse("incident " + id + " status " + status);
                Anchor from = anchor(r.get("suspectFrom"), byId, at, false, id), to = anchor(r.get("suspectTo"), byId, at, true, id);
                if (to == null && status.equals("closed")) throw refuse("incident " + id + " revision " + n + " is closed with an open suspectTo");
                revs.add(new Revision(n, at, status, from, to));
            }
            if (revs.isEmpty()) throw refuse("incident " + id + " has no revisions");
            incidents.add(new Incident(id, eq, stream.device(eq).station(), kind, Json.text(i, "evidenceRef"), Json.text(i, "source"), List.copyOf(revs)));
        }
        return new LiveScenario(name, scanMs, muninnPeriodMs, seed, occ.path("emit").booleanValue(), occSource, tool, List.copyOf(visits), List.copyOf(bad),
            List.copyOf(corrections), List.copyOf(incidents));
    }

    /** The visit of {@code id}. */
    public Visit visit(String id) {
        for (Visit v : visits) if (v.visitId().equals(id)) return v;
        throw new IllegalArgumentException("no visit " + id);
    }

    private static Anchor anchor(JsonNode n, Map<String, Visit> visits, long at, boolean nullable, String incident) {
        if (n == null) throw refuse("incident " + incident + " revision without a suspect bound");
        if (n.isNull()) { if (nullable) return null; throw refuse("incident " + incident + " suspectFrom may not be null"); }
        if (n.isTextual() && n.textValue().equals("detected")) return Anchor.DETECTED;
        fields(n, Set.of("visit", "at"), "anchor");
        Visit v = visits.get(Json.text(n, "visit"));
        if (v == null) throw refuse("incident " + incident + " names no visit " + n.path("visit").asText());
        String which = Json.text(n, "at");
        long t = switch (which) { case "enter" -> v.enter(); case "leave" -> v.leave(); default -> throw refuse("anchor at " + which); };
        if (t > at) throw refuse("incident " + incident + " names " + v.visitId() + " " + which + " after the revision is sent");
        return new Anchor(v.visitId(), which);
    }

    /** The site every device of the stream sits under (the first path segment). */
    static String site(LiveEquipment.LiveStream stream) {
        Set<String> sites = new TreeSet<>();
        for (String d : stream.equipment().keySet()) sites.add(d.substring(0, d.indexOf(':')));
        if (sites.size() != 1) throw refuse("stream " + stream.stream() + " spans sites " + sites);
        return sites.iterator().next();
    }

    private static String device(LiveEquipment.LiveStream stream, String site, String below) {
        String d = site + ":" + below;
        if (!stream.equipment().containsKey(d)) throw refuse("stream " + stream.stream() + " has no device " + d);
        return d;
    }

    private static long grid(JsonNode o, String field, long scanMs) {
        long v = Json.safeLong(o.get(field), field, Json.NON_NEGATIVE);
        if (v % scanMs != 0) throw refuse(field + " " + v + " is not a multiple of scanMs " + scanMs);
        return v;
    }

    private static BigDecimal decimal(JsonNode n, String field) {
        if (n == null || !n.isNumber()) throw refuse(field);
        return new BigDecimal(n.asText());
    }

    private static Iterable<JsonNode> array(JsonNode o, String field) {
        JsonNode a = o.get(field);
        if (a == null) return List.of();
        if (!a.isArray()) throw refuse(field + " is not an array");
        return a;
    }

    private static void fields(JsonNode o, Set<String> allowed, String what) {
        if (o == null || !o.isObject()) throw refuse(what + " is not an object");
        o.fieldNames().forEachRemaining(f -> { if (!allowed.contains(f)) throw refuse(what + " field " + f); });
    }

    private static IllegalArgumentException refuse(String what) { return new IllegalArgumentException("live scenario refused: " + what); }
}
