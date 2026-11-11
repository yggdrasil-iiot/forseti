package yggdrasil.forseti.core.stage2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Revisions;
import yggdrasil.forseti.core.stage1.DeadlineIndex;

/** The state of one station's stage-2 writer (rule sheet T1-T10). The caller owns it: {@link Stage2#decide} takes it, updates it and
 *  returns it; adapters keep it in their store through {@link #toJson} / {@link #fromJson} (versioned). Every map is sorted by its
 *  canonical key text, so equal states serialize to equal bytes.
 *  <ul>
 *  <li>{@code bases}: the current work record row of every visit of the station's partition, by visitRef (T3), until its retention
 *      timer ({@code leave} or, open, {@code enter}, plus {@code workRecordsMs}) drops it;</li>
 *  <li>{@code activity}: per device, the current sample rows of its activity member ({@code sourceTs, state, reason, active}), by sampleId;
 *      {@code lastObservation}: per device, the largest {@code sourceTs} of any of its sample rows (T8's open range);</li>
 *  <li>{@code incidents} by {@code (source, incidentId)}: the first accepted {@code stationRef} and every stored variant of every valid
 *      revision; a revision with two variants is in conflict and unusable;</li>
 *  <li>{@code units}: the live assessment rows per incident (status, reason, digest), for deletes and the set's {@code units[]};</li>
 *  <li>{@code evidenceRanges}: the verified telemetry range per device the adapter set at initialization ({@link #evidenceRanges});</li>
 *  <li>{@code deadlines}, the three revision chains (assessments, sets, dispositions), {@code lastW2}, {@code progressCount}.</li></ul> */
public final class Stage2State {
    public static final int VERSION = 1;

    record Basis(JsonNode visitRef, ObjectNode body, String bodyDigest, long revision) {}
    record Variant(Inputs.Origin origin, String payloadDigest, String digestRule, JsonNode value) {
        Inputs.IncidentEvent event() { return Incidents.parse(value); }
    }
    static final class Incident {
        final String source, incidentId;
        String stationRef;
        final TreeMap<Long, List<Variant>> revisions = new TreeMap<>();
        Incident(String source, String incidentId) { this.source = source; this.incidentId = incidentId; }
        /** Valid, non-conflicting revisions (delivered, whatever their disposition). */
        List<Inputs.IncidentEvent> delivered() {
            List<Inputs.IncidentEvent> l = new ArrayList<>();
            revisions.forEach((n, v) -> { if (v.size() == 1) l.add(v.get(0).event()); });
            return l;
        }
        /** The highest valid, non-conflicting revision; null when none. */
        Inputs.IncidentEvent current() {
            for (Map.Entry<Long, List<Variant>> e : revisions.descendingMap().entrySet()) if (e.getValue().size() == 1) return e.getValue().get(0).event();
            return null;
        }
    }

    final long generation;
    final String station;
    final int partition;
    final TreeMap<String, Basis> bases = new TreeMap<>();
    final TreeMap<String, TreeMap<String, SetCoverage.ActivityRow>> activity = new TreeMap<>();
    final TreeMap<String, Long> lastObservation = new TreeMap<>();
    final TreeMap<String, Incident> incidents = new TreeMap<>();
    final TreeMap<String, TreeMap<String, AffectedSet.Unit>> units = new TreeMap<>();
    final TreeMap<String, SetCoverage.EvidenceRange> evidenceRanges = new TreeMap<>();
    DeadlineIndex deadlines = new DeadlineIndex();
    Revisions assessments = new Revisions(), sets = new Revisions(), dispositions = new Revisions();
    Long lastW2;
    long progressCount;

    Stage2State(long generation, String station, int partition) {
        if (generation < 1) throw new IllegalArgumentException("generation >= 1");
        this.generation = generation; this.station = Objects.requireNonNull(station); this.partition = partition;
    }

    /** The verified evidence-retention ranges per device (spec 402; in step 3 the recording manifest's {@code evidenceRange}). */
    public void evidenceRanges(Map<String, SetCoverage.EvidenceRange> ranges) { evidenceRanges.clear(); evidenceRanges.putAll(ranges); }

    public long generation() { return generation; }
    public String station() { return station; }
    public int partition() { return partition; }
    public DeadlineIndex deadlines() { return deadlines; }
    /** W2 of the last cut close (or fired timer); null before the first. */
    public Long lastW2() { return lastW2; }
    public long progressCount() { return progressCount; }
    public int bases() { return bases.size(); }

    // ===================================================================================================== serialization

    public ObjectNode toJson() {
        ObjectNode n = Json.object().put("stateVersion", VERSION);
        Json.putSafe(n, "generation", generation);
        n.put("station", station).put("partition", partition);
        ObjectNode b = n.putObject("bases");
        bases.forEach((k, x) -> {
            ObjectNode e = b.putObject(k);
            e.set("visitRef", x.visitRef());
            e.set("body", x.body());
            e.put("bodyDigest", x.bodyDigest());
            Json.putSafe(e, "revision", x.revision());
        });
        ObjectNode a = n.putObject("activity");
        activity.forEach((d, rows) -> {
            ObjectNode dn = a.putObject(d);
            rows.forEach((k, r) -> {
                ObjectNode e = Json.putSafe(dn.putObject(k), "sourceTs", r.sourceTs());
                e.put("state", r.state()).put("reason", r.reason());
                e.set("active", r.active());
            });
        });
        ObjectNode lo = n.putObject("lastObservation");
        lastObservation.forEach((d, t) -> Json.putSafe(lo, d, t));
        ObjectNode in = n.putObject("incidents");
        incidents.forEach((k, x) -> {
            ObjectNode e = in.putObject(k).put("source", x.source).put("incidentId", x.incidentId).put("stationRef", x.stationRef);
            ObjectNode revs = e.putObject("revisions");
            x.revisions.forEach((r, vs) -> {
                ArrayNode l = revs.putArray(Long.toString(r));
                for (Variant v : vs) {
                    ObjectNode o = l.addObject();
                    o.set("origin", origin(v.origin()));
                    o.put("payloadDigest", v.payloadDigest()).put("digestRule", v.digestRule());
                    o.set("value", v.value());
                }
            });
        });
        ObjectNode un = n.putObject("units");
        units.forEach((k, m) -> {
            ObjectNode e = un.putObject(k);
            m.forEach((u, x) -> e.putObject(u).put("status", x.status()).put("reason", x.reason()).put("bodyDigest", x.bodyDigest()));
        });
        ObjectNode er = n.putObject("evidenceRanges");
        evidenceRanges.forEach((d, r) -> { ObjectNode e = Json.putSafe(er.putObject(d), "from", r.from()); Json.putSafe(e, "to", r.to()); });
        n.set("deadlines", deadlines.toJson());
        ObjectNode rev = n.putObject("revisions");
        rev.set("assessments", assessments.toJson());
        rev.set("sets", sets.toJson());
        rev.set("dispositions", dispositions.toJson());
        if (lastW2 == null) n.putNull("lastW2"); else Json.putSafe(n, "lastW2", lastW2);
        Json.putSafe(n, "progressCount", progressCount);
        return n;
    }

    public static Stage2State fromJson(JsonNode n) {
        if (n.path("stateVersion").intValue() != VERSION) throw new IllegalArgumentException("stage-2 state version " + n.path("stateVersion") + " is not " + VERSION);
        Stage2State s = new Stage2State(Json.safeLong(n.get("generation"), "generation", Json.NON_NEGATIVE), Json.text(n, "station"),
            (int) Json.safeLong(n.get("partition"), "partition", 0, Integer.MAX_VALUE));
        n.get("bases").fields().forEachRemaining(e -> {
            JsonNode x = e.getValue();
            s.bases.put(e.getKey(), new Basis(x.get("visitRef"), (ObjectNode) x.get("body"), Json.text(x, "bodyDigest"), Json.safeLong(x.get("revision"), "revision", Json.NON_NEGATIVE)));
        });
        n.get("activity").fields().forEachRemaining(d -> {
            TreeMap<String, SetCoverage.ActivityRow> rows = new TreeMap<>();
            d.getValue().fields().forEachRemaining(e -> {
                JsonNode x = e.getValue();
                rows.put(e.getKey(), new SetCoverage.ActivityRow(Json.safeLong(x.get("sourceTs"), "sourceTs", Json.NON_NEGATIVE), Json.text(x, "state"),
                    x.path("reason").isTextual() ? x.get("reason").textValue() : null, x.get("active")));
            });
            s.activity.put(d.getKey(), rows);
        });
        n.get("lastObservation").fields().forEachRemaining(e -> s.lastObservation.put(e.getKey(), Json.safeLong(e.getValue(), "lastObservation", Json.NON_NEGATIVE)));
        n.get("incidents").fields().forEachRemaining(e -> {
            JsonNode x = e.getValue();
            Incident inc = new Incident(Json.text(x, "source"), Json.text(x, "incidentId"));
            inc.stationRef = x.path("stationRef").isTextual() ? x.get("stationRef").textValue() : null;
            x.get("revisions").fields().forEachRemaining(r -> {
                List<Variant> vs = new ArrayList<>();
                for (JsonNode v : r.getValue()) vs.add(new Variant(origin(v.get("origin")), Json.text(v, "payloadDigest"), Json.text(v, "digestRule"), v.get("value")));
                inc.revisions.put(Long.parseLong(r.getKey()), vs);
            });
            s.incidents.put(e.getKey(), inc);
        });
        n.get("units").fields().forEachRemaining(e -> {
            TreeMap<String, AffectedSet.Unit> m = new TreeMap<>();
            e.getValue().fields().forEachRemaining(u -> m.put(u.getKey(), new AffectedSet.Unit(Json.text(u.getValue(), "status"), Json.text(u.getValue(), "reason"), Json.text(u.getValue(), "bodyDigest"))));
            s.units.put(e.getKey(), m);
        });
        n.get("evidenceRanges").fields().forEachRemaining(e -> s.evidenceRanges.put(e.getKey(),
            new SetCoverage.EvidenceRange(Json.safeLong(e.getValue().get("from"), "from", Json.ANY), Json.safeLong(e.getValue().get("to"), "to", Json.ANY))));
        s.deadlines = DeadlineIndex.fromJson(n.get("deadlines"));
        s.assessments = Revisions.fromJson(n.get("revisions").get("assessments"));
        s.sets = Revisions.fromJson(n.get("revisions").get("sets"));
        s.dispositions = Revisions.fromJson(n.get("revisions").get("dispositions"));
        JsonNode w = n.get("lastW2");
        s.lastW2 = w == null || w.isNull() ? null : Json.safeLong(w, "lastW2", Json.ANY);
        s.progressCount = Json.safeLong(n.get("progressCount"), "progressCount", Json.NON_NEGATIVE);
        return s;
    }

    static ObjectNode origin(Inputs.Origin o) {
        ObjectNode n = Json.object().put("topic", o.topic()).put("partition", o.partition());
        return Json.putSafe(n, "offset", o.offset());
    }
    static Inputs.Origin origin(JsonNode n) {
        return new Inputs.Origin(Json.text(n, "topic"), (int) Json.safeLong(n.get("partition"), "partition", 0, Integer.MAX_VALUE), Json.safeLong(n.get("offset"), "offset", Json.NON_NEGATIVE));
    }
}
