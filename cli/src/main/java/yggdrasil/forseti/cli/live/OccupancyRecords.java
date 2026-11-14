package yggdrasil.forseti.cli.live;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.regex.Pattern;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;

/** The occupancy events the live MES stand-in sends (spec "Occupancy", Decision 17), built in the exact field set and order of the
 *  step-3 recordings ({@code ScenarioBuilder.occupancyEvent}) so a live capture and a synthetic one carry the same shape.
 *
 *  <p>An arrival and a departure are EPCIS 2.0 {@code ObjectEvent}s with {@code action OBSERVE}; the departure names the arrival it
 *  closes in {@code arrivalEventId}. A correction re-sends the original event unchanged except {@code recordTime}, plus
 *  {@code errorDeclaration {declarationTime, reason, correctiveEventIDs}}; any other change would be a conflict, not a correction.
 *  The occupancy schema 1.0.0 has no visit-id field ({@code additionalProperties: false}), so the scenario's visit id travels as the
 *  stem of the event ids and as {@code visitSeq}.
 *
 *  <p>Why every record goes through {@link #checked} before it is sent: the core reads a record that fails the schema or the binding
 *  profile as {@code schema-invalid}, so a producer slip would become a business disposition in a live capture. The check is the
 *  core's own reader ({@link InputParser#parse}, schema 1.0.0 validation included) followed by the profile's vocabulary (spec line
 *  460: source, identity pattern, bizStep, correction reason), and it refuses instead of sending.
 *
 *  <p>The record is keyed by {@code readPoint} and written to the station partition the routing manifest gives (spec: "the producer
 *  writes it already partitioned by station"), never through the default partitioner. */
public final class OccupancyRecords {
    public static final String ARRIVING = "urn:epcglobal:cbv:bizstep:arriving";
    public static final String DEPARTING = "urn:epcglobal:cbv:bizstep:departing";

    private OccupancyRecords() {}

    public static ObjectNode arrival(String eventId, long eventTime, long recordTime, String unitId, String readPoint, long visitSeq, String source) {
        return event(eventId, eventTime, recordTime, unitId, ARRIVING, readPoint, visitSeq, null, source);
    }

    public static ObjectNode departure(String eventId, long eventTime, long recordTime, String unitId, String readPoint, long visitSeq,
                                       String arrivalEventId, String source) {
        if (arrivalEventId == null) throw new IllegalArgumentException("a departure names the arrival it closes");
        return event(eventId, eventTime, recordTime, unitId, DEPARTING, readPoint, visitSeq, arrivalEventId, source);
    }

    /** The EPCIS error declaration of {@code original}: the same event with a new {@code recordTime} and the declaration. */
    public static ObjectNode errorDeclaration(JsonNode original, long declarationTime, long recordTime, String reason, List<String> correctiveEventIds) {
        if (original.has("errorDeclaration")) throw new IllegalArgumentException("event " + original.path("eventId").asText() + " is already a declaration");
        ObjectNode o = original.deepCopy();
        Json.putSafe(o, "recordTime", recordTime);
        ObjectNode ed = o.putObject("errorDeclaration");
        Json.putSafe(ed, "declarationTime", declarationTime);
        ed.put("reason", reason);
        var ids = ed.putArray("correctiveEventIDs");
        correctiveEventIds.forEach(ids::add);
        return o;
    }

    private static ObjectNode event(String id, long time, long recordTime, String unit, String bizStep, String readPoint, long visitSeq, String arrival, String source) {
        ObjectNode o = Json.object();
        o.put("eventId", id);
        Json.putSafe(o, "eventTime", time);
        Json.putSafe(o, "recordTime", recordTime);
        o.put("unitId", unit);
        o.put("action", "OBSERVE");
        o.put("bizStep", bizStep);
        o.put("readPoint", readPoint);
        Json.putSafe(o, "visitSeq", visitSeq);
        if (arrival != null) o.put("arrivalEventId", arrival);
        o.put("source", source);
        return o;
    }

    /** {@code v} when the core reads it as a valid occupancy event that the profile admits; otherwise it throws, naming the check. */
    public static ObjectNode checked(ObjectNode v, LiveEquipment.OccupancyProfile p) {
        try { InputParser.parse(InputParser.OCCUPANCY, v); }
        catch (InputParser.Rejected r) { throw new IllegalArgumentException("occupancy refused: schema-invalid" + (r.check() == null ? "" : " (" + r.check() + ")") + ": " + v); }
        if (!p.sources().contains(v.get("source").textValue())) throw refused("sources", v);
        if (!Pattern.compile(p.pattern()).matcher(v.get("unitId").textValue()).matches()) throw refused("identity.pattern", v);
        String step = v.get("bizStep").textValue();
        if (!step.equals(p.enter()) && !step.equals(p.leave())) throw refused("bizStep", v);
        JsonNode ed = v.get("errorDeclaration");
        if (ed != null && !p.reasons().contains(ed.path("reason").asText())) throw refused("correction.reasons", v);
        return v;
    }

    private static IllegalArgumentException refused(String check, JsonNode v) {
        return new IllegalArgumentException("occupancy refused by the binding profile (" + check + "): " + v);
    }

    /** The record key: the {@code readPoint}. */
    public static String key(JsonNode v) { return v.get("readPoint").textValue(); }

    /** The station partition of the event, from the routing manifest. */
    public static int partition(RoutingManifest routing, JsonNode v) { return routing.partitionOf(key(v)); }
}
