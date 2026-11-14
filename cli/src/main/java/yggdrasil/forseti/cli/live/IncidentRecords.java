package yggdrasil.forseti.cli.live;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import yggdrasil.forseti.cli.recording.Murmur2;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;

/** The incident revisions the live quality stand-in sends (spec "Incidents"), in the field set and order of the step-3 recordings.
 *
 *  <p>Why {@link #checked} runs before a send: as for occupancy, a record the core reads as {@code schema-invalid} would turn a
 *  producer slip into a disposition of the capture. The check is the core reader ({@link InputParser#parse} with schema 1.0.0), the
 *  profile's {@code kinds[]}, the revision-1 rule {@code revisionEventTime == detectedAt} (spec line 308) and Decision 15's rule that
 *  {@code equipmentRef} is the station or a descendant of it.
 *
 *  <p>The record is keyed by {@code equipmentRef} and written to the partition Kafka's default partitioner gives that key on six
 *  partitions ({@link Murmur2}, the placement step 3 used), computed here so the plan is visible and testable without a broker. */
public final class IncidentRecords {
    private IncidentRecords() {}

    public static ObjectNode revision(String incidentId, long revision, String stationRef, String equipmentRef, String kind, long detectedAt,
                                      long revisionEventTime, long suspectFrom, Long suspectTo, String status, String evidenceRef, String source) {
        ObjectNode v = Json.object();
        v.put("incidentId", incidentId);
        Json.putSafe(v, "revision", revision);
        v.put("stationRef", stationRef);
        v.put("equipmentRef", equipmentRef);
        v.put("kind", kind);
        Json.putSafe(v, "detectedAt", detectedAt);
        Json.putSafe(v, "revisionEventTime", revisionEventTime);
        Json.putSafe(v, "suspectFrom", suspectFrom);
        if (suspectTo == null) v.putNull("suspectTo"); else Json.putSafe(v, "suspectTo", suspectTo);
        v.put("status", status);
        v.put("evidenceRef", evidenceRef);
        v.put("source", source);
        return v;
    }

    /** {@code v} when the core reads it as a valid incident the profile admits; otherwise it throws, naming the check. */
    public static ObjectNode checked(ObjectNode v, LiveEquipment.IncidentProfile p) {
        try { InputParser.parse(InputParser.INCIDENTS, v); }
        catch (InputParser.Rejected r) { throw new IllegalArgumentException("incident refused: schema-invalid" + (r.check() == null ? "" : " (" + r.check() + ")") + ": " + v); }
        if (!p.kinds().contains(v.get("kind").textValue())) throw new IllegalArgumentException("incident refused by the binding profile (kinds): " + v);
        if (v.get("revision").longValue() == 1 && v.get("revisionEventTime").longValue() != v.get("detectedAt").longValue())
            throw new IllegalArgumentException("incident refused (revisionEventTime): revision 1 must carry revisionEventTime == detectedAt: " + v);
        String station = v.get("stationRef").textValue(), equipment = v.get("equipmentRef").textValue();
        if (!equipment.equals(station) && !equipment.startsWith(station + ":"))
            throw new IllegalArgumentException("incident refused: equipmentRef " + equipment + " is not " + station + " or below it: " + v);
        return v;
    }

    public static String key(JsonNode v) { return v.get("equipmentRef").textValue(); }

    public static int partition(JsonNode v, int partitions) { return Murmur2.partition(key(v), partitions); }
}
