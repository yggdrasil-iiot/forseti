package yggdrasil.forseti.core.input;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

/** The four input kinds of spec "Input events", as parsed views. Every input carries its {@link Origin}. */
public final class Inputs {
    private Inputs() {}
    public record Origin(String topic, int partition, long offset) {}
    /** One Kafka record as delivered: raw value, origin, parsed view (null when schema-invalid), payload digest. */
    public record InputRecord(Origin origin, String key, byte[] rawValue, JsonNode value, Object parsed, Ids.Digest payloadDigest) {}
    /** A process message member; every integer a Long read through Json.safeLong. A numeric {@code value} is a DoubleNode (binary64). */
    public record Member(String metric, JsonNode value, Long uaStatusCode, Long quality, Long sourceTs, Long serverTs) {}
    /** A member that did not parse (its disposition is {@code member:<metric>} rejected schema-invalid); {@code check} names the field. */
    public record MemberRejection(String metric, String check) {}
    public record ProcessMessage(String edge, String device, long birthPublishTs, long bdSeq, long seq, long publishTs, Long bridgeReceivedAt,
                                 String definitionSha256, List<Member> members, List<MemberRejection> rejectedMembers) {}
    /** A session event. {@code birthPublishTs} and {@code atSeq} are null only for the birthless {@code session-ambiguous} variant
     *  (spec Session events, plan D4): the NDEATH of a connection whose NBIRTH the bridge never observed has no birth to name, and its
     *  {@code atSeq} is the device's last observed {@code seq} or null; the bridge never invents either. Every other kind carries both. */
    public record SessionEvent(String edge, String device, Long birthPublishTs, long bdSeq, String kind, Long atSeq, JsonNode payload) {
        /** The birthless variant: its identity adds the death's {@code rawPublishTs} and {@code payloadDigest} ({@link Ids#sessionEventId}). */
        public boolean birthless() { return birthPublishTs == null; }
    }
    public record OccupancyEvent(String eventId, long eventTime, long recordTime, String unitId, String bizStep, String readPoint, Long visitSeq,
                                 String arrivalEventId, String source, JsonNode errorDeclaration) {   // visitSeq is Long: the schema admits any safe integer, 2147483648 included
        public boolean arriving() { return bizStep.equals("urn:epcglobal:cbv:bizstep:arriving"); }
    }
    public record IncidentEvent(String incidentId, long revision, String stationRef, String equipmentRef, String kind, long detectedAt, long revisionEventTime,
                                long suspectFrom, Long suspectTo, String status, String evidenceRef, String source) {}
}
