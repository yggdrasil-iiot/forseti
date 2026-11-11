package yggdrasil.forseti.core.stage2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.Ids;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.rules.RuleSet;

/** Rule sheet T1, the stateless door of {@code quality.incidents} (spec 298-315, 449, 459): schema, {@code kinds[]}, the revision-1 half of
 *  the line-308 {@code revisionEventTime} contract, and the scope ({@code equipmentRef} in {@code incidentScope[]}, {@code stationRef} a
 *  rule station and the station of {@code equipmentRef}). A rejected incident's disposition goes to partition 6 through the stage-2 reject
 *  collector; a routed one goes to {@code stationRef}. The order-dependent checks (later revisions' {@code revisionEventTime}, identity,
 *  {@code station-change}, {@code superseded}) need the incident's stored revisions and are {@link Stage2#decide}'s. Also the incident
 *  snapshot and keys of the stage-2 bodies. */
public final class Incidents {
    public static final String INPUT_KIND = "incident";

    public sealed interface Outcome permits Routed, Rejected {}
    public record Routed(String station, Inputs.InputRecord record) implements Outcome {}
    /** The disposition body of a rejected incident; the adapter adds the key's generation and origin ({@code part event}). */
    public record Rejected(Inputs.InputRecord record, Disposition.Body body) implements Outcome {}

    private Incidents() {}

    public static Outcome door(RuleSet rules, InputParser.Read read) { return door(rules, read.record(), read.check()); }

    /** {@code check}: the parser's failing field for a schema-invalid record (null when none is named). */
    public static Outcome door(RuleSet rules, Inputs.InputRecord r, String check) {
        if (!(r.parsed() instanceof Inputs.IncidentEvent i)) return reject(r, NullNode.getInstance(), Disposition.Reason.SCHEMA_INVALID, check);
        ObjectNode id = Ids.incidentId(i);
        if (!rules.incident().kinds().contains(i.kind())) return reject(r, id, Disposition.Reason.SCHEMA_INVALID, Disposition.ProfileCheck.KINDS.wire());
        if (i.revision() == 1 && i.revisionEventTime() != i.detectedAt())
            return reject(r, id, Disposition.Reason.SCHEMA_INVALID, Disposition.ProfileCheck.REVISION_EVENT_TIME.wire());
        if (!rules.impact().incidentScope().contains(i.equipmentRef()) || !rules.attribution().stations().contains(i.stationRef())
            || !i.stationRef().equals(stationOf(rules, i.equipmentRef()).orElse(null)))
            return reject(r, id, Disposition.Reason.OUTSIDE_INCIDENT_SCOPE, null);
        return new Routed(i.stationRef(), r);
    }

    static Optional<String> stationOf(RuleSet rules, String path) {
        try { return rules.hierarchy().stationOf(rules.attribution().stations(), path); } catch (IllegalArgumentException e) { return Optional.empty(); }
    }

    static Rejected reject(Inputs.InputRecord r, JsonNode identity, Disposition.Reason reason, String check) {
        ObjectNode refs = Json.object();
        if (check != null) refs.put("check", check);
        if ("raw-bytes".equals(r.payloadDigest().rule())) refs.put("digestRule", "raw-bytes");
        return new Rejected(r, new Disposition.Body(INPUT_KIND, identity, r.payloadDigest().sha256(), reason.kind(), reason, refs));
    }

    /** The incident snapshot of the assessment and set bodies: the revision's twelve fields in schema order. */
    public static ObjectNode snapshot(Inputs.IncidentEvent i) {
        ObjectNode n = Json.object().put("incidentId", i.incidentId());
        Json.putSafe(n, "revision", i.revision());
        n.put("stationRef", i.stationRef()).put("equipmentRef", i.equipmentRef()).put("kind", i.kind());
        Json.putSafe(n, "detectedAt", i.detectedAt());
        Json.putSafe(n, "revisionEventTime", i.revisionEventTime());
        Json.putSafe(n, "suspectFrom", i.suspectFrom());
        if (i.suspectTo() == null) n.putNull("suspectTo"); else Json.putSafe(n, "suspectTo", i.suspectTo());
        return n.put("status", i.status()).put("evidenceRef", i.evidenceRef()).put("source", i.source());
    }

    /** The affected set key {@code (source, incidentId)}. */
    public static ObjectNode setKey(String source, String incidentId) { return Json.object().put("source", source).put("incidentId", incidentId); }
    /** The assessment key {@code (source, incidentId, unitId)}. */
    public static ObjectNode assessmentKey(String source, String incidentId, String unitId) { return setKey(source, incidentId).put("unitId", unitId); }

    static Inputs.IncidentEvent parse(JsonNode value) { return (Inputs.IncidentEvent) InputParser.parse(InputParser.INCIDENTS, value); }
}
