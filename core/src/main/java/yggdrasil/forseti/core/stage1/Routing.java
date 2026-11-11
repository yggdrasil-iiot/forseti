package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;
import java.util.Set;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.Ids;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.rules.RuleSet;

/** Rule sheet S1: routing and rejection of a stage-1 input before {@link Stage1#decide}. A rejected input's disposition goes to
 *  partition 6 through the reject collector; a routed one goes to its station. Pure: it reads the record and the rule set. */
public final class Routing {
    public sealed interface Outcome permits Routed, Rejected {}
    public record Routed(String station, Inputs.InputRecord record) implements Outcome {}
    /** The disposition body and part of a rejected input; the adapter adds the key's generation and origin. */
    public record Rejected(Inputs.InputRecord record, String part, Disposition.Body body) implements Outcome {}

    private final RuleSet rules;
    private final JsonNode definitions;
    private final Set<String> equipment;

    public Routing(RuleSet rules) {
        this.rules = rules;
        this.definitions = rules.pins().get("definitions");
        this.equipment = rules.members().equipment();
    }

    public Outcome route(InputParser.Read read) {
        Inputs.InputRecord r = read.record();
        String topic = r.origin().topic();
        switch (topic) {
            case InputParser.MESSAGES: return message(read);
            case InputParser.SESSIONS: return session(read);
            case InputParser.OCCUPANCY: return occupancy(read);
            default: throw new IllegalArgumentException("not a stage-1 input topic: " + topic);
        }
    }

    private Outcome message(InputParser.Read read) {
        Inputs.InputRecord r = read.record();
        if (read.rejected()) return reject(r, Disposition.Part.MESSAGE, NullNode.getInstance(), Disposition.Reason.SCHEMA_INVALID, read.check());
        Inputs.ProcessMessage m = (Inputs.ProcessMessage) r.parsed();
        ObjectNode id = Ids.messageId(m);
        Optional<String> station = stationOf(m.device());
        if (station.isEmpty()) return reject(r, Disposition.Part.MESSAGE, id, Disposition.Reason.UNKNOWN_DEVICE, null);
        if (Events.eventTime(r) == null) return reject(r, Disposition.Part.MESSAGE, id, Disposition.Reason.NO_SOURCE_TIMESTAMP, null);
        JsonNode def = definitions.path(m.device()).path("sha256");
        if (!def.isTextual() || !def.textValue().equals(m.definitionSha256())) return reject(r, Disposition.Part.MESSAGE, id, Disposition.Reason.DEFINITION_MISMATCH, null);
        return new Routed(station.get(), r);
    }

    private Outcome session(InputParser.Read read) {
        Inputs.InputRecord r = read.record();
        if (read.rejected()) return reject(r, Disposition.Part.EVENT, NullNode.getInstance(), Disposition.Reason.SCHEMA_INVALID, read.check());
        Inputs.SessionEvent s = (Inputs.SessionEvent) r.parsed();
        Optional<String> station = stationOf(s.device());
        if (station.isEmpty()) return reject(r, Disposition.Part.EVENT, Ids.sessionEventId(s), Disposition.Reason.UNKNOWN_DEVICE, null);
        return new Routed(station.get(), r);
    }

    private Outcome occupancy(InputParser.Read read) {
        Inputs.InputRecord r = read.record();
        JsonNode v = r.value();
        String part = v != null && v.isObject() && v.has("errorDeclaration") ? Disposition.Part.ERROR_DECLARATION : Disposition.Part.EVENT;
        if (read.rejected()) return reject(r, part, NullNode.getInstance(), Disposition.Reason.SCHEMA_INVALID, read.check());
        Inputs.OccupancyEvent o = (Inputs.OccupancyEvent) r.parsed();
        ObjectNode id = Ids.occupancyId(o);
        if (!rules.occupancy().sources().contains(o.source())) return reject(r, part, id, Disposition.Reason.SCHEMA_INVALID, Disposition.ProfileCheck.SOURCES.wire());
        if (!rules.occupancy().matches(o.unitId())) return reject(r, part, id, Disposition.Reason.SCHEMA_INVALID, Disposition.ProfileCheck.IDENTITY_PATTERN.wire());
        if (!o.bizStep().equals(rules.occupancy().enterBizStep()) && !o.bizStep().equals(rules.occupancy().leaveBizStep()))
            return reject(r, part, id, Disposition.Reason.SCHEMA_INVALID, Disposition.ProfileCheck.BIZ_STEP.wire());
        if (o.errorDeclaration() != null && !rules.occupancy().correctionReasons().contains(o.errorDeclaration().path("reason").asText()))
            return reject(r, part, id, Disposition.Reason.SCHEMA_INVALID, Disposition.ProfileCheck.CORRECTION_REASONS.wire());
        if (!rules.attribution().stations().contains(o.readPoint())) return reject(r, part, id, Disposition.Reason.UNKNOWN_READPOINT, null);
        return new Routed(o.readPoint(), r);
    }

    private Optional<String> stationOf(String device) {
        if (!equipment.contains(device)) return Optional.empty();
        return rules.hierarchy().stationOf(rules.attribution().stations(), device);
    }

    private static Rejected reject(Inputs.InputRecord r, String part, JsonNode identity, Disposition.Reason reason, String check) {
        ObjectNode refs = Json.object();
        if (check != null) refs.put("check", check);
        if ("raw-bytes".equals(r.payloadDigest().rule())) refs.put("digestRule", "raw-bytes");
        return new Rejected(r, part, new Disposition.Body(Events.inputKind(r.origin().topic()), identity, r.payloadDigest().sha256(), reason.kind(), reason, refs));
    }
}
