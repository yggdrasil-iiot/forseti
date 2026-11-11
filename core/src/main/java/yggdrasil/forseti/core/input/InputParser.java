package yggdrasil.forseti.core.input;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** Parses the four input kinds (spec Input events). Occupancy and incidents are validated against the built-in schemas 1.0.0
 *  (JSON Schema 2020-12); process messages and session events structurally. Every integer field is read with {@link Json#safeLong}
 *  against the range the schemas state, {@code 0 .. 2^53-1}, before the schema runs, so an out-of-range or non-integral value is
 *  {@code schema-invalid} with {@code refs.check} naming the field. A failure is a {@link Rejected}, never a stop: {@link #read}
 *  returns it as data. */
public final class InputParser {
    public static final String MESSAGES = "ot.messages";
    public static final String SESSIONS = "ot.sessions";
    public static final String OCCUPANCY = "mfg.occupancy";
    public static final String INCIDENTS = "quality.incidents";
    public static final String OCCUPANCY_SCHEMA_ID = "urn:forseti:schema:occupancy:1.0.0";
    public static final String INCIDENT_SCHEMA_ID = "urn:forseti:schema:incident:1.0.0";

    private static final long STATUS_CODE_MAX = 4294967295L;
    private static final List<String> SESSION_KINDS = List.of("seq-gap", "session-down", "session-up", "stale-death", "session-ambiguous", "definition-changed");
    private static final List<String> SESSION_PAYLOAD_INTEGERS = List.of("from", "to", "missing", "at", "detectedEventTime", "rawPublishTs", "bdSeq");

    private record Builtin(byte[] bytes, JsonNode node, JsonSchema schema) {}
    private static final Map<String, Builtin> SCHEMAS = Map.of(
        OCCUPANCY_SCHEMA_ID, load("occupancy-1.0.0.schema.json"),
        INCIDENT_SCHEMA_ID, load("incident-1.0.0.schema.json"));

    private InputParser() {}

    /** schema-invalid, with the field that failed when one is named ({@code refs.check}); null for a whole-value schema failure. */
    public static final class Rejected extends IllegalArgumentException {
        private final String check;
        public Rejected(String check) { super("schema-invalid"); this.check = check; }
        public String check() { return check; }
    }

    /** One delivered record read: the record (parsed null when rejected) and the rejection's check field. */
    public record Read(Inputs.InputRecord record, boolean rejected, String check) {}

    private static Builtin load(String file) {
        try (InputStream in = InputParser.class.getResourceAsStream("/forseti/schemas/" + file)) {
            if (in == null) throw new IllegalStateException("missing built-in schema " + file);
            byte[] b = in.readAllBytes();
            JsonNode n = Json.read(b);
            JsonSchema s = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(n);
            return new Builtin(b, n, s);
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    /** Raw-byte SHA-256 of a built-in schema: the profiles' {@code format.schemaSha256}. */
    public static String schemaSha256(String schemaId) {
        Builtin b = SCHEMAS.get(schemaId);
        if (b == null) throw new IllegalArgumentException("unknown schema " + schemaId);
        return Canonical.digestOf(b.bytes());
    }

    /** The incident schema's {@code kind} enum, in schema order. */
    public static List<String> incidentKinds() {
        List<String> l = new ArrayList<>();
        SCHEMAS.get(INCIDENT_SCHEMA_ID).node().path("properties").path("kind").path("enum").forEach(k -> l.add(k.textValue()));
        return List.copyOf(l);
    }

    /** Reads one delivered record; never throws for its content (a value that is not JSON is rejected schema-invalid). */
    public static Read read(Inputs.Origin origin, String key, byte[] rawValue) {
        JsonNode value;
        try { value = Json.read(rawValue); } catch (IllegalArgumentException e) { value = null; }
        Ids.Digest digest = Ids.payloadDigest(value, rawValue);
        if (value == null) return new Read(new Inputs.InputRecord(origin, key, rawValue, null, null, digest), true, null);
        try {
            Object parsed = parse(origin.topic(), value);
            return new Read(new Inputs.InputRecord(origin, key, rawValue, value, parsed, digest), false, null);
        } catch (Rejected r) {
            return new Read(new Inputs.InputRecord(origin, key, rawValue, value, null, digest), true, r.check());
        }
    }

    /** The parsed view of one input value; {@link Rejected} when it is schema-invalid. */
    public static Object parse(String topic, JsonNode value) {
        if (value == null || !value.isObject()) throw new Rejected(null);
        switch (topic) {
            case OCCUPANCY: return occupancy(value);
            case INCIDENTS: return incident(value);
            case MESSAGES: return message(value);
            case SESSIONS: return session(value);
            default: throw new IllegalArgumentException("not an input topic: " + topic);
        }
    }

    private static long integer(JsonNode n, String field, String check, long min, long max) {
        try { return Json.safeLong(n, field, min, max); } catch (IllegalArgumentException e) { throw new Rejected(check); }
    }
    private static long integer(JsonNode o, String field) { return integer(o.get(field), field, field, 0, Json.MAX_SAFE); }
    /** A present, non-null integer field is checked; an absent or null one is left to the schema (or is optional). */
    private static Long optionalInteger(JsonNode o, String field, String check, long min, long max) {
        JsonNode n = o.get(field);
        if (n == null || n.isNull()) return null;
        return integer(n, field, check, min, max);
    }
    private static void schema(String id, JsonNode value) {
        Set<ValidationMessage> errors = SCHEMAS.get(id).schema().validate(value);
        if (!errors.isEmpty()) throw new Rejected(null);
    }
    private static String text(JsonNode o, String field) {
        JsonNode n = o.get(field);
        if (n == null || !n.isTextual()) throw new Rejected(field);
        return n.textValue();
    }
    private static String optionalText(JsonNode o, String field) { JsonNode n = o.get(field); return n == null || n.isNull() ? null : n.textValue(); }

    private static Inputs.OccupancyEvent occupancy(JsonNode v) {
        for (String f : List.of("eventTime", "recordTime", "visitSeq")) optionalInteger(v, f, f, 0, Json.MAX_SAFE);
        JsonNode ed = v.get("errorDeclaration");
        if (ed != null && ed.isObject()) optionalInteger(ed, "declarationTime", "errorDeclaration.declarationTime", 0, Json.MAX_SAFE);
        schema(OCCUPANCY_SCHEMA_ID, v);
        return new Inputs.OccupancyEvent(v.get("eventId").textValue(), integer(v, "eventTime"), integer(v, "recordTime"), v.get("unitId").textValue(),
            v.get("bizStep").textValue(), v.get("readPoint").textValue(), integer(v, "visitSeq"), optionalText(v, "arrivalEventId"),
            v.get("source").textValue(), ed);
    }

    private static Inputs.IncidentEvent incident(JsonNode v) {
        optionalInteger(v, "revision", "revision", 1, Json.MAX_SAFE);
        for (String f : List.of("detectedAt", "revisionEventTime", "suspectFrom", "suspectTo")) optionalInteger(v, f, f, 0, Json.MAX_SAFE);
        schema(INCIDENT_SCHEMA_ID, v);
        return new Inputs.IncidentEvent(v.get("incidentId").textValue(), integer(v.get("revision"), "revision", "revision", 1, Json.MAX_SAFE),
            v.get("stationRef").textValue(), v.get("equipmentRef").textValue(), v.get("kind").textValue(), integer(v, "detectedAt"),
            integer(v, "revisionEventTime"), integer(v, "suspectFrom"), optionalInteger(v, "suspectTo", "suspectTo", 0, Json.MAX_SAFE),
            v.get("status").textValue(), v.get("evidenceRef").textValue(), v.get("source").textValue());
    }

    /** Structural: the message fields in contract order, then each member; a member that does not parse is a member rejection and the
     *  others stay (spec: a process message keeps every member that parses). A message with no member sourceTs is not refused here. */
    private static Inputs.ProcessMessage message(JsonNode v) {
        String edge = text(v, "edge"), device = text(v, "device");
        long birth = integer(v, "birthPublishTs"), bdSeq = integer(v, "bdSeq"), seq = integer(v, "seq"), publishTs = integer(v, "publishTs");
        Long received = optionalInteger(v, "bridgeReceivedAt", "bridgeReceivedAt", 0, Json.MAX_SAFE);
        String definition = text(v, "definitionSha256");
        JsonNode ms = v.get("members");
        if (ms == null || !ms.isArray()) throw new Rejected("members");
        Set<String> seen = new HashSet<>();
        for (JsonNode m : ms) {   // a member without a metric name has no part key of its own: the whole message is rejected
            if (!m.isObject() || !m.path("metric").isTextual() || !seen.add(m.get("metric").textValue())) throw new Rejected("metric");
        }
        List<Inputs.Member> members = new ArrayList<>();
        List<Inputs.MemberRejection> rejected = new ArrayList<>();
        for (JsonNode m : ms) {
            String metric = m.get("metric").textValue();
            try { members.add(member(metric, m)); }
            catch (Rejected r) { rejected.add(new Inputs.MemberRejection(metric, r.check())); }
        }
        return new Inputs.ProcessMessage(edge, device, birth, bdSeq, seq, publishTs, received, definition, List.copyOf(members), List.copyOf(rejected));
    }

    private static Inputs.Member member(String metric, JsonNode m) {
        JsonNode raw = m.get("value");
        JsonNode value;
        if (raw != null && raw.isNumber()) value = new DoubleNode(raw.doubleValue());   // binary64; a literal beyond the double range is a signed infinity
        else if (raw != null && (raw.isBoolean() || raw.isTextual())) value = raw;
        else throw new Rejected("value");
        Long status = optionalInteger(m, "uaStatusCode", "uaStatusCode", 0, STATUS_CODE_MAX);
        Long quality = optionalInteger(m, "quality", "quality", 0, Json.MAX_SAFE);
        Long sourceTs = optionalInteger(m, "sourceTs", "sourceTs", 0, Json.MAX_SAFE);
        Long serverTs = optionalInteger(m, "serverTs", "serverTs", 0, Json.MAX_SAFE);
        return new Inputs.Member(metric, value, status, quality, sourceTs, serverTs);
    }

    private static Inputs.SessionEvent session(JsonNode v) {
        String edge = text(v, "edge"), device = text(v, "device");
        long birth = integer(v, "birthPublishTs"), bdSeq = integer(v, "bdSeq");
        String kind = text(v, "kind");
        if (!SESSION_KINDS.contains(kind)) throw new Rejected("kind");
        long atSeq = integer(v, "atSeq");
        JsonNode payload = v.get("payload");
        if (payload == null || payload.isNull()) payload = com.fasterxml.jackson.databind.node.NullNode.getInstance();
        else if (!payload.isObject()) throw new Rejected("payload");
        else for (String f : SESSION_PAYLOAD_INTEGERS) optionalInteger(payload, f, "payload." + f, 0, Json.MAX_SAFE);
        return new Inputs.SessionEvent(edge, device, birth, bdSeq, kind, atSeq, payload);
    }
}
