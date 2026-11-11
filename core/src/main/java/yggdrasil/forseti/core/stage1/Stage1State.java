package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Revisions;

/** The state of one station's stage-1 writer (rule sheet S2-S11). The caller owns it: {@link Stage1#decide} takes it, updates it and
 *  returns it; adapters keep it in their store through {@link #toJson} / {@link #fromJson} (versioned). Maps are sorted by their
 *  canonical key text, so equal states serialize to equal bytes.
 *  <ul>
 *  <li>{@code deliveries}: every stored occupancy delivery per identity {@code (source, eventId)} (S2); visits, pending leaves and
 *      tombstones are derived from it ({@link #visits()}, {@link #pendingLeaves()}, {@link #tombstones()});</li>
 *  <li>{@code expired}: the frozen summary of a visit whose online state the expiry timer dropped (S3), still a window candidate;</li>
 *  <li>{@code samples} by sampleId, {@code measurements} by {@code (device, metric, sourceTs)}, {@code messages} (raw bytes of the
 *      admitted messages, for a later quarantine), {@code sessions} by session event identity;</li>
 *  <li>{@code quarantined}: origins already written to the quarantine;</li>
 *  <li>{@code deadlines}, the three revision chains (work records, samples, dispositions), {@code lastW1}, {@code admissionW},
 *      {@code progressCount} and the init evidence of the next first progress.</li></ul> */
public final class Stage1State {
    public static final int VERSION = 1;

    final long generation;
    final String station;
    final int partition;
    final TreeMap<String, Occupancy> deliveries = new TreeMap<>();
    final TreeMap<String, Expired> expired = new TreeMap<>();
    final TreeMap<String, Sample> samples = new TreeMap<>();
    final TreeMap<String, Measurement> measurements = new TreeMap<>();
    final TreeMap<String, String> messages = new TreeMap<>();
    final TreeMap<String, Session> sessions = new TreeMap<>();
    /** Origins of messages and session events whose raw bytes went to the quarantine already. */
    final TreeSet<String> quarantined = new TreeSet<>();
    DeadlineIndex deadlines = new DeadlineIndex();
    Revisions workRecords = new Revisions(), sampleRows = new Revisions(), dispositions = new Revisions();
    Long lastW1, admissionW;
    long progressCount;
    Progress.InitEvidence pendingInit;

    Stage1State(long generation, String station, int partition) {
        if (generation < 1) throw new IllegalArgumentException("generation >= 1");
        this.generation = generation; this.station = station; this.partition = partition;
    }

    /** The writer's initialization (spec 408): the init evidence its first progress after this initialization carries. */
    public void initialize(Progress.InitEvidence evidence) { this.pendingInit = evidence; }

    public long generation() { return generation; }
    public String station() { return station; }
    public int partition() { return partition; }
    public DeadlineIndex deadlines() { return deadlines; }
    public Long lastW1() { return lastW1; }
    /** The station's W1 at its last committed progress (null before the first): the admission bound of the next cut. */
    public Long admissionW() { return admissionW; }
    public long progressCount() { return progressCount; }
    /** Visits with online state: arrivals whose deliveries are stored. */
    public int onlineVisits() { return (int) deliveries.values().stream().filter(Occupancy::hasArrivingVariant).count(); }
    public int onlineSamples() { return samples.size(); }
    /** The occupancy identities whose content arrives (the visits' arrivals), by canonical key. */
    public Set<String> visits() { TreeSet<String> s = new TreeSet<>(); deliveries.forEach((k, o) -> { if (o.hasArrivingVariant()) s.add(k); }); return s; }
    /** Departures whose arrival is not stored (S4: pending until the arrival arrives or S3 expires it). */
    public Set<String> pendingLeaves() {
        Set<String> arrivals = new HashSet<>();
        deliveries.values().forEach(o -> { if (o.hasArrivingVariant()) arrivals.add(o.source + "\u0000" + o.eventId); });
        TreeSet<String> s = new TreeSet<>();
        deliveries.forEach((k, o) -> {
            for (OccDelivery d : o.list) if (!d.arriving() && !arrivals.contains(o.source + "\u0000" + d.content.path("arrivalEventId").asText())) s.add(k);
        });
        return s;
    }
    /** Identities stored only with error declarations (a correction ahead of its original, spec 296). */
    public Set<String> tombstones() {
        TreeSet<String> s = new TreeSet<>();
        deliveries.forEach((k, o) -> { if (!o.list.isEmpty() && o.list.stream().allMatch(d -> d.declaration != null)) s.add(k); });
        return s;
    }

    // ---------------------------------------------------------------------------------------------------------------- entries

    /** The stored deliveries of one occupancy identity, in arrival order. */
    static final class Occupancy {
        final String source, eventId;
        final List<OccDelivery> list = new ArrayList<>();
        Occupancy(String source, String eventId) { this.source = source; this.eventId = eventId; }
        boolean hasArrivingVariant() { return list.stream().anyMatch(OccDelivery::arriving); }
    }
    /** One stored occupancy delivery: business content (the record without recordTime, errorDeclaration and bridgeReceivedAt), the
     *  declaration or null, and what its disposition is built from. */
    static final class OccDelivery {
        final Inputs.Origin origin;
        final ObjectNode content;
        final JsonNode declaration;
        final String payloadDigest, digestRule, raw;
        boolean quarantined;
        OccDelivery(Inputs.Origin origin, ObjectNode content, JsonNode declaration, String payloadDigest, String digestRule, String raw) {
            this.origin = origin; this.content = content; this.declaration = declaration; this.payloadDigest = payloadDigest; this.digestRule = digestRule; this.raw = raw;
        }
        boolean arriving() { return "urn:epcglobal:cbv:bizstep:arriving".equals(content.path("bizStep").asText()); }
        String part() { return declaration == null ? "event" : "errorDeclaration"; }
    }
    /** A visit whose online state expired: what a later window needs, and the deadline the S3 test compares with. */
    record Expired(ObjectNode visitRef, long enter, Long leave, Long attributionEnd, boolean retracted, long onlineExpiresAt) {}

    /** One sample by sampleId. {@code value} is the parsed binary64, Boolean or String; {@code active} the S9 value. */
    static final class Sample {
        final ObjectNode sampleId;
        final String device, metric, memberKey;
        final Long sourceTs, status;
        final JsonNode value, active;
        final Inputs.Origin origin;
        final String payloadDigest, digestRule;
        final boolean replay;
        boolean idConflict;
        ObjectNode row;
        Sample(ObjectNode sampleId, String device, String metric, String memberKey, Long sourceTs, Long status, JsonNode value, JsonNode active,
               Inputs.Origin origin, String payloadDigest, String digestRule, boolean replay) {
            this.sampleId = sampleId; this.device = device; this.metric = metric; this.memberKey = memberKey; this.sourceTs = sourceTs; this.status = status;
            this.value = value; this.active = active; this.origin = origin; this.payloadDigest = payloadDigest; this.digestRule = digestRule; this.replay = replay;
        }
        boolean finite() { return !value.isDouble() || Double.isFinite(value.doubleValue()); }
    }
    /** One measurement triple {@code (device, metric, sourceTs)}: the samples that carry it and the visit its coverage counts for. */
    static final class Measurement {
        final String device, metric;
        final long sourceTs;
        final TreeSet<String> sampleKeys = new TreeSet<>();
        String coverageVisit;
        Measurement(String device, String metric, long sourceTs) { this.device = device; this.metric = metric; this.sourceTs = sourceTs; }
    }
    /** One session event identity: its deliveries (content = the payload's canonical text). */
    static final class Session {
        final ObjectNode identity;
        final String device, sessionKey, kind;
        final List<SessionDelivery> list = new ArrayList<>();
        Session(ObjectNode identity, String device, String sessionKey, String kind) { this.identity = identity; this.device = device; this.sessionKey = sessionKey; this.kind = kind; }
        boolean conflicted() { return list.stream().map(SessionDelivery::content).distinct().count() > 1; }
        JsonNode payload() { return list.get(0).payload(); }
    }
    record SessionDelivery(Inputs.Origin origin, JsonNode payload, String content, String payloadDigest, String digestRule, String raw) {}

    static String originKey(Inputs.Origin o) { return o.topic() + "/" + o.partition() + "/" + o.offset(); }

    // ---------------------------------------------------------------------------------------------------------------- JSON

    public ObjectNode toJson() {
        ObjectNode n = Json.object().put("stateVersion", VERSION);
        Json.putSafe(n, "generation", generation);
        n.put("station", station).put("partition", partition);
        ObjectNode d = n.putObject("deliveries");
        deliveries.forEach((k, o) -> {
            ObjectNode e = d.putObject(k).put("source", o.source).put("eventId", o.eventId);
            ArrayNode l = e.putArray("list");
            for (OccDelivery x : o.list) {
                ObjectNode y = l.addObject();
                y.set("origin", origin(x.origin));
                y.set("content", x.content);
                y.set("declaration", x.declaration == null ? NullNode.getInstance() : x.declaration);
                y.put("payloadDigest", x.payloadDigest).put("digestRule", x.digestRule).put("raw", x.raw).put("quarantined", x.quarantined);
            }
        });
        ObjectNode ex = n.putObject("expired");
        expired.forEach((k, v) -> {
            ObjectNode e = ex.putObject(k);
            e.set("visitRef", v.visitRef());
            Json.putSafe(e, "enter", v.enter());
            putLong(e, "leave", v.leave());
            putLong(e, "attributionEnd", v.attributionEnd());
            e.put("retracted", v.retracted());
            Json.putSafe(e, "onlineExpiresAt", v.onlineExpiresAt());
        });
        ObjectNode s = n.putObject("samples");
        samples.forEach((k, x) -> {
            ObjectNode e = s.putObject(k);
            e.set("sampleId", x.sampleId);
            e.put("device", x.device).put("metric", x.metric).put("memberKey", x.memberKey);
            putLong(e, "sourceTs", x.sourceTs);
            putLong(e, "status", x.status);
            e.set("value", value(x.value));
            e.set("active", x.active);
            e.set("origin", origin(x.origin));
            e.put("payloadDigest", x.payloadDigest).put("digestRule", x.digestRule).put("replay", x.replay).put("idConflict", x.idConflict);
            e.set("row", x.row == null ? NullNode.getInstance() : x.row);
        });
        ObjectNode m = n.putObject("measurements");
        measurements.forEach((k, x) -> {
            ObjectNode e = m.putObject(k).put("device", x.device).put("metric", x.metric);
            Json.putSafe(e, "sourceTs", x.sourceTs);
            ArrayNode keys = e.putArray("sampleKeys");
            x.sampleKeys.forEach(keys::add);
            e.put("coverageVisit", x.coverageVisit);
        });
        ObjectNode msg = n.putObject("messages");
        messages.forEach(msg::put);
        ObjectNode ss = n.putObject("sessions");
        sessions.forEach((k, x) -> {
            ObjectNode e = ss.putObject(k);
            e.set("identity", x.identity);
            e.put("device", x.device).put("sessionKey", x.sessionKey).put("kind", x.kind);
            ArrayNode l = e.putArray("list");
            for (SessionDelivery y : x.list) {
                ObjectNode z = l.addObject();
                z.set("origin", origin(y.origin()));
                z.set("payload", y.payload());
                z.put("content", y.content()).put("payloadDigest", y.payloadDigest()).put("digestRule", y.digestRule()).put("raw", y.raw());
            }
        });
        ArrayNode q = n.putArray("quarantined");
        quarantined.forEach(q::add);
        n.set("deadlines", deadlines.toJson());
        ObjectNode rev = n.putObject("revisions");
        rev.set("workRecords", workRecords.toJson());
        rev.set("samples", sampleRows.toJson());
        rev.set("dispositions", dispositions.toJson());
        putLong(n, "lastW1", lastW1);
        putLong(n, "admissionW", admissionW);
        Json.putSafe(n, "progressCount", progressCount);
        if (pendingInit == null) n.putNull("pendingInit");
        else {
            ObjectNode p = Json.putSafe(n.putObject("pendingInit"), "declarationOffset", pendingInit.declarationOffset());
            Json.putSafe(p, "endOffset", pendingInit.endOffset()).put("restore", pendingInit.restore());
        }
        return n;
    }

    public static Stage1State fromJson(JsonNode n) {
        if (n.path("stateVersion").intValue() != VERSION) throw new IllegalArgumentException("stage-1 state version " + n.path("stateVersion") + " is not " + VERSION);
        Stage1State st = new Stage1State(Json.safeLong(n.get("generation"), "generation", Json.NON_NEGATIVE), Json.text(n, "station"), n.get("partition").intValue());
        n.get("deliveries").fields().forEachRemaining(e -> {
            JsonNode o = e.getValue();
            Occupancy occ = new Occupancy(Json.text(o, "source"), Json.text(o, "eventId"));
            for (JsonNode y : o.get("list")) {
                OccDelivery x = new OccDelivery(origin(y.get("origin")), (ObjectNode) y.get("content"), y.get("declaration").isNull() ? null : y.get("declaration"),
                    Json.text(y, "payloadDigest"), Json.text(y, "digestRule"), Json.text(y, "raw"));
                x.quarantined = y.get("quarantined").booleanValue();
                occ.list.add(x);
            }
            st.deliveries.put(e.getKey(), occ);
        });
        n.get("expired").fields().forEachRemaining(e -> {
            JsonNode v = e.getValue();
            st.expired.put(e.getKey(), new Expired((ObjectNode) v.get("visitRef"), Json.safeLong(v.get("enter"), "enter", Json.ANY), getLong(v, "leave"),
                getLong(v, "attributionEnd"), v.get("retracted").booleanValue(), Json.safeLong(v.get("onlineExpiresAt"), "onlineExpiresAt", Json.ANY)));
        });
        n.get("samples").fields().forEachRemaining(e -> {
            JsonNode v = e.getValue();
            Sample x = new Sample((ObjectNode) v.get("sampleId"), Json.text(v, "device"), Json.text(v, "metric"), Json.text(v, "memberKey"), getLong(v, "sourceTs"),
                getLong(v, "status"), unvalue(v.get("value")), v.get("active"), origin(v.get("origin")), Json.text(v, "payloadDigest"), Json.text(v, "digestRule"),
                v.get("replay").booleanValue());
            x.idConflict = v.get("idConflict").booleanValue();
            x.row = v.get("row").isNull() ? null : (ObjectNode) longs(v.get("row"));
            st.samples.put(e.getKey(), x);
        });
        n.get("measurements").fields().forEachRemaining(e -> {
            JsonNode v = e.getValue();
            Measurement x = new Measurement(Json.text(v, "device"), Json.text(v, "metric"), Json.safeLong(v.get("sourceTs"), "sourceTs", Json.NON_NEGATIVE));
            v.get("sampleKeys").forEach(k -> x.sampleKeys.add(k.textValue()));
            x.coverageVisit = v.get("coverageVisit").isNull() ? null : v.get("coverageVisit").textValue();
            st.measurements.put(e.getKey(), x);
        });
        n.get("messages").fields().forEachRemaining(e -> st.messages.put(e.getKey(), e.getValue().textValue()));
        n.get("sessions").fields().forEachRemaining(e -> {
            JsonNode v = e.getValue();
            Session x = new Session((ObjectNode) v.get("identity"), Json.text(v, "device"), Json.text(v, "sessionKey"), Json.text(v, "kind"));
            for (JsonNode y : v.get("list"))
                x.list.add(new SessionDelivery(origin(y.get("origin")), y.get("payload"), Json.text(y, "content"), Json.text(y, "payloadDigest"), Json.text(y, "digestRule"), Json.text(y, "raw")));
            st.sessions.put(e.getKey(), x);
        });
        n.get("quarantined").forEach(k -> st.quarantined.add(k.textValue()));
        st.deadlines = DeadlineIndex.fromJson(n.get("deadlines"));
        st.workRecords = Revisions.fromJson(n.get("revisions").get("workRecords"));
        st.sampleRows = Revisions.fromJson(n.get("revisions").get("samples"));
        st.dispositions = Revisions.fromJson(n.get("revisions").get("dispositions"));
        st.lastW1 = getLong(n, "lastW1");
        st.admissionW = getLong(n, "admissionW");
        st.progressCount = Json.safeLong(n.get("progressCount"), "progressCount", Json.NON_NEGATIVE);
        JsonNode p = n.get("pendingInit");
        st.pendingInit = p.isNull() ? null : new Progress.InitEvidence(Json.safeLong(p.get("declarationOffset"), "declarationOffset", Json.NON_NEGATIVE),
            Json.safeLong(p.get("endOffset"), "endOffset", Json.NON_NEGATIVE), Json.text(p, "restore"));
        return st;
    }

    static ObjectNode origin(Inputs.Origin o) {
        ObjectNode n = Json.object().put("topic", o.topic()).put("partition", o.partition());
        return Json.putSafe(n, "offset", o.offset());
    }
    static Inputs.Origin origin(JsonNode n) { return new Inputs.Origin(Json.text(n, "topic"), n.get("partition").intValue(), Json.safeLong(n.get("offset"), "offset", Json.NON_NEGATIVE)); }
    /** Integral nodes as LongNode, the type the decision function builds rows with, so a stored row equals a rebuilt one. */
    static JsonNode longs(JsonNode n) {
        if (n.isIntegralNumber()) return com.fasterxml.jackson.databind.node.LongNode.valueOf(n.longValue());
        if (n.isObject()) { ObjectNode o = Json.object(); n.fields().forEachRemaining(e -> o.set(e.getKey(), longs(e.getValue()))); return o; }
        if (n.isArray()) { ArrayNode a = Json.JSON.createArrayNode(); n.forEach(x -> a.add(longs(x))); return a; }
        return n;
    }
    static void putLong(ObjectNode n, String f, Long v) { if (v == null) n.putNull(f); else Json.putSafe(n, f, v); }
    static Long getLong(JsonNode n, String f) { JsonNode v = n.get(f); return v == null || v.isNull() ? null : Json.safeLong(v, f, Json.ANY); }
    /** A sample value in the store: a non-finite double as {@code {"nonFinite": "Infinity"}} (the strict mapper reads no NaN literal). */
    static JsonNode value(JsonNode v) { return v.isDouble() && !Double.isFinite(v.doubleValue()) ? Json.object().put("nonFinite", Double.toString(v.doubleValue())) : v; }
    static JsonNode unvalue(JsonNode v) { return v.isObject() ? new DoubleNode(Double.parseDouble(v.get("nonFinite").textValue())) : v.isNumber() ? new DoubleNode(v.doubleValue()) : v; }
}
