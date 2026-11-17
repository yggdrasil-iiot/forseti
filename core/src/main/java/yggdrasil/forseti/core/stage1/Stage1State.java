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
import yggdrasil.forseti.core.segment.ChainSegments;
import yggdrasil.forseti.core.segment.Segments;
import yggdrasil.forseti.core.segment.TrackedMap;

/** The state of one station's stage-1 writer (rule sheet S2-S11). The caller owns it: {@link Stage1#decide} takes it, updates it and
 *  returns it; adapters keep it in their store through {@link #toJson} / {@link #fromJson} (versioned). Maps are sorted by their
 *  canonical key text, so equal states serialize to equal bytes.
 *  <ul>
 *  <li>{@code deliveries}: every stored occupancy delivery per identity {@code (source, eventId)} (S2); visits, pending leaves and
 *      tombstones are derived from it ({@link #visits()}, {@link #pendingLeaves()}, {@link #tombstones()});</li>
 *  <li>{@code expired}: the frozen summary of a visit whose online state the expiry timer dropped (S3), still a window candidate;</li>
 *  <li>{@code samples} by sampleId, {@code measurements} by {@code (device, metric, sourceTs)}, {@code messages} (raw bytes of the
 *      admitted messages, for a later quarantine), {@code sessions} by session event identity;</li>
 *  <li>{@code snapshots} (plc-stamped, rule sheet P1): one summary per admitted message of an identified equipment, by message
 *      identity; the lifecycle (P2) is derived from it;</li>
 *  <li>{@code quarantined}: origins already written to the quarantine;</li>
 *  <li>{@code deadlines}, the three revision chains (work records, samples, dispositions), {@code lastW1}, {@code admissionW},
 *      {@code progressCount} and the init evidence of the next first progress.</li></ul> */
public final class Stage1State {
    public static final int VERSION = 1;
    /** Segment kinds (see "Segments" below). */
    static final String HEAD = Segments.HEAD, DELIVERIES = "deliveries", ADMISSION = "admission", SESSIONS = "sessions", DEADLINES = "deadlines", EXPIRED = "expired", SAMPLES = "samples", MEASUREMENTS = "measurements", MESSAGES = "messages",
        SNAPSHOTS = "snapshots", QUARANTINED = "quarantined", REV_WORK_RECORDS = "rev.work-records", REV_SAMPLES = "rev.samples",
        REV_DISPOSITIONS = "rev.dispositions";

    final long generation;
    final String station;
    final int partition;
    /** The rule's attribution mode, {@code occupancy-join} or {@code plc-stamped}. */
    final String mode;
    /** The segments changed since the last {@link #drainChanged} (see "Segments" below). */
    final Segments.Dirty dirty = new Segments.Dirty();
    /** Bucketed by the eventTime of the identity's first delivery: an Occupancy is put once its first delivery is in its list, and a list
     *  only grows. */
    final TrackedMap<Occupancy> deliveries = TrackedMap.bucketed((k, o) -> DELIVERIES + "/" + Segments.visitBucket(o.list.get(0).content.path("eventTime").longValue()), dirty);
    final TrackedMap<Expired> expired = TrackedMap.bucketed((k, x) -> EXPIRED + "/" + Segments.visitBucket(x.enter()), dirty);
    final TrackedMap<Sample> samples = TrackedMap.bucketed((k, x) -> SAMPLES + "/" + Segments.token(x.device) + "/" + bucket(x.sourceTs, x.origin), dirty);
    final TrackedMap<Measurement> measurements = TrackedMap.bucketed((k, x) -> MEASUREMENTS + "/" + Segments.token(x.device) + "/" + Segments.timeBucket(x.sourceTs), dirty);
    final TrackedMap<String> messages = TrackedMap.bucketed((k, x) -> MESSAGES + "/" + Segments.originKeyBucket(k), dirty);
    final TrackedMap<Session> sessions = TrackedMap.bucketed((k, x) -> SESSIONS + "/" + Segments.hashBucket(k), dirty);
    final Snapshots snapshots = new Snapshots(TrackedMap.bucketed((k, x) -> SNAPSHOTS + "/" + Segments.token(x.device()) + "/" + bucket(x.sourceTs(), x.origin()), dirty));
    /** The occupancy visits as they stood at the last committed progress, by {@code source NUL arrivalEventId}: S3 admission and the
     *  {@code late-after-settlement} test (S4) judge an occupancy input against them, not against the visit as earlier inputs of the
     *  same cut changed it. Set at every cut close. */
    final TrackedMap<Admitted> admissionVisits = TrackedMap.bucketed((k, x) -> ADMISSION + "/" + Segments.hashBucket(k), dirty);
    /** Origins of messages and session events whose raw bytes went to the quarantine already (the value is always true). */
    final TrackedMap<Boolean> quarantined = TrackedMap.bucketed((k, x) -> QUARANTINED + "/" + Segments.originKeyBucket(k), dirty);
    DeadlineIndex deadlines;
    Revisions workRecords, sampleRows, dispositions;
    private final ChainSegments workRecordSegments = new ChainSegments(REV_WORK_RECORDS, (t, k) -> Segments.hashBucket(t), dirty);
    private final ChainSegments sampleRowSegments = new ChainSegments(REV_SAMPLES, Stage1State::sampleKeyBucket, dirty);
    private final ChainSegments dispositionSegments = new ChainSegments(REV_DISPOSITIONS, Stage1State::dispositionKeyBucket, dirty);
    Long lastW1, admissionW;
    long progressCount;
    Progress.InitEvidence pendingInit;

    Stage1State(long generation, String station, int partition, String mode) {
        if (generation < 1) throw new IllegalArgumentException("generation >= 1");
        this.generation = generation; this.station = station; this.partition = partition; this.mode = Objects.requireNonNull(mode);
        bind(new DeadlineIndex(), new Revisions(), new Revisions(), new Revisions());
    }

    private void bind(DeadlineIndex d, Revisions w, Revisions s, Revisions x) {
        deadlines = d;
        deadlines.onChange(t -> dirty.mark(DEADLINES + "/" + Segments.visitBucket(t)));
        workRecords = workRecordSegments.bind(w);
        sampleRows = sampleRowSegments.bind(s);
        dispositions = dispositionSegments.bind(x);
    }

    /** A head field changed (a scalar). */
    void touchHead() { dirty.mark(HEAD); }
    /** Records that an origin's raw bytes went to the quarantine. */
    void quarantine(String originKey) { quarantined.putIfAbsent(originKey, Boolean.TRUE); }

    /** The writer's initialization (spec 408): the init evidence its first progress after this initialization carries. */
    public void initialize(Progress.InitEvidence evidence) { this.pendingInit = evidence; touchHead(); }

    public long generation() { return generation; }
    public String station() { return station; }
    public int partition() { return partition; }
    public DeadlineIndex deadlines() { return deadlines; }
    public Long lastW1() { return lastW1; }
    /** The station's W1 at its last committed progress (null before the first): the admission bound of the next cut. */
    public Long admissionW() { return admissionW; }
    public long progressCount() { return progressCount; }
    /** Visits with online state: arrivals whose deliveries are stored ({@code occupancy-join}); under {@code plc-stamped} the visit IDs
     *  with a valid {@code VisitPresent=true} snapshot whose visit did not expire. */
    public int onlineVisits() {
        if (PlcStamped.MODE.equals(mode)) {
            Set<String> gone = new HashSet<>(), ids = new TreeSet<>();
            expired.values().forEach(x -> { if (PlcStamped.MODE.equals(x.visitRef().path("mode").asText())) gone.add(x.visitRef().path("visitId").asText()); });
            snapshots.values().forEach(x -> { if (x.valid() && Boolean.TRUE.equals(x.present()) && !gone.contains(x.visitId())) ids.add(x.visitId()); });
            return ids.size();
        }
        return (int) deliveries.values().stream().filter(Occupancy::hasArrivingVariant).count();
    }
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
    /** One visit at the last committed progress: its valid leave (or null) and its {@code onlineExpiresAt}. */
    record Admitted(Long leave, long onlineExpiresAt) {}

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
    /** One message of an identified equipment (rule sheet P1): {@code sourceTs} is the common sourceTs of a coherent message, else its
     *  largest member sourceTs; {@code valid} when coherent with a String {@code UnitId} and {@code VisitId} and a Boolean
     *  {@code VisitPresent} of accepted quality, and only then the three values are set. {@code identitySamples} maps each identity
     *  member's metric to its sampleId, for the identity-conflict dispositions (P2). */
    record Snapshot(String device, Long sourceTs, boolean coherent, boolean valid, String unitId, String visitId, Boolean present,
                    Inputs.Origin origin, String payloadDigest, String digestRule, ObjectNode identitySamples) {}

    static String originKey(Inputs.Origin o) { return o.topic() + "/" + o.partition() + "/" + o.offset(); }

    // ---------------------------------------------------------------------------------------------------------------- JSON

    public ObjectNode toJson() {
        ObjectNode n = Json.object().put("stateVersion", VERSION);
        Json.putSafe(n, "generation", generation);
        n.put("station", station).put("partition", partition).put("attributionMode", mode);
        ObjectNode d = n.putObject("deliveries");
        deliveries.forEach((k, o) -> d.set(k, occupancyJson(o)));
        ObjectNode ex = n.putObject("expired");
        expired.forEach((k, v) -> ex.set(k, expiredJson(v)));
        ObjectNode av = n.putObject("admissionVisits");
        admissionVisits.forEach((k, v) -> av.set(k, admittedJson(v)));
        ObjectNode s = n.putObject("samples");
        samples.forEach((k, x) -> s.set(k, sampleJson(x)));
        ObjectNode m = n.putObject("measurements");
        measurements.forEach((k, x) -> m.set(k, measurementJson(x)));
        ObjectNode msg = n.putObject("messages");
        messages.forEach(msg::put);
        ObjectNode ss = n.putObject("sessions");
        sessions.forEach((k, x) -> ss.set(k, sessionJson(x)));
        ObjectNode sn = n.putObject("snapshots");
        snapshots.forEach((k, x) -> sn.set(k, snapshotJson(x)));
        ArrayNode q = n.putArray("quarantined");
        quarantined.keySet().forEach(q::add);
        n.set("deadlines", deadlines.toJson());
        ObjectNode rev = n.putObject("revisions");
        rev.set("workRecords", workRecords.toJson());
        rev.set("samples", sampleRows.toJson());
        rev.set("dispositions", dispositions.toJson());
        putLong(n, "lastW1", lastW1);
        putLong(n, "admissionW", admissionW);
        Json.putSafe(n, "progressCount", progressCount);
        n.set("pendingInit", pendingInitJson());
        return n;
    }

    private static ObjectNode occupancyJson(Occupancy o) {
        ObjectNode e = Json.object().put("source", o.source).put("eventId", o.eventId);
        ArrayNode l = e.putArray("list");
        for (OccDelivery x : o.list) {
            ObjectNode y = l.addObject();
            y.set("origin", origin(x.origin));
            y.set("content", x.content);
            y.set("declaration", x.declaration == null ? NullNode.getInstance() : x.declaration);
            y.put("payloadDigest", x.payloadDigest).put("digestRule", x.digestRule).put("raw", x.raw).put("quarantined", x.quarantined);
        }
        return e;
    }
    private static ObjectNode expiredJson(Expired v) {
        ObjectNode e = Json.object();
        e.set("visitRef", v.visitRef());
        Json.putSafe(e, "enter", v.enter());
        putLong(e, "leave", v.leave());
        putLong(e, "attributionEnd", v.attributionEnd());
        e.put("retracted", v.retracted());
        Json.putSafe(e, "onlineExpiresAt", v.onlineExpiresAt());
        return e;
    }
    private static ObjectNode admittedJson(Admitted v) {
        ObjectNode e = Json.object();
        putLong(e, "leave", v.leave());
        Json.putSafe(e, "onlineExpiresAt", v.onlineExpiresAt());
        return e;
    }
    private static ObjectNode sampleJson(Sample x) {
        ObjectNode e = Json.object();
        e.set("sampleId", x.sampleId);
        e.put("device", x.device).put("metric", x.metric).put("memberKey", x.memberKey);
        putLong(e, "sourceTs", x.sourceTs);
        putLong(e, "status", x.status);
        e.set("value", value(x.value));
        e.set("active", x.active);
        e.set("origin", origin(x.origin));
        e.put("payloadDigest", x.payloadDigest).put("digestRule", x.digestRule).put("replay", x.replay).put("idConflict", x.idConflict);
        e.set("row", x.row == null ? NullNode.getInstance() : x.row);
        return e;
    }
    private static ObjectNode measurementJson(Measurement x) {
        ObjectNode e = Json.object().put("device", x.device).put("metric", x.metric);
        Json.putSafe(e, "sourceTs", x.sourceTs);
        ArrayNode keys = e.putArray("sampleKeys");
        x.sampleKeys.forEach(keys::add);
        e.put("coverageVisit", x.coverageVisit);
        return e;
    }
    private static ObjectNode sessionJson(Session x) {
        ObjectNode e = Json.object();
        e.set("identity", x.identity);
        e.put("device", x.device).put("sessionKey", x.sessionKey).put("kind", x.kind);
        ArrayNode l = e.putArray("list");
        for (SessionDelivery y : x.list) {
            ObjectNode z = l.addObject();
            z.set("origin", origin(y.origin()));
            z.set("payload", y.payload());
            z.put("content", y.content()).put("payloadDigest", y.payloadDigest()).put("digestRule", y.digestRule()).put("raw", y.raw());
        }
        return e;
    }
    private static ObjectNode snapshotJson(Snapshot x) {
        ObjectNode e = Json.object().put("device", x.device());
        putLong(e, "sourceTs", x.sourceTs());
        e.put("coherent", x.coherent()).put("valid", x.valid()).put("unitId", x.unitId()).put("visitId", x.visitId()).put("present", x.present());
        e.set("origin", origin(x.origin()));
        e.put("payloadDigest", x.payloadDigest()).put("digestRule", x.digestRule());
        e.set("identitySamples", x.identitySamples());
        return e;
    }
    private JsonNode pendingInitJson() {
        if (pendingInit == null) return NullNode.getInstance();
        ObjectNode p = Json.putSafe(Json.object(), "declarationOffset", pendingInit.declarationOffset());
        return Json.putSafe(p, "endOffset", pendingInit.endOffset()).put("restore", pendingInit.restore());
    }

    public static Stage1State fromJson(JsonNode n) {
        if (n.path("stateVersion").intValue() != VERSION) throw new IllegalArgumentException("stage-1 state version " + n.path("stateVersion") + " is not " + VERSION);
        Stage1State st = new Stage1State(Json.safeLong(n.get("generation"), "generation", Json.NON_NEGATIVE), Json.text(n, "station"), n.get("partition").intValue(),
            Json.text(n, "attributionMode"));
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
        n.get("admissionVisits").fields().forEachRemaining(e -> st.admissionVisits.put(e.getKey(),
            new Admitted(getLong(e.getValue(), "leave"), Json.safeLong(e.getValue().get("onlineExpiresAt"), "onlineExpiresAt", Json.ANY))));
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
        n.get("snapshots").fields().forEachRemaining(e -> {
            JsonNode v = e.getValue();
            st.snapshots.put(e.getKey(), new Snapshot(Json.text(v, "device"), getLong(v, "sourceTs"), v.get("coherent").booleanValue(), v.get("valid").booleanValue(),
                v.get("unitId").isNull() ? null : v.get("unitId").textValue(), v.get("visitId").isNull() ? null : v.get("visitId").textValue(),
                v.get("present").isNull() ? null : v.get("present").booleanValue(), origin(v.get("origin")), Json.text(v, "payloadDigest"), Json.text(v, "digestRule"),
                (ObjectNode) longs(v.get("identitySamples"))));
        });
        n.get("quarantined").forEach(k -> st.quarantined.put(k.textValue(), Boolean.TRUE));
        st.bind(DeadlineIndex.fromJson(n.get("deadlines")), Revisions.fromJson(n.get("revisions").get("workRecords")),
            Revisions.fromJson(n.get("revisions").get("samples")), Revisions.fromJson(n.get("revisions").get("dispositions")));
        st.lastW1 = getLong(n, "lastW1");
        st.admissionW = getLong(n, "admissionW");
        st.progressCount = Json.safeLong(n.get("progressCount"), "progressCount", Json.NON_NEGATIVE);
        JsonNode p = n.get("pendingInit");
        st.pendingInit = p.isNull() ? null : new Progress.InitEvidence(Json.safeLong(p.get("declarationOffset"), "declarationOffset", Json.NON_NEGATIVE),
            Json.safeLong(p.get("endOffset"), "endOffset", Json.NON_NEGATIVE), Json.text(p, "restore"));
        return st;
    }

    // ---------------------------------------------------------------------------------------------------------------- segments

    /* The segmented form (plan step 6), an addition to toJson/fromJson: one head segment and bounded buckets.
     *  head                                      the scalars and pendingInit
     *  deliveries/t<first eventTime / 10 min>    occupancy deliveries per identity
     *  admission/h<hash % 64>                    admission visits
     *  sessions/h<hash % 64>                     session events
     *  deadlines/t<deadline / 10 min>            deadline registrations
     *  expired/t<enter / 10 min>                 expired-visit summaries
     *  samples/<device>/t<sourceTs / 10 s>       samples (no sourceTs: samples/<device>/<partition token>/o<offset / 64>)
     *  measurements/<device>/t<sourceTs / 10 s>  measurements
     *  messages/<partition token>/o<offset / 64> raw bytes of admitted messages, by origin
     *  snapshots/<device>/t<sourceTs / 10 s>     plc-stamped snapshots (no sourceTs: by origin, as samples)
     *  quarantined/<partition token>/o<offset / 64>          origins already quarantined
     *  rev.work-records/<generation>/h<hash % 64>             the work-record revision chain, by visitRef hash
     *  rev.samples/<generation>/<device>/t<publishTs / 10 s>  the sample revision chain, by sampleId device and publishTs
     *  rev.dispositions/<generation>/<partition token>/o<inputOffset / 64>  the disposition chain, by input origin
     * Every change marks its segment where it happens (TrackedMap, Revisions and DeadlineIndex listeners, and the in-place changes
     * Stage1 reports with touch); reading or recomputing marks nothing. */

    private static String bucket(Long ts, Inputs.Origin origin) {
        if (ts != null) return Segments.timeBucket(ts);
        return origin == null ? "none" : Segments.originBucket(origin.topic(), origin.partition(), origin.offset());
    }
    private static String sampleKeyBucket(String text, JsonNode key) {
        JsonNode k = ChainSegments.parse(text, key);
        if (!k.path("device").isTextual() || !k.path("publishTs").isIntegralNumber()) return Segments.hashBucket(text);
        return Segments.token(k.get("device").textValue()) + "/" + Segments.timeBucket(k.get("publishTs").longValue());
    }
    static String dispositionKeyBucket(String text, JsonNode key) {
        JsonNode k = ChainSegments.parse(text, key);
        if (!k.path("inputTopic").isTextual() || !k.path("inputPartition").isIntegralNumber() || !k.path("inputOffset").isIntegralNumber()) return Segments.hashBucket(text);
        return Segments.originBucket(k.get("inputTopic").textValue(), k.get("inputPartition").intValue(), k.get("inputOffset").longValue());
    }

    /** The ids of every segment of the state. */
    public SortedSet<String> segmentIds() {
        TreeSet<String> ids = new TreeSet<>();
        ids.add(HEAD);
        ids.addAll(deliveries.segmentIds()); ids.addAll(admissionVisits.segmentIds()); ids.addAll(sessions.segmentIds());
        for (DeadlineIndex.Deadline d : deadlines.all()) ids.add(DEADLINES + "/" + Segments.visitBucket(d.deadline()));
        ids.addAll(expired.segmentIds()); ids.addAll(samples.segmentIds()); ids.addAll(measurements.segmentIds()); ids.addAll(messages.segmentIds());
        ids.addAll(snapshots.segmentIds()); ids.addAll(quarantined.segmentIds());
        ids.addAll(workRecordSegments.segmentIds()); ids.addAll(sampleRowSegments.segmentIds()); ids.addAll(dispositionSegments.segmentIds());
        return ids;
    }

    /** One segment's canonical bytes, or null when the state has no such segment. */
    public byte[] segment(String id) {
        ObjectNode n;
        switch (Segments.kind(id)) {
            case HEAD:
                if (!HEAD.equals(id)) return null;
                n = Segments.header(HEAD);
                n.set("state", headJson());
                break;
            case DELIVERIES: n = Segments.entries(id, deliveries.entries(id), Stage1State::occupancyJson); break;
            case ADMISSION: n = Segments.entries(id, admissionVisits.entries(id), Stage1State::admittedJson); break;
            case SESSIONS: n = Segments.entries(id, sessions.entries(id), Stage1State::sessionJson); break;
            case DEADLINES: {
                long w = Segments.visitWindowOf(id);
                ArrayNode a = deadlines.toJson(w * Segments.VISIT_WINDOW_MS, (w + 1) * Segments.VISIT_WINDOW_MS);
                if (a.isEmpty()) return null;
                n = Segments.header(id);
                n.set("entries", a);
                break;
            }
            case EXPIRED: n = Segments.entries(id, expired.entries(id), Stage1State::expiredJson); break;
            case SAMPLES: n = Segments.entries(id, samples.entries(id), Stage1State::sampleJson); break;
            case MEASUREMENTS: n = Segments.entries(id, measurements.entries(id), Stage1State::measurementJson); break;
            case MESSAGES: n = Segments.entries(id, messages.entries(id), com.fasterxml.jackson.databind.node.TextNode::valueOf); break;
            case SNAPSHOTS: n = Segments.entries(id, snapshots.entries(id), Stage1State::snapshotJson); break;
            case QUARANTINED: {
                SortedMap<String, Boolean> m = quarantined.entries(id);
                if (m.isEmpty()) return null;
                n = Segments.header(id);
                ArrayNode a = n.putArray("entries");
                m.keySet().forEach(a::add);
                break;
            }
            case REV_WORK_RECORDS: return workRecordSegments.segment(id);
            case REV_SAMPLES: return sampleRowSegments.segment(id);
            case REV_DISPOSITIONS: return dispositionSegments.segment(id);
            default: return null;
        }
        return n == null ? null : Segments.bytes(n);
    }

    /** The head's state fields: toJson without the bucketed collections. */
    private ObjectNode headJson() {
        ObjectNode n = Json.object().put("stateVersion", VERSION);
        Json.putSafe(n, "generation", generation);
        n.put("station", station).put("partition", partition).put("attributionMode", mode);
        putLong(n, "lastW1", lastW1);
        putLong(n, "admissionW", admissionW);
        Json.putSafe(n, "progressCount", progressCount);
        n.set("pendingInit", pendingInitJson());
        return n;
    }

    /** The whole state as segments: id to canonical bytes, deterministic (equal states give equal maps). */
    public SortedMap<String, byte[]> segments() {
        TreeMap<String, byte[]> m = new TreeMap<>();
        for (String id : segmentIds()) m.put(id, segment(id));
        return m;
    }

    /** The segments written or removed since the last drain; callable at any time (at a checkpoint, or at every cut close). The first
     *  drain of a state not built by {@link #fromSegments} is {@code full}: every segment, to replace what the store held. */
    public Segments.Drain drainChanged() { return Segments.drain(dirty, this::segment, this::segments); }

    /** The state a segment map holds ({@link #segments}, or a store kept with {@link #drainChanged}); its next drain holds only what
     *  changes after this. A map that is not exactly the state's segments (a missing or extra segment, a key in the wrong bucket, another
     *  version) is refused. */
    public static Stage1State fromSegments(Map<String, byte[]> segments) {
        byte[] h = segments.get(HEAD);
        if (h == null) throw new IllegalArgumentException("stage-1 segments without a head");
        ObjectNode n = ((ObjectNode) Segments.read(HEAD, h).get("state")).deepCopy();
        ObjectNode d = Json.object(), av = Json.object(), ss = Json.object(), ex = Json.object(), s = Json.object(), m = Json.object(), msg = Json.object(), sn = Json.object();
        ArrayNode dl = Json.JSON.createArrayNode();
        ObjectNode wr = Json.object(), sr = Json.object(), dr = Json.object();
        TreeSet<String> q = new TreeSet<>();
        for (Map.Entry<String, byte[]> e : segments.entrySet()) {
            String id = e.getKey();
            if (HEAD.equals(id)) continue;
            JsonNode seg = Segments.read(id, e.getValue());
            switch (Segments.kind(id)) {
                case DELIVERIES: merge(d, seg, id); break;
                case ADMISSION: merge(av, seg, id); break;
                case SESSIONS: merge(ss, seg, id); break;
                case DEADLINES: dl.addAll((ArrayNode) seg.get("entries")); break;
                case EXPIRED: merge(ex, seg, id); break;
                case SAMPLES: merge(s, seg, id); break;
                case MEASUREMENTS: merge(m, seg, id); break;
                case MESSAGES: merge(msg, seg, id); break;
                case SNAPSHOTS: merge(sn, seg, id); break;
                case QUARANTINED: seg.get("entries").forEach(x -> { if (!q.add(x.textValue())) throw new IllegalArgumentException("quarantined " + x + " in two segments"); }); break;
                case REV_WORK_RECORDS: ChainSegments.merge(wr, seg); break;
                case REV_SAMPLES: ChainSegments.merge(sr, seg); break;
                case REV_DISPOSITIONS: ChainSegments.merge(dr, seg); break;
                default: throw new IllegalArgumentException("not a stage-1 segment: " + id);
            }
        }
        n.set("deliveries", d); n.set("admissionVisits", av); n.set("sessions", ss); n.set("deadlines", dl);
        n.set("expired", ex); n.set("samples", s); n.set("measurements", m); n.set("messages", msg); n.set("snapshots", sn);
        ArrayNode qa = n.putArray("quarantined");
        q.forEach(qa::add);
        ObjectNode rev = n.putObject("revisions");
        rev.set("workRecords", wr); rev.set("samples", sr); rev.set("dispositions", dr);
        Stage1State st = fromJson(n);
        if (!st.segmentIds().equals(new TreeSet<>(segments.keySet())))
            throw new IllegalArgumentException("stage-1 segments: the map's " + segments.size() + " ids are not the state's " + st.segmentIds().size());
        st.dirty.clean();
        return st;
    }

    private static void merge(ObjectNode target, JsonNode segment, String id) {
        segment.get("entries").fields().forEachRemaining(e -> {
            if (target.has(e.getKey())) throw new IllegalArgumentException("key " + e.getKey() + " in two segments (" + id + ")");
            target.set(e.getKey(), e.getValue());
        });
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
