package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.NegativeControls;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.Ids;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.input.StatusCodes;
import yggdrasil.forseti.core.journal.*;
import yggdrasil.forseti.core.rules.MemberCatalog;
import yggdrasil.forseti.core.rules.RuleSet;

/** The stage-1 decision function for {@code occupancy-join} (rule sheet S2-S11). {@link #decide} reads only its arguments, the rule set
 *  and the negative controls it was constructed with: no clock, random, file, environment or static mutable state. It takes the state
 *  object, updates it and returns it with the outputs: {@link Row} (work records and samples; the envelope's {@code pins} and
 *  {@code eventTimeAt} are the adapter's, see {@link #stamp}), {@link Disposition}, {@link QuarantineRecord}, {@link Progress.Stage1} and
 *  {@link Progress.Ledger}. Routing and door rejections (S1) are {@link Routing}'s. */
public final class Stage1 {
    public static final String WORK_RECORDS = "forseti.work-records", SAMPLES = "forseti.samples";

    public record Result(Stage1State state, List<Object> outputs) {}

    final RuleSet rules;
    final NegativeControls controls;
    final MemberCatalog catalog;
    final String rulesDigest;
    final long P, L, H, O, tolE, tolL, maxMissing;
    final double maxBadShare;
    final boolean propagate;
    private final Map<String, MemberCatalog.Resolved> byDeviceMetric = new HashMap<>();
    private final Map<String, List<String>> equipmentByStation = new HashMap<>();
    private final List<String> stations;

    public Stage1(RuleSet rules, NegativeControls controls) {
        if (!"occupancy-join".equals(rules.attribution().mode())) throw new IllegalArgumentException("Stage1 decides occupancy-join; the rule is " + rules.attribution().mode());
        this.rules = rules;
        this.controls = Objects.requireNonNull(controls);
        this.catalog = rules.members();
        this.rulesDigest = rules.rulesDigest();
        var a = rules.attribution();
        this.P = a.expectedPeriodMs(); this.L = a.allowedLatenessMs(); this.H = a.horizonMs(); this.O = a.outOfOrdernessMs();
        this.tolE = a.enterToleranceMs(); this.tolL = a.leaveToleranceMs(); this.maxMissing = a.maxMissingPeriods(); this.maxBadShare = a.maxBadShare();
        this.propagate = "propagate".equals(a.uncertainInput());
        for (MemberCatalog.Resolved r : catalog.byMappingId().values()) {
            byDeviceMetric.put(r.equipmentPath() + "\u0000" + r.metric(), r);
            List<String> eq = equipmentByStation.computeIfAbsent(r.station(), k -> new ArrayList<>());
            if (!eq.contains(r.equipmentPath())) eq.add(r.equipmentPath());
        }
        equipmentByStation.values().forEach(Collections::sort);
        List<String> st = new ArrayList<>(a.stations());
        Collections.sort(st);
        this.stations = List.copyOf(st);
    }

    MemberCatalog.Resolved member(String device, String metric) { return byDeviceMetric.get(device + "\u0000" + metric); }
    List<String> equipmentOf(String station) { return equipmentByStation.getOrDefault(station, List.of()); }

    /** A fresh state for one station's writer; its partition is the station's index in the sorted {@code stations[]}. */
    public Stage1State newState(long generation, String station) {
        int p = stations.indexOf(station);
        if (p < 0) throw new IllegalArgumentException("not a station of the rule: " + station);
        return new Stage1State(generation, station, p);
    }

    /** {@code watermark}: admissionW for {@link Events.Admit} (null before the first progress), W1 for timers and the cut close. */
    public Result decide(Stage1State s, Events.Event e, Long watermark) {
        List<Object> out = new ArrayList<>();
        if (e instanceof Events.Admit a) admit(s, a.record(), watermark, out);
        else if (e instanceof Events.Timer t) timer(s, t, Objects.requireNonNull(watermark, "W1"), out);
        else cutClose(s, (Events.CutClose) e, out);
        return new Result(s, out);
    }

    /** The row with the adapter's pins and engine event time in its envelope. */
    public static Row stamp(Row r, Pins pins, Long eventTimeAt) {
        Envelope e = r.envelope();
        return new Row(r.topic(), r.logicalKey(), r.body(), r.bodyDigest(),
            new Envelope(e.evaluationGeneration(), e.revision(), e.supersedesDigest(), e.cause(), eventTimeAt, e.trigger(), e.basisRevisions(), pins));
    }

    /** A canonical text of one output, for comparisons (a row without its adapter-stamped pins). */
    public static String describe(Object o) {
        ObjectNode n = Json.object();
        if (o instanceof Row r) {
            n.put("row", r.topic());
            n.set("logicalKey", r.logicalKey());
            n.set("body", r.body());
            n.put("bodyDigest", r.bodyDigest());
            Json.putSafe(n, "revision", r.envelope().revision());
            n.put("supersedesDigest", r.envelope().supersedesDigest());
            n.set("cause", r.envelope().cause());
            Envelope.Trigger t = r.envelope().trigger();
            if (t == null) n.putNull("trigger"); else n.set("trigger", Stage1State.origin(new Inputs.Origin(t.topic(), t.partition(), t.offset())));
        } else if (o instanceof Disposition d) n.set("disposition", d.toJson());
        else if (o instanceof Progress p) n.set("progress", p.toJson());
        else if (o instanceof QuarantineRecord q) n.set("quarantine", q.toJson());
        else throw new IllegalArgumentException("not a stage-1 output: " + o);
        return Canonical.utf8(Canonical.bytes(n));
    }

    // ===================================================================================================== emission context

    private final class Ctx {
        final Stage1State s;
        final List<Object> out;
        final ObjectNode cause;
        final Envelope.Trigger trigger;
        final Long eventTimeAt;
        Ctx(Stage1State s, List<Object> out, ObjectNode cause, Envelope.Trigger trigger, Long eventTimeAt) {
            this.s = s; this.out = out; this.cause = cause; this.trigger = trigger; this.eventTimeAt = eventTimeAt;
        }
        void row(String topic, JsonNode key, ObjectNode body, Revisions chain) {
            String digest = Canonical.digest(body);
            chain.next(s.generation, key, digest).ifPresent(em -> out.add(new Row(topic, key, body, digest,
                new Envelope(s.generation, em.revision(), em.supersedesDigest(), cause, eventTimeAt, trigger, Json.JSON.createArrayNode(), null))));
        }
        void disposition(Inputs.Origin origin, String part, String inputKind, JsonNode identity, String payloadDigest, String digestRule,
                         Disposition.Reason reason, ObjectNode extraRefs) {
            Disposition.Key key = new Disposition.Key(s.generation, origin.topic(), origin.partition(), origin.offset(), part);
            ObjectNode refs = Json.object();
            if ("raw-bytes".equals(digestRule)) refs.put("digestRule", "raw-bytes");
            if (extraRefs != null) refs.setAll(extraRefs);
            Disposition.Body body = new Disposition.Body(inputKind, identity, payloadDigest, reason.kind(), reason, refs);
            s.dispositions.next(s.generation, key.toJson(), body.digest()).ifPresent(em -> out.add(new Disposition(key, body, em.revision())));
        }
        ObjectNode quarantine(Inputs.Origin origin, String raw, Disposition.Reason reason, boolean already) {
            if (!already && raw != null) out.add(new QuarantineRecord(s.generation, origin, reason.wire(), Base64.getDecoder().decode(raw)));
            ObjectNode refs = Json.object();
            refs.set("quarantine", QuarantineRecord.ref(origin));
            return refs;
        }
    }

    // ===================================================================================================== admission

    private void admit(Stage1State s, Inputs.InputRecord r, Long admissionW, List<Object> out) {
        String topic = r.origin().topic();
        ObjectNode cause = Json.object().put("kind", "input").put("inputKind", Events.inputKind(topic));
        cause.set("identity", Events.identity(r));
        Ctx c = new Ctx(s, out, cause, new Envelope.Trigger(topic, r.origin().partition(), r.origin().offset()), null);
        switch (topic) {
            case InputParser.OCCUPANCY: occupancy(c, r, admissionW); break;
            case InputParser.SESSIONS: session(c, r); break;
            case InputParser.MESSAGES: message(c, r, admissionW); break;
            default: throw new IllegalArgumentException("not a stage-1 input topic: " + topic);
        }
    }

    private static String raw(Inputs.InputRecord r) { return Base64.getEncoder().encodeToString(r.rawValue()); }

    /** S2-S4 for one occupancy delivery. */
    private void occupancy(Ctx c, Inputs.InputRecord r, Long aw) {
        Stage1State s = c.s;
        Inputs.OccupancyEvent o = (Inputs.OccupancyEvent) r.parsed();
        ObjectNode content = ((ObjectNode) r.value()).deepCopy();
        content.remove("recordTime"); content.remove("errorDeclaration"); content.remove("bridgeReceivedAt");
        JsonNode decl = o.errorDeclaration();
        String part = decl == null ? Disposition.Part.EVENT : Disposition.Part.ERROR_DECLARATION;
        ObjectNode identity = Ids.occupancyId(o);
        String idKey = VisitClaim.text(identity), kind = "occupancy";
        String digest = r.payloadDigest().sha256(), rule = r.payloadDigest().rule();

        List<VisitClaim> before = VisitClaim.derive(s);
        String refArrival = o.arriving() ? o.eventId() : o.arrivalEventId();
        VisitClaim ref = null;
        for (VisitClaim v : before) if (v.source.equals(o.source()) && v.arrivalEventId.equals(refArrival)) ref = v;

        // S3 admission
        if (aw != null) {
            boolean expired = o.arriving() && decl == null ? aw >= o.eventTime() + H + L
                : ref != null ? aw >= ref.onlineExpiresAt(H, L) : aw >= o.eventTime() + H + L;
            if (expired) {
                c.disposition(r.origin(), part, kind, identity, digest, rule, decl != null ? Disposition.Reason.CORRECTION_BEYOND_HORIZON : Disposition.Reason.BEYOND_HORIZON, null);
                return;
            }
        }
        if (ref != null && !ref.online()) {   // an input for a visit whose online state expired never revives it
            c.disposition(r.origin(), part, kind, identity, digest, rule, decl != null ? Disposition.Reason.CORRECTION_BEYOND_HORIZON : Disposition.Reason.BEYOND_HORIZON, null);
            return;
        }

        Stage1State.OccDelivery nd = new Stage1State.OccDelivery(r.origin(), content, decl, digest, rule, raw(r));
        Stage1State.Occupancy occ = s.deliveries.get(idKey);
        String ct = VisitClaim.text(content), dt = decl == null ? null : VisitClaim.text(decl);
        if (occ != null) {
            boolean differs = false;
            for (Stage1State.OccDelivery d : occ.list) if (!VisitClaim.text(d.content).equals(ct)) differs = true;
            if (differs) {   // (a) identity conflict: every variant
                occ.list.add(nd);
                for (Stage1State.OccDelivery d : occ.list) {
                    ObjectNode refs = c.quarantine(d.origin, d.raw, Disposition.Reason.IDENTITY_CONFLICT, d.quarantined);
                    d.quarantined = true;
                    c.disposition(d.origin, d.part(), kind, identity, d.payloadDigest, d.digestRule, Disposition.Reason.IDENTITY_CONFLICT, refs);
                }
                recompute(c, s.lastW1, null);
                return;
            }
            for (Stage1State.OccDelivery d : occ.list) {
                String other = d.declaration == null ? null : VisitClaim.text(d.declaration);
                if (Objects.equals(dt, other)) {   // (b) identical redelivery
                    c.disposition(r.origin(), part, kind, identity, digest, rule, Disposition.Reason.IDENTICAL_REDELIVERY, null);
                    return;
                }
            }
            boolean otherDeclaration = false;
            for (Stage1State.OccDelivery d : occ.list) if (d.declaration != null) otherDeclaration = true;
            if (dt != null && otherDeclaration) {   // (d) two different declarations: both correction-conflict
                occ.list.add(nd);
                for (Stage1State.OccDelivery d : occ.list) {
                    if (d.declaration == null) continue;
                    ObjectNode refs = c.quarantine(d.origin, d.raw, Disposition.Reason.CORRECTION_CONFLICT, d.quarantined);
                    d.quarantined = true;
                    c.disposition(d.origin, d.part(), kind, identity, d.payloadDigest, d.digestRule, Disposition.Reason.CORRECTION_CONFLICT, refs);
                }
                recompute(c, s.lastW1, null);
                return;
            }
        } else {
            occ = new Stage1State.Occupancy(o.source(), o.eventId());
            s.deliveries.put(idKey, occ);
        }
        occ.list.add(nd);   // (c) or a first delivery: accepted
        boolean late = ref != null && ref.leave != null && aw != null && aw >= ref.leave + L;
        c.disposition(r.origin(), part, kind, identity, digest, rule, late ? Disposition.Reason.LATE_AFTER_SETTLEMENT : Disposition.Reason.ACCEPTED, null);
        recompute(c, s.lastW1, null);
    }

    /** S2 for one session event; S8 is read by every recomputation. */
    private void session(Ctx c, Inputs.InputRecord r) {
        Stage1State s = c.s;
        Inputs.SessionEvent ev = (Inputs.SessionEvent) r.parsed();
        ObjectNode identity = Ids.sessionEventId(ev);
        String key = VisitClaim.text(identity), content = VisitClaim.text(ev.payload());
        String digest = r.payloadDigest().sha256(), rule = r.payloadDigest().rule();
        Stage1State.Session x = s.sessions.get(key);
        Stage1State.SessionDelivery nd = new Stage1State.SessionDelivery(r.origin(), ev.payload(), content, digest, rule, raw(r));
        if (x == null) {
            x = new Stage1State.Session(identity, ev.device(), Sessions.sessionKey(ev.edge(), ev.device(), ev.birthPublishTs(), ev.bdSeq()), ev.kind());
            s.sessions.put(key, x);
            x.list.add(nd);
            c.disposition(r.origin(), Disposition.Part.EVENT, "session", identity, digest, rule, Disposition.Reason.ACCEPTED, null);
        } else {
            for (Stage1State.SessionDelivery d : x.list) if (d.content().equals(content)) {
                c.disposition(r.origin(), Disposition.Part.EVENT, "session", identity, digest, rule, Disposition.Reason.IDENTICAL_REDELIVERY, null);
                return;
            }
            x.list.add(nd);
            for (Stage1State.SessionDelivery d : x.list) {
                String ok = Stage1State.originKey(d.origin());
                ObjectNode refs = c.quarantine(d.origin(), d.raw(), Disposition.Reason.IDENTITY_CONFLICT, s.quarantined.contains(ok));
                s.quarantined.add(ok);
                c.disposition(d.origin(), Disposition.Part.EVENT, "session", identity, d.payloadDigest(), d.digestRule(), Disposition.Reason.IDENTITY_CONFLICT, refs);
            }
        }
        recompute(c, s.lastW1, null);
    }

    static String valueKey(JsonNode v) {
        if (v.isBoolean()) return "b:" + v.booleanValue();
        if (v.isTextual()) return "s:" + v.textValue();
        double d = v.doubleValue();
        return "d:" + Double.toString(d == 0.0 ? 0.0 : d);
    }
    private static String variantKey(Stage1State.Sample x) { return valueKey(x.value) + "|" + x.status; }
    private static String measurementKey(String device, String metric, long t) { return device + "\u0000" + metric + "\u0000" + t; }
    private boolean acceptedQuality(Long status) { return StatusCodes.good(status) || (StatusCodes.uncertain(status) && propagate); }

    /** S2, S3, S9 admission of one process message: one disposition per member ({@code part member:<metric>}). */
    private void message(Ctx c, Inputs.InputRecord r, Long aw) {
        Stage1State s = c.s;
        Inputs.ProcessMessage m = (Inputs.ProcessMessage) r.parsed();
        String digest = r.payloadDigest().sha256(), rule = r.payloadDigest().rule(), kind = "message";
        if (Sessions.view(s, s.lastW1).definitionChanged().contains(Sessions.sessionKey(m.edge(), m.device(), m.birthPublishTs(), m.bdSeq()))) {
            c.disposition(r.origin(), Disposition.Part.MESSAGE, kind, Ids.messageId(m), digest, rule, Disposition.Reason.DEFINITION_MISMATCH, null);
            return;
        }
        JsonNode active = TextNode.valueOf("unknown");
        Optional<MemberCatalog.Resolved> act = catalog.activityOf(m.device());
        if (act.isPresent()) for (Inputs.Member x : m.members())
            if (x.metric().equals(act.get().metric()) && x.value().isBoolean() && acceptedQuality(x.uaStatusCode())) active = BooleanNode.valueOf(x.value().booleanValue());
        for (Inputs.MemberRejection x : m.rejectedMembers()) {
            ObjectNode refs = Json.object();
            if (x.check() != null) refs.put("check", x.check());
            c.disposition(r.origin(), Disposition.Part.member(x.metric()), kind, Ids.sampleId(m, x.metric()), digest, rule, Disposition.Reason.SCHEMA_INVALID, refs);
        }
        String originKey = Stage1State.originKey(r.origin());
        Set<String> touched = new TreeSet<>();
        boolean full = false;
        for (Inputs.Member x : m.members()) {
            ObjectNode sampleId = Ids.sampleId(m, x.metric());
            String key = VisitClaim.text(sampleId), part = Disposition.Part.member(x.metric());
            String memberKey = valueKey(x.value()) + "|" + x.uaStatusCode() + "|" + x.sourceTs() + "|" + x.serverTs();
            Stage1State.Sample old = s.samples.get(key);
            if (old != null) {
                if (old.memberKey.equals(memberKey)) { c.disposition(r.origin(), part, kind, sampleId, digest, rule, Disposition.Reason.IDENTICAL_REDELIVERY, null); continue; }
                old.idConflict = true;   // one sampleId, two contents: both identity-conflict, the sample withdrawn
                s.messages.putIfAbsent(originKey, raw(r));
                for (Inputs.Origin o : List.of(old.origin, r.origin())) {
                    String ok = Stage1State.originKey(o);
                    ObjectNode refs = c.quarantine(o, s.messages.get(ok), Disposition.Reason.IDENTITY_CONFLICT, s.quarantined.contains(ok));
                    s.quarantined.add(ok);
                    boolean mine = o.equals(r.origin());
                    c.disposition(o, part, kind, sampleId, mine ? digest : old.payloadDigest, mine ? rule : old.digestRule, Disposition.Reason.IDENTITY_CONFLICT, refs);
                }
                full = true;
                continue;
            }
            boolean replay = x.sourceTs() != null && aw != null && x.sourceTs() < aw - H;
            Stage1State.Sample smp = new Stage1State.Sample(sampleId, m.device(), x.metric(), memberKey, x.sourceTs(), x.uaStatusCode(), x.value(), active,
                r.origin(), digest, rule, replay);
            s.samples.put(key, smp);
            s.messages.putIfAbsent(originKey, raw(r));
            touched.add(key);
            if (replay) { c.disposition(r.origin(), part, kind, sampleId, digest, rule, Disposition.Reason.BEYOND_HORIZON, null); continue; }
            if (x.sourceTs() != null) {
                Stage1State.Measurement meas = s.measurements.computeIfAbsent(measurementKey(m.device(), x.metric(), x.sourceTs()),
                    k -> new Stage1State.Measurement(m.device(), x.metric(), x.sourceTs()));
                Set<String> variants = variants(s, meas);
                meas.sampleKeys.add(key);
                if (!variants.isEmpty() && !(variants.size() == 1 && variants.contains(variantKey(smp)))) {   // measurement conflict
                    for (String k : meas.sampleKeys) {
                        Stage1State.Sample y = s.samples.get(k);
                        if (y.idConflict) continue;
                        String ok = Stage1State.originKey(y.origin);
                        ObjectNode refs = c.quarantine(y.origin, s.messages.get(ok), Disposition.Reason.MEASUREMENT_CONFLICT, s.quarantined.contains(ok));
                        s.quarantined.add(ok);
                        c.disposition(y.origin, Disposition.Part.member(y.metric), kind, y.sampleId, y.payloadDigest, y.digestRule, Disposition.Reason.MEASUREMENT_CONFLICT, refs);
                    }
                    full = true;
                    continue;
                }
            }
            c.disposition(r.origin(), part, kind, sampleId, digest, rule, Disposition.Reason.ACCEPTED, null);
        }
        if (full) recompute(c, s.lastW1, null);
        else if (!touched.isEmpty()) recompute(c, s.lastW1, touched);
    }

    private static Set<String> variants(Stage1State s, Stage1State.Measurement meas) {
        Set<String> v = new TreeSet<>();
        for (String k : meas.sampleKeys) { Stage1State.Sample y = s.samples.get(k); if (!y.idConflict) v.add(variantKey(y)); }
        return v;
    }

    // ===================================================================================================== timers and cuts

    private void timer(Stage1State s, Events.Timer t, long w, List<Object> out) {
        if (s.lastW1 == null || w > s.lastW1) s.lastW1 = w;
        if (!s.deadlines.remove(t.deadline(), t.key(), t.purpose())) return;   // cancelled or replaced meanwhile
        ObjectNode cause = Json.putSafe(Json.object().put("kind", "timer"), "deadline", t.deadline());
        Ctx c = new Ctx(s, out, cause, null, w);
        if (DeadlineIndex.EXPIRY.equals(t.purpose())) expire(s, t.key());
        recompute(c, w, null);
    }

    /** S3: the expiry timer drops a visit's online state; its rows stay, and it stays a window candidate as a frozen summary. */
    private void expire(Stage1State s, JsonNode visitRef) {
        String key = VisitClaim.keyOf(visitRef);
        VisitClaim v = null;
        for (VisitClaim x : VisitClaim.derive(s)) if (x.key.equals(key) && x.online()) v = x;
        if (v == null) return;
        s.expired.put(key, new Stage1State.Expired(v.visitRef, v.enter, v.leave, v.leave == null ? v.attributionEnd : null, v.retracted(), v.onlineExpiresAt(H, L)));
        String arrivalId = v.arrivalEventId, source = v.source;
        s.deliveries.values().removeIf(o -> o.source.equals(source) && (o.eventId.equals(arrivalId)
            || o.list.stream().allMatch(d -> !d.arriving() && arrivalId.equals(d.content.path("arrivalEventId").asText()))));
        s.deadlines.cancelAll(visitRef);
        dropSamples(s, x -> {
            Stage1State.Measurement m = x.sourceTs == null ? null : s.measurements.get(measurementKey(x.device, x.metric, x.sourceTs));
            return (x.row != null && visitRef.equals(x.row.get("attributedTo"))) || (m != null && key.equals(m.coverageVisit));
        });
    }

    private static void dropSamples(Stage1State s, java.util.function.Predicate<Stage1State.Sample> drop) {
        List<String> gone = new ArrayList<>();
        s.samples.forEach((k, x) -> { if (drop.test(x)) gone.add(k); });
        for (String k : gone) {
            Stage1State.Sample x = s.samples.remove(k);
            if (x.sourceTs == null) continue;
            String mk = measurementKey(x.device, x.metric, x.sourceTs);
            Stage1State.Measurement m = s.measurements.get(mk);
            if (m != null) { m.sampleKeys.remove(k); if (m.sampleKeys.isEmpty()) s.measurements.remove(mk); }
        }
        if (gone.isEmpty()) return;
        Set<String> live = new HashSet<>();
        s.samples.values().forEach(x -> live.add(Stage1State.originKey(x.origin)));
        s.messages.keySet().removeIf(k -> !live.contains(k));
    }

    private void cutClose(Stage1State s, Events.CutClose cc, List<Object> out) {
        Long admissionBefore = s.admissionW;
        long w = cc.w1();
        s.lastW1 = w;
        ObjectNode cause = Json.putSafe(Json.object().put("kind", "timer"), "deadline", w);
        recompute(new Ctx(s, out, cause, null, w), w, null);
        // unattributed samples stay until no admissible arrival can claim them (S3: an arrival is admitted while admissionW < enter + H + L)
        dropSamples(s, x -> {
            Stage1State.Measurement m = x.sourceTs == null ? null : s.measurements.get(measurementKey(x.device, x.metric, x.sourceTs));
            boolean claimed = (x.row != null && !x.row.get("attributedTo").isNull()) || (m != null && m.coverageVisit != null);
            return !claimed && (x.sourceTs == null || x.sourceTs + tolE + H + L <= w);
        });
        List<Progress.BarrierRef> refs = new ArrayList<>();
        for (JsonNode b : cc.barrierRefs())
            refs.add(new Progress.BarrierRef(Json.safeLong(b.get("cutId"), "barrierRefs.cutId", Json.NON_NEGATIVE),
                new Progress.Origin(Json.text(b.path("origin"), "topic"), (int) Json.safeLong(b.path("origin").get("partition"), "barrierRefs.origin.partition", 0, Integer.MAX_VALUE)),
                Json.safeLong(b.get("throughOffset"), "barrierRefs.throughOffset", Json.THROUGH)));
        Progress.InitEvidence init = s.pendingInit;
        s.pendingInit = null;
        out.add(new Progress.Stage1(s.generation, s.station, cc.cutId(), 0, w, admissionBefore, refs, 0, null, List.of(), init));
        out.add(new Progress.Ledger(s.generation, 1, s.partition, s.station, cc.cutId(), 0, refs, List.of(), 0, null, init));
        s.admissionW = w;
        s.progressCount++;
    }

    // ===================================================================================================== recomputation

    /** Recomputes sample rows and work record bodies and emits those whose digest changed. {@code scope}: the sample keys a message added
     *  (only they and the visits whose J contains them can change), or null for everything (then the deadlines are re-registered). */
    private void recompute(Ctx c, Long w, Set<String> scope) {
        Stage1State s = c.s;
        List<VisitClaim> visits = VisitClaim.derive(s);
        Attribution att = new Attribution(visits, tolE, tolL);
        Sessions.View view = Sessions.view(s, w);
        Collection<String> keys = scope == null ? new ArrayList<>(s.samples.keySet()) : scope;
        Set<String> affected = scope == null ? null : new HashSet<>();
        for (String k : keys) {
            Stage1State.Sample x = s.samples.get(k);
            SampleRow.Outcome o = classify(s, x, att, view);
            if (x.sourceTs != null && !x.replay) {
                Stage1State.Measurement m = s.measurements.get(measurementKey(x.device, x.metric, x.sourceTs));
                if (m != null && !x.idConflict) m.coverageVisit = o.coverageVisit();
                if (affected != null) {
                    if (!view.unknown(x.device).isEmpty()) affected = null;
                    else for (VisitClaim v : visits) if (v.online() && x.sourceTs >= v.enter && x.sourceTs < WorkRecord.jEnd(v, w, H)) affected.add(v.key);
                }
            }
            ObjectNode row = SampleRow.body(x.sourceTs, o, x.active);
            if (!row.equals(x.row)) { c.row(SAMPLES, x.sampleId, row, s.sampleRows); x.row = row; }
        }
        for (VisitClaim v : visits) {
            if (!v.online() || (affected != null && !affected.contains(v.key))) continue;
            c.row(WORK_RECORDS, v.visitRef, WorkRecord.build(this, v, w, view, observations(s, v.key)), s.workRecords);
        }
        if (scope == null) for (VisitClaim v : visits) if (v.online()) register(s, v, w);
    }

    private void register(Stage1State s, VisitClaim v, Long w) {
        if (v.leave != null && (w == null || v.leave + L > w)) s.deadlines.register(v.leave + L, v.visitRef, DeadlineIndex.SETTLE);
        else s.deadlines.cancel(v.visitRef, DeadlineIndex.SETTLE);
        if (v.leave == null && (w == null || v.enter + H > w)) s.deadlines.register(v.enter + H, v.visitRef, DeadlineIndex.MISSING_DEPARTURE);
        else s.deadlines.cancel(v.visitRef, DeadlineIndex.MISSING_DEPARTURE);
        s.deadlines.register(v.onlineExpiresAt(H, L), v.visitRef, DeadlineIndex.EXPIRY);
    }

    /** The coverage observations of one visit: usable measurements (one variant, finite) whose quality-blind candidate is the visit. */
    private List<WorkRecord.Observation> observations(Stage1State s, String visitKey) {
        List<WorkRecord.Observation> l = new ArrayList<>();
        for (Stage1State.Measurement m : s.measurements.values()) {
            if (!visitKey.equals(m.coverageVisit)) continue;
            Stage1State.Sample x = null;
            for (String k : m.sampleKeys) { Stage1State.Sample y = s.samples.get(k); if (!y.idConflict) { x = y; break; } }
            if (x == null) continue;
            l.add(new WorkRecord.Observation(m.device, m.metric, m.sourceTs, x.value, x.status, StatusCodes.bad(x.status), acceptedQuality(x.status)));
        }
        return l;
    }

    /** S9 in order: no sourceTs, conflict, non-finite, quality, session, then the window rule. */
    private SampleRow.Outcome classify(Stage1State s, Stage1State.Sample x, Attribution att, Sessions.View view) {
        if (x.replay) return SampleRow.Outcome.of(SampleRow.REPLAY_REQUIRED, "beyond-horizon");
        if (x.sourceTs == null) return SampleRow.Outcome.of(SampleRow.UNATTRIBUTED, "no-source-timestamp");
        if (x.idConflict) return SampleRow.Outcome.of(SampleRow.QUARANTINED, "measurement-conflict");
        Stage1State.Measurement m = s.measurements.get(measurementKey(x.device, x.metric, x.sourceTs));
        if (m != null && variants(s, m).size() > 1) return SampleRow.Outcome.of(SampleRow.QUARANTINED, "measurement-conflict");
        if (!x.finite()) return SampleRow.Outcome.of(SampleRow.UNATTRIBUTED, "non-finite");
        long t = x.sourceTs;
        Attribution.Candidate cand = att.candidate(t);
        String blind = cand.kind() == Attribution.Kind.ATTRIBUTED ? cand.visit().key : null;
        if (StatusCodes.bad(x.status) || (StatusCodes.uncertain(x.status) && !propagate)) return new SampleRow.Outcome(SampleRow.UNATTRIBUTED, "bad-quality", null, blind);
        if (Sessions.inside(view.down(x.device), t)) return SampleRow.Outcome.of(SampleRow.UNATTRIBUTED, "outside-session");
        if (Sessions.inside(view.unknown(x.device), t)) return SampleRow.Outcome.of(SampleRow.UNATTRIBUTED, "session-ambiguous");
        switch (cand.kind()) {
            case ATTRIBUTED: return new SampleRow.Outcome(SampleRow.ATTRIBUTED, null, cand.visit().visitRef.deepCopy(), blind);
            case AMBIGUOUS: return SampleRow.Outcome.of(SampleRow.UNATTRIBUTED, "ambiguous");
            default: return SampleRow.Outcome.of(SampleRow.UNATTRIBUTED, "no-occupancy");
        }
    }
}
