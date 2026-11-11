package yggdrasil.forseti.core.stage2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.NegativeControls;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.Ids;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.*;
import yggdrasil.forseti.core.rules.ImpactRule;
import yggdrasil.forseti.core.rules.MemberCatalog;
import yggdrasil.forseti.core.rules.RuleSet;
import yggdrasil.forseti.core.stage1.Stage1;

/** The stage-2 decision function (rule sheet T1-T10). {@link #decide} reads only its arguments, the rule set and the negative controls it
 *  was constructed with: no clock, random, file, environment or static mutable state. It takes the state object, updates it and returns
 *  it with the outputs: {@link Row} ({@code forseti.affected} assessments and {@code forseti.affected-sets} sets; the envelope's
 *  {@code pins} are the adapter's, see {@link Stage1#stamp}), {@link Delete} (always of a {@code forseti.affected} key: a set row is
 *  never deleted), {@link Disposition} ({@code forseti.dispositions-stage2}), {@link Progress.Stage2} and {@link Progress.Ledger}.
 *  <p>Judgement: an incident delivery and a basis change re-judge at once every incident of the station (T10: both the old and the new
 *  relationship are seen because the row set is rebuilt from the current bases); a sample row only updates the sample-derived index,
 *  whose coverage is re-evaluated at the next judgement or at the cut close, which re-evaluates every set as W2 advances (T9). Rows are
 *  emitted only when their body digest changes. */
public final class Stage2 {
    public static final String WORK_RECORDS = Stage1.WORK_RECORDS, SAMPLES = Stage1.SAMPLES;
    public static final String AFFECTED = "forseti.affected", AFFECTED_SETS = "forseti.affected-sets", DISPOSITIONS = "forseti.dispositions-stage2";
    /** Deadline purposes: incident settlement {@code suspectTo + L} (key: the set key); basis retention {@code leave|enter + workRecordsMs}
     *  (key: the visitRef). */
    public static final String SETTLE = "incident-settle", RETENTION = "basis-retention";

    public record Result(Stage2State state, List<Object> outputs) {}

    final RuleSet rules;
    final NegativeControls controls;
    final ImpactRule impact;
    final MemberCatalog catalog;
    final String rulesDigest, overlap, match, onWithdrawn, onBasisRetracted;
    final long P, L, workRecordsMs;
    private final List<String> stations;

    public Stage2(RuleSet rules, NegativeControls controls) {
        this.rules = rules;
        this.controls = Objects.requireNonNull(controls);
        this.impact = rules.impact();
        this.catalog = rules.members();
        this.rulesDigest = rules.rulesDigest();
        this.overlap = impact.overlap(); this.match = impact.match();
        this.onWithdrawn = impact.onWithdrawn(); this.onBasisRetracted = impact.onBasisRetracted();
        this.P = rules.attribution().expectedPeriodMs(); this.L = rules.attribution().allowedLatenessMs(); this.workRecordsMs = impact.workRecordsMs();
        List<String> st = new ArrayList<>(rules.attribution().stations());
        Collections.sort(st);
        this.stations = List.copyOf(st);
    }

    /** A fresh state for one station's stage-2 writer; its partition is the station's index in the sorted {@code stations[]}. */
    public Stage2State newState(long generation, String station) {
        int p = stations.indexOf(station);
        if (p < 0) throw new IllegalArgumentException("not a station of the rule: " + station);
        return new Stage2State(generation, station, p);
    }

    /** {@code w2}: the station's W2 ({@link ProgressTracker#w2}); null before the first stage-1 progress of both evidence topics. A
     *  timer needs it; a cut close takes its own. */
    public Result decide(Stage2State s, Events2.Event e, Long w2) {
        List<Object> out = new ArrayList<>();
        if (e instanceof Events2.Basis b) basis(s, b, w2, out);
        else if (e instanceof Events2.Sample x) sample(s, x.row());
        else if (e instanceof Events2.Incident i) incident(s, i.record(), w2, out);
        else if (e instanceof Events2.Timer t) timer(s, t, Objects.requireNonNull(w2, "W2"), out);
        else cutClose(s, (Events2.CutClose) e, out);
        return new Result(s, out);
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
            n.set("basisRevisions", r.envelope().basisRevisions());
            Envelope.Trigger t = r.envelope().trigger();
            if (t == null) n.putNull("trigger"); else n.set("trigger", Stage2State.origin(new Inputs.Origin(t.topic(), t.partition(), t.offset())));
        } else if (o instanceof Delete d) n.set("delete", d.toJson());
        else if (o instanceof Disposition d) n.set("disposition", d.toJson());
        else if (o instanceof Progress p) n.set("progress", p.toJson());
        else throw new IllegalArgumentException("not a stage-2 output: " + o);
        return Canonical.utf8(Canonical.bytes(n));
    }

    // ===================================================================================================== emission context

    private static String text(JsonNode key) { return Canonical.utf8(Canonical.bytes(key)); }

    private final class Ctx {
        final Stage2State s;
        final List<Object> out;
        final ObjectNode cause;
        final Envelope.Trigger trigger;
        final Long w2;
        Ctx(Stage2State s, List<Object> out, ObjectNode cause, Envelope.Trigger trigger, Long w2) { this.s = s; this.out = out; this.cause = cause; this.trigger = trigger; this.w2 = w2; }
        /** Emits the row when its digest changed; returns the digest. */
        String row(String topic, ObjectNode key, ObjectNode body, Revisions chain, ArrayNode basisRevisions) {
            String digest = Canonical.digest(body);
            chain.next(s.generation, key, digest).ifPresent(em -> out.add(new Row(topic, key, body, digest,
                new Envelope(s.generation, em.revision(), em.supersedesDigest(), cause, w2, trigger, basisRevisions, null))));
            return digest;
        }
        void delete(ObjectNode key) {
            Revisions.Emission em = s.assessments.delete(s.generation, key);
            out.add(new Delete(s.generation, key, em.revision(), em.supersedesDigest()));
        }
        void disposition(Inputs.Origin origin, Disposition.Body body) {
            Disposition.Key key = new Disposition.Key(s.generation, origin.topic(), origin.partition(), origin.offset(), Disposition.Part.EVENT);
            s.dispositions.next(s.generation, key.toJson(), body.digest()).ifPresent(em -> out.add(new Disposition(key, body, em.revision())));
        }
        void disposition(Stage2State.Variant v, JsonNode identity, Disposition.Reason reason, String check) {
            ObjectNode refs = Json.object();
            if (check != null) refs.put("check", check);
            if ("raw-bytes".equals(v.digestRule())) refs.put("digestRule", "raw-bytes");
            disposition(v.origin(), new Disposition.Body(Incidents.INPUT_KIND, identity, v.payloadDigest(), reason.kind(), reason, refs));
        }
    }

    private static ObjectNode inputCause(String inputKind, JsonNode identity) {
        ObjectNode c = Json.object().put("kind", "input").put("inputKind", inputKind);
        c.set("identity", identity);
        return c;
    }
    private static Long effective(Stage2State s, Long w2) { return w2 != null ? w2 : s.lastW2; }

    // ===================================================================================================== inputs

    /** T3, T10: the current work record row of a visit (or its delete) replaces the basis; every incident is re-judged. */
    private void basis(Stage2State s, Events2.Basis b, Long w2, List<Object> out) {
        JsonNode visitRef = b.entry() instanceof Row r ? r.logicalKey() : ((Delete) b.entry()).logicalKey();
        String key = text(visitRef);
        if (b.entry() instanceof Row r) {
            s.bases.put(key, new Stage2State.Basis(visitRef.deepCopy(), (ObjectNode) r.body().deepCopy(), r.bodyDigest(), r.envelope().revision()));
            JsonNode in = r.body().path("interval");
            long anchor = in.path("leave").isIntegralNumber() ? in.path("leave").longValue() : in.path("enter").longValue();
            s.deadlines.register(anchor + workRecordsMs, visitRef, RETENTION);
        } else {
            s.bases.remove(key);
            s.deadlines.cancel(visitRef, RETENTION);
        }
        Ctx c = new Ctx(s, out, inputCause("work-record", visitRef), null, effective(s, w2));
        for (String k : new ArrayList<>(s.incidents.keySet())) judge(c, k);
    }

    /** T8 index: a sample row of a member; only rows of an equipment's activity member are kept, every row moves lastObservation. */
    private void sample(Stage2State s, Row r) {
        JsonNode key = r.logicalKey(), body = r.body();
        String device = key.path("device").asText(), metric = key.path("metric").asText();
        if (!body.path("sourceTs").isIntegralNumber()) return;
        long t = body.path("sourceTs").longValue();
        s.lastObservation.merge(device, t, Math::max);
        Optional<MemberCatalog.Resolved> act = catalog.activityOf(device);
        if (act.isEmpty() || !act.get().metric().equals(metric)) return;
        s.activity.computeIfAbsent(device, k -> new TreeMap<>()).put(text(key),
            new SetCoverage.ActivityRow(t, body.path("state").asText(), body.path("reason").isTextual() ? body.path("reason").textValue() : null, body.path("active").deepCopy()));
    }

    /** T1: the stateful checks of one routed incident delivery, in the sheet's order, then judgement of that incident. */
    private void incident(Stage2State s, Inputs.InputRecord r, Long w2, List<Object> out) {
        Ctx c = new Ctx(s, out, inputCause(Incidents.INPUT_KIND, r.parsed() instanceof Inputs.IncidentEvent i0 ? Ids.incidentId(i0) : Json.object()),
            new Envelope.Trigger(r.origin().topic(), r.origin().partition(), r.origin().offset()), effective(s, w2));
        Incidents.Outcome door = Incidents.door(rules, r, null);
        if (door instanceof Incidents.Rejected rej) { c.disposition(r.origin(), rej.body()); return; }
        Inputs.IncidentEvent i = (Inputs.IncidentEvent) r.parsed();
        ObjectNode identity = Ids.incidentId(i);
        Stage2State.Variant nv = new Stage2State.Variant(r.origin(), r.payloadDigest().sha256(), r.payloadDigest().rule(), r.value().deepCopy());
        String key = text(Incidents.setKey(i.source(), i.incidentId()));
        Stage2State.Incident inc = s.incidents.get(key);
        // revisionEventTime against the accepted lower and higher revisions, by revision number (spec 308, 459)
        if (inc != null) for (Map.Entry<Long, List<Stage2State.Variant>> e : inc.revisions.entrySet()) {
            if (e.getKey() == i.revision() || e.getValue().size() != 1) continue;
            long other = e.getValue().get(0).event().revisionEventTime();
            if ((e.getKey() < i.revision() && i.revisionEventTime() < other) || (e.getKey() > i.revision() && i.revisionEventTime() > other)) {
                c.disposition(nv, identity, Disposition.Reason.SCHEMA_INVALID, Disposition.ProfileCheck.REVISION_EVENT_TIME.wire());
                return;
            }
        }
        // identity (source, incidentId, revision)
        List<Stage2State.Variant> stored = inc == null ? null : inc.revisions.get(i.revision());
        if (stored != null) {
            for (Stage2State.Variant v : stored) if (v.payloadDigest().equals(nv.payloadDigest())) {
                c.disposition(nv, identity, Disposition.Reason.IDENTICAL_REDELIVERY, null);
                return;
            }
            stored.add(nv);
            for (Stage2State.Variant v : stored) c.disposition(v, identity, Disposition.Reason.IDENTITY_CONFLICT, null);
            judge(c, key);
            return;
        }
        if (inc != null && inc.stationRef != null && !inc.stationRef.equals(i.stationRef())) {
            c.disposition(nv, identity, Disposition.Reason.STATION_CHANGE, null);
            return;
        }
        if (inc == null) {
            if (!i.stationRef().equals(s.station)) throw new IllegalArgumentException("incident of " + i.stationRef() + " routed to " + s.station);
            inc = new Stage2State.Incident(i.source(), i.incidentId());
            s.incidents.put(key, inc);
        }
        if (inc.stationRef == null) inc.stationRef = i.stationRef();
        Inputs.IncidentEvent current = inc.current();
        inc.revisions.put(i.revision(), new ArrayList<>(List.of(nv)));
        boolean superseded = current != null && i.revision() < current.revision();
        c.disposition(nv, identity, superseded ? Disposition.Reason.SUPERSEDED : Disposition.Reason.ACCEPTED, null);
        judge(c, key);
    }

    // ===================================================================================================== timers and cuts

    private void timer(Stage2State s, Events2.Timer t, long w, List<Object> out) {
        if (s.lastW2 == null || w > s.lastW2) s.lastW2 = w;
        if (!s.deadlines.remove(t.deadline(), t.key(), t.purpose())) return;   // cancelled or replaced meanwhile
        if (RETENTION.equals(t.purpose())) { s.bases.remove(text(t.key())); return; }   // T9: evidence leaves stage-2 state, no re-judgement
        Ctx c = new Ctx(s, out, Json.putSafe(Json.object().put("kind", "timer"), "deadline", t.deadline()), null, w);
        String key = text(t.key());
        if (s.incidents.containsKey(key)) judge(c, key);
    }

    private void cutClose(Stage2State s, Events2.CutClose cc, List<Object> out) {
        long w = cc.w2();
        s.lastW2 = w;
        Ctx c = new Ctx(s, out, Json.putSafe(Json.object().put("kind", "timer"), "deadline", w), null, w);
        for (String k : new ArrayList<>(s.incidents.keySet())) judge(c, k);
        for (TreeMap<String, SetCoverage.ActivityRow> rows : s.activity.values()) rows.values().removeIf(x -> x.sourceTs() + workRecordsMs < w);
        s.activity.values().removeIf(Map::isEmpty);
        List<Progress.BarrierRef> refs = new ArrayList<>();
        for (Progress.IncidentOffset o : cc.incidentOffsets())
            refs.add(new Progress.BarrierRef(o.cutId(), new Progress.Origin(yggdrasil.forseti.core.input.InputParser.INCIDENTS, o.partition()), o.throughOffset()));
        out.add(new Progress.Stage2(s.generation, s.station, cc.cutId(), 0, w, cc.stage1ProgressRefs(), cc.incidentOffsets(), cc.mergedCutIds()));
        out.add(new Progress.Ledger(s.generation, 2, s.partition, s.station, cc.cutId(), 0, refs, cc.mergedCutIds(), 0, null, null));
        s.progressCount++;
    }

    // ===================================================================================================== judgement

    /** T3-T8 for one incident: the row set over every delivered revision, the decision per unit on the current revision, deletes for
     *  units that left the row set, the set row. Without a usable revision (every one in conflict) nothing is judged and the rows stay. */
    private void judge(Ctx c, String key) {
        Stage2State s = c.s;
        Stage2State.Incident inc = s.incidents.get(key);
        Inputs.IncidentEvent cur = inc.current();
        if (cur == null) return;
        List<Inputs.IncidentEvent> delivered = inc.delivered();
        ObjectNode snapshot = Incidents.snapshot(cur);
        Map<String, List<Stage2State.Basis>> byUnit = new TreeMap<>();
        for (Stage2State.Basis b : s.bases.values()) byUnit.computeIfAbsent(b.body().path("unitId").asText(), k -> new ArrayList<>()).add(b);
        TreeMap<String, AffectedSet.Unit> live = s.units.computeIfAbsent(key, k -> new TreeMap<>());
        Set<String> rowSet = new TreeSet<>();
        for (Map.Entry<String, List<Stage2State.Basis>> u : byUnit.entrySet())
            for (Stage2State.Basis b : u.getValue()) for (Inputs.IncidentEvent d : delivered) if (relation(b, d) != Relatedness.Kind.UNRELATED) rowSet.add(u.getKey());
        long gapOverlaps = 0;
        boolean ambiguous = false;
        Set<String> counted = new HashSet<>();
        for (String unit : rowSet) {
            List<DecisionTable.Judged> judged = new ArrayList<>();
            for (Stage2State.Basis b : byUnit.get(unit)) {
                Relatedness.Kind k = relation(b, cur);
                if (k != Relatedness.Kind.UNRELATED) judged.add(new DecisionTable.Judged(b.visitRef(), b.body(), b.bodyDigest(), k));
            }
            judged.sort((x, y) -> Arrays.compareUnsigned(Canonical.bytes(x.visitRef()), Canonical.bytes(y.visitRef())));
            DecisionTable.Decision d = DecisionTable.decide(match, onWithdrawn, onBasisRetracted, cur, judged);
            ArrayNode basisRevisions = Json.JSON.createArrayNode();
            for (DecisionTable.Judged j : judged) {
                ObjectNode e = basisRevisions.addObject();
                e.set("visitRef", j.visitRef());
                Json.putSafe(e, "revision", s.bases.get(text(j.visitRef())).revision());
                boolean gap = j.hasReason("session-gap"), amb = j.hasReason("session-ambiguous");
                if ((gap || amb) && counted.add(text(j.visitRef()))) gapOverlaps++;
                if (amb) ambiguous = true;
            }
            ObjectNode body = Assessment.body(d, snapshot.deepCopy(), judged, rulesDigest);
            String digest = c.row(AFFECTED, Incidents.assessmentKey(inc.source, inc.incidentId, unit), body, s.assessments, basisRevisions);
            live.put(unit, new AffectedSet.Unit(d.status(), d.reason(), digest));
        }
        for (String unit : new ArrayList<>(live.keySet())) if (!rowSet.contains(unit)) {
            c.delete(Incidents.assessmentKey(inc.source, inc.incidentId, unit));
            live.remove(unit);
        }
        List<SetCoverage.Unit> units = new ArrayList<>();
        live.values().forEach(u -> units.add(new SetCoverage.Unit(u.status(), u.reason())));
        List<String> scoped = List.of("station-visit".equals(match) ? cur.stationRef() : cur.equipmentRef());
        Map<String, List<SetCoverage.ActivityRow>> activity = new TreeMap<>();
        for (String d : scoped) activity.put(d, new ArrayList<>(s.activity.getOrDefault(d, new TreeMap<>()).values()));
        ObjectNode coverage = SetCoverage.compute(cur, scoped, new SetCoverage.Evidence(activity, s.lastObservation, s.evidenceRanges), c.w2, P, L, workRecordsMs,
            units, gapOverlaps, ambiguous);
        boolean withdrawn = "withdrawn".equals(cur.status());
        c.row(AFFECTED_SETS, Incidents.setKey(inc.source, inc.incidentId), AffectedSet.body(snapshot, live, withdrawn, coverage), s.sets, Json.JSON.createArrayNode());
        ObjectNode setKey = Incidents.setKey(inc.source, inc.incidentId);
        if (cur.suspectTo() != null && !withdrawn && (c.w2 == null || cur.suspectTo() + L > c.w2)) s.deadlines.register(cur.suspectTo() + L, setKey, SETTLE);
        else s.deadlines.cancel(setKey, SETTLE);
    }

    private Relatedness.Kind relation(Stage2State.Basis b, Inputs.IncidentEvent i) {
        JsonNode in = b.body().path("interval");
        Long leave = in.path("leave").isIntegralNumber() ? in.path("leave").longValue() : null;
        return Relatedness.of(overlap, in.path("enter").longValue(), leave, i.suspectFrom(), i.suspectTo());
    }
}
