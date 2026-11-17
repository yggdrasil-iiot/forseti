package yggdrasil.forseti.core.stage2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Revisions;
import yggdrasil.forseti.core.segment.ChainSegments;
import yggdrasil.forseti.core.segment.Segments;
import yggdrasil.forseti.core.segment.TrackedMap;
import yggdrasil.forseti.core.stage1.DeadlineIndex;

/** The state of one station's stage-2 writer (rule sheet T1-T10). The caller owns it: {@link Stage2#decide} takes it, updates it and
 *  returns it; adapters keep it in their store through {@link #toJson} / {@link #fromJson} (versioned). Every map is sorted by its
 *  canonical key text, so equal states serialize to equal bytes.
 *  <ul>
 *  <li>{@code bases}: the current work record row of every visit of the station's partition, by visitRef (T3), until its retention
 *      timer ({@code leave} or, open, {@code enter}, plus {@code workRecordsMs}) drops it;</li>
 *  <li>{@code activity}: per device, the current sample rows of its activity member ({@code sourceTs, state, reason, active}), by sampleId;
 *      {@code lastObservation}: per device, the largest {@code sourceTs} of any of its sample rows (T8's open range);</li>
 *  <li>{@code incidents} by {@code (source, incidentId)}: the first accepted {@code stationRef} and every stored variant of every valid
 *      revision; a revision with two variants is in conflict and unusable;</li>
 *  <li>{@code units}: the live assessment rows per incident (status, reason, digest), for deletes and the set's {@code units[]};</li>
 *  <li>{@code evidenceRanges}: the verified telemetry range per device the adapter set at initialization ({@link #evidenceRanges});</li>
 *  <li>{@code deadlines}, the three revision chains (assessments, sets, dispositions), {@code lastW2}, {@code progressCount}.</li></ul> */
public final class Stage2State {
    public static final int VERSION = 1;
    /** Segment kinds (see "Segments" below). */
    static final String HEAD = Segments.HEAD, DEADLINES = "deadlines", BASES = "bases", ACTIVITY = "activity", INCIDENTS = "incidents", UNITS = "units",
        REV_ASSESSMENTS = "rev.assessments", REV_SETS = "rev.sets", REV_DISPOSITIONS = "rev.dispositions";

    record Basis(JsonNode visitRef, ObjectNode body, String bodyDigest, long revision) {}
    record Variant(Inputs.Origin origin, String payloadDigest, String digestRule, JsonNode value) {
        Inputs.IncidentEvent event() { return Incidents.parse(value); }
    }
    static final class Incident {
        final String source, incidentId;
        String stationRef;
        final TreeMap<Long, List<Variant>> revisions = new TreeMap<>();
        Incident(String source, String incidentId) { this.source = source; this.incidentId = incidentId; }
        /** Valid, non-conflicting revisions (delivered, whatever their disposition). */
        List<Inputs.IncidentEvent> delivered() {
            List<Inputs.IncidentEvent> l = new ArrayList<>();
            revisions.forEach((n, v) -> { if (v.size() == 1) l.add(v.get(0).event()); });
            return l;
        }
        /** The highest valid, non-conflicting revision; null when none. */
        Inputs.IncidentEvent current() {
            for (Map.Entry<Long, List<Variant>> e : revisions.descendingMap().entrySet()) if (e.getValue().size() == 1) return e.getValue().get(0).event();
            return null;
        }
    }

    final long generation;
    final String station;
    final int partition;
    /** The segments changed since the last {@link #drainChanged} (see "Segments" below). */
    final Segments.Dirty dirty = new Segments.Dirty();
    final TrackedMap<Basis> bases = TrackedMap.bucketed((k, x) -> BASES + "/" + Segments.hashBucket(k), dirty);
    /** The activity rows by {@code device NUL sampleId text} (toJson nests them per device); {@link #activityOf} reads one device's. */
    final TrackedMap<SetCoverage.ActivityRow> activity = TrackedMap.bucketed((k, x) -> ACTIVITY + "/" + Segments.token(deviceOf(k)) + "/" + Segments.timeBucket(x.sourceTs()), dirty);
    final TrackedMap<Long> lastObservation = TrackedMap.in(HEAD, dirty);
    final TrackedMap<Incident> incidents = TrackedMap.bucketed((k, x) -> INCIDENTS + "/" + Segments.hashBucket(k), dirty);
    final TrackedMap<TreeMap<String, AffectedSet.Unit>> units = TrackedMap.bucketed((k, x) -> UNITS + "/" + Segments.hashBucket(k), dirty);
    final TrackedMap<SetCoverage.EvidenceRange> evidenceRanges = TrackedMap.in(HEAD, dirty);
    DeadlineIndex deadlines;
    Revisions assessments, sets, dispositions;
    private final ChainSegments assessmentSegments = new ChainSegments(REV_ASSESSMENTS, Stage2State::incidentKeyBucket, dirty);
    private final ChainSegments setSegments = new ChainSegments(REV_SETS, Stage2State::incidentKeyBucket, dirty);
    private final ChainSegments dispositionSegments = new ChainSegments(REV_DISPOSITIONS, Stage2State::dispositionKeyBucket, dirty);
    Long lastW2;
    long progressCount;

    Stage2State(long generation, String station, int partition) {
        if (generation < 1) throw new IllegalArgumentException("generation >= 1");
        this.generation = generation; this.station = Objects.requireNonNull(station); this.partition = partition;
        bind(new DeadlineIndex(), new Revisions(), new Revisions(), new Revisions());
    }

    private void bind(DeadlineIndex d, Revisions a, Revisions s, Revisions x) {
        deadlines = d;
        deadlines.onChange(t -> dirty.mark(DEADLINES + "/" + Segments.visitBucket(t)));
        assessments = assessmentSegments.bind(a);
        sets = setSegments.bind(s);
        dispositions = dispositionSegments.bind(x);
    }

    /** A head field changed (a scalar; lastObservation and evidenceRanges are TrackedMaps of the head). */
    void touchHead() { dirty.mark(HEAD); }

    static String activityKey(String device, String sampleKey) { return device + "\u0000" + sampleKey; }
    static String deviceOf(String activityKey) { return activityKey.substring(0, activityKey.indexOf('\u0000')); }
    /** One device's activity rows, by sampleId text. */
    Collection<SetCoverage.ActivityRow> activityOf(String device) { return activity.asMap().subMap(device + "\u0000", true, device + "\u0001", false).values(); }

    /** The verified evidence-retention ranges per device (spec 402; in step 3 the recording manifest's {@code evidenceRange}). */
    public void evidenceRanges(Map<String, SetCoverage.EvidenceRange> ranges) { evidenceRanges.clear(); ranges.forEach(evidenceRanges::put); }

    public long generation() { return generation; }
    public String station() { return station; }
    public int partition() { return partition; }
    public DeadlineIndex deadlines() { return deadlines; }
    /** W2 of the last cut close (or fired timer); null before the first. */
    public Long lastW2() { return lastW2; }
    public long progressCount() { return progressCount; }
    public int bases() { return bases.size(); }

    // ===================================================================================================== serialization

    public ObjectNode toJson() {
        ObjectNode n = Json.object().put("stateVersion", VERSION);
        Json.putSafe(n, "generation", generation);
        n.put("station", station).put("partition", partition);
        ObjectNode b = n.putObject("bases");
        bases.forEach((k, x) -> b.set(k, basisJson(x)));
        n.set("activity", activityJson(activity.asMap()));
        n.set("lastObservation", lastObservationJson());
        ObjectNode in = n.putObject("incidents");
        incidents.forEach((k, x) -> in.set(k, incidentJson(x)));
        ObjectNode un = n.putObject("units");
        units.forEach((k, m) -> un.set(k, unitsJson(m)));
        n.set("evidenceRanges", evidenceRangesJson());
        n.set("deadlines", deadlines.toJson());
        ObjectNode rev = n.putObject("revisions");
        rev.set("assessments", assessments.toJson());
        rev.set("sets", sets.toJson());
        rev.set("dispositions", dispositions.toJson());
        if (lastW2 == null) n.putNull("lastW2"); else Json.putSafe(n, "lastW2", lastW2);
        Json.putSafe(n, "progressCount", progressCount);
        return n;
    }

    private static ObjectNode basisJson(Basis x) {
        ObjectNode e = Json.object();
        e.set("visitRef", x.visitRef());
        e.set("body", x.body());
        e.put("bodyDigest", x.bodyDigest());
        Json.putSafe(e, "revision", x.revision());
        return e;
    }
    /** Activity rows by {@code device NUL sampleId} as {@code {device: {sampleId: row}}}. */
    private static ObjectNode activityJson(SortedMap<String, SetCoverage.ActivityRow> rows) {
        ObjectNode a = Json.object();
        rows.forEach((k, r) -> {
            String d = deviceOf(k);
            ObjectNode dn = a.has(d) ? (ObjectNode) a.get(d) : a.putObject(d);
            ObjectNode e = Json.putSafe(dn.putObject(k.substring(d.length() + 1)), "sourceTs", r.sourceTs());
            e.put("state", r.state()).put("reason", r.reason());
            e.set("active", r.active());
        });
        return a;
    }
    private ObjectNode lastObservationJson() {
        ObjectNode lo = Json.object();
        lastObservation.forEach((d, t) -> Json.putSafe(lo, d, t));
        return lo;
    }
    private static ObjectNode incidentJson(Incident x) {
        ObjectNode e = Json.object().put("source", x.source).put("incidentId", x.incidentId).put("stationRef", x.stationRef);
        ObjectNode revs = e.putObject("revisions");
        x.revisions.forEach((r, vs) -> {
            ArrayNode l = revs.putArray(Long.toString(r));
            for (Variant v : vs) {
                ObjectNode o = l.addObject();
                o.set("origin", origin(v.origin()));
                o.put("payloadDigest", v.payloadDigest()).put("digestRule", v.digestRule());
                o.set("value", v.value());
            }
        });
        return e;
    }
    private static ObjectNode unitsJson(TreeMap<String, AffectedSet.Unit> m) {
        ObjectNode e = Json.object();
        m.forEach((u, x) -> e.putObject(u).put("status", x.status()).put("reason", x.reason()).put("bodyDigest", x.bodyDigest()));
        return e;
    }
    private ObjectNode evidenceRangesJson() {
        ObjectNode er = Json.object();
        evidenceRanges.forEach((d, r) -> { ObjectNode e = Json.putSafe(er.putObject(d), "from", r.from()); Json.putSafe(e, "to", r.to()); });
        return er;
    }

    public static Stage2State fromJson(JsonNode n) {
        if (n.path("stateVersion").intValue() != VERSION) throw new IllegalArgumentException("stage-2 state version " + n.path("stateVersion") + " is not " + VERSION);
        Stage2State s = new Stage2State(Json.safeLong(n.get("generation"), "generation", Json.NON_NEGATIVE), Json.text(n, "station"),
            (int) Json.safeLong(n.get("partition"), "partition", 0, Integer.MAX_VALUE));
        n.get("bases").fields().forEachRemaining(e -> {
            JsonNode x = e.getValue();
            s.bases.put(e.getKey(), new Basis(x.get("visitRef"), (ObjectNode) x.get("body"), Json.text(x, "bodyDigest"), Json.safeLong(x.get("revision"), "revision", Json.NON_NEGATIVE)));
        });
        n.get("activity").fields().forEachRemaining(d -> d.getValue().fields().forEachRemaining(e -> {
            JsonNode x = e.getValue();
            s.activity.put(activityKey(d.getKey(), e.getKey()), new SetCoverage.ActivityRow(Json.safeLong(x.get("sourceTs"), "sourceTs", Json.NON_NEGATIVE), Json.text(x, "state"),
                x.path("reason").isTextual() ? x.get("reason").textValue() : null, x.get("active")));
        }));
        n.get("lastObservation").fields().forEachRemaining(e -> s.lastObservation.put(e.getKey(), Json.safeLong(e.getValue(), "lastObservation", Json.NON_NEGATIVE)));
        n.get("incidents").fields().forEachRemaining(e -> {
            JsonNode x = e.getValue();
            Incident inc = new Incident(Json.text(x, "source"), Json.text(x, "incidentId"));
            inc.stationRef = x.path("stationRef").isTextual() ? x.get("stationRef").textValue() : null;
            x.get("revisions").fields().forEachRemaining(r -> {
                List<Variant> vs = new ArrayList<>();
                for (JsonNode v : r.getValue()) vs.add(new Variant(origin(v.get("origin")), Json.text(v, "payloadDigest"), Json.text(v, "digestRule"), v.get("value")));
                inc.revisions.put(Long.parseLong(r.getKey()), vs);
            });
            s.incidents.put(e.getKey(), inc);
        });
        n.get("units").fields().forEachRemaining(e -> {
            TreeMap<String, AffectedSet.Unit> m = new TreeMap<>();
            e.getValue().fields().forEachRemaining(u -> m.put(u.getKey(), new AffectedSet.Unit(Json.text(u.getValue(), "status"), Json.text(u.getValue(), "reason"), Json.text(u.getValue(), "bodyDigest"))));
            s.units.put(e.getKey(), m);
        });
        n.get("evidenceRanges").fields().forEachRemaining(e -> s.evidenceRanges.put(e.getKey(),
            new SetCoverage.EvidenceRange(Json.safeLong(e.getValue().get("from"), "from", Json.ANY), Json.safeLong(e.getValue().get("to"), "to", Json.ANY))));
        s.bind(DeadlineIndex.fromJson(n.get("deadlines")), Revisions.fromJson(n.get("revisions").get("assessments")),
            Revisions.fromJson(n.get("revisions").get("sets")), Revisions.fromJson(n.get("revisions").get("dispositions")));
        JsonNode w = n.get("lastW2");
        s.lastW2 = w == null || w.isNull() ? null : Json.safeLong(w, "lastW2", Json.ANY);
        s.progressCount = Json.safeLong(n.get("progressCount"), "progressCount", Json.NON_NEGATIVE);
        return s;
    }

    // ===================================================================================================== segments

    /* The segmented form (plan step 6), an addition to toJson/fromJson:
     *  head                                          the scalars, lastObservation and evidenceRanges (one entry per device)
     *  deadlines/t<deadline / 10 min>                deadline registrations
     *  bases/h<hash % 64>                            work-record bases, by visitRef hash (retention bounds them)
     *  activity/<device>/t<sourceTs / 10 s>          activity rows
     *  incidents/h<hash % 64>, units/h<hash % 64>    incidents and their live unit rows, by set-key hash
     *  rev.assessments/<generation>/h<hash % 64>, rev.sets/<generation>/h<hash % 64>  by (source, incidentId) hash
     *  rev.dispositions/<generation>/<partition token>/o<inputOffset / 64>            by input origin
     * Every change marks its segment where it happens; reading or re-judging what did not change marks nothing. */

    private static String incidentKeyBucket(String text, JsonNode key) {
        JsonNode k = ChainSegments.parse(text, key);
        return Segments.hashBucket(k.path("source").asText() + "\u0000" + k.path("incidentId").asText());
    }
    private static String dispositionKeyBucket(String text, JsonNode key) {
        JsonNode k = ChainSegments.parse(text, key);
        if (!k.path("inputTopic").isTextual() || !k.path("inputPartition").isIntegralNumber() || !k.path("inputOffset").isIntegralNumber()) return Segments.hashBucket(text);
        return Segments.originBucket(k.get("inputTopic").textValue(), k.get("inputPartition").intValue(), k.get("inputOffset").longValue());
    }

    /** The ids of every segment of the state. */
    public SortedSet<String> segmentIds() {
        TreeSet<String> ids = new TreeSet<>();
        ids.add(HEAD);
        for (DeadlineIndex.Deadline d : deadlines.all()) ids.add(DEADLINES + "/" + Segments.visitBucket(d.deadline()));
        ids.addAll(bases.segmentIds()); ids.addAll(activity.segmentIds()); ids.addAll(incidents.segmentIds()); ids.addAll(units.segmentIds());
        ids.addAll(assessmentSegments.segmentIds()); ids.addAll(setSegments.segmentIds()); ids.addAll(dispositionSegments.segmentIds());
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
            case DEADLINES: {
                long w = Segments.visitWindowOf(id);
                ArrayNode a = deadlines.toJson(w * Segments.VISIT_WINDOW_MS, (w + 1) * Segments.VISIT_WINDOW_MS);
                if (a.isEmpty()) return null;
                n = Segments.header(id);
                n.set("entries", a);
                break;
            }
            case BASES: n = Segments.entries(id, bases.entries(id), Stage2State::basisJson); break;
            case ACTIVITY: {
                SortedMap<String, SetCoverage.ActivityRow> m = activity.entries(id);
                if (m.isEmpty()) return null;
                n = Segments.header(id);
                n.set("entries", activityJson(m));
                break;
            }
            case INCIDENTS: n = Segments.entries(id, incidents.entries(id), Stage2State::incidentJson); break;
            case UNITS: n = Segments.entries(id, units.entries(id), Stage2State::unitsJson); break;
            case REV_ASSESSMENTS: return assessmentSegments.segment(id);
            case REV_SETS: return setSegments.segment(id);
            case REV_DISPOSITIONS: return dispositionSegments.segment(id);
            default: return null;
        }
        return n == null ? null : Segments.bytes(n);
    }

    private ObjectNode headJson() {
        ObjectNode n = Json.object().put("stateVersion", VERSION);
        Json.putSafe(n, "generation", generation);
        n.put("station", station).put("partition", partition);
        n.set("lastObservation", lastObservationJson());
        n.set("evidenceRanges", evidenceRangesJson());
        if (lastW2 == null) n.putNull("lastW2"); else Json.putSafe(n, "lastW2", lastW2);
        Json.putSafe(n, "progressCount", progressCount);
        return n;
    }

    /** The whole state as segments: id to canonical bytes, deterministic (equal states give equal maps). */
    public SortedMap<String, byte[]> segments() {
        TreeMap<String, byte[]> m = new TreeMap<>();
        for (String id : segmentIds()) m.put(id, segment(id));
        return m;
    }

    /** As {@code Stage1State#drainChanged}. */
    public Segments.Drain drainChanged() { return Segments.drain(dirty, this::segment, this::segments); }

    /** As {@code Stage1State#fromSegments}. */
    public static Stage2State fromSegments(Map<String, byte[]> segments) {
        byte[] h = segments.get(HEAD);
        if (h == null) throw new IllegalArgumentException("stage-2 segments without a head");
        ObjectNode n = ((ObjectNode) Segments.read(HEAD, h).get("state")).deepCopy();
        ObjectNode b = Json.object(), a = Json.object(), in = Json.object(), un = Json.object(), as = Json.object(), st = Json.object(), dr = Json.object();
        ArrayNode dl = Json.JSON.createArrayNode();
        for (Map.Entry<String, byte[]> e : segments.entrySet()) {
            String id = e.getKey();
            if (HEAD.equals(id)) continue;
            JsonNode seg = Segments.read(id, e.getValue());
            switch (Segments.kind(id)) {
                case DEADLINES: dl.addAll((ArrayNode) seg.get("entries")); break;
                case BASES: merge(b, seg.get("entries"), id); break;
                case ACTIVITY: seg.get("entries").fields().forEachRemaining(d -> {
                    JsonNode existing = a.get(d.getKey());
                    merge(existing instanceof ObjectNode o ? o : a.putObject(d.getKey()), d.getValue(), id);
                }); break;
                case INCIDENTS: merge(in, seg.get("entries"), id); break;
                case UNITS: merge(un, seg.get("entries"), id); break;
                case REV_ASSESSMENTS: ChainSegments.merge(as, seg); break;
                case REV_SETS: ChainSegments.merge(st, seg); break;
                case REV_DISPOSITIONS: ChainSegments.merge(dr, seg); break;
                default: throw new IllegalArgumentException("not a stage-2 segment: " + id);
            }
        }
        n.set("deadlines", dl);
        n.set("bases", b); n.set("activity", a); n.set("incidents", in); n.set("units", un);
        ObjectNode rev = n.putObject("revisions");
        rev.set("assessments", as); rev.set("sets", st); rev.set("dispositions", dr);
        Stage2State s = fromJson(n);
        if (!s.segmentIds().equals(new TreeSet<>(segments.keySet())))
            throw new IllegalArgumentException("stage-2 segments: the map's " + segments.size() + " ids are not the state's " + s.segmentIds().size());
        s.dirty.clean();
        return s;
    }

    private static void merge(ObjectNode target, JsonNode entries, String id) {
        entries.fields().forEachRemaining(e -> {
            if (target.has(e.getKey())) throw new IllegalArgumentException("key " + e.getKey() + " in two segments (" + id + ")");
            target.set(e.getKey(), e.getValue());
        });
    }

    static ObjectNode origin(Inputs.Origin o) {
        ObjectNode n = Json.object().put("topic", o.topic()).put("partition", o.partition());
        return Json.putSafe(n, "offset", o.offset());
    }
    static Inputs.Origin origin(JsonNode n) {
        return new Inputs.Origin(Json.text(n, "topic"), (int) Json.safeLong(n.get("partition"), "partition", 0, Integer.MAX_VALUE), Json.safeLong(n.get("offset"), "offset", Json.NON_NEGATIVE));
    }
}
