package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import java.util.function.Predicate;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.Ids;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.rules.AttributionRule;
import yggdrasil.forseti.core.rules.MemberCatalog;
import yggdrasil.forseti.core.rules.RuleSet;

/** Rule sheet P1-P3 ({@code plc-stamped}, spec 135, 261, 506, 519): the controller stamps {@code UnitId}, {@code VisitId} and
 *  {@code VisitPresent} into the same message as the equipment's activity and measurements, so attribution needs no time join.
 *  <ul>
 *  <li>P1: {@link #snapshot} reads one message of an identified equipment; it is coherent when every rule member of that equipment is
 *      present with one common sourceTs, else its samples are {@code unattributed incoherent-snapshot};</li>
 *  <li>P2: {@link #derive} rebuilds every lifecycle per {@code (namespace, station, visitId)} from the stored snapshots, so arrival
 *      order does not matter: per equipment the earliest valid true starts, the earliest later valid false with the same ID ends, a
 *      later valid true with another ID closes it as {@code missing-visit-close}; equipment of one visit must agree on enter and
 *      leave and a true after the close is {@code inconsistent-visit-boundary}; two unit IDs for one visit ID is an input conflict;</li>
 *  <li>P3: {@link Frame#candidate} attributes a coherent sample of equipment q to the visit whose {@code [start, end)} on q contains
 *      its sourceTs, with no tolerance; {@link Frame#mismatch} is the {@code crossCheck: occupancy} diagnostic.</li></ul>
 *  Pure: it reads the rule set it was built with and the state it is given. */
final class PlcStamped {
    static final String MODE = "plc-stamped";

    /** The metrics of one identified equipment's identity members. */
    record Identity(String unitMetric, String visitMetric, String presentMetric) {
        List<String> metrics() { return List.of(unitMetric, visitMetric, presentMetric); }
    }

    final String namespace;
    /** {@code crossCheck: occupancy}: the occupancy stream is read, for {@code identity-mismatch} only. */
    final boolean crossCheck;
    private final Map<String, Identity> identity = new TreeMap<>();
    private final Map<String, SortedSet<String>> ruleMetrics = new TreeMap<>();
    private final Map<String, String> stationOf = new TreeMap<>();

    PlcStamped(RuleSet rules) {
        AttributionRule a = rules.attribution();
        this.namespace = a.namespace().orElseThrow(() -> new IllegalStateException("plc-stamped rule without identity.namespace"));
        this.crossCheck = "occupancy".equals(a.crossCheck());
        MemberCatalog cat = rules.members();
        for (AttributionRule.IdentityMember im : a.identityMembers())
            identity.put(im.equipmentPath(), new Identity(cat.byMappingId().get(im.unitMemberRef()).metric(), cat.byMappingId().get(im.visitMemberRef()).metric(),
                cat.byMappingId().get(im.presentMemberRef()).metric()));
        for (MemberCatalog.Resolved r : cat.byMappingId().values()) {
            stationOf.put(r.equipmentPath(), r.station());
            if (identity.containsKey(r.equipmentPath())) ruleMetrics.computeIfAbsent(r.equipmentPath(), k -> new TreeSet<>()).add(r.metric());
        }
    }

    static ObjectNode visitRef(String namespace, String station, String visitId) {
        return Json.object().put("mode", MODE).put("namespace", namespace).put("station", station).put("visitId", visitId);
    }

    /** The key of the snapshot a sample belongs to: its message identity (the sampleId without metric), canonical text. */
    static String messageKey(ObjectNode sampleId) {
        ObjectNode m = sampleId.deepCopy();
        m.remove("metric");
        return VisitClaim.text(m);
    }
    static String messageKey(Inputs.ProcessMessage m) { return VisitClaim.text(Ids.messageId(m)); }

    // ===================================================================================================== P1

    /** The snapshot of one message, or null when its device has no identity entry. {@code accepted}: the rule's accepted quality. */
    Stage1State.Snapshot snapshot(Inputs.ProcessMessage m, Inputs.Origin origin, String payloadDigest, String digestRule, Predicate<Long> accepted) {
        Identity id = identity.get(m.device());
        if (id == null) return null;
        Map<String, Inputs.Member> byMetric = new HashMap<>();
        boolean coherent = true;
        Long max = null;
        for (Inputs.Member x : m.members()) {
            if (byMetric.put(x.metric(), x) != null) coherent = false;
            if (x.sourceTs() != null && (max == null || x.sourceTs() > max)) max = x.sourceTs();
        }
        Long ts = null;
        for (String metric : ruleMetrics.get(m.device())) {
            Inputs.Member x = byMetric.get(metric);
            if (x == null || x.sourceTs() == null) { coherent = false; continue; }
            if (ts == null) ts = x.sourceTs();
            else if (!ts.equals(x.sourceTs())) coherent = false;
        }
        Inputs.Member unit = byMetric.get(id.unitMetric()), visit = byMetric.get(id.visitMetric()), present = byMetric.get(id.presentMetric());
        boolean valid = coherent
            && unit != null && unit.value().isTextual() && accepted.test(unit.uaStatusCode())
            && visit != null && visit.value().isTextual() && accepted.test(visit.uaStatusCode())
            && present != null && present.value().isBoolean() && accepted.test(present.uaStatusCode());
        ObjectNode samples = Json.object();
        for (String metric : id.metrics()) if (byMetric.containsKey(metric)) samples.set(metric, Ids.sampleId(m, metric));
        return new Stage1State.Snapshot(m.device(), coherent ? ts : max, coherent, valid, valid ? unit.value().textValue() : null,
            valid ? visit.value().textValue() : null, valid ? present.value().booleanValue() : null, origin, payloadDigest, digestRule, samples);
    }

    String stationOf(String device) { return stationOf.get(device); }

    // ===================================================================================================== P2

    /** One equipment's part of a visit: {@code [start, end)} with {@code end} null while open. */
    record Span(VisitClaim visit, long start, Long end) {
        boolean contains(long t) { return t >= start && (end == null || t < end); }
    }

    /** The lifecycle of one visit ID on one equipment. */
    private record Part(String device, long start, Long close, boolean change, long lastTrue, boolean trueAfterClose, String unitAtStart) {}

    /** The derived lifecycles of one station: the online visits, every equipment's spans and the snapshots of each conflicting visit. */
    static final class Frame {
        final List<VisitClaim> visits;
        final Map<String, List<Span>> spans;
        final Map<String, List<Stage1State.Snapshot>> conflicting;
        final String signature;
        private final Stage1State state;
        Frame(List<VisitClaim> visits, Map<String, List<Span>> spans, Map<String, List<Stage1State.Snapshot>> conflicting, String signature, Stage1State state) {
            this.visits = visits; this.spans = spans; this.conflicting = conflicting; this.signature = signature; this.state = state;
        }

        /** The snapshot a sample belongs to, or null (a device without identity, or a snapshot already pruned). */
        Stage1State.Snapshot snapshotOf(Stage1State.Sample x) { return state.snapshots.get(messageKey(x.sampleId)); }

        /** P3: the visit whose {@code [start, end)} on the sample's equipment contains t; no tolerance, no time join. */
        Attribution.Candidate candidate(String device, long t) {
            List<VisitClaim> in = new ArrayList<>();
            for (Span sp : spans.getOrDefault(device, List.of())) if (sp.contains(t)) in.add(sp.visit());
            if (in.isEmpty()) return new Attribution.Candidate(Attribution.Kind.NO_OCCUPANCY, null);
            if (in.size() == 1) return new Attribution.Candidate(Attribution.Kind.ATTRIBUTED, in.get(0));
            return new Attribution.Candidate(Attribution.Kind.AMBIGUOUS, null);
        }

        /** P3 cross-check: a non-retracted occupancy visit at the station whose {@code [enter, leave)} contains t with another unit. */
        static boolean mismatch(VisitClaim visit, long t, List<VisitClaim> occupants) {
            for (VisitClaim o : occupants)
                if (o.online() && !o.retracted() && o.readPoint.equals(visit.readPoint) && o.enter <= t && t < o.actualEnd() && !Objects.equals(o.unitId, visit.unitId)) return true;
            return false;
        }
    }

    /** P2 over every stored snapshot of the station's identified equipment; visits whose online state expired are left out. */
    Frame derive(Stage1State s) {
        String station = s.station;
        Map<String, List<Stage1State.Snapshot>> byDevice = new TreeMap<>();
        for (Stage1State.Snapshot x : s.snapshots.values())   // message-key order; the stable sort below keeps it for equal sourceTs
            if (x.valid() && station.equals(stationOf(x.device()))) byDevice.computeIfAbsent(x.device(), d -> new ArrayList<>()).add(x);
        Comparator<Stage1State.Snapshot> order = Comparator.comparingLong(Stage1State.Snapshot::sourceTs);
        Map<String, List<Part>> parts = new TreeMap<>();
        Map<String, List<Stage1State.Snapshot>> ofVisit = new TreeMap<>();
        for (Map.Entry<String, List<Stage1State.Snapshot>> e : byDevice.entrySet()) {
            List<Stage1State.Snapshot> l = e.getValue();
            l.sort(order);
            SortedSet<String> ids = new TreeSet<>();
            for (Stage1State.Snapshot x : l) {
                ofVisit.computeIfAbsent(x.visitId(), k -> new ArrayList<>()).add(x);
                if (x.present()) ids.add(x.visitId());
            }
            for (String v : ids) {
                int i = 0;
                while (!(l.get(i).visitId().equals(v) && l.get(i).present())) i++;
                long start = l.get(i).sourceTs(), lastTrue = start;
                Long close = null;
                boolean change = false, after = false;
                for (int j = i + 1; j < l.size(); j++) {
                    Stage1State.Snapshot y = l.get(j);
                    if (y.sourceTs() <= start) continue;   // "later": a snapshot at the start's own sourceTs decides nothing
                    if (y.visitId().equals(v)) {
                        if (y.present()) { if (close == null) lastTrue = y.sourceTs(); else after = true; }
                        else if (close == null) close = y.sourceTs();
                    } else if (y.present() && close == null) { close = y.sourceTs(); change = true; }
                }
                parts.computeIfAbsent(v, k -> new ArrayList<>()).add(new Part(e.getKey(), start, close, change, lastTrue, after, l.get(i).unitId()));
            }
        }
        List<VisitClaim> visits = new ArrayList<>();
        Map<String, List<Span>> spans = new TreeMap<>();
        Map<String, List<Stage1State.Snapshot>> conflicting = new TreeMap<>();
        StringBuilder sig = new StringBuilder();
        for (Map.Entry<String, List<Part>> e : parts.entrySet()) {
            String v = e.getKey();
            ObjectNode ref = visitRef(namespace, station, v);
            String key = VisitClaim.keyOf(ref);
            if (s.expired.containsKey(key)) continue;
            List<Part> ps = e.getValue();
            Part first = ps.get(0);
            for (Part p : ps) if (p.start() < first.start()) first = p;
            VisitClaim c = VisitClaim.plc(ref, first.unitAtStart(), station, first.start());
            SortedSet<String> units = new TreeSet<>();
            for (Stage1State.Snapshot x : ofVisit.get(v)) units.add(x.unitId());
            c.inputConflict = units.size() > 1;
            Set<Long> starts = new TreeSet<>(), closes = new TreeSet<>();
            boolean allEnded = true, anyChange = false, after = false;
            Long changeAt = null, maxEnd = null, minClose = null;
            for (Part p : ps) {
                starts.add(p.start());
                if (p.trueAfterClose()) after = true;
                if (p.close() == null) { allEnded = false; continue; }
                closes.add(p.close());
                minClose = minClose == null ? p.close() : Math.min(minClose, p.close());
                if (p.change()) { allEnded = false; anyChange = true; changeAt = changeAt == null ? p.close() : Math.min(changeAt, p.close()); }
                else maxEnd = maxEnd == null ? p.close() : Math.max(maxEnd, p.close());
            }
            boolean stillPresent = false;   // an equipment still reporting the visit present at or after another's close
            if (minClose != null) for (Part p : ps) if (p.close() == null && p.lastTrue() >= minClose) stillPresent = true;
            c.inconsistentBoundary = starts.size() > 1 || closes.size() > 1 || after || stillPresent;
            if (allEnded) c.leave = maxEnd;
            else if (anyChange) { c.missingVisitClose = true; c.attributionEnd = changeAt; }
            visits.add(c);
            if (c.inputConflict) conflicting.put(c.key, ofVisit.get(v));
            sig.append(c.key).append('|').append(c.unitId).append('|').append(c.enter).append('|').append(c.leave).append('|').append(c.attributionEnd)
                .append('|').append(c.inputConflict).append(c.missingVisitClose).append(c.inconsistentBoundary);
            for (Part p : ps) {
                spans.computeIfAbsent(p.device(), d -> new ArrayList<>()).add(new Span(c, p.start(), p.close()));
                sig.append('|').append(p.device()).append(':').append(p.start()).append('-').append(p.close());
            }
            sig.append('\n');
        }
        return new Frame(visits, spans, conflicting, sig.toString(), s);
    }
}
