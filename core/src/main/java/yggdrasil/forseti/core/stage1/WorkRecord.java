package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.rules.MemberCatalog;
import yggdrasil.forseti.core.stage1.Coverage.Interval;

/** The work record body of a visit (spec 337-358 as serialized by the Task 1 insertion at line 353; rule sheet S5, S6, and P2 for
 *  {@code plc-stamped}, whose visits add {@code missing-visit-close} and {@code inconsistent-visit-boundary}).
 *  Key: {@code visitRef}. Body fields: {@code unitId, readPoint, visitSeq, status, retraction (only when retracted), insufficiencyReasons,
 *  interval {enter, leave, attributionEnd}, members, equipmentEvidence, samplesDigest, coverage, attributionMode, rulesDigest}. */
final class WorkRecord {
    /** One coverage observation of a visit (a deduplicated, non-conflicting measurement whose quality-blind candidate is the visit). */
    record Observation(String device, String metric, long t, JsonNode value, Long status, boolean bad, boolean accepted) {
        ObjectNode digestEntry() {
            ObjectNode n = Json.object().put("device", device).put("metric", metric);
            Json.putSafe(n, "sourceTs", t);
            n.set("value", value);
            if (status == null) n.putNull("uaStatusCode"); else Json.putSafe(n, "uaStatusCode", status);
            return n;
        }
        Coverage.Obs obs() { return new Coverage.Obs(t, bad, accepted, value); }
    }

    private WorkRecord() {}

    /** The evaluation interval J's end (S6): {@code leave} closed; open {@code min(W1, attributionEnd, enter + H)} over the known bounds. */
    static long jEnd(VisitClaim v, Long w, long horizonMs) {
        if (v.leave != null) return v.leave;
        long end = v.enter + horizonMs;
        if (w != null) end = Math.min(end, w);
        if (v.attributionEnd != null) end = Math.min(end, v.attributionEnd);
        return end;
    }

    static ObjectNode build(Stage1 cfg, VisitClaim v, Long w, Sessions.View view, List<Observation> observations) {
        long p = cfg.P, enter = v.enter, end = jEnd(v, w, cfg.H);
        boolean nonEmpty = end > enter;
        List<Observation> inJ = new ArrayList<>();
        Map<String, List<Observation>> byMember = new TreeMap<>();
        for (Observation o : observations) {
            if (o.t() < enter || o.t() >= end) continue;
            MemberCatalog.Resolved r = cfg.member(o.device(), o.metric());
            if (r == null) continue;
            inJ.add(o);
            byMember.computeIfAbsent(r.mappingId(), k -> new ArrayList<>()).add(o);
        }
        String station = v.readPoint;
        SortedSet<String> reasons = new TreeSet<>();

        // members[]: every required (measurement) member of the station and its tools
        ArrayNode members = Json.JSON.createArrayNode();
        Map<String, Coverage.Counts> counts = new HashMap<>();
        for (MemberCatalog.Resolved r : cfg.catalog.requiredMembersOf(station)) {
            List<Observation> l = byMember.getOrDefault(r.mappingId(), List.of());
            Coverage.Counts c = Coverage.counts(obs(l), enter, end, p);
            counts.put(r.mappingId(), c);
            ObjectNode m = members.addObject().put("mappingId", r.mappingId()).put("metric", r.metric()).put("equipmentPath", r.equipmentPath()).put("valueType", r.valueType());
            Json.putSafe(m, "observedPeriods", c.observedPeriods());
            Json.putSafe(m, "missingPeriods", c.missingPeriods());
            Json.putSafe(m, "badSlots", c.badSlots());
            aggregate(m, r.valueType(), l);
        }
        if (nonEmpty) reasons.addAll(Coverage.memberReasons(counts.values(), cfg.maxMissing, cfg.maxBadShare));

        // equipmentEvidence[]
        ArrayNode evidence = Json.JSON.createArrayNode();
        long vObserved = Long.MAX_VALUE, vMissing = 0, vBad = 0, vGaps = 0;
        boolean anyEntry = false;
        for (String q : cfg.equipmentOf(station)) {
            Optional<MemberCatalog.Resolved> act = cfg.catalog.activityOf(q);
            List<Observation> actObs = act.isPresent() ? byMember.getOrDefault(act.get().mappingId(), List.of()) : List.of();
            Coverage.Activity activity = Coverage.activity(obs(actObs), enter, end, p, view.unknown(q));
            long unknownPeriods = act.isPresent() ? activity.unknownPeriods() : 0;
            if (nonEmpty && unknownPeriods > cfg.maxMissing) reasons.add("unknown-activity");
            List<Observation> qObs = new ArrayList<>();
            for (Observation o : inJ) if (o.device().equals(q)) qObs.add(o);
            if (qObs.isEmpty()) continue;
            anyEntry = true;
            SortedSet<String> refs = new TreeSet<>();
            for (Observation o : qObs) refs.add(cfg.member(o.device(), o.metric()).mappingId());
            List<MemberCatalog.Resolved> meas = cfg.catalog.measurementsOf(q);
            long observed, missing, bad;
            boolean badShare = false;
            if (!meas.isEmpty()) {
                observed = Long.MAX_VALUE; missing = 0; bad = 0;
                for (MemberCatalog.Resolved r : meas) {
                    Coverage.Counts c = counts.getOrDefault(r.mappingId(), Coverage.counts(obs(byMember.getOrDefault(r.mappingId(), List.of())), enter, end, p));
                    observed = Math.min(observed, c.observedPeriods()); missing = Math.max(missing, c.missingPeriods()); bad = Math.max(bad, c.badSlots());
                    if (c.badShare() > cfg.maxBadShare) badShare = true;
                }
            } else if (act.isPresent()) {
                Coverage.Counts c = Coverage.counts(obs(actObs), enter, end, p);
                observed = c.observedPeriods(); missing = c.missingPeriods(); bad = 0;
            } else { observed = 0; missing = Coverage.slots(enter, end, p); bad = 0; }
            long gaps = 0;
            for (Interval g : view.gaps(q)) if (g.overlaps(enter, end)) gaps++;
            boolean unknownSession = false;
            for (Interval u : view.unknown(q)) if (u.overlaps(enter, end)) unknownSession = true;
            String status = unknownPeriods > 0 || unknownSession ? "unknown"
                : missing > cfg.maxMissing || gaps > 0 || badShare ? "incomplete" : "complete";
            ObjectNode e = evidence.addObject().put("equipmentPath", q);
            ArrayNode mr = e.putArray("memberRefs");
            refs.forEach(mr::add);
            e.set("activityIntervals", Coverage.intervals(activity.active()));
            e.set("inactiveIntervals", Coverage.intervals(activity.inactive()));
            ObjectNode cov = e.putObject("coverage");
            Json.putSafe(cov, "observedPeriods", observed);
            Json.putSafe(cov, "missingPeriods", missing);
            Json.putSafe(cov, "badSlots", bad);
            Json.putSafe(cov, "unknownActivityPeriods", unknownPeriods);
            Json.putSafe(cov, "sessionGaps", gaps);
            cov.put("status", status);
            e.put("samplesDigest", samplesDigest(qObs));
            vObserved = Math.min(vObserved, observed); vMissing = Math.max(vMissing, missing); vBad = Math.max(vBad, bad); vGaps += gaps;
        }
        if (!anyEntry) { vObserved = 0; vMissing = Coverage.slots(enter, end, p); vBad = 0; vGaps = 0; }

        // session reasons (non-retracted visits only), missing departure, correction conflict
        if (!v.retracted() && nonEmpty) {
            for (String q : cfg.equipmentOf(station)) {
                if (!cfg.controls.ignoreSessionGap()) for (Interval g : view.gaps(q)) if (g.overlaps(enter, end)) reasons.add("session-gap");   // the ignore-session-gap control adds none
                for (Interval u : view.unknown(q)) if (u.overlaps(enter, end)) reasons.add("session-ambiguous");
            }
        }
        // P2: missing-departure only when neither a VisitPresent=false nor a VisitId change closed the visit
        if (v.leave == null && !v.missingVisitClose && w != null && w >= v.enter + cfg.H) reasons.add("missing-departure");
        if (v.correctionConflict) reasons.add("correction-conflict");
        if (v.missingVisitClose) reasons.add("missing-visit-close");
        if (v.inconsistentBoundary) reasons.add("inconsistent-visit-boundary");

        boolean settled = v.leave != null && w != null && w >= v.leave + cfg.L;
        String retraction = v.retractionReason();
        String status = retraction != null ? "retracted" : !reasons.isEmpty() ? "insufficient" : settled ? "complete" : "provisional";

        ObjectNode b = Json.object().put("unitId", v.unitId).put("readPoint", v.readPoint);
        if (v.visitSeq == null) b.putNull("visitSeq"); else Json.putSafe(b, "visitSeq", v.visitSeq);
        b.put("status", status);
        if (retraction != null) b.putObject("retraction").put("reason", retraction).put("errorReason", "error-declaration".equals(retraction) ? v.errorReason : null);
        ArrayNode ir = b.putArray("insufficiencyReasons");
        reasons.forEach(ir::add);
        ObjectNode interval = Json.putSafe(b.putObject("interval"), "enter", v.enter);
        Stage1State.putLong(interval, "leave", v.leave);
        Stage1State.putLong(interval, "attributionEnd", v.leave == null ? v.attributionEnd : null);
        b.set("members", members);
        b.set("equipmentEvidence", evidence);
        b.put("samplesDigest", samplesDigest(inJ));
        ObjectNode cov = Json.putSafe(b.putObject("coverage"), "expectedPeriodMs", p);
        Json.putSafe(cov, "observedPeriods", vObserved);
        Json.putSafe(cov, "missingPeriods", vMissing);
        Json.putSafe(cov, "badSlots", vBad);
        Json.putSafe(cov, "sessionGaps", vGaps);
        b.put("attributionMode", v.mode);
        b.put("rulesDigest", cfg.rulesDigest);
        return b;
    }

    private static List<Coverage.Obs> obs(List<Observation> l) { List<Coverage.Obs> o = new ArrayList<>(l.size()); l.forEach(x -> o.add(x.obs())); return o; }

    /** Aggregates over accepted-quality observations: Double {count, min, max, last}, Boolean {count, trueCount, last}, String
     *  {count, distinctCount, last}; {@code last} by sourceTs. */
    private static void aggregate(ObjectNode m, String valueType, List<Observation> l) {
        List<Observation> acc = new ArrayList<>();
        for (Observation o : l) if (o.accepted()) acc.add(o);
        acc.sort(Comparator.comparingLong(Observation::t));
        Json.putSafe(m, "count", acc.size());
        JsonNode last = acc.isEmpty() ? null : acc.get(acc.size() - 1).value();
        switch (valueType) {
            case "Boolean": {
                long trues = acc.stream().filter(o -> o.value().isBoolean() && o.value().booleanValue()).count();
                Json.putSafe(m, "trueCount", trues);
                break;
            }
            case "String": {
                Json.putSafe(m, "distinctCount", acc.stream().map(o -> o.value().asText()).distinct().count());
                break;
            }
            default: {
                if (acc.isEmpty()) { m.putNull("min"); m.putNull("max"); break; }
                double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;
                for (Observation o : acc) { min = Math.min(min, o.value().doubleValue()); max = Math.max(max, o.value().doubleValue()); }
                m.put("min", min).put("max", max);
            }
        }
        if (last == null) m.putNull("last"); else m.set("last", last);
    }

    /** JCS SHA-256 of the distinct contributing observations sorted by device, metric, sourceTs, then canonical bytes. */
    static String samplesDigest(List<Observation> l) {
        List<Observation> s = new ArrayList<>(l);
        s.sort(Comparator.comparing(Observation::device).thenComparing(Observation::metric).thenComparingLong(Observation::t));
        ArrayNode a = Json.JSON.createArrayNode();
        Observation prev = null;   // a coverage observation is one per (device, metric, sourceTs): the measurement triple is deduplicated
        for (Observation o : s) {
            if (prev != null && prev.device().equals(o.device()) && prev.metric().equals(o.metric()) && prev.t() == o.t()) continue;
            prev = o;
            a.add(o.digestEntry());
        }
        return Canonical.digest(a);
    }
}
