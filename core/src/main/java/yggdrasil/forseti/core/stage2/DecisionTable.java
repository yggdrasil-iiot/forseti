package yggdrasil.forseti.core.stage2;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import yggdrasil.forseti.core.input.Inputs;

/** Rule sheet T5-T6 (spec 519-531): the decision table of one {@code (incident, unit)}, first match from the top, over the unit's bases
 *  that are related or possibly-related to the current revision's interval. Pure. */
public final class DecisionTable {
    public static final String AFFECTED = "affected-candidate", NOT_AFFECTED = "not-affected-within-scope", INSUFFICIENT = "insufficient-evidence";
    public static final String DID_NOT_OCCUR = "urn:epcglobal:cbv:er:did_not_occur", INCORRECT_DATA = "urn:epcglobal:cbv:er:incorrect_data";

    /** A basis as judged: its visitRef, current work record body and digest, and its relation to the current revision. */
    public record Judged(JsonNode visitRef, JsonNode body, String bodyDigest, Relatedness.Kind relation) {
        String status() { return body.path("status").asText(); }
        boolean related() { return relation == Relatedness.Kind.RELATED; }
        long enter() { return body.path("interval").path("enter").longValue(); }
        Long leave() { JsonNode l = body.path("interval").path("leave"); return l.isIntegralNumber() ? l.longValue() : null; }
        String retraction() { JsonNode r = body.path("retraction"); return r.isObject() ? r.path("reason").asText(null) : null; }
        String errorReason() { JsonNode r = body.path("retraction"); return r.isObject() && r.path("errorReason").isTextual() ? r.path("errorReason").textValue() : null; }
        boolean hasReason(String reason) { for (JsonNode x : body.path("insufficiencyReasons")) if (reason.equals(x.asText())) return true; return false; }
        JsonNode evidenceOf(String equipmentPath) {
            for (JsonNode e : body.path("equipmentEvidence")) if (equipmentPath.equals(e.path("equipmentPath").asText())) return e;
            return null;
        }
    }

    /** {@code row}: the table row that matched, 1-9. */
    public record Decision(String status, String reason, int row) {}

    private DecisionTable() {}

    public static Decision decide(String match, String onWithdrawn, String onBasisRetracted, Inputs.IncidentEvent inc, List<Judged> bases) {
        return decide(match, onWithdrawn, onBasisRetracted, inc, bases, false);
    }

    /** {@code stationGranularity}: the {@code station-granularity} negative control, under which rows 2 and 7 read the station's activity
     *  evidence as the target tool's ({@code equipmentEvidence} of {@code stationRef} instead of {@code equipmentRef}). */
    public static Decision decide(String match, String onWithdrawn, String onBasisRetracted, Inputs.IncidentEvent inc, List<Judged> bases, boolean stationGranularity) {
        String tool = stationGranularity ? inc.stationRef() : inc.equipmentRef();
        // (1) withdrawn
        if ("withdrawn".equals(inc.status()))
            return new Decision("insufficient".equals(onWithdrawn) ? INSUFFICIENT : NOT_AFFECTED, "incident-withdrawn", 1);
        long sf = inc.suspectFrom(), st = inc.suspectTo() == null ? Long.MAX_VALUE : inc.suspectTo();
        // (2) positive evidence on a related complete basis
        for (Judged b : bases) {
            if (!b.related() || !"complete".equals(b.status())) continue;
            if ("station-visit".equals(match)) return new Decision(AFFECTED, "station-visit", 2);
            Long leave = b.leave();
            long lo = Math.max(b.enter(), sf), hi = Math.min(leave == null ? Long.MAX_VALUE : leave, st);
            JsonNode e = b.evidenceOf(tool);
            if (e != null && lo < hi) for (long[] x : intervals(e.path("activityIntervals")))
                if (Math.max(x[0], lo) < Math.min(x[1], hi)) return new Decision(AFFECTED, "equipment-activity", 2);
        }
        // (3) no related and no possibly-related basis
        if (bases.isEmpty()) return new Decision(NOT_AFFECTED, "interval-narrowed", 3);
        // (4) a related basis with correction-conflict
        for (Judged b : bases) if (b.related() && b.hasReason("correction-conflict")) return new Decision(INSUFFICIENT, "correction-conflict", 4);
        // (5) a related or possibly-related provisional or insufficient basis
        boolean provisional = false, insufficient = false;
        for (Judged b : bases) { if ("provisional".equals(b.status())) provisional = true; if ("insufficient".equals(b.status())) insufficient = true; }
        if (provisional) return new Decision(INSUFFICIENT, "basis-provisional", 5);
        if (insufficient) return new Decision(INSUFFICIENT, "basis-insufficient", 5);
        // (6) a related basis retracted for overlap, input conflict or incorrect_data, in that order
        for (String[] cause : new String[][] {{"overlap", "basis-retracted-overlap"}, {"input-conflict", "basis-retracted-input-conflict"}})
            for (Judged b : bases) if (b.related() && cause[0].equals(b.retraction())) return new Decision(INSUFFICIENT, cause[1], 6);
        for (Judged b : bases)
            if (b.related() && "error-declaration".equals(b.retraction()) && INCORRECT_DATA.equals(b.errorReason())) return new Decision(INSUFFICIENT, "basis-retracted-error-declaration", 6);
        // (7) related complete evidence proves the target tool inactive over [enter, leave) intersect S
        for (Judged b : bases) {
            if (!b.related() || !"complete".equals(b.status()) || b.leave() == null) continue;
            long lo = Math.max(b.enter(), sf), hi = Math.min(b.leave(), st);
            JsonNode e = b.evidenceOf(tool);
            if (lo < hi && e != null && covered(intervals(e.path("inactiveIntervals")), lo, hi)) return new Decision(NOT_AFFECTED, "no-equipment-activity", 7);
        }
        // (8) every remaining basis cancelled with did_not_occur
        boolean all = true;
        for (Judged b : bases) if (!("error-declaration".equals(b.retraction()) && DID_NOT_OCCUR.equals(b.errorReason()))) all = false;
        if (all) return new Decision("always-insufficient".equals(onBasisRetracted) ? INSUFFICIENT : NOT_AFFECTED, "basis-retracted-error-declaration", 8);
        // (9)
        return new Decision(INSUFFICIENT, "basis-insufficient", 9);
    }

    static List<long[]> intervals(JsonNode a) {
        List<long[]> l = new ArrayList<>();
        for (JsonNode x : a) l.add(new long[] {x.get(0).longValue(), x.get(1).longValue()});
        l.sort(Comparator.comparingLong(x -> x[0]));
        return l;
    }

    /** {@code [lo, hi)} lies inside the union of the intervals. */
    static boolean covered(List<long[]> sorted, long lo, long hi) {
        long reach = lo;
        for (long[] x : sorted) {
            if (x[0] > reach) break;
            if (x[1] > reach) reach = x[1];
            if (reach >= hi) return true;
        }
        return reach >= hi;
    }
}
