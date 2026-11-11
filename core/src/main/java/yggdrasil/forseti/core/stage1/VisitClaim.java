package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** One visit of the station as derived from the stored occupancy deliveries (rule sheet S4, S5 retractions, S7 overlap): the arrival
 *  gives {@code visitRef}, {@code enter} and the unit; valid leave claims give {@code leave}; declarations and content conflicts give the
 *  retraction cause. Derivation reads only the stored deliveries, so it does not depend on arrival order. */
final class VisitClaim {
    static final String MODE = "occupancy-join";

    final String key;
    final ObjectNode visitRef;
    final String source, arrivalEventId, unitId, readPoint;
    final Long visitSeq;
    final long enter;
    final Stage1State.Expired frozen;   // non-null: online state expired, kept as a window candidate only
    Long leave;
    boolean arrivalDeclared, inputConflict, correctionConflict, overlap;
    String errorReason;
    Long attributionEnd;

    private VisitClaim(ObjectNode visitRef, String source, String arrivalEventId, String unitId, String readPoint, Long visitSeq, long enter, Stage1State.Expired frozen) {
        this.visitRef = visitRef; this.key = keyOf(visitRef); this.source = source; this.arrivalEventId = arrivalEventId; this.unitId = unitId;
        this.readPoint = readPoint; this.visitSeq = visitSeq; this.enter = enter; this.frozen = frozen;
    }

    static ObjectNode visitRef(String source, String arrivalEventId) {
        return Json.object().put("mode", MODE).put("source", source).put("arrivalEventId", arrivalEventId);
    }
    static String keyOf(JsonNode visitRef) { return Canonical.utf8(Canonical.bytes(visitRef)); }
    static String text(JsonNode n) { return Canonical.utf8(Canonical.bytes(n)); }

    /** S5 precedence of the retraction causes: error declaration on the arrival, then a content conflict, then a decided overlap. */
    String retractionReason() {
        if (frozen != null) return frozen.retracted() ? "frozen" : null;
        if (arrivalDeclared) return "error-declaration";
        if (inputConflict) return "input-conflict";
        if (overlap) return "overlap";
        return null;
    }
    boolean retracted() { return retractionReason() != null; }
    boolean online() { return frozen == null; }
    /** The attribution window's upper bound: {@code leave + tolL} closed, {@code attributionEnd} open, null when unbounded. */
    Long windowUpper(long leaveToleranceMs) { return leave != null ? Long.valueOf(leave + leaveToleranceMs) : attributionEnd; }
    /** The end of the actual interval {@code [enter, leave)}; an open visit reaches its attributionEnd, else is unbounded. */
    long actualEnd() { return leave != null ? leave : attributionEnd != null ? attributionEnd : Long.MAX_VALUE; }
    /** {@code onlineExpiresAt} (spec 518): {@code leave + H + L} closed, {@code enter + H + L} open. */
    long onlineExpiresAt(long horizonMs, long allowedLatenessMs) {
        if (frozen != null) return frozen.onlineExpiresAt();
        return (leave != null ? leave : enter) + horizonMs + allowedLatenessMs;
    }

    /** Every visit of the station: one per stored arrival identity, plus the expired ones; overlap and attributionEnd decided. */
    static List<VisitClaim> derive(Stage1State s) {
        Map<String, List<Stage1State.Occupancy>> departures = new HashMap<>();
        for (Stage1State.Occupancy o : s.deliveries.values()) {
            Set<String> arrivalIds = new TreeSet<>();
            for (Stage1State.OccDelivery d : o.list) if (!d.arriving()) arrivalIds.add(d.content.path("arrivalEventId").asText());
            for (String a : arrivalIds) departures.computeIfAbsent(o.source + "\u0000" + a, k -> new ArrayList<>()).add(o);
        }
        List<VisitClaim> visits = new ArrayList<>();
        for (Stage1State.Occupancy o : s.deliveries.values()) {
            if (!o.hasArrivingVariant()) continue;
            TreeMap<String, ObjectNode> variants = variants(o);
            ObjectNode rep = null;
            for (ObjectNode v : variants.values()) if ("urn:epcglobal:cbv:bizstep:arriving".equals(v.path("bizStep").asText())) { rep = v; break; }
            JsonNode seq = rep.get("visitSeq");
            VisitClaim c = new VisitClaim(visitRef(o.source, o.eventId), o.source, o.eventId, rep.get("unitId").asText(), rep.get("readPoint").asText(),
                seq == null || seq.isNull() ? null : Json.safeLong(seq, "visitSeq", Json.NON_NEGATIVE), Json.safeLong(rep.get("eventTime"), "eventTime", Json.NON_NEGATIVE), null);
            c.inputConflict = variants.size() > 1;
            TreeMap<String, JsonNode> decls = declarations(o);
            if (decls.size() == 1) { c.arrivalDeclared = true; c.errorReason = decls.firstEntry().getValue().path("reason").asText(); }
            else if (decls.size() > 1) c.correctionConflict = true;
            TreeSet<Long> leaves = new TreeSet<>();
            for (Stage1State.Occupancy d : departures.getOrDefault(o.source + "\u0000" + o.eventId, List.of())) {
                if (variants(d).size() > 1) { c.inputConflict = true; continue; }       // a departure in conflict withdraws its claim
                TreeMap<String, JsonNode> dd = declarations(d);
                if (dd.size() > 1) { c.correctionConflict = true; continue; }          // two differing declarations: unapplied
                if (dd.size() == 1) continue;                                           // cancelled (or a tombstone)
                leaves.add(Json.safeLong(d.list.get(0).content.get("eventTime"), "eventTime", Json.NON_NEGATIVE));
            }
            if (leaves.size() == 1) c.leave = leaves.first();
            else if (leaves.size() > 1) c.correctionConflict = true;                    // two claims with different leaves stay unapplied
            visits.add(c);
        }
        for (Map.Entry<String, Stage1State.Expired> e : s.expired.entrySet()) {
            Stage1State.Expired x = e.getValue();
            VisitClaim c = new VisitClaim(x.visitRef(), x.visitRef().get("source").asText(), x.visitRef().get("arrivalEventId").asText(), null, s.station, null, x.enter(), x);
            c.leave = x.leave();
            c.attributionEnd = x.attributionEnd();
            visits.add(c);
        }
        decide(visits);
        return visits;
    }

    /** S7 overlap between visits with a valid arrival and a valid leave, then attributionEnd of each open visit (S4). */
    static void decide(List<VisitClaim> visits) {
        List<VisitClaim> valid = new ArrayList<>();
        for (VisitClaim v : visits) if (v.online() && !v.arrivalDeclared && !v.inputConflict && v.leave != null) valid.add(v);
        for (int i = 0; i < valid.size(); i++) for (int j = i + 1; j < valid.size(); j++) {
            VisitClaim a = valid.get(i), b = valid.get(j);
            if (a.readPoint.equals(b.readPoint) && a.enter < b.leave && b.enter < a.leave) { a.overlap = true; b.overlap = true; }
        }
        for (VisitClaim v : visits) {
            if (!v.online() || v.leave != null) continue;
            Long next = null;
            for (VisitClaim o : visits) if (o != v && !o.retracted() && o.enter > v.enter && (next == null || o.enter < next)) next = o.enter;
            v.attributionEnd = next;
        }
    }

    /** The distinct business contents of an identity's deliveries, by canonical text. */
    static TreeMap<String, ObjectNode> variants(Stage1State.Occupancy o) {
        TreeMap<String, ObjectNode> m = new TreeMap<>();
        for (Stage1State.OccDelivery d : o.list) m.putIfAbsent(text(d.content), d.content);
        return m;
    }
    /** The distinct error declarations of an identity's deliveries, by canonical text. */
    static TreeMap<String, JsonNode> declarations(Stage1State.Occupancy o) {
        TreeMap<String, JsonNode> m = new TreeMap<>();
        for (Stage1State.OccDelivery d : o.list) if (d.declaration != null) m.putIfAbsent(text(d.declaration), d.declaration);
        return m;
    }
}
