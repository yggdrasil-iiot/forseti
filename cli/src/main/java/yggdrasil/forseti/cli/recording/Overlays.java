package yggdrasil.forseti.cli.recording;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;

/** The injection overlays of a scenario (plan Task 16, Step 16.4), parsed and validated once. Parameters are milliseconds from T0. Each
 *  overlay is recorded verbatim in {@code manifest.source.overlays[]}; the builder asks this class what an overlay does to a delivered
 *  record (a message member, an occupancy event, a session) and the ground truth is then derived from the records as delivered.
 *  Nothing here reads a clock or a random source. */
final class Overlays {
    static final List<String> TYPES = List.of("plc-identity", "hold-back", "redeliver", "shift-source-ts", "bad-status", "overlap-visit",
        "delay-occupancy", "late-correction", "error-declaration", "resend-different", "activity-without-visit", "session-gap",
        "session-down", "stale-death", "session-ambiguous", "plc-cross-check-mismatch", "mixed-snapshot", "id-change-without-false",
        "no-visit-end", "no-departure", "incident");
    static final String DID_NOT_OCCUR = "urn:epcglobal:cbv:er:did_not_occur", INCORRECT_DATA = "urn:epcglobal:cbv:er:incorrect_data";

    record HoldBack(String topic, Set<String> devices, long from, long to, long delayMs) {}
    record Redeliver(String topic, String eventId, String device, Long at, long deliverAt) {}
    record BadStatus(String device, String metric, long from, long to, long code) {}
    record Activity(String device, long from, long to) {}
    record SessionGap(String device, long fromSeq, long toSeq) {}
    record SessionDown(String device, long at, long upAt) {}
    record StaleDeath(String device, long detectedEventTime, long rawPublishTs, long bdSeq) {}
    record SessionAmbiguous(String device, long detectedEventTime, long rawPublishTs, long bdSeq, long upAt) {}
    record DelayOccupancy(String eventId, long deliverAt) {}
    record LateCorrection(String departureEventId, long declarationTime, String newDepartureEventId, long newLeave, long deliverAt) {}
    record ErrorDeclaration(String eventId, String reason, long declarationTime, long deliverAt) {}
    record ResendDifferent(String eventId, String field, JsonNode value, long deliverAt) {}
    record CrossCheckMismatch(String station, long from, long to, String otherUnit) {}
    record MixedSnapshot(String device, long at) {}
    record IdChange(String station, long at, String newVisitId, String newUnit) {}
    record NoVisitEnd(String station, String visitId) {}

    final List<JsonNode> all = new ArrayList<>();
    final List<JsonNode> plcIdentity = new ArrayList<>(), overlapVisits = new ArrayList<>();
    final List<HoldBack> holdBacks = new ArrayList<>();
    final List<Redeliver> redelivers = new ArrayList<>();
    final Map<String, Long> shift = new TreeMap<>();
    final List<BadStatus> bad = new ArrayList<>();
    final List<Activity> activity = new ArrayList<>();
    final List<SessionGap> gaps = new ArrayList<>();
    final List<SessionDown> downs = new ArrayList<>();
    final List<StaleDeath> staleDeaths = new ArrayList<>();
    final List<SessionAmbiguous> ambiguous = new ArrayList<>();
    final List<DelayOccupancy> delays = new ArrayList<>();
    final List<LateCorrection> lateCorrections = new ArrayList<>();
    final List<ErrorDeclaration> declarations = new ArrayList<>();
    final List<ResendDifferent> resends = new ArrayList<>();
    final List<CrossCheckMismatch> mismatches = new ArrayList<>();
    final List<MixedSnapshot> mixed = new ArrayList<>();
    final List<IdChange> idChanges = new ArrayList<>();
    final List<NoVisitEnd> noVisitEnds = new ArrayList<>();
    final Set<String> noDeparture = new TreeSet<>();

    Overlays(JsonNode injections, Set<String> devices) {
        for (JsonNode inj : injections) {
            String type = Json.text(inj, "type");
            if (!TYPES.contains(type)) throw refuse("injection type " + type + " is not supported");
            all.add(inj.deepCopy());
            switch (type) {
                case "plc-identity" -> plcIdentity.add(inj);
                case "overlap-visit" -> overlapVisits.add(inj);
                case "hold-back" -> {
                    Set<String> ds = new TreeSet<>();
                    inj.path("devices").forEach(d -> ds.add(device(d.asText(), devices)));
                    String topic = Json.text(inj, "topic");
                    if (!topic.equals("ot.messages")) throw refuse("hold-back of " + topic + " (only ot.messages is held by the bridge)");
                    holdBacks.add(new HoldBack(topic, ds, ms(inj, "from"), ms(inj, "to"), ms(inj, "delayMs")));
                }
                case "redeliver" -> {
                    String topic = Json.text(inj, "topic");
                    JsonNode id = inj.path("identity");
                    if (topic.equals("mfg.occupancy")) redelivers.add(new Redeliver(topic, Json.text(id, "eventId"), null, null, ms(inj, "deliverAt")));
                    else if (topic.equals("ot.messages")) redelivers.add(new Redeliver(topic, null, device(Json.text(id, "device"), devices), ms(id, "at"), ms(inj, "deliverAt")));
                    else throw refuse("redeliver on " + topic);
                }
                case "shift-source-ts" -> {
                    long delta = Json.safeLong(inj.get("deltaMs"), "deltaMs", Json.ANY);
                    inj.path("devices").forEach(d -> { if (shift.put(device(d.asText(), devices), delta) != null) throw refuse("device shifted twice"); });
                }
                case "bad-status" -> bad.add(new BadStatus(device(Json.text(inj, "device"), devices), Json.text(inj, "metric"), ms(inj, "from"), ms(inj, "to"),
                    Json.safeLong(inj.get("code"), "code", 0, 0xFFFFFFFFL)));
                case "activity-without-visit" -> activity.add(new Activity(device(Json.text(inj, "device"), devices), ms(inj, "from"), ms(inj, "to")));
                case "session-gap" -> gaps.add(new SessionGap(device(Json.text(inj, "device"), devices), ms(inj, "fromSeq"), ms(inj, "toSeq")));
                case "session-down" -> downs.add(new SessionDown(device(Json.text(inj, "device"), devices), ms(inj, "at"), ms(inj, "upAt")));
                case "stale-death" -> staleDeaths.add(new StaleDeath(device(Json.text(inj, "device"), devices), ms(inj, "detectedEventTime"), ms(inj, "rawPublishTs"), ms(inj, "bdSeq")));
                case "session-ambiguous" -> ambiguous.add(new SessionAmbiguous(device(Json.text(inj, "device"), devices), ms(inj, "detectedEventTime"),
                    ms(inj, "rawPublishTs"), ms(inj, "bdSeq"), ms(inj, "upAt")));
                case "delay-occupancy" -> delays.add(new DelayOccupancy(Json.text(inj, "eventId"), ms(inj, "deliverAt")));
                case "late-correction" -> lateCorrections.add(new LateCorrection(Json.text(inj, "departureEventId"), ms(inj, "declarationTime"),
                    Json.text(inj, "newDepartureEventId"), ms(inj, "newLeave"), ms(inj, "deliverAt")));
                case "error-declaration" -> {
                    String reason = Json.text(inj, "reason");
                    String urn = reason.equals("did_not_occur") ? DID_NOT_OCCUR : reason.equals("incorrect_data") ? INCORRECT_DATA : null;
                    if (urn == null) throw refuse("error-declaration reason " + reason);
                    declarations.add(new ErrorDeclaration(Json.text(inj, "eventId"), urn, ms(inj, "declarationTime"), ms(inj, "deliverAt")));
                }
                case "resend-different" -> {
                    String field = Json.text(inj, "field");
                    if (List.of("eventId", "recordTime", "errorDeclaration").contains(field)) throw refuse("resend-different on " + field);
                    resends.add(new ResendDifferent(Json.text(inj, "eventId"), field, inj.get("value"), ms(inj, "deliverAt")));
                }
                case "plc-cross-check-mismatch" -> mismatches.add(new CrossCheckMismatch(Json.text(inj, "station"), ms(inj, "from"), ms(inj, "to"), Json.text(inj, "otherUnit")));
                case "mixed-snapshot" -> mixed.add(new MixedSnapshot(device(Json.text(inj, "device"), devices), ms(inj, "at")));
                case "id-change-without-false" -> idChanges.add(new IdChange(Json.text(inj, "station"), ms(inj, "at"), Json.text(inj, "newVisitId"), Json.text(inj, "newUnit")));
                case "no-visit-end" -> noVisitEnds.add(new NoVisitEnd(Json.text(inj, "station"), Json.text(inj, "visitId")));
                case "no-departure" -> noDeparture.add(Json.text(inj, "eventId"));
                case "incident" -> throw refuse("incident overlays are written as the scenario's incidents[]");
                default -> throw new IllegalStateException(type);
            }
        }
    }

    /** Every time an overlay delivers or declares something: part of the scenario's last business event (plan Task 16). */
    long lastBusiness() {
        long m = 0;
        for (Redeliver r : redelivers) m = Math.max(m, r.deliverAt());
        for (DelayOccupancy d : delays) m = Math.max(m, d.deliverAt());
        for (LateCorrection l : lateCorrections) m = Math.max(m, Math.max(l.deliverAt(), Math.max(l.declarationTime(), l.newLeave())));
        for (ErrorDeclaration e : declarations) m = Math.max(m, Math.max(e.deliverAt(), e.declarationTime()));
        for (ResendDifferent r : resends) m = Math.max(m, r.deliverAt());
        for (SessionDown d : downs) m = Math.max(m, d.upAt());
        for (StaleDeath d : staleDeaths) m = Math.max(m, d.detectedEventTime());
        for (SessionAmbiguous a : ambiguous) m = Math.max(m, Math.max(a.detectedEventTime(), a.upAt()));
        return m;
    }

    long shiftOf(String device) { return shift.getOrDefault(device, 0L); }

    /** The delivery time of a telemetry scan of {@code device} at scan time {@code t}: delayed by a hold-back, else the scan time. */
    long telemetryDeliverAt(String device, long t) {
        for (HoldBack h : holdBacks) if ((h.devices().isEmpty() || h.devices().contains(device)) && h.from() <= t && t < h.to()) return t + h.delayMs();
        return t;
    }

    Long badCode(String device, String metric, long t) {
        for (BadStatus b : bad) if (b.device().equals(device) && b.metric().equals(metric) && b.from() <= t && t < b.to()) return b.code();
        return null;
    }

    boolean forcedActive(String device, long t) {
        for (Activity a : activity) if (a.device().equals(device) && a.from() <= t && t < a.to()) return true;
        return false;
    }

    boolean mixedSnapshot(String device, long t) {
        for (MixedSnapshot m : mixed) if (m.device().equals(device) && m.at() == t) return true;
        return false;
    }

    static long ms(JsonNode o, String f) { return Json.safeLong(o.get(f), f, Json.NON_NEGATIVE); }
    private static String device(String d, Set<String> devices) {
        if (!devices.contains(d)) throw refuse("overlay names an undeclared device " + d);
        return d;
    }
    static IllegalArgumentException refuse(String what) { return new IllegalArgumentException("scenario refused: " + what); }

    /** The occupancy event value with one field replaced (resend-different). */
    static ObjectNode withField(ObjectNode v, String field, JsonNode value) {
        ObjectNode c = v.deepCopy();
        c.set(field, value.deepCopy());
        return c;
    }
}
