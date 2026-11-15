package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.core.Json;

/** The acceptance of a live capture's visits (step-5 plan PR 8), judged in {@code recordings compare} on the core's work records at
 *  the last cut, against the injections the capture declares ({@code source.injections}, written by the recorder from the scenario
 *  and the runner's options). A capture that passes the core-against-oracle comparison may still show that the loop did not deliver
 *  what the scenario drove: this check says so.
 *
 *  <ul>
 *  <li>A visit is touched by an injection with a window when it is a visit of the injection's station and its interval
 *  {@code [enter, leave)} overlaps the window {@code [T0 + from, T0 + to)} (an open visit extends to the end of time). The bridge
 *  hold (injection 1) and the source offset (injection 9) have no window: they delay or move telemetry and remove none of it, so they
 *  touch no visit.</li>
 *  <li>Every visit no injection touches is {@code complete}; a capture without injections therefore has every visit complete.</li>
 *  <li>Every touched visit is {@code insufficient} and its {@code insufficiencyReasons} hold the reason of each injection that touches
 *  it: {@code bad-share-exceeded} for a Bad status (3), {@code session-gap} for a killed Muninn (8, its NDEATH a {@code session-down}),
 *  {@code missing-periods} for a host offline (25(a): the bridge makes the death a {@code stale-death}, so no gap is reported and the
 *  absence shows as missing periods). Other reasons may stand beside it.</li>
 *  <li>A host offline (25(a)) also needs the captured session events to show the injection: a {@code stale-death} of its device, no
 *  {@code session-down} for the session it ended, and a {@code session-up} of a newer session of that device.</li>
 *  </ul>
 *  The windows are declared in the scenario's clock (milliseconds from the drive's start); the runner times its options from its own
 *  start of the drive, a second or two before the first scan the drive commits ({@code T0}). A scenario places its windows well inside
 *  a visit and well away from the others, so that offset decides nothing; a window that grazes a visit is a scenario error this check
 *  reports. A recording without {@code source.capture} (a synthetic step-3 recording) is not a live capture: the check does not apply. */
final class LiveAcceptance {
    private LiveAcceptance() {}

    /** The insufficiency reason each windowed injection kind causes. */
    static final Map<String, String> REASONS = Map.of("bad-status", "bad-share-exceeded", "kill-muninn", "session-gap", "host-offline", "missing-periods");
    static final Set<String> UNWINDOWED = Set.of("bridge-hold", "bridge-source-offset");

    /** One visit as the core's last revision states it. */
    record Visit(String ref, String station, long enter, Long leave, String status, List<String> reasons) {}

    /** The acceptance section of the compare report; {@code ok} is its verdict. */
    static ObjectNode check(Recording rec, Map<String, RunFold.Rev> workRecords) {
        JsonNode source = rec.manifest().path("source");
        ObjectNode r = Json.object();
        if (!source.path("capture").isObject()) return r.put("applies", false).put("ok", true);
        r.put("applies", true);
        List<String> problems = new ArrayList<>();
        JsonNode injections = source.get("injections");
        if (injections == null || !injections.isArray()) {
            problems.add("the capture declares no source.injections list");
            injections = Json.JSON.createArrayNode();
        }
        List<Visit> visits = new ArrayList<>();
        for (Map.Entry<String, RunFold.Rev> e : workRecords.entrySet()) {
            JsonNode b = e.getValue().node();
            List<String> reasons = new ArrayList<>();
            b.path("insufficiencyReasons").forEach(x -> reasons.add(x.asText()));
            JsonNode in = b.path("interval");
            visits.add(new Visit(ref(e.getKey()), b.path("readPoint").asText(), in.path("enter").asLong(), in.path("leave").isNumber() ? in.path("leave").asLong() : null,
                b.path("status").asText(), List.copyOf(reasons)));
        }
        r.set("problems", Json.JSON.createArrayNode());
        ArrayNode declared = r.putArray("injections");
        injections.forEach(i -> declared.add(i.path("injection").asText() + " " + i.path("kind").asText()));
        problems.addAll(judge(rec.t0(), injections, visits, r.putArray("visits")));
        for (JsonNode i : injections)
            if ("host-offline".equals(i.path("kind").asText())) problems.addAll(staleDeath(sessions(rec), i.path("equipment").asText()));
        ArrayNode p = (ArrayNode) r.get("problems");
        problems.forEach(p::add);
        r.put("ok", problems.isEmpty());
        return r;
    }

    /** The visit judgement: every visit's status against the injections that touch it, each visit listed in {@code out}. */
    static List<String> judge(long t0, JsonNode injections, List<Visit> visits, ArrayNode out) {
        List<String> problems = new ArrayList<>();
        for (JsonNode i : injections) {
            String kind = i.path("kind").asText();
            if (!REASONS.containsKey(kind) && !UNWINDOWED.contains(kind)) problems.add("injection " + i.path("injection").asText() + ": unknown kind " + kind);
        }
        if (visits.isEmpty()) problems.add("the core wrote no work record");
        for (Visit v : visits) {
            List<String> touchedBy = new ArrayList<>();
            SortedSet<String> needed = new TreeSet<>();
            for (JsonNode i : injections) {
                String reason = REASONS.get(i.path("kind").asText());
                if (reason == null || !v.station().equals(i.path("station").asText())) continue;
                long from = t0 + i.path("from").asLong(), to = t0 + i.path("to").asLong();
                if (from < (v.leave() == null ? Long.MAX_VALUE : v.leave()) && v.enter() < to) {
                    touchedBy.add(i.path("injection").asText() + " " + i.path("kind").asText());
                    needed.add(reason);
                }
            }
            ObjectNode n = out.addObject().put("visit", v.ref()).put("station", v.station()).put("status", v.status());
            ArrayNode rs = n.putArray("reasons");
            v.reasons().forEach(rs::add);
            ArrayNode tb = n.putArray("touchedBy");
            touchedBy.forEach(tb::add);
            if (touchedBy.isEmpty()) {
                if (!"complete".equals(v.status())) problems.add(v.ref() + " at " + v.station() + " is " + v.status() + " " + v.reasons() + ", touched by no injection");
            } else if (!"insufficient".equals(v.status()) || !v.reasons().containsAll(needed)) {
                problems.add(v.ref() + " at " + v.station() + " is " + v.status() + " " + v.reasons() + ", touched by " + touchedBy + " (needs insufficient with " + needed + ")");
            }
        }
        return problems;
    }

    /** Injection 25(a) in the captured session events of {@code device}: a stale-death, no session-down of the session it ended, and
     *  a session-up of a newer session. */
    static List<String> staleDeath(List<JsonNode> sessions, String device) {
        List<String> problems = new ArrayList<>();
        List<JsonNode> stale = new ArrayList<>();
        for (JsonNode s : sessions) if (device.equals(s.path("device").asText()) && "stale-death".equals(s.path("kind").asText())) stale.add(s);
        if (stale.isEmpty()) { problems.add("host-offline " + device + ": no stale-death was captured"); return problems; }
        for (JsonNode d : stale) {
            long birth = d.path("birthPublishTs").asLong(), bdSeq = d.path("bdSeq").asLong();
            boolean down = false, newer = false;
            for (JsonNode s : sessions) {
                if (!device.equals(s.path("device").asText())) continue;
                String kind = s.path("kind").asText();
                if ("session-down".equals(kind) && s.path("birthPublishTs").asLong() == birth && s.path("bdSeq").asLong() == bdSeq) down = true;
                if ("session-up".equals(kind) && s.path("birthPublishTs").asLong() > birth) newer = true;
            }
            if (down) problems.add("host-offline " + device + ": the session that stale-died (bdSeq " + bdSeq + ") also has a session-down");
            if (!newer) problems.add("host-offline " + device + ": no newer session came up after the stale-death (bdSeq " + bdSeq + ")");
        }
        return problems;
    }

    /** Every captured {@code ot.sessions} record, partition by partition. */
    static List<JsonNode> sessions(Recording rec) {
        List<JsonNode> l = new ArrayList<>();
        for (int p : new TreeSet<>(rec.routing().subscribed("ot.sessions"))) for (Recording.Record x : rec.records("ot.sessions", p)) l.add(x.value());
        return l;
    }

    /** A work record's key as the scenario names the visit: the arrival event without {@code -arrive} (occupancy-join) or the visitId
     *  (plc-stamped). */
    static String ref(String keyText) {
        JsonNode k = Json.read(keyText.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        JsonNode ref = k.has("visitRef") ? k.get("visitRef") : k;
        if (ref.hasNonNull("arrivalEventId")) {
            String a = ref.get("arrivalEventId").asText();
            return a.endsWith("-arrive") ? a.substring(0, a.length() - "-arrive".length()) : a;
        }
        if (ref.hasNonNull("visitId")) return ref.get("visitId").asText();
        return keyText;
    }
}
