package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import yggdrasil.forseti.core.stage1.Coverage.Interval;

/** Session intervals per device (rule sheet S8; spec 68, 271-278, 502, 515-517): {@code seq-gap {from, to}} is a gap
 *  {@code [min(from, to), max(from, to) + 1)}, every instant between its bounds inclusive, so equal or reversed bounds still give one;
 *  {@code session-down {from}} is a gap {@code [from, end)}, where {@code end} is the {@code at} of the first delivered session-up of a
 *  different session identity (edge, device, birthPublishTs, bdSeq) with {@code at >= max(from, the down's birthPublishTs)}, whatever the
 *  station's W1; W1 ends it only while no such session-up is delivered, and the dead session's own session-up never ends it.
 *  {@code stale-death} opens nothing; {@code session-ambiguous {detectedEventTime}} is an unknown interval from the device's last
 *  observation before detectedEventTime to the end found by the same rule from detectedEventTime (excluding the ambiguous event's own
 *  session identity); {@code definition-changed} marks its session. An interval whose end is not after its start is empty and dropped.
 *  A session event in conflict is withdrawn. With no W1 yet an open interval is unbounded. */
final class Sessions {
    record View(Map<String, List<Interval>> down, Map<String, List<Interval>> gaps, Map<String, List<Interval>> unknown, Set<String> definitionChanged) {
        List<Interval> down(String device) { return down.getOrDefault(device, List.of()); }
        List<Interval> gaps(String device) { return gaps.getOrDefault(device, List.of()); }
        List<Interval> unknown(String device) { return unknown.getOrDefault(device, List.of()); }
        boolean any() { return !down.isEmpty() || !gaps.isEmpty() || !unknown.isEmpty(); }
    }

    private Sessions() {}

    /** A birthless session event (null birthPublishTs, plan D4) keys as {@code null}, which no message's integer birth can equal. */
    static String sessionKey(String edge, String device, Long birthPublishTs, long bdSeq) { return edge + "\u0000" + device + "\u0000" + birthPublishTs + "\u0000" + bdSeq; }

    static View view(Stage1State s, Long w) {
        long open = w == null ? Long.MAX_VALUE : w;
        Map<String, List<Up>> ups = new HashMap<>();
        List<Stage1State.Session> live = new ArrayList<>();
        for (Stage1State.Session x : s.sessions.values()) {
            if (x.conflicted()) continue;
            live.add(x);
            if (x.kind.equals("session-up")) { Long at = longOrNull(x.payload().get("at")); if (at != null) ups.computeIfAbsent(x.device, k -> new ArrayList<>()).add(new Up(x.sessionKey, at)); }
        }
        Map<String, List<Interval>> down = new TreeMap<>(), gaps = new TreeMap<>(), unknown = new TreeMap<>();
        Set<String> changed = new TreeSet<>();
        for (Stage1State.Session x : live) {
            JsonNode p = x.payload();
            switch (x.kind) {
                case "seq-gap": {
                    Long from = longOrNull(p.get("from")), to = longOrNull(p.get("to"));
                    // closed and reversal-normalised: every instant from the earlier to the later bound, never dropped (spec 273, 515)
                    if (from != null && to != null) add(gaps, x.device, new Interval(Math.min(from, to), Math.max(from, to) + 1));
                    break;
                }
                case "session-down": {
                    Long from = longOrNull(p.get("from"));
                    if (from == null) break;
                    long end = nextUp(ups.get(x.device), x.sessionKey, Math.max(from, birthOf(x)), open);
                    if (end > from) { add(down, x.device, new Interval(from, end)); add(gaps, x.device, new Interval(from, end)); }
                    break;
                }
                case "session-ambiguous": {
                    Long det = longOrNull(p.get("detectedEventTime"));
                    if (det == null) break;
                    long start = det;
                    for (Stage1State.Measurement m : s.measurements.values())
                        if (m.device.equals(x.device) && m.sourceTs < det && (start == det || m.sourceTs > start)) start = m.sourceTs;
                    long end = nextUp(ups.get(x.device), x.sessionKey, Math.max(det, birthOf(x)), open);
                    if (end > start) add(unknown, x.device, new Interval(start, end));
                    break;
                }
                case "definition-changed": changed.add(x.sessionKey); break;
                default: break;   // session-up closes intervals; stale-death opens nothing
            }
        }
        for (List<Interval> l : down.values()) l.sort(Comparator.comparingLong(Interval::start));
        for (List<Interval> l : gaps.values()) l.sort(Comparator.comparingLong(Interval::start));
        for (List<Interval> l : unknown.values()) l.sort(Comparator.comparingLong(Interval::start));
        return new View(down, gaps, unknown, changed);
    }

    private record Up(String sessionKey, long at) {}

    /** The end of a session-down or unknown interval: the {@code at} of the first session-up of a session other than {@code own} with
     *  {@code at >= from}, whatever W1 ({@code open}); {@code open} only while there is none. Residual: a no-telemetry session and its
     *  successor born in the same millisecond with the same bdSeq share one identity, so the interval stays open (fail-safe). */
    private static long nextUp(List<Up> ups, String own, long from, long open) {
        Long best = null;
        if (ups != null) for (Up u : ups) if (!u.sessionKey().equals(own) && u.at() >= from && (best == null || u.at() < best)) best = u.at();
        return best != null ? best : open;
    }
    /** The session's birthPublishTs, or {@code Long.MIN_VALUE} for a birthless event (plan D4). */
    private static long birthOf(Stage1State.Session x) { Long b = longOrNull(x.identity.get("birthPublishTs")); return b == null ? Long.MIN_VALUE : b; }
    private static void add(Map<String, List<Interval>> m, String device, Interval i) { m.computeIfAbsent(device, k -> new ArrayList<>()).add(i); }
    static boolean inside(List<Interval> l, long t) { for (Interval i : l) if (i.contains(t)) return true; return false; }
    static Long longOrNull(JsonNode n) { return n == null || !n.isIntegralNumber() ? null : n.longValue(); }
}
