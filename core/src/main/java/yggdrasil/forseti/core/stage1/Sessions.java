package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import yggdrasil.forseti.core.stage1.Coverage.Interval;

/** Session intervals per device (rule sheet S8; spec 68, 271-278, 502): {@code seq-gap {from, to}} is a gap {@code [from, to)};
 *  {@code session-down {from}} is a gap {@code [from, next session-up at)}, or {@code [from, W1)} while open; {@code stale-death} opens
 *  nothing; {@code session-ambiguous {detectedEventTime}} is an unknown interval from the device's last observation before
 *  detectedEventTime to the next session-up, or W1 while none; {@code definition-changed} marks its session. A session event in conflict
 *  is withdrawn. With no W1 yet an open interval is unbounded. */
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
        Map<String, List<Long>> ups = new HashMap<>();
        List<Stage1State.Session> live = new ArrayList<>();
        for (Stage1State.Session x : s.sessions.values()) {
            if (x.conflicted()) continue;
            live.add(x);
            if (x.kind.equals("session-up")) { Long at = longOrNull(x.payload().get("at")); if (at != null) ups.computeIfAbsent(x.device, k -> new ArrayList<>()).add(at); }
        }
        Map<String, List<Interval>> down = new TreeMap<>(), gaps = new TreeMap<>(), unknown = new TreeMap<>();
        Set<String> changed = new TreeSet<>();
        for (Stage1State.Session x : live) {
            JsonNode p = x.payload();
            switch (x.kind) {
                case "seq-gap": {
                    Long from = longOrNull(p.get("from")), to = longOrNull(p.get("to"));
                    if (from != null && to != null && to > from) add(gaps, x.device, new Interval(from, to));
                    break;
                }
                case "session-down": {
                    Long from = longOrNull(p.get("from"));
                    if (from == null) break;
                    long end = nextUp(ups.get(x.device), from, open);
                    if (end > from) { add(down, x.device, new Interval(from, end)); add(gaps, x.device, new Interval(from, end)); }
                    break;
                }
                case "session-ambiguous": {
                    Long det = longOrNull(p.get("detectedEventTime"));
                    if (det == null) break;
                    long start = det;
                    for (Stage1State.Measurement m : s.measurements.values())
                        if (m.device.equals(x.device) && m.sourceTs < det && (start == det || m.sourceTs > start)) start = m.sourceTs;
                    long end = nextUp(ups.get(x.device), det, open);
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

    private static long nextUp(List<Long> ups, long from, long open) {
        long best = open;
        if (ups != null) for (long at : ups) if (at >= from && at < best) best = at;
        return best;
    }
    private static void add(Map<String, List<Interval>> m, String device, Interval i) { m.computeIfAbsent(device, k -> new ArrayList<>()).add(i); }
    static boolean inside(List<Interval> l, long t) { for (Interval i : l) if (i.contains(t)) return true; return false; }
    static Long longOrNull(JsonNode n) { return n == null || !n.isIntegralNumber() ? null : n.longValue(); }
}
