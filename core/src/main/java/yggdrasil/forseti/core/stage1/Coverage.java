package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.*;
import yggdrasil.forseti.core.Json;

/** Slot coverage and activity support over an evaluation interval {@code J = [enter, end)} (spec 351-353, rule sheet S6). Slots have width
 *  P from {@code enter}; the last partial slot counts. Inputs are the coverage observations of one member inside J. */
public final class Coverage {
    private Coverage() {}

    /** One coverage observation: its sourceTs, raw Bad (severity bits), accepted quality (Good, or Uncertain under propagate), value. */
    public record Obs(long t, boolean bad, boolean accepted, JsonNode value) {}
    public record Counts(long observedPeriods, long missingPeriods, long badSlots) {
        public double badShare() { return observedPeriods == 0 ? 0.0 : (double) badSlots / observedPeriods; }
    }
    public record Interval(long start, long end) {
        public ArrayNode toJson() { ArrayNode a = Json.JSON.createArrayNode(); a.add(start); a.add(end); return a; }
        public boolean overlaps(long s, long e) { return start < e && s < end; }
        public boolean contains(long t) { return start <= t && t < end; }
    }
    public record Activity(List<Interval> active, List<Interval> inactive, List<Interval> unknown, long unknownPeriods) {}

    public static long slots(long enter, long end, long p) { return end <= enter ? 0 : (end - enter + p - 1) / p; }
    public static long slot(long t, long enter, long p) { return Math.floorDiv(t - enter, p); }

    /** observedPeriods, missingPeriods and badSlots of one member over J; observations outside J are ignored. */
    public static Counts counts(Collection<Obs> obs, long enter, long end, long p) {
        Set<Long> seen = new HashSet<>(), bad = new HashSet<>();
        for (Obs o : obs) {
            if (o.t() < enter || o.t() >= end) continue;
            long s = slot(o.t(), enter, p);
            seen.add(s);
            if (o.bad()) bad.add(s);
        }
        return new Counts(seen.size(), slots(enter, end, p) - seen.size(), bad.size());
    }

    /** The coverage reasons of the required members: {@code missing-periods} (missingPeriods above the maximum), {@code no-observed-periods}
     *  (none observed), {@code bad-share-exceeded} (bad share above maxBadShare). */
    public static SortedSet<String> memberReasons(Collection<Counts> required, long maxMissingPeriods, double maxBadShare) {
        SortedSet<String> r = new TreeSet<>();
        for (Counts c : required) {
            if (c.missingPeriods() > maxMissingPeriods) r.add("missing-periods");
            if (c.observedPeriods() == 0) r.add("no-observed-periods");
            if (c.badShare() > maxBadShare) r.add("bad-share-exceeded");
        }
        return r;
    }

    /** Activity support of one equipment over J: an observation at t supports {@code [t, min(next observation, t + P))}; an accepted true is
     *  active, an accepted false inactive; Bad, rejected Uncertain and unsupported time are unknown, and so is every part of
     *  {@code unknownSession}. Supports are clipped to J; only adjacent intervals of the same kind are merged. */
    public static Activity activity(Collection<Obs> observations, long enter, long end, long p, List<Interval> unknownSession) {
        List<Obs> obs = new ArrayList<>(observations);
        obs.sort(Comparator.comparingLong(Obs::t));
        List<Interval> active = new ArrayList<>(), inactive = new ArrayList<>();
        for (int i = 0; i < obs.size(); i++) {
            Obs o = obs.get(i);
            long until = o.t() + p;
            if (i + 1 < obs.size()) until = Math.min(until, obs.get(i + 1).t());
            if (!o.accepted() || o.value() == null || !o.value().isBoolean()) continue;
            long s = Math.max(o.t(), enter), e = Math.min(until, end);
            if (s >= e) continue;
            append(o.value().booleanValue() ? active : inactive, new Interval(s, e));
        }
        List<Interval> session = new ArrayList<>(unknownSession);
        session.sort(Comparator.comparingLong(Interval::start));
        active = subtract(active, session);
        inactive = subtract(inactive, session);
        List<Interval> known = new ArrayList<>(active);
        known.addAll(inactive);
        known.sort(Comparator.comparingLong(Interval::start));
        List<Interval> unknown = subtract(end > enter ? List.of(new Interval(enter, end)) : List.of(), known);
        return new Activity(List.copyOf(active), List.copyOf(inactive), List.copyOf(unknown), slotsOverlapping(unknown, enter, p));
    }

    /** The number of distinct slots overlapping any of the (sorted, disjoint) parts. */
    public static long slotsOverlapping(List<Interval> parts, long enter, long p) {
        long count = 0, lastSlot = Long.MIN_VALUE;
        for (Interval i : parts) {
            if (i.end() <= i.start()) continue;
            long a = slot(i.start(), enter, p), b = slot(i.end() - 1, enter, p);
            if (lastSlot != Long.MIN_VALUE && a <= lastSlot) a = lastSlot + 1;
            if (b >= a) count += b - a + 1;
            lastSlot = Math.max(lastSlot, b);
        }
        return count;
    }

    private static void append(List<Interval> l, Interval i) {
        if (!l.isEmpty() && l.get(l.size() - 1).end() == i.start()) l.set(l.size() - 1, new Interval(l.get(l.size() - 1).start(), i.end()));
        else l.add(i);
    }

    /** {@code a} minus every interval of {@code b}; both sorted by start, {@code a} disjoint. */
    static List<Interval> subtract(List<Interval> a, List<Interval> b) {
        if (b.isEmpty()) return a;
        List<Interval> out = new ArrayList<>();
        for (Interval x : a) {
            long s = x.start();
            for (Interval y : b) {
                if (y.end() <= s || y.start() >= x.end()) continue;
                if (y.start() > s) out.add(new Interval(s, y.start()));
                s = Math.max(s, y.end());
                if (s >= x.end()) break;
            }
            if (s < x.end()) out.add(new Interval(s, x.end()));
        }
        return out;
    }

    public static ArrayNode intervals(List<Interval> l) { ArrayNode a = Json.JSON.createArrayNode(); l.forEach(i -> a.add(i.toJson())); return a; }
}
