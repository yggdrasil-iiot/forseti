package yggdrasil.forseti.core.stage2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.Inputs;

/** Rule sheet T8: the affected set's {@code coverage} object (spec 393-402 as fixed by Task 1's lines 392-398), in the spec's field order
 *  {@code status, unattributedActivityPeriods, unknownActivityPeriods, provisionalUnits, insufficientUnits, sessionGapOverlaps, open,
 *  settled, beyondRetention, retentionGap}. Pure: it reads its arguments only. */
public final class SetCoverage {
    /** One sample row of a scoped device's activity member as stage 1 wrote it (S9 body fields). */
    public record ActivityRow(long sourceTs, String state, String reason, JsonNode active) {
        /** Unattributed activity (spec 368): unattributed with reason {@code no-occupancy} or {@code ambiguous} and {@code active true}. */
        boolean unattributedActivity() {
            return "unattributed".equals(state) && ("no-occupancy".equals(reason) || "ambiguous".equals(reason)) && active.isBoolean() && active.booleanValue();
        }
        /** An accepted-quality activity observation: a boolean {@code active} (S9: the accepted-quality value of the member) on an
         *  attributed or unattributed row whose reason is none of {@code bad-quality}, {@code session-ambiguous}, {@code outside-session}. */
        boolean accepted() {
            if (!active.isBoolean()) return false;
            if (!"attributed".equals(state) && !"unattributed".equals(state)) return false;
            return !"bad-quality".equals(reason) && !"session-ambiguous".equals(reason) && !"outside-session".equals(reason);
        }
    }
    /** The verified telemetry range of one device, half-open {@code [from, to)} (spec 402; the recording manifest's {@code evidenceRange}). */
    public record EvidenceRange(long from, long to) {
        public EvidenceRange { if (to < from) throw new IllegalArgumentException("evidence range to < from"); }
        boolean covers(long a, long b) { return b <= a || (from <= a && b <= to); }
    }
    /** One live unit row of the set: its assessment status and reason. */
    public record Unit(String status, String reason) {}
    /** The sample-derived evidence of the station: activity rows per device, the last delivered observation per device, the verified ranges. */
    public record Evidence(Map<String, List<ActivityRow>> activity, Map<String, Long> lastObservation, Map<String, EvidenceRange> ranges) {}

    private SetCoverage() {}

    /** Slots of width {@code p} from {@code from} over {@code [from, end)}; the last partial slot counts. */
    public static long slots(long from, long end, long p) { return end <= from ? 0 : (end - from + p - 1) / p; }

    public static ObjectNode compute(Inputs.IncidentEvent inc, List<String> scoped, Evidence ev, Long w2, long p, long allowedLatenessMs, long workRecordsMs,
                                     Collection<Unit> units, long sessionGapOverlaps, boolean sessionAmbiguous) {
        boolean withdrawn = "withdrawn".equals(inc.status());
        long sf = inc.suspectFrom();
        long end;
        if (inc.suspectTo() != null) end = inc.suspectTo();
        else {
            Long last = null;
            for (String d : scoped) { Long x = ev.lastObservation().get(d); if (x != null && (last == null || x > last)) last = x; }
            end = last == null ? sf : Math.max(sf, last + p);
        }
        long n = slots(sf, end, p), unattributed = 0, unknown = 0;
        for (String d : scoped) {
            Set<Long> ua = new HashSet<>(), known = new HashSet<>();
            for (ActivityRow r : ev.activity().getOrDefault(d, List.of())) {
                if (r.sourceTs() < sf || r.sourceTs() >= end) continue;
                long slot = (r.sourceTs() - sf) / p;
                if (r.unattributedActivity()) ua.add(slot);
                if (r.accepted()) known.add(slot);
            }
            known.addAll(ua);
            unattributed += ua.size();
            unknown += n - known.size();
        }
        long provisional = 0, insufficient = 0;
        for (Unit u : units) {
            if ("basis-provisional".equals(u.reason())) provisional++;
            else if ("insufficient-evidence".equals(u.status())) insufficient++;
        }
        boolean open = !withdrawn && "open".equals(inc.status());
        boolean settled = withdrawn || (inc.suspectTo() != null && w2 != null && w2 >= inc.suspectTo() + allowedLatenessMs);
        boolean beyond = sf < inc.revisionEventTime() - workRecordsMs;
        long reqEnd = inc.suspectTo() != null ? inc.suspectTo() : w2 == null ? sf : w2;
        boolean gap = false;
        for (String d : scoped) { EvidenceRange r = ev.ranges().get(d); if (r == null || !r.covers(sf, reqEnd)) gap = true; }
        String status;
        if (unknown > 0 || sessionAmbiguous) status = "unknown";
        else if (unattributed > 0 || provisional > 0 || insufficient > 0 || sessionGapOverlaps > 0 || open || !settled || beyond || gap) status = "incomplete";
        else status = withdrawn ? "incomplete" : "complete";   // a withdrawn incident is never complete
        ObjectNode c = Json.object().put("status", status);
        Json.putSafe(c, "unattributedActivityPeriods", unattributed);
        Json.putSafe(c, "unknownActivityPeriods", unknown);
        Json.putSafe(c, "provisionalUnits", provisional);
        Json.putSafe(c, "insufficientUnits", insufficient);
        Json.putSafe(c, "sessionGapOverlaps", sessionGapOverlaps);
        return c.put("open", open).put("settled", settled).put("beyondRetention", beyond).put("retentionGap", gap);
    }
}
