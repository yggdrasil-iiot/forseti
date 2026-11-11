package yggdrasil.forseti.core.stage1;

import java.util.ArrayList;
import java.util.List;

/** The window rule of rule sheet S9 (spec 492-494): candidates are the station's visits whose window {@code [enter - tolE, upper)}
 *  contains t, whatever their status; one non-retracted candidate wins; two or more make the sample ambiguous; with none non-retracted, the
 *  retracted candidate whose actual {@code [enter, leave)} contains t wins, and none or several are ambiguous; no candidate is no-occupancy.
 *  The same rule, evaluated before the quality, finiteness and session steps, is the quality-blind candidate coverage counts by. */
final class Attribution {
    enum Kind { ATTRIBUTED, AMBIGUOUS, NO_OCCUPANCY }
    record Candidate(Kind kind, VisitClaim visit) {}

    private final List<VisitClaim> visits;
    private final long enterToleranceMs, leaveToleranceMs;

    Attribution(List<VisitClaim> visits, long enterToleranceMs, long leaveToleranceMs) {
        this.visits = visits; this.enterToleranceMs = enterToleranceMs; this.leaveToleranceMs = leaveToleranceMs;
    }

    Candidate candidate(long t) {
        List<VisitClaim> in = new ArrayList<>(), open = new ArrayList<>();
        for (VisitClaim v : visits) {
            Long upper = v.windowUpper(leaveToleranceMs);
            if (t >= v.enter - enterToleranceMs && (upper == null || t < upper)) { in.add(v); if (!v.retracted()) open.add(v); }
        }
        if (in.isEmpty()) return new Candidate(Kind.NO_OCCUPANCY, null);
        if (open.size() == 1) return new Candidate(Kind.ATTRIBUTED, open.get(0));
        if (open.size() > 1) return new Candidate(Kind.AMBIGUOUS, null);
        VisitClaim inside = null;
        int n = 0;
        for (VisitClaim v : in) if (v.enter <= t && t < v.actualEnd()) { inside = v; n++; }
        return n == 1 ? new Candidate(Kind.ATTRIBUTED, inside) : new Candidate(Kind.AMBIGUOUS, null);
    }
}
