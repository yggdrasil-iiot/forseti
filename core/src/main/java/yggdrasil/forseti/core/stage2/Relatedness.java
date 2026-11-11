package yggdrasil.forseti.core.stage2;

/** Rule sheet T4 (spec 517): how a visit interval {@code V = [enter, leave)} relates to an incident interval
 *  {@code S = [suspectFrom, suspectTo)} under the impact rule's {@code interval.overlap} mode; a null end is open. When an open end means
 *  the relation can be neither confirmed nor excluded the basis is {@code possibly-related} (counted as insufficient evidence). */
public final class Relatedness {
    public static final String ANY_OVERLAP = "any-overlap", FULLY_WITHIN = "fully-within", ENTER_WITHIN = "enter-within";

    public enum Kind {
        RELATED("related"), POSSIBLY_RELATED("possibly-related"), UNRELATED("unrelated");
        private final String wire;
        Kind(String wire) { this.wire = wire; }
        public String wire() { return wire; }
    }

    private Relatedness() {}

    public static Kind of(String mode, long enter, Long leave, long suspectFrom, Long suspectTo) {
        switch (mode) {
            case ANY_OVERLAP: return anyOverlap(enter, leave, suspectFrom, suspectTo);
            case FULLY_WITHIN: return fullyWithin(enter, leave, suspectFrom, suspectTo);
            case ENTER_WITHIN: return suspectFrom <= enter && (suspectTo == null || enter < suspectTo) ? Kind.RELATED : Kind.UNRELATED;
            default: throw new IllegalArgumentException("not an interval.overlap mode: " + mode);
        }
    }

    private static Kind anyOverlap(long enter, Long leave, long sf, Long st) {
        if (leave != null && st != null) return enter < st && leave > sf ? Kind.RELATED : Kind.UNRELATED;
        if (leave != null) return leave > sf ? Kind.RELATED : Kind.UNRELATED;   // closed visit, open incident
        if (st != null && enter >= st) return Kind.UNRELATED;                    // open visit
        return enter >= sf ? Kind.RELATED : Kind.POSSIBLY_RELATED;
    }

    private static Kind fullyWithin(long enter, Long leave, long sf, Long st) {
        if (enter < sf) return Kind.UNRELATED;
        if (leave != null && st != null) return leave <= st ? Kind.RELATED : Kind.UNRELATED;
        if (leave == null && st != null) return enter < st ? Kind.POSSIBLY_RELATED : Kind.UNRELATED;   // leave > enter >= suspectTo excludes it
        return Kind.POSSIBLY_RELATED;   // suspectTo open: the known ends do not exclude it
    }
}
