package yggdrasil.forseti.core.stage2;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.core.stage2.Relatedness.Kind.*;

import org.junit.jupiter.api.Test;

/** Rule sheet T4 (spec 517): V = [enter, leave), S = [suspectFrom, suspectTo), null for an open end. */
class RelatednessTest {
    static final String ANY = Relatedness.ANY_OVERLAP, FULLY = Relatedness.FULLY_WITHIN, ENTER = Relatedness.ENTER_WITHIN;

    @Test void anyOverlapWithBothEndsKnown() {
        assertEquals(RELATED, Relatedness.of(ANY, 100, 200L, 150, 300L));
        assertEquals(RELATED, Relatedness.of(ANY, 100, 200L, 0, 101L));
        assertEquals(RELATED, Relatedness.of(ANY, 100, 200L, 120, 130L));
        assertEquals(UNRELATED, Relatedness.of(ANY, 100, 200L, 300, 400L));
        assertEquals(UNRELATED, Relatedness.of(ANY, 300, 400L, 100, 200L));
    }

    @Test void fullyWithin() {
        assertEquals(RELATED, Relatedness.of(FULLY, 100, 200L, 100, 200L));
        assertEquals(UNRELATED, Relatedness.of(FULLY, 100, 201L, 100, 200L));
        assertEquals(UNRELATED, Relatedness.of(FULLY, 99, 150L, 100, 200L));
        assertEquals(POSSIBLY_RELATED, Relatedness.of(FULLY, 100, null, 100, 200L), "open visit entering inside: its leave may still fall inside");
        assertEquals(UNRELATED, Relatedness.of(FULLY, 200, null, 100, 200L), "open visit entering at suspectTo can never lie within");
        assertEquals(POSSIBLY_RELATED, Relatedness.of(FULLY, 150, 900L, 100, null), "open incident: suspectTo may still reach the leave");
        assertEquals(POSSIBLY_RELATED, Relatedness.of(FULLY, 150, null, 100, null));
        assertEquals(UNRELATED, Relatedness.of(FULLY, 50, null, 100, null), "the known enter is before suspectFrom");
    }

    @Test void enterWithinIsNeverPossiblyRelated() {
        assertEquals(RELATED, Relatedness.of(ENTER, 100, 900L, 100, 200L));
        assertEquals(RELATED, Relatedness.of(ENTER, 199, null, 100, 200L));
        assertEquals(UNRELATED, Relatedness.of(ENTER, 99, 150L, 100, 200L));
        assertEquals(RELATED, Relatedness.of(ENTER, 5000, null, 100, null));
        assertEquals(UNRELATED, Relatedness.of(ENTER, 50, null, 100, null));
    }

    @Test void anyOverlapWithOpenEnds() {
        assertEquals(RELATED, Relatedness.of(ANY, 50, 150L, 100, null), "closed visit, open incident: related iff leave > suspectFrom");
        assertEquals(UNRELATED, Relatedness.of(ANY, 50, 100L, 100, null));
        assertEquals(RELATED, Relatedness.of(ANY, 100, null, 100, 200L), "open visit entering at or after suspectFrom and before suspectTo");
        assertEquals(POSSIBLY_RELATED, Relatedness.of(ANY, 50, null, 100, 200L), "open visit entering before suspectFrom may still reach it");
        assertEquals(POSSIBLY_RELATED, Relatedness.of(ANY, 50, null, 100, null));
        assertEquals(RELATED, Relatedness.of(ANY, 150, null, 100, null));
    }

    @Test void enterEqualToSuspectToIsUnrelated() {
        assertEquals(UNRELATED, Relatedness.of(ANY, 200, 300L, 100, 200L));
        assertEquals(UNRELATED, Relatedness.of(ANY, 200, null, 100, 200L));
        assertEquals(UNRELATED, Relatedness.of(ENTER, 200, 300L, 100, 200L));
    }

    @Test void leaveEqualToSuspectFromIsUnrelated() {
        assertEquals(UNRELATED, Relatedness.of(ANY, 0, 100L, 100, 200L));
        assertEquals(UNRELATED, Relatedness.of(ANY, 0, 100L, 100, null));
        assertEquals("possibly-related", POSSIBLY_RELATED.wire());
    }
}
