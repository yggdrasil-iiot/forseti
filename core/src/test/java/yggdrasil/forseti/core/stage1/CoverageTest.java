package yggdrasil.forseti.core.stage1;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.stage1.Coverage.Interval;
import yggdrasil.forseti.core.stage1.Coverage.Obs;

class CoverageTest {
    static final long P = 1000, END = 60_500;

    static List<Obs> everySecond(long from, long to) {
        List<Obs> l = new ArrayList<>();
        for (long t = from; t <= to; t += 1000) l.add(new Obs(t, false, true, new DoubleNode(1.0)));
        return l;
    }

    @Test void aPartialLastSlotCountsAndObservationsEverySecondCoverIt() {
        assertEquals(61, Coverage.slots(0, END, P));
        Coverage.Counts c = Coverage.counts(everySecond(0, 60_000), 0, END, P);
        assertEquals(new Coverage.Counts(61, 0, 0), c);
        assertEquals(Set.of(), Coverage.memberReasons(List.of(c), 3, 0.1));
    }

    @Test void aFourSlotHoleIsMissingPeriodsAtMaxThree() {
        List<Obs> obs = everySecond(0, 60_000);
        obs.removeIf(o -> o.t() >= 10_000 && o.t() < 14_000);
        Coverage.Counts c = Coverage.counts(obs, 0, END, P);
        assertEquals(57, c.observedPeriods());
        assertEquals(4, c.missingPeriods());
        assertEquals(Set.of("missing-periods"), Coverage.memberReasons(List.of(c), 3, 0.1));
        obs.add(new Obs(10_000, false, true, new DoubleNode(1.0)));
        assertEquals(Set.of(), Coverage.memberReasons(List.of(Coverage.counts(obs, 0, END, P)), 3, 0.1));
    }

    @Test void activitySupportAndUnknownPeriods() {
        List<Obs> obs = new ArrayList<>();
        for (long s = 0; s <= 60; s++) {
            long t = s * 1000;
            if (s == 30) obs.add(new Obs(t, true, false, BooleanNode.TRUE));
            else obs.add(new Obs(t, false, true, BooleanNode.valueOf(s >= 5 && s <= 54)));
        }
        Coverage.Activity a = Coverage.activity(obs, 0, END, P, List.of());
        assertEquals(List.of(new Interval(5000, 30000), new Interval(31000, 55000)), a.active());
        assertEquals(List.of(new Interval(0, 5000), new Interval(55000, 60500)), a.inactive());
        assertEquals(List.of(new Interval(30000, 31000)), a.unknown());
        assertEquals(1, a.unknownPeriods());
    }
}
