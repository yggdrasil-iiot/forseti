package yggdrasil.forseti.core.stage1;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

class DeadlineIndexTest {
    static JsonNode key(String a) { return Json.object().put("mode", "occupancy-join").put("source", "mes-line2").put("arrivalEventId", a); }
    static List<String> purposes(List<DeadlineIndex.Deadline> l) { return l.stream().map(d -> d.key().get("arrivalEventId").asText() + "/" + d.purpose()).toList(); }

    @Test void aDeadlineIsDueWhenWReachesItInclusively() {
        DeadlineIndex ix = new DeadlineIndex();
        ix.register(1000, key("a"), DeadlineIndex.SETTLE);
        assertEquals(List.of(), ix.due(999));
        assertEquals(1, ix.due(1000).size());
        assertEquals(1, ix.due(1001).size());
        assertTrue(ix.remove(1000, key("a"), DeadlineIndex.SETTLE));
        assertEquals(List.of(), ix.due(5000));
    }

    @Test void severalPurposesAtOneInstantFireInKeyOrder() {
        DeadlineIndex ix = new DeadlineIndex();
        ix.register(1000, key("b"), DeadlineIndex.SETTLE);
        ix.register(1000, key("a"), DeadlineIndex.SETTLE);
        ix.register(1000, key("a"), DeadlineIndex.EXPIRY);
        ix.register(1000, key("a"), DeadlineIndex.MISSING_DEPARTURE);
        ix.register(900, key("c"), DeadlineIndex.SETTLE);
        assertEquals(List.of("c/settle", "a/expiry", "a/missing-departure", "a/settle", "b/settle"), purposes(ix.due(1000)));
    }

    @Test void cancellationAndReplacementRemoveObsoleteRegistrations() {
        DeadlineIndex ix = new DeadlineIndex();
        ix.register(1000, key("a"), DeadlineIndex.SETTLE);
        ix.register(2000, key("a"), DeadlineIndex.SETTLE);   // replaces the obsolete 1000
        assertEquals(List.of(), ix.due(1500));
        assertFalse(ix.remove(1000, key("a"), DeadlineIndex.SETTLE));
        ix.register(3000, key("a"), DeadlineIndex.EXPIRY);
        ix.cancel(key("a"), DeadlineIndex.SETTLE);
        assertEquals(List.of("a/expiry"), purposes(ix.due(5000)));
        ix.cancelAll(key("a"));
        assertEquals(0, ix.size());
    }

    @Test void everyExpiredDeadlineIsReturnedAfterALargeJump() {
        DeadlineIndex ix = new DeadlineIndex();
        for (int i = 0; i < 50; i++) ix.register(1000L * i, key("v" + i), DeadlineIndex.SETTLE);
        ix.register(Long.parseLong("9007199254740991"), key("late"), DeadlineIndex.EXPIRY);
        List<DeadlineIndex.Deadline> due = ix.due(86_400_000L);
        assertEquals(50, due.size());
        for (int i = 1; i < due.size(); i++) assertTrue(due.get(i - 1).deadline() <= due.get(i).deadline());
    }

    @Test void theIndexRoundTripsThroughJson() {
        DeadlineIndex ix = new DeadlineIndex();
        ix.register(1794183780000L, key("occ-1"), DeadlineIndex.SETTLE);
        ix.register(1794269400000L, key("occ-1"), DeadlineIndex.EXPIRY);
        ix.register(1794269400000L, key("occ-3"), DeadlineIndex.MISSING_DEPARTURE);
        DeadlineIndex back = DeadlineIndex.fromJson(Json.read(Canonical.bytes(ix.toJson())));
        assertEquals(ix.all(), back.all());
        assertArrayEquals(Canonical.bytes(ix.toJson()), Canonical.bytes(back.toJson()));
    }
}
