package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.cli.gate.RunFold.Disp;

/** The disposition groups the ground-truth check reads ({@link Expectations.Groups}): the same groups, in the fold's order within each
 *  group, as a map of (identity, part) to the fold's dispositions, and the same unlisted groups, without a key string per delivery. */
class ExpectationsGroupsTest {
    static Disp d(String identity, String part, long offset, String disposition) {
        return new Disp("forseti.dispositions-stage1", "ot.messages", 0, offset, part, identity, disposition, "r", 1, 1, "x");
    }

    @Test void groupsAreTheMapOfIdentityAndPartInTheFoldsOrder() {
        Random rnd = new Random(7);
        List<Disp> fold = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            String id = "{\"id\":" + rnd.nextInt(300) + "}", part = rnd.nextBoolean() ? "member:m" + rnd.nextInt(3) : "message";
            fold.add(d(id, part, i, rnd.nextInt(4) == 0 ? "duplicate" : "accepted"));
        }
        Map<String, List<Disp>> have = new LinkedHashMap<>(), byIdentity = new HashMap<>();
        for (Disp x : fold) {
            have.computeIfAbsent(x.identity() + "\u0000" + x.part(), k -> new ArrayList<>()).add(x);
            byIdentity.computeIfAbsent(x.identity(), k -> new ArrayList<>()).add(x);
        }
        Expectations.Groups g = new Expectations.Groups(fold);
        for (Map.Entry<String, List<Disp>> e : have.entrySet()) {
            String[] k = e.getKey().split("\u0000");
            assertEquals(e.getValue(), g.group(k[0], k[1]), e.getKey());
        }
        for (Map.Entry<String, List<Disp>> e : byIdentity.entrySet()) assertEquals(new HashSet<>(e.getValue()), new HashSet<>(g.identity(e.getKey())));
        assertNull(g.group("{\"id\":-1}", "message"));
        assertEquals(List.of(), g.identity("{\"id\":-1}"));
        // list half of the groups, and one key without a disposition
        Set<String> listed = new HashSet<>();
        int i = 0;
        for (String k : have.keySet()) if (i++ % 2 == 0) { String[] p = k.split("\u0000"); assertTrue(g.list(p[0], p[1])); listed.add(k); }
        assertFalse(g.list("{\"id\":-1}", "message"));
        for (String k : have.keySet()) { String[] p = k.split("\u0000"); assertEquals(listed.contains(k), g.listed(p[0], p[1]), k); }
        Set<String> unlisted = new HashSet<>();
        g.unlisted(l -> {
            String k = l.get(0).identity() + "\u0000" + l.get(0).part();
            assertEquals(have.get(k), l);
            assertTrue(unlisted.add(k), "reported twice: " + k);
        });
        Set<String> want = new HashSet<>(have.keySet());
        want.removeAll(listed);
        assertEquals(want, unlisted);
    }
}
