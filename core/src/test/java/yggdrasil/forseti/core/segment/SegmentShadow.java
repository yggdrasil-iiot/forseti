package yggdrasil.forseti.core.segment;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import java.util.function.Function;
import yggdrasil.forseti.core.canon.Canonical;

/** A segment store kept the way an engine keeps it: after every step the state's {@code drainChanged()} is applied to a map (a full
 *  drain replaces it, written ids are put, removed ids deleted). Each check asserts that the map is byte for byte the state's
 *  {@code segments()} and that {@code fromSegments(map).toJson()} is the live state's {@code toJson()} (canonical bytes). A missed
 *  mark anywhere in a state's change paths leaves a stale segment in the map and fails the check at the step that made it. */
public final class SegmentShadow<S> {
    /** Steps checked by every shadow of the run. */
    public static long checks;

    private final Function<S, Segments.Drain> drain;
    private final Function<S, SortedMap<String, byte[]>> segments;
    private final Function<Map<String, byte[]>, S> restore;
    private final Function<S, ObjectNode> json;
    final TreeMap<String, byte[]> store = new TreeMap<>();
    public long steps, checked, drainedBytes, fullDrains;
    public final List<Long> drainedPerStep = new ArrayList<>();

    public SegmentShadow(Function<S, Segments.Drain> drain, Function<S, SortedMap<String, byte[]>> segments, Function<Map<String, byte[]>, S> restore,
                         Function<S, ObjectNode> json) {
        this.drain = drain; this.segments = segments; this.restore = restore; this.json = json;
    }

    /** Applies the state's drain to the store and checks it. */
    public Segments.Drain step(S state) { return step(state, true); }

    /** Applies the state's drain to the store; checks it when {@code check} (a long recording checks at its cut closes only: a missed
     *  mark leaves its segment stale until that segment is marked again, so a check per cut can miss what a later step of the same cut
     *  rewrote, which the per-step checks of the scenarios and the random sequences do not). */
    public Segments.Drain step(S state, boolean check) {
        Segments.Drain d = drain.apply(state);
        if (d.full()) { store.clear(); fullDrains++; }
        store.putAll(d.written());
        d.removed().forEach(store::remove);
        long n = d.writtenBytes();
        drainedBytes += n;
        drainedPerStep.add(n);
        steps++;
        if (check) { check(state); checks++; checked++; }
        return d;
    }

    public void check(S state) {
        SortedMap<String, byte[]> expected = segments.apply(state);
        assertEquals(expected.keySet(), store.keySet(), "the stored segment ids are the state's");
        for (Map.Entry<String, byte[]> e : expected.entrySet())
            assertArrayEquals(e.getValue(), store.get(e.getKey()), () -> "segment " + e.getKey() + " is stale: stored "
                + Canonical.utf8(store.get(e.getKey())) + "\n  live " + Canonical.utf8(e.getValue()));
        byte[] live = Canonical.bytes(json.apply(state)), restored = Canonical.bytes(json.apply(restore.apply(store)));
        assertArrayEquals(live, restored, "fromSegments(store).toJson() is the live toJson()");
    }

    public Map<String, byte[]> store() { return Collections.unmodifiableMap(store); }
}
