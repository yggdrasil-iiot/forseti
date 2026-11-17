package yggdrasil.forseti.core.segment;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

/** A map of a writer's state sorted by key text, whose every change marks the segment of the changed entry ({@link Segments.Dirty}).
 *  Reads go through unmodifiable views, so no change can bypass the marks; a change inside a stored value is reported with
 *  {@link #touch}. A {@code put} of a value equal to the stored one marks nothing. The segment of an entry is a function of its key and
 *  of fields of its value that never change ({@link #bucketed}), or one fixed segment ({@link #in}). */
public final class TrackedMap<V> {
    private final TreeMap<String, V> map = new TreeMap<>();
    private final NavigableMap<String, V> view = Collections.unmodifiableNavigableMap(map);
    private final String fixed;
    private final BiFunction<String, ? super V, String> segmentOf;
    private final HashMap<String, TreeSet<String>> bySegment;
    private final Segments.Dirty dirty;

    private TrackedMap(String fixed, BiFunction<String, ? super V, String> segmentOf, Segments.Dirty dirty) {
        this.fixed = fixed; this.segmentOf = segmentOf; this.dirty = Objects.requireNonNull(dirty);
        this.bySegment = segmentOf == null ? null : new HashMap<>();
    }

    /** Every entry in the one segment {@code segment}. */
    public static <V> TrackedMap<V> in(String segment, Segments.Dirty dirty) { return new TrackedMap<>(Objects.requireNonNull(segment), null, dirty); }
    /** Each entry in the segment {@code segmentOf(key, value)}. */
    public static <V> TrackedMap<V> bucketed(BiFunction<String, ? super V, String> segmentOf, Segments.Dirty dirty) { return new TrackedMap<>(null, Objects.requireNonNull(segmentOf), dirty); }

    // ---------------------------------------------------------------------------------------------------------------- reads

    public V get(String k) { return map.get(k); }
    public V getOrDefault(String k, V d) { return map.getOrDefault(k, d); }
    public boolean containsKey(String k) { return map.containsKey(k); }
    public int size() { return map.size(); }
    public boolean isEmpty() { return map.isEmpty(); }
    public Collection<V> values() { return view.values(); }
    public NavigableSet<String> keySet() { return view.navigableKeySet(); }
    public Set<Map.Entry<String, V>> entrySet() { return view.entrySet(); }
    public void forEach(BiConsumer<String, ? super V> f) { map.forEach(f); }
    /** A read-only view of the whole map. */
    public NavigableMap<String, V> asMap() { return view; }

    // ---------------------------------------------------------------------------------------------------------------- changes

    public V put(String k, V v) {
        Objects.requireNonNull(v);
        V old = map.put(k, v);
        if (old != null) {
            if (old.equals(v)) return old;   // an equal value: the same bytes
            unindex(k, old);
        }
        index(k, v);
        return old;
    }
    public V putIfAbsent(String k, V v) { V old = map.get(k); if (old == null) put(k, v); return old; }
    public V computeIfAbsent(String k, Function<String, ? extends V> f) {
        V x = map.get(k);
        if (x == null) { x = f.apply(k); put(k, x); }
        return x;
    }
    public V remove(String k) {
        V old = map.remove(k);
        if (old != null) unindex(k, old);
        return old;
    }
    /** Removes every entry the predicate holds for; the predicate reads an entry before it is removed. */
    public boolean removeIf(BiPredicate<String, ? super V> p) {
        boolean any = false;
        for (Iterator<Map.Entry<String, V>> it = map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, V> e = it.next();
            String k = e.getKey();
            V v = e.getValue();   // read before remove: TreeMap may move the successor into the removed entry
            if (!p.test(k, v)) continue;
            it.remove();
            unindex(k, v);
            any = true;
        }
        return any;
    }
    public void clear() { if (!map.isEmpty()) removeIf((k, v) -> true); }
    /** The stored value of {@code k} was changed in place. */
    public void touch(String k) {
        V v = map.get(k);
        if (v == null) throw new IllegalStateException("touch of an absent key " + k);
        dirty.mark(segment(k, v));
    }

    // ---------------------------------------------------------------------------------------------------------------- segments

    public String segment(String k, V v) { return fixed != null ? fixed : segmentOf.apply(k, v); }
    /** The segments holding an entry. */
    public Set<String> segmentIds() {
        if (fixed != null) return map.isEmpty() ? Set.of() : Set.of(fixed);
        return Collections.unmodifiableSet(bySegment.keySet());
    }
    /** The entries of one segment, sorted by key (empty when it holds none). */
    public SortedMap<String, V> entries(String segment) {
        if (fixed != null) return fixed.equals(segment) ? view : Collections.emptySortedMap();
        TreeSet<String> keys = bySegment.get(segment);
        TreeMap<String, V> m = new TreeMap<>();
        if (keys != null) for (String k : keys) m.put(k, map.get(k));
        return m;
    }

    private void index(String k, V v) {
        String s = segment(k, v);
        if (bySegment != null) bySegment.computeIfAbsent(s, x -> new TreeSet<>()).add(k);
        dirty.mark(s);
    }
    private void unindex(String k, V v) {
        String s = segment(k, v);
        if (bySegment != null) {
            TreeSet<String> keys = bySegment.get(s);
            if (keys == null || !keys.remove(k)) throw new IllegalStateException("segment index: " + k + " is not in " + s);
            if (keys.isEmpty()) bySegment.remove(s);
        }
        dirty.mark(s);
    }
}
