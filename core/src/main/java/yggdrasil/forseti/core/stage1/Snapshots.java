package yggdrasil.forseti.core.stage1;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/** The stored snapshots of rule sheet P1 ({@code plc-stamped}), by message key, with an index of the valid ones per equipment that
 *  {@link PlcStamped#derive} reads instead of sorting every stored snapshot again for every message. The index orders an equipment's
 *  valid snapshots by {@code (sourceTs, message key)}, the order P2 reads them in (message-key order, stably sorted by sourceTs), and
 *  keeps per visit ID the {@code VisitPresent=true} and {@code false} snapshots apart and the unit IDs seen. The index is derived: it is
 *  not serialized, and every change goes through {@link #put} and {@link #removeIf}. */
final class Snapshots {
    /** A valid snapshot's position in its equipment's P2 order. */
    record Pos(long ts, String key) implements Comparable<Pos> {
        public int compareTo(Pos o) { int c = Long.compare(ts, o.ts); return c != 0 ? c : key.compareTo(o.key); }
    }

    /** One equipment's valid snapshots. */
    static final class Device {
        final TreeMap<Pos, Stage1State.Snapshot> all = new TreeMap<>();
        /** By visit ID: the positions of {@code VisitPresent=true} snapshots, and of {@code false} ones. */
        final TreeMap<String, TreeSet<Pos>> present = new TreeMap<>(), absent = new TreeMap<>();
        /** By visit ID: how many snapshots carry each unit ID. */
        final TreeMap<String, TreeMap<String, Integer>> units = new TreeMap<>();

        void add(Pos p, Stage1State.Snapshot x) {
            all.put(p, x);
            (x.present() ? present : absent).computeIfAbsent(x.visitId(), k -> new TreeSet<>()).add(p);
            units.computeIfAbsent(x.visitId(), k -> new TreeMap<>()).merge(x.unitId(), 1, Integer::sum);
        }
        void remove(Pos p, Stage1State.Snapshot x) {
            all.remove(p);
            TreeMap<String, TreeSet<Pos>> m = x.present() ? present : absent;
            TreeSet<Pos> s = m.get(x.visitId());
            s.remove(p);
            if (s.isEmpty()) m.remove(x.visitId());
            TreeMap<String, Integer> u = units.get(x.visitId());
            if (u.merge(x.unitId(), -1, Integer::sum) == 0) u.remove(x.unitId());
            if (u.isEmpty()) units.remove(x.visitId());
        }
    }

    private final TreeMap<String, Stage1State.Snapshot> byKey = new TreeMap<>();
    private final TreeMap<String, Device> devices = new TreeMap<>();

    Stage1State.Snapshot get(String key) { return byKey.get(key); }
    boolean containsKey(String key) { return byKey.containsKey(key); }
    Collection<Stage1State.Snapshot> values() { return Collections.unmodifiableCollection(byKey.values()); }
    void forEach(BiConsumer<String, Stage1State.Snapshot> f) { byKey.forEach(f); }
    int size() { return byKey.size(); }

    void put(String key, Stage1State.Snapshot x) {
        Stage1State.Snapshot old = byKey.put(key, x);
        if (old != null) unindex(key, old);
        if (x.valid()) devices.computeIfAbsent(x.device(), d -> new Device()).add(new Pos(x.sourceTs(), key), x);
    }

    void removeIf(Predicate<Stage1State.Snapshot> p) {
        Iterator<Map.Entry<String, Stage1State.Snapshot>> it = byKey.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Stage1State.Snapshot> e = it.next();
            String key = e.getKey();
            Stage1State.Snapshot x = e.getValue();   // read before remove: TreeMap may move the successor into the removed entry
            if (!p.test(x)) continue;
            it.remove();
            unindex(key, x);
        }
    }

    private void unindex(String key, Stage1State.Snapshot x) {
        if (!x.valid()) return;
        Device d = devices.get(x.device());
        d.remove(new Pos(x.sourceTs(), key), x);
        if (d.all.isEmpty()) devices.remove(x.device());
    }

    /** The valid snapshots' index, by equipment path. */
    SortedMap<String, Device> devices() { return Collections.unmodifiableSortedMap(devices); }
}
