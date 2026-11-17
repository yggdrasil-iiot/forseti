package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The persisted deadline index of a station (spec 491, 495-496): registrations ordered by (deadline, logical key bytes, purpose), one per
 *  (key, purpose); registering again replaces the obsolete registration. A deadline is due when {@code W >= deadline}. */
public final class DeadlineIndex {
    public static final String SETTLE = "settle", MISSING_DEPARTURE = "missing-departure", EXPIRY = "expiry";

    public record Deadline(long deadline, JsonNode key, String purpose) {}
    private record Entry(Deadline d, byte[] keyBytes) {}
    private static final Comparator<Entry> ORDER = (a, b) -> {
        int c = Long.compare(a.d().deadline(), b.d().deadline());
        if (c != 0) return c;
        c = Arrays.compareUnsigned(a.keyBytes(), b.keyBytes());
        return c != 0 ? c : a.d().purpose().compareTo(b.d().purpose());
    };

    private final TreeSet<Entry> ordered = new TreeSet<>(ORDER);
    private final Map<String, Entry> byKeyAndPurpose = new HashMap<>();

    private static String id(JsonNode key, String purpose) { return Canonical.utf8(Canonical.bytes(key)) + "\u0000" + purpose; }

    private java.util.function.LongConsumer listener;
    /** Told the deadline of every registration that was added or removed (a move reports both deadlines; a cancel or removal the one it
     *  removed); not of registering again what is registered already. The segmented state store follows the index through it. */
    public void onChange(java.util.function.LongConsumer l) { this.listener = l; }
    private void changed(long deadline) { if (listener != null) listener.accept(deadline); }

    public void register(long deadline, JsonNode key, String purpose) {
        Entry e = new Entry(new Deadline(deadline, key.deepCopy(), purpose), Canonical.bytes(key));
        Entry old = byKeyAndPurpose.put(id(key, purpose), e);
        if (old != null) ordered.remove(old);
        ordered.add(e);
        if (old == null || old.d().deadline() != deadline) { if (old != null) changed(old.d().deadline()); changed(deadline); }
    }
    public void cancel(JsonNode key, String purpose) {
        Entry old = byKeyAndPurpose.remove(id(key, purpose));
        if (old != null) { ordered.remove(old); changed(old.d().deadline()); }
    }
    public void cancelAll(JsonNode key) { for (String p : List.of(SETTLE, MISSING_DEPARTURE, EXPIRY)) cancel(key, p); }
    public Optional<Deadline> registered(JsonNode key, String purpose) {
        Entry e = byKeyAndPurpose.get(id(key, purpose));
        return e == null ? Optional.empty() : Optional.of(e.d());
    }
    /** Every registration with {@code deadline <= w}, in index order; nothing is removed. */
    public List<Deadline> due(long w) {
        List<Deadline> out = new ArrayList<>();
        for (Entry e : ordered) { if (e.d().deadline() > w) break; out.add(e.d()); }
        return out;
    }
    /** Removes a fired registration; false when it was cancelled or replaced meanwhile. */
    public boolean remove(long deadline, JsonNode key, String purpose) {
        String id = id(key, purpose);
        Entry e = byKeyAndPurpose.get(id);
        if (e == null || e.d().deadline() != deadline) return false;
        byKeyAndPurpose.remove(id);
        ordered.remove(e);
        changed(deadline);
        return true;
    }
    public List<Deadline> all() { List<Deadline> l = new ArrayList<>(); ordered.forEach(e -> l.add(e.d())); return l; }
    public int size() { return ordered.size(); }

    public ArrayNode toJson() { return toJson(ordered); }
    /** The registrations with {@code from <= deadline < to}, as {@link #toJson} writes them. */
    public ArrayNode toJson(long from, long to) {
        return toJson(ordered.subSet(new Entry(new Deadline(from, null, ""), new byte[0]), true, new Entry(new Deadline(to, null, ""), new byte[0]), false));
    }
    private static ArrayNode toJson(SortedSet<Entry> entries) {
        ArrayNode a = Json.JSON.createArrayNode();
        for (Entry e : entries) {
            ObjectNode n = Json.putSafe(a.addObject(), "deadline", e.d().deadline());
            n.set("key", e.d().key());
            n.put("purpose", e.d().purpose());
        }
        return a;
    }
    public static DeadlineIndex fromJson(JsonNode a) {
        DeadlineIndex ix = new DeadlineIndex();
        for (JsonNode n : a) ix.register(Json.safeLong(n.get("deadline"), "deadline", Json.ANY), n.get("key"), Json.text(n, "purpose"));
        return ix;
    }
}
