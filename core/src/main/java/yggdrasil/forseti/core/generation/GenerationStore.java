package yggdrasil.forseti.core.generation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** The transport boundary of {@code forseti.generations}: rows keyed by scope (the {@code streamId} of the release that carries the
 *  attribution rule), appended and never rewritten. Generation numbers are unique across the whole store, so {@link #byNumber} needs no
 *  scope. Only the operator CLI appends; the harness, the jobs and the gate only read. */
public interface GenerationStore {
    /** One stored row with its record key. */
    record Entry(String scope, Generation row) {}

    void append(String scope, Generation row);

    /** Every row in append order. */
    List<Entry> all();

    /** The rows of one generation number in append order (empty when the number was never registered). */
    default List<Entry> byNumber(long generation) {
        List<Entry> l = new ArrayList<>();
        for (Entry e : all()) if (e.row().generation() == generation) l.add(e);
        return l;
    }

    /** The generation of {@code scope} whose latest row is {@code current}, if any (the latest such row wins). */
    default Optional<Entry> current(String scope) {
        Entry found = null;
        for (Entry e : all()) {
            if (!e.scope().equals(scope)) continue;
            List<Entry> rows = byNumber(e.row().generation());
            Entry latest = rows.get(rows.size() - 1);
            if (Generation.CURRENT.equals(latest.row().status())) found = latest;
        }
        return Optional.ofNullable(found);
    }
}
