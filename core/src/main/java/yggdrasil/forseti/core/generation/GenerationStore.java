package yggdrasil.forseti.core.generation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** The transport boundary of {@code forseti.generations}: rows keyed by scope (the {@code streamId} of the release that carries the
 *  attribution rule), appended and never rewritten. Generation numbers are unique across the whole store, so {@link #byNumber} needs no
 *  scope. Only the operator CLI appends ({@code generation new} and {@code retire}; {@code promote} from PR 5b); the harness and the jobs
 *  only read, and the gate appends no row by any path of its own (it invokes the verbs). The initial recovery declaration is not a
 *  generation row: the run preparation of a generation's first run writes it. Every read sees a history {@link #validate} accepts. */
public interface GenerationStore {
    /** One stored row with its record key. */
    record Entry(String scope, Generation row) {}

    void append(String scope, Generation row);

    /** Every row in append order. */
    List<Entry> all();

    /** Throws {@code control-log-corrupt} unless the whole history satisfies {@link GenerationPolicy#validate}. */
    default void validate() { GenerationPolicy.validate(all()); }

    /** The rows of one generation number in append order (empty when the number was never registered). */
    default List<Entry> byNumber(long generation) {
        List<Entry> l = new ArrayList<>();
        for (Entry e : all()) if (e.row().generation() == generation) l.add(e);
        return l;
    }

    /** The {@code registered} row of a number (in a valid history its first and only one), if the number was registered. */
    default Optional<Entry> registered(long generation) {
        for (Entry e : all()) if (e.row().generation() == generation) return Generation.REGISTERED.equals(e.row().status()) ? Optional.of(e) : Optional.empty();
        return Optional.empty();
    }

    /** The status of the latest row of a number, if the number was registered. */
    default Optional<String> latestStatus(long generation) {
        List<Entry> rows = byNumber(generation);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(rows.size() - 1).row().status());
    }

    /** The highest number of the whole store, 0 when it is empty. */
    default long highest() {
        long h = 0;
        for (Entry e : all()) h = Math.max(h, e.row().generation());
        return h;
    }

    /** The generation of {@code scope} whose latest row is {@code current}, if any; a valid history has at most one. */
    default Optional<Entry> current(String scope) {
        List<Entry> all = all();
        GenerationPolicy.validate(all);
        java.util.Map<Long, Entry> latest = new java.util.LinkedHashMap<>();
        for (Entry e : all) latest.put(e.row().generation(), e);
        for (Entry e : latest.values()) if (e.scope().equals(scope) && Generation.CURRENT.equals(e.row().status())) return Optional.of(e);
        return Optional.empty();
    }
}
