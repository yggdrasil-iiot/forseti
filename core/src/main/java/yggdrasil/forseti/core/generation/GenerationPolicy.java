package yggdrasil.forseti.core.generation;

import java.util.*;

/** The transition policy of {@code forseti.generations} (spec "Generations and revisions"), pure: numbers are allocated once,
 *  increasing across the whole store whatever the scope ({@link #next}); a generation moves {@code registered -> replaying -> verified
 *  -> current -> retired} or {@code registered -> retired} and never returns; a scope has at most one {@code current} generation.
 *  {@link #validate} applies these rules and the row rules to a whole history; any violation is {@code control-log-corrupt}. */
public final class GenerationPolicy {
    private GenerationPolicy() {}

    public static final String CORRUPT = "control-log-corrupt";

    private static final Map<String, Set<String>> NEXT = Map.of(
        Generation.REGISTERED, Set.of(Generation.REPLAYING, Generation.RETIRED),
        Generation.REPLAYING, Set.of(Generation.VERIFIED),
        Generation.VERIFIED, Set.of(Generation.CURRENT),
        Generation.CURRENT, Set.of(Generation.RETIRED),
        Generation.RETIRED, Set.of());

    /** The number the next registration takes, given the highest number of the whole store (0 for an empty store). */
    public static long next(long highest) {
        if (highest < 0) throw new IllegalArgumentException("highest >= 0");
        return highest + 1;
    }

    /** Whether a generation whose latest status is {@code from} may append a row of status {@code to}; {@code from} null means the
     *  number has no row yet, and then only {@code registered} is allowed. */
    public static boolean allowed(String from, String to) {
        if (!Generation.STATUSES.contains(to)) return false;
        if (from == null) return Generation.REGISTERED.equals(to);
        Set<String> n = NEXT.get(from);
        return n != null && n.contains(to);
    }

    /** Throws {@code control-log-corrupt} unless the history is valid: the first row of a number is its only {@code registered} row;
     *  numbers increase in registration order across the store; {@code rulesDigest}, {@code inputReleases}, {@code replayFrom},
     *  {@code headVersion}, {@code releaseEvidence} and the record key are identical on every row of a number; every transition is
     *  allowed; {@code run} is null exactly on {@code registered} and {@code retired} rows and {@code gateReportSha256} is set exactly on
     *  {@code verified} and {@code current} rows; at most one generation per scope rests at {@code current}; no generation rests at
     *  {@code replaying} or {@code verified} (those rows exist only inside one promotion batch, so such a tail is an interrupted
     *  promotion). */
    public static void validate(List<GenerationStore.Entry> history) {
        Map<Long, GenerationStore.Entry> first = new HashMap<>();
        Map<Long, String> latest = new LinkedHashMap<>();
        long highest = 0;
        for (int i = 0; i < history.size(); i++) {
            GenerationStore.Entry e = history.get(i);
            Generation g = e.row();
            long n = g.generation();
            String where = "row " + i + " (generation " + n + ", " + g.status() + ")";
            boolean withRun = Generation.REPLAYING.equals(g.status()) || Generation.VERIFIED.equals(g.status()) || Generation.CURRENT.equals(g.status());
            if (withRun != (g.run() != null)) throw corrupt(where + ": run is " + (withRun ? "missing" : "set"));
            boolean withReport = Generation.VERIFIED.equals(g.status()) || Generation.CURRENT.equals(g.status());
            if (withReport != (g.gateReportSha256() != null)) throw corrupt(where + ": gateReportSha256 is " + (withReport ? "missing" : "set"));
            GenerationStore.Entry f = first.get(n);
            if (f == null) {
                if (!Generation.REGISTERED.equals(g.status())) throw corrupt(where + ": the first row of a number is its registered row");
                if (n <= highest) throw corrupt(where + ": numbers increase in registration order (highest so far " + highest + ")");
                highest = n;
                first.put(n, e);
            } else {
                Generation r = f.row();
                if (!f.scope().equals(e.scope()) || !r.rulesDigest().equals(g.rulesDigest()) || !r.inputReleases().equals(g.inputReleases())
                    || !r.replayFrom().equals(g.replayFrom()) || r.headVersion() != g.headVersion() || !r.releaseEvidence().equals(g.releaseEvidence()))
                    throw corrupt(where + ": the pins or the key differ from the registered row");
            }
            if (!allowed(latest.get(n), g.status())) throw corrupt(where + ": transition " + latest.get(n) + " -> " + g.status());
            latest.put(n, g.status());
        }
        Map<String, Long> current = new HashMap<>();
        for (Map.Entry<Long, String> l : latest.entrySet()) {
            String s = l.getValue();
            if (Generation.REPLAYING.equals(s) || Generation.VERIFIED.equals(s))
                throw corrupt("generation " + l.getKey() + " rests at " + s + " (an interrupted promotion)");
            if (Generation.CURRENT.equals(s)) {
                String scope = first.get(l.getKey()).scope();
                Long other = current.put(scope, l.getKey());
                if (other != null) throw corrupt("scope " + scope + " has two current generations, " + other + " and " + l.getKey());
            }
        }
    }

    private static IllegalStateException corrupt(String what) { return new IllegalStateException(CORRUPT + ": " + what); }
}
