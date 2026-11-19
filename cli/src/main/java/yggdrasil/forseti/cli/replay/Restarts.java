package yggdrasil.forseti.cli.replay;

import java.util.*;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.stage1.Events;

/** The restart injections of the replay gate (spec 678-679, 685; plan D7, D8), engine-neutral: their kinds, the run names that carry
 *  them, the placement of an injection in a recording (spec 418, 626) and the names of the stage-1 progress writers the expected and
 *  attributed sets list. A restart run is a named run of a scenario on a generation of its own, {@code <scenario>@restart-<kind>}, made
 *  beside the scenario's uninterrupted run by a shard that plans it ({@code restartRuns} in {@code gate/<engine>-shards.json}). */
public final class Restarts {
    private Restarts() {}

    /** A restart injection: its run-name suffix, its injection ids (one per kill), whether it restarts stage 1 (a stage-1 restart moves
     *  the recovery epoch, spec 412) and which stages it kills. */
    public enum Kind {
        /** Injection 21: kill -9 of the stage-1 and stage-2 jobs together. */
        I21("restart-21", List.of("injection-21"), true, true),
        /** Injection 22(i): kill of the stage-1 TaskManager during an uncommitted stage-1 sink transaction (plan D8). */
        I22_I("restart-22i", List.of("injection-22i"), true, false),
        /** Injection 22(ii): kill of stage 2 after stage-1 output is committed and before stage-2 processing; stage 1 untouched. */
        I22_II("restart-22ii", List.of("injection-22ii"), false, true),
        /** Injection 22(iii): a Kafka Streams stage-1 rebalance (a stage-1 restart; only the reinitialized tasks recover). An engine
         *  without rebalances declares it not applicable ({@link ReplayEngine#notApplicable}), which is not a skip. */
        I22_III("restart-22iii", List.of("injection-22iii"), true, false),
        /** Injection 22(iv): 22(i), then a second stage-1 kill once every job is RUNNING and before batch k+1 (chained). */
        I22_IV("restart-22iv", List.of("injection-22iv-first", "injection-22iv-second"), true, false);

        public final String suffix;
        public final List<String> injectionIds;
        public final boolean stage1, stage2;
        Kind(String suffix, List<String> injectionIds, boolean stage1, boolean stage2) { this.suffix = suffix; this.injectionIds = injectionIds; this.stage1 = stage1; this.stage2 = stage2; }

        /** The spec's name of the injection ({@code 21}, {@code 22(i)}, ...). */
        public String spec() {
            return switch (this) { case I21 -> "21"; case I22_I -> "22(i)"; case I22_II -> "22(ii)"; case I22_III -> "22(iii)"; case I22_IV -> "22(iv)"; };
        }

        public static Kind ofSuffix(String suffix) {
            for (Kind k : values()) if (k.suffix.equals(suffix)) return k;
            return null;
        }
    }

    /** How many fresh runs an injection may take before it is recorded as {@code error} (plan D8: invalid attempts repeat on a fresh run
     *  up to N, then the status is error, never pass). */
    public static final int ATTEMPTS = 3;


    /** A restart run name {@code <scenario>@restart-<kind>}: its scenario and kind, null when {@code name} is not one. */
    public record RunName(String name, String scenario, Kind kind) {
        public String label() { return scenario.replace('/', '.') + "." + kind.suffix; }
    }

    public static RunName parse(String name) {
        int at = name.lastIndexOf('@');
        if (at <= 0) return null;
        Kind k = Kind.ofSuffix(name.substring(at + 1));
        return k == null ? null : new RunName(name, name.substring(0, at), k);
    }

    /** The restart of one run: its kind and the registered generations of its attempts, one per fresh run it may take. */
    public record Restart(Kind kind, List<ReplayEngine.Registered> attempts) {
        public Restart { attempts = List.copyOf(attempts); if (attempts.isEmpty()) throw new IllegalArgumentException("a restart without a registered attempt"); }
    }

    /** The name of the stage-1 reject collector as a progress writer (its partition 6 of {@code forseti.dispositions-stage1}). */
    public static final String REJECTS = "stage1-rejects";

    /** Every stage-1 progress writer of a routing: its stations (by name) and the stage-1 reject collector. */
    public static List<String> stage1Writers(Collection<String> stations) {
        List<String> l = new ArrayList<>(new TreeSet<>(stations));
        l.add(REJECTS);
        return l;
    }

    /** The cut k at which the injection is placed in {@code rec}: after batch k is written and before batch k+1 (spec 418): never the
     *  recording's last batch, never a bracketing batch of a deadline nor the batch before them (cuts {@code cutA - 1}, {@code cutA},
     *  {@code cutB}), at cut 3 or later (a completed checkpoint to restore exists), and batch k+1 carries on every subscribed raw telemetry
     *  partition a record whose event time is at or above that partition's maximum through cut k (so the restored watermark reaches the
     *  W1 formula at cut k+1, spec 418, 718). The first such cut at or after the middle of the recording; an
     *  {@link IllegalStateException} ({@code error}) when none exists. */
    public static long place(Recording rec, List<Integer> telemetry) {
        List<Recording.Batch> batches = rec.batches();
        Set<Long> forbidden = new HashSet<>();
        for (Recording.Deadline d : rec.deadlines()) { forbidden.add(d.cutA() - 1); forbidden.add(d.cutA()); forbidden.add(d.cutB()); }
        // per partition: the largest event time through each cut, and the largest within each batch
        Map<Integer, long[]> through = new HashMap<>(), within = new HashMap<>();
        for (int p : telemetry) {
            long[] t = new long[batches.size()], w = new long[batches.size()];
            Arrays.fill(t, Long.MIN_VALUE);
            Arrays.fill(w, Long.MIN_VALUE);
            int i = 0;
            long max = Long.MIN_VALUE;
            try (Recording.Reader r = rec.reader(InputParser.MESSAGES, p)) {
                for (Recording.Line l; (l = r.next()) != null; ) {
                    long off = l.record().offset();
                    while (i < batches.size() && off > batches.get(i).through(InputParser.MESSAGES, p)) { t[i] = max; i++; }
                    if (i >= batches.size()) break;
                    InputParser.Read read = InputParser.read(new Inputs.Origin(InputParser.MESSAGES, p, off), l.record().key(), Recording.valueBytes(l.bytes()));
                    Long ts = read.rejected() ? null : Events.eventTime(read.record());
                    if (ts != null) { max = Math.max(max, ts); w[i] = Math.max(w[i], ts); }
                }
            }
            for (; i < batches.size(); i++) t[i] = max;
            through.put(p, t);
            within.put(p, w);
        }
        int start = batches.size() / 2;
        for (int pass = 0; pass < 2; pass++) {
            for (int i = pass == 0 ? start : 0; i < (pass == 0 ? batches.size() - 1 : start); i++) {
                long k = batches.get(i).cutId();
                if (k < 3 || forbidden.contains(k)) continue;
                boolean ok = true;
                for (int p : telemetry) if (within.get(p)[i + 1] == Long.MIN_VALUE || within.get(p)[i + 1] < through.get(p)[i]) { ok = false; break; }
                if (ok) return k;
            }
        }
        throw new IllegalStateException("error: no batch of " + rec.dir().getFileName() + " takes a restart injection (spec 418: not the last batch, not a bracketing batch of a"
            + " deadline nor the batch before them, and batch k+1 at or above every telemetry partition's previous maximum)");
    }
}
