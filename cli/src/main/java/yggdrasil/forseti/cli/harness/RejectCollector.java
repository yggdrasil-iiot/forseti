package yggdrasil.forseti.cli.harness;

import java.util.ArrayList;
import java.util.List;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Revisions;
import yggdrasil.forseti.core.stage1.Routing;

/** A reject collector: the single writer of partition 6 of {@code forseti.dispositions-stage1} (stage 1) or
 *  {@code forseti.dispositions-stage2} (stage 2). It receives the inputs refused at the door (S1's {@link Routing.Rejected}, T1's
 *  {@code Incidents.Rejected}), holds them in its {@link CutAssembler} and, when the assembler closes a cut, writes their disposition rows
 *  (one revision per body change) and its ledger progress. Stage 1's required barrier set is every subscribed raw telemetry, session and
 *  occupancy partition and its first progress carries the {@code initEvidence} of its own initialization; stage 2's is the six incident
 *  partitions, without initEvidence (it never skips). Both count every barrier they receive, whatever its {@code idleInputs} say. */
final class RejectCollector {
    static final int PARTITION = 6;

    private record Pending(Inputs.InputRecord record, String part, Disposition.Body body) {}

    private final long generation;
    private final int stage;
    private final Revisions revisions = new Revisions();
    private final CutAssembler<Pending> cuts;
    private Progress.InitEvidence init;

    RejectCollector(long generation, Progress.InitEvidence init, List<Progress.Origin> required) { this(generation, 1, init, required); }

    RejectCollector(long generation, int stage, Progress.InitEvidence init, List<Progress.Origin> required) {
        if (stage == 2 && init != null) throw new IllegalArgumentException("initEvidence on stage-1 ledgers only");
        this.generation = generation;
        this.stage = stage;
        this.init = init;
        this.cuts = new CutAssembler<>(null, required, false);
    }

    void add(Routing.Rejected r) { add(r.record(), r.part(), r.body()); }

    void add(Inputs.InputRecord record, String part, Disposition.Body body) {
        cuts.input(new Progress.Origin(record.origin().topic(), record.origin().partition()), new Pending(record, part, body));
    }

    void barrier(CutAssembler.Barrier b) {
        cuts.barrier(b, () -> { throw new IllegalStateException("the reference harness writes no recovery declaration after the initial one"); });
    }

    /** The closed cut's disposition rows in origin order, then the partition-6 ledger progress; a collector never waits for event time,
     *  so every call after the cut's barriers closes it. */
    List<Object> close() {
        CutAssembler.Closed<Pending> c = cuts.poll(null).orElseThrow(() -> new IllegalStateException("reject collector " + stage + ": the cut's barriers are incomplete"));
        if (stage == 2 && !c.mergedCutIds().isEmpty()) throw new IllegalStateException("the stage-2 reject collector never skips");
        List<Object> out = new ArrayList<>();
        List<Pending> pending = new ArrayList<>(c.inputs());
        pending.sort((a, b) -> ContractOrder.ORIGIN.compare(a.record(), b.record()));
        for (Pending r : pending) {
            Disposition.Key key = new Disposition.Key(generation, r.record().origin().topic(), r.record().origin().partition(), r.record().origin().offset(), r.part());
            revisions.next(generation, key.toJson(), r.body().digest()).ifPresent(em -> out.add(new Disposition(key, r.body(), em.revision())));
        }
        out.add(new Progress.Ledger(generation, stage, PARTITION, null, c.cutId(), 0, c.barrierRefs(), c.mergedCutIds(), c.restartEpoch(), c.recoveryRef(), init));
        init = null;
        return out;
    }
}
