package yggdrasil.forseti.cli.harness;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Revisions;
import yggdrasil.forseti.core.stage1.Routing;

/** A reject collector: the single writer of partition 6 of {@code forseti.dispositions-stage1} (stage 1) or
 *  {@code forseti.dispositions-stage2} (stage 2). It receives the inputs refused at the door (S1's {@link Routing.Rejected}, T1's
 *  {@code Incidents.Rejected}), writes their disposition rows (one revision per body change) and, per cut, its ledger progress. Stage 1's
 *  required barrier set is every subscribed raw telemetry, session and occupancy partition and its first progress carries the
 *  {@code initEvidence} of its own initialization; stage 2's is the six incident partitions, without initEvidence (it never skips). */
final class RejectCollector {
    static final int PARTITION = 6;

    private record Pending(Inputs.InputRecord record, String part, Disposition.Body body) {}

    private final long generation;
    private final int stage;
    private final Revisions revisions = new Revisions();
    private final List<Pending> pending = new ArrayList<>();
    private Progress.InitEvidence init;

    RejectCollector(long generation, Progress.InitEvidence init) { this(generation, 1, init); }

    RejectCollector(long generation, int stage, Progress.InitEvidence init) {
        if (stage == 2 && init != null) throw new IllegalArgumentException("initEvidence on stage-1 ledgers only");
        this.generation = generation;
        this.stage = stage;
        this.init = init;
    }

    void add(Routing.Rejected r) { pending.add(new Pending(r.record(), r.part(), r.body())); }

    void add(Inputs.InputRecord record, String part, Disposition.Body body) { pending.add(new Pending(record, part, body)); }

    /** The cut's disposition rows in origin order, then the partition-6 ledger progress. */
    List<Object> cut(long cutId, List<JsonNode> barrierRefs) {
        List<Object> out = new ArrayList<>();
        pending.sort((a, b) -> ContractOrder.ORIGIN.compare(a.record(), b.record()));
        for (Pending r : pending) {
            Disposition.Key key = new Disposition.Key(generation, r.record().origin().topic(), r.record().origin().partition(), r.record().origin().offset(), r.part());
            revisions.next(generation, key.toJson(), r.body().digest()).ifPresent(em -> out.add(new Disposition(key, r.body(), em.revision())));
        }
        pending.clear();
        List<Progress.BarrierRef> refs = new ArrayList<>();
        for (JsonNode b : barrierRefs)
            refs.add(new Progress.BarrierRef(Json.safeLong(b.get("cutId"), "barrierRefs.cutId", Json.NON_NEGATIVE),
                new Progress.Origin(Json.text(b.path("origin"), "topic"), (int) Json.safeLong(b.path("origin").get("partition"), "barrierRefs.origin.partition", 0, Integer.MAX_VALUE)),
                Json.safeLong(b.get("throughOffset"), "barrierRefs.throughOffset", Json.THROUGH)));
        out.add(new Progress.Ledger(generation, stage, PARTITION, null, cutId, 0, refs, List.of(), 0, null, init));
        init = null;
        return out;
    }
}
