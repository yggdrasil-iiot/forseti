package yggdrasil.forseti.cli.harness;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Revisions;
import yggdrasil.forseti.core.stage1.Routing;

/** The stage-1 reject collector: the single writer of partition 6 of {@code forseti.dispositions-stage1}. It receives the inputs S1
 *  refuses at the door ({@link Routing.Rejected}), writes their disposition rows (one revision per body change) and, per cut, its ledger
 *  progress whose required barrier set is every subscribed raw telemetry, session and occupancy partition. Its first progress carries the
 *  {@code initEvidence} of its own initialization. */
final class RejectCollector {
    static final int PARTITION = 6;

    private final long generation;
    private final Revisions revisions = new Revisions();
    private final List<Routing.Rejected> pending = new ArrayList<>();
    private Progress.InitEvidence init;

    RejectCollector(long generation, Progress.InitEvidence init) {
        this.generation = generation;
        this.init = init;
    }

    void add(Routing.Rejected r) { pending.add(r); }

    /** The cut's disposition rows in origin order, then the partition-6 ledger progress. */
    List<Object> cut(long cutId, List<JsonNode> barrierRefs) {
        List<Object> out = new ArrayList<>();
        pending.sort((a, b) -> ContractOrder.ORIGIN.compare(a.record(), b.record()));
        for (Routing.Rejected r : pending) {
            Disposition.Key key = new Disposition.Key(generation, r.record().origin().topic(), r.record().origin().partition(), r.record().origin().offset(), r.part());
            revisions.next(generation, key.toJson(), r.body().digest()).ifPresent(em -> out.add(new Disposition(key, r.body(), em.revision())));
        }
        pending.clear();
        List<Progress.BarrierRef> refs = new ArrayList<>();
        for (JsonNode b : barrierRefs)
            refs.add(new Progress.BarrierRef(Json.safeLong(b.get("cutId"), "barrierRefs.cutId", Json.NON_NEGATIVE),
                new Progress.Origin(Json.text(b.path("origin"), "topic"), (int) Json.safeLong(b.path("origin").get("partition"), "barrierRefs.origin.partition", 0, Integer.MAX_VALUE)),
                Json.safeLong(b.get("throughOffset"), "barrierRefs.throughOffset", Json.THROUGH)));
        out.add(new Progress.Ledger(generation, 1, PARTITION, null, cutId, 0, refs, List.of(), 0, null, init));
        init = null;
        return out;
    }
}
