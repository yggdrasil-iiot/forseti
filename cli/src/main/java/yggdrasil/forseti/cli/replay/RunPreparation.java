package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.generation.GenerationStore;

/** The run preparation of a replay run over the transport stores, with the rules of the step-3 file one ({@code GenerationCli.prepareRun},
 *  spec "Recovery control"): the generation must be registered in a valid history ({@code generation-unregistered} otherwise); when the
 *  recovery control holds no declaration of it, the initial declaration {@code {generation, injectionId: null, stage: 1, epochBefore:
 *  null, targetEpoch: 0, lastWrittenCutId: 0, routingDigest}} is appended (and confirmed committed) with the recording's
 *  {@code routingDigest}; otherwise every declaration of the generation must carry it ({@code generation-routing-mismatch}) and nothing
 *  is written. No generation row is ever written here. The file store's lock is replaced by the control store's single writer: a fenced
 *  writer appends nothing. */
public final class RunPreparation {
    private RunPreparation() {}

    /** The initial declaration of a generation. */
    public static ObjectNode initialDeclaration(long generation, String routingDigest) {
        ObjectNode decl = Json.putSafe(Json.object(), "generation", generation);
        return decl.putNull("injectionId").put("stage", 1).putNull("epochBefore").put("targetEpoch", 0).put("lastWrittenCutId", 0).put("routingDigest", routingDigest);
    }

    /** Returns the offset of the declaration the run starts from (the appended initial one, or the latest existing one). */
    public static long prepare(GenerationStore store, RecoveryControlStore control, long generation, String routingDigest) {
        if (store.registered(generation).isEmpty())
            throw new ReferenceEngine.Refused("generation-unregistered", Json.putSafe(Json.object(), "generation", generation));
        RecoveryControlStore.Snapshot s = control.read();
        List<RecoveryControlStore.Declaration> mine = new ArrayList<>();
        for (RecoveryControlStore.Declaration d : s.declarations()) if (d.generation() == generation) mine.add(d);
        if (mine.isEmpty()) return control.append(initialDeclaration(generation, routingDigest));
        for (RecoveryControlStore.Declaration d : mine) {
            String declared = d.value().path("routingDigest").textValue();
            if (!routingDigest.equals(declared))
                throw new ReferenceEngine.Refused("generation-routing-mismatch", Json.putSafe(Json.object(), "generation", generation)
                    .put("declared", declared).put("recording", routingDigest));
        }
        return mine.get(mine.size() - 1).offset();
    }
}
