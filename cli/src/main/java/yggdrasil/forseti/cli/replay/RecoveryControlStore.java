package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Optional;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.journal.Progress;

/** The transport boundary of {@code forseti.recovery-control} (spec "Recovery control", 416): one partition of recovery declarations
 *  {@code {generation, injectionId, stage, epochBefore, targetEpoch, lastWrittenCutId, routingDigest}}, written only by the harness (the
 *  run preparation writes a generation's initial declaration, a restart injection its next one) and read by every stage-1 progress
 *  writer at its initialization, with {@code read_committed}, up to the partition's current end offset H. The row shape is the step-3
 *  file's; only the transport changes. */
public interface RecoveryControlStore extends AutoCloseable {
    /** One committed declaration and its offset in the control partition. */
    record Declaration(long offset, JsonNode value) {
        public long generation() { return Json.safeLong(value.get("generation"), "generation", 1, Json.MAX_SAFE); }
        public long targetEpoch() { return Json.safeLong(value.get("targetEpoch"), "targetEpoch", Json.NON_NEGATIVE); }
        public String injectionId() { return value.path("injectionId").isNull() ? null : Json.text(value, "injectionId"); }
    }

    /** One read of the control partition: every committed declaration up to {@code endOffset} (H, the end offset read before the
     *  records, so a declaration committed during the read is never half seen). */
    record Snapshot(List<Declaration> declarations, long endOffset) {
        public Snapshot { declarations = List.copyOf(declarations); }

        /** The latest declaration of {@code generation}, if any. */
        public Optional<Declaration> latest(long generation) {
            Declaration found = null;
            for (Declaration d : declarations) if (d.generation() == generation) found = d;
            return Optional.ofNullable(found);
        }

        /** The {@code initEvidence} of a writer initialized on this read: the selected declaration's offset, H and the engine's restore
         *  evidence. No declaration of the generation is an {@link IllegalStateException}: the run preparation writes it first. */
        public Progress.InitEvidence initEvidence(long generation, String restore) {
            Declaration d = latest(generation).orElseThrow(() -> new IllegalStateException("no recovery declaration of generation " + generation + " in " + ReplayTopics.RECOVERY_CONTROL));
            return new Progress.InitEvidence(d.offset(), endOffset, restore);
        }

        /** The latest declaration of {@code generation} in the cut assembler's terms (null when there is none). */
        public CutAssembler.Declaration assemblerDeclaration(long generation) {
            return latest(generation).map(d -> new CutAssembler.Declaration(d.offset(), d.targetEpoch(), d.injectionId())).orElse(null);
        }
    }

    /** Appends one declaration and returns only once it is committed and readable with {@code read_committed}: its offset. A writer
     *  that another writer of the store has fenced fails ({@link IllegalStateException}) and writes nothing. */
    long append(JsonNode declaration);

    /** A fresh read up to the current end offset: never a cached value. */
    Snapshot read();

    @Override void close();
}
