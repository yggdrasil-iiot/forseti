package yggdrasil.forseti.core.stage2;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Delete;
import yggdrasil.forseti.core.journal.JournalFold;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.stage1.Stage1;

/** The events {@link Stage2#decide} takes (rule sheet T2-T3): a stage-1 work record row or delete of partition {@code p(s)} (a basis), a
 *  stage-1 sample row, an incident routed to the station, a fired stage-2 timer, the close of a cut. The adapter owns the order: the cut's
 *  stage-1 rows and deletes as written, the cut's incidents in {@code (identity bytes, payloadDigest)} order, the timers due at W2, then
 *  the close. */
public final class Events2 {
    private Events2() {}

    public sealed interface Event permits Basis, Sample, Incident, Timer, CutClose {}

    /** A {@code forseti.work-records} row or a delete of one. */
    public record Basis(JournalFold.Entry entry) implements Event {
        public Basis {
            if (entry instanceof Row r) { if (!Stage1.WORK_RECORDS.equals(r.topic())) throw new IllegalArgumentException("a basis is a work record row, not " + r.topic()); }
            else if (!(entry instanceof Delete)) throw new IllegalArgumentException("a basis is a work record row or a delete");
        }
    }
    /** A {@code forseti.samples} row. */
    public record Sample(Row row) implements Event {
        public Sample { if (!Stage1.SAMPLES.equals(row.topic())) throw new IllegalArgumentException("a sample is a forseti.samples row, not " + row.topic()); }
    }
    /** One delivered {@code quality.incidents} record routed to the station (T1). */
    public record Incident(Inputs.InputRecord record) implements Event {}
    /** A due deadline of the stage-2 state's index. */
    public record Timer(long deadline, JsonNode key, String purpose) implements Event {}
    /** The cut is complete at this station (ProgressTracker.ready): its W2, the stage-1 progress records and incident barriers processed,
     *  and the {@code mergedCutIds} of the stage-1 progress it consumed (spec 432: stage 2 writes its progress with the same ones). */
    public record CutClose(long cutId, long w2, List<Progress.Stage1ProgressRef> stage1ProgressRefs, List<Progress.IncidentOffset> incidentOffsets,
                           List<Long> mergedCutIds) implements Event {
        public CutClose { stage1ProgressRefs = List.copyOf(stage1ProgressRefs); incidentOffsets = List.copyOf(incidentOffsets); mergedCutIds = List.copyOf(mergedCutIds); }
    }
}
