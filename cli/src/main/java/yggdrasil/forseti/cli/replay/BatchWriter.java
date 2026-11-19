package yggdrasil.forseti.cli.replay;

import java.util.Map;
import yggdrasil.forseti.cli.recording.Recording;

/** The batch and barrier writer of a replay run (spec 412, 418): batch k is the business records {@code (throughOffsets[k-1],
 *  throughOffsets[k]]} of every subscribed input partition, written to that partition in recording order with their recorded key,
 *  headers and value bytes, followed on every subscribed input partition by the cut's {@link CutBarrierRecord}. A call returns only
 *  once the broker acknowledged every record of the batch; the broker offsets it assigned are added to {@link #offsets}. The harness
 *  calls it for batch k+1 only after cut k is complete ({@link CompletionReader}), except where the spec's restart rule says otherwise. */
public interface BatchWriter extends AutoCloseable {
    /** What one call wrote: the business records and barriers, per partition the broker through-offset its barrier carries, and
     *  {@code firstAppendNanos}: the batch's first append (spec 681: the earliest broker append time of its records), on the harness's
     *  {@link System#nanoTime} clock. */
    record Written(long cutId, long records, int barriers, Map<String, Map<Integer, Long>> brokerThrough, long firstAppendNanos) {}

    Written write(Recording.Batch batch, long restartEpoch, String recoveryRef);

    /** The recording-to-broker offset mapping of everything written so far. */
    OffsetMap offsets();

    @Override void close();
}
