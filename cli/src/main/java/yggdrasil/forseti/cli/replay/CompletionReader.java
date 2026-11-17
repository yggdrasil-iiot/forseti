package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.*;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.Stage2;

/** The completion reader of a replay run (spec 418, "the harness waits for both business and ledger progress"): it reads the progress
 *  records of the output journals with {@code read_committed} and tells when a cut is complete, that is when every expected progress
 *  writer has committed a progress that closes it (its own, or a later one whose {@code mergedCutIds} contain it). */
public interface CompletionReader extends AutoCloseable {
    /** One progress partition: an output topic (contract name) and its partition. */
    record Writer(String topic, int partition) {
        @Override public String toString() { return topic + "/" + partition; }
    }

    /** The reject collectors' partition of both ledgers. */
    int REJECTS = 6;

    /** The progress writers a cut waits for: at every station the cut does not declare {@code starved}, partition {@code p(s)} of the
     *  two stage-1 evidence topics, the stage-1 ledger, the two stage-2 topics and the stage-2 ledger; and partition 6 of both ledgers. */
    static List<Writer> expected(RoutingManifest routing, List<CutAssembler.Idle> idleInputs) {
        Set<String> starved = new TreeSet<>();
        for (CutAssembler.Idle i : idleInputs) if (i.kind().equals(CutAssembler.STARVED)) starved.add(i.station());
        List<Writer> l = new ArrayList<>();
        for (Map.Entry<String, Integer> s : new TreeMap<>(routing.stations()).entrySet()) {
            if (starved.contains(s.getKey())) continue;
            for (String t : List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES, ReplayTopics.DISPOSITIONS1, Stage2.AFFECTED, Stage2.AFFECTED_SETS, ReplayTopics.DISPOSITIONS2))
                l.add(new Writer(t, s.getValue()));
        }
        l.add(new Writer(ReplayTopics.DISPOSITIONS1, REJECTS));
        l.add(new Writer(ReplayTopics.DISPOSITIONS2, REJECTS));
        return List.copyOf(l);
    }

    /** Waits until every writer of {@code expected} has committed a progress closing {@code cutId} and returns, per writer, the
     *  progress value that closed it. Not within {@code timeout} is an {@link IllegalStateException} naming the writers still missing. */
    Map<Writer, JsonNode> await(long cutId, List<Writer> expected, Duration timeout);

    @Override void close();
}
