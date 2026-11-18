package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** What an external engine gives the engine-neutral replay gate ({@code yggdrasil.forseti.cli.gate.ReplayGate}, plan D1, D10): one run
 *  of one recording over Kafka, written in the step-3 run layout ({@code journals/}, {@code quarantine/}, {@code cuts.json},
 *  {@code run.json}, {@code evidence.json} and {@code offset-map.json} under {@link Run#out}), and the findings of the gate assertions
 *  that only the engine can observe. The Flink engine implements it in step 6, the Kafka Streams engine in step 7; the gate, the oracle
 *  comparison and every engine-independent assertion stay one implementation. An engine opens its shared infrastructure (the broker)
 *  when it is made and releases it on {@link #close}. */
public interface ReplayEngine extends AutoCloseable {
    /** The engine's name ({@code flink}). */
    String name();

    /** One registration of {@code forseti generation new} before the run: on {@code registry}, with the attribution rule {@code rule}
     *  ({@code id@version}, null for the recording's own), as {@code variant} ({@code online}, {@code rule-change}, {@code event-time-replay}). */
    record Registration(Path registry, String rule, String variant) {}

    /** One run. {@code recording}: the built recording, already checked against its lock (an event-time replay's: the materialized
     *  replay recording); {@code registrations}: in order, the run reads the last one; {@code state}: the directory of the store's floors
     *  (the gate's pipeline store, or an assertion's own); {@code base}: for an event-time replay, the online run whose generation and
     *  recovery control topics the replay shares (null otherwise); {@code causeKind}: {@code input}, or {@code replayed} for an event-time
     *  replay; {@code settings}: the shard's engine settings ({@code gate/<engine>-shards.json}), never a gate criterion. */
    record Request(String label, Path scenario, Path recording, Path registry, Path trustedKeys, Path state, List<Registration> registrations,
                   String negativeControl, String causeKind, Run base, Path work, JsonNode settings) {}

    /** A registered generation of a run, as {@code generation new} printed it. */
    record Registered(long generation, String rulesDigest, String registeredRowSha256, String scope) {}

    /** A finished run: {@code out} holds the run files; {@code generations} what was registered, in order (the run's generation last);
     *  {@code evidence} the run's {@code evidence.json}; {@code summary} the engine's measurements for the report (wall time, memory,
     *  checkpoints); {@code handle} the engine's own reference to the run (the topics an event-time replay shares, the journals it dumps
     *  again). */
    record Run(Path out, List<Registered> generations, JsonNode evidence, ObjectNode summary, Object handle) {
        public Registered generation() { return generations.get(generations.size() - 1); }
    }

    /** Runs one recording and returns its files; throws when the run could not complete (the gate records the failure). */
    Run run(Request request) throws Exception;

    /** Dumps the journals of {@code base}'s generation again into {@code base.out()}, with {@code read_committed}, after a later
     *  generation ran on the same store (injection 23: the earlier generation's rows remain). */
    void redump(Run base) throws Exception;

    /** The findings of the engine-specific per-run assertions on {@code run} (spec 715-717 and the engine's timer and initialization
     *  evidence): assertion id to the problems found, an empty list for an assertion that was checked and holds. Every id this returns for
     *  any run must be returned for every run. */
    Map<String, List<String>> findings(Run run, Path recording) throws Exception;

    /** The W1 form the engine reports (spec 620): {@code per-station} or {@code min-over-partitions}. */
    String w1Form();

    @Override void close();
}
