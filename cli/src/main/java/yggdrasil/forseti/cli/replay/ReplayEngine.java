package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import yggdrasil.forseti.core.Json;

/** What an external engine gives the engine-neutral replay gate ({@code yggdrasil.forseti.cli.gate.ReplayGate}, plan D1, D10): its
 *  control stores, the registrations the gate makes on them through the operator CLI ({@code generation new}), one run of one recording
 *  over Kafka on a registered generation, written in the step-3 run layout ({@code journals/}, {@code quarantine/}, {@code cuts.json},
 *  {@code run.json}, {@code evidence.json} and {@code offset-map.json} under {@link Run#out}), and the findings of the gate assertions
 *  that only the engine can observe. Registration is separate from execution (spec 577): the gate registers every pipeline generation,
 *  in a fixed order, before any run starts. The Flink engine implements it in step 6, the Kafka Streams engine in step 7. An engine opens
 *  its shared infrastructure (the broker) when it is made and releases it on {@link #close}. */
public interface ReplayEngine extends AutoCloseable {
    /** The engine's name ({@code flink}). */
    String name();

    /** A control store: the gate's name for it ({@code pipeline}, {@code assertion:I-20}) and its identity, the cluster and the
     *  topics of {@code forseti.generations} and {@code forseti.recovery-control}. */
    record ControlStore(String name, String cluster, String generationsTopic, String recoveryControlTopic) {
        public ObjectNode toJson() {
            return Json.object().put("name", name).put("cluster", cluster).put("generationsTopic", generationsTopic).put("recoveryControlTopic", recoveryControlTopic);
        }
    }

    /** One registration of {@code forseti generation new}: on {@code registry}, with the attribution rule {@code rule}
     *  ({@code id@version}, null for the recording's own), as {@code variant} ({@code online}, {@code rule-change}, {@code event-time-replay},
     *  {@code self-test}). */
    record Registration(Path registry, String rule, String variant) {}

    /** A registered generation: the store it lives in and what {@code generation new} printed. */
    record Registered(ControlStore store, long generation, String rulesDigest, String registeredRowSha256, String scope, String variant) {}

    /** One run on a registered generation. {@code recording}: the built recording, already checked against its lock (an event-time
     *  replay's: the materialized replay recording); {@code causeKind}: {@code input}, or {@code replayed} for an event-time replay;
     *  {@code settings}: the shard's engine settings ({@code gate/<engine>-shards.json}), never a gate criterion. The run prepares its
     *  generation on the store's {@code forseti.recovery-control} (the initial declaration) and writes no generation row. {@code restart}:
     *  the restart injection the run carries ({@link Restarts}), null for none; its attempts' generations replace {@code generation}, one
     *  fresh run each, until an attempt is valid, and {@link Run#generation} names the one the run's files belong to. A restart run
     *  writes its injection manifest beside the run files ({@code injections.json}, {@link #INJECTIONS}). */
    record Request(String label, Path scenario, Path recording, Path registry, Registered generation, String negativeControl, String causeKind,
                   Path work, JsonNode settings, Restarts.Restart restart) {
        /** A run without a restart injection. */
        public Request(String label, Path scenario, Path recording, Path registry, Registered generation, String negativeControl, String causeKind,
                       Path work, JsonNode settings) {
            this(label, scenario, recording, registry, generation, negativeControl, causeKind, work, settings, null);
        }
    }

    /** A finished run: {@code out} holds the run files; {@code generation} what it ran on; {@code declarationOffset} and
     *  {@code controlEnd} the offset of its recovery declaration and the end offset H of the control topic after it; {@code evidence} the
     *  run's {@code evidence.json}; {@code summary} the engine's measurements for the report; {@code handle} the engine's own reference to
     *  the run (the topics an event-time replay's base journals are dumped from again). */
    record Run(Path out, Registered generation, long declarationOffset, long controlEnd, JsonNode evidence, ObjectNode summary, Object handle) {}

    /** The injection manifest of a restart run under {@link Run#out} (spec 685). */
    String INJECTIONS = "injections.json";

    /** Creates a control store of its own for the gate run ({@code forseti.generations}, {@code forseti.recovery-control}). */
    ControlStore controlStore(String name) throws Exception;

    /** {@code forseti generation new} on {@code store} through the operator CLI, with {@code state} as its floor directory, the
     *  recording's rule ({@code rule}, the scenario's {@code rule} object) unless the registration names another, and
     *  {@code --recording recording}. */
    Registered register(ControlStore store, Path state, Path trustedKeys, JsonNode rule, Registration registration, Path recording) throws Exception;

    /** Runs one recording and returns its files; throws when the run could not complete (the gate records the failure). */
    Run run(Request request) throws Exception;

    /** Dumps the journals of {@code base}'s generation again into {@code base.out()}, with {@code read_committed}, after a later
     *  generation ran on the same store (injection 23: the earlier generation's rows remain). */
    void redump(Run base) throws Exception;

    /** The partitions of each input topic the run's jobs actually consumed, as the engine observed them (spec 710): contract topic to
     *  partitions, every input topic present (empty when the jobs subscribe none of it). */
    Map<String, List<Integer>> observedSubscription(Run run) throws Exception;

    /** The restore evidence a writer's first progress carries at a first start (spec 723: {@code initEvidence.restore}). */
    String firstStartRestore();

    /** The findings of the engine-specific per-run assertions on {@code run} (spec 715-717, the engine's job settings and timer
     *  evidence): assertion id to the problems found, an empty list for an assertion that was checked and holds. Every id this returns for
     *  any run must be returned for every run, and none may be an engine-independent id of the gate. */
    Map<String, List<String>> findings(Run run, Path recording) throws Exception;

    /** The restart injections that do not apply to this engine, each with the reason (spec 685: the Flink run records 22(iii) as not
     *  applicable, which is not a skip). The gate records them; a kind listed here is never run. */
    Map<Restarts.Kind, String> notApplicable();

    /** The required barrier set of station {@code station}'s stage-1 writer under this engine (spec 412: Flink and Kafka Streams differ),
     *  on the routing {@code routing} and the run's stage-1 subscription {@code subscribed}. */
    List<yggdrasil.forseti.core.journal.Progress.Origin> stationBarriers(String station, yggdrasil.forseti.cli.recording.RoutingManifest routing, Map<String, List<Integer>> subscribed);

    /** The W1 form the engine reports (spec 620): {@code per-station} or {@code min-over-partitions}; the gate requires every run's
     *  {@code cuts.json} to declare it. */
    String w1Form();

    /** Deletes the topics only the run labelled {@code label} wrote and read (its input and output topics, of every attempt of a
     *  restart run; never a control store's), once nothing of the gate run reads them again, and waits until the broker no longer lists
     *  them; returns what it deleted ({@code deleted}, {@code remaining}). The unsharded gate calls it after each run, so the one broker
     *  that serves every run holds the topics of the run in progress, as the broker of a shard does. */
    default ObjectNode release(String label) { return Json.object(); }

    /** The engine's shared infrastructure after a run, for the nightly resource record only, never a gate criterion
     *  ({@code forseti.gate.memoryLog}): for a broker the engine started, its container's memory and data and the topics it holds. */
    default ObjectNode resources() { return Json.object(); }

    @Override void close();
}
