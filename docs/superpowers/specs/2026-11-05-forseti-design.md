# Forseti: governed late-binding attribution and affected-unit reconciliation

## Purpose

The yggdrasil project family comprises Mimir (models OPC UA type space), Tyr (authors mappings and rules), Bifrost (approves and publishes governed releases; CI gates; Heimdall write boundary), Muninn (streams governed OPC UA data as Sparkplug B over MQTT), Huginn (observes network traffic against declared policy), and Vor (governed read path and, through Heimdall, a governed write path for AI agents). Applications couple only through published data and wire contracts; zero shared code.

This PoC assumes the following operating situation. In the field, process values (tightening torque, weld current) arrive by tag polling; which unit (car body) was at the station arrives separately from MES or scanners. After a network outage, edge store-and-forward pushes minutes-to-hours-old values at once, and a pipeline that joins by arrival order attaches them to the unit currently at the station. Quality teams also learn of incidents late: a nutrunner fails its periodic check, and every unit that nutrunner tightened since its last good check is suspect. They must answer "which units did this tool work since its last valid check, and where are they now" while units keep moving.

Forseti puts a core principle into executable form: once a part has a durable identity, measurements late-bind to that identity, and the delays and corrections of asynchronous uplinks are reconciled. Like Muninn, forseti has no write path at all: it never writes to the plant, never issues NCMD, and knows nothing of Heimdall. Holds and releases stay in MES.

Processing operates across two stages:
- Stage 1 (attribution) joins process samples with station occupancy by event time and emits one work record per unit visit, with per-tool evidence inside the visit, plus a state journal for every accepted sample; every delivered input also gets a disposition in a per-stage ledger.
- Stage 2 (impact) joins quality incidents with stage-1 evidence and emits per-unit impact assessments and a per-incident affected set, revised as evidence changes. A late correction in stage 1 re-drives stage 2 (cascading correction).

Forseti is implemented twice, on Apache Flink and on Kafka Streams, behind the same gates and the same OT requirements, so the engine choice rests on measured results. Requirements are never relaxed to suit an engine or the broker; an engine that cannot meet one is reported as failing it.

## Hypotheses

- **H-order**: "At a settled cut, the stage-1 and stage-2 results for a given set of observations and rules do not depend on arrival order, for input that arrives within the online correction horizon." A settled cut is a completed cut at which every non-starved active station's reported `W1` is at or past that scenario's `settleTargetW` (the latest ground-truth deadline of the scenario, recorded in the manifest). Direct body comparisons between runs are made at comparable cuts (defined under Testing and gates); the fixtures for H-order and for the engine comparison contain settled cuts at which every visit and every session gap is closed, and those cuts are compared directly. Intermediate revisions are not compared. Falsified if the run with telemetry held back 10 minutes and the on-time run disagree on any `bodyDigest` at the same settled cut.
- **H-cascade**: "A stage-1 correction (a visit retracted, a sample moved between visits, an unattributed sample becoming attributed) re-judges every unit it touches in stage 2, and a unit keeps its affected status while other evidence remains." Falsified if, after the cascade settles, a stage-2 row cites a stage-1 body that is no longer current.
- **H-gap**: "Absence never resolves silently." A sample beyond the horizon, a session gap, unattributed activity inside an incident interval, an open visit, a visit without a close signal, a death that cannot be assigned to one session, or an incident older than work-record retention shows up as a disposition, an `insufficient` visit, an `insufficient-evidence` assessment, or an affected set whose coverage is not `complete`; "not in the list" is never "good". Falsified if any input has no disposition, or a set reports `complete` while any of those conditions holds.
- **H-pin**: "Every output row names the rules, input releases, generation and job artifact that produced it, and an independent oracle reproduces its body from those pins at the same cut." Falsified if the oracle cannot reproduce a row body.
- **H-tool**: "An incident on one tool marks only the visits in which that tool shows activity inside the suspect interval." Falsified if a unit worked only by the other tool at the same station becomes `affected-candidate`.
- **H-engine**: "Whether Flink stays is decided by the measured comparison against Kafka Streams on the same gates." Falsified as a design claim if the comparison report contains estimates instead of measurements.
- **H-isolation**: "A stalled station does not stop the other stations from settling." Falsified if, with the telemetry supply of one declared station cut, any other station's work records or affected sets have not settled by the observation deadline of 60 s. This is a requirement for both engines; an engine that misses it is reported as missing it.
- **H-need**: "When the tool controller stamps the unit ID, the visit ID and a visit-present signal into the same message as the measurement, attribution needs no time join." Shown when `plc-stamped` attribution with telemetry held back 10 minutes and the occupancy topic not consumed yields the same bodies as the on-time run of the same `plc-stamped` rule, and that run contains at least one valid `complete` visit.
## Decisions

### 1. Repository
- Decision: Establish a new public repository, `yggdrasil-iiot/forseti`, with its own value types, its own SHA-256 digest computation, and its own release reader; there is no dependency on Bifrost, Muninn, Vor, Tyr, or the lab repositories.
- Reason: Zero shared code is the project family's invariant; Muninn and Vor already read releases this way.

### 2. Two stages and a cascade
- Decision: Implement a stage-1 attribution job and a stage-2 impact job, connected by `forseti.work-records` and `forseti.samples`, which also carry stage-1 progress records; stage-2 rows carry the stage-1 keys and `bodyDigest` they were judged on (`basis`).
- Reason: Each stage has its own rule, owner, and gate, stage-1 output serves other consumers, and the cascade is recorded rather than hoped for.

### 3. Rules governed in the spine
- Decision: Attribution and impact rules are authored in Tyr and approved and published by Bifrost as two new document kinds, `attribution-rule` and `impact-rule`, both in mapping contract 2.27.0; they contain declarative fields and a closed set of operations, with no SQL or scripts. Both Bifrost and Tyr already carry 2.26.0 (`endpointRef`): Bifrost PR #52 (`3bd48e9`), Tyr PR #20 (`da764c0`).
- Reason: Two kinds are used because the attribution rule (production and data owners) and the impact rule (quality owner) change for different reasons; one version is used because an `impact-rule` cannot be validated without its `attribution-rule`.

### 4. Input event formats
- Decision: Forseti holds the JSON Schemas of the occupancy and incident formats; how a rule accepts a format is governed through `occupancy-binding` and `incident-binding` profiles, each the file of an evidence-artifact (schemaVersion 1.1.0) in the release bundle, parsed by Bifrost with its own code as it parses the Vor read-binding profile (`ReadBindingProfile`) for 2.25.0. A profile approves which schema version and bytes are accepted, the vocabulary, and the identity scheme; it does not rename fields.
- Reason: Unit identity belongs to MES; what forseti accepts as identity, time, and correction is the governance question, and the evidence-artifact route already carries approval and attestation without a third kind.
- Consequence: If a producer's format differs from the approved schema bytes, the job refuses to start (`binding.schema-mismatch`).

### 5. Work record unit with tool evidence
- Decision: Emit one stage-1 work record per visit, keyed by `visitRef` (`occupancy-join`: {`mode`, `source`, `arrivalEventId`}; `plc-stamped`: {`mode`, `namespace`, `station`, `visitId`}), with `unitId` in the body, holding per-member aggregates and an `equipmentEvidence[]` list, with one entry per tool or station equipment with its members, activity and inactivity intervals, coverage, and samples digest. Members may belong to a station or to a tool below it. `visitSeq` is only a producer correlation field in the body.
- Reason: The quality question is "what did this tool do while this unit was here"; one row per visit keeps the stage-2 join and oracle comparison small.
- Consequence: An incident on tool T1 matches only T1's evidence, not every visit at the station.

### 6. Journals, body and envelope
- Decision: Every output topic is an append-only journal whose identity is `(evaluationGeneration, logical key)`. Each row has a `body`, which is digested (`bodyDigest`), and an `envelope` (generation, revision, `supersedesDigest`, cause, event-time progress, trigger offset, basis revisions, job pins), which is not. A new revision is emitted only when `bodyDigest` changes; a restart or a new run id emits nothing. Revisions start at 1 in each generation and have no gaps. Every cause that decides a result (why a visit was retracted, why a unit was judged) is in the body; the envelope cause only records which input triggered the computation. Progress records share the journal topics but are not rows. Disposition decisions for inputs go to two per-stage ledgers, `forseti.dispositions-stage1` and `forseti.dispositions-stage2`, keyed by input delivery, not into the state journals. A row that leaves the current result set is ended by an explicit `delete` control record; delete records are checked for revision continuity but carry no business body.
- Reason: Two engines, repeated runs, and the oracle can only agree on content that excludes run-specific data; tying revisions to body changes removes restart noise.

### 7. Canonical form
- Decision: Compute `bodyDigest` as SHA-256 over the UTF-8 bytes of the RFC 8785 (JCS) form of the body, with pinned JCS libraries in Java and Python and a shared cross-language vector file run by both test suites. Times are epoch milliseconds as integers. Non-finite values are not aggregated; such a sample gets the reason `non-finite`.
- Reason: Java and Python print doubles differently; JCS fixes number serialization and key order.

### 8. OT time requirements, not engine-friendly ones
- Decision: The gate runs the field values: out-of-orderness 30 s, allowed lateness 12 min (above the 10 min late burst), online correction horizon 24 h, enter and leave tolerance 45 s, sample period 1000 ms. Event time advances virtually in replay, so a 24 h horizon runs in minutes; the requirement is not shortened for the gate. Engine idleness based on elapsed wall time never decides a result: in gate runs it is off, and the harness declares idle inputs explicitly. Event time advances only from data, through each engine's own mechanism; harness records never advance it, and cut barriers only mark completeness. Inputs inside a cut are admitted against the station's `W1` at its last written progress (`admissionW`); timers still fire on the engine's own event time. The rule contract has no idle timeout.
- Reason: OT requirements take priority over what Kafka or an engine finds easy; an engine that fails them is reported as failing.

### 9. Three retention layers
- Decision: Stage-1 online correction covers the 24 h horizon; `impact-rule.retention.workRecordsMs` is 7 d and must be at least the horizon; input topics keep 30 d. Kafka record timestamps are broker append time (`LogAppendTime`) and event time lives in the payload, so virtual event time cannot trigger early deletion. An incident revision whose `suspectFrom` is older than its `revisionEventTime` minus work-record retention marks its set `beyondRetention`; the value is computed once per revision, never against a moving watermark.
- Reason: An incident found a day later still needs the work records, and a replay needs the input log.

### 10. Completeness from source signals; QoS 0
- Decision: The bridge emits per-device session events (`seq-gap` with the event times of the messages before and after and the missing count, `session-down` from the last received event time until the next birth, `session-up`), records an observed NCMD Rebirth as the flag `rebirth: true` on the following `session-up`, and counts a DBIRTH as having consumed `seq` 1, so a birth is not reported as a `seq-gap`. Muninn's `bdSeq` starts at 0 in every process and increases only on the host-offline path, and `seq` restarts at 0 at every NBIRTH, so session identity includes the NBIRTH payload timestamp (`birthPublishTs`). Muninn builds the NDEATH Will at connect, before the birth; a normal stop builds a new NDEATH with a new timestamp; a rebirth publishes a new NBIRTH without a new connect or Will; NBIRTH is published at QoS 0; so a Will's timestamp is at or before its own connection's first birth, and a stop-NDEATH's timestamp is at or after its own connection's last birth. Therefore an NDEATH timestamp is never used as a time of death, and a gap starts after the last normal observation. For NDEATH assignment, candidates are the connections of the same edge with the same `bdSeq` (a connection is a run of births with no assigned death between them; a rebirth replaces the earlier birth within its connection): (i) if an unended connection has a latest birth with `birthPublishTs ≤` the death's timestamp, the death closes the latest such connection (stop path); (ii) otherwise it closes the connection of the first birth with `birthPublishTs >` the death's timestamp (Will path); (iii) otherwise it is `session-ambiguous`. When a connection boundary cannot be established — a missing NBIRTH is signalled (data without a birth, a session whose first `seq` is not 0 or 1, a DBIRTH without an NBIRTH in the connection, or a `seq` returning to 0 without an NBIRTH), another connection's birth has the same millisecond as the death, birth timestamps go backwards, or a second NBIRTH with the same `bdSeq` arrives with neither an assigned death nor an NCMD Rebirth observed by the bridge between it and the earlier birth (the earlier connection then ends at that birth as `session-ambiguous` with coverage `unknown` from its last normal observation, and a later death with that `bdSeq` closes the new connection by rule (i) only when its timestamp is after the new birth, otherwise it is `session-ambiguous`) — the death is `session-ambiguous` and coverage is `unknown`, and forseti never assumes the current session is fine. Muninn's refusal at first birth publishes an NDEATH from a connection that never had a birth; such a death is recorded as `session-ambiguous` but opens no gap, because a gap starts after the last normal observation and there is none. A death that closes a connection other than the latest is `stale-death`, leaving the current session open. A visit overlapping a gap is `insufficient` with reason `session-gap`; an ambiguous session gives reason `session-ambiguous`. `seq` and `bdSeq` wrap, so forseti reports "no loss signal", never "no loss". Exactly-once is claimed only from Kafka input to Kafka output.
- Reason: Muninn publishes NDATA and DDATA at QoS 0 and only NDEATH and the Will at QoS 1, and never publishes DDEATH, so data delivery is at most once and loss is visible only as seq gaps and NDEATH. Muninn builds the stop-NDEATH and the Will with the same builder, so their contents cannot tell them apart.

### 11. Rule changes by evaluation generation
- Decision: A generation pins both rules, the hierarchy, the binding profiles and the input releases. An operator CLI (`forseti generation new|promote|retire`) writes generations to the control topic `forseti.generations`; jobs never write it and refuse to start on an unregistered generation. A new generation replays the retained input in event-time order, passes the gates, and only then is promoted to current, which requires its gate report digest. Consumers first select the current generation, then the highest revision per key within it.
- Reason: The same procedure works on both engines (Kafka Streams has no savepoints), the oracle reproduces a generation exactly, and a savepoint does not prove that a new rule reads old state with the same meaning.

### 12. Engine-independent core
- Decision: `forseti-core` (Java 17, no Flink or Kafka Streams dependency) holds value types, the release reader, rule and profile parsing, and the two decision functions as pure functions `decide(keyState, event, watermark) -> (newState, outputs)`. Adapters handle partitioning, timers, state, side outputs and sinks with low-level APIs. Stage-1 timers run on each engine's own event-time mechanism — Flink's operator watermark and event-time timers; Kafka Streams' task stream-time, a `STREAM_TIME` punctuator and a persisted deadline index from which every expired deadline is processed, because a punctuator may skip intermediate calls when stream-time jumps. Kafka Streams fires a stage-1 timer when stream-time > deadline + `outOfOrdernessMs`. Flink uses the bound of its own `BoundedOutOfOrdernessWatermarks` (largest timestamp − out-of-orderness − 1), emitted for every record by a thin delegating generator that emits that value in `onEvent`, with the automatic watermark interval set to 0 (the built-in generator emits only in `onPeriodicEmit`); occupancy and session sources neither advance nor hold the watermark, and telemetry sources are never marked idle. Flink does not checkpoint or restore watermarks, so after a restore the operator watermark advances only from records read after the restore; Kafka Streams restores partition time from the committed offset metadata (confirmed for the pinned version). Flink source parallelism equals the number of subscribed partitions of each topic, and the fixtures are chosen so that each source subtask owns exactly one split, verified by preflight; keyed operators and sinks keep parallelism 6. Cut barriers are a readiness condition for applying timers, not a common watermark; stage 2 does not use an engine watermark: a core `ProgressTracker` advances each station from the stage-1 progress records and fires stage-2 timers. The set of barriers a station must collect follows the scope of the engine's time — Flink's operator watermark is a minimum over every subscribed raw telemetry partition, so a Flink station waits for barriers from all of them; Kafka Streams stream-time is per task, so a Kafka Streams station waits only for the partitions that feed it. In stage 1 Flink keys all three stage-1 inputs by station (routing from device keys) into one `KeyedProcessFunction`; Kafka Streams routes `ot.messages` and `ot.sessions` to `forseti.messages-by-station` and `forseti.sessions-by-station`, reads `mfg.occupancy` already partitioned by station, and uses one `Processor` with three source parents. Interval joins and the Kafka Streams DSL are not used, because stage 1 must also emit "no match".
- Reason: The comparison must measure the engines, not two implementations of the rules.

### 13. Bridge in forseti, one record per Sparkplug message
- Decision: A `forseti-bridge` module carries Sparkplug B from MQTT to Kafka, one Kafka record per Sparkplug message with its member array, passes metric names through and refuses alias-only input (Muninn sets no aliases), fans session events out per device, and holds no business logic. It follows the dead-letter pattern of the ADR-0009 bridge in `sparkplug-governance-lab` without its code, and does not use its "key = metric identity + compaction" design, because stage 1 needs history.
- Reason: Muninn reads the plant, never writes and has no Kafka dependency; in a plant the broker's commercial Kafka bridge takes this place. Activity and identity read from the same message as the measurement.

### 14. Release reader at Vor's level
- Decision: Forseti verifies `heads → release.json → payload` by raw-byte SHA-256 and verifies each head as Vor does (`vor/src/vor/head.py`): two signatures from distinct trusted keys, a version floor that never goes down, and an expiry.
- Reason: A hash match alone does not authenticate an approver.

### 15. Tools as child equipment
- Decision: A tool is a child of its station in the ISA-95 hierarchy (contract 2.23.0); stations in a rule may not nest (`attribution.station-nested`); an incident on a tool is routed to the one station that is the tool's ancestor-or-self. An incident carries `stationRef`, which every revision must keep; a revision that changes it is rejected with the stage-2 disposition reason `station-change` and a new incident id is required.
- Reason: It reuses the published hierarchy and gives each incident exactly one station partition.
- Consequence: Tools that move between stations are out of scope.

### 16. Proving it is not forced
- Decision: Three regimes are stated in the README and two run in the sim: (a) the tool controller stamps the unit ID, the visit ID and a visit-present signal in the same message as the measurement, so forseti attributes without a time join (`plc-stamped`) and may cross-check occupancy without changing the attribution; (b) MES already provides process history and affected-unit queries in time, so forseti is not needed; (c) record keepers are separate (tool controller, inspection, MES) and an updated list is needed before the next transfer, which is where forseti applies.
- Reason: The PoC must show where it does not belong.

### 17. Occupancy corrections follow EPCIS
- Decision: An occupancy correction is an EPCIS 2.0 error declaration: the producer re-sends the original event with the same `eventId` plus `errorDeclaration` {`declarationTime`, `reason`, `correctiveEventIDs`}. A re-sent record whose fields other than `errorDeclaration` and `recordTime` equal the stored event cancels it; any other difference is a conflict, which retracts the visit with reason `input-conflict`. A departure carries `arrivalEventId`, the `eventId` of the arrival it closes; an error declaration on a departure cancels only the leave claim and reopens the visit; a corrective arrival is a new event and a new visit; a correction beyond the horizon is not applied online and is recorded as `replay-required` with reason `correction-beyond-horizon`.
- Reason: The occupancy schema already uses EPCIS `ObjectEvent` field names, and the project follows an open standard where one exists.
- Consequence: The reason vocabulary (`did_not_occur`, `incorrect_data`) decides the stage-2 outcome; forseti does not claim full EPCIS conformance.

### 18. Station isolation is a requirement
- Decision: One stalled station must not stop other stations from settling (H-isolation), and both engines are measured against it with their own stage-1 event-time mechanisms (Flink operator watermark, Kafka Streams task stream-time). In Flink, starving a station whose telemetry has a dedicated raw partition still lets the other stations complete cuts (their barriers arrive), but their `W1` does not move, so they do not settle and the requirement records a miss.
- Reason: Flink's operator watermark is the minimum over all subscribed raw telemetry partitions, while Kafka Streams keeps one task per partition; keeping each engine's own mechanism makes the difference visible instead of hiding it.
- Consequence: An engine that misses the requirement is reported as missing it; the requirement is not dropped. The semantic gate, the requirements run and engine eligibility are separate results; a missed requirement makes the engine ineligible, and the comparison report is still produced.

## Scope and non-goals

### In scope
- Bifrost 2.27.0 schemas, validators and profile parsers; Tyr authoring for both kinds; a Bifrost sim extension (three stations, two nutrunners under one station; stations and nutrunners carry unit ID, visit ID and visit-present members, value control nodes, configurable scan period); forseti core, oracle, bridge, synthetic occupancy and incident producers, generation CLI, Flink and Kafka Streams adapters, gates, and a measured comparison report.
- Business assumptions of the synthetic scenario: the first scenario is a nutrunner that fails a periodic check found late; normal lateness 30 s, late burst 10 min, online correction horizon 24 h, work records 7 d, input log 30 d; p99 5 s for event-triggered rows, measured as `read_committed` visibility from the input append to the output append, for incidents and corrections on evidence that is already complete; no automatic hold release. Scale: 1,000 units, 3 stations, 2 nutrunners, 2 incident kinds.

### Non-goals
- Writing to the plant or issuing commands; automatic hold or release (MES keeps that authority); building genealogy or issuing unit IDs (MES); tools that move between stations; real MES integration; production HA and Kubernetes; Avro and Schema Registry (JSON in the PoC; ADR-0009 already states production would use Avro and a registry); degradation or physics models; SQL or scripted rules; following a release head while running.

## Contracts

Attribution rules and impact rules are new in mapping contract 2.27.0. All fields are required unless marked optional. Schemas permit no additional properties. Bifrost validates independently of Tyr with byte-identical schema copies. Each refusal has a code.

### Attribution rule (mapping contract 2.27.0)

An attribution rule maps process telemetry to visits across stations and tools.

Fields:
- `kind`: contract kind identifier.
- `schemaVersion`: const `2.27.0`.
- `id`: rule identifier.
- `version`: rule version.
- `label`: rule label.
- `description`: rule description.
- `equipmentRef` {`id`, `version`}: references the ISA-95 hierarchy (contract 2.23.0). Missing hierarchy fails with `attribution.hierarchy-missing`. A hierarchy that is itself invalid keeps its own `hierarchy.*` refusal.
- `stations[]`: hierarchy paths, root first, joined with `:`, matching the string Muninn publishes as the Sparkplug device id and the occupancy `readPoint`. Unresolved paths fail with `attribution.station-unresolved`. A station that is an ancestor of another fails with `attribution.station-nested`. The list is non-empty and has no duplicates.
- `members[]` {`mappingSetRef`, `mappingId`, `semanticId`, `metric`}: member equipment resolves as mapping set `equipmentInstanceId` → hierarchy path (the rule Bifrost's `CommandBindings` uses for device paths) and must be one of `stations` or a descendant (a tool). `mappingId` must exist in the set, and `metric` and `semanticId` must equal the mapping. The mapping's `memberPath` has exactly one `member` step. `metric` equals, byte for byte, the `name` of the semantic-definition leaf member the mapping resolves to (mapping set `targetInstanceRef` → target instance → `semanticDefinitionRef` → member), which is the name Muninn publishes as the Sparkplug metric; `semanticId` equals that member's `semanticId`. Within the semantic definitions a rule uses, no two leaf members share a `name`. For a mapping whose source is an `opcua-node`, `access.identifier` ends with `.` followed by `metric`. `members[]` is non-empty, lists each `mappingId` at most once, and no two entries resolve to the same equipment path and `metric`; each of these failures is `attribution.member-unresolved`. The member references below (`memberRef`, `unitMemberRef`, `visitMemberRef`, `presentMemberRef`) are strings, each the `mappingId` of exactly one `members[]` entry. Missing mappings fail with `attribution.member-unresolved`; invalid placement fails with `attribution.member-equipment`.
- `activity[]` {`equipmentPath`, `memberRef`, `activeWhen` (const `true`)}: one Boolean member per station or tool that indicates the equipment is working. The list is non-empty. A `memberRef` that names no `members[]` entry, a non-Boolean member, a member whose equipment path differs from `equipmentPath`, an `equipmentPath` that is neither a station nor a descendant of one, or two entries for one equipment fail with `attribution.activity-member`. An equipment without an activity entry is allowed.
- `occupancyBindingRef` {`id`, `version`}: references an occupancy binding profile. Unresolved bindings fail with `attribution.binding-unresolved`. Invalid profiles fail with `attribution.binding-profile`.
- `identity` {`mode`: `occupancy-join` | `plc-stamped`, `namespace` (optional; required when `plc-stamped`; the namespace of producer visit IDs), `members[]` (required and non-empty when `plc-stamped`, absent otherwise), `crossCheck`: `none` | `occupancy`}: each `members[]` entry is {`equipmentPath`, `unitMemberRef` (String member carrying the unit ID), `visitMemberRef` (String member carrying the producer's visit ID), `presentMemberRef` (Boolean member, true while a unit is present at that equipment)}. Under `plc-stamped`, every equipment that has measurement or activity members in the rule is covered by exactly one `members[]` entry, and identity, presence, activity and measurement are read from that equipment's same message. A missing or duplicate entry, or an entry for an equipment that has no measurement or activity member in the rule, fails with `attribution.identity-equipment`. A reference that names no `members[]` entry, a member of the wrong type, or a member whose equipment path differs from the entry's `equipmentPath` fails with `attribution.identity-member`. `crossCheck: occupancy` is allowed under both modes.
- `temporal` {`timestampBasis` const `source`, `outOfOrdernessMs`, `allowedLatenessMs`, `horizonMs`, `enterToleranceMs`, `leaveToleranceMs`}: bounds are 0–86400000 ms as the 2.x `temporalPolicy`. Validation requires `outOfOrdernessMs ≤ allowedLatenessMs ≤ horizonMs`, else `attribution.temporal-order`. `allowedLatenessMs` sets when a closed visit is first reported `complete`; a later input within the horizon may still revise it. Engine idleness is never a rule; there is no idle timeout.
- `quality` {`uncertainInput`: `propagate` | `reject`, `maxBadShare`}: Bad samples are never attributed, judged from raw `ua_statuscode` severity bits because Muninn's `Quality` 192 also covers some Uncertain codes. The mapping's `temporalPolicy.badInput=propagate` is not read as "Bad is usable evidence". `maxBadShare` is a finite number in [0, 1]. Bad share is computed per measurement member as the number of slots holding a Bad observation divided by that member's `observedPeriods`; a visit where any member exceeds `maxBadShare` is `insufficient` (reason `bad-share-exceeded`); a visit where any required member has `observedPeriods` = 0 is `insufficient` (reason `no-observed-periods`).
- `coverage` {`expectedPeriodMs` (integer, 1–86400000), `maxMissingPeriods` (integer ≥ 0)}: coverage is counted per member over the visit's evaluation interval in slots of `expectedPeriodMs` starting at `enter` (the last partial slot counts as one slot; see the work record body). `maxMissingPeriods` applies to each required member: a visit where any member has `missingPeriods > maxMissingPeriods` is `insufficient` (reason `missing-periods`), and a visit where any equipment has `unknownActivityPeriods > maxMissingPeriods` is `insufficient` (reason `unknown-activity`).
- `overlap` {`policy` const `reject-ambiguous`}: an overlap is decided only between two visits whose arrival and departure are both valid, by comparing their actual `[enter, leave)` intervals, not the tolerance windows; an open visit never retracts another visit.
- `evidenceRefs[]`: evidence reference list.

| Refusal Code | Condition |
| :--- | :--- |
| `attribution.hierarchy-missing` | Hierarchy referenced by `equipmentRef` does not exist or fails resolution. |
| `attribution.station-unresolved` | Path in `stations[]` cannot be resolved in the hierarchy. |
| `attribution.station-nested` | A station in `stations[]` is an ancestor of another station in `stations[]`. |
| `attribution.member-unresolved` | `mappingId` does not exist in mapping set, or `metric` or `semanticId` differs from the mapping. |
| `attribution.member-equipment` | Member equipment is not one of `stations` or a descendant tool. |
| `attribution.activity-member` | Member in `activity[]` is not a Boolean member of the target equipment. |
| `attribution.binding-unresolved` | `occupancyBindingRef` cannot be resolved. |
| `attribution.binding-profile` | Occupancy binding profile fails schema, version, or structural validation. |
| `attribution.identity-member` | `unitMemberRef` or `visitMemberRef` is not a String member, or `presentMemberRef` is not a Boolean member. |
| `attribution.identity-equipment` | Under `plc-stamped`, an equipment with measurement or activity members is not covered by exactly one `identity.members[]` entry. |
| `attribution.temporal-order` | Temporal constraint `outOfOrdernessMs ≤ allowedLatenessMs ≤ horizonMs` is violated. |

### Impact rule (mapping contract 2.27.0)

An impact rule evaluates equipment incidents against attributed visits within a shared scope.

Fields:
- `kind`: contract kind identifier.
- `schemaVersion`: const `2.27.0`.
- `id`: rule identifier.
- `version`: rule version.
- `label`: rule label.
- `description`: rule description.
- `attributionRuleRef` {`id`, `version`}: references an attribution rule in the same bundle. Missing reference fails with `impact.attribution-missing`.
- `incidentScope[]`: equipment paths (station or tool) accepted for incidents. Each must exist in the hierarchy and be a descendant-or-self of a station of the attribution rule, else `impact.scope-containment`. The list is non-empty and has no duplicates.
- `incidentBindingRef` {`id`, `version`}: references an incident binding profile. Unresolved bindings fail with `impact.binding-unresolved`. Invalid profiles fail with `impact.binding-profile`.
- `interval` {`start` const `suspectFrom`, `overlap`: `any-overlap` | `fully-within` | `enter-within`, `openEnd` const `provisional-until-closed`}: the incident producer writes the last good check time into `suspectFrom` and points to it with `evidenceRef`. For `tool-check-failed` the producer closes the interval with `suspectTo = detectedAt` (the tool is quarantined when the check fails); `gauge-drift` incidents may stay open. The scenario uses `overlap` = `any-overlap`.
- `evidence` {`match`: `equipment-evidence` | `station-visit`, `onBasisRetracted`: `by-cause` | `always-insufficient`, `onWithdrawn`: `not-affected` | `insufficient`}: the scenario uses `equipment-evidence`, `by-cause`, `not-affected`. Under `by-cause`, visit cancellation reasons evaluate as defined in the outcome table.
- `retention` {`workRecordsMs`}: maximum 90 d. Bifrost refuses `impact.retention-order` unless the attribution rule's `horizonMs ≤ workRecordsMs`.
- `evidenceRefs[]`: evidence reference list.

| Retraction Cause | Outcome Under `by-cause` |
| :--- | :--- |
| Error declaration reason `urn:epcglobal:cbv:er:did_not_occur` | `not-affected-within-scope` |
| Error declaration reason `urn:epcglobal:cbv:er:incorrect_data` | `insufficient-evidence` |
| Retracted for overlap (unknown) | `insufficient-evidence` |
| Retracted for input conflict | `insufficient-evidence` |

| Refusal Code | Condition |
| :--- | :--- |
| `impact.attribution-missing` | `attributionRuleRef` not found in the same bundle. |
| `impact.scope-containment` | An `incidentScope` path is not in the hierarchy or not a descendant-or-self of an attribution station. |
| `impact.binding-unresolved` | `incidentBindingRef` cannot be resolved. |
| `impact.binding-profile` | Incident binding profile fails schema, version, or structural validation. |
| `impact.retention-order` | Attribution rule `horizonMs` exceeds impact rule `workRecordsMs`. |

### Binding profiles

Each binding profile is the file of an evidence-artifact `schemaVersion` `1.1.0`. Bifrost classes `OccupancyBindingProfile` and `IncidentBindingProfile` parse them with the same strictness as `ReadBindingProfile`: exact field set, constant version. Validation failures yield `*.binding-unresolved` and `*.binding-profile`; the refusal lists of both rule kinds include `attribution.binding-profile` and `impact.binding-profile`. The job compares `schemaSha256` with its built-in schema and refuses to start on a difference (`binding.schema-mismatch`).

| Artifact Property | `occupancy-binding` | `incident-binding` |
| :--- | :--- | :--- |
| Artifact ID prefix | `occupancy-binding-` | `incident-binding-` |
| Profile version | `0.1.0` | `0.1.0` |
| `file.mediaType` | `application/vnd.forseti.occupancy-binding+json` | `application/vnd.forseti.incident-binding+json` |
| `scope` | Station IDs covered (Bifrost reads as equipment instance IDs) | Station IDs covered (Bifrost reads as equipment instance IDs) |
| Topic origin | `topic` field only | `topic` field only |
| Format `schemaId` | `urn:forseti:schema:occupancy:1.0.0` | `urn:forseti:schema:incident:1.0.0` |
| Format `schemaSha256` | SHA-256 of the approved schema bytes | SHA-256 of the approved schema bytes |

A wrong artifact ID prefix or `file.mediaType` fails with `*.binding-profile`; a reference that is not an evidence artifact `1.1.0` of the bundle fails with `*.binding-unresolved`. Bifrost does not compare `scope` with the rule's stations, and checks `format.schemaSha256` only for its form (64 lowercase hexadecimal characters); whether it equals the job's built-in schema is the job's start-up check.

`occupancy-binding` 0.1.0 fields:
- `profileVersion`: const `0.1.0`.
- `topic`: ingestion topic string.
- `format` {`schemaId` (const `urn:forseti:schema:occupancy:1.0.0`), `schemaSha256`}.
- `identity` {`scheme` (const `mes-unit-id`), `pattern`}.
- `enter` {`bizStep`}.
- `leave` {`bizStep`}.
- `correction` {`errorDeclaration` (const `reissue-same-event-id`), `reasons`: a non-empty list of distinct values from `urn:epcglobal:cbv:er:did_not_occur` and `urn:epcglobal:cbv:er:incorrect_data`}.
- `timestampBasis`: const `eventTime`.
- `sources[]`: array of sources.

`incident-binding` 0.1.0 fields:
- `profileVersion`: const `0.1.0`.
- `topic`: ingestion topic string.
- `format` {`schemaId` (const `urn:forseti:schema:incident:1.0.0`), `schemaSha256`}.
- `kinds[]`: array of incident kinds.
- `equipmentRefBasis`: const `isa95-path`.

- `sources[]` is a non-empty array of unique source strings; `kinds[]` is a non-empty array of unique incident kinds; Bifrost checks only that, and the job checks that its incident schema supports each kind.
- `identity.pattern` must match the whole unit ID. Only the syntax common to Java and ECMA-262 is accepted: literals, `.`, bracket character classes with ranges and negation (no property or POSIX classes), the quantifiers `*`, `+`, `?`, `{m}`, `{m,}`, `{m,n}`, alternation `|`, and non-capturing groups `(?:…)`, and a backslash before a non-alphanumeric ASCII character as a literal. Lookaround, backreferences, flags, named and capturing groups, shorthand classes, lazy and possessive quantifiers, a bare `{` and the anchors `^` and `$` are refused (the match is already whole-string).
- Additional fields, empty values and unsupported syntax fail with `*.binding-profile`.

### Input events

Input handling follows uniform redelivery and conflict policies:
- Identical identity and identical content (JCS bytes, ignoring diagnostic `recordTime` and `bridgeReceivedAt`) is a redelivery: disposition `duplicate`, reason `identical-redelivery`, no state change. Which delivery is marked `duplicate` may depend on arrival order; business bodies may not.
- Identical identity and differing content gives every variant the disposition `conflict` (reason `identity-conflict`; `measurement-conflict` and `correction-conflict` for those cases). No variant wins by arriving first; any contribution already applied from that identity is withdrawn and recomputed. If an input first accepted is later found in conflict, its disposition gets a new revision under the same delivery key. The raw bytes of conflicting inputs are kept in `forseti.quarantine` and linked from the disposition's `refs`.

The `pbs-events` stream of resequence-twin-lab is not reused: it carries sequence numbers only, without timestamps or station enter and leave transitions.

#### Process messages

Process messages originate from the bridge, with one Kafka record per Sparkplug DDATA message. Business inputs are DDATA messages; a DBIRTH is counted as having consumed `seq` 1.

| Field | Definition |
| :--- | :--- |
| `edge` | Edge node identifier |
| `device` | Hierarchy path |
| `birthPublishTs` | NBIRTH payload timestamp of the session |
| `bdSeq` | Sparkplug birth sequence number |
| `seq` | Sparkplug message sequence number |
| `publishTs` | Sparkplug payload timestamp set by the publisher |
| `bridgeReceivedAt` | Bridge receipt timestamp (diagnostic) |
| `definitionSha256` | SHA-256 of the NBIRTH `governed_definition` bytes |
| `members[]` | Array of measurement objects |

Each member record contains:
- `metric`: Metric name
- `value`: Sampled value
- `uaStatusCode`: OPC UA status code
- `quality`: Quality indicator
- `sourceTs`: OPC UA source timestamp. Event time is `sourceTs`; other times are diagnostic. Muninn omits `sourceTs` when the server provides none and never fills it with send time.
- `serverTs`: OPC UA server timestamp (diagnostic)

Delivery identity is `sampleId = (edge, device, birthPublishTs, metric, bdSeq, seq, publishTs)`. Measurement identity is `(device, metric, sourceTs, value, uaStatusCode)`: two samples with the same `(device, metric, sourceTs)`, `value` and `uaStatusCode` are one measurement (two polls can read the same sim scan). `quality` is lossy (several status codes map to one quality value) and is excluded from identity and digests. The same `(device, metric, sourceTs)` with a different `value` or `uaStatusCode` is a measurement conflict; both are excluded. Each sample's business time is its own `sourceTs`. A record's telemetry time candidate is the largest valid member `sourceTs`. A member without `sourceTs` is never attributed (sample reason `no-source-timestamp`); a message with no member `sourceTs` at all gets disposition `rejected` with reason `no-source-timestamp` and does not advance event time. Under `plc-stamped`, a message is coherent for an equipment when that equipment's identity, presence, activity and measurement members all carry a `sourceTs` and all are equal; otherwise its samples are `incoherent-snapshot`.

An NBIRTH `definitionSha256` differing from the generation pin rejects that session's samples (`definition-mismatch`) and emits the session event `definition-changed`.

#### Session events

Session events originate from the bridge per device, with identity `(edge, device, birthPublishTs, bdSeq, kind, atSeq)`. The `birthPublishTs` field is the NBIRTH payload timestamp recorded by the bridge at each birth and retained through recording and replay. The Will payload timestamp is built before CONNECT and is not a death time.

| Kind | Payload | Description |
| :--- | :--- | :--- |
| `seq-gap` | {`from`, `to`, `missing`} | Gap detected in message sequence |
| `session-down` | {`from`, `to` null} | Session termination |
| `session-up` | {`at`, `rebirth`} | Session establishment (`rebirth` is true when the bridge observed an NCMD Rebirth for that edge before this birth) |
| `stale-death` | {`detectedEventTime`, `rawPublishTs`, `bdSeq`, `payloadDigest`} | NDEATH uniquely assigned to an earlier session |
| `session-ambiguous` | {`detectedEventTime`, `rawPublishTs`, `bdSeq`, `payloadDigest`} | NDEATH not assignable to exactly one session |
| `definition-changed` | None | Definition digest mismatch against generation pin |

Session event time: `seq-gap` uses `to`, `session-up` uses `at`, `session-down` uses `from`, `definition-changed` uses the birth time. `stale-death` and `session-ambiguous` carry `detectedEventTime` plus the raw death's `rawPublishTs`, `bdSeq` and payload digest, kept apart from the death's own timestamp. `detectedEventTime` is the largest valid telemetry event time the bridge holds for that edge when it observes the death; if none, the latest birth time; if neither, null. It is recorded once and never recomputed in replay; it is used for ordering only and never advances event time. Session event time orders inputs; it never advances stage-1 event time.

#### Occupancy

Occupancy events are generated by a synthetic producer standing in for MES, using EPCIS 2.0 ObjectEvent field names without EPCIS capture. Forseti never issues unit IDs. Event identity is `(source, eventId)`.

- `eventId`: Event identifier
- `eventTime`: Event occurrence timestamp
- `recordTime`: Event ingestion timestamp (diagnostic)
- `unitId`: Unit identifier
- `action`: `OBSERVE`
- `bizStep`: `urn:epcglobal:cbv:bizstep:arriving` or `urn:epcglobal:cbv:bizstep:departing`
- `readPoint`: Read point identifier
- `visitSeq`: Producer correlation field; not part of any identity
- `arrivalEventId`: Departing events only: `eventId` of the arrival this departure closes
- `source`: Producer identifier
- `errorDeclaration`: Optional object containing {`declarationTime`, `reason`, `correctiveEventIDs`}

A correction re-sends the original event with identical `eventId` plus `errorDeclaration`. If every field other than `errorDeclaration` and `recordTime` equals the stored event, it cancels that event; otherwise it is a conflict. A conflicting re-send retracts the visit with reason `input-conflict`. A departure re-sent with different content is an `input-conflict` and its leave claim is withdrawn; two departures with different `eventId` claiming different leaves for the same arrival are both left unapplied and the visit is `insufficient` with reason `correction-conflict`, until a valid cancellation resolves it. An error declaration on a departure cancels only the leave claim and the visit is open again. A correction arriving before its original is verified against the original content it carries and confirms cancellation; it is kept as a tombstone until the horizon. A later copy of the original event does not restore the visit, and a copy beyond the horizon is marked `replay-required`. A correction beyond the horizon is not applied online and gets `replay-required` with reason `correction-beyond-horizon` (a newer declaration time does not extend the online deadline of an old visit). A late occupancy input within the horizon may revise a visit already reported `complete`, with disposition `accepted` and reason `late-after-settlement`. Two differing corrections for one event both get disposition `conflict`, and the visit becomes `insufficient` with reason `correction-conflict`. The `correctiveEventIDs` field is retained as a link only; a corrective arrival is its own event.

#### Incidents

Incident events are emitted by a synthetic producer, with identity `(source, incidentId, revision)`.

- `incidentId`: Incident identifier
- `revision`: Revision sequence number
- `stationRef`: Station identifier
- `equipmentRef`: Descendant-or-self of `stationRef`
- `kind`: `tool-check-failed` or `gauge-drift`
- `detectedAt`: Detection timestamp. `detectedAt` is never the start of the suspect interval.
- `revisionEventTime`: Equal to `detectedAt` for revision 1; for subsequent revisions, the producer's fixed declaration time, which never decreases. Used only for retention.
- `suspectFrom`: Suspect interval start timestamp
- `suspectTo`: Suspect interval end timestamp (null while open)
- `status`: `open`, `closed`, or `withdrawn`
- `evidenceRef`: Reference to external evidence
- `source`: Producer identifier

Incidents carry no event time for stream progress; `revisionEventTime` is used only for retention. Every revision of one `(source, incidentId)` maintains the same `stationRef`; a revision altering it gets disposition `rejected` with reason `station-change`. The highest revision is current; a lower revision arriving later is marked `duplicate` with reason `superseded`. Two differing contents for one revision both get disposition `conflict`.

### Output rows

Every cause deciding a result and every coverage fact resides in the body; execution paths and triggering inputs reside in the envelope. 

All four output journals share a common envelope:
- `evaluationGeneration`: Evaluation generation identifier
- `revision`: Monotonic revision number
- `supersedesDigest`: Digest of the superseded record
- `cause`: Triggering input (diagnostic)
- `eventTimeAt`: The emitting engine's own event-time progress: Flink watermark or Kafka Streams stream-time in stage 1, `W2` in stage 2; diagnostic
- `trigger`: Object containing `topic`, `partition`, and `offset`
- `basisRevisions`: Set of input/basis revision numbers
- `pins`: Pin set containing `rules`, `hierarchy`, `bindings`, `definitions` (map `devicePath` → {`templateRef`, `version`, `sha256`}), and `job` {`engine`, `engineVersion`, `artifactSha256`, `runId`, `timestampSource`, `idleness`, `routingDigest`}

When a row's basis for existing disappears (for example a late departure narrows an open visit's claim so that it no longer overlaps any delivered incident revision), the job writes a `delete` control record {`generation`, `logicalKey`, `revision`, `supersedesDigest`}. Consumers and the oracle fold the journal and compare the live rows. Delete records are part of revision continuity and recovery checks but carry no business body. If the row appears again, it continues with the next revision of the same key.

The `bodyDigest` covers the specific body of each journal. Output data contracts are ODCS documents in forseti; their digests are published in OpenLineage output facets. Each run emits OpenLineage RunEvents (`START`, `COMPLETE`, `FAIL`) with a custom versioned facet carrying head, release, rule, and generation pins.

#### Work record body

Key: `visitRef` (`occupancy-join`: {`mode`, `source`, `arrivalEventId`}; `plc-stamped`: {`mode`, `namespace`, `station`, `visitId`})

- `unitId`: Unit identifier. Under `plc-stamped`, the same `visitId` with a different `unitId` is a conflict.
- `readPoint`: Read point identifier
- `visitSeq`: `occupancy-join` only; producer correlation field, null otherwise
- `status`: `provisional`, `complete`, `insufficient`, or `retracted`
- `retraction`: Present when retracted, containing {`reason`, `errorReason`}, where `reason` is `error-declaration`, `input-conflict`, or `overlap`, and `errorReason` is a CBV URN or null. Computed from the current set of observations. Status precedence: an error declaration on the visit's own arrival → `retracted` (`error-declaration`); else an arrival or departure content conflict → `retracted` (`input-conflict`); else a decided overlap → `retracted` (`overlap`); else any insufficiency reason → `insufficient`; else closed and settled → `complete`; else `provisional`. A closed visit is settled when it has a valid leave and `W1 ≥ leave + allowedLatenessMs`; settled is not the same as `complete` (a settled visit may be `insufficient`). When the other overlapping visit is cancelled, the visit returns to `provisional` or `complete` and `retraction` is removed.
- `insufficiencyReasons[]`: Sorted set of `session-gap`, `session-ambiguous`, `missing-departure`, `missing-visit-close`, `missing-periods`, `bad-share-exceeded`, `no-observed-periods`, `unknown-activity`, `correction-conflict`, `inconsistent-visit-boundary`
- `interval`: {`enter`, `leave`} with `leave` null while open, plus `attributionEnd`: for an open visit, the `enter` of the next valid arrival at the same read point when known, else null. `attributionEnd` bounds sample attribution only; it is not a departure and never makes a visit `complete`.
- `members[]`: Aggregated measurements grouped by result type:
  - Double: {`count`, `min`, `max`, `last`}
  - Boolean: {`count`, `trueCount`, `last`}
  - String: {`count`, `distinctCount`, `last`}
  - `last` is ordered by `sourceTs`; ties are broken by comparing `sampleId` fields in contract order.
- `equipmentEvidence[]`: Array containing {`equipmentPath`, `memberRefs`, `activityIntervals`, `inactiveIntervals`, `coverage` {`observedPeriods`, `missingPeriods`, `badSlots`, `unknownActivityPeriods`, `sessionGaps`, `status`}, `samplesDigest`}. Construction: an activity observation at `t` supports `[t, min(nextObservationTs, t + expectedPeriodMs))`; an accepted-quality true is active, false is inactive; time not supported, Bad, a rejected Uncertain value or an ambiguous session is unknown; intervals are clipped to the evaluation interval J and only adjacent intervals of the same kind are merged; a true is never extended to the next false. Every fact that can change a stage-2 unit judgement is in this body.
- `samplesDigest`: JCS SHA-256 of the sorted list of distinct measurements {`device`, `metric`, `sourceTs`, `value`, `uaStatusCode`}
- `coverage`: Object containing `expectedPeriodMs`, `observedPeriods`, `missingPeriods`, `badSlots`, and `sessionGaps`. Coverage is evaluated over interval J: `[enter, leave)` for a closed visit; for an open visit `[enter, min(W1, attributionEnd, enter + horizonMs))`, omitting bounds that are not known. Tolerances widen only the attribution candidate window, never J. Each member is counted in slots `floor((sourceTs − enter) / expectedPeriodMs)` over J, the last partial slot counting as one. Per member: `observedPeriods` = slots holding at least one deduplicated observation (Bad included, measurement conflicts excluded); `missingPeriods` = slots with no observation; `badSlots` = slots holding a Bad observation. The body lists these per member. Activity: the part of J not covered by valid true or false support intervals is unknown; `unknownActivityPeriods` = activity slots overlapping an unknown part.
- `attributionMode`: Attribution mode name
- `rulesDigest`: JCS SHA-256 of the generation's rule, hierarchy, binding, and `definitions` pins

#### Sample journal body

Key: `sampleId`

Tracks the current state of every sample with a valid `sampleId` (including samples later found in conflict):
- `sourceTs`: OPC UA source timestamp
- `state`: `unattributed`, `attributed`, `replay-required`, or `quarantined`
- `reason`: Status reason code (`no-occupancy`, `ambiguous`, `bad-quality`, `non-finite`, `no-source-timestamp`, `outside-session`, `session-ambiguous`, `identity-mismatch`, `incoherent-snapshot`, `measurement-conflict`, or `beyond-horizon` paired with state `replay-required`)
- `active`: Value of the activity member of the sample's own equipment in the same message; `unknown` if that equipment has none
- `attributedTo`: Work record `visitRef` when attributed

A transition from `unattributed` to `attributed` creates the next revision. A sample inside the attribution windows of two or more non-retracted visits at the same read point is `unattributed` with reason `ambiguous`, contributes to no visit, is not an overlap retraction cause, and is re-attributed when only one candidate remains; when its activity is true it counts as unattributed activity.

#### Impact assessment body

Key: `(source, incidentId, unitId)`

- `status`: `affected-candidate`, `not-affected-within-scope`, or `insufficient-evidence`. `not-affected-within-scope` is not a quality release.
- `reason`: `incident-withdrawn`, `equipment-activity`, `station-visit`, `no-equipment-activity`, `interval-narrowed`, `basis-provisional`, `basis-insufficient`, `basis-retracted-error-declaration`, `basis-retracted-input-conflict`, `basis-retracted-overlap`, or `correction-conflict`
- `incident`: Incident snapshot
- `basis[]`: Array of {`visitRef`, `bodyDigest`} (revision numbers remain in the envelope)
- `evidenceWarnings[]`: Array of {`visitRef`, `insufficiencyReasons`}
- `rulesDigest`: JCS SHA-256 of the generation's rule, hierarchy, binding, and definition pins

#### Affected set body

Key: `(source, incidentId)`

- `incident`: Incident snapshot
- `units[]`: Array of {`unitId`, `status`, `reason`, `bodyDigest`}
- `resolution`: `withdrawn` or null
- `coverage`: Coverage assessment object:
  - `status`: `complete`, `incomplete`, or `unknown`. Evaluates to `unknown` when any unknown evidence exists; else `incomplete` when any known gap remains; else `complete`. A `complete` evaluation requires a closed incident, a settled set, no `beyondRetention`, and no `retentionGap`.
  - `unattributedActivityPeriods`: Count of slots where the activity member was true, no valid open visit existed, and nothing was attributed, scoped to the incident's exact `equipmentRef` in `equipment-evidence` mode and to the station itself in `station-visit` mode. Slots have width `expectedPeriodMs` starting at `suspectFrom`; the range is `[suspectFrom, suspectTo)` for a closed incident and, for an open incident, `[suspectFrom, last delivered observation of the scoped equipment + expectedPeriodMs)`; the last partial slot counts as one; slots are counted per device and summed over the devices in scope.
  - `unknownActivityPeriods`: Count of slots where the activity is Bad, missing, an Uncertain value not accepted by the rule, or in an unknown session, scoped to the incident's exact `equipmentRef` in `equipment-evidence` mode and to the station itself in `station-visit` mode. Slots have width `expectedPeriodMs` starting at `suspectFrom`; the range is `[suspectFrom, suspectTo)` for a closed incident and, for an open incident, `[suspectFrom, last delivered observation of the scoped equipment + expectedPeriodMs)`; the last partial slot counts as one; slots are counted per device and summed over the devices in scope.
  - `provisionalUnits`: Count of units in provisional state
  - `insufficientUnits`: Count of units in insufficient state
  - `sessionGapOverlaps`: Count of overlaps with session gaps
  - `open`: True while incident is open
  - `settled`: True when the station's `W2(s)` has passed `suspectTo` + allowed lateness at the current cut; a later in-horizon input may revise the set
  - `beyondRetention`: Evaluated as `suspectFrom < revisionEventTime − workRecordsMs`, computed once per incident revision
  - `retentionGap`: True unless the verified evidence-retention range recorded in the input and restore manifests (ranges verified to be empty included) covers the required interval; first and last sample times are not used

A withdrawn incident sets `resolution` to `withdrawn`, `open` to false, `settled` to true, and is never `complete`.

### Cut barriers, progress records and the disposition ledgers

Single writer: each station output partition has exactly one owning task. Flink `parallelism=6` and `keyBy(station)` do not guarantee station p → subtask p, so the job records the actual key-group and subtask assignment and selects the Kafka destination partition explicitly; Kafka Streams has one task per partition. Every consumer reads with `isolation.level=read_committed`, set explicitly; the gate asserts it. The gate asserts that progress `cutId` order and reported event time never decrease within a partition and that no business row of a cut follows that cut's progress record.

Cut barriers: after fixing a batch's input offsets, the harness writes a `cut-barrier` record {`generation`, `cutId`, `routingDigest`, `origin` {`topic`, `partition`}, `throughOffset` (last business input offset included), `idleInputs`, `restartEpoch` (integer per generation, starting at 0; the harness increments it for every stage-1 restart injection, that is injections 21, 22(i) and 22(iii); injection 22(ii) does not change it; all barriers of one cut carry the same value), `recoveryRef` (the `injectionId` of the recovery declaration that set that epoch; null at epoch 0)} to every subscribed input partition; a starved station's telemetry is held back by the harness before it is written to raw Kafka (and appended later as a late burst on resume). An input belongs to cut k when its offset on its origin partition is above `throughOffset` of cut k−1 and at or below `throughOffset` of cut k (the first lower bound comes from the batch manifest); routed records carry their origin `{topic, partition, offset}`; a barrier itself is not a business input. The first batch carries telemetry on every subscribed raw telemetry partition; a fixture that does not is refused. In Kafka Streams, each router forwards a barrier, after all of its earlier outputs, to every destination station the raw partition feeds, except a station declared `starved` in that barrier's `idleInputs`, even when the cut produced no business output; the set of raw inputs each station requires is fixed by the pinned routing manifest (`routingDigest`). In Flink, the router forwards the barrier of every subscribed raw telemetry partition, after all of its earlier outputs, to every active station except stations declared `starved` in that barrier's `idleInputs`, including stations that partition does not feed, and forwards occupancy and session barriers to every destination station the raw partition feeds except stations declared `starved`, even when the cut produced no business output; a Flink station's required barrier set is every subscribed raw telemetry partition plus the occupancy and session partitions that feed it, fixed by the pinned routing manifest (`routingDigest`). The required barrier set of `quality.incidents` is all six partitions in both engines; `incidentOffsets` is the list of their `{cutId, partition, throughOffset}` values. A station never writes the cut's progress before it has collected every required barrier; a `starved` input is not a completed barrier for station writers only; the two reject collectors count every barrier they receive, whatever its `idleInputs` says. Barriers are a readiness condition for applying timers; barriers and recovery declarations never advance event time. Within one source, a telemetry record's watermark is emitted before the later barrier and channels are first-in-first-out, so when a Flink station has every required barrier of a cut its operator watermark equals the formula for that cut. In Kafka Streams, when the last barrier arrives after the punctuator ran, pending expired deadlines are checked again against the already observed native stream-time, with no virtual advance.

Recovery control: the harness writes recovery declarations to `forseti.recovery-control` (single partition). A declaration is {`generation`, `injectionId`, `stage` (1), `epochBefore`, `targetEpoch`, `lastWrittenCutId` (the last cut whose batch was written before the injection), `routingDigest`}. Generation creation writes an initial declaration with `targetEpoch` 0 and `lastWrittenCutId` 0. For every stage-1 restart injection the harness sets `targetEpoch = epochBefore + 1`, writes the declaration and confirms it is committed before it kills or rebalances anything (also before starting an added instance). A stage-1 progress writer that is reinitialized, meaning any (re)initialization of that writer's task after its first start in the generation, whether or not a checkpoint or store had anything to restore (Kafka Streams: `forseti gate` pins the cooperative rebalance protocol, under which a rebalance reinitializes only the moved tasks, by recording `group.protocol=classic` and `upgrade.from` unset in `pins.job`; it also pins `acceptable.recovery.lag` = Long.MAX_VALUE, `num.standby.replicas` = 0 and `num.stream.threads` = 1, so that a moved task always changes instance and a rebalance places a moved task as active at once, with no warm-up replica and no later probing rebalance), before it processes any business input, inside the writer's initialization (Flink `initializeState`; Kafka Streams `Processor.init`, so that a job reported RUNNING has completed it), reads `forseti.recovery-control` with `read_committed` up to the partition's current end offset H, selects the latest declaration of its generation and fixes `targetEpoch` E for this initialization; it never reuses a restored control offset or a cached value, and the first progress it writes after that initialization (its business and stage-1 ledger progress alike) carries `initEvidence` {the offset of the selected declaration, H, the engine's restore evidence}; only stage-1 progress writers carry `initEvidence`, because stage 2 never reads the control topic. For each restart injection the harness computes the expected reinitialized set independently of `initEvidence`, from records it takes before and after the injection: for a Flink kill, the restart attempt of the restarted stage-1 job, and the expected set is every stage-1 progress writer; for a Kafka Streams kill of the stage-1 application, the stage-1 progress writers of every task; for a Kafka Streams rebalance, the active-task assignment of every instance before and after the rebalance (read from the instances' local thread metadata; both the before and the after snapshot are taken only at a stable assignment), and the expected set is the stage-1 progress writers of the moved tasks. Writers are mapped from tasks through the recorded task-to-writer map (the station tasks and the stage-1 reject collector task); tasks that host no stage-1 progress writer, such as router tasks, are excluded. A Kafka Streams assignment is stable when every instance is RUNNING, the active-task sets of the instances are pairwise disjoint and their union equals the full task set, and no instance recorded a state transition while the snapshot was read; the harness waits for a stable assignment before it declares a rebalance injection and before it takes each snapshot. For chained injections, the attributed set of injection i is its expected set minus the writers that a later chained injection reinitialized before they wrote any progress after injection i (in 22(iv): empty for Flink, and for Kafka Streams the writers whose tasks the second injection did not move); it is computed from the harness records and the presence of progress records, never from `initEvidence`. For an injection with no later chained injection, the attributed set equals the expected set. First start and recovery are told apart by the declaration and the engine's restore evidence, not by whether a progress exists. Its last committed progress `j` (0 and `admissionW` null when it has none) is read from state consistent with the commit (Kafka Streams: the committed store; Flink: the restored checkpoint and its committed sink transactions). While recovering, the writer keeps cuts `≤ j` as committed; for a barrier with `restartEpoch < E` it keeps the inputs and writes no progress; once every required barrier of the first cut r with `restartEpoch == E` has arrived, it applies the inputs of cuts j+1 … r as one sequence in the contract order, fires timers with `deadline ≤ timerW` in deadline order, writes progress r with `mergedCutIds` = [j+1 … r−1], and leaves recovery. Barriers with different `restartEpoch` or `recoveryRef` are never combined into one recovered cut; when a recovering writer receives a barrier with `restartEpoch > E`, it reads `forseti.recovery-control` again up to its current end; if the latest declaration of its generation has `targetEpoch` at or above that barrier's epoch, the writer adopts that latest `targetEpoch` as E, keeps j and every held input, and continues recovery; otherwise it records `error` (a later restart injection may leave a recovering writer untouched, for example a Kafka Streams rebalance that moves only other tasks, and barriers of different partitions are not ordered among themselves). A writer that was not reinitialized never enters recovery: it continues as before, and only the receive-order trigger applies to it. The reading of the control topic and the recovery time are measured under comparison item 7 and item 3. After the injection, the injection manifest records the expected reinitialized set, the attributed set and, separately, the stage-1 writers whose first progress after the injection carries `initEvidence` pointing to that injection's declaration; neither list is a precondition of initialization.

The harness writes batch k+1 only after cut k is complete, or after injection 24's observation deadline has recorded a miss, with one exception: after a stage-1 restart injection (injections 21, 22(i) and 22(iii), a Kafka Streams rebalance counting as a stage-1 restart; comparison item 3), once every forseti job of the run is RUNNING (Flink: the job and all its tasks RUNNING; Kafka Streams: a stable assignment, see Recovery control), the harness writes batch k+1 without waiting for cut k, as the first batch of the new `restartEpoch`. The harness writes the first batch of a generation only after every forseti job is RUNNING (Kafka Streams: a stable assignment). When only the stage-2 job was restarted, the harness waits for cut k as usual. A further restart injection may follow before the merged cut of the previous one completes; it waits until every forseti job is RUNNING (Kafka Streams: a stable assignment), which implies the writers' initialization (see Recovery control). A restart injection is never placed while injection 24 is in effect (injection 24 runs only in `forseti requirements`, restart injections only in `forseti gate`). That batch carries, on every subscribed raw telemetry partition, a record whose `sourceTs` is at or above that partition's previous maximum; if it cannot, the injection is recorded as `error`. A restart injection is never placed on the recording's last batch, nor on the two bracketing batches of a mandatory deadline, nor on the batch before them. In both stages, a progress writer keeps a cut's inputs in recoverable state until every required barrier of the cut has arrived, and keeps, for each of its input partitions, the inputs of every later cut in recoverable state until it has written progress for the current cut; Stage-2 inputs are `forseti.work-records` and `forseti.samples` partition `p(s)` (a stage-1 progress record is the barrier) and `quality.incidents` (the incident barriers). Once every required barrier of the cut has arrived, the station applies the cut's inputs in the contract order, then fires timers with `deadline ≤ timerW` in deadline order, where `timerW` is the engine's own event time. An engine timer callback that fires before the barriers are complete is held and rechecked when they complete. Every reinitialized stage-1 progress writer (as defined in Recovery control: the station tasks for business and stage-1 ledger progress, and the stage-1 reject collector) follows the recovery control rule above; writers that were not reinitialized do not skip. Likewise, if a stage-1 station receives inputs of cut k+1 before it has written progress for cut k, it does not write progress for cut k: cut k is skipped at that station (not completed, not compared). The writer applies the inputs of the merged cuts as one sequence in the contract order once the barriers of cut k+1 are complete, then fires timers with `deadline ≤ timerW` in deadline order, and lists the merged cuts in `mergedCutIds` of its next progress; `barrierRefs` lists only barriers actually received. A starved station's stage 1, on resume at cut r with last committed progress m, writes progress r with `mergedCutIds` = [m+1 … r−1]; the reason (the cuts' `idleInputs` declaring it `starved`) is checked by the gate against the harness cut records, not by the writer. A cut that injection 24 recorded as a miss stays recorded with its evidence; on resume the next completed cut covers it, no past progress is written for it, and it is not counted as a restart skip. A cut appears in `mergedCutIds` only for a restart recovery, the receive-order trigger within a restart window (a writer's restart window is the cuts above its last progress before a restart injection and below the first cut whose `restartEpoch` is at or above that injection's `targetEpoch`; for chained injections the windows are joined), a starvation under injection 24, or by following stage 1 (stage 2 and ledgers); restart skips are counted under comparison item 3 as pairs (logical writer, cut), without counting a writer's two evidence topics or ledger twice and without counting an earlier unfinished recovery attempt again; reject collectors are reported separately. Inputs are admitted against `admissionW`, the station's `W1` at its last committed progress (as restored) (null before its first progress, meaning no admission bound yet): the horizon test, `onlineExpiresAt` expiry, `correction-beyond-horizon` and `late-after-settlement` use it. The inputs of all merged cuts are admitted against the `admissionW` of the writer's last committed progress. `admissionW` is persisted and restored consistently with the outputs; after recovery the job never reports `max(restored value, native time)` as the engine's time, and new timers fire only after the native time has caught up. A station whose required barriers are incomplete applies no timers and writes no progress; its reported `W1` stays at its last completed cut.

After collecting every required barrier of a cut, once the station's engine event time is at or past its last reported `W1` (no bound at its first progress), reporting the current engine event time as `W1`, and writing every business, timer and delete row caused by it, the stage-1 task of station `s` writes a progress record to partition `p(s)` of both `forseti.work-records` and `forseti.samples`, after the cut's related outputs in each destination partition and committed consistently with them on recovery. It writes progress even when no body changed. Progress records have no envelope and are excluded from journal rows, dispositions, oracle body comparison, "highest revision per key" and the equal-consecutive-body check. The stage-1 progress record body contains:
- `type`: `progress`
- `generation`
- `station`
- `cutId`
- `producerEpoch`
- `W1` (the event time up to which stage-1 timers have fired, which the gate asserts for equality against the per-engine formula: Kafka Streams `W1(s) = max telemetry sourceTs routed to s − outOfOrdernessMs − 1`; Flink `W1 = min over subscribed raw telemetry partitions p of (largest delivered sourceTs in p − outOfOrdernessMs − 1)`, where "largest" is taken over everything delivered in the generation, before and after any restart, and "delivered" means up to the cut's `throughOffset` on each partition, up to the `throughOffset` of the cut the progress closes; both formulas use the same `−1` because out-of-orderness means a delay of at most `outOfOrdernessMs`, so a deadline d is safe once the largest timestamp is at least d + `outOfOrdernessMs` + 1, matching the rule of Flink's `BoundedOutOfOrdernessWatermarks`, confirmed for the pinned version)
- `admissionW`
- `barrierRefs` (`{cutId, origin {topic, partition}, throughOffset}` of each collected barrier)
- `restartEpoch`
- `recoveryRef` (of the barriers of this cut)
- `mergedCutIds` (sorted cutIds folded into this progress without their own progress, excluding the current cutId; empty for a normal cut)
- `initEvidence` (only on the first progress after an initialization)

Stage 2 writes its progress record to partition `p(s)` of `forseti.affected` and `forseti.affected-sets` after the station has processed the cut's progress on both evidence topics and the incident routing barriers and has written every judgement, timer, delete and set change they caused. Stage 2 never skips on its own restart (its inputs are durable in Kafka). A stage-2 station waiting for cut c that receives, on either evidence topic, a stage-1 progress with `cutId` r > c whose `mergedCutIds` contain c holds c; it writes progress r only after it has the progress r on both evidence topics with equal `mergedCutIds` and the incident barriers of r and of every merged cut, applies the inputs of all of them, fires timers with `deadline ≤ W2(s)` and writes progress r with the same `mergedCutIds`. A missing cut without such evidence is waited for, never skipped:
- `type`: `progress`
- `generation`
- `station`
- `cutId`
- `producerEpoch`
- `W2`
- `stage1ProgressRefs` (the stage-1 progress records it processed)
- `incidentOffsets` (`{cutId, partition, throughOffset}` for each collected incident barrier: 6 × (1 + number of `mergedCutIds`); incident barriers already processed are taken from stored processing evidence and not applied again; `incidentOffsets` is a completion condition; it is not part of `W2`)
- `mergedCutIds`

`forseti.dispositions-stage1` owns the dispositions of `ot.messages`, `ot.sessions` and `mfg.occupancy`; `forseti.dispositions-stage2` owns those of `quality.incidents`. Each has station partitions 0–5 plus partition 6 for unroutable inputs, written by that stage's single reject collector. The key is {`generation`, `inputTopic`, `inputPartition`, `inputOffset`, `part`}, with `part` separating an original from its error declaration when both are judged: `part` is `event` | `errorDeclaration` for occupancy and `message` (whole-message rejection) | `member:<metric>` (one member's disposition; the other members stay accepted) for `ot.messages`. The body contains:
- `inputKind`
- `identity` (semantic identity)
- `payloadDigest`
- `disposition`: `accepted` | `duplicate` | `conflict` | `rejected` | `replay-required`
- `reason`
- `refs`

Reasons form a closed vocabulary:
- `accepted`: `accepted`, `late-after-settlement`
- `duplicate`: `identical-redelivery`, `superseded`
- `conflict`: `identity-conflict`, `measurement-conflict`, `correction-conflict`
- `rejected`: `schema-invalid`, `unknown-device`, `unknown-readpoint`, `definition-mismatch`, `outside-incident-scope`, `station-change`, `no-source-timestamp`
- `replay-required`: `beyond-horizon`, `correction-beyond-horizon`

When an accepted input is later found in conflict, its delivery key gets the next disposition revision, every variant of that identity is updated to `conflict`, and their contributions are withdrawn. Ledgers carry cut progress too, and the harness waits for both business and ledger progress. A ledger progress follows the business progress of the same writer: a stage-1 station ledger skips the same cuts as its station; the stage-1 reject collector follows the recovery control rule; stage-2 ledgers follow stage 2. The stage-1 reject collector's required barrier set is every subscribed raw telemetry, session and occupancy partition; the stage-2 reject collector's is the six incident partitions; both partition-6 progresses count toward cut completion; the stage-2 reject collector never skips (`mergedCutIds` always empty). The ledger progress body is {`type`, `generation`, `stage`, `partition`, `station` (null for partition 6), `cutId`, `producerEpoch`, `barrierRefs`, `mergedCutIds`, `restartEpoch`, `recoveryRef`, `initEvidence` (stage-1 ledgers only, on the first progress after an initialization)}, without event time, written even when the cut rejected nothing. Which delivery was accepted first and which became `duplicate` is an order-dependent diagnostic, excluded from the oracle business comparison; complete input coverage, final conflict exclusion and missing dispositions are checked separately. `forseti.quarantine` is a side store of raw bytes, linked from `refs`, with no judgement state of its own.

## Topics and partitioning

Logical keys and Kafka partition keys are different things; the topic contract states the partition rule separately. The PoC uses JSON and Apache Kafka in KRaft mode, single node. Record timestamps are `LogAppendTime`. Input topics retain 30 d, output journals at least 7 d, and the gate asserts these topic settings. Six partitions exist everywhere except `forseti.generations` (one), `forseti.recovery-control` (one) and the two disposition ledgers (seven).

| Topic | Partition key | Description |
| :--- | :--- | :--- |
| `ot.messages` | `device` | Sparkplug messages |
| `ot.sessions` | `device` | session events fanned out per device |
| `mfg.occupancy` | `readPoint` through the station partitioner (the producer writes it already partitioned by station) | occupancy |
| `quality.incidents` | `equipmentRef` | incidents |
| `forseti.messages-by-station` | station | Kafka Streams internal: stage-1 routing of messages |
| `forseti.sessions-by-station` | station | Kafka Streams internal: stage-1 routing of session events |
| `forseti.incidents-by-station` | station | Kafka Streams internal: incident routing, owned by stage 2 |
| `forseti.work-records` | station | stage 1, with progress |
| `forseti.samples` | station | stage 1 sample journal, with progress |
| `forseti.affected` | station | stage 2 per unit, with progress |
| `forseti.affected-sets` | station | stage 2 per incident, with progress |
| `forseti.dispositions-stage1` | station, partition 6 for unroutable | disposition ledger, stage 1 inputs |
| `forseti.dispositions-stage2` | station, partition 6 for unroutable | disposition ledger, incidents |
| `forseti.quarantine` | input delivery | raw bytes of conflicting inputs, linked from dispositions |
| `forseti.generations` | scope | control, written only by the CLI, single partition |
| `forseti.recovery-control` | generation | control, recovery declarations written only by the harness, single partition |
| `forseti.lineage` | run id | OpenLineage |

Station partitions are fixed by a custom partitioner from the sorted `stations[]` of the pinned rule: Station1 → 0, Station2 → 1, Station3 → 2; partitions 3, 4 and 5 are unassigned, and the harness asserts the assignment.

Raw topics keep device keys. Raw placement uses the standard Kafka partitioner on device keys; the device → raw partition → station placement is pinned in the routing manifest and verified by preflight. Stage 1 routes inputs to stations: Flink keys `ot.messages`, `ot.sessions` and `mfg.occupancy` by station (network shuffle) and never assumes raw device keys already share a partition; Kafka Streams routes messages and sessions with `addSink → forseti.messages-by-station / forseti.sessions-by-station → addSource` (a Kafka hop) and reads occupancy directly. The routing cost of each engine is measured and reported. When a raw partition carries several stations, starving one station withholds only that station's telemetry and barriers; the other stations' records and barriers pass.

The incident router `incident-router` belongs to the stage-2 topology (Flink `keyBy`; Kafka Streams `addSink → forseti.incidents-by-station → addSource`) and stays outside the decision function. The incident router forwards every incident barrier to every active station, including stations declared `starved`. Stage 2 reads work records, the sample journal (for activity coverage) and incidents by station with no further repartition.

Each comparison run writes to its own topic prefix so the engines never overwrite each other.

## Correction semantics

### Stage 1
Timers run on each engine's own event-time mechanism. In Flink, the operator watermark advances from `ot.messages` `sourceTs` minus out-of-orderness, and event-time timers fire on it; Flink uses the bound of its own `BoundedOutOfOrdernessWatermarks`, emitted for every record by a delegating generator (automatic interval 0), so its watermark is the minimum over subscribed raw telemetry partitions of (largest `sourceTs` − out-of-orderness − 1); occupancy and session sources neither advance nor hold it, and telemetry sources are never marked idle. In Kafka Streams, task stream-time is the largest timestamp seen; deadlines are converted with `outOfOrdernessMs`, kept in a persisted deadline index, and a `STREAM_TIME` punctuator processes every expired deadline; a stage-1 timer fires when stream-time > deadline + out-of-orderness, which gives the same −1 bound. Harness records never advance event time. Occupancy and session inputs neither advance nor hold event time, but their partitions carry cut barriers. Kafka Streams `partitionTime` is not returned as a timestamp because it can be -1, and Flink `MAX_WATERMARK` is not used.

Intervals are half-open `[start, end)`. A sample attributes to a visit when `enter − enterToleranceMs ≤ sourceTs` and `sourceTs` is below the visit's upper bound: `leave + leaveToleranceMs` for a closed visit; for an open visit, the `attributionEnd` (the next valid arrival at the same read point) when known, else no upper bound yet. A sample inside the attribution windows of two or more non-retracted visits at the same read point is `unattributed` (`ambiguous`), contributes to no visit and is not an overlap retraction cause; it is re-attributed when only one candidate remains. A timer fires when `W ≥ deadline`. A sample is inside the horizon when `sourceTs ≥ admissionW − horizonMs`, where `admissionW` is the station's `W1` at its last committed progress, as restored (no bound before its first progress). Inputs with equal event time are ordered by input kind (session < occupancy < sample), then the canonical bytes of their identity, then `payloadDigest`; every input of a cut applies before that cut's timers, which then fire in deadline order up to the engine's own event time.

Visit states transition as follows:
- none → `provisional` on arriving.
- `provisional` → `complete` when departing is seen and the watermark passes leave + allowed lateness.
- `provisional` → `insufficient` (reason `missing-departure`) when the horizon passes without departing.

An error declaration on the arrival transitions the visit to `retracted` with `retraction.reason` `error-declaration` and its CBV reason. An error declaration on a departure cancels only the leave claim; the visit is open again and becomes `provisional` or `insufficient` (`missing-departure`) depending on the current event time. Two departures claiming different leaves for one arrival are both unapplied and the visit is `insufficient` (`correction-conflict`). Overlap is decided only between two visits with valid arrival and departure, comparing actual `[enter, leave)`; an open visit never retracts another; when a late departure reveals an overlap, both visits are recomputed then. When two units overlap at one station, both transition to `retracted` with reason `overlap`; if one is later cancelled, the other returns to `provisional` or `complete` and loses its retraction. There is no first or last choice. Status precedence is: arrival error declaration → `retracted` (`error-declaration`); arrival or departure content conflict → `retracted` (`input-conflict`); decided overlap → `retracted` (`overlap`); any insufficiency reason → `insufficient`; closed and settled → `complete`; otherwise `provisional`. All diagnostic reasons are kept in `insufficiencyReasons[]`; only the top status follows the precedence. Allowed lateness is not an immutability deadline: an occupancy input within the horizon may still revise a visit already reported `complete`.

A late sample within the horizon produces the next revision of the affected visit. Unattributed samples stay in state until the horizon, so an arrival that comes after its samples attributes them (`unattributed` → `attributed`). Beyond the horizon, the sample's state is `replay-required`; the visit's coverage counts it missing, and a later generation replayed in event-time order attributes it.

A session gap overlapping a live visit transitions the visit to `insufficient` with reason `session-gap`. A death that cannot be assigned to one session makes overlapping visits `insufficient` with reason `session-ambiguous`. Open gaps end at the station's `W1` and are counted only inside the evaluation interval J. Gap rules apply only to visits that are not retracted.

An observation with activity true and no valid open visit is unattributed activity (reason `no-occupancy`); it does not change any visit. A quiet occupancy stream during an open visit changes nothing.

Under `plc-stamped`, each identified equipment reports `UnitId`, `VisitId` and `VisitPresent` in the same scan snapshot as its activity and measurements. On an equipment, the visit start is the valid `VisitPresent=true` snapshot with the earliest `sourceTs` (not the first to arrive), and the end is the earliest later valid `VisitPresent=false` snapshot with the same ID (the end snapshot does not clear the IDs); late snapshots are included when the lifecycle is rebuilt. Measurements in the end snapshot are not attributed to the visit. A `VisitPresent=true` with the same ID after the end is `inconsistent-visit-boundary`. An ID change without a preceding false leaves the earlier visit `insufficient` with `missing-visit-close`. With neither a false snapshot nor an ID change, the visit becomes `insufficient` (`missing-departure`) when `W1 ≥ enter + horizonMs`. Equipment of one station visit uses the same namespace, `VisitId` and unit ID; if they claim different boundaries the visit is `insufficient` with `inconsistent-visit-boundary`. A closed visit settles at `leave + allowedLatenessMs`. `Running=false` never ends a visit: a tool not used during a visit still reports the visit's IDs with `VisitPresent=true` and `Running=false`, which is its inactivity evidence. The same `VisitId` with a different unit ID is a conflict. A message mixing snapshots is not attributed (`incoherent-snapshot`). With `crossCheck occupancy`, a different occupant at `sourceTs` marks `identity-mismatch` without changing the attribution.

Online state of a visit expires at `onlineExpiresAt` — `leave + horizonMs + allowedLatenessMs` for a closed visit, `enter + horizonMs + allowedLatenessMs` for an open one. After expiry, an input that needs that visit's arrival never creates a new visit; it gets `replay-required`. Emitted insufficient records and stage-2 evidence follow their own retention.

### Stage 2
Stage 2 executes per incident and unit, from the unit's own basis and the incident only. Stage 2 uses no engine watermark: a core `ProgressTracker` holds the last progress record per input topic and station, computes `W2(s) = min(progress[forseti.work-records][s], progress[forseti.samples][s])`, and fires stage-2 timers with `deadline ≤ W2(s)`. Flink stage 2 uses `noWatermarks()` and no event-time timers; Kafka Streams stage 2 uses no punctuator. Incidents neither advance nor hold `W2`.

Stage-2 timers cover incident settlement, coverage re-evaluation as progress advances, re-evaluation of relatedness for open ends, and evidence-retention cleanup. A narrowed or withdrawn incident revision is recomputed immediately.

The row set of a cut is computed from the visit claims rebuilt from every input delivered up to that cut, crossed with the intervals of the delivered incident revisions (incident-side widening accumulates across revisions). Open intervals an engine computed in passing are never lasting grounds for a row. A cancelled original visit claim is kept for audit and for injection 18, but is not current evidence. When a row's grounds disappear, a `delete` control record ends it. Judgement uses the highest incident revision and the current basis.

A basis is `related` when its visit interval and the current highest incident revision's interval satisfy the rule's `interval.overlap` mode; when an open boundary means overlap can be neither confirmed nor excluded, the basis is `possibly-related` and counts as insufficient evidence.

The decision table resolves on the first match from the top:

| Priority | Condition | Status | Reason and details |
|---|---|---|---|
| 1 | incident `withdrawn` | per `onWithdrawn` | `incident-withdrawn` |
| 2 | a related `complete` basis whose tool activity meets the interval rule | `affected-candidate` | `equipment-activity` (`station-visit` in station-visit mode); other incomplete evidence goes to `evidenceWarnings` and never removes positive evidence |
| 3 | no related and no possibly-related basis | `not-affected-within-scope` | `interval-narrowed` |
| 4 | a related basis with `correction-conflict` | `insufficient-evidence` | `correction-conflict` |
| 5 | a related or possibly-related `provisional` or `insufficient` basis | `insufficient-evidence` | `basis-provisional` or `basis-insufficient` |
| 6 | a related basis retracted for overlap, input conflict or `incorrect_data` | `insufficient-evidence` | `basis-retracted-overlap`, `basis-retracted-input-conflict` or `basis-retracted-error-declaration` |
| 7 | related `complete` evidence proves the target tool inactive: the whole judged interval (`[enter, leave) ∩` the current suspect interval, non-empty) for the target tool is covered by valid inactive intervals with no unknown or missing period | `not-affected-within-scope` | `no-equipment-activity` (absence of Good torque alone proves nothing) |
| 8 | every remaining related basis cancelled with `did_not_occur` | per `onBasisRetracted` | `basis-retracted-error-declaration` under both `by-cause` and `always-insufficient` |
| 9 | anything else | `insufficient-evidence` | `basis-insufficient` |

A unit row's revision triggers on an incident revision or a change of a basis `bodyDigest`. Set coverage changes revise only the set row.

For retention, `beyondRetention` and `retentionGap` are evaluated per incident revision (see Output rows); the operator replays a generation. Cancelled bases stay in stage-2 state for the work-record retention period.

### Generations and revisions
Journal identity is `(evaluationGeneration, key)`. Revisions start at 1 per generation. `delete` control records take part in revision numbering.

Rows in `forseti.generations` contain:
- `generation` (integer, increasing)
- `status` (`registered` | `replaying` | `verified` | `current` | `retired`)
- `rulesDigest`
- `inputReleases`
- `replayFrom` offsets
- `headVersion`
- `releaseEvidence` (snapshot of the approved release and its verification evidence)
- `gateReportSha256`
- `at`

Each Kafka record carries the header `forseti-generation`. A recovered job continues its generation's revisions. Consecutive revisions with equal `bodyDigest` are a gate failure.

The command `generation new` verifies the head against the real current time and pins the snapshot. A replay uses the snapshot. The command `promote` again requires approval evidence valid now. The head version floor lives in forseti state at `<stateDir>/head-floors/<stream>.json` and survives generation changes (the same shape as Vor's floor store, no shared code).

## Engine comparison
Evaluation uses the same core jar, the same Kafka, 6 partitions, keyed operator and sink parallelism equal to partitions (6) and Flink source parallelism equal to the number of subscribed partitions of each topic, the same JVM and heap, RocksDB state on both engines, and exactly-once processing on both (Flink `EXACTLY_ONCE` with checkpoint interval 1000 ms; Kafka Streams `exactly_once_v2` with `commit.interval.ms` 1000). Both run the same rules over the same recording replayed, excluding one warm-up, under pinned versions. Engines run one after the other. Memory counts the whole process and state store. Completion time and transaction visibility time are recorded too.

Gate runs set Flink `withIdleness` off. Gate runs fix Kafka Streams `max.task.idle.ms = -1`, record it in `pins.job` and assert it (0 does not disable task idling; -1 does). The Flink watermark emission mode (delegating per-record generator) and source parallelism are recorded in `pins.job`. In the live full loop only, a Flink adapter setting `flink.idleness.ms` is allowed, recorded in `pins.job.idleness` and listed under item 7; gate runs assert it is off. Results are published in `docs/COMPARISON.md` as measured tables without estimates.

| Item | Evaluation dimension | Scope and criteria | Engine notes |
|---|---|---|---|
| 1 | semantic equality | oracle match of bodies at every cut on every injection | both must pass first |
| 2 | late data | longest lateness still corrected online, journal counts, zero inputs without disposition | measured in stage 1: Flink timers and side outputs against Kafka Streams punctuators over a persisted deadline index and manual branching implementing the same rule; Flink's allowed lateness is a window feature and windows are not used |
| 3 | restart | duplicates, losses, revision gaps after `kill -9`; wall time to resume; restart skips per engine as (logical writer, cut) pairs (reflecting the commit granularity and, for Kafka Streams, the pinned rebalance protocol), counted only for writers actually reinitialized, plus receive-order skips of writers that kept running | checkpoint restore and two-phase-commit sink vs changelog restore, rebalance, EOS v2 |
| 4 | generation replay | wall time and resources for a 24 h event-time replay and promotion | same procedure on both |
| 5 | station isolation (requirement) | (a) occupancy silent 5 min during an open visit: no change; (b) activity true with no open visit: zero false `complete`; (c) the telemetry supply of one declared station cut: every other station settles by the observation deadline of 60 s; (d) after resume (injection 24's resume), after restart (injections 21 and 22, comparing cuts both runs completed, skipped cuts excluded), and after a speed change (the baseline recording replayed with the harness batch interval ×2 and ×½): the same bodies at every cut both runs completed | both must pass; a miss is recorded as a miss; `forseti requirements` records pass, miss or error with evidence. Flink's stage-1 watermark is the minimum over all subscribed raw telemetry partitions, so (c) is expected to be its weak point; (c) runs on a dedicated and a shared raw placement; Kafka Streams keeps one task per partition |
| 6 | determinism | bodies over 3 runs at each comparable cut; revision counts reported | |
| 7 | adapter cost | adapter lines of code, workarounds listed (including the Flink delegating watermark generator), the Flink barrier fan-out (per cut, the number of subscribed raw telemetry partitions × active stations barrier records), measured on the dedicated and on the shared placement, the reading of `forseti.recovery-control` at writer initialization and the recovery time it adds | includes the routing cost: Flink shuffles three inputs by station; Kafka Streams adds two Kafka hops for messages and sessions |
| 8 | footprint | containers, memory, start time | a Kafka Streams job is one process |
| 9 | latency | `L-commit`: trigger input append → output append visible under `read_committed`, measured by the gate from offsets outside the engines, p50/p99, target p99 5 s for incidents and corrections on evidence that is already complete; `L-timer`: rows emitted by timers, output append minus timer event time, reported without target; `L-business`: departing event time → publication, dominated by allowed lateness; correction convergence: correction input append → stage 1, stage 2 and set settled | |

Reasons to keep Flink must come from items 2, 3 and 4; item 5 is a requirement both engines must meet. Kafka Streams is expected to win 7 and 8 and the report says so. A requirement that an engine misses is recorded as missed, not relaxed. An engine is eligible only when it passes the semantic gate and every required requirement; a miss makes it ineligible, and the comparison report is still produced.

## Testing and gates

### Cuts and the independent oracle

A cut is defined as `C = (cutId, inputOffsets, idleInputs)`. Field list for `idleInputs[]`:
* `station`
* `topic`
* `partition`
* `frozenAt`
* `kind` (`unassigned` | `starved`)
* `evidenceRef`

In station-partitioned topics, partitions mapped to no station (3, 4, 5) are not subscribed and not waited on; raw-topic subscriptions are the partitions the routing manifest lists as carrying a declared device; `quality.incidents` subscribes all six. A starved active input freezes its station's frontier and never lets coverage become `complete`. The test harness feeds the recording in batches. After each batch, the harness writes cut barriers (see Contracts) to every subscribed input partition and writes the next batch only after the cut is complete (or injection 24 has recorded a miss, or after a stage-1 restart injection, where the harness writes the first batch of the new `restartEpoch` without waiting for cut k; see Contracts); a starved station's telemetry is withheld before it reaches raw Kafka; barriers never advance event time. Flink subscribes statically only to partitions assigned by the pinned routing; raw topics are keyed by device, so child tools count toward partition assignment, and the routing manifest records which raw partitions carry which stations.

Station s completed cut k when s's stage-1 progress k on both evidence topics, stage-2 progress k and s's two ledger progresses k are committed. Cut k is complete when every active, non-starved station completed k, both reject collectors (partition 6) wrote progress k and the incident barriers are processed. Completing cut k+1 never requires a skipped cut k; every writer that did not write progress for k lists k in the `mergedCutIds` of its next progress. A skipped cut (see Contracts) is neither complete nor compared; the oracle evaluates the next completed cut with the inputs of all merged cuts, receives each station's actual completed-cut history, the merged cuts (`mergedCutIds`) and their offset ranges and the real `admissionW`, and never assumes a completion of a skipped cut. The harness never treats "no output for a while" as completion. The gate then folds the journals (current generation, highest revision per key, `delete` records applied) and compares the live rows with `forseti-oracle(events before offsets)`, where the oracle receives, per cut, the included offset ranges, the batch manifest, each station's `cutId` and `admissionW`, and evaluates inputs admitted against `admissionW` and timers up to the `W1` the station reports, with `W2(s, cut) = W1(s, cut)`. The gate asserts that each reported `W1` equals the engine's formula (Kafka Streams: max telemetry `sourceTs` routed to the station − `outOfOrdernessMs` − 1; Flink: min over subscribed raw telemetry partitions of (largest delivered `sourceTs` − `outOfOrdernessMs` − 1)). Starved stations are frozen at their last collected barrier. The oracle is a Python batch program that shares no code with `forseti-core` and executes a sort-and-sweep algorithm. Intermediate revisions are checked only for monotonicity, integrity, and absence of equal consecutive bodies.

A cut is comparable between two runs (repeated runs, Flink against Kafka Streams, held-back against on-time) when, at every non-starved station, no ground-truth deadline (`leave + allowedLatenessMs`, `enter + horizonMs`, `suspectTo + allowedLatenessMs`, `onlineExpiresAt`, or `sourceTs + horizonMs` of any sample or correction) lies between the two runs' `W1` values or between their `admissionW` values and, at every station where either run has an open visit or an open session gap, the two runs' `W1` are equal. Direct body comparisons are made only at comparable cuts; the baseline fixture must contain settled cuts at which every visit and session gap is closed, compared directly between Flink and Kafka Streams; a missing comparison is a gate failure. Elsewhere each engine is compared with the oracle at its own `W1`.

For every deadline d (each visit's `leave + allowedLatenessMs`, injection 6's `enter + horizonMs`), the harness ends one batch at the last scan with `sourceTs ≤ d + outOfOrdernessMs` and the next batch at the following scan; the gate finds, per engine and station, two consecutive cuts that the station completed (adjacent in that station's sequence of completed cuts; a restart injection is never placed on the two bracketing batches or the batch before them) with `W1_A < d ≤ W1_B`, compares both with the oracle and asserts the state transition at B. Injection 23 uses a planned A: the batch boundary is placed at a scan where every subscribed raw telemetry partition has the same largest `sourceTs` T on the scan grid, so `A = T − outOfOrdernessMs − 1` in both engines; the harness asserts that the reported `W1` equals A before sending the late samples, and records `error` otherwise (the hole is never moved); the next batch carries three late samples with `sourceTs = A − horizonMs + 1`, `A − horizonMs` and `A − horizonMs − 1`; the synthetic recording leaves a hole on one member at the scan `A − horizonMs + 1`, which the first late sample fills; the three late samples carry new `sampleId`s continuing the session's `seq`, so no `seq-gap` and no `measurement-conflict` arise; the first two are admitted online, the third gets `replay-required` (`beyond-horizon`).

Horizon scenarios (injections 6, 10e, 23 and the after-horizon cuts) use synthesized continuous recordings, marked `synthetic` in the manifest with source and digest; every recording carries a telemetry tail of at least `allowedLatenessMs + outOfOrdernessMs + expectedPeriodMs` on every subscribed device and partition after the scenario's last business event (to `enter + horizonMs` for horizon scenarios). Synthetic tails are ordinary data and follow every quality, session and coverage rule.

The generator keeps the complete ground truth separate from delivered records (delayed, deleted, duplicated). With complete input, the result equals ground truth. With lossy input, execution is judged on observable evidence only. A second check compares Flink output bodies directly to Kafka Streams output bodies. The JCS vector file executes in both core and oracle tests.

The scenario fixture defines visits of 60 s duration, 120 s between adjacent visits, and the last measurement 1 s before leave. The scenario manifest lists:
* `unitId`
* `visitId`
* `station`
* `enter`
* `leave`
* `toolsUsed[]`
* the expected attribution of every `sampleId`

The driver includes T1-only, T2-only, both-tool, and no-tool visits at Station1; a tool not used in a visit reports the visit's IDs with `VisitPresent=true` and `Running=false`. Semantic gate fixtures are small; the 1,000-unit scale is used only by comparison runs; sizes are pinned in the plan.

### Fault injections

| ID | Stage / Category | Fault injection | Expected result |
| :--- | :--- | :--- | :--- |
| 1 | Stage 1 | Telemetry held 10 min in the bridge and released | Same bodies as on time at the settled cut |
| 2 | Stage 1 | Departing after the samples | `provisional` then `complete` |
| 2b | Stage 1 | Arriving 2 min after its samples | Samples `unattributed` then `attributed`, included in the visit |
| 3 | Stage 1 | Bad via `Sim/<member>.StatusCode` | `bad-quality`, visit `insufficient` past `maxBadShare` |
| 4 | Stage 1 | Two closed visits overlapping at one station | Both `retracted` (reason `overlap`) |
| 5 | Stage 1 | Occupancy redelivered | Disposition `duplicate` (`identical-redelivery`), no state change |
| 6 | Stage 1 | Arriving without departing past the horizon | `insufficient` (`missing-departure`); later visits at that station are not retracted |
| 7 | Stage 1 | Activity true with no open visit | `no-occupancy`, counted in set coverage, zero false `complete` |
| 8 | Stage 1 | Muninn killed mid-visit → NDEATH | `insufficient` (`session-gap`) |
| 9 | Stage 1 | Source clock +30 s in the bridge | Attributed (inside 45 s tolerance) |
| 9b | Stage 1 | Clock +90 s on visits 60 s long, 120 s apart, tolerance 45 s | Per sample: original `sourceTs` 0–14 s into a visit stays attributed to it, 15–44 s → `no-occupancy`, 45–59 s → the next visit |
| 10 | Stage 1 | Error declaration `did_not_occur` | `retracted` (`error-declaration`) |
| 10b | Stage 1 | Error declaration arriving before its arrival | Same settled body as 10 |
| 10c | Stage 1 | Error declaration on a departure | Visit open again: `provisional`, then `insufficient` (`missing-departure`) past the horizon; rows built on it are revised |
| 10d | Stage 1 | Arrival re-sent with different content | Both variants `conflict`; visit `retracted` (`input-conflict`); stage 2 `insufficient-evidence` (`basis-retracted-input-conflict`) |
| 10e | Stage 1 | Error declaration beyond the horizon | `replay-required` (`correction-beyond-horizon`); online state unchanged |
| 10f | Stage 1 | Occupancy within the horizon after a visit is `complete` | Next revision; disposition `accepted` (`late-after-settlement`) |
| 11 | Stage 1 | `plc-stamped` with `crossCheck occupancy` and a different occupant | `identity-mismatch`, attribution unchanged |
| 11b | Stage 1 | `plc-stamped`, `crossCheck none`, telemetry held 10 min, occupancy not consumed | Same bodies as the on-time run of the same `plc-stamped` rule, which contains at least one valid `complete` visit |
| 11c | Stage 1 | A mixed scan snapshot | `incoherent-snapshot` |
| 11d | Stage 1 | `plc-stamped` ID change without `VisitPresent=false` | Earlier visit `insufficient` (`missing-visit-close`) |
| 11e | Stage 1 | `plc-stamped` visit with neither `VisitPresent=false` nor an ID change | `insufficient` (`missing-departure`) once `W1 ≥ enter + horizonMs` |
| 12 | Stage 2 | Incident after complete records | Unit rows `affected-candidate` (`equipment-activity`), set settled |
| 13 | Stage 2 | Incident before the records | Set starts empty and not settled, grows as records arrive, settles |
| 14 | Stage 2 | Open `gauge-drift` incident on T1 at Station1 narrowed and closed | Visits entering after `suspectTo` become `not-affected-within-scope` (`interval-narrowed`) whatever tools they used; inside the interval T1 visits are `affected-candidate`, T2-only and no-tool visits `no-equipment-activity`; set `open=false` |
| 15 | Stage 2 | Withdrawn | All units per `onWithdrawn` (`incident-withdrawn`), set `resolution=withdrawn` |
| 16 | Stage 2 | Incident on nutrunner T1 at Station1 with T1 and T2 | Only visits with T1 activity are candidates, T2-only visits `no-equipment-activity` |
| 17 | Stage 2 | Unattributed activity inside the interval | Set coverage not complete |
| 18 | Cascade | Injection 10 applied to the visit behind a candidate | The candidate becomes `not-affected-within-scope` (`basis-retracted-error-declaration`) |
| 19 | Cascade | After 2b, late attribution | Late attribution adds the unit to the set |
| 20 | Cascade | A new attribution rule version in a new generation | Every row of that generation carries the new `rulesDigest`, no generation mixes rules |
| 21 | Restart | `kill -9` of the stage-1 and stage-2 jobs together during 13 (a stage-1 restart) | No duplicates, losses or revision gaps |
| 22 | Restart | Kill (i) during an uncommitted stage-1 sink transaction (stage-1 job restarts), (ii) between stage-1 output and stage-2 processing (stage-2 job restarts), (iii) a Kafka Streams stage-1 rebalance (a stage-1 restart; only the reinitialized tasks recover; Kafka Streams only), (iv) a chained restart: a second stage-1 restart injection before the merged cut of the first completes (Flink: (i) followed by another kill; Kafka Streams: (i) followed by a rebalance (iii) that moves at least one task and leaves at least one stage-1 writer recovering from the first restart in place) | Convergence |
| 23 | Horizon | Planned late samples at `A − horizonMs + 1`, `A − horizonMs`, `A − horizonMs − 1` after a cut whose reported `W1` equals the planned A | First two admitted online; the third `replay-required` (`beyond-horizon`), then `attributed` in the next generation replayed in event-time order (cause `replayed`); earlier generation's rows remain |
| 24 | Isolation | Telemetry of one declared station withheld, on a dedicated and on a shared raw placement | Its visits and incidents not yet settled before the starvation stay not settled; every other station settles by the 60 s deadline, counted from the append of the first batch in which, for every non-starved station s, the largest `sourceTs` of telemetry routed to s is at least the checked deadline + `outOfOrdernessMs` + `expectedPeriodMs` (judged on routed telemetry, not on engine `W1`), else a miss is recorded; after resume, recovery and the same bodies (item 5 (c), (d)) |
| 25 | Session | Muninn restarted with the same edge id: (a) host-offline path: two connections with different `bdSeq`, the earlier connection's death delayed past the newer birth; (b) a recording where the NBIRTH (QoS 0) of a connection is lost, followed by that `bdSeq`'s NDEATH | (a) `stale-death`, the current session stays open; (b) `session-ambiguous`, coverage `unknown` |
| 26 | Stage 1 | Occupancy silent 5 min during an open visit | Bodies equal to a control run with the same telemetry (item 5 (a)) |

The injection manifest records, for each restart injection, the jobs restarted, `epochBefore`, `targetEpoch`, `lastWrittenCutId`, the recovery declaration, the records the expected reinitialized set is computed from (Flink: the restart attempt; Kafka Streams: the active-task assignment before and after) together with that set, the attributed set, and the stage-1 writers whose `initEvidence` points to its declaration. Restart injections may be chained: a later one may come before the merged cut of an earlier one completes. Injection 22(iv) runs the chained case. In 22(iv) the second injection comes after every forseti job is RUNNING following the first injection and before batch k+1 is written, so no cut carries the first injection's `targetEpoch`; a writer that the second injection leaves untouched therefore meets only barriers of the second `targetEpoch` and takes the adoption path. In the Kafka Streams run, if the recorded assignments show no stage-1 writer recovering from the first injection that stayed on its instance, the injection is recorded as `error`. A Kafka Streams rebalance injection that moves no task hosting a stage-1 progress writer is recorded as `error`. Injection 22(iii) does not apply to Flink: the Flink run records it as not applicable, which is not a skip.

### Negative controls

Negative controls execute via `--negative-control <name>`. The gate must exit non-zero for each control.

| Control | Flag name | Target failure |
| :--- | :--- | :--- |
| α | `processing-time` | Fails 1 |
| β | `drop-late` | 23 leaves an input without disposition |
| γ | `no-cascade` | Fails 18 |
| δ | `lenient-oracle` | Oracle counts `provisional` as `complete`; fails 2 and 6 at the mandatory cuts |
| ε | `first-wins` | Fails 4 |
| ζ | `station-granularity` | Ignores tool evidence; fails 16 |
| η | `ignore-session-gap` | Fails 8 |
| θ | `non-transactional-output` | An injected failure between writing output and committing state yields duplicates, losses or revision gaps; fails 21 and 22 |
| ι | `wrong-rules-pin` | Cross-check of body `rulesDigest`, registered manifest and envelope pins fails; an output's own digest is never trusted |

### Proving the gates run

There are two commands: `forseti gate` and `forseti requirements`. `forseti gate` is the semantic gate: it runs every injection in the table except 24, listed by explicit id (1, 2, 2b, 3, 4, 5, 6, 7, 8, 9, 9b, 10, 10b, 10c, 10d, 10e, 10f, 11, 11b, 11c, 11d, 11e, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26), plus every negative control and comparison items 1, 2 and 6. The gate carries a manifest of assertion ids, outputs "assertions run N / expected N", and exits 1 on any assertion not run, zero scenarios, a skip (a scenario or assertion that did not run; an allowed skipped cut is not a skip, and the skip range, coverage and count are themselves assertions that must run), a missing report, or an input mismatch under Vor's gate rule (exit 0 only when every assertion ran and held). CI runs the normal gate (asserting exit 0) and every negative control (asserting exit 1; any negative control exiting 0 fails CI). Every input receives a disposition, reconciled by identity. Oracle diffs are preserved as CI artifacts. `forseti requirements` runs injection 24 on both raw placements and comparison item 5 (c) and (d) for H-isolation (where item 5 (d) runs resume from injection 24's resume, restart from the results of injections 21 and 22, and speed change on the baseline recording at batch interval ×2 and ×½), records 5 (a) and 5 (b) from the gate's results for injections 26 and 7, records pass, miss or error with evidence for each requirement, and exits 0 when every requirement was run and recorded. Engine eligibility is a separate result: semantic gate passed and every required requirement passed.

The gate asserts:
* `read_committed`
* `pins.job.timestampSource` = `payload`
* Partition assignment
* Topic retention settings
* Kafka Streams `max.task.idle.ms = -1`
* Kafka Streams `group.protocol=classic`, `upgrade.from` unset, `acceptable.recovery.lag` = Long.MAX_VALUE, `num.standby.replicas` = 0 and `num.stream.threads` = 1
* The active-task assignment of the Kafka Streams stage-1 application changes only at generation start and at the restart of a killed stage-1 application, each until the assignment is stable, and at declared rebalance injections (every instance's state transitions are recorded)
* The recorded Flink key-group and subtask assignment and the `routingDigest`
* The Flink watermark emission mode is per record
* Each Flink source subtask owns exactly one split
* The reported `W1` equals the engine formula at every cut the station completed
* Every progress's `mergedCutIds` are exactly the cutIds between that writer's previous progress and this one
* Every merged cutId is justified by a restart recovery of a reinitialized writer, the receive-order trigger inside a restart window, a starvation under injection 24 (the cut's `idleInputs` declare the station `starved`) or a miss recorded under injection 24, or by following stage 1; otherwise the gate fails
* A reinitialized writer writes no progress for a cut above its last committed progress and below the first cut whose `restartEpoch` is at or above the E the writer last fixed or adopted; committed is judged from the restored sink transactions and restored state, not from a snapshot at the injection
* Every recovered progress carries the `recoveryRef` and `restartEpoch` its writer selected at initialization (or adopted on re-reading `forseti.recovery-control`), and no barrier below that epoch closed it
* Every stage-1 progress writer's first progress after an initialization carries `initEvidence`, and for every restart injection the attributed set computed from the harness records equals the set of stage-1 writers whose `initEvidence` points to that injection's declaration
* In the Kafka Streams run of 22(iv), every stage-1 writer that recovered from the first injection and was not moved by the second has a recovered progress carrying the second injection's `recoveryRef`, with `initEvidence` pointing to the first injection's declaration
* `barrierRefs` list only barriers actually received; both evidence topics carry the same `cutId` and `mergedCutIds`; stage-2 `incidentOffsets` count 6 × (1 + number of `mergedCutIds`); every input of a merged cut has a disposition
* Comparison item 3 counts only restart skips, as (logical writer, cut) pairs

The replay gate (Kafka and engines) runs on every PR. The full-loop gate (HiveMQ CE, Bifrost sim, one Muninn per device with `--session --hierarchy --primary-host host-<edge>`, bridge; the harness publishes a retained online `spBv1.0/STATE/host-<edge>` for each of the five edges at start; injection 25(a) toggles one of them offline then online; the delay of the earlier connection's death past the newer birth is produced by permuting delivery order in the recording) runs manually or nightly. It asserts `rule.coverage.expectedPeriodMs == PERIOD_MS ≥ sim scan period` using compose settings (Bifrost cannot check this because the period is absent from the birth), records the rate of repeated `sourceTs`, and produces recordings committed with `.gitattributes` keeping bytes fixed under the resequence-twin-lab precedent.

Each test run records:
* Seed
* Fixture digest
* Pins
* Engine and job digests
* Offset ranges

A delivery step is complete when the controller runs the commands and retains the execution logs.

## Delivery plan

Each step uses a dedicated feature branch and PR. Core comes before engines: when the meaning is wrong both engines must be fixed.

| Step | Target | Scope | Completion gate |
| :--- | :--- | :--- | :--- |
| 0 | forseti #1 | create the public repository; this spec | spec merged |
| 1 | Bifrost | 2.27.0 schemas, validators and refusal codes, the `String` value type for typed (OPC UA) source representations in the 2.27.0 common schema, plus a 2.27.0 source-definition schema that uses it (Bifrost validates each source definition against the schemas of its own `schemaVersion`), so that the unit and visit members of a `plc-stamped` rule can be published, `OccupancyBindingProfile`, `IncidentBindingProfile`, MAPPING-PUBLICATION section; `MappingBundle` accepts 2.27.0 in its proposal-version, document-kind, `mapping.required` and envelope sets, allows 2.26.0 and 2.27.0 for the command-binding envelope, and admits 2.27.0 documents only in a 2.27.0 proposal; tests that a 2.27.0 proposal accepts existing documents mixed with the new rules and that a 2.26.0 proposal cannot carry the new kinds (its proposal schema refuses them); a new refusal code `rule.envelope` refuses any document whose `schemaVersion` is 2.27.0, including a 2.27.0 source definition, unless the proposal is 2.27.0 | tests and CI pass; a fixture bundle passes `gates mapping approve` |
| 2 | Tyr | authoring for both kinds and both profiles, fixtures, Bifrost acceptance test in CI | Bifrost approves a bundle Tyr produced |
| 3 | forseti | `forseti-core`, `ProgressTracker`, generation CLI, oracle, JCS vectors, hand-made recordings, synthesized continuous recordings for horizon scenarios | without an engine, core equals the oracle on every engine-independent injection |
| 4 | Bifrost sim | new folder `Line2` with three `StationType` instances {`Running` Boolean, `UnitId` String, `VisitId` String, `VisitPresent` Boolean} and two `ToolType` nutrunners {`Torque` Double, `Running` Boolean, `UnitId` String, `VisitId` String, `VisitPresent` Boolean}, both under Station1, every member re-stamped every scan and the tool members updated as one snapshot, all new `Line2` members of one scan share one source timestamp, `Sim/<member>.Value` value control nodes outside the governance filter beside the existing `Sim/<member>.StatusCode`, writes to `Line2` `Sim/<member>.Value` and `Sim/<member>.StatusCode` take effect together at the next scan, scan period as a system property; existing Mixer1 behaviour and bytes unchanged; hierarchy and mapping sets live in forseti fixtures authored in Tyr and approved by Bifrost | sim tests pass; Mimir and Muninn gates unchanged |
| 5 | forseti | bridge, producers, scenario driver, docker-compose (HiveMQ CE, Kafka KRaft, sim, one Muninn per device: five, unique edge ids), recorder, preflight (preflight refuses a duplicate MQTT client id), two fixtures whose device paths give a dedicated and a shared raw placement under the standard partitioner and whose subscribed raw partitions are distinct modulo their count, so that each Flink source subtask owns one split (owner `(((topic.hashCode() * 31) & 0x7FFFFFFF) % n + partition) % n`), verified by preflight | full loop produces and commits recordings |
| 6 | forseti | Flink adapter, replay gate, negative controls | semantic gate exit 0, each control exit 1, requirements run recorded, CI; a missed requirement makes the engine ineligible |
| 7 | forseti | Kafka Streams adapter on the same gate | same, on the same gate |
| 8 | forseti | comparison run, `docs/COMPARISON.md`, decision on Flink | measured tables and a one-paragraph conclusion |
| 9 | Bifrost docs | forseti consumer paragraph in ENTERPRISE and INTEGRATION | merged |

## Open questions

* Exact pinned versions of Flink, its Kafka connector, Kafka Streams, and the JCS libraries (`org.erdtman:java-json-canonicalization`, Python `rfc8785`) are confirmed before delivery step 3 and written here.
* The JDK 17 Ed25519 path and a self-written DSSE PAE for the release reader are confirmed before delivery step 3.
* The adapter mechanism that keeps occupancy and session inputs from advancing or holding stage-1 event time while their partitions carry cut barriers (including a non-negative control timestamp for the Kafka Streams extractor) is fixed in the plan, per engine.
* Checkpoint hooks, the deadline index structure, routing module classes, reject collector placement and sim locking are fixed in the plan; they do not change contracts or gate meaning.
* Before delivery step 6, confirm for the pinned versions: Flink Kafka source split ownership and split-less readers; the `−1` of `BoundedOutOfOrdernessWatermarks` and the delegating generator's per-record emission path; that Flink does not restore watermarks after a restore; Kafka Streams partition-time restore from committed offset metadata; that Flink runs `initializeState` before the task reports RUNNING; that with `acceptable.recovery.lag` = Long.MAX_VALUE a Kafka Streams rebalance assigns a moved stateful task as active without a warm-up replica or a probing rebalance, and that the pinned Kafka Streams version has no eager rebalance protocol; whether a Kafka Streams instance can report RUNNING between the rounds of a cooperative rebalance, and whether a rebalance can move a task between stream threads of one instance (the stable-assignment rule and `num.stream.threads` = 1 cover both); and that every change to a Kafka Streams instance's active tasks takes it through REBALANCING, recorded as a state transition (the assignment-change assertion and the stable-assignment rule rely on it).
* Moving tools need an assignment event; deferred.
* Persisting `bdSeq` across Muninn restarts is a Muninn follow-up candidate; forseti does not depend on it.
* A compacted current-state view per output topic is deferred; consumers derive it from the journal.
* SLSA provenance for the job artifact, following the recipe-publication pattern, is deferred.
