# Forseti Step 5: Live Input Loop Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** forseti gains the live input loop of delivery step 5: a Sparkplug B to Kafka bridge, occupancy and incident producers, a scenario driver for the Bifrost sim, a docker-compose stack (HiveMQ CE, Kafka KRaft, the sim, one Muninn per device: five, unique edge ids), a recorder, a preflight, and two placement fixtures; the full loop produces recordings in the step-3 recording format and they are committed.
**Architecture:** a new Maven module `bridge` (MQTT to Kafka); Kafka-free logic (recording writer, live ground truth, placement search, preflight rules) in `cli/src/main/java/yggdrasil/forseti/cli/live/` so the gate jar runs it; a thin new module `loop` with the Kafka, MQTT and OPC UA shells (topic admin, producers, driver, Kafka reader), which depends on `cli` and never the reverse. `core` keeps no transport or engine dependency. Sibling repositories are used only through released artifacts and published bytes (zero shared code).
**Tech Stack:** Java 17, Maven; Kafka broker and `kafka-clients` 4.2.2 (KRaft, single node, image pinned by digest); HiveMQ CE pinned to a dated tag by digest (not `latest`, fixed in PR 7); Eclipse Tahu `tahu-core` 1.0.14 and Paho 1.2.5 (the versions Muninn uses) for Sparkplug decoding; Eclipse Milo 1.1.8 client for the driver; Python 3.13 oracle unchanged. Branches per PR off main.

---

## Spec and rules applied

The implementation adheres to `docs/superpowers/specs/2026-11-05-forseti-design.md` and project constraints:

| Reference | Rule / Requirement | Application in Step 5 |
| :--- | :--- | :--- |
| Row 5 (line 750) | Step 5 delivery row and completion gate | Full loop produces and commits live recordings. |
| Decision 10 (line ~69) | Session events contract | Edge session tracking: birth, death, seq gaps, re-birth, ambiguous sessions. |
| Decision 12 (line 77) | Engine-independent core | Transport and engines remain outside `core` and `cli`. |
| Decision 13 (line 80) | Bridge specification | One record per Sparkplug message; no business logic; dead-letter pattern; refuses alias-only input. |
| Lines 231-320 | Input events specification | Structural validation of OT messages, state transitions, occupancy, and incidents. |
| Lines 465-496, 588, 65 | Topics and partitioning; 6 partitions (line 588); input log 30 d (Decision 9, line 65) | Six partitions per input topic and 30-day input retention; the timestamp type (`LogAppendTime`) is fixed in PR 5. |
| Line ~641 | Scenario driver visit profile | Station1 visits 60 s long, 120 s apart; T1-only, T2-only, both-tool, and no-tool visits; unused tools report visit IDs with `VisitPresent=true` and `Running=false`. |
| Lines 643-686 | Fault injections table | Injections 1, 3, 8, 9, 25(a), and 25(b) executed across bridge, sim, and transport. |
| Line 728 | Full-loop gate | HiveMQ CE, the Bifrost sim and one Muninn per device with `--session --hierarchy --primary-host`; the loop that produces the live recordings. |
| Line 756 | Open questions | Pinned to one Kafka client line (`4.2.2`) against broker version `4.2.2`. |
| Bifrost step-4 note | OPC UA write semantics | Driver writes one equipment snapshot per Write request; `-Dbifrost.sim.scanMs` sets scan period; Milo 1.1.8 read time is `ServerTimestamp`, so `sourceTimestamp` alone identifies scans. |

Fixed rules:
- OT requirements over tooling: no relaxed criteria; horizons are not shortened.
- Zero shared code: sibling repositories are used only via released artifacts and published bytes.
- Open standards used where they exist (Sparkplug B, OPC UA, EPCIS).

## Decisions

- **D1 (Module layout):** A new Maven module `bridge` carries MQTT-to-Kafka translation. Kafka-free live logic (recording writer, live ground truth, placement search, preflight rules) resides in `cli/src/main/java/yggdrasil/forseti/cli/live/` so the gate jar can execute it. A thin new module `loop` contains Kafka, MQTT, and OPC UA shells (topic admin, producers, driver, Kafka reader); `loop` depends on `cli`, and `cli` never depends on `loop`. `core` maintains zero transport or engine dependencies.
  - *Reason:* Keeps transport client libraries off the `cli` classpath used by the core gate, while pure logic directly reuses `Recording`, `RoutingManifest`, `GroundTruth`, and `ReplayRecording`.
- **D2 (Dependency closure):** `scripts/check-dependency-closure.sh` is refactored to enforce checks per module. `core` and `cli` enforce the complete ban (including `org.apache.kafka`). `bridge` and `loop` are permitted to depend on `kafka-clients`, `tahu-core`, `paho`, and `milo`. Flink, Kafka Streams, and any `yggdrasil.*` sibling code remain strictly banned across all modules.
  - *Reason:* Step 5 requires Kafka producers and an MQTT bridge in repository modules, while Decision 12 requires keeping engines out of the core only. This is recorded here as an interpretation of the spec; the spec text does not change for it.
- **D3 (Injections 1 and 9 in bridge):** Injections 1 ("held 10 min in the bridge") and 9 ("source clock +30 s in the bridge") are implemented as two declared, default-off bridge stages located immediately before the Kafka writer. Both stages log execution in the run record and operate outside the session tracker.
  - *Reason:* Implements spec lines 643-686 while preserving Decision 13 ("no business logic in bridge") and maintaining raw wire evidence.
- **D4 (Owner-approved: Birthless DDATA):** DDATA received from a connection whose NBIRTH was never observed by the bridge is never written to `ot.messages`. It is routed directly to dead-letter with raw bytes and receipt metadata; no `birthPublishTs`, `bdSeq`, or definition digest is invented. The eventual NDEATH of that connection generates a `session-ambiguous` event where `birthPublishTs` and `atSeq` are null (`bdSeq` takes the NDEATH metric value; `atSeq` is the last observed sequence number or null). The payload remains unchanged (`detectedEventTime` is null under spec line 282; `rawPublishTs`, `bdSeq`, and `payloadDigest` = SHA-256 of raw NDEATH bytes). The event identity is `(edge, device, null, bdSeq, "session-ambiguous", atSeq, rawPublishTs, payloadDigest)`. Exactly one event is emitted per distinct device named in that edge's dead-lettered birthless DDATA. If no device was named, no session event is emitted (NDEATH is dead-lettered only). No unknown interval is opened without a prior valid observation. With no event time, the event sorts first within its cut under existing contract order and never advances a watermark. The spec session-event table, Java core parser, and Python oracle parser are updated accordingly; all other session kinds retain strict integer requirements.
  - *Reason:* Injection 25(b) requires `session-ambiguous` with coverage `unknown`, and the contract's required integer fields cannot represent a death whose birth was never observed; no value is invented.
- **D5 (Core-input key unchanged):** `bridge/` and `loop/` are excluded from `scripts/core-input-key.sh` because neither module is in the core gate's dependency closure. `cli/.../live/`, `scripts/`, workflows, and committed captures under `fixtures/` remain included.
  - *Reason:* Core input key validity depends strictly on core verification inputs and gates.
- **D6 (Recorder):** Fresh input topics are generated per run (starting offsets at 0). At completion, publishers stop, bridge and pending writes drain, and the recorder reads fixed terminal offsets using `read_committed`. Output matches the step-3 directory format: per-topic, per-partition JSONL with real offsets and append timestamps, `manifest.json` with real definition digests and `synthetic: false`, plus routing and expected files. Batches and deadline brackets are derived from captured telemetry using established step-3 rules; unprovable brackets are rejected. No markers or barriers are written into input topics during capture.
  - *Reason:* A recording is the input topics only; fencing live writes would change what is observed; fresh topics make every partition start at offset 0.
- **D7 (PR sequence):** Work is partitioned into eight sequential pull requests off `main`. The implementation plan resides directly in `forseti`.
  - *Reason:* Preserves incremental verification and maintains an auditable development trail matching step 3.
- **D8 (No Muninn change):** Muninn reads OPC UA nodes sequentially, which can cause a poll to straddle an internal simulator scan. Straddled, mixed-scan messages are preserved as emitted (the core reports `incoherent-snapshot`). The recorder measures and reports incoherent snapshot and repeated-`sourceTs` rates.
  - *Reason:* Dropping mixed messages would hide the condition injection 11c covers; a batched read in Muninn stays a possible follow-up, outside step 5.
- **D9 (Upstream definitions derivation and pinning):** Definitions for Muninn originate upstream. Mimir derives `station-model` and `tool-model` candidate definitions from the simulator's `StationType` and `ToolType`. Bifrost admits and publishes them. Forseti imports the published definition bytes unchanged, locking them by commit hash and SHA-256 digest.
  - *Reason:* Maintains zero shared code between repositories while ensuring strict schema governance.
- **Period configuration:** Muninn `PERIOD_MS` = rule `expectedPeriodMs` = simulator `scanMs` = 1000. Preflight checks assert equality across all three values before execution.
- **Placement configuration:** The smoke placement subscribes raw partitions {1, 2, 4, 5}. Modulo 4, these map to residues {1, 2, 0, 1}, violating row 5 ("distinct modulo their count"). Two replacement fixtures (`dedicated`: each device on its own raw partition; `shared`: one partition shared by two stations) are generated via deterministic search over hierarchy device paths using `Murmur2` (partition numbers are never manually overridden). The same station is starved in both fixtures. Each partition residue is distinct modulo the subscription count, and split-ownership `(((topic.hashCode() * 31) & 0x7FFFFFFF) % n + partition) % n` is validated per raw topic.
- **Live recording verification:** Live recordings are lock-verified against recorded manifests and digests, rather than byte-reproducible across runs, due to real-world wall-clock timestamps (`sourceTimestamp`, birth, publish, append). Committed fixtures remain byte-stable in git via `.gitattributes` (`fixtures/** -text`).
- **Horizon scenarios:** Multi-day horizon scenarios remain synthetic; engine restarts, rebalances, isolation verdicts, and formal requirements runs belong to steps 6 and 7.

## Upstream prerequisites (other repositories)

The following upstream PRs must be merged and released prior to PR 3:

1. **Mimir PR:** Derives `station-model` and `tool-model` candidate definitions from the Line2 types of the released Bifrost simulator.
2. **Bifrost PR:** Admits and publishes the derived definitions; publishes placement mapping releases.
3. **Tyr PR:** Authors hierarchy, rule, and mapping bundles for the two placement fixtures across Line2 (three stations).

Each upstream repository runs its own CI. Forseti PR 3 imports the resulting published bytes and locks them by commit hash and SHA-256 digest. Sibling repositories share zero code.

## Pull requests and tasks

```
PR 1 (Plan & spec) ──► PR 2 (Modules & D4 reader) ──► PR 3 (Governed fixtures) ──► PR 4 (Bridge)
                                                                                         │
PR 8 (Committed captures) ◄── PR 7 (Compose & workflows) ◄── PR 6 (Recorder) ◄── PR 5 (Driver & preflight)
```

### PR 1: Plan and spec amendment
- **Tasks:**
  - Commit this implementation plan to `docs/superpowers/plans/2026-11-14-forseti-step5-live-loop.md`.
  - Amend `docs/superpowers/specs/2026-11-05-forseti-design.md`:
    - Add the owner-approved D4 birthless `session-ambiguous` variant to the Session events section (identity, nullable fields, fan-out, ordering).
    - D2 stays an interpretation recorded in this plan; the spec text is unchanged for it.
- **Verification:**
  - The repository's ASCII-name check and CI pass; the amended spec lines are checked against Decision 10 and row 5 by review; line numbers in this plan refer to the spec after this PR.

### PR 2: Module foundation and the birthless session-ambiguous reader
- **Tasks:**
  - Update `scripts/check-dependency-closure.sh` to execute per module. Keep strict bans on `core` and `cli`. Permit client libraries (`kafka-clients` 4.2.2, `tahu-core` 1.0.14, `paho` 1.2.5, `milo` 1.1.8) in `bridge` and `loop`. Ban Flink, Kafka Streams, and `yggdrasil.*` everywhere.
  - Create empty Maven modules `bridge` and `loop` in root `pom.xml`. Update CI test splits.
  - Implement D4 reader support in Java core:
    - Update session-event parsers and identity functions to accept null `birthPublishTs` and null `atSeq` for `session-ambiguous` events.
    - Enforce contract order: null event time sorts first within its cut and advances no watermark.
  - Implement D4 reader support in Python oracle (`oracle/`):
    - Update session event deserialization, tuple identity, and sort contracts.
  - Add unit tests in Java and Python for null-time event sorting and interval handling.
- **Verification:**
  - `mvn clean test`
  - `python -m pytest -q oracle`
  - `bash scripts/check-dependency-closure.sh`

### PR 3: Governed live fixtures
- **Tasks:**
  - Implement deterministic placement search script in `cli/src/main/java/yggdrasil/forseti/cli/live/` using `Murmur2` to generate:
    - `dedicated`: 5 devices mapped across 5 distinct partitions.
    - `shared`: two stations mapped to a single shared partition.
    - Identical starved station across both configurations.
    - Distinct residues modulo subscription count.
  - Import published bundle bytes from Mimir, Bifrost, and Tyr into `fixtures/live/{dedicated,shared}/`.
  - Generate and commit routing manifests, definition bytes, placement search outputs, and lock files recording upstream commits and SHA-256 digests.
  - Add unit tests validating partitioner calculations and split-owner formula `(((topic.hashCode() * 31) & 0x7FFFFFFF) % n + partition) % n`.
- **Verification:**
  - `mvn test -pl cli` verifies placement properties and lock digests against imported bytes.

### PR 4: Bridge
- **Tasks:**
  - Implement `bridge` Maven module for Sparkplug B to Kafka ingestion:
    - Per-edge ordered Sparkplug payload decoding via `tahu-core` 1.0.14 and Paho 1.2.5.
    - Decision 10 session tracker: `seq-gap`, `session-down`, `session-up` with `rebirth`, DBIRTH sequence 1 consumption, `stale-death`, `definition-changed`, and `detectedEventTime` emitted once.
    - D4 birthless DDATA handling: route unparented DDATA to dead-letter; emit single `session-ambiguous` record upon NDEATH per referenced device.
    - Extract `definitionSha256` from NBIRTH `governed_definition` bytes.
    - Refuse alias-only Sparkplug messages.
    - Idempotent acknowledged Kafka producer using `kafka-clients` 4.2.2 with bounded queues; message delivery failure aborts the run.
    - Fault injection stages (D3): configurable hold-and-release stage (Injection 1) and source timestamp offset stage (Injection 9) before Kafka write.
  - Add table-driven unit tests on recorded MQTT frames without external brokers, covering Decision 10 states and Injections 25(a) and 25(b).
- **Verification:**
  - `mvn test -pl bridge`
  - `bash scripts/check-dependency-closure.sh`

### PR 5: Topics, preflight, producers, scenario driver
- **Tasks:**
  - In module `loop` and `cli/src/main/java/yggdrasil/forseti/cli/live/`:
    - Implement `forseti-loop topics`: creates six partitions per input topic, configures `LogAppendTime`, and sets 30-day retention.
    - Implement `preflight` command: verifies unique MQTT client IDs (including Muninn's `muninn-edge-<edge>`), asserts partition counts, verifies `LogAppendTime`, checks `PERIOD_MS == expectedPeriodMs == scanMs == 1000`, validates placement split-owner rules, checks simulator controls, and asserts definition digests.
    - Implement occupancy and incident Kafka producers conforming to committed 1.0.0 schemas.
    - Implement scenario driver using Eclipse Milo 1.1.8 client:
      - Reads wall-clock schedule from scenario file matching spec visit profiles (60 s visit, 120 s gap; T1-only, T2-only, both-tool, no-tool visits).
      - Executes single OPC UA Write request per equipment snapshot to `Sim/<member>.Value` and `.StatusCode`.
      - Drives unused tool status with `VisitPresent=true` and `Running=false`.
      - Derives occupancy and incident event times from read-back OPC UA source timestamps.
  - Add unit tests using in-process test doubles for MQTT, Kafka, and OPC UA.
- **Verification:**
  - `mvn test -pl loop,cli`

### PR 6: Recorder and capture verification
- **Tasks:**
  - Implement recorder in `cli/src/main/java/yggdrasil/forseti/cli/live/` (D6):
    - Reads input topics at terminal offsets with `read_committed`.
    - Outputs step-3 directory format: JSONL per partition with real offsets and append timestamps, `manifest.json` (`synthetic: false`), routing manifests, and expected files.
    - Computes batch boundaries and deadline brackets from captured telemetry; rejects unprovable brackets.
    - Reports incoherent snapshot rate and repeated-`sourceTs` rate (D8).
  - Implement `recordings verify-capture`: verifies captured files and batch boundaries against lock files without synthetic regeneration.
  - Implement live ground truth generator from driver plans and captured telemetry via `GroundTruth`.
  - Add tests for round-trip parsing through `Recording.open`, oracle recording reader, truncated partitions, corrupt records, and missing tails.
- **Verification:**
  - `mvn test -pl cli`
  - `python -m pytest -q oracle`

### PR 7: Compose stack, full-loop runner, manual and nightly workflow
- **Tasks:**
  - Author `docker-compose.yml`:
    - HiveMQ CE pinned to a dated tag by digest (not `latest`, fixed in this task).
    - Single-node Kafka KRaft pinned by digest (Kafka 4.2.2).
    - Bifrost simulator container configured with `-Dbifrost.sim.scanMs=1000`.
    - Five Muninn processes started with `--session --hierarchy --primary-host host-<edge>` and unique edge IDs.
    - Bridge service subscribing to MQTT prior to the publication of the five retained online STATE messages.
  - Commit lock files for container image digests and simulator/Muninn commit hashes.
  - Create orchestrator script `scripts/run-full-loop-gate.sh`.
  - Add documentation `docs/LIVE-LOOP.md` detailing setup, execution, and triage.
  - Create GitHub Actions workflow (`.github/workflows/live-loop.yml`) triggered by `workflow_dispatch` and nightly schedule; uploads test artifacts without committing to repository.
- **Verification:**
  - Local validation of container startup sequence, network namespace sharing, and clean teardown.

### PR 8: Committed live recordings
- **Tasks:**
  - Execute live capture runs locally via `scripts/run-full-loop-gate.sh` across required test matrix into `build/`:
    - Baseline run on `dedicated` placement.
    - Baseline run on `shared` placement.
    - Fault injection 3: Bad via StatusCode.
    - Fault injection 8: termination of one Muninn instance.
    - Fault injection 25(a): host offline followed by online recovery.
    - One `plc-stamped` attribution run.
    - Bridge fault injection 1: 10-minute hold-and-release.
    - Bridge fault injection 9: source clock +30 s offset.
  - Verify all captures using `forseti recordings verify-capture`, core harness, and Python oracle.
  - Commit verified capture directories under `fixtures/live-recordings/` with lock files, preflight summaries, provenance metadata, and gate execution logs. Mark byte-stable in `.gitattributes`.
- **Verification:**
  - Step-5 completion gate execution on committed fixtures.

## CI placement

- **Per-PR checks (standard CI):**
  - `mvn test` across all modules (runs unit tests for `bridge` and `loop` using in-memory doubles, without Docker).
  - `python -m pytest -q oracle`.
  - `bash scripts/check-dependency-closure.sh` enforcing per-module bans.
  - Digest check verifying committed fixtures under `fixtures/**` remain unmodified.
  - Core gate and verification evidence reuse unchanged.
- **Live loop execution:**
  - Full docker-compose live runs run on developer workstations and scheduled nightly CI / `workflow_dispatch`.
  - Never runs as a blocking PR check.
  - A single baseline live run requires approximately 15 minutes of wall-clock time (60 s visit execution plus 751 s horizon tail).

## Verification (step-5 completion gate)

The delivery step 5 completion gate passes when all of the following commands and checks succeed:

```bash
mvn -B -ntp install
python -m pytest -q oracle
bash scripts/check-dependency-closure.sh
bash scripts/run-full-loop-gate.sh --out build/full-loop
```

Gate criteria:
1. `run-full-loop-gate.sh` exits with code 0 on both `dedicated` and `shared` placements.
2. `forseti recordings verify-capture` matches every committed lock file under `fixtures/live-recordings/`.
3. `forseti run --engine core` and the Python oracle produce identical verdicts at every cut across all committed live recordings.
4. All five devices, both placement layouts, both attribution regimes (`occupancy-join` and `plc-stamped`), and all four Station1 tool usage patterns (T1-only, T2-only, both tools, no tools) are present in the committed captures.
5. The incoherent-snapshot and repeated-`sourceTs` rates are recorded in each capture's report.
6. Execution logs and run records are preserved in the run output directory.

## Risks

| Risk | Impact | Mitigation |
| :--- | :--- | :--- |
| Mixed snapshots from per-member reads | Muninn reads nodes individually, allowing polls to straddle simulator updates. | Mixed-scan messages are preserved as emitted; core handles them via `incoherent-snapshot`; recorder measures and reports rates. |
| Scan jitter and lack of phase lock | Identical 1000 ms scan and poll rates without synchronization produce skipped or duplicated scans. | Assertions evaluate observable timestamp and sequence evidence rather than assuming uniform sample counts. |
| OPC UA endpoint binding | Bifrost simulator requires exact endpoint address `localhost:48400`. | Muninn and driver JVMs share the simulator's container network namespace or utilize host networking. |
| Unreleased upstream dependencies | Simulator and Muninn lack published Maven releases. | Binaries are compiled from locked upstream git commits and pinned by SHA-256 digest. |
| Local port collision | Port 9090 is occupied on the controller's machine. | Heimdall is not part of the step-5 loop. |
| Cross-platform host variance | Controller host variations (Windows / Linux) affect shell scripts and socket bindings. | Docker Compose standardizes container networking and runtime isolation across host platforms. |
| Five edges | Live recordings carry five edge ids; the step-3 recordings carried one (`edge-line2`), so the core and the oracle have only seen one edge. | PR 6 round-trip tests and the PR 8 core-and-oracle comparison run on five-edge captures. |
