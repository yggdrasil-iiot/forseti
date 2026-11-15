# Forseti Step 6: Flink Adapter, Replay Gate and Negative Controls Implementation Plan

- **Goal:** Implement the Flink engine adapter, engine-neutral Kafka replay harness and gate, negative controls, and requirements reporting for Forseti step 6.
- **Architecture:** Decoupled engine-neutral replay harness (`cli/replay/`, `replay-kafka`), pure core cut assembly (`CutAssembler`), pluggable Flink engine (`engine-flink`), and Docker-managed Kafka verification gate.
- **Tech Stack:** Java, Apache Flink 2.2.1, org.apache.flink:flink-connector-kafka:5.0.0-2.2, Apache Kafka 4.2.2 (Docker), RocksDB, Python (pytest).
- **Spec:** docs/superpowers/specs/2026-11-05-forseti-design.md
- **Base:** main 0b57a07

## Spec and rules applied

- Decision 12 defines an engine-independent core; adapters supply partitioning, timers, state, side outputs and sinks (lines 76–78).
- Cut barriers, progress tracking, and recovery control protocols govern cut assembly, epoch adoption, and stage progress (lines 408–464).
- Topic definitions, partitioning, and record schemas establish input and output streams (lines 465–496).
- Engine comparison pins fix runtime properties, state backends, checkpointing intervals, and execution semantics (lines 588–590).
- Engine comparison evaluation items define required behaviors across engines (lines 592–604).
- Cuts and the independent Python oracle govern output comparison, including the two W1 forms (lines 608–641, 620, 622).
- Every deadline d is bracketed by two consecutive completed cuts with `W1_A < d ≤ W1_B`; injection 23 plans an A at which every subscribed raw partition has the same largest `sourceTs` (line 626).
- Every recording carries a telemetry tail of at least `allowedLatenessMs + outOfOrdernessMs + expectedPeriodMs` on every subscribed device and partition after the last business event (line 628).
- Fault injections test crash-recovery and fencing: injection 21 (`kill -9` of both jobs), injection 22 (targeted restarts), and injection 24 (telemetry of one declared station withheld) (lines 678–679, 681).
- Fault injection 22(iii) is recorded as not applicable to Flink (line 685).
- Negative controls validate gate sensitivity, including control θ for non-transactional output (lines 689–701, 700).
- `forseti gate` runs the 36 injection ids listed in line 705 (every injection except 24), every negative control and items 1, 2 and 6; `forseti requirements` runs injection 24 and item 5 (line 705).
- Gate assertions verify state convergence, deterministic cuts, progress tracking, and offset alignment (lines 707–726).
- Replay gate execution is mandatory on every pull request (line 728).
- A delivery step is complete when the controller runs the commands and retains the execution logs (line 737).
- Delivery plan row 6: Flink adapter, replay gate, negative controls; semantic gate exit 0, each control exit 1, requirements run recorded, CI (line 751).
- Delivery plan row 7: the Kafka Streams adapter on the same gate (line 752).
- Pinned dependency versions specify Apache Flink 2.2.1, org.apache.flink:flink-connector-kafka:5.0.0-2.2, and Apache Kafka 4.2.2 (line 758).
- Concrete engine mechanisms not dictated by the spec are fixed by this implementation plan (lines 760–761).
- Assumptions regarding engine runtime behaviors require confirmation before adapter implementation (line 762).

## Decisions

**Scope:** This step delivers the Flink stage-1 and stage-2 jobs, the engine-neutral Kafka replay harness and gate, and runs the 36 injection ids of line 705 on Flink (injection 24 runs only under `forseti requirements`). Injection 22(iii) is recorded as not applicable to Flink (line 685), never as a skip. Nine negative controls are implemented, including the new control θ (line 700). Gate assertions 707–726 are enforced, excluding the Kafka Streams-specific assertions on lines 712–714 and 724, and evaluating items 1, 2, and 6. Topics `forseti.generations` and `forseti.recovery-control` become single-partition Kafka topics with unchanged row shapes and single-writer semantics enforced by fixed transactional.id fencing (line 748). Step 7 will evaluate Kafka Streams against this same gate.

**Confirmation rule:** In accordance with line 762, PR 1 adds an engine-assumptions document (mapping each assumption, pinned artifact, class or method, and test ID) along with executable Flink MiniCluster spike tests that are retained as permanent regression tests. Kafka Streams assumptions are verified by named-class source reading in step 6 PR 1 with a complete argument per case, followed by executable spikes in step 7 PR 1. An unresolved claim is not confirmed. Any false assumption stops the dependent PR and requires a spec-correction PR first; no gate criterion changes.

- **D1 (Modules and Engine Pluggability):** `cli/replay/` defines transport-free, engine-neutral interfaces. `replay-kafka` (depending on `cli` and `kafka-clients:4.2.2`) implements stores, topic settings, batch and barrier writing, recording-to-broker offset mapping, completion reading, journal dumping, collectors, and process supervision. `engine-flink` (depending on `core`, Flink 2.2.1, and connector 5.0.0-2.2) contains the stage-1 and stage-2 jobs. The single launcher is `cli.Main`. Running with `--engine core` executes in-process as before, while external engines are discovered via `ServiceLoader` over an engine path; a missing provider produces a usage error with exit code 2. The literal command `forseti gate --engine flink` holds (line 705). The core gate classpath remains Kafka-free, and dependency closure scripts are enforced per module.
  - *Reason:* Decoupling engine-neutral replay interfaces from Kafka and Flink prevents classpath contamination in the core gate, satisfies Decision 12 (lines 76–78), and enables adding Kafka Streams in step 7 without changing the replay infrastructure.

- **D2 (Cut Protocol in Core):** A pure, serializable `CutAssembler` in core manages required barriers, held inputs, receive-order merging, recovery epoch selection and adoption, mergedCutIds, and admissionW with a versioned core codec. It is shared by the reference harness, the Flink adapter, and the future Kafka Streams adapter. Core provides `ProgressTracker` for stage 2. Adapters supply native time, durable state, and initialization evidence. Core performs no Kafka or checkpoint operations (lines 77–78, 412–418). All core modifications are confined to a single pull request (PR 2), keeping the core gate at 82/82.
  - *Reason:* Placing cut assembly logic in a pure core class ensures identical cut semantics across all engines, avoids duplicating complex barrier and epoch convergence rules in engine adapters, and preserves the architectural boundary between business logic and streaming runtimes.

- **D3 (Stage-1 Barrier Time and Operator Mechanism):** Stage 1 is implemented as a custom two-input keyed operator receiving telemetry on input 1 and sessions/occupancy on input 2 (line 760). Input 2 watermark and stream status are ignored, so the operator watermark is strictly the minimum across telemetry channels without auxiliary MAX_WATERMARK markers. The idle-status approach is rejected because idle status cannot be emitted at initialization when the auto-watermark interval is 0, causing post-restart watermarks to lag behind the first barrier and violating W1 == formula (lines 412, 718). The telemetry watermark generator delegates on each event and emits immediately with auto-watermark interval set to 0 and stream idleness disabled (line 590). PR 1 reconciles the spec line 77 reference to "KeyedProcessFunction" as permitted by lines 760–761 and records it under item 7 (line 600).
  - *Reason:* Standard Flink operators combine watermarks across all inputs using minimum logic, which causes second-input idle status or low watermarks to stall telemetry cut barriers; ignoring input 2's watermark keeps the operator watermark equal to the telemetry formula at every completed barrier, including after a restart.

- **D4 (Timer Management and Cut Drainage):** The core `DeadlineIndex` remains the authoritative source of pending deadlines. For each distinct deadline, exactly one Flink event-time timer is registered to mark the deadline as held. Held deadlines are drained strictly in `DeadlineIndex` order upon cut closure. Stage 2 uses a persisted `ProgressTracker` without Flink watermarks or native timers.
  - *Reason:* Using core `DeadlineIndex` order rather than Flink's internal timer firing order ensures deterministic, engine-independent cut evaluation, while stage 2's progress tracking relies purely on deterministic offset advancement rather than event-time watermarks.

- **D5 (State Backend, Checkpointing, and Sinks):** State and persistence use RocksDB with incremental checkpointing, EXACTLY_ONCE mode, an interval of 1000 ms, and exactly one concurrent checkpoint (line 588). Per-station state is stored as versioned bytes. Each logical writer chain uses a single transactional `KafkaSink` so that station output rows and progress records commit in the same transaction to explicit destination partitions. Operators use stable UIDs and unique transactional prefixes, with `transaction.timeout.ms` configured above the longest observed restart time. During `initializeState`, stage 1 reads the `forseti.recovery-control` topic up to end offset H under `read_committed` isolation and fixes epoch E. Restore evidence captures `isRestored`, the restored checkpoint ID, and the execution attempt number. Commits are never triggered from `snapshotState`.
  - *Reason:* Atomic progress tracking requires transactional alignment between data outputs and progress records in Kafka, while out-of-band initialization reading of recovery control guarantees epoch fencing before stream processing begins.

- **D6 (Partition Routing and Source Parallelism):** Sources use `KafkaSource` configured with static partition assignments derived from the routing manifest, partition discovery disabled, and source parallelism equal to the number of subscribed partitions. The router operator executes `InputParser` and core `Routing`, fans cut barriers out per line 412, and routes records via `keyBy(station)` with max parallelism explicitly pinned. The key-group to subtask mapping is recorded and verified (line 715). Reject collectors run at parallelism 1 and write ledger partition 6.
  - *Reason:* Static partition assignment and matching source parallelism prevent non-deterministic partition redistribution across restarts, while pinned key groups guarantee predictable station-to-operator assignment across recovery cycles.

- **D7 (Process Supervision and Fault Injection Lifecycle):** Each Flink job executes in a dedicated application-mode cluster consisting of one JobManager JVM and one TaskManager JVM, without using Flink Docker images. Fault injection 21 kills the stage-1 and stage-2 jobs together. Injection 22(i) terminates the stage-1 TaskManager. Injection 22(ii) terminates stage 2 after stage-1 output is committed and before stage-2 processing begins (implemented as a process kill, not a pause). Injection 22(iv) executes 22(i), followed by a second kill after all jobs reach RUNNING state and before batch k+1. Restarting stage 2 does not increment or alter the stage-1 recovery epoch.
  - *Reason:* Application-mode JVMs provide lightweight, rapid, and precise process supervision and signal control directly from the replay harness, enabling exact timing of process termination without Docker container overhead.

- **D8 (Fault Injection 22(i) Execution and Verification Mechanism):** The 22(i) fault injection executes in three phases:
  (1) Send SIGSTOP to the stage-1 TaskManager; query the Kafka Admin API with `describeTransactions` for the identified sink transaction (verifying transactional ID, producer ID, and producer epoch), asserting that transaction state is `Ongoing` and that the `read_uncommitted` end offset exceeds the Last Stable Offset (LSO); then send SIGKILL.
  (2) Issue a second `describeTransactions` call after SIGKILL and before the replacement TaskManager starts, asserting that the same transaction remains `Ongoing`. If not, the injection attempt is invalid. Missing or ambiguous evidence invalidates the attempt. Invalid attempts are repeated on a fresh run; after N invalid attempts, the injection status becomes `error` and never `pass`. Every attempt is captured in the run record. The post-restart outcome (aborted, or committed by the restored committer for a completed checkpoint) is recorded, and the gate's convergence assertions evaluate the run.
  (3) As a fallback, used only if the PR 1 spike demonstrates that steps (1)–(2) are unreliable: a transaction-specific commit hold implemented as a separate sink wrapper, enabled via a job parameter, recorded in `pins.job`, and asserted absent in every run except 22(i) and the first leg of 22(iv).
  - *Reason:* Verifying in-flight uncommitted transaction state via Kafka Admin metadata before and after SIGKILL guarantees that the fault injection genuinely tests transactional producer recovery rather than an idle window between checkpoints.

- **D9 (Negative Control θ for Non-Transactional Output):** Negative control θ introduces a job flag that switches Kafka sinks from transactional exactly-once mode to non-transactional at-least-once mode. The replay harness terminates the stage-1 process immediately after record publication and before checkpoint completion. This control causes injections 21 and 22 to fail with their intended assertion IDs rather than an unhandled process crash (line 700).
  - *Reason:* Control θ proves that the gate assertions actively detect duplicate or uncommitted outputs and that passing injection 21 and 22 requires true end-to-end exactly-once transactional sinks.

- **D10 (Replay Gate Output Consumption and Oracle Verification):** The gate employs a single consumer with `isolation.level` set explicitly to `read_committed`. It dumps consumed Kafka outputs directly into the step-3 journal format, allowing `Comparison`, `JournalFold`, and the reference Python oracle to run completely unchanged. The oracle executes after all engine processes have terminated. Effective consumer configurations are captured in `pins.job` and asserted by the gate.
  - *Reason:* Formatting engine outputs into the existing step-3 journal format lets the existing oracle and comparison code run without modification, while `read_committed` ensures uncommitted transaction aborts are filtered out.

- **D11 (Engine Recordings, Placement Layouts, and Brackets):** Every step-3 semantic scenario is rebuilt by the step-3 builder on the shared `site-019` placement, where raw partition 0 carries two stations (line 491). Baseline and smoke scenarios are also built on the dedicated `site-071` placement. Fault injection 24 is generated on both placements (line 681). Speed change is evaluated on the baseline scenario. The step-3 placement `{1,2,4,5}` yields partition owners `1,2,0,1` under parallelism 4, which cannot satisfy line 717; placements are configured to ensure distinct subtask ownership. Brackets are generated using the W1 form as a builder parameter (line 622) and verified against reported W1 values. Scenario 23 enforces equal partition maxima (line 626). Tails are generated in accordance with line 628. All engine recordings and lock files land in PR 2, validated by running the reference harness in the Flink W1 form against the oracle. Recordings are stored outside core-input-key paths so the core gate does not replay them, but within the replay input key. Builder code remains in the core key.
  - *Reason:* The step-3 placement cannot give each Flink source subtask exactly one split (line 717), while both governed placements can, and parameterizing W1 form generation guarantees oracle alignment before engine implementation.

- **D12 (Test Infrastructure and Operating System Boundaries):** Infrastructure utilizes Docker-managed Apache Kafka 4.2.2 using the step-5 pinned digest, avoiding embedded brokers or Testcontainers. The replay gate is supported on Linux only; Windows environments run unit tests and the core gate (satisfying the rule of line 728 that the replay gate runs on every PR, without requiring support across every operating system). Memory budgets are measured directly during execution rather than estimated.
  - *Reason:* Docker-based Kafka pinned to the step-5 digest guarantees reproducible broker behavior without JVM classloader interference, while restricting process-signal testing to Linux avoids fragile cross-platform process management.

- **D13 (Continuous Integration, Sharding, and Cache Boundaries):** A fresh Flink replay gate execution runs on every pull request; core-evidence reuse is not permitted to replace Flink execution (line 728). Caching is restricted to dependencies, Docker images, and verified fixture bytes. A replay input key binds shard evidence. Shards are defined in a committed `gate/flink-shards.json` file, with one stack per shard job: each horizon recording runs in isolation, restart injections run alone, and negative controls run only on shards containing their intended target IDs. Shards are split iteratively until the slowest measured shard completes within the core-changing budget; duration in minutes is measured during PR 4 rather than stated in advance. Nightly builds execute an unsharded gate, three-run determinism verification, and the requirements command.
  - *Reason:* Mandatory per-PR replay gate execution prevents regressions in distributed streaming state, while isolated sharding keeps pull request turnaround within CI limits without masking cross-scenario state interference.

- **D14 (Requirements Command and Engine Eligibility):** The `forseti requirements` command executes on pushes to main, nightly builds, manual invocations, and during the controller completion run; it is not pull request blocking. Requirements accounting logic is unit-tested on every PR. The command outputs `pass`, `miss`, or `error` with accompanying evidence for each requirement, keeping engine eligibility distinct from step completion. A recorded miss makes Flink ineligible but allows step 6 to complete successfully with the miss formally recorded (Decision 18 predicts a requirement 5(c) miss on the dedicated placement).
  - *Reason:* Separating requirement satisfaction and production eligibility from pull request gates ensures that known engine design limitations (such as Flink's dedicated placement behavior) are transparently recorded without halting CI delivery.

## Confirmations before the adapter (spec line 762)

| Assumption | Pinned artifact | Class or method | Evidence |
| :--- | :--- | :--- | :--- |
| Split ownership and split-less readers | org.apache.flink:flink-connector-kafka:5.0.0-2.2 | KafkaSourceEnumerator (owner computation), KafkaSourceReader | The owner of every subscribed partition of both governed placements is distinct. |
| Split-less reader watermark behaviour | Flink 2.2.1 | SourceOperator, WatermarkOutputMultiplexer | Parallelism n+1 over n splits. |
| The -1 of BoundedOutOfOrdernessWatermarks and per-record emission | Flink 2.2.1 | BoundedOutOfOrdernessWatermarks, SourceOutputWithWatermarks | With interval 0 and the delegating generator the watermark equals maxTs - outOfOrdernessMs - 1 after each record. |
| Watermarks not restored | Flink 2.2.1 | SourceOperator, InternalTimeServiceManager | After a restore currentWatermark is Long.MIN_VALUE until a post-restore record. |
| initializeState before RUNNING | Flink 2.2.1 | StreamTask, Task | A blocked initializeState never shows RUNNING. |
| Partition-time restore from committed offset metadata | org.apache.kafka:kafka-streams:4.2.2 | StreamTask, PartitionGroup | Named-class source reading in step 6 PR 1; executable spikes in step 7 PR 1. |
| Active assignment without warm-up or probing with acceptable.recovery.lag = Long.MAX_VALUE, and no eager protocol | org.apache.kafka:kafka-streams:4.2.2 | StreamsPartitionAssignor, HighAvailabilityTaskAssignor | Named-class source reading in step 6 PR 1; executable spikes in step 7 PR 1. |
| RUNNING between cooperative rounds, moves between stream threads, every active-task change through REBALANCING | org.apache.kafka:kafka-streams:4.2.2 | KafkaStreams, StreamThread, TaskManager | Named-class source reading in step 6 PR 1; executable spikes in step 7 PR 1. |

Rule: An unresolved claim is not confirmed. A false assumption stops the dependent PR and goes to a spec-correction PR first; no gate criterion changes.

## Pull requests and tasks

### PR 1: Plan, Spec Clarifications, Dependencies, Spikes, and Confirmations
- **Tasks:**
  - Commit implementation plan for step 6.
  - Reconcile spec line 77 KeyedProcessFunction wording with custom two-input operator mechanism under line 760 and record under item 7 (line 600).
  - Add POM dependencies for Flink 2.2.1, connector 5.0.0-2.2, and Kafka 4.2.2.
  - Extend `scripts/check-dependency-closure.sh` to the new modules: Flink only in `engine-flink`, `kafka-clients` in `replay-kafka`, the full ban kept on `core` and `cli`.
  - Create the engine-assumptions document mapping assumptions, artifacts, classes, and test IDs.
  - Implement Flink MiniCluster executable spike tests for the 5 Flink assumptions (split ownership, split-less watermarks, -1 bounded out-of-orderness, watermarks not restored, initializeState before RUNNING) and retain them as regression tests.
  - Complete named-class source reading documentation and arguments for the 3 Kafka Streams assumptions.
- **Verification:**
  - `mvn -B -ntp install` passes across all modules.
  - `python -m pytest -q oracle` passes.
  - `bash scripts/check-dependency-closure.sh` passes.
  - Core gate passes at 82/82.
  - All 5 Flink MiniCluster spike tests pass; no engine claim is made.

### PR 2: Core CutAssembler, Reference Harness Integration, W1-Form Parameter, and Engine Recordings
- **Tasks:**
  - Implement pure, serializable `CutAssembler` in core handling required barriers, held inputs, receive-order merging, recovery epoch selection and adoption, mergedCutIds, and admissionW with versioned codec (lines 77–78, 412–418).
  - Adapt the reference test harness to use `CutAssembler`.
  - Add W1-form parameter to the step-3 scenario builder (line 622).
  - Rebuild all step-3 semantic scenarios on shared `site-019` (partition 0 carrying two stations, line 491); rebuild baseline and smoke on dedicated `site-071`; rebuild injection 24 on both (line 681); rebuild speed change on baseline.
  - Plan brackets with W1-form builder parameter (line 622) and verify against reported W1; configure equal partition maxima for scenario 23 (line 626); generate tails per line 628.
  - Commit all engine recordings and locks in replay input key paths outside core-input-key paths; builder code remains in core key.
- **Verification:**
  - `mvn -B -ntp install` passes.
  - `python -m pytest -q oracle` passes.
  - `bash scripts/check-dependency-closure.sh` passes.
  - Full core gate run passes at 82/82.
  - Reference harness executed in Flink W1 form equals oracle output on every engine recording.

### PR 3: Engine-Neutral Replay Interfaces, Launcher Pluggability, Replay-Kafka, and Test Driver
- **Tasks:**
  - Implement transport-free interfaces in `cli/replay/` for stores, topic management, writer, completion reader, journal dump, collectors, and process supervision.
  - Implement ServiceLoader provider lookup in `cli.Main` over an engine path, maintaining `--engine core` in-process and returning exit code 2 on missing engine provider.
  - Implement `replay-kafka` module with `kafka-clients:4.2.2` implementing topic settings, batch and barrier writing, recording-to-broker offset mapping, completion reading, journal dumping, collectors, and process supervision.
  - Implement single-partition Kafka topics `forseti.generations` and `forseti.recovery-control` with unchanged row shapes and single-writer transactional.id fencing (line 748).
  - Implement test-scope driver executing reference tasks over Kafka on the smoke scenario.
- **Verification:**
  - `mvn -B -ntp install` passes.
  - `python -m pytest -q oracle` passes.
  - `bash scripts/check-dependency-closure.sh` passes.
  - Core gate remains Kafka-free and passes at 82/82.
  - Kafka test-scope driver runs reference tasks over Kafka and matches oracle outputs at every cut of smoke.

### PR 4: Flink Engine Jobs, Stage Adapters, Smoke Shard Execution, and Metric Baselines
- **Tasks:**
  - Implement `engine-flink` module containing stage-1 and stage-2 jobs.
  - Implement custom two-input keyed operator for stage 1 (input 1 telemetry with delegating generator, interval 0, idleness off; input 2 sessions/occupancy ignored, lines 590, 760).
  - Wire authoritative core `DeadlineIndex` and Flink event-time timers marking held deadlines for drain at cut close.
  - Implement stage-2 `ProgressTracker` without watermarks or native timers.
  - Configure RocksDB incremental checkpointing, EXACTLY_ONCE, 1000 ms interval, one concurrent checkpoint (line 588), and versioned station state bytes.
  - Configure transactional `KafkaSink` per writer chain with explicit destination partitions, stable UIDs, and transactional prefixes.
  - Implement stage-1 `initializeState` out-of-band recovery control reading to end offset H under `read_committed` and fix epoch E.
  - Configure `KafkaSource` with static partitions, discovery off, and matching source parallelism; record key-group/subtask map (line 715).
  - Implement application-mode process supervision (JobManager JVM and TaskManager JVM per job) without Docker images.
  - Enable literal CLI execution `forseti gate --engine flink` (line 705) on smoke shard.
  - Measure execution times and memory budgets; record baseline metrics in the PR.
- **Verification:**
  - `mvn -B -ntp install` passes.
  - `python -m pytest -q oracle` passes.
  - `bash scripts/check-dependency-closure.sh` passes.
  - Core gate passes at 82/82.
  - `forseti gate --engine flink` on smoke shard outputs "assertions run N / expected N" and exits 0.
  - Shard execution times and memory consumption are measured and recorded in the PR.

### PR 5: Engine-Independent Injections, Baseline Controls, Assertions 708–718, and CI Sharding
- **Tasks:**
  - Run every engine-independent fault injection of line 705 on Flink (injection 24 belongs to PR 7).
  - Implement initial eight negative controls (lines 689–700).
  - Implement and verify gate assertions 708–718 except the Kafka Streams lines 712–714 (including the subtask map of line 715 and one split per source subtask, line 717).
  - Commit `gate/flink-shards.json` allocating one stack per shard job, isolating horizon recordings, isolating restart injections, and scoping controls to shards owning intended target IDs.
  - Split shards until the slowest measured shard satisfies the CI execution budget.
- **Verification:**
  - `mvn -B -ntp install` passes.
  - `python -m pytest -q oracle` passes.
  - `bash scripts/check-dependency-closure.sh` passes.
  - Core gate passes at 82/82.
  - Replay gate across CI shards outputs "assertions run N / expected N" and exits 0 (scope not yet semantic).
  - Each of the eight negative controls fails with its intended assertion IDs.

### PR 6: Fault Injections 21, 22, Assertions 719–726, and Control θ
- **Tasks:**
  - Implement fault injection 21 (kill JobManager and TaskManager).
  - Implement fault injection 22(i) using the SIGSTOP, Admin `describeTransactions` validation, SIGKILL, and second `describeTransactions` validation protocol (Decision D8), recording post-restart commit/abort outcome; prepare fallback wrapper if needed.
  - Implement fault injection 22(ii) killing stage 2 after stage-1 output is committed and before stage-2 processing.
  - Implement fault injection 22(iv) combining 22(i) with a second kill after all jobs are RUNNING and before batch k+1.
  - Record injection 22(iii) as not applicable to Flink (line 685), never as a skip.
  - Implement negative control θ (flag switching sinks to non-transactional/at-least-once, line 700).
  - Implement and verify gate assertions 719–726 (excluding Kafka Streams line 724).
- **Verification:**
  - `mvn -B -ntp install` passes.
  - `python -m pytest -q oracle` passes.
  - `bash scripts/check-dependency-closure.sh` passes.
  - Core gate passes at 82/82.
  - Semantic replay gate exits 0 with "assertions run N / expected N".
  - Each of the nine negative controls exits 1 with its intended failure IDs verified via `python scripts/check-control-report.py`.

### PR 7: Requirements Accounting, Eligibility Reporting, Workflows, and Step Completion
- **Tasks:**
  - Implement `forseti requirements`: injection 24 on the dedicated and the shared placement, item 5 (c) and (d) (resume from injection 24, restart from the gate's results for 21 and 22, the baseline at batch interval ×2 and ×½), and 5 (a) and (b) recorded from the gate's results for 26 and 7 (line 705).
  - Add unit tests for requirements accounting logic across pass, miss, and error states.
  - Record requirement 5(c) miss on dedicated placement per Decision 18 without failing requirements command execution.
  - Produce explicit engine eligibility output line.
  - Wire GitHub Actions workflows for PR replay shards, nightly unsharded runs with three-run determinism, and main push requirements evaluation.
  - Update repository documentation for Flink engine execution and gate verification.
- **Verification:**
  - `mvn -B -ntp install` passes.
  - `python -m pytest -q oracle` passes.
  - `bash scripts/check-dependency-closure.sh` passes.
  - Core gate passes at 82/82.
  - Replay gate exits 0 on all PR shards.
  - Nine negative controls exit 1 with intended IDs.
  - `forseti requirements --engine flink --gate-report build/flink/report.json --out build/requirements` exits 0 with pass/miss/error recorded for every requirement and outputs an eligibility line.
  - All CI workflows pass green on main branch.

## CI placement

- **Per-PR workflow (`.github/workflows/ci.yml`):**
  - Executes on Linux runners for all pull requests.
  - Executes `mvn -B -ntp install`, `python -m pytest -q oracle`, and `bash scripts/check-dependency-closure.sh`.
  - Runs fresh Flink replay gate across shards defined in `gate/flink-shards.json` on every PR; core-evidence reuse is not permitted to replace Flink execution (line 728).
  - Shard jobs run with isolated stacks, isolated horizon recordings, isolated restart injections, and scoped negative controls.
  - Replay input key binds shard evidence; dependencies, Docker images, and fixture bytes are cached.
  - Windows runners execute unit tests and the core gate only.
- **Nightly workflow (`.github/workflows/nightly-gate.yml`):**
  - Executes on Linux runners.
  - Runs unsharded Flink replay gate across all scenarios.
  - Executes three-run determinism verification.
  - Executes `forseti requirements --engine flink`.
- **Main push (`.github/workflows/ci.yml`):**
  - Executes on Linux runners upon merge to main.
  - Runs the per-PR jobs and `forseti requirements --engine flink`.
  - Retains all build and gate report artifacts for controller consumption (line 737).

## Verification (step-6 completion gate)

```bash
mvn -B -ntp install
python -m pytest -q oracle
bash scripts/check-dependency-closure.sh
forseti gate --engine flink --out build/flink
forseti gate --engine flink --negative-control <name> --out build/flink-controls/<name>   # each of the nine controls
forseti requirements --engine flink --gate-report build/flink/report.json --out build/requirements
```

1. The normal gate exits 0 with "assertions run N / expected N".
2. Each of the nine negative controls exits 1 with its intended failures (verified by `scripts/check-control-report.py`).
3. The requirements command exits 0 with pass, miss, or error recorded for every requirement, alongside engine eligibility.
4. CI is green on the main branch.
5. The controller reads the CI artifacts and retains the logs (line 737).
6. A recorded miss makes Flink ineligible and the step still completes with the miss recorded.

## Risks

| Risk | Impact | Mitigation |
| :--- | :--- | :--- |
| Whole-station state serialization on every checkpoint under a 24 h horizon | Checkpoint serialization overhead may degrade performance or exceed checkpoint intervals under long horizons | Measured in PR 4; a segmented state API in core is the fix if needed. |
| Checkpoint commit cost per cut makes shards slow | Shard execution duration exceeds CI budgets | Measured directly; shards are split; no cut is dropped. |
| 22(i) race between an in-flight commit and the kill | Process kill may occur during an idle period or after commit completion, invalidating the fault injection | The D8 validity checks using Kafka Admin describeTransactions before and after SIGKILL, with invalid attempts retried up to N times, and the fallback sink wrapper if needed. |
| The oracle's merged-cut path has not run on restart cuts | Undetected discrepancies between engine cut merges and oracle cut merges following a restart | Tests for asymmetric station completion and merged cuts are added to the validation suite. |
| Decision 18 predicts a Flink miss of item 5 (c) on the dedicated placement | Requirement 5(c) is not satisfied for Flink on dedicated placement | Recorded as a miss; Flink is ineligible; step 6 completes. |
| Linux-only replay gate | Replay gate cannot execute natively on Windows developer workstations | Windows environments keep unit tests and the core gate; replay gate execution is enforced in Linux CI for every PR (satisfying line 728). |
