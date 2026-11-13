# Forseti Step 3: Engine-Independent Core and Oracle Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Forseti gets its first code: `forseti-core` (Java 17; value types, RFC 8785 canonical digests, the release reader at Vor's level, rule and binding-profile loading from a Bifrost-published release, the two stage decision functions as pure functions, `ProgressTracker`), a `forseti` CLI (`generation new|promote|retire|list`, a reference harness that drives the core without an engine through cut barriers, a recording builder and synthesizer, and `forseti gate --engine core`), an independent Python oracle (sort-and-sweep, no shared code), the shared JCS vector file run by both test suites, the two input JSON Schemas (`urn:forseti:schema:occupancy:1.0.0`, `urn:forseti:schema:incident:1.0.0`), hand-made recordings and synthesized continuous recordings for the horizon scenarios. The step is complete when `forseti gate --engine core` runs every engine-independent injection of the spec's table, prints `assertions run N / expected N` with N equal to its committed manifest, exits 0, and each of the eight engine-independent negative controls exits 1 for its intended assertion; the spec's completion gate: "without an engine, core equals the oracle on every engine-independent injection". A step-3 report names its scope `core` and never claims the semantic gate of spec line 671 or engine eligibility.

**Architecture:** Two Maven modules and one Python package, coupled only by files. `core` holds everything the engine adapters of steps 6-7 will call: `decide(keyState, event, watermark) -> (newState, outputs)` for stage 1 (per station) and stage 2 (per station, driven by `ProgressTracker`), the journal row and progress record types with their envelopes, the disposition ledger types, the canonical digest, the release reader and the rule set, and the pure generation transition policy. `cli` holds what runs the core without an engine: the reference harness (stations as keyed state, cut barriers per subscribed partition, the per-station `W1` formula, timers fired in deadline order after the cut's inputs, journals written as JSONL files in the record shape the Kafka topics will carry), the recording builder and synthesizer (scenario JSON -> recording directory, ground truth kept apart from delivered records), the generation CLI over a locked, durable file-backed control log (the Kafka transport is step 6), and the gate that folds the journals, runs the oracle as a subprocess, compares live rows and dispositions by identity, and counts assertions against a committed manifest. `oracle/` is a Python package that reads the same recording, the cut log the harness exports and the release payload named by the pins, and recomputes every live row body from scratch per cut by sort-and-sweep. Nothing in forseti imports Bifrost, Tyr, Vor or Muninn code; the only cross-repository execution is Bifrost's `gates` CLI inside the fixture-generation script and one seam test, as Tyr runs it.

**Tech Stack:** Java 17 (Temurin 17 in CI), Maven multi-module (`forseti-parent`, `core`, `cli`), Jackson 2.17.2, `io.github.erdtman:java-json-canonicalization:1.1` (JCS), `com.networknt:json-schema-validator:1.5.1` (draft 2020-12, as Bifrost), JUnit 5.10.3, maven-compiler 3.13.0, surefire 3.2.5, maven-shade 3.6.0 for the `forseti` jar. Python 3.13 pinned in CI (`>=3.11` declared), `rfc8785==0.1.4`, `jsonschema==4.26.0`, `pytest==8.3.5`; the oracle runs under `python -I`. Engines are not dependencies of step 3; the spec records the proposed pins Flink `2.2.1`, `org.apache.flink:flink-connector-kafka:5.0.0-2.2`, Kafka Streams `4.2.2` (Task 1). Files are LF (`.gitattributes`); fixture, vector and schema bytes are `-text`. Product names in prose are ASCII (Mimir, Tyr). Branches off forseti `main` (`384933d` when planned; `7dce7c0` after PR 0, then the merge of the amendment PR of 2026-11-11), one per PR: `docs/step3-pins-and-decisions`, `feat/step3-foundation-and-release-reader`, `feat/step3-attribution-core`, `feat/step3-impact-and-progress`, `feat/step3-recordings-and-oracle`, `feat/step3-generation-cli`, `test/step3-core-gate`.

---

All spec line references are lines of `docs/superpowers/specs/2026-11-05-forseti-design.md` at `384933d`, before the Task 1 edits, except on the lines the Amendment 2026-11-11 section rewrites, which cite the current spec. Commands and expected outputs below are acceptance criteria for implementation; none was executed while planning.

## Spec and rules applied

- Spec: Delivery plan row 3 (line 714). Decisions 1 (30), 2 (34), 4 (42-44), 5 (47), 6 (52), 7 (56), 8 (60), 9 (64), 11 (72), 12 (76), 14 (84), 15 (88), 16 (93), 17 (97). Contracts: input events (231-233), process messages (237-263), session events (265-278), occupancy (280-296), incidents (298-315), output rows and envelope (317-333), work record body (335-355), sample journal body (357-368), impact assessment body (370-379), affected set body (381-400), cut barriers, progress records and the disposition ledgers (402-452), topics and partitioning (454-484). Correction semantics (486-535), generations and revisions (537-553). Testing and gates: cuts and the independent oracle (576-607), fault injections (609-651), negative controls (653-667), proving the gates run (669-701). Open questions (722-732).
- Owner decisions of 2026-11-09: Q1 the split below (a spec-correction PR, then six code PRs in this order); Q2 Kafka Streams pinned at 4.2.2 (same 4.2 client line as the connector's parent, one broker version); Q3 as replaced on 2026-11-11 (Amendment 2026-11-11, decision 2): Tyr's re-authored bundles carry the real `format.schemaSha256` values and the incident topic `quality.incidents`, forseti publishes both Tyr bundles byte for byte, `binding.schema-mismatch` is proved on a forseti-derived copy that Bifrost's `gates` CLI approves and publishes into `fixtures/registry-mismatch`, and the Tyr re-author PR and the Bifrost fixture PR come before PR 1 (section Prerequisites before PR 1); Q4 every step-3 interpretation the spec leaves open is written into the spec by the first PR, before any code freezes bytes (Task 1 gives the exact text). Merge rules: gate command `forseti gate --engine core` (steps 6-7 widen the same command with `flink` and `kafka-streams`); pytest for the oracle.
- Facts checked by the controller on 2026-11-09 and used as given: the newest `flink-connector-kafka` is `5.0.0-2.2` (built for Flink 2.2; parent `kafka.version` 4.2.0); Flink 2.2.x releases are 2.2.0 and 2.2.1; Kafka Streams 4.2.2 exists (4.4.0 is the latest); the Java JCS library is published as `io.github.erdtman:java-json-canonicalization` (1.1 latest; the spec's `org.erdtman` is not a published coordinate); Python `rfc8785` latest 0.1.4; JDK 17 Ed25519 (JEP 339) with a self-written DSSE PAE is what Bifrost runs on Temurin 17 in CI.
- Rules the plan applies as the spec states them. Line 56: `bodyDigest` = SHA-256 over the UTF-8 JCS bytes of the body; pinned libraries in both languages; one shared vector file run by both suites; times are epoch milliseconds as integers; non-finite values are not aggregated. Line 76: `forseti-core` has no Flink or Kafka Streams dependency; decision functions are pure; `ProgressTracker` drives stage 2 from stage-1 progress records. Line 84: `heads -> release.json -> payload` by raw-byte SHA-256; each head verified as Vor's `head.py` does (two signatures from distinct trusted keys, a version floor that never goes down, expiry). Line 72: a generation pins both rules, the hierarchy, the binding profiles and the input releases; jobs refuse an unregistered generation; `promote` requires the gate report digest. Line 553: the floor at `<stateDir>/head-floors/<stream>.json` in Vor's shape. Lines 192, 204: the job compares `format.schemaSha256` with its built-in schema and refuses to start on a difference (`binding.schema-mismatch`). Line 588: the oracle is a Python batch program sharing no code with core, sort-and-sweep; it receives per cut the included offset ranges, the batch manifest, each station's `cutId` and `admissionW`, evaluates inputs admitted against `admissionW` and timers up to the reported `W1`, with `W2(s, cut) = W1(s, cut)`; intermediate revisions are checked only for monotonicity, integrity and no equal consecutive bodies. Lines 592-594: mandatory deadline-bracketing cuts, injection 23's planned `A`, synthesized continuous recordings with telemetry tails. Line 596: ground truth kept separate from delivered records; the JCS vector file executes in both suites. Lines 598-607: scenario fixture shape; sizes pinned in the plan. Lines 671, 696-703: the gate's assertion manifest, `assertions run N / expected N`, exit 1 on any assertion not run; every run records seed, fixture digest, pins, engine and job digests, offset ranges; a step is complete when the controller runs the commands and retains the logs.
- Zero shared code (line 5, Decision 1): no Bifrost, Tyr, Vor or Muninn artifact on any Maven or Python classpath (a dependency-tree check enforces it, Task 2); published files only (Tyr's bundles as inputs to a fixture script, Bifrost's registry layout, Bifrost-published heads); Bifrost's `gates` CLI runs only inside `scripts/make_registry_fixture.py` and the seam test.
- Precedents read: Bifrost step-1 plan and Tyr step-2 plan (plan form); Bifrost `core/.../identity/{Ed25519Keys,Dsse}.java`, `mapping/MappingHeadAttestation.java`, `MappingGate.java` (CLI syntax), `docs/MAPPING-PUBLICATION.md` "Head freshness (TUF rules)", `MappingTyrAttributionRuleTest.java`; Vor `head.py`, `release.py`, `tests/fixtures/registries/active`, `scripts/make_fixtures.py`, `tests/helpers.py`, CI; Tyr `fixtures/attribution-rule/{bundle,plc-stamped-bundle}` (verified byte-identical to Bifrost's `tyr-attribution-impact` and `tyr-attribution-plc-stamped` with `diff -r`), Tyr CI (sibling Bifrost checkout).

## Amendment 2026-11-11

Owner decisions of 2026-11-11, applied throughout this plan. PR 0 is merged (forseti #6, `main` = `7dce7c0`). "Current spec" means the spec at that commit: every line this amendment rewrites cites current spec lines, every other line keeps its reference to the spec at `384933d`, and references of the form "Task 1's line N" keep Task 1's own numbering. The spec is not edited: its lines 111, 487, 603 and 734 already state three stations and its line 472 already names `quality.incidents`.

1. **Open points.** Points 1-3 are accepted as recommended. Point 4 is rejected: Line2 has three stations (`station1`, `station2`, `station3`; two nutrunners under Station1) in every repository. Station partitions: `station1` -> 0, `station2` -> 1, `station3` -> 2; 3-5 unassigned.
2. **Q3 replaced.** Tyr's re-authored bundles carry the real `format.schemaSha256` values and the incident topic `quality.incidents`, and forseti publishes both Tyr bundles byte for byte (no `fix_profiles`, no submission re-stamp: Tyr authors the plc bundle on stream `line2-plc`). `binding.schema-mismatch` is proved on a forseti-derived copy (one profile's `format.schemaSha256` changed, re-pinned, approved and published by Bifrost's `gates` CLI) in the fixture `registry-mismatch`; the trust path head -> `release.json` -> payload is unchanged.
3. **Order.** The former Follow-up (Tyr re-author, Bifrost copy) comes before PR 1: (a) this amendment PR, which also commits `schemas/occupancy-1.0.0.schema.json`, `schemas/incident-1.0.0.schema.json` and `.gitattributes` exactly as Step 4.1 and Step 2.1 give them (raw-byte SHA-256: occupancy `e97cd7c97b1ab5e2d63d5b06e6057415945b82cff6d2ca0345106d2daf9d29dc`, incident `7141c3115d90812f16b22049c66bfaa64942ebacf854b343fb99b3dce168fb34`), so Task 4 copies the committed files instead of creating them; (b) the Tyr re-author PR; (c) the Bifrost fixture PR; (d) the Tyr PR that restores its two copy equalities; (e) forseti PR 1, whose `fixtures/registry.lock.json` pins the merged Tyr and Bifrost heads. Section Prerequisites before PR 1 gives each PR's content and gate.
4. **Scenarios.** Station3 gets four baseline visits (units 1-4) at `[780000, 840000)`, `[960000, 1020000)`, `[1140000, 1200000)` and `[1320000, 1380000)`; existing visit ids and times are unchanged; 12 -> 16 visits, 4 -> 5 devices (Task 16).
5. **plc-stamped.** The plc-stamped rule keeps identity on `station1` and `nutrunner1` only; its `stations[]` gains `station3`.
6. **Recordings.** Replaces the decision of 2026-11-10 that every hand-made recording is committed (owner decision of 2026-11-11): only the smoke recordings of Task 8 are committed; the twenty hand-made recordings of Task 16 are built at gate time into `build/recordings/<name>/` from their committed scenario files and verified against their committed locks before every run, as the five horizon recordings are. Reason: every rebuild of a committed recording adds a copy to git history (up to about 11 MB packed per full rebuild) and the recordings change through steps 3-7; after delivery step 7 the owner decides again whether to commit them as one snapshot. The ledger entry counts two smoke recordings and eighteen others; this plan has three (`smoke`, `smoke-plc`, `smoke-incident`) and twenty, and applies the decision by kind.

Unchanged: every assertion id, task number, PR number and branch name, the manifest growth 18 -> 82, contract 2.27.0 and the spec. Recomputed: file, message and sample counts (five devices instead of four; Tyr's bundles five files larger each). A count that depends on Tyr's re-authored bundle, which does not exist yet, is given with its derivation and marked "to be confirmed when the Tyr PR merges".

## Amendment 2026-11-12

Owner decision of 2026-11-12, taken after two rounds of advice, applied in PR 4 (forseti #11); the spec is not edited.

1. Oracle output (Task 18). Oracle equality at every completed cut of every scenario stays as spec line 579 requires, and the oracle still recomputes every live row from scratch at every cut. Its output becomes per-cut deltas: a full set at the first cut, then per cut the keys whose recomputed body changed (with the body), the deleted keys, and a count and set hash computed from the freshly reconstructed set, for the four journals and both disposition maps. With `--stream` it hands the cuts to the gate one at a time; the gate folds the deltas, recomputes every body digest itself, checks count and set hash, compares with the harness and deletes the cut. `--snapshots` writes full cuts for the diff files; `--no-memo` disables every cache that crosses cuts. Reason: full snapshots took 9.2 GB per horizon recording; the largest cut on disk is now 21.8 MB.
2. Gate runtime and CI layout (Open point 2, Step 19.3). Measured locally on a shared Windows machine, the hand-made and smoke scenarios with their shuffled runs take 6 min 42 s and one horizon recording about 21 minutes (harness 302 s, oracle 826 s, comparison 147 s), so the plan's 13-23 minutes do not hold. Measured on GitHub's ubuntu runner (run of 2026-11-12): build and tests 3 min, shards hand-made 13 min, horizon-correction-beyond 13 min, horizon-no-departure 15 min, horizon-planned-late 11 min, horizon-reopened 16 min, plc-horizon-no-end 25 min, aggregate under 1 min. The plc-horizon-no-end shard first timed out at 2 hours: under plc-stamped attribution the core re-derived every lifecycle from every stored snapshot for each message (quadratic in a recording whose visit never ends); the core now keeps an index of the valid snapshots per equipment in derivation order, checked equal to the previous derivation on random snapshot sets. The gate runs in shards given by `gate/core-shards.json`: one shard for the hand-made and smoke scenarios and the shuffled runs, one per horizon recording (with the hand-made scenario its assertion compares against); the assertions over every scenario run (the seven `J-*` aggregates, `J-AFTER-HORIZON-CUTS`, `C-2-REFERENCE`) are evaluated by an aggregate run. `forseti gate --engine core --shard <name>` writes `report.json` and `summaries.json`; `--aggregate <dir>` passes only when every shard ran against the same manifest, plan, fixtures, seed and pins, recorded exactly its assertions, compared every scenario at every cut its lock plans, and the aggregate assertions hold; it prints `assertions run 72 / expected 72`. The manifest is unchanged. `report.json` gains `shard`, `shardPlanSha256` and `results` (every result with its evidence). CI: `build` (tests, jar), `gate` (one matrix job per shard, horizon heap 6g, 60-minute timeout), `core-gate` (the aggregate and the negative-control loop), plus `bifrost-seam` and `windows`; JDK 17.0.21 and Python 3.13.18 are pinned exactly so every shard records the same pins; a newer push to a pull request cancels the older run; a nightly workflow runs the same gate unsharded as the equivalence check. The aggregate run also reconciles each shard report's own accounting with its results (exit 0, `failed` equal to the failing results, `notRun` empty, assertion counts equal to the shard's plan, no assertion recorded twice), checks cuts by identity (the recording manifest digest equals the one the scenario lock pins; the planned cut ids, as many as the lock plans and strictly increasing, equal the cuts the harness completed and the cuts the gate compared, in order), refuses a scenario or shuffled run summarised twice or with an unplanned seed, and requires two shards that made the same scenario to agree on every summary field except timings.
3. Expectations that followed the plan instead of the spec now follow the spec text: `I-10f` (spec 648, "Next revision; disposition accepted (late-after-settlement)"): the visit, already complete, gets a next revision and both deliveries are accepted late-after-settlement; the provisional step between two completes cannot occur in `corrections`, because W1 is already past the new `leave + L` when the correction is delivered. `I-15` (spec 657): units per `onWithdrawn` with `incident-withdrawn`, resolution withdrawn; "never complete" holds from the cut that applies the withdrawn revision 2 (revision 1 is a closed incident whose set may settle complete earlier). `C-2-REFERENCE`: lateness is `admissionW - sourceTs` of an accepted sample; `held-back-10min`'s longest is `delayMs - scanMs - O - 1 = 568999`, not 600000.
4. Ground truth. Four errors of the scenario generator (not of the spec) are fixed where core and oracle agreed with the spec, and five scenario locks change: session events of devices outside the plc rule's catalog are rejected `unknown-device`; `attributionEnd` at a VisitId change; no occupancy dispositions when occupancy is not subscribed; the third planned late sample is `replay-required`.

## Amendment 2026-11-13

Owner decisions of 2026-11-13 (items 1-4 and, after a review of the first draft, items 5-8, listed below in this amendment), taken
after two proposals for Tasks 20-21 under the sharded gate, binding with the owner decisions of 2026-11-12 on the sharded gate
(every PR runs all shards and the aggregate with no recording cache; promotion accepts a whole-gate report only; negative
controls run in their owning shard only; the aggregate checks shard exit codes, failed and not-run results before PR #11
merged). Applied from
PR 5 on; "current spec" means the spec at `bc299d5` as edited by Step 20.0. Chunk 6 is replaced as a whole and cites current spec
lines; the lines of Chunk 7, Open points and Verification that this amendment rewrites cite current spec lines too, every other
line keeps its reference as Amendment 2026-11-11 states.

1. **PR 5 is delivered as two PRs.** PR 5a (`feat/step3-generation-cli`, Task 20, manifest 72 -> 75) and PR 5b
   (`feat/step3-promotion-and-replay`, Task 21, manifest 75 -> 79). PR 6 keeps its number, branch and gate (82). Task numbers and
   assertion ids are unchanged (Chunk 6).
2. **The spec is edited first.** Step 20.0 writes three additions into the spec before any code or lock: the run citation `run` on
   `replaying`, `verified` and `current` rows (null otherwise) and the address of a generation outside its store (its store and
   `registeredRowSha256`); the rule that the gate registers only through `generation new` in its pipeline store, with isolated
   assertion stores as the only place a gate run may call `retire`, while harness and jobs only read; the event-time replay
   (order, batches, watermarks and deadlines). Spec lines 414, 483 and 570 are adjusted so nothing contradicts. `Generation`
   therefore gains the field `run` (the plan said it "gains nothing").
3. **`generation new` may register without a recording** and writes no recovery declaration. The initial declaration is written by
   the run preparation of the generation's first run, when the routing is known (Step 20.3). This replaces "appends `registered` and
   the initial recovery declaration" of the former Step 20.2.
4. **One pipeline store per gate run, registered through the verb.** Every run of a gate run (full or shard) reads a generation
   registered by `generation new`, sequentially, in a fixed order, before the scenario pipeline starts; `Stage1Assertions.register`
   (the hand-written row, always number 1) is deleted. `gate --state <dir>` is valid for a full run and a `--shard` run and a usage
   error (exit 2) with `--aggregate` and `--negative-control`. With `--state` the registrations use the executable verb and the
   system clock and are `shared`; without it they go to a temporary store with the fixture clock and are `isolated`. The sentence
   "the gate appends nothing" now reads: the gate appends no row by any path of its own (Step 20.4).
5. **Report format.** `report.json` gains `kind`, `layout`, `negativeControl`, `summariesSha256`, `results[].shard`, `shards[]`,
   `generations[]` and `runs[]`; every generation and run entry carries `store`, a bundle-local identifier of the control store,
   and identities are unique and resolved per `(shard, store)`, never by a digest alone. A full run writes `summaries.json` like a
   shard run. `run.json` gains `registeredRowSha256`, `scope`, `replay` and `originalRecordingManifestSha256` in 5a (the last two
   null until 5b sets them). The aggregate report binds every shard's `report.json` and `summaries.json` by raw SHA-256. One
   validator, `GateEvidence.validate`, binds every run to its successful summary, its locked recording and its exact cut ids in
   both layouts (Step 20.4).
6. **Promotion policy** (2026-11-12 owner decision: promotion accepts a whole-gate report only, a full or aggregate report). `promote` accepts a whole-gate report only (`kind: gate`, `layout: full` or
   `aggregate`), requires the result-id set to equal the committed manifest's, addresses the generation by store and
   `registeredRowSha256`, re-reads the evidence files of either layout, refuses `isolated` generations, performs its final
   approval, floor and history checks under the same hold of `generations.lock` as the publication (lock order: generation lock,
   then floor lock), and appends its rows as one batch (Steps 21.1-21.2). Real registration and promotion use the system clock:
   the fixture heads expire on 2027-11-12, after which they must be re-published through Bifrost for the required check to pass;
   no flag sets the clock.
7. **Shard ownership.** `F-GEN-NEW`, `F-GEN-REFUSES-UNREGISTERED`, `F-GEN-FLOOR` (5a) and `F-GEN-LIFECYCLE`,
   `F-GEN-PROMOTE-REQUIRES-REPORT`, `I-20` (5b) belong to shard `hand-made`; `I-23` (5b) to `horizon-planned-late`, which from 5b on
   also makes the generation run `horizon-planned-late@replay`. From 5b on no shard is empty: the exit-0 `0 / 0` branch of
   `CoreGate.run` is removed and the plan refuses a shard without assertions. `F-GEN-FLOOR` and `I-20`'s generations stay in
   isolated assertion stores.
8. **`F-GEN-LIFECYCLE` writes no `current` row.** The gate never promotes on a report it wrote; positive promotion is proved by unit
   tests on fixtures and, on real evidence, by `scripts/gate-then-promote.sh` (Step 21.7).
9. **End-to-end promotion moves from PR 6 to PR 5b and promotes twice** (owner decision 4): `scripts/gate-then-promote.sh
   --from-shards` runs in the `core-gate` job, promotes the base generation of `horizon-planned-late` and then its replay generation,
   and requires the base generation to end `retired`. The nightly unsharded run uses the script's full mode. Task 23 no longer adds
   that step.
10. **Event-time replay** (owner decisions 5-7) is a materialized recording pinned by a committed lock
    (`fixtures/scenarios/<name>.replay.lock.json`). Its batches follow telemetry-derived cut targets; the materializer computes the
    actual replay watermark of every station at every cut and keeps a deadline's cut ids only where `W1_A < d ≤ W1_B` is proved on
    that progression, replans the bracket otherwise and refuses when none exists; untimed partitions are exempt and an untimed
    head keeps its original batches. A message is ordered by its smallest valid member `sourceTs`, while its largest still
    advances the watermark. A replay timer row carries `{"kind": "replayed", "inputKind": "timer", "identity": {"deadline"}}`, and
    `RunFold`, `JournalFold` and the stage-1 segment check read both timer causes through one function (Step 21.3).
11. **Earlier-generation rows are kept as bytes.** The journals of the base run and of the replay run of a generation-run scenario
    are preserved under distinct stable paths of the shard output and travel in the artifact; `I-23` hashes the base journals
    before the replay, rereads them afterwards and requires unchanged bytes and fold results (Steps 21.4-21.5).
12. **Corrections found on the way.** `LockedFile` did not exist (`FloorStore` inlined the lock): it is extracted in Step 20.1. The
    former Step 20.4 expected `Tests run: 14` for sixteen listed tests: the counts of Chunk 6 replace it. The manifest description of
    `C-2-REFERENCE` still said 600000: it reads 568999 (Amendment 2026-11-12, item 3). Spec references of Chunk 6 are the current ones
    (Decision 11 = 73, recovery control = 414, topics = 483-484, generations = 552-570, engine-independent harness = 607, injection 23
    planning = 611, injection 20 = 662, injection 23 = 665, proving the gates run = 690).

Unchanged: every assertion id and task number, the manifest growth 72 -> 79 -> 82, the six shards and the aggregate job as the
required check, no recording cache, PR 6's content.

## Amendment 2026-11-14

PR 6 (`test/step3-core-gate`, Tasks 22-23, manifest 79 -> 82) applies the owner decision of 2026-11-12 on negative controls, which the
Chunk 7 text predates; where Chunk 7 or Verification item 6 describe whole-gate control runs, this amendment replaces them.

1. `gate/core-negative-controls.json` lists each control with its intended ids and the shards that own or feed them; the gate
   refuses a file whose controls, intended ids or shards disagree with the implementation, the manifest or the shard plan.
   `wrong-rules-pin` intends every `I-*` id (34) and runs on all six shards.
2. A control runs as `--negative-control <c> --shard <s>` per listed shard, with the same manifest, shard plan, fixtures, seed
   and jar; it must exit 1 with the intended ids that shard evaluates in `failed`, and a shard that only feeds an aggregate id
   may exit 0. A crash, a failed run summary or a missing run never counts as the intended failure, and every cut and oracle
   comparison is kept.
3. A control with an intended aggregate id (`drop-late`: `J-DISPOSITION-COVERAGE`; `no-cascade`: `J-BASIS-CURRENT`) is then proved
   by `--aggregate <dir> --negative-control <c>` over that control's shard runs only, which must exit 1 with the aggregate ids
   failed.
4. `report.json.intendedFailures` holds the intended ids the run's layout evaluates; `report.json.control` records the control's
   name, all intended ids, its shards, the ids this run owns, the intended ids that failed, problems and whether the verdict held;
   `run.json` gains `negativeControl`. `scripts/check-control-report.py` checks one control run.
5. CI replaces the `core-gate` control loop with a `control` matrix (one job per control and shard) and a `control-aggregate`
   matrix; `core-gate` requires both, and checks that an unlisted control name exits 2.
6. The spec gains one sentence under Negative controls: a control run uses the same manifest, shard plan, fixtures, seed and jar
   as the gate, and evaluates its declared assertions (owner approved, 2026-11-12). The plan file itself is the one already
   committed; Task 23 creates no second copy.

## Sub-steps

Eight PRs in this order (owner decision Q1; PR 5 is delivered as 5a and 5b, Amendment 2026-11-13). Each has its own branch off the merged predecessor, an executable gate (`forseti gate --engine core` with the committed manifest `gate/core-assertions.json` growing from 18 to 82 assertions, plus the Maven and pytest commands of its tasks), and a pull-request task. Earlier gates report their sub-step; only PR 6 closes step 3. No `tools/verify.py` wrapper is kept: the gate binary, Maven and pytest are the three commands, and a wrapper would be a fourth place where a skipped test could hide.

| PR | Branch | Tasks | Executable gate | Why the boundary is here |
|---|---|---|---|---|
| 0 | `docs/step3-pins-and-decisions` | 1 | `bash scripts/check-spec-decisions.sh` exits 0 (every required phrase present in the spec; only the spec and the script changed against `main`) | The pins and every step-3 interpretation are written into the spec before any code freezes bytes (Q4); a reviewer sees the decisions without code |
| 1 | `feat/step3-foundation-and-release-reader` | 2-9 | manifest at 18 `F-*` assertions; `mvn install`; `pytest` | Everything the decision functions consume is fixed as files and types (canonical bytes, schemas, identities, journal reducers, verified releases, rule set, recording format); none of it depends on a semantic decision |
| 2 | `feat/step3-attribution-core` | 10-13 | manifest at 24 (+6 `S-*`): stage 1 through the reference harness on the committed smoke recordings and inline sequences, without the oracle | Stage 1 is one rule and one owner; its outputs (`forseti.work-records`, `forseti.samples`) are stage 2's published inputs; the harness is how the core runs without an engine, so it belongs with the first decision function |
| 3 | `feat/step3-impact-and-progress` | 14-15 | manifest at 29 (+5 `T-*`): `ProgressTracker`, stage 2, cascade, deletes on the same recording | The second rule and owner; the cascade and the progress protocol must exist before differential testing crosses two stages |
| 4 | `feat/step3-recordings-and-oracle` | 16-19 | manifest at 72 (+32 `I-*`, +8 `J-*`, +3 `C-*`): every engine-independent injection except 20 and 23, the oracle, integrity, determinism, item 2 reference values | The independent implementation and the fixtures it is compared on are reviewed together; the synthesized horizon recordings (about 440k messages each) enter here |
| 5a | `feat/step3-generation-cli` | 20 | manifest at 75 (+`F-GEN-NEW`, `F-GEN-REFUSES-UNREGISTERED`, `F-GEN-FLOOR`); six shards and the aggregate | The spec additions, the locked control store and the move of every run onto a verb-registered generation are reviewed apart from new semantics; a mistake breaks the 72 existing assertions |
| 5b | `feat/step3-promotion-and-replay` | 21 | manifest at 79 (+`F-GEN-LIFECYCLE`, `F-GEN-PROMOTE-REQUIRES-REPORT`, `I-20`, `I-23`); `scripts/gate-then-promote.sh --from-shards` exits 0 in `core-gate` | Promotion bound to whole-gate evidence and the event-time replay are reviewed apart from business evaluation |
| 6 | `test/step3-core-gate` | 22-23 | manifest at 82 (+`C-1`, +2 gate self-tests); eight negative controls exit 1; CI | Closes the step: assertion accounting proven to run, controls, CI, documentation |

PR 0 is merged (forseti #6, `main` = `7dce7c0`). Between PR 0 and PR 1 come the amendment PR and three PRs in the sibling repositories (Amendment 2026-11-11, decision 3; section Prerequisites before PR 1). They add no forseti code and no assertion; the PRs above keep their numbers, branches and gates (PR 5 as 5a and 5b, Amendment 2026-11-13).

## File Structure

| File | Responsibility |
|---|---|
| `docs/superpowers/specs/2026-11-05-forseti-design.md` | **PR 0** - Task 1 insertions only (pins, reader path, step-3 interpretations); contracts and gate meaning unchanged |
| `.gitattributes`, `.gitignore` | LF everywhere; `fixtures/** -text`, `vectors/** -text`, `schemas/** -text`, `core/src/main/resources/forseti/** -text`; ignore `target/`, `build/`, caches, private keys |
| `.github/workflows/ci.yml` | Linux: ASCII names, dependency-closure check, `mvn -B -ntp install`, oracle `pytest`, the core gate in one job per shard of `gate/core-shards.json` with `--state` (PR 5a), the `core-gate` job (aggregate, from PR 5b `scripts/gate-then-promote.sh --from-shards`, every control in `gate/core-negative-controls.json` exits 1), the Bifrost seam job with pinned sibling checkouts; Windows: `mvn -B -ntp install` + oracle tests |
| `scripts/check-ascii-names.sh`, `scripts/check-dependency-closure.sh` | the family's product-name check; `mvn dependency:tree` grep for banned groups (`yggdrasil.bifrost`, `org.apache.flink`, `org.apache.kafka`) |
| `pom.xml`, `core/pom.xml`, `cli/pom.xml` | `forseti-parent` 0.1.0-SNAPSHOT (pins above); `forseti-core` (Jackson, JCS, networknt only); `forseti-cli` -> shaded `cli/target/forseti.jar`, main `yggdrasil.forseti.cli.Main` |
| `core/src/main/resources/forseti/schemas/{occupancy,incident}-1.0.0.schema.json` | the two input schemas (spec 280-315); their raw-byte SHA-256 is the profiles' `format.schemaSha256` |
| `schemas/{occupancy,incident}-1.0.0.schema.json` | the published copies at the root (what Tyr and Bifrost cite), committed by the amendment PR of 2026-11-11 together with `.gitattributes` so that their digests exist before Tyr re-authors its profiles; a test asserts byte equality with the core resources |
| `vectors/jcs-vectors.json` | the shared JCS vector file: inputs as JSON text, expected canonical text and SHA-256, refusal vectors |
| `core/.../core/Json.java` | the one Jackson mapper (strict, duplicate keys refused, no NaN/Infinity, big integers kept) and the pretty writer in Tyr's shape |
| `core/.../core/canon/Canonical.java` | `bytes(JsonNode)`, `digest(JsonNode)`, `digestOf(byte[])`; the two pre-checks; the only call site of the JCS library |
| `core/.../core/input/{Inputs,Ids,InputParser,StatusCodes}.java` | input records with `Origin`; semantic identities and `payloadDigest`; schema validation (`schema-invalid`) and structural parsing; OPC UA severity bits |
| `core/.../core/journal/{Envelope,Row,Delete,Pins,Progress,Disposition,Revisions,JournalFold}.java` | the common envelope (spec 321-331), `delete` control record, the three progress bodies (412-452), the ledger key and body (437-450), revision emission (one revision per body change, deletes in the chain), the fold (current generation, highest revision per key, deletes applied, progress excluded) |
| `core/.../core/release/{Refusal,TrustedKeys,FloorStore,Dsse,HeadVerifier,ReleaseReader}.java` | the reader at Vor's level (Decision 14); the closed refusal set of Task 1 |
| `core/.../core/rules/{Hierarchy,MemberCatalog,AttributionRule,ImpactRule,OccupancyBinding,IncidentBinding,RuleSet}.java` | typed views of the released documents, member resolution and roles (spec 132), `binding.schema-mismatch`, `pins()` and `rulesDigest()` (spec 355) |
| `core/.../core/stage1/{Events,Stage1State,Stage1,PlcStamped,VisitClaim,Attribution,Coverage,Sessions,WorkRecord,SampleRow}.java` | stage 1 (rule sheet S1-S11 and P1-P3) |
| `core/.../core/stage2/{Events2,ProgressTracker,Stage2State,Stage2,Incidents,Relatedness,DecisionTable,SetCoverage,Assessment,AffectedSet}.java` | stage 2 (rule sheet T1-T10) |
| `core/.../core/generation/{Generation,GenerationStore}.java` | the generation row (current spec 555-566; `run` added in **PR 5a**) and the store interface (**PR 2**, Task 11: the harness reads them from its first run; history validation in PR 5a) |
| `core/.../core/generation/GenerationPolicy.java` | the pure transition policy (`registered -> replaying -> verified -> current -> retired`, one current per scope) (**PR 5a**) |
| `core/.../core/release/LockedFile.java` | the two-level lock (in-JVM lock per real path, then the `FileLock`) shared by `FloorStore` and the generation store (**PR 5a**) |
| `cli/.../cli/{Main,Args}.java` | `forseti generation ...`, `forseti recordings build|synth|verify`, `forseti run --engine core`, `forseti gate --engine core` |
| `cli/.../cli/recording/{Recording,RoutingManifest,Murmur2}.java` | recording directory format, routing manifest and `routingDigest`, Kafka's default partitioner for raw device keys (**PR 1**) |
| `cli/.../cli/recording/{ScenarioBuilder,Overlays,GroundTruth,Synthesizer,Telemetry}.java` | scenario JSON -> recording + ground truth; injection overlays; streamed continuous recordings and lock files (**PR 4**) |
| `core/.../core/NegativeControls.java` | the record of control flags the decision functions are constructed with, `NONE` in production (**PR 2**, Task 10, as a parameter nothing reads yet); PR 6 adds the behaviours and cli composes its values from `--negative-control`; core never depends on cli |
| `cli/.../cli/harness/{ReferenceEngine,StationTask,RejectCollector,ContractOrder,JournalFiles,CutLog,RunRecord}.java` | the engine-independent reference adapter (stage 1 in PR 2, stage 2 in PR 3, controls in PR 6) |
| `cli/.../cli/generation/{GenerationCli,FileGenerationStore,GateReportReader}.java` | `new`, `retire`, `list`, the run preparation and the locked, durable `append` (**PR 5a**); `promote` and the whole-gate report reader (**PR 5b**); `FileGenerationStore`'s read side arrives in **PR 2** (Task 11) |
| `cli/.../cli/gate/GateEvidence.java` | the one evidence validator of a gate output directory: store-qualified identities, every run bound to its successful summary, locked recording and exact cut ids (**PR 5a**; generation runs and kept journals in PR 5b) |
| `cli/.../cli/recording/ReplayRecording.java` | materializes the event-time replay recording of current spec 570: order, telemetry-derived batches, replay watermarks, proved or replanned deadline brackets (**PR 5b**) |
| `gate/core-shards.json` | the shard plan: scenarios, shuffled runs, assertions and (PR 5b) `generationRuns` per shard; the aggregate list |
| `fixtures/scenarios/{horizon-planned-late,smoke}.replay.lock.json` | the pinned digests of the two materialized replay recordings (**PR 5b**) |
| `scripts/gate-then-promote.sh` | gate (full or `--from-shards`), then promote the base and the replay generation of `horizon-planned-late` through the CLI (**PR 5b**; run by `core-gate` and nightly) |
| `cli/.../cli/gate/{CoreGate,Assertions,Comparison,Integrity,Injections}.java` | manifest loading, assertion accounting, folding, oracle comparison, integrity checks, per-injection assertions |
| `gate/core-assertions.json`, `gate/core-negative-controls.json` | the committed assertion manifest of `--engine core` (ids below) and the control list CI loops over |
| `oracle/pyproject.toml`, `oracle/src/forseti_oracle/{canon,inputs,rules,recording,cuts,stage1,stage2,fold,cli}.py`, `oracle/tests/*.py` | the oracle and its pytest suite |
| `fixtures/registry/**`, `fixtures/registry-next/**`, `fixtures/registry-mismatch/**`, `fixtures/trusted-keys.jsonl`, `fixtures/registry.lock.json` | Bifrost-published registries: `registry` holds Tyr's two bundles published byte for byte on streams `line2` and `line2-plc`, and on stream `line2-plc-nocheck` a forseti-derived, re-pinned copy of the plc bundle (the plc rule with `crossCheck: none`, for 11b); `registry-next` adds rule 1.0.1 on stream `line2` (head version 2; a re-pinned copy); `registry-mismatch` publishes a re-pinned copy of Tyr's `bundle` whose occupancy profile carries another `format.schemaSha256` (the `binding.schema-mismatch` fixture reached after head and release verification); the trusted public keys outside the registries; the lock (Bifrost and Tyr heads, schema digests, every file digest of the three registries) |
| `fixtures/upstream/{bundle,plc-stamped-bundle}/**`, `fixtures/upstream/manifest.json` | Tyr's two bundles copied byte for byte (44 + 64 files expected, to be confirmed when the Tyr PR merges) with their digests; they stay untouched and are the bytes forseti publishes and executes (Amendment 2026-11-11) |
| `fixtures/recordings/{smoke,smoke-plc,smoke-incident}/**`, `fixtures/scenarios/smoke*.json` (+ locks) | the three committed one-visit recordings (4,060, 4,060 and 7,760 messages, built from their scenarios with the full telemetry tail and re-verified by `recordings verify`) that show the format and carry PRs 2-3; an estimated 5.6 MB in the working tree (Task 16). No other recording is committed (Amendment 2026-11-11, decision 6): the twenty hand-made recordings of PR 4 and the five horizon recordings are built at gate time into `build/recordings/<name>/` and verified against their locks |
| `fixtures/scenarios/*.json`, `fixtures/scenarios/*.lock.json` | scenario definitions and pinned digests of the recordings they build, all committed (smoke: the recordings are committed under `fixtures/recordings/` too; the twenty hand-made scenarios of Task 16 and the five horizon scenarios: the recordings are built at gate time under `build/recordings/`) |
| `scripts/make_registry_fixture.py` | copies Tyr's bundles, refuses them unless their profiles carry the real schema digests and `quality.incidents`, approves and publishes them untouched with Bifrost's CLI into `registry`, and publishes the three re-pinned variants (`line2-plc-nocheck` in `registry`, rule 1.0.1 in `registry-next`, the changed schema digest in `registry-mismatch`) |
| `README.md`, `docs/CORE-GATE.md` | **PR 6** - what the core and oracle are, the three regimes of Decision 16, how to run the gate and the controls, the fixture provenance, what the core scope does not establish; the assertion table, formats, controls |

## Engine-independent injection list

"In" = run by `forseti gate --engine core`; "Out" = needs an engine, Kafka or the full loop. Included means the specified observable input and business semantics are tested on recordings; it does not claim a bridge, simulator, engine timer, transaction or broker was exercised.

| Id | In/Out | PR / later step | Reason (one line) |
|---|---|---|---|
| 1 | In | 4 | a delivery-order permutation of the same recording; compared at the settled cut |
| 2 | In | 4 | occupancy after samples; the stage-1 transition at the bracketing cuts |
| 2b | In | 4 | samples before their arrival; `unattributed` -> `attributed` revision |
| 3 | In | 4 | Bad is a `uaStatusCode` in the recording; the sim's `Sim/<member>.StatusCode` is how the loop produces it (step 4-5) |
| 4 | In | 4 | two overlapping closed visits are occupancy content |
| 5 | In | 4 | a duplicated record in the recording |
| 6 | In | 4 | a 24 h continuous recording (synthesized) |
| 7 | In | 4 | activity true with no open visit is telemetry content |
| 8 | In (consumption) | 4; production in 5 | the core consumes `seq-gap`/`session-down`/`session-up`; Muninn's termination, the Will and the bridge's event production are step 5 |
| 9, 9b | In | 4 | `sourceTs` shifted +30 s / +90 s in the recording; 9b's bands are per-sample expectations |
| 10, 10b, 10c, 10d, 10f | In | 4 | EPCIS error declarations and re-sends are occupancy content |
| 10e | In | 4 | needs the horizon (synthesized) |
| 11, 11c, 11d | In | 4 | plc-stamped identity members are telemetry content |
| 11b | In | 4 | held-back plc-stamped run with the occupancy partitions not subscribed |
| 11e | In | 4 | needs the horizon |
| 12-17 | In | 4 | stage 2 from stage-1 journals and incident content |
| 18, 19 | In | 4 | the cascade through the harness: stage-1 output re-drives stage 2 |
| 20 | In | 5b | two generations of one recording with two rule versions through the generation CLI and the two registry fixtures |
| 21 | Out | 6, 7 | `kill -9`, checkpoint restore and transactional sinks are engine behaviour |
| 22 (i)-(iv) | Out | 6, 7 | restart, rebalance and recovery control need the engines and Kafka |
| 23 | In | 5b | planned late samples at `A - horizonMs +- 1`; the second generation replays in event-time order |
| 24 | Out | 6, 7 (`forseti requirements`) | starvation and the 60 s observation deadline are engine time behaviour; both raw placements are step-5 fixtures |
| 25 (a), (b) | In (consumption) | 4; classification in 5 | the core consumes `stale-death` (session stays open) and `session-ambiguous` (coverage `unknown`); NDEATH assignment and missing-NBIRTH detection (Decision 10) are bridge logic |
| 26 | In | 4 | a quiet occupancy stream is a recording variant compared with its control run |
| alpha `processing-time` | In | 6 | the harness orders by arrival instead of event time; fails `I-1` |
| beta `drop-late` | In | 6 | the harness drops a beyond-horizon input without a disposition; fails `J-DISPOSITION-COVERAGE` on 23 |
| gamma `no-cascade` | In | 6 | stage 2 ignores basis changes; fails `I-18`, `J-BASIS-CURRENT` |
| delta `lenient-oracle` | In | 6 | the oracle counts `provisional` as `complete`; fails `I-2` and `I-6` at the mandatory cuts |
| epsilon `first-wins` | In | 6 | the first visit wins an overlap; fails `I-4` |
| zeta `station-granularity` | In | 6 | stage 2 matches station activity; fails `I-16` |
| eta `ignore-session-gap` | In | 6 | fails `I-8` |
| theta `non-transactional-output` | Out | 6, 7 | a failure between output and state commit is sink behaviour |
| iota `wrong-rules-pin` | In | 6 | the gate's cross-check of body `rulesDigest`, generation row and envelope pins |
| item 1 | In | 6 (aggregate) | core equals the oracle at every compared cut on every injection above |
| item 2 | In (reference values) | 4 | longest lateness corrected online, journal counts, zero inputs without disposition; engine values in 6-7 |
| item 6 | In | 4 | three runs with shuffled delivery order inside every batch |
| items 3, 4, 5(c)(d), 7, 8, 9 | Out | 6-8 | restart, replay wall time, isolation, adapter cost, footprint, latency are engine measurements (5(a) and 5(b) are the semantic obligations of 26 and 7, included) |

34 injection ids are included. Pure protocol types for `restartEpoch`, `recoveryRef`, `mergedCutIds` and `initEvidence` exist in the progress records from PR 1 on; they do not discharge 21 or 22.

## Conventions the tasks follow

- **Names come from the spec, as corrected by Task 1.** Statuses, reasons, dispositions, topics, body fields, progress fields and refusal codes are the spec's strings byte for byte. The names step 3 adds (CLI flags, file names) are listed in Task 1's insertion for line 714 or in this plan's File Structure, nowhere else.
- **Pure decision functions.** `Stage1.decide(state, event, watermark)` and `Stage2.decide(state, event, w2)` read nothing but their arguments and the `RuleSet` and the core's `NegativeControls` record they were constructed with (all false outside a control run; the cli only composes its values); no clock, random, file, environment or static mutable state. The caller owns the state object and gets it back (adapters load it from their store and write it back); a test runs one sequence twice from fresh states and asserts identical outputs and identical serialized state.
- **Emission.** A row is emitted when its `bodyDigest` differs from the last emitted digest of its logical key; `Revisions` assigns `revision` (from 1 per generation per key) and `supersedesDigest`; `delete` control records take the next revision; a run id or envelope-only change emits nothing. Progress records have no envelope and are never rows.
- **Contract order inside a cut.** `(eventTime, kindRank session 0 < occupancy 1 < sample 2, identity bytes, payloadDigest)` (spec 491); every input of a cut applies before the cut's timers, which fire in deadline order up to the engine's own event time; incidents (no event time) apply in stage 2 after the cut's stage-1 progress in `(identity bytes, payloadDigest)` order.
- **Oracle independence.** The oracle is written from the spec text (as corrected by Task 1), not from the Java. No constants file, schema loader, fixture helper or expected body is shared; the two meet only at the vector file, the recording files, the cut log, the released bytes and the journal files. An independence test asserts the oracle imports nothing from `cli/` or `core/` and never launches Java, and that Java never launches Python for a business decision (only the gate launches the oracle, to compare).
- **Tests.** Java: JUnit 5, `mvn -q -pl core -Dtest=<Class> test`; `cli` tests need `mvn -q -pl core install -DskipTests` first. Python: pytest from `oracle/` (parametrized vector and expectation tables; Vor's precedent). Every harness run writes `run.json` {seed (default `20261109`), fixture digests, pins, JCS library versions, engine `core-harness`, artifact digest, offset ranges, command}; a unit test without topic inputs records an empty offset-range list; the gate writes `report.json` with the same fields plus the manifest digest and scope `core`.
- **Commits.** One per task, conventional subjects (`feat(core): ...`, `feat(cli): ...`, `feat(oracle): ...`, `test(...)`, `docs(...)`, `build: ...`), the attribution trailer the session requires. Never on `main`. `git diff --check` clean before each commit.
- **Byte stability.** `Canonical.bytes` is the only serializer for anything digested; raw-file SHA-256 and canonical SHA-256 are never mixed (`Canonical.digestOf(bytes)` vs `Canonical.digest(node)`). Pretty JSON written by forseti uses two-space indent, `\n`, UTF-8, trailing newline, `": "` separators (Tyr's shape) through `Json.pretty`.
- **Time.** Epoch milliseconds as integers everywhere; `T0 = 1794182400000` (2026-11-09T00:00:00Z) is the scenario epoch; scenario durations in milliseconds from `T0`.
- **Partitions.** Six everywhere except the two ledgers (seven), `forseti.generations` and `forseti.recovery-control` (one). Station partitions from the sorted `stations[]` of the pinned rule (`synthetic-site:line2:station1` -> 0, `station2` -> 1, `station3` -> 2; 3-5 unassigned, current spec 487). Raw topics keep device keys with Kafka's default partitioner placement, recorded in `routing.json`.

---

## Chunk 1: Spec correction PR (PR 0)

### Task 0: Branches and the reference checkouts

**Files:** none

- [x] PR 0 (Task 1, branch `docs/step3-pins-and-decisions`) is merged: forseti #6, `main` = `7dce7c0`. Task 1 below is kept as merged; its line numbers are those of the spec at `384933d`.
- [ ] The amendment PR (Prerequisites (a)): `git -C forseti switch -c docs/step3-three-stations main` from `7dce7c0`; it carries this plan, the two schema files and `.gitattributes`, and no code.
- [ ] Before PR 1 starts: Prerequisites (a)-(d) are merged; `git -C forseti status --short` is empty and `git -C forseti log -1 --format=%h main` prints the merge of the amendment PR (record that head in the PR bodies).
- [ ] Sibling checkouts used read-only, at merged heads only, never at an unmerged branch: `../tyr` at `main` after Prerequisite (d) (`fixtures/attribution-rule/{bundle,plc-stamped-bundle}`: three stations, real schema digests, `quality.incidents`, the plc bundle on stream `line2-plc`), `../bifrost` at `main` after Prerequisite (c) with `gates/target/bifrost-gates.jar` built (`mvn -B -ntp -pl gates -am package -DskipTests`). Record both heads (`git rev-parse HEAD`); `scripts/make_registry_fixture.py` writes them into `fixtures/registry.lock.json` and CI checks out exactly those refs.

### Task 1: Spec correction PR: pins, the reader path, and every step-3 interpretation (Q4)

**Files:**
- Modify: `docs/superpowers/specs/2026-11-05-forseti-design.md`
- Create: `scripts/check-spec-decisions.sh` (the executable gate of this PR)

Each insertion below is given with the spec line it follows (or replaces) and its exact text. Nothing changes a contract field list, a reason vocabulary, a hypothesis or the meaning of a gate; the insertions fix readings the spec left open so that both implementations and the fixtures freeze the same bytes. Apply them from the bottom of the file upward so earlier line numbers stay valid.

- [ ] **Step 1.1** Replace line 724 (first Open question) with:

```
* Pinned versions (confirmed 2026-11-09 against Maven Central and PyPI): JCS in Java is `io.github.erdtman:java-json-canonicalization:1.1` (this spec earlier named the group `org.erdtman`, which is not the published coordinate), JCS in Python is `rfc8785==0.1.4`. For delivery steps 6-7 the pins are Flink `2.2.1` with `org.apache.flink:flink-connector-kafka:5.0.0-2.2` (the newest connector, built for Flink 2.2; its parent pins `kafka.version` 4.2.0; Flink 2.3.0 has no connector release) and Kafka Streams `4.2.2` (the 4.2 client line the connector's parent names, so both engines use one client line against one broker version; 4.4.0 is the latest release). Step 3 depends on neither engine; the step-6 plan confirms the native-time assumptions listed below for these versions.
```

- [ ] **Step 1.2** Replace line 725 with:

```
* The JDK 17 Ed25519 path (JEP 339: `KeyFactory`/`Signature` "Ed25519", X.509 SubjectPublicKeyInfo public keys, raw 64-byte signatures) and a self-written DSSE PAE (`"DSSEv1" SP LEN(type) SP type SP LEN(body) SP body`, lengths in ASCII decimal) are confirmed (2026-11-09): Bifrost signs and verifies heads with exactly this API on Temurin 17 in CI. Forseti writes its own reader and proves it in a test against a head Bifrost publishes (step 3).
```

- [ ] **Step 1.3** Replace line 714 (Delivery plan row 3) with (the scope wording; the completion gate is unchanged):

```
| 3 | forseti | `forseti-core`, `ProgressTracker`, generation CLI, oracle, JCS vectors, hand-made recordings, synthesized continuous recordings for horizon scenarios; the engine-independent subset of the injection table (1, 2, 2b, 3, 4, 5, 6, 7, 8 and 25 as consumption of recorded session events, 9, 9b, 10, 10b, 10c, 10d, 10e, 10f, 11, 11b, 11c, 11d, 11e, 12, 13, 14, 15, 16, 17, 18, 19, 20, 23, 26), the negative controls other than `non-transactional-output`, and comparison items 1, 2 (reference values) and 6, run by `forseti gate --engine core` against its own assertion manifest; a `core` report never claims the semantic gate or engine eligibility; the generation store and recovery control are files in step 3 and Kafka topics from step 6, with the row shapes of `forseti.generations` and `forseti.recovery-control` unchanged | without an engine, core equals the oracle on every engine-independent injection |
```

- [ ] **Step 1.4** Insert after line 588 (end of the oracle paragraph in "Cuts and the independent oracle"):

```
In the engine-independent reference harness of step 3, which runs the core with no engine, `W1(s)` is the per-station formula (the largest telemetry `sourceTs` routed to `s` over everything delivered through the cut, minus `outOfOrdernessMs`, minus 1), `admissionW` is the previous cut's `W1`, `restartEpoch` is 0, `recoveryRef` is null, `mergedCutIds` is empty, and the single recovery declaration is the generation's initial one; the Flink minimum-over-partitions form becomes a harness option in step 6.
```

- [ ] **Step 1.5** Insert after line 553 (end of "Generations and revisions"):

```
Generation status moves `registered` -> `replaying` -> `verified` -> `current` -> `retired`. Every status row is written by the operator CLI and by nothing else (jobs, the reference harness and the gate never write `forseti.generations`): `generation new` writes `registered`; `generation promote --run <run record> --gate-report <report>` writes, in one locked operation, `replaying` (citing the run's `runId` and offset ranges), `verified` (citing the gate report's digest, whose runs must include that run and whose scope the operator accepts explicitly) and `current`, and a `retired` row for the previous `current` of the scope; `generation retire` writes `retired`. Every change appends a row, no row is rewritten, and generation numbers are allocated once, increasing across the whole control store (never per scope, so a number names one generation whatever its scope, as the journal identity `(evaluationGeneration, key)` and the recovery declaration carry no scope); `current` is kept per scope, the scope being the `streamId` of the release that carries the attribution rule (the record key of `forseti.generations`). A job or the harness reads a registered generation's pinned `releaseEvidence` and verifies the release files against those digests; it does not re-verify the head (that is `new` and `promote`, against the current time). A generation replayed in event-time order (`forseti run --replay event-time`, Decision 11) carries `cause` `{"kind": "replayed", "inputKind", "identity"}` on every row it emits; other rows carry `{"kind": "input", "inputKind", "identity"}` or `{"kind": "timer", "deadline"}`.
```

- [ ] **Step 1.5b** Insert after line 515 (the row-set paragraph of "Stage 2"):

```
The row set accumulates over every delivered revision that passed the schema, scope, station and `revisionEventTime` checks, whatever its disposition: a superseded lower revision still widens the row set, while only the highest valid revision is judged; a revision in conflict or rejected contributes nothing. The final key set and bodies therefore do not depend on the order in which revisions arrive.
```

- [ ] **Step 1.6** Insert after line 502 (session gaps in "Stage 1"):

```
A `session-ambiguous` event opens an unknown interval for its device from the device's last valid observation before `detectedEventTime` to the next `session-up` `at`, or to `W1` while none has arrived; coverage over it is `unknown`. A sample whose `sourceTs` lies inside a `session-down` interval of its device is `unattributed` with reason `outside-session`; one inside an unknown interval is `unattributed` with reason `session-ambiguous`.
```

- [ ] **Step 1.7** Insert after line 491 (sample attribution in "Stage 1"):

```
Candidates are every visit of the read point whose window contains the sample, whatever its status; when exactly one candidate is not retracted the sample attributes to it; two or more non-retracted candidates make the sample `ambiguous`; when no candidate is non-retracted, the sample attributes to the retracted candidate whose actual `[enter, leave)` contains it (a retracted visit keeps the evidence of what happened), and when none or several do it is `unattributed` with reason `ambiguous`. Coverage counts observations by their quality-blind candidate (the same window rule with the quality, finiteness and session steps skipped): a Bad or rejected-Uncertain observation is never attributed and never aggregated, yet it counts for that visit's `observedPeriods`; `badSlots` counts only slots holding a raw Bad observation (line 137: Bad is judged from the status code's severity bits; a rejected Uncertain observation is not Bad and adds nothing to `badSlots` or the bad share); a sample that is `ambiguous`, `outside-session`, `session-ambiguous`, `non-finite` or without `sourceTs` counts for no visit.
```

- [ ] **Step 1.8** Insert after line 450 (the `replay-required` line, the last line of the ledger reason vocabulary block, so the block itself stays byte-identical for Step 1.15's hash):

```
An occupancy event whose `source` is not in the profile's `sources[]`, whose `unitId` does not match `identity.pattern`, whose `bizStep` is neither the profile's `enter` nor its `leave` value, or whose `errorDeclaration.reason` is not in the profile's `correction.reasons`, and an incident whose `kind` is not in the profile's `kinds[]`, or whose `revisionEventTime` breaks line 308 (revision 1 must carry `revisionEventTime == detectedAt`; a later revision's `revisionEventTime` must be at least that of every accepted lower revision and at most that of every accepted higher revision, judged by revision number whatever the arrival order), are `rejected` with reason `schema-invalid` and `refs.check` naming the field (`sources`, `identity.pattern`, `bizStep`, `correction.reasons`, `kinds`, `revisionEventTime`): the profile approves the vocabulary, the identity scheme and the correction reasons as part of the accepted format (Decision 4; line 213), and the retention rule of line 397 reads `revisionEventTime`, so its producer contract is checked at the door.
```

- [ ] **Step 1.8b** Insert after line 440 (the `payloadDigest` bullet of the ledger body):

```
- `payloadDigest` is the JCS SHA-256 of the record value without the diagnostic fields; when that value cannot be canonicalized (an integer outside [-(2^53-1), 2^53-1], a non-finite number, an unpaired surrogate, or a value that is not a JSON object) the `payloadDigest` is the SHA-256 of the raw record value bytes as delivered and `refs.digestRule` is `raw-bytes` (absent otherwise). The digest rule never decides the disposition: an occupancy event or incident whose value fails its schema or is not an object is `rejected schema-invalid`; a process message keeps every member that parses, a measurement member's numeric `value` is read as a binary64 double on both sides, so `9007199254740992`, `9007199254740992.0` and `9.007199254740992e15` are one value (the integer-domain refusal guards only the identity and body digests and never changes a sample's reason), and a member whose `value` is non-finite (`NaN`, `Infinity`, or a literal beyond the double range in either notation, such as `1e400` or `1` followed by 400 zeros, which both sides map to a signed infinity) gets the sample reason `non-finite` (lines 56, 364), with no effect on the message's other members or on its event time; a record that is not JSON at all is `rejected schema-invalid`. Such inputs never stop the job and always have their ledger rows.
```

- [ ] **Step 1.9a** Insert after line 406 (the cut-barrier paragraph):

```
An inclusive `throughOffset`, wherever it appears (the barrier itself, `barrierRefs`, `incidentOffsets`, a batch manifest), ranges over [-1, 2^53-1]: `-1` means that no business record of that partition is included yet, every other value is the offset of the last business record included; actual record offsets stay at or above 0. A barrier with `-1` is a complete barrier like any other.
```

- [ ] **Step 1.9** Replace line 424 (`initEvidence` bullet of the stage-1 progress body) with:

```
- `initEvidence` (only on the first progress after an initialization): {`declarationOffset` (the offset of the selected recovery declaration), `endOffset` (the control partition's end offset H read at initialization), `restore` (the engine's restore evidence; `"none"` in the step-3 reference harness)}
```

- [ ] **Step 1.10** Replace lines 392-393 (`provisionalUnits`, `insufficientUnits`) with:

```
  - `provisionalUnits`: Count of units whose assessment reason is `basis-provisional`
  - `insufficientUnits`: Count of units whose assessment status is `insufficient-evidence` with any other reason
```

and replace line 398 (`retentionGap`) with:

```
  - `retentionGap`: True unless the verified evidence-retention range recorded in the input and restore manifests (ranges verified to be empty included; in step 3 the recording manifest's verified telemetry range per subscribed device) covers the required interval `[suspectFrom, suspectTo)` (or `[suspectFrom, W2(s))` while open); first and last sample times are not used
```

- [ ] **Step 1.11** Insert after line 364 (`reason` of the sample journal body):

```
- `reason` is null when the sample is `attributed` and no diagnostic applies; an attributed sample under `crossCheck: occupancy` may carry `identity-mismatch`.
```

- [ ] **Step 1.12** Insert after line 353 (the `coverage` bullet of the work record body):

```
- Serialization of the aggregates: each `members[]` entry is `{mappingId, metric, equipmentPath, valueType, observedPeriods, missingPeriods, badSlots, <aggregate by result type>}`, one entry per required member and per any other measurement member with at least one coverage observation inside J, sorted by `mappingId`; the aggregate counts accepted-quality observations only, that is Good observations and, when the rule's `quality.uncertainInput` is `propagate`, Uncertain ones; Bad and rejected-Uncertain observations are never aggregated and never attributed, though they count for `observedPeriods`, and only raw Bad observations count for `badSlots` and the bad share (`count` may be 0, then `min`, `max` and `last` are null, `trueCount` and `distinctCount` 0); the activity support of line 351 uses the same accepted-quality set; the visit-level `coverage` takes `observedPeriods` as the minimum and `missingPeriods` and `badSlots` as the maximum over the `equipmentEvidence[]` entries' `coverage` (which equals the extremes over the required members when the station or its tools have measurement members, and falls back to the activity members when the station has none, as a station whose only member is `Running` does: its visit-level values are then its activity member's `observedPeriods` and `missingPeriods` and `badSlots` 0), and `sessionGaps` as the sum over the equipment entries; each `equipmentEvidence[]` entry takes its `coverage` values the same way over that equipment's measurement members (an equipment with none takes its activity member's `observedPeriods` and `missingPeriods` and `badSlots` 0), lists `memberRefs` sorted, lists intervals in time order as `[start, end)` pairs, and is sorted by `equipmentPath`; its `coverage.status` is `unknown` when `unknownActivityPeriods > 0` or an unknown session interval overlaps J, else `incomplete` when `missingPeriods > maxMissingPeriods`, `sessionGaps > 0` or any of its measurement members exceeds `maxBadShare`, else `complete`; every `samplesDigest` is taken over the distinct contributing observations sorted by `device`, `metric`, `sourceTs`, then canonical bytes; `insufficiencyReasons[]` is sorted by code.
```

- [ ] **Step 1.13** Insert after line 84 (Decision 14's Decision sentence):

```
- The reader's refusals form a closed set with Vor's names for the same failures, `head-unverified`, `head-rollback`, `head-expired`, `provenance-mismatch`, plus the start-up refusal `binding.schema-mismatch`; a refusal names its rule and evidence and is never a disposition reason.
```

- [ ] **Step 1.14** Insert after line 56 (Decision 7's Decision sentence):

```
- Before canonicalization, forseti refuses a body holding a non-finite number or an integer outside [-(2^53-1), 2^53-1], so the two pinned libraries are never asked to print what they would print differently; the shared vector file carries refusal vectors for both.
```

- [ ] **Step 1.15** `scripts/check-spec-decisions.sh`, the executable gate of PR 0: (a) the changed-file set is exactly `docs/superpowers/specs/2026-11-05-forseti-design.md` and `scripts/check-spec-decisions.sh`: with `--working-tree` (the pre-commit mode) the set is `git diff --name-only main` plus `git ls-files --others --exclude-standard`, with `--committed` (the post-commit mode, what CI and the PR review run) it is `git diff --name-only main...HEAD`, which sees nothing before the first commit; (b) each of these seventeen phrases occurs in the spec exactly once (`grep -c -F`): `io.github.erdtman:java-json-canonicalization:1.1`, `rfc8785==0.1.4`, `flink-connector-kafka:5.0.0-2.2`, `Kafka Streams \`4.2.2\``, `JEP 339`, `forseti gate --engine core`, `per-station formula`, `generation promote --run`, `"kind": "replayed"`, `last valid observation before \`detectedEventTime\``, `retracted candidate whose actual`, `refs.check`, `declarationOffset`, `head-unverified`, `over the \`equipmentEvidence[]\` entries' \`coverage\``, `no business record of that partition is included yet`, `a superseded lower revision still widens the row set` (each string is quoted verbatim from the insertion texts of Steps 1.1-1.14 above, 1.5b, 1.8b and 1.9a included; before committing, run the script against the edited spec and against this plan's fenced insertion blocks, both must count every string exactly once); (c) `grep -c -F "org.erdtman:java-json-canonicalization"` is 0; (d) the contract tables are unchanged: the SHA-256 of the two refusal-code tables (the blocks from the lines starting `| Refusal Code |` to their next blank line) and of the ledger reason vocabulary (the block from `Reasons form a closed vocabulary:` to its next blank line), extracted by pattern so the insertions above and below do not move them, equals the values the script records from `main`. Prints `spec decisions: 17/17 present, contracts unchanged`, exit 1 otherwise. Run `bash scripts/check-spec-decisions.sh --working-tree` before the edits (red: `0/17`) and after them (green), then commit, then `bash scripts/check-spec-decisions.sh --committed` (green; this is the mode CI runs). `git diff --check` clean; re-read every inserted paragraph against its surroundings. Commit `docs(spec): record the step-3 pins, the reader path and the interpretations step 3 fixes`; push; open PR `docs: step 3 pins and decisions` against `main`. The PR body lists the seventeen insertions by spec line (seventeen check strings) and states that no contract field list, reason vocabulary or gate meaning changed. Merge before PR 1; every later line reference in this plan is to the file before these edits.

## Prerequisites before PR 1 (Amendment 2026-11-11; the amendment PR and the sibling repositories)

The former Follow-up, moved before PR 1 by the owner decision of 2026-11-11 and widened to three stations. Each PR merges before the next starts. Zero shared code holds throughout: the repositories meet only at committed files, and each repository's CI reads its sibling at a merged head.

- [ ] **(a) forseti amendment PR** (branch `docs/step3-three-stations`): this plan, plus `schemas/occupancy-1.0.0.schema.json`, `schemas/incident-1.0.0.schema.json` and `.gitattributes` with exactly the bytes Step 4.1 and Step 2.1 give. The digests Tyr must cite exist from this PR on: raw-byte SHA-256 occupancy `e97cd7c97b1ab5e2d63d5b06e6057415945b82cff6d2ca0345106d2daf9d29dc`, incident `7141c3115d90812f16b22049c66bfaa64942ebacf854b343fb99b3dce168fb34`. Gate: `git diff --name-only main...HEAD` lists exactly the plan, the two schema files and `.gitattributes`; `sha256sum schemas/*.json` prints those two digests; `git diff --check main...HEAD` prints nothing. The spec and `scripts/check-spec-decisions.sh` are unchanged (that script is PR 0's gate; its changed-file check holds only on PR 0's branch and is not run here). A schema byte changed after this PR merges reruns (b)-(d).
- [ ] **(b) Tyr re-author PR.** Both workspaces `fixtures/attribution-rule/{workspace,plc-stamped-workspace}.json` gain `station3`: the hierarchy node and CSV row, connection `sim-opcua-station3`, source `station3-running` (`Line2/Station3.Running`) with its manual evidence, target `station3` and mapping set `station3-mapping`. The occupancy-join rule's `stations[]`, `members[]` and `activity[]` gain station3. The plc-stamped rule's `stations[]` gains station3 and nothing else: identity, measurement and activity members stay on `station1` and `nutrunner1` (Amendment decision 5). Both profile records carry `format.schemaSha256` = the two digests of (a) and the incident `topic` `quality.incidents`; the plc bundle's submission names stream `line2-plc`. Bundles rebuilt by `publication.build`; expected sizes 44 and 64 files (today's 39 and 59 plus five station3 files each: one source, one manual text, one evidence artifact, one mapping set, one target; to be confirmed when this PR merges). Tyr's two assertions that its bundles equal Bifrost's copies are dropped in this PR, because Bifrost still holds the old bytes, and return in (d). Gate: Tyr's `python -B scripts/ci_run.py` with the sibling Bifrost jar (its own tests and the Bifrost approval seam).
- [ ] **(c) Bifrost fixture PR.** `core/src/test/resources/mapping-fixtures/{tyr-attribution-impact,tyr-attribution-plc-stamped}` replaced by byte copies of the Tyr bundles of (b) (`diff -r` against the merged Tyr head prints nothing); Bifrost's own step-1 fixtures `attribution-impact` and `attribution-plc-stamped` extended to three stations, the incident topic `quality.incidents` and the real digests, the plc identity scope unchanged; the four mapping test classes follow. Contract 2.27.0 and its schemas do not change: these are legal 2.27.0 instances. Gate: Bifrost's `mvn -B -ntp clean test`.
- [ ] **(d) Tyr copy-equality PR.** The two assertions dropped in (b) restored against Bifrost `main` after (c). Gate: `python -B scripts/ci_run.py`.
- [ ] **(e) forseti PR 1** (Chunk 2 below). `fixtures/registry.lock.json` pins the Tyr head after (d) and the Bifrost head after (c), and CI checks out exactly those refs (Step 2.6).

## Chunk 2: Foundation and release reader (PR 1)

Branch: `git switch -c feat/step3-foundation-and-release-reader main` after PR 0 and Prerequisites (a)-(d) merged.

### Task 2: Repository skeleton, CI, LF, ASCII names, dependency closure

**Files:**
- Exists (committed by the amendment PR of 2026-11-11, unchanged here): `.gitattributes`
- Create: `pom.xml`, `core/pom.xml`, `cli/pom.xml`, `.gitignore`, `.github/workflows/ci.yml`, `scripts/check-ascii-names.sh`, `scripts/check-dependency-closure.sh`, `oracle/pyproject.toml`, `oracle/src/forseti_oracle/__init__.py`, `oracle/tests/__init__.py`

- [ ] **Step 2.1** `.gitattributes` (committed by the amendment PR of 2026-11-11 with exactly these bytes; this step checks `git diff main -- .gitattributes` prints nothing and creates only `.gitignore`):

```
# LF everywhere a text file is written; digested bytes are never normalised.
* text=auto eol=lf
fixtures/** -text
vectors/** -text
schemas/** -text
core/src/main/resources/forseti/** -text
```

`.gitignore`: `target/`, `build/`, `oracle/.pytest_cache/`, `oracle/**/__pycache__/`, `oracle/src/*.egg-info/`, `.venv/`, `*.key`.

- [ ] **Step 2.2** `pom.xml` (parent; the shape of Bifrost's):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>yggdrasil.forseti</groupId>
  <artifactId>forseti-parent</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>pom</packaging>
  <modules><module>core</module><module>cli</module></modules>
  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <jackson.version>2.17.2</jackson.version>
    <jcs.version>1.1</jcs.version>
    <networknt.version>1.5.1</networknt.version>
    <junit.version>5.10.3</junit.version>
    <maven-compiler-plugin.version>3.13.0</maven-compiler-plugin.version>
    <maven-surefire-plugin.version>3.2.5</maven-surefire-plugin.version>
    <maven-shade-plugin.version>3.6.0</maven-shade-plugin.version>
  </properties>
  <dependencyManagement><dependencies>
    <dependency><groupId>com.fasterxml.jackson.core</groupId><artifactId>jackson-databind</artifactId><version>${jackson.version}</version></dependency>
    <dependency><groupId>io.github.erdtman</groupId><artifactId>java-json-canonicalization</artifactId><version>${jcs.version}</version></dependency>
    <dependency><groupId>com.networknt</groupId><artifactId>json-schema-validator</artifactId><version>${networknt.version}</version></dependency>
    <dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId><version>${junit.version}</version><scope>test</scope></dependency>
  </dependencies></dependencyManagement>
  <build><pluginManagement><plugins>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>${maven-compiler-plugin.version}</version></plugin>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-surefire-plugin</artifactId><version>${maven-surefire-plugin.version}</version></plugin>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-shade-plugin</artifactId><version>${maven-shade-plugin.version}</version></plugin>
  </plugins></pluginManagement></build>
</project>
```

`core/pom.xml`: parent above, `artifactId` `forseti-core`, dependencies `jackson-databind`, `java-json-canonicalization`, `json-schema-validator`, `junit-jupiter` (test); a comment at the top: "No Flink, no Kafka Streams, no Kafka client, no other Yggdrasil artifact: the adapters of steps 6-7 depend on this module, never the reverse." `cli/pom.xml`: `forseti-cli`, depends on `forseti-core` and `junit-jupiter`; shade bound to `package`, `finalName` `forseti`, `mainClass` `yggdrasil.forseti.cli.Main`.

- [ ] **Step 2.3** `scripts/check-dependency-closure.sh` (`set -euo pipefail`): `OUT="$(pwd)/build/deps.txt"; rm -f "$OUT"; mvn -B -ntp -q dependency:tree -DoutputFile="$OUT" -DappendOutput=true` (an absolute path, because a relative `outputFile` is resolved per module and the root file would hold only the parent's tree); then the file must exist and contain the two module headers `yggdrasil.forseti:forseti-core:` and `yggdrasil.forseti:forseti-cli:` (else exit 1, "incomplete tree"), and `grep -E "yggdrasil\.(bifrost|tyr|vor|muninn)|org\.apache\.flink|org\.apache\.kafka"` over it must find nothing (prints `dependency closure: clean`, exit 1 otherwise). A Maven failure fails the script. Transitive dependencies are checked, not only imports.

- [ ] **Step 2.4** `oracle/pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "forseti-oracle"
version = "0.1.0"
description = "Independent oracle for forseti: recomputes every journal row body per cut by sort-and-sweep; shares no code with forseti-core"
requires-python = ">=3.11"
license = "Apache-2.0"
dependencies = ["rfc8785==0.1.4", "jsonschema==4.26.0"]

[project.optional-dependencies]
dev = ["pytest==8.3.5"]

[project.scripts]
forseti-oracle = "forseti_oracle.cli:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 2.5** `scripts/check-ascii-names.sh`: Vor's script in form (grep for the accented spellings of Mimir, Tyr and Vor outside `fixtures/`, with the spec's first-mention allowance; exit 1 on a hit; prints `product names: ASCII only`).

- [ ] **Step 2.6** `.github/workflows/ci.yml` (sibling checkouts pinned to the refs in `fixtures/registry.lock.json`, read in a first step; Tyr's sibling-checkout pattern):

The job layout below is the PR 1 layout; PR 4 splits the gate into shards (Amendment 2026-11-12, item 2).

```yaml
name: CI
on:
  push: {branches: [main]}
  pull_request:
  workflow_dispatch:
permissions: {contents: read}
jobs:
  build:
    name: core + cli tests, oracle tests, core gate
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: {path: forseti}
      - name: Product names in ASCII
        working-directory: forseti
        run: bash scripts/check-ascii-names.sh
      - uses: actions/setup-java@v4
        with: {java-version: '17', distribution: temurin, cache: maven}
      - uses: actions/setup-python@v5
        with: {python-version: '3.13'}
      - name: Install the oracle
        working-directory: forseti/oracle
        run: python -m pip install -e ".[dev]"
      - name: Oracle tests
        working-directory: forseti/oracle
        run: python -m pytest -q
      - name: Build and test (core, cli)
        working-directory: forseti
        run: mvn -B -ntp install
      - name: Dependency closure
        working-directory: forseti
        run: bash scripts/check-dependency-closure.sh
      - name: Core gate
        working-directory: forseti
        run: java -jar cli/target/forseti.jar gate --engine core --out build/gate --python "$(command -v python)"
      - name: Negative controls exit 1 for their intended assertion
        working-directory: forseti
        run: |
          for c in $(python -I -c 'import json;print(" ".join(json.load(open("gate/core-negative-controls.json"))["controls"]))'); do
            set +e; java -jar cli/target/forseti.jar gate --engine core --negative-control "$c" --out "build/gate-$c" --python "$(command -v python)"; rc=$?; set -e
            [ "$rc" -eq 1 ] || { echo "negative control $c exited $rc, expected 1"; exit 1; }
            python -I -c "import json,sys;r=json.load(open('build/gate-$c/report.json'));sys.exit(0 if set(r['intendedFailures'])<=set(r['failed']) else 1)" || { echo "$c did not fail its intended assertion"; exit 1; }
          done
      - name: Gate reports
        if: always()
        uses: actions/upload-artifact@v4
        with: {name: core-gate, path: forseti/build/gate*/, if-no-files-found: error}
  bifrost-seam:
    name: release reader against a head Bifrost publishes now
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: {path: forseti}
      - id: pins
        working-directory: forseti
        run: |
          echo "bifrost=$(python3 -c 'import json;print(json.load(open("fixtures/registry.lock.json"))["bifrost"])')" >> "$GITHUB_OUTPUT"
          echo "tyr=$(python3 -c 'import json;print(json.load(open("fixtures/registry.lock.json"))["tyr"])')" >> "$GITHUB_OUTPUT"
      - uses: actions/checkout@v4
        with: {repository: yggdrasil-iiot/bifrost, path: bifrost, ref: "${{ steps.pins.outputs.bifrost }}"}
      - uses: actions/checkout@v4
        with: {repository: yggdrasil-iiot/tyr, path: tyr, ref: "${{ steps.pins.outputs.tyr }}"}
      - uses: actions/setup-java@v4
        with: {java-version: '17', distribution: temurin, cache: maven}
      - uses: actions/setup-python@v5
        with: {python-version: '3.13'}
      - name: Build Bifrost's gates CLI
        working-directory: bifrost
        run: mvn -B -ntp -pl gates -am package -DskipTests
      - name: Seam test
        working-directory: forseti
        env: {FORSETI_BIFROST_JAR: ../bifrost/gates/target/bifrost-gates.jar, FORSETI_TYR_FIXTURES: ../tyr/fixtures/attribution-rule}
        run: mvn -B -ntp -pl core -am -Dtest=BifrostSeamTest -Dsurefire.failIfNoSpecifiedTests=false test
  windows:
    name: tests on Windows
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: {java-version: '17', distribution: temurin, cache: maven}
      - uses: actions/setup-python@v5
        with: {python-version: '3.13'}
      - run: python -m pip install -e "oracle[dev]"
      - run: python -m pytest -q oracle
      - run: mvn -B -ntp install
```

The gate and control steps fail until Task 9 adds the gate command and the manifest; intended (CI is red on the branch until PR 1's last task). The `windows` job runs the oracle tests and the Maven build; the gate runs on Linux. During implementation pin the `uses:` actions to full commit SHAs and record them in the PR body (none is invented here).

- [ ] **Step 2.7** `mvn -B -ntp -q install` -> `BUILD SUCCESS` (empty modules); `bash scripts/check-dependency-closure.sh` -> `dependency closure: clean`; `python -m pip install -e "oracle[dev]" && python -m pytest -q oracle` -> `no tests ran`. Commit `build: Maven skeleton, oracle package, CI, LF, ASCII-name and dependency-closure rules`.

### Task 3: Canonical form (JCS) in Java and Python, the shared vector file

**Files:**
- Create: `vectors/jcs-vectors.json`
- Create: `core/src/main/java/yggdrasil/forseti/core/canon/Canonical.java`, `core/src/main/java/yggdrasil/forseti/core/Json.java`
- Create: `core/src/test/java/yggdrasil/forseti/core/canon/CanonicalVectorsTest.java`
- Create: `oracle/src/forseti_oracle/canon.py`, `oracle/tests/test_jcs_vectors.py`

The vector file holds inputs as JSON *text* so each side parses with its own parser (Jackson, `json`) and canonicalizes with its own pinned library; the expected canonical text and SHA-256 are the same for both. Refusal vectors pin that both sides refuse what JCS cannot carry deterministically across the two libraries: a non-finite number and an integer outside `[-(2^53-1), 2^53-1]` (Python `rfc8785` raises `IntegerDomainError` there; the Java library would print it; forseti refuses before either library is reached, per Task 1's insertion at line 56), plus parser-level refusals (duplicate keys, invalid JSON) that both parsers must share.

- [ ] **Step 3.1** `vectors/jcs-vectors.json` (the `canonical` strings are what RFC 8785 produces; compute `sha256` of the UTF-8 canonical bytes with `python -I` after writing; a wrong expected value fails both suites):

```json
{
  "vectorsVersion": 1,
  "vectors": [
    {"id": "empty-object", "inputText": "{}", "canonical": "{}"},
    {"id": "empty-array", "inputText": "[]", "canonical": "[]"},
    {"id": "key-order-utf16", "inputText": "{\"b\":1,\"a\":2,\"\\u00e9\":3,\"B\":4,\"aa\":5,\"\\ud83d\\ude00\":6}", "canonical": "{\"B\":4,\"a\":2,\"aa\":5,\"b\":1,\"\u00e9\":3,\"\ud83d\ude00\":6}"},
    {"id": "integers", "inputText": "[0,-0,1,-1,9007199254740991,-9007199254740991,1794182400000]", "canonical": "[0,0,1,-1,9007199254740991,-9007199254740991,1794182400000]"},
    {"id": "doubles-es6", "inputText": "[1.0,100.0,0.1,1e21,1e-7,1.2345678901234568e20,5e-324,1.7976931348623157e308,0.30000000000000004,-0.0,2.5]", "canonical": "[1,100,0.1,1e+21,1e-7,123456789012345680000,5e-324,1.7976931348623157e+308,0.30000000000000004,0,2.5]"},
    {"id": "strings-escapes", "inputText": "[\"\\u0000\",\"\\u001f\",\"\\\"\",\"\\\\\",\"/\",\"\\b\\f\\n\\r\\t\",\"\\u007f\",\"\\u2028\",\"e\\u0301\"]", "canonical": "[\"\\u0000\",\"\\u001f\",\"\\\"\",\"\\\\\",\"/\",\"\\b\\f\\n\\r\\t\",\"\u007f\",\"\u2028\",\"e\u0301\"]"},
    {"id": "nested", "inputText": "{\"z\":[{\"y\":null,\"x\":true,\"w\":false}],\"a\":{\"c\":[],\"b\":{}}}", "canonical": "{\"a\":{\"b\":{},\"c\":[]},\"z\":[{\"w\":false,\"x\":true,\"y\":null}]}"},
    {"id": "whitespace-ignored", "inputText": "{ \"k\" : [ 1 , 2 ] , \"j\" : \"v\" }", "canonical": "{\"j\":\"v\",\"k\":[1,2]}"},
    {"id": "work-record-shape", "inputText": "{\"status\":\"complete\",\"interval\":{\"enter\":1794183000000,\"leave\":1794183060000,\"attributionEnd\":null},\"members\":[{\"mappingId\":\"nutrunner1-torque\",\"count\":55,\"min\":10.5,\"max\":42.0,\"last\":41.25}],\"insufficiencyReasons\":[]}", "canonical": "{\"insufficiencyReasons\":[],\"interval\":{\"attributionEnd\":null,\"enter\":1794183000000,\"leave\":1794183060000},\"members\":[{\"count\":55,\"last\":41.25,\"mappingId\":\"nutrunner1-torque\",\"max\":42,\"min\":10.5}],\"status\":\"complete\"}"}
  ],
  "refusals": [
    {"id": "integer-above-safe", "inputText": "[9007199254740992]", "reason": "integer-domain"},
    {"id": "integer-below-safe", "inputText": "[-9007199254740992]", "reason": "integer-domain"},
    {"id": "nan-literal", "inputText": "[NaN]", "reason": "non-finite"},
    {"id": "infinity-literal", "inputText": "[Infinity]", "reason": "non-finite"},
    {"id": "duplicate-key", "inputText": "{\"a\":1,\"a\":2}", "reason": "not-json"},
    {"id": "lone-surrogate", "inputText": "[\"\\ud83d\"]", "reason": "not-json"},
    {"id": "truncated", "inputText": "{\"a\":", "reason": "not-json"},
    {"id": "two-values", "inputText": "{} {}", "reason": "not-json"}
  ]
}
```

After writing, add `"sha256"` to every vector: `python -I -` reading the file, computing `hashlib.sha256(v["canonical"].encode("utf-8")).hexdigest()`, rewriting with `json.dumps(indent=2, ensure_ascii=False) + "\n"`. (`NaN`/`Infinity` are not JSON: Jackson refuses them with `ALLOW_NON_NUMERIC_NUMBERS` off while Python's `json` accepts them by default, so each side's test states which layer refused: both refusals are forseti's, the reason is what the test pins. A lone surrogate is refused by Python's `rfc8785` (it cannot encode it) and by forseti's Java pre-check (`Canonical.check` refuses an unpaired surrogate in any string); the duplicate key is refused by both parsers with the strict mapper of Step 3.2 and `json.loads(..., object_pairs_hook=...)` in the oracle.)

- [ ] **Step 3.2** `Json.java` (one Jackson mapper for the whole core; strict, no non-numeric numbers, duplicate keys refused, big integers kept until the domain check, pretty writer in Tyr's shape):

```java
package yggdrasil.forseti.core;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.Separators;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

/** The one JSON mapper of forseti: strict parsing, no NaN/Infinity, duplicate keys refused, pretty output in the shape Tyr writes. */
public final class Json {
    public static final ObjectMapper JSON = new ObjectMapper()
        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
        .disable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)
        .disable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);   // "{} {}" is two values, not JSON; Python's json.loads refuses it too (Extra data)
    private Json() {}
    public static JsonNode read(byte[] b) {
        try { return JSON.readTree(b); }
        catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw new IllegalArgumentException("not-json: " + e.getOriginalMessage(), e); }
        catch (IOException e) { throw new IllegalArgumentException("not-json: " + e.getMessage(), e); }
    }
    public static JsonNode read(Path p) { try { return read(Files.readAllBytes(p)); } catch (IOException e) { throw new IllegalArgumentException(p + ": " + e.getMessage(), e); } }
    public static ObjectNode object() { return JSON.createObjectNode(); }
    public static byte[] pretty(JsonNode n) {
        DefaultIndenter two = new DefaultIndenter("  ", "\n");
        DefaultPrettyPrinter pp = new DefaultPrettyPrinter().withObjectIndenter(two).withArrayIndenter(two)
            .withSeparators(Separators.createDefaultInstance().withObjectFieldValueSpacing(Separators.Spacing.AFTER));
        try { return (JSON.writer(pp).writeValueAsString(n) + "\n").getBytes(StandardCharsets.UTF_8); } catch (IOException e) { throw new IllegalStateException(e); }
    }
    public static void write(Path p, JsonNode n) { try { Files.createDirectories(p.getParent()); Files.write(p, pretty(n)); } catch (IOException e) { throw new IllegalStateException(p.toString(), e); } }
    public static String text(JsonNode n, String k) { JsonNode v = n.path(k); if (!v.isTextual()) throw new IllegalArgumentException("missing text field " + k); return v.asText(); }
}
```

`Json` also carries the one integer policy of forseti: `public static long safeLong(JsonNode n, String field, long min, long max)` returns the value when `n` is an integral node whose `bigIntegerValue()` lies in `[min, max]` with `-9007199254740991 <= min <= max <= 9007199254740991` (`Canonical.MAX_SAFE`, the JCS domain), and throws `IllegalArgumentException("schema-invalid: " + field)` otherwise (a non-integral node, a value below `min`, `2^53` or anything larger, `Long.MAX_VALUE + 2` included, never reaches a `long` conversion). Three named ranges cover every integer forseti reads: `Json.NON_NEGATIVE = [0, MAX_SAFE]` for record offsets, counts, epochs, partitions and the business times of inputs (their schemas say `minimum 0`); `Json.THROUGH = [-1, MAX_SAFE]` for inclusive through-offsets, where `-1` means "no record of that partition included" (spec 406 defines `throughOffset` as the last business input offset *included*, so the inclusive convention is kept and `-1` is its empty value; Kafka's exclusive next-offset is not used); and `Json.ANY = [-MAX_SAFE, MAX_SAFE]` for computed times (`W1`, `W2`, `admissionW`, `eventTimeAt`, which the formulas may in principle drive below zero and which the spec never bounds). `public static ObjectNode putSafe(ObjectNode o, String field, long v)` writes a `long` and throws `IllegalStateException` when `|v| > 2^53-1`; per-field minima are the reader's (a forseti-written integer outside the safe range is a programming error, never a wire value). Every integer any forseti reader takes from a file goes through `safeLong`, every integer it writes through `putSafe`, so each schema range, each Java `long` and the JCS domain agree by construction.

- [ ] **Step 3.3** `Canonical.java`:

```java
package yggdrasil.forseti.core.canon;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Iterator;
import org.erdtman.jcs.JsonCanonicalizer;

/** RFC 8785 (JCS) bytes and SHA-256 of a JSON value (Decision 7). The pinned library is io.github.erdtman
 *  java-json-canonicalization 1.1; forseti refuses, before the library runs, what the two pinned libraries would not
 *  print identically: non-finite numbers, integers outside [-(2^53-1), 2^53-1], unpaired surrogates. The oracle applies
 *  the same refusals in its own code; the shared vector file pins both. digestOf(bytes) is the raw-file digest and is
 *  never mixed with digest(node), the canonical one. */
public final class Canonical {
    public static final BigInteger MAX_SAFE = BigInteger.valueOf(9007199254740991L);
    private Canonical() {}

    public static byte[] bytes(JsonNode value) {
        check(value);
        try { return new JsonCanonicalizer(Json.JSON.writeValueAsString(value)).getEncodedUTF8(); }
        catch (Exception e) { throw new IllegalArgumentException("not canonicalizable: " + e.getMessage(), e); }
    }
    public static String digest(JsonNode value) { return digestOf(bytes(value)); }
    public static String digestOf(byte[] b) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b)); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    static void check(JsonNode n) {
        if (n.isFloatingPointNumber()) { double d = n.doubleValue(); if (Double.isNaN(d) || Double.isInfinite(d)) throw new IllegalArgumentException("non-finite"); }
        else if (n.isIntegralNumber()) { BigInteger i = n.bigIntegerValue(); if (i.abs().compareTo(MAX_SAFE) > 0) throw new IllegalArgumentException("integer-domain"); }
        else if (n.isTextual()) { String s = n.textValue(); for (int i = 0; i < s.length(); i++) { char c = s.charAt(i);
            if (Character.isHighSurrogate(c)) { if (i + 1 >= s.length() || !Character.isLowSurrogate(s.charAt(i + 1))) throw new IllegalArgumentException("not-json: lone surrogate"); i++; }
            else if (Character.isLowSurrogate(c)) throw new IllegalArgumentException("not-json: lone surrogate"); } }
        else if (n.isObject()) { for (Iterator<String> it = n.fieldNames(); it.hasNext();) check(new com.fasterxml.jackson.databind.node.TextNode(it.next())); for (Iterator<JsonNode> it = n.elements(); it.hasNext();) check(it.next()); }
        else if (n.isArray()) { for (Iterator<JsonNode> it = n.elements(); it.hasNext();) check(it.next()); }
    }
    public static String utf8(byte[] b) { return new String(b, StandardCharsets.UTF_8); }
}
```

- [ ] **Step 3.4** `CanonicalVectorsTest.java` (red first: write it, run, see the compilation failure, then add the sources):

```java
package yggdrasil.forseti.core.canon;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import yggdrasil.forseti.core.Json;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

/** The shared JCS vector file (vectors/jcs-vectors.json) run by the Java side; the oracle runs the same file. */
class CanonicalVectorsTest {
    static final Path VECTORS = Path.of("..", "vectors", "jcs-vectors.json");
    static JsonNode file;
    @BeforeAll static void load() { file = Json.read(VECTORS); assertEquals(1, file.path("vectorsVersion").asInt()); assertEquals(9, file.path("vectors").size()); assertEquals(8, file.path("refusals").size()); }

    @Test void everyVectorCanonicalizesAndDigestsAsPinned() {
        for (JsonNode v : file.path("vectors")) {
            JsonNode in = Json.read(Json.text(v, "inputText").getBytes(StandardCharsets.UTF_8));
            byte[] c = Canonical.bytes(in);
            assertEquals(Json.text(v, "canonical"), Canonical.utf8(c), Json.text(v, "id"));
            assertEquals(Json.text(v, "sha256"), Canonical.digestOf(c), Json.text(v, "id"));
        }
    }
    @Test void everyRefusalVectorIsRefusedWithItsReason() {
        for (JsonNode r : file.path("refusals")) {
            String reason = Json.text(r, "reason"), text = Json.text(r, "inputText");
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Canonical.bytes(Json.read(text.getBytes(StandardCharsets.UTF_8))), Json.text(r, "id"));
            // NaN/Infinity are not JSON: Jackson refuses them at parse time (not-json), which is also forseti's refusal; the domain cases reach the pre-check.
            assertTrue(e.getMessage().startsWith(reason) || reason.equals("non-finite") && e.getMessage().startsWith("not-json"), Json.text(r, "id") + ": " + e.getMessage());
        }
    }
    @Test void aNaNPlacedInATreeIsRefusedBeforeTheLibrary() {
        assertEquals("non-finite", assertThrows(IllegalArgumentException.class, () -> Canonical.bytes(new DoubleNode(Double.NaN))).getMessage());
        assertEquals("non-finite", assertThrows(IllegalArgumentException.class, () -> Canonical.bytes(new DoubleNode(Double.POSITIVE_INFINITY))).getMessage());
    }
    @Test void digestIsSha256OfTheCanonicalUtf8Bytes() {
        assertEquals("44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a", Canonical.digest(Json.object()));   // sha256("{}")
    }
}
```

- [ ] **Step 3.5** `mvn -q -pl core -Dtest=CanonicalVectorsTest test`: before 3.2-3.3 a compilation error; after, `Tests run: 4, Failures: 0`. The `doubles-es6` input writes `1.2345678901234568e20` in exponent form on purpose: both parsers read it as a double, and RFC 8785 prints an integer-valued double below 1e21 without an exponent (`123456789012345680000`); written as a plain integer literal the same digits would parse as an integer above 2^53 and be refused by the domain check (that case is the `integer-above-safe` refusal vector). The `canonical` strings of `key-order-utf16` and `strings-escapes` are JSON strings whose escapes decode to the real characters JCS emits (`é`, the emoji, U+007F, U+2028, the combining accent) and to the literal escapes JCS keeps (`\u0000`, `\u001f`, `\b\f\n\r\t`, `\"`, `\\`).

- [ ] **Step 3.6** `oracle/src/forseti_oracle/canon.py`:

```python
"""RFC 8785 bytes and SHA-256 of a JSON value, with forseti's refusals applied before the library (rfc8785 0.1.4)."""
from __future__ import annotations

import hashlib
import json
import math

import rfc8785

MAX_SAFE = 9007199254740991


class CanonicalRefusal(ValueError):
    pass


def _no_duplicates(pairs):
    d = {}
    for k, v in pairs:
        if k in d:
            raise CanonicalRefusal("not-json: duplicate key")
        d[k] = v
    return d


def loads(text: str):
    """The oracle's strict parser: duplicate keys refused (Python's json keeps the last one by default)."""
    try:
        return json.loads(text, object_pairs_hook=_no_duplicates)
    except json.JSONDecodeError as e:
        raise CanonicalRefusal(f"not-json: {e.msg}") from e


def _check(v) -> None:
    if isinstance(v, bool):
        return
    if isinstance(v, int):
        if abs(v) > MAX_SAFE:
            raise CanonicalRefusal("integer-domain")
    elif isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            raise CanonicalRefusal("non-finite")
    elif isinstance(v, str):
        try:
            v.encode("utf-8")
        except UnicodeEncodeError:
            raise CanonicalRefusal("not-json: lone surrogate")
    elif isinstance(v, dict):
        for k, x in v.items():
            _check(k)
            _check(x)
    elif isinstance(v, list):
        for x in v:
            _check(x)


def canonical_bytes(value) -> bytes:
    _check(value)
    return rfc8785.dumps(value)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest(value) -> str:
    return sha256(canonical_bytes(value))
```

- [ ] **Step 3.7** `oracle/tests/test_jcs_vectors.py`:

```python
import json
from pathlib import Path

import pytest

from forseti_oracle.canon import CanonicalRefusal, canonical_bytes, loads, sha256

VECTORS = json.loads((Path(__file__).resolve().parents[2] / "vectors" / "jcs-vectors.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("v", VECTORS["vectors"], ids=[v["id"] for v in VECTORS["vectors"]])
def test_vector(v):
    c = canonical_bytes(loads(v["inputText"]))
    assert c.decode("utf-8") == v["canonical"]
    assert sha256(c) == v["sha256"]


@pytest.mark.parametrize("r", VECTORS["refusals"], ids=[r["id"] for r in VECTORS["refusals"]])
def test_refusal(r):
    # Python's json accepts NaN/Infinity literals; forseti's pre-check refuses them with the pinned reason.
    with pytest.raises(CanonicalRefusal, match=r["reason"]):
        canonical_bytes(loads(r["inputText"]))


def test_vector_count_matches_java_side():
    assert len(VECTORS["vectors"]) == 9 and len(VECTORS["refusals"]) == 8
```

- [ ] **Step 3.8** `cd oracle && python -m pytest -q` -> `18 passed`. Commit `feat(core,oracle): RFC 8785 canonical digest with the shared vector file`.

### Task 4: Input schemas, input types, identities, payload digests

**Files:**
- Exists (committed by the amendment PR of 2026-11-11, unchanged here): `schemas/occupancy-1.0.0.schema.json`, `schemas/incident-1.0.0.schema.json`
- Create: byte copies of the two under `core/src/main/resources/forseti/schemas/`
- Create: `core/src/main/java/yggdrasil/forseti/core/input/{Inputs,Ids,InputParser,StatusCodes}.java`
- Create: `core/src/test/java/yggdrasil/forseti/core/input/{SchemaCopiesTest,InputParserTest,IdsTest}.java`
- Create: `oracle/src/forseti_oracle/inputs.py`, `oracle/tests/test_inputs.py`

- [ ] **Step 4.1** Copy the two committed schema files byte for byte into `core/src/main/resources/forseti/schemas/`; neither root file is created or changed here (the amendment PR of 2026-11-11 committed them so that Tyr's profiles could cite their digests, and `sha256sum schemas/*.json` must still print the two digests below, because Tyr's and Bifrost's merged fixtures carry them). `schemas/occupancy-1.0.0.schema.json`, raw-byte SHA-256 `e97cd7c97b1ab5e2d63d5b06e6057415945b82cff6d2ca0345106d2daf9d29dc` (current spec 284-298; EPCIS field names; the spec's optionality kept exactly: every listed field is required unless the spec marks it otherwise (current spec 119), so `recordTime` (diagnostic) and `visitSeq` (correlation) are required; `arrivalEventId` is "departing events only", so it is required on a departure and must be absent on an arrival; `errorDeclaration` is the one optional field; no additional properties):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:forseti:schema:occupancy:1.0.0",
  "title": "forseti occupancy event 1.0.0 (EPCIS 2.0 ObjectEvent field names, no EPCIS capture)",
  "type": "object",
  "properties": {
    "eventId": {"type": "string", "minLength": 1, "maxLength": 256},
    "eventTime": {"type": "integer", "minimum": 0, "maximum": 9007199254740991},
    "recordTime": {"type": "integer", "minimum": 0, "maximum": 9007199254740991},
    "unitId": {"type": "string", "minLength": 1, "maxLength": 256},
    "action": {"const": "OBSERVE"},
    "bizStep": {"enum": ["urn:epcglobal:cbv:bizstep:arriving", "urn:epcglobal:cbv:bizstep:departing"]},
    "readPoint": {"type": "string", "pattern": "^[a-z][a-z0-9._/-]*(:[a-z][a-z0-9._/-]*)*$", "maxLength": 4096},
    "visitSeq": {"type": "integer", "minimum": 0, "maximum": 9007199254740991},
    "arrivalEventId": {"type": "string", "minLength": 1, "maxLength": 256},
    "source": {"type": "string", "minLength": 1, "maxLength": 256},
    "errorDeclaration": {
      "type": "object",
      "properties": {
        "declarationTime": {"type": "integer", "minimum": 0, "maximum": 9007199254740991},
        "reason": {"enum": ["urn:epcglobal:cbv:er:did_not_occur", "urn:epcglobal:cbv:er:incorrect_data"]},
        "correctiveEventIDs": {"type": "array", "items": {"type": "string", "minLength": 1}, "maxItems": 64}
      },
      "required": ["declarationTime", "reason", "correctiveEventIDs"],
      "additionalProperties": false
    }
  },
  "required": ["eventId", "eventTime", "recordTime", "unitId", "action", "bizStep", "readPoint", "visitSeq", "source"],
  "additionalProperties": false,
  "allOf": [
    {"if": {"properties": {"bizStep": {"const": "urn:epcglobal:cbv:bizstep:departing"}}}, "then": {"required": ["arrivalEventId"]}},
    {"if": {"properties": {"bizStep": {"const": "urn:epcglobal:cbv:bizstep:arriving"}}}, "then": {"not": {"required": ["arrivalEventId"]}}}
  ]
}
```

`schemas/incident-1.0.0.schema.json`, raw-byte SHA-256 `7141c3115d90812f16b22049c66bfaa64942ebacf854b343fb99b3dce168fb34` (current spec 304-317):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:forseti:schema:incident:1.0.0",
  "title": "forseti quality incident 1.0.0",
  "type": "object",
  "properties": {
    "incidentId": {"type": "string", "minLength": 1, "maxLength": 256},
    "revision": {"type": "integer", "minimum": 1, "maximum": 9007199254740991},
    "stationRef": {"type": "string", "pattern": "^[a-z][a-z0-9._/-]*(:[a-z][a-z0-9._/-]*)*$", "maxLength": 4096},
    "equipmentRef": {"type": "string", "pattern": "^[a-z][a-z0-9._/-]*(:[a-z][a-z0-9._/-]*)*$", "maxLength": 4096},
    "kind": {"enum": ["tool-check-failed", "gauge-drift"]},
    "detectedAt": {"type": "integer", "minimum": 0, "maximum": 9007199254740991},
    "revisionEventTime": {"type": "integer", "minimum": 0, "maximum": 9007199254740991},
    "suspectFrom": {"type": "integer", "minimum": 0, "maximum": 9007199254740991},
    "suspectTo": {"type": ["integer", "null"], "minimum": 0, "maximum": 9007199254740991},
    "status": {"enum": ["open", "closed", "withdrawn"]},
    "evidenceRef": {"type": "string", "minLength": 1, "maxLength": 2048},
    "source": {"type": "string", "minLength": 1, "maxLength": 256}
  },
  "required": ["incidentId", "revision", "stationRef", "equipmentRef", "kind", "detectedAt", "revisionEventTime", "suspectFrom", "suspectTo", "status", "evidenceRef", "source"],
  "additionalProperties": false,
  "allOf": [
    {"if": {"properties": {"status": {"const": "closed"}}}, "then": {"properties": {"suspectTo": {"type": "integer"}}}}
  ]
}
```

Write both pretty (two-space indent, trailing newline), copy byte for byte to `core/src/main/resources/forseti/schemas/`. Record their digests: `sha256sum schemas/*.json` -> two 64-hex values used by Task 6's fixture script and cited in the README. `SchemaCopiesTest` asserts the resource bytes equal `../schemas/*.json` when that directory exists (never absent in the repository build).

- [ ] **Step 4.2** `Inputs.java` (records; every input carries its `Origin`):

```java
package yggdrasil.forseti.core.input;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public final class Inputs {
    private Inputs() {}
    public record Origin(String topic, int partition, long offset) {}
    /** One Kafka record as delivered: raw value, origin, parsed view (null when schema-invalid), payload digest. */
    public record InputRecord(Origin origin, String key, byte[] rawValue, JsonNode value, Object parsed, Ids.Digest payloadDigest) {}
    public record Member(String metric, JsonNode value, Long uaStatusCode, Long quality, Long sourceTs, Long serverTs) {}   // every integer a Long read through Json.safeLong
    public record ProcessMessage(String edge, String device, long birthPublishTs, long bdSeq, long seq, long publishTs, Long bridgeReceivedAt,
                                 String definitionSha256, List<Member> members) {}
    public record SessionEvent(String edge, String device, long birthPublishTs, long bdSeq, String kind, long atSeq, JsonNode payload) {}
    public record OccupancyEvent(String eventId, long eventTime, long recordTime, String unitId, String bizStep, String readPoint, Long visitSeq,
                                 String arrivalEventId, String source, JsonNode errorDeclaration) {   // visitSeq is Long: the schema admits any safe integer, 2147483648 included
        public boolean arriving() { return bizStep.equals("urn:epcglobal:cbv:bizstep:arriving"); }
    }
    public record IncidentEvent(String incidentId, long revision, String stationRef, String equipmentRef, String kind, long detectedAt, long revisionEventTime,
                                long suspectFrom, Long suspectTo, String status, String evidenceRef, String source) {}
}
```

- [ ] **Step 4.3** `Ids.java` (identities as canonical bytes, spec 261, 267, 282, 300, 232-233):

```java
package yggdrasil.forseti.core.input;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** Semantic identities (spec Input events). Each is an ObjectNode in contract field order; `bytes` is its JCS form, used for
 *  ordering (spec 491) and as the ledger `identity`. `payloadDigest` is the JCS SHA-256 of the record value without the
 *  diagnostic fields recordTime and bridgeReceivedAt (spec 232). */
public final class Ids {
    private Ids() {}
    public static ObjectNode sampleId(Inputs.ProcessMessage m, String metric) {
        return Json.object().put("edge", m.edge()).put("device", m.device()).put("birthPublishTs", m.birthPublishTs()).put("metric", metric)
            .put("bdSeq", m.bdSeq()).put("seq", m.seq()).put("publishTs", m.publishTs());
    }
    public static ObjectNode messageId(Inputs.ProcessMessage m) {   // the whole-message delivery identity: sampleId without metric
        return Json.object().put("edge", m.edge()).put("device", m.device()).put("birthPublishTs", m.birthPublishTs()).put("bdSeq", m.bdSeq()).put("seq", m.seq()).put("publishTs", m.publishTs());
    }
    public static ObjectNode measurementId(String device, String metric, long sourceTs, JsonNode value, Long uaStatusCode) {
        ObjectNode n = Json.object().put("device", device).put("metric", metric).put("sourceTs", sourceTs); n.set("value", value);
        if (uaStatusCode == null) n.putNull("uaStatusCode"); else n.put("uaStatusCode", uaStatusCode); return n;
    }
    public static ObjectNode sessionEventId(Inputs.SessionEvent s) {
        return Json.object().put("edge", s.edge()).put("device", s.device()).put("birthPublishTs", s.birthPublishTs()).put("bdSeq", s.bdSeq()).put("kind", s.kind()).put("atSeq", s.atSeq());
    }
    public static ObjectNode occupancyId(Inputs.OccupancyEvent o) { return Json.object().put("source", o.source()).put("eventId", o.eventId()); }
    public static ObjectNode incidentId(Inputs.IncidentEvent i) { return Json.object().put("source", i.source()).put("incidentId", i.incidentId()).put("revision", i.revision()); }
    public static byte[] bytes(JsonNode identity) { return Canonical.bytes(identity); }
    /** JCS SHA-256 of the record value without the diagnostic fields recordTime and bridgeReceivedAt and without each member's lossy
     *  `quality` (spec 232, 261): two deliveries that differ only there are one redelivery, never a conflict. */
    /** The ledger's payloadDigest with the rule that produced it: "jcs" for a canonicalizable object value, "raw-bytes" for an input
     *  that cannot be canonicalized (Task 1's insertion after spec line 440), which is rejected schema-invalid but still gets its row. */
    public record Digest(String sha256, String rule) {}
    public static Digest payloadDigest(JsonNode value, byte[] rawValue) {
        if (value != null && value.isObject()) {
            ObjectNode v = value.deepCopy(); v.remove("recordTime"); v.remove("bridgeReceivedAt");
            if (v.path("members").isArray()) for (JsonNode m : v.path("members")) if (m.isObject()) ((ObjectNode) m).remove("quality");
            try { return new Digest(Canonical.digest(v), "jcs"); } catch (IllegalArgumentException e) { /* integer-domain, non-finite, lone surrogate */ }
        }
        return new Digest(Canonical.digestOf(rawValue), "raw-bytes");
    }
}
```

- [ ] **Step 4.4** `StatusCodes.java`: `static boolean bad(Long code) { return code != null && (code & 0xC0000000L) == 0x80000000L; }`, `uncertain` = `0x40000000L`, `good` otherwise (spec 137: raw severity bits; a null status code is neither Bad nor Uncertain).

- [ ] **Step 4.5** `InputParser.java`: `parse(String topic, JsonNode value) -> Object` for the four input kinds; a process member's numeric `value` is normalized to a `double` (`node.doubleValue()`, so integer, fractional and exponent notations of one number are one value; the oracle does `float(v)` and, because Python raises `OverflowError: int too large to convert to float` for an integer literal beyond the double range while `json.loads("1e400")` already yields `inf`, catches `OverflowError` and substitutes `math.inf if v >= 0 else -math.inf` (not `math.copysign(math.inf, v)`, whose second argument is itself converted to a double and raises the same `OverflowError` for such an `int`); on the Java side such a literal is a `BigIntegerNode` and `BigInteger.doubleValue()` returns `Double.POSITIVE_INFINITY` or `NEGATIVE_INFINITY` for a magnitude too great for a double, so both sides reach the same signed infinity and the sample is `non-finite`), a Boolean or String value kept as is; occupancy and incidents validated against the built-in schemas with networknt (`SpecVersion.VersionFlag.V202012`; a non-empty validation set -> `IllegalArgumentException("schema-invalid")`); process messages and session events structurally (required fields and types; `members[]` entries with optional `sourceTs`; a message with no member `sourceTs` is not refused here: that is a disposition, spec 261); every integer field of every input kind is read with `Json.safeLong` against the same range the schemas state, `0 .. 9007199254740991` (`uaStatusCode` additionally `<= 4294967295`, a 32-bit status code): occupancy `eventTime`, `recordTime`, `visitSeq`, `errorDeclaration.declarationTime`; incident `revision` (`>= 1`), `detectedAt`, `revisionEventTime`, `suspectFrom`, `suspectTo`; process message `birthPublishTs`, `bdSeq`, `seq`, `publishTs`, `bridgeReceivedAt`, member `uaStatusCode`, `quality`, `sourceTs`, `serverTs`; session event `birthPublishTs`, `bdSeq`, `atSeq` and the payload fields `from`, `to`, `missing`, `at`, `detectedEventTime`, `rawPublishTs`, `bdSeq`; an out-of-range or non-integral value is `rejected schema-invalid` (`refs.check <field>`) with the raw-bytes digest of Task 1's line-440 insertion when the value is also not canonicalizable, and processing continues (the Sparkplug 0-255 range of `seq`/`bdSeq` is the bridge's concern, step 5, not a parser bound here). Exposes `schemaSha256(String schemaId)` over the resource bytes and `incidentKinds()` (the schema's `kind` enum). `InputParserTest`: a valid arrival without `arrivalEventId` and without `errorDeclaration` parses; `visitSeq 2147483648` parses to the `Long` 2147483648; `integerRangeBoundariesPerInputKind` (one boundary case per input kind, on `visitSeq`, incident `revision`, process-message `seq` and session-event `atSeq`: `9007199254740991` accepted, `9007199254740992` and `18446744073709551617` (above `Long.MAX_VALUE`) each `rejected schema-invalid` with `refs.check` naming the field, no exception, the next record processed); an arrival without `recordTime` or without `visitSeq` is `schema-invalid`; a departing event without `arrivalEventId` is `schema-invalid`; an arriving event carrying `arrivalEventId` is `schema-invalid`; an incident `closed` with `suspectTo` null is `schema-invalid`; an unknown extra field is `schema-invalid`; `schemaSha256("urn:forseti:schema:occupancy:1.0.0")` equals `sha256sum schemas/occupancy-1.0.0.schema.json`. `IdsTest`: `payloadDigest` ignores `recordTime`, `bridgeReceivedAt` and every member's `quality` and nothing else (two process messages differing only in a member's `quality` share the digest; a changed `value` does not; pins the literal digest of the smoke recording's first occupancy record of Task 8); `aNonCanonicalizableValueGetsARawBytesDigest` (`[9007199254740992]`, `{"eventTime": 9007199254740992, ...}` and `"text"` give `rule raw-bytes` with the SHA-256 of the delivered bytes, no exception); the harness test of Task 11 then shows such a record `rejected schema-invalid` with `refs.digestRule raw-bytes` and the run continuing; sample ids of two members differ only in `metric`; identity bytes order `{"a":..}` before `{"b":..}`.

- [ ] **Step 4.6** Oracle `inputs.py`: the same identities and `payload_digest` written independently (dicts in contract field order, `canonical_bytes`), schema validation with `jsonschema.Draft202012Validator` over `schemas/*.json` given by path, `is_bad(code)`. `oracle/tests/test_inputs.py`: the same cases as `InputParserTest` plus `test_payload_digest_equals_java_pin` (the same literal `IdsTest` pins), `test_payload_digest_ignores_member_quality`, `test_visit_seq_above_int32_is_preserved` (2147483648 parsed and kept), `test_integer_range_boundaries_per_input_kind` (the same four fields and three values as the Java test: Python's unbounded `int` is bounded by the oracle's own `safe_int(v, field, lo, hi)` to `0 .. 2^53-1`, so `18446744073709551617` is `schema-invalid`, never a silent big integer) and `test_non_canonicalizable_value_gets_raw_bytes_digest` (the same three inputs, `rule raw-bytes`, digest of the delivered bytes) and, in `test_stage1.py`, `test_mixed_message_keeps_its_good_members` (the same `1e400` message: `Running` processed, `Torque` row `non-finite`, `observedPeriods 59`, `missingPeriods 1`, `count 59`, two `accepted` dispositions) and `test_three_notations_are_one_measurement`, `test_overflowing_integer_and_exponent_literals_are_non_finite` (`1e400`, `-1e400`, `1` followed by 400 zeros and its negative, each as a `Torque` value beside `Running true`: `Torque` row `non-finite`, `Running` processed, both dispositions `accepted`, no exception), so the two sides agree on which bytes identify an input.

- [ ] **Step 4.7** `mvn -q -pl core -Dtest='SchemaCopiesTest,InputParserTest,IdsTest' test` -> `Tests run: 14, Failures: 0`; `pytest -q oracle` -> `28 passed`. Commit `feat(core,oracle): input schemas 1.0.0, input types, identities and payload digests`.

### Task 5: Journal types: envelope, rows, deletes, progress, dispositions, revisions, fold

**Files:**
- Create: `core/src/main/java/yggdrasil/forseti/core/journal/{Envelope,Row,Delete,Pins,Progress,Disposition,Revisions,JournalFold}.java`
- Create: `core/src/test/java/yggdrasil/forseti/core/journal/{RevisionsTest,DispositionsTest,JournalFoldTest}.java`

- [ ] **Step 5.1** Types (every integer field of every record below, `revision`, `eventTimeAt`, `trigger.offset`, `cutId`, `producerEpoch`, `W1`, `W2`, `admissionW`, `restartEpoch`, `barrierRefs[].throughOffset`, `incidentOffsets[].throughOffset`, `initEvidence.declarationOffset`/`endOffset`, `inputPartition`, `inputOffset`, `generation`, is written with `Json.putSafe` and read back with `Json.safeLong`, so progress records, ledger rows, `cuts.json` and the journals never carry an integer outside the JCS domain; the ranges on reading: `barrierRefs[].throughOffset`, `incidentOffsets[].throughOffset` and the recording `throughOffsets` are `Json.THROUGH` (`-1` = no record of that partition included yet, the inclusive convention of spec 406 and 434; a cut with no incident writes six `incidentOffsets` entries with `throughOffset -1`, and a `barrierRefs` entry likewise), `W1`, `W2`, `admissionW` (nullable before the first progress) and `eventTimeAt` are `Json.ANY`, everything else `Json.NON_NEGATIVE`; no other sweep field has a sentinel: `producerEpoch` and `restartEpoch` start at 0 (spec 406), `initEvidence.endOffset >= 1`, and the `-1` of the `W1` formula is arithmetic, not a stored sentinel). `Pins` (`rules, hierarchy, bindings, definitions, job {engine, engineVersion, artifactSha256, runId, timestampSource, idleness, routingDigest}`, spec 329), `Envelope` (`evaluationGeneration, revision, supersedesDigest, cause, eventTimeAt, trigger {topic, partition, offset}, basisRevisions, pins`, spec 321-329; `cause` is `{"kind": "input"|"timer"|"replayed", ...}` per Task 1's insertion at line 553), `Row(String topic, JsonNode logicalKey, JsonNode body, String bodyDigest, Envelope envelope)`, `Delete(generation, logicalKey, revision, supersedesDigest)` (spec 331), `Progress.Stage1` / `Stage2` / `Ledger` records with `toJson()` in the exact field lists of spec 412-452 (`initEvidence` fields per Task 1's line 424), `Disposition(key {generation, inputTopic, inputPartition, inputOffset, part}, body {inputKind, identity, payloadDigest, disposition, reason, refs}, revision)` with the closed vocabulary of spec 445-450 as enums (plus the `refs.check` values of Task 1's line 449).

- [ ] **Step 5.2** `Revisions` (per logical key `(nextRevision, lastDigest)`; `next(key, bodyDigest) -> Optional<(revision, supersedesDigest)>` empty when the digest equals the last emitted one; `delete(key) -> (revision, supersedesDigest)` and a later `next` continues the numbering; per generation). `JournalFold` reads one generation's records of a partition up to a progress record, keeps the highest revision per key, drops deleted keys, ignores progress, and returns `Violations` {revision gaps, equal consecutive digests, a delete whose `supersedesDigest` is not the last digest, a business row after its cut's progress}.

- [ ] **Step 5.3** Tests (red first): `RevisionsTest` (first revision 1; changed body revision 2 with `supersedesDigest`; unchanged body emits nothing; an envelope-only change emits nothing; delete continuity; reappearance after delete continues at the next number; another generation starts at 1). `DispositionsTest` (a delivery key with `part`, a later `conflict` revision under the same key, `member:<metric>` parts for one message). `JournalFoldTest` (fold with two keys and a delete; the four violations each detected).

- [ ] **Step 5.4** `mvn -q -pl core -Dtest='RevisionsTest,DispositionsTest,JournalFoldTest' test` -> `Tests run: 13, Failures: 0`. Commit `feat(core): journal envelope, rows, deletes, progress and disposition types; revision emission and fold`.

### Task 6: Release reader at Vor's level, Tyr's bundles published byte for byte, forseti's re-pinned registry variants

**Files:**
- Create: `core/src/main/java/yggdrasil/forseti/core/release/{Refusal,TrustedKeys,FloorStore,Dsse,HeadVerifier,ReleaseReader}.java`
- Create: `scripts/make_registry_fixture.py`
- Create: `fixtures/upstream/{bundle,plc-stamped-bundle}/**`, `fixtures/upstream/manifest.json`, `fixtures/registry/**`, `fixtures/registry-next/**`, `fixtures/registry-mismatch/**`, `fixtures/trusted-keys.jsonl`, `fixtures/registry.lock.json`
- Create: `core/src/test/java/yggdrasil/forseti/core/release/{HeadVerifierTest,ReleaseReaderTest,BifrostSeamTest,TestKeys}.java`

Fixture arrangement (Amendment 2026-11-11, replacing Q3): Tyr's two re-authored bundles are copied byte for byte to `fixtures/upstream/` with a manifest of every file digest and the Tyr head; they are never modified, and they are what forseti publishes and executes: their two profile files already carry the real digests of Task 4 as `format.schemaSha256` and the incident `topic` `quality.incidents` (current spec 472), and the plc bundle's submission already names stream `line2-plc`. The script refuses to run when any of these does not hold, then approves and publishes the two bundles with Bifrost's CLI without changing a byte: `line2-rules 1.0.0` (occupancy-join) on stream `line2` and `line2-rules-plc 1.0.0` (plc-stamped, `crossCheck: occupancy` as Tyr authored it) on stream `line2-plc`, into `fixtures/registry`. Three variants are forseti-derived copies, each edited as listed and re-pinned (evidence sha, proposal, submission) exactly as the step-1 plan's `pin-fixture.py` does before Bifrost approves and publishes it: `line2-rules-plc-nocheck 1.0.0` (the plc bundle with the attribution rule at `1.0.1` and `identity.crossCheck` `none`, the impact rule referencing `1.0.1`, the submission on stream `line2-plc-nocheck`; the pinned rule both runs of injection 11b use, current spec 650) into `fixtures/registry`; `line2-rules 1.0.1` (attribution rule 1.0.1 with `coverage.maxMissingPeriods` 4, impact rule 1.0.1 referencing it) on stream `line2` into `fixtures/registry-next` (head version 2, `previous` names version 1; for injection 20); and `line2-rules 1.0.0` on stream `line2` into `fixtures/registry-mismatch`, Tyr's `bundle` with one value changed, the occupancy profile's `format.schemaSha256` set to `0000000000000000000000000000000000000000000000000000000000000001`. Bifrost checks the form of that digest and forseti its equality (current spec 206), so Bifrost approves and publishes the copy and the `binding.schema-mismatch` refusal is reached only after a verified head and release, as at a real start-up; the trust path head -> `release.json` -> payload is the one every other fixture uses, and no refused bundle is committed in Tyr or Bifrost. A second release on a stream carries the current head's `{id, version, sha256}` as `baseRelease` in both `proposal.json` and `submission.json` (Bifrost's `submission.base-reference` and `mapping.base-conflict` checks), set before the proposal digest is re-pinned. Private keys live in a temporary directory and are deleted; the public keys go to `fixtures/trusted-keys.jsonl`, outside the registries (a registry cannot vouch for itself). Heads are published with `--expires-in P366D`; every test passes `nowMs`, never the wall clock.

- [ ] **Step 6.1** `scripts/make_registry_fixture.py` (run with `python -I scripts/make_registry_fixture.py --bifrost ../bifrost --tyr ../tyr/fixtures/attribution-rule`; the form of Vor's `make_fixtures.py`):

```python
"""Publish forseti's fixture registries with Bifrost's own CLI from Tyr's committed bundles.

Tyr's bundles are copied untouched to fixtures/upstream and published byte for byte: their profiles carry the real
schema digests of schemas/*.json and the incident topic quality.incidents, and the plc bundle names stream line2-plc
(checked before anything is published). Three variants are forseti-derived copies, edited as listed and re-pinned
(evidence sha256, proposal sha256 list, submission digests):
  registry:          line2-rules 1.0.0 on stream line2 and line2-rules-plc 1.0.0 on stream line2-plc (Tyr's bytes);
                     line2-rules-plc-nocheck 1.0.0 on stream line2-plc-nocheck (variant: crossCheck none)
  registry-next:     the same plus line2-rules 1.0.1 (variant: maxMissingPeriods 4) on stream line2 (head version 2)
  registry-mismatch: line2-rules 1.0.0 on stream line2 (variant: another occupancy format.schemaSha256, the
                     binding.schema-mismatch fixture)
Needs <bifrost>/gates/target/bifrost-gates.jar. Private keys are deleted; public keys go to fixtures/trusted-keys.jsonl.
"""
from __future__ import annotations

import argparse, hashlib, json, shutil, subprocess, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STREAMS = {"bundle": ("line2", "line2-rules"), "plc-stamped-bundle": ("line2-plc", "line2-rules-plc")}
MISMATCH_SHA = "0" * 63 + "1"   # a well-formed digest that is not the occupancy schema's: Bifrost checks the form, forseti the equality


def sha(b: bytes) -> str: return hashlib.sha256(b).hexdigest()
def load(p: Path): return json.loads(p.read_bytes())
def dump(p: Path, d) -> None: p.write_bytes((json.dumps(d, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))
def git_head(p: Path) -> str: return subprocess.run(["git", "-C", str(p), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
def inventory(root: Path) -> dict: return {str(p.relative_to(root)).replace("\\", "/"): sha(p.read_bytes()) for p in sorted(q for q in root.rglob("*") if q.is_file())}


def repin(bundle: Path) -> None:
    p = load(bundle / "proposal.json")
    for f in p["files"]:
        if f["kind"] == "evidence-artifact":
            e = load(bundle / f["path"]); e["file"]["sha256"] = sha((bundle / e["file"]["path"]).read_bytes()); dump(bundle / f["path"], e)
    for f in p["files"]:
        d = load(bundle / f["path"]); f["id"], f["version"] = d["id"], d["version"]; f["sha256"] = sha((bundle / f["path"]).read_bytes())
    dump(bundle / "proposal.json", p)
    s = load(bundle / "submission.json"); s["proposalSha256"] = s["workspaceSha256"] = sha((bundle / "proposal.json").read_bytes()); dump(bundle / "submission.json", s)


def check_upstream(bundle: Path, stream: str, occupancy_sha: str, incident_sha: str) -> None:
    occ = load(bundle / "data" / "occupancy-binding-0.json"); inc = load(bundle / "data" / "incident-binding-0.json")
    found = (occ["format"]["schemaSha256"], inc["format"]["schemaSha256"], inc["topic"], load(bundle / "submission.json")["streamId"])
    if found != (occupancy_sha, incident_sha, "quality.incidents", stream):
        raise SystemExit(f"{bundle}: Tyr's bundle must carry the real schema digests, quality.incidents and stream {stream}; found {found}")

def bump_rules(bundle: Path) -> None:
    a = bundle / "attribution-rules" / "0.json"; d = load(a); d["version"] = "1.0.1"; d["coverage"]["maxMissingPeriods"] = 4; dump(a, d)
    i = bundle / "impact-rules" / "0.json"; d = load(i); d["version"] = "1.0.1"; d["attributionRuleRef"]["version"] = "1.0.1"; dump(i, d)


def mismatch(bundle: Path) -> None:
    occ = bundle / "data" / "occupancy-binding-0.json"; d = load(occ); d["format"]["schemaSha256"] = MISMATCH_SHA; dump(occ, d)


class Gates:
    def __init__(self, jar: Path): self.jar = jar
    def __call__(self, *args: str) -> None:
        r = subprocess.run(["java", "-jar", str(self.jar), *args], capture_output=True, text=True)
        if r.returncode != 0:
            raise SystemExit(f"gates {' '.join(args[:2])} failed ({r.returncode}): {r.stdout}{r.stderr}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bifrost", type=Path, default=ROOT.parent / "bifrost")
    ap.add_argument("--tyr", type=Path, default=ROOT.parent / "tyr" / "fixtures" / "attribution-rule")
    ap.add_argument("--out", type=Path, default=ROOT / "fixtures")
    a = ap.parse_args()
    gates = Gates(a.bifrost / "gates" / "target" / "bifrost-gates.jar")
    occupancy_sha = sha((ROOT / "schemas" / "occupancy-1.0.0.schema.json").read_bytes())
    incident_sha = sha((ROOT / "schemas" / "incident-1.0.0.schema.json").read_bytes())
    upstream = a.out / "upstream"
    if upstream.exists(): shutil.rmtree(upstream)
    for name, (stream, _) in STREAMS.items():
        shutil.copytree(a.tyr / name, upstream / name); check_upstream(upstream / name, stream, occupancy_sha, incident_sha)
    dump(upstream / "manifest.json", {"tyr": git_head(a.tyr), "files": inventory(upstream)})
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp); keys = tmp / "keys"; reg = tmp / "registry"; (reg / "identity").mkdir(parents=True)
        lines = []
        for principal in ("proposer", "approver"):
            gates("identity", "keygen", principal, "--out", str(keys))
            lines.append(json.dumps({"principal": principal, "publicKey": (keys / f"{principal}.pub").read_text().strip()}))
        (reg / "identity" / "authorized-keys.jsonl").write_text("\n".join(lines) + "\n", newline="\n")
        rules = []
        for stream in ("line2", "line2-plc", "line2-plc-nocheck"):
            rules += [{"id": f"publish-{stream}", "principal": "proposer", "action": "publish", "target": "mapping-publication", "kind": "mapping-release", "ref": stream},
                      {"id": f"approve-{stream}", "principal": "approver", "action": "approve", "target": "mapping-publication", "kind": "mapping-release", "ref": stream}]
        dump(reg / "identity" / "activation-policy.json", {"version": "1.0.0", "default": "deny", "rules": rules})
        signers = ["--by", "proposer", "--by-key", str(keys / "proposer.key"), "--approved-by", "approver", "--approved-by-key", str(keys / "approver.key")]

        def publish(reg: Path, name: str, stream: str, release: str, version: str, edit=None) -> None:
            bundle = tmp / f"{reg.name}-{release}-{version}"; shutil.copytree(a.tyr / name, bundle)   # unique per release: the plc bundle is published twice (line2-plc and line2-plc-nocheck)
            if edit:   # a forseti-derived variant; without an edit Tyr's bytes are published exactly as copied
                edit(bundle)
                # A second release on a stream must name the current head exactly ({id, version, sha256}) in BOTH
                # proposal.json and submission.json (MappingBundle: submission.base-reference; MappingRegistry.base:
                # mapping.base-conflict compares the whole head object), before the proposal digest is re-pinned.
                head = reg / "mappings" / "heads" / f"{stream}.json"
                base = load(head) if head.exists() else None
                p = load(bundle / "proposal.json"); p["baseRelease"] = base; dump(bundle / "proposal.json", p)
                s = load(bundle / "submission.json"); s["baseRelease"] = base; s["streamId"] = stream; s["version"] = version; dump(bundle / "submission.json", s)
                repin(bundle)
            gates("mapping", "validate", str(bundle))
            gates("mapping", "approve", str(reg), str(bundle), f"approval-{release}-{version}", *signers)
            gates("mapping", "publish", str(reg), str(bundle), release, version, "--approval", f"approval-{release}-{version}", *signers, "--expires-in", "P366D")

        def nocheck(bundle: Path) -> None:
            r = bundle / "attribution-rules" / "0.json"; d = load(r); d["version"] = "1.0.1"; d["identity"]["crossCheck"] = "none"; dump(r, d)
            i = bundle / "impact-rules" / "0.json"; d = load(i); d["version"] = "1.0.1"; d["attributionRuleRef"]["version"] = "1.0.1"; dump(i, d)

        def snapshot(reg: Path, name: str) -> Path:
            out = a.out / name
            if out.exists(): shutil.rmtree(out)
            shutil.copytree(reg, out, ignore=shutil.ignore_patterns(".lock")); return out

        for name, (stream, release) in STREAMS.items(): publish(reg, name, stream, release, "1.0.0")
        publish(reg, "plc-stamped-bundle", "line2-plc-nocheck", "line2-rules-plc-nocheck", "1.0.0", edit=nocheck)
        target = snapshot(reg, "registry")
        publish(reg, "bundle", "line2", "line2-rules", "1.0.1", edit=bump_rules)
        nxt = snapshot(reg, "registry-next")
        # A copy of Tyr's bundle with another occupancy schema digest, in its own registry: Bifrost approves and publishes it,
        # head and release verify, then RuleSet.load refuses binding.schema-mismatch.
        regm = tmp / "registry-mismatch"; (regm / "identity").mkdir(parents=True)
        shutil.copy(reg / "identity" / "authorized-keys.jsonl", regm / "identity" / "authorized-keys.jsonl")
        dump(regm / "identity" / "activation-policy.json", {"version": "1.0.0", "default": "deny", "rules": rules})
        publish(regm, "bundle", "line2", "line2-rules", "1.0.0", edit=mismatch)
        mis = snapshot(regm, "registry-mismatch")
        (a.out / "trusted-keys.jsonl").write_text("\n".join(lines) + "\n", newline="\n")
        dump(a.out / "registry.lock.json", {"bifrost": git_head(a.bifrost), "tyr": git_head(a.tyr),
                                            "occupancySchemaSha256": occupancy_sha, "incidentSchemaSha256": incident_sha,
                                            "files": inventory(target), "next": inventory(nxt), "mismatchRegistry": inventory(mis)})
    bundles = sum(1 for p in upstream.rglob("*") if p.is_file() and p.name != "manifest.json")
    print(f"wrote {upstream} ({bundles} bundle files + manifest), {target} ({len(inventory(target))} files), {nxt} ({len(inventory(nxt))} files), {mis} ({len(inventory(mis))} files)")


if __name__ == "__main__":
    main()
```

The activation policy names the three streams `line2`, `line2-plc` and `line2-plc-nocheck`. The two publications of Tyr's bundles change no bundle byte at all (their submissions already say `line2` and `line2-plc`, `baseRelease null`; Tyr authors the plc bundle on stream `line2-plc`, so nothing is re-stamped). Only a variant's submission has its `streamId`, `version` and `baseRelease` set by the script, because a stream has one head and the `nocheck` variant goes to its own stream; Bifrost pins the proposal digest, not the stream, into the submission. Never change rule or profile bytes beyond the listed edits (`nocheck`, `bump_rules`, `mismatch`).

- [ ] **Step 6.2** Run it. Expected: `wrote .../fixtures/upstream (108 bundle files + manifest), .../fixtures/registry (192 files), .../fixtures/registry-next (239 files), .../fixtures/registry-mismatch (52 files)`. Counts: identity 2; per stream heads 1 + head-attestations 1 + anchor 1; per release approvals 1 + attestations 1 + release.json 1 + payload files (44 for `bundle`, 64 for `plc-stamped-bundle`: Tyr's 39 and 59 of today plus five station3 files each, one source, one manual text, one evidence artifact, one mapping set, one target). `registry` = 2 + 3 x 3 + (47 + 67 + 67) = 192; `registry-next` = 192 + 47 (the 1.0.1 release; the `line2` head, attestation and anchor are replaced, not added) = 239; `registry-mismatch` = 2 + 3 + 47 = 52. The four totals follow from 44 and 64, which are expected values, to be confirmed when the Tyr PR merges; the formulas hold whatever the two sizes turn out to be. `file fixtures/registry/mappings/heads/line2.json` shows no CRLF. Decode the `registry-next` head statement: `python -I -c "import json,base64;e=json.load(open('fixtures/registry-next/mappings/head-attestations/line2.dsse.json'));print(json.dumps(json.loads(base64.b64decode(e['payload'])),indent=1))"` -> `headVersion 2`, `cause publish`, `previous {headVersion 1, release {...1.0.0...}}`.

- [ ] **Step 6.3** `Refusal.java` (the closed set of Task 1's insertion at line 84):

```java
package yggdrasil.forseti.core.release;

import java.util.*;

/** The reader's closed refusal set: Vor's four names for the same failures plus the spec's start-up refusal. A refusal names
 *  its rule and evidence, never carries secrets, and is never a disposition reason. */
public final class Refusal extends RuntimeException {
    public static final List<String> REASONS = List.of("head-unverified", "head-rollback", "head-expired", "provenance-mismatch", "binding.schema-mismatch");
    public final String reason; public final Map<String, Object> evidence;
    public Refusal(String reason, Object... kv) {
        super(reason); if (!REASONS.contains(reason)) throw new IllegalArgumentException("not a refusal reason: " + reason);
        this.reason = reason; Map<String, Object> e = new LinkedHashMap<>(); for (int i = 0; i + 1 < kv.length; i += 2) e.put(String.valueOf(kv[i]), kv[i + 1]); this.evidence = Collections.unmodifiableMap(e);
    }
}
```

- [ ] **Step 6.4** `TrustedKeys.java` (JDK only):

```java
package yggdrasil.forseti.core.release;

import yggdrasil.forseti.core.Json;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;

/** principal -> Ed25519 public keys from JSON lines {"principal", "publicKey"} (X.509 SubjectPublicKeyInfo, base64), the file
 *  Bifrost's `identity keygen` prints and Vor reads. The operator gives it to forseti outside the registry. */
public final class TrustedKeys {
    private final Map<String, List<PublicKey>> keys = new HashMap<>();
    public static TrustedKeys load(Path file) throws java.io.IOException {
        TrustedKeys t = new TrustedKeys();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            var e = Json.read(line.getBytes(StandardCharsets.UTF_8));
            t.keys.computeIfAbsent(Json.text(e, "principal"), k -> new ArrayList<>()).add(publicKey(Json.text(e, "publicKey")));
        }
        return t;
    }
    public static PublicKey publicKey(String b64) {
        try { return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(b64))); }
        catch (GeneralSecurityException | IllegalArgumentException e) { throw new IllegalArgumentException("not an Ed25519 public key", e); }
    }
    public List<PublicKey> of(String principal) { return keys.getOrDefault(principal, List.of()); }
}
```

- [ ] **Step 6.5** `Dsse.java` (PAE and raw Ed25519 verification; `verify` never throws):

```java
package yggdrasil.forseti.core.release;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Base64;

public final class Dsse {
    public static final String IN_TOTO = "application/vnd.in-toto+json";
    private Dsse() {}
    /** "DSSEv1" SP LEN(type) SP type SP LEN(body) SP body, lengths as ASCII decimal. */
    public static byte[] pae(String type, byte[] body) {
        byte[] t = type.getBytes(StandardCharsets.UTF_8); ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(("DSSEv1 " + t.length + " ").getBytes(StandardCharsets.US_ASCII)); out.writeBytes(t);
        out.writeBytes((" " + body.length + " ").getBytes(StandardCharsets.US_ASCII)); out.writeBytes(body); return out.toByteArray();
    }
    /** The envelope spec allows standard or URL-safe Base64. */
    public static byte[] decode(String b64) {
        try { return Base64.getDecoder().decode(b64); } catch (IllegalArgumentException e) { return Base64.getUrlDecoder().decode(b64); }
    }
    public static boolean verify(byte[] message, byte[] signature, PublicKey key) {
        try { Signature s = Signature.getInstance("Ed25519"); s.initVerify(key); s.update(message); return s.verify(signature); }
        catch (Exception e) { return false; }
    }
}
```

- [ ] **Step 6.6** `FloorStore.java`: `record Floor(long headVersion, String statementSha256)`; `<stateDir>/head-floors/<stream>.json` = `{"headVersion": n, "statementSha256": "<hex>"}`; `get(stream) -> Optional<Floor>` (read only, for reports); `streams()`; and the one mutating entry point, `update(stream, candidateVersion, statementSha, boolean raise) -> Outcome`, which first takes an in-JVM `ReentrantLock` keyed by the canonical path of `<stateDir>/head-floors/.lock` (a static `ConcurrentHashMap<Path, ReentrantLock>`, because overlapping `FileLock` requests from one JVM throw `OverlappingFileLockException` instead of waiting) and then the `FileLock` on that file against other processes, re-reads the current floor, returns `ROLLBACK(floor)` when `candidate < floor.headVersion` or (`candidate == floor.headVersion` and the statement digest differs), otherwise, when `raise` is true and `candidate > floor.headVersion` (or no floor exists), writes `<stream>.json.tmp`, `FileChannel.force(true)`, `Files.move(tmp, file, ATOMIC_MOVE, REPLACE_EXISTING)` and returns `RAISED`, else `UNCHANGED`. Read, compare and write happen inside one lock hold, so two concurrent verifiers cannot interleave a lower version behind a higher one (spec 84: the floor never goes down). A test with two threads of one JVM and versions 3 and 2 ends with the floor at 3 whichever thread runs first and throws no `OverlappingFileLockException`; a second test with two processes (the test forks `java -cp ... FloorStoreMain`) shows the same. The same two-level lock (`cli/.../LockedFile` helper, shared by nothing else) guards the generation store's `append` in Task 20.

- [ ] **Step 6.7** `HeadVerifier.java` (Vor's order; every refusal names its rule; the signature is counted by the verified trusted key under the claimed principal, never by `keyid`; the decoded original payload bytes are verified, never a re-serialization):

```java
package yggdrasil.forseti.core.release;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import java.nio.file.*;
import java.security.PublicKey;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;

/** Verifies mappings/heads/<stream>.json through mappings/head-attestations/<stream>.dsse.json as Vor's head.py does:
 *  envelope shape, two distinct principals, one verifying signature per principal under two different trusted keys, the
 *  statement names exactly this head and its release.json by raw-byte SHA-256, signedAt not in the future, a version floor
 *  that never goes down (the same version must carry the same statement), then expiry. Pure: `nowMs` is a parameter. */
public final class HeadVerifier {
    public static final String STATEMENT = "https://in-toto.io/Statement/v1";
    public static final String PREDICATE = "https://github.com/yggdrasil-iiot/bifrost/attestation/mapping-head/v1";
    static final Pattern STREAM = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
    static final Pattern INSTANT = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z");
    private HeadVerifier() {}

    public record Head(String stream, String releaseId, String version, String releaseSha256, String headFile, String headSha256,
                       String attestationFile, String attestationSha256, String statementSha256, long headVersion,
                       String signedAt, String expires, String publishedBy, String approvedBy) {
        public String releasePath() { return "releases/" + releaseId + "/" + version + "/release.json"; }
    }

    public static Head verify(Path mappings, String stream, TrustedKeys trusted, FloorStore floors, long nowMs) {
        if (!STREAM.matcher(stream).matches()) throw new Refusal("head-unverified", "stream", stream, "rule", "stream-id");
        String headFile = "heads/" + stream + ".json", attFile = "head-attestations/" + stream + ".dsse.json";
        byte[] headBytes, attBytes;
        try { headBytes = Files.readAllBytes(mappings.resolve(headFile)); attBytes = Files.readAllBytes(mappings.resolve(attFile)); }
        catch (java.io.IOException e) { throw new Refusal("head-unverified", "stream", stream, "rule", "missing"); }
        String headSha = Canonical.digestOf(headBytes);
        JsonNode head, envelope, statement, p; byte[] body; String published, approved;
        try {
            head = Json.read(headBytes); envelope = Json.read(attBytes);
            if (!Dsse.IN_TOTO.equals(envelope.path("payloadType").asText(null)) || !envelope.path("payload").isTextual()) throw new IllegalArgumentException("envelope");
            body = Base64.getDecoder().decode(envelope.path("payload").asText()); statement = Json.read(body); p = statement.path("predicate");
            published = Json.text(p, "publishedBy"); approved = Json.text(p, "approvedBy");
        } catch (IllegalArgumentException e) { throw new Refusal("head-unverified", "stream", stream, "rule", "envelope"); }
        if (published.equals(approved)) throw new Refusal("head-unverified", "stream", stream, "rule", "principals", "publishedBy", published, "approvedBy", approved);
        byte[] message = Dsse.pae(Dsse.IN_TOTO, body); List<PublicKey> used = new ArrayList<>();
        for (String principal : List.of(published, approved)) {
            PublicKey key = signer(envelope, message, trusted.of(principal));
            if (key == null) throw new Refusal("head-unverified", "stream", stream, "rule", "signature", "principal", principal);
            for (PublicKey u : used) if (Arrays.equals(u.getEncoded(), key.getEncoded())) throw new Refusal("head-unverified", "stream", stream, "rule", "same-key", "principal", principal);
            used.add(key);
        }
        String releaseId, version, releaseSha; Instant signedAt, expires;
        try {
            releaseId = Json.text(head, "id"); version = Json.text(head, "version"); releaseSha = Json.text(head, "sha256");
            String releasePath = "releases/" + releaseId + "/" + version + "/release.json"; JsonNode subject = statement.path("subject");
            boolean ok = STATEMENT.equals(statement.path("_type").asText(null)) && PREDICATE.equals(statement.path("predicateType").asText(null))
                && stream.equals(p.path("streamId").asText(null)) && head.equals(p.path("release"))
                && p.path("headVersion").isIntegralNumber() && p.path("headVersion").bigIntegerValue().signum() > 0
                && p.path("headVersion").bigIntegerValue().compareTo(Canonical.MAX_SAFE) <= 0 && subject.size() == 2
                && headFile.equals(subject.get(0).path("name").asText(null)) && headSha.equals(subject.get(0).path("digest").path("sha256").asText(null))
                && releasePath.equals(subject.get(1).path("name").asText(null)) && releaseSha.equals(subject.get(1).path("digest").path("sha256").asText(null));
            signedAt = instant(Json.text(p, "signedAt")); expires = instant(Json.text(p, "expires"));
            if (!ok) throw new IllegalArgumentException("subject");
        } catch (IllegalArgumentException | DateTimeParseException e) {
            throw new Refusal("head-unverified", "stream", stream, "rule", "subject", "expected", headSha);
        }
        Instant now = Instant.ofEpochMilli(nowMs);
        if (signedAt.isAfter(now)) throw new Refusal("head-unverified", "stream", stream, "rule", "signed-in-future", "signedAt", signedAt.toString(), "now", now.toString());
        String statementSha = Canonical.digestOf(body); long v = p.path("headVersion").asLong();
        // Rollback is judged before expiry (Vor's order), and the floor is read, compared and raised inside one lock hold;
        // an expired head is refused but never raises the floor.
        boolean expired = !now.isBefore(expires);
        FloorStore.Outcome outcome = floors.update(stream, v, statementSha, !expired);
        if (outcome instanceof FloorStore.Outcome.Rollback r)
            throw new Refusal("head-rollback", "stream", stream, "floor", r.floor().headVersion(), "candidate", v, "recordedStatementSha256", r.floor().statementSha256(), "candidateStatementSha256", statementSha);
        if (expired) throw new Refusal("head-expired", "stream", stream, "expires", expires.toString(), "now", now.toString());
        return new Head(stream, releaseId, version, releaseSha, "mappings/" + headFile, headSha, "mappings/" + attFile, Canonical.digestOf(attBytes), statementSha, v,
                        signedAt.toString(), expires.toString(), published, approved);
    }
    static Instant instant(String text) { if (!INSTANT.matcher(text).matches()) throw new IllegalArgumentException(text); return Instant.parse(text); }
    static PublicKey signer(JsonNode envelope, byte[] message, List<PublicKey> keys) {
        for (JsonNode s : envelope.path("signatures")) {
            if (!s.path("sig").isTextual()) continue; byte[] raw;
            try { raw = Dsse.decode(s.path("sig").asText()); } catch (IllegalArgumentException e) { continue; }
            for (PublicKey k : keys) if (Dsse.verify(message, raw, k)) return k;
        }
        return null;
    }
}
```

- [ ] **Step 6.8** `ReleaseReader.java`: `read(Path mappings, Head head) -> Release` with `record Release(Head head, String root, JsonNode doc, Map<String,String> files, Map<DocKey, Doc> documents, Map<String, byte[]> payload)`: `release.json` bytes digest equals `head.releaseSha256()` else `provenance-mismatch` (`rule release-digest`); `doc.id/version/streamId` equal the head's (`release-identity`); every `files[]` entry: normalised path (no `..`, no leading `/`), starts with `payload/`, unique (`release-path`), file exists and its raw digest equals `sha256` (`release-file`); the bytes are captured once at verification and served from memory afterwards (no second read of a mutable file); `payload/proposal.json` parsed and each listed `{path, kind, id, version, sha256}` has `files.get("payload/" + path).equals(sha256)` (`proposal-file`); documents indexed by `(kind, id)` with the version checked on lookup (`document(kind, ref)` -> `provenance-mismatch document-missing`); `payload(path)` only for listed, verified files (`unlisted`).

- [ ] **Step 6.9** Tests. `TestKeys.java` generates Ed25519 pairs and re-signs a statement body (mirroring Vor's `helpers.py`) so tampered variants are produced from a temp copy of the fixture; fixed clock `NOW = signedAt + 60_000` read from the envelope. `HeadVerifierTest`: `verifiesTheBifrostPublishedHead` (`line2`, `line2-plc`, `line2-plc-nocheck`; `headVersion 1`; floor written at `tmp/state/head-floors/line2.json`); `verifiesRegistryMismatch` (the derived copy with another occupancy schema digest: head and release verify; the refusal comes later, in `RuleSetTest`); `verifiesRegistryNextAtVersion2AndRaisesTheFloor`; `registryAfterRegistryNextIsRollback`; `aMissingAttestationIsUnverified` (`missing`); `aWrongPayloadTypeIsUnverified` (`envelope`); `onePrincipalIsUnverified` (`principals`); `aSignatureFromAnUntrustedKeyIsUnverified` (`signature`); `aForgedKeyidDoesNotCount` (keyid says `approver`, signature by an untrusted key: `signature`); `twoSignaturesFromOneKeyAreUnverified` (`same-key`); `aChangedHeadFileIsUnverified` (`subject`); `aModifiedPayloadByteIsUnverified` (`signature`: the PAE no longer matches); `aStatementSignedInTheFutureIsUnverified`; `aLowerVersionThanTheFloorIsRollback`; `theSameVersionWithAnotherStatementIsRollback`; `anExpiredHeadIsExpired` (now = expires); `rollbackIsCheckedBeforeExpiry`. `ReleaseReaderTest`: `readsBothReleasesAndTheirPayloads` (44 and 64 payload files, expected, to be confirmed when the Tyr PR merges; one `attribution-rule`, one `impact-rule`, the two profile `evidence-artifact`s); `aChangedPayloadByteIsAMismatch` (`release-file`); `aChangedReleaseJsonIsAMismatch` (`release-digest`); `anUnlistedPayloadIsRefused`; `anEscapingPathIsRefused` (`release-path`); `aProposalEntryNotInReleaseIsAMismatch`. `BifrostSeamTest` (requires `FORSETI_BIFROST_JAR` and `FORSETI_TYR_FIXTURES`; `Assumptions.assumeTrue` otherwise, and the CI job `bifrost-seam` always sets them): runs `scripts/make_registry_fixture.py --out <tmp>` against the live jar, verifies both heads and version 2 of `registry-next` with the forseti reader at `signedAt + 60 s` and once at the real current time (`System.currentTimeMillis()`, inside the P366D window), reads both releases, and tampers one payload byte (`provenance-mismatch`). A missing jar in CI is a failure, never a skip.

- [ ] **Step 6.10** `mvn -q -pl core -Dtest='HeadVerifierTest,ReleaseReaderTest' test` -> `Tests run: 23, Failures: 0`. With the jar: `FORSETI_BIFROST_JAR=../bifrost/gates/target/bifrost-gates.jar FORSETI_TYR_FIXTURES=../tyr/fixtures/attribution-rule mvn -q -pl core -Dtest=BifrostSeamTest test` -> `Tests run: 1, Failures: 0`. Two commits: `test(fixtures): Tyr's bundles published byte for byte by Bifrost; forseti's re-pinned variants (nocheck, rule 1.0.1, schema mismatch)` and `feat(core): release reader at Vor's level (head, release, payload by raw-byte SHA-256)`.

### Task 7: Rule set from a release: both rules, both profiles, hierarchy, member catalog, `binding.schema-mismatch`, `rulesDigest`

**Files:**
- Create: `core/src/main/java/yggdrasil/forseti/core/rules/{Hierarchy,MemberCatalog,AttributionRule,ImpactRule,OccupancyBinding,IncidentBinding,RuleSet}.java`
- Create: `core/src/test/java/yggdrasil/forseti/core/rules/RuleSetTest.java`
- Create: `oracle/src/forseti_oracle/rules.py`, `oracle/tests/test_rules.py`

Bifrost approved the bundle, so forseti does not re-run the eleven `attribution.*` checks; it resolves what the job needs and refuses only what the spec assigns to the job: `binding.schema-mismatch` (line 192) and the incident schema supporting each `kinds[]` entry (line 224). Resolution follows spec 132: `members[].mappingSetRef + mappingId` -> mapping of the set -> `memberPath[0].memberId` in the semantic definition of the set's target instance -> `name` (= `metric`), `valueType`, `semanticId`; the set's `equipmentInstanceId` -> hierarchy path; roles by reference (activity, identity, measurement).

- [ ] **Step 7.1** `Hierarchy.java`: `from(JsonNode doc)`: nodes by id, `path(id)` root-first joined by `:` (a cycle or a missing parent is `IllegalArgumentException`; Bifrost already refused it), `stationOf(List<String> stations, String path)` = the station equal to `path` or a prefix `station + ":"` (exactly one by `attribution.station-nested`; `Optional.empty()` otherwise), `isDescendantOrSelf(ancestor, path)`.

- [ ] **Step 7.2** `MemberCatalog.java`:

```java
public record Resolved(String mappingId, String equipmentPath, String station, String metric, String valueType, String semanticId, Role role) {}
public enum Role { MEASUREMENT, ACTIVITY, IDENTITY_UNIT, IDENTITY_VISIT, IDENTITY_PRESENT }
public Map<String, Resolved> byMappingId(); public Optional<Resolved> byEquipmentAndMetric(String path, String metric);
public Optional<Resolved> activityOf(String equipmentPath); public Set<String> equipment(); public List<Resolved> measurementsOf(String equipmentPath);
public List<Resolved> requiredMembersOf(String station)   // every measurement member of the station and of its tools (spec 138)
```

- [ ] **Step 7.3** `AttributionRule.java` (typed accessors: `id, version, stations, outOfOrdernessMs, allowedLatenessMs, horizonMs, enterToleranceMs, leaveToleranceMs, uncertainInput, maxBadShare, expectedPeriodMs, maxMissingPeriods, mode, namespace, crossCheck, identityMembers`), `ImpactRule.java` (`incidentScope, overlap, match, onBasisRetracted, onWithdrawn, workRecordsMs`), `OccupancyBinding.java` (`topic, schemaId, schemaSha256, pattern` compiled once and matched whole-string with `matcher(unitId).matches()`, `enterBizStep, leaveBizStep, correctionReasons, sources`), `IncidentBinding.java` (`topic, schemaId, schemaSha256, kinds`). Thin views over the released `JsonNode`; node and released file digest kept for the pins.

- [ ] **Step 7.4** `RuleSet.load(Release release, String attributionRuleId, String version, JsonNode definitions)`: the attribution rule; its `equipmentRef` hierarchy; the impact rule whose `attributionRuleRef` names it (exactly one, else `IllegalStateException`); both profile artifacts and data files (bytes through `release.payload`); `OccupancyBinding.schemaSha256` must equal `InputParser.schemaSha256("urn:forseti:schema:occupancy:1.0.0")` and the incident one likewise, else `new Refusal("binding.schema-mismatch", "profile", id, "expected", builtIn, "approved", declared)`; every `kinds[]` entry in `InputParser.incidentKinds()`, else `binding.schema-mismatch` (`rule kind-unsupported`); `MemberCatalog`. `pins()` = `{"rules": {"attribution": {id, version, sha256}, "impact": {...}}, "hierarchy": {id, version, sha256}, "bindings": {"occupancy": {...}, "incident": {...}}, "definitions": <devicePath -> {templateRef, version, sha256}>}` with each `sha256` the release's `files[]` digest of that document; `rulesDigest()` = `Canonical.digest(pins())` (spec 355). The envelope's `Pins` is this object plus `job`; the gate's cross-check recomputes the digest over the four pin fields only (`Pins.withoutJob()`), never over `job`, which differs per run.

- [ ] **Step 7.5** `RuleSetTest`: loads `line2` and `line2-plc` from `fixtures/registry` with `definitions = {}`: stations (three in both rules: `station1`, `station2`, `station3`); `byMappingId` sizes 7 and 9; roles (occupancy-join: 2 measurement, 5 activity, `station3-running` being the fifth; plc-stamped unchanged: 1 measurement, 2 activity, 6 identity, because that rule gains station3 in `stations[]` only and keeps identity on `station1` and `nutrunner1`); `requiredMembersOf(station1)` = `[nutrunner1-torque, nutrunner2-torque]` and `[nutrunner1-torque]`; `activityOf(station1:nutrunner1)` Boolean `nutrunner1-running`; impact rule found; bindings (`topic` `mfg.occupancy` / `quality.incidents`; pattern matches `UNIT0000000000001`, refuses `unit-1`); `rulesDigest` 64 hex, changes with `definitions`; the literal digest printed once and pinned. `loadsRule101FromRegistryNext` (`maxMissingPeriods 4`, different `rulesDigest`). `aProfileWithAnotherDigestIsASchemaMismatch`: on `fixtures/registry-mismatch` (the derived copy of Tyr's `bundle` published by Bifrost, Task 6) the head verifies and the release reads with every digest intact, then `RuleSet.load` throws `Refusal binding.schema-mismatch` with `approved` = `...0001` and `expected` = the real occupancy digest `e97cd7c9...` (Amendment 2026-11-11: an approved profile with another digest is refused, and the refusal is reached only after head and release verification, as at a real start-up; Tyr's own bytes load, which the first test of this step shows). `loadsThePlcNocheckRule` (`line2-plc-nocheck`: `crossCheck none`, version `1.0.1`). `anUnsupportedKindIsASchemaMismatch`.

- [ ] **Step 7.6** Oracle `rules.py`: `load_release(mappings, release_id, version)` (release.json and payload digests re-checked by raw bytes), `rule_set(release, attribution_rule_id, version)` with the same resolved fields, `pins(definitions)`, `rules_digest(definitions)`. `test_rules.py`: the same assertions on `fixtures/registry` and `registry-next`, plus `test_rules_digest_equals_java_pin` (the literal `RuleSetTest` pins): the first cross-language equality beyond the vector file.

- [ ] **Step 7.7** `mvn -q -pl core -Dtest=RuleSetTest test` -> `Tests run: 11, Failures: 0`; `pytest -q oracle` -> `37 passed`. Commit `feat(core,oracle): rule set loaded from a verified release; binding.schema-mismatch on a published profile with another digest; rulesDigest`.

### Task 8: Recording format, routing manifest, minimal builder, the committed smoke recordings

**Files:**
- Create: `cli/src/main/java/yggdrasil/forseti/cli/recording/{Recording,RoutingManifest,Murmur2,ScenarioBuilder,Telemetry}.java` (`ScenarioBuilder` in its minimal form: visits, telemetry with tails, sessions, batches with deadline brackets, digests; Task 16 adds overlays and ground truth); Modify: `Main` (`recordings build|verify`)
- Create: `fixtures/scenarios/{smoke,smoke-plc,smoke-incident}.json` (+ `.lock.json` each), `fixtures/recordings/{smoke,smoke-plc,smoke-incident}/**` (built from those scenarios and committed; `smoke-plc` and `smoke-incident` are used by Tasks 12 and 15)
- Create: `cli/src/test/java/yggdrasil/forseti/cli/recording/{RecordingTest,Murmur2Test}.java`
- Create: `oracle/src/forseti_oracle/recording.py`, `oracle/tests/test_recording.py`

A recording is a directory with the shape the step-5 recorder will write from Kafka, so step-3 recordings and step-5 captures are read by one `Recording.open` and the engines of steps 6-7 replay both:

```
<recording>/
  manifest.json       recordingVersion 1; name; synthetic (bool); source {scenario, generator, seed, overlays[]}; digests {relative file -> sha256};
                      rule {stream, releaseId, version, attributionRuleId, attributionRuleVersion}; definitions {devicePath -> {templateRef, version, sha256}};
                      T0; scanMs; evidenceRange {devicePath -> {from, to}} (the verified telemetry range, Task 1's line 398);
                      batches[] {cutId, throughOffsets {topic -> {partition -> offset}} (inclusive, the barrier's throughOffset; -1 while no record of that partition is included, so an empty partition such as every quality.incidents partition of smoke carries -1 in every batch), idleInputs[]};
                      deadlines[] {kind, ref, at, cutA, cutB}; settleTargetW; tailMs
  routing.json        partitions 6; stations {path -> partition}; devices {path -> {station, rawPartition}}; subscribed {topic -> [partitions]}; routingDigest
  expected.json       ground truth (visits, every sampleId's expected attribution, dispositions per identity, incident units and set coverage at the last cut, transitions at the bracketing cuts)
  ot.messages/<p>.jsonl  ot.sessions/<p>.jsonl  mfg.occupancy/<p>.jsonl  quality.incidents/<p>.jsonl
```

One record per line: `{"offset": <long>, "timestamp": null, "key": "<partition key>", "headers": {}, "value": {...}}` (offsets per partition from 0 and contiguous; `timestamp` is the broker append time in step 5; `headers` carries `forseti-generation` on output topics only). `routingDigest` = `Canonical.digest` of `routing.json` without that field. Raw device keys are placed with Kafka's default partitioner (`toPositive(murmur2(keyBytes)) % 6`; `Murmur2.java` is the 40-line reference algorithm) so the step-5 preflight finds the same placement; `quality.incidents` keeps its raw key `equipmentRef` (spec 463) and is placed by the same default partitioner (all six partitions subscribed; the harness routes each incident to its `stationRef`, the internal routed input of spec 466); only `mfg.occupancy` is written already partitioned by station through the station partitioner (sorted `stations[]`, spec 462).

- [ ] **Step 8.1** `fixtures/recordings/smoke/`: one Station1 visit (`UNIT0000000000001`, enter `T0 + 600000`, leave `T0 + 660000`, nutrunner1 used), two occupancy events, one `session-up` per device, no incidents, and telemetry on all five devices every second from `T0 + 600000` to `T0 + 660000 + tailMs` = `T0 + 1411000` (the spec's tail `L + O + P` = 751,000 ms after the last business event, current spec 613): 812 scans per device, 5 x 812 = 4,060 messages, 7 x 812 = 5,684 samples (the three station devices carry one member, the two nutrunners two). Without the tail the last `sourceTs` would be `T0 + 660000` and `W1` would stop at `T0 + 629999`, below the settlement deadline `leave + L = T0 + 1380000`; with it the bracketing cuts are the batch ending at the scan `T0 + 1410000` (`W1 = T0 + 1379999 < d`) and the next at `T0 + 1411000` (`W1 = T0 + 1380999 >= d`). Batches every 30 s of event time: boundaries at `T0 + 630000, 660000, ..., 1410000` (`(1410000 - 600000) / 30000 = 27` boundaries; the bracket `cutA` at scan `1410000` coincides with the 27th), plus `cutB` at the last scan `1411000`: 28 cuts. A recording this size is not typed by hand: `fixtures/scenarios/smoke.json` describes it and the minimal builder of this task (`ScenarioBuilder` without overlays and ground truth, which Task 16 adds) writes it; the files are committed with `fixtures/scenarios/smoke.lock.json` so the format is visible in review and `forseti recordings verify` proves the bytes. The first `mfg.occupancy` line (an arrival carries neither `arrivalEventId` nor `errorDeclaration`, current spec 294, 296):

```json
{"offset":0,"timestamp":null,"key":"synthetic-site:line2:station1","headers":{},"value":{"eventId":"occ-1","eventTime":1794183000000,"recordTime":1794183000200,"unitId":"UNIT0000000000001","action":"OBSERVE","bizStep":"urn:epcglobal:cbv:bizstep:arriving","readPoint":"synthetic-site:line2:station1","visitSeq":1,"source":"mes-line2"}}
```

A process message line (nutrunner1, scan at `T0 + 600000`; all members one `sourceTs`; `definitionSha256` is the synthetic pin `manifest.definitions[device].sha256`; the DBIRTH is counted as having consumed `seq` 1, spec 239, so the first DDATA of a session carries `seq` 2):

```json
{"offset":0,"timestamp":null,"key":"synthetic-site:line2:station1:nutrunner1","headers":{},"value":{"edge":"edge-line2","device":"synthetic-site:line2:station1:nutrunner1","birthPublishTs":1794182400000,"bdSeq":0,"seq":2,"publishTs":1794183000050,"bridgeReceivedAt":1794183000120,"definitionSha256":"<pin>","members":[{"metric":"Torque","value":0.0,"uaStatusCode":0,"quality":192,"sourceTs":1794183000000,"serverTs":1794183000010},{"metric":"Running","value":false,"uaStatusCode":0,"quality":192,"sourceTs":1794183000000,"serverTs":1794183000010}]}}
```

- [ ] **Step 8.2** `Recording.java`: `open(Path)` (manifest, routing, `records(topic, partition) -> List<Record>`, `verifyDigests()`), `Record(long offset, Long timestamp, String key, JsonNode headers, JsonNode value)`; `RoutingManifest.java`: `stationOf(device)`, `partitionOf(station)`, `subscribed(topic)`, `routingDigest()` recomputed and compared. `RecordingTest`: opens `smoke`, 4,060 messages (812 per device) and 5,684 samples, offsets contiguous (every `offset`, `timestamp`, `cutId`, `T0`, `scanMs` and `evidenceRange` bound is read with `Json.safeLong` in `Json.NON_NEGATIVE`, every `throughOffsets` value in `Json.THROUGH`; `smoke`'s six `quality.incidents` partitions carry `throughOffsets -1` in all 28 batches and `smoke-incident`'s carry `-1` until the batch that delivers the incident at offset 0; a manifest with an offset of `9007199254740992`, or a through-offset of `-2`, is refused as malformed, the boundary cases of the recording kind), digests verified and equal to `fixtures/scenarios/smoke.lock.json`, `routingDigest` recomputed equals the file's, a wrong digest in the manifest is refused, `forseti recordings verify --scenario fixtures/scenarios/smoke.json --out fixtures/recordings/smoke` rebuilds byte-identically; incidents of `smoke-incident` sit in the partition Murmur2 gives their `equipmentRef`, not in the station's. `Murmur2Test`: three vectors pinned by running the reference algorithm once (comment: "Kafka DefaultPartitioner, 6 partitions").

- [ ] **Step 8.3** Oracle `recording.py`: `open_recording(path)`, `records(topic, partition)`, routing, `verify_digests()`; `test_recording.py` opens `smoke`, asserts the same counts, recomputes `routingDigest` with the oracle's canonicalizer (equal to the file's: the second cross-language digest).

- [ ] **Step 8.4** `mvn -q -pl core install -DskipTests && mvn -q -pl cli -Dtest='RecordingTest,Murmur2Test' test` -> `Tests run: 6, Failures: 0`; `pytest -q oracle` -> `41 passed`. Commit `feat(cli,oracle): recording format, routing manifest, minimal scenario builder and the committed smoke recordings`.

### Task 9: `forseti gate --engine core` frame, the foundation manifest, pull request 1

**Files:**
- Create: `cli/src/main/java/yggdrasil/forseti/cli/{Main,Args}.java`, `cli/src/main/java/yggdrasil/forseti/cli/gate/{CoreGate,Assertions}.java`
- Create: `gate/core-assertions.json`, `gate/core-negative-controls.json`
- Create: `cli/src/test/java/yggdrasil/forseti/cli/gate/CoreGateTest.java`

- [ ] **Step 9.1** `Assertions.java` (the accountant: `check(id, condition, evidence)` refuses an id not in the manifest and a duplicate id; `finish()` prints `assertions run N / expected N` and returns 1 when any expected id did not run or failed) and `CoreGate.java`: loads `gate/core-assertions.json` `{"engine": "core", "assertions": [{"id", "pr", "description"}]}`, runs every registered assertion, writes `build/gate/report.json` `{exit, scope: "core", engine, manifestSha256, assertionsExpected, assertionsRun, failed[], notRun[], intendedFailures[] (controls only), pins {jcs {java, python}, python, jdk, forseti artifactSha256}, seed, fixtureDigests {registry, registryNext, upstream, vectors, schemas, scenarios}, startedAt, finishedAt, command}`, prints the line last, exits 0 only when every manifest id ran and held and no unknown id ran; exits 1 otherwise; a crash or a missing artifact still writes a failure report and exits 1; `--negative-control <name>` not in `gate/core-negative-controls.json` exits 2 (usage) so CI's `== 1` check catches an unimplemented control. Foundation assertions (`F-*`, 18):

| Id | What runs |
|---|---|
| `F-JCS-JAVA` | the vector file through `Canonical` (the checks of `CanonicalVectorsTest`) |
| `F-JCS-PY` | `python -I -m pytest -q oracle/tests/test_jcs_vectors.py` exits 0 (the gate's `--python`) |
| `F-JCS-REFUSALS` | the eight refusal vectors refused on both sides (Java in-process; `python -I -m forseti_oracle.cli canon-check vectors/jcs-vectors.json` exit 0) |
| `F-SCHEMA-COPIES` | `schemas/*.json` bytes equal the core resources; their digests equal `fixtures/registry.lock.json`, the two literals the amendment PR fixed (occupancy `e97cd7c9...`, incident `7141c311...`, written in full in the assertion) and the `format.schemaSha256` of both profiles in each bundle of `fixtures/upstream` |
| `F-UPSTREAM-UNTOUCHED` | every file of `fixtures/upstream/{bundle,plc-stamped-bundle}/**` matches `fixtures/upstream/manifest.json` (108 bundle files expected, to be confirmed when the Tyr PR merges; the manifest itself is one more file of the directory and is not in its own inventory), and every payload file of the releases `line2-rules 1.0.0` and `line2-rules-plc 1.0.0` in `fixtures/registry` has the digest the manifest records for the same bundle path (what Bifrost published is Tyr's bytes) |
| `F-JOURNAL-REDUCERS` | the `Revisions`, `Disposition` and `JournalFold` behaviours of Task 5 on inline journals (first revision 1, unchanged body emits nothing, delete continuity, reappearance, a `conflict` revision under the same delivery key, the four fold violations detected) |
| `F-HEAD-OK` | both fixture heads and `registry-next`'s version 2 verify at `signedAt + 60 s` with `fixtures/trusted-keys.jsonl` |
| `F-HEAD-SIGNATURE`, `F-HEAD-SAME-KEY`, `F-HEAD-SUBJECT`, `F-HEAD-FUTURE`, `F-HEAD-ROLLBACK`, `F-HEAD-EXPIRED` | the tampered variants of `HeadVerifierTest` in a temp copy (the gate carries its own signer) |
| `F-RELEASE-DIGEST`, `F-PAYLOAD-DIGEST` | a changed `release.json` byte and a changed payload byte are `provenance-mismatch` |
| `F-RULES-LOAD-OJ`, `F-RULES-LOAD-PLC` | `RuleSet.load` on both releases; counts and roles as in `RuleSetTest`; `rulesDigest` equals the oracle's (`python -I -m forseti_oracle.cli rules-digest --mappings ... --release <id>@<version> --rule <id>@<version>`) |
| `F-BINDING-SCHEMA-MISMATCH` | on `fixtures/registry-mismatch` (the derived copy of Tyr's bundle with another occupancy schema digest, approved and published by Bifrost) the head verifies and the release reads, then `RuleSet.load` is refused with `binding.schema-mismatch` (`approved` `...0001`) |

`oracle/src/forseti_oracle/cli.py` is created here with `canon-check` and `rules-digest`; Task 18 adds `stage1`, `stage2`, `all`. `gate/core-negative-controls.json` is `{"controls": []}` until PR 6.

- [ ] **Step 9.2** `CoreGateTest`: `everyManifestIdHasARegisteredAssertion` (both ways); `anAssertionNotRunFailsTheGate` (a manifest copy with an extra id: exit 1, `notRun` lists it, last line `assertions run 18 / expected 19`); `anUnknownAssertionFailsTheGate`; `aCrashStillWritesAFailureReport`; `theReportRecordsPinsAndDigests`.

- [ ] **Step 9.3** `mvn -B -ntp -q install` -> `BUILD SUCCESS`; `java -jar cli/target/forseti.jar gate --engine core --out build/gate --python "$(command -v python)"` -> last line `assertions run 18 / expected 18`, exit 0; `python -I -c "import json;r=json.load(open('build/gate/report.json'));print(r['exit'],r['scope'])"` -> `0 core`. Commit `feat(cli): forseti gate --engine core with assertion accounting and the foundation manifest`.

- [ ] **Step 9.4** Pull request 1: `git log --oneline main..` shows Tasks 2-9 (9 commits); CI green (`build` with the gate at 18/18 and an empty control loop, `bifrost-seam`, `windows`). PR body: scope, the gate line, fixture provenance (`registry.lock.json`: Bifrost and Tyr heads, schema digests; `upstream/manifest.json`), the fixture arrangement of the Amendment 2026-11-11 (Tyr's bytes published untouched; the three derived variants), the action SHAs pinned. Wait for the merge; rebase PR 2.

## Chunk 3: Stage-1 attribution core (PR 2)

Branch: `git switch -c feat/step3-attribution-core main` after PR 1 merged.

### Task 10: Stage 1 decision function (occupancy-join): state, visit claims, attribution, coverage, work record and sample bodies

**Files:**
- Create: `core/src/main/java/yggdrasil/forseti/core/NegativeControls.java` (B2: the record with `NONE`, a constructor parameter of `Stage1` from its first commit; PR 6 adds the behaviours)
- Create: `core/src/main/java/yggdrasil/forseti/core/stage1/{Events,Stage1State,Stage1,VisitClaim,Attribution,Coverage,Sessions,WorkRecord,SampleRow,DeadlineIndex}.java`
- Create: `core/src/test/java/yggdrasil/forseti/core/stage1/{Stage1OccupancyJoinTest,CoverageTest,DeadlineIndexTest}.java`

The stage-1 rule sheet below is the two implementations' shared reading of the spec as corrected by Task 1 (the oracle implements it from the spec text in Task 18). Per station `s`; `P = expectedPeriodMs`, `L = allowedLatenessMs`, `H = horizonMs`, `O = outOfOrdernessMs`, `tolE/tolL` enter/leave tolerance.

**Stage-1 rule sheet (occupancy-join).**

- **S1 Routing and rejection** (harness side, before `decide`; dispositions to partition 6 through the reject collector). `ot.messages`: the device must be an equipment of the catalog, else `rejected unknown-device` (`part message`); a message whose members carry no `sourceTs` is `rejected no-source-timestamp` (`part message`) and does not advance event time (spec 261); `definitionSha256` different from `definitions[device].sha256` is `rejected definition-mismatch` for the whole message (spec 263); otherwise routed to `stationOf(device)`. `ot.sessions`: device as above. `mfg.occupancy`: schema failure `rejected schema-invalid`; `source` outside `sources[]`, `unitId` not matching `identity.pattern`, `bizStep` outside the profile's two values, `errorDeclaration.reason` outside `correction.reasons` (the schema admits both CBV reasons; the binding approves a subset, spec 213): `rejected schema-invalid` with `refs.check` (Task 1's insertion after line 450); `readPoint` not a rule station: `rejected unknown-readpoint`. `quality.incidents` is stage 2 (T1).
- **S2 Redelivery and conflict** (spec 232-233). Occupancy identity `(source, eventId)`; every stored delivery of the identity is kept as `(business content, declaration)` where business content is the record without `recordTime`, `errorDeclaration` and (when present) `bridgeReceivedAt`. Classification of a new delivery, in this order: (a) business content different from a stored delivery's: `conflict identity-conflict` for this delivery and a new disposition revision `conflict identity-conflict` for every stored delivery of the identity; the identity's contribution is withdrawn (an arrival in conflict makes its visit `retracted input-conflict`; a departure in conflict withdraws its leave claim, spec 296); (b) business content equal and both without a declaration, or both with the same declaration: `duplicate identical-redelivery`, no state change; (c) business content equal, one side with a declaration and the other without, in either arrival order: the declaration applies to that original (S4; a declaration stored first is a tombstone the later original confirms, spec 296) and the delivery is `accepted` (`part event` for the original, `part errorDeclaration` for the declaration, spec 437); (d) business content equal and two different declarations: both `conflict correction-conflict` (S4). Process messages: delivery identity `sampleId` per member, so every disposition of a message's members carries `part member:<metric>`, a byte-identical redelivered message included (each member `duplicate identical-redelivery`); `part message` is used only for whole-message rejections (S1: `unknown-device`, `no-source-timestamp`, `definition-mismatch`, spec 437); measurement identity `(device, metric, sourceTs, value, uaStatusCode)`: two deliveries with the same measurement identity are one observation (both `accepted`, one observation in digests and aggregates); the same `(device, metric, sourceTs)` with another `value` or `uaStatusCode` is `conflict measurement-conflict` for every delivery of the triple (new revisions for earlier ones), every such sample row `state quarantined, reason measurement-conflict`, and the raw record bytes go to `forseti.quarantine` (`quarantine/<p>.jsonl` in step 3), linked from `refs`. Session events: identity `(edge, device, birthPublishTs, bdSeq, kind, atSeq)`, same rule; `conflict identity-conflict` withdraws the event.
- **S3 Admission** (spec 491, 508). `admissionW` is the station's `W1` at its last committed progress (null before the first). A sample with `sourceTs < admissionW - H` is `replay-required beyond-horizon` (sample row `state replay-required, reason beyond-horizon`; disposition `replay-required beyond-horizon`). An occupancy input whose visit is expired is `replay-required`: an arrival when `admissionW >= eventTime + H + L`; a departure or error declaration when the visit it refers to has `admissionW >= onlineExpiresAt` (`leave + H + L` closed, `enter + H + L` open) or, when that visit is unknown, `admissionW >= eventTime + H + L`; an error declaration so refused has reason `correction-beyond-horizon`, an original `beyond-horizon` (spec 296). Online state of a visit is dropped by the `onlineExpiresAt` timer (its rows stay).
- **S4 Visit claims.** An accepted arrival `(eventId a, eventTime e, unitId u, visitSeq q)` creates `visitRef = {mode: "occupancy-join", source, arrivalEventId: a}` with `enter = e`. An accepted departure `(arrivalEventId a, eventTime l)` is a leave claim on `a`; one distinct claimed value gives `leave = l`; two claims with different `eventId` and different leaves stay unapplied and add `correction-conflict`; a claim whose arrival is unknown is pending until the arrival arrives or S3 expires it. An error declaration on the arrival: `retraction = {reason: "error-declaration", errorReason: <CBV URN>}`; arriving before its original it is a tombstone and the later original confirms it (the visit is never `provisional`); a later copy of the original restores nothing. An error declaration on a departure cancels that leave claim; the visit is open again. Two differing declarations for one event are both `conflict correction-conflict` and the visit is `insufficient correction-conflict` until a valid cancellation resolves it. An accepted occupancy input on a visit settled at admission (`leave` valid and `admissionW >= leave + L`) is `accepted late-after-settlement` (spec 296, 629). `attributionEnd` of an open visit = the `enter` of the earliest non-retracted arrival at the read point with `enter` greater than this visit's, else null.
- **S5 Status** (spec 343, 498). Settled: `leave` valid and `W1 >= leave + L`. Precedence: error declaration on the arrival -> `retracted error-declaration`; arrival or departure content conflict -> `retracted input-conflict`; a decided overlap (S7) -> `retracted overlap`; any insufficiency reason -> `insufficient`; closed and settled -> `complete`; else `provisional`. `insufficiencyReasons[]` (sorted by code, always listed): `missing-departure` when open and `W1 >= enter + H`; `correction-conflict`; `session-gap`, `session-ambiguous` (S8); `missing-periods`, `bad-share-exceeded`, `no-observed-periods`, `unknown-activity` (S6); `inconsistent-visit-boundary`, `missing-visit-close` (P2). When the other overlapping visit is cancelled the retraction is removed.
- **S6 Evaluation interval, coverage, aggregates, evidence** (spec 346-353 as serialized by Task 1's line 353). `J = [enter, leave)` closed; open: `[enter, min(W1, attributionEnd, enter + H))` over the bounds that exist; coverage only when `J` is non-empty. `slot(t) = floor((t - enter) / P)`; `slotsOf(J) = ceil((end - enter) / P)` (the last partial slot counts). Coverage observations: the deduplicated, non-conflicting observations (S2) of the station's equipment with `sourceTs` in `J` whose quality-blind candidate (S9's window rule with the quality, finiteness and session steps skipped) is this visit: the Good ones are the attributed observations, the Bad and rejected-Uncertain ones are `unattributed bad-quality` rows that still count for `observedPeriods`, and only the raw Bad ones (severity bits, spec 137) count for `badSlots`, so a rejected Uncertain observation never raises the bad share; `ambiguous`, `outside-session`, `session-ambiguous`, `non-finite` and `no-source-timestamp` samples count for no visit (Task 1's line 491 insertion). Per measurement member `m` of the station or a tool under it: `observedPeriods(m)`, `missingPeriods(m) = slotsOf(J) - observedPeriods(m)`, `badSlots(m)`, `badShare(m) = badSlots / observedPeriods` (0 when 0). Required members = every measurement member of the station and its tools (spec 138). Reasons: `missing-periods` when a required member has `missingPeriods > maxMissingPeriods`; `bad-share-exceeded` when a measurement member has `badShare > maxBadShare`; `no-observed-periods` when a required member has `observedPeriods = 0`. Activity per equipment `q` with an activity member: a Good observation at `t` supports `[t, min(nextObservationTs_q, t + P))`; `true` active, `false` inactive; Bad, a rejected Uncertain value, unsupported time and an unknown session interval are unknown; clipped to `J`, adjacent intervals of one kind merged, a `true` never extended to the next `false`; `unknownActivityPeriods(q)` = slots of `J` overlapping an unknown part; `unknown-activity` when any `q` has `unknownActivityPeriods > maxMissingPeriods`. `members[]`: one entry per required member and per any other measurement member with `>= 1` coverage observation in `J`, sorted by `mappingId`: `{mappingId, metric, equipmentPath, valueType, observedPeriods, missingPeriods, badSlots, <aggregate>}` with Double `{count, min, max, last}`, Boolean `{count, trueCount, last}`, String `{count, distinctCount, last}` over accepted-quality observations only (Good, plus Uncertain when the rule's `uncertainInput` is `propagate`; Bad and rejected-Uncertain never; `count 0` gives null `min`/`max`/`last`, `trueCount`/`distinctCount` 0; a required member with no observation at all is listed with `observedPeriods 0`, which is what `no-observed-periods` reads); the activity support set is the same accepted-quality set (spec 351: "an accepted-quality true is active"); `last` by `sourceTs`, ties by `sampleId` fields in contract order. `equipmentEvidence[]` sorted by `equipmentPath`, one entry per equipment of the station with a contributing observation of any role: `{equipmentPath, memberRefs (sorted), activityIntervals [[start, end)...] (time order), inactiveIntervals, coverage {observedPeriods (min over its measurement members), missingPeriods (max), badSlots (max), unknownActivityPeriods, sessionGaps (gap intervals of its device overlapping J), status}, samplesDigest}`; an equipment with no measurement member takes its activity member's `observedPeriods` and `missingPeriods` and `badSlots` 0; `status` `unknown` when `unknownActivityPeriods > 0` or an unknown session interval overlaps `J`, else `incomplete` when `missingPeriods > maxMissingPeriods`, `sessionGaps > 0` or a measurement member's `badShare > maxBadShare`, else `complete`; `samplesDigest` = `Canonical.digest` of the distinct contributing observations `{device, metric, sourceTs, value, uaStatusCode}` sorted by `device`, `metric`, `sourceTs`, then canonical bytes. Visit-level `coverage = {expectedPeriodMs, observedPeriods (min over the equipmentEvidence[] entries' coverage), missingPeriods (max), badSlots (max), sessionGaps (sum over equipment entries)}` (equal to the extremes over the required members when measurement members exist; for a station whose only member is the activity member `Running`, like Station2 of the fixtures, the required set is empty and the values are the activity member's `observedPeriods`/`missingPeriods` and `badSlots` 0, Task 1's line-353 insertion); visit-level `samplesDigest` over every contributing observation of every equipment.
- **S7 Overlap** (spec 139, 498). Decided only between two visits at the read point that both have a valid arrival and a valid leave claim, by `[enter, leave)` intersection; both `retracted overlap` unless a higher-precedence retraction applies; an open visit never retracts another; recomputed whenever a claim changes; never first or last.
- **S8 Sessions** (spec 68, 271-278, 502 as extended by Task 1). Per device: `seq-gap {from, to}` gives a gap interval `[from, to)`; `session-down {from}` gives `[from, nextUp.at)` or `[from, W1)` while open; `stale-death` opens no gap; `session-ambiguous {detectedEventTime}` gives an unknown interval `[lastObservationBefore(detectedEventTime), nextUp.at or W1)`; `definition-changed` marks the session so its samples are `rejected definition-mismatch`. A gap interval of a device of the station overlapping `J` of a non-retracted visit adds `session-gap`; an unknown interval adds `session-ambiguous`. Open gaps end at `W1` and count only inside `J`.
- **S9 Sample attribution** (spec 491 as extended by Task 1, 364-368). For an admitted sample (device `d`, member `m`, `sourceTs t`), in order: no `sourceTs` -> `unattributed no-source-timestamp`; measurement conflict -> `quarantined measurement-conflict`; a non-finite Double (`NaN`, `Infinity`, or an overflowing literal in either notation: `1e400` and `-1e400`, which Jackson's `Double.parseDouble` and Python's `json` map to a signed infinity, and an integer literal beyond the double range such as `1` followed by 400 zeros, which Jackson's `BigInteger.doubleValue()` maps to a signed infinity and the oracle maps the same way after catching Python's `OverflowError`) -> `unattributed non-finite`, the message's other members unaffected; a finite value is one value whatever its notation (`9007199254740992`, `9007199254740992.0`, `9.007199254740992e15` parse to the same double on both sides; the canonicalizer's integer-domain refusal never reaches a sample's reason); Bad, or Uncertain under `reject` -> `unattributed bad-quality`; `t` inside a `session-down` interval of `d` -> `unattributed outside-session`; `t` inside an unknown interval of `d` -> `unattributed session-ambiguous`; else candidates = visits of the station whose window `[enter - tolE, upper)` contains `t` (`upper = leave + tolL` closed; `attributionEnd` when open and known; else unbounded): two or more non-retracted candidates -> `unattributed ambiguous` (contributes to no visit, not an overlap cause); exactly one non-retracted -> `attributed`; no non-retracted candidate -> `attributed` to the retracted candidate whose actual `[enter, leave)` contains `t`, and `unattributed ambiguous` when none or several do; no candidate -> `unattributed no-occupancy`. The quality-blind candidate (the same rule evaluated before the quality, finiteness and session steps) decides which visit a Bad or rejected-Uncertain observation counts for in S6. `active` = the accepted-quality value of `d`'s activity member in the same message (Good, or Uncertain when the rule's `uncertainInput` is `propagate`), else `"unknown"`. Row body `{sourceTs, state, reason (null when attributed and no diagnostic applies), active, attributedTo}`. A change of state, reason or `attributedTo` is a new revision; an `unattributed` sample with reason `no-occupancy` or `ambiguous` and `active true` is unattributed activity (spec 368).
- **S10 Timers and `W1`** (spec 491, 495-496, 508; Task 1's line 588). Deadlines per visit: `leave + L` (settle), `enter + H` (missing departure), `onlineExpiresAt` (expiry); the `DeadlineIndex` is ordered by `(deadline, logical key bytes, purpose)`, deduplicated, with obsolete registrations removed; a timer fires when `W1 >= deadline`, after every input of the cut, in that order, and recomputes its visit. The harness computes `W1(s) = max sourceTs of telemetry routed to s over everything delivered through the cut - O - 1`, never decreasing.
- **S11 Emission** (Decision 6; spec 331, 412). A work record row is emitted when its `bodyDigest` changes; `revision` from 1 per key per generation; `supersedesDigest`; `cause` `{"kind": "input", "inputKind", "identity"}` or `{"kind": "timer", "deadline"}` (`replayed` in PR 5b); `basisRevisions []`. No stage-1 row is ever deleted. Progress per cut after the timers: `{type: "progress", generation, station, cutId, producerEpoch: 0, W1, admissionW, barrierRefs, restartEpoch: 0, recoveryRef: null, mergedCutIds: [], initEvidence (first progress only: {declarationOffset: the offset of this generation's latest declaration in `recovery-control.jsonl` as read at initialization, endOffset: the file's record count H at that read, restore: "none"}; spec 408: the chosen declaration's real offset and H, never constants, because every `generation new` appends a declaration to the shared control log)}`; the station's ledger progress the same cut.

- [ ] **Step 10.1** `core/.../core/NegativeControls.java` (created here, B2: the record of the eight boolean flags, `processingTime, dropLate, noCascade, firstWins, stationGranularity, ignoreSessionGap, wrongRulesPin, lenientOracle`, with `NegativeControls.NONE`; in this PR every flag is false and no code path reads one yet, so the record is a constructor parameter of `Stage1` from the first commit and PR 6 only adds the behaviours). `Events.java`: `sealed interface Event permits Admit, Timer, CutClose`; `Admit(InputRecord record, int kindRank)`, `Timer(long deadline, JsonNode key, String purpose)`, `CutClose(long cutId, long w1, List<JsonNode> barrierRefs)`. `Stage1.decide(Stage1State state, Event e, Long watermark) -> Result(state, List<Object> outputs)` (`watermark` = `admissionW` for `Admit`, nullable before the first progress; `W1` for `Timer` and `CutClose`); outputs are `Row`, `Disposition`, `Progress.Stage1`, `Progress.Ledger`, `QuarantineRecord`. `Stage1State`: `visits`, `pendingLeaves`, `tombstones`, `deliveries` (S2), `measurements`, `samples`, `sessions`, `deadlines: DeadlineIndex`, `revisions`, `lastW1`, `admissionW`, `progressCount`; versioned `toJson()`/`fromJson()` for the adapters' stores.

- [ ] **Step 10.2** `DeadlineIndexTest`: inclusive `deadline <= W`; several purposes at one instant in key order; cancellation; every expired deadline returned after a large jump; serialized round trip. `CoverageTest`: `J = [0, 60500)`, `P 1000` -> 61 slots; observations every second -> `observedPeriods 61`; a 4-slot hole -> `missing-periods` at `maxMissingPeriods 3`; activity true 5-54 s, false elsewhere, one Bad at 30 s -> `activityIntervals [[5000, 30000), [31000, 55000)]`, `inactiveIntervals [[0, 5000), [55000, 60500)]` (the `false` observed at 60,000 supports `[60000, 61000)`, clipped to J's end 60,500), `unknownActivityPeriods 1`.

- [ ] **Step 10.3** `Stage1OccupancyJoinTest` (red first; events built from the `smoke` records and inline literals through a tiny in-test driver; expected bodies typed from the sheet): `aVisitIsProvisionalThenCompleteAtSettlement`; `samplesBeforeTheirArrivalAreAttributedLater` (2b); `anIdenticalRedeliveryIsADuplicateWithNoStateChange` (5); `aResentArrivalWithAnotherUnitIsAConflict` (10d); `anErrorDeclarationRetractsTheVisit` (10) and `anErrorDeclarationBeforeItsOriginalIsATombstone` (10b: same final digest in both orders); `anErrorDeclarationOnTheDepartureReopensTheVisit` (10c); `twoDeparturesWithDifferentLeavesAreACorrectionConflict`; `aCancellationResolvesADepartureConflict`; `twoClosedOverlappingVisitsAreBothRetractedAndRecoverWhenOneIsCancelled` (4, S7); `aBadShareAboveTheRuleIsInsufficient` (3: 7 Bad of 60 observed slots in the 60 s visit, `badShare 7/60 = 0.117 > 0.1`; the Bad rows are `unattributed bad-quality` yet counted through the quality-blind candidate); `rejectedUncertainOnlyHasBadSlotsZero` (7 Uncertain torque observations under `reject`: `observedPeriods 60`, `badSlots 0`, `count 53`, no `bad-share-exceeded`; M3 of round 5); `aSeqGapOverlappingTheVisitIsInsufficientSessionGap` (8); `aStaleDeathChangesNothing` (25a); `aSessionAmbiguousIntervalMakesCoverageUnknown` (25b); `aSampleInTwoNonRetractedWindowsIsAmbiguous`; `aSampleInOnlyARetractedWindowAttributesToIt`; `aShiftedClockMovesSamplesAcrossVisitsAsTheBandsSay` (9b); `lateAfterSettlementIsAcceptedWithItsReason` (10f); `uncertainPropagateAggregatesAndSupportsActivity` (a rule copy with `uncertainInput: propagate` vs `reject`: an Uncertain `Running=true` enters the aggregate, the activity support and the sample row's `active true` under `propagate`; under `reject` the same sample is `bad-quality` with `active "unknown"`); `aRedeliveredMessageGetsOneDuplicatePerMember` (`part member:<metric>`, spec 437); `aDisallowedCorrectionReasonIsSchemaInvalid` (`refs.check correction.reasons`); `admissionBoundaries` (`sourceTs = admissionW - H`, `+1`, `-1`); `expiryDropsOnlineStateAndKeepsRows`; `smokeSamplesDigest` (pins the literal `samplesDigest` of the smoke visit; the oracle pins the same); `aStation2VisitBodyHasNoMeasurementMembers` (a Station2 visit of the baseline shape: `members []`, one `equipmentEvidence` entry `{equipmentPath station2, memberRefs [station2-running], activityIntervals [[enter, leave)], inactiveIntervals [], coverage {observedPeriods 60, missingPeriods 0, badSlots 0, unknownActivityPeriods 0, sessionGaps 0, status complete}}`, visit `coverage {expectedPeriodMs 1000, observedPeriods 60, missingPeriods 0, badSlots 0, sessionGaps 0}`, `status complete`, `insufficiencyReasons []`; the exact body and its digest are pinned, and the oracle test pins the same literal); `visitSeqAboveInt32IsPreservedInTheBody` (`visitSeq 2147483648` appears unchanged in the work record body); `decideIsDeterministicAndReadsOnlyItsArguments`.

- [ ] **Step 10.4** Implement to the sheet. `mvn -q -pl core -Dtest='Stage1OccupancyJoinTest,CoverageTest,DeadlineIndexTest' test` -> `Tests run: 31, Failures: 0`. Commit `feat(core): stage-1 decision function (occupancy-join), coverage, work record and sample bodies, deadline index`.

### Task 11: Reference harness for stage 1: stations, cuts, `W1`, timers, journals, ledgers, cut log, run record

**Files:**
- Create: `core/src/main/java/yggdrasil/forseti/core/generation/{Generation,GenerationStore}.java` (B2: the row record and the store interface the harness reads), `cli/src/main/java/yggdrasil/forseti/cli/generation/FileGenerationStore.java` (read side only; `append` unsupported until PR 5a)
- Create: `cli/src/main/java/yggdrasil/forseti/cli/harness/{ReferenceEngine,StationTask,RejectCollector,ContractOrder,JournalFiles,CutLog,RunRecord}.java`
- Create: `cli/src/test/java/yggdrasil/forseti/cli/harness/ReferenceEngineTest.java`

`forseti run --engine core --recording <dir> --registry <dir> --state <dir> --generation <n> --out <run dir> [--seed <n>] [--shuffle] [--subscribe <topic>=<p,p>] [--artifact-sha256 <hex>]`. The harness is the engine-independent reference adapter; it owns only Decision 12's adapter duties (partitioning, timers, state, side outputs, sinks). It never writes a generation row and never verifies a head: it runs the registered generation from its pinned snapshot (spec 553, "a replay uses the snapshot"; Task 1's line 553 insertion). This task creates the core types the harness reads (B2): `core/.../core/generation/Generation.java` (the row record of spec 540-548 with `toJson`/`fromJson`; `generation`, `headVersion`, `replayFrom` offsets and `at` written with `Json.putSafe` and read with `Json.safeLong`, `generation >= 1`) and `GenerationStore.java` (the interface: `append`, `all`, `current(scope)`, `byNumber`), plus the read side of `cli/.../cli/generation/FileGenerationStore.java` (`<stateDir>/generations.jsonl`, one transport record per line; `append` throws `UnsupportedOperationException` until PR 5 adds the locked, durable writer). Until PR 5 a test writes the one `registered` row (carrying `releaseEvidence`) by hand; PR 5 adds the CLI, the policy and the writer.

- [ ] **Step 11.1** `ReferenceEngine.run(...)`: (1) loads the generation row (refuses `generation-unregistered`), reconstructs the `Head` record from `releaseEvidence` (stream, release id and version, `releaseSha256`, `statementSha256`, `headVersion`) and calls `ReleaseReader.read(mappings, head)`, which verifies `release.json` and every payload file against the pinned digests (`provenance-mismatch` otherwise) without `HeadVerifier`; loads the `RuleSet` with the recording's `definitions` and refuses when its `rulesDigest` differs from the generation's (`generation-rules-mismatch`); (2) opens the recording, verifies digests, checks `routing.json` against the rule set, applies `--subscribe` overrides; refuses a first batch without telemetry on every subscribed raw telemetry partition (spec 406); (3) per batch `k`: collects records with offsets in `(throughOffsets[k-1], throughOffsets[k]]` per subscribed partition (with `throughOffsets[0] = -1`, so the first batch starts at offset 0 and an empty partition contributes nothing while its barrier is still counted), parses (S1), routes to a `StationTask` or the `RejectCollector`, captures `admissionW` per station before applying, sorts the cut's inputs (`ContractOrder`; with `--shuffle` the records are shuffled by `--seed` first, the sort makes the order irrelevant), calls `Stage1.decide(state, Admit, admissionW)` per input, computes `W1(s)` (S10), fires `Timer` events for deadlines `<= W1` in index order, calls `CutClose`, writes rows (envelope: `evaluationGeneration`, `eventTimeAt = W1`, `trigger`, `pins`), dispositions, quarantine records, the two stage-1 progress records (`forseti.work-records` and `forseti.samples` partition `p(s)`, after the cut's rows; the first progress of every stage-1 writer, the station writers and the reject collector alike, carries `initEvidence` from one read of `<stateDir>/recovery-control.jsonl` at the writer's initialization: the offset of the latest declaration whose `generation` is this run's and the record count H at that read), the station's ledger progress, and the reject collector's partition-6 progress (its required barrier set is every subscribed raw telemetry, session and occupancy partition); (4) writes `cuts.json` (per cut `{cutId, perStation {station -> {W1, admissionW, mergedCutIds: [], onlineVisits}}, offsetRanges {topic -> {partition -> [from, to]}} (half-open, distinct from the inclusive `throughOffset`), incidentOffsets}`), `run.json` (`RunRecord`), journals under `<out>/journals/<topic>/<p>.jsonl` (`{"key": <logical key JSON>, "headers": {"forseti-generation": "<n>"}, "value": {"body", "bodyDigest", "envelope"}}` for rows, `{"value": {"delete": {...}}}` for deletes, `{"key": null, "value": {"type": "progress", ...}}` for progress) and `<out>/quarantine/<p>.jsonl`. Pins: `job = {engine: "core-harness", engineVersion: "0.1.0", artifactSha256: <sha256 of the running jar, or the --artifact-sha256 value, or "unpacked">, runId: <UUID>, timestampSource: "payload", idleness: "off", routingDigest}`.

- [ ] **Step 11.2** `ReferenceEngineTest` (on `smoke`): `writesProgressAfterTheCutsRowsOnBothEvidenceTopics`; `W1FollowsThePerStationFormula`; `admissionWIsThePreviousW1`; `everyInputHasADisposition`; `theRejectCollectorWritesPartition6Progress`; `theRunRecordCarriesSeedPinsDigestsAndOffsetRanges`; `twoRunsEmitIdenticalBodiesKeysAndRevisions` (only `runId` differs); `anUnregisteredGenerationRefusesToStart`; `aDifferentRulesDigestRefusesToStart`; `aRunUsesThePinnedSnapshotNotTheCurrentHead` (the registry's head file replaced after `new`: the run still succeeds on the pinned release; a changed payload byte is `provenance-mismatch`); `aRunWritesNoGenerationRow`; `initEvidencePointsAtItsOwnDeclaration` (two generations registered in one state before either runs: generation 1's business and ledger first progresses carry `initEvidence {declarationOffset 0, endOffset 2}`, generation 2's `{1, 2}`, on every stage-1 writer including the reject collector); `aNonCanonicalizableRecordIsRejectedAndTheRunContinues` (a `mfg.occupancy` line whose value is `[9007199254740992]`: ledger row `rejected schema-invalid`, `refs.digestRule raw-bytes`, every other row unchanged); `aMixedMessageKeepsItsGoodMembers` (a nutrunner1 message with `Torque` `value 1e400`, which the parser maps to +Infinity, and `Running true`: the `Running` sample is attributed as usual and the message advances event time, the `Torque` sample row is `unattributed non-finite`, both member dispositions `accepted` with `refs.digestRule raw-bytes`, and that slot contributes to no visit: `members[nutrunner1-torque]` has `observedPeriods 59`, `missingPeriods 1`, `count 59` for the 60 s visit); `overflowingIntegerAndExponentLiteralsAreNonFinite` (`1e400`, `-1e400`, `1` followed by 400 zeros and its negative as `Torque` values beside `Running true`: `Torque` row `non-finite`, `Running` attributed, both dispositions `accepted`, the run continues); `threeNotationsAreOneMeasurement` (three deliveries of `Torque` at one `sourceTs` written `9007199254740992`, `9007199254740992.0` and `9.007199254740992e15`: one observation, no `measurement-conflict`, the same aggregate `max`, the value printed in the body as `9007199254740992`); `aFirstBatchWithoutTelemetryOnEverySubscribedPartitionIsRefused`.

- [ ] **Step 11.3** `mvn -q -pl cli -Dtest=ReferenceEngineTest test` -> `Tests run: 17, Failures: 0`; before any `java -jar cli/target/forseti.jar run ...` by hand, `mvn -B -ntp -q install` so the jar carries this task's harness. Commit `feat(cli): reference harness for stage 1 (stations, cuts, W1, timers, journals, ledgers, cut log, run record)`.

### Task 12: Stage 1 plc-stamped and cross-check

**Files:**
- Create: `core/src/main/java/yggdrasil/forseti/core/stage1/PlcStamped.java`; Create: `core/src/test/java/yggdrasil/forseti/core/stage1/Stage1PlcStampedTest.java`; Uses: `fixtures/recordings/smoke-plc/**` (built in Task 8 from `fixtures/scenarios/smoke-plc.json`: the smoke visit with `UnitId`, `VisitId`, `VisitPresent` stamped on `station1` and `nutrunner1`, rule `line2-plc`, the same telemetry tail)

Rule sheet addition (spec 135, 261, 506; `visitRef = {mode: "plc-stamped", namespace, station, visitId}`, `readPoint` = the station path, `visitSeq` null, `unitId` from the identity members):
- **P1 Coherence.** For an equipment `q` with an identity entry, a message is coherent when its identity (`UnitId`, `VisitId`, `VisitPresent`), activity and measurement members of `q` all carry a `sourceTs` and all are equal; otherwise every sample of that message for `q` is `unattributed incoherent-snapshot`.
- **P2 Lifecycle per `(namespace, station, visitId)`.** On each identified equipment the visit start is the coherent `VisitPresent=true` snapshot with the earliest `sourceTs`; the end is the earliest later coherent `VisitPresent=false` snapshot with the same `VisitId` (its measurements are not attributed; it keeps the IDs). Late snapshots rebuild the lifecycle. A `true` with the same `VisitId` after the end -> `inconsistent-visit-boundary`. A `VisitId` change while `VisitPresent` stays `true` -> the earlier visit `insufficient missing-visit-close` (open; `attributionEnd` = the change snapshot's `sourceTs`). Neither a `false` nor a change -> `missing-departure` when `W1 >= enter + H`. Identified equipment of one station visit must agree on `enter` and `leave`, else `inconsistent-visit-boundary`. The same `VisitId` with a different `UnitId` is a conflict: the visit `retracted input-conflict`, the identity samples of the conflicting snapshots `conflict identity-conflict`. `Running=false` never ends a visit. Settlement at `leave + L`.
- **P3 Attribution.** A coherent sample of `q` attributes to the visit whose `[start, end)` on `q` contains its `sourceTs` (no tolerance, no time join). With `crossCheck: occupancy`, a non-retracted occupancy visit at the station whose `[enter, leave)` contains `sourceTs` with another `unitId` marks the row `attributed` with `reason identity-mismatch`, attribution unchanged; with `crossCheck: none` the occupancy stream is not read.

- [ ] **Step 12.1** `Stage1PlcStampedTest`: `aVisitIsFramedByTheEarliestTrueAndTheEarliestLaterFalse`; `theEndSnapshotIsNotAttributed`; `aLateEarlierStartRebuildsTheLifecycle`; `aTrueAfterTheEndIsInconsistent`; `anIdChangeWithoutFalseLeavesMissingVisitClose`; `equipmentMustAgreeOnBoundaries`; `aMixedSnapshotIsIncoherent` (11c); `crossCheckMarksIdentityMismatchWithoutChangingAttribution` (11); `crossCheckNoneReadsNoOccupancy`; `theSameVisitIdWithAnotherUnitIsAConflict`; `runningFalseNeverEndsAVisit`.

- [ ] **Step 12.2** `mvn -q -pl core -Dtest=Stage1PlcStampedTest test` -> `Tests run: 11, Failures: 0`; `ReferenceEngineTest.runsThePlcSmokeRecording` added (1 complete visit). Commit `feat(core): plc-stamped attribution lifecycle and occupancy cross-check`.

### Task 13: Stage-1 harness assertions, pull request 2

**Files:**
- Modify: `cli/.../gate/CoreGate.java`, `gate/core-assertions.json` (+6)

- [ ] **Step 13.1** Assertions (`S-*`, run on the committed `smoke` and `smoke-plc` recordings and inline sequences, no oracle yet):

| Id | What runs |
|---|---|
| `S-SMOKE-VISIT` | the harness on `smoke`: one work record `complete` at the last cut, 5,684 sample rows (4,060 messages x their members), the pinned `samplesDigest`, the transition `provisional` at the cut ending at scan `T0 + 1410000` -> `complete` at the cut ending at `T0 + 1411000` |
| `S-SMOKE-PLC` | the harness on `smoke-plc`: one `complete` visit with `visitRef.mode plc-stamped`, `readPoint` the station path |
| `S-REVISIONS` | every key's revisions contiguous from 1, no equal consecutive digests (the `JournalFold` violations are empty) |
| `S-PROGRESS` | both evidence topics carry progress per cut after the cut's rows; the ledger and partition-6 progress exist per cut; `admissionW` equals the previous `W1`; `initEvidence` only on the first |
| `S-W1` | reported `W1` equals the per-station formula at every cut |
| `S-DETERMINISM` | two runs with different `--seed` and `--shuffle`: identical bodies, keys and revisions at every cut |

- [ ] **Step 13.2** Rebuild the jar first, because the shade plugin runs only at `package` and the local gate otherwise runs the previous PR's jar: `mvn -B -ntp -q install` (core and cli, tests included), then `java -jar cli/target/forseti.jar gate --engine core --out build/gate --python "$(command -v python)"` -> `assertions run 24 / expected 24`, exit 0. Commit `feat(cli): stage-1 harness assertions in the core gate`. Pull request 2: Tasks 10-13 (5 commits); CI green. PR body: the rule sheet S1-S11 and P1-P3 as applied (pointing at the Task 1 spec lines), the gate line. Wait for the merge; rebase PR 3.

## Chunk 4: Impact, progress and cascade (PR 3)

Branch: `git switch -c feat/step3-impact-and-progress main` after PR 2 merged.

### Task 14: `ProgressTracker` and the stage-2 decision function

**Files:**
- Create: `core/src/main/java/yggdrasil/forseti/core/stage2/{Events2,ProgressTracker,Stage2State,Stage2,Incidents,Relatedness,DecisionTable,SetCoverage,Assessment,AffectedSet}.java`
- Create: `core/src/test/java/yggdrasil/forseti/core/stage2/{ProgressTrackerTest,Stage2Test,RelatednessTest,SetCoverageTest}.java`

**Stage-2 rule sheet.** Per station `s`; inputs are the stage-1 rows of partition `p(s)` of `forseti.work-records` and `forseti.samples` (current revisions, deletes applied), their stage-1 progress records, and incidents routed to `s`.

- **T1 Incident routing and dispositions** (harness side; `forseti.dispositions-stage2`, partition 6 unroutable; spec 298-315, 449). Schema failure `rejected schema-invalid`; `kind` outside `kinds[]` `rejected schema-invalid` (`refs.check kinds`); the line-308 contract of `revisionEventTime` (Task 1's vocabulary insertion): revision 1 with `revisionEventTime != detectedAt`, or a revision whose `revisionEventTime` is below that of an accepted lower revision or above that of an accepted higher revision of the same `(source, incidentId)`, judged by revision number whatever the arrival order, is `rejected schema-invalid` (`refs.check revisionEventTime`) and not applied; `equipmentRef` outside `incidentScope[]`, or `stationRef` not the ancestor-or-self station of `equipmentRef`, or not a rule station: `rejected outside-incident-scope`; routed to `stationRef`. Identity `(source, incidentId, revision)`: identical redelivery `duplicate identical-redelivery`; differing content for one revision `conflict identity-conflict` for every variant (that revision is unusable; the current revision is the highest accepted one); a revision whose `stationRef` differs from the first accepted revision `rejected station-change`; a lower revision after a higher `duplicate superseded` (its disposition, spec 315) and never current; a higher revision `accepted`, becomes current. Two things are kept apart: the *current* revision (the highest valid one, which alone is judged) and the *delivered* valid revisions (every revision that passed the schema, scope, station and `revisionEventTime` checks, whatever its disposition), whose intervals T3 accumulates for the row set; a superseded lower revision is therefore `duplicate superseded` in the ledger and still widens the row set (spec 515: "the intervals of the delivered incident revisions"), so the final key set and bodies do not depend on arrival order. (Spec 315 governs the disposition and spec 515 the row set; they do not conflict once the two are separated.)
- **T2 Progress** (spec 511, 426-435). `ProgressTracker.observe(topic, station, Stage1Progress)`; `W2(s) = min(W1 of the last work-records progress, W1 of the last samples progress)`; `ready(s, cutId)` when both evidence topics carry progress `cutId` with equal `mergedCutIds` and the six incident barriers of the cut (and of every merged cut) were collected; processed barriers are kept and never applied twice; a missing cut without merged-cut evidence is waited for; stage-2 timers fire with `deadline <= W2(s)` after the cut's inputs, in index order; stage-2 progress `{type: "progress", generation, station, cutId, producerEpoch: 0, W2, stage1ProgressRefs [{topic, partition, cutId}], incidentOffsets [{cutId, partition, throughOffset}] (6 x (1 + mergedCutIds.size())), mergedCutIds: []}` to partition `p(s)` of `forseti.affected` and `forseti.affected-sets` after the cut's rows; the stage-2 ledger progress likewise; the stage-2 reject collector's progress on partition 6 (required barriers: the six incident partitions; it never skips). Incidents, barriers and recovery declarations never advance `W2`.
- **T3 Bases and the row set** (spec 515). The basis of a visit is its current work record row. The row set of incident `I = (source, incidentId)` at `s` is every unit with a visit claim at `s` whose interval is `related` or `possibly-related` (T4) to the interval of *any* valid revision of `I` delivered so far, superseded lower revisions included (incident-side widening accumulates over delivered revisions, spec 515; only conflicting or rejected revisions contribute nothing). Judgement uses the current revision and the current bases. A unit whose bases are related or possibly-related to no delivered revision any longer ends with a `delete`; if it appears again it continues with the next revision. Cancelled bases stay in state for `workRecordsMs` after `leave` (or `enter` when open) and are evidence only through the retraction rows of T6.
- **T4 Relatedness** (spec 517). `V = [enter, leave)` (`leave` null when open), `S = [suspectFrom, suspectTo)` (`suspectTo` null when open). `any-overlap`: both ends known -> `related` iff `enter < suspectTo && leave > suspectFrom`, else `unrelated`; visit closed, incident open -> `related` iff `leave > suspectFrom`; visit open -> `unrelated` if `suspectTo != null && enter >= suspectTo`, else `related` if `enter >= suspectFrom`, else `possibly-related`. `fully-within`: `related` iff both ends known and `suspectFrom <= enter && leave <= suspectTo`; `possibly-related` when a needed end is open and the known ends do not exclude it; else `unrelated`. `enter-within`: `related` iff `suspectFrom <= enter && (suspectTo == null || enter < suspectTo)`; never `possibly-related`.
- **T5 Positive evidence** (row 2; spec 524). `equipment-evidence`: a related `complete` basis whose `equipmentEvidence[]` entry for `incident.equipmentRef` has an `activityIntervals` element intersecting the current `S` clipped to `[enter, leave)`; reason `equipment-activity`. `station-visit`: any related `complete` basis; reason `station-visit`. Incomplete evidence goes to `evidenceWarnings[]` and never removes positive evidence.
- **T6 Decision table** (spec 519-531), per `(incident, unit)` over the unit's related and possibly-related bases at `s`, first match: (1) `withdrawn` -> `onWithdrawn`: `not-affected` -> `not-affected-within-scope incident-withdrawn`, `insufficient` -> `insufficient-evidence incident-withdrawn`; (2) T5 -> `affected-candidate equipment-activity|station-visit`; (3) no related and no possibly-related basis -> `not-affected-within-scope interval-narrowed`; (4) a related basis with `correction-conflict` -> `insufficient-evidence correction-conflict`; (5) a related or possibly-related `provisional` basis -> `insufficient-evidence basis-provisional`, an `insufficient` one -> `basis-insufficient` (both present: `basis-provisional`); (6) a related basis retracted for overlap, input conflict or `incorrect_data` -> `insufficient-evidence basis-retracted-overlap|basis-retracted-input-conflict|basis-retracted-error-declaration`; (7) a related `complete` basis whose judged interval `I = [enter, leave) ∩ S` (non-empty) for `equipmentRef` is entirely covered by that equipment's `inactiveIntervals` (spec 529: inactive support with no unknown or missing period inside `I`; support intervals exist only where accepted-quality observations are, so an uncovered part of `I` is exactly an unknown or missing period there; gaps outside `I` do not matter and the whole-visit counters are not consulted) -> `not-affected-within-scope no-equipment-activity`; (8) every remaining related basis retracted with `did_not_occur` -> `onBasisRetracted`: `by-cause` -> `not-affected-within-scope basis-retracted-error-declaration`, `always-insufficient` -> `insufficient-evidence basis-retracted-error-declaration`; (9) else `insufficient-evidence basis-insufficient`.
- **T7 Assessment body** (spec 372-379). Key `(source, incidentId, unitId)`; `{status, reason, incident (the current revision's twelve fields), basis[] [{visitRef, bodyDigest}] (related or possibly-related bases, sorted by canonical `visitRef` bytes), evidenceWarnings[] [{visitRef, insufficiencyReasons}] (bases that are not `complete`), rulesDigest}`. A revision triggers on an incident revision or a basis digest change (spec 533); emitted only when the digest changes.
- **T8 Affected set body** (spec 383-400 as fixed by Task 1's lines 392-398). Key `(source, incidentId)`; `{incident, units[] [{unitId, status, reason, bodyDigest}] sorted by unitId, resolution, coverage {...}}` (exactly the spec's field list; the affected set carries no `rulesDigest`, unlike the work record and the assessment). Scoped devices `[equipmentRef]` under `equipment-evidence`, `[stationRef]` under `station-visit`; range `R = [suspectFrom, suspectTo)` closed, else `[suspectFrom, lastDeliveredObservation(scoped devices) + P)`; slots of width `P` from `suspectFrom`, last partial counted; `unattributedActivityPeriods` = slots per scoped device holding a sample row of its activity member with `state unattributed`, reason `no-occupancy` or `ambiguous` (spec 368: an ambiguous sample with activity true counts as unattributed activity), and `active true`, summed; `unknownActivityPeriods` = slots per scoped device with no accepted-quality activity observation (Good, or Uncertain under `uncertainInput: propagate`; a slot with none, or only `bad-quality`, `session-ambiguous` or `outside-session` rows, is unknown), excluding slots counted as unattributed activity, summed; `provisionalUnits` = units with reason `basis-provisional`; `insufficientUnits` = `insufficient-evidence` with any other reason; `sessionGapOverlaps` = bases whose `insufficiencyReasons` contain `session-gap` or `session-ambiguous`; `open` = status `open`; `settled` = `suspectTo != null && W2(s) >= suspectTo + L` (true when withdrawn); `beyondRetention` = `suspectFrom < revisionEventTime - workRecordsMs`, once per revision; `retentionGap` = the manifest's `evidenceRange` of the scoped devices does not cover `R` (or `[suspectFrom, W2)` while open); `status` `unknown` when `unknownActivityPeriods > 0` or any basis is `session-ambiguous`, else `incomplete` when `unattributedActivityPeriods > 0 || provisionalUnits > 0 || insufficientUnits > 0 || sessionGapOverlaps > 0 || open || !settled || beyondRetention || retentionGap`, else `complete`. Withdrawn: `resolution withdrawn`, `open false`, `settled true`, never `complete`. Set coverage changes revise only the set row.
- **T9 Timers** (spec 513). `suspectTo + L` (settlement); every cut re-evaluates coverage and open-end relatedness as `W2(s)` advances (emits only on a digest change); `leave + workRecordsMs` (or `enter + ...` open) drops the basis from stage-2 state. A narrowed or withdrawn revision is recomputed on delivery.
- **T10 Cascade** (H-cascade). A new revision or delete of a work record re-judges, in the same cut, every unit row of every incident that cites that `visitRef` or whose row set it now enters or leaves (both the old and the new relationship); a sample row change updates the sample-derived coverage. After every completed cut, every `basis[].bodyDigest` of every live assessment equals the current digest of that `visitRef` in the folded work records (`J-BASIS-CURRENT`).

- [ ] **Step 14.1** `ProgressTracker.java` (core): `observe`, `w2(station) -> OptionalLong`, `ready(station, cutId)`, `fire(station) -> List<Deadline>`. `ProgressTrackerTest`: `W2 is the minimum of the two topics`; `not ready until both topics carry the cut and the six incident barriers`; `unequal mergedCutIds is an IllegalStateException`; `a decreasing progress is refused`; `incident barriers are counted 6 x (1 + mergedCutIds)`; `a processed barrier is not applied twice`; `timers fire in order up to W2`.

- [ ] **Step 14.2** `Stage2State` (bases by `visitRef`; sample activity index per device and slot; incidents by `(source, incidentId)` with accepted revisions and variants; row sets; deadlines; revisions; `lastW2`), `Events2` (`Basis(Row|Delete)`, `Sample(Row)`, `Incident(InputRecord)`, `Timer`, `CutClose(cutId, w2, stage1ProgressRefs, incidentOffsets)`), `Stage2.decide(state, event, w2) -> Result` with outputs `Row` (assessment, set), `Delete`, `Disposition`, `Progress.Stage2`, `Progress.Ledger`.

- [ ] **Step 14.3** `Stage2Test` (inline work record bodies built with Task 10's body builders; red first): `anIncidentAfterCompleteRecordsMarksToolVisitsCandidates` (12, 16); `anIncidentBeforeTheRecordsStartsEmptyAndGrows` (13); `aNarrowedAndClosedIncidentMarksLaterVisitsIntervalNarrowed` (14); `aWithdrawnIncidentFollowsOnWithdrawn` (15, both values); `unattributedActivityInsideTheIntervalKeepsCoverageIncomplete` (17); `aRetractedBasisWithDidNotOccurMakesTheCandidateNotAffected` (18, `by-cause` and `always-insufficient`); `aLateAttributionAddsTheUnit` (19); `aProvisionalBasisIsInsufficientEvidence`; `aDepartureThatNarrowsAnOpenVisitDeletesTheRow`; `rowSetWidensAcrossRevisionsButJudgesTheCurrentOne`; `rowSetIsTheSameInBothArrivalOrders` (a wide revision 1 and a narrow revision 2: delivered 1 then 2, and 2 then 1 where revision 1 is `duplicate superseded`; the final key set, every assessment body and the set body are identical, the outer unit `not-affected-within-scope interval-narrowed` in both runs); the oracle has `test_row_set_is_the_same_in_both_arrival_orders` in `test_stage2.py`; `uncertainActivityUnderPropagateIsNotUnknownCoverage` (sample rows with Uncertain `active true` under `propagate` count as unattributed activity, not as `unknownActivityPeriods`; under `reject` the same slots are unknown; M1 of round 4); `noActivityIsJudgedInsideTheIntersectionOnly` (a `complete` basis with a 2-slot inactive-support gap outside `[enter, leave) ∩ S` is `no-equipment-activity`; the same gap inside it falls through to `basis-insufficient`; M10); `aStationChangeIsRejectedWithoutReplacingTheCurrentRevision`; `aLowerRevisionIsSuperseded`; `revisionEventTimeIsCheckedByRevisionNumber` (revision 1 with `revisionEventTime != detectedAt` rejected; revision 3 arriving before revision 2 with a lower `revisionEventTime` than revision 2's rejects the later-arriving revision 2, not the applied 3; `refs.check revisionEventTime`; spec 308); `aSetOnlyCoverageChangeDoesNotReviseUnits`; `eachDecisionRowInOrder` (parametrized over the nine rows). `RelatednessTest` (three modes, open ends, `enter == suspectTo` unrelated, `leave == suspectFrom` unrelated). `SetCoverageTest` (slots from `suspectFrom`, last partial slot, unattributed vs unknown exclusivity, `retentionGap` from the manifest range including a verified empty range, `beyondRetention` at the boundary and one millisecond outside, once per revision, the `status` ladder).

- [ ] **Step 14.4** `mvn -q -pl core -Dtest='ProgressTrackerTest,Stage2Test,RelatednessTest,SetCoverageTest' test` -> `Tests run: 34, Failures: 0`. Commit `feat(core): ProgressTracker and stage-2 decision function (relatedness, decision table, assessment and affected-set bodies)`.

### Task 15: Stage 2 in the harness, cascade and deletes, stage-2 assertions, pull request 3

**Files:**
- Modify: `ReferenceEngine` (stage-2 tasks fed from the cut's stage-1 outputs and routed incidents; stage-2 reject collector; deletes; `cuts.json` gains `W2`), `JournalFold` (deletes across topics), `gate/core-assertions.json` (+5)
- Uses: `fixtures/recordings/smoke-incident/**` (built in Task 8 from `fixtures/scenarios/smoke-incident.json`: the smoke visit plus one closed `tool-check-failed` incident on nutrunner1 over the visit, keyed by `equipmentRef` on `quality.incidents`, `suspectFrom 600000`, `suspectTo = detectedAt = leave = 660000` (current spec 172: for `tool-check-failed` the producer closes the interval with `suspectTo = detectedAt`; the builder refuses a closed `tool-check-failed` revision 1 whose `suspectTo != detectedAt` or whose `revisionEventTime != detectedAt`), revision 1 `revisionEventTime 660000`, delivered at `1,400,000`, after the visit's settlement bracket; its settlement `suspectTo + L = 1,380,000` equals the visit's. Under the one rule for the telemetry end (Task 8: `lastBusinessEvent` is the latest of every occupancy `eventTime`, every incident `detectedAt` and `revisionEventTime`, every session event time, every overlay `deliverAt` and `declarationTime`, and every listed horizon deadline), `lastBusinessEvent = 1,400,000` (the delivery), `E = 1,400,000 + 751,000 = 2,151,000`: `(2,151,000 - 600,000) / 1000 + 1 = 1,552` scans per device, 5 x 1,552 = 7,760 messages, 7 x 1,552 = 10,864 samples; cuts: 30 s boundaries `630,000 ... 2,130,000` = 51, plus the bracket `cutB` at `1,411,000` (`cutA` is the boundary `1,410,000`), plus the final batch ending at the last scan `2,151,000` = 53 cuts, pinned by `smoke-incident.lock.json`); Create: `cli/src/test/java/yggdrasil/forseti/cli/harness/CascadeTest.java`

- [ ] **Step 15.1** Harness: after stage 1 of cut `k`, each station's `Stage2` receives the cut's stage-1 rows and deletes of `p(s)`, the two stage-1 progress records (through `ProgressTracker`), the incidents routed to `s` (T1; rejections to the stage-2 collector), fires timers `<= W2(s)`, writes rows, deletes, dispositions, stage-2 progress, ledger progress, partition-6 progress. `cuts.json` records `W2` per station (equal to `W1` by construction, spec 588).

- [ ] **Step 15.2** `CascadeTest` (in-process, `smoke-incident` plus inline late inputs): `aStage1RetractionReJudgesTheCitingRowInTheSameCut`; `noLiveAssessmentCitesAStaleDigest`; `aNarrowedOpenVisitEndsWithADeleteAndContinuesRevisions`; `aUnitWithTwoVisitsKeepsPositiveEvidenceWhenOneIsCancelled`; `progressFollowsEveryBusinessDeleteAndDispositionRow`; `aCutWithNoIncidentThenTheFirstIncidentAtOffsetZero` (on `smoke-incident`, back to back: a cut before 1,400,000 completes with six incident barriers whose `throughOffset` is `-1`, stage-2 progress written with `incidentOffsets` of six `-1` entries and no incident applied; the cut that delivers the incident has `throughOffset 0` on its Murmur2 partition and `-1` on the other five, the incident is applied once, and the next cut's barrier for that partition is again `0` with nothing applied twice). `mvn -q -pl cli -Dtest=CascadeTest test` -> `Tests run: 6, Failures: 0`.

- [ ] **Step 15.3** Assertions (`T-*`):

| Id | What runs |
|---|---|
| `T-PROGRESS-TRACKER` | on `smoke-incident`: stage-2 progress per cut after both stage-1 progresses and six incident barriers; `incidentOffsets` count 6; `W2 == W1` per station per cut |
| `T-SMOKE-INCIDENT` | one assessment `affected-candidate equipment-activity`, one set with `units` of one, `settled true` at the last cut, `coverage.status complete` (the smoke incident is closed, inside the evidence range, no gaps) |
| `T-CASCADE` | an error declaration appended to the recording's last batch (a `smoke-incident-cascade` variant written by the test): the assessment becomes `not-affected-within-scope basis-retracted-error-declaration` in the same cut; `J-BASIS-CURRENT`'s check holds at every cut |
| `T-DELETE` | a `smoke-incident-narrow` variant (open visit, late departure before `suspectFrom`): the unit row ends with a `delete`, revision continuity across it |
| `T-LEDGER-2` | stage-2 dispositions per incident delivery; partition-6 progress per cut; `outside-incident-scope` for an incident on `station2` (variant) |

- [ ] **Step 15.4** `mvn -B -ntp -q install` (rebuilds the jar with this PR's core and cli), then the gate -> `assertions run 29 / expected 29`, exit 0. Commit `feat(cli): stage 2 in the reference harness, cascade, deletes and the stage-2 assertions`. Pull request 3: Tasks 14-15 (3 commits); CI green. PR body: the rule sheet T1-T10, the gate line. Wait for the merge; rebase PR 4.

## Chunk 5: Recordings and the independent oracle (PR 4)

Branch: `git switch -c feat/step3-recordings-and-oracle main` after PR 3 merged.

### Task 16: Scenario builder, ground truth, injection overlays, the hand-made scenarios

**Files:**
- Create: `cli/src/main/java/yggdrasil/forseti/cli/recording/{ScenarioBuilder,Overlays,GroundTruth,Telemetry}.java`; Modify: `Main` (`recordings build|verify`)
- Create: `fixtures/scenarios/{baseline,held-back-10min,redelivery,overlap,bad-quality,clock-plus-30,clock-plus-90,corrections,corrections-10b-before,corrections-10b-after,corrections-10c,session,quiet-occupancy,quiet-occupancy-control,plc-stamped,plc-nocheck,plc-held-back,stage2,stage2-early,cascade}.json` (+ `.lock.json` each)
- Create: `cli/src/test/java/yggdrasil/forseti/cli/recording/ScenarioBuilderTest.java`

Hand-made recordings are authored as scenario files and built deterministically by the builder. The scenario files and their locks are committed; the twenty recordings of this task are **not committed** (owner decision of 2026-11-11, replacing the decision of 2026-11-10: only the smoke recordings of Task 8 are committed; every other recording, hand-made or synthesized, is built at gate time): the gate builds each into `build/recordings/<name>/` and verifies it against its committed lock before every run, and a recording whose bytes differ from its lock fails the gate before any run. The lock pins every file's digest, the message, sample and occupancy counts, `firstBatchTelemetryPartitions` and the cut count. `ScenarioBuilderTest` proves that a fresh build of every hand-made scenario has the digests of its lock and that a fresh build of each smoke scenario equals the committed files byte for byte. Size (estimates): scaling the previous rounded recording-size estimates (the 4.5 MB and 90 MB of 2026-11-10) by 5/4 for the fifth device, a 15,760-message recording is about 5.6 MB, so the twenty recordings of this task take about 112 MB under `build/` at gate time and nothing in git; the committed set is the three smoke recordings, 4,060 + 4,060 + 7,760 = 15,880 messages, about 5.6 MB in the working tree at the same bytes per message (about 1.4, 1.4 and 2.8 MB; `smoke-plc` carries more members per message, so somewhat more) and about 0.6 MB in git objects (JSONL packs roughly tenfold); `fixtures/** -text` keeps the committed bytes fixed. Sizes (current spec 626): three stations (the fixtures' rule), 5 devices, 16 visits (8 at Station1 in the four tool patterns, 4 at Station2, 4 at Station3), and, by the builder's rule "telemetry from `telemetryFrom` to `E = max(telemetryTo, lastBusinessEvent) + tailMs`, where `lastBusinessEvent` is the latest of every occupancy `eventTime`, every incident `detectedAt` and `revisionEventTime`, every session event time, every overlay `deliverAt` and `declarationTime`, and every listed horizon deadline (one rule for every scenario, hand-made and synthesized, current spec 613), every 1,000 ms, both ends included, so `scans = (E - telemetryFrom) / 1000 + 1`": `baseline` `E = 2,400,000 + 751,000 = 3,151,000` -> 3,152 scans per device, 5 x 3,152 = 15,760 messages, 7 x 3,152 = 22,064 samples (7 members per scan: 1 + 1 + 1 + 2 + 2); `stage2` (last business event `inc-3` revision 2 at 2,600,000) `E = 3,351,000` -> 3,352 scans, 16,760 messages, 23,464 samples; `cascade` (last at 2,700,000) `E = 3,451,000` -> 3,452 scans, 17,260 messages, 24,164 samples; every other hand-made scenario's count follows the same rule from its last business event and is pinned by its lock (three stations by the owner decision of 2026-11-11, Open point 4; the Station3 visits end at 1,380,000, before the last Station1 departure at 1,920,000, so no scenario's last business event, `E` or scan count moves).

- [ ] **Step 16.1** `fixtures/scenarios/baseline.json`:

```json
{
  "scenarioVersion": 1,
  "name": "baseline",
  "rule": {"stream": "line2", "releaseId": "line2-rules", "version": "1.0.0", "attributionRuleId": "line2-attribution", "attributionRuleVersion": "1.0.0"},
  "mode": "occupancy-join",
  "T0": 1794182400000,
  "seed": 20261109,
  "scanMs": 1000,
  "edge": "edge-line2",
  "devices": {
    "synthetic-site:line2:station1": {"members": {"Running": "Boolean"}},
    "synthetic-site:line2:station2": {"members": {"Running": "Boolean"}},
    "synthetic-site:line2:station3": {"members": {"Running": "Boolean"}},
    "synthetic-site:line2:station1:nutrunner1": {"members": {"Torque": "Double", "Running": "Boolean"}},
    "synthetic-site:line2:station1:nutrunner2": {"members": {"Torque": "Double", "Running": "Boolean"}}
  },
  "telemetryFrom": 0,
  "telemetryTo": 2400000,
  "visits": [
    {"unit": "UNIT0000000000001", "station": "synthetic-site:line2:station1", "enter": 600000,  "leave": 660000,  "tools": ["nutrunner1"], "visitSeq": 1},
    {"unit": "UNIT0000000000002", "station": "synthetic-site:line2:station1", "enter": 780000,  "leave": 840000,  "tools": ["nutrunner2"], "visitSeq": 2},
    {"unit": "UNIT0000000000003", "station": "synthetic-site:line2:station1", "enter": 960000,  "leave": 1020000, "tools": ["nutrunner1", "nutrunner2"], "visitSeq": 3},
    {"unit": "UNIT0000000000004", "station": "synthetic-site:line2:station1", "enter": 1140000, "leave": 1200000, "tools": [], "visitSeq": 4},
    {"unit": "UNIT0000000000005", "station": "synthetic-site:line2:station1", "enter": 1320000, "leave": 1380000, "tools": ["nutrunner1"], "visitSeq": 5},
    {"unit": "UNIT0000000000006", "station": "synthetic-site:line2:station1", "enter": 1500000, "leave": 1560000, "tools": ["nutrunner2"], "visitSeq": 6},
    {"unit": "UNIT0000000000007", "station": "synthetic-site:line2:station1", "enter": 1680000, "leave": 1740000, "tools": ["nutrunner1", "nutrunner2"], "visitSeq": 7},
    {"unit": "UNIT0000000000008", "station": "synthetic-site:line2:station1", "enter": 1860000, "leave": 1920000, "tools": [], "visitSeq": 8},
    {"unit": "UNIT0000000000001", "station": "synthetic-site:line2:station2", "enter": 690000,  "leave": 750000,  "tools": [], "visitSeq": 1},
    {"unit": "UNIT0000000000002", "station": "synthetic-site:line2:station2", "enter": 870000,  "leave": 930000,  "tools": [], "visitSeq": 2},
    {"unit": "UNIT0000000000003", "station": "synthetic-site:line2:station2", "enter": 1050000, "leave": 1110000, "tools": [], "visitSeq": 3},
    {"unit": "UNIT0000000000004", "station": "synthetic-site:line2:station2", "enter": 1230000, "leave": 1290000, "tools": [], "visitSeq": 4},
    {"unit": "UNIT0000000000001", "station": "synthetic-site:line2:station3", "enter": 780000,  "leave": 840000,  "tools": [], "visitSeq": 1},
    {"unit": "UNIT0000000000002", "station": "synthetic-site:line2:station3", "enter": 960000,  "leave": 1020000, "tools": [], "visitSeq": 2},
    {"unit": "UNIT0000000000003", "station": "synthetic-site:line2:station3", "enter": 1140000, "leave": 1200000, "tools": [], "visitSeq": 3},
    {"unit": "UNIT0000000000004", "station": "synthetic-site:line2:station3", "enter": 1320000, "leave": 1380000, "tools": [], "visitSeq": 4}
  ],
  "toolActivity": {"startAfterEnter": 5000, "endBeforeLeave": 1000, "torque": {"min": 10.0, "max": 45.0}},
  "incidents": [],
  "batchMs": 60000,
  "tailMs": 751000,
  "injections": []
}
```

Times are offsets from `T0`. Visits 60 s long and 120 s apart (spec 598), the last tool measurement 1 s before `leave`, a tool not used reports `Running=false` throughout (spec 607), a station `Running=true` while a unit is present; `tailMs` = `L + O + P` = 751,000 (spec 594); telemetry on every device from `telemetryFrom` to `max(last business event) + tailMs`, every `scanMs`, every member of a device stamped with one `sourceTs` per scan; torque while running `min + (max - min) * seeded uniform`, two decimals (`Telemetry.torque(seed, device, scanIndex)`), idle `0.0`.

Station3 (Amendment 2026-11-11, decision 4): units 1-4 visit it after Station2, at `[780000, 840000)`, `[960000, 1020000)`, `[1140000, 1200000)` and `[1320000, 1380000)`, each 30 s after the unit leaves Station2 (as Station2 follows Station1), 60 s long and 120 s apart. The four are appended after the twelve existing visits, so no existing visit id, event id or time moves and "visit 1" to "visit 8" keep naming the Station1 visits. Station3 is its own read point (station partition 2): its visits coincide with Station1 visits 2-5, but the distinct read point prevents cross-station overlap retractions or attribution ambiguity, and its settlement deadlines `leave + L` (1,560,000, 1,740,000, 1,920,000, 2,100,000) are those of Station1's visits 2-5, so `baseline` gains no cut. Like Station2 it carries `Running` alone, its visits have `tools []` and no measurement member, and it is outside the impact rule's `incidentScope` (the two nutrunners). Every scenario built on `baseline` carries the station3 device and these visits; the one-visit `smoke` scenarios carry the device only; the plc scenarios carry the device, and no stamped visit at Station3 (the plc rule's identity stays on `station1` and `nutrunner1`).

- [ ] **Step 16.2** `ScenarioBuilder.build(scenario, registry, out)`: loads the rule set (station partitioner, catalog, `temporal`), one `session-up` at `T0` per device (`bdSeq 0`, `birthPublishTs T0`, `seq` from 2), writes the four topics (raw telemetry and sessions keyed by `device`, incidents keyed by `equipmentRef`, all placed by Murmur2; occupancy by the station partitioner), applies `Overlays`, plans batches: a boundary every `batchMs` plus, for every ground-truth deadline (`leave + L` of each visit; `enter + H` where asked), a boundary after the last scan with `sourceTs <= d + O` and the next at the following scan (spec 592); `deadlines[] {kind, ref, at, cutA, cutB}`; `throughOffsets` per topic partition (records assigned to batches by event time, or by `deliverAt` when an overlay moved them); `settleTargetW` = the latest ground-truth deadline; `definitions` = `{devicePath -> {templateRef: "<model>", version: "1.0.0", sha256: sha256(devicePath + ":" + model)}}` (synthetic; step 5 records real NBIRTH digests); `evidenceRange` per device; `expected.json`; `manifest.digests`; refuses a scenario whose overlays leave a deadline without bracketing cuts, whose tail is shorter than `tailMs`, or whose first batch carries no telemetry on some subscribed raw telemetry partition (the harness would refuse such a recording, spec 406); the lock records `firstBatchTelemetryPartitions` and `recordings verify` re-checks it.

- [ ] **Step 16.3** `GroundTruth` -> `expected.json`: `visits[] {visitRef, unitId, station, enter, leave, toolsUsed, finalStatus, insufficiencyReasons, retraction}`; `samples {<canonical sampleId> -> {attributedTo, state, reason}}` (every sampleId, spec 605); `dispositions {<canonical identity> -> {disposition, reason}}` final per identity; `incidents {(source, incidentId) -> {units {unitId -> {status, reason}}, coverage}}` at the last cut; `transitions[] {assertion, station, visitRef, cutA, cutB, atA, atB}`; `settleTargetW`, `settledCut`. Expected values are derived by the generator from the scenario it wrote (never by running the Java core); the gate asserts them on both the core's and the oracle's output.

- [ ] **Step 16.4** `Overlays` (parameters in milliseconds from `T0`; each records itself in `manifest.source.overlays[]` and adjusts the ground truth):

| `type` | Parameters | Effect on delivered records | Ground truth | Used by |
|---|---|---|---|---|
| `hold-back` | `topic, devices[], from, to, delayMs` | records with event time in `[from, to)` get `deliverAt = eventTime + delayMs` (moved to that batch; offsets renumbered in delivery order) | none (bodies at the settled cut equal the on-time run) | 1, 11b |
| `redeliver` | `topic, identity, deliverAt` | the record appended again, byte-identical except `recordTime`/`bridgeReceivedAt` | the second delivery `duplicate identical-redelivery` | 5 |
| `shift-source-ts` | `devices[], deltaMs` | every member `sourceTs` `+ deltaMs` (publish times unchanged) | per-sample attribution recomputed against the unshifted visits (9: all stay; 9b: 0-14 s stay, 15-44 s `no-occupancy`, 45-59 s next visit) | 9, 9b |
| `bad-status` | `device, metric, from, to, code` | members in the window carry `uaStatusCode = code` (`0x80000000`), `quality 0` | those samples `unattributed bad-quality`; `badSlots`; `insufficient bad-share-exceeded` when share > `maxBadShare` | 3 |
| `overlap-visit` | `unit, station, enter, leave, visitSeq` | an extra visit whose `[enter, leave)` intersects another | both `retracted overlap`; samples in the intersection `ambiguous` | 4 |
| `delay-occupancy` | `eventId, deliverAt` | the event delivered later | 2: `provisional` until the departure, `complete` at the next bracketing cut; 2b: samples `no-occupancy` then `attributed`; 19: the unit joins an existing set when its visit appears | 2, 2b, 19 |
| `late-correction` | `departureEventId, declarationTime, newDepartureEventId, newLeave, deliverAt` | after the visit is settled (`deliverAt` past `leave + L + O`): an `incorrect_data` error declaration on the departure, then a corrective departure with a new `eventId` and a later `leave` | both inputs `accepted late-after-settlement`; the `complete` visit gets a new revision (`provisional`, then `complete` again with the new `leave` once `W1 >= newLeave + L`): spec 296, 629 | 10f |
| `error-declaration` | `eventId, reason, declarationTime, deliverAt` | the original re-sent with `errorDeclaration` | 10: `retracted error-declaration`; 10b (before the original): same settled body; 10c (on a departure): `provisional` again, `insufficient missing-departure` past the horizon (Task 17) | 10, 10b, 10c, 10e |
| `resend-different` | `eventId, field, value, deliverAt` | the original re-sent with one business field changed | both deliveries `conflict identity-conflict`; visit `retracted input-conflict` | 10d |
| `activity-without-visit` | `device, from, to` | `Running=true` and torque in a window with no visit | samples `unattributed no-occupancy` with `active true`; counted by set coverage | 7, 17 |
| `session-gap` | `device, fromSeq, toSeq` | messages with `seq` in `(fromSeq, toSeq)` removed; a `seq-gap {from, to, missing}` event | overlapping visits `insufficient session-gap`; `sessionGaps 1` | 8 |
| `session-down` | `device, at, upAt` | messages in `[at, upAt)` removed; `session-down {from: lastObs, to: null}` then `session-up {at: upAt, rebirth: false}` with a new `birthPublishTs` and `bdSeq + 1` | as above | 8 |
| `stale-death` | `device, detectedEventTime, rawPublishTs, bdSeq` | a `stale-death` event | nothing changes | 25 (a) |
| `session-ambiguous` | `device, detectedEventTime, rawPublishTs, bdSeq, upAt` | a `session-ambiguous` event; messages removed from the last observation before `detectedEventTime` to `upAt`; then `session-up` | overlapping visits `insufficient session-ambiguous`; equipment coverage `unknown` | 25 (b) |
| `plc-identity` | `station, equipment[], visitId, unit, present {from, to}` | `UnitId`, `VisitId`, `VisitPresent` stamped per scan | plc visits | 11, 11b, 11c, 11d, 11e |
| `plc-cross-check-mismatch` | `station, from, to, otherUnit` | the occupancy stream names `otherUnit` at the station in the window | samples `attributed` with `reason identity-mismatch` | 11 |
| `mixed-snapshot` | `device, at` | one message whose identity members carry the previous scan's `sourceTs` | its samples `incoherent-snapshot` | 11c |
| `id-change-without-false` | `station, at, newVisitId, newUnit` | `VisitId`/`UnitId` change while `VisitPresent` stays true | the earlier visit `insufficient missing-visit-close` | 11d |
| `no-visit-end` | `station, visitId` | the `VisitPresent=false` snapshot never comes | `insufficient missing-departure` at `W1 >= enter + H` (Task 17) | 11e |
| `no-departure` | `eventId` | the departing event removed | `insufficient missing-departure` at `enter + H`; later visits unaffected (Task 17) | 6 |
| `planned-late-samples` | `device, metric` | Task 17 | 23 | 23 |
| `incident` | `incidentId, revisions[] {revision, deliverAt, fields}` | incident records | stage-2 expectations | 12-19 |

- [ ] **Step 16.5** Scenario files (`baseline` plus overlays): `held-back-10min` (`hold-back` all telemetry in `[600000, 1200000)` by 600,000; `settleTargetW = 1920000 + 720000`); `redelivery` (`redeliver` `occ-3` and one nutrunner1 message); `overlap` (`overlap-visit` `UNIT0000000000009` at station1 `[1000000, 1060000)` crossing visit 3's `[960000, 1020000)`, both retracted; plus two valid station2 visits `UNIT0000000000010` `[1500000, 1560000)` and `UNIT0000000000011` `[1602000, 1662000)`, a gap of 42 s so their tolerance windows `[1455000, 1605000)` and `[1557000, 1707000)` overlap on `[1557000, 1605000)` while the actual intervals do not: the ambiguity case of current spec 372 without a retraction, and with `45000 - 42000 = 3000` ms = 3 ambiguous slots inside each visit, within `maxMissingPeriods 3`, so both visits stay `complete`; station2's baseline visits end at 1,290,000); `bad-quality` (`bad-status` nutrunner1 `Torque` for 7 scans in visit 1); `clock-plus-30`, `clock-plus-90`; `corrections` (`delay-occupancy` of visit 2's departure to after its samples (2), of visit 5's arrival by 120,000 (2b); `late-correction` on visit 6's departure delivered at 2,400,000 (after `leave + L + O = 2,310,000`), new leave 1,565,000 (10f); `error-declaration did_not_occur` on visit 4's arrival (10); `resend-different` on visit 1's arrival `unitId` (10d)); `corrections-10b-before` and `corrections-10b-after` (10b, on visit 8's arrival alone: `did_not_occur` delivered before the original in the first, after it in the second; nothing else differs, so the settled bodies must be equal); `corrections-10c` (10c online part, on visit 8's departure alone; visit 8 `[1860000, 1920000)` is the last Station1 visit, so the reopened visit has no later arrival, `attributionEnd` null, and no later tolerance window makes its tail samples `ambiguous` (a reopened visit 7 would lose `[1815000, 1860000)` to visit 8's window and be `insufficient missing-periods` at once); the departure is cancelled only after the visit is settled: settlement `1920000 + 720000 = 2,640,000`, the builder brackets it, `cutB` ends at the first scan `T` with `T - O - 1 >= 2,640,000`, `T = 2,671,000` (`W1 = 2,640,999`), and the `incorrect_data` declaration is delivered in the next batch at `2,672,000`, so the visit is `complete` when the declaration arrives (`accepted late-after-settlement`) and becomes `provisional` with `insufficiencyReasons []`; `E = 2,672,000 + 751,000 = 3,423,000`, 3,424 scans, 5 x 3,424 = 17,120 messages); for 10f in `corrections` the same cut rule holds: visit 6 settles at `1560000 + 720000 = 2,280,000`, the 60 s boundary at `2,340,000` reports `W1 = 2,309,999 >= 2,280,000`, and the `late-correction` inputs at `2,400,000` fall in a later batch; `session` (`session-gap` nutrunner1 in visit 3 (8); `stale-death` station2 during visit 2 (25a); `session-ambiguous` nutrunner2 in visit 6 (25b); `activity-without-visit` nutrunner1 `[2100000, 2160000)` (7)); `quiet-occupancy` (a 7-minute visit with no occupancy record for 5 minutes) and `quiet-occupancy-control` (one station2 arrival inside the window; 26); `plc-stamped` (rule `line2-plc`, `plc-identity` on `station1` and `nutrunner1` for the eight Station1 visits with `visitId V<n>`, occupancy for the cross-check; `plc-cross-check-mismatch` on visit 3 (11), `mixed-snapshot` visit 5's 10th scan (11c), `id-change-without-false` at visit 7 (11d)); `plc-nocheck` (the same stamped visits on the rule of stream `line2-plc-nocheck`, `crossCheck: none`, no cross-check overlay, occupancy subscribed but never read by the rule) and `plc-held-back` (the same rule and visits with `hold-back` of the telemetry in `[600000, 1200000)` by 600,000 ms and `subscribed {"mfg.occupancy": []}`: the scans of `[0, 600000)` stay on time, so the first batch carries telemetry on every subscribed raw partition as current spec 410 requires and the harness never refuses it (a hold-back of all telemetry would leave the first `batchMs` batch empty); the delayed interval covers visits `V1`-`V3` and is pinned in the scenario (`heldBack {from: 600000, to: 1200000, delayMs: 600000}`) and in the lock (`firstBatchTelemetryPartitions` = every subscribed raw partition); counts are unchanged by a hold-back; 11b compares these two runs of one pinned `crossCheck: none` rule at the settled cut, current spec 650); `stage2` (`inc-1` `tool-check-failed` on `nutrunner1`, `stationRef station1`, revision 1 `closed`, `suspectFrom 600000`, `suspectTo = detectedAt = 1590000`, `revisionEventTime 1590000`, delivered at 2,400,000 (12, 16); `inc-2` `gauge-drift` on `nutrunner1`, revision 1 `open` `suspectFrom 900000` `detectedAt 1200000` delivered at 1,250,000, revision 2 `closed` `suspectTo 1400000` `revisionEventTime 2500000` delivered at 2,500,000 (14); `inc-3` `tool-check-failed` on `nutrunner2` over `[600000, 1590000)` with `suspectTo = detectedAt = revisionEventTime = 1590000` delivered at 2,400,000, revision 2 `withdrawn` at 2,600,000 (15); `activity-without-visit` nutrunner1 `[1246000, 1274000)` (17; the window sits between visit 4's tolerance window end `1200000 + 45000 = 1245000` and visit 5's window start `1320000 - 45000 = 1275000`, so none of its 28 scans is a candidate of any visit; a window reaching `1275000` would attribute its samples to visit 5); tail to `W2 >= 1590000 + L`); `stage2-early` (`inc-1` delivered at 300,000, without the `activity-without-visit` overlay of `stage2` (13: the injection's purpose is a set that starts empty and not settled, grows as records arrive, and settles; a scenario that can reach `complete` shows the whole ladder `unknown` -> `incomplete` -> `complete`, while injection 17's unattributed activity is asserted on `stage2` itself)); `cascade` (`stage2`'s `inc-1` delivered at 2,400,000 and `error-declaration did_not_occur` on visit 1's arrival at 2,700,000 (18); `stage2`'s `inc-2` revision 1 only, open `gauge-drift` on nutrunner1 from 900,000, delivered at 1,250,000, and `delay-occupancy` of visit 5's arrival to 1,440,000, after that incident exists and before the visit's settlement at 2,100,000, so the late attribution adds unit 5 to an existing set as `basis-provisional` first and `affected-candidate` after settlement (19); the departure at 1,380,000 arrives first and waits as a pending leave claim, S4; tail to `2,700,000 + tailMs`).

- [ ] **Step 16.6** `ScenarioBuilderTest`: the baseline builds deterministically (two builds, identical bytes); 15,760 messages; every device has a tail `>= tailMs`; every deadline has `cutA`/`cutB` with `W1_A < d <= W1_B` under the per-station formula; `expected.json` names every sampleId once; each overlay type produces its documented effect on one small case; a scenario without bracketing cuts is refused; `forseti recordings verify` fails on a changed lock digest; within the determinism test, a fresh build of every hand-made scenario that has a lock equals that lock (every file digest, the counts, `firstBatchTelemetryPartitions`, the cut count), and the fresh builds of the three smoke scenarios equal the committed files of `fixtures/recordings/` byte for byte (the test is run again after the locks below are written). `mvn -q -pl cli -Dtest=ScenarioBuilderTest test` -> `Tests run: 9, Failures: 0`. Then `mvn -B -ntp -q install` (the jar must carry this task's builder and overlays; without it `java -jar cli/target/forseti.jar` is the previous PR's jar). Build and lock the twenty scenarios of this task into `build/recordings/<name>/` (the lock files sit beside the scenarios and are skipped; the smoke scenarios keep their committed recordings and locks from Task 8): `for s in fixtures/scenarios/*.json; do case "$s" in *.lock.json|*/smoke*.json) continue;; esac; java -jar cli/target/forseti.jar recordings build --scenario "$s" --registry fixtures/registry --out "build/recordings/$(basename "$s" .json)" --lock; done` (the scenario files and their locks are committed in the same commit; the recordings are not: `build/` is git-ignored and holds every gate-built recording and the run output; `git status --short fixtures/recordings` prints nothing). Commit `feat(cli): scenario builder with ground truth, deadline-bracketing batches, injection overlays and the hand-made scenarios`.

### Task 17: Synthesizer and the horizon scenarios

**Files:**
- Create: `cli/src/main/java/yggdrasil/forseti/cli/recording/Synthesizer.java`; Modify: `ScenarioBuilder` (`"synthetic": true` delegates telemetry to the synthesizer), `Main` (`recordings synth`)
- Create: `fixtures/scenarios/{horizon-no-departure,horizon-reopened,horizon-correction-beyond,plc-horizon-no-end,horizon-planned-late}.json` (+ locks)
- Create: `cli/src/test/java/yggdrasil/forseti/cli/recording/SynthesizerTest.java`

Horizon scenarios (current spec 613) need continuous telemetry on every subscribed device from `T0` (`telemetryFrom 0`) to the scenario's end `E`, every 1,000 ms, both ends included: `scans = E / 1000 + 1` per device, `messages = 5 x scans` (five devices), samples = scans x members per scan (7 for the occupancy-join devices: 1 + 1 + 1 + 2 + 2; 13 for the plc devices: 4 + 1 + 1 + 5 + 2; station3 carries `Running` alone in both). The Station3 visits end at 1,380,000, so every `E` below is the one planned for two stations. Per scenario: `horizon-no-departure` `E = leave(visit 9) + tailMs = 88,560,000 + 751,000 = 89,311,000` (visit 9 is the later same-station visit of injection 6, below; its settlement `88,560,000 + 720,000 = 89,280,000` is bracketed inside that tail: `cutB` at scan `89,311,000`, `W1 = 89,280,999`) -> 89,312 scans, 446,560 messages, 625,184 samples; `horizon-reopened` `E = enter(visit 8) + H + tailMs = 1,860,000 + 86,400,000 + 751,000 = 89,011,000` -> 89,012 scans, 445,060 messages, 623,084 samples; `horizon-correction-beyond` `E = deliverAt + tailMs = 87,992,000 + 751,000 = 88,743,000` -> 88,744 scans, 443,720 messages, 621,208 samples; `plc-horizon-no-end` `E = enter(V3) + H + tailMs = 960,000 + 86,400,000 + 751,000 = 88,111,000` -> 88,112 scans, 440,560 messages, 1,145,456 samples; `horizon-planned-late` `E = T + tailMs = 87,410,000 + 751,000 = 88,161,000` -> 88,162 scans, a base of 440,810 messages and 617,134 samples; after the overlay the delivered counts are 440,813 messages (the three late samples are three DDATA records, one `Torque` member each, with `seq` continuing the session) and 617,136 samples (617,134 - 1 removed `Torque` observation at `holeAt`, whose message keeps its `Running` member, + 3 late samples); the locks pin the delivered counts; they are built at gate time to `build/recordings/<name>` and verified against the lock; never committed. Synthetic tails are ordinary data (`Running=false`, idle torque, Good quality) and follow every rule, so a horizon run produces one sample row per sample and one disposition per member; the volume is the requirement (Decision 8).

- [ ] **Step 17.1** `Synthesizer`: streams records to the JSONL writers batch by batch (never the whole recording in memory); the same seeded `Telemetry.torque`; `batchMs` 3,600,000 for horizon scenarios plus the mandatory brackets and a cut every 60 s inside the first 40 minutes (about 70 cuts); `manifest.synthetic true`, `source {scenario, generator: "forseti recordings synth 0.1.0", seed}`.

- [ ] **Step 17.2** Scenarios: `horizon-no-departure` (6 and the after-horizon cuts: `no-departure` on visit 8's departure, `horizonDeadlines ["visit-8"]`; visit 8 `[1860000, 1920000)` is the last Station1 visit, so the open visit has `attributionEnd` null and `J = [enter, min(W1, enter + H))`; its tail telemetry (`Running=false`, idle torque, every second) keeps `missingPeriods 0`, so the visit is really `provisional` at `cutA` and `insufficient missing-departure` only at `cutB` of `enter + H`; with visit 4 the open window up to visit 5's `enter` 1,320,000 would overlap visit 5's tolerance window from `1320000 - 45000 = 1,275,000`, 45 ambiguous slots > `maxMissingPeriods 3`, `insufficient missing-periods` long before the horizon, and `lenient-oracle` would have nothing to fail at `cutA`; current spec 638 also asks that later visits at that station are not retracted, so a visit 9 `UNIT0000000000012` `[88500000, 88560000)` with `nutrunner1` follows the horizon bracket: the bracket of `enter(visit 8) + H = 88,260,000` has `cutB` at the first scan `T` with `T - O - 1 >= 88,260,000`, `T = 88,291,000`, and visit 9's window `[88455000, 88605000)` begins after it, so visit 8's `J = [1860000, min(W1, 88500000, 88260000))` ends at the horizon untouched by visit 9 (the samples in `[88455000, 88500000)` are `ambiguous`, inside both windows, outside both `J`s); visit 8 is open, so S7 never pairs it with visit 9 (overlap is decided only between two closed visits), visit 9 settles at `89,280,000` and is `complete`); `horizon-reopened` (10c's horizon half: the `corrections-10c` declaration on visit 8's departure delivered at 2,672,000 after the settlement bracket, `horizonDeadlines ["visit-8"]`: `complete` -> `provisional` -> `insufficient missing-departure` at `enter(visit 8) + H`); `horizon-correction-beyond` (10e: `error-declaration did_not_occur` on visit 2's arrival; visit 2 `[780000, 840000)` has `onlineExpiresAt = leave + H + L = 840,000 + 86,400,000 + 720,000 = 87,960,000`; a declaration delivered at that very time would still be admitted, because admission uses the previous cut's `admissionW` and a cut's inputs apply before its timers (current spec 416, 521); so the builder lists `onlineExpiresAt` as a deadline, brackets it (`cutB` ends at the first scan `T` with `T - O - 1 >= 87,960,000`, `T = 87,991,000`, `W1 = 87,960,999`), and delivers the declaration in the batch after `cutB`, `deliverAt = 87,992,000`, where `admissionW = 87,960,999 >= onlineExpiresAt`); `plc-horizon-no-end` (11e, current spec 653: "neither `VisitPresent=false` nor an ID change": the plc scenario reduced to the stamped visits `V1`-`V3` at Station1; `no-visit-end` on `V3` keeps `VisitPresent=true` with `V3`'s `UnitId`/`VisitId` on `station1` and `nutrunner1` for every scan to the end of the recording, no later stamped visit follows, so no ID change occurs (a later `V4` would make it `missing-visit-close`, injection 11d); telemetry to `enter(V3) + H + tailMs`; expected `insufficient missing-departure` at the bracketing cut of `enter(V3) + H`); `horizon-planned-late` (23: `planned-late-samples` on nutrunner1 `Torque`: the hole is chosen first, inside visit 3's evaluation interval, `holeAt = enter(visit 3) + 20000` (the 20th scan, `T0 + 980000`); the planned `A` follows from it, `A = holeAt + H - 1`, and the batch boundary `T = A + O + 1 = holeAt + H + O` (`T0 + 980000 + 86,400,000 + 30,000 = T0 + 87,410,000`), which lies on the scan grid, so every subscribed raw telemetry partition has the same largest `sourceTs` `T` at that boundary (asserted by the builder) and the per-station formula gives exactly `W1 = T - O - 1 = A`; the builder ends a batch at `T`, removes the on-time scan at `holeAt` from nutrunner1's `Torque`, and puts three late samples with `sourceTs = A - H + 1 = holeAt`, `A - H = holeAt - 1` and `A - H - 1 = holeAt - 2` and `seq` continuing the session into the next batch (new `sampleId`s, no `seq-gap`, no measurement conflict). Admission at that batch is against `admissionW = A`: `holeAt >= A - H` and `holeAt - 1 >= A - H` are admitted, `holeAt - 2 < A - H` is `replay-required beyond-horizon`; visit 3 is still online (`onlineExpiresAt = leave + H + L > A`). Expected: the first fills visit 3's hole (`missingPeriods` 1 -> 0), the second is a second observation of slot 19 (already observed by the scan at `enter + 19000`), the third `replay-required`; `transitions[]` carry `A` and the cut whose reported `W1` must equal it; telemetry runs to `T + tailMs`; delivered counts 440,813 messages, 617,136 samples).

- [ ] **Step 17.3** `SynthesizerTest`: `horizon-no-departure` builds in under 60 s with 446,560 messages and 625,184 samples (the arithmetic above); two builds byte-identical; the lock verifies; the planned-late manifest has `W1 == A` at the planned cut under the formula; the hole is absent on time and present as the first late sample. `mvn -q -pl cli -Dtest=SynthesizerTest test` -> `Tests run: 5, Failures: 0`. `mvn -B -ntp -q install` (the jar gains the synthesizer), then build and lock the five with `java -jar cli/target/forseti.jar recordings build --scenario fixtures/scenarios/<name>.json --registry fixtures/registry --out build/recordings/<name> --lock`. Commit `feat(cli): recording synthesizer for 24 h horizon scenarios with pinned locks`.

### Task 18: The independent Python oracle (stages 1 and 2)

**Files:**
- Create: `oracle/src/forseti_oracle/{cuts,stage1,stage2,fold}.py`; Modify: `cli.py` (`stage1`, `stage2`, `all`)
- Create: `oracle/tests/{test_stage1,test_stage2,test_fold,test_independence}.py`

`forseti-oracle all --recording <dir> --cuts <run>/cuts.json --mappings <registry>/mappings --release <id>@<version> --rule <id>@<version> --definitions <manifest> --out <dir> [--lenient] [--subscribe mfg.occupancy=]`. For each completed cut `k` of the cut log the oracle recomputes from scratch the live row set of every station from the inputs delivered through `k`, admitted against the `admissionW` the cut log records for the cut each input was delivered in (admission history is remembered across cuts: rebuilding without it would admit beyond-horizon inputs), with timers evaluated up to the recorded `W1` and `W2 = W1`. It writes `<out>/cut-<k>/<topic>.json` (`{logicalKey JSON -> {body, bodyDigest}}` for the four journals), `<out>/cut-<k>/dispositions-stage{1,2}.json` (`{identity JSON -> {disposition, reason}}` final per identity), `oracle-run.json` (pins, `rfc8785` version, Python version, recording digests, offset ranges). It shares nothing with the Java side; it implements the rule sheets from the spec text; it computes `rulesDigest` from the released bytes it hashed itself; it never reads the harness's journals (the gate compares). The output files described here are replaced by per-cut deltas handed over one cut at a time, Amendment 2026-11-12, item 1.

- [ ] **Step 18.1** `stage1.py`: `classify(deliveries, admission_w_by_cut, rule, catalog)` (S1-S3 as a sweep in delivery order per identity), `visits_of(station, accepted_occupancy, w1)` (S4, S5, S7), `sessions_of(device, accepted_sessions, w1)` (S8), `attribute(samples_sorted, visits, sessions, rule)` (S9, one sweep over samples sorted by `sourceTs` against visits sorted by `enter`), `coverage(visit, observations, activity, rule)` (S6), `work_record_body`, `sample_row_body`, `plc_visits_of` (P1-P3). `stage2.py`: stands on the oracle's own stage-1 rows for `k` (a stage-1 disagreement surfaces once, at stage 1); incidents delivered through `k` classified per T1; row set per T3 by a sweep over visits sorted by `enter` against the interval of every delivered valid revision (superseded lower revisions included, spec 515), judgement by the highest valid revision; T4-T8 bodies; deletes as absent keys. `fold.py`: the oracle's own fold and integrity checks of a harness run (`--check-journals`: revision gaps, equal consecutive digests, a row after its progress, missing dispositions, a stale `basis[]` digest). `cuts.py`: reads `cuts.json` and refuses a history inconsistent with the manifest's batches; through-offsets are inclusive with `-1` for "nothing included" (the oracle's `safe_int` ranges mirror the Java ones: record offsets and times `0 ..`, through-offsets `-1 ..`, computed times the full JCS domain), and `test_cuts.py` has `test_through_offset_minus_one_means_an_empty_partition` (a cut with all six incident through-offsets at `-1` selects no incident; the next cut with `0` on one partition selects exactly the first record).

- [ ] **Step 18.2** Tests: `test_stage1.py` and `test_stage2.py` mirror the cases of `Stage1OccupancyJoinTest`, `Stage1PlcStampedTest` and `Stage2Test` with inline inputs typed from the rule sheets (no Java output copied), including `test_uncertain_propagate_aggregates_and_supports_activity` (a rule copy with `uncertainInput: propagate`: Uncertain values enter `count`/`min`/`max`/`last`, the activity support, the sample row's `active`, and are not `unknownActivityPeriods` in the affected set; under `reject` the same inputs are `bad-quality`, `active "unknown"` and unknown coverage), `test_declaration_before_original_in_both_orders`, `test_rejected_uncertain_only_has_bad_slots_zero`, `test_revision_event_time_checked_by_revision_number`, `test_non_canonicalizable_input_has_a_raw_bytes_disposition` (the oracle's classification keeps a `schema-invalid` row with `refs.digestRule raw-bytes` for `[9007199254740992]` delivered on `mfg.occupancy`), `test_no_activity_judged_inside_the_intersection_only` (a `complete` basis with a 2-slot gap outside `[enter, leave) ∩ S` is `no-equipment-activity`; the same gap inside it is `basis-insufficient`), and `test_disallowed_correction_reason_is_schema_invalid`; the `samplesDigest` of the smoke visit equals the literal the Java test pins; `test_station2_visit_body_without_measurement_members` pins the same Station2 body and digest as the Java test; `test_visit_seq_above_int32_is_preserved_in_the_body`; every supported rule option is exercised (`fully-within`, `enter-within`, `station-visit`, `always-insufficient`, `onWithdrawn insufficient`), not only the fixtures' choices. `test_fold.py`: the five violations each detected on inline journals. `test_independence.py`: the oracle package imports nothing from outside `forseti_oracle`, `rfc8785`, `jsonschema` and the standard library; no module launches a subprocess; the shared files are data.

- [ ] **Step 18.3** `pytest -q oracle` -> `+78 passed` (about 119 total). By hand on the smoke run: `python -I -m forseti_oracle.cli all --recording fixtures/recordings/smoke-incident --cuts build/runs/smoke-incident/cuts.json --mappings fixtures/registry/mappings --release line2-rules@1.0.0 --rule line2-attribution@1.0.0 --definitions fixtures/recordings/smoke-incident/manifest.json --out build/oracle/smoke-incident` -> `cuts 53, rows N`; the one visit's `bodyDigest` equals the folded harness journal's. Commit `feat(oracle): independent stage-1 and stage-2 oracle, fold and cut-log reader`.

### Task 19: Comparison, integrity, injection assertions, determinism, item 2 reference values, pull request 4

**Files:**
- Create: `cli/src/main/java/yggdrasil/forseti/cli/gate/{Comparison,Integrity,Injections}.java`; Modify: `CoreGate`, `gate/core-assertions.json` (+43 = 72)
- Create: `cli/src/test/java/yggdrasil/forseti/cli/gate/{ComparisonTest,IntegrityTest}.java`

- [ ] **Step 19.1** `Comparison`: for every completed cut `k` of a run, fold the harness journals through progress `k` per partition (`JournalFold`), load the oracle's `cut-<k>` files, compare key sets and recomputed `bodyDigest` per key for the four journals (a reported digest alone is not trusted: the gate recomputes `Canonical.digest(body)` on both sides); on a difference write `build/gate/diffs/<scenario>/cut-<k>/<topic>/<n>.json` {generation, cut, key, JSON path of the first difference, expected, actual, input offset ranges} (CI artifact, spec 671) and fail; compare dispositions by identity (`disposition` and `reason`; which delivery is `duplicate` is not compared, spec 452); apply the comparable-cut predicate (spec 590) for direct run-to-run comparisons (1, 11b, 26, determinism) and fail when a scenario that needs a settled cut has none. `Integrity`: revisions contiguous from 1 per key; no equal consecutive digests; progress `cutId` increasing and `eventTimeAt` non-decreasing per partition; no business row of cut `k` after its progress; every delivery (with its parts) has a disposition; `W1` equals the formula; `admissionW` equals the previous `W1`; stage-2 `W2 == W1`; `incidentOffsets` count 6; every `basis[].bodyDigest` current; pins cross-check: for every row, `Canonical.digest(envelope.pins.withoutJob())` (the four fields `rules`, `hierarchy`, `bindings`, `definitions`; never `job`, which differs per run) equals the generation row's `rulesDigest`, and for the two bodies that carry `rulesDigest` (work record, spec 355; impact assessment, spec 379; the sample journal and the affected set carry none, spec 362-366, 385-398) the body value equals it too, all recomputed by the gate (the hook `wrong-rules-pin` fails in PR 6). `Injections`: one assertion per id below; each builds its recording into `build/recordings/<name>/` from the committed scenario file and verifies it against the committed lock (the twenty hand-made scenarios of Task 16 and the five horizon scenarios, never committed; build, then verify, then run: a recording whose bytes differ from its lock fails the assertion before any run), or verifies the committed recording against its lock in place (the smoke recordings), registers a generation in a temp state (the PR 1-4 stub store; PR 5 switches to the CLI), runs the harness, runs the oracle (`--python`), compares every completed cut, checks integrity, asserts `expected.json` at the last cut and `transitions[]` at the bracketing cuts.

| Assertion id | Scenario | Asserted beyond oracle equality and integrity |
|---|---|---|
| `I-1` | `held-back-10min` vs `baseline` | bodies equal at the settled cut (every work record and sample row digest) |
| `I-2` | `corrections` (visit 2) | `provisional` at `cutA`, `complete` at `cutB` of `leave + L` |
| `I-2b` | `corrections` (visit 5) | sample rows `unattributed no-occupancy` then `attributed`; `members[]` include them |
| `I-3` | `bad-quality` | 7 rows `unattributed bad-quality`; they count for visit 1 through the quality-blind candidate: `members[nutrunner1-torque]` has `observedPeriods 60`, `badSlots 7`, `count 53`; `badShare 7/60 = 0.117 > 0.1`; visit 1 `insufficient` with `bad-share-exceeded` |
| `I-4` | `overlap` | visit 3 (`[960000, 1020000)`) and the extra visit (`[1000000, 1060000)`) `retracted overlap`; samples inside exactly one actual interval are `attributed` to that retracted visit; samples in the actual intersection `[1000000, 1020000)` are `unattributed ambiguous`; at station2 the two valid visits (`[1500000, 1560000)`, `[1602000, 1662000)`) are both `complete`: actual intervals disjoint, so no overlap retraction; their windows overlap on `[1557000, 1605000)` (48 slots of station2 `Running` samples, all `unattributed ambiguous`, two non-retracted candidates, spec 368), of which 3 slots lie inside the first visit (`[1557000, 1560000)`) and 3 inside the second (`[1602000, 1605000)`); station2 has no measurement member, so only activity support is lost: `equipmentEvidence[station2].coverage.unknownActivityPeriods 3`, equal to `maxMissingPeriods 3` and therefore not `unknown-activity`; the 42 ambiguous slots between the visits are outside both `J` |
| `I-5` | `redelivery` | the second deliveries `duplicate identical-redelivery`; row counts equal `baseline`'s |
| `I-6` | `horizon-no-departure` | visit 8 `provisional` at `cutA` (`missingPeriods 0`, `insufficiencyReasons []`) and `insufficient missing-departure` at `cutB` of `enter + H = 88,260,000`; visits 1-7 unchanged against `baseline`; the later same-station visit 9 (`[88500000, 88560000)`, spec 619) is `provisional` on arrival and `complete` at its settlement bracket of `89,280,000`, never `retracted`, and visit 8 keeps `insufficient missing-departure` with no `overlap`; `lenient-oracle` fails here because the oracle reports `complete` at `cutA` |
| `I-7` | `session` | 120 nutrunner1 sample rows (60 scans x `Torque` and `Running`) `unattributed no-occupancy` with `active true`; no `complete` visit for that window |
| `I-8` | `session` | visit 3 `insufficient session-gap`; `equipmentEvidence[nutrunner1].coverage.sessionGaps 1` |
| `I-9` | `clock-plus-30` | every sample whose original `sourceTs` lies inside a visit's `[enter, leave)` is attributed to the same visit as in `baseline` (the 45 s tolerance absorbs +30 s, spec 622); the complete per-sample expectation is `expected.json` recomputed on the shifted timestamps (a sample at original 550,000, unattributed in `baseline`, becomes attributed at 580,000 inside visit 1's window `[555000, 705000)`) |
| `I-9b` | `clock-plus-90` | per-sample attribution equals `expected.json` (three bands) |
| `I-10` | `corrections` (visit 4) | `retracted error-declaration`, `errorReason did_not_occur` |
| `I-10b` | `corrections-10b-before` vs `corrections-10b-after` | the same settled body digest for visit 8 (`retracted error-declaration`, `errorReason did_not_occur`) in both delivery orders; in `-before` the declaration's disposition is `accepted` (`part errorDeclaration`) and the later original `accepted` (`part event`), S2 (c) |
| `I-10c` | `corrections-10c` + `horizon-reopened` (visit 8) | `complete` at the cut ending at scan `2,671,000` (`W1 = 2,640,999`); the declaration at `2,672,000` is `accepted late-after-settlement`; the visit becomes `provisional` with `insufficiencyReasons []` (no later window, no ambiguity); in `horizon-reopened` it is `insufficient missing-departure` at `enter + H`; sample rows keep `attributedTo` |
| `I-10d` | `corrections` (visit 1) | both deliveries `conflict identity-conflict`; visit `retracted input-conflict` |
| `I-10e` | `horizon-correction-beyond` | the cut before the declaration's batch reports `W1 = 87,960,999 >= onlineExpiresAt`; the declaration `replay-required correction-beyond-horizon`; visit 2's body unchanged (`complete`) |
| `I-10f` | `corrections` (visit 6) | the declaration and the corrective departure are both `accepted late-after-settlement`; the visit, already `complete`, gets a new revision (`provisional`), then `complete` with `leave 1565000` once `W1 >= 1565000 + L` Corrected to the spec text: Amendment 2026-11-12, item 3. |
| `I-11` | `plc-stamped` | visit 3's samples `attributed` with `reason identity-mismatch`; `attributedTo` unchanged |
| `I-11b` | `plc-held-back` vs `plc-nocheck` (both on the pinned `crossCheck: none` rule of stream `line2-plc-nocheck`) | bodies equal at the settled cut; at least one `complete` visit |
| `I-11c` | `plc-stamped` | the mixed snapshot's samples `incoherent-snapshot` |
| `I-11d` | `plc-stamped` | visit 7 `insufficient missing-visit-close` |
| `I-11e` | `plc-horizon-no-end` | `V3` `insufficient missing-departure` once `W1 >= enter + H` |
| `I-12` | `stage2` (`inc-1`) | units 1, 3, 5 `affected-candidate equipment-activity`; set `settled true` at the last cut |
| `I-13` | `stage2-early` | first set row (the cut that delivers the incident at 300,000): `units []`, `settled false`, `coverage.status unknown` with `unknownActivityPeriods 990` (no activity observation yet in `[600000, 1590000)`, 990 one-second slots, T8); as telemetry arrives `unknownActivityPeriods` falls cut by cut and the status stays `unknown` until it reaches 0; `units[]` grows by one unit per completed visit (`basis-provisional` first, then the T6 outcome); `settled true` once `W2 >= 1,590,000 + 720,000 = 2,310,000`; at the last cut `coverage.status complete` (`stage2-early` carries no `activity-without-visit` overlay, so `unattributedActivityPeriods 0`; nothing unknown, nothing provisional or insufficient, evidence range covers the interval) |
| `I-14` | `stage2` (`inc-2`) | while open: T1 visits after 900000 `affected-candidate`, T2-only and no-tool `no-equipment-activity`; after revision 2: visits 6, 7, 8 `not-affected-within-scope interval-narrowed` whatever their tools, visits 3 and 5 `affected-candidate`; `open false` |
| `I-15` | `stage2` (`inc-3`) | every unit `not-affected-within-scope incident-withdrawn`; `resolution withdrawn`, `open false`, `settled true`, never `complete` Corrected to the spec text: Amendment 2026-11-12, item 3. |
| `I-16` | `stage2` (`inc-1`) | T2-only visit 2 `no-equipment-activity`; both-tool visit 3 `affected-candidate`; no-tool visit 4 `no-equipment-activity` |
| `I-17` | `stage2` (`inc-1`) | `coverage.unattributedActivityPeriods 28` (the 28 one-second slots of `[1246000, 1274000)`, aligned to the slot grid from `suspectFrom 600000`), `coverage.status incomplete`; the 56 sample rows (28 scans x `Torque`, `Running`) are `unattributed no-occupancy` with `active true` |
| `I-18` | `cascade` | unit 1 `affected-candidate` then `not-affected-within-scope basis-retracted-error-declaration`, in the same cut as the retraction |
| `I-19` | `cascade` | `inc-2` (open `gauge-drift` on nutrunner1 from `suspectFrom 900000`, delivered at 1,250,000) has a set without unit 5 (no visit claim yet); visit 5's arrival is delivered at 1,440,000, before its settlement at `leave + L = 1380000 + 720000 = 2100000`: the pending departure applies, the samples move `no-occupancy` -> `attributed`, the visit is `provisional` (`W1` at that cut is about 1,409,999 < 2,100,000) and unit 5 enters the set as `insufficient-evidence basis-provisional`; at the bracketing cut of 2,100,000 it becomes `affected-candidate equipment-activity` (T1 active inside the open interval); each step is a set revision (spec 642) |
| `I-25` | `session` | `stale-death`: station2's visit 2 unchanged vs `baseline`; `session-ambiguous`: visit 6 `insufficient session-ambiguous`, `equipmentEvidence[nutrunner2].coverage.status unknown` |
| `I-26` | `quiet-occupancy` vs `quiet-occupancy-control` | direct body equality at every comparable cut for Station1 only (the station whose occupancy stream is silent; every Station1 work record and sample row digest equal between the two runs); Station2, which receives the control's extra arrival, is compared with the oracle in each run, as every scenario is |
| `J-DISPOSITION-COVERAGE`, `J-REVISION-CONTINUITY`, `J-NO-EQUAL-CONSECUTIVE`, `J-PROGRESS-ORDER`, `J-W1-FORMULA`, `J-W2-EQUALS-W1`, `J-BASIS-CURRENT` | all scenarios | the integrity checks above, aggregated over every run of the gate |
| `J-AFTER-HORIZON-CUTS` | the five horizon scenarios | cuts exist with `W1 >= enter + H + L` of the open visits; `onlineVisits` drops after expiry while the rows remain in the fold |
| `C-6-STAGE1`, `C-6-STAGE2` | `baseline`, `stage2` x 3 seeds (`--shuffle`) | identical bodies at every cut; revision counts in `report.json` |
| `C-2-REFERENCE` | `held-back-10min`, `horizon-planned-late` (its online run only; `I-23` is PR 5b) | `report.json` records `longestLatenessCorrectedOnlineMs` (568999 from 1, Amendment 2026-11-12, item 3; `H` from 23's second late sample), journal row counts per topic per scenario, `inputsWithoutDisposition 0` Corrected to the spec text: Amendment 2026-11-12, item 3. |

(43 new ids: 32 injections, 8 `J-*`, 3 `C-*`; the manifest now has 72.)

- [ ] **Step 19.2** `ComparisonTest` (agreeing outputs pass; a body changed in the oracle output fails with a diff file; a reported digest that does not match its body fails) and `IntegrityTest` (a revision gap, equal consecutive digests, a row after its progress, a missing disposition, a stale basis digest, a wrong pins digest: each caught). `mvn -q -pl cli -Dtest='ComparisonTest,IntegrityTest' test` -> `Tests run: 9, Failures: 0`.

- [ ] **Step 19.3** `mvn -B -ntp -q install` (the jar gains `Comparison`, `Integrity` and `Injections`), then the gate -> every scenario built or verified, every run compared, last line `assertions run 72 / expected 72`, exit 0; `build/gate/diffs` absent. Expected wall time 13-23 minutes (the 10-18 minutes planned for four devices, times 5/4 for the fifth; the five horizon recordings dominate; building the twenty hand-made recordings at gate time, about 330k messages together and fewer than one horizon recording, is estimated at a minute or less within that range; above 15 minutes measured, the `build` job is split as Open point 2 decides). Commit `feat(cli): core gate folds, compares with the oracle, checks integrity and asserts the engine-independent injections`. Pull request 4: Tasks 16-19 (5 commits); CI green; PR body records the gate's wall time, the scenario sizes, the diff-artifact path, and that the twenty hand-made recordings are built at gate time from committed scenarios and locks, not committed (Amendment 2026-11-11, decision 6). The expected wall time above is superseded by Amendment 2026-11-12, item 2 (measured times, sharded CI).

## Chunk 6: Generation control (PR 5a and PR 5b)

Two PRs (Amendment 2026-11-13, decision 1). PR 5a changes the generation every existing run reads and the report format, with no
new semantics; PR 5b adds promotion and the event-time replay. Spec references are current lines (Decision 11 = 73; generations =
552-570 as edited by Step 20.0).

### Report format (written by 5a, extended by 5b)

`report.json` keeps every field it has and gains:

| Field | Value |
|---|---|
| `kind` | `gate`, or `negative-control` exactly when `--negative-control` was given. Never inferred from `intendedFailures` |
| `negativeControl` | null, or the control's name |
| `layout` | `full` (no `--shard`, no `--aggregate`), `shard`, or `aggregate`. The existing field `shard` is non-null exactly when `layout` is `shard` |
| `summariesSha256` | `full` and `shard`: the raw-byte SHA-256 of the `summaries.json` this run wrote beside the report (a full run writes one too, from 5a on). `aggregate`: null (the shards' are in `shards[]`) |
| `results[].shard` | `full`: null. `shard`: the shard's name. `aggregate`: the plan owner of the id (a shard name, or the literal `aggregate` for an id of the plan's `aggregate` list) |
| `shards[]` | `aggregate` only, else `[]`: one entry per shard of the committed plan, in plan order, `{name, exit, reportSha256, summariesSha256}`; the two digests are the raw-byte SHA-256 of the exact bytes the aggregate run parsed (read once; hash and parse the same buffer) |
| `generations[]` | every generation this gate run registered: `{shard, store, generation, scope, rulesDigest, registeredRowSha256, state, scenario, variant, assertion}`. `shard`: null in `full`. `store`: see below. `generation`: the number inside its store. `scope`: the record key. `state`: `shared` or `isolated`. `variant`: `online`, `self-test` (5a), `event-time-replay`, `rule-change` (5b). `assertion`: the id that registered it outside the pipeline, null for a pipeline generation |
| `runs[]` | every harness run of this gate run: `{shard, store, scenario, variant, seed, shuffle, runId, generation, registeredRowSha256, state, runRecord, runRecordSha256, cutLog, cutLogSha256, recordingManifestSha256, originalRecordingManifestSha256, offsetRanges}` (5b adds `journals`, `journalsIndexSha256`). `runRecord` and `cutLog` are paths relative to the output directory of the gate run that made the run (`runs/<scenario>.run.json`, `runs/<scenario>.cuts.json`, `runs/<scenario>.shuffle-<seed>.run.json`, `runs/<scenario>.replay.run.json`, ...), never absolute and never containing `..`. In `full` and `shard` layout that directory is the report's own. In an aggregate report it is the producing shard's directory under the directory given to `--aggregate` and to `promote --shards`, `<shards dir>/<shard>/`, never the aggregate run's `--out`. The two digests are raw-byte SHA-256. `originalRecordingManifestSha256` is null unless `variant` is `event-time-replay`. `offsetRanges` is copied from the run record |

`store` is the bundle-local identifier of the control store an entry lives in, deterministic for a selection: the literal
`pipeline` for the gate run's own store (the `--state` directory, or the temporary store of a run without `--state`), and
`assertion:<ID>` for the store an assertion creates for itself (`assertion:<ID>#2`, `#3`, ... for a further store of the same
assertion, in creation order; an assertion creates its stores sequentially). It names no path. A generation number and a
`registeredRowSha256` are unique only inside one `(shard, store)`: isolated stores each start at generation 1, and identical
registrations under the fixture clock produce identical rows. Every uniqueness rule and every reference in a report therefore uses
`(shard, store, generation)` or `(shard, store, registeredRowSha256)`; a digest alone never identifies a generation or a run.

`state`: `shared` = registered by the executable `generation new` (system clock) in the store given by `--state` of the gate run that
registered it, and kept (for a shard run, that shard's own directory; "shared" never means shared between runners). Only entries
with `store: pipeline` can be `shared`. `isolated` = registered in a temporary store the gate deletes: every generation of a run
without `--state`, and every generation of an `assertion:` store. An aggregate report's `generations[]` and `runs[]` are the shard
reports' arrays concatenated in plan order, unchanged; the aggregate run registers and runs nothing.

`registeredRowSha256` = `Canonical.digest` of the transport record `{topic, key, headers, value}` of the `registered` row (spec 566
paragraph "Run citation"). Because the store writes exactly `Canonical.bytes(record) + LF`, it equals the raw SHA-256 of the stored
line without its LF.

`generations[]` and `runs[]` live in `report.json` only. `summaries.json` carries no generation number, row digest, store or run id
in a compared field, so two shards that make the same scenario still agree on every field outside `Shards.NOT_COMPARED`.

## PR 5a

Branch: `git switch -c feat/step3-generation-cli main` (`main` = `bc299d5`, PR 4 merged).

### Task 20: Generation rows, transition policy, the locked durable file store, the CLI verbs, one store per gate run

**Files:**
- Modify: `docs/superpowers/specs/2026-11-05-forseti-design.md` (the SPEC block of this amendment), this plan (Amendment 2026-11-13)
- Create: `core/src/main/java/yggdrasil/forseti/core/release/LockedFile.java`, `core/src/test/java/yggdrasil/forseti/core/release/LockedFileTest.java`; Modify: `FloorStore` (uses `LockedFile`; behaviour and `FloorStoreTest` unchanged)
- Create: `core/.../generation/GenerationPolicy.java`, `core/src/test/.../generation/GenerationPolicyTest.java`; Modify: `core/.../generation/Generation.java` (+ `run`), `GenerationStore.java` (+ `latestStatus(n)`, `registered(n)`, `validate()`)
- Create: `cli/.../generation/GenerationCli.java`, `cli/src/test/.../generation/{GenerationCliTest,FileGenerationStoreTest,GenerationStoreMain}.java`; Modify: `cli/.../generation/FileGenerationStore.java` (locked durable `append`, `registeredRowSha256(n)`), `Main` (the `generation` command, usage text; `run` calls the run preparation), `ReferenceEngine` (reads the store, writes nothing to it), `RunRecord` (+ `registeredRowSha256`, `scope`, `replay`, `originalRecordingManifestSha256`)
- Create: `cli/.../gate/GenerationAssertions.java`, `cli/.../gate/GateEvidence.java`; Modify: `cli/.../gate/{CoreGate,Scenarios,Summaries,Shards,Stage1Assertions,Stage2Assertions,Foundation}.java` (every hand-written registration path replaced, the smoke, shuffle and derived-recording helpers among them), `gate/core-assertions.json` (+3 = 75; descriptions, see OTHER-EDITS), `gate/core-shards.json`, `.github/workflows/{ci,nightly-gate}.yml`; tests `CoreGateTest`, `ShardsTest`, `ReferenceEngineTest` (its private `append` helper is replaced by the verb)

**Decisions.**

Decision 11 (spec 73), spec 552-570 as edited by Step 20.0. `GenerationStore` is the transport boundary; `FileGenerationStore` is the
step-3 implementation: `<stateDir>/generations.jsonl`, one transport record per line `{"topic": "forseti.generations", "key":
"<scope>", "headers": {"forseti-generation": "<n>"}, "value": <the generation row>}`, `<stateDir>/generations.lock`,
`<stateDir>/recovery-control.jsonl`, `<stateDir>/head-floors/`.

1. **Writers.** Only `GenerationCli` appends to `generations.jsonl`. The harness and the jobs read. The gate appends no row by any
   path of its own: in its pipeline store it invokes `generation new` and no other writing verb (decision 6); an assertion that
   tests the lifecycle may invoke `new` and `retire`, and `promote` for a refusal, in its own `assertion:` store (spec, Gate
   registration). No verb other than `new`, `promote` (5b) and `retire` writes a row.
2. **Lock, lock order and durability.** Every verb holds `LockedFile` on `<stateDir>/generations.lock` for its whole operation (an
   in-JVM `ReentrantLock` per real path, then the `FileLock`; one JVM's threads would otherwise meet
   `OverlappingFileLockException`): read and validate the whole log, verify what the verb verifies, allocate, append, `force(true)`.
   The head verification of `new` (and of `promote`, 5b) runs inside that hold and takes the floor lock of `FloorStore` inside it:
   the order is always generation lock, then floor lock, and no code path takes the floor lock first and then waits for the
   generation lock. A partial last line (no LF, or invalid JSON) fails closed with `control-log-corrupt` on every verb and on every
   read, and is never discarded.
3. **History validation** (`GenerationStore.validate()`, run on every read): the first row of a number is its only `registered`
   row; numbers increase in registration order across the whole store, whatever the scope; `rulesDigest`, `inputReleases`,
   `replayFrom`, `headVersion`, `releaseEvidence` and the record key are identical on every row of a number; every transition is
   allowed by `GenerationPolicy`; at most one generation per scope has `current` as its latest status; no generation rests at
   `replaying` or `verified` (in the file store those rows exist only inside one promotion batch, so such a tail is an interrupted
   promotion). Any violation is `control-log-corrupt`. `current(scope)` never picks "the last current" out of a contradictory log.
4. **Policy** (`GenerationPolicy`, core, pure): `next(highest number of the whole store)`; the transitions `registered -> replaying
   -> verified -> current -> retired` and `registered -> retired`; one current per scope. The scope of a generation is the `streamId`
   of the release holding its attribution rule (the key of `forseti.generations`, spec 483). `run` and `retire` address a generation
   by its number inside the store they are given; outside a store a generation is named by its store and `registeredRowSha256`.
5. **`new` without a recording, no declaration** (owner decision 3). `new` writes exactly one row. With `--recording` it pins
   `replayFrom` at offset 0 of every subscribed partition of that recording; without one `replayFrom` is `{}`. The initial recovery
   declaration `{generation, injectionId: null, stage: 1, epochBefore: null, targetEpoch: 0, lastWrittenCutId: 0, routingDigest}`
   (spec 414 as edited) is written by the run preparation: `forseti run`, before the engine starts and under `generations.lock`,
   appends it with the recording's `routingDigest` when the generation has no declaration, and otherwise requires the declared
   `routingDigest` to equal the recording's (`generation-routing-mismatch`, exit 1) and writes nothing. Shuffled runs and the replay
   run of a generation therefore find the declaration of the first run.
6. **The gate and the store.** `gate --state <dir>` is valid for a full run and a `--shard` run; with `--aggregate` or
   `--negative-control` it is a usage error (exit 2). In both layouts the directory must be `<out>/state` (compared by real path;
   anything else is a usage error, exit 2), so the store of a shared run is always inside its own bundle: a shard run's at
   `<shards dir>/<shard>/state`, a full run's at `<out>/state`. No external-state binding exists. With `--state` the gate runs `forseti generation new` as a child JVM on its
   own classpath (as it already launches `forseti run`), system clock, entries `shared`; the directory is never deleted and an
   existing store is continued above its highest number. Without `--state` it calls `GenerationCli.run` in-process with the fixture
   clock (`GateSigner.now`) on a temporary store, entries `isolated`. Both are `store: pipeline`. Same code, same lock, same
   numbering. The assertions' private states (`Stage1Assertions`, `Stage2Assertions`, `Foundation`, `GenerationAssertions`) always
   use the in-process form on their own temporary stores, `store: assertion:<ID>`.
7. **Fixed order.** Before `Scenarios.Pipeline` submits anything, the gate registers sequentially one generation per selected
   scenario in `Scenarios.all()` order (5b: then one per generation run in plan order), each with `--recording`. Shuffled runs read
   their scenario's generation and register none. The child harness gets `--generation <n>` from that table (`"--generation", "1"` is
   hard-coded at `Scenarios.java` 276 today); `RunFold`, the diagnostics and the smoke and shuffle helpers take the number and the
   `rulesDigest` from the table, never from the first line of the store (as `Scenarios.java` 204 does today).
8. **Clock.** `GenerationCli.run(args, out, err, Clock)`; `Main` passes `Clock.systemUTC()`. No flag and no environment variable sets
   the clock.
9. **Kept evidence.** After folding, the gate copies each run's `run.json` and `cuts.json` to `<out>/runs/` (names in Report format)
   before deleting the run directory (`Scenarios.java` 210-212 delete the oracle output, the harness output and the state today;
   the state of a pipeline run is now the gate run's store and is not deleted). A full run writes `summaries.json` beside its
   report exactly as a shard run does, and both bind it in `summariesSha256`.
10. **One evidence validator, `GateEvidence.validate(root, bundle)`**, used by a full run and a shard run on their own output
    before they report, by `Shards.read` on every shard directory, and by `GateReportReader` (5b) on either layout. A bundle is one
    output directory: `report.json`, `summaries.json`, `runs/`, and `state/` when the run had `--state`. It returns problems; any
    problem is a coverage problem (exit 1). It requires:
    - identity: `(store, generation)` and `(store, registeredRowSha256)` are unique in the bundle's `generations[]`; every `runs[]`
      entry resolves to exactly one generation entry by `(store, registeredRowSha256)` and repeats its `generation` and `state`;
      `runId` is unique;
    - coverage, both directions: every `store: pipeline` run resolves to exactly one summary of the bundle's `summaries.json`
      (`runs[<scenario>]` for the non-shuffled `online` run, `shuffled[<scenario>@<seed>]` for a shuffled run, from 5b
      `generationRuns[<name>]`), and every summary has exactly one such run; a run entry without a summary, a summary without a
      run entry, and two entries for one summary are problems. The summary holds (`ok` true; for a shuffled summary
      `failure` null, `ok` true and `equal` true);
    - the locked recording: the summary's `recordingManifestSha256`, the entry's, the run record's
      `fixtureDigests.recordingManifestSha256` and the committed lock's `files["manifest.json"]` are one value;
    - exact cuts: the summary's `plannedCutIds`, `cutLogIds` and `comparedCutIds` are equal lists of length `counts.cuts` of the lock
      (the existing `Shards.coverage` rules, moved here), and the cut ids of the kept `cuts.json` (whose raw digest is
      `cutLogSha256`) are that list. This holds for a shuffled summary exactly as for a scenario summary: its three lists are
      its own (decision 12), and a shuffled summary without `comparedCutIds`, with a shorter list, or with `equal` false is a
      problem;
    - the run record (raw digest `runRecordSha256`) against the entry, the cut log and the report: `runId`, `generation`,
      `registeredRowSha256`, `scope`, `seed`, `shuffle`, `offsetRanges` and `cuts`; `subscribed` equals the partitions of the cut
      log's `offsetRanges` and `offsetRanges` is their union from offset 0; `fixtureDigests.rulesDigest` and
      the rules digest of the envelope pin set `pins` (computed as `RunFold` computes it for its `PINS` finding) equal the
      generation entry's `rulesDigest`; `pins.job.routingDigest` equals the cut log's `routingDigest`;
      `artifactSha256` equals the report's `pins.forseti.artifactSha256`; `jcs.java` equals the report's `pins.jcs.java`;
    - for every `shared` entry, the store: `state/generations.jsonl` holds exactly one `registered` row with that digest under that
      number, whose `rulesDigest`, key and pinned `releaseSha256` equal the entry's `rulesDigest`, `scope` and the run record's
      `fixtureDigests.releaseSha256`.
11. **Aggregate checks added to `Shards.read`.** Hash and parse the same bytes of each shard's `report.json` and `summaries.json`
    (`shards[]`); the shard report's own `summariesSha256` equals the digest of the `summaries.json` read; a shard report with
    `kind != gate` or `layout != shard` is a problem; `GateEvidence.validate` runs on every shard directory; merged `generations[]`
    and `runs[]` carry that shard's name, so `(shard, store, generation)` and `(shard, store, registeredRowSha256)` are unique and
    every run resolves by `(shard, store, registeredRowSha256)`; every planned scenario has exactly one non-shuffled `online`
    pipeline run in each shard that plans it. A missing or different file is a coverage problem (exit 1), so an incomplete artifact
    never reaches `promote`.
12. **Shuffled runs are compared with the oracle directly.** Today `Scenarios.shuffle` (394-420) folds the shuffled run and never
    calls the oracle, and `Summaries.of(Shuffled)` (127-135) writes `name, seed, failure, findings, rows` only; the per-cut equality
    with the base run lives in `Injections.determinism`. From 5a on a shuffled run goes through `Scenarios.fold` (306) like its
    scenario's run: the oracle is run on the same locked recording with the shuffled run's own `cuts.json`, and every cut is
    compared. Of the two possible meanings of "compared" this is the stricter one, and it is implementable: the oracle does not
    depend on delivery order inside a cut and already takes any run's `cuts.json`, so the code path exists; equality with the base
    run would rest the shuffled evidence on the base run and would leave out the cuts `Comparison.notComparable` skips. The
    equality with the base run stays where it is (`Injections.determinism`) as an additional check; it is not what "compared"
    means. Shuffled-summary schema: `{name, seed, failure, ok, summary, recordingManifestSha256, plannedCutIds, cutLogCuts,
    cutLogIds, cutsCompared, comparedCutIds, equal, findings, rows, ms}`. `recordingManifestSha256` is the raw digest of the
    manifest file the shuffled harness read (recomputed from the file, not copied from the base summary); `plannedCutIds` are that
    manifest's batch cut ids; `cutLogIds` are the ids of the shuffled run's own `cuts.json`; `comparedCutIds` are
    `tally.comparedIds` of the shuffled run's own oracle comparison, one id per cut actually compared, and are never derived from
    `plannedCutIds` or `cutLogIds`; `equal` is that tally's; `ok` = no failure, `equal`, and the three lists equal. A shuffled run
    whose base run failed, whose oracle run crashed, or whose comparison stopped early has a `failure` or a short
    `comparedCutIds` and is a coverage problem. Cost: one oracle run per shuffled run (two hand-made scenarios, three seeds), in
    the `hand-made` shard.
13. **Not in 5a:** `promote`, `GateReportReader`, `--replay` (the run record's `replay` and `originalRecordingManifestSha256` exist
    and are null), `I-20`, `I-23`, the non-empty-shard rule (`horizon-planned-late` still owns no assertion and still reports
    `0 / 0`, exit 0; no reader of reports exists yet).

- [ ] **Step 20.0** Spec and plan. Apply the SPEC block (a1, a2, b1-b5, c) and add Amendment 2026-11-13, the replaced Chunk 6 and the
  OTHER-EDITS to this plan. `git diff --stat` shows the spec and the plan only. Commit `docs(spec,plan): run citation on generation
  rows, gate registration through generation new, event-time replay; PR 5 as 5a and 5b`.

- [ ] **Step 20.1** `LockedFile` (+ `LockedFileTest`, 2: `threadsOfOneJvmWaitInsteadOfThrowing`, `theLockIsReleasedAfterAnException`);
  `FloorStore` on it. `Generation` gains `run` (null, or an object with exactly `runId` text, `runRecordSha256` 64 hex and
  `offsetRanges` an object; written after `gateReportSha256`, before `at`; a missing `run` is `IllegalArgumentException`).
  `GenerationPolicy` (+ `GenerationPolicyTest`, 6: `everyAllowedTransitionIsAllowed`, `everyOtherTransitionIsRefused`,
  `oneCurrentPerScope`, `numbersIncreaseAcrossScopes`, `aRetiredGenerationNeverReturns`,
  `aGenerationRestingAtReplayingOrVerifiedIsCorrupt`). `GenerationStore` gains `latestStatus(n)`, `registered(n)`, `validate()`.
  `mvn -q -pl core -Dtest='LockedFileTest,FloorStoreTest,GenerationPolicyTest' test` -> `Tests run: 2`, `Tests run: 3`,
  `Tests run: 6`, 0 failures.

- [ ] **Step 20.2** `FileGenerationStore.append` and `registeredRowSha256(n)` (+ `FileGenerationStoreTest`, 10: `appendAndReadBack`,
  `theKeyAndHeaderAreWritten`, `twoThreadsAllocateDistinctNumbers`, `twoProcessesAllocateDistinctNumbers` (two child JVMs through
  `GenerationStoreMain`, as `FloorStoreMain` does for the floor), `twoScopesShareOneNumberSequence` (`line2` -> 1, `line2-plc` -> 2,
  `line2` -> 3), `aTruncatedLastLineFailsClosed`, `aContradictoryHistoryFailsClosed` (a second `registered` row of one number;
  changed pins on a later row; two currents in one scope; a tail at `verified`: each `control-log-corrupt`), `everyAppendIsForced`
  (a spy on the channel), `anAppendNeverRewritesEarlierBytes`, `theRegisteredRowDigestIsTheDigestOfTheStoredLine`).

- [ ] **Step 20.3** `GenerationCli` (exit 0 ok, 1 refusal, 2 usage; `new` and `retire` print one JSON object, `list` prints the
  store's transport records one per line, byte for byte):

```
forseti generation new     --state <dir> --mappings <dir> --trusted-keys <file> --stream <s> [--stream <s2>] --rule <id>@<version> [--recording <dir>] [--definitions <file>]
forseti generation retire  --state <dir> --generation <n>
forseti generation list    --state <dir>
```

  `new`, inside one hold of `generations.lock` (decision 2): for every `--stream`, `HeadVerifier.verify(mappings, stream, trusted,
  new FloorStore(state), clock.millis())` (spec 568: the floor survives generation changes), `ReleaseReader.read`, `RuleSet.load`
  (a `Refusal` prints `{"refusal", ...evidence}`, exit 1); pins `rulesDigest`, `inputReleases [{stream, releaseId, version,
  releaseSha256, headVersion, statementSha256}]`, `replayFrom` (decision 5), `headVersion`, `releaseEvidence` (the verified `Head`
  summary, the `release.json` digest, the `pins` object), `gateReportSha256 null`, `run null`, `at`; appends `registered`; prints
  `{"generation", "status": "registered", "scope", "rulesDigest", "registeredRowSha256"}`. A floor raised by a verified head is
  never rolled back by a later failure of the same call. `retire` appends `retired` (`run null`, `gateReportSha256 null`); refusals
  `generation-unregistered`, `generation-transition`. `forseti run` gains the run preparation of decision 5 and records
  `registeredRowSha256`, `scope`, `replay` (null) and `originalRecordingManifestSha256` (null) in `run.json`; the two null fields
  are part of the record from this PR on, so every reader and test of 5b reads run records of one shape. It requires a unique valid
  registration and a valid history, verifies the release files against the pinned `releaseEvidence`, does not re-verify the head,
  and appends no generation row.

  `GenerationCliTest` (14; fixed clock = fixture `signedAt + 60 s`): `newRegistersAGenerationWithPins` (row fields, `run` null,
  `head-floors/line2.json`), `newWithoutARecordingPinsNoOffsetsAndWritesNoDeclaration`,
  `newWithARecordingPinsOffsetZeroAndWritesNoDeclaration`, `theFirstRunPreparationWritesTheInitialDeclarationOnce` (two runs of one
  generation: one declaration), `newRefusesAnExpiredHead` (`head-expired`), `newRefusesAProfileWithAnotherDigest`
  (`binding.schema-mismatch` on `fixtures/registry-mismatch`), `newFromRegistryNextRaisesTheFloor`,
  `anUnregisteredGenerationRefusesTheHarness` (`generation-unregistered`), `runAppendsNoGenerationRow`, `retireAppendsARetiredRow`,
  `twoScopesInOneStoreAreAddressedByNumber` (`new` on `line2` -> 1, on `line2-plc` -> 2; `run` and `retire` on each by number;
  `byNumber(1)` and `byNumber(2)` return one row sequence each), `aCorruptControlLogFailsClosed`,
  `listPrintsTheStoredRecordsByteForByte`, `theClockIsNotSettableFromTheCommandLine` (`--now` is a usage error, exit 2).
  `ReferenceEngineTest` +2 (19 -> 21): `aRunOnAnotherRoutingThanTheDeclaredOneIsRefused`,
  `anOnlineRunRecordsItsRegistrationAndNullReplayFields`.
  `mvn -q -pl core install -DskipTests && mvn -q -pl cli -Dtest='FileGenerationStoreTest,GenerationCliTest,ReferenceEngineTest' test`
  -> `Tests run: 10`, `Tests run: 14`, `Tests run: 21`, 0 failures. Commit `feat(core,cli): generation policy, locked durable
  control log, generation new|retire|list, run preparation`.

- [ ] **Step 20.4** The gate on one store. `CoreGate`: `--state`, the registration table (decisions 6-7), `kind`, `layout`,
  `negativeControl`, `summariesSha256`, `results[].shard`, `generations[]`, `runs[]` (each with `store`), `<out>/runs/`, and
  `summaries.json` in a full run. `Scenarios`: the generation from the table; `shuffle` runs the oracle comparison and `Summaries.of(Shuffled)` writes the schema of
  decision 12. `GateEvidence.validate` (decision 10), called by the
  full and the shard run on their own output; `Shards.coverage`'s rules move into it. `Shards.read`: decision 11 and `shards[]`.
  `Stage1Assertions.register` and `Stage1Assertions.append` are deleted; the private states register through the in-process verb.
  Tests: `CoreGateTest` +8 (8 -> 16): `stateIsRefusedWithAggregateAndWithANegativeControl` (exit 2),
  `theReportNamesItsKindAndLayout`, `theGateRegistersThroughTheVerbInAFixedOrder` (two runs of one selection give the same
  scenario -> number table; the store holds only `registered` rows), `aRunWithoutStateMarksEveryGenerationIsolated`,
  `aScenarioSummaryDoesNotDependOnItsGenerationNumber` (`smoke` as generation 1 of one store and as generation 3 of another:
  summaries equal on every field outside `Shards.NOT_COMPARED`), `aFullRunWritesItsSummariesAndBindsTheirDigest`,
  `stateOutsideTheOutputDirectoryIsAUsageError` (full and shard), `aFullRunWithAMissingOrFailedShuffledComparisonExitsOne` (full
  layout: a shuffled run whose oracle comparison stops one cut short, and one whose comparison differs at one cut: a coverage
  problem and exit 1, each). `ShardsTest` +11 (14 -> 25): `theAggregateReportBindsEveryShardReportAndSummariesDigest`, `generationsAndRunsAreMergedUnderTheirShard`,
  `aSharedGenerationWhoseRowRunRecordOrCutLogIsNotInTheArtifactIsAProblem`, `aControlOrAggregateReportAmongTheShardsIsAProblem`,
  `twoShardsMayRegisterTheSameScenarioUnderDifferentNumbers`, `identicalRegistrationsInTwoStoresOfOneShardStayDistinct` (the
  pipeline store and two `assertion:` stores each hold generation 1; two of them hold byte-identical rows under the fixture clock:
  no problem, and each run resolves to the entry of its own store), `aRunThatResolvesOnlyByDigestInAnotherStoreIsAProblem`,
  `aRunWithoutASuccessfulSummaryOrWithOtherCutIdsIsAProblem` (an inserted run entry with no summary; a summary whose `ok` is false;
  compared cut ids one short; a kept cut log with another id: each a problem),
  `aRunRecordWhoseSubscriptionsPinsRoutingOrArtifactDifferIsAProblem` (each of the four),
  `aShuffledSummaryWithoutItsOwnComparedCutIdsIsAProblem` (aggregate layout: the old five-field summary; `comparedCutIds` absent;
  one id short; a `recordingManifestSha256` other than the lock's), `aShuffledSummaryWhoseComparisonFailedIsAProblem` (aggregate
  layout: `equal` false with complete lists; `failure` set).
  `mvn -q -pl cli -Dtest='CoreGateTest,ShardsTest' test` -> `Tests run: 16`, `Tests run: 25`, 0 failures.

- [ ] **Step 20.5** Assertions (+3, `pr: 5`, class `GenerationAssertions`), all owned by shard `hand-made`
  (`gate/core-shards.json`: `hand-made.assertions` += `F-GEN-NEW`, `F-GEN-REFUSES-UNREGISTERED`, `F-GEN-FLOOR`; 59 -> 62; no other
  shard and the `aggregate` list change):

| Id | What runs |
|---|---|
| `F-GEN-NEW` | `generation new` in its own temporary state with the fixture clock: the `registered` row and its pins, `run` null, `head-floors/line2.json`, no recovery declaration; `registeredRowSha256` printed = digest of the stored line; then `forseti run` on `smoke`: exactly one declaration with the recording's `routingDigest`, still one generation row; a second run adds neither. Listed `store: assertion:F-GEN-NEW`, `state: isolated`, `variant: self-test` |
| `F-GEN-REFUSES-UNREGISTERED` | the harness refuses generation 7 (`generation-unregistered`) and writes no declaration |
| `F-GEN-FLOOR` | in its own temporary state (never the gate's `--state`): `new` on `registry` (floor 1), on `registry-next` (floor 2), on `registry` again: `head-rollback`; the two registered generations are listed `store: assertion:F-GEN-FLOOR`, `state: isolated`, `variant: self-test`, `assertion: F-GEN-FLOOR` |

  Report-format additions of this PR: the table "Report format" above, with `variant` values `online` and `self-test` only.

- [ ] **Step 20.6** CI. `ci.yml`, job `gate`: the step "Core gate shard" gains `--state "build/shards/${{ matrix.shard }}/state"`
  (inside the uploaded path), so each `gate-shard-<shard>` artifact holds `build/shards/<shard>/{report.json, summaries.json, logs/,
  diffs/ (on failure), runs/*.run.json, runs/*.cuts.json, state/{generations.jsonl, generations.lock, recovery-control.jsonl,
  head-floors/<stream>.json}}`. `upload-artifact` v4 leaves hidden files out, which drops `head-floors/.lock`; `FloorStore`
  recreates it and no lock file is evidence. Job `core-gate` gains a first step that fails the always-running required job when a
  dependency did not succeed:

```yaml
      - name: Build and every shard job succeeded
        if: needs.build.result != 'success' || needs.gate.result != 'success'
        run: |
          echo "build: ${{ needs.build.result }}; gate shards: ${{ needs.gate.result }}"
          exit 1
```

  Its aggregate step is unchanged in this PR (the aggregate now also checks decisions 10-11). `nightly-gate.yml`: the gate step
  gains `--state build/gate/state` (the full run's `<out>/state`); the artifact path stays `forseti/build/gate/`, which now holds
  the store. Pins, the six-shard
  matrix, `fail-fast: false`, the 60-minute shard timeout, PR cancellation and "no recording cache" are unchanged.

- [ ] **Step 20.7** `mvn -B -ntp -q install`; then locally each of the six shards with `--state build/shards/<shard>/state`
  (`hand-made`: `assertions run 62 / expected 62`; `horizon-no-departure`, `horizon-reopened`, `horizon-correction-beyond`,
  `plc-horizon-no-end`: `1 / 1`; `horizon-planned-late`: `0 / 0`, exit 0), then `--aggregate build/shards` -> last line
  `assertions run 75 / expected 75`, exit 0 (62 + 4 + 9); a full unsharded run with `--out build/gate-full --state build/gate-full/state` -> the same line, and without
  `--state` -> the same line with every `generations[].state` `isolated`; `--negative-control` list still empty. Commits `feat(cli):
  the gate registers through generation new into one store per run; report kind, layout, shards, generations, runs, store; one
  evidence validator; F-GEN-NEW, F-GEN-REFUSES-UNREGISTERED, F-GEN-FLOOR` and `ci: shard artifacts carry the control store and run
  records; core-gate fails on a failed dependency`.

- [ ] **Step 20.8** Pull request 5a: Task 20 (4 commits); CI green. PR body: the spec additions, the store and lock rules, the
  registration order, the report format, the gate line `assertions run 75 / expected 75`, the shard times.

  **What 5a's CI proves:** `assertions run 75 / expected 75` from six shards and the aggregate; every scenario is still compared
  with the oracle at every planned cut after its generation moved from a hand-written row to the verb (the change that touches
  every run, so a mistake breaks the 72 existing assertions); the numbers of a gate run are a function of its selection; the
  aggregate report commits to its inputs by digest, resolves every generation and run inside its own `(shard, store)`, and refuses
  an artifact without its store, run records or cut logs, or with a run that no successful summary covers; the nightly unsharded
  run with `--state` agrees and passes the same validator.

## PR 5b

Branch: `git switch -c feat/step3-promotion-and-replay main` after PR 5a merged.

### Task 21: Promotion bound to verified whole-gate evidence; event-time replay; injections 20 and 23

**Files:**
- Create: `cli/.../generation/GateReportReader.java`, `cli/src/test/.../generation/GateReportReaderTest.java`; `cli/.../recording/ReplayRecording.java`, `cli/src/test/.../recording/ReplayRecordingTest.java`; `cli/src/test/.../gate/RunFoldTest.java`; `scripts/gate-then-promote.sh`; `fixtures/scenarios/horizon-planned-late.replay.lock.json`, `fixtures/scenarios/smoke.replay.lock.json`
- Modify: `GenerationCli` (`promote`), `GenerationPolicy`, `FileGenerationStore` (`appendBatch`), `Main` (`run --replay event-time`, `recordings replay`), `ReferenceEngine` and `RunRecord` (set `replay`, `originalRecordingManifestSha256`), `core/.../journal/Envelope.java` (`timerDeadline(cause)`), `core/.../journal/JournalFold.java`, `cli/.../gate/RunFold.java`, `Stage1Assertions` (the three timer-cause readers), `Stage1`/`Stage2` (cause kind), `Scenarios`/`Summaries` (generation runs, kept journals), `GateEvidence` (generation runs, journals), `Shards` (no empty shard; plan field `generationRuns`), `CoreGate` (the `emptyShard` branch removed), `Injections` (+ `I-20`, `I-23`), `GenerationAssertions` (+ `F-GEN-LIFECYCLE`, `F-GEN-PROMOTE-REQUIRES-REPORT`), `gate/core-assertions.json` (+4 = 79), `gate/core-shards.json`, `.github/workflows/{ci,nightly-gate}.yml`, `oracle/src/forseti_oracle/recording.py` (accepts the replay `source` fields), `oracle/tests`, `core/src/test/.../journal/JournalFoldTest.java`

- [ ] **Step 21.1** `GateReportReader`. Everything is checked against files the reader loads itself from the repository root it
  runs in, `gate/core-assertions.json` and `gate/core-shards.json`; no flag and no report field points it at another manifest or
  plan. The evidence bundle is, for `full`, the report's own directory, and for `aggregate`, each `<shards dir>/<shard>/`.
  Refusals are closed codes, exit 1, with evidence, checked in this order:

| # | Check | Refusal |
|---|---|---|
| 1 | the file parses strictly (duplicate keys refused, every required field present with its type); its raw SHA-256 is the digest `promote` records | `gate-report-unreadable` |
| 2 | `kind == gate`, `negativeControl` null, `intendedFailures` empty | `gate-report-negative-control` |
| 3 | `layout` is `full` or `aggregate`; `shard` null | `gate-report-shard` |
| 4 | `exit == 0`, `error` null, `failed` and `notRun` empty, every result `ok` | `gate-report-failed` |
| 5 | `manifestSha256` equals the committed manifest's raw SHA-256 | `gate-report-manifest` |
| 6 | the set of `results[].id` equals the committed manifest's id set, each id once; then `assertionsExpected == assertionsRun == N`, N the manifest's entry count. Counts alone never pass: `0/0`, a reduced `k/k`, a count above N, and N results with one id replaced by a repeat of another are all refused (spec 690) | `gate-report-assertions` |
| 7 | `full`: `shardPlanSha256` null, `shards[]` empty, every `results[].shard` null, `summariesSha256` 64 hex. `aggregate`: `summariesSha256` null; `shardPlanSha256` equals the committed plan's; `Shards.validate(plan, manifest ids)` holds; `shards[]` names equal the plan's shard names in plan order, each once, each `exit == 0` with two 64-hex digests; every `results[].shard` equals the plan owner of its id | `gate-report-layout` |
| 8 | evidence files, both layouts, by the one validator. `aggregate`: `--shards <dir>` is given; for every shard the raw SHA-256 of `<dir>/<name>/report.json` and `summaries.json` equal `shards[]`; `Shards.read(<dir>)` (the aggregate run's own checks: accounting, ownership, pins, cut identities, duplicate summaries, `GateEvidence.validate` on every shard, generation-run coverage) reports no problem and its merged `results`, `generations[]` and `runs[]` equal the report's. `full`: `--shards` is absent; the raw SHA-256 of `<report dir>/summaries.json` equals `summariesSha256`; `GateEvidence.validate(<report dir>)` reports no problem; the scenario, shuffled and generation-run sets of `summaries.json` are exactly the ones a full run of the committed plan makes (every scenario of `Scenarios.all()`, every planned shuffled key, every planned generation run; none missing, none extra). So in either layout every `runs[]` entry is one that a successful summary, a locked recording and the exact planned, completed and compared cut ids cover, and an entry added to `runs[]` beside a complete result-id set is refused. Every `runRecord`, `cutLog` and `journals` path is relative, has no `..` and resolves inside its bundle | `gate-report-evidence` |
| 9 | the target generation is selected by producer and store first, then by digest: among the `generations[]` entries with `store == pipeline` whose `shard` is the bundle of `--state` (the location is enforced by real path in both layouts: `full`: `shard` null and `--state` is `<report dir>/state`; `aggregate`: `--state` is `<dir>/<entry.shard>/state`; any other directory is refused, even one holding a byte-identical log, so promotion happens in the bundle's own preserved store), exactly one has the `registeredRowSha256` given by `--registered-row-sha256`. An entry with that digest in another shard or another store neither qualifies nor makes the selection ambiguous. The digest `promote` computes from the `registered` row of `--generation n` in its own `--state` equals it; that entry's `generation == n`, `scope` equals the row's key, `rulesDigest` equals the row's | `gate-report-generation` |
| 10 | that entry's `state == shared` | `gate-report-generation-isolated` |
| 11 | exactly one `runs[]` entry with the target's `(shard, store, registeredRowSha256)` has `runRecordSha256` equal to the raw SHA-256 of the `--run` file; `shuffle` false; the run record's own `generation`, `registeredRowSha256`, `scope`, `runId`, `fixtureDigests.rulesDigest`, `fixtureDigests.recordingManifestSha256`, `replay`, `originalRecordingManifestSha256` and `offsetRanges` equal the entry's and the row's; the cut log beside it has `cutLogSha256`; when `journals` is non-null the journal index has `journalsIndexSha256` and every listed file its digest | `gate-report-run` |
| 12 | the target run's coverage, read from the validated bundle (check 8): its summary (`runs[<scenario>]` for `variant: online`, `generationRuns[<name>]` for `event-time-replay`) holds, names the committed lock of its recording (the replay lock for a replay run), and its planned, completed and compared cut ids are the cut ids of the cited cut log, one list; its subscriptions, release and rule pins, `routingDigest` and `artifactSha256` equal the run record's and the report's pins | `gate-report-coverage` |
| 13 | `scope` is accepted: `semantic` by default, `core` only with `--accept-scope core` | `gate-report-scope` |

  Digests establish integrity between the files. The report is not authenticated: what makes its success trustworthy is the
  operator-selected bundle or the CI run that produced it. Closing that (an in-toto attestation over the report digest) belongs to
  the semantic gate of steps 6-7.

- [ ] **Step 21.2** `promote`:

```
forseti generation promote --state <dir> --mappings <dir> --trusted-keys <file> --generation <n> --registered-row-sha256 <hex>
                           --run <run.json> --gate-report <report.json> [--shards <dir>] [--accept-scope core]
```

  A missing `--run`, `--gate-report`, `--generation` or `--registered-row-sha256` is a usage error (exit 2). Everything after
  argument parsing happens in one hold of `generations.lock`, in this order (lock order: generation lock, then floor lock; Task 20,
  decision 2):

  1. re-read and validate the whole history (`control-log-corrupt`);
  2. the reader checks of Step 21.1 against the store as it is now;
  3. the final approval and floor verification: for every stream of `inputReleases`, the head verified with the system clock and
     the store's own floor (spec 568; `head-expired`, `head-rollback`), which must still name the pinned release (`releaseSha256` and
     `statementSha256` equal the pin; else `generation-release-superseded`). Because this runs inside the hold, a registration that
     raised the floor before the hold began is seen, and none can raise it between this check and the publication;
  4. the generation's latest status is `registered` (`generation-transition`; a second promote is refused);
  5. publication: one buffer holding `replaying`, `verified`, then, when the scope has a current generation, that generation's
     `retired` row, then `current` (so that no prefix of the batch holds two currents); the three rows of the promoted generation
     carry `run {runId, runRecordSha256, offsetRanges}`; `verified` and `current` carry `gateReportSha256` = the raw SHA-256 of the
     whole report given, also when it is an aggregate. Three rows when the scope has no current generation, four otherwise. The
     buffer is appended at the end of the file by a loop that continues until it is drained (a short write is continued, never
     treated as done), then `force(true)`; success is printed only after `force` returns.

  Refusals before publication (steps 1-4: usage, validation and policy refusals) write nothing: the bytes of `generations.jsonl`
  are those the hold found. That is the meaning of "no refusal writes a row". An I/O failure during publication is different: when
  a write or `force` throws, or the channel stops accepting bytes, the verb exits 1 with `control-log-io` and the count of bytes
  it appended, never prints success, and makes no promise about the file, which may hold a partial batch. The next read decides:
  zero appended bytes is the old valid state; any non-empty proper prefix of the batch is `control-log-corrupt` on every reader
  and every verb (a tail at `replaying` or `verified`, a retired previous current with the target at `verified`, or a partial
  line; Task 20, decision 3). The design is fail-closed under prefix truncation, consistent with spec 570; it is not an atomic
  filesystem transaction and does not claim to be.
  Output `{"generation", "status": "current", "registeredRowSha256", "gateReportSha256", "run", "retired": <number or null>}`.

  Tests. `GateReportReaderTest` (34): `aValidFullReportIsAccepted`, `aValidAggregateReportIsAccepted`,
  `theManifestAndPlanAreReadFromTheRepositoryNotFromTheReport`, `aReportThatIsNotStrictJsonIsRefused`,
  `aNegativeControlReportIsRefused`, `aShardReportIsRefused`, `aFailedOrErroredReportIsRefused` (exit 1; `error` set; a `failed`
  entry; a `notRun` entry; a result with `ok` false: each alternative exercised), `anotherManifestDigestIsRefused`,
  `zeroOfZeroIsRefused`, `reducedEqualCountsAreRefused`, `aCountAboveTheManifestIsRefused`, `aRepeatedIdInPlaceOfAnotherIsRefused`,
  `anotherShardPlanDigestIsRefused`, `aMissingDuplicateOrUnplannedShardIsRefused` (each of the three), `aResultUnderTheWrongOwnerIsRefused`,
  `aFullReportWithShardsIsRefused`, `aChangedShardReportOrSummariesIsRefused` (each of the two),
  `aTargetPresentOnlyInTheAggregateIsRefused`, `anEvidencePathOutsideTheBundleIsRefused` (absolute; `..`),
  `anotherRegisteredRowDigestIsRefused`, `aStateThatIsNotTheShardsOwnStoreIsRefused`, `anIsolatedGenerationIsRefused`,
  `anotherRunRecordDigestIsRefused`, `aRunUnderAnotherShardOrRegistrationIsRefused` (each of the two), `aShuffledRunIsRefused`,
  `aChangedCutLogIsRefused`, `coreScopeWithoutAcceptanceIsRefused`, `aFullReportWithAnInsertedUnvalidatedRunIsRefused` (a complete
  result-id set plus one `runs[]` entry no summary covers, its run record and cut log present: `gate-report-evidence`),
  `aFullReportMissingVariantOrCutCoverageIsRefused` (the replay generation run absent from `summaries.json`; a compared cut id
  removed; a planned scenario absent: each refused), `aFullReportWhoseSummariesChangedIsRefused`,
  `aDigestThatMatchesOnlyInAnotherStoreOrShardSelectsNothing` (the same `registeredRowSha256` in an `assertion:` store and in
  another shard's pipeline store: `gate-report-generation`; and with the right entry present beside them: accepted),
  `aTargetWhoseJournalsChangedIsRefused`, `aFullReportPromotedInAStateOutsideItsBundleIsRefused` (a copy of the store elsewhere:
  `gate-report-generation`), `aMissingOrFailedShuffledComparisonIsRefusedInBothLayouts` (a full and an aggregate bundle, each
  with a shuffled summary lacking one compared cut id and with one whose `equal` is false: `gate-report-evidence`). The manifest size is read from the committed file; the positive fixtures list every
  committed manifest id and are validation fixtures, not delivery evidence.
  `GenerationCliTest` +10 (14 -> 24; fixed clock, fixture reports): `promoteAppendsReplayingVerifiedCurrentInOneOperation` (the three
  rows carry the same `run`), `promoteRetiresThePreviousCurrentOfTheScope`, `promoteVerifiesTheHeadAtTheCurrentTime` (an expired
  head: `head-expired`; a lowered head: `head-rollback`; no row either time), `promoteRefusesAGenerationWhoseHeadMovedOn`,
  `promoteWithoutARunOrAReportIsAUsageError`, `twoScopesAreCurrentAtOnce`, `aSecondPromoteOfTheSameGenerationIsRefused`,
  `aFloorRaisedByAnotherRegistrationBeforePublicationRefusesThePromotion` (the interleaving test: a package-private hook that
  `promote` calls after parsing its arguments and before taking the lock runs `generation new` on `registry-next` in the same
  store, raising the floor to 2; the promotion of the generation pinned at head 1 is refused `head-rollback`; the log holds the new
  `registered` row and no `replaying`, `verified`, `current` or `retired` row),
  `aRegistrationStartedDuringAPromotionLandsAfterItsRows` (a hook inside the hold starts `generation new` on another thread and
  the promotion completes: the log's order is the whole promotion batch, then the `registered` row; the registration never falls
  between verification and publication), `aWriteOrForceFailureIsControlLogIoAndNeverSuccess`.
  `FileGenerationStoreTest` +5 (10 -> 15), the interruption obligations of a promotion batch, on a fake channel that the test
  drives: `zeroAppendedBytesLeaveTheOldValidState` (the write throws before any byte: the store reads as before, the previous
  current is still current, verbs work); `everyNonemptyProperBytePrefixOfAPromotionFailsClosed` (for a three-row and a four-row
  batch, every length from 1 to the batch length minus 1, among them the complete-line boundaries after `replaying`, after
  `verified` and after the previous current's `retired`: `validate`, `current(scope)`, `byNumber`, `list`, `new`, `retire`,
  `promote` and the harness's read each refuse with `control-log-corrupt`, and none repairs or truncates the file);
  `aCompleteForcedBatchValidatesAndPreservesEveryEarlierByte` (the file's earlier bytes are a prefix of the new file, byte for
  byte; three rows without a previous current, four with one; the rows' `run` and `gateReportSha256` are consistent with one run
  record and one report; exactly one current for the scope); `aShortWriteIsContinuedAndNeverReportedAsSuccessEarly` (a channel
  that accepts a few bytes per call: the batch is complete before `force`, and success is returned only after `force`; a channel
  that accepts some bytes and then nothing: no success); `aWriteOrForceExceptionNeverProducesASuccess` (an exception at each
  write call and at `force`: an exception propagates, no success response).
  `GenerationPolicyTest` +1 (6 -> 7): `aPromotionCitesOneRunAndOneReportDigest`.
  `mvn -q -pl core -Dtest=GenerationPolicyTest test && mvn -q -pl core install -DskipTests && mvn -q -pl cli
  -Dtest='GateReportReaderTest,GenerationCliTest,FileGenerationStoreTest' test` -> `Tests run: 7`; `Tests run: 34`, `Tests run: 24`,
  `Tests run: 15`; 0 failures. Commit `feat(cli): promotion bound to whole-gate evidence by store, registered-row and run digest,
  verified under the generation lock`.

- [ ] **Step 21.3** `--replay event-time` (Decision 11, spec 73; the four paragraphs "Event-time replay", "Replay order", "Replay
  batches", "Replay watermarks and deadlines" after spec 570; injection 23, spec 665; owner decisions 5-7).

  `ReplayRecording.materialize(original, outOfOrdernessMs, out)`; also `forseti recordings replay --recording <dir>
  --out-of-orderness-ms <n> --out <dir> [--lock <file>]`. In order:
  1. Refuse `replay-input-unconsumed` (a record beyond the last batch's through-offset) and `replay-idle-inputs` (a non-empty
     `idleInputs`; every committed recording has `[]`).
  2. Ordering key per record, by the spec: a process message's smallest valid member `sourceTs` (validity = the per-member
     validation the harness already applies to each member, unchanged; an invalid member contributes no key and keeps its ordinary disposition);
     session event time; occupancy `eventTime`; untimed records take the key of the nearest earlier keyed record of their
     partition; the untimed head stays first. Sort each keyed partition by (key, original offset). A partition without any key
     (the six `quality.incidents` partitions) is copied: order, offsets, through-offsets. The ordering key is computed by
     `ReplayRecording` and is not `Events.eventTime` (`core/.../stage1/Events.java` 51-57), which keeps returning a message's largest
     member `sourceTs` for the contract order inside a cut; nothing in `Stage1` or the watermark changes.
  3. Cut targets from telemetry only: `T_k(p)` and `C_k` from the original batches' `ot.messages` records (largest valid member
     `sourceTs`); the qualifying-prefix rule per partition; undefined targets include only the untimed-head records the original
     batches include; the last batch includes everything. Through-offsets per batch follow; cut ids are the original's.
  4. Watermark progression: `W1_k(s)` over the original and `W1'_k(s)` over the replay batches for every station and cut, by the
     formula of spec 607 with the given `outOfOrdernessMs` (stations by `routing.json`, as the harness routes).
  5. Deadlines, never copied unproved: stations of each original deadline (`replay-deadline-unproved` when none); keep `cutA`,
     `cutB` only when `W1'_cutA(s) < at ≤ W1'_cutB(s)` for every one of its stations; else replan to the consecutive pair that
     satisfies it for all of them; else `replay-deadline-unbracketed` (exit 1, with the deadline, its stations and their `W1'`
     sequences). The replay manifest's `deadlines` carry the resulting cut ids (the shape `Recording.readDeadlines` reads, lines
     111-121), and `source.deadlines` the proof (`stations`, `originalCutA`, `originalCutB`, `cutA`, `cutB`, `w1A`, `w1B`).
  6. Files: every record once, bytes unchanged, contiguous offsets from 0; `offset-map/<topic>/<partition>.jsonl` (replay offset ->
     original offset), listed in the manifest's `digests`; `idleInputs` `[]`; `manifest.json` with `source.replayOf` = the original
     manifest's raw SHA-256 and `source.outOfOrdernessMs`; `routing.json` unchanged (equal `routingDigest`, so the run preparation
     finds the generation's declaration valid); `expected.json` `{"expectedVersion": 1, "scope": "replay", "mode", "visits": [...],
     "transitions": [...]}`. `visits` are the immutable facts of the input, copied per visit from the original's `expected.json`:
     `visitRef`, `key`, `unitId`, `station`, `enter`, `leave`, `attributionEnd` (what the recording's occupancy or stamps say, the
     same in every generation; the expiry `enter + H + L` or `leave + H + L` is computed from them with the run's rule). The
     online outcomes are not carried over: no `finalStatus`, `insufficiencyReasons`, `retraction`, `equipmentEvidence`, `samples`,
     `sampleRuns`, `dispositions`, `messageDispositionRuns` or `incidents`. `transitions` are the replay brackets per station.
     `Expectations` treats scope `replay` like `minimal` (no per-sample ground truth), and replay output is never compared with
     the online run's late-arrival expectations. An original whose `expected.json` has no `visits` (the minimal scope of the smoke
     recordings) gives `visits: []`.

  After-horizon evidence of a replay summary. `Summaries.afterHorizon` (`Summaries.java` 111-125) computes `need`, `station`,
  `lastW1`, the largest and the last `onlineVisits`, `workRecords` and `visits` for a replay run from the replay recording's
  `expected.visits` and the replay run's own cut log and fold, exactly as for an online run; no online sample outcome is read.
  `Injections.afterHorizonNeed` (904-915) no longer returns `Long.MIN_VALUE` (which `Json.putSafe` rejects): without visits it
  throws, the summary of a synthetic run then carries a `failure` naming the missing visit facts, and `J-AFTER-HORIZON-CUTS`
  fails for that run by name.

  `forseti run --replay event-time` materializes into `<out>/replay-recording/` with the `outOfOrdernessMs` of the generation's
  attribution rule, verifies it against `--replay-lock <file>` when given, runs on it, and refuses a replay recording whose
  `source.outOfOrdernessMs` is not its rule's (`replay-recording-rule-mismatch`). It stamps `cause.kind replayed` on every row: an
  input-triggered row keeps its `inputKind`, `identity` and `trigger` (replay offsets); a timer-emitted row carries `inputKind`
  `timer`, `identity {"deadline"}` and a null `trigger` (`Stage1` lines 373 and 432 and `Stage2` lines 226 and 234 build the timer
  cause today; they take the kind as a constructor value; outside a replay nothing changes). It sets `replay: "event-time"`,
  `originalRecordingManifestSha256` and the replay manifest's digest (`fixtureDigests.recordingManifestSha256`) in `run.json`;
  `--replay` with `--shuffle` is a usage error (exit 2). The generation's `replayFrom` is offset 0 of every partition of the
  original; `run.json.offsetRanges` and `cuts.json` name replay offsets.

  One deadline check for both timer causes (owner decision 6). `Envelope.timerDeadline(cause)` (core) returns the deadline of
  `{"kind": "timer", "deadline": d}` and of `{"kind": "replayed", "inputKind": "timer", "identity": {"deadline": d}}`, and null for
  every other cause. It replaces the three places that recognize only `kind: timer` today: `RunFold.belongsToEarlier` (265-273:
  a replayed timer row has a null trigger and would otherwise pass as belonging to no cut), `JournalFold.belongsTo` (timer branch
  at 87) and the stage-1 segment check of `Stage1Assertions` (371). A row whose `timerDeadline` is at or below the event time of
  a progress belongs to that cut or an earlier one, whichever cause carried it.

  The oracle is given the materialized recording and that run's `cuts.json`, unchanged apart from accepting the replay `source`
  fields (`oracle/tests`: +1, `test_a_replay_recording_is_read_like_any_recording`). The replay recording is pinned like any other:
  `fixtures/scenarios/<name>.replay.lock.json` (file digests, `counts.cuts`, the original lock's `scenarioSha256`, the
  `outOfOrdernessMs` used) is committed for `horizon-planned-late` and `smoke`, and the gate compares the materialized bytes with
  it before the run. After a replay run the gate checks, on the `W1` the run reported in `cuts.json`, `W1_cutA(s) < at ≤ W1_cutB(s)`
  for every deadline and station of `source.deadlines`, and `W1` equal to the manifest's `w1A`, `w1B`.

  `ReplayRecordingTest` (15): `everyRecordOfTheOriginalAppearsOnceWithItsBytes`,
  `eachPartitionIsOrderedByOrderingKeyThenOriginalOffset`, `aRecordWithoutEventTimeTravelsWithItsPredecessor`,
  `aPartitionWithoutEventTimesKeepsItsOrderAndBatches`, `materializingTwiceGivesTheSameBytes`, `theSmokeReplayEqualsItsCommittedLock`,
  `aFutureDatedOccupancyOrSessionEventSetsNoCutTarget` (an occupancy and a session event dated beyond every telemetry timestamp in
  original batch 1: no later telemetry is pulled into replay batch 1, every station's `W1'` at every cut is as without them, and
  the two events wait for the last batch), `partitionsWithUnequalProgressFollowTheirOwnTargets` (raw partition 0 ahead of
  partition 1: each `ot.messages` partition is cut at its own `T_k(p)`, a station fed by the slower one does not advance, and a
  session event up to `C_k` is included), `aMixedMemberMessageIsOrderedByItsSmallestAndAdvancesByItsLargestTimestamp` (the message
  enters the batch its smallest timestamp qualifies for; `W1'` of that cut is computed from its largest; the manifest records the
  larger watermark), `aDeadlineWhoseBracketNoLongerHoldsIsReplanned` (that message lifts `W1'_cutA` to the deadline: the bracket
  moves to the earlier consecutive pair, `source.deadlines` names both pairs), `aDeadlineWithNoBracketRefusesTheMaterialization`
  (`replay-deadline-unbracketed`; nothing is written), `theUntimedHeadKeepsItsOriginalBatches`,
  `aBatchPrefixWithoutTimestampedTelemetryIncludesOnlyTheUntimedHead`, `thePlannedLateFixtureKeepsEveryBracketAndItsPlannedA` (the
  real `horizon-planned-late` recording, built as the gate builds it: every deadline keeps its cut ids, `W1'_k(s) = W1_k(s)` at
  every cut, the planned cut's `W1'` equals `A`, the three late samples sit at their event-time positions, and the bytes equal
  the committed replay lock), `thePlannedLateReplayKeepsTheVisitFactsItsAfterHorizonNeedIsComputedFrom` (the real fixture: the
  replay `expected.json` holds every visit of the original with equal `station`, `enter` and `leave` and no outcome field;
  `afterHorizonNeed` on it equals the original's and is at or below the last cut's `W1'` of its station; and the summary built
  by `Summaries.of` from a replay run of that recording, with a cut log carrying the manifest's `W1'`, has a complete
  `afterHorizon` object that `J-AFTER-HORIZON-CUTS` accepts).
  `RunFoldTest` (new, 3): `anOrdinaryTimerRowAfterItsClosingProgressFailsProgressOrder`,
  `aReplayTimerRowAfterItsClosingProgressFailsProgressOrder` (a row with the replayed timer cause, a null trigger and a deadline at
  or below the `W1` of a progress is placed after that progress: the fold's findings contain `J-PROGRESS-ORDER`),
  `aReplayTimerRowBeforeItsClosingProgressHolds`. `JournalFoldTest` +1 (6 -> 7): `aReplayTimerRowIsJudgedByItsDeadlineLikeATimerRow`.
  `ReferenceEngineTest` +4 (21 -> 25): `aReplayedRunCarriesCauseReplayedOnEveryRow` (input rows keep their trigger; timer rows carry
  the replayed timer cause and a null trigger), `aReplayedRunRecordsBothManifestDigests`, `replayWithShuffleIsAUsageError`,
  `aReplayRecordingOfAnotherOutOfOrdernessIsRefused`.
  `mvn -q -pl core -Dtest=JournalFoldTest test && mvn -q -pl core install -DskipTests && mvn -q -pl cli
  -Dtest='ReplayRecordingTest,RunFoldTest,ReferenceEngineTest' test` -> `Tests run: 7`; `Tests run: 15`, `Tests run: 3`,
  `Tests run: 25`; 0 failures; `python -m pytest -q oracle` -> all passed.

- [ ] **Step 21.4** Generation runs in the pipeline. A generation run is a named extra run of a scenario:
  `horizon-planned-late@replay` (shard `horizon-planned-late`, the gate run's pipeline store, registered after the scenario
  generations, run on the heavy executor after its base run, `variant: event-time-replay`) and `baseline@rule-1.0.1` (shard
  `hand-made`, the store `assertion:I-20`, `variant: rule-change`). `summaries.json` gains `generationRuns` keyed by that name with
  the fields of `runs`; `GateEvidence.validate` covers them (lock digest, here the replay lock; planned = completed = compared cut
  ids; exactly one run entry each) and the eight `J-*` aggregates include them: `J-AFTER-HORIZON-CUTS` (the loop over
  `Scenarios.HORIZON` in `Injections`) also reads the `afterHorizon` object of the generation run of a horizon scenario, here
  `horizon-planned-late@replay`, and fails when it is absent. `C-2-REFERENCE` keeps reading the online runs only.
  `gate/core-shards.json` gains the per-shard field `generationRuns`; `Shards.validate` refuses a generation run no shard makes or
  two make, and a shard that owns no assertion (`shard <name> owns no assertion`); `Shards.read` requires each planned generation
  run exactly once. `CoreGate.run` loses the `emptyShard` branch (`CoreGate.java` 222-223): after this no code path writes an
  `exit 0`, `0 / 0` report.

  Kept journals. For a scenario that has a generation run, the harness output of the base run and of the generation run are not
  deleted (today `Scenarios.java` 211 deletes the harness output after the fold, and `cuts.json` holds no journal row): after its
  fold each is moved to its own stable path, `<out>/journals/<scenario>/online/` and `<out>/journals/<scenario>/event-time-replay/`,
  and an index `<out>/journals/<scenario>/<variant>.index.json` (`{relative file: raw SHA-256}`, canonical bytes) is written. The
  run's `runs[]` entry carries `journals` (the directory, relative like `runRecord`) and `journalsIndexSha256`; both are null for
  every other run. The two directories share no file. `GateEvidence.validate` requires, for an entry with `journals`, the index
  with its digest and every listed file with its digest, and no unlisted file.

  Tests: `ShardsTest` +5 (25 -> 30): `aPlannedGenerationRunMissingFromItsShardIsAProblem`,
  `aReplayRunIsCheckedAgainstItsReplayLock`, `keptJournalsMissingOrChangedInTheArtifactAreAProblem`,
  `aReplaySummaryWithoutAfterHorizonValuesFailsJAfterHorizonCuts`, `theAggregateReadsTheAfterHorizonValuesOfThePlannedLateReplay`
  (a `generationRuns` summary of the shape the planned-late replay run writes: `need` from the visit facts, `lastW1 >= need`,
  `onlineVisits` falling, `workRecords` = `visits`; and each of the four conditions broken once); the existing
  `aShardWithoutAssertionsOfItsOwnExitsZero` becomes `aShardWithoutAssertionsIsRefusedByThePlan`. `CoreGateTest` +3 (16 -> 19):
  `theAggregateRegistersNothingAndRunsNothing`, `floorAndRuleChangeGenerationsAreIsolated`,
  `aGenerationRunIsListedOnceWithItsVariant`. `mvn -q -pl cli -Dtest='CoreGateTest,ShardsTest' test` -> `Tests run: 19`,
  `Tests run: 30`, 0 failures. On real evidence the aggregate of Step 21.6 evaluates `J-AFTER-HORIZON-CUTS` over the real
  `horizon-planned-late@replay` summary; its evidence line names that run.

- [ ] **Step 21.5** Assertions (+4, `pr: 5`):

| Id | Shard | What runs |
|---|---|---|
| `F-GEN-LIFECYCLE` | `hand-made` | in its own temporary state (`store: assertion:F-GEN-LIFECYCLE`, as the spec's Gate registration allows for a lifecycle assertion), through the CLI only: `new` -> one `registered` row; `run` on `smoke` -> still one row, one declaration; `promote` without a report -> exit 2, no row; `retire` -> `retired`; `retire` again and `run` on the retired generation refused; every pair of `GenerationPolicy` that is not an allowed transition refused. It writes no `replaying`, `verified` or `current` row: the gate never promotes on a report it wrote, and the positive path on real evidence is Step 21.7. No gate is launched inside the gate. The gate run's pipeline store receives nothing from it |
| `F-GEN-PROMOTE-REQUIRES-REPORT` | `hand-made` | in its own temporary state with one registered generation and its `smoke` run: no report or no run: exit 2. A complete fixture bundle (report with every committed manifest id, `kind: gate`, `layout: full`; `summaries.json`; the temporary store's row and run digests, `store: pipeline`, `state: shared`) passes `GateReportReader` with scope `core` accepted, checked without calling `promote`; then one change at a time is refused by `promote` with its code and leaves the store's bytes unchanged: a shard report; a control report; `exit 1`; another manifest digest; `0/0`; a reduced `k/k`; a count above N; N results with one id repeated; an aggregate report without one shard in `shards[]`; another plan digest; a result under the wrong owner; an aggregate report without `--shards`; an inserted run no summary covers; a summary with a compared cut removed; another `registeredRowSha256`; the right digest under `store: assertion:X` only; `state: isolated`; another `runRecordSha256`; a shuffled run; scope `core` without `--accept-scope` |
| `I-20` | `hand-made` | A: the pipeline's `baseline` run (`registry`, rule 1.0.0, the gate run's pipeline store). In the store `assertion:I-20`, through the verb: generation 1 on `registry` (floor 1), then generation 2 on `registry-next` (rule 1.0.1; that store's floor rises to 2, the pipeline store keeps floor 1); B = `baseline@rule-1.0.1` runs under generation 2. Every row of B carries the new `rulesDigest`, `pins.rules.attribution.version 1.0.1` and `evaluationGeneration 2`; every row of A the old digest; the two digests differ; the fold of each generation holds only its own digest; the oracle agrees with both at every cut. A and B are named by `(store, registeredRowSha256)`. Both generations of `assertion:I-20` are listed `state: isolated`, `assertion: I-20` (B with `variant: rule-change`) |
| `I-23` | `horizon-planned-late` | the scenario's base run and `horizon-planned-late@replay`, both in the gate run's pipeline store. Online: at the planned cut the reported `W1` equals `A` (else `error`, never a moved hole; spec 611); the three boundaries hold at one millisecond (`holeAt` and `holeAt - 1` admitted online, `holeAt - 2` `replay-required` `beyond-horizon`); visit 3's `missingPeriods` 1 before the late batch and 0 after. Replay (run and oracled on the materialized, locked replay recording): every deadline of `source.deadlines` keeps its original cut ids and the reported `W1` satisfies `W1_cutA(s) < at ≤ W1_cutB(s)` for each of its stations; the third sample is `attributed` with `cause.kind replayed`; no `replay-required` disposition exists; every timer-emitted row carries the replayed timer cause and precedes the progress of its cut (`J-PROGRESS-ORDER` findings empty for the replay fold). Earlier generation's rows remain (spec 665), proved on bytes: after the base run's fold its journals are moved to `journals/horizon-planned-late/online/` and every file is hashed (the index) before the replay run starts; after the replay run has finished and its journals are at `journals/horizon-planned-late/event-time-replay/`, the base files are read again from disk: every byte digest equals the index, no file was added or removed, and a second fold of the reread base journals equals the first (rows per topic, final revisions, fold digest); the replay journals are a different directory and every row in them carries the replay generation's number. Both generations `state: shared` under `--state` |

  `gate/core-shards.json`: `hand-made.assertions` += `F-GEN-LIFECYCLE`, `F-GEN-PROMOTE-REQUIRES-REPORT`, `I-20` (62 -> 65);
  `hand-made.generationRuns = ["baseline@rule-1.0.1"]`; `horizon-planned-late.assertions = ["I-23"]`,
  `horizon-planned-late.generationRuns = ["horizon-planned-late@replay"]`; every other shard `generationRuns = []`; `aggregate`
  unchanged (nine ids: eight `J-*` and `C-2-REFERENCE`). The aggregate run records the seven `pr: 5` ids from the shard reports
  like every other shard-owned id.
  Report-format additions of this PR: `variant` values `event-time-replay` and `rule-change`; `runs[].journals` and
  `runs[].journalsIndexSha256`; `runs[].originalRecordingManifestSha256` non-null for a replay run; `summaries.json.generationRuns`;
  `run.json.replay` and `originalRecordingManifestSha256` set for a replay run (the fields exist since 5a).

- [ ] **Step 21.6** `mvn -B -ntp -q install`; each of the six shards with `--state build/shards/<shard>/state` (`hand-made`:
  `assertions run 65 / expected 65`; each of the five horizon shards `1 / 1`), then `--aggregate build/shards` -> last line
  `assertions run 79 / expected 79`, exit 0 (65 + 5 + 9); a full unsharded run -> the same line; `--negative-control` list still
  empty. Record in the PR body the size of `build/shards/horizon-planned-late/journals/` and the shard's time. Commit `feat(cli):
  event-time replay generation on a locked replay recording with proved deadline brackets; injections 20 and 23; kept journals;
  no empty shard`.

- [ ] **Step 21.7** `scripts/gate-then-promote.sh <out> <python>` (full, unsharded; the store is `<out>/state`, there is no state
  argument) or `scripts/gate-then-promote.sh --from-shards <dir> <out> <python>`. It registers nothing by hand. Any outcome other
  than the stated one exits 1:
  1. Full: `forseti gate --engine core --state <out>/state --out <out>`. Shards: `forseti gate --engine core --aggregate <dir> --out
     <out>` (no `--state`). Exit 0 and the last line `assertions run N / expected N`, N computed by the script from the committed
     manifest.
  2. From `<out>/report.json`: BASE = the one `generations[]` entry with `store pipeline`, `scenario horizon-planned-late`, `variant
     online`, `state shared`, and its one non-shuffled `runs[]` entry (matched by `shard`, `store` and `registeredRowSha256`);
     REPLAY = the one entry with `store pipeline`, `variant event-time-replay`, `state shared`, the same `shard`, and its run. State
     directory: `<out>/state`, or `<dir>/<entry.shard>/state`; run records relative to `<out>`, or to `<dir>/<entry.shard>/`. Zero or
     two matches exit 1.
  3. `promote` BASE without `--accept-scope` -> exit 1, `gate-report-scope`; `generations.jsonl` byte-identical before and after.
  4. Shards mode only: `promote` BASE with `--gate-report <dir>/<entry.shard>/report.json --accept-scope core` -> exit 1,
     `gate-report-shard` (the report of the very shard that registered the generation is refused).
  5. `promote` BASE with `--accept-scope core` (shards mode: `--shards <dir>`) -> exit 0, `status current`, `gateReportSha256` = the
     raw SHA-256 of `<out>/report.json`.
  6. `promote` REPLAY the same way -> exit 0, `status current`, `retired` = BASE's number (owner decision 4).
  7. `generation list`: BASE's statuses are `registered, replaying, verified, current, retired`; REPLAY's are `registered,
     replaying, verified, current`; each promotion's three rows carry the `run` of its run record (`runRecordSha256` recomputed by
     the script); every other generation of the store has `registered` only; `head-floors/line2.json` still reads `headVersion 1`.
     Shards mode: no other shard's `state/generations.jsonl` holds a row other than `registered`.
  8. The base journals after both promotions: every file under `journals/horizon-planned-late/online/` still has the digest of
     its index, and the index the `journalsIndexSha256` of BASE's run entry.

- [ ] **Step 21.8** CI. `ci.yml`, job `core-gate`: the step "Core gate over every shard" becomes

```yaml
      - name: Core gate over every shard, then promote through the CLI
        working-directory: forseti
        run: bash scripts/gate-then-promote.sh --from-shards build/shards build/gate "$(command -v python)"
```

  and the step "Gate reports" uploads the post-promotion store with the reports:

```yaml
        with:
          name: core-gate
          path: |
            forseti/build/gate*/
            forseti/build/shards/horizon-planned-late/state/
          if-no-files-found: error
```

  The control loop stays as it is (empty list; PR 6 replaces it with per-shard control runs, which take no `--state`, so their
  reports are `kind: negative-control` with `isolated` generations only and are refused twice over). The `gate-shard-horizon-planned-late`
  artifact now holds: `report.json`, `summaries.json`, `logs/`, `diffs/` (on failure), `runs/horizon-planned-late.run.json`,
  `runs/horizon-planned-late.cuts.json`, `runs/horizon-planned-late.replay.run.json`, `runs/horizon-planned-late.replay.cuts.json`,
  `runs/horizon-planned-late.replay.recording/{manifest.json, routing.json, expected.json}` (the replay manifest, whose
  `source.replayOf`, `source.deadlines` and `digests` bind the materialized files, the bracket proof and the offset map),
  `journals/horizon-planned-late/online/` with `online.index.json` (the base generation's journal rows as bytes),
  `journals/horizon-planned-late/event-time-replay/` with `event-time-replay.index.json`, and `state/{generations.jsonl` (two
  `registered` rows: base and replay), `generations.lock`, `recovery-control.jsonl` (one declaration per generation),
  `head-floors/line2.json}`. The recordings themselves do not travel. Lock files are recreated where the store is restored; no path
  of the producing runner is identity.
  `nightly-gate.yml` runs `bash scripts/gate-then-promote.sh build/gate "$(command -v python)"` (store `build/gate/state`, inside
  the uploaded `forseti/build/gate/`) with
  `-Dforseti.gate.horizonXmx=6g` passed through `JAVA_TOOL_OPTIONS`, which exercises the reader's `full` branch, the same evidence
  validator included, every night. Expected shard time for `horizon-planned-late`: about twice its 11 minutes (a second harness and
  oracle run), inside the 60-minute timeout. Commit `ci: gate-then-promote from the shard artifacts in core-gate and unsharded
  nightly`.

- [ ] **Step 21.9** Locally: the six shards, then `bash scripts/gate-then-promote.sh --from-shards build/shards build/gate
  "$(command -v python)"` -> exit 0 (`assertions run 79 / expected 79`; BASE `retired`, REPLAY `current`); `bash
  scripts/gate-then-promote.sh build/gate-e2e "$(command -v python)"` -> exit 0. Pull request 5b: Task 21
  (3 commits); CI green. PR body: the lifecycle, the promotion policy (Open point 3), the replay order, batches and bracket proof
  and its lock, the gate line `assertions run 79 / expected 79`, the artifact contents, the shard times.

  **What 5b's CI proves:** `assertions run 79 / expected 79` from six shards and the aggregate; two generations registered and run
  in the `horizon-planned-late` shard are promoted in the aggregate job, in the store that travelled as an artifact, from the
  aggregate report and from nothing less (the shard's own report and an unaccepted core scope are refused live); the previous
  current ends `retired` and one current per scope holds on real gate evidence; the store's floor stays at 1; the replay run is
  compared with the oracle at every cut of a locked recording whose deadline brackets are proved on its own watermark progression;
  the base generation's journal bytes are unchanged after the replay run and after both promotions; the full-layout branch of the
  reader runs nightly. It does not prove the report's origin (unsigned), and from 2027-11-12 it fails until the fixture heads are
  re-published.

## Chunk 7: Negative controls, CI, documentation, the step-3 gate (PR 6)

Branch: `git switch -c test/step3-core-gate main` after PR 5b merged.

### Task 22: The eight negative controls and the gate self-tests

**Files:**
- Modify: `core/src/main/java/yggdrasil/forseti/core/NegativeControls.java` (created in Task 10 with `NONE`; unchanged shape, now read by the code paths below), `Stage1`, `Stage2`, `ReferenceEngine` (composes the record from `--negative-control`), `CoreGate`, `oracle/src/forseti_oracle/stage1.py` (`--lenient`), `gate/core-negative-controls.json` (8), `gate/core-assertions.json` (+2 = 81)
- Create: `cli/src/test/java/yggdrasil/forseti/cli/gate/NegativeControlsTest.java`

The controls are flags the harness passes to the core (the core's `NegativeControls` record, `NONE` in production; `Stage1` and `Stage2` are constructed with it; core never references cli) or to the oracle. They prove the gate's assertions can fail (current spec 674-686). A control run uses the same manifest, never takes `--state` (a usage error, exit 2), writes a report of `kind: negative-control` whose generations are all `isolated`, so it can never authorize a promotion, and must exit 1 with its intended assertion in `failed`; `report.json.intendedFailures` lists the ids and CI checks the subset.

| Control | Mutation | Intended failure |
|---|---|---|
| `processing-time` | the harness replaces every sample `sourceTs` and occupancy `eventTime` by the delivery sequence number before `decide` | `I-1` |
| `drop-late` | the harness drops a beyond-horizon input without its `replay-required` disposition | `J-DISPOSITION-COVERAGE` (on `horizon-planned-late`, current spec 679) |
| `no-cascade` | stage 2 ignores basis revisions after a row exists | `I-18`, `J-BASIS-CURRENT` |
| `lenient-oracle` | the oracle (`--lenient`) reports `provisional` visits as `complete` | `I-2`, `I-6` at the mandatory cuts |
| `first-wins` | S7 keeps the visit with the earlier arrival delivery and retracts only the later one | `I-4` |
| `station-granularity` | T5 uses the station's activity as the tool's | `I-16` |
| `ignore-session-gap` | S8 adds no `session-gap` | `I-8` |
| `wrong-rules-pin` | the harness writes `pins.rules.attribution.sha256` from another release while the body `rulesDigest` stays correct | the pins cross-check inside every `I-*` |

- [ ] **Step 22.1** Implement the eight; `NegativeControlsTest`: each control through `CoreGate` in-process exits 1 and `failed` contains the intended id(s); an unlisted name exits 2; `drop-late` fails even though the remaining bodies match. Gate self-tests as assertions `G-SELF-OMITTED` (a manifest copy with one id removed from the registry -> exit 1 with `notRun`) and `G-SELF-MISSING-REPORT` (the report directory made read-only -> exit 1, a failure report written to stderr); both run inside the normal gate on a temp copy.

- [ ] **Step 22.2** Locally, after `mvn -B -ntp -q install` (the jar gains the control code paths): `for c in processing-time drop-late no-cascade lenient-oracle first-wins station-granularity ignore-session-gap wrong-rules-pin; do java -jar cli/target/forseti.jar gate --engine core --negative-control $c --out build/gate-$c --python "$(command -v python)" >/dev/null; echo "$c -> $?"; done` -> each `-> 1`. Commit `feat(cli,oracle): the eight engine-independent negative controls and the gate self-tests`.

### Task 23: Item 1 aggregate, CI, README and documentation, pull request 6

**Files:**
- Modify: `CoreGate` (`C-1`), `gate/core-assertions.json` (+1 = 82), `.github/workflows/ci.yml` (already loops the controls and, since PR 5b, runs `scripts/gate-then-promote.sh --from-shards` in `core-gate`; PR 6 changes only the expected manifest size, which the script reads from the committed manifest, and the control runs), `README.md`; Create: `docs/CORE-GATE.md`, `docs/superpowers/plans/2026-11-09-forseti-step3-core-and-oracle.md` (this plan)

- [ ] **Step 23.1** `C-1`: aggregate over every scenario of the gate: every compared cut matched the oracle (item 1, current spec 579); `report.json` lists scenarios, cuts compared, rows compared, dispositions reconciled. `C-1` joins the `aggregate` list of `gate/core-shards.json` and covers `runs`, `shuffled` and `generationRuns`; the two gate self-tests are owned by `hand-made` (67 hand-made + 5 horizon + 10 aggregate = 82).

- [ ] **Step 23.2** `README.md`: what forseti is (the spec's purpose in two sentences); the three regimes of Decision 16 as text (a), (b), (c) (current spec 95); the module layout; how to run the core gate and the controls; the fixture provenance (`registry.lock.json`, `upstream/manifest.json`); the step-3 completion gate statement with `assertions run 82 / expected 82`; the pins; what the core scope does not establish (native event time, recovery, exactly-once, broker retention, transaction visibility, isolation, bridge classification, engine eligibility: steps 5-8). `docs/CORE-GATE.md`: the assertion ids with their spec rows (the table of this plan), the scenario list with sizes and which recordings are committed (smoke) or built at gate time (every other), the recording, journal and cut-log formats, the controls, the deferred obligations table. `bash scripts/check-ascii-names.sh` -> `product names: ASCII only`.

- [ ] **Step 23.3** `scripts/gate-then-promote.sh` exists and runs in CI since PR 5b (Step 21.7, Step 21.8): it promotes the base generation of `horizon-planned-late` and then its replay generation from the whole-gate report and requires the base generation to end `retired`. This task adds nothing to it; with the manifest at 82 its first step prints `assertions run 82 / expected 82`. Full run: `mvn -B -ntp install`; `python -m pytest -q oracle`; gate -> `assertions run 82 / expected 82`; eight controls -> exit 1 each with their intended failures; `bash scripts/check-dependency-closure.sh` -> clean. Commit `docs: README, core gate documentation and the step-3 plan; item 1 aggregate`.

- [ ] **Step 23.4** Pull request 6: Tasks 22-23 (3 commits); CI green (`build`: tests, oracle; six `gate` shards; `core-gate`: aggregate 82/82, promotion of the base and the replay generation, eight controls; `bifrost-seam`; `windows`). PR body: the step-3 completion gate statement (spec row 3), the gate line, the engine-independent injection list and the deferred obligations with their steps, the remaining Open points, the gate wall time, the fixture provenance. After the merge the controller runs the Verification commands from `main` and retains the logs (current spec 722).

## Follow-up (moved before PR 1)

By the owner decision of 2026-11-11 the two follow-up PRs (the Tyr re-author and the Bifrost copy) no longer come after PR 6: they are Prerequisites (b)-(d) in section Prerequisites before PR 1, widened to three stations, and PR 1 reads their merged heads. Nothing remains to follow PR 6 in the other repositories.

## Open points

Everything the owner decided on 2026-11-09 (the split and PR order, Kafka Streams 4.2.2, the interpretations written into the spec by Task 1, `forseti gate --engine core`, pytest, file-backed generation store and recovery control, horizon recordings built at gate time) and the fixture arrangement that replaced Q3 on 2026-11-11 are applied above and not repeated here. The four points that remained were decided by the owner on 2026-11-11; each is kept here with its outcome, and none is open.

1. **The rule change of `registry-next` (injection 20). Decided: accepted as recommended.** The plan bumps `line2-attribution` to `1.0.1` with `coverage.maxMissingPeriods` 3 -> 4 (and the impact rule to `1.0.1`). Reason: any observable change would do; this one changes no body of the baseline, so `I-20` tests the pins and the generation isolation, not a behaviour difference, which is what current spec line 662 asks ("every row of that generation carries the new `rulesDigest`, no generation mixes rules").
2. **Gate runtime and CI layout. Decided: accepted as recommended.** Five horizon recordings of about 443k messages each (five devices) are built, run and oracled per gate, and the twenty hand-made recordings are built per gate too (Amendment 2026-11-11, decision 6); 13-23 minutes are expected in CI (Step 19.3). When the measured time exceeds 15 minutes, the `build` job is split into `gate-hand-made` and `gate-horizon` with the same jar and the manifest filtered by `pr`/scenario, both required. Reason: Decision 8 (current spec line 61) forbids shortening the horizon; parallel jobs keep the requirement and the PR feedback time. Applied as `gate/core-shards.json` with six shards and an aggregate job, Amendment 2026-11-12, item 2. From PR 5b on every shard owns at least one assertion (`horizon-planned-late` owns `I-23`) and the plan refuses a shard without one; from PR 5a on every shard runs with `--state` and its artifact carries its control store.
3. **Promotion scope policy. Decided: accepted as recommended.** `promote` accepts a `core`-scope report only with the explicit flag `--accept-scope core`; the default requires scope `semantic` (steps 6-7); step 3's Verification and the `core-gate` job promote two generations this way (the base generation of `horizon-planned-late`, then its replay generation, which retires the first) as evidence of the lifecycle. Owner decisions of 2026-11-12 and 2026-11-13: `promote` reads only a whole-gate report (`kind: gate`, `layout: full` or `aggregate`); a shard report and a control report are refused whatever their counts; the result-id set must equal the committed manifest's ids; the generation is addressed by its store (the gate run's pipeline store) and `registeredRowSha256` and the run by `runRecordSha256`, never by a bare number and never by a digest found in another store; the evidence files of either layout are re-read and re-hashed, and every run must be covered by a successful summary, its locked recording and its exact cut ids; an `isolated` generation is never a target; promotion happens in the store that registered the generation, and its final approval and floor checks run under the same lock hold as its publication. Reason: Decision 11 (current spec line 73) requires the gate report digest without naming the gate; a core report must never impersonate the semantic gate of current spec line 690, and an explicit flag keeps the local test environment's policy out of the business rows.
4. **Two-station scenario shape against the three-station sim of step 4. Decided: the recommendation is rejected.** The recommendation was to keep the fixtures' two stations in step 3 and extend the scenarios in step 5. The owner decided that Line2 has three stations (`station1`, `station2`, `station3`; two nutrunners under Station1) in every repository, as current spec lines 111, 487 and 734 state for the sim and the partitions. Applied in this plan: station partitions 0, 1, 2 (Conventions); Tyr's and Bifrost's fixtures re-authored before PR 1 (Prerequisites before PR 1); five devices and sixteen baseline visits (Task 16); the recomputed counts of Tasks 6-8, 13, 15, 16, 17 and 19. The plc-stamped rule keeps identity on `station1` and `nutrunner1` and gains station3 in `stations[]` only. The Tyr plan's Open point 7, which recorded the two-station choice for the contract fixtures, is superseded by Prerequisite (b).

## Verification

Run from the forseti repository root on `main` after the PR 6 merge (or on `test/step3-core-gate` before it), with Temurin 17, Maven, Python 3.13 and the sibling checkouts `../bifrost` (jar built) and `../tyr` at the refs of `fixtures/registry.lock.json`; every command must end as stated. The controller retains the console logs (spec 703).

```bash
git status --short                                        # empty
git log --oneline 384933d..                               # Tasks 1-23 (about 35 commits over eight PRs)
git diff --check 384933d...HEAD                           # nothing
bash scripts/check-ascii-names.sh                         # product names: ASCII only
bash scripts/check-dependency-closure.sh                  # dependency closure: clean
file fixtures/registry/mappings/heads/line2.json schemas/*.json vectors/jcs-vectors.json | grep -c CRLF   # 0

# 0. pins
grep -n "jcs.version\|erdtman" pom.xml                    # io.github.erdtman / <jcs.version>1.1</jcs.version>
grep -n "rfc8785\|pytest" oracle/pyproject.toml           # rfc8785==0.1.4, pytest==8.3.5
java -version 2>&1 | head -1                              # 17.x (Temurin)
python --version                                          # Python 3.13.x

# 1. Java modules
mvn -B -ntp install                                       # BUILD SUCCESS; core and cli: Failures: 0, Errors: 0, Skipped: 0 (BifrostSeamTest skipped only when the two env vars are unset)
FORSETI_BIFROST_JAR=../bifrost/gates/target/bifrost-gates.jar FORSETI_TYR_FIXTURES=../tyr/fixtures/attribution-rule \
  mvn -q -pl core -Dtest=BifrostSeamTest test             # Tests run: 1, Failures: 0 (a head Bifrost publishes now verifies with forseti's reader, at the fixed clock and at the real time)

# 2. Oracle
python -m pip install -e "oracle[dev]"
python -m pytest -q oracle                                # all passed (about 119)

# 3. The shared vector file on both sides, by hand
python -I -c "import json;v=json.load(open('vectors/jcs-vectors.json'));print(len(v['vectors']),len(v['refusals']))"   # 9 8
mvn -q -pl core -Dtest=CanonicalVectorsTest test          # Tests run: 4, Failures: 0

# 4. Fixture provenance (Amendment 2026-11-11): Tyr's bytes published untouched, forseti's three variants derived
python -I - <<'PY'
import json,hashlib,pathlib
up=json.load(open('fixtures/upstream/manifest.json'));root=pathlib.Path('fixtures/upstream')
print('upstream files',len(up['files']),'mismatches',sum(hashlib.sha256((root/p).read_bytes()).hexdigest()!=h for p,h in up['files'].items()),'tyr',up['tyr'][:7])
lock=json.load(open('fixtures/registry.lock.json'));root=pathlib.Path('fixtures/registry');nxt=pathlib.Path('fixtures/registry-next')
print('registry files',len(lock['files']),'mismatches',sum(hashlib.sha256((root/p).read_bytes()).hexdigest()!=h for p,h in lock['files'].items()))
print('registry-next files',len(lock['next']),'mismatches',sum(hashlib.sha256((nxt/p).read_bytes()).hexdigest()!=h for p,h in lock['next'].items()))
occ=hashlib.sha256(open('schemas/occupancy-1.0.0.schema.json','rb').read()).hexdigest();inc=hashlib.sha256(open('schemas/incident-1.0.0.schema.json','rb').read()).hexdigest()
print('schemas',occ==lock['occupancySchemaSha256']=='e97cd7c97b1ab5e2d63d5b06e6057415945b82cff6d2ca0345106d2daf9d29dc',inc==lock['incidentSchemaSha256']=='7141c3115d90812f16b22049c66bfaa64942ebacf854b343fb99b3dce168fb34')
for b in ('bundle','plc-stamped-bundle'):
    o=json.load(open(f'fixtures/upstream/{b}/data/occupancy-binding-0.json'));i=json.load(open(f'fixtures/upstream/{b}/data/incident-binding-0.json'))
    print(b,'real digests',o['format']['schemaSha256']==occ and i['format']['schemaSha256']==inc,'topic',i['topic'],'stream',json.load(open(f'fixtures/upstream/{b}/submission.json'))['streamId'])
PY
#   -> upstream files 108 mismatches 0 tyr <hash>; registry files 192 mismatches 0; registry-next files 239 mismatches 0; schemas True True;
#      bundle real digests True topic quality.incidents stream line2; plc-stamped-bundle real digests True topic quality.incidents stream line2-plc
#      (108 = 44 + 64, and 192 and 239 follow from it by Step 6.2: expected values, to be confirmed when the Tyr PR merges)
python -I -c "import json,hashlib,pathlib;lock=json.load(open('fixtures/registry.lock.json'));r=pathlib.Path('fixtures/registry-mismatch');print('registry-mismatch files',len(lock['mismatchRegistry']),'mismatches',sum(hashlib.sha256((r/p).read_bytes()).hexdigest()!=h for p,h in lock['mismatchRegistry'].items()))"
#   -> registry-mismatch files 52 mismatches 0
diff -r fixtures/upstream/bundle ../tyr/fixtures/attribution-rule/bundle && diff -r fixtures/upstream/plc-stamped-bundle ../tyr/fixtures/attribution-rule/plc-stamped-bundle && echo IDENTICAL   # IDENTICAL (at the pinned Tyr ref)
diff -r fixtures/upstream/bundle ../bifrost/core/src/test/resources/mapping-fixtures/tyr-attribution-impact && diff -r fixtures/upstream/plc-stamped-bundle ../bifrost/core/src/test/resources/mapping-fixtures/tyr-attribution-plc-stamped && echo IDENTICAL   # IDENTICAL (at the pinned Bifrost ref)

# 5. The step-3 completion gate: without an engine, core equals the oracle on every engine-independent injection
java -jar cli/target/forseti.jar gate --engine core --out build/gate --python "$(command -v python)"
#   ... last line: assertions run 82 / expected 82        exit 0
python -I -c "import json;r=json.load(open('build/gate/report.json'));print(r['exit'],r['scope'],r['assertionsRun'],r['assertionsExpected'],len(r['failed']),len(r['notRun']),r['pins']['jcs'])"
#   -> 0 core 82 82 0 0 {'java': 'io.github.erdtman:java-json-canonicalization:1.1', 'python': 'rfc8785==0.1.4'}
ls build/gate/diffs 2>/dev/null | wc -l                   # 0
git ls-files fixtures/recordings | cut -d/ -f3 | sort -u | tr '\n' ' '   # smoke smoke-incident smoke-plc (no other recording is committed)
ls build/recordings | wc -l                               # 25 (twenty hand-made and five horizon recordings, built by the gate)
java -jar cli/target/forseti.jar recordings verify --scenario fixtures/scenarios/baseline.json --out build/recordings/baseline   # lock matches, exit 0
java -jar cli/target/forseti.jar recordings verify --scenario fixtures/scenarios/smoke.json --out fixtures/recordings/smoke      # lock matches, exit 0

# 6. Every engine-independent negative control exits 1 for its intended assertion (theta non-transactional-output is steps 6-7)
for c in processing-time drop-late no-cascade lenient-oracle first-wins station-granularity ignore-session-gap wrong-rules-pin; do
  java -jar cli/target/forseti.jar gate --engine core --negative-control "$c" --out "build/gate-$c" --python "$(command -v python)" >/dev/null; rc=$?
  python -I -c "import json;r=json.load(open('build/gate-$c/report.json'));print('$c ->',$rc,sorted(r['intendedFailures']),set(r['intendedFailures'])<=set(r['failed']))"
done
#   -> each line: <control> -> 1 [<ids>] True   (processing-time I-1; drop-late J-DISPOSITION-COVERAGE; no-cascade I-18,J-BASIS-CURRENT; lenient-oracle I-2,I-6;
#      first-wins I-4; station-granularity I-16; ignore-session-gap I-8; wrong-rules-pin every I-*)
java -jar cli/target/forseti.jar gate --engine core --negative-control no-such-control --out build/gate-x; echo "-> $?"   # -> 2 (usage), so the CI loop fails it

# 7. The generation lifecycle and injection 23 by hand, outside the gate
java -jar cli/target/forseti.jar recordings build --scenario fixtures/scenarios/horizon-planned-late.json --registry fixtures/registry --out build/recordings/horizon-planned-late
java -jar cli/target/forseti.jar recordings verify --scenario fixtures/scenarios/horizon-planned-late.json --out build/recordings/horizon-planned-late   # lock matches, exit 0
rm -rf build/state && java -jar cli/target/forseti.jar generation new --state build/state --mappings fixtures/registry/mappings --trusted-keys fixtures/trusted-keys.jsonl \
  --stream line2 --rule line2-attribution@1.0.0 --recording build/recordings/horizon-planned-late
#   -> {"generation":1,"status":"registered","rulesDigest":"<64 hex>",...}; build/state/head-floors/line2.json exists; build/state/recovery-control.jsonl does not exist yet (the first run preparation writes the declaration)
java -jar cli/target/forseti.jar run --engine core --recording build/recordings/horizon-planned-late --registry fixtures/registry \
  --state build/state --generation 1 --out build/runs/planned-late-g1                      # runs from the pinned releaseEvidence; writes the generation's initial recovery declaration; appends no generation row
python -I -m forseti_oracle.cli all --recording build/recordings/horizon-planned-late --cuts build/runs/planned-late-g1/cuts.json --mappings fixtures/registry/mappings \
  --release line2-rules@1.0.0 --rule line2-attribution@1.0.0 --definitions build/recordings/horizon-planned-late/manifest.json --out build/oracle/planned-late-g1
python -I - <<'PY'
import json
cuts=json.load(open('build/runs/planned-late-g1/cuts.json'));last=cuts['cuts'][-1]['cutId']
rows=json.load(open(f'build/oracle/planned-late-g1/cut-{last}/forseti.samples.json'))
from collections import Counter
print(Counter(f"{v['body']['state']}/{v['body']['reason']}" for v in rows.values() if v['body']['state']!='unattributed' or v['body']['reason']!='no-occupancy'))
PY
#   -> Counter({'attributed/None': <n>, 'replay-required/beyond-horizon': 1})
java -jar cli/target/forseti.jar generation new --state build/state --mappings fixtures/registry/mappings --trusted-keys fixtures/trusted-keys.jsonl \
  --stream line2 --rule line2-attribution@1.0.0 --recording build/recordings/horizon-planned-late      # generation 2
java -jar cli/target/forseti.jar run --engine core --replay event-time --recording build/recordings/horizon-planned-late --registry fixtures/registry \
  --state build/state --generation 2 --out build/runs/planned-late-g2
ls build/runs/planned-late-g2/replay-recording/manifest.json                        # the materialized replay recording (source.replayOf = the original manifest digest; offsets in replay order, batches by telemetry-derived cut targets, source.deadlines = the proved brackets; current spec 570)
grep -c '"replayed"' build/runs/planned-late-g2/journals/forseti.samples/0.jsonl    # > 0
grep -c 'beyond-horizon' build/runs/planned-late-g2/journals/forseti.samples/0.jsonl   # 0

# the lifecycle through the CLI only, as CI runs it: scripts/gate-then-promote.sh (Task 21, Step 21.7; run by the core-gate job with
# --from-shards and nightly in this full form) exits 1 if any step refuses; the hand run below shows its steps one by one. The gate
# registers its own generations through `generation new` in its own store, build/gate-state/state (a shared gate run's store is
# always <out>/state; the two generations made by hand above stay in build/state and are not touched); that store's line2 floor
# stays at 1, because F-GEN-FLOOR and I-20's generations live in isolated assertion stores.
ST=build/gate-state/state
java -jar cli/target/forseti.jar gate --engine core --state "$ST" --out build/gate-state --python "$(command -v python)"   # assertions run 82 / expected 82; exit 0
sel() { python -I -c "import json,sys;r=json.load(open('build/gate-state/report.json'));g=[x for x in r['generations'] if x['store']=='pipeline' and x['scenario']=='horizon-planned-late' and x['variant']==sys.argv[1] and x['state']=='shared'];assert len(g)==1;u=[x for x in r['runs'] if x['store']=='pipeline' and x['registeredRowSha256']==g[0]['registeredRowSha256'] and not x['shuffle']];assert len(u)==1;print(g[0]['generation'],g[0]['registeredRowSha256'],'build/gate-state/'+u[0]['runRecord'])" "$1"; }
read G D R <<< "$(sel online)"; read G2 D2 R2 <<< "$(sel event-time-replay)"
P="java -jar cli/target/forseti.jar generation promote --state $ST --mappings fixtures/registry/mappings --trusted-keys fixtures/trusted-keys.jsonl --gate-report build/gate-state/report.json"
$P --generation "$G" --registered-row-sha256 "$D" --run "$R"; echo "-> $?"   # -> 1 {"refusal":"gate-report-scope",...}  (a core report needs --accept-scope core)
$P --generation "$G" --registered-row-sha256 "$D" --run "$R" --accept-scope core      # {"generation":<G>,"status":"current","gateReportSha256":"<sha256 of report.json>","retired":null,...}
$P --generation "$G2" --registered-row-sha256 "$D2" --run "$R2" --accept-scope core   # {"generation":<G2>,"status":"current",...,"retired":<G>}
java -jar cli/target/forseti.jar generation list --state "$ST" | python -I -c "import json,sys;rows=[json.loads(l)['value'] for l in sys.stdin];print([[r['status'] for r in rows if r['generation']==g] for g in ($G,$G2)])"
#   -> [['registered','replaying','verified','current','retired'], ['registered','replaying','verified','current']]
#      (every other generation of that store stays registered; the gate wrote no row itself: every `registered` row of its runs was written by `generation new`)
python -I -c "import json,sys;print(json.load(open(sys.argv[1]+'/head-floors/line2.json'))['headVersion'])" "$ST"   # 1
bash scripts/gate-then-promote.sh build/gate-e2e "$(command -v python)"; echo "-> $?"   # -> 0 (the same steps on a fresh output directory, store build/gate-e2e/state)

# 8. On GitHub: the eight PRs merged (docs, PR 1-4, 5a, 5b, 6); main CI green (build; six gate shards; core-gate with the aggregate 82/82, the promotion of the base and the replay generation and eight controls; bifrost-seam at the pinned refs; windows); the gate artifact `core-gate` attached to the last run
```

(On Windows under Git Bash, pass the jar, registry, recording and key paths through `cygpath -m` if `java` cannot open them, as Bifrost's gate scripts do; the controller machine's JDK 25 runs the jar too, but the recorded run should use Temurin 17 to match CI.)

### Assertion coverage

| Spec row | Assertion id(s) | PR |
|---|---|---|
| Decision 7, line 56 (as extended); line 596 (vector file in both suites) | `F-JCS-JAVA`, `F-JCS-PY`, `F-JCS-REFUSALS` | 1 |
| Decision 4, current spec lines 42-44, 194, 206 (`binding.schema-mismatch` on a published profile with another digest, Amendment 2026-11-11) | `F-SCHEMA-COPIES`, `F-UPSTREAM-UNTOUCHED`, `F-BINDING-SCHEMA-MISMATCH` | 1 |
| Decision 14, line 84 (as extended); line 725 (a Bifrost-published head) | `F-HEAD-*`, `F-RELEASE-DIGEST`, `F-PAYLOAD-DIGEST`, `BifrostSeamTest` | 1 |
| Decision 6 (line 52), lines 331, 412-452 (revisions, deletes, progress, ledgers) | `F-JOURNAL-REDUCERS`, `S-REVISIONS`, `S-PROGRESS`, `T-PROGRESS-TRACKER`, `T-DELETE`, `T-LEDGER-2`, `J-REVISION-CONTINUITY`, `J-NO-EQUAL-CONSECUTIVE`, `J-PROGRESS-ORDER` | 2, 3, 4 |
| Decision 12 (line 76): pure decisions, `ProgressTracker` | `S-DETERMINISM`, `T-PROGRESS-TRACKER`, `J-W2-EQUALS-W1` | 2, 3, 4 |
| Decision 11, current spec lines 73, 552-570 (as edited by Step 20.0) | `F-GEN-*`, `I-20`, `I-23`; `scripts/gate-then-promote.sh` | 5a, 5b |
| Injections 1-5, 7-11d, 25, 26 (lines 613-650) | `I-1` ... `I-26` as listed | 4 |
| Injections 6, 10e, 11e; horizon scenarios (lines 592-594) | `I-6`, `I-10e`, `I-11e`, `J-AFTER-HORIZON-CUTS` | 4 |
| Injection 23 (current spec lines 611, 665) and the event-time replay (current spec 570) | `I-23`, `C-2-REFERENCE` | 4, 5b |
| Injections 12-19 (lines 635-642), H-cascade, H-tool | `T-SMOKE-INCIDENT`, `T-CASCADE`, `I-12` ... `I-19`, `J-BASIS-CURRENT` | 3, 4 |
| H-gap (line 21), dispositions (lines 437-452) | `J-DISPOSITION-COVERAGE`, `I-17`, `I-25` | 4 |
| H-pin (line 22), item 1 (line 562) | `C-1`, the pins cross-check in `Integrity`, `wrong-rules-pin` control | 4, 6 |
| H-order (line 19), item 6 (line 567) | `I-1`, `I-11b`, `C-6-STAGE1`, `C-6-STAGE2`, `S-DETERMINISM` | 2, 4 |
| H-need (line 26) | `I-11b` | 4 |
| Item 2 (line 563) reference values | `C-2-REFERENCE` | 4 |
| Step-3 `W1` formula (Task 1's line 588) | `S-W1`, `J-W1-FORMULA` | 2, 4 |
| Negative controls alpha-iota except theta (lines 657-667) | the eight controls exit 1 with their intended failures | 6 |
| Proving the gates run (current spec line 690) | `assertions run N / expected N`, `G-SELF-OMITTED`, `G-SELF-MISSING-REPORT`, the CI control loop | 1, 6 |
