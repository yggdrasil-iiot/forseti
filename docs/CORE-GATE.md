# The core gate

`forseti gate --engine core` is the step-3 gate. It runs the reference harness on recorded scenarios, compares the harness journals with an independent Python oracle at every completed cut of every scenario, checks journal integrity and the pins, and evaluates the engine-independent injections, comparison items 1, 2 and 6, and the gate's own self-tests. The committed manifest `gate/core-assertions.json` holds 82 assertions.

## Accounting

The gate loads the manifest, runs every registered assertion, and prints `assertions run N / expected N`. It exits 0 only when every expected assertion ran and held; it exits 1 when any assertion failed or did not run, when a report cannot be written (the failure report then goes to stderr), or on an input mismatch; usage errors exit 2. `G-SELF-OMITTED` proves that an assertion missing from the registry is reported as not run; `G-SELF-MISSING-REPORT` proves that an unwritable report directory fails the gate.

## Shards and the aggregate

`gate/core-shards.json` splits the gate into six shards: `hand-made` (the hand-made and smoke scenarios, the shuffled runs, and the rule-change generation run) and one shard per horizon recording, together with any hand-made scenario its assertion compares against.

`--shard <name> --out <dir> --state <dir>/state` runs one shard and writes `report.json` and `summaries.json`; `--aggregate <dir>` reads every shard's output and evaluates the ten cross-scenario assertions (the eight `J-*` aggregates, `C-1` and `C-2-REFERENCE`). The aggregate passes only when every shard ran against the same manifest, shard plan, fixtures, seed and pins, recorded exactly its own assertions, and compared every scenario at every cut its lock plans; it prints `assertions run 82 / expected 82`.

CI runs each shard as its own job with a 60-minute timeout and the aggregate in the `core-gate` job; a nightly workflow runs the gate unsharded. Every gate run registers its generations through `forseti generation new`; with `--state` they are kept in `<out>/state`.

| Shard | Assertions | Scenarios | Generation runs |
|---|---|---|---|
| `hand-made` | 67 | `baseline`, `held-back-10min`, `redelivery`, `overlap`, `bad-quality`, `clock-plus-30`, `clock-plus-90`, `corrections`, `corrections-10b-before`, `corrections-10b-after`, `corrections-10c`, `session`, `quiet-occupancy`, `quiet-occupancy-control`, `plc-stamped`, `plc-nocheck`, `plc-held-back`, `stage2`, `stage2-early`, `cascade`, `smoke`, `smoke-plc`, `smoke-incident` | `baseline@rule-1.0.1` |
| `horizon-no-departure` | 1 | `horizon-no-departure`, `baseline` | - |
| `horizon-reopened` | 1 | `horizon-reopened`, `corrections-10c` | - |
| `horizon-correction-beyond` | 1 | `horizon-correction-beyond` | - |
| `horizon-planned-late` | 1 | `horizon-planned-late` | `horizon-planned-late@replay` |
| `plc-horizon-no-end` | 1 | `plc-horizon-no-end` | - |
| aggregate | 10 | every shard's summaries | - |

## Assertions

The table lists every id of the manifest with the PR that added it, the shard that owns it and the manifest's description.

Prefixes:
- `F-` foundation (canonical form, schemas, release reader, generation CLI)
- `S-` stage-1 core
- `T-` stage-2 core
- `J-` journal-wide aggregates
- `I-` engine-independent injections (spec injection table)
- `C-` comparison items
- `G-SELF-` the gate's self-tests

| Id | PR | Shard | What it asserts |
|---|---|---|---|
| `F-JCS-JAVA` | 1 | hand-made | the shared vector file through Canonical: every vector's canonical text and SHA-256 as pinned |
| `F-JCS-PY` | 1 | hand-made | python -I -m pytest -q oracle/tests/test_jcs_vectors.py exits 0 (the gate's --python) |
| `F-JCS-REFUSALS` | 1 | hand-made | the eight refusal vectors are refused on both sides: Java in-process, and python -I -m forseti_oracle.cli canon-check vectors/jcs-vectors.json exits 0 |
| `F-SCHEMA-COPIES` | 1 | hand-made | schemas/*.json bytes equal the core resources; their digests equal fixtures/registry.lock.json, the literals e97cd7c97b1ab5e2d63d5b06e6057415945b82cff6d2ca0345106d2daf9d29dc (occupancy) and 7141c3115d90812f16b22049c66bfaa64942ebacf854b343fb99b3dce168fb34 (incident), and format.schemaSha256 of both profiles in each bundle of fixtures/upstream |
| `F-UPSTREAM-UNTOUCHED` | 1 | hand-made | every file of fixtures/upstream/{bundle,plc-stamped-bundle} matches fixtures/upstream/manifest.json (108 files), and every payload file of line2-rules 1.0.0 and line2-rules-plc 1.0.0 in fixtures/registry has the digest the manifest records for the same bundle path |
| `F-JOURNAL-REDUCERS` | 1 | hand-made | Revisions, Disposition and JournalFold on inline journals: first revision 1, unchanged body emits nothing, delete continuity, reappearance, a conflict revision under the same delivery key, the four fold violations detected |
| `F-HEAD-OK` | 1 | hand-made | the three heads of fixtures/registry and registry-next's version 2 verify at signedAt + 60 s with fixtures/trusted-keys.jsonl |
| `F-HEAD-SIGNATURE` | 1 | hand-made | untrusted keys, a forged keyid and a payload changed after signing are head-unverified (signature), in a temp copy |
| `F-HEAD-SAME-KEY` | 1 | hand-made | two signatures from one key are head-unverified (same-key), in a temp copy |
| `F-HEAD-SUBJECT` | 1 | hand-made | a changed head file and a statement naming another release.json are head-unverified (subject), in a temp copy |
| `F-HEAD-FUTURE` | 1 | hand-made | a statement signed after now is head-unverified (signed-in-future) and raises no floor, in a temp copy |
| `F-HEAD-ROLLBACK` | 1 | hand-made | a head version below the floor and the same version with another statement are head-rollback |
| `F-HEAD-EXPIRED` | 1 | hand-made | a head read at its expires instant is head-expired and raises no floor |
| `F-RELEASE-DIGEST` | 1 | hand-made | a changed release.json byte is provenance-mismatch (release-digest), in a temp copy |
| `F-PAYLOAD-DIGEST` | 1 | hand-made | a changed payload byte is provenance-mismatch (release-file), in a temp copy |
| `F-RULES-LOAD-OJ` | 1 | hand-made | RuleSet.load on line2-rules 1.0.0 (occupancy-join): counts and roles as in RuleSetTest; rulesDigest equals the oracle's rules-digest |
| `F-RULES-LOAD-PLC` | 1 | hand-made | RuleSet.load on line2-rules-plc 1.0.0 (plc-stamped): counts and roles as in RuleSetTest; rulesDigest equals the oracle's rules-digest |
| `F-BINDING-SCHEMA-MISMATCH` | 1 | hand-made | on fixtures/registry-mismatch the head verifies and the release reads, then RuleSet.load is refused with binding.schema-mismatch (approved 0000000000000000000000000000000000000000000000000000000000000001) |
| `S-SMOKE-VISIT` | 2 | hand-made | the harness on fixtures/recordings/smoke: one work record (occupancy-join, occ-1) complete at the last cut, 5684 sample row keys (4060 messages x their members), samplesDigest e518a18ba8c533fb5e3fb00529aaf1ebff54d67a07c810a5012f7fe1a926096f, provisional at the cut ending at scan T0 + 1410000 and complete at the next cut, ending at scan T0 + 1411000 |
| `S-SMOKE-PLC` | 2 | hand-made | the harness on fixtures/recordings/smoke-plc: one complete visit keyed by visitRef mode plc-stamped (urn:line2:visits, station1, V1), readPoint the station path |
| `S-REVISIONS` | 2 | hand-made | on both recordings, every journal partition folded by JournalFold through the last cut: revisions contiguous from 1 per key, no equal consecutive digests, no delete mismatch, no row after its cut's progress |
| `S-PROGRESS` | 2 | hand-made | on both recordings, work-records and samples carry one progress per cut after the cut's rows (stamped with its W1, triggered inside its offsets or by a due timer); station and partition-6 ledger progress per cut; admissionW equals the previous W1; initEvidence only on the first progress |
| `S-W1` | 2 | hand-made | on both recordings, W1 in cuts.json and in both progress topics equals max routed telemetry sourceTs - outOfOrdernessMs - 1, never decreasing, at every cut and station |
| `S-DETERMINISM` | 2 | hand-made | on both recordings, the default run and two --shuffle runs with seeds 7 and 20261110: journal and quarantine lines equal once value.envelope.pins.job.runId is removed, cuts.json equal byte for byte, run.json equal but for runId, pins.job.runId, seed, shuffle and command |
| `T-PROGRESS-TRACKER` | 3 | hand-made | on fixtures/recordings/smoke-incident: forseti.affected and forseti.affected-sets carry one stage-2 progress per cut and station after both stage-1 progresses of the cut (stage1ProgressRefs) with the six incident barriers of the batch manifest (incidentOffsets count 6), W2 == W1 in cuts.json and = min of the two stage-1 W1, rows stamped with W2; the stage-2 ledger progress per cut on the station partitions and partition 6, barriers the six incident partitions |
| `T-SMOKE-INCIDENT` | 3 | hand-made | on fixtures/recordings/smoke-incident at the last cut: one live assessment, UNIT0000000000001 affected-candidate equipment-activity, one live affected set with units of one, coverage.settled true and coverage.status complete |
| `T-CASCADE` | 3 | hand-made | a smoke-incident-cascade variant written by the gate (an error declaration did_not_occur of occ-1 in the last batch): the assessment becomes not-affected-within-scope basis-retracted-error-declaration in the cut the work record is retracted, citing the retracted digest; J-BASIS-CURRENT holds at every cut on smoke-incident and the variant |
| `T-DELETE` | 3 | hand-made | a smoke-incident-narrow variant written by the gate (incident suspectFrom T+30 s, the visit open, a late departure at T+20 s delivered at cut 30): the unit row ends with a delete at cut 30 superseding the last digest, revisions contiguous, JournalFold violations empty, the set has no units |
| `T-LEDGER-2` | 3 | hand-made | on smoke-incident and a variant with an incident on station2: one forseti.dispositions-stage2 disposition per incident delivery (inc-1 accepted on station1 partition 0, the station2 incident rejected outside-incident-scope on partition 6), stage-2 ledger progress per cut on partitions 0-2 and 6 |
| `I-1` | 4 | hand-made | held-back-10min vs baseline: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; every work record and sample row digest equal at the settled cut (comparable-cut predicate) |
| `I-2` | 4 | hand-made | corrections: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; visit 2 provisional at cutA and complete at cutB of leave + L |
| `I-2b` | 4 | hand-made | corrections: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; visit 5's sample rows unattributed no-occupancy first, then attributed; members[] include them |
| `I-3` | 4 | hand-made | bad-quality: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; 7 rows unattributed bad-quality; visit 1 members[nutrunner1-torque] observedPeriods 60, badSlots 7, count 53; insufficient bad-share-exceeded |
| `I-4` | 4 | hand-made | overlap: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; visit 3 and the extra visit retracted overlap; samples in one actual interval attributed to it, in [1000000, 1020000) unattributed ambiguous; station2's two visits complete with unknownActivityPeriods 3; 48 station2 Running rows in [1557000, 1605000) unattributed ambiguous |
| `I-5` | 4 | hand-made | redelivery: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; the second deliveries duplicate identical-redelivery; live row counts equal baseline's |
| `I-6` | 4 | horizon-no-departure | horizon-no-departure: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; visit 8 provisional at cutA (missingPeriods 0, reasons []) and insufficient missing-departure at cutB of enter + H = 88260000; visits 1-7 equal baseline's; visit 9 provisional then complete at 89280000, never retracted; visit 8 without overlap |
| `I-7` | 4 | hand-made | session: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; 120 nutrunner1 sample rows unattributed no-occupancy active true; no complete visit for that window |
| `I-8` | 4 | hand-made | session: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; visit 3 insufficient session-gap, equipmentEvidence[nutrunner1].coverage.sessionGaps 1 |
| `I-9` | 4 | hand-made | clock-plus-30: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; every sample inside a visit's [enter, leave) attributed to the same visit as in baseline; the sample at original 550000 attributed to visit 1 |
| `I-9b` | 4 | hand-made | clock-plus-90: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; per-sample attribution equals expected.json (three bands) |
| `I-10` | 4 | hand-made | corrections: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; visit 4 retracted error-declaration, errorReason did_not_occur |
| `I-10b` | 4 | hand-made | corrections-10b-before vs -after: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; the same settled body digest of visit 8 (retracted error-declaration did_not_occur) in both orders; in -before the declaration accepted (part errorDeclaration) before the original accepted (part event) |
| `I-10c` | 4 | horizon-reopened | corrections-10c and horizon-reopened: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; visit 8 complete at the cut of W1 2640999, the declaration accepted late-after-settlement, the visit provisional with reasons []; in horizon-reopened insufficient missing-departure at enter + H |
| `I-10d` | 4 | hand-made | corrections: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; both deliveries of occ-1 conflict identity-conflict; visit 1 retracted input-conflict |
| `I-10e` | 4 | horizon-correction-beyond | horizon-correction-beyond: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; the cut before the declaration's batch reports W1 87960999 >= onlineExpiresAt; the declaration replay-required correction-beyond-horizon; visit 2 unchanged complete |
| `I-10f` | 4 | hand-made | corrections: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; the declaration and the corrective departure accepted late-after-settlement; visit 6 complete, provisional, then complete with leave 1565000 once W1 >= 1565000 + L |
| `I-11` | 4 | hand-made | plc-stamped: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; visit 3's samples attributed with reason identity-mismatch, attributedTo unchanged |
| `I-11b` | 4 | hand-made | plc-held-back vs plc-nocheck (stream line2-plc-nocheck): built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; bodies equal at the settled cut; at least one complete visit |
| `I-11c` | 4 | hand-made | plc-stamped: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; the mixed snapshot's samples incoherent-snapshot |
| `I-11d` | 4 | hand-made | plc-stamped: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; visit 7 insufficient missing-visit-close |
| `I-11e` | 4 | plc-horizon-no-end | plc-horizon-no-end: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; V3 insufficient missing-departure once W1 >= enter + H |
| `I-12` | 4 | hand-made | stage2: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; inc-1 units 1, 3, 5 affected-candidate equipment-activity; set settled true at the last cut |
| `I-13` | 4 | hand-made | stage2-early: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; the first set row has units [], settled false, coverage unknown with unknownActivityPeriods 990; it falls cut by cut; units grow one per visit; settled once W2 >= 2310000; complete at the last cut |
| `I-14` | 4 | hand-made | stage2: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; inc-2 while open: T1 visits affected-candidate, T2-only and no-tool no-equipment-activity; after revision 2 visits 6-8 interval-narrowed, 3 and 5 affected-candidate, open false |
| `I-15` | 4 | hand-made | stage2: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; inc-3 every unit not-affected-within-scope incident-withdrawn; resolution withdrawn, open false, settled true, never complete |
| `I-16` | 4 | hand-made | stage2: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; inc-1 T2-only visit 2 no-equipment-activity, both-tool visit 3 affected-candidate, no-tool visit 4 no-equipment-activity |
| `I-17` | 4 | hand-made | stage2: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; inc-1 coverage.unattributedActivityPeriods 28, status incomplete; 56 nutrunner1 rows unattributed no-occupancy active true |
| `I-18` | 4 | hand-made | cascade: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; inc-1 unit 1 affected-candidate then not-affected-within-scope basis-retracted-error-declaration in the cut of the retraction |
| `I-19` | 4 | hand-made | cascade: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; inc-2 without unit 5 first; unit 5 insufficient-evidence basis-provisional while visit 5 is provisional, then affected-candidate equipment-activity at the bracketing cut of 2100000; each step a set revision |
| `I-25` | 4 | hand-made | session: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; stale-death leaves station2's visit 2 equal to baseline's; session-ambiguous: visit 6 insufficient session-ambiguous, nutrunner2 coverage unknown |
| `I-26` | 4 | hand-made | quiet-occupancy vs quiet-occupancy-control: built from its committed scenario into build/recordings/ and verified against its lock, run under a generation registered through generation new, equal to the oracle at every cut (recomputed digests, dispositions per identity), integrity and pins cross-check clean, expected.json at the last cut and transitions[] at the bracketing cuts; Station1 work records and sample rows equal at every comparable cut, the settled cut among them |
| `J-DISPOSITION-COVERAGE` | 4 | aggregate | every scenario run and shuffled run: every delivered input has a disposition, every message member its part; inputsWithoutDisposition 0 |
| `J-REVISION-CONTINUITY` | 4 | aggregate | every scenario run and shuffled run: revisions contiguous from 1 per key on every journal and ledger, deletes superseding the last digest |
| `J-NO-EQUAL-CONSECUTIVE` | 4 | aggregate | every scenario run and shuffled run: no two consecutive equal digests of one key; every reported bodyDigest recomputed |
| `J-PROGRESS-ORDER` | 4 | aggregate | every scenario run and shuffled run: progress cutId increasing and event time non-decreasing per partition; no business row or disposition of a cut after its progress; a progress record of every cut on every partition |
| `J-W1-FORMULA` | 4 | aggregate | every scenario run and shuffled run: W1 = max routed sourceTs - outOfOrdernessMs - 1 per station and cut in cuts.json and in the stage-1 progress; admissionW = the previous W1 |
| `J-W2-EQUALS-W1` | 4 | aggregate | every scenario run and shuffled run: stage-2 progress W2 equals the cut's W1, six incidentOffsets |
| `J-BASIS-CURRENT` | 4 | aggregate | every scenario run and shuffled run: every basis[].bodyDigest of every live assessment is the current work record digest at every cut |
| `J-AFTER-HORIZON-CUTS` | 4 | aggregate | the five horizon scenarios: cuts exist with W1 >= enter + H + L of the open visits (or the first onlineExpiresAt); onlineVisits drops after expiry while the work records remain in the fold |
| `C-6-STAGE1` | 4 | hand-made | baseline and stage2, the default run and three --shuffle runs (seeds 20261109, 7, 20261110): identical stage-1 bodies at every comparable cut; revision counts in report.json |
| `C-6-STAGE2` | 4 | hand-made | stage2, the default run and three --shuffle runs: identical stage-2 bodies at every comparable cut; revision counts in report.json |
| `C-2-REFERENCE` | 4 | aggregate | report.json measurements record longestLatenessCorrectedOnlineMs (568999 from held-back-10min, H from horizon-planned-late's online run), journal records per topic per scenario and inputsWithoutDisposition 0 |
| `F-GEN-NEW` | 5 | hand-made | generation new in its own temporary store with the fixture clock: one registered row with its pins, run null, head-floors/line2.json, no recovery declaration, the printed registeredRowSha256 = digest of the stored line; then forseti run on smoke writes exactly one declaration with the recording's routingDigest and no generation row, a second run adds neither (store assertion:F-GEN-NEW, isolated, self-test) |
| `F-GEN-REFUSES-UNREGISTERED` | 5 | hand-made | the harness refuses generation 7 (generation-unregistered) and writes no declaration |
| `F-GEN-FLOOR` | 5 | hand-made | in its own temporary store (never the gate's --state): generation new on registry (floor 1), on registry-next (floor 2), on registry again: head-rollback; the two registered generations listed store assertion:F-GEN-FLOOR, isolated, self-test |
| `F-GEN-LIFECYCLE` | 5 | hand-made | in its own temporary store through the CLI only (store assertion:F-GEN-LIFECYCLE, isolated, self-test): new -> one registered row; forseti run on smoke -> still one row, one declaration; promote without a report -> exit 2, no row; retire -> retired; retire again refused generation-transition and forseti run on the retired generation refused generation-retired, the store unchanged; every status pair GenerationPolicy does not allow refused. No replaying, verified or current row; no gate is launched inside the gate |
| `F-GEN-PROMOTE-REQUIRES-REPORT` | 5 | hand-made | in its own temporary store with a registered generation and its smoke run: promote without a report or without a run exits 2; a complete full fixture bundle passes GateReportReader with scope core accepted (without calling promote); then each of twenty single changes (shard or control report, exit 1, another manifest digest, 0/0, k/k, above N, a repeated id, an aggregate report missing a shard, with another plan digest, with a result under the wrong owner, without --shards, an inserted run, a removed compared cut, another registeredRowSha256, the digest only under an assertion store, state isolated, another runRecordSha256, a shuffled run, scope core without --accept-scope) is refused by promote with its code and leaves the store's bytes unchanged |
| `I-20` | 5 | hand-made | rule change in a new generation: A = the pipeline baseline run (rule 1.0.0); in store assertion:I-20 generation 1 on registry and generation 2 on registry-next (rule 1.0.1, that store's floor 2, the pipeline store's 1); B = baseline@rule-1.0.1 under generation 2, compared with the oracle at every cut; every row of B carries the new rulesDigest, rule version 1.0.1 and evaluationGeneration 2, every row of A the old digest, the digests differ, each fold holds only its own; A and B named by (store, registeredRowSha256), both I-20 generations isolated |
| `I-23` | 5 | horizon-planned-late | horizon-planned-late online: W1 = A at the planned cut, holeAt and holeAt - 1 admitted, holeAt - 2 replay-required beyond-horizon, visit 3 missingPeriods 1 before the late batch and 0 after; horizon-planned-late@replay on the locked replay recording: every deadline keeps its cut ids and is bracketed by the reported W1, the third sample attributed with cause replayed, no replay-required disposition, every timer row carries the replayed timer cause and precedes its progress; the base journals reread after the replay equal their index and fold alike, the replay journals carry only the replay generation; both generations shared under --state |
| `G-SELF-OMITTED` | 6 | hand-made | the gate on a temporary copy of the manifest whose registry leaves one manifest id without an assertion: exit 1, that id in notRun, the accounting line one short |
| `G-SELF-MISSING-REPORT` | 6 | hand-made | the gate with a report directory it cannot write: exit 1, and the failure report written to stderr instead |
| `C-1` | 6 | aggregate | item 1 (spec 579): every scenario run, shuffled run and generation run of the gate was compared with the oracle at every cut its recording plans and every compared cut matched; report.json measurements.item1 lists the scenarios and the cuts, rows and dispositions compared |

## Scenarios

Every scenario is a committed scenario file with a committed lock (file digests and counts). The three smoke recordings are committed; every other recording is built deterministically at gate time and verified byte for byte against its lock. The five horizon recordings span the 24-hour horizon with about 440,000 messages each; they are never shortened. The replay recordings are materialized from their originals at gate time and verified against their own locks. `T0` is 1794182400000 (2026-11-09T00:00:00Z); times are epoch milliseconds.

| Scenario | Messages | Samples | Sessions | Occupancy | Incidents | Cuts | Recording |
|---|---|---|---|---|---|---|---|
| `bad-quality` | 15760 | 22064 | 5 | 32 | 0 | 73 | built at gate time |
| `baseline` | 15760 | 22064 | 5 | 32 | 0 | 73 | built at gate time |
| `cascade` | 17260 | 24164 | 5 | 33 | 2 | 79 | built at gate time |
| `clock-plus-30` | 15760 | 22064 | 5 | 32 | 0 | 69 | built at gate time |
| `clock-plus-90` | 15760 | 22064 | 5 | 32 | 0 | 69 | built at gate time |
| `corrections-10b-after` | 15760 | 22064 | 5 | 33 | 0 | 73 | built at gate time |
| `corrections-10b-before` | 15760 | 22064 | 5 | 33 | 0 | 73 | built at gate time |
| `corrections-10c` | 17120 | 23968 | 5 | 33 | 0 | 78 | built at gate time |
| `corrections` | 15760 | 22064 | 5 | 36 | 0 | 75 | built at gate time |
| `held-back-10min` | 15760 | 22064 | 5 | 32 | 0 | 73 | built at gate time |
| `horizon-correction-beyond` | 443720 | 621208 | 5 | 33 | 0 | 87 | built at gate time |
| `horizon-no-departure` | 446560 | 625184 | 5 | 33 | 0 | 86 | built at gate time |
| `horizon-planned-late` | 440813 | 617136 | 5 | 32 | 0 | 87 | built at gate time |
| `horizon-reopened` | 445060 | 623084 | 5 | 33 | 0 | 87 | built at gate time |
| `overlap` | 15760 | 22064 | 5 | 38 | 0 | 77 | built at gate time |
| `plc-held-back` | 15760 | 40976 | 5 | 16 | 0 | 69 | built at gate time |
| `plc-horizon-no-end` | 440560 | 1145456 | 5 | 6 | 0 | 71 | built at gate time |
| `plc-nocheck` | 15760 | 40976 | 5 | 16 | 0 | 69 | built at gate time |
| `plc-stamped` | 15760 | 40976 | 5 | 16 | 0 | 69 | built at gate time |
| `quiet-occupancy-control` | 16060 | 22484 | 5 | 35 | 0 | 75 | built at gate time |
| `quiet-occupancy` | 16060 | 22484 | 5 | 34 | 0 | 75 | built at gate time |
| `redelivery` | 15761 | 22066 | 5 | 33 | 0 | 73 | built at gate time |
| `session` | 15755 | 22054 | 9 | 32 | 0 | 73 | built at gate time |
| `smoke-incident` | 7760 | 10864 | 5 | 2 | 1 | 53 | committed |
| `smoke-plc` | 4060 | 10556 | 5 | 2 | 0 | 28 | committed |
| `smoke` | 4060 | 5684 | 5 | 2 | 0 | 28 | committed |
| `stage2-early` | 15760 | 22064 | 5 | 32 | 1 | 74 | built at gate time |
| `stage2` | 16760 | 23464 | 5 | 32 | 5 | 79 | built at gate time |
| `horizon-planned-late` replay | - | - | - | - | - | 87 | materialized at gate time, `outOfOrdernessMs` 30000 |
| `smoke` replay | - | - | - | - | - | 28 | materialized at gate time, `outOfOrdernessMs` 30000 |

## Formats

### Recordings

A recording directory holds one JSONL file per topic partition (`ot.messages`, `ot.sessions`, `mfg.occupancy`, `quality.incidents`), `routing.json` (device-key placement and its `routingDigest`), `expected.json` (ground truth written from the scenario, never from the core) and `manifest.json` (`recordingVersion`, `name`, `synthetic`, `source`, `rule`, `definitions`, `T0`, `scanMs`, `evidenceRange`, `batches`, `deadlines`, `settleTargetW`, `tailMs`, `digests`). Each batch is a cut: `{cutId, throughOffsets per topic and partition, idleInputs}`. A replay recording adds `source.replayOf` (the original manifest's raw SHA-256), `source.outOfOrdernessMs`, `source.deadlines` (the bracket proof of every deadline) and `offset-map/` (replay offset to original offset).

### Journals

The harness writes one JSONL file per partition of each output topic: `forseti.work-records`, `forseti.samples`, `forseti.affected`, `forseti.affected-sets`, `forseti.dispositions-stage1`, `forseti.dispositions-stage2`. Each row has a body and an envelope; `bodyDigest` is the RFC 8785 canonical SHA-256 of the body, recomputed by the gate on both sides of every comparison. For a scenario with a generation run, the base and generation-run journals are kept under `journals/<scenario>/<variant>/` with an index of raw SHA-256 digests.

### Cut log and run record

`cuts.json` is `{generation, routingDigest, cuts}`; each cut has `cutId`, per station `W1`, `admissionW`, `W2`, `mergedCutIds`, `onlineVisits`, and the offset ranges delivered. `run.json` records `runId`, `engine`, `artifactSha256`, `generation`, `registeredRowSha256`, `scope`, `seed`, `shuffle`, `replay`, `originalRecordingManifestSha256`, `pins`, `fixtureDigests`, `jcs`, `subscribed`, `offsetRanges`, `cuts` and `command`.

### Gate report

`report.json` records `kind` (`gate` or `negative-control`), `layout` (`full`, `shard` or `aggregate`), `exit`, `scope` (`core`), `manifestSha256`, `assertionsExpected`, `assertionsRun`, `failed`, `notRun`, `results` with evidence, `generations[]` and `runs[]` (each qualified by shard and store), the shard plan and summaries digests, the pins and the seed. `forseti generation promote` accepts only a whole-gate report (`kind: gate`, layout `full` or `aggregate`) with its evidence bundle, and accepts a `core` scope only with `--accept-scope core`.

## Negative controls

The eight engine-independent controls are flags the harness passes to the core (or `--lenient` to the oracle); production code runs with every flag off. Mutations: `processing-time` replaces every sample `sourceTs` and occupancy `eventTime` by the delivery sequence number before the decision; `drop-late` drops a beyond-horizon input without its `replay-required` disposition; `no-cascade` makes stage 2 ignore basis revisions after a row exists; `lenient-oracle` makes the oracle report `provisional` visits as `complete`; `first-wins` keeps the visit with the earlier arrival and retracts only the later one; `station-granularity` uses the station's activity as the tool's; `ignore-session-gap` adds no `session-gap`; `wrong-rules-pin` writes the envelope's attribution rule pin from another release while the body `rulesDigest` stays correct.

A control runs only on the shards that own or feed its intended assertions, with the same manifest, shard plan, fixtures, seed and jar; it must exit 1 with its intended assertions in `failed` (a shard that only feeds an aggregate assertion may exit 0); a crash or a missing run never counts as the intended failure; every cut and oracle comparison is kept. A control with an aggregate assertion (`drop-late`, `no-cascade`) is then proved by `--aggregate <dir> --negative-control <name>` over that control's shard runs only. `scripts/check-control-report.py` checks each run. A control report has `kind: negative-control` and isolated generations only, so it can never authorize a promotion. The ninth control of the spec, `non-transactional-output`, is sink behaviour and belongs to delivery steps 6 and 7.

| Control | Intended failures | Shards |
|---|---|---|
| `processing-time` | `I-1` | `hand-made` |
| `drop-late` | `J-DISPOSITION-COVERAGE` | `horizon-planned-late` |
| `no-cascade` | `I-18`, `J-BASIS-CURRENT` | `hand-made` |
| `lenient-oracle` | `I-2`, `I-6` | `hand-made`, `horizon-no-departure` |
| `first-wins` | `I-4` | `hand-made` |
| `station-granularity` | `I-16` | `hand-made` |
| `ignore-session-gap` | `I-8` | `hand-made` |
| `wrong-rules-pin` | every `I-*` assertion (34) | `hand-made`, `horizon-no-departure`, `horizon-reopened`, `horizon-correction-beyond`, `horizon-planned-late`, `plc-horizon-no-end` |

## Deferred obligations

| Obligation | Step | Reason |
| --- | --- | --- |
| Injection 21 (kill -9, checkpoint restore, transactional sinks) | 6, 7 | engine behaviour. |
| Injection 22 (i)-(iv) (restart, rebalance, recovery control) | 6, 7 | needs the engines and Kafka. |
| Injection 24 (starvation, the 60 s observation deadline) | 6, 7 (`forseti requirements`) | engine time behaviour; both raw placements are step-5 fixtures. |
| Injection 25 classification (NDEATH assignment, missing NBIRTH) | 5 | bridge logic; the core consumes the recorded session events. |
| Control `non-transactional-output` | 6, 7 | sink behaviour. |
| Comparison items 3, 4, 5(c), 5(d), 7, 8, 9 | 6-8 | restart, replay wall time, isolation, adapter cost, footprint and latency are engine measurements. |
