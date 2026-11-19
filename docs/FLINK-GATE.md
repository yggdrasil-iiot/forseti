# The Flink replay gate and requirements

`forseti gate --engine flink` is the step-6 semantic gate (spec 705, 728). It replays the engine recordings of `engine-fixtures/` through Kafka into the Flink stage-1 and stage-2 jobs. It dumps the committed outputs with one `read_committed` consumer into the step-3 journal format. It compares them with the unchanged Python oracle at every cut, with the same comparison, integrity checks and injection assertions as the core gate. `forseti requirements --engine flink` records the station-isolation requirement (spec 25, comparison item 5). The Kafka Streams adapter of step 7 runs on the engine-neutral parts (`cli/replay/`, `replay-kafka/`, `cli/gate/ReplayGate`, `ReplayRequirements`).

## Running it

```bash
mvn -B -ntp install
python -m pip install -e "oracle[dev]"
java -Xmx3g -Dforseti.engine.path=engine-flink/target/engine -jar cli/target/forseti.jar gate --engine flink --shard inj-stage1 --out build/flink-shards/inj-stage1 --python "$(command -v python)"
```

`mvn install` builds the engine path `engine-flink/target/engine/` (the Flink adapter and its dependencies). The launcher finds the `flink` provider there through `-Dforseti.engine.path`, the `FORSETI_ENGINE_PATH` environment variable or an `engines/` directory beside the jar. Without a provider, `--engine flink` is a usage error (exit 2). The broker is the pinned `apache/kafka:4.2.2` image of `live/stack-lock.json`. The command starts and removes it in Docker (`--kafka docker`, the default), or uses a running broker (`--kafka <host:port>`). Each job of each run runs in its own application-mode cluster: one JobManager JVM and one TaskManager JVM, no Flink image. The replay gate is supported on Linux (plan D12). Injections 22(i) and 22(iv) stop the TaskManager with SIGSTOP and therefore run on Linux.

Like every gate, it prints one line per assertion, then `assertions run N / expected 66`. It exits 0 only when every expected assertion ran and held (1 otherwise, 2 for a usage error). `--aggregate <dir>` evaluates the cross-shard assertions over every shard's `report.json` and `summaries.json`. Without `--shard` or `--aggregate`, the gate runs every shard's runs in one process.

## Shards

`gate/flink-shards.json` holds 13 shards and 30 aggregate assertions. Each shard is one CI job with its own broker and clusters: six `inj-*` shards of hand-made scenarios, `restart-21` and `restart-22` (each restart injection beside its uninterrupted run), and one shard per horizon recording. A shard's `settings` give the TaskManager task heap of each stage (512m for the hand-made and restart shards; 5g for stage 1 and 2g for stage 2 on a horizon shard). Settings are never a gate criterion. In the unsharded layout, each run takes the settings of the first shard that plans it.

Every run directory keeps `run.json`, `cuts.json`, `evidence.json` and `comparison.json`: the ground-truth deadlines, and per cut the stations' `W1` and `admissionW`, the stations with an open visit, and the body fingerprint of every output partition. A later comparison between runs reads this file. The fingerprints hold the bodies only, never a run id, topic name, path or checkpoint id.

## Negative controls

`gate/flink-negative-controls.json` lists nine controls: the eight of the core gate and `non-transactional-output` (control theta, spec 700). `non-transactional-output` switches the sinks to at-least-once and kills stage 1 between writing and committing, so injections 21 and 22 fail on duplicates, losses or revision gaps. Each runs on the shards that own or feed its intended assertions and must exit 1 with them failed. `scripts/check-control-report.py` verifies this.

```bash
java -Xmx3g -Dforseti.engine.path=engine-flink/target/engine -jar cli/target/forseti.jar gate --engine flink --negative-control first-wins --shard inj-stage1 --out build/flink-controls/first-wins/inj-stage1 --python "$(command -v python)"
python -I scripts/check-control-report.py build/flink-controls/first-wins/inj-stage1/report.json $? first-wins inj-stage1
```

## Requirements

```bash
java -Xmx3g -Dforseti.engine.path=engine-flink/target/engine -jar cli/target/forseti.jar requirements --engine flink --gate-report build/flink/report.json --out build/requirements --python "$(command -v python)"
```

`gate/flink-requirements.json` lists 13 requirements and the 10 runs they need: `baseline`, `starved-station3` (injection 24), `starved-station3-control` (the same recording without the starvation), `baseline-batch-x2` and `baseline-batch-half` on the dedicated (`site-071`) and on the shared (`site-019`) raw placement. `starved-station3` and its control pin the placement's starvation release (`line2-rules-dedicated-starvation`, `line2-rules-shared-starvation`), whose impact rule also accepts incidents on station3, and carry one closed incident on station3 delivered before the starvation. Each run is compared with the oracle at every cut as a gate run is.

| Id | Item | Recorded from |
|---|---|---|
| `R-5A` | 5 (a) | the gate report's result for `I-26` |
| `R-5B` | 5 (b) | the gate report's result for `I-7` |
| `R-24-DEDICATED`, `R-24-SHARED` | injection 24 | the starved run equals the oracle; in the committed journals the starved station's writers commit no progress of a starved cut and their first progress after it merges exactly the starved cuts; none of its visits changes during the starvation; its incident accepted before the starvation with an unsettled affected set keeps its set and assessment bodies, unsettled, at every starved cut and at the last cut has the control's bodies; an `error` when the recording has no such incident |
| `R-5C-DEDICATED`, `R-5C-SHARED` | 5 (c) | the starved run's observation (`isolation.json`) |
| `R-5D-RESUME-DEDICATED`, `R-5D-RESUME-SHARED` | 5 (d) | the starved run against its control, from the first cut after the starvation |
| `R-5D-RESTART` | 5 (d) | the gate report's results for `I-21` and `I-22` |
| `R-5D-SPEED-X2-*`, `R-5D-SPEED-HALF-*` | 5 (d) | the run at batch interval x2 or x1/2 against the baseline |

5 (c): in a run of a recording with a starvation, the harness checks every ground-truth deadline of every other station whose counting batch is starved. The counting batch is the first batch after which every non-starved station's routed telemetry has a `sourceTs` at or past the deadline + `outOfOrdernessMs` + `expectedPeriodMs`. A check settles when the station's committed stage-1 progress reports `W1` at or past the deadline and its committed stage-2 progress reports `W2` at or past it, within 60 s of the counting batch's first append (the earliest broker append time of its records). Otherwise it is a miss. The checks are observed while the harness waits for the cut. The harness writes the next batch once the cut is complete and every check has settled or recorded its miss, or at once when a check recorded its miss and the cut is not complete (spec 418). Every non-starved station with a deadline in the starvation window must have a check, or the requirement is an `error`.

5 (d): two runs are compared at their common cuts on every output body partition. Common cuts are the cuts with the same replay input boundary (both recordings have delivered the same input records), found at exactly one cut of each run. A common cut is compared where both runs completed every station and the comparability rule of spec 624 holds, and it must show bodies on both sides. The settled and the last common cut must be among the compared, or the requirement is an `error`.

A requirement records `pass`, `miss` or `error` with its evidence. A requirement taken from the gate report is an `error` when the report is missing or unreadable, is not of this engine, assertion manifest and engine artifact, records a result twice, or lacks the assertion; the runs still execute. The command prints one line per requirement, `requirements recorded N / expected 13` and the engine eligibility line. It writes `requirements.json` and exits 0 when every requirement was run and recorded; it exits 1 when the runs crashed or a run was not executed. A miss is a recorded result, not a failure. The engine is eligible only when the semantic gate passed (a full or aggregate gate report that exited 0 with exactly one successful result for every assertion of the current manifest) and every requirement passed. Eligibility is a separate result from the completion of the delivery step (plan D14). Spec Decision 18 predicts a miss of 5 (c) on the dedicated placement: the Flink stage-1 watermark is the minimum over every subscribed raw telemetry partition, so a starved station alone on its partition holds every station's `W1`.

## Determinism

```bash
java -jar cli/target/forseti.jar determinism --runs build/d/run-1,build/d/run-2,build/d/run-3 --out build/d
```

Comparison item 6 for an engine: three executions of the same gate run (one shard, or the whole gate), each a `gate` report that exited 0 with the same shard, engine artifact, assertion manifest and runs. For every run, every pair of executions is compared at every comparable cut both completed, on every body partition; a restart run's executions may complete different cuts. Revision counts come from each execution's `summaries.json`. It exits 0 when every run held with at least one comparable cut for every pair.

## CI

- Every pull request and push (`ci.yml`): the shards (`flink-gate`), the controls (`flink-control`, `flink-control-aggregate`) and the aggregate (`flink-aggregate`), fresh on every pull request; core-evidence reuse never replaces them. The requirements unit tests run in the test jobs on Linux and Windows.
- Push to main and manual runs (`ci.yml`, `flink-requirements`): `forseti requirements --engine flink` on that run's aggregate report. It never runs on a pull request, so it never blocks one.
- Nightly (`nightly-gate.yml`): the unsharded Flink gate, three executions of every shard with `forseti determinism`, and `forseti requirements --engine flink` on the unsharded gate's report. In the unsharded gate one broker serves every run; each run's own input and output topics are deleted once the run is dumped, compared with the oracle and its per-run findings are made (the online base of the event-time replay after the replay's redump), so the broker holds one run's topics at a time, as a shard's broker does. The nightly job records, per run, the harness heap after a full collection, live child processes, the broker container's memory and data, its topics and both TaskManagers' RSS (`forseti.gate.memoryLog`, `build/memory/flink-runs.jsonl`, one printed line per run), and samples memory, free disk and the broker every 2 s, printing a sample a minute to the job log.

Each job uploads its reports and run directories as artifacts.
