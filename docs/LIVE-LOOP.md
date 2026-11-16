# The live loop (step 5)

## What runs

- `live/docker-compose.yml`: HiveMQ CE 2026.5 and Apache Kafka 4.2.2 (single-node KRaft), both by digest, both with health checks.
  Host ports `FORSETI_MQTT_PORT` (default 1883) and `FORSETI_KAFKA_PORT` (default 9092). `auto.create.topics.enable=false`.
- Host JVMs (JDK 17), all on localhost, so Muninn and the driver reach the sim at the endpoint URL it advertises
  (`opc.tcp://localhost:48400`):
  - the Bifrost sim, `-Dbifrost.sim.scanMs=<scenario scanMs>` (500);
  - one Muninn per routed device (five): `feed ... --hierarchy fixtures/live/registry/mappings <stream> <instanceId>
    --session <scenario muninnPeriodMs> --primary-host host-<edge>` (250); edge id = device path below the site with `:` -> `-`
    (`line2-station1-nutrunner1`);
  - the bridge (`bridge/target/forseti-bridge.jar`), the driver and the recorder (`loop/target/forseti-loop.jar`).
- `live/stack-lock.json`: the two image digests; the Bifrost and Muninn commits and the SHA-256 of the sim and Muninn jars built
  from them. Jars are not committed.
- `live/logback.xml`: INFO logging for every JVM the runner starts.

## Periods

- The rule's period and the acquisition cadence are separate settings. The rule (`coverage.expectedPeriodMs` 1000,
  `maxMissingPeriods` 3) is the deployment's `rule-period-ms`; the sim's scan S (`scanMs`, 500) and Muninn's poll M
  (`muninnPeriodMs`, 250) are the live scenario's.
- Preflight, `forseti-loop drive` and `forseti-loop record` require `M + S + 100 <= expectedPeriodMs`. Muninn polls free-running and
  publishes the last scan it read, which is at most S old, so two consecutive distinct source timestamps lie less than `M + S` plus
  jitter apart; under the bound every 1000 ms slot holds an observation wherever a visit's `enter` falls. The margin of 100 ms covers
  the largest jitter seen in the live captures at a 1000 ms scan (consecutive distinct source timestamps 928 to 1072 ms apart, at most
  72 ms off, typically 16 ms). At M = S = 1000 any gap above 1000 ms left a slot without an observation, and every visit of those
  captures was insufficient.
- Preflight, `forseti-loop drive` and `forseti-loop record` also require `2 M <= S`. Neither the sim's scan nor Muninn's read is
  atomic (the sim sets a scan's nodes one by one, Muninn reads a device's members one by one), so a read that straddles a scan
  boundary mixes two scans; that message is an incoherent snapshot, which plc-stamped attribution leaves unattributed. With at
  least two reads per scan, a straddling read is not a scan's only reading. At S = 250 and M = 500 (one read every other scan)
  the plc-stamped capture had 0.45 % incoherent snapshots; in a 1000 ms slot holding a single message that message was sometimes
  incoherent, the slot had no attributed sample, and a visit with no injection was insufficient (missing-periods). The baseline
  captures in occupancy-join mode passed, since that mode does not require a coherent snapshot.
- Source timestamps stay the sim's real acquisition times; none is moved onto a grid.
- The driver steps on the scan grid (every 500 ms) and fails a commit seen 1.5 scan periods (750 ms) after the last scan without it.

## Setup

- JDK 17.0.21, Maven 3.9.16 (the locked jar bytes depend on the Maven version, because the upstream POMs leave some plugin versions to its defaults; `build-live-jars.sh` also needs LF checkouts and sets UTC and an LF line separator through `live/mvn-repro.args`), Docker with compose v2, Python 3.13 with the oracle installed (`pip install -e "oracle[dev]"`).
- Host ports 1883, 9092, 9093 (inside the Kafka container only) and 48400 free.
- `mvn -B -ntp install -DskipTests` (cli, bridge and loop jars).
- Upstream jars, from checkouts at the locked commits (clean trees):

  ```bash
  git clone https://github.com/yggdrasil-iiot/bifrost ../bifrost-lock && git -C ../bifrost-lock checkout <bifrost commit of the lock>
  git clone https://github.com/yggdrasil-iiot/muninn ../muninn-lock && git -C ../muninn-lock checkout <muninn commit of the lock>
  bash scripts/build-live-jars.sh ../bifrost-lock ../muninn-lock   # -> build/live-jars/{bifrost-sim,muninn}.jar, checked against the lock
  ```

  - The build sets `project.build.outputTimestamp` to the commit time, so the jar bytes depend only on the commit and the JDK.
  - On Windows, check the trees out with `core.autocrlf=false` (CRLF resources change the jar bytes).
  - Extra Maven arguments: `MVN_ARGS="-Dmaven.repo.local=..."`.

## One run

```bash
bash scripts/run-full-loop-gate.sh --placement dedicated --scenario fixtures/live-scenarios/baseline.json [--out build/full-loop/<run>]
```

- Options: `--attribution occupancy-join|plc-stamped` (stream `line2-<placement>[-plc]`), `--python <interpreter with the oracle>`,
  `--sim-jar`, `--muninn-jar`, `--tail-margin-ms` (default 15000).
- Injections:
  - 1: `--bridge-hold <startMs>:<holdMs>` (bridge hold-and-release stage);
  - 9: `--bridge-source-offset <ms>` (bridge source-clock stage);
  - 8: `--kill-muninn <device>@<atMs>+<downMs>` (`kill -9`, restart with the same edge id);
  - 25(a): `--host-offline <device>@<atMs>+<downMs>` (host STATE offline, then online; the bridge runs with `--hold-death <edge>`,
    its death-after-birth input stage: the edge's host-offline NDEATH reaches the session tracker only after the edge's next NBIRTH,
    so it is the earlier session's `stale-death` and the newer session stays open; the raw-frame log keeps the wire order);
  - 3: a scenario `badStatus` entry (`fixtures/live-scenarios/bad-status.json`: nutrunner1 Torque Bad over 30 s of S1-V1);
    plc-stamped attribution: `--attribution plc-stamped`.
  - `<device>` is below the site (`line2:station1:nutrunner1`); `<atMs>` counts from the drive start.
- Duration: drive (baseline 660 s) + tail 751 s (`allowedLatenessMs + outOfOrdernessMs + expectedPeriodMs`) + margin; about 25 min.
- Steps, in order (each named in `steps.jsonl` and, on failure, on stderr and in `run.json`):
  1. `lock`: compose file and jars against `live/stack-lock.json` (`forseti-loop stack-check`).
  2. `compose-up`: `docker compose up -d --wait`; then the running images against the locked digests.
  3. `topics`: `forseti-loop topics --prefix run-<runId>.` (fresh topics, six partitions, LogAppendTime, 30 d).
  4. `deployment`: `forseti-loop deployment` writes `pins.json` and lists the edges and the tail.
  5. `sim`: waits for `OPC-UA sim listening`.
  6. `bridge`: waits for `[BRIDGE] subscribed`.
  7. `muninn`: five sessions waiting for their host, then the retained online `spBv1.0/STATE/host-<edge>` of each, then
     `[BRIDGE] session session-up <edge> <device>` for every device.
  8. `preflight`: `forseti-loop preflight` (client ids, topics, periods `M + S + 100 <= expectedPeriodMs`, routing, sim nodes, pins,
     schemas).
  9. `drive`: `forseti-loop drive`; Muninn and host injections run beside it.
  10. `tail`: waits `tailMs + margin` after the drive.
  11. `stop-bridge`: the stop file; the bridge drains every write and must end `stopped`. With `--host-offline` its run record must
      show the death-after-birth stage `completed` (a held death with no newer birth is released at the drain and fails the run).
  12. `stop-publishers`: the Muninns, then the sim.
  13. `record`: `forseti-loop record` (refuses a capture it cannot prove); the runner's injections are declared in the manifest
      (`source.injections`, with the scenario's bad statuses).
  14. `verify-capture`: `forseti recordings verify-capture <recording> --lock <lock> --registry <registry> --write-lock`, then with
      `--lock` (the registry resolves the manifest's rule pin; only equipment carrying a member of the rule advances a station's W1,
      as in the recorder).
  15. `core`: `forseti generation new` on `<out>/state`, then `forseti run --engine core`.
  16. `oracle`: `python -I -m forseti_oracle.cli all ... --check-journals <out>/core`.
  17. `compare`: `forseti recordings compare <recording> --out <out>/compare`, the core gate's per-cut comparison on this one
      recording: a fresh generation on the fixture clock, the reference harness, the oracle with `--stream` beside the gate's journal
      fold (the bodies of the six tables at every cut), the gate's integrity checks (every delivered input has a disposition, the W1
      formula, W2 = W1) and the capture's ground truth (`outcome` `equal`); then the acceptance of the visits against the declared
      injections (`accepted`): with no injection every visit is `complete`; a visit an injection touches (its station, its window
      overlapping the visit) is `insufficient` with the injection's reason (`bad-share-exceeded` for 3, `session-gap` for 8,
      `missing-periods` for 25(a)), every other visit `complete`; injections 1 and 9 touch no visit; with 25(a) the captured session
      events also show a `stale-death` of the device, no `session-down` of that session and a newer `session-up`. Exit 0 only when all
      hold; the report (no paths, no times) is copied to `recording.compare-report.json`.
  - Teardown (always, also on failure or Ctrl-C): every started process stopped, `docker compose down -v --remove-orphans`; leftover
    processes or containers fail the run.
- Why the bridge stops before Muninn: a stopping Muninn's NDEATH would become each device's `session-down`, a business event with no
  telemetry tail after it, and the recorder refuses that capture (spec 628).

## Outputs (`<out>/`)

- `run.json`: outcome, failed step, parameters, injections, stack-lock digest, jar digests, steps.
- `steps.jsonl`, `logs/<process>.log` (sim, bridge, `muninn-<edge>`, drive, record, core, oracle, compose).
- `pins.json`, `deployment.tsv`, `preflight.txt`, `drive.jsonl`, `injections.log` (when injecting).
- `bridge-run.json` (bridge run record: input stages with the held and releasing frames, D3 stages), `bridge-frames.jsonl` (raw MQTT
  frames, in wire order).
- `recording/` (step-3 format; `expected.json` version 2, the samples in `expected-samples/`), `recording.capture-report.json` (rates, `lastBusinessEvent`, tails),
  `recording.dead-letter.jsonl`, `recording.capture-lock.json`.
- `state/`, `generation.json`, `core/` (harness journals, `cuts.json`, `run.json`), `core-run.json`, `oracle/`.
- `compare/` (its own state, harness run, `diffs/` on a difference, `compare-report.json`), `recording.compare-report.json`.

## Committed recordings (`fixtures/live-recordings/<name>/`)

- None is committed yet: the eight runs below are captured at S = 500, M = 250 once a plc-stamped pilot shows no drive failure,
  every visit complete, per device consecutive distinct source timestamps of coherent messages at most 900 ms apart and no slot of
  a visit without a coherent sample, and then committed together.
- One directory per passed run, copied by `python -I scripts/commit-live-recording.py <run out> <name>` (it refuses a run that did not
  pass or whose compare report is not `equal` and `accepted`): `recording/` (step-3 format), `recording.capture-lock.json`,
  `recording.capture-report.json` (counts, incoherent-snapshot and repeated-`sourceTs` rates per device), `recording.dead-letter.jsonl`,
  `recording.compare-report.json`, `run.json`, `bridge-run.json`, `preflight.txt` and `provenance.json` (stack-lock digest, upstream
  commits, image digests, the SHA-256 of the jars the run used, live fixture lock, placement, attribution, stream, scenario digest,
  runner options). The core and oracle output, the raw frame log and the process logs stay in `build/`.
- Byte-stable in git: `fixtures/** -text` (and `fixtures/live-recordings/** -text`).
- The runs:

  | name | placement | attribution | scenario and options | injection |
  | :--- | :--- | :--- | :--- | :--- |
  | `baseline-dedicated` | dedicated | occupancy-join | `baseline.json` | none |
  | `baseline-shared` | shared | occupancy-join | `baseline.json` | none |
  | `bad-status` | dedicated | occupancy-join | `bad-status.json` | 3 |
  | `muninn-restart` | dedicated | occupancy-join | `baseline.json`, `--kill-muninn line2:station1@440000+20000` | 8 |
  | `plc-stamped` | dedicated | plc-stamped | `baseline.json`, `--attribution plc-stamped` | none |
  | `bridge-hold` | dedicated | occupancy-join | `baseline.json`, `--bridge-hold 60000:600000` | 1 |
  | `bridge-offset` | dedicated | occupancy-join | `baseline.json`, `--bridge-source-offset 30000` | 9 |
  | `host-offline` | dedicated | occupancy-join | `baseline.json`, `--host-offline line2:station1:nutrunner1@440000+20000` | 25(a) |

- Check (CI `tests`, group `gate-generation`): `RecordingCompareTest` runs `verify-capture --lock` on every committed live recording,
  then `recordings compare` (equal and accepted), and requires the committed `recording.compare-report.json` byte for byte; with no
  committed live recording it checks nothing.

## CI

- `.github/workflows/live-loop.yml`: nightly and `workflow_dispatch` (inputs `placement`, `scenario`); builds the locked jars from the
  public repositories at the locked commits, runs the runner, uploads `build/full-loop/` as `live-loop-run`. Not a required check;
  commits nothing.

## Triage

- `lock` fails on a jar: the jar was not built from the locked commit, with another JDK, or from a CRLF checkout.
- `compose-up` fails: a port is taken (`FORSETI_MQTT_PORT`, `FORSETI_KAFKA_PORT`), or the image is missing locally and cannot be pulled.
- `muninn` times out on `session-up`: read `logs/muninn-<edge>.log` (`[MUNINN] refuse` = a definition or hierarchy that does not
  verify) and `logs/bridge.log` (`[BRIDGE] failed`).
- `preflight`: every problem is a line of `preflight.txt`.
- `drive`: `logs/drive.log` names the step; a commit not seen within 1.5 scan periods fails the drive (try the fallback periods).
- `record`: the refusal is the last line of `logs/record.log` (offset gaps, short tails, unpinned definitions, unrouted devices).
- `verify-capture`: each problem is a line of `logs/verify-capture.log`.
- `core` / `oracle`: `core-run.json`, `logs/core.log`, `logs/oracle.log`, `oracle/journal-check.json`.
- `stop-bridge` on 25(a): `bridge-run.json` `inputStages[0]` (`held`, `releasedAfter`, `releasedBy`).
- `compare`: `logs/compare.log` (the report: differences by topic, the first examples, integrity findings, ground-truth problems,
  `acceptance.problems` with every visit's status, reasons and the injections touching it) and
  `compare/diffs/<scenario>/cut-<k>/<topic>/<n>.json` (expected = the oracle's body, actual = the core's).
- Teardown left something: `docker ps -a --filter label=com.docker.compose.project=forseti-live-<runId>`;
  the pids are in `<out>/pids/`.
