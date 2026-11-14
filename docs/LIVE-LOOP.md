# The live loop (step 5)

## What runs

- `live/docker-compose.yml`: HiveMQ CE 2026.5 and Apache Kafka 4.2.2 (single-node KRaft), both by digest, both with health checks.
  Host ports `FORSETI_MQTT_PORT` (default 1883) and `FORSETI_KAFKA_PORT` (default 9092). `auto.create.topics.enable=false`.
- Host JVMs (JDK 17), all on localhost, so Muninn and the driver reach the sim at the endpoint URL it advertises
  (`opc.tcp://localhost:48400`):
  - the Bifrost sim, `-Dbifrost.sim.scanMs=<scenario scanMs>` (1000);
  - one Muninn per routed device (five): `feed ... --hierarchy fixtures/live/registry/mappings <stream> <instanceId> --session 1000
    --primary-host host-<edge>`; edge id = device path below the site with `:` -> `-` (`line2-station1-nutrunner1`);
  - the bridge (`bridge/target/forseti-bridge.jar`), the driver and the recorder (`loop/target/forseti-loop.jar`).
- `live/stack-lock.json`: the two image digests; the Bifrost and Muninn commits and the SHA-256 of the sim and Muninn jars built
  from them. Jars are not committed.
- `live/logback.xml`: INFO logging for every JVM the runner starts.

## Setup

- JDK 17.0.21, Maven 3.9, Docker with compose v2, Python 3.13 with the oracle installed (`pip install -e "oracle[dev]"`).
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
  - 25(a): `--host-offline <device>@<atMs>+<downMs>` (host STATE offline, then online);
  - 3: a scenario `badStatus` entry; plc-stamped attribution: `--attribution plc-stamped`.
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
  8. `preflight`: `forseti-loop preflight` (client ids, topics, periods, routing, sim nodes, pins, schemas).
  9. `drive`: `forseti-loop drive`; Muninn and host injections run beside it.
  10. `tail`: waits `tailMs + margin` after the drive.
  11. `stop-bridge`: the stop file; the bridge drains every write and must end `stopped`.
  12. `stop-publishers`: the Muninns, then the sim.
  13. `record`: `forseti-loop record` (refuses a capture it cannot prove).
  14. `verify-capture`: `forseti recordings verify-capture <recording> --lock <lock> --write-lock`, then with `--lock`.
  15. `core`: `forseti generation new` on `<out>/state`, then `forseti run --engine core`.
  16. `oracle`: `python -I -m forseti_oracle.cli all ... --check-journals <out>/core`.
  - Teardown (always, also on failure or Ctrl-C): every started process stopped, `docker compose down -v --remove-orphans`; leftover
    processes or containers fail the run.
- Why the bridge stops before Muninn: a stopping Muninn's NDEATH would become each device's `session-down`, a business event with no
  telemetry tail after it, and the recorder refuses that capture (spec 628).

## Outputs (`<out>/`)

- `run.json`: outcome, failed step, parameters, injections, stack-lock digest, jar digests, steps.
- `steps.jsonl`, `logs/<process>.log` (sim, bridge, `muninn-<edge>`, drive, record, core, oracle, compose).
- `pins.json`, `deployment.tsv`, `preflight.txt`, `drive.jsonl`, `injections.log` (when injecting).
- `bridge-run.json` (bridge run record, D3 stages), `bridge-frames.jsonl` (raw MQTT frames).
- `recording/` (step-3 format), `recording.capture-report.json` (rates, `lastBusinessEvent`, tails),
  `recording.dead-letter.jsonl`, `recording.capture-lock.json`.
- `state/`, `generation.json`, `core/` (harness journals, `cuts.json`, `run.json`), `core-run.json`, `oracle/`.

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
- `drive`: `logs/drive.log` names the step; a commit not seen within 1.5 scan periods fails the drive.
- `record`: the refusal is the last line of `logs/record.log` (offset gaps, short tails, unpinned definitions, unrouted devices).
- `verify-capture`: each problem is a line of `logs/verify-capture.log`.
- `core` / `oracle`: `core-run.json`, `logs/core.log`, `logs/oracle.log`, `oracle/journal-check.json`.
- Teardown left something: `docker ps -a --filter label=com.docker.compose.project=forseti-live-<runId>`;
  the pids are in `<out>/pids/`.
