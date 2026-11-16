#!/usr/bin/env bash
# One run of the step-5 full loop, end to end (docs/LIVE-LOOP.md):
#
#   scripts/run-full-loop-gate.sh [--placement dedicated|shared] [--scenario fixtures/live-scenarios/baseline.json]
#       [--attribution occupancy-join|plc-stamped] [--out build/full-loop/<run>] [--python python]
#       [--sim-jar build/live-jars/bifrost-sim.jar] [--muninn-jar build/live-jars/muninn.jar]
#       [--bridge-hold <startMs>:<holdMs>] [--bridge-source-offset <ms>]
#       [--kill-muninn <device>@<atMs>+<downMs>] [--host-offline <device>@<atMs>+<downMs>] [--tail-margin-ms 15000]
#
# <device> is a path below the site (line2:station1:nutrunner1); <atMs> counts from the start of the drive. Injection 3 (a Bad
# StatusCode) is a scenario's badStatus, not an option. The forseti jars (cli, bridge, loop) must be built (mvn install) and the
# upstream jars built from the locked commits (scripts/build-live-jars.sh).
#
# Steps, in order: lock check -> compose up (healthy) -> image check -> fresh prefixed topics -> sim -> bridge (subscribed) -> one
# Muninn per routed device, then the retained online STATE of each one's host -> every session up -> preflight -> drive (with the
# declared Muninn and host injections beside it) -> the spec-628 tail -> stop the bridge (it drains every write) -> stop Muninn and
# the sim -> record -> verify-capture --write-lock, then --lock -> core harness and oracle on the capture -> recordings compare (the
# core gate's per-cut comparison of the core with the oracle, its integrity checks, the ground truth and the acceptance of the visits
# against the declared injections). Teardown always runs.
#
# Why the bridge stops before Muninn: a stopping Muninn publishes NDEATH, which the bridge would write as each device's session-down
# dated at its last sample, a business event with no telemetry tail after it; the recorder refuses such a capture (spec 628). Stopping
# the bridge first ends the capture with every session open, as at any instant of a running plant.
#
# Every process log, the run record (run.json), the capture report and the dead-letter side file land in the output directory. Exit 0
# when every step passed; otherwise non-zero with the failed step named on stderr and in run.json.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PLACEMENT=dedicated; SCENARIO=fixtures/live-scenarios/baseline.json; ATTRIBUTION=occupancy-join; OUT=""; PYTHON=python
SIM_JAR=build/live-jars/bifrost-sim.jar; MUNINN_JAR=build/live-jars/muninn.jar
BRIDGE_HOLD=""; BRIDGE_OFFSET=""; KILL_MUNINN=""; HOST_OFFLINE=""; TAIL_MARGIN_MS=15000
usage() { sed -n '2,12p' "$0" >&2; exit 2; }
while [ $# -gt 0 ]; do
  [ $# -ge 2 ] || usage
  case "$1" in
    --placement) PLACEMENT="$2";; --scenario) SCENARIO="$2";; --attribution) ATTRIBUTION="$2";; --out) OUT="$2";; --python) PYTHON="$2";;
    --sim-jar) SIM_JAR="$2";; --muninn-jar) MUNINN_JAR="$2";; --bridge-hold) BRIDGE_HOLD="$2";; --bridge-source-offset) BRIDGE_OFFSET="$2";;
    --kill-muninn) KILL_MUNINN="$2";; --host-offline) HOST_OFFLINE="$2";; --tail-margin-ms) TAIL_MARGIN_MS="$2";;
    *) usage;;
  esac
  shift 2
done
case "$PLACEMENT" in dedicated|shared) ;; *) echo "run-full-loop-gate: --placement is dedicated or shared" >&2; exit 2;; esac
case "$ATTRIBUTION" in occupancy-join) STREAM="line2-$PLACEMENT";; plc-stamped) STREAM="line2-$PLACEMENT-plc";; *) echo "run-full-loop-gate: --attribution is occupancy-join or plc-stamped" >&2; exit 2;; esac
[ -f "$SCENARIO" ] || { echo "run-full-loop-gate: no scenario $SCENARIO" >&2; exit 2; }
[ -z "$BRIDGE_HOLD" ] || [[ "$BRIDGE_HOLD" =~ ^[0-9]+:[1-9][0-9]*$ ]] || { echo "run-full-loop-gate: --bridge-hold is <startMs>:<holdMs>" >&2; exit 2; }
[ -z "$BRIDGE_OFFSET" ] || [[ "$BRIDGE_OFFSET" =~ ^-?[0-9]+$ ]] || { echo "run-full-loop-gate: --bridge-source-offset is an integer" >&2; exit 2; }
for v in "$KILL_MUNINN" "$HOST_OFFLINE"; do
  [ -z "$v" ] || [[ "$v" =~ ^[A-Za-z0-9_:-]+@[0-9]+\+[1-9][0-9]*$ ]] || { echo "run-full-loop-gate: an injection is <device>@<atMs>+<downMs>, not $v" >&2; exit 2; }
done
[[ "$TAIL_MARGIN_MS" =~ ^[0-9]+$ ]] || { echo "run-full-loop-gate: --tail-margin-ms is an integer" >&2; exit 2; }

RUN_ID="$(date -u +%Y%m%dt%H%M%Sz)"
NAME="$(basename "$SCENARIO" .json)"
OUT="${OUT:-build/full-loop/$RUN_ID-$PLACEMENT-$NAME}"
if [ -e "$OUT" ] && [ -n "$(ls -A "$OUT" 2>/dev/null)" ]; then echo "run-full-loop-gate: $OUT exists and is not empty" >&2; exit 2; fi
mkdir -p "$OUT/logs" "$OUT/pids"
PREFIX="run-$RUN_ID."
PROJECT="forseti-live-$RUN_ID"
MQTT_PORT="${FORSETI_MQTT_PORT:-1883}"; KAFKA_PORT="${FORSETI_KAFKA_PORT:-9092}"
export FORSETI_MQTT_PORT="$MQTT_PORT" FORSETI_KAFKA_PORT="$KAFKA_PORT"
MQTT="tcp://localhost:$MQTT_PORT"; KAFKA="localhost:$KAFKA_PORT"; OPCUA="opc.tcp://localhost:48400"
COMPOSE=(docker compose -f live/docker-compose.yml -p "$PROJECT")
LOCK=live/stack-lock.json
REGISTRY=fixtures/live/registry; DEFINITIONS=fixtures/live/definitions; ROUTING="fixtures/live/routing/$PLACEMENT.json"
FORSETI=cli/target/forseti.jar; LOOP=loop/target/forseti-loop.jar; BRIDGE=bridge/target/forseti-bridge.jar
JLOG=(-Dlogback.configurationFile=live/logback.xml)
GROUP=line2
BRIDGE_ID="forseti-bridge-$RUN_ID"; DRIVER_ID="forseti-driver-$RUN_ID"
REC="$OUT/recording"

STEP=setup
STEPS="$OUT/steps.jsonl"
: > "$STEPS"
now_ms() { date +%s%3N; }
step() { STEP="$1"; printf '{"step":"%s","startedAt":%s}\n' "$1" "$(now_ms)" >> "$STEPS"; echo "== $1"; }
fail() { echo "run-full-loop-gate: step $STEP failed: $*" >&2; FAILURE="$*"; exit 1; }
FAILURE=""

# start <name> <command...>: in the background, stdout and stderr to logs/<name>.log, the pid to pids/<name>.
start() {
  local name="$1"; shift
  "$@" > "$OUT/logs/$name.log" 2>&1 &
  echo "$!" > "$OUT/pids/$name"
}
pid_of() { cat "$OUT/pids/$1"; }
alive() { kill -0 "$(pid_of "$1")" 2>/dev/null; }
# await <name> <pattern> <seconds>: until the pattern is in the process's log; fails when the process exits first or time runs out.
await() {
  local name="$1" pattern="$2" secs="$3" i
  for ((i = 0; i < secs * 4; i++)); do
    grep -qF -- "$pattern" "$OUT/logs/$name.log" 2>/dev/null && return 0
    alive "$name" || fail "$name exited before '$pattern' (see $OUT/logs/$name.log)"
    sleep 0.25
  done
  fail "$name: no '$pattern' within $secs s (see $OUT/logs/$name.log)"
}
# stop <name> [<seconds>]: SIGTERM, then SIGKILL after the grace period. A process already gone is fine.
stop() {
  local name="$1" grace="${2:-10}" pid i
  [ -f "$OUT/pids/$name" ] || return 0
  pid="$(pid_of "$name")"
  kill "$pid" 2>/dev/null || return 0
  for ((i = 0; i < grace * 4; i++)); do kill -0 "$pid" 2>/dev/null || return 0; sleep 0.25; done
  kill -9 "$pid" 2>/dev/null || true
}
json() { "$PYTHON" -I -c 'import json, sys; d = json.load(open(sys.argv[1], encoding="utf-8")); print(eval(sys.argv[2], {"d": d}))' "$@"; }

teardown() {
  local rc=$? name leftovers=""
  trap - EXIT INT TERM
  set +e
  echo "== teardown"
  [ -f "$OUT/pids/injections" ] && stop injections 1
  for f in "$OUT"/pids/*; do [ -f "$f" ] && stop "$(basename "$f")" 5; done
  for f in "$OUT"/pids/*; do [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null && leftovers="$leftovers $(basename "$f")"; done
  "${COMPOSE[@]}" down -v --remove-orphans > "$OUT/logs/compose-down.log" 2>&1
  local containers
  containers="$(docker ps -a --filter "label=com.docker.compose.project=$PROJECT" --format '{{.Names}}' 2>/dev/null)"
  if [ -n "$leftovers" ] || [ -n "$containers" ]; then
    echo "run-full-loop-gate: teardown left processes [$leftovers ] containers [$containers]" >&2
    [ "$rc" -ne 0 ] || { rc=1; STEP=teardown; FAILURE="teardown left processes or containers"; }
  fi
  local outcome=passed
  [ "$rc" -eq 0 ] || outcome=failed
  {
    printf '{\n  "kind": "forseti-full-loop-run",\n  "runId": "%s",\n  "outcome": "%s",\n' "$RUN_ID" "$outcome"
    if [ "$rc" -ne 0 ]; then printf '  "failedStep": "%s",\n  "failure": "%s",\n' "$STEP" "$(printf '%s' "$FAILURE" | sed 's/\\/\\\\/g; s/"/\\"/g')"; fi
    printf '  "placement": "%s",\n  "attribution": "%s",\n  "stream": "%s",\n  "scenario": "%s",\n  "topicPrefix": "%s",\n' \
      "$PLACEMENT" "$ATTRIBUTION" "$STREAM" "$SCENARIO" "$PREFIX"
    printf '  "injections": {"bridgeHold": "%s", "bridgeSourceOffsetMs": "%s", "killMuninn": "%s", "hostOffline": "%s"},\n' \
      "$BRIDGE_HOLD" "$BRIDGE_OFFSET" "$KILL_MUNINN" "$HOST_OFFLINE"
    printf '  "tailMarginMs": %s,\n  "scenarioSha256": "%s",\n' "$TAIL_MARGIN_MS" "$(sha256sum "$SCENARIO" | cut -c1-64)"
    printf '  "stackLockSha256": "%s",\n' "$(sha256sum "$LOCK" | cut -c1-64)"
    printf '  "jars": {"bifrost-sim": "%s", "muninn": "%s", "forseti": "%s", "forseti-loop": "%s", "forseti-bridge": "%s"},\n' \
      "$(sha256sum "$SIM_JAR" 2>/dev/null | cut -c1-64)" "$(sha256sum "$MUNINN_JAR" 2>/dev/null | cut -c1-64)" "$(sha256sum "$FORSETI" 2>/dev/null | cut -c1-64)" \
      "$(sha256sum "$LOOP" 2>/dev/null | cut -c1-64)" "$(sha256sum "$BRIDGE" 2>/dev/null | cut -c1-64)"
    printf '  "steps": [\n'
    sed '$!s/$/,/; s/^/    /' "$STEPS"
    printf '  ]\n}\n'
  } > "$OUT/run.json"
  echo "run record: $OUT/run.json ($outcome)"
  exit "$rc"
}
trap teardown EXIT
trap 'FAILURE="interrupted"; exit 130' INT TERM

for f in "$FORSETI" "$LOOP" "$BRIDGE"; do [ -f "$f" ] || fail "no $f (run mvn install first)"; done

step lock
java -jar "$LOOP" stack-check --lock "$LOCK" --compose live/docker-compose.yml --jar "bifrost-sim=$SIM_JAR" --jar "muninn=$MUNINN_JAR" \
  > "$OUT/logs/stack-check.log" 2>&1 || { cat "$OUT/logs/stack-check.log" >&2; fail "the compose file or a jar is not the locked one"; }

step compose-up
"${COMPOSE[@]}" up -d --wait > "$OUT/logs/compose-up.log" 2>&1 || { tail -20 "$OUT/logs/compose-up.log" >&2; fail "the stack did not come up healthy"; }
IMAGES=()
for s in hivemq kafka; do
  cid="$("${COMPOSE[@]}" ps -q "$s")"
  IMAGES+=(--image "$s=$(docker image inspect --format '{{join .RepoDigests ","}}' "$(docker inspect --format '{{.Image}}' "$cid")")")
done
java -jar "$LOOP" stack-check --lock "$LOCK" "${IMAGES[@]}" > "$OUT/logs/image-check.log" 2>&1 \
  || { cat "$OUT/logs/image-check.log" >&2; fail "a running container is not the locked image"; }
"${COMPOSE[@]}" logs --no-color > "$OUT/logs/compose-start.log" 2>&1 || true

step topics
java "${JLOG[@]}" -jar "$LOOP" topics --kafka "$KAFKA" --prefix "$PREFIX" --client-id "forseti-loop-admin-$RUN_ID" > "$OUT/logs/topics.log" 2>&1 \
  || { cat "$OUT/logs/topics.log" >&2; fail "topics"; }

step deployment
java -jar "$LOOP" deployment --registry "$REGISTRY" --definitions "$DEFINITIONS" --stream "$STREAM" --routing "$ROUTING" --pins-out "$OUT/pins.json" \
  > "$OUT/deployment.tsv" 2> "$OUT/logs/deployment.log" || { cat "$OUT/logs/deployment.log" >&2; fail "deployment"; }
TAIL_MS="$(awk -F'\t' '$1 == "tail-ms" {print $2}' "$OUT/deployment.tsv")"
# The rule's period (deployment) and the acquisition cadence (scenario) are separate settings: Muninn polls every MUNINN_MS, the sim
# scans every SCAN_MS, and preflight checks MUNINN_MS + SCAN_MS + its jitter margin <= RULE_PERIOD_MS.
RULE_PERIOD_MS="$(awk -F'\t' '$1 == "rule-period-ms" {print $2}' "$OUT/deployment.tsv")"
SCAN_MS="$(json "$SCENARIO" 'd["scanMs"]')"
MUNINN_MS="$(json "$SCENARIO" 'd["muninnPeriodMs"]')"
echo "periods: rule expectedPeriodMs $RULE_PERIOD_MS, Muninn $MUNINN_MS, sim scan $SCAN_MS"
mapfile -t EDGES < <(awk -F'\t' '$1 == "edge"' "$OUT/deployment.tsv")
[ "${#EDGES[@]}" -gt 0 ] || fail "no edges"
edge_of() { local d="$1"; d="${d//:/-}"; for e in "${EDGES[@]}"; do IFS=$'\t' read -r _ edge _ <<< "$e"; [ "$edge" = "$d" ] && { echo "$edge"; return 0; }; done; fail "no routed device $1"; }
for v in "$KILL_MUNINN" "$HOST_OFFLINE"; do [ -z "$v" ] || edge_of "${v%%@*}" > /dev/null; done

step sim
start sim java "${JLOG[@]}" -Xmx512m -Dbifrost.sim.scanMs="$SCAN_MS" -jar "$SIM_JAR"
await sim "OPC-UA sim listening" 60

step bridge
BRIDGE_ARGS=(--mqtt "$MQTT" --kafka "$KAFKA" --client-id "$BRIDGE_ID" --pins "$OUT/pins.json" --routing "$ROUTING"
  --messages-topic "${PREFIX}ot.messages" --sessions-topic "${PREFIX}ot.sessions" --dead-letter-topic "${PREFIX}ot.dead-letter"
  --raw-log "$OUT/bridge-frames.jsonl" --run-record "$OUT/bridge-run.json" --stop-file "$OUT/bridge.stop")
[ -z "$BRIDGE_HOLD" ] || BRIDGE_ARGS+=(--hold-start-ms "${BRIDGE_HOLD%%:*}" --hold-ms "${BRIDGE_HOLD##*:}")
[ -z "$BRIDGE_OFFSET" ] || BRIDGE_ARGS+=(--source-offset-ms "$BRIDGE_OFFSET")
# Injection 25(a): the bridge's input stage hands the edge's host-offline NDEATH to its session tracker only after the edge's next
# NBIRTH, so the earlier connection's death arrives past the newer birth (a stale-death); the raw-frame log keeps the wire order.
[ -z "$HOST_OFFLINE" ] || BRIDGE_ARGS+=(--hold-death "$(edge_of "${HOST_OFFLINE%%@*}")")
start bridge java "${JLOG[@]}" -Xmx512m -jar "$BRIDGE" "${BRIDGE_ARGS[@]}"
await bridge "[BRIDGE] subscribed" 60

# muninn <edge> <device> <ref> <version> <ns> <node> <instanceId>: one Muninn session for one device.
muninn() {
  start "muninn-$1" java "${JLOG[@]}" -Xmx256m -jar "$MUNINN_JAR" feed "$DEFINITIONS" "$3" "$4" "$OPCUA" "$5" "$6" "$MQTT" "$GROUP" "$1" \
    --hierarchy "$REGISTRY/mappings" "$STREAM" "$7" --session "$MUNINN_MS" --primary-host "host-$1"
}
host_state() { java "${JLOG[@]}" -jar "$MUNINN_JAR" host "$MQTT" state "host-$1" "$2" "$(now_ms)" >> "$OUT/logs/host-state.log" 2>&1; }

step muninn
for e in "${EDGES[@]}"; do
  IFS=$'\t' read -r _ edge device ref version ns node instance <<< "$e"
  muninn "$edge" "$device" "$ref" "$version" "$ns" "$node" "$instance"
done
for e in "${EDGES[@]}"; do IFS=$'\t' read -r _ edge _ <<< "$e"; await "muninn-$edge" "waiting for host host-$edge" 60; done
for e in "${EDGES[@]}"; do IFS=$'\t' read -r _ edge _ <<< "$e"; host_state "$edge" true || fail "host state of host-$edge"; done
for e in "${EDGES[@]}"; do IFS=$'\t' read -r _ edge device _ <<< "$e"; await bridge "[BRIDGE] session session-up $edge $device" 60; done

step preflight
PF_EDGES=()
for e in "${EDGES[@]}"; do IFS=$'\t' read -r _ edge device _ <<< "$e"; PF_EDGES+=(--edge "$edge=$device"); done
java "${JLOG[@]}" -jar "$LOOP" preflight --kafka "$KAFKA" --prefix "$PREFIX" --registry "$REGISTRY" --definitions "$DEFINITIONS" --stream "$STREAM" \
  --routing "$ROUTING" --pins "$OUT/pins.json" --muninn-period-ms "$MUNINN_MS" --scan-ms "$SCAN_MS" --opcua "$OPCUA" "${PF_EDGES[@]}" \
  --bridge-client-id "$BRIDGE_ID" --driver-client-id "$DRIVER_ID" > "$OUT/preflight.txt" 2> "$OUT/logs/preflight.log" \
  || { cat "$OUT/preflight.txt" >&2; fail "preflight found problems"; }

step drive
# The declared Muninn and host injections run beside the drive, timed from its start.
injections() {
  local t0="$1" spec edge at down
  if [ -n "$KILL_MUNINN" ]; then
    spec="${KILL_MUNINN#*@}"; at="${spec%%+*}"; down="${spec##*+}"; edge="$(edge_of "${KILL_MUNINN%%@*}")"
    sleep_until "$t0" "$at"
    echo "$(now_ms) kill -9 muninn-$edge" >> "$OUT/injections.log"
    kill -9 "$(pid_of "muninn-$edge")"
    sleep_until "$t0" "$((at + down))"
    mv "$OUT/logs/muninn-$edge.log" "$OUT/logs/muninn-$edge.before-kill.log"
    IFS=$'\t' read -r _ _ device ref version ns node instance < <(printf '%s\n' "${EDGES[@]}" | awk -F'\t' -v e="$edge" '$2 == e')
    muninn "$edge" "$device" "$ref" "$version" "$ns" "$node" "$instance"
    echo "$(now_ms) restarted muninn-$edge" >> "$OUT/injections.log"
  fi
  if [ -n "$HOST_OFFLINE" ]; then
    spec="${HOST_OFFLINE#*@}"; at="${spec%%+*}"; down="${spec##*+}"; edge="$(edge_of "${HOST_OFFLINE%%@*}")"
    sleep_until "$t0" "$at"
    host_state "$edge" false; echo "$(now_ms) host-$edge offline" >> "$OUT/injections.log"
    sleep_until "$t0" "$((at + down))"
    host_state "$edge" true; echo "$(now_ms) host-$edge online" >> "$OUT/injections.log"
  fi
}
# A background sleep and wait: a signal to the runner interrupts the wait at once (a foreground sleep would delay the trap).
sleep_until() {
  local wait=$(( $1 + $2 - $(now_ms) ))
  [ "$wait" -le 0 ] && return 0
  sleep "$(awk -v w="$wait" 'BEGIN {printf "%.3f", w / 1000}')" &
  wait "$!"
}
DRIVE_T0="$(now_ms)"
if [ -n "$KILL_MUNINN$HOST_OFFLINE" ]; then
  injections "$DRIVE_T0" > "$OUT/logs/injections.log" 2>&1 &
  echo "$!" > "$OUT/pids/injections"
fi
java "${JLOG[@]}" -Xmx512m -jar "$LOOP" drive --kafka "$KAFKA" --prefix "$PREFIX" --registry "$REGISTRY" --definitions "$DEFINITIONS" --stream "$STREAM" \
  --routing "$ROUTING" --scenario "$SCENARIO" --opcua "$OPCUA" --driver-client-id "$DRIVER_ID" --log "$OUT/drive.jsonl" > "$OUT/logs/drive.log" 2>&1 \
  || { tail -5 "$OUT/logs/drive.log" >&2; fail "drive"; }
if [ -f "$OUT/pids/injections" ]; then
  wait "$(pid_of injections)" || fail "an injection failed (see $OUT/logs/injections.log)"
  rm -f "$OUT/pids/injections"
fi
for e in "${EDGES[@]}"; do IFS=$'\t' read -r _ edge _ <<< "$e"; alive "muninn-$edge" || fail "muninn-$edge is not running after the drive"; done

step tail
# The capture needs tailMs (L + O + P of the rule) of telemetry after its last business event on every device and partition; the
# drive's last event is no later than its end.
echo "tail: $TAIL_MS ms + margin $TAIL_MARGIN_MS ms"
sleep_until "$(now_ms)" "$((TAIL_MS + TAIL_MARGIN_MS))"
alive bridge || fail "the bridge exited during the tail"

step stop-bridge
touch "$OUT/bridge.stop"
for ((i = 0; i < 600; i++)); do alive bridge || break; sleep 0.25; done
alive bridge && fail "the bridge did not stop within 150 s"
wait "$(pid_of bridge)" || fail "the bridge exited non-zero (see $OUT/bridge-run.json)"
[ "$(json "$OUT/bridge-run.json" 'd["outcome"]')" = stopped ] || fail "the bridge run record is not 'stopped'"
if [ -n "$HOST_OFFLINE" ]; then
  [ "$(json "$OUT/bridge-run.json" '[s["stage"] for s in d["inputStages"] if s["completed"]] == ["death-after-birth"]')" = True ] \
    || fail "injection 25(a) did not run: the death-after-birth stage did not complete (no newer birth after the held death; see $OUT/bridge-run.json)"
fi
rm -f "$OUT/pids/bridge"

step stop-publishers
for e in "${EDGES[@]}"; do IFS=$'\t' read -r _ edge _ <<< "$e"; stop "muninn-$edge" 10; rm -f "$OUT/pids/muninn-$edge"; done
stop sim 10; rm -f "$OUT/pids/sim"

step record
# The run's injections, declared in the manifest (source.injections) beside the scenario's bad statuses: recordings compare judges
# the visits by them (every visit complete except those an injection touches, with its reason).
INJECTIONS=()
[ -z "$BRIDGE_HOLD" ] || INJECTIONS+=(--injection "bridge-hold=$BRIDGE_HOLD")
[ -z "$BRIDGE_OFFSET" ] || INJECTIONS+=(--injection "bridge-source-offset=$BRIDGE_OFFSET")
[ -z "$KILL_MUNINN" ] || INJECTIONS+=(--injection "kill-muninn=$KILL_MUNINN")
[ -z "$HOST_OFFLINE" ] || INJECTIONS+=(--injection "host-offline=$HOST_OFFLINE")
java "${JLOG[@]}" -Xmx2g -jar "$LOOP" record --kafka "$KAFKA" --prefix "$PREFIX" --registry "$REGISTRY" --definitions "$DEFINITIONS" --stream "$STREAM" \
  --routing "$ROUTING" --scenario "$SCENARIO" --drive-log "$OUT/drive.jsonl" --out "$REC" --name "$NAME" \
  --report "$OUT/recording.capture-report.json" --dead-letter "$OUT/recording.dead-letter.jsonl" --client-id "forseti-recorder-$RUN_ID" "${INJECTIONS[@]}" \
  > "$OUT/logs/record.log" 2>&1 || { tail -5 "$OUT/logs/record.log" >&2; fail "the recorder refused the capture"; }
grep -v '^[0-9][0-9]:' "$OUT/logs/record.log" | tail -1

step verify-capture
java -jar "$FORSETI" recordings verify-capture "$REC" --lock "$OUT/recording.capture-lock.json" --registry "$REGISTRY" --write-lock > "$OUT/logs/verify-capture.log" 2>&1 \
  || { cat "$OUT/logs/verify-capture.log" >&2; fail "verify-capture --write-lock"; }
java -jar "$FORSETI" recordings verify-capture "$REC" --lock "$OUT/recording.capture-lock.json" --registry "$REGISTRY" >> "$OUT/logs/verify-capture.log" 2>&1 \
  || { cat "$OUT/logs/verify-capture.log" >&2; fail "verify-capture --lock"; }
tail -1 "$OUT/logs/verify-capture.log"

step core
RULE_ID="$(json "$REC/manifest.json" 'd["rule"]["attributionRuleId"] + "@" + d["rule"]["attributionRuleVersion"]')"
RELEASE="$(json "$REC/manifest.json" 'd["rule"]["releaseId"] + "@" + d["rule"]["version"]')"
java -jar "$FORSETI" generation new --state "$OUT/state" --mappings "$REGISTRY/mappings" --trusted-keys fixtures/live/trusted-keys.jsonl \
  --stream "$STREAM" --rule "$RULE_ID" --recording "$REC" > "$OUT/generation.json" 2> "$OUT/logs/generation.log" \
  || { cat "$OUT/generation.json" "$OUT/logs/generation.log" >&2; fail "generation new"; }
GENERATION="$(json "$OUT/generation.json" 'd["generation"]')"
java -Xmx2g -jar "$FORSETI" run --engine core --recording "$REC" --registry "$REGISTRY" --state "$OUT/state" --generation "$GENERATION" --out "$OUT/core" \
  > "$OUT/core-run.json" 2> "$OUT/logs/core.log" || { cat "$OUT/core-run.json" "$OUT/logs/core.log" >&2; fail "forseti run --engine core"; }
cat "$OUT/core-run.json"

step oracle
SUBSCRIBE=()
while IFS= read -r s; do SUBSCRIBE+=(--subscribe "$s"); done < <(json "$OUT/core/run.json" '"\n".join(t + "=" + ",".join(str(p) for p in ps) for t, ps in d["subscribed"].items())')
"$PYTHON" -I -m forseti_oracle.cli all --recording "$REC" --cuts "$OUT/core/cuts.json" --mappings "$REGISTRY/mappings" --release "$RELEASE" --rule "$RULE_ID" \
  --definitions "$REC/manifest.json" --out "$OUT/oracle" "${SUBSCRIBE[@]}" --check-journals "$OUT/core" > "$OUT/logs/oracle.log" 2>&1 \
  || { tail -5 "$OUT/logs/oracle.log" >&2; fail "the oracle on the capture"; }
cat "$OUT/logs/oracle.log"

step compare
java -Xmx2g -jar "$FORSETI" recordings compare "$REC" --registry "$REGISTRY" --trusted-keys fixtures/live/trusted-keys.jsonl --python "$PYTHON" \
  --out "$OUT/compare" > "$OUT/logs/compare.log" 2>&1 \
  || { tail -40 "$OUT/logs/compare.log" >&2; fail "recordings compare: the core and the oracle differ, a check failed, or the visits are not the ones the injections explain"; }
cp "$OUT/compare/compare-report.json" "$OUT/recording.compare-report.json"
json "$OUT/recording.compare-report.json" '"compare: " + d["outcome"] + ", accepted " + str(d["accepted"]) + ", " + str(d["cutsCompared"]) + " cuts, " + str(d["rowsCompared"]) + " rows, " + str(d["differences"]) + " differences"'

step done
echo "full loop passed: $OUT"
