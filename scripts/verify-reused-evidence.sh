#!/usr/bin/env bash
# Re-verifies a main run's core-gate evidence for a pull request whose core-input key equals that run's (owner decision of 2026-11-14).
#
#   scripts/verify-reused-evidence.sh <evidence dir> <python>
#
# <evidence dir> holds what CI downloaded from the producer run: jar/forseti.jar, shards/<shard>/ (the gate shard artifacts),
# controls/<control>/<shard>/ (the control artifacts). FORSETI_INPUT_KEY must hold the key computed on this checkout. The script runs
# no harness and no oracle and promotes nothing. It passes only when:
#   - every shard report and every control report carries this checkout's key and the producer jar's SHA-256 in its pins;
#   - the aggregate over the producer's shard outputs, run here with the producer's jar, prints `assertions run N / expected N` and exits 0;
#   - every (control, shard) run of gate/core-negative-controls.json passes scripts/check-control-report.py with its recorded exit;
#   - every control with an intended aggregate id passes its aggregate control run, run here over the producer's control outputs.
# It writes <evidence dir>/out/ (the aggregate and control-aggregate reports) and prints one claim line at the end.
set -euo pipefail
[ $# -eq 2 ] || { echo "usage: $0 <evidence dir> <python>" >&2; exit 2; }
EV="$1"; PY="$2"
cd "$(dirname "$0")/.."
fail() { echo "verify-reused-evidence: $*" >&2; exit 1; }
[ -n "${FORSETI_INPUT_KEY:-}" ] || fail "FORSETI_INPUT_KEY is not set"
JAR="$EV/jar/forseti.jar"
[ -f "$JAR" ] || fail "no producer jar at $JAR"
OUT="$EV/out"
rm -rf "$OUT"; mkdir -p "$OUT"

# 1. Identity: this checkout's key and the producer jar in every report.
"$PY" -I - "$EV" "$JAR" "$FORSETI_INPUT_KEY" <<'PY'
import hashlib, json, pathlib, sys
ev, jar, key = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
sha = hashlib.sha256(open(jar, "rb").read()).hexdigest()
shards = [s["name"] for s in json.load(open("gate/core-shards.json", encoding="utf-8"))["shards"]]
runs = [(c["name"], s) for c in json.load(open("gate/core-negative-controls.json", encoding="utf-8"))["controls"] for s in c["shards"]]
reports = [ev / "shards" / s / "report.json" for s in shards] + [ev / "controls" / c / s / "report.json" for c, s in runs]
problems = []
for r in reports:
    if not r.is_file():
        problems.append(f"missing {r.relative_to(ev)}")
        continue
    f = json.load(open(r, encoding="utf-8")).get("pins", {}).get("forseti", {})
    if f.get("inputKey") != key:
        problems.append(f"{r.relative_to(ev)}: inputKey {f.get('inputKey')}, this checkout's {key}")
    if f.get("artifactSha256") != sha:
        problems.append(f"{r.relative_to(ev)}: artifactSha256 {f.get('artifactSha256')}, the producer jar's {sha}")
for p in problems:
    print(p)
print(f"identity: {len(reports)} reports, key {key}, jar {sha}: " + ("ok" if not problems else f"{len(problems)} problems"))
sys.exit(1 if problems else 0)
PY

# 2. The aggregate over the producer's shard outputs.
N=$("$PY" -I -c 'import json;print(len(json.load(open("gate/core-assertions.json"))["assertions"]))')
set +e
java -jar "$JAR" gate --engine core --aggregate "$EV/shards" --out "$OUT/gate" --python "$PY" | tee "$OUT/gate.log"
rc=${PIPESTATUS[0]}
set -e
[ "$rc" -eq 0 ] || fail "the aggregate exited $rc"
[ "$(tail -1 "$OUT/gate.log" | tr -d '\r')" = "assertions run $N / expected $N" ] || fail "the aggregate's last line is not 'assertions run $N / expected $N'"

# 3. Every control run, as the control jobs check it, with the exit code its report recorded.
"$PY" -I -c 'import json
for c in json.load(open("gate/core-negative-controls.json"))["controls"]:
    for s in c["shards"]: print(c["name"], s)' | tr -d '\r' | while read -r c s; do
  r="$EV/controls/$c/$s/report.json"
  e=$("$PY" -I -c 'import json,sys;print(json.load(open(sys.argv[1],encoding="utf-8"))["exit"])' "$r" | tr -d '\r')
  "$PY" -I scripts/check-control-report.py "$r" "$e" "$c" "$s"
done

# 4. The aggregate control runs, run here over the producer's control outputs.
for c in $("$PY" -I -c 'import json
agg=set(json.load(open("gate/core-shards.json"))["aggregate"])
print(" ".join(x["name"] for x in json.load(open("gate/core-negative-controls.json"))["controls"] if agg & set(x["intended"])))' | tr -d '\r'); do
  set +e
  java -jar "$JAR" gate --engine core --aggregate "$EV/controls/$c" --negative-control "$c" --out "$OUT/control-aggregate/$c" --python "$PY"
  rc=$?
  set -e
  "$PY" -I scripts/check-control-report.py "$OUT/control-aggregate/$c/report.json" "$rc" "$c" aggregate
done
echo "core PASS, evidence reused: key $FORSETI_INPUT_KEY, producer ${FORSETI_PRODUCER:-unknown}"
