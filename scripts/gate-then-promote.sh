#!/usr/bin/env bash
# Gate, then promote two generations through the CLI only (plan Task 21, Step 21.7).
#
#   scripts/gate-then-promote.sh <out> <python>                      full, unsharded; the store is <out>/state
#   scripts/gate-then-promote.sh --from-shards <dir> <out> <python>  aggregate over <dir>; each shard's store is <dir>/<shard>/state
#
# It registers nothing by hand. Any outcome other than the stated one exits 1.
set -euo pipefail

SHARDS=""
if [ "${1:-}" = "--from-shards" ]; then
  [ $# -eq 4 ] || { echo "usage: $0 --from-shards <dir> <out> <python>" >&2; exit 2; }
  SHARDS="$2"; OUT="$3"; PY="$4"
else
  [ $# -eq 2 ] || { echo "usage: $0 <out> <python> | $0 --from-shards <dir> <out> <python>" >&2; exit 2; }
  OUT="$1"; PY="$2"
fi
cd "$(dirname "$0")/.."
JAR=cli/target/forseti.jar
F="java -jar $JAR"
fail() { echo "gate-then-promote: $*" >&2; exit 1; }
sha() { "$PY" -I -c 'import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$1"; }

# 1. The gate.
N=$("$PY" -I -c 'import json;print(len(json.load(open("gate/core-assertions.json"))["assertions"]))')
mkdir -p "$OUT"
set +e
if [ -n "$SHARDS" ]; then
  $F gate --engine core --aggregate "$SHARDS" --out "$OUT" --python "$PY" | tee "$OUT/gate.log"
else
  $F gate --engine core --state "$OUT/state" --out "$OUT" --python "$PY" | tee "$OUT/gate.log"
fi
rc=${PIPESTATUS[0]}
set -e
[ "$rc" -eq 0 ] || fail "gate exited $rc"
[ "$(tail -1 "$OUT/gate.log" | tr -d '\r')" = "assertions run $N / expected $N" ] || fail "the gate's last line is not 'assertions run $N / expected $N'"
REPORT="$OUT/report.json"

# 2. BASE and REPLAY: the pipeline generations of horizon-planned-late, each with its one non-shuffled run.
pick() {
  "$PY" -I - "$REPORT" "$1" "$OUT" "$SHARDS" <<'PY'
import json, sys
report, variant, out, shards = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
r = json.load(open(report, encoding="utf-8"))
g = [x for x in r["generations"] if x["store"] == "pipeline" and x["variant"] == variant and x["state"] == "shared"
     and (variant != "online" or x["scenario"] == "horizon-planned-late")]
if variant == "event-time-replay":
    g = [x for x in g if x["scenario"] == "horizon-planned-late"]
if len(g) != 1:
    sys.exit(f"{variant}: {len(g)} generations match, expected 1")
g = g[0]
u = [x for x in r["runs"] if x["shard"] == g["shard"] and x["store"] == "pipeline"
     and x["registeredRowSha256"] == g["registeredRowSha256"] and not x["shuffle"]]
if len(u) != 1:
    sys.exit(f"{variant}: {len(u)} runs match, expected 1")
base = out if not shards else f"{shards}/{g['shard']}"
print(g["generation"], g["registeredRowSha256"], f"{base}/{u[0]['runRecord']}", f"{base}/state", g["shard"] or "-", f"{base}/{u[0]['journals']}")
PY
}
read -r G D R ST SH J <<< "$(pick online)" || fail "no single BASE"
read -r G2 D2 R2 ST2 SH2 J2 <<< "$(pick event-time-replay)" || fail "no single REPLAY"
[ "$SH" = "-" ] && SH=""
[ "$ST" = "$ST2" ] || fail "BASE and REPLAY live in different stores ($ST, $ST2)"
[ -f "$ST/generations.jsonl" ] || fail "no store at $ST"

P="$F generation promote --state $ST --mappings fixtures/registry/mappings --trusted-keys fixtures/trusted-keys.jsonl"
EXTRA=""
[ -n "$SHARDS" ] && EXTRA="--shards $SHARDS"

refused() {  # refused <code> <args...>: exit 1 with that refusal, the store byte-identical
  local code="$1"; shift
  local before; before=$(sha "$ST/generations.jsonl")
  set +e; local o; o=$($P "$@" 2>&1); local c=$?; set -e
  echo "$o"
  [ "$c" -eq 1 ] || fail "expected exit 1 ($code), got $c"
  echo "$o" | grep -q "\"refusal\":\"$code\"" || fail "expected refusal $code"
  [ "$(sha "$ST/generations.jsonl")" = "$before" ] || fail "a refused promote ($code) changed generations.jsonl"
}

# 3. Without --accept-scope: gate-report-scope.
refused gate-report-scope --generation "$G" --registered-row-sha256 "$D" --run "$R" --gate-report "$REPORT" $EXTRA

# 4. Shards mode: the report of the very shard that registered the generation is refused.
if [ -n "$SHARDS" ]; then
  refused gate-report-shard --generation "$G" --registered-row-sha256 "$D" --run "$R" --gate-report "$SHARDS/$SH/report.json" --accept-scope core
fi

promoted() {  # promoted <expected retired or null> <args...>
  local want="$1"; shift
  local o; o=$($P "$@") || fail "promote exited non-zero: $o"
  echo "$o"
  "$PY" -I - "$o" "$want" "$(sha "$REPORT")" <<'PY' || fail "unexpected promote output"
import json, sys
o, want, digest = json.loads(sys.argv[1]), sys.argv[2], sys.argv[3]
assert o["status"] == "current", o
assert o["gateReportSha256"] == digest, o
assert o["retired"] == (None if want == "null" else int(want)), o
PY
}

# 5. BASE, then 6. REPLAY, which retires BASE.
promoted null --generation "$G" --registered-row-sha256 "$D" --run "$R" --gate-report "$REPORT" --accept-scope core $EXTRA
promoted "$G" --generation "$G2" --registered-row-sha256 "$D2" --run "$R2" --gate-report "$REPORT" --accept-scope core $EXTRA

# 7. The lifecycle as the store records it.
$F generation list --state "$ST" > "$OUT/generations.list.jsonl" || fail "generation list failed"
"$PY" -I - "$OUT/generations.list.jsonl" "$G" "$G2" "$(sha "$R")" "$(sha "$R2")" "$ST" "$SHARDS" "$SH" <<'PY' || fail "the lifecycle does not hold"
import json, os, sys
listing, g, g2, run1, run2, st, shards, sh = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8]
rows = [json.loads(l) for l in open(listing, encoding="utf-8") if l.strip()]
rows = [r.get("value", r) for r in rows]
def statuses(n): return [r["status"] for r in rows if r["generation"] == n]
assert statuses(g) == ["registered", "replaying", "verified", "current", "retired"], (g, statuses(g))
assert statuses(g2) == ["registered", "replaying", "verified", "current"], (g2, statuses(g2))
for n, digest in ((g, run1), (g2, run2)):
    cited = [r["run"]["runRecordSha256"] for r in rows if r["generation"] == n and r["status"] in ("replaying", "verified", "current")]
    assert cited == [digest] * 3, (n, cited, digest)
for n in {r["generation"] for r in rows} - {g, g2}:
    assert statuses(n) == ["registered"], (n, statuses(n))
assert json.load(open(os.path.join(st, "head-floors", "line2.json"), encoding="utf-8"))["headVersion"] == 1
if shards:
    for name in sorted(os.listdir(shards)):
        if name == sh: continue
        p = os.path.join(shards, name, "state", "generations.jsonl")
        if not os.path.exists(p): continue
        for line in open(p, encoding="utf-8"):
            if line.strip():
                rec = json.loads(line)
                assert rec.get("value", rec)["status"] == "registered", (name, rec)
print("lifecycle holds: generation", g, "retired, generation", g2, "current")
PY

# 8. The base journals after both promotions: every file still has the digest of its index, and the index its run entry's.
"$PY" -I - "$REPORT" "$J" "$D" "$SH" <<'PY' || fail "the base journals changed"
import hashlib, json, os, sys
report, journals, digest, sh = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] or None
r = json.load(open(report, encoding="utf-8"))
run = [x for x in r["runs"] if x["shard"] == sh and x["store"] == "pipeline" and x["registeredRowSha256"] == digest and not x["shuffle"]][0]
index = journals.rstrip("/") + ".index.json"
def h(p):
    s = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""): s.update(b)
    return s.hexdigest()
assert h(index) == run["journalsIndexSha256"], "index digest"
listed = json.load(open(index, encoding="utf-8"))
present = {os.path.relpath(os.path.join(d, f), journals).replace(os.sep, "/") for d, _, fs in os.walk(journals) for f in fs}
assert present == set(listed), sorted(present ^ set(listed))[:5]
for rel, d in listed.items():
    assert h(os.path.join(journals, rel)) == d, rel
print("base journals unchanged:", len(listed), "files")
PY
echo "gate-then-promote: ok"
