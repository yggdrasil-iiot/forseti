#!/usr/bin/env bash
# Executable gate of step-3 PR 0 (plan docs/superpowers/plans/2026-11-10-forseti-step3-core-and-oracle.md,
# Task 1, Step 1.15). It proves that the step-3 pins and interpretations are written into the spec and
# that nothing else changed:
#   (a) the changed-file set is exactly the spec and this script;
#   (b) each of the seventeen decision phrases occurs in the spec exactly once;
#   (c) the unpublished JCS coordinate `org.erdtman:java-json-canonicalization` is gone;
#   (d) the two refusal-code tables and the ledger reason vocabulary are byte-identical to main.
#
# Usage: bash scripts/check-spec-decisions.sh --working-tree   (before committing)
#        bash scripts/check-spec-decisions.sh --committed      (after committing; what CI and review run)
set -euo pipefail
cd "$(dirname "$0")/.."

SPEC="docs/superpowers/specs/2026-11-05-forseti-design.md"
SELF="scripts/check-spec-decisions.sh"
BASE="${SPEC_DECISIONS_BASE:-main}"
MODE="${1:-}"

fail=0
note() { echo "spec decisions: $*" >&2; }

# (a) changed-file set
case "$MODE" in
  --working-tree)
    changed=$( { git diff --name-only "$BASE"; git ls-files --others --exclude-standard; } | sort -u ) ;;
  --committed)
    changed=$(git diff --name-only "$BASE...HEAD" | sort -u) ;;
  *)
    echo "usage: $0 --working-tree | --committed" >&2; exit 2 ;;
esac
expected=$(printf '%s\n%s\n' "$SPEC" "$SELF" | sort -u)
if [ "$changed" != "$expected" ]; then
  note "changed files differ from {$SPEC, $SELF}:"
  printf '  %s\n' $changed >&2
  fail=1
fi

# The spec as LF text, so the counts and hashes do not depend on a checkout's line endings.
spec_text=$(tr -d '\r' < "$SPEC")

# (b) seventeen phrases, each exactly once
phrases=(
  'io.github.erdtman:java-json-canonicalization:1.1'
  'rfc8785==0.1.4'
  'flink-connector-kafka:5.0.0-2.2'
  'Kafka Streams `4.2.2`'
  'JEP 339'
  'forseti gate --engine core'
  'per-station formula'
  'generation promote --run'
  '"kind": "replayed"'
  'last valid observation before `detectedEventTime`'
  'retracted candidate whose actual'
  'refs.check'
  'declarationOffset'
  'head-unverified'
  'over the `equipmentEvidence[]` entries'"'"' `coverage`'
  'no business record of that partition is included yet'
  'a superseded lower revision still widens the row set'
)
present=0
for p in "${phrases[@]}"; do
  n=$(printf '%s\n' "$spec_text" | grep -c -F -- "$p" || true)
  if [ "$n" = "1" ]; then
    present=$((present + 1))
  else
    note "phrase found $n times (want 1): $p"
  fi
done
[ "$present" = "${#phrases[@]}" ] || fail=1

# (c) the unpublished coordinate is gone
old=$(printf '%s\n' "$spec_text" | grep -c -F 'org.erdtman:java-json-canonicalization' || true)
if [ "$old" != "0" ]; then
  note "org.erdtman:java-json-canonicalization still occurs $old times"
  fail=1
fi

# (d) contract blocks unchanged. Each block runs from its first line to the next blank line, inclusive.
# Expected SHA-256 values were recorded from main at 2e5be57 (spec unchanged since 384933d).
block() {  # $1 = awk pattern of the first line, $2 = which occurrence (1-based)
  printf '%s\n' "$spec_text" | awk -v want="$2" "/$1/{n++; if (n==want) f=1} f{print} f&&/^[[:space:]]*\$/{exit}"
}
check_block() {  # $1 = label, $2 = pattern, $3 = occurrence, $4 = expected sha256
  local got
  got=$(block "$2" "$3" | sha256sum | cut -c1-64)
  if [ "$got" != "$4" ]; then
    note "$1 changed (sha256 $got, want $4)"
    fail=1
    return 1
  fi
}
unchanged=1
check_block "first refusal-code table"  '^\| Refusal Code \|' 1 9b365a7e208b2120f4f5b3867634e5b1d3c510263ae95e2df8a98de21f3928b6 || unchanged=0
check_block "second refusal-code table" '^\| Refusal Code \|' 2 299e539a100e49734e1008223c826a940405d5e900fb32153554087ce4a31297 || unchanged=0
check_block "ledger reason vocabulary"  '^Reasons form a closed vocabulary:' 1 4bdb651a17263f86326671f14ddb36e78680f6bf741d7fda5e8dc10125e20d53 || unchanged=0

contracts=$([ "$unchanged" = "1" ] && echo "contracts unchanged" || echo "contracts CHANGED")
echo "spec decisions: $present/${#phrases[@]} present, $contracts"
exit "$fail"
