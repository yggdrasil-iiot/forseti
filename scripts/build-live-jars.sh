#!/usr/bin/env bash
# Builds the two upstream jars of the full loop from the commits live/stack-lock.json locks, and checks them against the lock:
#   scripts/build-live-jars.sh <bifrost checkout> <muninn checkout> [<out dir, default build/live-jars>]
# Each checkout must be at its locked commit with a clean tree (the script refuses otherwise; it never switches branches). The build is
# reproducible: project.build.outputTimestamp is the commit's own time, so the same commit and JDK give the same jar bytes on every
# host, and the lock can pin the jar's SHA-256. Extra Maven arguments (e.g. -Dmaven.repo.local=...) come from MVN_ARGS.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
[ $# -ge 2 ] || { echo "usage: $0 <bifrost checkout> <muninn checkout> [<out dir>]" >&2; exit 2; }
BIFROST="$1"; MUNINN="$2"; OUT="${3:-$ROOT/build/live-jars}"
LOCK="$ROOT/live/stack-lock.json"
mkdir -p "$OUT"

field() {   # field <jar name> <field>: one string field of a locked jar
  sed -n "/\"$1\": {/,/}/p" "$LOCK" | sed -n "s/^ *\"$2\": \"\(.*\)\",\{0,1\}$/\1/p"
}

build() {   # build <jar name> <checkout>
  local name="$1" dir="$2" commit module path head ts
  commit="$(field "$name" commit)"; module="$(field "$name" module)"; path="$(field "$name" path)"
  [ -n "$commit" ] && [ -n "$module" ] && [ -n "$path" ] || { echo "build-live-jars: $name is not in $LOCK" >&2; exit 1; }
  head="$(git -C "$dir" rev-parse HEAD)"
  [ "$head" = "$commit" ] || { echo "build-live-jars: $dir is at $head, the lock names $commit for $name" >&2; exit 1; }
  git -C "$dir" diff --quiet HEAD || { echo "build-live-jars: $dir has local changes" >&2; exit 1; }
  ts="$(git -C "$dir" log -1 --format=%ct)"
  local pl=()
  [ "$module" = "." ] || pl=(-pl "$module" -am)
  # shellcheck disable=SC2086
  (cd "$dir" && mvn -B -ntp -q ${MVN_ARGS:-} -Dproject.build.outputTimestamp="$ts" -DskipTests "${pl[@]}" clean package)
  cp "$dir/$path" "$OUT/$name.jar"
  echo "$name $(sha256sum "$OUT/$name.jar" | cut -c1-64) (from $commit)"
}

build bifrost-sim "$BIFROST"
build muninn "$MUNINN"
java -jar "$ROOT/loop/target/forseti-loop.jar" stack-check --lock "$LOCK" --jar "bifrost-sim=$OUT/bifrost-sim.jar" --jar "muninn=$OUT/muninn.jar"
