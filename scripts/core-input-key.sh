#!/usr/bin/env bash
# The core-input key of the checked-out commit: one SHA-256 over the git tree (or blob) id of every path the core gate's verdict
# depends on (the Java modules, the oracle, the fixtures, schemas, vectors, gate plan files, scripts, workflows, the root POM and the
# line-ending rules). CI passes it to every gate run as FORSETI_INPUT_KEY, so each report records it in pins.forseti.inputKey and the
# aggregate refuses shards with another key. A path that does not exist contributes "absent". It refuses a working tree whose
# tracked files under those paths differ from the commit, since the key then names bytes that are not the ones on disk.
set -euo pipefail
cd "$(dirname "$0")/.."
PATHS=(core cli oracle fixtures schemas vectors gate scripts .github/workflows pom.xml .gitattributes)
git diff --quiet HEAD -- "${PATHS[@]}" || { echo "core-input-key: tracked files differ from HEAD" >&2; exit 1; }
for p in "${PATHS[@]}"; do
  printf '%s %s\n' "$p" "$(git rev-parse --verify --quiet "HEAD:$p" || echo absent)"
done | sha256sum | cut -c1-64
