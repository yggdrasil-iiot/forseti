#!/usr/bin/env bash
# Zero shared code: no Bifrost, Tyr, Vor or Muninn artifact and no engine (Flink, Kafka) on any Maven classpath of
# forseti. Transitive dependencies are checked, not only imports.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p build
OUT="$(pwd)/build/deps.txt"   # absolute: a relative outputFile is resolved per module
rm -f "$OUT"
mvn -B -ntp -q dependency:tree -DoutputFile="$OUT" -DappendOutput=true
if [ ! -f "$OUT" ] || ! grep -q "yggdrasil.forseti:forseti-core:" "$OUT" || ! grep -q "yggdrasil.forseti:forseti-cli:" "$OUT"; then
  echo "dependency closure: incomplete tree ($OUT)" >&2
  exit 1
fi
if grep -E "yggdrasil\.(bifrost|tyr|vor|muninn)|org\.apache\.flink|org\.apache\.kafka" "$OUT"; then
  echo "dependency closure: banned artifact on the classpath (see the lines above)" >&2
  exit 1
fi
echo "dependency closure: clean"
