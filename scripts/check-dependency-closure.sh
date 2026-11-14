#!/usr/bin/env bash
# Zero shared code, per module: no Bifrost, Tyr, Vor, Muninn or other Yggdrasil artifact and no engine (Flink, Kafka Streams) on any
# Maven classpath of forseti. Transitive dependencies are checked, not only imports.
#
# core and cli keep the full ban: no Kafka at all and no transport client (Tahu, Paho, Milo), because the core gate runs from the cli
# jar and Decision 12 keeps engines and transports out of it. bridge and loop are the transport shells of step 5 (plan D1, D2): they
# may carry exactly the pinned client lines - kafka-clients 4.2.2, tahu-core 1.0.14, the Paho MQTT v3 client 1.2.5, and (loop only,
# which drives the simulator over OPC UA) Eclipse Milo 1.1.8. Any other artifact of those groups, or another version, is refused, so
# a client upgrade or a stray Kafka server artifact is a reviewed change of this file and not a silent drift.
set -euo pipefail
cd "$(dirname "$0")/.."
MODULES=(core cli bridge loop)
# One tree file per module: a relative outputFile is resolved against each module's own directory. (appendOutput into one absolute
# file is not used: the dependency plugin 3.7.0 overwrites it per module, which left only the last module's tree to be checked.)
for m in "${MODULES[@]}"; do rm -f "$m/target/deps.txt"; done
mvn -B -ntp -q dependency:tree -DoutputFile=target/deps.txt
OUT=""
for m in "${MODULES[@]}"; do
  if [ ! -f "$m/target/deps.txt" ] || ! grep -q "^yggdrasil.forseti:forseti-$m:" "$m/target/deps.txt"; then
    echo "dependency closure: incomplete tree, no forseti-$m ($m/target/deps.txt)" >&2
    exit 1
  fi
  OUT="$OUT $m/target/deps.txt"
done
# Each tree file holds a root line (groupId:artifactId:packaging:version) and its dependencies below it, each prefixed with tree
# characters. A dependency is groupId:artifactId:type[:classifier]:version:scope.
BANNED="$(awk '
  /^[^|+\\ ]/ { split($0, r, ":"); module = r[2]; sub(/^forseti-/, "", module); next }
  {
    dep = $0; sub(/^[|+\\ -]+/, "", dep); sub(/[ (].*$/, "", dep)
    n = split(dep, c, ":"); g = c[1]; a = c[2]; v = (n >= 6 ? c[5] : c[4])
    why = ""
    if (g ~ /^yggdrasil(\.|$)/ && g != "yggdrasil.forseti") why = "other Yggdrasil code"
    else if (g ~ /^org\.apache\.flink(\.|$)/) why = "engine (Flink)"
    else if (g == "org.apache.kafka" && a ~ /^kafka-streams/) why = "engine (Kafka Streams)"
    else if (g ~ /^org\.apache\.kafka(\.|$)|^org\.eclipse\.(tahu|paho|milo)(\.|$)/) {
      transport = (module == "bridge" || module == "loop")
      if (!transport) why = "transport client outside bridge and loop"
      else if (g == "org.apache.kafka") { if (a != "kafka-clients" || v != "4.2.2") why = "not kafka-clients 4.2.2" }
      else if (g == "org.eclipse.tahu") { if (a != "tahu-core" || v != "1.0.14") why = "not tahu-core 1.0.14" }
      else if (g == "org.eclipse.paho") { if (a != "org.eclipse.paho.client.mqttv3" || v != "1.2.5") why = "not paho mqttv3 1.2.5" }
      else if (module != "loop") why = "Milo outside loop"
      else if (v != "1.1.8") why = "not Milo 1.1.8"
    }
    if (why != "") printf "  %s: %s (%s)\n", module, dep, why
  }
' $OUT)"
if [ -n "$BANNED" ]; then
  echo "$BANNED"
  echo "dependency closure: banned artifact on the classpath (see the lines above)" >&2
  exit 1
fi
echo "dependency closure: clean (core, cli, bridge, loop)"
