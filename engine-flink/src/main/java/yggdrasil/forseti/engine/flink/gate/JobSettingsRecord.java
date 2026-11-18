package yggdrasil.forseti.engine.flink.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import yggdrasil.forseti.core.Json;

/** The run record's {@code jobSettings} (spec 590; reviewer decision 1 of 2026-11-18): one engine-neutral object per stage plus the
 *  gate's {@code read_committed} dump consumer, beside {@code pins}, with run-invariant values only (topic prefixes, client and group ids,
 *  transactional prefixes and checkpoint directories stay in {@code evidence.json}). Each stage's values are taken from what was
 *  observed: the checkpoint configuration, the JobManager's effective configuration and every vertex's parallelism as the cluster
 *  reports them over REST, the keyed parallelism and max parallelism from the stage's operator init records ({@code TaskInfo} at run
 *  time), the source and sink client settings from the job's own record of the properties it built its clients with (the run-invariant
 *  keys only). {@code FlinkEngine} asserts them against the engine pins. */
final class JobSettingsRecord {
  private JobSettingsRecord() {}

  static final List<String> SOURCE_KEYS = List.of("isolation.level", "enable.auto.commit", "commit.offsets.on.checkpoint", "partition.discovery.interval.ms");
  static final List<String> SINK_KEYS = List.of("deliveryGuarantee", "acks", "enable.idempotence", "transaction.timeout.ms");
  static final List<String> DUMP_KEYS = List.of("isolation.level", "enable.auto.commit", "auto.offset.reset", "max.poll.records");

  /** {@code observed}: per stage ({@code stage1}, {@code stage2}) {@link FlinkCluster#observedSettings}; {@code jobs}: the job evidence
   *  files; {@code dump}: the dump consumer's configuration. */
  static ObjectNode of(ObjectNode observed, ObjectNode jobs, ObjectNode dump) {
    ObjectNode n = Json.object();
    for (int stage : List.of(1, 2)) {
      String k = "stage" + stage;
      JsonNode o = observed.path(k), pins = jobs.path(k + "-pins");
      ObjectNode s = n.putObject(k);
      s.put("engine", pins.path("engine").asText(null)).put("engineVersion", pins.path("engineVersion").asText(null));
      ObjectNode cp = s.putObject("checkpointing");
      JsonNode cc = o.path("checkpointConfig");
      cp.put("mode", cc.path("mode").asText(null));
      if (cc.path("interval").isNumber()) Json.putSafe(cp, "intervalMs", cc.path("interval").asLong()); else cp.putNull("intervalMs");
      if (cc.path("max_concurrent").isNumber()) Json.putSafe(cp, "maxConcurrent", cc.path("max_concurrent").asLong()); else cp.putNull("maxConcurrent");
      cp.put("stateBackend", cc.path("state_backend").asText(null)).put("checkpointStorage", cc.path("checkpoint_storage").asText(null));
      JsonNode conf = o.path("configuration");
      cp.put("incremental", conf.path("execution.checkpointing.incremental").asText(null)).put("stateBackendType", conf.path("state.backend.type").asText(null));
      s.put("autoWatermarkInterval", conf.path("pipeline.auto-watermark-interval").asText(null));
      s.put("restartStrategy", conf.path("restart-strategy.type").asText(null));
      s.put("configuredMaxParallelism", conf.path("pipeline.max-parallelism").asText(null));
      // watermarkEmission, idleness and timestampSource are job constants (the jobs are built with the per-record delegating generator,
      // no idleness and the payload timestamp source), backed at run time by the stage-1 operator failing on an idle telemetry status
      s.put("watermarkEmission", stage == 1 ? pins.path("watermarkEmission").asText(null) : "none (stage 2 uses no engine watermark)");
      s.put("idleness", pins.path("idleness").asText(null));
      s.put("timestampSource", "payload");
      // the jobs' own readers of the control store (spec 708): forseti.generations in each job's main, forseti.recovery-control in
      // every stage-1 writer's initialization
      ObjectNode readers = s.putObject("readers");
      if (pins.path("generationsReader").isObject()) readers.set("generations", filtered(pins.get("generationsReader"), DUMP_KEYS));
      if (stage == 1) {
        JsonNode cr = null;
        for (Iterator<Map.Entry<String, JsonNode>> it = jobs.fields(); it.hasNext();) {
          Map.Entry<String, JsonNode> e = it.next();
          if (e.getKey().startsWith("stage1-stations-") && e.getValue().path("recoveryControlReader").isObject()) cr = e.getValue().get("recoveryControlReader");
        }
        if (cr != null) readers.set("recoveryControl", filtered(cr, DUMP_KEYS));
      }
      // keyed parallelism and max parallelism as the station operators saw them at run time
      Set<Long> par = new TreeSet<>(), max = new TreeSet<>();
      jobs.fields().forEachRemaining(e -> {
        if (!e.getKey().startsWith(k + "-stations-")) return;
        par.add(e.getValue().path("parallelism").asLong());
        max.add(e.getValue().path("maxParallelism").asLong());
      });
      if (par.size() == 1) Json.putSafe(s, "keyedParallelism", par.iterator().next()); else s.put("keyedParallelism", "inconsistent " + par);
      if (max.size() == 1) Json.putSafe(s, "maxParallelism", max.iterator().next()); else s.put("maxParallelism", "inconsistent " + max);
      ObjectNode sp = s.putObject("sourceParallelism"), src = s.putObject("sources");
      pins.path("sources").fields().forEachRemaining(e -> {
        String topic = e.getKey();
        Long p = null;
        for (Iterator<Map.Entry<String, JsonNode>> it = o.path("vertices").fields(); it.hasNext();) {
          Map.Entry<String, JsonNode> v = it.next();
          if (v.getKey().contains(k + "-source-" + topic)) p = v.getValue().asLong();
        }
        if (p != null) Json.putSafe(sp, topic, p); else sp.putNull(topic);
        ObjectNode x = src.putObject(topic);
        for (String key : SOURCE_KEYS) x.put(key, e.getValue().path(key).asText(null));
      });
      ObjectNode sinks = s.putObject("sinks");
      pins.path("sinks").fields().forEachRemaining(e -> {
        ObjectNode x = sinks.putObject(e.getKey());
        for (String key : SINK_KEYS) x.put(key, e.getValue().path(key).asText(null));
      });
      ObjectNode vp = s.putObject("vertexParallelism");
      o.path("vertices").fields().forEachRemaining(e -> vp.set(e.getKey().replaceAll("[^A-Za-z0-9 >:._-]", "_"), e.getValue()));
    }
    ObjectNode d = n.putObject("dumpConsumer");
    for (String key : DUMP_KEYS) d.put(key, dump.path(key).asText(null));
    return n;
  }

  static ObjectNode filtered(JsonNode n, List<String> keys) {
    ObjectNode x = Json.object();
    for (String k : keys) x.put(k, n.path(k).asText(null));
    return x;
  }

  /** {@code record} with {@code jobSettings} placed right after {@code pins}. */
  static ObjectNode insert(ObjectNode record, ObjectNode jobSettings) {
    ObjectNode n = Json.object();
    record.fields().forEachRemaining(e -> {
      n.set(e.getKey(), e.getValue());
      if (e.getKey().equals("pins")) n.set("jobSettings", jobSettings);
    });
    if (!n.has("jobSettings")) throw new IllegalStateException("a run record without pins");
    return n;
  }
}
