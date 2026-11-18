package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.gate.Scenarios.Run;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.cli.replay.ReplayEngine;
import yggdrasil.forseti.core.Json;

/** The engine-independent per-run replay assertions (spec 708-711; reviewer decision 1 of 2026-11-18), recorded on every run of the
 *  replay gate and evaluated over every run summary ({@link ReplayGate}). Each returns the problems it found on one run:
 *  <ul><li>{@code R-READ-COMMITTED}: every consumer of the run read with {@code isolation.level=read_committed}: the harness's clients as
 *  configured, and every consumer of the run record's {@code jobSettings} (both stages' sources and the gate's dump consumer, observed);</li>
 *  <li>{@code R-JOB-SETTINGS}: the run record carries {@code jobSettings} beside {@code pins} with one object per stage and the dump
 *  consumer, holding no run-variant value (client or group id, transactional prefix, topic prefix, checkpoint directory, bootstrap); its
 *  {@code pins} name the run record's {@code runId} and {@code artifactSha256}; every row's envelope {@code pins.job.runId} and
 *  {@code pins.job.artifactSha256} equal the run record's (an output's own claims are never trusted);</li>
 *  <li>{@code R-TIMESTAMP-SOURCE}: {@code pins.job.timestampSource} is {@code payload} in the run record and on every row;</li>
 *  <li>{@code R-PARTITION-ASSIGNMENT}: the run's subscription is the routing manifest's, and every output record is on its writer's
 *  partition: station topics on the stations' partitions only and every one of them present, the ledgers on those and partition 6;</li>
 *  <li>{@code R-TOPIC-RETENTION}: every planned topic exists with its planned partitions, timestamp type and retention, as the broker
 *  reported them;</li>
 *  <li>{@code R-RECONCILE}: the shared identity-bound reconciliation of the ledgers and row triggers with the recording at every
 *  completed cut and at the end.</li></ul> */
final class ReplayChecks {
    private ReplayChecks() {}

    static final List<String> IDS = List.of("R-READ-COMMITTED", "R-JOB-SETTINGS", "R-TIMESTAMP-SOURCE", "R-PARTITION-ASSIGNMENT", "R-TOPIC-RETENTION", "R-RECONCILE");
    static final List<String> STATION_TOPICS = List.of("forseti.work-records", "forseti.samples", "forseti.affected", "forseti.affected-sets");
    static final List<String> LEDGERS = List.of("forseti.dispositions-stage1", "forseti.dispositions-stage2");
    /** Keys whose values differ between two runs of one recording: never in {@code jobSettings} (spec 590). */
    static final List<String> VARIANT_KEYS = List.of("client.id", "client.id.prefix", "group.id", "transactional.id", "transactionalIdPrefix", "bootstrap.servers",
        "state.checkpoints.dir", "execution.checkpointing.dir", "prefix", "topic", "checkpointDir", "runId");

    static Map<String, List<String>> findings(Run r, ReplayEngine.Run er, JsonNode runJson) throws IOException {
        Map<String, List<String>> f = new TreeMap<>();
        for (String id : IDS) f.put(id, new ArrayList<>());
        JsonNode evidence = er.evidence(), settings = runJson.path("jobSettings");
        // read_committed: the harness's clients as configured, the jobs' and the dump consumer's as observed
        evidence.path("clients").fields().forEachRemaining(e -> {
            if (e.getValue().has("isolation.level") && !"read_committed".equals(e.getValue().path("isolation.level").asText())) f.get("R-READ-COMMITTED").add("harness client " + e.getKey());
        });
        List<String> consumers = new ArrayList<>();
        walk(settings, "jobSettings", (path, n) -> {
            if (path.endsWith(".isolation.level")) { consumers.add(path); if (!"read_committed".equals(n.asText())) f.get("R-READ-COMMITTED").add(path + " = " + n.asText()); }
        });
        if (!settings.path("dumpConsumer").path("isolation.level").isTextual()) f.get("R-READ-COMMITTED").add("jobSettings.dumpConsumer has no isolation.level");
        for (String stage : List.of("stage1", "stage2"))
            if (consumers.stream().noneMatch(p -> p.startsWith("jobSettings." + stage + "."))) f.get("R-READ-COMMITTED").add("jobSettings." + stage + " records no consumer");

        // the job settings and the pins binding
        List<String> js = f.get("R-JOB-SETTINGS");
        if (!settings.isObject()) js.add("run.json has no jobSettings");
        for (String k : List.of("stage1", "stage2", "dumpConsumer")) if (!settings.path(k).isObject()) js.add("jobSettings." + k + " missing");
        walk(settings, "jobSettings", (path, n) -> { String last = path.substring(path.lastIndexOf('.') + 1); if (VARIANT_KEYS.contains(last)) js.add(path + " is run-variant"); });
        List<String> keys = new ArrayList<>();
        runJson.fieldNames().forEachRemaining(keys::add);
        if (keys.indexOf("jobSettings") != keys.indexOf("pins") + 1) js.add("jobSettings does not follow pins in run.json: " + keys);
        String runId = runJson.path("runId").asText(null), artifact = runJson.path("artifactSha256").asText(null);
        JsonNode job = runJson.path("pins").path("job");
        if (!Objects.equals(runId, job.path("runId").asText(null))) js.add("run.json pins.job.runId " + job.path("runId") + " is not its runId " + runId);
        if (!Objects.equals(artifact, job.path("artifactSha256").asText(null))) js.add("run.json pins.job.artifactSha256 " + job.path("artifactSha256") + " is not its artifactSha256 " + artifact);
        if (!"payload".equals(job.path("timestampSource").asText(null))) f.get("R-TIMESTAMP-SOURCE").add("run.json pins.job.timestampSource " + job.path("timestampSource"));
        long rows = scanRows(er.out(), runId, artifact, js, f.get("R-TIMESTAMP-SOURCE"));
        if (rows == 0) js.add("no row to check");

        // partition assignment: the subscription, then where every output record went
        Recording rec = Recording.open(r.recording);
        RoutingManifest routing = rec.routing();
        List<String> pa = f.get("R-PARTITION-ASSIGNMENT");
        runJson.path("subscribed").fields().forEachRemaining(e -> {
            List<Integer> have = new ArrayList<>(), want = routing.subscribed(e.getKey());
            e.getValue().forEach(x -> have.add(x.intValue()));
            if (!have.isEmpty() && !have.equals(want)) pa.add("subscribed " + e.getKey() + " " + have + ", routing " + want);
        });
        Set<Integer> stations = new TreeSet<>(routing.stations().values());
        Path journals = er.out().resolve("journals");
        for (String t : STATION_TOPICS) {
            Set<Integer> have = partitions(journals.resolve(t));
            if (!have.equals(stations)) pa.add(t + " partitions " + have + ", the stations' " + stations);
        }
        Set<Integer> ledger = new TreeSet<>(stations);
        ledger.add(6);
        for (String t : LEDGERS) {
            Set<Integer> have = partitions(journals.resolve(t));
            if (!ledger.containsAll(have) || !have.contains(6)) pa.add(t + " partitions " + have + ", allowed " + ledger + " with 6");
        }

        // topic settings as the broker reported them
        JsonNode topics = evidence.path("topics");
        if (!topics.path("observed").isObject() || topics.path("observed").isEmpty()) f.get("R-TOPIC-RETENTION").add("no observed topic settings");
        topics.path("problems").forEach(p -> f.get("R-TOPIC-RETENTION").add(p.asText()));
        topics.path("observed").fields().forEachRemaining(e -> { if (!e.getValue().has("retention.ms")) f.get("R-TOPIC-RETENTION").add(e.getKey() + " has no observed retention.ms"); });

        // the shared reconciliation at every completed cut
        Reconciliation rc = Reconciliation.open(r.recording, er.out());
        for (JsonNode x : r.cutLog.path("cuts")) f.get("R-RECONCILE").addAll(rc.cut(x.path("cutId").asLong()));
        f.get("R-RECONCILE").addAll(rc.finish());
        return f;
    }

    /** Every row of the run's row journals: its envelope pins name the run record's runId, artifact and the payload timestamp source.
     *  Returns the number of rows read. */
    static long scanRows(Path out, String runId, String artifact, List<String> pins, List<String> ts) throws IOException {
        long n = 0;
        int reported = 0;
        for (String t : STATION_TOPICS) {
            Path dir = out.resolve("journals").resolve(t);
            if (!Files.isDirectory(dir)) continue;
            List<Path> files;
            try (Stream<Path> s = Files.list(dir)) { files = s.sorted().toList(); }
            for (Path file : files) try (BufferedReader in = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.isEmpty() || !line.contains("\"envelope\"")) continue;
                    JsonNode job = RunFold.readLine(line).path("value").path("envelope").path("pins").path("job");
                    if (job.isMissingNode()) continue;
                    n++;
                    if (reported < 20 && (!Objects.equals(runId, job.path("runId").asText(null)) || !Objects.equals(artifact, job.path("artifactSha256").asText(null)))) {
                        reported++;
                        pins.add(t + "/" + file.getFileName() + ": a row with runId " + job.path("runId") + ", artifactSha256 " + job.path("artifactSha256"));
                    }
                    if (!"payload".equals(job.path("timestampSource").asText(null)) && ts.size() < 20) ts.add(t + "/" + file.getFileName() + ": a row with timestampSource " + job.path("timestampSource"));
                }
            }
        }
        return n;
    }

    static Set<Integer> partitions(Path dir) throws IOException {
        Set<Integer> s = new TreeSet<>();
        if (!Files.isDirectory(dir)) return s;
        try (Stream<Path> l = Files.list(dir)) {
            for (Path p : l.toList()) {
                String n = p.getFileName().toString();
                if (n.endsWith(".jsonl")) s.add(Integer.parseInt(n.substring(0, n.length() - ".jsonl".length())));
            }
        }
        return s;
    }

    interface Visit { void at(String path, JsonNode n); }

    static void walk(JsonNode n, String path, Visit v) {
        if (n == null || n.isMissingNode()) return;
        if (n.isObject()) n.fields().forEachRemaining(e -> walk(e.getValue(), path + "." + e.getKey(), v));
        else if (n.isArray()) { int i = 0; for (JsonNode x : n) walk(x, path + "[" + i++ + "]", v); }
        else v.at(path, n);
    }
}
