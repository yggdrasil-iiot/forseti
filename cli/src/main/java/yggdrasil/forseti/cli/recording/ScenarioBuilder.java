package yggdrasil.forseti.cli.recording;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** Scenario JSON -> recording directory, in its minimal form (Task 8): visits, continuous telemetry with the tail, one
 *  {@code session-up} per device, incidents, the {@code plc-identity} stamping, batches with deadline brackets, digests and the lock.
 *  Overlays other than {@code plc-identity} and the full ground truth are Task 16's. Times in a scenario are milliseconds from
 *  {@code T0}; everything written is epoch milliseconds. Nothing reads a clock, a random source, the environment or a platform path,
 *  so a rebuild writes the same bytes on every OS. */
public final class ScenarioBuilder {
    public static final String GENERATOR = "forseti recordings build 0.1.0";
    public static final int PARTITIONS = 6;
    // The bridge and producer timing the scenario file may override (plan Step 8.1's example lines).
    static final long SERVER_DELAY_MS = 10, PUBLISH_DELAY_MS = 50, BRIDGE_DELAY_MS = 120, RECORD_DELAY_MS = 200;
    static final String OCCUPANCY_SOURCE = "mes-line2";
    static final String ARRIVING = "urn:epcglobal:cbv:bizstep:arriving", DEPARTING = "urn:epcglobal:cbv:bizstep:departing";
    static final List<String> SUPPORTED_INJECTIONS = List.of("plc-identity");

    /** What a build produced: the counts the lock pins, and the lock itself. */
    public record Built(long messages, long samples, long sessions, long occupancy, long incidents, long cuts, ObjectNode lock) {}

    private record Visit(int index, String unit, String station, long enter, long leave, List<String> tools, long visitSeq) {}
    private record Stamp(String station, List<String> equipment, String visitId, String unit, long from, long to) {}
    private record Item(String topic, int partition, String key, int batch, ObjectNode value) {}

    private ScenarioBuilder() {}

    /** The lock file that sits beside a scenario: {@code smoke.json} -> {@code smoke.lock.json}. */
    public static Path lockOf(Path scenario) {
        String n = scenario.getFileName().toString();
        if (!n.endsWith(".json") || n.endsWith(".lock.json")) throw new IllegalArgumentException("not a scenario file: " + n);
        return scenario.resolveSibling(n.substring(0, n.length() - ".json".length()) + ".lock.json");
    }

    public static Built build(Path scenarioFile, Path out) {
        byte[] raw;
        try { raw = Files.readAllBytes(scenarioFile); } catch (IOException e) { throw new UncheckedIOException(e); }
        return build(Json.read(raw), Canonical.digestOf(raw), out);
    }

    public static Built build(JsonNode s, String scenarioSha256, Path out) {
        if (Json.safeLong(s.get("scenarioVersion"), "scenarioVersion", 1, 1) != 1) throw refuse("scenarioVersion");
        String name = Json.text(s, "name");
        String mode = Json.text(s, "mode");
        if (!mode.equals("occupancy-join") && !mode.equals("plc-stamped")) throw refuse("mode " + mode);
        long t0 = Json.safeLong(s.get("T0"), "T0", Json.NON_NEGATIVE);
        long seed = Json.safeLong(s.get("seed"), "seed", Json.NON_NEGATIVE);
        long scanMs = Json.safeLong(s.get("scanMs"), "scanMs", 1, Json.MAX_SAFE);
        long batchMs = Json.safeLong(s.get("batchMs"), "batchMs", 1, Json.MAX_SAFE);
        long tailMs = Json.safeLong(s.get("tailMs"), "tailMs", Json.NON_NEGATIVE);
        long from = Json.safeLong(s.get("telemetryFrom"), "telemetryFrom", Json.NON_NEGATIVE);
        long to = Json.safeLong(s.get("telemetryTo"), "telemetryTo", Json.NON_NEGATIVE);
        String edge = Json.text(s, "edge");
        JsonNode temporal = s.path("temporal");
        long oooMs = Json.safeLong(temporal.get("outOfOrdernessMs"), "temporal.outOfOrdernessMs", Json.NON_NEGATIVE);
        long latenessMs = Json.safeLong(temporal.get("allowedLatenessMs"), "temporal.allowedLatenessMs", Json.NON_NEGATIVE);
        JsonNode timing = s.path("messageTiming");
        long serverDelay = optional(timing, "serverDelayMs", SERVER_DELAY_MS), publishDelay = optional(timing, "publishDelayMs", PUBLISH_DELAY_MS),
             bridgeDelay = optional(timing, "bridgeDelayMs", BRIDGE_DELAY_MS);
        JsonNode occ = s.path("occupancy");
        long recordDelay = optional(occ, "recordDelayMs", RECORD_DELAY_MS);
        String occSource = occ.path("source").isTextual() ? occ.path("source").textValue() : OCCUPANCY_SOURCE;

        // Devices and stations: a station is a declared device no other declared device is a prefix of; tools sit under one.
        TreeMap<String, JsonNode> devices = new TreeMap<>();
        if (!s.path("devices").isObject() || s.path("devices").isEmpty()) throw refuse("devices");
        s.path("devices").fields().forEachRemaining(e -> devices.put(e.getKey(), e.getValue()));
        List<String> stations = new ArrayList<>();
        Map<String, String> stationOf = new TreeMap<>();
        for (String d : devices.keySet()) {
            String parent = null;
            for (String o : devices.keySet()) if (!o.equals(d) && d.startsWith(o + ":") && (parent == null || o.length() < parent.length())) parent = o;
            if (parent == null) stations.add(d);
            stationOf.put(d, parent == null ? d : parent);
        }
        ObjectNode routingNode = RoutingManifest.build(PARTITIONS, stations, stationOf);
        RoutingManifest routing = RoutingManifest.read(routingNode);

        JsonNode toolActivity = s.path("toolActivity");
        long startAfter = Json.safeLong(toolActivity.get("startAfterEnter"), "toolActivity.startAfterEnter", Json.NON_NEGATIVE);
        long endBefore = Json.safeLong(toolActivity.get("endBeforeLeave"), "toolActivity.endBeforeLeave", Json.NON_NEGATIVE);
        BigDecimal tMin = decimal(toolActivity.path("torque").get("min"), "toolActivity.torque.min");
        BigDecimal tMax = decimal(toolActivity.path("torque").get("max"), "toolActivity.torque.max");
        if (tMin.compareTo(tMax) > 0) throw refuse("toolActivity.torque min > max");

        List<Visit> visits = new ArrayList<>();
        int vi = 0;
        for (JsonNode v : s.path("visits")) {
            vi++;
            String station = Json.text(v, "station");
            if (!stations.contains(station)) throw refuse("visit " + vi + " at an undeclared station " + station);
            long enter = Json.safeLong(v.get("enter"), "visits.enter", Json.NON_NEGATIVE), leave = Json.safeLong(v.get("leave"), "visits.leave", Json.NON_NEGATIVE);
            if (leave <= enter) throw refuse("visit " + vi + " leave <= enter");
            List<String> tools = new ArrayList<>();
            for (JsonNode t : v.path("tools")) {
                String path = station + ":" + t.asText();
                if (!devices.containsKey(path) || !stationOf.get(path).equals(station) || path.equals(station)) throw refuse("visit " + vi + " tool " + t.asText());
                tools.add(path);
            }
            visits.add(new Visit(vi, Json.text(v, "unit"), station, enter, leave, List.copyOf(tools), Json.safeLong(v.get("visitSeq"), "visits.visitSeq", Json.NON_NEGATIVE)));
        }

        List<Stamp> stamps = new ArrayList<>();
        ArrayNode overlays = Json.JSON.createArrayNode();
        for (JsonNode inj : s.path("injections")) {
            String type = Json.text(inj, "type");
            if (!SUPPORTED_INJECTIONS.contains(type)) throw refuse("injection type " + type + " is not supported by the minimal builder");
            String station = Json.text(inj, "station");
            List<String> equipment = new ArrayList<>();
            inj.path("equipment").forEach(e -> equipment.add(e.asText()));
            for (String e : equipment) {
                if (!devices.containsKey(e) || !stationOf.get(e).equals(station)) throw refuse("plc-identity equipment " + e);
                for (String m : List.of("UnitId", "VisitId", "VisitPresent"))
                    if (!devices.get(e).path("members").has(m)) throw refuse("plc-identity equipment " + e + " declares no member " + m);
            }
            long pf = Json.safeLong(inj.path("present").get("from"), "present.from", Json.NON_NEGATIVE), pt = Json.safeLong(inj.path("present").get("to"), "present.to", Json.NON_NEGATIVE);
            if (pt <= pf) throw refuse("plc-identity present.to <= present.from");
            stamps.add(new Stamp(station, List.copyOf(equipment), Json.text(inj, "visitId"), Json.text(inj, "unit"), pf, pt));
            overlays.add(inj.deepCopy());
        }
        if (mode.equals("plc-stamped") && stamps.isEmpty()) throw refuse("a plc-stamped scenario without plc-identity");

        // Incidents: revisions with their fields; closed tool-check-failed revision 1 must have suspectTo = detectedAt = revisionEventTime.
        record Revision(String incidentId, long revision, long deliverAt, ObjectNode value, String equipmentRef, Long suspectTo) {}
        List<Revision> revisions = new ArrayList<>();
        for (JsonNode inc : s.path("incidents")) {
            String id = Json.text(inc, "incidentId");
            for (JsonNode r : inc.path("revisions")) {
                long rev = Json.safeLong(r.get("revision"), "revision", 1, Json.MAX_SAFE);
                long deliverAt = Json.safeLong(r.get("deliverAt"), "deliverAt", Json.NON_NEGATIVE);
                JsonNode f = r.path("fields");
                long detected = Json.safeLong(f.get("detectedAt"), "detectedAt", Json.NON_NEGATIVE);
                long revTime = Json.safeLong(f.get("revisionEventTime"), "revisionEventTime", Json.NON_NEGATIVE);
                long suspectFrom = Json.safeLong(f.get("suspectFrom"), "suspectFrom", Json.NON_NEGATIVE);
                Long suspectTo = f.path("suspectTo").isNull() ? null : Json.safeLong(f.get("suspectTo"), "suspectTo", Json.NON_NEGATIVE);
                String kind = Json.text(f, "kind"), status = Json.text(f, "status");
                if (rev == 1 && revTime != detected) throw refuse(id + " revision 1 revisionEventTime != detectedAt");
                if (rev == 1 && kind.equals("tool-check-failed") && status.equals("closed") && (suspectTo == null || suspectTo != detected))
                    throw refuse(id + " closed tool-check-failed revision 1 with suspectTo != detectedAt");
                String stationRef = Json.text(f, "stationRef"), equipmentRef = Json.text(f, "equipmentRef");
                ObjectNode v = Json.object();
                v.put("incidentId", id);
                Json.putSafe(v, "revision", rev);
                v.put("stationRef", stationRef);
                v.put("equipmentRef", equipmentRef);
                v.put("kind", kind);
                Json.putSafe(v, "detectedAt", t0 + detected);
                Json.putSafe(v, "revisionEventTime", t0 + revTime);
                Json.putSafe(v, "suspectFrom", t0 + suspectFrom);
                if (suspectTo == null) v.putNull("suspectTo"); else Json.putSafe(v, "suspectTo", t0 + suspectTo);
                v.put("status", status);
                v.put("evidenceRef", Json.text(f, "evidenceRef"));
                v.put("source", Json.text(f, "source"));
                revisions.add(new Revision(id, rev, deliverAt, v, equipmentRef, suspectTo));
            }
        }

        // The telemetry end: E = max(telemetryTo, lastBusinessEvent) + tailMs (one rule for every scenario, plan Task 16).
        long lastBusiness = 0;   // the session-up at T0
        for (Visit v : visits) lastBusiness = Math.max(lastBusiness, v.leave());
        for (Stamp st : stamps) lastBusiness = Math.max(lastBusiness, st.to());
        for (Revision r : revisions) {
            lastBusiness = Math.max(lastBusiness, r.deliverAt());
            lastBusiness = Math.max(lastBusiness, r.value().get("detectedAt").longValue() - t0);
            lastBusiness = Math.max(lastBusiness, r.value().get("revisionEventTime").longValue() - t0);
        }
        long end = Math.max(to, lastBusiness) + tailMs;
        if (end < from || (end - from) % scanMs != 0) throw refuse("telemetry end " + end + " is not on the scan grid from " + from);
        long scans = (end - from) / scanMs + 1;

        // Deadlines: settlement of every visit the rule evaluates (leave + L) and of every closed incident (suspectTo + L).
        record Due(String kind, String ref, long at) {}
        List<Due> dues = new ArrayList<>();
        if (mode.equals("occupancy-join")) for (Visit v : visits) dues.add(new Due("visit-settlement", "visit-" + v.index(), v.leave() + latenessMs));
        else for (Stamp st : stamps) dues.add(new Due("visit-settlement", st.visitId(), st.to() + latenessMs));
        Map<String, Long> latestSuspectTo = new TreeMap<>();
        Map<String, Long> latestRevision = new TreeMap<>();
        for (Revision r : revisions) if (r.revision() >= latestRevision.getOrDefault(r.incidentId(), 0L)) {
            latestRevision.put(r.incidentId(), r.revision());
            if (r.suspectTo() == null) latestSuspectTo.remove(r.incidentId()); else latestSuspectTo.put(r.incidentId(), r.suspectTo());
        }
        latestSuspectTo.forEach((id, st) -> dues.add(new Due("incident-settlement", id, st + latenessMs)));

        // Batch boundaries (scan times): every batchMs, the two brackets of every deadline, the last scan.
        TreeSet<Long> bounds = new TreeSet<>();
        for (long b = from + batchMs; b <= end; b += batchMs) bounds.add(b);
        Map<Due, long[]> bracket = new LinkedHashMap<>();
        for (Due d : dues) {
            long limit = d.at() + oooMs;
            if (limit < from) throw refuse("deadline " + d.ref() + " before the telemetry starts");
            long a = from + Math.floorDiv(Math.min(limit, end) - from, scanMs) * scanMs;
            long b = a + scanMs;
            if (b > end) throw refuse("deadline " + d.kind() + " " + d.ref() + " has no bracketing cuts inside the telemetry");
            bounds.add(a); bounds.add(b);
            bracket.put(d, new long[] {a, b});
        }
        bounds.add(end);
        List<Long> boundaries = new ArrayList<>(bounds);

        // Records, in generation order per topic; each assigned to the batch of its event time (incidents: deliverAt).
        List<Item> items = new ArrayList<>();
        Map<String, String> definitionSha = new TreeMap<>();
        ObjectNode definitions = Json.object();
        for (Map.Entry<String, JsonNode> d : devices.entrySet()) {
            String model = Json.text(d.getValue(), "model");
            String sha = Canonical.digestOf((d.getKey() + ":" + model).getBytes(StandardCharsets.UTF_8));
            definitionSha.put(d.getKey(), sha);
            definitions.putObject(d.getKey()).put("templateRef", model).put("version", "1.0.0").put("sha256", sha);
        }
        for (String d : devices.keySet()) {
            ObjectNode v = Json.object();
            v.put("edge", edge);
            v.put("device", d);
            Json.putSafe(v, "birthPublishTs", t0);
            Json.putSafe(v, "bdSeq", 0);
            v.put("kind", "session-up");
            Json.putSafe(v, "atSeq", 1);   // the DBIRTH, counted as having consumed seq 1 (spec Process messages)
            ObjectNode p = v.putObject("payload");
            Json.putSafe(p, "at", t0);
            p.put("rebirth", false);
            items.add(new Item("ot.sessions", routing.rawPartitionOf(d), d, batchOf(boundaries, 0, "session-up " + d), v));
        }
        List<ObjectNode> occupancy = new ArrayList<>();
        for (Visit v : visits) {
            occupancy.add(occupancyEvent("occ-" + (2 * v.index() - 1), t0 + v.enter(), recordDelay, v, ARRIVING, null, occSource));
            occupancy.add(occupancyEvent("occ-" + (2 * v.index()), t0 + v.leave(), recordDelay, v, DEPARTING, "occ-" + (2 * v.index() - 1), occSource));
        }
        occupancy.sort(Comparator.comparingLong((ObjectNode o) -> o.get("eventTime").longValue()).thenComparing(o -> o.get("eventId").textValue()));
        for (ObjectNode o : occupancy) {
            String rp = o.get("readPoint").textValue();
            items.add(new Item("mfg.occupancy", routing.partitionOf(rp), rp, batchOf(boundaries, o.get("eventTime").longValue() - t0, o.get("eventId").textValue()), o));
        }
        Map<String, Long> seq = new HashMap<>();
        long samples = 0;
        for (long i = 0; i < scans; i++) {
            long t = from + i * scanMs;
            int batch = batchOf(boundaries, t, "scan " + t);
            for (Map.Entry<String, JsonNode> d : devices.entrySet()) {
                String dev = d.getKey();
                boolean isStation = stationOf.get(dev).equals(dev);
                long sq = seq.merge(dev, 1L, Long::sum) + 1;   // first DDATA of the session carries seq 2
                ObjectNode v = Json.object();
                v.put("edge", edge);
                v.put("device", dev);
                Json.putSafe(v, "birthPublishTs", t0);
                Json.putSafe(v, "bdSeq", 0);
                Json.putSafe(v, "seq", sq);
                Json.putSafe(v, "publishTs", t0 + t + publishDelay);
                Json.putSafe(v, "bridgeReceivedAt", t0 + t + bridgeDelay);
                v.put("definitionSha256", definitionSha.get(dev));
                ArrayNode members = v.putArray("members");
                boolean running = isStation ? stationOccupied(visits, dev, t) : toolRunning(visits, dev, t, startAfter, endBefore);
                Stamp stamp = null, lastStamp = null;
                for (Stamp st : stamps) if (st.equipment().contains(dev)) {
                    if (st.from() <= t && t < st.to()) stamp = st;
                    if (st.from() <= t && (lastStamp == null || st.from() > lastStamp.from())) lastStamp = st;
                }
                Iterator<Map.Entry<String, JsonNode>> it = d.getValue().path("members").fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> m = it.next();
                    String metric = m.getKey(), type = m.getValue().asText();
                    ObjectNode mo = members.addObject();
                    mo.put("metric", metric);
                    switch (metric + ":" + type) {
                        case "Running:Boolean" -> mo.put("value", running);
                        case "Torque:Double" -> {
                            if (isStation) throw refuse("Torque on a station device " + dev);
                            mo.set("value", new DecimalNode(running ? Telemetry.torque(seed, dev, t / scanMs, tMin, tMax) : Telemetry.IDLE));
                        }
                        case "UnitId:String" -> mo.put("value", lastStamp == null ? "" : lastStamp.unit());
                        case "VisitId:String" -> mo.put("value", lastStamp == null ? "" : lastStamp.visitId());
                        case "VisitPresent:Boolean" -> mo.put("value", stamp != null);
                        default -> throw refuse("device " + dev + " member " + metric + " " + type + " is not generated by the builder");
                    }
                    Json.putSafe(mo, "uaStatusCode", 0);
                    Json.putSafe(mo, "quality", 192);
                    Json.putSafe(mo, "sourceTs", t0 + t);
                    Json.putSafe(mo, "serverTs", t0 + t + serverDelay);
                    samples++;
                }
                items.add(new Item("ot.messages", routing.rawPartitionOf(dev), dev, batch, v));
            }
        }
        revisions.sort(Comparator.comparingLong(Revision::deliverAt).thenComparing(Revision::incidentId).thenComparingLong(Revision::revision));
        for (Revision r : revisions)
            items.add(new Item("quality.incidents", Murmur2.partition(r.equipmentRef(), PARTITIONS), r.equipmentRef(), batchOf(boundaries, r.deliverAt(), r.incidentId()), r.value()));

        // Offsets per partition in batch order (stable: generation order inside a batch).
        Map<String, List<Item>> byPartition = new TreeMap<>();
        for (String topic : RoutingManifest.TOPICS) for (int p : routing.subscribed(topic)) byPartition.put(topic + "/" + p, new ArrayList<>());
        for (Item it : items) {
            List<Item> l = byPartition.get(it.topic() + "/" + it.partition());
            if (l == null) throw refuse(it.topic() + " record on the unsubscribed partition " + it.partition());
            l.add(it);
        }
        byPartition.values().forEach(l -> l.sort(Comparator.comparingInt(Item::batch)));

        TreeSet<Integer> firstPartitions = new TreeSet<>();
        for (int p : routing.subscribed("ot.messages")) if (!byPartition.get("ot.messages/" + p).isEmpty() && byPartition.get("ot.messages/" + p).get(0).batch() == 0) firstPartitions.add(p);
        if (!firstPartitions.equals(new TreeSet<>(routing.subscribed("ot.messages"))))
            throw refuse("the first batch carries no telemetry on some subscribed raw partition (spec 406): " + firstPartitions);

        // Batches and deadlines.
        ArrayNode batches = Json.JSON.createArrayNode();
        for (int k = 0; k < boundaries.size(); k++) {
            ObjectNode b = batches.addObject();
            Json.putSafe(b, "cutId", k + 1);
            ObjectNode through = b.putObject("throughOffsets");
            for (String topic : RoutingManifest.TOPICS) {
                if (routing.subscribed(topic).isEmpty()) continue;
                ObjectNode tp = through.putObject(topic);
                for (int p : routing.subscribed(topic)) {
                    List<Item> l = byPartition.get(topic + "/" + p);
                    long last = -1;
                    for (int i = 0; i < l.size() && l.get(i).batch() <= k; i++) last = i;
                    Json.putSafe(tp, Integer.toString(p), last);
                }
            }
            b.putArray("idleInputs");
        }
        ArrayNode deadlines = Json.JSON.createArrayNode();
        long settleTarget = Long.MIN_VALUE;
        for (Map.Entry<Due, long[]> e : bracket.entrySet()) {
            Due d = e.getKey();
            int a = boundaries.indexOf(e.getValue()[0]), b = boundaries.indexOf(e.getValue()[1]);
            // Every device reports every scan, so W1 at a cut is the cut's last scan - O - 1 at every station.
            long w1a = boundaries.get(a) - oooMs - 1, w1b = boundaries.get(b) - oooMs - 1;
            if (!(w1a < d.at() && d.at() <= w1b) || b != a + 1) throw new IllegalStateException("bracket of " + d.ref() + " does not hold");
            ObjectNode o = deadlines.addObject();
            o.put("kind", d.kind());
            o.put("ref", d.ref());
            Json.putSafe(o, "at", t0 + d.at());
            Json.putSafe(o, "cutA", a + 1);
            Json.putSafe(o, "cutB", b + 1);
            settleTarget = Math.max(settleTarget, t0 + d.at());
        }
        if (settleTarget == Long.MIN_VALUE) settleTarget = t0 + end - oooMs - 1;

        // Files.
        TreeMap<String, byte[]> files = new TreeMap<>();
        for (Map.Entry<String, List<Item>> e : byPartition.entrySet()) {
            StringBuilder sb = new StringBuilder();
            long offset = 0;
            for (Item it : e.getValue()) {
                ObjectNode line = Json.object();
                Json.putSafe(line, "offset", offset++);
                line.putNull("timestamp");
                line.put("key", it.key());
                line.putObject("headers");
                line.set("value", it.value());
                try { sb.append(Json.JSON.writeValueAsString(line)).append('\n'); } catch (IOException x) { throw new UncheckedIOException(x); }
            }
            files.put(e.getKey() + ".jsonl", sb.toString().getBytes(StandardCharsets.UTF_8));
        }
        files.put(Recording.ROUTING, Json.pretty(routingNode));
        files.put(Recording.EXPECTED, Json.pretty(expected(t0, visits, stamps, revisions.stream().map(Revision::incidentId).distinct().sorted().toList(), settleTarget)));

        ObjectNode manifest = Json.object();
        Json.putSafe(manifest, "recordingVersion", 1);
        manifest.put("name", name);
        manifest.put("synthetic", false);
        ObjectNode source = manifest.putObject("source");
        source.put("scenario", name);
        source.put("scenarioSha256", scenarioSha256);
        source.put("generator", GENERATOR);
        Json.putSafe(source, "seed", seed);
        source.set("overlays", overlays);
        manifest.set("rule", s.path("rule").deepCopy());
        manifest.set("definitions", definitions);
        Json.putSafe(manifest, "T0", t0);
        Json.putSafe(manifest, "scanMs", scanMs);
        ObjectNode range = manifest.putObject("evidenceRange");
        for (String d : devices.keySet()) { ObjectNode r = range.putObject(d); Json.putSafe(r, "from", t0 + from); Json.putSafe(r, "to", t0 + end); }
        manifest.set("batches", batches);
        manifest.set("deadlines", deadlines);
        Json.putSafe(manifest, "settleTargetW", settleTarget);
        Json.putSafe(manifest, "tailMs", tailMs);
        ObjectNode digests = manifest.putObject("digests");
        files.forEach((f, b) -> digests.put(f, Canonical.digestOf(b)));
        byte[] manifestBytes = Json.pretty(manifest);

        write(out, files, manifestBytes);

        long messages = items.stream().filter(i -> i.topic().equals("ot.messages")).count();
        long sessions = items.stream().filter(i -> i.topic().equals("ot.sessions")).count();
        long occCount = items.stream().filter(i -> i.topic().equals("mfg.occupancy")).count();
        long incidents = items.stream().filter(i -> i.topic().equals("quality.incidents")).count();
        ObjectNode lock = Json.object();
        Json.putSafe(lock, "lockVersion", 1);
        lock.put("scenario", name);
        lock.put("scenarioSha256", scenarioSha256);
        lock.put("generator", GENERATOR);
        ObjectNode counts = lock.putObject("counts");
        Json.putSafe(counts, "messages", messages);
        Json.putSafe(counts, "samples", samples);
        Json.putSafe(counts, "sessions", sessions);
        Json.putSafe(counts, "occupancy", occCount);
        Json.putSafe(counts, "incidents", incidents);
        Json.putSafe(counts, "cuts", boundaries.size());
        ArrayNode fb = lock.putArray("firstBatchTelemetryPartitions");
        firstPartitions.forEach(fb::add);
        ObjectNode lf = lock.putObject("files");
        TreeMap<String, String> all = new TreeMap<>();
        files.forEach((f, b) -> all.put(f, Canonical.digestOf(b)));
        all.put(Recording.MANIFEST, Canonical.digestOf(manifestBytes));
        all.forEach(lf::put);
        return new Built(messages, samples, sessions, occCount, incidents, boundaries.size(), lock);
    }

    private static ObjectNode expected(long t0, List<Visit> visits, List<Stamp> stamps, List<String> incidents, long settleTarget) {
        ObjectNode e = Json.object();
        Json.putSafe(e, "expectedVersion", 1);
        e.put("scope", "minimal");   // the scenario's facts only; Task 16's GroundTruth adds statuses, samples, dispositions, transitions
        ArrayNode vs = e.putArray("visits");
        for (Visit v : visits) {
            ObjectNode o = vs.addObject();
            o.put("visitRef", "visit-" + v.index());
            o.put("unitId", v.unit());
            o.put("station", v.station());
            Json.putSafe(o, "enter", t0 + v.enter());
            Json.putSafe(o, "leave", t0 + v.leave());
            ArrayNode tu = o.putArray("toolsUsed");
            v.tools().forEach(tu::add);
        }
        ArrayNode ps = e.putArray("plcVisits");
        for (Stamp st : stamps) {
            ObjectNode o = ps.addObject();
            o.put("visitId", st.visitId());
            o.put("unitId", st.unit());
            o.put("station", st.station());
            ArrayNode eq = o.putArray("equipment");
            st.equipment().forEach(eq::add);
            Json.putSafe(o, "enter", t0 + st.from());
            Json.putSafe(o, "leave", t0 + st.to());
        }
        ArrayNode is = e.putArray("incidents");
        incidents.forEach(is::add);
        Json.putSafe(e, "settleTargetW", settleTarget);
        return e;
    }

    private static ObjectNode occupancyEvent(String id, long time, long recordDelay, Visit v, String bizStep, String arrival, String source) {
        ObjectNode o = Json.object();
        o.put("eventId", id);
        Json.putSafe(o, "eventTime", time);
        Json.putSafe(o, "recordTime", time + recordDelay);
        o.put("unitId", v.unit());
        o.put("action", "OBSERVE");
        o.put("bizStep", bizStep);
        o.put("readPoint", v.station());
        Json.putSafe(o, "visitSeq", v.visitSeq());
        if (arrival != null) o.put("arrivalEventId", arrival);
        o.put("source", source);
        return o;
    }

    private static boolean stationOccupied(List<Visit> visits, String station, long t) {
        for (Visit v : visits) if (v.station().equals(station) && v.enter() <= t && t < v.leave()) return true;
        return false;
    }

    /** A used tool runs from {@code enter + startAfterEnter} to {@code leave - endBeforeLeave}, both included (the last measurement
     *  1 s before leave); an unused tool reports {@code Running=false} throughout (spec 607). */
    private static boolean toolRunning(List<Visit> visits, String tool, long t, long startAfter, long endBefore) {
        for (Visit v : visits) if (v.tools().contains(tool) && v.enter() + startAfter <= t && t <= v.leave() - endBefore) return true;
        return false;
    }

    /** Index of the batch whose interval (previous boundary, boundary] contains {@code t}; times before the first boundary go to batch 0. */
    private static int batchOf(List<Long> boundaries, long t, String what) {
        for (int i = 0; i < boundaries.size(); i++) if (t <= boundaries.get(i)) return i;
        throw refuse(what + " at " + t + " lies after the last scan " + boundaries.get(boundaries.size() - 1));
    }

    private static long optional(JsonNode o, String field, long dflt) {
        return o.has(field) ? Json.safeLong(o.get(field), field, Json.NON_NEGATIVE) : dflt;
    }
    private static BigDecimal decimal(JsonNode n, String field) {
        if (n == null || !n.isNumber()) throw refuse(field);
        return Telemetry.printable(new BigDecimal(n.asText()));
    }
    private static IllegalArgumentException refuse(String what) { return new IllegalArgumentException("scenario refused: " + what); }

    /** Replaces {@code out} with the recording; refuses to clear a non-empty directory that is not a recording. */
    private static void write(Path out, Map<String, byte[]> files, byte[] manifest) {
        try {
            if (Files.exists(out)) {
                boolean empty;
                try (Stream<Path> l = Files.list(out)) { empty = l.findAny().isEmpty(); }
                if (!empty && !Files.isRegularFile(out.resolve(Recording.MANIFEST))) throw new IllegalArgumentException("refusing to replace " + out + ": not a recording");
                try (Stream<Path> w = Files.walk(out)) {
                    for (Path p : w.sorted(Comparator.reverseOrder()).toList()) if (!p.equals(out)) Files.delete(p);
                }
            }
            Files.createDirectories(out);
            for (Map.Entry<String, byte[]> e : files.entrySet()) {
                Path p = out;
                for (String part : e.getKey().split("/")) p = p.resolve(part);
                Files.createDirectories(p.getParent());
                Files.write(p, e.getValue());
            }
            Files.write(out.resolve(Recording.MANIFEST), manifest);
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }
}
