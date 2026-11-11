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

/** Scenario JSON -> recording directory: visits, continuous telemetry with the tail, sessions, occupancy, incidents, the injection
 *  overlays ({@link Overlays}), batches with deadline brackets, digests, the ground truth ({@link GroundTruth}) and the lock.
 *
 *  <p>Two scenario forms. A scenario that carries its own {@code temporal} block is a Task 8 smoke scenario: it is built without a
 *  registry and its {@code expected.json} keeps the minimal scope (the scenario's facts), so the committed smoke recordings stay byte for
 *  byte what they are. Every other scenario reads its rule from the registry's release payload ({@code --registry}) and gets the full
 *  ground truth. Times in a scenario are milliseconds from {@code T0}; everything written is epoch milliseconds. Nothing reads a clock,
 *  a random source, the environment or a platform path, so a rebuild writes the same bytes on every OS. */
public final class ScenarioBuilder {
    public static final String GENERATOR = "forseti recordings build 0.1.0";
    public static final int PARTITIONS = 6;
    // The bridge and producer timing the scenario file may override (plan Step 8.1's example lines).
    static final long SERVER_DELAY_MS = 10, PUBLISH_DELAY_MS = 50, BRIDGE_DELAY_MS = 120, RECORD_DELAY_MS = 200;
    static final String OCCUPANCY_SOURCE = "mes-line2";
    static final String ARRIVING = "urn:epcglobal:cbv:bizstep:arriving", DEPARTING = "urn:epcglobal:cbv:bizstep:departing";
    static final Set<String> IDENTITY_METRICS = Set.of("UnitId", "VisitId", "VisitPresent");

    /** What a build produced: the counts the lock pins, and the lock itself. */
    public record Built(long messages, long samples, long sessions, long occupancy, long incidents, long cuts, ObjectNode lock) {}

    private record Visit(int index, String unit, String station, long enter, long leave, List<String> tools, long visitSeq) {}
    private record Stamp(String station, List<String> equipment, String visitId, String unit, long from, long to, boolean closes) {}
    private record Item(String topic, int partition, String key, int batch, ObjectNode value) {}
    private record Occ(ObjectNode value, long deliverAt, int order) {}
    private record Due(String kind, String ref, String station, long at) {}

    private ScenarioBuilder() {}

    /** The lock file that sits beside a scenario: {@code smoke.json} -> {@code smoke.lock.json}. */
    public static Path lockOf(Path scenario) {
        String n = scenario.getFileName().toString();
        if (!n.endsWith(".json") || n.endsWith(".lock.json")) throw new IllegalArgumentException("not a scenario file: " + n);
        return scenario.resolveSibling(n.substring(0, n.length() - ".json".length()) + ".lock.json");
    }

    public static Built build(Path scenarioFile, Path out) { return build(scenarioFile, null, out); }

    public static Built build(Path scenarioFile, Path registry, Path out) {
        byte[] raw;
        try { raw = Files.readAllBytes(scenarioFile); } catch (IOException e) { throw new UncheckedIOException(e); }
        return build(Json.read(raw), Canonical.digestOf(raw), registry, out);
    }

    public static Built build(JsonNode s, String scenarioSha256, Path out) { return build(s, scenarioSha256, null, out); }

    public static Built build(JsonNode s, String scenarioSha256, Path registry, Path out) {
        Synthesizer[] streaming = new Synthesizer[1];
        try { return build(s, scenarioSha256, registry, out, streaming); }
        catch (RuntimeException | Error e) { if (streaming[0] != null) streaming[0].abort(); throw e; }
    }

    /** {@code "synthetic": true} (plan Task 17): the telemetry is streamed to its files by a {@link Synthesizer} as it is generated
     *  instead of being collected; every other input, the batches, the deadlines and the ground truth are planned here as for a
     *  hand-made scenario, under the same rules. */
    private static Built build(JsonNode s, String scenarioSha256, Path registry, Path out, Synthesizer[] streaming) {
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
        if (s.has("synthetic") && !s.get("synthetic").isBoolean()) throw refuse("synthetic is a boolean");
        boolean synthetic = s.path("synthetic").asBoolean(false);

        // The rule's parameters: inline (Task 8 smoke form, minimal ground truth) or from the registry (full ground truth).
        boolean full = !s.has("temporal");
        GroundTruth.Rule rule = null;
        long oooMs, latenessMs;
        if (full) {
            if (registry == null) throw refuse("scenario " + name + " reads its rule from the registry: pass --registry");
            rule = GroundTruth.load(registry, s.path("rule"));
            if (!rule.mode().equals(mode)) throw refuse("scenario mode " + mode + " differs from the rule's " + rule.mode());
            oooMs = rule.ooo(); latenessMs = rule.lateness();
        } else {
            JsonNode temporal = s.path("temporal");
            oooMs = Json.safeLong(temporal.get("outOfOrdernessMs"), "temporal.outOfOrdernessMs", Json.NON_NEGATIVE);
            latenessMs = Json.safeLong(temporal.get("allowedLatenessMs"), "temporal.allowedLatenessMs", Json.NON_NEGATIVE);
            if (registry != null) {
                GroundTruth.Rule r = GroundTruth.load(registry, s.path("rule"));
                if (r.ooo() != oooMs || r.lateness() != latenessMs) throw refuse("the scenario's temporal block differs from the pinned rule's");
            }
        }
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
        if (full && !new TreeSet<>(rule.stations()).equals(new TreeSet<>(stations))) throw refuse("declared stations " + stations + " differ from the rule's " + rule.stations());
        ObjectNode routingNode = RoutingManifest.build(PARTITIONS, stations, stationOf);
        RoutingManifest routing = RoutingManifest.read(routingNode);

        JsonNode toolActivity = s.path("toolActivity");
        long startAfter = Json.safeLong(toolActivity.get("startAfterEnter"), "toolActivity.startAfterEnter", Json.NON_NEGATIVE);
        long endBefore = Json.safeLong(toolActivity.get("endBeforeLeave"), "toolActivity.endBeforeLeave", Json.NON_NEGATIVE);
        BigDecimal tMin = decimal(toolActivity.path("torque").get("min"), "toolActivity.torque.min");
        BigDecimal tMax = decimal(toolActivity.path("torque").get("max"), "toolActivity.torque.max");
        if (tMin.compareTo(tMax) > 0) throw refuse("toolActivity.torque min > max");

        Overlays ov = new Overlays(s.path("injections"), devices.keySet());
        if (synthetic) {
            if (!full) throw refuse("a synthetic scenario reads its rule from the registry");
            if (!ov.holdBacks.isEmpty()) throw refuse("hold-back in a synthetic scenario (its telemetry is streamed in scan order)");
            for (Overlays.Redeliver r : ov.redelivers) if ("ot.messages".equals(r.topic())) throw refuse("redeliver of a message in a synthetic scenario");
        }
        if (!ov.plannedLate.isEmpty() && (!synthetic || !mode.equals("occupancy-join"))) throw refuse("planned-late-samples needs a synthetic occupancy-join scenario");
        List<Visit> visits = new ArrayList<>();
        int vi = 0;
        List<JsonNode> visitNodes = new ArrayList<>();
        s.path("visits").forEach(visitNodes::add);
        visitNodes.addAll(ov.overlapVisits);
        for (JsonNode v : visitNodes) {
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
        for (JsonNode inj : ov.plcIdentity) {
            String station = Json.text(inj, "station");
            List<String> equipment = new ArrayList<>();
            inj.path("equipment").forEach(e -> equipment.add(e.asText()));
            for (String e : equipment) {
                if (!devices.containsKey(e) || !stationOf.get(e).equals(station)) throw refuse("plc-identity equipment " + e);
                for (String m : IDENTITY_METRICS)
                    if (!devices.get(e).path("members").has(m)) throw refuse("plc-identity equipment " + e + " declares no member " + m);
            }
            long pf = Json.safeLong(inj.path("present").get("from"), "present.from", Json.NON_NEGATIVE), pt = Json.safeLong(inj.path("present").get("to"), "present.to", Json.NON_NEGATIVE);
            if (pt <= pf) throw refuse("plc-identity present.to <= present.from");
            stamps.add(new Stamp(station, List.copyOf(equipment), Json.text(inj, "visitId"), Json.text(inj, "unit"), pf, pt, true));
        }
        for (Overlays.IdChange c : ov.idChanges) {
            int i = indexOfStamp(stamps, c.station(), c.at());
            Stamp st = stamps.get(i);
            stamps.set(i, new Stamp(st.station(), st.equipment(), st.visitId(), st.unit(), st.from(), c.at(), false));
            stamps.add(i + 1, new Stamp(st.station(), st.equipment(), c.newVisitId(), c.newUnit(), c.at(), st.to(), st.closes()));
        }
        for (Overlays.NoVisitEnd n : ov.noVisitEnds) {
            int i = -1;
            for (int j = 0; j < stamps.size(); j++) if (stamps.get(j).station().equals(n.station()) && stamps.get(j).visitId().equals(n.visitId())) i = j;
            if (i < 0) throw refuse("no-visit-end names no stamped visit " + n.visitId());
            Stamp st = stamps.get(i);
            stamps.set(i, new Stamp(st.station(), st.equipment(), st.visitId(), st.unit(), st.from(), Long.MAX_VALUE, false));
        }
        if (mode.equals("plc-stamped") && stamps.isEmpty()) throw refuse("a plc-stamped scenario without plc-identity");
        if (!mode.equals("plc-stamped") && (!ov.idChanges.isEmpty() || !ov.noVisitEnds.isEmpty() || !ov.mixed.isEmpty())) throw refuse("plc overlays on an occupancy-join scenario");

        // Incidents: revisions with their fields; closed tool-check-failed revision 1 must have suspectTo = detectedAt = revisionEventTime.
        record Revision(String incidentId, long revision, long deliverAt, ObjectNode value, String equipmentRef, String stationRef, Long suspectTo) {}
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
                revisions.add(new Revision(id, rev, deliverAt, v, equipmentRef, stationRef, suspectTo));
            }
        }

        // Occupancy deliveries: the two events of every visit, then the overlays' re-sends, delays, removals and corrections.
        List<Occ> occList = new ArrayList<>();
        Map<String, Visit> visitOfEvent = new HashMap<>();
        for (Visit v : visits) {
            String a = "occ-" + (2 * v.index() - 1), d = "occ-" + (2 * v.index());
            String unit = v.unit();
            for (Overlays.CrossCheckMismatch m : ov.mismatches) if (m.station().equals(v.station()) && v.enter() < m.to() && m.from() < v.leave()) unit = m.otherUnit();
            occList.add(new Occ(occupancyEvent(a, t0 + v.enter(), recordDelay, v, unit, ARRIVING, null, occSource), v.enter(), occList.size()));
            occList.add(new Occ(occupancyEvent(d, t0 + v.leave(), recordDelay, v, unit, DEPARTING, a, occSource), v.leave(), occList.size()));
            visitOfEvent.put(a, v); visitOfEvent.put(d, v);
        }
        applyOccupancyOverlays(ov, occList, visitOfEvent, t0, recordDelay);
        occList.sort(Comparator.comparingLong(Occ::deliverAt).thenComparingLong(o -> o.value().get("eventTime").longValue())
            .thenComparing(o -> o.value().get("eventId").textValue()).thenComparingInt(Occ::order));

        // The telemetry end: E = max(telemetryTo, lastBusinessEvent) + tailMs (one rule for every scenario, plan Task 16).
        long lastBusiness = 0;   // the session-up at T0
        for (Occ o : occList) lastBusiness = Math.max(lastBusiness, Math.max(o.value().get("eventTime").longValue() - t0, o.deliverAt()));
        for (Visit v : visits) lastBusiness = Math.max(lastBusiness, v.leave());
        for (Stamp st : stamps) lastBusiness = Math.max(lastBusiness, st.to() == Long.MAX_VALUE ? st.from() : st.to());
        for (Revision r : revisions) {
            lastBusiness = Math.max(lastBusiness, r.deliverAt());
            lastBusiness = Math.max(lastBusiness, r.value().get("detectedAt").longValue() - t0);
            lastBusiness = Math.max(lastBusiness, r.value().get("revisionEventTime").longValue() - t0);
        }
        lastBusiness = Math.max(lastBusiness, ov.lastBusiness());
        for (Overlays.HoldBack h : ov.holdBacks)   // the delivery of the last held scan
            if (h.to() > from) lastBusiness = Math.max(lastBusiness, from + Math.floorDiv(h.to() - 1 - from, scanMs) * scanMs + h.delayMs());
        List<String> horizonRefs = new ArrayList<>();
        s.path("horizonDeadlines").forEach(h -> horizonRefs.add(h.asText()));
        if (!full && !horizonRefs.isEmpty()) throw refuse("horizonDeadlines need the registry's horizonMs");
        for (String ref : horizonRefs) lastBusiness = Math.max(lastBusiness, enterOf(ref, visits, stamps) + rule.horizon());
        // onlineExpiresAt (spec 521): leave + H + L for a visit with a delivered departure, enter + H + L for an open one.
        List<String> expiryRefs = new ArrayList<>();
        s.path("onlineExpiryDeadlines").forEach(h -> expiryRefs.add(h.asText()));
        if (!full && !expiryRefs.isEmpty()) throw refuse("onlineExpiryDeadlines need the registry's horizonMs");
        if (!expiryRefs.isEmpty() && !mode.equals("occupancy-join")) throw refuse("onlineExpiryDeadlines on a plc-stamped scenario");
        Map<String, Long> expiry = new LinkedHashMap<>();
        for (String ref : expiryRefs) {
            Visit v = null;
            for (Visit x : visits) if (("visit-" + x.index()).equals(ref)) v = x;
            if (v == null) throw refuse("onlineExpiryDeadlines names no visit " + ref);
            long base = v.enter();
            for (Occ o : occList) {
                ObjectNode x = o.value();
                if (DEPARTING.equals(x.get("bizStep").asText()) && !x.has("errorDeclaration") && ("occ-" + (2 * v.index() - 1)).equals(x.path("arrivalEventId").asText()))
                    base = Math.max(base, x.get("eventTime").longValue() - t0);
            }
            expiry.put(ref, base + rule.horizon() + rule.lateness());
            lastBusiness = Math.max(lastBusiness, base + rule.horizon() + rule.lateness());
        }
        // Injection 23's planned A (current spec 592): the hole first, then A = holeAt + H - 1 and the batch boundary T = A + O + 1.
        Overlays.PlannedLate late = ov.plannedLate.isEmpty() ? null : ov.plannedLate.get(0);
        long lateA = 0, lateT = 0;
        String lateRef = null;
        if (late != null) {
            if (!devices.get(late.device()).path("members").has(late.metric())) throw refuse("planned-late-samples metric " + late.metric() + " is not a member of " + late.device());
            lateA = late.holeAt() + rule.horizon() - 1;
            lateT = lateA + oooMs + 1;
            if ((late.holeAt() - from) % scanMs != 0 || (lateT - from) % scanMs != 0 || late.holeAt() < from)
                throw refuse("planned-late-samples: holeAt " + late.holeAt() + " and T " + lateT + " must lie on the scan grid");
            for (Visit v : visits) if (v.station().equals(stationOf.get(late.device())) && v.enter() <= late.holeAt() && late.holeAt() < v.leave()) lateRef = "visit-" + v.index();
            if (lateRef == null) throw refuse("planned-late-samples: holeAt " + late.holeAt() + " lies in no visit of " + stationOf.get(late.device()));
            lastBusiness = Math.max(lastBusiness, lateT);   // the planned cut; the late samples follow it in the next batch
        }
        long end = Math.max(to, lastBusiness) + tailMs;
        if (end < from || (end - from) % scanMs != 0) throw refuse("telemetry end " + end + " is not on the scan grid from " + from);
        long scans = (end - from) / scanMs + 1;

        // Deadlines: settlement of every visit the rule evaluates (leave + L), every listed horizon (enter + H), and of every closed
        // incident (suspectTo + L).
        List<Due> dues = new ArrayList<>();
        if (mode.equals("occupancy-join")) {
            if (!full) for (Visit v : visits) dues.add(new Due("visit-settlement", "visit-" + v.index(), v.station(), v.leave() + latenessMs));
            else {
                TreeMap<Integer, TreeSet<Long>> leaves = new TreeMap<>();
                for (Occ o : occList) {
                    ObjectNode x = o.value();
                    if (!DEPARTING.equals(x.get("bizStep").asText()) || x.has("errorDeclaration")) continue;
                    Visit v = visitOfEvent.get(x.get("arrivalEventId").asText());
                    leaves.computeIfAbsent(v.index(), k -> new TreeSet<>()).add(x.get("eventTime").longValue() - t0);
                }
                leaves.forEach((i, ls) -> { for (long l : ls) dues.add(new Due("visit-settlement", "visit-" + i, visits.get(i - 1).station(), l + latenessMs)); });
            }
        } else for (Stamp st : stamps) if (st.closes()) dues.add(new Due("visit-settlement", st.visitId(), st.station(), st.to() + latenessMs));
        for (String ref : horizonRefs) dues.add(new Due("visit-horizon", ref, stationOfRef(ref, visits, stamps), enterOf(ref, visits, stamps) + rule.horizon()));
        expiry.forEach((ref, at) -> dues.add(new Due("visit-online-expiry", ref, stationOfRef(ref, visits, stamps), at)));
        Due lateDue = late == null ? null : new Due("planned-late", lateRef, stationOf.get(late.device()), lateA);
        if (lateDue != null) dues.add(lateDue);
        Map<String, Long> latestSuspectTo = new TreeMap<>();
        Map<String, Long> latestRevision = new TreeMap<>();
        Map<String, String> incidentStation = new TreeMap<>();
        for (Revision r : revisions) if (r.revision() >= latestRevision.getOrDefault(r.incidentId(), 0L)) {
            latestRevision.put(r.incidentId(), r.revision());
            incidentStation.put(r.incidentId(), r.stationRef());
            if (r.suspectTo() == null) latestSuspectTo.remove(r.incidentId()); else latestSuspectTo.put(r.incidentId(), r.suspectTo());
        }
        latestSuspectTo.forEach((id, st) -> dues.add(new Due("incident-settlement", id, incidentStation.get(id), st + latenessMs)));

        // Batch boundaries (scan times): every batchMs, the two brackets of every deadline, the last scan. A shifted source clock moves
        // the scan whose sourceTs reaches d + O.
        TreeSet<Long> bounds = new TreeSet<>();
        for (long b = from + batchMs; b <= end; b += batchMs) bounds.add(b);
        if (s.has("denseBatches")) {   // a synthesized scenario: a cut every denseBatches.batchMs up to denseBatches.until, then every batchMs
            long dense = Json.safeLong(s.path("denseBatches").get("batchMs"), "denseBatches.batchMs", 1, Json.MAX_SAFE);
            long until = Json.safeLong(s.path("denseBatches").get("until"), "denseBatches.until", Json.NON_NEGATIVE);
            for (long b = from + dense; b <= Math.min(until, end); b += dense) bounds.add(b);
        }
        Map<Due, long[]> bracket = new LinkedHashMap<>();
        for (Due d : dues) {
            long shift = Long.MIN_VALUE;
            for (String dev : devices.keySet()) if (stationOf.get(dev).equals(d.station())) shift = Math.max(shift, ov.shiftOf(dev));
            long limit = d.at() + oooMs - (shift == Long.MIN_VALUE ? 0 : shift);
            if (limit < from) throw refuse("deadline " + d.ref() + " before the telemetry starts");
            long a = from + Math.floorDiv(Math.min(limit, end) - from, scanMs) * scanMs;
            long b = a + scanMs;
            if (b > end) throw refuse("deadline " + d.kind() + " " + d.ref() + " has no bracketing cuts inside the telemetry");
            bounds.add(a); bounds.add(b);
            bracket.put(d, new long[] {a, b});
        }
        bounds.add(end);
        List<Long> boundaries = new ArrayList<>(bounds);

        // Records, in generation order per topic; each assigned to the batch of its delivery time.
        List<Item> items = new ArrayList<>();
        Map<String, String> definitionSha = new TreeMap<>();
        ObjectNode definitions = Json.object();
        for (Map.Entry<String, JsonNode> d : devices.entrySet()) {
            String model = Json.text(d.getValue(), "model");
            String sha = Canonical.digestOf((d.getKey() + ":" + model).getBytes(StandardCharsets.UTF_8));
            definitionSha.put(d.getKey(), sha);
            definitions.putObject(d.getKey()).put("templateRef", model).put("version", "1.0.0").put("sha256", sha);
        }
        for (String d : devices.keySet())
            items.add(new Item("ot.sessions", routing.rawPartitionOf(d), d, batchOf(boundaries, 0, "session-up " + d), sessionEvent(edge, d, t0, 0, "session-up", 1, upPayload(t0))));
        for (Occ o : occList) {
            String rp = o.value().get("readPoint").textValue();
            items.add(new Item("mfg.occupancy", routing.partitionOf(rp), rp, batchOf(boundaries, o.deliverAt(), o.value().get("eventId").textValue()), o.value()));
        }

        Set<String> routed = new TreeSet<>(devices.keySet());   // S1: only equipment of the rule's catalog is routed and advances W1
        if (full) { routed.clear(); for (GroundTruth.Member m : rule.members()) routed.add(m.equipment()); }
        Synthesizer syn = null;
        if (synthetic) streaming[0] = syn = new Synthesizer(out, routing.subscribed("ot.messages"), boundaries.size(), stations, oooMs);

        // Telemetry with its sessions: one session per device from T0, replaced where an overlay ends it.
        final class Session { long birth = t0, bdSeq = 0, seq = 1, lastSeq = 1; Long lastTs; }
        Map<String, Session> session = new HashMap<>();
        devices.keySet().forEach(d -> session.put(d, new Session()));
        List<Item> sessionItems = new ArrayList<>();
        List<GroundTruth.Sample> gtSamples = new ArrayList<>();
        Map<String, Integer> duplicates = new TreeMap<>();
        Map<String, Long> seqTime = new HashMap<>();
        Map<String, Long> maxTs = new TreeMap<>();
        long samples = 0;
        for (long i = 0; i < scans; i++) {
            long t = from + i * scanMs;
            for (Map.Entry<String, JsonNode> d : devices.entrySet()) {
                String dev = d.getKey();
                Session ss = session.get(dev);
                // Session overlays in force at this scan.
                boolean removed = false;
                for (Overlays.SessionDown x : ov.downs) if (x.device().equals(dev)) {
                    if (t == x.at()) sessionItems.add(new Item("ot.sessions", routing.rawPartitionOf(dev), dev, batchOf(boundaries, t, "session-down"),
                        sessionEvent(edge, dev, ss.birth, ss.bdSeq, "session-down", ss.lastSeq, Json.putSafe(Json.object(), "from", t0 + (ss.lastTs == null ? t : ss.lastTs)).putNull("to"))));
                    if (x.at() <= t && t < x.upAt()) removed = true;
                    if (t == x.upAt()) { ss.birth = t0 + t; ss.bdSeq++; ss.seq = 1; addUp(sessionItems, routing, boundaries, edge, dev, ss.birth, ss.bdSeq, t, t0 + t); }
                }
                for (Overlays.SessionAmbiguous x : ov.ambiguous) if (x.device().equals(dev)) {
                    if (t == x.detectedEventTime()) {
                        ObjectNode p = Json.object();
                        Json.putSafe(p, "detectedEventTime", t0 + x.detectedEventTime()); Json.putSafe(p, "rawPublishTs", t0 + x.rawPublishTs()); Json.putSafe(p, "bdSeq", x.bdSeq());
                        p.put("payloadDigest", Canonical.digestOf(("ndeath:" + edge + ":" + dev + ":" + x.bdSeq()).getBytes(StandardCharsets.UTF_8)));
                        sessionItems.add(new Item("ot.sessions", routing.rawPartitionOf(dev), dev, batchOf(boundaries, t, "session-ambiguous"),
                            sessionEvent(edge, dev, ss.birth, ss.bdSeq, "session-ambiguous", ss.lastSeq, p)));
                    }
                    if (x.detectedEventTime() <= t && t < x.upAt()) removed = true;
                    if (t == x.upAt()) { ss.birth = t0 + t; ss.bdSeq = x.bdSeq() + 1; ss.seq = 1; addUp(sessionItems, routing, boundaries, edge, dev, ss.birth, ss.bdSeq, t, t0 + t); }
                }
                for (Overlays.StaleDeath x : ov.staleDeaths) if (x.device().equals(dev) && t == x.detectedEventTime()) {
                    ObjectNode p = Json.object();
                    Json.putSafe(p, "detectedEventTime", t0 + x.detectedEventTime()); Json.putSafe(p, "rawPublishTs", t0 + x.rawPublishTs()); Json.putSafe(p, "bdSeq", x.bdSeq());
                    p.put("payloadDigest", Canonical.digestOf(("ndeath:" + edge + ":" + dev + ":" + x.bdSeq()).getBytes(StandardCharsets.UTF_8)));
                    sessionItems.add(new Item("ot.sessions", routing.rawPartitionOf(dev), dev, batchOf(boundaries, t, "stale-death"),
                        sessionEvent(edge, dev, ss.birth, ss.bdSeq, "stale-death", ss.lastSeq, p)));
                }
                if (removed) continue;
                long sq = ++ss.seq;   // first DDATA of a session carries seq 2
                boolean gap = false;
                for (Overlays.SessionGap g : ov.gaps) if (g.device().equals(dev) && ss.bdSeq == 0 && g.fromSeq() < sq && sq < g.toSeq()) gap = true;
                if (gap) continue;
                for (Overlays.SessionGap g : ov.gaps) if (g.device().equals(dev) && ss.bdSeq == 0 && sq == g.toSeq()) {
                    ObjectNode p = Json.object();
                    Json.putSafe(p, "from", seqTime.get(dev + "|" + g.fromSeq())); Json.putSafe(p, "to", t0 + t + ov.shiftOf(dev)); Json.putSafe(p, "missing", g.toSeq() - g.fromSeq() - 1);
                    sessionItems.add(new Item("ot.sessions", routing.rawPartitionOf(dev), dev, batchOf(boundaries, t, "seq-gap"), sessionEvent(edge, dev, ss.birth, ss.bdSeq, "seq-gap", sq, p)));
                }
                boolean isStation = stationOf.get(dev).equals(dev);
                long sourceTs = t0 + t + ov.shiftOf(dev);
                if (!ov.gaps.isEmpty()) seqTime.put(dev + "|" + sq, sourceTs);
                ObjectNode v = Json.object();
                v.put("edge", edge);
                v.put("device", dev);
                Json.putSafe(v, "birthPublishTs", ss.birth);
                Json.putSafe(v, "bdSeq", ss.bdSeq);
                Json.putSafe(v, "seq", sq);
                Json.putSafe(v, "publishTs", t0 + t + publishDelay);
                Json.putSafe(v, "bridgeReceivedAt", t0 + t + bridgeDelay);
                v.put("definitionSha256", definitionSha.get(dev));
                ArrayNode members = v.putArray("members");
                boolean running = isStation ? stationOccupied(visits, dev, t) : toolRunning(visits, dev, t, startAfter, endBefore) || ov.forcedActive(dev, t);
                Stamp stamp = null, lastStamp = null;
                for (Stamp st : stamps) if (st.equipment().contains(dev)) {
                    if (st.from() <= t && t < st.to()) stamp = st;
                    if (st.from() <= t && (lastStamp == null || st.from() > lastStamp.from())) lastStamp = st;
                }
                boolean mixed = ov.mixedSnapshot(dev, t);
                int batch = batchOf(boundaries, ov.telemetryDeliverAt(dev, t), "scan " + t);
                List<Object[]> memberFacts = new ArrayList<>();
                Iterator<Map.Entry<String, JsonNode>> it = d.getValue().path("members").fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> m = it.next();
                    String metric = m.getKey(), type = m.getValue().asText();
                    if (late != null && dev.equals(late.device()) && t == late.holeAt() && metric.equals(late.metric())) continue;   // the planned hole
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
                    Long badCode = ov.badCode(dev, metric, t);
                    Json.putSafe(mo, "uaStatusCode", badCode == null ? 0 : badCode);
                    Json.putSafe(mo, "quality", badCode == null ? 192 : 0);
                    long ts = mixed && IDENTITY_METRICS.contains(metric) ? sourceTs - scanMs : sourceTs;
                    Json.putSafe(mo, "sourceTs", ts);
                    Json.putSafe(mo, "serverTs", t0 + t + serverDelay);
                    memberFacts.add(new Object[] {metric, ts, badCode == null ? 0L : badCode, mo.get("value")});
                    samples++;
                }
                ss.lastSeq = sq;
                ss.lastTs = t;
                if (syn != null) syn.message(routing.rawPartitionOf(dev), dev, batch, v, routed.contains(dev) ? stationOf.get(dev) : null);
                else items.add(new Item("ot.messages", routing.rawPartitionOf(dev), dev, batch, v));
                maxTs.merge(dev, sourceTs, Math::max);
                for (Object[] f : memberFacts)
                    gtSamples.add(new GroundTruth.Sample(edge, ss.birth, ss.bdSeq, sq, t0 + t + publishDelay, dev, (String) f[0], (Long) f[1], (Long) f[2], (JsonNode) f[3], batch, !mixed,
                        lastStamp == null ? "" : lastStamp.visitId(), lastStamp == null ? "" : lastStamp.unit(), stamp != null));
                // Injection 23: after the first scan past the planned boundary T, three late DDATA of one member each, seq continuing the
                // session, sourceTs holeAt, holeAt - 1, holeAt - 2 (A - H + 1, A - H, A - H - 1), published 1, 2, 3 ms after that scan's
                // message; the value is the seeded torque of the scan the sourceTs falls in (holeAt: the removed observation's value).
                if (late != null && dev.equals(late.device()) && t == lateT + scanMs) {
                    String type = d.getValue().path("members").path(late.metric()).asText();
                    for (int j = 0; j < 3; j++) {
                        long lt = late.holeAt() - j, lsq = ++ss.seq;
                        ObjectNode lv = Json.object();
                        lv.put("edge", edge);
                        lv.put("device", dev);
                        Json.putSafe(lv, "birthPublishTs", ss.birth);
                        Json.putSafe(lv, "bdSeq", ss.bdSeq);
                        Json.putSafe(lv, "seq", lsq);
                        Json.putSafe(lv, "publishTs", t0 + t + publishDelay + j + 1);
                        Json.putSafe(lv, "bridgeReceivedAt", t0 + t + bridgeDelay + j + 1);
                        lv.put("definitionSha256", definitionSha.get(dev));
                        ObjectNode mo = lv.putArray("members").addObject();
                        mo.put("metric", late.metric());
                        long scan = Math.floorDiv(lt - from, scanMs) * scanMs + from;
                        boolean lateRunning = toolRunning(visits, dev, scan, startAfter, endBefore) || ov.forcedActive(dev, scan);
                        JsonNode value = switch (late.metric() + ":" + type) {
                            case "Torque:Double" -> new DecimalNode(lateRunning ? Telemetry.torque(seed, dev, scan / scanMs, tMin, tMax) : Telemetry.IDLE);
                            default -> throw refuse("planned-late-samples on " + late.metric() + " " + type + " (Torque only)");
                        };
                        mo.set("value", value);
                        Json.putSafe(mo, "uaStatusCode", 0);
                        Json.putSafe(mo, "quality", 192);
                        Json.putSafe(mo, "sourceTs", t0 + lt);
                        Json.putSafe(mo, "serverTs", t0 + lt + serverDelay);
                        samples++;
                        syn.message(routing.rawPartitionOf(dev), dev, batch, lv, routed.contains(dev) ? stationOf.get(dev) : null);
                        gtSamples.add(new GroundTruth.Sample(edge, ss.birth, ss.bdSeq, lsq, t0 + t + publishDelay + j + 1, dev, late.metric(), t0 + lt, 0L, value, batch, true, "", "", false));
                        ss.lastSeq = lsq;
                    }
                }
                for (Overlays.Redeliver r : ov.redelivers) if ("ot.messages".equals(r.topic()) && r.device().equals(dev) && r.at() == t) {
                    ObjectNode copy = v.deepCopy();
                    Json.putSafe(copy, "bridgeReceivedAt", t0 + r.deliverAt() + bridgeDelay);
                    items.add(new Item("ot.messages", routing.rawPartitionOf(dev), dev, batchOf(boundaries, r.deliverAt(), "redelivery"), copy));
                    samples += members.size();
                    for (Object[] f : memberFacts) duplicates.merge(sampleKey(edge, dev, ss.birth, (String) f[0], ss.bdSeq, sq, t0 + t + publishDelay), 1, Integer::sum);
                }
            }
        }
        for (Overlays.Redeliver r : ov.redelivers) if ("ot.messages".equals(r.topic()) && duplicates.isEmpty()) throw refuse("redeliver names no delivered message");
        items.addAll(sessionItems);
        revisions.sort(Comparator.comparingLong(Revision::deliverAt).thenComparing(Revision::incidentId).thenComparingLong(Revision::revision));
        for (Revision r : revisions)
            items.add(new Item("quality.incidents", Murmur2.partition(r.equipmentRef(), PARTITIONS), r.equipmentRef(), batchOf(boundaries, r.deliverAt(), r.incidentId()), r.value()));

        // The tail: every device's last delivered sourceTs at least tailMs after the last business event.
        for (Map.Entry<String, Long> e : maxTs.entrySet())
            if (e.getValue() - (t0 + lastBusiness) < tailMs) throw refuse("the tail of " + e.getKey() + " is shorter than tailMs " + tailMs);

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
        if (syn != null) firstPartitions.addAll(syn.firstBatchPartitions());
        else for (int p : routing.subscribed("ot.messages")) if (!byPartition.get("ot.messages/" + p).isEmpty() && byPartition.get("ot.messages/" + p).get(0).batch() == 0) firstPartitions.add(p);
        if (!firstPartitions.equals(new TreeSet<>(routing.subscribed("ot.messages"))))
            throw refuse("the first batch carries no telemetry on some subscribed raw partition (spec 406): " + firstPartitions);

        // W1 per station after every batch (the reference harness's formula): largest delivered sourceTs routed to it - O - 1.
        Map<String, long[]> w1 = new TreeMap<>();
        for (String st : stations) { long[] a = new long[boundaries.size()]; Arrays.fill(a, Long.MIN_VALUE); w1.put(st, a); }
        if (syn != null) syn.contributeW1(w1);
        for (Item it : items) if (it.topic().equals("ot.messages") && routed.contains(it.key())) {
            long[] a = w1.get(stationOf.get(it.key()));
            for (JsonNode m : it.value().path("members")) a[it.batch()] = Math.max(a[it.batch()], m.get("sourceTs").longValue() - oooMs - 1);
        }
        for (long[] a : w1.values()) for (int k = 1; k < a.length; k++) a[k] = Math.max(a[k], a[k - 1]);

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
                    if (syn != null && topic.equals("ot.messages")) last = syn.through(p, k);
                    else for (int i = 0; i < l.size() && l.get(i).batch() <= k; i++) last = i;
                    Json.putSafe(tp, Integer.toString(p), last);
                }
            }
            b.putArray("idleInputs");
        }
        ArrayNode deadlines = Json.JSON.createArrayNode();
        List<GroundTruth.Deadline> gtDeadlines = new ArrayList<>();
        long settleTarget = Long.MIN_VALUE;
        for (Map.Entry<Due, long[]> e : bracket.entrySet()) {
            Due d = e.getKey();
            int a = boundaries.indexOf(e.getValue()[0]), b = boundaries.indexOf(e.getValue()[1]);
            long at = t0 + d.at();
            long w1a = w1.get(d.station())[a], w1b = w1.get(d.station())[b];
            if (!(w1a < at && at <= w1b) || b != a + 1)
                throw refuse("the overlays leave deadline " + d.kind() + " " + d.ref() + " (" + at + ") without bracketing cuts: W1 " + w1a + ", " + w1b);
            ObjectNode o = deadlines.addObject();
            o.put("kind", d.kind());
            o.put("ref", d.ref());
            Json.putSafe(o, "at", at);
            Json.putSafe(o, "cutA", a + 1);
            Json.putSafe(o, "cutB", b + 1);
            gtDeadlines.add(new GroundTruth.Deadline(d.kind(), d.ref(), d.station(), at, a, b));
            settleTarget = Math.max(settleTarget, at);
            if (d == lateDue) {   // the planned cut: every subscribed raw partition's largest sourceTs is T, so W1 = T - O - 1 = A exactly
                if (w1b != at) throw refuse("planned-late-samples: W1 " + w1b + " at the planned cut is not A " + at);
                for (int p : routing.subscribed("ot.messages"))
                    if (syn.maxSourceTsThrough(p, b) != t0 + lateT) throw refuse("planned-late-samples: raw partition " + p + " does not end at T " + (t0 + lateT) + " at the planned cut");
            }
        }
        if (settleTarget == Long.MIN_VALUE) settleTarget = t0 + end - oooMs - 1;

        // Files.
        TreeMap<String, byte[]> files = new TreeMap<>();
        for (Map.Entry<String, List<Item>> e : byPartition.entrySet()) {
            if (syn != null && e.getKey().startsWith("ot.messages/")) continue;   // streamed; digested by the synthesizer
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
        ObjectNode range = Json.object();
        Map<String, long[]> evidence = new TreeMap<>();
        for (String d : devices.keySet()) { ObjectNode r = range.putObject(d); Json.putSafe(r, "from", t0 + from); Json.putSafe(r, "to", t0 + end); evidence.put(d, new long[] {t0 + from, t0 + end}); }
        ObjectNode expected;
        if (!full) expected = expected(t0, visits, stamps, revisions.stream().map(Revision::incidentId).distinct().sorted().toList(), settleTarget);
        else {
            Map<String, GroundTruth.VisitFacts> facts = new HashMap<>();
            if (mode.equals("occupancy-join")) for (Visit v : visits) facts.put("occ-" + (2 * v.index() - 1), new GroundTruth.VisitFacts("visit-" + v.index(), "occ-" + (2 * v.index() - 1), v.tools()));
            else for (Stamp st : stamps) {
                List<String> tools = List.of();
                for (Visit v : visits) if (v.station().equals(st.station()) && v.enter() <= st.from() && st.from() < v.leave()) tools = v.tools();
                facts.put(st.visitId(), new GroundTruth.VisitFacts(st.visitId(), null, tools));
            }
            List<GroundTruth.Delivery> occD = new ArrayList<>(), sesD = new ArrayList<>(), incD = new ArrayList<>();
            for (Map.Entry<String, List<Item>> e : byPartition.entrySet()) for (Item it : e.getValue()) switch (it.topic()) {
                case "mfg.occupancy" -> occD.add(new GroundTruth.Delivery(it.topic(), it.value(), it.batch()));
                case "ot.sessions" -> sesD.add(new GroundTruth.Delivery(it.topic(), it.value(), it.batch()));
                case "quality.incidents" -> incD.add(new GroundTruth.Delivery(it.topic(), it.value(), it.batch()));
                default -> { }
            }
            Comparator<GroundTruth.Delivery> byBatch = Comparator.comparingInt(GroundTruth.Delivery::batch);
            occD.sort(byBatch); sesD.sort(byBatch); incD.sort(byBatch);
            expected = new GroundTruth(rule, occSource, stationOf, gtSamples, occD, sesD, incD, w1, boundaries.size(), facts, evidence, duplicates)
                .expected(t0, gtDeadlines, settleTarget, null, synthetic ? scanMs : 0);
        }
        files.put(Recording.EXPECTED, Json.pretty(expected));

        ObjectNode manifest = Json.object();
        Json.putSafe(manifest, "recordingVersion", 1);
        manifest.put("name", name);
        manifest.put("synthetic", synthetic);
        ObjectNode source = manifest.putObject("source");
        source.put("scenario", name);
        source.put("scenarioSha256", scenarioSha256);
        source.put("generator", synthetic ? Synthesizer.GENERATOR : GENERATOR);
        Json.putSafe(source, "seed", seed);
        ArrayNode overlays = source.putArray("overlays");
        ov.all.forEach(overlays::add);
        manifest.set("rule", s.path("rule").deepCopy());
        manifest.set("definitions", definitions);
        Json.putSafe(manifest, "T0", t0);
        Json.putSafe(manifest, "scanMs", scanMs);
        manifest.set("evidenceRange", range);
        manifest.set("batches", batches);
        manifest.set("deadlines", deadlines);
        Json.putSafe(manifest, "settleTargetW", settleTarget);
        Json.putSafe(manifest, "tailMs", tailMs);
        if (s.has("subscribed")) manifest.set("subscribe", s.get("subscribed").deepCopy());
        TreeMap<String, String> fileDigests = new TreeMap<>();
        files.forEach((f, b) -> fileDigests.put(f, Canonical.digestOf(b)));
        if (syn != null) fileDigests.putAll(syn.close());
        ObjectNode digests = manifest.putObject("digests");
        fileDigests.forEach(digests::put);
        byte[] manifestBytes = Json.pretty(manifest);

        if (syn == null) write(out, files, manifestBytes);
        else writeFiles(out, files, manifestBytes);

        long messages = syn != null ? syn.messages() : items.stream().filter(i -> i.topic().equals("ot.messages")).count();
        long sessions = items.stream().filter(i -> i.topic().equals("ot.sessions")).count();
        long occCount = items.stream().filter(i -> i.topic().equals("mfg.occupancy")).count();
        long incidents = items.stream().filter(i -> i.topic().equals("quality.incidents")).count();
        ObjectNode lock = Json.object();
        Json.putSafe(lock, "lockVersion", 1);
        lock.put("scenario", name);
        lock.put("scenarioSha256", scenarioSha256);
        lock.put("generator", synthetic ? Synthesizer.GENERATOR : GENERATOR);
        ObjectNode counts = lock.putObject("counts");
        Json.putSafe(counts, "messages", messages);
        Json.putSafe(counts, "samples", samples);
        Json.putSafe(counts, "sessions", sessions);
        Json.putSafe(counts, "occupancy", occCount);
        Json.putSafe(counts, "incidents", incidents);
        Json.putSafe(counts, "cuts", boundaries.size());
        ArrayNode fb = lock.putArray("firstBatchTelemetryPartitions");
        firstPartitions.forEach(fb::add);
        if (!ov.holdBacks.isEmpty()) {
            ArrayNode hb = lock.putArray("heldBack");
            for (Overlays.HoldBack h : ov.holdBacks) { ObjectNode o = hb.addObject(); Json.putSafe(o, "from", h.from()); Json.putSafe(o, "to", h.to()); Json.putSafe(o, "delayMs", h.delayMs()); }
        }
        if (s.has("subscribed")) lock.set("subscribe", s.get("subscribed").deepCopy());
        ObjectNode lf = lock.putObject("files");
        TreeMap<String, String> all = new TreeMap<>(fileDigests);
        all.put(Recording.MANIFEST, Canonical.digestOf(manifestBytes));
        all.forEach(lf::put);
        return new Built(messages, samples, sessions, occCount, incidents, boundaries.size(), lock);
    }

    private static void applyOccupancyOverlays(Overlays ov, List<Occ> occ, Map<String, Visit> visitOf, long t0, long recordDelay) {
        Map<String, Occ> byId = new HashMap<>();
        for (Occ o : occ) byId.put(o.value().get("eventId").textValue(), o);
        java.util.function.Function<String, Occ> original = id -> {
            Occ o = byId.get(id);
            if (o == null) throw refuse("overlay names no occupancy event " + id);
            return o;
        };
        for (Overlays.DelayOccupancy d : ov.delays) {
            Occ o = original.apply(d.eventId());
            Occ moved = new Occ(o.value(), d.deliverAt(), o.order());
            occ.set(occ.indexOf(o), moved);
            byId.put(d.eventId(), moved);
        }
        for (Overlays.Redeliver r : ov.redelivers) if ("mfg.occupancy".equals(r.topic())) {
            ObjectNode v = original.apply(r.eventId()).value().deepCopy();
            Json.putSafe(v, "recordTime", t0 + r.deliverAt() + recordDelay);
            occ.add(new Occ(v, r.deliverAt(), occ.size()));
        }
        for (Overlays.ErrorDeclaration e : ov.declarations) {
            ObjectNode v = original.apply(e.eventId()).value().deepCopy();
            Json.putSafe(v, "recordTime", t0 + e.deliverAt() + recordDelay);
            ObjectNode ed = v.putObject("errorDeclaration");
            Json.putSafe(ed, "declarationTime", t0 + e.declarationTime());
            ed.put("reason", e.reason());
            ed.putArray("correctiveEventIDs");
            occ.add(new Occ(v, e.deliverAt(), occ.size()));
        }
        for (Overlays.ResendDifferent r : ov.resends) {
            ObjectNode v = Overlays.withField(original.apply(r.eventId()).value(), r.field(), r.value());
            Json.putSafe(v, "recordTime", t0 + r.deliverAt() + recordDelay);
            occ.add(new Occ(v, r.deliverAt(), occ.size()));
        }
        for (Overlays.LateCorrection l : ov.lateCorrections) {
            ObjectNode dep = original.apply(l.departureEventId()).value();
            if (!DEPARTING.equals(dep.get("bizStep").asText())) throw refuse("late-correction on an arrival " + l.departureEventId());
            if (byId.containsKey(l.newDepartureEventId())) throw refuse("late-correction reuses the eventId " + l.newDepartureEventId());
            ObjectNode decl = dep.deepCopy();
            Json.putSafe(decl, "recordTime", t0 + l.deliverAt() + recordDelay);
            ObjectNode ed = decl.putObject("errorDeclaration");
            Json.putSafe(ed, "declarationTime", t0 + l.declarationTime());
            ed.put("reason", Overlays.INCORRECT_DATA);
            ed.putArray("correctiveEventIDs").add(l.newDepartureEventId());
            occ.add(new Occ(decl, l.deliverAt(), occ.size()));
            ObjectNode corr = dep.deepCopy();
            corr.put("eventId", l.newDepartureEventId());
            Json.putSafe(corr, "eventTime", t0 + l.newLeave());
            Json.putSafe(corr, "recordTime", t0 + l.deliverAt() + recordDelay);
            occ.add(new Occ(corr, l.deliverAt(), occ.size()));
            visitOf.put(l.newDepartureEventId(), visitOf.get(l.departureEventId()));
        }
        for (String id : ov.noDeparture) {
            Occ o = original.apply(id);
            if (!DEPARTING.equals(o.value().get("bizStep").asText())) throw refuse("no-departure on an arrival " + id);
            occ.removeIf(x -> x.value().get("eventId").textValue().equals(id));
        }
    }

    private static int indexOfStamp(List<Stamp> stamps, String station, long at) {
        for (int i = 0; i < stamps.size(); i++) if (stamps.get(i).station().equals(station) && stamps.get(i).from() < at && at < stamps.get(i).to()) return i;
        throw refuse("id-change-without-false at " + at + " falls in no stamped visit");
    }

    private static long enterOf(String ref, List<Visit> visits, List<Stamp> stamps) {
        for (Visit v : visits) if (("visit-" + v.index()).equals(ref)) return v.enter();
        for (Stamp st : stamps) if (st.visitId().equals(ref)) return st.from();
        throw refuse("horizonDeadlines names no visit " + ref);
    }
    private static String stationOfRef(String ref, List<Visit> visits, List<Stamp> stamps) {
        for (Visit v : visits) if (("visit-" + v.index()).equals(ref)) return v.station();
        for (Stamp st : stamps) if (st.visitId().equals(ref)) return st.station();
        throw refuse("horizonDeadlines names no visit " + ref);
    }

    private static ObjectNode sessionEvent(String edge, String device, long birth, long bdSeq, String kind, long atSeq, ObjectNode payload) {
        ObjectNode v = Json.object();
        v.put("edge", edge);
        v.put("device", device);
        Json.putSafe(v, "birthPublishTs", birth);
        Json.putSafe(v, "bdSeq", bdSeq);
        v.put("kind", kind);
        Json.putSafe(v, "atSeq", atSeq);   // session-up: the DBIRTH, counted as having consumed seq 1 (spec Process messages)
        v.set("payload", payload);
        return v;
    }
    private static ObjectNode upPayload(long at) {
        ObjectNode p = Json.object();
        Json.putSafe(p, "at", at);
        p.put("rebirth", false);
        return p;
    }
    /** A new session's {@code session-up} at scan time {@code t} (milliseconds from T0), {@code at} the same instant in epoch ms. */
    private static void addUp(List<Item> l, RoutingManifest routing, List<Long> boundaries, String edge, String dev, long birth, long bdSeq, long t, long at) {
        l.add(new Item("ot.sessions", routing.rawPartitionOf(dev), dev, batchOf(boundaries, t, "session-up"), sessionEvent(edge, dev, birth, bdSeq, "session-up", 1, upPayload(at))));
    }

    static String sampleKey(String edge, String device, long birth, String metric, long bdSeq, long seq, long publishTs) {
        ObjectNode n = Json.object().put("edge", edge).put("device", device);
        Json.putSafe(n, "birthPublishTs", birth).put("metric", metric);
        Json.putSafe(n, "bdSeq", bdSeq); Json.putSafe(n, "seq", seq); Json.putSafe(n, "publishTs", publishTs);
        return Canonical.utf8(Canonical.bytes(n));
    }

    private static ObjectNode expected(long t0, List<Visit> visits, List<Stamp> stamps, List<String> incidents, long settleTarget) {
        ObjectNode e = Json.object();
        Json.putSafe(e, "expectedVersion", 1);
        e.put("scope", "minimal");   // the scenario's facts only: a smoke scenario carries no full ground truth (its rule is inline)
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

    private static ObjectNode occupancyEvent(String id, long time, long recordDelay, Visit v, String unit, String bizStep, String arrival, String source) {
        ObjectNode o = Json.object();
        o.put("eventId", id);
        Json.putSafe(o, "eventTime", time);
        Json.putSafe(o, "recordTime", time + recordDelay);
        o.put("unitId", unit);
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
        prepare(out);
        writeFiles(out, files, manifest);
    }

    /** Empties {@code out} (creating it), refusing a non-empty directory that is not a recording. */
    static void prepare(Path out) {
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
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    /** Removes everything this build wrote under {@code out} (a refused synthesized build). */
    static void clear(Path out) {
        if (!Files.exists(out)) return;
        try (Stream<Path> w = Files.walk(out)) {
            for (Path p : w.sorted(Comparator.reverseOrder()).toList()) if (!p.equals(out)) Files.deleteIfExists(p);
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    private static void writeFiles(Path out, Map<String, byte[]> files, byte[] manifest) {
        try {
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
