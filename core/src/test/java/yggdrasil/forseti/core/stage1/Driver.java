package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.NegativeControls;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.Ids;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.rules.RuleSet;

/** A tiny in-test driver of one station: routes inputs (S1), keeps a cut's inputs, applies them in contract order, computes W1 (S10)
 *  unless a test sets it, fires due timers in index order and closes the cut. It folds the outputs for the assertions. */
final class Driver {
    static final long T = 1794183000000L;   // T0 + 600000, where the smoke telemetry starts
    static final String SOURCE = "mes-line2", EDGE = "edge-line2";
    static final long BIRTH = 1794182400000L;
    static final String ARRIVING = "urn:epcglobal:cbv:bizstep:arriving", DEPARTING = "urn:epcglobal:cbv:bizstep:departing";
    static final String DID_NOT_OCCUR = "urn:epcglobal:cbv:er:did_not_occur", INCORRECT_DATA = "urn:epcglobal:cbv:er:incorrect_data";
    static final long BAD = 0x80000000L, UNCERTAIN = 0x40000000L;

    final RuleSet rules;
    final Stage1 stage1;
    final Routing routing;
    final String station;
    Stage1State state;
    final List<Object> outputs = new ArrayList<>();
    final List<Routing.Rejected> rejections = new ArrayList<>();
    final List<Inputs.InputRecord> pending = new ArrayList<>();
    final Map<String, Long> offsets = new HashMap<>();
    final Map<String, Long> seqs = new HashMap<>();
    long maxTs = Long.MIN_VALUE;
    Long w1;
    long cutId;
    /** When set, the state is serialized and read back after every cut (the adapters' store round trip). */
    boolean roundTrip;

    Driver(RuleSet rules, String station) { this(rules, station, NegativeControls.NONE); }
    Driver(RuleSet rules, String station, NegativeControls controls) {
        this.rules = rules;
        this.stage1 = new Stage1(rules, controls);
        this.routing = new Routing(rules);
        this.station = station;
        this.state = stage1.newState(1, station);
    }
    static Driver station1() { return new Driver(Fixtures.line2(), Fixtures.S1); }

    // --- inputs ---
    Inputs.InputRecord input(String topic, JsonNode value) {
        try { return input(topic, Json.JSON.writeValueAsBytes(value)); } catch (JsonProcessingException e) { throw new IllegalStateException(e); }
    }
    Inputs.InputRecord input(String topic, byte[] raw) {
        long off = offsets.merge(topic, 1L, Long::sum) - 1;
        return input(topic, 0, off, raw);
    }
    Inputs.InputRecord input(String topic, int partition, long offset, byte[] raw) {
        InputParser.Read read = InputParser.read(new Inputs.Origin(topic, partition, offset), null, raw);
        Routing.Outcome o = routing.route(read);
        if (o instanceof Routing.Rejected r) { rejections.add(r); return read.record(); }
        Routing.Routed routed = (Routing.Routed) o;
        if (!routed.station().equals(station)) return read.record();
        pending.add(read.record());
        if (read.record().parsed() instanceof Inputs.ProcessMessage) { Long t = Events.eventTime(read.record()); if (t != null) maxTs = Math.max(maxTs, t); }
        return read.record();
    }
    /** Applies one input now, outside the cut order (for tests of a single decision). */
    List<Object> admitNow(Inputs.InputRecord r) {
        pending.remove(r);
        Stage1.Result res = stage1.decide(state, new Events.Admit(r, Events.kindRank(r.origin().topic())), state.admissionW());
        state = res.state();
        outputs.addAll(res.outputs());
        return res.outputs();
    }

    void cut() {
        Long w = maxTs == Long.MIN_VALUE ? w1 : Long.valueOf(maxTs - rules.attribution().outOfOrdernessMs() - 1);
        if (w1 != null && (w == null || w < w1)) w = w1;
        cut(w == null ? 0L : w);
    }
    void cut(long w) {
        cutId++;
        List<Inputs.InputRecord> in = new ArrayList<>(pending);
        pending.clear();
        in.sort(Events.CONTRACT_ORDER);
        Long admissionW = state.admissionW();
        for (Inputs.InputRecord r : in) apply(new Events.Admit(r, Events.kindRank(r.origin().topic())), admissionW);
        w1 = w;
        while (true) {
            List<DeadlineIndex.Deadline> due = state.deadlines().due(w);
            if (due.isEmpty()) break;
            DeadlineIndex.Deadline d = due.get(0);
            apply(new Events.Timer(d.deadline(), d.key(), d.purpose()), w);
        }
        apply(new Events.CutClose(cutId, w, List.of()), w);
        if (roundTrip) state = Stage1State.fromJson(Json.read(Canonical.bytes(state.toJson())));
    }
    private void apply(Events.Event e, Long watermark) {
        Stage1.Result res = stage1.decide(state, e, watermark);
        state = res.state();
        outputs.addAll(res.outputs());
    }

    /** Feeds a recording directory cut by cut (its manifest batches), every topic but incidents. */
    void runRecording(Path dir) {
        JsonNode manifest = Json.read(dir.resolve("manifest.json"));
        Map<String, Map<Integer, List<JsonNode>>> records = new TreeMap<>();
        for (String topic : List.of(InputParser.MESSAGES, InputParser.SESSIONS, InputParser.OCCUPANCY)) {
            try (var files = Files.list(dir.resolve(topic))) {
                for (Path f : files.sorted().toList()) {
                    int p = Integer.parseInt(f.getFileName().toString().replace(".jsonl", ""));
                    List<JsonNode> l = new ArrayList<>();
                    for (String line : Files.readAllLines(f)) if (!line.isBlank()) l.add(Json.read(line.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                    records.computeIfAbsent(topic, k -> new TreeMap<>()).put(p, l);
                }
            } catch (IOException e) { throw new UncheckedIOException(e); }
        }
        Map<String, Long> prev = new HashMap<>();
        for (JsonNode b : manifest.get("batches")) {
            for (var t : records.entrySet()) for (var p : t.getValue().entrySet()) {
                String k = t.getKey() + "/" + p.getKey();
                long from = prev.getOrDefault(k, -1L), through = b.get("throughOffsets").get(t.getKey()).get(Integer.toString(p.getKey())).longValue();
                for (JsonNode rec : p.getValue()) {
                    long off = rec.get("offset").longValue();
                    if (off > from && off <= through) {
                        try { input(t.getKey(), p.getKey(), off, Json.JSON.writeValueAsBytes(rec.get("value"))); }
                        catch (JsonProcessingException e) { throw new IllegalStateException(e); }
                    }
                }
                prev.put(k, through);
            }
            cut();
        }
    }

    // --- builders ---
    static ObjectNode arrival(String eventId, long t, String unit, String readPoint) {
        ObjectNode n = Json.object().put("eventId", eventId);
        Json.putSafe(n, "eventTime", t);
        Json.putSafe(n, "recordTime", t + 200);
        n.put("unitId", unit).put("action", "OBSERVE").put("bizStep", ARRIVING).put("readPoint", readPoint).put("visitSeq", 1).put("source", SOURCE);
        return n;
    }
    static ObjectNode departure(String eventId, long t, String unit, String readPoint, String arrivalEventId) {
        ObjectNode n = arrival(eventId, t, unit, readPoint).put("bizStep", DEPARTING);
        n.remove("source");
        return n.put("arrivalEventId", arrivalEventId).put("source", SOURCE);
    }
    static ObjectNode declared(ObjectNode event, String reason, long declarationTime) {
        ObjectNode n = event.deepCopy();
        Json.putSafe(n, "recordTime", declarationTime + 100);
        ObjectNode d = Json.putSafe(n.putObject("errorDeclaration"), "declarationTime", declarationTime).put("reason", reason);
        d.putArray("correctiveEventIDs");
        return n;
    }
    static ObjectNode member(String metric, JsonNode value, long status, Long ts) {
        ObjectNode m = Json.object().put("metric", metric);
        m.set("value", value);
        Json.putSafe(m, "uaStatusCode", status).put("quality", status == 0 ? 192 : status == BAD ? 0 : 64);
        if (ts != null) { Json.putSafe(m, "sourceTs", ts); Json.putSafe(m, "serverTs", ts + 10); }
        return m;
    }
    ObjectNode message(String device, long ts, ObjectNode... members) {
        long seq = seqs.merge(device, 1L, Long::sum) + 1;
        ObjectNode n = Json.object().put("edge", EDGE).put("device", device);
        Json.putSafe(n, "birthPublishTs", BIRTH).put("bdSeq", 0);
        Json.putSafe(n, "seq", seq);
        Json.putSafe(n, "publishTs", ts + 50);
        Json.putSafe(n, "bridgeReceivedAt", ts + 120);
        n.put("definitionSha256", rules.pins().get("definitions").get(device).get("sha256").asText());
        n.putArray("members").addAll(List.of(members));
        return n;
    }
    static ObjectNode session(String device, String kind, long atSeq, ObjectNode payload) {
        ObjectNode n = Json.object().put("edge", EDGE).put("device", device);
        Json.putSafe(n, "birthPublishTs", BIRTH).put("bdSeq", 0).put("kind", kind);
        Json.putSafe(n, "atSeq", atSeq);
        n.set("payload", payload);
        return n;
    }

    /** One override of a scan member: a value (null removes the member) and a status code. */
    record Override(String device, String metric, JsonNode value, long status) {}
    static Override over(String device, String metric, JsonNode value, long status) { return new Override(device, metric, value, status); }

    /** One scan of station1's devices at {@code t}: station1 Running true, nutrunner1 Torque 20.5 and Running true, nutrunner2 Torque 0.0
     *  and Running false; for station2 its Running true. */
    List<Inputs.InputRecord> scan(long t, Override... overrides) {
        Map<String, List<String[]>> devices = new LinkedHashMap<>();
        if (station.equals(Fixtures.S1)) {
            devices.put(Fixtures.S1, Collections.singletonList(new String[]{"Running", "true"}));
            devices.put(Fixtures.NR1, List.of(new String[]{"Torque", "20.5"}, new String[]{"Running", "true"}));
            devices.put(Fixtures.NR2, List.of(new String[]{"Torque", "0.0"}, new String[]{"Running", "false"}));
        } else devices.put(station, Collections.singletonList(new String[]{"Running", "true"}));
        List<Inputs.InputRecord> out = new ArrayList<>();
        for (var d : devices.entrySet()) {
            List<ObjectNode> ms = new ArrayList<>();
            for (String[] m : d.getValue()) {
                JsonNode v = m[1].equals("true") || m[1].equals("false") ? BooleanNode.valueOf(Boolean.parseBoolean(m[1])) : new DoubleNode(Double.parseDouble(m[1]));
                long status = 0;
                boolean drop = false;
                for (Override o : overrides) if (o.device().equals(d.getKey()) && o.metric().equals(m[0])) { if (o.value() == null) drop = true; else { v = o.value(); status = o.status(); } }
                if (!drop) ms.add(member(m[0], v, status, t));
            }
            out.add(input(InputParser.MESSAGES, message(d.getKey(), t, ms.toArray(new ObjectNode[0]))));
        }
        return out;
    }
    /** Scans every second over {@code [from, to)}. */
    void scans(long from, long to, Override... overrides) { for (long t = from; t < to; t += 1000) scan(t, overrides); }

    // --- folds ---
    static ObjectNode visitRef(String arrivalEventId) {
        return Json.object().put("mode", "occupancy-join").put("source", SOURCE).put("arrivalEventId", arrivalEventId);
    }
    List<Row> rows(String topic, JsonNode key) {
        String k = Canonical.utf8(Canonical.bytes(key));
        List<Row> l = new ArrayList<>();
        for (Object o : outputs) if (o instanceof Row r && r.topic().equals(topic) && Canonical.utf8(Canonical.bytes(r.logicalKey())).equals(k)) l.add(r);
        return l;
    }
    List<Row> workRecordRows(String arrivalEventId) { return rows(Stage1.WORK_RECORDS, visitRef(arrivalEventId)); }
    JsonNode workRecord(String arrivalEventId) {
        List<Row> l = workRecordRows(arrivalEventId);
        return l.isEmpty() ? null : l.get(l.size() - 1).body();
    }
    static JsonNode sampleId(Inputs.InputRecord message, String metric) { return Ids.sampleId((Inputs.ProcessMessage) message.parsed(), metric); }
    JsonNode sampleRow(Inputs.InputRecord message, String metric) {
        List<Row> l = rows(Stage1.SAMPLES, sampleId(message, metric));
        return l.isEmpty() ? null : l.get(l.size() - 1).body();
    }
    List<Disposition> dispositions(Inputs.InputRecord r) {
        List<Disposition> l = new ArrayList<>();
        for (Object o : outputs) if (o instanceof Disposition d && d.key().inputTopic().equals(r.origin().topic())
            && d.key().inputPartition() == r.origin().partition() && d.key().inputOffset() == r.origin().offset()) l.add(d);
        return l;
    }
    /** The latest disposition revision of one part of an input. */
    Disposition disposition(Inputs.InputRecord r, String part) {
        Disposition last = null;
        for (Disposition d : dispositions(r)) if (d.key().part().equals(part)) last = d;
        return last;
    }
    static ObjectNode memberEntry(JsonNode body, String mappingId) {
        for (JsonNode m : body.get("members")) if (m.get("mappingId").asText().equals(mappingId)) return (ObjectNode) m;
        return null;
    }
    static ObjectNode evidence(JsonNode body, String equipmentPath) {
        for (JsonNode m : body.get("equipmentEvidence")) if (m.get("equipmentPath").asText().equals(equipmentPath)) return (ObjectNode) m;
        return null;
    }
    static List<String> reasons(JsonNode body) { List<String> l = new ArrayList<>(); body.get("insufficiencyReasons").forEach(x -> l.add(x.asText())); return l; }

    /** Every output as comparable JSON (rows without their adapter-stamped pins). */
    List<String> outputText() {
        List<String> l = new ArrayList<>();
        for (Object o : outputs) l.add(Stage1.describe(o));
        return l;
    }
    List<Progress> progress() { List<Progress> l = new ArrayList<>(); for (Object o : outputs) if (o instanceof Progress p) l.add(p); return l; }
}
