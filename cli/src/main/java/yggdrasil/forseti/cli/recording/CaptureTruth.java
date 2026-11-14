package yggdrasil.forseti.cli.recording;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;

/** The ground truth and the deadlines of a captured (step-5) recording, through the step-3 {@link GroundTruth} (step-5 plan D6, PR 6).
 *
 *  <p>What is derived from what. The builder hands {@link GroundTruth} the records it generated; a capture hands it the records the
 *  broker delivered, read back from the input topics, each with the batch the recorder placed it in. So every sample, occupancy
 *  event, session event and incident revision of {@code expected.json} is a captured record, and nothing is taken from the drive's
 *  intent: a scan Muninn never published is absent here as it is absent from the input. The drive contributes only the facts no
 *  record carries: the reference name of each visit and the tools the visit uses ({@link Visit}), taken from the live scenario the
 *  driver ran, and (in the recorder) the check that every occupancy and incident record the drive log says was sent was captured.
 *  The deadlines are the builder's: the settlement {@code leave + L} of every visit that a delivered departure closes (occupancy-join)
 *  or that a coherent {@code VisitPresent=false} snapshot closes (plc-stamped, the visits {@link GroundTruth#plcClaims} derives), and
 *  {@code suspectTo + L} of the latest revision of every incident with a bound. */
public final class CaptureTruth {
    private CaptureTruth() {}

    /** A visit's facts no record carries: its reference name and the tools it uses. {@code arrivalEventId} keys an occupancy-join visit
     *  and is null for a plc-stamped one (keyed by its visitId). */
    public record Visit(String visitRef, String arrivalEventId, List<String> tools) {}
    /** One captured input record and the batch (0-based) the recorder placed it in. */
    public record Input(String topic, int partition, long offset, String key, byte[] value, int batch) {}
    /** A deadline of the ground truth: kind, visit or incident reference, station, epoch milliseconds. */
    public record Due(String kind, String ref, String station, long at) {}
    /** A deadline and the 0-based batches that bracket it. */
    public record Bracket(Due due, int cutA, int cutB) {}
    /** The rule parameters the recorder plans with. */
    public record Temporal(String mode, String crossCheck, long outOfOrdernessMs, long allowedLatenessMs, long expectedPeriodMs) {}

    public static Temporal temporal(Path registry, JsonNode pin) {
        GroundTruth.Rule r = GroundTruth.load(registry, pin);
        return new Temporal(r.mode(), r.crossCheck(), r.ooo(), r.lateness(), r.period());
    }

    /** The deadlines of the capture (their brackets are the recorder's to plan). Sorted by time, then kind and reference. */
    public static List<Due> deadlines(Path registry, JsonNode pin, Map<String, String> stationOf, List<Input> inputs, Map<String, Visit> facts) {
        GroundTruth.Rule rule = GroundTruth.load(registry, pin);
        List<Due> dues = new ArrayList<>();
        if (rule.mode().equals("plc-stamped")) {
            List<Input> all = new ArrayList<>();
            for (Input i : inputs) all.add(new Input(i.topic(), i.partition(), i.offset(), i.key(), i.value(), 0));
            GroundTruth gt = truth(rule, "", stationOf, all, null, 1, facts, Map.of(), stationsOf(stationOf));
            for (GroundTruth.Claim c : gt.plcClaims(0)) if (c.leave != null) dues.add(new Due("visit-settlement", c.ref, c.station, c.leave + rule.lateness()));
        } else {
            TreeMap<String, TreeSet<Long>> leaves = new TreeMap<>();
            Map<String, String> stationOfRef = new HashMap<>();
            for (Input i : inputs) {
                if (!i.topic().equals(InputParser.OCCUPANCY)) continue;
                JsonNode v = Json.read(i.value());
                if (!ScenarioBuilder.DEPARTING.equals(v.path("bizStep").asText()) || v.has("errorDeclaration") || !v.path("arrivalEventId").isTextual()) continue;
                Visit f = facts.get(v.get("arrivalEventId").textValue());
                String ref = f == null ? v.get("arrivalEventId").textValue() : f.visitRef();
                leaves.computeIfAbsent(ref, k -> new TreeSet<>()).add(Json.safeLong(v.get("eventTime"), "eventTime", Json.NON_NEGATIVE));
                stationOfRef.put(ref, Json.text(v, "readPoint"));
            }
            leaves.forEach((ref, ls) -> { for (long l : ls) dues.add(new Due("visit-settlement", ref, stationOfRef.get(ref), l + rule.lateness())); });
        }
        Map<String, JsonNode> latest = new TreeMap<>();
        for (Input i : inputs) {
            if (!i.topic().equals(InputParser.INCIDENTS)) continue;
            JsonNode v = Json.read(i.value());
            String id = Json.text(v, "incidentId");
            JsonNode before = latest.get(id);
            if (before == null || v.path("revision").asLong() >= before.path("revision").asLong()) latest.put(id, v);
        }
        latest.forEach((id, v) -> {
            if (v.path("suspectTo").isIntegralNumber()) dues.add(new Due("incident-settlement", id, Json.text(v, "stationRef"), v.get("suspectTo").longValue() + rule.lateness()));
        });
        dues.sort(Comparator.comparingLong(Due::at).thenComparing(Due::kind).thenComparing(Due::ref));
        return List.copyOf(dues);
    }

    /** {@code expected.json} of the capture: {@code w1} per station per batch ({@link ReplayRecording#NO_TELEMETRY} before any routed telemetry),
     *  {@code evidence} per device {@code [from, to]}. */
    public static ObjectNode expected(Path registry, JsonNode pin, String occupancySource, Map<String, String> stationOf, List<Input> inputs,
                                      Map<String, long[]> w1, int batches, Map<String, Visit> facts, Map<String, long[]> evidence, long t0,
                                      List<Bracket> brackets, long settleTarget) {
        GroundTruth.Rule rule = GroundTruth.load(registry, pin);
        GroundTruth gt = truth(rule, occupancySource, stationOf, inputs, w1, batches, facts, evidence, null);
        List<GroundTruth.Deadline> ds = new ArrayList<>();
        for (Bracket b : brackets) ds.add(new GroundTruth.Deadline(b.due().kind(), b.due().ref(), b.due().station(), b.due().at(), b.cutA(), b.cutB()));
        return gt.expected(t0, ds, settleTarget, null);
    }

    private static Set<String> stationsOf(Map<String, String> stationOf) { return new TreeSet<>(stationOf.values()); }

    /** The builder's hand-over, from captured records: samples of every kept member with a {@code sourceTs} (a message whose identity
     *  was delivered before is a redelivery: counted, its samples not repeated), and the occupancy (unless the rule reads none),
     *  session and incident deliveries in batch order. {@code stations} non-null: a one-batch evaluation with no watermark. */
    private static GroundTruth truth(GroundTruth.Rule rule, String source, Map<String, String> stationOf, List<Input> inputs, Map<String, long[]> w1,
                                     int batches, Map<String, Visit> facts, Map<String, long[]> evidence, Set<String> stations) {
        if (stations != null) {
            Map<String, long[]> none = new TreeMap<>();
            for (String s : stations) none.put(s, new long[] {ReplayRecording.NO_TELEMETRY});
            w1 = none;
        }
        List<GroundTruth.Sample> samples = new ArrayList<>();
        List<GroundTruth.Delivery> occ = new ArrayList<>(), ses = new ArrayList<>(), inc = new ArrayList<>();
        Map<String, Integer> duplicates = new TreeMap<>();
        Set<String> seen = new HashSet<>();
        boolean occupancyRead = !(rule.mode().equals("plc-stamped") && rule.crossCheck().equals("none"));
        for (Input i : inputs) {
            switch (i.topic()) {
                case InputParser.MESSAGES -> {
                    InputParser.Read read = InputParser.read(new Inputs.Origin(i.topic(), i.partition(), i.offset()), i.key(), i.value());
                    if (read.rejected() || !(read.record().parsed() instanceof Inputs.ProcessMessage m) || !stationOf.containsKey(m.device())) continue;
                    String id = ScenarioBuilder.sampleKey(m.edge(), m.device(), m.birthPublishTs(), "", m.bdSeq(), m.seq(), m.publishTs());
                    if (!seen.add(id)) {
                        for (Inputs.Member x : m.members())
                            duplicates.merge(ScenarioBuilder.sampleKey(m.edge(), m.device(), m.birthPublishTs(), x.metric(), m.bdSeq(), m.seq(), m.publishTs()), 1, Integer::sum);
                        continue;
                    }
                    Set<Long> ts = new HashSet<>();
                    String visitId = "", unitId = "";
                    boolean present = false;
                    for (Inputs.Member x : m.members()) {
                        ts.add(x.sourceTs());
                        if (x.metric().equals("VisitId") && x.value().isTextual()) visitId = x.value().textValue();
                        if (x.metric().equals("UnitId") && x.value().isTextual()) unitId = x.value().textValue();
                        if (x.metric().equals("VisitPresent") && x.value().isBoolean()) present = x.value().booleanValue();
                    }
                    boolean coherent = ts.size() == 1 && !ts.contains(null);
                    for (Inputs.Member x : m.members()) {
                        if (x.sourceTs() == null) continue;
                        samples.add(new GroundTruth.Sample(m.edge(), m.birthPublishTs(), m.bdSeq(), m.seq(), m.publishTs(), m.device(), x.metric(), x.sourceTs(),
                            x.uaStatusCode() == null ? 0L : x.uaStatusCode(), x.value(), i.batch(), coherent, visitId, unitId, present));
                    }
                }
                case InputParser.SESSIONS -> {
                    ObjectNode v = (ObjectNode) Json.read(i.value());
                    if (stationOf.containsKey(v.path("device").asText())) ses.add(new GroundTruth.Delivery(i.topic(), v, i.batch()));
                }
                case InputParser.OCCUPANCY -> { if (occupancyRead) occ.add(new GroundTruth.Delivery(i.topic(), (ObjectNode) Json.read(i.value()), i.batch())); }
                case InputParser.INCIDENTS -> inc.add(new GroundTruth.Delivery(i.topic(), (ObjectNode) Json.read(i.value()), i.batch()));
                default -> throw new IllegalArgumentException("not an input topic: " + i.topic());
            }
        }
        Comparator<GroundTruth.Delivery> byBatch = Comparator.comparingInt(GroundTruth.Delivery::batch);
        occ.sort(byBatch); ses.sort(byBatch); inc.sort(byBatch);
        Map<String, GroundTruth.VisitFacts> vf = new HashMap<>();
        facts.forEach((k, v) -> vf.put(k, new GroundTruth.VisitFacts(v.visitRef(), v.arrivalEventId(), v.tools())));
        return new GroundTruth(rule, source, stationOf, samples, occ, ses, inc, w1, batches, vf, evidence, duplicates);
    }
}
