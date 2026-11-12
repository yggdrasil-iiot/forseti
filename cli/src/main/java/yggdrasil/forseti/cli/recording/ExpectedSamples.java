package yggdrasil.forseti.cli.recording;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The per-sample expectations and the per-identity dispositions of {@code expected.json} in their two encodings, and the one reader
 *  the gate uses for both.
 *
 *  <p>Hand-made recordings write {@code "samples": {<JCS sampleId> -> {attributedTo, state, reason}}} and
 *  {@code "dispositions": {<JCS identity> -> {topic, parts, ...}}}, every identity spelled out. Synthesized recordings (hundreds of
 *  thousands of samples, most of them one idle expectation for hours) write {@code "samplesEncoding": "runs"} and, instead of
 *  {@code samples}, {@code "sampleRuns": [...]}, each run
 *  {@code {"sampleId": {edge, device, birthPublishTs, metric, bdSeq}, "seq": [from, to], "publishTs": p, "publishStepMs": s,
 *  "expected": {attributedTo, state, reason}}}. A run stands for the {@code to - from + 1} sampleIds made of its {@code sampleId}
 *  fields plus {@code seq} from {@code from} to {@code to} (both included) and {@code publishTs = p + (seq - from) * s}, every one with
 *  the run's expectation. The message dispositions of devices outside the rule's catalog ({@code rejected unknown-device}, one per
 *  message) are likewise written as {@code "messageDispositionRuns": [...]}, each run {@code {"messageId": {edge, device,
 *  birthPublishTs, bdSeq}, "seq", "publishTs", "publishStepMs", "disposition": {topic, parts}}} standing for the message identities
 *  {@code messageId + {seq, publishTs}}; every other disposition stays in {@code dispositions}. Runs are sorted by the JCS bytes of
 *  their stream fields, then by {@code seq}; two runs never share an identity, and the expansion names exactly the identities the full
 *  encoding would name, with the same values, in a different order. {@link #forEachSample} and {@link #forEachDisposition} expand
 *  either encoding, so a comparison asserts every sample and every disposition whichever encoding a recording carries. */
public final class ExpectedSamples {
    public static final String RUNS = "runs";

    private ExpectedSamples() {}

    /** Every (JCS sampleId, expectation) of {@code expected}: the {@code samples} object as written, or every run expanded. */
    public static void forEachSample(JsonNode expected, BiConsumer<String, JsonNode> f) {
        if (!runs(expected)) { expected.path("samples").fields().forEachRemaining(e -> f.accept(e.getKey(), e.getValue())); return; }
        expand(expected.path("sampleRuns"), "sampleId", "expected", f);
    }

    /** Every (JCS identity, disposition entry) of {@code expected}: {@code dispositions} as written, then every message run expanded. */
    public static void forEachDisposition(JsonNode expected, BiConsumer<String, JsonNode> f) {
        expected.path("dispositions").fields().forEachRemaining(e -> f.accept(e.getKey(), e.getValue()));
        if (runs(expected)) expand(expected.path("messageDispositionRuns"), "messageId", "disposition", f);
    }

    /** The number of sampleIds {@code expected} names. */
    public static long sampleCount(JsonNode expected) {
        if (!runs(expected)) return expected.path("samples").size();
        long n = 0;
        for (JsonNode run : expected.path("sampleRuns")) n += run.path("seq").get(1).longValue() - run.path("seq").get(0).longValue() + 1;
        return n;
    }

    private static boolean runs(JsonNode expected) {
        if (!expected.has("samplesEncoding")) return false;
        if (!RUNS.equals(expected.path("samplesEncoding").asText())) throw new IllegalArgumentException("samplesEncoding " + expected.path("samplesEncoding"));
        return true;
    }

    private static void expand(JsonNode runs, String idField, String valueField, BiConsumer<String, JsonNode> f) {
        for (JsonNode run : runs) {
            JsonNode id = run.path(idField);
            if (!id.isObject() || id.has("seq") || id.has("publishTs")) throw new IllegalArgumentException("run " + idField + " " + id);
            long from = Json.safeLong(run.path("seq").get(0), "seq.from", Json.NON_NEGATIVE), to = Json.safeLong(run.path("seq").get(1), "seq.to", Json.NON_NEGATIVE);
            long p = Json.safeLong(run.get("publishTs"), "publishTs", Json.NON_NEGATIVE), step = Json.safeLong(run.get("publishStepMs"), "publishStepMs", Json.NON_NEGATIVE);
            if (to < from) throw new IllegalArgumentException("run seq " + from + " > " + to);
            JsonNode value = run.path(valueField);
            for (long seq = from; seq <= to; seq++) {
                ObjectNode n = ((ObjectNode) id).deepCopy();
                Json.putSafe(n, "seq", seq);
                Json.putSafe(n, "publishTs", p + (seq - from) * step);
                f.accept(Canonical.utf8(Canonical.bytes(n)), value);
            }
        }
    }

    /** Collects runs: an identity extends its stream's open run while the value is equal, {@code seq} is the next one and
     *  {@code publishTs} advances by the step; the same identity added again (another member of one message) is absorbed. */
    static final class Builder {
        private static final class Run {
            final ObjectNode stream; final long seqFrom, publishFrom; final JsonNode value; long seqTo, publishTo;
            Run(ObjectNode stream, long seq, long publishTs, JsonNode value) { this.stream = stream; seqFrom = seqTo = seq; publishFrom = publishTo = publishTs; this.value = value; }
        }
        private final long step;
        private final String idField, valueField;
        private final Map<String, Run> open = new HashMap<>();
        private final Map<String, ObjectNode> streams = new HashMap<>();
        private final List<Run> done = new ArrayList<>();

        Builder(long stepMs, String idField, String valueField) { this.step = stepMs; this.idField = idField; this.valueField = valueField; }

        void add(String streamKey, Supplier<ObjectNode> stream, long seq, long publishTs, JsonNode value) {
            Run r = open.get(streamKey);
            if (r != null && r.value.equals(value)) {
                if (seq == r.seqTo && publishTs == r.publishTo) return;
                if (seq == r.seqTo + 1 && publishTs == r.publishTo + step) { r.seqTo = seq; r.publishTo = publishTs; return; }
            }
            if (r != null) done.add(r);
            open.put(streamKey, new Run(streams.computeIfAbsent(streamKey, k -> stream.get()), seq, publishTs, value));
        }

        ArrayNode toJson() {
            List<Run> all = new ArrayList<>(done);
            all.addAll(open.values());
            Map<ObjectNode, String> keys = new IdentityHashMap<>();
            for (Run r : all) keys.computeIfAbsent(r.stream, s -> Canonical.utf8(Canonical.bytes(s)));
            all.sort(Comparator.comparing((Run r) -> keys.get(r.stream)).thenComparingLong(r -> r.seqFrom));
            ArrayNode out = Json.JSON.createArrayNode();
            for (Run r : all) {
                ObjectNode o = out.addObject();
                o.set(idField, r.stream.deepCopy());
                ArrayNode seq = o.putArray("seq");
                seq.add(r.seqFrom); seq.add(r.seqTo);
                Json.putSafe(o, "publishTs", r.publishFrom);
                Json.putSafe(o, "publishStepMs", step);
                o.set(valueField, r.value.deepCopy());
            }
            return out;
        }
    }

    /** The sample runs of {@code outcomes} (in sample order). */
    static ArrayNode sampleRuns(Collection<Map.Entry<GroundTruth.Sample, GroundTruth.Outcome>> outcomes, long stepMs) {
        Builder b = new Builder(stepMs, "sampleId", "expected");
        for (Map.Entry<GroundTruth.Sample, GroundTruth.Outcome> e : outcomes) {
            GroundTruth.Sample x = e.getKey();
            b.add(x.device() + "\u0000" + x.metric() + "\u0000" + x.birth() + "\u0000" + x.bdSeq(), () -> {
                ObjectNode n = Json.object().put("edge", x.edge()).put("device", x.device());
                Json.putSafe(n, "birthPublishTs", x.birth()).put("metric", x.metric());
                return Json.putSafe(n, "bdSeq", x.bdSeq());
            }, x.seq(), x.publishTs(), GroundTruth.expectation(e.getValue()));
        }
        return b.toJson();
    }
}
