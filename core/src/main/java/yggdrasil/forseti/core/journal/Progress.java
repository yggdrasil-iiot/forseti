package yggdrasil.forseti.core.journal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import yggdrasil.forseti.core.Json;

/** Cut progress records (spec "Cut barriers, progress records and the disposition ledgers"): stage 1, stage 2 and ledger. They have no
 *  envelope and are never journal rows. Field lists and order are the spec's; every integer is written with {@link Json#putSafe} and read
 *  with {@link Json#safeLong}: through-offsets in {@link Json#THROUGH}, W1, W2 and admissionW in {@link Json#ANY}, the rest non-negative. */
public sealed interface Progress extends JournalFold.Entry permits Progress.Stage1, Progress.Stage2, Progress.Ledger {
    long cutId();
    ObjectNode toJson();

    record Origin(String topic, int partition) {}
    record BarrierRef(long cutId, Origin origin, long throughOffset) {
        ObjectNode toJson() {
            ObjectNode n = Json.putSafe(Json.object(), "cutId", cutId);
            n.putObject("origin").put("topic", origin.topic()).put("partition", origin.partition());
            return Json.putSafe(n, "throughOffset", throughOffset);
        }
        static BarrierRef fromJson(JsonNode n) {
            JsonNode o = JournalFold.required(n, "origin");
            return new BarrierRef(Json.safeLong(n.get("cutId"), "barrierRefs.cutId", Json.NON_NEGATIVE),
                new Origin(Json.text(o, "topic"), JournalFold.partition(o.get("partition"), "barrierRefs.origin.partition")),
                Json.safeLong(n.get("throughOffset"), "barrierRefs.throughOffset", Json.THROUGH));
        }
    }
    record IncidentOffset(long cutId, int partition, long throughOffset) {
        ObjectNode toJson() {
            ObjectNode n = Json.putSafe(Json.object(), "cutId", cutId);
            n.put("partition", partition);
            return Json.putSafe(n, "throughOffset", throughOffset);
        }
        static IncidentOffset fromJson(JsonNode n) {
            return new IncidentOffset(Json.safeLong(n.get("cutId"), "incidentOffsets.cutId", Json.NON_NEGATIVE),
                JournalFold.partition(n.get("partition"), "incidentOffsets.partition"), Json.safeLong(n.get("throughOffset"), "incidentOffsets.throughOffset", Json.THROUGH));
        }
    }
    /** A stage-1 progress record a stage-2 progress processed: {topic, partition, cutId}. */
    record Stage1ProgressRef(String topic, int partition, long cutId) {
        ObjectNode toJson() { ObjectNode n = Json.object().put("topic", topic).put("partition", partition); return Json.putSafe(n, "cutId", cutId); }
        static Stage1ProgressRef fromJson(JsonNode n) {
            return new Stage1ProgressRef(Json.text(n, "topic"), JournalFold.partition(n.get("partition"), "stage1ProgressRefs.partition"),
                Json.safeLong(n.get("cutId"), "stage1ProgressRefs.cutId", Json.NON_NEGATIVE));
        }
    }
    /** Only on the first stage-1 progress after an initialization; {@code restore} is "none" in the step-3 reference harness. */
    record InitEvidence(long declarationOffset, long endOffset, String restore) {
        public InitEvidence {
            if (declarationOffset < 0) throw new IllegalArgumentException("initEvidence.declarationOffset >= 0");
            if (endOffset < 1) throw new IllegalArgumentException("initEvidence.endOffset >= 1");
            if (restore == null) throw new IllegalArgumentException("initEvidence.restore");
        }
        ObjectNode toJson() {
            ObjectNode n = Json.putSafe(Json.object(), "declarationOffset", declarationOffset);
            Json.putSafe(n, "endOffset", endOffset);
            return n.put("restore", restore);
        }
        static InitEvidence fromJson(JsonNode n) {
            return new InitEvidence(Json.safeLong(n.get("declarationOffset"), "initEvidence.declarationOffset", Json.NON_NEGATIVE),
                Json.safeLong(n.get("endOffset"), "initEvidence.endOffset", 1, Json.MAX_SAFE), Json.text(n, "restore"));
        }
    }

    record Stage1(long generation, String station, long cutId, long producerEpoch, long W1, Long admissionW, List<BarrierRef> barrierRefs,
                  long restartEpoch, String recoveryRef, List<Long> mergedCutIds, InitEvidence initEvidence) implements Progress {
        public Stage1 { barrierRefs = List.copyOf(barrierRefs); mergedCutIds = List.copyOf(mergedCutIds); }
        @Override public ObjectNode toJson() {
            ObjectNode n = head(generation).put("station", station);
            Json.putSafe(n, "cutId", cutId);
            Json.putSafe(n, "producerEpoch", producerEpoch);
            Json.putSafe(n, "W1", W1);
            if (admissionW == null) n.putNull("admissionW"); else Json.putSafe(n, "admissionW", admissionW);
            n.set("barrierRefs", array(barrierRefs, BarrierRef::toJson));
            Json.putSafe(n, "restartEpoch", restartEpoch);
            n.put("recoveryRef", recoveryRef);
            n.set("mergedCutIds", longs(mergedCutIds));
            if (initEvidence != null) n.set("initEvidence", initEvidence.toJson());
            return n;
        }
    }

    record Stage2(long generation, String station, long cutId, long producerEpoch, long W2, List<Stage1ProgressRef> stage1ProgressRefs,
                  List<IncidentOffset> incidentOffsets, List<Long> mergedCutIds) implements Progress {
        public Stage2 { stage1ProgressRefs = List.copyOf(stage1ProgressRefs); incidentOffsets = List.copyOf(incidentOffsets); mergedCutIds = List.copyOf(mergedCutIds); }
        @Override public ObjectNode toJson() {
            ObjectNode n = head(generation).put("station", station);
            Json.putSafe(n, "cutId", cutId);
            Json.putSafe(n, "producerEpoch", producerEpoch);
            Json.putSafe(n, "W2", W2);
            n.set("stage1ProgressRefs", array(stage1ProgressRefs, Stage1ProgressRef::toJson));
            n.set("incidentOffsets", array(incidentOffsets, IncidentOffset::toJson));
            n.set("mergedCutIds", longs(mergedCutIds));
            return n;
        }
    }

    /** Ledger progress; {@code station} is null for partition 6; {@code initEvidence} only on stage-1 ledgers. */
    record Ledger(long generation, int stage, int partition, String station, long cutId, long producerEpoch, List<BarrierRef> barrierRefs,
                  List<Long> mergedCutIds, long restartEpoch, String recoveryRef, InitEvidence initEvidence) implements Progress {
        public Ledger {
            if (stage != 1 && stage != 2) throw new IllegalArgumentException("ledger stage is 1 or 2");
            if (stage == 2 && initEvidence != null) throw new IllegalArgumentException("initEvidence on stage-1 ledgers only");
            barrierRefs = List.copyOf(barrierRefs); mergedCutIds = List.copyOf(mergedCutIds);
        }
        @Override public ObjectNode toJson() {
            ObjectNode n = head(generation).put("stage", stage).put("partition", partition).put("station", station);
            Json.putSafe(n, "cutId", cutId);
            Json.putSafe(n, "producerEpoch", producerEpoch);
            n.set("barrierRefs", array(barrierRefs, BarrierRef::toJson));
            n.set("mergedCutIds", longs(mergedCutIds));
            Json.putSafe(n, "restartEpoch", restartEpoch);
            n.put("recoveryRef", recoveryRef);
            if (initEvidence != null) n.set("initEvidence", initEvidence.toJson());
            return n;
        }
    }

    /** A progress record from its JSON form: a ledger carries {@code stage}, stage 2 carries {@code W2}, stage 1 {@code W1}. */
    static Progress fromJson(JsonNode n) {
        if (!"progress".equals(n.path("type").textValue())) throw new IllegalArgumentException("not a progress record");
        long generation = Json.safeLong(n.get("generation"), "generation", Json.NON_NEGATIVE);
        long cutId = Json.safeLong(n.get("cutId"), "cutId", Json.NON_NEGATIVE);
        long producerEpoch = Json.safeLong(n.get("producerEpoch"), "producerEpoch", Json.NON_NEGATIVE);
        if (n.has("stage")) {
            return new Ledger(generation, (int) Json.safeLong(n.get("stage"), "stage", 1, 2), JournalFold.partition(n.get("partition"), "partition"),
                JournalFold.textOrNull(n, "station"), cutId, producerEpoch, list(n, "barrierRefs", BarrierRef::fromJson), cutIds(n),
                Json.safeLong(n.get("restartEpoch"), "restartEpoch", Json.NON_NEGATIVE), JournalFold.textOrNull(n, "recoveryRef"), init(n));
        }
        if (n.has("W2")) {
            return new Stage2(generation, Json.text(n, "station"), cutId, producerEpoch, Json.safeLong(n.get("W2"), "W2", Json.ANY),
                list(n, "stage1ProgressRefs", Stage1ProgressRef::fromJson), list(n, "incidentOffsets", IncidentOffset::fromJson), cutIds(n));
        }
        JsonNode admission = JournalFold.required(n, "admissionW");
        return new Stage1(generation, Json.text(n, "station"), cutId, producerEpoch, Json.safeLong(n.get("W1"), "W1", Json.ANY),
            admission.isNull() ? null : Json.safeLong(admission, "admissionW", Json.ANY), list(n, "barrierRefs", BarrierRef::fromJson),
            Json.safeLong(n.get("restartEpoch"), "restartEpoch", Json.NON_NEGATIVE), JournalFold.textOrNull(n, "recoveryRef"), cutIds(n), init(n));
    }

    private static ObjectNode head(long generation) { return Json.putSafe(Json.object().put("type", "progress"), "generation", generation); }
    private static <T> ArrayNode array(List<T> items, Function<T, ObjectNode> f) { ArrayNode a = Json.JSON.createArrayNode(); items.forEach(i -> a.add(f.apply(i))); return a; }
    private static ArrayNode longs(List<Long> l) {
        ArrayNode a = Json.JSON.createArrayNode();
        for (long v : l) { if (v < 0 || v > Json.MAX_SAFE) throw new IllegalStateException("integer outside the JCS domain: mergedCutIds=" + v); a.add(v); }
        return a;
    }
    private static <T> List<T> list(JsonNode n, String field, Function<JsonNode, T> f) {
        JsonNode a = JournalFold.required(n, field);
        if (!a.isArray()) throw new IllegalArgumentException(field + " is not an array");
        List<T> out = new ArrayList<>();
        a.forEach(e -> out.add(f.apply(e)));
        return out;
    }
    private static List<Long> cutIds(JsonNode n) { return list(n, "mergedCutIds", e -> Json.safeLong(e, "mergedCutIds", Json.NON_NEGATIVE)); }
    private static InitEvidence init(JsonNode n) { JsonNode e = n.get("initEvidence"); return e == null ? null : InitEvidence.fromJson(e); }
}
