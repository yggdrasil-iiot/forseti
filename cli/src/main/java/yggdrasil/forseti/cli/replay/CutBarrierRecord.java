package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.journal.Progress;

/** The {@code cut-barrier} record the harness writes to every subscribed input partition after a batch (spec 412): the value is exactly
 *  the spec's fields {@code {generation, cutId, routingDigest, origin {topic, partition}, throughOffset, idleInputs, restartEpoch,
 *  recoveryRef}} as RFC 8785 bytes, the record key is null, and two headers mark it: {@value #TYPE_HEADER} {@code = cut-barrier}, which
 *  tells a barrier from a business input before any parse (an input record carrying that header is refused by the writer), and
 *  {@code forseti-generation}. {@code origin.topic} is the contract topic name, whatever the run's prefix; {@code throughOffset} is the
 *  offset, on the partition the barrier is written to, of the last business record the cut includes ({@code -1}: none yet). */
public record CutBarrierRecord(long generation, String routingDigest, CutAssembler.Barrier barrier) {
    public static final String TYPE_HEADER = "forseti-record-type", TYPE = "cut-barrier", GENERATION_HEADER = "forseti-generation";
    private static final Set<String> FIELDS = Set.of("generation", "cutId", "routingDigest", "origin", "throughOffset", "idleInputs", "restartEpoch", "recoveryRef");

    public ObjectNode toJson() {
        ObjectNode n = Json.putSafe(Json.object(), "generation", generation);
        Json.putSafe(n, "cutId", barrier.cutId());
        n.put("routingDigest", routingDigest);
        n.putObject("origin").put("topic", barrier.origin().topic()).put("partition", barrier.origin().partition());
        Json.putSafe(n, "throughOffset", barrier.throughOffset());
        ArrayNode idle = n.putArray("idleInputs");
        barrier.idleInputs().forEach(i -> idle.add(i.toJson()));
        Json.putSafe(n, "restartEpoch", barrier.restartEpoch());
        return n.put("recoveryRef", barrier.recoveryRef());
    }

    /** A barrier as an adapter consumes it (the rule every engine adapter applies): read strictly ({@link #fromJson}), its
     *  {@code origin} must be the partition it was consumed from ({@code topic}, the contract name, and {@code partition}), its
     *  {@code generation} and {@code routingDigest} the run's. Anything else is an {@link IllegalArgumentException} ({@code barrier-origin},
     *  {@code barrier-generation}, {@code barrier-routing}): a barrier is never applied to a channel it does not name. */
    public static CutBarrierRecord consumed(String topic, int partition, byte[] value, long generation, String routingDigest) {
        CutBarrierRecord r = fromJson(Json.read(value));
        Progress.Origin o = r.barrier().origin();
        if (!o.topic().equals(topic) || o.partition() != partition)
            throw new IllegalArgumentException("barrier-origin: a barrier of " + o.topic() + "/" + o.partition() + " consumed from " + topic + "/" + partition);
        if (r.generation() != generation) throw new IllegalArgumentException("barrier-generation: a barrier of generation " + r.generation() + " in the run of " + generation);
        if (!r.routingDigest().equals(routingDigest)) throw new IllegalArgumentException("barrier-routing: a barrier of routing " + r.routingDigest());
        return r;
    }

    /** The record {@link #toJson} wrote, read strictly: another field set or a malformed field is an {@link IllegalArgumentException}. */
    public static CutBarrierRecord fromJson(JsonNode n) {
        if (!n.isObject() || n.size() != FIELDS.size()) throw new IllegalArgumentException("a cut barrier has exactly the fields " + FIELDS);
        n.fieldNames().forEachRemaining(f -> { if (!FIELDS.contains(f)) throw new IllegalArgumentException("cut barrier field " + f); });
        JsonNode o = n.get("origin");
        if (!o.isObject() || o.size() != 2) throw new IllegalArgumentException("cut barrier origin is {topic, partition}");
        List<CutAssembler.Idle> idle = new ArrayList<>();
        if (!n.get("idleInputs").isArray()) throw new IllegalArgumentException("cut barrier idleInputs is an array");
        for (JsonNode i : n.get("idleInputs")) idle.add(CutAssembler.Idle.fromJson(i));
        JsonNode ref = n.get("recoveryRef");
        if (!ref.isNull() && !ref.isTextual()) throw new IllegalArgumentException("cut barrier recoveryRef is a string or null");
        CutAssembler.Barrier b = new CutAssembler.Barrier(Json.safeLong(n.get("cutId"), "cutId", 1, Json.MAX_SAFE),
            new Progress.Origin(Json.text(o, "topic"), (int) Json.safeLong(o.get("partition"), "origin.partition", 0, Integer.MAX_VALUE)),
            Json.safeLong(n.get("throughOffset"), "throughOffset", Json.THROUGH), Json.safeLong(n.get("restartEpoch"), "restartEpoch", Json.NON_NEGATIVE),
            ref.isNull() ? null : ref.textValue(), idle);
        return new CutBarrierRecord(Json.safeLong(n.get("generation"), "generation", 1, Json.MAX_SAFE), Json.text(n, "routingDigest"), b);
    }
}
