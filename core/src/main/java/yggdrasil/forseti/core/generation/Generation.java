package yggdrasil.forseti.core.generation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import yggdrasil.forseti.core.Json;

/** One row of {@code forseti.generations} (spec "Generations and revisions"): {@code generation}, {@code status}, {@code rulesDigest},
 *  {@code inputReleases}, {@code replayFrom} offsets, {@code headVersion}, {@code releaseEvidence}, {@code gateReportSha256}, {@code at}.
 *  Every integer is written with {@link Json#putSafe} and read with {@link Json#safeLong}; {@code generation >= 1}. {@code replayFrom} is
 *  {@code {topic -> {partition -> offset}}}. The row carries no scope: the scope is the record key of the store. */
public record Generation(long generation, String status, String rulesDigest, JsonNode inputReleases, JsonNode replayFrom, long headVersion,
                         JsonNode releaseEvidence, String gateReportSha256, long at) {
    public static final String REGISTERED = "registered", REPLAYING = "replaying", VERIFIED = "verified", CURRENT = "current", RETIRED = "retired";
    public static final List<String> STATUSES = List.of(REGISTERED, REPLAYING, VERIFIED, CURRENT, RETIRED);

    public Generation {
        if (generation < 1 || generation > Json.MAX_SAFE) throw new IllegalArgumentException("generation >= 1");
        if (!STATUSES.contains(status)) throw new IllegalArgumentException("not a generation status: " + status);
        if (rulesDigest == null || !rulesDigest.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("rulesDigest is a SHA-256");
        if (inputReleases == null || !inputReleases.isArray()) throw new IllegalArgumentException("inputReleases is an array");
        if (replayFrom == null || !replayFrom.isObject()) throw new IllegalArgumentException("replayFrom is an object");
        for (Iterator<Map.Entry<String, JsonNode>> t = replayFrom.fields(); t.hasNext();) {
            Map.Entry<String, JsonNode> e = t.next();
            if (!e.getValue().isObject()) throw new IllegalArgumentException("replayFrom." + e.getKey() + " is an object");
            for (Iterator<Map.Entry<String, JsonNode>> p = e.getValue().fields(); p.hasNext();) {
                Map.Entry<String, JsonNode> o = p.next();
                if (!o.getKey().matches("0|[1-9][0-9]{0,8}")) throw new IllegalArgumentException("replayFrom." + e.getKey() + " partition " + o.getKey());
                Json.safeLong(o.getValue(), "replayFrom." + e.getKey() + "." + o.getKey(), Json.NON_NEGATIVE);
            }
        }
        if (headVersion < 1 || headVersion > Json.MAX_SAFE) throw new IllegalArgumentException("headVersion >= 1");
        if (releaseEvidence == null || !releaseEvidence.isObject()) throw new IllegalArgumentException("releaseEvidence is an object");
        if (gateReportSha256 != null && !gateReportSha256.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("gateReportSha256 is a SHA-256 or null");
        if (at < 0 || at > Json.MAX_SAFE) throw new IllegalArgumentException("at >= 0");
        inputReleases = inputReleases.deepCopy();
        replayFrom = replayFrom.deepCopy();
        releaseEvidence = releaseEvidence.deepCopy();
    }

    public ObjectNode toJson() {
        ObjectNode n = Json.putSafe(Json.object(), "generation", generation);
        n.put("status", status).put("rulesDigest", rulesDigest);
        n.set("inputReleases", inputReleases.deepCopy());
        n.set("replayFrom", replayFrom.deepCopy());
        Json.putSafe(n, "headVersion", headVersion);
        n.set("releaseEvidence", releaseEvidence.deepCopy());
        n.put("gateReportSha256", gateReportSha256);
        return Json.putSafe(n, "at", at);
    }

    public static Generation fromJson(JsonNode n) {
        if (n == null || !n.isObject()) throw new IllegalArgumentException("a generation row is an object");
        JsonNode gate = n.get("gateReportSha256");
        if (gate == null || !(gate.isNull() || gate.isTextual())) throw new IllegalArgumentException("gateReportSha256 is text or null");
        return new Generation(Json.safeLong(n.get("generation"), "generation", 1, Json.MAX_SAFE), Json.text(n, "status"), Json.text(n, "rulesDigest"),
            required(n, "inputReleases"), required(n, "replayFrom"), Json.safeLong(n.get("headVersion"), "headVersion", 1, Json.MAX_SAFE),
            required(n, "releaseEvidence"), gate.isNull() ? null : gate.textValue(), Json.safeLong(n.get("at"), "at", Json.NON_NEGATIVE));
    }

    private static JsonNode required(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null) throw new IllegalArgumentException("missing field " + field);
        return v;
    }
}
