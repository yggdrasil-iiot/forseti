package yggdrasil.forseti.cli.harness;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.Map;
import yggdrasil.forseti.core.Json;

/** {@code <out>/cuts.json}: {@code {generation, routingDigest, cuts: [...]}}, per cut {@code {cutId, perStation {station -> {W1,
 *  admissionW, W2 (stage 2's, equal to W1 by construction, spec 588), mergedCutIds: [], onlineVisits}}, offsetRanges {topic -> {partition -> [from, to]}}, incidentOffsets}}. The offset ranges are
 *  half-open ({@code [from, to)}, empty when equal), distinct from the inclusive through-offsets of barriers; {@code incidentOffsets} are
 *  the {@code quality.incidents} through-offsets of the cut as {@code {cutId, partition, throughOffset}}. */
final class CutLog {
    record Range(long from, long to) {
        Range { if (from < 0 || to < from || to > Json.MAX_SAFE) throw new IllegalArgumentException("offset range [" + from + ", " + to + ")"); }
    }
    record StationCut(long w1, Long admissionW, long w2, int onlineVisits) {}

    private final ObjectNode root = Json.object();
    private final ArrayNode cuts;

    CutLog(long generation, String routingDigest) {
        Json.putSafe(root, "generation", generation).put("routingDigest", routingDigest);
        cuts = root.putArray("cuts");
    }

    void cut(long cutId, Map<String, StationCut> perStation, Map<String, Map<Integer, Range>> offsetRanges, Map<Integer, Long> incidentThrough) {
        ObjectNode c = Json.putSafe(Json.object(), "cutId", cutId);
        ObjectNode ps = c.putObject("perStation");
        perStation.forEach((s, v) -> {
            ObjectNode o = Json.putSafe(ps.putObject(s), "W1", v.w1());
            if (v.admissionW() == null) o.putNull("admissionW"); else Json.putSafe(o, "admissionW", v.admissionW());
            Json.putSafe(o, "W2", v.w2());
            o.putArray("mergedCutIds");
            Json.putSafe(o, "onlineVisits", v.onlineVisits());
        });
        c.set("offsetRanges", ranges(offsetRanges));
        ArrayNode inc = c.putArray("incidentOffsets");
        incidentThrough.forEach((p, t) -> {
            ObjectNode o = Json.putSafe(Json.object(), "cutId", cutId);
            Json.putSafe(o, "partition", p);
            inc.add(Json.putSafe(o, "throughOffset", t));
        });
        cuts.add(c);
    }

    static ObjectNode ranges(Map<String, Map<Integer, Range>> offsetRanges) {
        ObjectNode r = Json.object();
        offsetRanges.forEach((t, parts) -> {
            ObjectNode tn = r.putObject(t);
            parts.forEach((p, range) -> {
                ArrayNode a = tn.putArray(Integer.toString(p));
                a.add(range.from()).add(range.to());
            });
        });
        return r;
    }

    int size() { return cuts.size(); }

    void write(Path out) { Json.write(out.resolve("cuts.json"), root); }
}
