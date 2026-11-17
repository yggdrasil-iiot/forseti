package yggdrasil.forseti.cli.harness;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import yggdrasil.forseti.core.Json;

/** {@code <out>/cuts.json}: {@code {generation, routingDigest, cuts: [...]}}, per cut {@code {cutId, perStation {station -> {W1,
 *  admissionW, W2 (stage 2's, equal to W1 by construction, spec 588), mergedCutIds, onlineVisits}}, offsetRanges {topic -> {partition -> [from, to]}}, incidentOffsets}}; {@code perStation} lists
 *  the stations that wrote the cut's progress (a starved station is absent until the cut that merges the ones it skipped), and
 *  {@code w1Form} is present when the run reported the min-over-partitions form. The offset ranges are
 *  half-open ({@code [from, to)}, empty when equal), distinct from the inclusive through-offsets of barriers; {@code incidentOffsets} are
 *  the {@code quality.incidents} through-offsets of the cut as {@code {cutId, partition, throughOffset}}. */
public final class CutLog {
    public record Range(long from, long to) {
        public Range { if (from < 0 || to < from || to > Json.MAX_SAFE) throw new IllegalArgumentException("offset range [" + from + ", " + to + ")"); }
    }
    record StationCut(long w1, Long admissionW, long w2, List<Long> mergedCutIds, int onlineVisits) {}

    private final ObjectNode root = Json.object();
    private final ArrayNode cuts;

    CutLog(long generation, String routingDigest, String w1Form) {
        Json.putSafe(root, "generation", generation).put("routingDigest", routingDigest);
        if (w1Form != null) root.put("w1Form", w1Form);
        cuts = root.putArray("cuts");
    }

    void cut(long cutId, Map<String, StationCut> perStation, Map<String, Map<Integer, Range>> offsetRanges, Map<Integer, Long> incidentThrough) {
        ObjectNode c = Json.putSafe(Json.object(), "cutId", cutId);
        ObjectNode ps = c.putObject("perStation");
        perStation.forEach((s, v) -> {
            ObjectNode o = Json.putSafe(ps.putObject(s), "W1", v.w1());
            if (v.admissionW() == null) o.putNull("admissionW"); else Json.putSafe(o, "admissionW", v.admissionW());
            Json.putSafe(o, "W2", v.w2());
            ArrayNode merged = o.putArray("mergedCutIds");
            v.mergedCutIds().forEach(merged::add);
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

    public static ObjectNode ranges(Map<String, Map<Integer, Range>> offsetRanges) {
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
