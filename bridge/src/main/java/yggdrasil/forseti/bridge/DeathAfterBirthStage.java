package yggdrasil.forseti.bridge;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Injection 25(a), "the earlier connection's death delayed past the newer birth" (spec fault injections): an input stage that holds
 *  the first NDEATH frame of one edge received after a birth of that edge, until the edge's next NBIRTH frame has been handed to the
 *  core, and then hands the held NDEATH on. The core therefore classifies the death after the newer birth, as it would a delayed Will:
 *  a {@code stale-death} of the earlier session, with the current session left open. The permutation happens here, before session
 *  classification; the raw-frame log keeps the wire order, and no frame is changed or dropped. It runs once per bridge run.
 *
 *  <p>The run record names the held frame (edge, {@code bdSeq}, payload timestamp, receipt), the frame it was released after (the
 *  NBIRTH: the same fields) and whether the permutation ran as declared ({@code completed}). A run that ends with the death still held
 *  (no successor birth arrived) releases it at the drain, so the core still sees it, and is recorded {@code completed: false}: the
 *  injection did not happen. */
public final class DeathAfterBirthStage implements InputStage {
    private final String edge;
    private boolean born;
    private Frame held;
    private Map<String, Object> heldFrame, releasedAfter;
    private String releasedBy;

    /** @param edge the edge id (the topic's edge level, {@code spBv1.0/<group>/NDEATH/<edge>}) whose death is held */
    public DeathAfterBirthStage(String edge) {
        if (edge == null || edge.isEmpty() || edge.contains("/")) throw new IllegalArgumentException("the death-after-birth stage needs an edge id");
        this.edge = edge;
    }

    @Override public List<Frame> apply(Frame f) {
        if (releasedBy != null) return List.of(f);
        SparkplugTopic t = SparkplugTopic.parse(f.topic());
        if (t == null || !edge.equals(t.edge()) || t.device() != null) return List.of(f);
        if (held == null) {
            if (t.type().equals("NBIRTH")) born = true;
            else if (t.type().equals("NDEATH") && born) {
                held = f;
                heldFrame = describe(f);
                return List.of();
            }
            return List.of(f);
        }
        if (!t.type().equals("NBIRTH")) return List.of(f);
        Frame death = held;
        held = null;
        releasedAfter = describe(f);
        releasedBy = "next-birth";
        return List.of(f, death);
    }

    @Override public List<Frame> drain() {
        if (held == null) return List.of();
        Frame death = held;
        held = null;
        releasedBy = "drain";
        return List.of(death);
    }

    /** The permutation ran as declared: the death was held and handed on after the edge's next birth. */
    public boolean completed() { return "next-birth".equals(releasedBy); }

    /** {@code edge}, {@code bdSeq} and the payload {@code timestamp} of a frame (null when the payload does not carry them), and its
     *  receipt time. */
    private Map<String, Object> describe(Frame f) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("topic", f.topic());
        m.put("edge", edge);
        Long bdSeq = null, ts = null;
        try {
            SparkplugPayloads.Decoded p = SparkplugPayloads.decode(f.payload());
            SparkplugPayloads.DecodedMetric bd = p.aliasOnly() ? null : p.metric(SparkplugPayloads.BD_SEQ);
            bdSeq = bd == null ? null : SparkplugPayloads.integral(bd.value());
            ts = p.timestamp();
        } catch (SparkplugPayloads.Malformed e) { /* recorded as nulls; the core dead-letters the frame */ }
        m.put("bdSeq", bdSeq);
        m.put("timestamp", ts);
        m.put("receivedAt", f.receivedAt());
        return m;
    }

    @Override public Map<String, Object> record() {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("stage", "death-after-birth");
        r.put("injection", "25(a)");
        r.put("edge", edge);
        r.put("held", heldFrame);
        r.put("releasedAfter", releasedAfter);
        r.put("releasedBy", releasedBy);
        r.put("completed", completed());
        return r;
    }
}
