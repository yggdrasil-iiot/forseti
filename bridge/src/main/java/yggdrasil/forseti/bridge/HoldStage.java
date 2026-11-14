package yggdrasil.forseti.bridge;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Injection 1, "telemetry held 10 min in the bridge and released" (plan D3). Process messages received inside one window of bridge
 *  time, {@code [first + startOffsetMs, first + startOffsetMs + holdMs)} where {@code first} is the receipt of the first process
 *  message, are held and then released together, in arrival order, before anything received after the window. Session events and
 *  dead-letters pass untouched: the injection delays telemetry only. The records are not changed: {@code bridgeReceivedAt} stays the
 *  true receipt time, so the delay is visible as the gap between receipt and append. The run record states whether the window ran to
 *  its end ({@code completed}); a run stopped inside the window is never recorded as an executed injection. */
public final class HoldStage implements Stage {
    private final long startOffsetMs, holdMs;
    private final List<Emitted> held = new ArrayList<>();
    private Long windowStart, releasedAt;
    private long heldCount;
    private String releasedBy;

    public HoldStage(long startOffsetMs, long holdMs) {
        if (startOffsetMs < 0 || holdMs <= 0) throw new IllegalArgumentException("hold needs startOffsetMs >= 0 and holdMs > 0");
        this.startOffsetMs = startOffsetMs;
        this.holdMs = holdMs;
    }

    @Override public List<Emitted> apply(Emitted e, long now) {
        List<Emitted> out = new ArrayList<>(tick(now));
        if (!(e instanceof Emitted.ProcessMessage m)) { out.add(e); return out; }
        if (windowStart == null) windowStart = m.bridgeReceivedAt() + startOffsetMs;
        long t = m.bridgeReceivedAt();
        if (releasedAt == null && t >= windowStart && t < windowStart + holdMs) { held.add(e); heldCount++; }
        else out.add(e);
        return out;
    }

    @Override public List<Emitted> tick(long now) {
        if (releasedAt != null || windowStart == null || now < windowStart + holdMs) return List.of();
        return release(now, "window-end");
    }

    /** A drain before the window ends releases what is held, so nothing is dropped, but the injection is then recorded as incomplete
     *  ({@code completed: false}): the telemetry was not held for the declared time. */
    @Override public List<Emitted> drain(long now) { return releasedAt != null ? List.of() : release(now, "drain"); }

    /** The injection ran as declared: the window opened and was released at its end, not by a drain. */
    public boolean completed() { return "window-end".equals(releasedBy); }

    private List<Emitted> release(long now, String by) {
        releasedAt = now;
        releasedBy = by;
        List<Emitted> out = List.copyOf(held);
        held.clear();
        return out;
    }

    @Override public Map<String, Object> record() {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("stage", "hold-and-release");
        r.put("injection", "1");
        r.put("startOffsetMs", startOffsetMs);
        r.put("holdMs", holdMs);
        r.put("windowStart", windowStart);
        r.put("windowEnd", windowStart == null ? null : windowStart + holdMs);
        r.put("held", heldCount);
        r.put("releasedAt", releasedAt);
        r.put("releasedBy", releasedBy);
        r.put("completed", completed());
        return r;
    }
}
