package yggdrasil.forseti.bridge;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Injection 9, "source clock +30 s in the bridge" (plan D3): every member {@code sourceTs} of every process message is moved by
 *  {@code offsetMs}; a member without one stays without one, and nothing else changes. It runs after the session tracker, so seq-gap
 *  and session-down times keep the source's own clock, and the raw frames (raw-frame log, dead-letter) keep the wire's. */
public final class SourceOffsetStage implements Stage {
    private final long offsetMs;
    private long messages, members;

    public SourceOffsetStage(long offsetMs) {
        if (offsetMs == 0) throw new IllegalArgumentException("a source offset of 0 is no stage");
        this.offsetMs = offsetMs;
    }

    @Override public List<Emitted> apply(Emitted e, long now) {
        if (!(e instanceof Emitted.ProcessMessage m)) return List.of(e);
        Emitted.ProcessMessage shifted = m.withSourceOffset(offsetMs);
        for (Emitted.Member x : shifted.members()) {
            if (x.sourceTs() == null) continue;
            if (x.sourceTs() < 0 || x.sourceTs() > Records.MAX_SAFE)
                throw new BridgeFailure("the source offset moved a sourceTs outside 0..2^53-1 on " + m.device() + " seq " + m.seq());
            members++;
        }
        messages++;
        return List.of(shifted);
    }
    @Override public List<Emitted> tick(long now) { return List.of(); }
    @Override public List<Emitted> drain(long now) { return List.of(); }

    @Override public Map<String, Object> record() {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("stage", "source-offset");
        r.put("injection", "9");
        r.put("offsetMs", offsetMs);
        r.put("messages", messages);
        r.put("membersShifted", members);
        return r;
    }
}
