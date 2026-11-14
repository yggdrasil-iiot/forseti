package yggdrasil.forseti.loop;

import java.util.*;
import yggdrasil.forseti.cli.live.DrivePlan;
import yggdrasil.forseti.cli.live.LiveEquipment;

/** An in-process stand-in for the Bifrost sim's Line2 contract (step-4 plan Decisions 3, 5, 6) and the driver's clock in one: a Write
 *  request only sets controls; a scan, every {@code scanMs} of fake time, commits every control to its live member and stamps every
 *  member with ONE source timestamp, strictly increasing and offset far from the client clock ({@link #SKEW}) so a test can tell a
 *  sim time from a driver time. Time moves only in {@link #sleep}. */
final class FakeSim implements SimPort, Driver.Clock {
    static final long SKEW = 1_790_000_000_000L;
    static final long BAD_NODE = 0x80340000L;

    final String ns;
    final long scanMs;
    long now = 0, nextScan;
    long sourceTs;
    final Map<String, Object> live = new HashMap<>(), control = new HashMap<>();
    final Map<String, Long> liveStatus = new HashMap<>(), statusControl = new HashMap<>();
    /** Every Write request: its items, the fake time it arrived and the source timestamp of the scan that committed it. */
    record Request(List<DrivePlan.Write> items, long at, long[] committedAt) {}
    final List<Request> requests = new ArrayList<>();
    final List<Request> pending = new ArrayList<>();
    int reads;
    /** Test knobs: scan once right before applying the next write; answer the next read with one member a scan old; refuse a node. */
    boolean scanBeforeNextWrite, straddleNextRead;
    String refuse;
    final Set<String> missing = new HashSet<>();

    FakeSim(LiveEquipment.LiveStream stream, long scanMs, long phase) {
        this.scanMs = scanMs;
        this.nextScan = phase;
        this.ns = stream.equipment().values().iterator().next().namespaceUri();
        this.sourceTs = SKEW;
        for (LiveEquipment.Equipment e : stream.equipment().values()) for (LiveEquipment.Member m : e.members()) {
            Object initial = switch (m.type()) { case "Boolean" -> false; case "String" -> ""; default -> 0.0; };
            live.put(e.live(m.name()), initial);
            control.put(e.live(m.name()), initial);
            liveStatus.put(e.live(m.name()), 0L);
            statusControl.put(e.live(m.name()), 0L);
        }
    }

    void scan() {
        sourceTs = Math.max(sourceTs + 1, SKEW + now);
        live.putAll(control);
        liveStatus.putAll(statusControl);
        for (Request r : pending) r.committedAt()[0] = sourceTs;
        pending.clear();
    }

    @Override public long now() { return now; }

    @Override
    public void sleep(long ms) {
        long target = now + ms;
        while (nextScan <= target) { now = nextScan; scan(); nextScan += scanMs; }
        now = target;
    }

    @Override
    public List<Value> read(String namespaceUri, List<String> ids) {
        reads++;
        List<Value> out = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            boolean exists = namespaceUri.equals(ns) && !missing.contains(id) && (live.containsKey(id) || isControl(id));
            if (!exists) { out.add(new Value(false, null, BAD_NODE, null)); continue; }
            if (isControl(id)) { out.add(new Value(true, null, 0, null)); continue; }
            long ts = straddleNextRead && i == 0 ? sourceTs - scanMs : sourceTs;
            out.add(new Value(true, live.get(id), liveStatus.get(id), ts));
        }
        straddleNextRead = false;
        return out;
    }

    private boolean isControl(String id) {
        if (!id.startsWith("Sim/")) return false;
        String rest = id.substring(4);
        if (rest.endsWith(".Value")) return live.containsKey(rest.substring(0, rest.length() - 6));
        if (rest.endsWith(".StatusCode")) return live.containsKey(rest.substring(0, rest.length() - 11));
        return false;
    }

    @Override
    public List<Long> write(String namespaceUri, List<DrivePlan.Write> items) {
        if (scanBeforeNextWrite) { scanBeforeNextWrite = false; scan(); }
        List<Long> codes = new ArrayList<>();
        for (DrivePlan.Write w : items) {
            if (w.nodeId().equals(refuse)) { codes.add(0x80740000L); continue; }   // Bad_TypeMismatch
            String rest = w.nodeId().substring(4);
            if (rest.endsWith(".Value")) control.put(rest.substring(0, rest.length() - 6), w.value());
            else statusControl.put(rest.substring(0, rest.length() - 11), ((Number) w.value()).longValue());
            codes.add(0L);
        }
        Request r = new Request(List.copyOf(items), now, new long[] {-1});
        requests.add(r);
        pending.add(r);
        return codes;
    }

    @Override public void close() {}
}
