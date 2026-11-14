package yggdrasil.forseti.bridge;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.eclipse.tahu.message.model.MetricDataType;

/**
 * The transport-free core of the bridge: one call per MQTT frame, in arrival order, returning what to write. It decodes Sparkplug B,
 * writes one process message per DDATA (Decision 13), and keeps the session state of Decision 10 per edge, fanned out per device. It
 * holds no business logic: no attribution, no quality judgement, no time correction; the core reads what it writes.
 *
 * <p>Per edge it keeps the connections it has seen. A connection is a run of births with no assigned death between them; a rebirth
 * (an NBIRTH with the same {@code bdSeq} after an observed NCMD Rebirth) replaces the earlier birth within its connection. A session is
 * identified by the NBIRTH payload timestamp ({@code birthPublishTs}) because Muninn restarts {@code bdSeq} at 0 in every process. The
 * edge's {@code seq} restarts at 0 at every NBIRTH, a DBIRTH consumes {@code seq} 1, and both wrap at 255.
 *
 * <p>An NDEATH timestamp is never a time of death: Muninn builds the Will at connect, before the birth, and a stop-NDEATH after it.
 * While the edge's connection boundary can be established, an NDEATH is assigned among the unended connections of the same edge with
 * the same {@code bdSeq}: (i) the latest whose latest birth is at or before the death's timestamp (stop path); (ii) otherwise the
 * connection of the first birth after it (Will path); (iii) otherwise {@code session-ambiguous}. The boundary cannot be established
 * (spec Decision 10) while a missing NBIRTH is signalled (data without a birth, a DBIRTH repeated without an NBIRTH, a {@code seq}
 * returning to 0 without an NBIRTH), while two unended connections have births out of arrival order, or when another connection's
 * birth has the death's millisecond; every death is then {@code session-ambiguous}. A death closing the latest connection is
 * {@code session-down}; one closing an earlier connection is {@code stale-death} and leaves the current session open.
 *
 * <p>Data of a connection whose NBIRTH the bridge never observed is never written as a process message (plan D4): it goes to the
 * dead-letter, and the edge's next NDEATH yields the birthless {@code session-ambiguous} once per device that data named, or nothing.
 */
public final class BridgeCore {
    public static final String SEQ_GAP = "seq-gap", SESSION_DOWN = "session-down", SESSION_UP = "session-up", STALE_DEATH = "stale-death",
        SESSION_AMBIGUOUS = "session-ambiguous", DEFINITION_CHANGED = "definition-changed";

    /** Dead-letter reasons; every dead-lettered frame is dead-lettered exactly once. */
    public static final String MALFORMED = "malformed", ALIAS_ONLY = "alias-only", BIRTHLESS = "birthless", MISSING_BIRTH = "missing-birth",
        NO_DEVICE = "no-device", UNSUPPORTED_TYPE = "unsupported-message-type", REPEAT_DEATH = "repeat-death";

    private static final int SEQ_MOD = 256;

    private final Map<String, String> pins;
    private final Map<String, Edge> edges = new HashMap<>();
    private final Set<String> unpinned = new TreeSet<>();
    /** Identity of every born stale-death and session-ambiguous emitted, to the payload digest of the death that made it. */
    private final Map<String, String> deaths = new HashMap<>();

    /** @param pins pinned {@code definitionSha256} per device (the generation pin); a device without a pin is not compared. */
    public BridgeCore(Map<String, String> pins) { this.pins = Map.copyOf(pins); }

    /** Devices that came up in this run without a pin (recorded in the run record when the bridge runs without a routing manifest). */
    public Set<String> unpinnedSeen() { return Set.copyOf(unpinned); }

    /** A seq-gap whose following message carried no valid event time: it waits for the next one, which gives its {@code to}. */
    private record PendingGap(long atSeq, long missing, long from, List<String> devices) {}

    /** A connection: one MQTT session of the edge, from its first observed birth to its assigned death. */
    private static final class Connection {
        final long bdSeq;
        final List<Long> births = new ArrayList<>();
        String definitionSha256;
        boolean rebirth;          // the latest birth followed an NCMD Rebirth the bridge observed
        boolean strictAfter;      // opened by a second NBIRTH with the same bdSeq: a death closes it only by (i) with a later timestamp
        boolean ended;
        final Set<String> bornDevices = new LinkedHashSet<>();          // devices that came up under the latest birth
        final Map<String, Long> deviceSeq = new LinkedHashMap<>();       // every device of the connection and its last seq
        final List<PendingGap> pending = new ArrayList<>();
        Long lastEventTime;       // event time of the last received telemetry message (DBIRTH or DDATA) of the connection that had one
        Connection(long bdSeq) { this.bdSeq = bdSeq; }
        long latestBirth() { return births.get(births.size() - 1); }
        long firstBirth() { return births.get(0); }
        long maxBirth() { long m = Long.MIN_VALUE; for (long b : births) m = Math.max(m, b); return m; }
        void end() { ended = true; pending.clear(); }
    }

    /** Data of the edge while no observed birth covers it (plan D4): the devices its DDATA named and each one's last seq, and the
     *  dead-letter reason of its frames ({@code missing-birth} after a signal inside a born connection, {@code birthless} otherwise). */
    private static final class Birthless {
        final Map<String, Long> deviceSeq = new LinkedHashMap<>();
        final String reason;
        Birthless(String reason) { this.reason = reason; }
    }

    private static final class Edge {
        final List<Connection> connections = new ArrayList<>();
        Connection current;       // the connection of the latest observed birth
        Birthless birthless;      // non-null while a missing NBIRTH is signalled or no birth covers the data, until an NBIRTH or death
        long expectedSeq;
        boolean rebirthRequested;
        Long latestBirthTs;
        Long maxEventTime;        // the largest valid telemetry event time the bridge holds for the edge
        boolean covered() { return current != null && !current.ended && birthless == null; }
        /** Two unended connections whose births are out of arrival order: an earlier-arrived one has a birth after a later one's. */
        boolean birthsBackwards() {
            List<Connection> open = new ArrayList<>();
            for (Connection c : connections) if (!c.ended) open.add(c);
            for (int i = 0; i < open.size(); i++)
                for (int j = i + 1; j < open.size(); j++) if (open.get(i).maxBirth() > open.get(j).firstBirth()) return true;
            return false;
        }
    }

    /** Everything to write for one frame, in order. */
    public List<Emitted> accept(Frame f) {
        List<Emitted> out = new ArrayList<>();
        SparkplugTopic t = SparkplugTopic.parse(f.topic());
        if (t == null) { out.add(dead(f, MALFORMED, null, null)); return out; }
        switch (t.type()) {
            case "STATE", "DCMD" -> { return out; }   // host traffic: no edge session evidence
            case "NBIRTH", "NDEATH", "NCMD", "DBIRTH", "DDATA", "NDATA" -> { }
            default -> { out.add(dead(f, UNSUPPORTED_TYPE, t.edge(), t.device())); return out; }
        }
        boolean device = t.device() != null;
        if (device != (t.type().startsWith("D"))) { out.add(dead(f, MALFORMED, t.edge(), t.device())); return out; }
        SparkplugPayloads.Decoded p;
        try { p = SparkplugPayloads.decode(f.payload()); }
        catch (SparkplugPayloads.Malformed e) { out.add(dead(f, MALFORMED, t.edge(), t.device())); return out; }
        Edge e = edges.computeIfAbsent(t.edgeKey(), k -> new Edge());
        switch (t.type()) {
            case "NCMD" -> ncmd(e, p);
            case "NBIRTH" -> nbirth(e, t, f, p, out);
            case "NDEATH" -> ndeath(e, t, f, p, out);
            case "DBIRTH" -> dbirth(e, t, f, p, out);
            default -> data(e, t, f, p, out);
        }
        return out;
    }

    private static void ncmd(Edge e, SparkplugPayloads.Decoded p) {
        SparkplugPayloads.DecodedMetric m = p.aliasOnly() ? null : p.metric(SparkplugPayloads.REBIRTH);
        if (m != null && Boolean.TRUE.equals(m.value())) e.rebirthRequested = true;
    }

    private void nbirth(Edge e, SparkplugTopic t, Frame f, SparkplugPayloads.Decoded p, List<Emitted> out) {
        SparkplugPayloads.DecodedMetric def = p.aliasOnly() ? null : p.metric(SparkplugPayloads.GOVERNED_DEFINITION);
        SparkplugPayloads.DecodedMetric bd = p.aliasOnly() ? null : p.metric(SparkplugPayloads.BD_SEQ);
        Long bdSeq = bd == null ? null : SparkplugPayloads.integral(bd.value());
        if (p.aliasOnly()) { out.add(dead(f, ALIAS_ONLY, t.edge(), null)); return; }
        // Without its timestamp, bdSeq or definition bytes a birth cannot name a session; its data is then birthless (D4).
        if (p.timestamp() == null || bdSeq == null || def == null || !(def.value() instanceof byte[] bytes)) {
            out.add(dead(f, MALFORMED, t.edge(), null));
            return;
        }
        long ts = p.timestamp();
        Connection cur = e.current;
        boolean rebirth = e.rebirthRequested;
        if (cur != null && !cur.ended && e.birthless == null && cur.bdSeq == bdSeq && rebirth) {
            cur.births.add(ts);    // a rebirth: same connection, a new session
            cur.rebirth = true;
            cur.definitionSha256 = sha256(bytes);
            cur.bornDevices.clear();
            cur.pending.clear();
        } else {
            boolean strict = false;
            if (cur != null && !cur.ended && e.birthless == null && cur.bdSeq == bdSeq) {
                // A second NBIRTH with the same bdSeq, with neither an assigned death nor an observed rebirth between: the earlier
                // connection ends here as session-ambiguous, coverage unknown from its last normal observation (spec Decision 10).
                List<Emitted> events = new ArrayList<>();
                boolean repeated = bornDeaths(e, cur, t, SESSION_AMBIGUOUS, deathPayload(detected(e), ts, bdSeq, f.payload()), null, events);
                out.addAll(events);
                if (events.isEmpty()) out.add(dead(f, repeated ? REPEAT_DEATH : NO_DEVICE, t.edge(), null));
                cur.end();
                strict = true;
            }
            Connection c = new Connection(bdSeq);
            c.births.add(ts);
            c.rebirth = rebirth;
            c.strictAfter = strict;
            c.definitionSha256 = sha256(bytes);
            e.connections.add(c);
            e.current = c;
        }
        e.birthless = null;
        e.expectedSeq = 1;
        e.rebirthRequested = false;
        e.latestBirthTs = ts;
    }

    private void dbirth(Edge e, SparkplugTopic t, Frame f, SparkplugPayloads.Decoded p, List<Emitted> out) {
        if (p.seq() == null) { out.add(dead(f, MALFORMED, t.edge(), t.device())); return; }
        long seq = p.seq();
        // A DBIRTH never names a device for the birthless events (D4 counts dead-lettered DDATA only).
        if (!e.covered()) { out.add(dead(f, birthlessRun(e, BIRTHLESS).reason, t.edge(), t.device())); return; }
        Connection c = e.current;
        if (c.bornDevices.contains(t.device())) {
            // A DBIRTH repeated without an NBIRTH: the birth of a connection was lost (QoS 0). No event and no connection ends here;
            // from this frame on, the edge's data belongs to the unobserved birth until the next NBIRTH.
            out.add(dead(f, birthlessRun(e, MISSING_BIRTH).reason, t.edge(), t.device()));
            return;
        }
        Long et = p.aliasOnly() ? null : eventTime(p);
        up(c, t, seq, out);
        gap(e, c, t, seq, et, out);
        // A DBIRTH is never written as a process message; its member timestamps are observations of the session. An alias-only one
        // still names its device and consumes its seq, and its frame is kept on the dead-letter as the refused input.
        if (p.aliasOnly()) out.add(dead(f, ALIAS_ONLY, t.edge(), t.device()));
        else observe(e, c, et);
    }

    private void data(Edge e, SparkplugTopic t, Frame f, SparkplugPayloads.Decoded p, List<Emitted> out) {
        if (p.seq() == null || p.timestamp() == null) { out.add(dead(f, MALFORMED, t.edge(), t.device())); return; }
        long seq = p.seq();
        if (!e.covered()) { birthlessData(e, t, f, seq, BIRTHLESS, out); return; }
        Connection c = e.current;
        // seq returning to 0 without an NBIRTH (an expected wrap 255 -> 0 is not one): the signal, handled like any later birthless data.
        if (seq == 0 && e.expectedSeq != 0) { birthlessData(e, t, f, seq, MISSING_BIRTH, out); return; }
        Long et = p.aliasOnly() ? null : eventTime(p);
        if (t.device() != null && !c.bornDevices.contains(t.device())) up(c, t, seq, out);   // its DBIRTH was lost: the data names it
        gap(e, c, t, seq, et, out);
        if (t.device() != null) c.deviceSeq.put(t.device(), seq);
        observe(e, c, et);
        if (p.aliasOnly()) { out.add(dead(f, ALIAS_ONLY, t.edge(), t.device())); return; }
        if (t.device() == null) { out.add(dead(f, NO_DEVICE, t.edge(), null)); return; }   // NDATA: no device to key it
        List<Emitted.Member> members = new ArrayList<>();
        for (SparkplugPayloads.DecodedMetric m : p.metrics())
            members.add(new Emitted.Member(m.name(), value(m), SparkplugPayloads.property(m.properties(), SparkplugPayloads.UA_STATUSCODE),
                SparkplugPayloads.property(m.properties(), SparkplugPayloads.QUALITY), m.timestamp(),
                SparkplugPayloads.property(m.properties(), SparkplugPayloads.UA_SERVER_TIMESTAMP)));
        out.add(new Emitted.ProcessMessage(t.edge(), t.device(), c.latestBirth(), c.bdSeq, seq, p.timestamp(), f.receivedAt(),
            c.definitionSha256, List.copyOf(members)));
    }

    private void ndeath(Edge e, SparkplugTopic t, Frame f, SparkplugPayloads.Decoded p, List<Emitted> out) {
        SparkplugPayloads.DecodedMetric bd = p.aliasOnly() ? null : p.metric(SparkplugPayloads.BD_SEQ);
        Long b = bd == null ? null : SparkplugPayloads.integral(bd.value());
        if (p.aliasOnly()) { out.add(dead(f, ALIAS_ONLY, t.edge(), null)); return; }
        if (b == null || p.timestamp() == null) { out.add(dead(f, MALFORMED, t.edge(), null)); return; }
        long d = p.timestamp();
        if (e.birthless != null || e.current == null || e.birthsBackwards()) { unknownBoundaryDeath(e, t, f, d, b, out); return; }
        Connection target = null;
        boolean byStopPath = false;
        for (Connection c : e.connections)   // (i) the latest unended connection whose latest birth is at or before the death
            if (!c.ended && c.bdSeq == b && c.latestBirth() <= d && (target == null || c.latestBirth() > target.latestBirth())) target = c;
        if (target != null) byStopPath = true;
        else {
            Long first = null;
            for (Connection c : e.connections)   // (ii) the connection of the first birth after it
                if (!c.ended && c.bdSeq == b) for (long birth : c.births) if (birth > d && (first == null || birth < first)) { first = birth; target = c; }
        }
        for (Connection c : e.connections) if (c != target) for (long birth : c.births) if (birth == d) { unknownBoundaryDeath(e, t, f, d, b, out); return; }
        if (target == null || (target.strictAfter && !(byStopPath && d > target.latestBirth()))) {
            // (iii), or a death a strict connection does not take: session-ambiguous on the latest connection, which stays open.
            List<Emitted> events = new ArrayList<>();
            boolean repeated = bornDeaths(e, e.current, t, SESSION_AMBIGUOUS, deathPayload(detected(e), d, b, f.payload()), null, events);
            out.addAll(events);
            if (events.isEmpty() || repeated) out.add(dead(f, repeated ? REPEAT_DEATH : NO_DEVICE, t.edge(), null));
            return;
        }
        target.end();
        if (target.deviceSeq.isEmpty()) { out.add(dead(f, NO_DEVICE, t.edge(), null)); return; }
        if (target == e.current) {
            long from = target.lastEventTime != null ? target.lastEventTime : target.latestBirth();
            for (Map.Entry<String, Long> x : target.deviceSeq.entrySet()) {
                Map<String, Object> pl = new LinkedHashMap<>();
                pl.put("from", from);
                pl.put("to", null);
                out.add(new Emitted.SessionEvent(t.edge(), x.getKey(), target.latestBirth(), target.bdSeq, SESSION_DOWN, x.getValue(), pl));
            }
        } else {
            List<Emitted> events = new ArrayList<>();
            boolean repeated = bornDeaths(e, target, t, STALE_DEATH, deathPayload(detected(e), d, b, f.payload()), null, events);
            out.addAll(events);
            if (repeated) out.add(dead(f, REPEAT_DEATH, t.edge(), null));
        }
    }

    /** A death while the connection boundary cannot be established: session-ambiguous for every device. Devices named by the edge's
     *  dead-lettered birthless DDATA get the birthless shape (plan D4); the other devices of the unended born connection get the born
     *  shape with this death's payload, and that connection ends. A death that names no device is dead-lettered. */
    private void unknownBoundaryDeath(Edge e, SparkplugTopic t, Frame f, long d, long b, List<Emitted> out) {
        Map<String, Long> named = e.birthless == null ? Map.of() : e.birthless.deviceSeq;
        Long detected = detected(e);
        Map<String, Object> payload = deathPayload(detected, d, b, f.payload());
        List<Emitted> events = new ArrayList<>();
        for (Map.Entry<String, Long> x : named.entrySet())
            events.add(new Emitted.SessionEvent(t.edge(), x.getKey(), null, b, SESSION_AMBIGUOUS, x.getValue(), payload));
        boolean repeated = false;
        Connection c = e.current;
        if (c != null && !c.ended) {
            repeated = bornDeaths(e, c, t, SESSION_AMBIGUOUS, payload, named.keySet(), events);
            c.end();
        }
        out.addAll(events);
        if (events.isEmpty() || repeated) out.add(dead(f, repeated ? REPEAT_DEATH : NO_DEVICE, t.edge(), null));
        e.birthless = null;
    }

    /** Born-shape stale-death or session-ambiguous for every device of {@code c} not in {@code skip}, at most one per identity (edge,
     *  device, birthPublishTs, bdSeq, kind, atSeq): a later death with a different payload digest for the same identity emits nothing
     *  (its unknown interval lies inside the first one's) and the method returns true so the frame is dead-lettered repeat-death. */
    private boolean bornDeaths(Edge e, Connection c, SparkplugTopic t, String kind, Map<String, Object> payload, Set<String> skip, List<Emitted> out) {
        if (c == null) return false;
        boolean repeated = false;
        String digest = (String) payload.get("payloadDigest");
        for (Map.Entry<String, Long> x : c.deviceSeq.entrySet()) {
            if (skip != null && skip.contains(x.getKey())) continue;
            String id = t.edgeKey() + "\u0000" + x.getKey() + "\u0000" + c.latestBirth() + "\u0000" + c.bdSeq + "\u0000" + kind + "\u0000" + x.getValue();
            String seen = deaths.putIfAbsent(id, digest);
            if (seen != null && !seen.equals(digest)) { repeated = true; continue; }
            out.add(new Emitted.SessionEvent(t.edge(), x.getKey(), c.latestBirth(), c.bdSeq, kind, x.getValue(), payload));
        }
        return repeated;
    }

    /** Opens (or continues) the edge's birthless run; the reason of the run that is already open is kept. */
    private static Birthless birthlessRun(Edge e, String reason) {
        if (e.birthless == null) e.birthless = new Birthless(reason);
        return e.birthless;
    }

    /** Birthless DDATA (or NDATA): dead-lettered once, and a DDATA's device is named for the edge's next death. */
    private static void birthlessData(Edge e, SparkplugTopic t, Frame f, long seq, String reason, List<Emitted> out) {
        Birthless run = birthlessRun(e, reason);
        if (t.device() != null) run.deviceSeq.put(t.device(), seq);
        out.add(dead(f, run.reason, t.edge(), t.device()));
    }

    private void up(Connection c, SparkplugTopic t, long seq, List<Emitted> out) {
        c.bornDevices.add(t.device());
        c.deviceSeq.put(t.device(), seq);
        Map<String, Object> pl = new LinkedHashMap<>();
        pl.put("at", c.latestBirth());
        pl.put("rebirth", c.rebirth);
        out.add(new Emitted.SessionEvent(t.edge(), t.device(), c.latestBirth(), c.bdSeq, SESSION_UP, seq, pl));
        String pin = pins.get(t.device());
        if (pin == null) unpinned.add(t.device());
        else if (!pin.equals(c.definitionSha256))
            out.add(new Emitted.SessionEvent(t.edge(), t.device(), c.latestBirth(), c.bdSeq, DEFINITION_CHANGED, seq, null));
    }

    /** seq-gap with the event times of the messages before and after and the missing count; seq wraps at 256. A gap whose following
     *  message has no valid event time waits in the connection until a message that has one; that time is its {@code to}. No seq-gap
     *  is written with a null {@code to}, and no time is made up. */
    private static void gap(Edge e, Connection c, SparkplugTopic t, long seq, Long after, List<Emitted> out) {
        if (seq != e.expectedSeq) {
            long missing = Math.floorMod(seq - e.expectedSeq, SEQ_MOD);
            long before = c.lastEventTime != null ? c.lastEventTime : c.latestBirth();
            c.pending.add(new PendingGap(seq, missing, before, List.copyOf(c.deviceSeq.keySet())));
        }
        e.expectedSeq = (seq + 1) % SEQ_MOD;
        if (after == null) return;
        for (PendingGap g : c.pending)
            for (String dev : g.devices()) {
                Map<String, Object> pl = new LinkedHashMap<>();
                pl.put("from", g.from());
                pl.put("to", after);
                pl.put("missing", g.missing());
                out.add(new Emitted.SessionEvent(t.edge(), dev, c.latestBirth(), c.bdSeq, SEQ_GAP, g.atSeq(), pl));
            }
        c.pending.clear();
    }

    private static void observe(Edge e, Connection c, Long eventTime) {
        if (eventTime == null) return;
        c.lastEventTime = eventTime;
        if (e.maxEventTime == null || eventTime > e.maxEventTime) e.maxEventTime = eventTime;
    }

    /** detectedEventTime: the largest valid telemetry event time held for the edge; if none, the latest birth time; if neither, null.
     *  Taken once, when the death (or the second birth) is observed. */
    private static Long detected(Edge e) { return e.maxEventTime != null ? e.maxEventTime : e.latestBirthTs; }

    private static Map<String, Object> deathPayload(Long detected, long rawPublishTs, long bdSeq, byte[] raw) {
        Map<String, Object> pl = new LinkedHashMap<>();
        pl.put("detectedEventTime", detected);
        pl.put("rawPublishTs", rawPublishTs);
        pl.put("bdSeq", bdSeq);
        pl.put("payloadDigest", sha256(raw));
        return pl;
    }

    /** A message's event time: the largest member source timestamp; null when none carries one. */
    private static Long eventTime(SparkplugPayloads.Decoded p) {
        Long t = null;
        for (SparkplugPayloads.DecodedMetric m : p.metrics()) if (m.timestamp() != null && m.timestamp() >= 0 && m.timestamp() <= Records.MAX_SAFE && (t == null || m.timestamp() > t)) t = m.timestamp();
        return t;
    }

    /** A metric value as a record value, member by member, so one value the contract cannot carry never costs the message its healthy
     *  members: the types Muninn maps from OPC UA (Boolean, Int32, UInt32, Int64, Float, Double, String, DateTime) and the other scalar
     *  Sparkplug types; a Float widens exactly to binary64; a DateTime is epoch ms. A signed infinity stays a Double infinity (written as
     *  the literal 1e400 or -1e400, which both readers read as that infinity: spec reason {@code non-finite}). NaN, which neither reader
     *  accepts as a token, and the types with no JSON value in the contract (Bytes, DataSet, Template, File, arrays) become null, which
     *  the readers reject as that member's value; the member keeps its name and metadata. */
    private static Object value(SparkplugPayloads.DecodedMetric m) {
        if (m.isNull()) return null;
        Object v = m.value();
        MetricDataType type = m.type();
        if (v == null || type == null) return null;
        // Matched by name (Tahu's MetricDataType is a class whose toString is the type name, the vocabulary Muninn writes).
        switch (type.toString()) {
            case "Boolean": return v instanceof Boolean ? v : null;
            case "String": case "Text": case "UUID": return v instanceof String ? v : null;
            case "Float": case "Double": {
                if (!(v instanceof Float) && !(v instanceof Double)) return null;
                double d = ((Number) v).doubleValue();
                return Double.isNaN(d) ? null : (Object) d;
            }
            case "Int8": case "Int16": case "Int32": case "Int64": case "UInt8": case "UInt16": case "UInt32": case "UInt64": {
                BigInteger i;
                if (v instanceof BigInteger b) i = b;
                else if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) i = BigInteger.valueOf(((Number) v).longValue());
                else return null;
                // Both readers take a numeric value as binary64 and digest it in JCS, whose integers stop at 2^53 - 1: an integer beyond
                // that is written as the binary64 the readers would hold, so the record keeps its canonical digest.
                return i.abs().compareTo(BigInteger.valueOf(Records.MAX_SAFE)) <= 0 ? (Object) i.longValue() : (Object) i.doubleValue();
            }
            case "DateTime": return v instanceof Date dt ? (Object) dt.getTime() : null;
            default: return null;
        }
    }

    private static Emitted.DeadLetter dead(Frame f, String reason, String edge, String device) {
        return new Emitted.DeadLetter(f.topic(), f.payload().clone(), f.receivedAt(), reason, edge, device);
    }

    static String sha256(byte[] b) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b)); }
        catch (NoSuchAlgorithmException x) { throw new IllegalStateException(x); }
    }
}
