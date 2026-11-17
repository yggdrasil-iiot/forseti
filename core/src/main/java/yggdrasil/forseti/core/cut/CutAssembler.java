package yggdrasil.forseti.core.cut;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import java.util.function.Function;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.journal.Progress;

/** The cut protocol of one progress writer (spec "Cut barriers, progress records and the disposition ledgers", Decision 12): which
 *  barriers the writer requires, which cut each received input belongs to, when a cut closes, which cuts a progress merges, the
 *  recovery epoch a reinitialized writer selects and adopts, and the {@code admissionW} the inputs of a closing cut are admitted against.
 *  It is pure and engine-free: no transport, no checkpoint, no clock. The adapter feeds it the inputs and barriers in the order its
 *  channels deliver them, supplies its native event time and, when a recovering writer meets a barrier of a later epoch, a fresh read
 *  of the recovery control; it persists the assembler's state with {@link #toJson} and restores it with {@link #fromJson}.
 *
 *  <p>Rules, each the spec's:
 *  <ul>
 *  <li>An input belongs to the cut after the last barrier received on its origin (channels are first-in-first-out, so an input above
 *      {@code throughOffset} of cut k-1 and at or below that of cut k arrives after barrier k-1 and before barrier k).</li>
 *  <li>A cut is complete when every required barrier of it was received with one {@code restartEpoch} and one {@code recoveryRef}; a
 *      station writer does not count a barrier whose {@code idleInputs} declare it {@code starved} (the reject collectors are built
 *      with {@code starvable = false} and count every barrier).</li>
 *  <li>A writer that is not recovering closes the lowest complete cut r above its last progress for which it holds no input of a later
 *      cut (the receive-order trigger: inputs of cut r+1 received before progress r skip r), once its native time is at or past its last
 *      reported {@code W1}. Every cut between its last progress and r is merged into r ({@code mergedCutIds}).</li>
 *  <li>A reinitialized writer ({@link #reinitialize}) keeps cuts up to its last committed progress j as committed, keeps the inputs of
 *      barriers with {@code restartEpoch < E} and writes no progress for them, and closes the first complete cut r with
 *      {@code restartEpoch == E}, merging j+1 .. r-1; a barrier above E makes it read the control again and adopt the latest
 *      {@code targetEpoch} when that is at or above the barrier's epoch, otherwise the protocol fails ({@link ProtocolError}).</li>
 *  <li>The inputs of a closing cut, merged cuts included, are admitted against the {@code W1} of the last committed progress
 *      ({@code admissionW}, null before the first progress).</li>
 *  </ul> */
public final class CutAssembler<T> {
    public static final int VERSION = 1;
    public static final String STARVED = "starved", UNASSIGNED = "unassigned";

    /** One {@code idleInputs[]} entry of a barrier (spec 600). */
    public record Idle(String station, String topic, int partition, long frozenAt, String kind, String evidenceRef) {
        public Idle {
            Objects.requireNonNull(station); Objects.requireNonNull(topic); Objects.requireNonNull(kind);
            if (!kind.equals(STARVED) && !kind.equals(UNASSIGNED)) throw new IllegalArgumentException("idleInputs kind " + kind);
        }
        public ObjectNode toJson() {
            ObjectNode n = Json.object().put("station", station).put("topic", topic).put("partition", partition);
            Json.putSafe(n, "frozenAt", frozenAt);
            return n.put("kind", kind).put("evidenceRef", evidenceRef);
        }
        public static Idle fromJson(JsonNode n) {
            return new Idle(Json.text(n, "station"), Json.text(n, "topic"), (int) Json.safeLong(n.get("partition"), "idleInputs.partition", 0, Integer.MAX_VALUE),
                Json.safeLong(n.get("frozenAt"), "idleInputs.frozenAt", Json.ANY), Json.text(n, "kind"), n.path("evidenceRef").isNull() ? null : Json.text(n, "evidenceRef"));
        }
    }

    /** A cut barrier as a writer receives it on one of its input channels. */
    public record Barrier(long cutId, Progress.Origin origin, long throughOffset, long restartEpoch, String recoveryRef, List<Idle> idleInputs) {
        public Barrier {
            if (cutId < 1 || cutId > Json.MAX_SAFE) throw new IllegalArgumentException("barrier cutId " + cutId);
            if (throughOffset < -1 || throughOffset > Json.MAX_SAFE) throw new IllegalArgumentException("barrier throughOffset " + throughOffset);
            if (restartEpoch < 0) throw new IllegalArgumentException("barrier restartEpoch " + restartEpoch);
            Objects.requireNonNull(origin);
            idleInputs = List.copyOf(idleInputs);
        }
        /** A barrier of epoch 0 without idle inputs (the step-3 reference harness). */
        public Barrier(long cutId, Progress.Origin origin, long throughOffset) { this(cutId, origin, throughOffset, 0, null, List.of()); }
        public Progress.BarrierRef ref() { return new Progress.BarrierRef(cutId, origin, throughOffset); }
        boolean starves(String writer) {
            if (writer == null) return false;
            for (Idle i : idleInputs) if (i.kind().equals(STARVED) && i.station().equals(writer)) return true;
            return false;
        }
        ObjectNode toJson() {
            ObjectNode n = ref().toJson();
            Json.putSafe(n, "restartEpoch", restartEpoch);
            n.put("recoveryRef", recoveryRef);
            ArrayNode a = n.putArray("idleInputs");
            idleInputs.forEach(i -> a.add(i.toJson()));
            return n;
        }
        static Barrier fromJson(JsonNode n) {
            Progress.BarrierRef r = Progress.BarrierRef.fromJson(n);
            List<Idle> idle = new ArrayList<>();
            for (JsonNode i : n.path("idleInputs")) idle.add(Idle.fromJson(i));
            return new Barrier(r.cutId(), r.origin(), r.throughOffset(), Json.safeLong(n.get("restartEpoch"), "restartEpoch", Json.NON_NEGATIVE),
                n.path("recoveryRef").isNull() ? null : Json.text(n, "recoveryRef"), idle);
        }
    }

    /** The latest recovery declaration of the writer's generation, read from {@code forseti.recovery-control} up to its end. */
    public record Declaration(long offset, long targetEpoch, String recoveryRef) {}

    /** A fresh read of the recovery control (never a cached value). */
    @FunctionalInterface public interface Control { Declaration latest(); }

    /** A cut the writer closes now: the adapter applies {@code inputs} in the contract order against {@code admissionW}, fires its timers
     *  up to {@code w1} and writes its progress {@code cutId} with these {@code mergedCutIds}, {@code barrierRefs}, {@code restartEpoch}
     *  and {@code recoveryRef}. {@code recovered}: the close ends a recovery. {@code w1} is null for a writer without event time. */
    public record Closed<T>(long cutId, List<Long> mergedCutIds, List<Progress.BarrierRef> barrierRefs, long restartEpoch, String recoveryRef,
                            Long admissionW, Long w1, List<T> inputs, boolean recovered) {}

    /** A violation of the protocol the writer cannot resolve itself: the adapter records it as {@code error} (spec 418). */
    public static final class ProtocolError extends IllegalStateException {
        public final String code;
        public ProtocolError(String code, String detail) { super(code + ": " + detail); this.code = code; }
    }

    private final String writer;
    private final List<Progress.Origin> required;
    private final boolean starvable;
    private long lastProgress;
    private Long lastW1;
    private boolean recovering;
    private long epoch;
    private final Map<Progress.Origin, Long> lastBarrier = new HashMap<>();
    private final TreeMap<Long, Map<Progress.Origin, Barrier>> barriers = new TreeMap<>();
    private final TreeMap<Long, List<T>> held = new TreeMap<>();
    /** Whether the protocol state changed since the last {@link #drainChanged} (true until the first). */
    private boolean changed = true;

    /** A writer at its first start in the generation: no progress, {@code admissionW} null. {@code writer} is the station a
     *  {@code starved} declaration names (null for a reject collector); {@code required} its required barrier set, in the order its
     *  {@code barrierRefs} list them; {@code starvable} whether a {@code starved} declaration naming {@code writer} leaves a barrier
     *  incomplete (station writers only). */
    public CutAssembler(String writer, List<Progress.Origin> required, boolean starvable) {
        if (required.isEmpty()) throw new IllegalArgumentException("a writer requires at least one barrier");
        if (new HashSet<>(required).size() != required.size()) throw new IllegalArgumentException("required barriers repeat an origin: " + required);
        if (starvable && writer == null) throw new IllegalArgumentException("a starvable writer names its station");
        this.writer = writer;
        this.required = List.copyOf(required);
        this.starvable = starvable;
    }

    /** A reinitialization (any (re)initialization of the writer's task after its first start, whether or not a checkpoint or store had
     *  anything to restore): the writer enters recovery with {@code E = declaration.targetEpoch}, keeping its restored last progress,
     *  {@code admissionW} and held inputs. The adapter reads the declaration inside the writer's initialization, before any business
     *  input. */
    public void reinitialize(Declaration declaration) {
        recovering = true;
        epoch = declaration.targetEpoch();
        changed = true;
    }

    public String writer() { return writer; }
    public List<Progress.Origin> required() { return required; }
    public long lastProgress() { return lastProgress; }
    /** The W1 of the last committed progress: the bound every input of the next closing cut is admitted against (null before the
     *  first progress). */
    public Long admissionW() { return lastW1; }
    public boolean recovering() { return recovering; }
    /** The epoch E fixed at the last reinitialization or adopted since (0 for a writer never reinitialized). */
    public long epoch() { return epoch; }
    /** Inputs received and not yet closed. */
    public int heldInputs() { int n = 0; for (List<T> l : held.values()) n += l.size(); return n; }

    /** One input received on {@code origin}: held in the cut after the last barrier received there. */
    public void input(Progress.Origin origin, T item) {
        if (!required.contains(origin)) throw new IllegalArgumentException(writer + ": an input on " + origin + ", which is not a required barrier channel");
        held.computeIfAbsent(lastBarrier.getOrDefault(origin, lastProgress) + 1, k -> new ArrayList<>()).add(item);
        changed = true;
    }

    /** One barrier received. {@code control} is read only when a recovering writer meets a barrier above its epoch. */
    public void barrier(Barrier b, Control control) {
        if (!required.contains(b.origin())) throw new IllegalArgumentException(writer + ": a barrier on " + b.origin() + ", which is not required");
        long last = lastBarrier.getOrDefault(b.origin(), lastProgress);
        if (b.cutId() <= last && b.cutId() > lastProgress)
            throw new ProtocolError("barrier-order", writer + ": barrier of cut " + b.cutId() + " on " + b.origin() + " after cut " + last);
        if (b.cutId() <= lastProgress) return;   // a cut already committed (a replay after a restore): never applied twice
        lastBarrier.put(b.origin(), b.cutId());
        changed = true;
        if (recovering && b.restartEpoch() > epoch) {
            Declaration d = control.latest();
            if (d == null || d.targetEpoch() < b.restartEpoch())
                throw new ProtocolError("recovery-epoch", writer + ": barrier of cut " + b.cutId() + " with restartEpoch " + b.restartEpoch() + " above E " + epoch
                    + ", latest declaration " + (d == null ? "none" : "targetEpoch " + d.targetEpoch()));
            epoch = d.targetEpoch();
        }
        if (starvable && b.starves(writer)) return;   // a starved input is not a completed barrier for a station writer
        barriers.computeIfAbsent(b.cutId(), k -> new LinkedHashMap<>()).put(b.origin(), b);
    }

    /** The barriers of {@code cutId} when every required one was received, null otherwise; barriers of one cut with different epochs or
     *  recovery references are never combined. */
    private Map<Progress.Origin, Barrier> complete(long cutId) {
        Map<Progress.Origin, Barrier> m = barriers.get(cutId);
        if (m == null || m.size() < required.size()) return null;
        Barrier first = m.values().iterator().next();
        for (Barrier b : m.values())
            if (b.restartEpoch() != first.restartEpoch() || !Objects.equals(b.recoveryRef(), first.recoveryRef()))
                throw new ProtocolError("mixed-epochs", writer + ": cut " + cutId + " has barriers of restartEpoch/recoveryRef " + first.restartEpoch() + "/" + first.recoveryRef()
                    + " and " + b.restartEpoch() + "/" + b.recoveryRef());
        return m;
    }

    /** The cut the writer closes now, if any: {@code nativeTime} is the adapter's event time (null for a writer without one, which is
     *  never held back by it). The close is recorded: the next call starts above it. */
    public Optional<Closed<T>> poll(Long nativeTime) {
        Long r = null;
        for (long c : barriers.keySet()) {
            if (complete(c) == null) continue;
            Barrier any = barriers.get(c).values().iterator().next();
            if (recovering) {
                if (any.restartEpoch() < epoch) continue;   // kept, no progress
                if (any.restartEpoch() > epoch) throw new ProtocolError("recovery-epoch", writer + ": cut " + c + " of restartEpoch " + any.restartEpoch() + " above E " + epoch);
                r = c;
                break;
            }
            if (held.ceilingKey(c + 1) != null) continue;   // the receive-order trigger: inputs of a later cut arrived first
            r = c;
            break;
        }
        if (r == null) return Optional.empty();
        if (nativeTime != null && lastW1 != null && nativeTime < lastW1) return Optional.empty();
        List<Long> merged = new ArrayList<>();
        for (long c = lastProgress + 1; c < r; c++) merged.add(c);
        List<Progress.BarrierRef> refs = new ArrayList<>();
        for (Map.Entry<Long, Map<Progress.Origin, Barrier>> e : barriers.headMap(r, true).entrySet())
            for (Progress.Origin o : required) { Barrier b = e.getValue().get(o); if (b != null) refs.add(b.ref()); }
        List<T> inputs = new ArrayList<>();
        for (List<T> l : held.headMap(r, true).values()) inputs.addAll(l);
        Barrier closing = barriers.get(r).values().iterator().next();
        Closed<T> closed = new Closed<>(r, List.copyOf(merged), List.copyOf(refs), closing.restartEpoch(), closing.recoveryRef(), lastW1, nativeTime,
            Collections.unmodifiableList(inputs), recovering);
        barriers.headMap(r, true).clear();
        held.headMap(r, true).clear();
        lastProgress = r;
        if (nativeTime != null) lastW1 = nativeTime;
        recovering = false;
        changed = true;
        return Optional.of(closed);
    }

    // --- the versioned codec ---------------------------------------------------------------------------------------------------

    /** The writer's protocol state, {@code assemblerVersion} {@value #VERSION}; held inputs through {@code encode}. */
    public ObjectNode toJson(Function<T, JsonNode> encode) {
        ObjectNode n = Json.object().put("assemblerVersion", VERSION).put("writer", writer).put("starvable", starvable);
        ArrayNode req = n.putArray("required");
        required.forEach(o -> req.addObject().put("topic", o.topic()).put("partition", o.partition()));
        Json.putSafe(n, "lastProgress", lastProgress);
        if (lastW1 == null) n.putNull("lastW1"); else Json.putSafe(n, "lastW1", lastW1);
        n.put("recovering", recovering);
        Json.putSafe(n, "epoch", epoch);
        ArrayNode lb = n.putArray("lastBarrier");
        for (Progress.Origin o : required) if (lastBarrier.containsKey(o)) Json.putSafe(lb.addObject().put("topic", o.topic()).put("partition", o.partition()), "cutId", lastBarrier.get(o));
        ArrayNode bs = n.putArray("barriers");
        barriers.values().forEach(m -> m.values().forEach(b -> bs.add(b.toJson())));
        ArrayNode hs = n.putArray("held");
        held.forEach((c, l) -> {
            ObjectNode h = Json.putSafe(hs.addObject(), "cutId", c);
            ArrayNode items = h.putArray("inputs");
            l.forEach(x -> items.add(encode.apply(x)));
        });
        return n;
    }

    /** The state {@link #toJson} wrote; another {@code assemblerVersion} or a malformed field is refused. */
    public static <T> CutAssembler<T> fromJson(JsonNode n, Function<JsonNode, T> decode) {
        if (!n.path("assemblerVersion").isIntegralNumber() || n.path("assemblerVersion").longValue() != VERSION)
            throw new IllegalArgumentException("cut assembler version " + n.path("assemblerVersion") + " is not " + VERSION);
        List<Progress.Origin> required = new ArrayList<>();
        for (JsonNode o : n.path("required")) required.add(origin(o));
        if (!n.path("starvable").isBoolean() || !n.path("recovering").isBoolean()) throw new IllegalArgumentException("cut assembler flags");
        CutAssembler<T> a = new CutAssembler<>(n.path("writer").isNull() ? null : Json.text(n, "writer"), required, n.get("starvable").booleanValue());
        a.lastProgress = Json.safeLong(n.get("lastProgress"), "lastProgress", Json.NON_NEGATIVE);
        a.lastW1 = n.path("lastW1").isNull() ? null : Json.safeLong(n.get("lastW1"), "lastW1", Json.ANY);
        a.recovering = n.get("recovering").booleanValue();
        a.epoch = Json.safeLong(n.get("epoch"), "epoch", Json.NON_NEGATIVE);
        for (JsonNode x : n.path("lastBarrier")) a.lastBarrier.put(a.requiredOrigin(origin(x)), Json.safeLong(x.get("cutId"), "lastBarrier.cutId", Json.NON_NEGATIVE));
        for (JsonNode x : n.path("barriers")) {
            Barrier b = Barrier.fromJson(x);
            a.requiredOrigin(b.origin());
            if (b.cutId() <= a.lastProgress) throw new IllegalArgumentException("cut assembler: barrier of committed cut " + b.cutId());
            if (a.barriers.computeIfAbsent(b.cutId(), k -> new LinkedHashMap<>()).put(b.origin(), b) != null) throw new IllegalArgumentException("cut assembler: two barriers of cut " + b.cutId() + " on " + b.origin());
        }
        for (JsonNode h : n.path("held")) {
            long c = Json.safeLong(h.get("cutId"), "held.cutId", 1, Json.MAX_SAFE);
            if (c <= a.lastProgress || a.held.containsKey(c)) throw new IllegalArgumentException("cut assembler: held inputs of cut " + c);
            List<T> l = new ArrayList<>();
            for (JsonNode x : h.path("inputs")) l.add(decode.apply(x));
            a.held.put(c, l);
        }
        return a;
    }

    // --- the segmented form -------------------------------------------------------------------------------------------------------

    /** The assembler is one segment of its writer's segmented state (plan step 6): its protocol state and held inputs. */
    public static final String SEGMENT = "assembler";

    /** Whether the assembler changed since the last call (an input held, a barrier recorded, a cut closed, a reinitialization); true
     *  at the first call. An engine writes {@link #segment} when it is. */
    public boolean drainChanged() { boolean c = changed; changed = false; return c; }

    /** The segment {@value #SEGMENT}: the JCS form of {@code {"segmentVersion": 1, "segment": "assembler", "assembler": toJson}}. */
    public byte[] segment(Function<T, JsonNode> encode) {
        ObjectNode n = yggdrasil.forseti.core.segment.Segments.header(SEGMENT);
        n.set("assembler", toJson(encode));
        return yggdrasil.forseti.core.segment.Segments.bytes(n);
    }

    /** The assembler a {@link #segment} holds; unchanged until its next change. */
    public static <T> CutAssembler<T> fromSegment(byte[] segment, Function<JsonNode, T> decode) {
        CutAssembler<T> a = fromJson(yggdrasil.forseti.core.segment.Segments.read(SEGMENT, segment).get("assembler"), decode);
        a.changed = false;
        return a;
    }

    private Progress.Origin requiredOrigin(Progress.Origin o) {
        if (!required.contains(o)) throw new IllegalArgumentException("cut assembler: " + o + " is not a required origin");
        return o;
    }

    private static Progress.Origin origin(JsonNode o) {
        return new Progress.Origin(Json.text(o, "topic"), (int) Json.safeLong(o.get("partition"), "partition", 0, Integer.MAX_VALUE));
    }
}
