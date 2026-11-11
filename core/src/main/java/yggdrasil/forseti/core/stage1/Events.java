package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import yggdrasil.forseti.core.input.Ids;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;

/** The events {@link Stage1#decide} takes: an admitted input, a fired timer, the close of a cut. The adapter owns the order: the inputs
 *  of a cut in contract order ({@link #CONTRACT_ORDER}), then the timers the {@link DeadlineIndex} reports due at the cut's W1, then the
 *  close. */
public final class Events {
    private Events() {}

    public sealed interface Event permits Admit, Timer, CutClose {}
    /** One routed input record; {@code kindRank} is session 0, occupancy 1, sample 2 (spec 491). */
    public record Admit(Inputs.InputRecord record, int kindRank) implements Event {}
    /** A due deadline of the index: its instant, the logical key (a visitRef) and the purpose. */
    public record Timer(long deadline, JsonNode key, String purpose) implements Event {}
    /** The cut is complete at this station: {@code w1} is the station's W1, {@code barrierRefs} the collected barriers as
     *  {@code {cutId, origin {topic, partition}, throughOffset}}. */
    public record CutClose(long cutId, long w1, List<JsonNode> barrierRefs) implements Event {}

    public static final int SESSION = 0, OCCUPANCY = 1, SAMPLE = 2;

    public static int kindRank(String topic) {
        switch (topic) {
            case InputParser.SESSIONS: return SESSION;
            case InputParser.OCCUPANCY: return OCCUPANCY;
            case InputParser.MESSAGES: return SAMPLE;
            default: throw new IllegalArgumentException("not a stage-1 input topic: " + topic);
        }
    }

    /** The input kind named in dispositions and causes: {@code session}, {@code occupancy}, {@code message}. */
    public static String inputKind(String topic) {
        switch (topic) {
            case InputParser.SESSIONS: return "session";
            case InputParser.OCCUPANCY: return "occupancy";
            case InputParser.MESSAGES: return "message";
            default: throw new IllegalArgumentException("not a stage-1 input topic: " + topic);
        }
    }

    /** The event time an input is ordered by (spec 261, 278, 491): occupancy {@code eventTime}; a message its largest member
     *  {@code sourceTs}; a session event per kind ({@code seq-gap} to, {@code session-up} at, {@code session-down} from,
     *  {@code definition-changed} the birth time, {@code stale-death} and {@code session-ambiguous} detectedEventTime). Null when the input
     *  has none (a message without any member sourceTs, a death without detectedEventTime); such inputs sort first. */
    public static Long eventTime(Inputs.InputRecord r) {
        Object p = r.parsed();
        if (p instanceof Inputs.OccupancyEvent o) return o.eventTime();
        if (p instanceof Inputs.ProcessMessage m) {
            Long max = null;
            for (Inputs.Member x : m.members()) if (x.sourceTs() != null && (max == null || x.sourceTs() > max)) max = x.sourceTs();
            return max;
        }
        if (p instanceof Inputs.SessionEvent s) {
            switch (s.kind()) {
                case "seq-gap": return longOrNull(s.payload().get("to"));
                case "session-up": return longOrNull(s.payload().get("at"));
                case "session-down": return longOrNull(s.payload().get("from"));
                case "definition-changed": return s.birthPublishTs();
                default: return longOrNull(s.payload().get("detectedEventTime"));
            }
        }
        throw new IllegalArgumentException("not a parsed stage-1 input");
    }

    /** The semantic identity an input is ordered by: occupancyId, the message identity (sampleId without metric), sessionEventId. */
    public static JsonNode identity(Inputs.InputRecord r) {
        Object p = r.parsed();
        if (p instanceof Inputs.OccupancyEvent o) return Ids.occupancyId(o);
        if (p instanceof Inputs.ProcessMessage m) return Ids.messageId(m);
        if (p instanceof Inputs.SessionEvent s) return Ids.sessionEventId(s);
        throw new IllegalArgumentException("not a parsed stage-1 input");
    }

    /** Contract order inside a cut (spec 491): (eventTime, kindRank, identity bytes, payloadDigest). */
    public static final Comparator<Inputs.InputRecord> CONTRACT_ORDER = (a, b) -> {
        Long ta = eventTime(a), tb = eventTime(b);
        int c = ta == null ? (tb == null ? 0 : -1) : tb == null ? 1 : Long.compare(ta, tb);
        if (c != 0) return c;
        c = Integer.compare(kindRank(a.origin().topic()), kindRank(b.origin().topic()));
        if (c != 0) return c;
        c = Arrays.compareUnsigned(Ids.bytes(identity(a)), Ids.bytes(identity(b)));
        if (c != 0) return c;
        return a.payloadDigest().sha256().compareTo(b.payloadDigest().sha256());
    };

    private static Long longOrNull(JsonNode n) { return n == null || !n.isIntegralNumber() ? null : n.longValue(); }
}
