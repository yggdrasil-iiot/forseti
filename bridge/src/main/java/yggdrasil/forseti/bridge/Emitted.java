package yggdrasil.forseti.bridge;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** What the bridge core emits for one frame, before the D3 stages and the Kafka writer: a process message, a session event, or a
 *  dead-letter. They stay structured until {@link Records} encodes them, so a stage changes a value and never edits JSON text. */
public sealed interface Emitted {

    /** One measurement of a DDATA message (spec Process messages). {@code value} is a Boolean, an integral Number, a finite Double or a
     *  String, or null when the metric is flagged null on the wire. {@code sourceTs} and {@code serverTs} are null when the wire carries
     *  none: Muninn omits {@code sourceTs} when the server provides none and the bridge never fills it. */
    record Member(String metric, Object value, Long uaStatusCode, Long quality, Long sourceTs, Long serverTs) {
        Member withSourceOffset(long ms) { return sourceTs == null ? this : new Member(metric, value, uaStatusCode, quality, sourceTs + ms, serverTs); }
    }

    /** One Kafka record per Sparkplug DDATA message (Decision 13), keyed by device. */
    record ProcessMessage(String edge, String device, long birthPublishTs, long bdSeq, long seq, long publishTs, long bridgeReceivedAt,
                          String definitionSha256, List<Member> members) implements Emitted {
        /** The largest member sourceTs: the message's telemetry time candidate (spec line 263); null when no member carries one. */
        public Long eventTime() {
            Long t = null;
            for (Member m : members) if (m.sourceTs() != null && (t == null || m.sourceTs() > t)) t = m.sourceTs();
            return t;
        }
        ProcessMessage withSourceOffset(long ms) {
            List<Member> l = new ArrayList<>();
            for (Member m : members) l.add(m.withSourceOffset(ms));
            return new ProcessMessage(edge, device, birthPublishTs, bdSeq, seq, publishTs, bridgeReceivedAt, definitionSha256, List.copyOf(l));
        }
    }

    /** One per-device session event (Decision 10, spec Session events). {@code birthPublishTs} and {@code atSeq} are null only on the
     *  birthless {@code session-ambiguous} of plan D4. {@code payload} keeps its field order; null for {@code definition-changed}. */
    record SessionEvent(String edge, String device, Long birthPublishTs, long bdSeq, String kind, Long atSeq, Map<String, Object> payload) implements Emitted {}

    /** A frame the bridge cannot represent as an input record (malformed, alias-only, birthless data, a death that names no device):
     *  the raw MQTT bytes exactly as received, the MQTT topic and the receipt metadata. */
    record DeadLetter(String mqttTopic, byte[] raw, long receivedAt, String reason, String edge, String device) implements Emitted {}
}
