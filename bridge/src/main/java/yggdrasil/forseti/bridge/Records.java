package yggdrasil.forseti.bridge;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** Encodes emitted records as the Kafka records of the input topics: compact JSON with the fields in contract order (spec Process
 *  messages, Session events), keyed by device (spec Topics and partitioning). Optional member fields are omitted when absent, never
 *  written as a filler. The dead-letter value is the raw MQTT payload byte for byte; its topic, receipt time and reason ride in headers,
 *  so the evidence is never re-encoded. */
public final class Records {
    /** 2^53 - 1: every integer the readers of the input topics accept (core Json.MAX_SAFE, oracle SAFE_MAX). */
    public static final long MAX_SAFE = 9007199254740991L;
    public static final String H_MQTT_TOPIC = "forseti.mqtt-topic";
    public static final String H_RECEIVED_AT = "forseti.received-at";
    public static final String H_REASON = "forseti.reason";
    public static final String H_EDGE = "forseti.edge";
    public static final String H_DEVICE = "forseti.device";

    private static final ObjectMapper JSON = new ObjectMapper();

    /** One Kafka record: topic, key and value bytes, and headers (empty except on the dead-letter). */
    public record Out(String topic, String key, byte[] value, Map<String, String> headers) {}

    /** The three topics the bridge writes. */
    public record Topics(String messages, String sessions, String deadLetter) {
        /** Spec Topics and partitioning names the first two; it names no dead-letter topic, so the bridge names it ot.dead-letter. */
        public static final Topics DEFAULT = new Topics("ot.messages", "ot.sessions", "ot.dead-letter");
    }

    private Records() {}

    public static Out encode(Emitted e, Topics topics) {
        if (e instanceof Emitted.ProcessMessage m) return new Out(topics.messages(), m.device(), message(m), Map.of());
        if (e instanceof Emitted.SessionEvent s) return new Out(topics.sessions(), s.device(), session(s), Map.of());
        Emitted.DeadLetter d = (Emitted.DeadLetter) e;
        Map<String, String> h = new LinkedHashMap<>();
        h.put(H_MQTT_TOPIC, d.mqttTopic());
        h.put(H_RECEIVED_AT, Long.toString(d.receivedAt()));
        h.put(H_REASON, d.reason());
        if (d.edge() != null) h.put(H_EDGE, d.edge());
        if (d.device() != null) h.put(H_DEVICE, d.device());
        return new Out(topics.deadLetter(), d.mqttTopic(), d.raw().clone(), Map.copyOf(h));
    }

    @FunctionalInterface private interface Body { void write(JsonGenerator g) throws IOException; }

    private static byte[] write(Body body) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (JsonGenerator g = JSON.getFactory().createGenerator(out)) { body.write(g); }
        catch (IOException e) { throw new UncheckedIOException(e); }
        return out.toByteArray();
    }

    static byte[] message(Emitted.ProcessMessage m) {
        return write(g -> {
            g.writeStartObject();
            g.writeStringField("edge", m.edge());
            g.writeStringField("device", m.device());
            safe(g, "birthPublishTs", m.birthPublishTs());
            safe(g, "bdSeq", m.bdSeq());
            safe(g, "seq", m.seq());
            safe(g, "publishTs", m.publishTs());
            safe(g, "bridgeReceivedAt", m.bridgeReceivedAt());
            g.writeStringField("definitionSha256", m.definitionSha256());
            g.writeArrayFieldStart("members");
            for (Emitted.Member x : m.members()) {
                g.writeStartObject();
                g.writeStringField("metric", x.metric());
                g.writeFieldName("value");
                value(g, x.value());
                if (x.uaStatusCode() != null) safe(g, "uaStatusCode", x.uaStatusCode());
                if (x.quality() != null) safe(g, "quality", x.quality());
                if (x.sourceTs() != null) safe(g, "sourceTs", x.sourceTs());
                if (x.serverTs() != null) safe(g, "serverTs", x.serverTs());
                g.writeEndObject();
            }
            g.writeEndArray();
            g.writeEndObject();
        });
    }

    static byte[] session(Emitted.SessionEvent s) {
        return write(g -> {
            g.writeStartObject();
            g.writeStringField("edge", s.edge());
            g.writeStringField("device", s.device());
            nullable(g, "birthPublishTs", s.birthPublishTs());
            safe(g, "bdSeq", s.bdSeq());
            g.writeStringField("kind", s.kind());
            nullable(g, "atSeq", s.atSeq());
            g.writeFieldName("payload");
            if (s.payload() == null) g.writeNull();
            else {
                g.writeStartObject();
                for (Map.Entry<String, Object> f : s.payload().entrySet()) { g.writeFieldName(f.getKey()); value(g, f.getValue()); }
                g.writeEndObject();
            }
            g.writeEndObject();
        });
    }

    private static void safe(JsonGenerator g, String field, long v) throws IOException {
        if (v < 0 || v > MAX_SAFE) throw new IllegalStateException("integer outside 0..2^53-1: " + field + "=" + v);
        g.writeNumberField(field, v);
    }
    private static void nullable(JsonGenerator g, String field, Long v) throws IOException {
        if (v == null) g.writeNullField(field); else safe(g, field, v);
    }
    private static void value(JsonGenerator g, Object v) throws IOException {
        if (v == null) g.writeNull();
        else if (v instanceof Boolean b) g.writeBoolean(b);
        else if (v instanceof String s) g.writeString(s);
        else if (v instanceof Long l) g.writeNumber(l);
        else if (v instanceof Integer i) g.writeNumber(i);
        else if (v instanceof BigInteger b) g.writeNumber(b);
        else if (v instanceof Double d) {
            // JSON has no infinity token; a literal beyond the binary64 range is read by both readers as the signed infinity (spec
            // reason non-finite). NaN has no such literal and never reaches here (the core writes it as null).
            if (Double.isNaN(d)) throw new IllegalStateException("NaN is not a record value");
            if (d == Double.POSITIVE_INFINITY) g.writeNumber("1e400");
            else if (d == Double.NEGATIVE_INFINITY) g.writeNumber("-1e400");
            else g.writeNumber(d);
        }
        else throw new IllegalStateException("not a record value: " + v.getClass().getName());
    }

    static String utf8(byte[] b) { return new String(b, StandardCharsets.UTF_8); }
}
