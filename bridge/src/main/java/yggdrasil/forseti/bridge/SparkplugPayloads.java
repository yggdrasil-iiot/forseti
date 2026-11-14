package yggdrasil.forseti.bridge;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.PropertySet;
import org.eclipse.tahu.message.model.PropertyValue;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.protobuf.SparkplugBProto;

/** Decodes a Sparkplug B payload with Eclipse Tahu and reads exactly what Muninn writes (its {@code SparkplugCodec}, mirrored by wire
 *  format, never by code): the payload timestamp is the publish time; a sample metric's own timestamp is the OPC UA source timestamp and
 *  is absent when the server returned none; the properties {@code Quality} (Int32), {@code ua_statuscode} (UInt32) and
 *  {@code ua_server_timestamp} (UInt64, absent when none) carry the rest; NBIRTH carries the governed definition's exact bytes in the
 *  Bytes metric {@code governed_definition} and {@code bdSeq}; NDEATH carries {@code bdSeq}; NCMD {@code Node Control/Rebirth = true}
 *  asks for a rebirth.
 *
 *  <p>Presence is read from the protobuf itself, not from Tahu's model: whether a metric has a name (alias-only input is refused, Decision
 *  13) and whether it has a timestamp (a missing source timestamp is never filled). Tahu's model converts the values and properties. */
final class SparkplugPayloads {
    static final String GOVERNED_DEFINITION = "governed_definition";
    static final String BD_SEQ = "bdSeq";
    static final String REBIRTH = "Node Control/Rebirth";
    static final String QUALITY = "Quality";
    static final String UA_STATUSCODE = "ua_statuscode";
    static final String UA_SERVER_TIMESTAMP = "ua_server_timestamp";

    private SparkplugPayloads() {}

    /** A payload that does not decode, or lacks a field its message type needs. */
    static final class Malformed extends Exception {
        Malformed(String m) { super(m); }
        Malformed(String m, Throwable c) { super(m, c); }
    }

    /** One metric: its name (null for an alias-only metric), Tahu type and value, and its own timestamp only when the wire carries one. */
    record DecodedMetric(String name, boolean aliasOnly, MetricDataType type, Object value, boolean isNull, Long timestamp, PropertySet properties) {}

    /** A decoded payload: timestamp and seq as on the wire (null when absent), and its metrics in wire order. */
    record Decoded(Long timestamp, Long seq, List<DecodedMetric> metrics) {
        boolean aliasOnly() { for (DecodedMetric m : metrics) if (m.aliasOnly()) return true; return false; }
        DecodedMetric metric(String name) { for (DecodedMetric m : metrics) if (name.equals(m.name())) return m; return null; }
    }

    static Decoded decode(byte[] raw) throws Malformed {
        SparkplugBProto.Payload proto;
        try { proto = SparkplugBProto.Payload.parseFrom(raw); }
        catch (Exception e) { throw new Malformed("not a Sparkplug B payload: " + e.getMessage(), e); }
        Long ts = proto.hasTimestamp() ? proto.getTimestamp() : null;
        Long seq = proto.hasSeq() ? proto.getSeq() : null;
        boolean aliasOnly = false;
        for (SparkplugBProto.Payload.Metric m : proto.getMetricsList()) {
            if (!m.hasName()) {
                if (!m.hasAlias()) throw new Malformed("a metric with neither name nor alias");
                aliasOnly = true;
            }
        }
        List<DecodedMetric> out = new ArrayList<>();
        if (aliasOnly) {
            // Without a name Tahu cannot resolve the type of a data metric; the frame is refused whole, so only presence is kept.
            for (SparkplugBProto.Payload.Metric m : proto.getMetricsList())
                out.add(new DecodedMetric(m.hasName() ? m.getName() : null, !m.hasName(), null, null, false, m.hasTimestamp() ? m.getTimestamp() : null, null));
            return new Decoded(ts, seq, List.copyOf(out));
        }
        SparkplugBPayload payload;
        try { payload = new SparkplugBPayloadDecoder().buildFromByteArray(raw, null); }
        catch (Exception e) { throw new Malformed("Tahu could not decode the payload: " + e.getMessage(), e); }
        List<Metric> metrics = payload.getMetrics();
        if (metrics.size() != proto.getMetricsCount()) throw new Malformed("metric count differs between the wire and Tahu's model");
        for (int i = 0; i < metrics.size(); i++) {
            Metric m = metrics.get(i);
            SparkplugBProto.Payload.Metric w = proto.getMetrics(i);
            out.add(new DecodedMetric(w.getName(), false, m.getDataType(), m.getValue(), Boolean.TRUE.equals(m.isNull()),
                w.hasTimestamp() ? w.getTimestamp() : null, m.getProperties()));
        }
        return new Decoded(ts, seq, List.copyOf(out));
    }

    /** An integral metric or property value as a non-negative long; null when it is absent, not integral or out of range. */
    static Long integral(Object v) {
        long l;
        if (v instanceof Long x) l = x;
        else if (v instanceof Integer x) l = x;
        else if (v instanceof Short x) l = x;
        else if (v instanceof Byte x) l = x;
        else if (v instanceof BigInteger x) { if (x.bitLength() > 62) return null; l = x.longValue(); }
        else if (v instanceof Date d) l = d.getTime();
        else return null;
        return l < 0 || l > Records.MAX_SAFE ? null : l;
    }

    static Long property(PropertySet p, String name) {
        if (p == null) return null;
        PropertyValue<?> v = p.getPropertyValue(name);
        return v == null || Boolean.TRUE.equals(v.isNull()) ? null : integral(v.getValue());
    }
}
