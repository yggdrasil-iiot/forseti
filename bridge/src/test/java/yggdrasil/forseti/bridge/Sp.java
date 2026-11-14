package yggdrasil.forseti.bridge;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.eclipse.tahu.message.SparkplugBPayloadEncoder;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.PropertyDataType;
import org.eclipse.tahu.message.model.PropertySet.PropertySetBuilder;
import org.eclipse.tahu.message.model.PropertyValue;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugBPayload.SparkplugBPayloadBuilder;

/** Sparkplug B frames built with Tahu in the shape Muninn publishes them (its SparkplugCodec and EdgeSession, read as a wire format):
 *  NBIRTH seq 0 with governed_definition, bdSeq and Node Control/Rebirth = false; DBIRTH and DDATA with one metric per sample whose
 *  own timestamp is the source timestamp (absent when none) and whose properties are Quality, ua_statuscode and ua_server_timestamp;
 *  NDEATH with bdSeq only and no seq; NCMD Node Control/Rebirth = true. */
final class Sp {
    static final String GROUP = "line2";
    static final byte[] DEF = "{\"idShort\":\"StationType\",\"version\":\"1.0.0\"}\n".getBytes(StandardCharsets.UTF_8);
    static final String DEF_SHA = BridgeCore.sha256(DEF);

    private Sp() {}

    static byte[] encode(SparkplugBPayload p) {
        try { return new SparkplugBPayloadEncoder().getBytes(p, false); } catch (Exception e) { throw new IllegalStateException(e); }
    }
    /** A payload with a timestamp and no metrics. */
    static byte[] empty() {
        try { return encode(new SparkplugBPayloadBuilder().setTimestamp(new Date(1)).createPayload()); } catch (Exception e) { throw new IllegalStateException(e); }
    }
    static String topic(String type, String edge) { return "spBv1.0/" + GROUP + "/" + type + "/" + edge; }
    static String topic(String type, String edge, String device) { return topic(type, edge) + "/" + device; }

    static Frame nbirth(String edge, long bdSeq, long ts) { return nbirth(edge, bdSeq, ts, DEF); }
    static Frame nbirth(String edge, long bdSeq, long ts, byte[] def) {
        try {
            SparkplugBPayload p = new SparkplugBPayloadBuilder().setTimestamp(new Date(ts)).setSeq(0L)
                .addMetric(new MetricBuilder(SparkplugPayloads.GOVERNED_DEFINITION, MetricDataType.Bytes, def).createMetric())
                .addMetric(new MetricBuilder(SparkplugPayloads.BD_SEQ, MetricDataType.Int64, bdSeq).createMetric())
                .addMetric(new MetricBuilder(SparkplugPayloads.REBIRTH, MetricDataType.Boolean, false).createMetric())
                .createPayload();
            return new Frame(topic("NBIRTH", edge), encode(p), ts + 5);
        } catch (Exception e) { throw new IllegalStateException(e); }
    }

    static Frame ndeath(String edge, long bdSeq, long ts) {
        try {
            SparkplugBPayload p = new SparkplugBPayloadBuilder().setTimestamp(new Date(ts))
                .addMetric(new MetricBuilder(SparkplugPayloads.BD_SEQ, MetricDataType.Int64, bdSeq).createMetric()).createPayload();
            return new Frame(topic("NDEATH", edge), encode(p), ts + 5);
        } catch (Exception e) { throw new IllegalStateException(e); }
    }

    static Frame ncmdRebirth(String edge) {
        try {
            SparkplugBPayload p = new SparkplugBPayloadBuilder().setTimestamp(new Date(1))
                .addMetric(new MetricBuilder(SparkplugPayloads.REBIRTH, MetricDataType.Boolean, true).createMetric()).createPayload();
            return new Frame(topic("NCMD", edge), encode(p), 1);
        } catch (Exception e) { throw new IllegalStateException(e); }
    }

    /** A sample metric as Muninn writes it; {@code sourceTs} and {@code serverTs} may be null. */
    static Metric sample(String name, MetricDataType type, Object value, Long sourceTs, long statusCode, int quality, Long serverTs) {
        try {
            PropertySetBuilder ps = new PropertySetBuilder()
                .addProperty(SparkplugPayloads.QUALITY, new PropertyValue<>(PropertyDataType.Int32, quality))
                .addProperty(SparkplugPayloads.UA_STATUSCODE, new PropertyValue<>(PropertyDataType.UInt32, statusCode));
            if (serverTs != null) ps.addProperty(SparkplugPayloads.UA_SERVER_TIMESTAMP, new PropertyValue<>(PropertyDataType.UInt64, BigInteger.valueOf(serverTs)));
            return new MetricBuilder(name, type, value).timestamp(sourceTs == null ? null : new Date(sourceTs)).properties(ps.createPropertySet()).createMetric();
        } catch (Exception e) { throw new IllegalStateException(e); }
    }

    /** The two members of a station sample at one scan: VisitPresent and Torque, both stamped {@code sourceTs}. */
    static List<Metric> scan(Long sourceTs) {
        List<Metric> l = new ArrayList<>();
        l.add(sample("VisitPresent", MetricDataType.Boolean, true, sourceTs, 0L, 192, sourceTs == null ? null : sourceTs + 3));
        l.add(sample("Torque", MetricDataType.Double, 12.5, sourceTs, 0L, 192, sourceTs == null ? null : sourceTs + 3));
        return l;
    }

    static Frame device(String type, String edge, String device, long seq, long publishTs, List<Metric> metrics) {
        try {
            SparkplugBPayloadBuilder b = new SparkplugBPayloadBuilder().setTimestamp(new Date(publishTs)).setSeq(seq);
            for (Metric m : metrics) b.addMetric(m);
            return new Frame(topic(type, edge, device), encode(b.createPayload()), publishTs + 7);
        } catch (Exception e) { throw new IllegalStateException(e); }
    }
    /** A DBIRTH at {@code seq}, sampled at {@code ts}. */
    static Frame dbirth(String edge, String device, long seq, long ts) { return device("DBIRTH", edge, device, seq, ts + 50, scan(ts)); }
    /** A DDATA at {@code seq}, sampled at {@code ts} and published 50 ms later. */
    static Frame ddata(String edge, String device, long seq, long ts) { return device("DDATA", edge, device, seq, ts + 50, scan(ts)); }
}
