package yggdrasil.forseti.loop;

import java.util.ArrayList;
import java.util.List;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import yggdrasil.forseti.cli.live.DrivePlan;

/** {@link SimPort} over the Eclipse Milo 1.1.8 client, anonymous on the sim's {@code SecurityPolicy.None} endpoint.
 *
 *  <p>Why the None endpoint is chosen explicitly: {@code OpcUaClient.create(url)} picks the most secure endpoint on offer, and the sim
 *  also offers Basic256Sha256 when started with identity; the {@code Sim/} controls are writable anonymously by design (step-4 plan
 *  Decision 3). Why the source timestamp alone dates a scan: Milo answers every Value read with the time of the read as its
 *  ServerTimestamp ("Note for step 5"), so only the SourceTimestamp the sim set at its scan identifies the snapshot.
 *
 *  <p>Values are encoded with the exact built-in type of their control (Boolean, String, Double, UInt32 for status codes): the sim
 *  refuses any other type with Bad_TypeMismatch and never coerces. */
public final class MiloSimPort implements SimPort {
    private final OpcUaClient client;

    private MiloSimPort(OpcUaClient client) { this.client = client; }

    public static MiloSimPort connect(String endpoint, String applicationName) {
        try {
            OpcUaClient c = OpcUaClient.create(endpoint,
                endpoints -> endpoints.stream().filter(e -> SecurityPolicy.None.getUri().equals(e.getSecurityPolicyUri())).findFirst(),
                transport -> { },
                cfg -> cfg.setApplicationName(LocalizedText.english(applicationName)).setApplicationUri("urn:forseti:loop:" + applicationName));
            c.connect();
            return new MiloSimPort(c);
        } catch (UaException e) { throw new IllegalStateException("cannot connect to the sim at " + endpoint + ": " + e.getMessage(), e); }
    }

    private List<NodeId> nodeIds(String namespaceUri, List<String> identifiers) {
        UShort ns = client.getNamespaceTable().getIndex(namespaceUri);
        if (ns == null) return null;
        List<NodeId> l = new ArrayList<>();
        for (String id : identifiers) l.add(new NodeId(ns, id));
        return l;
    }

    @Override
    public List<Value> read(String namespaceUri, List<String> identifiers) {
        List<NodeId> ids = nodeIds(namespaceUri, identifiers);
        List<Value> out = new ArrayList<>();
        if (ids == null) { for (String id : identifiers) out.add(new Value(false, null, StatusCodes.Bad_NodeIdUnknown, null)); return out; }
        try {
            for (DataValue dv : client.readValues(0.0, TimestampsToReturn.Both, ids)) {
                long sc = dv.getStatusCode() == null ? 0 : dv.getStatusCode().getValue();
                boolean exists = sc != StatusCodes.Bad_NodeIdUnknown;
                Object v = dv.getValue() == null ? null : dv.getValue().getValue();
                if (v instanceof org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger u) v = u.longValue();
                out.add(new Value(exists, v, sc, dv.getSourceTime() == null ? null : dv.getSourceTime().getJavaTime()));
            }
        } catch (UaException e) { throw new IllegalStateException("sim read failed: " + e.getMessage(), e); }
        return out;
    }

    @Override
    public List<Long> write(String namespaceUri, List<DrivePlan.Write> items) {
        List<String> identifiers = items.stream().map(DrivePlan.Write::nodeId).toList();
        List<NodeId> ids = nodeIds(namespaceUri, identifiers);
        if (ids == null) throw new IllegalStateException("the sim has no namespace " + namespaceUri);
        List<DataValue> values = new ArrayList<>();
        for (DrivePlan.Write w : items) values.add(new DataValue(new Variant(encode(w))));
        try {
            List<Long> codes = new ArrayList<>();
            for (StatusCode sc : client.writeValues(ids, values)) codes.add(sc.getValue());
            return codes;
        } catch (UaException e) { throw new IllegalStateException("sim write failed: " + e.getMessage(), e); }
    }

    static Object encode(DrivePlan.Write w) {
        Object v = w.value();
        return switch (w.type()) {
            case "Boolean" -> (Boolean) v;
            case "String" -> (String) v;
            case "Double" -> (Double) v;
            case "UInt32" -> Unsigned.uint(((Number) v).longValue());
            default -> throw new IllegalArgumentException("no encoding for " + w.type());
        };
    }

    @Override
    public void close() {
        try { client.disconnect(); } catch (UaException e) { /* best effort on shutdown */ }
    }
}
