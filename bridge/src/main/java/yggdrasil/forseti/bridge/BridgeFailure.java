package yggdrasil.forseti.bridge;

/** A condition that ends the run: a Kafka send that failed, a full input queue, a lost MQTT connection. The bridge never drops a record
 *  silently, so every such condition is thrown to the run, which exits non-zero (plan PR 4). */
public final class BridgeFailure extends RuntimeException {
    public BridgeFailure(String message) { super(message); }
    public BridgeFailure(String message, Throwable cause) { super(message, cause); }
}
