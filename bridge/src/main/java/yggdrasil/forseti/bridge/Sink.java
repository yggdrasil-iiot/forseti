package yggdrasil.forseti.bridge;

/** Where encoded records go: the Kafka writer in a run, a list in the tests. A failed write throws {@link BridgeFailure}. */
public interface Sink extends AutoCloseable {
    void send(Records.Out record);
    /** Blocks until every record sent is acknowledged; throws when any of them failed. */
    void flush();
    @Override void close();
}
