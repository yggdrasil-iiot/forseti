package yggdrasil.forseti.bridge;

/** One MQTT message as the bridge received it: the topic, the payload bytes exactly as delivered, and the bridge's own receipt time
 *  (epoch ms). The receipt time is diagnostic ({@code bridgeReceivedAt}) and the clock of the D3 hold stage; it never becomes an event
 *  time. */
public record Frame(String topic, byte[] payload, long receivedAt) {}
