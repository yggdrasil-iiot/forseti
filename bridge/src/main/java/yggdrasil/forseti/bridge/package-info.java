/**
 * The MQTT-to-Kafka bridge of step 5 (Decision 13, plan PR 4): it turns each Sparkplug DDATA of the broker into one record of
 * {@code ot.messages}, keeps the session state of Decision 10 per edge and writes it to {@code ot.sessions} per device, and routes what it
 * cannot represent (malformed or alias-only input, data of a connection whose birth it never observed) to the dead-letter with its raw
 * bytes. It holds no business logic; the core reads what it writes.
 *
 * <p>{@link yggdrasil.forseti.bridge.BridgeCore} is transport-free and carries every decision; {@link yggdrasil.forseti.bridge.MqttSource}
 * and {@link yggdrasil.forseti.bridge.KafkaSink} are thin shells around it, and the two D3 injection stages sit between the core and
 * the writer. The wire format read is Muninn's, mirrored from its published behaviour, never from its code.
 */
package yggdrasil.forseti.bridge;
