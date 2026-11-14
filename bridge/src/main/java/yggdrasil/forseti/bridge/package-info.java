/**
 * The MQTT-to-Kafka bridge of step 5: it turns each Sparkplug message of the broker into one record of an input topic and routes what
 * it cannot represent (alias-only input, data of a connection whose birth it never observed) to the dead-letter with its raw bytes.
 * It holds no business logic; the core reads what it writes. Empty until the bridge pull request of the step-5 plan.
 */
package yggdrasil.forseti.bridge;
