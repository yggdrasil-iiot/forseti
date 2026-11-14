/**
 * The transport shells of the step-5 live loop (topic admin, producers, the scenario driver, the Kafka reader). The Kafka-free logic
 * they drive lives in the cli module so that the gate jar can run it; this module depends on cli and never the reverse. Empty until
 * the later pull requests of the step-5 plan.
 */
package yggdrasil.forseti.loop;
