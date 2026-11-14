/**
 * The transport shells of the step-5 live loop: topic admin ({@link yggdrasil.forseti.loop.KafkaTopicAdmin}), the occupancy and
 * incident producers ({@link yggdrasil.forseti.loop.InputProducer}), the scenario driver ({@link yggdrasil.forseti.loop.Driver}) over
 * the Milo client ({@link yggdrasil.forseti.loop.MiloSimPort}), and the {@code forseti-loop} command
 * ({@link yggdrasil.forseti.loop.LoopMain}). The Kafka-free rules they apply live in {@code yggdrasil.forseti.cli.live} so that the
 * gate jar can run them; this module depends on cli and never the reverse.
 */
package yggdrasil.forseti.loop;
