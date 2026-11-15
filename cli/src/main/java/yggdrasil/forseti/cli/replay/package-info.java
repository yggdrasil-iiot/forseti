/**
 * The engine-neutral, transport-free interfaces of the step-6 replay harness (plan D1): stores, topic management, the batch and barrier
 * writer, the completion reader, the journal dump, the collectors and process supervision. They hold no Kafka or engine type, so the
 * core gate jar stays free of both; {@code forseti-replay-kafka} implements them.
 */
package yggdrasil.forseti.cli.replay;
