package yggdrasil.forseti.cli.generation;

import java.util.Map;

/** A transport store of {@code forseti generation --store <name>}, found by {@link java.util.ServiceLoader} on the engine path
 *  ({@code yggdrasil.forseti.cli.replay.EngineProviders}), so the cli jar carries no transport client. {@code options} holds the
 *  verb's store flags ({@code bootstrap}, {@code topic}) and {@code mode}: {@code writer} for the verbs that append, {@code reader} for
 *  {@code list}. */
public interface GenerationStoreProvider {
    String name();

    OperatorStore open(Map<String, String> options);
}
