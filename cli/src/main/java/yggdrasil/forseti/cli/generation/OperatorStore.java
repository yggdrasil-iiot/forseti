package yggdrasil.forseti.cli.generation;

import java.util.List;
import java.util.Optional;
import yggdrasil.forseti.core.generation.GenerationStore;
import yggdrasil.forseti.core.release.LockedFile;

/** The control store {@code forseti generation} writes (spec 485, 575-577: only the operator CLI writes {@code forseti.generations}),
 *  whatever its transport: the step-3 file ({@link FileGenerationStore}, {@code --store file}, the default) or a store a
 *  {@link GenerationStoreProvider} on the engine path opens ({@code --store <name>}, step 6: {@code kafka}). It adds to the read side of
 *  {@link GenerationStore} what the verbs need: one hold for a whole verb, a promotion batch appended as one write, the registration
 *  digest, and the stored transport records for {@code list}. */
public interface OperatorStore extends GenerationStore, AutoCloseable {
    /** Runs {@code body} as one operation against concurrent writers (the file store's lock; a single-writer transport store fences
     *  every other writer instead, so the body runs as is). */
    <T> T locked(LockedFile.Body<T> body);

    /** Appends {@code rows} as one write after validating the history with every one of them; nothing is written on a refusal. */
    void appendBatch(List<Entry> rows);

    /** The digest of the transport record of the {@code registered} row of {@code generation}, if registered. */
    Optional<String> registeredRowSha256(long generation);

    /** Every stored transport record as its RFC 8785 bytes and an LF, in append order. */
    byte[] bytes();

    @Override default void close() {}
}
