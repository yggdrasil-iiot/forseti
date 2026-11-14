package yggdrasil.forseti.bridge;

import java.util.List;
import java.util.Map;

/** A declared, default-off stage between the bridge core and the Kafka writer (plan D3). It sees only what the core emitted, so it
 *  never changes the session tracker's view; every stage that runs logs itself in the run record. */
public interface Stage {
    /** What to pass on for one emitted record seen at {@code now}; may be empty while the stage holds it. */
    List<Emitted> apply(Emitted e, long now);
    /** What to release because time passed with no new record. */
    List<Emitted> tick(long now);
    /** Everything still held, at the end of the run: a stage never drops a record. */
    List<Emitted> drain(long now);
    /** The stage's declaration and what it did, for the run record. */
    Map<String, Object> record();
}
