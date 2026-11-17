package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** A collector of the evidence a replay run records beside its journals (spec "Each test run records", plan D10): the effective client
 *  configurations of the harness's consumers and producers (the gate asserts {@code isolation.level=read_committed} from them), the
 *  observed topic settings, and the harness's own records of a run. Entries are named; a name is collected once. */
public interface EvidenceCollector {
    void collect(String name, JsonNode evidence);

    /** Everything collected so far, by name. */
    ObjectNode evidence();
}
