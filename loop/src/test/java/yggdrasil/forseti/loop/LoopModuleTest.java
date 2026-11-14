package yggdrasil.forseti.loop;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** The loop module builds and its tests run, so the CI test split (scripts/check-test-split.py) sees its source root. */
class LoopModuleTest {
    /** loop depends on cli (the Kafka-free live logic and the gate jar), which is why cli may never depend on loop. */
    @Test
    void theLoopSeesTheCli() throws Exception {
        assertEquals("yggdrasil.forseti.cli.Main", Class.forName("yggdrasil.forseti.cli.Main").getName());
    }
}
