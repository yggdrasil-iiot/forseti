package yggdrasil.forseti.bridge;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** The bridge module builds and its tests run, so the CI test split (scripts/check-test-split.py) sees its source root. */
class BridgeModuleTest {
    /** Nothing to translate yet: the module only has to compile, run under surefire and report into its own package. */
    @Test
    void theModuleRunsItsTests() {
        assertEquals("yggdrasil.forseti.bridge", BridgeModuleTest.class.getPackageName());
    }
}
