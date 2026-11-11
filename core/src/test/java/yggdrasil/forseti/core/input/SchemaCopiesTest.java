package yggdrasil.forseti.core.input;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.canon.Canonical;

/** The built-in schemas are byte copies of the published ones at the repository root (whose digests Tyr's and Bifrost's fixtures carry). */
class SchemaCopiesTest {
    static final Path ROOT = Path.of("..", "schemas");

    static byte[] resource(String name) throws Exception {
        try (InputStream in = SchemaCopiesTest.class.getResourceAsStream("/forseti/schemas/" + name)) {
            assertNotNull(in, name);
            return in.readAllBytes();
        }
    }

    @Test void theResourceCopiesEqualTheRootSchemasByteForByteAndKeepTheirPinnedDigests() throws Exception {
        assertTrue(Files.isDirectory(ROOT), "the repository build always has ../schemas");
        for (String[] f : new String[][] {
                {"occupancy-1.0.0.schema.json", "e97cd7c97b1ab5e2d63d5b06e6057415945b82cff6d2ca0345106d2daf9d29dc"},
                {"incident-1.0.0.schema.json", "7141c3115d90812f16b22049c66bfaa64942ebacf854b343fb99b3dce168fb34"}}) {
            byte[] copy = resource(f[0]);
            assertArrayEquals(Files.readAllBytes(ROOT.resolve(f[0])), copy, f[0]);
            assertEquals(f[1], Canonical.digestOf(copy), f[0]);
        }
    }
}
