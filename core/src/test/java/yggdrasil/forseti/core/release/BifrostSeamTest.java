package yggdrasil.forseti.core.release;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The seam against the live Bifrost CLI: scripts/make_registry_fixture.py publishes fresh registries with the jar named by
 *  FORSETI_BIFROST_JAR from the Tyr bundles at FORSETI_TYR_FIXTURES, and forseti's reader verifies them. Bifrost is only ever a
 *  subprocess of the script; nothing of it is on this classpath. Skipped without the two variables; the CI job bifrost-seam
 *  always sets them, so a missing jar there is a failure. */
class BifrostSeamTest {
    @TempDir Path tmp;

    @Test void freshlyPublishedRegistriesVerifyAndATamperedPayloadIsAMismatch() throws Exception {
        String jarEnv = System.getenv("FORSETI_BIFROST_JAR"), tyrEnv = System.getenv("FORSETI_TYR_FIXTURES");
        Assumptions.assumeTrue(jarEnv != null && !jarEnv.isBlank() && tyrEnv != null && !tyrEnv.isBlank(), "FORSETI_BIFROST_JAR and FORSETI_TYR_FIXTURES not set");
        // Relative values are read from the repository root, where the plan's command line runs (Maven runs this test in core/).
        Path repo = Path.of("..").toAbsolutePath().normalize(), out = tmp.resolve("fixtures");
        Path jar = repo.resolve(jarEnv).normalize(), tyr = repo.resolve(tyrEnv).normalize();
        assertTrue(Files.isRegularFile(jar), "FORSETI_BIFROST_JAR is set but names no file: " + jar);
        assertTrue(Files.isDirectory(tyr), "FORSETI_TYR_FIXTURES is set but names no directory: " + tyr);
        Path bifrost = jar.getParent().getParent().getParent();   // <bifrost>/gates/target/bifrost-gates.jar, the layout the script expects
        String python = System.getenv().getOrDefault("FORSETI_PYTHON", "python");
        Process p = new ProcessBuilder(python, "-I", repo.resolve("scripts").resolve("make_registry_fixture.py").toString(),
            "--bifrost", bifrost.toString(), "--tyr", tyr.toString(), "--out", out.toString())
            .directory(repo.toFile()).redirectErrorStream(true).start();
        String log = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(10, TimeUnit.MINUTES), "make_registry_fixture.py did not finish");
        assertEquals(0, p.exitValue(), log);

        TrustedKeys keys = TrustedKeys.load(out.resolve("trusted-keys.jsonl"));
        Path registry = out.resolve("registry").resolve("mappings"), next = out.resolve("registry-next").resolve("mappings");
        int state = 0;
        for (boolean wallClock : new boolean[] {false, true}) {
            for (String[] s : new String[][] {{"line2", "line2-rules", "44"}, {"line2-plc", "line2-rules-plc", "64"}}) {
                long now = wallClock ? System.currentTimeMillis() : TestKeys.signedAtMs(registry, s[0]) + 60_000;
                HeadVerifier.Head h = HeadVerifier.verify(registry, s[0], keys, new FloorStore(tmp.resolve("state-" + state++)), now);
                assertEquals(s[1], h.releaseId());
                assertEquals(1, h.headVersion());
                ReleaseReader.Release r = ReleaseReader.read(registry, h);
                assertEquals(Integer.parseInt(s[2]), r.payload().size(), "payload files of " + s[1]);
            }
            long now = wallClock ? System.currentTimeMillis() : TestKeys.signedAtMs(next, "line2") + 60_000;
            HeadVerifier.Head h2 = HeadVerifier.verify(next, "line2", keys, new FloorStore(tmp.resolve("state-" + state++)), now);
            assertEquals(2, h2.headVersion());
            assertEquals("1.0.1", h2.version());
            ReleaseReader.read(next, h2);
        }

        HeadVerifier.Head h = HeadVerifier.verify(registry, "line2", keys, new FloorStore(tmp.resolve("state-tamper")), TestKeys.signedAtMs(registry, "line2") + 60_000);
        Path f = registry.resolve(h.releasePath()).getParent().resolve("payload").resolve("attribution-rules").resolve("0.json");
        byte[] b = Files.readAllBytes(f); b[b.length - 2] ^= 1; Files.write(f, b);
        Refusal r = assertThrows(Refusal.class, () -> ReleaseReader.read(registry, h));
        assertEquals("provenance-mismatch", r.reason);
        assertEquals("release-file", r.evidence.get("rule"));
    }
}
