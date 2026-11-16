package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.core.Json;

/** {@code forseti recordings compare}: the core gate's per-cut comparison of the core with the oracle on one recording, here on the
 *  committed smoke recordings, and the check of every committed live recording ({@code fixtures/live-recordings/<name>/}):
 *  {@code verify-capture --lock} against its capture lock, then {@code recordings compare} equal at every cut and its report the
 *  committed {@code recording.compare-report.json}, byte for byte. Needs the oracle ({@code -Dforseti.python}, default {@code python}). */
class RecordingCompareTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    static final Path LIVE = ROOT.resolve("fixtures/live-recordings");
    static final String PYTHON = System.getProperty("forseti.python", "python");

    @TempDir Path tmp;

    record Ran(int exit, String out, String err) {}

    static Ran forseti(String... args) {
        ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
        int exit = Main.run(args, new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8));
        return new Ran(exit, o.toString(StandardCharsets.UTF_8), e.toString(StandardCharsets.UTF_8));
    }

    @Test
    void theSmokeRecordingsEqualTheOracleAtEveryCutWithEveryInputDisposed() throws Exception {
        for (String name : List.of("smoke", "smoke-plc", "smoke-incident")) {
            Path out = tmp.resolve(name);
            Ran r = forseti("recordings", "compare", ROOT.resolve("fixtures/recordings").resolve(name).toString(), "--registry", ROOT.resolve("fixtures/registry").toString(),
                "--python", PYTHON, "--out", out.toString());
            assertEquals(0, r.exit(), name + ": " + r.out() + r.err());
            JsonNode rep = Json.read(out.resolve(RecordingCompare.REPORT));
            assertEquals(r.out(), Files.readString(out.resolve(RecordingCompare.REPORT), StandardCharsets.UTF_8), "the printed report is the written one");
            assertEquals("equal", rep.path("outcome").asText(), name);
            assertEquals(name, rep.path("recording").asText());
            assertTrue(rep.path("cuts").asLong() > 0, name);
            assertEquals(rep.path("cuts").asLong(), rep.path("cutsCompared").asLong(), name + ": every cut compared");
            assertTrue(rep.path("rowsCompared").asLong() > 0 && rep.path("dispositionsCompared").asLong() > 0, name);
            assertEquals(0, rep.path("differences").asLong(), name);
            assertEquals(0, rep.path("dispositionCoverage").path("inputsWithoutDisposition").asLong(), name);
            assertTrue(rep.path("dispositionCoverage").path("deliveries").asLong() > 0, name);
            assertEquals(0, rep.path("findings").size(), name);
            assertEquals("minimal", rep.path("groundTruth").path("scope").asText(), name);
            assertTrue(rep.path("accepted").asBoolean(), name);
            assertFalse(rep.path("acceptance").path("applies").asBoolean(), name + ": a synthetic recording is no live capture");
            String text = r.out();
            assertFalse(text.contains(ROOT.toString()) || text.contains(tmp.toString()) || text.contains("\\"), name + ": the report carries no path");
            assertTrue(Files.isRegularFile(out.resolve("core/cuts.json")), "--out keeps the harness run");
        }
    }

    @Test
    void theReportIsTheSameBytesOnARerun() {
        Path rec = ROOT.resolve("fixtures/recordings/smoke");
        Ran a = forseti("recordings", "compare", rec.toString(), "--registry", ROOT.resolve("fixtures/registry").toString(), "--python", PYTHON);
        Ran b = forseti("recordings", "compare", rec.toString(), "--registry", ROOT.resolve("fixtures/registry").toString(), "--python", PYTHON,
            "--out", tmp.resolve("b").toString());
        assertEquals(0, a.exit(), a.err());
        assertEquals(0, b.exit(), b.err());
        assertEquals(a.out(), b.out());
    }

    @Test
    void usageAndInputErrorsExitTwo() throws Exception {
        assertEquals(2, forseti("recordings", "compare").exit());
        assertEquals(2, forseti("recordings", "compare", "--out", tmp.toString()).exit());
        Files.writeString(tmp.resolve("x"), "x");
        Ran full = forseti("recordings", "compare", ROOT.resolve("fixtures/recordings/smoke").toString(), "--registry", ROOT.resolve("fixtures/registry").toString(),
            "--out", tmp.toString());
        assertEquals(2, full.exit());
        assertTrue(full.err().contains("not empty"), full.err());
        assertEquals(2, forseti("recordings", "compare", tmp.resolve("missing").toString()).exit());
        assertEquals(2, forseti("recordings", "compare", ROOT.resolve("fixtures/recordings/smoke").toString(), "--seed", "1").exit());
    }

    /** Every committed live recording: its capture lock verifies, the core equals the oracle at every cut with every input disposed,
     *  the ground truth holds, its visits are accepted against its injections, and the report is the committed one. With no committed
     *  live recording yet (the directory missing or empty) there is nothing to check. */
    @Test
    void everyCommittedLiveRecordingVerifiesAgainstItsLockAndEqualsTheOracle() throws Exception {
        List<Path> dirs = List.of();
        if (Files.isDirectory(LIVE)) try (Stream<Path> s = Files.list(LIVE)) { dirs = s.filter(Files::isDirectory).sorted().toList(); }
        if (dirs.isEmpty()) System.out.println("no committed live recording in " + ROOT.relativize(LIVE));
        Path registry = ROOT.resolve("fixtures/live/registry");
        for (Path d : dirs) {
            long t = System.nanoTime();
            Ran v = forseti("recordings", "verify-capture", d.resolve("recording").toString(), "--lock", d.resolve("recording.capture-lock.json").toString(),
                "--registry", registry.toString());
            assertEquals(0, v.exit(), d.getFileName() + " verify-capture: " + v.err());
            long verifyMs = (System.nanoTime() - t) / 1_000_000;
            t = System.nanoTime();
            Ran c = forseti("recordings", "compare", d.resolve("recording").toString(), "--registry", registry.toString(), "--python", PYTHON);
            assertEquals(0, c.exit(), d.getFileName() + " compare: " + c.out() + c.err());
            JsonNode rep = Json.read(c.out().getBytes(StandardCharsets.UTF_8));
            assertTrue(rep.path("accepted").asBoolean() && rep.path("acceptance").path("applies").asBoolean(), d.getFileName() + ": a live capture, accepted");
            assertEquals(Files.readString(d.resolve("recording.compare-report.json"), StandardCharsets.UTF_8), c.out(), d.getFileName() + ": the committed compare report");
            System.out.println("live recording " + d.getFileName() + ": verify-capture " + verifyMs + " ms, compare " + (System.nanoTime() - t) / 1_000_000 + " ms");
        }
    }
}
