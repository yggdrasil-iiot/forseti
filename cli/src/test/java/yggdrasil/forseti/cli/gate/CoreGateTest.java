package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.cli.gate.CoreGate.Check;
import yggdrasil.forseti.cli.gate.CoreGate.Result;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The gate's accounting and report. The assertions that launch the oracle run in the jar gate (CI's {@code forseti gate}); here the
 *  accounting runs on stand-in bodies, and the assertions that need no interpreter run for real against the repository's fixtures. */
class CoreGateTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    static final Path MANIFEST = ROOT.resolve(CoreGate.MANIFEST);
    static final String PYTHON = System.getProperty("forseti.python", "python");
    static final Set<String> NEEDS_PYTHON = Set.of("F-JCS-PY", "F-JCS-REFUSALS", "F-RULES-LOAD-OJ", "F-RULES-LOAD-PLC");
    static final List<String> REPORT_FIELDS = List.of("exit", "scope", "engine", "manifestSha256", "assertionsExpected", "assertionsRun", "failed",
        "notRun", "intendedFailures", "pins", "seed", "fixtureDigests", "startedAt", "finishedAt", "command", "error");

    @TempDir Path tmp;

    record Run(int exit, List<String> lines, JsonNode report) {
        String last() { return lines.get(lines.size() - 1); }
        List<String> list(String field) { List<String> l = new ArrayList<>(); report.path(field).forEach(n -> l.add(n.asText())); return l; }
    }

    Run run(Path root, Path manifest, List<Check> checks) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(), err = new ByteArrayOutputStream();
        Path dir = tmp.resolve("out-" + UUID.randomUUID());
        int exit = CoreGate.run(new CoreGate.Options(root, manifest, dir, PYTHON, null, List.of(), CoreGate.DEFAULT_SEED, "forseti gate --engine core (test)"),
            checks, new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
        List<String> lines = out.toString(StandardCharsets.UTF_8).lines().toList();
        return new Run(exit, lines, Json.read(dir.resolve("report.json")));
    }
    Run run(Path manifest, List<Check> checks) { return run(ROOT, manifest, checks); }

    static List<String> manifestIds(Path manifest) { return CoreGate.manifestIds(Json.read(manifest)); }
    static List<String> registeredIds() { return CoreGate.registered().stream().map(Check::id).toList(); }
    /** Stand-ins that hold, one per registered id (the accounting does not depend on what an assertion runs). */
    static List<Check> holding() { return registeredIds().stream().map(id -> new Check(id, c -> new Result(true, "stand-in"))).toList(); }

    Path manifestWith(java.util.function.Consumer<ArrayNode> edit) throws Exception {
        ObjectNode m = (ObjectNode) Json.read(MANIFEST);
        edit.accept((ArrayNode) m.path("assertions"));
        Path f = tmp.resolve("manifest-" + UUID.randomUUID() + ".json");
        Json.write(f, m);
        return f;
    }

    @Test void everyManifestIdHasARegisteredAssertion() {
        List<String> manifest = manifestIds(MANIFEST), registered = registeredIds();
        assertEquals(18, manifest.size());
        assertEquals(manifest.size(), new HashSet<>(manifest).size(), "no id twice in the manifest");
        assertEquals(registered.size(), new HashSet<>(registered).size(), "no id registered twice");
        assertEquals(new TreeSet<>(manifest), new TreeSet<>(registered), "every manifest id is registered and every registered id is in the manifest");
        for (JsonNode a : Json.read(MANIFEST).path("assertions")) {
            assertEquals(1, a.path("pr").asInt(), a.toString());
            assertFalse(a.path("description").asText().isBlank(), a.toString());
        }
    }

    @Test void theAssertionsThatNeedNoInterpreterHoldOnTheFixtures() throws Exception {
        Path manifest = manifestWith(a -> { for (int i = a.size() - 1; i >= 0; i--) if (NEEDS_PYTHON.contains(a.get(i).path("id").asText())) a.remove(i); });
        List<Check> checks = CoreGate.registered().stream().filter(c -> !NEEDS_PYTHON.contains(c.id())).toList();
        Run r = run(manifest, checks);
        assertEquals(List.of(), r.list("failed"), String.join("\n", r.lines()));
        assertEquals(0, r.exit());
        assertEquals("assertions run 14 / expected 14", r.last());
    }

    @Test void anAssertionNotRunFailsTheGate() throws Exception {
        Path manifest = manifestWith(a -> a.addObject().put("id", "F-NOT-REGISTERED").put("pr", 1).put("description", "listed, never registered"));
        Run r = run(manifest, holding());
        assertEquals(1, r.exit());
        assertEquals(List.of("F-NOT-REGISTERED"), r.list("notRun"));
        assertEquals(List.of(), r.list("failed"));
        assertEquals("assertions run 18 / expected 19", r.last());
        assertEquals(1, r.report().path("exit").asInt());
        assertEquals(19, r.report().path("assertionsExpected").asInt());
        assertEquals(18, r.report().path("assertionsRun").asInt());
    }

    @Test void anUnknownAssertionFailsTheGate() {
        List<Check> checks = new ArrayList<>(holding());
        checks.add(new Check("F-UNKNOWN", c -> new Result(true, "holds, but is not in the manifest")));
        Run r = run(MANIFEST, checks);
        assertEquals(1, r.exit());
        assertEquals(List.of("F-UNKNOWN"), r.list("failed"));
        assertEquals(List.of(), r.list("notRun"));
        assertEquals("assertions run 18 / expected 18", r.last());

        List<Check> twice = new ArrayList<>(holding());
        twice.add(new Check("F-JCS-JAVA", c -> new Result(true, "a second run of one id")));
        Run d = run(MANIFEST, twice);
        assertEquals(1, d.exit());
        assertEquals(List.of("F-JCS-JAVA"), d.list("failed"));
    }

    @Test void aFailingAssertionFailsTheGate() {
        List<Check> checks = new ArrayList<>(holding());
        checks.set(3, new Check(checks.get(3).id(), c -> new Result(false, "does not hold")));
        Run r = run(MANIFEST, checks);
        assertEquals(1, r.exit());
        assertEquals(List.of(checks.get(3).id()), r.list("failed"));
        assertEquals("assertions run 18 / expected 18", r.last());
    }

    @Test void aCrashStillWritesAFailureReport() throws Exception {
        List<Check> checks = new ArrayList<>(holding());
        checks.set(0, new Check(checks.get(0).id(), c -> { throw new IllegalStateException("boom"); }));
        Run crashed = run(MANIFEST, checks);
        assertEquals(1, crashed.exit());
        assertEquals(List.of("F-JCS-JAVA"), crashed.list("failed"));
        assertTrue(crashed.lines().stream().anyMatch(l -> l.startsWith("FAIL F-JCS-JAVA crashed: java.lang.IllegalStateException: boom")), crashed.lines().toString());

        Run noManifest = run(tmp.resolve("no-such-manifest.json"), holding());
        assertEquals(1, noManifest.exit());
        assertEquals(1, noManifest.report().path("exit").asInt());
        assertTrue(noManifest.report().path("error").asText().contains("no-such-manifest.json"), noManifest.report().toString());
        assertEquals("assertions run 0 / expected 0", noManifest.last());

        Path empty = Files.createDirectories(tmp.resolve("empty-root"));   // every fixture missing: every assertion fails, none is skipped
        Run missing = run(empty, MANIFEST, CoreGate.registered().stream().filter(c -> !NEEDS_PYTHON.contains(c.id())).toList());
        assertEquals(1, missing.exit());
        Set<String> wantFailed = new TreeSet<>(registeredIds());
        wantFailed.removeAll(NEEDS_PYTHON);
        wantFailed.remove("F-JOURNAL-REDUCERS");   // inline journals: the one foundation assertion that reads no file
        assertEquals(wantFailed, new TreeSet<>(missing.list("failed")), missing.lines().toString());
        assertEquals(new TreeSet<>(NEEDS_PYTHON), new TreeSet<>(missing.list("notRun")));
        assertEquals("assertions run 14 / expected 18", missing.last());
        assertTrue(missing.report().path("fixtureDigests").path("registry").isNull());
    }

    @Test void theReportRecordsPinsAndDigests() throws Exception {
        Run r = run(MANIFEST, holding());
        assertEquals(0, r.exit());
        assertEquals("assertions run 18 / expected 18", r.last());
        JsonNode rep = r.report();
        List<String> fields = new ArrayList<>();
        rep.fieldNames().forEachRemaining(fields::add);
        assertEquals(REPORT_FIELDS, fields);
        assertEquals("core", rep.path("scope").asText());
        assertEquals("core", rep.path("engine").asText());
        assertEquals(Canonical.digestOf(Files.readAllBytes(MANIFEST)), rep.path("manifestSha256").asText());
        assertEquals(18, rep.path("assertionsExpected").asInt());
        assertEquals(18, rep.path("assertionsRun").asInt());
        assertEquals(20261109L, rep.path("seed").asLong());
        assertEquals("io.github.erdtman:java-json-canonicalization:1.1", rep.path("pins").path("jcs").path("java").asText());
        assertTrue(rep.path("pins").has("python") && rep.path("pins").path("jcs").has("python"));
        assertTrue(rep.path("pins").path("jdk").asText().contains(System.getProperty("java.runtime.version")));
        assertEquals("unpacked", rep.path("pins").path("forseti").path("artifactSha256").asText(), "the tests run from classes, not the jar");
        for (String k : List.of("registry", "registryNext", "registryMismatch", "upstream", "vectors", "schemas", "scenarios"))
            assertTrue(rep.path("fixtureDigests").path(k).asText("").matches("[0-9a-f]{64}"), k);
        assertEquals(CoreGate.treeDigest(ROOT.resolve("fixtures/registry")), rep.path("fixtureDigests").path("registry").asText());
        assertNotEquals(rep.path("fixtureDigests").path("registry").asText(), rep.path("fixtureDigests").path("registryNext").asText());
        assertTrue(rep.path("startedAt").asText().compareTo(rep.path("finishedAt").asText()) <= 0);
        assertTrue(rep.path("error").isNull());
        assertEquals(List.of(), r.list("intendedFailures"));
    }

    @Test void anUnlistedNegativeControlAndAnotherEngineAreUsageErrors() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CoreGate.control(ROOT, "nope"));
        assertTrue(e.getMessage().contains("not in " + CoreGate.CONTROLS), e.getMessage());
        PrintStream sink = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        assertEquals(2, Main.run(new String[] {"gate", "--engine", "flink"}, sink, sink));
        assertEquals(2, Main.run(new String[] {"gate", "--engine", "core", "--negative-control", "nope", "--out", tmp.resolve("nc").toString()}, sink, sink));
        assertFalse(Files.exists(tmp.resolve("nc").resolve("report.json")), "a usage error runs nothing");
        assertEquals(2, Main.run(new String[] {"gate", "--engine", "core", "--unknown", "x"}, sink, sink));
    }
}
