package yggdrasil.forseti.cli.generation;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;
import yggdrasil.forseti.core.release.FloorStore;

/** {@code forseti generation new|retire|list} and the run preparation of {@code forseti run}, on the committed fixtures (never written:
 *  every state directory is temporary). The clock is fixed at the head's signedAt + 60 s. */
class GenerationCliTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize(), FIXTURES = ROOT.resolve("fixtures");
    static final Path REGISTRY = FIXTURES.resolve("registry"), NEXT = FIXTURES.resolve("registry-next"), MISMATCH = FIXTURES.resolve("registry-mismatch");
    static final Path SMOKE = FIXTURES.resolve("recordings/smoke"), SMOKE_PLC = FIXTURES.resolve("recordings/smoke-plc");
    static final Path KEYS = FIXTURES.resolve("trusted-keys.jsonl");
    static final String RULE = "line2-attribution@1.0.0", ARTIFACT = "ab".repeat(32);

    @TempDir Path tmp;

    record Out(int exit, String stdout, String stderr, byte[] raw) {
        JsonNode json() { return Json.read(stdout.strip().getBytes(StandardCharsets.UTF_8)); }
    }

    static long signedAt(Path registry, String stream) {
        JsonNode env = Json.read(registry.resolve("mappings/head-attestations/" + stream + ".dsse.json"));
        JsonNode st = Json.read(Base64.getDecoder().decode(env.get("payload").asText()));
        return Instant.parse(st.path("predicate").path("signedAt").asText()).toEpochMilli();
    }

    static Clock fixed(long ms) { return Clock.fixed(Instant.ofEpochMilli(ms), ZoneOffset.UTC); }
    static Clock clock(Path registry, String stream) { return fixed(signedAt(registry, stream) + 60_000); }

    static Out cli(Clock clock, String... args) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = GenerationCli.run(args, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8), clock);
        return new Out(exit, so.toString(StandardCharsets.UTF_8), se.toString(StandardCharsets.UTF_8), so.toByteArray());
    }

    static Out main(String... args) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = Main.run(args, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8));
        return new Out(exit, so.toString(StandardCharsets.UTF_8), se.toString(StandardCharsets.UTF_8), so.toByteArray());
    }

    static String[] newArgs(Path state, Path registry, String stream, String rule, Path recording, String... more) {
        List<String> a = new ArrayList<>(List.of("new", "--state", state.toString(), "--mappings", registry.resolve("mappings").toString(),
            "--trusted-keys", KEYS.toString(), "--stream", stream, "--rule", rule));
        if (recording != null) a.addAll(List.of("--recording", recording.toString()));
        a.addAll(List.of(more));
        return a.toArray(String[]::new);
    }

    /** {@code generation new} on {@code registry}'s head of {@code stream} at its signedAt + 60 s; exit 0 required. */
    static JsonNode register(Path state, Path registry, String stream, String rule, Path recording) {
        Out o = cli(clock(registry, stream), newArgs(state, registry, stream, rule, recording));
        assertEquals(0, o.exit(), o.stdout() + o.stderr());
        return o.json();
    }

    static Out run(Path state, Path recording, long generation, Path out) {
        return main("run", "--engine", "core", "--recording", recording.toString(), "--registry", REGISTRY.toString(), "--state", state.toString(),
            "--generation", Long.toString(generation), "--out", out.toString(), "--artifact-sha256", ARTIFACT);
    }

    static List<JsonNode> lines(Path f) throws IOException {
        List<JsonNode> l = new ArrayList<>();
        for (String s : Files.readAllLines(f, StandardCharsets.UTF_8)) l.add(Json.read(s.getBytes(StandardCharsets.UTF_8)));
        return l;
    }

    static Path declarations(Path state) { return state.resolve(ReferenceEngine.RECOVERY_CONTROL); }

    // ================================================================================================================== new

    @Test void newRegistersAGenerationWithPins() throws IOException {
        Path state = tmp.resolve("state");
        JsonNode printed = register(state, REGISTRY, "line2", RULE, SMOKE);
        assertEquals(List.of("generation", "registeredRowSha256", "rulesDigest", "scope", "status"), fields(printed));
        assertEquals(1, printed.get("generation").longValue());
        assertEquals("registered", printed.get("status").asText());
        assertEquals("line2", printed.get("scope").asText());
        List<JsonNode> rows = lines(state.resolve(FileGenerationStore.FILE));
        assertEquals(1, rows.size());
        JsonNode v = rows.get(0).get("value");
        assertEquals("line2", rows.get(0).get("key").asText());
        assertEquals("1", rows.get(0).path("headers").get("forseti-generation").asText());
        assertEquals("registered", v.get("status").asText());
        assertEquals(printed.get("rulesDigest"), v.get("rulesDigest"));
        assertTrue(v.get("run").isNull());
        assertTrue(v.get("gateReportSha256").isNull());
        assertEquals(signedAt(REGISTRY, "line2") + 60_000, v.get("at").longValue());
        JsonNode head = Json.read(REGISTRY.resolve("mappings/heads/line2.json"));
        assertEquals(1, v.get("inputReleases").size());
        JsonNode in = v.get("inputReleases").get(0);
        assertEquals(sorted(List.of("stream", "releaseId", "version", "releaseSha256", "headVersion", "statementSha256")), fields(in));
        assertEquals("line2", in.get("stream").asText());
        assertEquals(head.get("id"), in.get("releaseId"));
        assertEquals(head.get("version"), in.get("version"));
        assertEquals(head.get("sha256"), in.get("releaseSha256"));
        assertEquals(1, in.get("headVersion").longValue());
        assertEquals(1, v.get("headVersion").longValue());
        JsonNode ev = v.get("releaseEvidence");
        assertEquals(List.of("head", "pins", "releaseSha256"), sorted(fields(ev)));
        assertEquals(head.get("sha256"), ev.get("releaseSha256"));
        assertEquals(in.get("statementSha256"), ev.path("head").get("statementSha256"));
        assertEquals("line2-attribution", ev.path("pins").path("rules").path("attribution").get("id").asText());
        assertEquals(printed.get("rulesDigest").asText(), Canonical.digest(ev.get("pins")), "rulesDigest is the digest of the pinned pins");
        assertEquals(Json.read(SMOKE.resolve("manifest.json")).get("definitions"), ev.path("pins").get("definitions"));
        assertEquals(Canonical.digestOf(Arrays.copyOf(Files.readAllBytes(state.resolve(FileGenerationStore.FILE)),
            (int) Files.size(state.resolve(FileGenerationStore.FILE)) - 1)), printed.get("registeredRowSha256").asText());
        assertEquals(new FloorStore.Floor(1, in.get("statementSha256").asText()), new FloorStore(state).get("line2").orElseThrow());
        assertTrue(Files.isRegularFile(state.resolve("head-floors/line2.json")));
    }

    @Test void newWithoutARecordingPinsNoOffsetsAndWritesNoDeclaration() throws IOException {
        Path state = tmp.resolve("state");
        register(state, REGISTRY, "line2", RULE, null);
        JsonNode v = lines(state.resolve(FileGenerationStore.FILE)).get(0).get("value");
        assertEquals(Json.object(), v.get("replayFrom"));
        assertEquals(Json.object(), v.path("releaseEvidence").path("pins").get("definitions"), "no recording and no --definitions: none pinned");
        assertFalse(Files.exists(declarations(state)), "generation new writes no recovery declaration");
    }

    @Test void newWithARecordingPinsOffsetZeroAndWritesNoDeclaration() throws IOException {
        Path state = tmp.resolve("state");
        register(state, REGISTRY, "line2", RULE, SMOKE);
        JsonNode v = lines(state.resolve(FileGenerationStore.FILE)).get(0).get("value");
        Recording rec = Recording.open(SMOKE);
        Map<String, Map<String, Long>> want = new TreeMap<>();
        for (String t : List.of("ot.messages", "ot.sessions", "mfg.occupancy", "quality.incidents"))
            for (int p : rec.routing().subscribed(t)) want.computeIfAbsent(t, x -> new TreeMap<>()).put(Integer.toString(p), 0L);
        Map<String, Map<String, Long>> have = new TreeMap<>();
        v.get("replayFrom").fields().forEachRemaining(t -> t.getValue().fields().forEachRemaining(p ->
            have.computeIfAbsent(t.getKey(), x -> new TreeMap<>()).put(p.getKey(), p.getValue().longValue())));
        assertEquals(want, have);
        assertEquals(4, have.size());
        assertFalse(Files.exists(declarations(state)), "generation new writes no recovery declaration");
    }

    @Test void theFirstRunPreparationWritesTheInitialDeclarationOnce() throws IOException {
        Path state = tmp.resolve("state");
        register(state, REGISTRY, "line2", RULE, SMOKE);
        Out a = run(state, SMOKE, 1, tmp.resolve("a"));
        assertEquals(0, a.exit(), a.stdout() + a.stderr());
        byte[] after = Files.readAllBytes(declarations(state));
        Out b = run(state, SMOKE, 1, tmp.resolve("b"));
        assertEquals(0, b.exit(), b.stdout() + b.stderr());
        assertArrayEquals(after, Files.readAllBytes(declarations(state)), "the second run declares nothing");
        List<JsonNode> d = lines(declarations(state));
        assertEquals(1, d.size());
        String want = "{\"epochBefore\":null,\"generation\":1,\"injectionId\":null,\"lastWrittenCutId\":0,\"routingDigest\":\""
            + Recording.open(SMOKE).routing().routingDigest() + "\",\"stage\":1,\"targetEpoch\":0}\n";
        assertEquals(want, Files.readString(declarations(state), StandardCharsets.UTF_8));
    }

    @Test void newRefusesAnExpiredHead() throws IOException {
        Path state = tmp.resolve("state");
        JsonNode env = Json.read(REGISTRY.resolve("mappings/head-attestations/line2.dsse.json"));
        long expires = Instant.parse(Json.read(Base64.getDecoder().decode(env.get("payload").asText())).path("predicate").path("expires").asText()).toEpochMilli();
        Out o = cli(fixed(expires), newArgs(state, REGISTRY, "line2", RULE, SMOKE));
        assertEquals(1, o.exit(), o.stdout() + o.stderr());
        assertEquals("head-expired", o.json().get("refusal").asText());
        assertTrue(new FileGenerationStore(state).all().isEmpty(), "no row");
        assertTrue(new FloorStore(state).get("line2").isEmpty(), "an expired head never raises the floor");
    }

    @Test void newRefusesAProfileWithAnotherDigest() throws IOException {
        Path state = tmp.resolve("state");
        Out o = cli(clock(MISMATCH, "line2"), newArgs(state, MISMATCH, "line2", RULE, SMOKE));
        assertEquals(1, o.exit(), o.stdout() + o.stderr());
        assertEquals("binding.schema-mismatch", o.json().get("refusal").asText());
        assertTrue(new FileGenerationStore(state).all().isEmpty(), "no row");
        assertFalse(Files.exists(declarations(state)));
    }

    @Test void newFromRegistryNextRaisesTheFloor() throws IOException {
        Path state = tmp.resolve("state");
        assertEquals(1, register(state, REGISTRY, "line2", RULE, null).get("generation").longValue());
        assertEquals(1, new FloorStore(state).get("line2").orElseThrow().headVersion());
        JsonNode second = register(state, NEXT, "line2", "line2-attribution@1.0.1", null);
        assertEquals(2, second.get("generation").longValue());
        assertEquals(2, new FloorStore(state).get("line2").orElseThrow().headVersion());
        GenerationStore.Entry g2 = new FileGenerationStore(state).registered(2).orElseThrow();
        assertEquals(2, g2.row().headVersion());
        assertEquals("1.0.1", g2.row().inputReleases().get(0).get("version").asText());
        // the floor survives: the version-1 head is now a rollback
        Out back = cli(clock(REGISTRY, "line2"), newArgs(state, REGISTRY, "line2", RULE, null));
        assertEquals(1, back.exit());
        assertEquals("head-rollback", back.json().get("refusal").asText());
        assertEquals(2, new FileGenerationStore(state).all().size());
    }

    // ===================================================================================================== run preparation

    @Test void anUnregisteredGenerationRefusesTheHarness() throws IOException {
        Path state = tmp.resolve("state");
        register(state, REGISTRY, "line2", RULE, SMOKE);
        Out o = run(state, SMOKE, 7, tmp.resolve("out"));
        assertEquals(1, o.exit(), o.stdout() + o.stderr());
        assertEquals("generation-unregistered", o.json().get("refusal").asText());
        assertEquals(7, o.json().get("generation").longValue());
        assertFalse(Files.exists(declarations(state)), "no declaration for an unregistered generation");
        assertFalse(Files.exists(tmp.resolve("out")));
    }

    @Test void runAppendsNoGenerationRow() throws IOException {
        Path state = tmp.resolve("state");
        register(state, REGISTRY, "line2", RULE, SMOKE);
        byte[] before = Files.readAllBytes(state.resolve(FileGenerationStore.FILE));
        Out o = run(state, SMOKE, 1, tmp.resolve("out"));
        assertEquals(0, o.exit(), o.stdout() + o.stderr());
        assertArrayEquals(before, Files.readAllBytes(state.resolve(FileGenerationStore.FILE)));
        assertEquals(1, lines(declarations(state)).size());
    }

    // =============================================================================================================== retire

    @Test void retireAppendsARetiredRow() throws IOException {
        Path state = tmp.resolve("state");
        JsonNode reg = register(state, REGISTRY, "line2", RULE, SMOKE);
        Clock later = fixed(signedAt(REGISTRY, "line2") + 120_000);
        Out o = cli(later, "retire", "--state", state.toString(), "--generation", "1");
        assertEquals(0, o.exit(), o.stdout() + o.stderr());
        assertEquals("retired", o.json().get("status").asText());
        assertEquals(reg.get("registeredRowSha256"), o.json().get("registeredRowSha256"));
        List<GenerationStore.Entry> rows = new FileGenerationStore(state).byNumber(1);
        assertEquals(List.of(Generation.REGISTERED, Generation.RETIRED), rows.stream().map(e -> e.row().status()).toList());
        Generation r = rows.get(1).row();
        assertNull(r.run());
        assertNull(r.gateReportSha256());
        assertEquals(later.millis(), r.at());
        assertEquals(rows.get(0).row().rulesDigest(), r.rulesDigest());
        assertEquals("line2", rows.get(1).scope());
        Out again = cli(later, "retire", "--state", state.toString(), "--generation", "1");
        assertEquals(1, again.exit());
        assertEquals("generation-transition", again.json().get("refusal").asText());
        Out none = cli(later, "retire", "--state", state.toString(), "--generation", "5");
        assertEquals(1, none.exit());
        assertEquals("generation-unregistered", none.json().get("refusal").asText());
        assertEquals(2, new FileGenerationStore(state).all().size());
    }

    @Test void twoScopesInOneStoreAreAddressedByNumber() throws IOException {
        Path state = tmp.resolve("state");
        assertEquals(1, register(state, REGISTRY, "line2", RULE, SMOKE).get("generation").longValue());
        assertEquals(2, register(state, REGISTRY, "line2-plc", RULE, SMOKE_PLC).get("generation").longValue());
        Out a = run(state, SMOKE, 1, tmp.resolve("a")), b = run(state, SMOKE_PLC, 2, tmp.resolve("b"));
        assertEquals(0, a.exit(), a.stdout() + a.stderr());
        assertEquals(0, b.exit(), b.stdout() + b.stderr());
        assertEquals("line2", Json.read(tmp.resolve("a/run.json")).get("scope").asText());
        assertEquals("line2-plc", Json.read(tmp.resolve("b/run.json")).get("scope").asText());
        Clock c = clock(REGISTRY, "line2");
        assertEquals(0, cli(c, "retire", "--state", state.toString(), "--generation", "2").exit());
        assertEquals(0, cli(c, "retire", "--state", state.toString(), "--generation", "1").exit());
        FileGenerationStore s = new FileGenerationStore(state);
        for (long n : List.of(1L, 2L)) {
            List<GenerationStore.Entry> rows = s.byNumber(n);
            assertEquals(List.of(Generation.REGISTERED, Generation.RETIRED), rows.stream().map(e -> e.row().status()).toList(), "generation " + n);
            assertEquals(Set.of(n == 1 ? "line2" : "line2-plc"), new HashSet<>(rows.stream().map(GenerationStore.Entry::scope).toList()));
        }
        List<JsonNode> d = lines(declarations(state));
        assertEquals(List.of(1L, 2L), d.stream().map(x -> x.get("generation").longValue()).toList());
    }

    // ======================================================================================================== fail closed

    @Test void aCorruptControlLogFailsClosed() throws IOException {
        Path state = tmp.resolve("state");
        register(state, REGISTRY, "line2", RULE, SMOKE);
        Path log = state.resolve(FileGenerationStore.FILE);
        byte[] line = Files.readAllBytes(log);
        Files.write(log, Arrays.copyOfRange(line, 0, line.length / 2), StandardOpenOption.APPEND);   // a second line torn half-way, no LF
        byte[] torn = Files.readAllBytes(log);
        Clock c = clock(REGISTRY, "line2");
        List<Out> outs = List.of(cli(c, newArgs(state, REGISTRY, "line2", RULE, SMOKE)), cli(c, "retire", "--state", state.toString(), "--generation", "1"),
            cli(c, "list", "--state", state.toString()), run(state, SMOKE, 1, tmp.resolve("out")));
        for (Out o : outs) {
            assertEquals(1, o.exit(), o.stdout() + o.stderr());
            assertEquals("control-log-corrupt", o.json().get("refusal").asText(), o.stdout());
        }
        assertArrayEquals(torn, Files.readAllBytes(log), "never discarded or repaired");
        assertFalse(Files.exists(declarations(state)));
        assertFalse(Files.exists(tmp.resolve("out")));
        // a torn recovery-control log fails the run preparation the same way
        Path ok = tmp.resolve("ok");
        register(ok, REGISTRY, "line2", RULE, SMOKE);
        Files.writeString(declarations(ok), "{\"generation\":1", StandardCharsets.UTF_8);
        Out o = run(ok, SMOKE, 1, tmp.resolve("out2"));
        assertEquals(1, o.exit(), o.stdout() + o.stderr());
        assertEquals("control-log-corrupt", o.json().get("refusal").asText());
        assertEquals("{\"generation\":1", Files.readString(declarations(ok), StandardCharsets.UTF_8));
    }

    @Test void listPrintsTheStoredRecordsByteForByte() throws IOException {
        Path state = tmp.resolve("state");
        register(state, REGISTRY, "line2", RULE, SMOKE);
        register(state, REGISTRY, "line2-plc", RULE, SMOKE_PLC);
        assertEquals(0, cli(clock(REGISTRY, "line2"), "retire", "--state", state.toString(), "--generation", "1").exit());
        Out o = cli(clock(REGISTRY, "line2"), "list", "--state", state.toString());
        assertEquals(0, o.exit(), o.stderr());
        assertArrayEquals(Files.readAllBytes(state.resolve(FileGenerationStore.FILE)), o.raw());
        assertEquals(3, o.stdout().split("\n", -1).length - 1);
        Out empty = cli(clock(REGISTRY, "line2"), "list", "--state", tmp.resolve("empty").toString());
        assertEquals(0, empty.exit());
        assertEquals(0, empty.raw().length);
    }

    @Test void theClockIsNotSettableFromTheCommandLine() throws IOException {
        Path state = tmp.resolve("state");
        Out o = cli(clock(REGISTRY, "line2"), newArgs(state, REGISTRY, "line2", RULE, SMOKE, "--now", "1794182400000"));
        assertEquals(2, o.exit());
        assertTrue(o.stderr().contains("--now"), o.stderr());
        List<String> viaMain = new ArrayList<>(List.of("generation"));
        viaMain.addAll(List.of(newArgs(state, REGISTRY, "line2", RULE, SMOKE, "--now", "1794182400000")));
        assertEquals(2, main(viaMain.toArray(String[]::new)).exit());
        assertEquals(2, cli(clock(REGISTRY, "line2"), "retire", "--state", state.toString(), "--generation", "1", "--clock", "fixed").exit());
        assertEquals(2, cli(clock(REGISTRY, "line2"), "promote", "--state", state.toString()).exit(), "no promote verb in this PR");
        assertFalse(Files.exists(state.resolve(FileGenerationStore.FILE)), "a usage error writes nothing");
    }

    static List<String> fields(JsonNode n) { List<String> l = new ArrayList<>(); n.fieldNames().forEachRemaining(l::add); return l; }
    static List<String> sorted(List<String> l) { List<String> s = new ArrayList<>(l); Collections.sort(s); return s; }
}
