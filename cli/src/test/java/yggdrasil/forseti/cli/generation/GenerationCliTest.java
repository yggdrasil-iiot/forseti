package yggdrasil.forseti.cli.generation;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.cli.gate.GateBundles;
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
        assertEquals(2, cli(clock(REGISTRY, "line2"), "promote", "--state", state.toString()).exit(), "promote without its flags is a usage error");
        assertFalse(Files.exists(state.resolve(FileGenerationStore.FILE)), "a usage error writes nothing");
    }

    // ============================================================================================================== promote

    /** A time at which every fixture head the promotions verify is valid (after each signedAt, before each expiry). */
    static Clock promoteClock() {
        return fixed(Math.max(Math.max(signedAt(REGISTRY, "line2"), signedAt(REGISTRY, "line2-plc")), signedAt(NEXT, "line2")) + 120_000);
    }

    /** {@code generation promote} in-process with the repository root. */
    static Out promoteCli(Clock clock, String... args) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = GenerationCli.run(args, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8), clock, ROOT);
        return new Out(exit, so.toString(StandardCharsets.UTF_8), se.toString(StandardCharsets.UTF_8), so.toByteArray());
    }

    /** The arguments promoting the full bundle's {@code scenario} run on its own store, {@code mappings} the registry's. */
    static List<String> promoteArgs(GateBundles.Bundle b, String scenario, Path registry) {
        JsonNode r = b.reportJson();
        JsonNode g = GateBundles.generationEntry(r, null, scenario), e = GateBundles.runEntry(r, null, scenario);
        return new ArrayList<>(List.of("promote", "--state", b.state().toString(), "--mappings", registry.resolve("mappings").toString(), "--trusted-keys", KEYS.toString(),
            "--generation", g.get("generation").asText(), "--registered-row-sha256", g.get("registeredRowSha256").asText(),
            "--run", b.file(e.get("runRecord").asText()).toString(), "--gate-report", b.report().toString(), "--accept-scope", "core"));
    }

    static Out promote(GateBundles.Bundle b, String scenario) { return promoteCli(promoteClock(), promoteArgs(b, scenario, REGISTRY).toArray(String[]::new)); }

    GateBundles.Bundle bundle() throws IOException { return GateBundles.full(tmp.resolve("bundle"), GateBundles.Options.DEFAULT); }

    static long generationOf(GateBundles.Bundle b, String scenario) { return GateBundles.generationEntry(b.reportJson(), null, scenario).get("generation").longValue(); }

    static byte[] log(GateBundles.Bundle b) throws IOException { return Files.readAllBytes(b.state().resolve(FileGenerationStore.FILE)); }

    @Test void promoteAppendsReplayingVerifiedCurrentInOneOperation() throws IOException {
        GateBundles.Bundle b = bundle();
        long n = generationOf(b, "smoke");
        byte[] before = log(b);
        List<String> events = new ArrayList<>();
        GenerationCli.stores = s -> new FileGenerationStore(s, f -> new FileGenerationStoreTest.Spy(FileChannel.open(f, StandardOpenOption.CREATE,
            StandardOpenOption.WRITE, StandardOpenOption.APPEND), events));
        Out o;
        try { o = promote(b, "smoke"); } finally { GenerationCli.stores = FileGenerationStore::new; }
        assertEquals(0, o.exit(), o.stdout() + o.stderr());
        assertEquals("write", events.get(0));
        assertEquals(List.of("force:true", "close"), events.subList(events.size() - 2, events.size()), "one channel, one force: " + events);
        assertTrue(events.subList(0, events.size() - 2).stream().allMatch("write"::equals), events.toString());
        JsonNode p = o.json();
        assertEquals(sorted(List.of("generation", "status", "registeredRowSha256", "gateReportSha256", "run", "retired")), fields(p));
        assertEquals(n, p.get("generation").longValue());
        assertEquals("current", p.get("status").asText());
        assertTrue(p.get("retired").isNull());
        String reportSha = Canonical.digestOf(Files.readAllBytes(b.report()));
        assertEquals(reportSha, p.get("gateReportSha256").asText());
        Path runFile = b.file(GateBundles.runEntry(b.reportJson(), null, "smoke").get("runRecord").asText());
        JsonNode rr = Json.read(runFile);
        assertEquals(rr.get("runId"), p.path("run").get("runId"));
        assertEquals(Canonical.digestOf(Files.readAllBytes(runFile)), p.path("run").get("runRecordSha256").asText());
        byte[] after = log(b);
        assertArrayEquals(before, Arrays.copyOf(after, before.length), "earlier bytes unchanged");
        List<GenerationStore.Entry> rows = new FileGenerationStore(b.state()).byNumber(n);
        assertEquals(List.of(Generation.REGISTERED, Generation.REPLAYING, Generation.VERIFIED, Generation.CURRENT), rows.stream().map(x -> x.row().status()).toList());
        for (GenerationStore.Entry x : rows.subList(1, 4)) assertArrayEquals(Canonical.bytes(p.get("run")), Canonical.bytes(x.row().run()), "the three rows carry the same run");
        assertNull(rows.get(1).row().gateReportSha256());
        assertEquals(reportSha, rows.get(2).row().gateReportSha256());
        assertEquals(reportSha, rows.get(3).row().gateReportSha256());
        assertEquals(promoteClock().millis(), rows.get(3).row().at());
        List<JsonNode> all = lines(b.state().resolve(FileGenerationStore.FILE));
        assertEquals(List.of("replaying", "verified", "current"), all.subList(all.size() - 3, all.size()).stream().map(x -> x.path("value").path("status").asText()).toList(),
            "the batch is the end of the log");
        assertEquals(n, new FileGenerationStore(b.state()).current("line2").orElseThrow().row().generation());
    }

    @Test void promoteRetiresThePreviousCurrentOfTheScope() throws IOException {
        GateBundles.Bundle b = bundle();
        long first = generationOf(b, "smoke"), second = generationOf(b, "baseline");
        assertEquals(0, promote(b, "smoke").exit());
        Out o = promote(b, "baseline");
        assertEquals(0, o.exit(), o.stdout() + o.stderr());
        assertEquals(first, o.json().get("retired").longValue());
        List<JsonNode> all = lines(b.state().resolve(FileGenerationStore.FILE));
        List<String> tail = new ArrayList<>();
        for (JsonNode x : all.subList(all.size() - 4, all.size())) tail.add(x.path("value").path("generation").asLong() + " " + x.path("value").path("status").asText());
        assertEquals(List.of(second + " replaying", second + " verified", first + " retired", second + " current"), tail);
        FileGenerationStore s = new FileGenerationStore(b.state());
        assertEquals(second, s.current("line2").orElseThrow().row().generation());
        GenerationStore.Entry retired = s.byNumber(first).get(4);
        assertNull(retired.row().run());
        assertNull(retired.row().gateReportSha256());
    }

    @Test void promoteVerifiesTheHeadAtTheCurrentTime() throws IOException {
        GateBundles.Bundle b = bundle();
        byte[] before = log(b);
        JsonNode env = Json.read(REGISTRY.resolve("mappings/head-attestations/line2.dsse.json"));
        long expires = Instant.parse(Json.read(Base64.getDecoder().decode(env.get("payload").asText())).path("predicate").path("expires").asText()).toEpochMilli();
        Out expired = promoteCli(fixed(expires), promoteArgs(b, "smoke", REGISTRY).toArray(String[]::new));
        assertEquals(1, expired.exit(), expired.stdout() + expired.stderr());
        assertEquals("head-expired", expired.json().get("refusal").asText());
        assertArrayEquals(before, log(b), "no row");
        // a later registration raised the floor to 2: the head the generation pinned (1) is now a rollback
        register(b.state(), NEXT, "line2", "line2-attribution@1.0.1", null);
        byte[] raised = log(b);
        Out lowered = promote(b, "smoke");
        assertEquals(1, lowered.exit(), lowered.stdout() + lowered.stderr());
        assertEquals("head-rollback", lowered.json().get("refusal").asText());
        assertArrayEquals(raised, log(b), "no row");
    }

    @Test void promoteRefusesAGenerationWhoseHeadMovedOn() throws IOException {
        GateBundles.Bundle b = bundle();
        byte[] before = log(b);
        Out o = promoteCli(promoteClock(), promoteArgs(b, "smoke", NEXT).toArray(String[]::new));
        assertEquals(1, o.exit(), o.stdout() + o.stderr());
        assertEquals("generation-release-superseded", o.json().get("refusal").asText());
        assertNotEquals(o.json().get("pinnedReleaseSha256"), o.json().get("headReleaseSha256"));
        assertArrayEquals(before, log(b), "no row");
    }

    @Test void promoteWithoutARunOrAReportIsAUsageError() throws IOException {
        GateBundles.Bundle b = bundle();
        byte[] before = log(b);
        for (String flag : List.of("--run", "--gate-report", "--generation", "--registered-row-sha256")) {
            List<String> a = promoteArgs(b, "smoke", REGISTRY);
            int i = a.indexOf(flag);
            a.remove(i + 1);
            a.remove(i);
            Out o = promoteCli(promoteClock(), a.toArray(String[]::new));
            assertEquals(2, o.exit(), flag + ": " + o.stdout() + o.stderr());
            assertTrue(o.stderr().contains(flag), o.stderr());
        }
        List<String> semantic = promoteArgs(b, "smoke", REGISTRY);
        semantic.set(semantic.indexOf("core"), "semantic");
        assertEquals(2, promoteCli(promoteClock(), semantic.toArray(String[]::new)).exit(), "--accept-scope takes core only");
        assertArrayEquals(before, log(b), "a usage error writes nothing");
    }

    @Test void twoScopesAreCurrentAtOnce() throws IOException {
        GateBundles.Bundle b = GateBundles.full(tmp.resolve("bundle"), new GateBundles.Options("smoke-plc", null));
        Out a = promote(b, "smoke"), c = promote(b, "smoke-plc");
        assertEquals(0, a.exit(), a.stdout() + a.stderr());
        assertEquals(0, c.exit(), c.stdout() + c.stderr());
        assertTrue(c.json().get("retired").isNull(), "another scope's current is not retired");
        FileGenerationStore s = new FileGenerationStore(b.state());
        assertEquals(generationOf(b, "smoke"), s.current("line2").orElseThrow().row().generation());
        assertEquals(generationOf(b, "smoke-plc"), s.current("line2-plc").orElseThrow().row().generation());
    }

    @Test void aSecondPromoteOfTheSameGenerationIsRefused() throws IOException {
        GateBundles.Bundle b = bundle();
        assertEquals(0, promote(b, "smoke").exit());
        byte[] once = log(b);
        Out again = promote(b, "smoke");
        assertEquals(1, again.exit(), again.stdout() + again.stderr());
        assertEquals("generation-transition", again.json().get("refusal").asText());
        assertEquals("current", again.json().get("from").asText());
        assertArrayEquals(once, log(b));
    }

    @Test void aFloorRaisedByAnotherRegistrationBeforePublicationRefusesThePromotion() throws IOException {
        GateBundles.Bundle b = bundle();
        long highest = new FileGenerationStore(b.state()).highest();
        GenerationCli.afterParse = () -> register(b.state(), NEXT, "line2", "line2-attribution@1.0.1", null);
        Out o;
        try { o = promote(b, "smoke"); } finally { GenerationCli.afterParse = () -> {}; }
        assertEquals(1, o.exit(), o.stdout() + o.stderr());
        assertEquals("head-rollback", o.json().get("refusal").asText());
        FileGenerationStore s = new FileGenerationStore(b.state());
        assertEquals(highest + 1, s.highest(), "the interleaved registration landed");
        assertEquals(2, s.registered(highest + 1).orElseThrow().row().headVersion());
        assertTrue(s.all().stream().allMatch(x -> x.row().status().equals(Generation.REGISTERED)), "no replaying, verified, current or retired row");
    }

    @Test void aRegistrationStartedDuringAPromotionLandsAfterItsRows() throws Exception {
        GateBundles.Bundle b = bundle();
        long n = generationOf(b, "smoke"), highest = new FileGenerationStore(b.state()).highest();
        Thread[] t = new Thread[1];
        Throwable[] failure = new Throwable[1];
        GenerationCli.insideHold = () -> {
            GenerationCli.insideHold = () -> {};
            t[0] = new Thread(() -> { try { register(b.state(), REGISTRY, "line2", RULE, null); } catch (Throwable x) { failure[0] = x; } });
            t[0].start();
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            assertTrue(t[0].isAlive(), "the registration waits for the promotion's hold");
        };
        Out o;
        try { o = promote(b, "smoke"); } finally { GenerationCli.insideHold = () -> {}; }
        assertEquals(0, o.exit(), o.stdout() + o.stderr());
        t[0].join(60_000);
        assertNull(failure[0]);
        List<JsonNode> all = lines(b.state().resolve(FileGenerationStore.FILE));
        List<String> tail = new ArrayList<>();
        for (JsonNode x : all.subList(all.size() - 4, all.size())) tail.add(x.path("value").path("generation").asLong() + " " + x.path("value").path("status").asText());
        assertEquals(List.of(n + " replaying", n + " verified", n + " current", (highest + 1) + " registered"), tail);
    }

    @Test void aWriteOrForceFailureIsControlLogIoAndNeverSuccess() throws IOException {
        for (boolean atForce : List.of(false, true)) {
            GateBundles.Bundle b = GateBundles.full(tmp.resolve("bundle-" + atForce), GateBundles.Options.DEFAULT);
            byte[] before = log(b);
            GenerationCli.stores = s -> new FileGenerationStore(s, f -> new FileGenerationStoreTest.Driven(f, 64, atForce ? Long.MAX_VALUE : 0, false, -1, atForce));
            Out o;
            try { o = promote(b, "smoke"); } finally { GenerationCli.stores = FileGenerationStore::new; }
            assertEquals(1, o.exit(), o.stdout() + o.stderr());
            JsonNode p = o.json();
            assertEquals("control-log-io", p.get("refusal").asText());
            assertFalse(o.stdout().contains("\"status\":\"current\""), "never a success");
            byte[] after = log(b);
            assertEquals(after.length - before.length, p.get("appended").longValue(), "the count of bytes appended");
            if (!atForce) assertArrayEquals(before, after);
            else assertTrue(p.get("appended").longValue() > 0);
        }
    }

    static List<String> fields(JsonNode n) { List<String> l = new ArrayList<>(); n.fieldNames().forEachRemaining(l::add); return l; }
    static List<String> sorted(List<String> l) { List<String> s = new ArrayList<>(l); Collections.sort(s); return s; }
}
