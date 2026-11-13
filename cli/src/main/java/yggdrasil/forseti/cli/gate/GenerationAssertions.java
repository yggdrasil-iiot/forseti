package yggdrasil.forseti.cli.gate;

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
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.cli.gate.CoreGate.Check;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.CoreGate.Result;
import yggdrasil.forseti.cli.gate.Foundation.Problems;
import yggdrasil.forseti.cli.generation.FileGenerationStore;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.release.FloorStore;

/** The generation-control assertions of pull request 5a ({@code F-GEN-*}): {@code forseti generation new} and the run preparation of
 *  {@code forseti run}, each in its own temporary store ({@code store: assertion:<ID>}, {@code isolated}, {@code variant: self-test}),
 *  never in the gate's pipeline store or its {@code --state}. The clock is the fixture clock, the head's signedAt + 60 s. */
final class GenerationAssertions {
    private GenerationAssertions() {}

    static final String SMOKE = Stage1Assertions.SMOKE, RULE = "line2-attribution@1.0.0", RULE_NEXT = "line2-attribution@1.0.1";

    static List<Check> checks() {
        return List.of(
            new Check("F-GEN-NEW", GenerationAssertions::created),
            new Check("F-GEN-REFUSES-UNREGISTERED", GenerationAssertions::unregistered),
            new Check("F-GEN-FLOOR", GenerationAssertions::floor));
    }

    record Out(int exit, String stdout) {
        JsonNode json() { return Json.read(stdout.strip().getBytes(StandardCharsets.UTF_8)); }
    }

    static Out main(String... args) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = Main.run(args, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8));
        return new Out(exit, (so.toString(StandardCharsets.UTF_8).strip() + " " + se.toString(StandardCharsets.UTF_8).strip()).strip());
    }

    static Out run(Context c, Path state, Path recording, long generation, Path out) {
        return main("run", "--engine", "core", "--recording", recording.toString(), "--registry", c.fixtures().resolve("registry").toString(),
            "--state", state.toString(), "--generation", Long.toString(generation), "--out", out.toString());
    }

    static List<String> lines(Path f) throws IOException { return Files.exists(f) ? Files.readAllLines(f, StandardCharsets.UTF_8) : List.of(); }

    /** {@code F-GEN-NEW}: {@code new} with {@code --recording smoke} writes exactly one {@code registered} row with its pins and the head
     *  floor, and no recovery declaration; the printed {@code registeredRowSha256} is the digest of the stored line. The first
     *  {@code forseti run} writes exactly one declaration with the recording's routing; a second run adds neither a declaration nor a row. */
    static Result created(Context c) throws Exception {
        Problems p = new Problems();
        Path rec = c.path(SMOKE), registry = c.fixtures().resolve("registry");
        GateEvidence.Store store = c.assertionStore();
        Path state = store.dir();
        GateEvidence.Registered g = GateEvidence.register(c, store, registry, rec, "smoke", GateEvidence.SELF_TEST, c.current);
        Path file = state.resolve(FileGenerationStore.FILE), declarations = state.resolve(ReferenceEngine.RECOVERY_CONTROL);
        List<String> rows = lines(file);
        p.equal(1, rows.size(), "generation rows after new");
        if (rows.size() != 1) return p.result("");
        byte[] stored = Files.readAllBytes(file);
        JsonNode record = Json.read(rows.get(0).getBytes(StandardCharsets.UTF_8)), v = record.path("value");
        p.equal(1L, g.generation(), "printed generation");
        p.equal("registered", v.path("status").asText(null), "row status");
        p.equal("line2", record.path("key").asText(null), "record key (scope)");
        p.equal("1", record.path("headers").path(FileGenerationStore.HEADER).asText(null), "record header");
        p.require(v.path("run").isNull(), "run is not null on the registered row: " + v.path("run"));
        p.require(v.path("gateReportSha256").isNull(), "gateReportSha256 is not null on the registered row");
        p.equal(g.rulesDigest(), v.path("rulesDigest").asText(null), "printed rulesDigest");
        p.equal(g.rulesDigest(), Canonical.digest(v.path("releaseEvidence").path("pins")), "rulesDigest = digest of the pinned pins");
        JsonNode head = Json.read(registry.resolve("mappings/heads/line2.json"));
        p.equal(head.path("sha256").asText(), v.path("releaseEvidence").path("releaseSha256").asText(null), "pinned releaseSha256");
        p.equal(1, v.path("inputReleases").size(), "inputReleases");
        p.equal(Json.read(rec.resolve(Recording.MANIFEST)).path("definitions"), v.path("releaseEvidence").path("pins").path("definitions"), "pinned definitions");
        RoutingManifest routing = Recording.open(rec).routing();
        Set<String> want = new TreeSet<>(), pinned = new TreeSet<>();
        for (String t : RoutingManifest.TOPICS) for (int part : routing.subscribed(t)) want.add(t + "/" + part + "@0");
        v.path("replayFrom").fields().forEachRemaining(t -> t.getValue().fields().forEachRemaining(e -> pinned.add(t.getKey() + "/" + e.getKey() + "@" + e.getValue().asLong())));
        p.equal(want, pinned, "replayFrom: offset 0 of every subscribed partition");
        p.equal(Canonical.digestOf(Arrays.copyOf(stored, stored.length - 1)), g.registeredRowSha256(), "printed registeredRowSha256 = digest of the stored line");
        p.require(Files.isRegularFile(state.resolve("head-floors/line2.json")), "no head-floors/line2.json");
        p.equal(1L, new FloorStore(state).get("line2").map(FloorStore.Floor::headVersion).orElse(null), "floor");
        p.require(!Files.exists(declarations), "generation new wrote a recovery declaration");

        String routingDigest = routing.routingDigest();
        for (int i = 1; i <= 2; i++) {
            Path out = c.newDir("gen-new-run").resolve("out");
            Out o = run(c, state, rec, g.generation(), out);
            p.equal(0, o.exit(), "run " + i + " exit (" + o.stdout() + ")");
            if (o.exit() != 0) break;
            GateEvidence.keep(c, g, out, rec, "smoke", GateEvidence.SELF_TEST, ReferenceEngine.DEFAULT_SEED, false, GateEvidence.safe(store.name()) + ".smoke.run-" + i);
            List<String> d = lines(declarations);
            p.equal(1, d.size(), "recovery declarations after run " + i);
            if (d.size() == 1) {
                JsonNode decl = Json.read(d.get(0).getBytes(StandardCharsets.UTF_8));
                p.equal(1L, decl.path("generation").asLong(), "declared generation");
                p.equal(routingDigest, decl.path("routingDigest").asText(null), "declared routingDigest");
                p.equal(0L, decl.path("targetEpoch").asLong(-1), "declared targetEpoch");
            }
            p.require(Arrays.equals(stored, Files.readAllBytes(file)), "run " + i + " changed generations.jsonl");
        }
        return p.result("generation new (smoke, fixture clock): one registered row, run null, rulesDigest = digest of the pinned pins, replayFrom offset 0 on "
            + want.size() + " partitions, floor 1, no declaration, registeredRowSha256 = digest of the stored line; two forseti run: one declaration (routing "
            + routingDigest.substring(0, 12) + "...), still one generation row");
    }

    /** {@code F-GEN-REFUSES-UNREGISTERED}: in a store holding generation 1, the harness refuses generation 7 and writes nothing. */
    static Result unregistered(Context c) throws Exception {
        Problems p = new Problems();
        Path rec = c.path(SMOKE);
        GateEvidence.Store store = c.assertionStore();
        GateEvidence.register(c, store, c.fixtures().resolve("registry"), rec, "smoke", GateEvidence.SELF_TEST, c.current);
        Path file = store.dir().resolve(FileGenerationStore.FILE);
        byte[] before = Files.readAllBytes(file);
        Path out = c.newDir("gen-unregistered").resolve("out");
        Out o = run(c, store.dir(), rec, 7, out);
        p.equal(1, o.exit(), "forseti run --generation 7 exit (" + o.stdout() + ")");
        if (o.exit() == 1) p.equal("generation-unregistered", o.json().path("refusal").asText(null), "refusal");
        p.require(!Files.exists(store.dir().resolve(ReferenceEngine.RECOVERY_CONTROL)), "a recovery declaration was written");
        p.require(Arrays.equals(before, Files.readAllBytes(file)), "generations.jsonl changed");
        p.require(!Files.exists(out.resolve("run.json")), "a run record was written");
        return p.result("forseti run --generation 7 in a store holding generation 1: refused generation-unregistered (exit 1), no declaration, no run record, the store unchanged");
    }

    /** {@code F-GEN-FLOOR}: in its own store, {@code new} on {@code registry} (floor 1), on {@code registry-next} (floor 2), on
     *  {@code registry} again: {@code head-rollback}, no third row, the floor stays 2. */
    static Result floor(Context c) throws Exception {
        Problems p = new Problems();
        Path registry = c.fixtures().resolve("registry"), next = c.fixtures().resolve("registry-next");
        GateEvidence.Store store = c.assertionStore();
        Path state = store.dir();
        GateEvidence.Registered a = GateEvidence.register(c, store, registry, "line2", RULE, null, "registry", GateEvidence.SELF_TEST, c.current);
        p.equal(1L, a.generation(), "first generation");
        p.equal(1L, new FloorStore(state).get("line2").map(FloorStore.Floor::headVersion).orElse(null), "floor after registry");
        GateEvidence.Registered b = GateEvidence.register(c, store, next, "line2", RULE_NEXT, null, "registry-next", GateEvidence.SELF_TEST, c.current);
        p.equal(2L, b.generation(), "second generation");
        p.equal(2L, new FloorStore(state).get("line2").map(FloorStore.Floor::headVersion).orElse(null), "floor after registry-next");
        Path m = registry.resolve("mappings");
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = GenerationCli.run(new String[] {"new", "--state", state.toString(), "--mappings", m.toString(), "--trusted-keys", c.fixtures().resolve("trusted-keys.jsonl").toString(),
                "--stream", "line2", "--rule", RULE}, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8),
            Clock.fixed(Instant.ofEpochMilli(GateSigner.now(m, "line2")), ZoneOffset.UTC));
        String stdout = so.toString(StandardCharsets.UTF_8).strip();
        p.equal(1, exit, "new on registry after registry-next exit (" + stdout + " " + se.toString(StandardCharsets.UTF_8).strip() + ")");
        if (exit == 1) p.equal("head-rollback", Json.read(stdout.getBytes(StandardCharsets.UTF_8)).path("refusal").asText(null), "refusal");
        p.equal(2, lines(state.resolve(FileGenerationStore.FILE)).size(), "generation rows");
        p.equal(2L, new FloorStore(state).get("line2").map(FloorStore.Floor::headVersion).orElse(null), "floor after the refused rollback");
        return p.result("own store: new on registry -> generation 1, floor 1; on registry-next -> generation 2, floor 2; on registry again -> head-rollback, "
            + "2 rows, floor 2");
    }
}
