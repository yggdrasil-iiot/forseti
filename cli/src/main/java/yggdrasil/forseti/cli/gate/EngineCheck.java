package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import yggdrasil.forseti.cli.generation.FileGenerationStore;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.cli.recording.W1Form;
import yggdrasil.forseti.core.Json;

/** {@code forseti recordings check-engine}: one engine recording validated before any engine runs it (step-6 plan PR 2): the scenario
 *  is built and its recording compared with its committed lock, a generation is registered on it in a fresh store (on the gate's clock of
 *  the head, {@link GateSigner#now}, never the wall clock), the reference harness
 *  runs it in the scenario's W1 form ({@code --w1-form} overrides it), and the oracle's cut stream is compared with the harness journals
 *  at every cut the harness reported, exactly as the core gate compares a scenario run ({@link Comparison}, {@link RunFold}). Prints one
 *  JSON line {@code {scenario, w1Form, lock, cuts, compared, rowsCompared, dispositionsCompared, differences, findings, equal}}; exit 0
 *  when the lock holds, every cut equals the oracle and the fold found nothing, 1 otherwise. */
public final class EngineCheck {
    private EngineCheck() {}

    public record Options(Path scenario, Path registry, Path trustedKeys, String python, Path out, W1Form w1Form, Path root) {}

    public static int run(Options o, PrintStream out, PrintStream err) throws IOException, InterruptedException {
        JsonNode scenario = Json.read(o.scenario());
        W1Form form = o.w1Form() != null ? o.w1Form() : W1Form.of(scenario.path("w1Form").isTextual() ? scenario.get("w1Form").textValue() : null);
        Path rec = o.out().resolve("recording"), state = o.out().resolve("state"), run = o.out().resolve("run"), oracle = o.out().resolve("oracle");
        for (Path p : List.of(state, run, oracle)) if (Files.exists(p)) CoreGate.delete(p);
        Files.createDirectories(o.out());

        // (1) the recording and its lock
        ScenarioBuilder.Built built = ScenarioBuilder.build(o.scenario(), o.registry(), rec);
        Path lockFile = ScenarioBuilder.lockOf(o.scenario());
        String lock = !Files.isRegularFile(lockFile) ? "missing" : Arrays.equals(Files.readAllBytes(lockFile), Json.pretty(built.lock())) ? "equal" : "differs";

        // (2) a generation on it, (3) the reference harness
        JsonNode rule = scenario.path("rule");
        ByteArrayOutputStream gout = new ByteArrayOutputStream(), gerr = new ByteArrayOutputStream();
        int g = GenerationCli.run(new String[] {"new", "--state", state.toString(), "--mappings", o.registry().resolve("mappings").toString(),
            "--trusted-keys", o.trustedKeys().toString(), "--stream", Json.text(rule, "stream"),
            "--rule", Json.text(rule, "attributionRuleId") + "@" + Json.text(rule, "attributionRuleVersion"), "--recording", rec.toString()},
            new PrintStream(gout, true, StandardCharsets.UTF_8), new PrintStream(gerr, true, StandardCharsets.UTF_8),
            Clock.fixed(Instant.ofEpochMilli(GateSigner.now(o.registry().resolve("mappings"), Json.text(rule, "stream"))), ZoneOffset.UTC), o.root());
        if (g != 0) throw new IllegalStateException("generation new exit " + g + ": " + gout.toString(StandardCharsets.UTF_8) + gerr.toString(StandardCharsets.UTF_8));
        GenerationCli.prepareRun(state, 1, rec);
        String rulesDigest = new FileGenerationStore(state).registered(1).orElseThrow().row().rulesDigest();
        ReferenceEngine.run(new ReferenceEngine.Options(rec, o.registry(), state, 1, run, ReferenceEngine.DEFAULT_SEED, false, Map.of(), null,
            "forseti recordings check-engine", null, null, null, form));

        // (4) the oracle's stream against the journals, cut by cut
        Compared c = compare(rec, run, scenario, o.registry(), o.python(), o.root(), oracle, o.out().resolve("oracle.log"), rulesDigest);
        Comparison.Tally tally = c.tally();
        String broken = c.broken(), findings = c.findings();
        int exit = c.exit();
        List<Long> cutIds = c.cutIds();
        boolean equal = "equal".equals(lock) && broken == null && exit == 0 && tally.equal() && tally.cuts == cutIds.size() && findings == null;
        ObjectNode n = Json.object().put("scenario", o.scenario().getParent().getFileName() + "/" + Json.text(scenario, "name")).put("w1Form", form.wire).put("lock", lock);
        Json.putSafe(n, "cuts", built.cuts());
        Json.putSafe(n, "reported", cutIds.size());
        Json.putSafe(n, "compared", tally.cuts);
        Json.putSafe(n, "rowsCompared", tally.rowsCompared);
        Json.putSafe(n, "dispositionsCompared", tally.dispositionsCompared);
        Json.putSafe(n, "differences", tally.differences);
        n.put("oracle", broken != null ? broken : exit != 0 ? "exit " + exit : "ok");
        n.put("findings", findings);
        n.put("comparison", tally.describe());
        n.put("equal", equal);
        out.println(yggdrasil.forseti.core.canon.Canonical.utf8(yggdrasil.forseti.core.canon.Canonical.bytes(n)));
        if (!equal) err.println(tally.describe());
        return equal ? 0 : 1;
    }

    /** The oracle's cut stream compared with a run's journals at every cut its {@code cuts.json} reports, as the core gate compares a
     *  scenario run ({@link Comparison}, {@link RunFold}): {@code run} holds {@code journals/}, {@code cuts.json} and {@code run.json}
     *  (its {@code subscribed}); the oracle reads {@code rec} and writes under {@code oracle}, its standard error to {@code log}. Any
     *  harness that writes those files is compared this way (the step-6 Kafka test driver included). */
    public record Compared(Comparison.Tally tally, List<Long> cutIds, String broken, int exit, String findings) {
        public boolean equal() { return broken == null && exit == 0 && tally.equal() && tally.cuts == cutIds.size() && findings == null; }
        /** The comparison's counts and description, for a harness outside this package (an engine provider). */
        public long cutsCompared() { return tally.cuts; }
        public long rowsCompared() { return tally.rowsCompared; }
        public long dispositionsCompared() { return tally.dispositionsCompared; }
        public long differences() { return tally.differences; }
        public String describe() { return tally.describe(); }
    }

    public static Compared compare(Path rec, Path run, JsonNode scenario, Path registry, String python, Path root, Path oracle, Path log, String rulesDigest)
            throws IOException, InterruptedException {
        JsonNode rule = scenario.path("rule");
        JsonNode cutLog = Json.read(run.resolve("cuts.json")), runJson = Json.read(run.resolve("run.json"));
        List<String> cmd = new ArrayList<>(List.of(python, "-I", "-m", "forseti_oracle.cli", "all", "--recording", rec.toString(),
            "--cuts", run.resolve("cuts.json").toString(), "--mappings", registry.resolve("mappings").toString(),
            "--release", Json.text(rule, "releaseId") + "@" + Json.text(rule, "version"),
            "--rule", Json.text(rule, "attributionRuleId") + "@" + Json.text(rule, "attributionRuleVersion"),
            "--definitions", rec.resolve(Recording.MANIFEST).toString(), "--out", oracle.toString(), "--stream"));
        runJson.path("subscribed").fields().forEachRemaining(e -> {
            List<String> ps = new ArrayList<>();
            e.getValue().forEach(p -> ps.add(p.asText()));
            cmd.add("--subscribe");
            cmd.add(e.getKey() + "=" + String.join(",", ps));
        });
        Files.createDirectories(oracle);
        Map<Long, JsonNode> entries = new HashMap<>();
        List<Long> cutIds = new ArrayList<>();
        for (JsonNode c : cutLog.path("cuts")) { entries.put(c.path("cutId").asLong(), c); cutIds.add(c.path("cutId").asLong()); }
        Comparison.Tally tally = new Comparison.Tally();
        Comparison.OracleFold of = new Comparison.OracleFold();
        List<String> trailing = new ArrayList<>();
        boolean synthetic = scenario.path("synthetic").asBoolean(false);
        Process p = new ProcessBuilder(cmd).directory(root.toFile()).redirectError(log.toFile()).start();
        String broken;
        String findings;
        int exit;
        try (RunFold f = new RunFold(run, rulesDigest, 1, !synthetic)) {
            try (BufferedReader announced = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
                 Writer acks = new OutputStreamWriter(p.getOutputStream(), StandardCharsets.UTF_8)) {
                broken = Comparison.consume(announced, acks, oracle, cutIds, trailing, (k, dir) -> {
                    f.advance(k);
                    Comparison.compareCut(f, k, dir, of, Json.text(scenario, "name"), entries.get(k), tally);
                });
            }
            exit = p.waitFor();
            f.finish();
            findings = f.findings.clean() ? null : f.findings.describe();
        } finally {
            if (p.isAlive()) p.destroyForcibly();
        }
        return new Compared(tally, List.copyOf(cutIds), broken, exit, findings);
    }
}
