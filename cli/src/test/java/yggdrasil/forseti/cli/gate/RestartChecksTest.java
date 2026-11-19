package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.replay.ReplayEngine;
import yggdrasil.forseti.cli.replay.Restarts;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The restart assertions of spec 719-726 (but 724) on the reference harness's run of the committed {@code smoke} recording (a run with
 *  no restart: every check holds), and on copies of its journals edited the way a broken engine or a broken restart would leave them:
 *  each edit is found by the assertion it breaks. The restart placement of {@link Restarts#place} on the same recording. */
class RestartChecksTest {
    static final Path ROOT = Path.of("..").toAbsolutePath().normalize(), FIXTURES = ROOT.resolve("fixtures"), SMOKE = FIXTURES.resolve("recordings/smoke"),
        REGISTRY = FIXTURES.resolve("registry");
    static final String WR = "forseti.work-records", SAMPLES = "forseti.samples", L1 = "forseti.dispositions-stage1";
    static final String S2 = "synthetic-site:line2:station2";
    @TempDir static Path shared;
    @TempDir Path tmp;
    static Path base;
    static long declaration, end;
    static String restore;

    @BeforeAll static void run() throws IOException {
        Path state = Files.createDirectories(shared.resolve("state"));
        JsonNode rule = Json.read(SMOKE.resolve("manifest.json")).get("rule");
        String stream = rule.get("stream").asText();
        Path m = REGISTRY.resolve("mappings");
        JsonNode statement = Json.read(Base64.getDecoder().decode(Json.read(m.resolve("head-attestations/" + stream + ".dsse.json")).get("payload").asText()));
        Clock clock = Clock.fixed(Instant.parse(statement.path("predicate").path("signedAt").asText()).plusSeconds(60), ZoneOffset.UTC);
        ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
        int exit = GenerationCli.run(new String[] {"new", "--state", state.toString(), "--mappings", m.toString(), "--trusted-keys", FIXTURES.resolve("trusted-keys.jsonl").toString(),
            "--stream", stream, "--rule", rule.get("attributionRuleId").asText() + "@" + rule.get("attributionRuleVersion").asText(), "--recording", SMOKE.toString()},
            new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8), clock);
        assertEquals(0, exit, o.toString(StandardCharsets.UTF_8) + e.toString(StandardCharsets.UTF_8));
        GenerationCli.prepareRun(state, 1, SMOKE);
        base = shared.resolve("base");
        ReferenceEngine.run(new ReferenceEngine.Options(SMOKE, REGISTRY, state, 1, base, ReferenceEngine.DEFAULT_SEED, false, Map.of(), "ab".repeat(32), "test"));
        // the barrier epochs as a harness of epoch 0 writes them
        ArrayNode cuts = Json.JSON.createArrayNode();
        for (Recording.Batch b : Recording.open(SMOKE).batches()) { ObjectNode c = Json.putSafe(Json.object(), "cutId", b.cutId()); c.put("restartEpoch", 0).putNull("recoveryRef"); cuts.add(c); }
        Json.write(base.resolve("cut-epochs.json"), Json.object().<ObjectNode>set("cuts", cuts));
        JsonNode first = progress(base, WR, 0).get(0).get("initEvidence");
        declaration = first.path("declarationOffset").asLong();
        end = first.path("endOffset").asLong();
        restore = first.path("restore").asText();
    }

    static List<JsonNode> lines(Path run, String topic, int p) throws IOException {
        List<JsonNode> l = new ArrayList<>();
        for (String s : Files.readAllLines(run.resolve("journals").resolve(topic).resolve(p + ".jsonl"), StandardCharsets.UTF_8)) if (!s.isEmpty()) l.add(Json.read(s.getBytes(StandardCharsets.UTF_8)));
        return l;
    }

    static List<JsonNode> progress(Path run, String topic, int p) throws IOException {
        List<JsonNode> l = new ArrayList<>();
        for (JsonNode n : lines(run, topic, p)) if ("progress".equals(n.path("value").path("type").asText())) l.add(n.get("value"));
        return l;
    }

    static void write(Path run, String topic, int p, List<JsonNode> lines) throws IOException {
        StringBuilder b = new StringBuilder();
        for (JsonNode n : lines) b.append(Canonical.utf8(Canonical.bytes(n))).append('\n');
        Files.writeString(run.resolve("journals").resolve(topic).resolve(p + ".jsonl"), b.toString(), StandardCharsets.UTF_8);
    }

    Path copy() throws IOException {
        Path d = tmp.resolve("run-" + UUID.randomUUID());
        try (var w = Files.walk(base)) {
            for (Path s : w.toList()) {
                Path t = d.resolve(base.relativize(s).toString());
                if (Files.isDirectory(s)) Files.createDirectories(t); else Files.copy(s, t);
            }
        }
        return d;
    }

    static Map<String, List<String>> check(Path run) throws IOException {
        RestartChecks.Inputs in = new RestartChecks.Inputs(SMOKE, run, Json.read(run.resolve("cuts.json")), Json.read(run.resolve("run.json")), declaration, end, restore);
        return RestartChecks.findings(in, MEASURES, Json.object());
    }

    /** Integrity's measures of a run every input of which has a disposition. */
    static final Integrity.Measures MEASURES = new Integrity.Measures(0, null, 0, 0, 0);

    /** Edits the progress of cut {@code cut} on {@code topic/p} with {@code edit}; a null result drops the progress line. */
    static void editProgress(Path run, String topic, int p, long cut, java.util.function.UnaryOperator<ObjectNode> edit) throws IOException {
        List<JsonNode> out = new ArrayList<>();
        for (JsonNode n : lines(run, topic, p)) {
            JsonNode v = n.path("value");
            if ("progress".equals(v.path("type").asText()) && v.path("cutId").asLong() == cut) {
                ObjectNode x = edit.apply(((ObjectNode) n).deepCopy());
                if (x != null) out.add(x);
            } else out.add(n);
        }
        write(run, topic, p, out);
    }

    static ObjectNode merge(ObjectNode n, long... cuts) { ArrayNode a = ((ObjectNode) n.get("value")).putArray("mergedCutIds"); for (long c : cuts) a.add(c); return n; }

    @Test void anUninterruptedRunHoldsEveryRestartAssertionAndItsCutsAreAllComplete() throws IOException {
        ObjectNode acc = Json.object();
        RestartChecks.Inputs in = new RestartChecks.Inputs(SMOKE, base, Json.read(base.resolve("cuts.json")), Json.read(base.resolve("run.json")), declaration, end, restore);
        Map<String, List<String>> f = RestartChecks.findings(in, null, acc);
        f.forEach((id, l) -> assertEquals(List.of(), l, id));
        assertEquals(Recording.open(SMOKE).batches().size(), acc.path("completedCutIds").size(), acc.toString());
        assertEquals(0, acc.path("item3").asLong());
    }

    @Test void aMergedCutWithoutARestartWindowIsUnjustifiedAndTheCutLogDisagrees() throws IOException {
        Path run = copy();
        int p = 1;   // station2
        for (String t : List.of(WR, SAMPLES, L1)) { editProgress(run, t, p, 5, n -> null); editProgress(run, t, p, 6, n -> merge(n, 5)); }
        Map<String, List<String>> f = check(run);
        assertEquals(List.of(), f.get("R-MERGED-EXACT"), "the merged cut is exactly the one between");
        assertFalse(f.get("R-MERGED-JUSTIFIED").isEmpty(), "no restart window and no starvation: unjustified");
        assertTrue(f.get("R-MERGED-JUSTIFIED").stream().anyMatch(x -> x.contains("merges cut 5")), f.toString());
        assertTrue(f.get("C-3-RESTART-SKIPS").stream().anyMatch(x -> x.contains("cut 5 " + S2) && x.contains("listed in cuts.json although merged")), f.get("C-3-RESTART-SKIPS").toString());
    }

    @Test void mergedCutIdsThatAreNotTheCutsBetweenTwoProgressesAreFound() throws IOException {
        Path run = copy();
        editProgress(run, WR, 1, 6, n -> merge(n, 5));
        Map<String, List<String>> f = check(run);
        assertTrue(f.get("R-MERGED-EXACT").stream().anyMatch(x -> x.contains("progress 6") && x.contains("mergedCutIds [5]")), f.get("R-MERGED-EXACT").toString());
        assertTrue(f.get("R-BARRIER-REFS").stream().anyMatch(x -> x.contains("work-records and samples progress differ")), "both evidence topics carry the same mergedCutIds");
    }

    @Test void aBarrierReferenceTheHarnessNeverWroteIsFound() throws IOException {
        Path run = copy();
        editProgress(run, WR, 0, 3, n -> {
            ObjectNode r = (ObjectNode) n.get("value").get("barrierRefs").get(0);
            r.put("throughOffset", r.path("throughOffset").asLong() + 1);
            return n;
        });
        assertTrue(check(run).get("R-BARRIER-REFS").stream().anyMatch(x -> x.contains("progress 3") && x.contains("the harness wrote")), "a wrong through-offset");
    }

    @Test void initEvidenceOnALaterProgressOrADeclarationNoInjectionWroteIsFound() throws IOException {
        Path run = copy();
        JsonNode ie = progress(base, WR, 0).get(0).get("initEvidence");
        editProgress(run, WR, 0, 4, n -> { ((ObjectNode) n.get("value")).set("initEvidence", ie); return n; });
        assertTrue(check(run).get("R-INIT-EVIDENCE").stream().anyMatch(x -> x.contains("progress 4") && x.contains("which no restart injection of the run wrote")));
        Path run2 = copy();
        editProgress(run2, "forseti.affected", 0, 2, n -> { ((ObjectNode) n.get("value")).set("initEvidence", ie); return n; });
        assertTrue(check(run2).get("R-INIT-EVIDENCE").stream().anyMatch(x -> x.contains("a stage-2 progress carries initEvidence")));
    }

    @Test void aProgressWhoseEpochIsNotItsBarriersIsFound() throws IOException {
        Path run = copy();
        editProgress(run, L1, 2, 7, n -> { ((ObjectNode) n.get("value")).put("restartEpoch", 1).put("recoveryRef", "injection-21"); return n; });
        assertTrue(check(run).get("R-RECOVERED-EPOCH").stream().anyMatch(x -> x.contains("progress 7") && x.contains("its barriers' 0/null")), check(run).get("R-RECOVERED-EPOCH").toString());
    }

    /** A restart run: station2's writer recovers from a kill after cut 5 with cut 6 merging nothing (it had committed 5) and the reject
     *  collector merging 5; the injection manifest of the harness. Consistent, every assertion holds; a progress appended after the kill
     *  for a cut below the injection's first cut, a recovered progress without initEvidence, an attributed set the initEvidence does not
     *  match, are each found. */
    @Test void aRestartIsCheckedAgainstTheHarnessRecords() throws Exception {
        Path run = copy();
        long k = 5;
        String id = "injection-21";
        long declOffset = 1;
        Recording rec = Recording.open(SMOKE);
        List<String> writers = Restarts.stage1Writers(rec.routing().stations().keySet());
        // the epochs: barriers after k at epoch 1, recoveryRef id
        ArrayNode cuts = Json.JSON.createArrayNode();
        for (Recording.Batch b : rec.batches()) {
            ObjectNode c = Json.putSafe(Json.object(), "cutId", b.cutId());
            if (b.cutId() <= k) c.put("restartEpoch", 0).putNull("recoveryRef"); else c.put("restartEpoch", 1).put("recoveryRef", id);
            cuts.add(c);
        }
        Json.write(run.resolve("cut-epochs.json"), Json.object().<ObjectNode>set("cuts", cuts));
        String restored = "{\"restored\":true}";
        ObjectNode ie = Json.object();
        Json.putSafe(ie, "declarationOffset", declOffset);
        Json.putSafe(ie, "endOffset", declOffset + 1);
        ie.put("restore", restored);
        // every stage-1 progress after k at epoch 1; each writer's first after k carries the restart's initEvidence; the reject collector
        // merges cut 5 into 6 (it had not committed 5 at the kill)
        Map<String, List<Integer>> parts = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> s : rec.routing().stations().entrySet()) parts.put(s.getKey(), List.of(s.getValue()));
        for (String t : List.of(WR, SAMPLES, L1)) for (int p : List.of(0, 1, 2, 6)) {
            if (t.equals(WR) && p == 6 || t.equals(SAMPLES) && p == 6) continue;
            List<JsonNode> out = new ArrayList<>();
            boolean first = true;
            for (JsonNode n : lines(run, t, p)) {
                ObjectNode v = (ObjectNode) n.get("value");
                if ("progress".equals(v.path("type").asText())) {
                    long c = v.path("cutId").asLong();
                    if (p == 6 && c == k) continue;   // the collector's progress 5 never committed
                    if (c > k) {
                        v.put("restartEpoch", 1).put("recoveryRef", id);
                        if (first) { v.set("initEvidence", ie); first = false; if (p == 6) merge((ObjectNode) n, k); }
                    }
                }
                out.add(n);
            }
            write(run, t, p, out);
        }
        // the harness's records: the high watermark at the kill above every progress through k, the progress log with offsets
        ObjectNode manifest = Json.object().put("kind", "21").put("status", "valid");
        ObjectNode inj = manifest.putArray("injections").addObject().put("injectionId", id).put("stage1Restart", true);
        Json.putSafe(inj, "lastWrittenCutId", k);
        Json.putSafe(inj, "firstCutAtTarget", k + 1);
        inj.put("epochBefore", 0).put("targetEpoch", 1);
        Json.putSafe(inj.putObject("declaration"), "offset", declOffset);
        inj.putObject("launches").putObject("stage1").put("restore", restored);
        ArrayNode ex = inj.putArray("expected"), at = inj.putArray("attributed"), iw = inj.putArray("initEvidenceWriters");
        for (String w : writers) { ex.add(w); at.add(w); iw.add(w); }
        ObjectNode hw = inj.putObject("atKill").putObject("highWatermark"), last = ((ObjectNode) inj.get("atKill")).putObject("lastProgressCutId");
        ObjectNode log = manifest.putObject("progress");
        for (String w : writers) {
            List<String> ps = w.equals(Restarts.REJECTS) ? List.of(L1 + "/6") : List.of(WR + "/" + parts.get(w).get(0), SAMPLES + "/" + parts.get(w).get(0), L1 + "/" + parts.get(w).get(0));
            Json.putSafe(last, w, w.equals(Restarts.REJECTS) ? k - 1 : k);
            for (String part : ps) {
                String[] tp = part.split("/");
                ArrayNode a = log.putArray(part);
                long offset = 0, kill = -1;
                for (JsonNode v : progress(run, tp[0], Integer.parseInt(tp[1]))) {
                    if (v.path("cutId").asLong() > k && kill < 0) { kill = offset; offset += 10; }
                    ObjectNode x = Json.putSafe(Json.object(), "offset", offset++);
                    Json.putSafe(x, "cutId", v.path("cutId").asLong());
                    x.set("initEvidence", v.has("initEvidence") ? v.get("initEvidence") : null);
                    a.add(x);
                }
                Json.putSafe(hw, part, kill);
            }
        }
        Json.write(run.resolve(ReplayEngine.INJECTIONS), manifest);
        // the run's own declaration is the first start's: the first progress keeps its initEvidence
        Map<String, List<String>> f = check(run);
        f.forEach((x, l) -> assertEquals(List.of(), l, x));
        ObjectNode acc = Json.object();
        RestartChecks.findings(new RestartChecks.Inputs(SMOKE, run, Json.read(run.resolve("cuts.json")), Json.read(run.resolve("run.json")), declaration, end, restore), MEASURES, acc);
        assertEquals(List.of("stage1-rejects@5"), RestartAssertions.texts(acc.path("rejectRestartSkips")), acc.toString());

        // a recovered progress without initEvidence
        Path a = copyOf(run);
        editProgress(a, WR, 1, k + 1, n -> { ((ObjectNode) n.get("value")).remove("initEvidence"); return n; });
        Map<String, List<String>> fa = check(a);
        assertTrue(fa.get("R-INIT-EVIDENCE").stream().anyMatch(x -> x.contains("no initEvidence after the restart")), fa.toString());
        assertTrue(fa.get("R-RECOVERY-WINDOW").stream().anyMatch(x -> x.contains("progress log")), "the harness's log no longer equals the journal");

        // the attributed set without a writer whose initEvidence points to the declaration
        Path b = copyOf(run);
        ObjectNode mb = (ObjectNode) Json.read(b.resolve(ReplayEngine.INJECTIONS));
        ((ArrayNode) mb.get("injections").get(0).get("attributed")).remove(0);
        Json.write(b.resolve(ReplayEngine.INJECTIONS), mb);
        assertTrue(check(b).get("R-INIT-EVIDENCE").stream().anyMatch(x -> x.contains("attributed set")), check(b).toString());

        // a progress appended after the kill for a cut below the first cut at the epoch
        Path c = copyOf(run);
        ObjectNode mc = (ObjectNode) Json.read(c.resolve(ReplayEngine.INJECTIONS));
        Json.putSafe((ObjectNode) mc.get("injections").get(0).get("atKill").get("highWatermark"), WR + "/0", 0);
        Json.write(c.resolve(ReplayEngine.INJECTIONS), mc);
        assertTrue(check(c).get("R-RECOVERY-WINDOW").stream().anyMatch(x -> x.contains("below the first cut " + (k + 1))), check(c).toString());
    }

    Path copyOf(Path run) throws IOException {
        Path d = tmp.resolve("run-" + UUID.randomUUID());
        try (var w = Files.walk(run)) {
            for (Path s : w.toList()) {
                Path t = d.resolve(run.relativize(s).toString());
                if (Files.isDirectory(s)) Files.createDirectories(t); else Files.copy(s, t);
            }
        }
        return d;
    }

    @Test void aRestartIsPlacedWhereNoBracketIsAndTheNextBatchRaisesEveryPartition() {
        Recording rec = Recording.open(SMOKE);
        List<Integer> telemetry = rec.routing().subscribed("ot.messages");
        long k = Restarts.place(rec, telemetry);
        List<Long> ids = new ArrayList<>();
        for (Recording.Batch b : rec.batches()) ids.add(b.cutId());
        assertTrue(k >= 3 && k < ids.get(ids.size() - 1), "k " + k);
        for (Recording.Deadline d : rec.deadlines()) {
            assertNotEquals(d.cutA() - 1, k, d.toString());
            assertNotEquals(d.cutA(), k, d.toString());
            assertNotEquals(d.cutB(), k, d.toString());
        }
        assertTrue(k >= ids.get(ids.size() / 2), "the first valid cut from the middle of the recording: " + k);
    }
}
