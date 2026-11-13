package yggdrasil.forseti.cli.generation;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.gate.GateBundles;
import yggdrasil.forseti.cli.gate.GateBundles.Aggregate;
import yggdrasil.forseti.cli.gate.GateBundles.Bundle;
import yggdrasil.forseti.cli.gate.GateBundles.Options;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The reader of the gate report {@code generation promote} is given (plan Step 21.1), on complete full and aggregate bundles
 *  ({@link GateBundles}) and one change at a time. The manifest size is read from the committed file; the positive bundles list every
 *  committed manifest id and are validation fixtures, not delivery evidence. */
class GateReportReaderTest {
    static final Path ROOT = GateBundles.ROOT;
    /** The full bundle's target: a scenario with no shuffled runs. */
    static final String TARGET = "smoke";
    @TempDir Path tmp;

    // --- requests ------------------------------------------------------------------------------------------------------------------

    Bundle full() throws IOException { return GateBundles.full(tmp.resolve("full-" + UUID.randomUUID()), Options.DEFAULT); }
    Aggregate aggregate() throws IOException { return aggregate(Options.DEFAULT); }
    Aggregate aggregate(Options o) throws IOException { return GateBundles.aggregate(tmp.resolve("agg-" + UUID.randomUUID()), o); }

    static String aggTarget() { return GateBundles.first(GateBundles.handMadeScenarios()); }

    /** The request that promotes the full bundle's {@code scenario} run on its own store. */
    static GateReportReader.Request request(Bundle b, String scenario) {
        JsonNode r = b.reportJson();
        ObjectNode g = GateBundles.generationEntry(r, null, scenario), e = GateBundles.runEntry(r, null, scenario);
        return new GateReportReader.Request(ROOT, b.report(), null, b.state(), g.get("generation").longValue(), g.get("registeredRowSha256").asText(),
            b.file(e.get("runRecord").asText()), true);
    }

    static GateReportReader.Request request(Aggregate a, String scenario) {
        JsonNode r = a.reportJson();
        ObjectNode g = GateBundles.generationEntry(r, GateBundles.HAND_MADE, scenario), e = GateBundles.runEntry(r, GateBundles.HAND_MADE, scenario);
        Bundle hm = a.shard(GateBundles.HAND_MADE);
        return new GateReportReader.Request(ROOT, a.report(), a.shards(), hm.state(), g.get("generation").longValue(), g.get("registeredRowSha256").asText(),
            hm.file(e.get("runRecord").asText()), true);
    }

    static GateReportReader.Request with(GateReportReader.Request q, Path root, Path report, Path shards, Path state, long n, String digest, Path run, boolean core) {
        return new GateReportReader.Request(root, report, shards, state, n, digest, run, core);
    }

    static GateReportReader.Accepted read(GateReportReader.Request q) { return GateReportReader.read(q, new FileGenerationStore(q.state())); }

    static ReferenceEngine.Refused refused(String code, GateReportReader.Request q) {
        ReferenceEngine.Refused r = assertThrows(ReferenceEngine.Refused.class, () -> read(q), "expected " + code);
        assertEquals(code, r.code, r.toJson().toString());
        return r;
    }

    static void refusedIn(String code, GateReportReader.Request q, String fragment) {
        ReferenceEngine.Refused r = refused(code, q);
        assertTrue(r.toJson().toString().contains(fragment), "evidence names " + fragment + ": " + r.toJson());
    }

    /** Edits the report file (its digest changes; the reader hashes what it reads). */
    static void edit(Path report, java.util.function.Consumer<ObjectNode> change) throws IOException { GateBundles.rewrite(report, change); }

    static ArrayNode results(ObjectNode r) { return (ArrayNode) r.get("results"); }

    // --- accepted ------------------------------------------------------------------------------------------------------------------

    @Test void aValidFullReportIsAccepted() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        GateReportReader.Accepted a = read(q);
        assertEquals(Canonical.digestOf(Files.readAllBytes(b.report())), a.gateReportSha256());
        JsonNode rr = Json.read(q.run());
        assertEquals(List.of("runId", "runRecordSha256", "offsetRanges"), fields(a.run()));
        assertEquals(rr.get("runId"), a.run().get("runId"));
        assertEquals(Canonical.digestOf(Files.readAllBytes(q.run())), a.run().get("runRecordSha256").asText());
        assertEquals(rr.get("offsetRanges"), a.run().get("offsetRanges"));
        assertEquals(TARGET, a.generation().get("scenario").asText());
        assertEquals(TARGET, a.runEntry().get("scenario").asText());
    }

    @Test void aValidAggregateReportIsAccepted() throws IOException {
        Aggregate agg = aggregate();
        GateReportReader.Request q = request(agg, aggTarget());
        GateReportReader.Accepted a = read(q);
        assertEquals(Canonical.digestOf(Files.readAllBytes(agg.report())), a.gateReportSha256(), "the aggregate report's own digest");
        assertEquals(GateBundles.HAND_MADE, a.generation().get("shard").asText());
        assertEquals("shared", a.generation().get("state").asText());
    }

    @Test void theManifestAndPlanAreReadFromTheRepositoryNotFromTheReport() throws IOException {
        Bundle b = full();
        edit(b.report(), r -> r.put("manifest", "elsewhere/core-assertions.json").put("shardPlan", "elsewhere/core-shards.json"));
        read(request(b, TARGET));   // fields that point elsewhere are not read
        // a repository whose committed manifest is another one refuses the same report
        Path other = copyRoot(tmp.resolve("other-root"));
        GateBundles.rewrite(other.resolve("gate/core-assertions.json"), m -> ((ArrayNode) m.get("assertions")).remove(0));
        GateReportReader.Request q = request(b, TARGET);
        refused(GateReportReader.MANIFEST, with(q, other, q.report(), q.shards(), q.state(), q.generation(), q.registeredRowSha256(), q.run(), true));
        // and one whose committed plan is another one refuses an aggregate report
        Aggregate agg = aggregate();
        Path plan = copyRoot(tmp.resolve("plan-root"));
        Files.writeString(plan.resolve("gate/core-shards.json"), Files.readString(plan.resolve("gate/core-shards.json"), StandardCharsets.UTF_8) + "\n", StandardCharsets.UTF_8);
        GateReportReader.Request qa = request(agg, aggTarget());
        refusedIn(GateReportReader.LAYOUT, with(qa, plan, qa.report(), qa.shards(), qa.state(), qa.generation(), qa.registeredRowSha256(), qa.run(), true), "shardPlanSha256");
    }

    @Test void aReportThatIsNotStrictJsonIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        String text = Files.readString(b.report(), StandardCharsets.UTF_8);
        Files.writeString(b.report(), text.replaceFirst("\\{", "{\"exit\": 0, "), StandardCharsets.UTF_8);   // a duplicate key
        refused(GateReportReader.UNREADABLE, q);
        Files.writeString(b.report(), text + "}", StandardCharsets.UTF_8);
        refused(GateReportReader.UNREADABLE, q);
        Files.writeString(b.report(), text, StandardCharsets.UTF_8);
        edit(b.report(), r -> r.remove("runs"));
        refusedIn(GateReportReader.UNREADABLE, q, "report.runs is missing");
        Files.writeString(b.report(), text, StandardCharsets.UTF_8);
        edit(b.report(), r -> r.put("exit", "0"));
        refusedIn(GateReportReader.UNREADABLE, q, "report.exit is not integer");
        Files.writeString(b.report(), text, StandardCharsets.UTF_8);
        edit(b.report(), r -> ((ObjectNode) results(r).get(0)).remove("ok"));
        refusedIn(GateReportReader.UNREADABLE, q, "results[0].ok is missing");
    }

    @Test void aNegativeControlReportIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        byte[] good = Files.readAllBytes(b.report());
        edit(b.report(), r -> r.put("kind", "negative-control"));
        refused(GateReportReader.NEGATIVE_CONTROL, q);
        Files.write(b.report(), good);
        edit(b.report(), r -> r.put("negativeControl", "processing-time"));
        refused(GateReportReader.NEGATIVE_CONTROL, q);
        Files.write(b.report(), good);
        edit(b.report(), r -> ((ArrayNode) r.get("intendedFailures")).add("I-1"));
        refused(GateReportReader.NEGATIVE_CONTROL, q);
    }

    @Test void aShardReportIsRefused() throws IOException {
        Aggregate agg = aggregate();
        GateReportReader.Request q = request(agg, aggTarget());
        Bundle hm = agg.shard(GateBundles.HAND_MADE);
        refused(GateReportReader.SHARD, with(q, ROOT, hm.report(), null, hm.state(), q.generation(), q.registeredRowSha256(), q.run(), true));
        Bundle b = full();
        edit(b.report(), r -> r.put("shard", "hand-made"));
        refused(GateReportReader.SHARD, request(b, TARGET));
    }

    @Test void aFailedOrErroredReportIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        byte[] good = Files.readAllBytes(b.report());
        List<java.util.function.Consumer<ObjectNode>> changes = List.of(
            r -> Json.putSafe(r, "exit", 1),
            r -> r.put("error", "java.lang.IllegalStateException: coverage"),
            r -> ((ArrayNode) r.get("failed")).add("I-1"),
            r -> ((ArrayNode) r.get("notRun")).add("I-1"),
            r -> ((ObjectNode) results(r).get(3)).put("ok", false));
        for (var c : changes) {
            Files.write(b.report(), good);
            edit(b.report(), c);
            refused(GateReportReader.FAILED, q);
        }
    }

    @Test void anotherManifestDigestIsRefused() throws IOException {
        Bundle b = full();
        edit(b.report(), r -> r.put("manifestSha256", "0".repeat(64)));
        refused(GateReportReader.MANIFEST, request(b, TARGET));
    }

    @Test void zeroOfZeroIsRefused() throws IOException {
        Bundle b = full();
        edit(b.report(), r -> { results(r).removeAll(); Json.putSafe(r, "assertionsExpected", 0); Json.putSafe(r, "assertionsRun", 0); });
        refused(GateReportReader.ASSERTIONS, request(b, TARGET));
    }

    @Test void reducedEqualCountsAreRefused() throws IOException {
        Bundle b = full();
        int n = manifestSize();
        edit(b.report(), r -> {
            for (int i = 0; i < 5; i++) results(r).remove(results(r).size() - 1);
            Json.putSafe(r, "assertionsExpected", n - 5);
            Json.putSafe(r, "assertionsRun", n - 5);
        });
        refused(GateReportReader.ASSERTIONS, request(b, TARGET));
    }

    @Test void aCountAboveTheManifestIsRefused() throws IOException {
        Bundle b = full();
        int n = manifestSize();
        GateReportReader.Request q = request(b, TARGET);
        byte[] good = Files.readAllBytes(b.report());
        edit(b.report(), r -> { Json.putSafe(r, "assertionsExpected", n + 1); Json.putSafe(r, "assertionsRun", n + 1); });
        refused(GateReportReader.ASSERTIONS, q);
        Files.write(b.report(), good);
        edit(b.report(), r -> {
            results(r).addObject().put("id", "X-EXTRA").put("ok", true).put("evidence", "stand-in").putNull("shard");
            Json.putSafe(r, "assertionsExpected", n + 1);
            Json.putSafe(r, "assertionsRun", n + 1);
        });
        refusedIn(GateReportReader.ASSERTIONS, q, "X-EXTRA");
    }

    @Test void aRepeatedIdInPlaceOfAnotherIsRefused() throws IOException {
        Bundle b = full();
        edit(b.report(), r -> ((ObjectNode) results(r).get(results(r).size() - 1)).put("id", results(r).get(0).get("id").asText()));
        refusedIn(GateReportReader.ASSERTIONS, request(b, TARGET), "recorded twice");
    }

    @Test void anotherShardPlanDigestIsRefused() throws IOException {
        Aggregate agg = aggregate();
        edit(agg.report(), r -> r.put("shardPlanSha256", "0".repeat(64)));
        refusedIn(GateReportReader.LAYOUT, request(agg, aggTarget()), "shardPlanSha256");
    }

    @Test void aMissingDuplicateOrUnplannedShardIsRefused() throws IOException {
        Aggregate agg = aggregate();
        GateReportReader.Request q = request(agg, aggTarget());
        byte[] good = Files.readAllBytes(agg.report());
        edit(agg.report(), r -> ((ArrayNode) r.get("shards")).remove(2));
        refused(GateReportReader.LAYOUT, q);
        Files.write(agg.report(), good);
        edit(agg.report(), r -> ((ArrayNode) r.get("shards")).add(r.get("shards").get(1).deepCopy()));
        refused(GateReportReader.LAYOUT, q);
        Files.write(agg.report(), good);
        edit(agg.report(), r -> { ObjectNode x = r.get("shards").get(1).deepCopy(); x.put("name", "unplanned"); ((ArrayNode) r.get("shards")).add(x); });
        refusedIn(GateReportReader.LAYOUT, q, "unplanned");
    }

    @Test void aResultUnderTheWrongOwnerIsRefused() throws IOException {
        Aggregate agg = aggregate();
        edit(agg.report(), r -> { for (JsonNode x : results(r)) if (x.get("id").asText().equals("I-6")) ((ObjectNode) x).put("shard", GateBundles.HAND_MADE); });
        refusedIn(GateReportReader.LAYOUT, request(agg, aggTarget()), "I-6");
    }

    @Test void aFullReportWithShardsIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        byte[] good = Files.readAllBytes(b.report());
        edit(b.report(), r -> ((ArrayNode) r.get("shards")).addObject().put("name", "hand-made").put("exit", 0).put("reportSha256", "a".repeat(64)).put("summariesSha256", "b".repeat(64)));
        refused(GateReportReader.LAYOUT, q);
        Files.write(b.report(), good);
        refusedIn(GateReportReader.EVIDENCE, with(q, ROOT, q.report(), tmp, q.state(), q.generation(), q.registeredRowSha256(), q.run(), true), "--shards");
    }

    @Test void aChangedShardReportOrSummariesIsRefused() throws IOException {
        Aggregate agg = aggregate();
        GateReportReader.Request q = request(agg, aggTarget());
        Bundle other = agg.shard("horizon-reopened");
        byte[] report = Files.readAllBytes(other.report()), summaries = Files.readAllBytes(other.summaries());
        edit(other.report(), r -> r.put("startedAt", "changed"));
        refusedIn(GateReportReader.EVIDENCE, q, "report.json");
        Files.write(other.report(), report);
        Files.write(other.summaries(), (new String(summaries, StandardCharsets.UTF_8) + "\n").getBytes(StandardCharsets.UTF_8));
        refusedIn(GateReportReader.EVIDENCE, q, "summaries.json");
        Files.write(other.summaries(), summaries);
        read(q);
    }

    @Test void aTargetPresentOnlyInTheAggregateIsRefused() throws IOException {
        Aggregate agg = aggregate();
        GateReportReader.Request q = request(agg, aggTarget());
        ObjectNode r = agg.reportJson();
        ObjectNode target = GateBundles.generationEntry(r, GateBundles.HAND_MADE, aggTarget());
        // the target and its run listed only in the aggregate report: removed from the shard's own report (rebound, so the bundle
        // digests still match the aggregate's shards[])
        Bundle hm = agg.shard(GateBundles.HAND_MADE);
        GateBundles.rewrite(hm.report(), s -> {
            ArrayNode gens = (ArrayNode) s.get("generations");
            for (int i = 0; i < gens.size(); i++) if (gens.get(i).equals(target)) { gens.remove(i); break; }
        });
        GateBundles.rebind(agg, GateBundles.HAND_MADE);
        refused(GateReportReader.EVIDENCE, q);
    }

    @Test void anEvidencePathOutsideTheBundleIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        byte[] good = Files.readAllBytes(b.report());
        String abs = b.file("runs/baseline.cuts.json").toAbsolutePath().toString().replace('\\', '/');
        edit(b.report(), r -> GateBundles.runEntry(r, null, "baseline").put("cutLog", abs));
        refusedIn(GateReportReader.EVIDENCE, q, "cutLog");
        Files.write(b.report(), good);
        edit(b.report(), r -> GateBundles.runEntry(r, null, "baseline").put("runRecord", "runs/../../outside/baseline.run.json"));
        refusedIn(GateReportReader.EVIDENCE, q, "runRecord");
        Files.write(b.report(), good);
        edit(b.report(), r -> GateBundles.runEntry(r, null, "baseline").put("journals", "../journals/baseline/online").put("journalsIndexSha256", "a".repeat(64)));
        refusedIn(GateReportReader.EVIDENCE, q, "journals");
        assertTrue(GateReportReader.relativeInside(b.dir(), "runs/x.run.json"));
        assertFalse(GateReportReader.relativeInside(b.dir(), "/runs/x"));
        assertFalse(GateReportReader.relativeInside(b.dir(), "C:/runs/x"));
        assertFalse(GateReportReader.relativeInside(b.dir(), "runs\\x"));
    }

    @Test void anotherRegisteredRowDigestIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        String baseline = GateBundles.generationEntry(b.reportJson(), null, "baseline").get("registeredRowSha256").asText();
        refused(GateReportReader.GENERATION, with(q, ROOT, q.report(), null, q.state(), q.generation(), baseline, q.run(), true));
        refused(GateReportReader.GENERATION, with(q, ROOT, q.report(), null, q.state(), q.generation(), "e".repeat(64), q.run(), true));
        refused(GateReportReader.GENERATION, with(q, ROOT, q.report(), null, q.state(), 1, q.registeredRowSha256(), q.run(), true));   // another number
    }

    @Test void aStateThatIsNotTheShardsOwnStoreIsRefused() throws IOException {
        Aggregate agg = aggregate();
        GateReportReader.Request q = request(agg, aggTarget());
        Path copy = copyTree(q.state(), tmp.resolve("copy-state"));
        refused(GateReportReader.GENERATION, with(q, ROOT, q.report(), q.shards(), copy, q.generation(), q.registeredRowSha256(), q.run(), true));
        Path otherShard = Files.createDirectories(agg.shards().resolve(GateBundles.DECOY_SHARD).resolve("state"));
        copyTree(q.state(), otherShard);
        refused(GateReportReader.GENERATION, with(q, ROOT, q.report(), q.shards(), otherShard, q.generation(), q.registeredRowSha256(), q.run(), true));
    }

    @Test void anIsolatedGenerationIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        edit(b.report(), r -> {
            GateBundles.generationEntry(r, null, TARGET).put("state", "isolated");
            GateBundles.runEntry(r, null, TARGET).put("state", "isolated");
        });
        refused(GateReportReader.ISOLATED, q);
    }

    @Test void anotherRunRecordDigestIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        Path changed = tmp.resolve("changed.run.json");
        Files.write(changed, (Files.readString(q.run(), StandardCharsets.UTF_8) + "\n").getBytes(StandardCharsets.UTF_8));
        refused(GateReportReader.RUN, with(q, ROOT, q.report(), null, q.state(), q.generation(), q.registeredRowSha256(), changed, true));
    }

    @Test void aRunUnderAnotherShardOrRegistrationIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        Path baseline = b.file(GateBundles.runEntry(b.reportJson(), null, "baseline").get("runRecord").asText());
        refused(GateReportReader.RUN, with(q, ROOT, q.report(), null, q.state(), q.generation(), q.registeredRowSha256(), baseline, true));
        Aggregate agg = aggregate();
        GateReportReader.Request qa = request(agg, aggTarget());
        Bundle other = agg.shard(GateBundles.DECOY_SHARD);
        Path foreign = other.file(GateBundles.runEntry(agg.reportJson(), GateBundles.DECOY_SHARD, aggTarget()).get("runRecord").asText());
        refused(GateReportReader.RUN, with(qa, ROOT, qa.report(), qa.shards(), qa.state(), qa.generation(), qa.registeredRowSha256(), foreign, true));
    }

    @Test void aShuffledRunIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, "baseline");
        read(q);
        Path shuffled = b.file("runs/baseline.shuffle-7.run.json");
        assertTrue(Files.isRegularFile(shuffled));
        refusedIn(GateReportReader.RUN, with(q, ROOT, q.report(), null, q.state(), q.generation(), q.registeredRowSha256(), shuffled, true), "shuffled");
    }

    @Test void aChangedCutLogIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        Path cuts = b.file(GateBundles.runEntry(b.reportJson(), null, TARGET).get("cutLog").asText());
        Files.write(cuts, (Files.readString(cuts, StandardCharsets.UTF_8) + "\n").getBytes(StandardCharsets.UTF_8));
        refusedIn(GateReportReader.EVIDENCE, q, "cutLog");
    }

    @Test void coreScopeWithoutAcceptanceIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        GateReportReader.Request strict = with(q, ROOT, q.report(), null, q.state(), q.generation(), q.registeredRowSha256(), q.run(), false);
        refusedIn(GateReportReader.SCOPE, strict, "--accept-scope core");
        edit(b.report(), r -> r.put("scope", "semantic"));
        read(strict);
        edit(b.report(), r -> r.put("scope", "partial"));
        refused(GateReportReader.SCOPE, q);
    }

    @Test void aFullReportWithAnInsertedUnvalidatedRunIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        ObjectNode entry = GateBundles.runEntry(b.reportJson(), null, TARGET).deepCopy();
        String rr = GateBundles.rewrite(copy(b.file(entry.get("runRecord").asText()), b.file("runs/smoke.extra.run.json")), n -> n.put("runId", "run-extra"));
        copy(b.file(entry.get("cutLog").asText()), b.file("runs/smoke.extra.cuts.json"));
        entry.put("runId", "run-extra").put("runRecord", "runs/smoke.extra.run.json").put("runRecordSha256", rr).put("cutLog", "runs/smoke.extra.cuts.json");
        edit(b.report(), r -> ((ArrayNode) r.get("runs")).add(entry));
        refusedIn(GateReportReader.EVIDENCE, q, "two run entries for one summary");
    }

    @Test void aFullReportMissingVariantOrCutCoverageIsRefused() throws IOException {
        // the replay generation run planned but absent from summaries.json
        Bundle b = full();
        Path planned = copyRoot(tmp.resolve("planned-root"));
        GateBundles.rewrite(planned.resolve("gate/core-shards.json"), p -> {
            for (JsonNode s : p.get("shards")) if (s.get("name").asText().equals("horizon-planned-late")) ((ObjectNode) s).putArray("generationRuns").add("horizon-planned-late@replay");
        });
        GateReportReader.Request q = request(b, TARGET);
        refusedIn(GateReportReader.EVIDENCE, with(q, planned, q.report(), null, q.state(), q.generation(), q.registeredRowSha256(), q.run(), true), "horizon-planned-late@replay");
        // and a generation-run summary nothing plans
        ObjectNode s = b.summariesJson();
        ObjectNode extra = s.putArray("generationRuns").addObject();
        extra.setAll(GateBundles.runEntry(b.reportJson(), null, TARGET).deepCopy().put("name", "smoke@replay"));
        GateBundles.rewriteSummaries(b, s);
        refusedIn(GateReportReader.EVIDENCE, q, "smoke@replay");
        // a compared cut id removed
        Bundle c = full();
        ObjectNode cs = c.summariesJson();
        for (JsonNode r : cs.get("runs")) if (r.get("name").asText().equals("baseline")) { ArrayNode ids = (ArrayNode) r.get("comparedCutIds"); ids.remove(ids.size() - 1); }
        GateBundles.rewriteSummaries(c, cs);
        refused(GateReportReader.EVIDENCE, request(c, TARGET));
        // a planned scenario absent
        Bundle d = full();
        ObjectNode ds = d.summariesJson();
        ArrayNode runs = (ArrayNode) ds.get("runs");
        for (int i = 0; i < runs.size(); i++) if (runs.get(i).get("name").asText().equals("smoke-incident")) { runs.remove(i); break; }
        GateBundles.rewriteSummaries(d, ds);
        edit(d.report(), r -> {
            ArrayNode rs = (ArrayNode) r.get("runs");
            for (int i = 0; i < rs.size(); i++) if (rs.get(i).get("scenario").asText().equals("smoke-incident")) { rs.remove(i); break; }
        });
        refusedIn(GateReportReader.EVIDENCE, request(d, TARGET), "smoke-incident");
    }

    @Test void aFullReportWhoseSummariesChangedIsRefused() throws IOException {
        Bundle b = full();
        ObjectNode s = b.summariesJson();
        ((ObjectNode) s.get("runs").get(0)).put("summary", "changed");
        Files.write(b.summaries(), Json.pretty(s));
        refusedIn(GateReportReader.EVIDENCE, request(b, TARGET), "summaries.json");
    }

    @Test void aDigestThatMatchesOnlyInAnotherStoreOrShardSelectsNothing() throws IOException {
        String decoy = "f".repeat(64);
        Aggregate agg = aggregate(new Options(null, decoy));
        GateReportReader.Request q = request(agg, aggTarget());
        assertEquals(1, q.generation());
        JsonNode r = agg.reportJson();
        long holders = Stream.of(r.get("generations")).flatMap(a -> { List<JsonNode> l = new ArrayList<>(); a.forEach(l::add); return l.stream(); })
            .filter(g -> g.get("registeredRowSha256").asText().equals(decoy)).count();
        assertEquals(2, holders, "the digest is held by an assertion store of the target's shard and by another shard's pipeline store");
        refusedIn(GateReportReader.GENERATION, with(q, ROOT, q.report(), q.shards(), q.state(), 1, decoy, q.run(), true), "0 pipeline generations");
        // the same in a full bundle: an assertion store's digest selects nothing
        Bundle b = GateBundles.full(tmp.resolve("full-decoy"), new Options(null, decoy));
        GateReportReader.Request qf = request(b, GateBundles.first(GateBundles.allScenarios()));
        refused(GateReportReader.GENERATION, with(qf, ROOT, qf.report(), null, qf.state(), 1, decoy, qf.run(), true));
        // with the right entry present beside two entries of the same digest: accepted
        Aggregate same = aggregate(new Options(null, "target"));
        GateReportReader.Request qs = request(same, aggTarget());
        long n = 0;
        for (JsonNode g : same.reportJson().get("generations")) if (g.get("registeredRowSha256").asText().equals(qs.registeredRowSha256())) n++;
        assertEquals(3, n);
        assertEquals(GateBundles.HAND_MADE, read(qs).generation().get("shard").asText());
    }

    @Test void aTargetWhoseJournalsChangedIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        Path dir = Files.createDirectories(b.file("journals/smoke/online"));
        Files.writeString(dir.resolve("forseti.work-records.jsonl"), "{\"k\":1}\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("forseti.samples.jsonl"), "{\"k\":2}\n", StandardCharsets.UTF_8);
        ObjectNode index = Json.object();
        index.put("forseti.samples.jsonl", Canonical.digestOf(Files.readAllBytes(dir.resolve("forseti.samples.jsonl"))));
        index.put("forseti.work-records.jsonl", Canonical.digestOf(Files.readAllBytes(dir.resolve("forseti.work-records.jsonl"))));
        byte[] ib = Canonical.bytes(index);
        Files.write(b.file("journals/smoke/online.index.json"), ib);
        edit(b.report(), r -> GateBundles.runEntry(r, null, TARGET).put("journals", "journals/smoke/online").put("journalsIndexSha256", Canonical.digestOf(ib)));
        read(q);
        Files.writeString(dir.resolve("forseti.samples.jsonl"), "{\"k\":3}\n", StandardCharsets.UTF_8);
        refusedIn(GateReportReader.RUN, q, "journal forseti.samples.jsonl changed");
        Files.writeString(dir.resolve("forseti.samples.jsonl"), "{\"k\":2}\n", StandardCharsets.UTF_8);
        Files.write(b.file("journals/smoke/online.index.json"), Canonical.bytes(index.put("forseti.samples.jsonl", "0".repeat(64))));
        refusedIn(GateReportReader.RUN, q, "journal index");
    }

    @Test void aFullReportPromotedInAStateOutsideItsBundleIsRefused() throws IOException {
        Bundle b = full();
        GateReportReader.Request q = request(b, TARGET);
        Path copy = copyTree(q.state(), tmp.resolve("elsewhere").resolve("state"));
        assertArrayEquals(Files.readAllBytes(q.state().resolve(FileGenerationStore.FILE)), Files.readAllBytes(copy.resolve(FileGenerationStore.FILE)));
        refusedIn(GateReportReader.GENERATION, with(q, ROOT, q.report(), null, copy, q.generation(), q.registeredRowSha256(), q.run(), true), "own store");
    }

    @Test void aMissingOrFailedShuffledComparisonIsRefusedInBothLayouts() throws IOException {
        for (int variant = 0; variant < 2; variant++) {
            final int v = variant;
            java.util.function.Consumer<ObjectNode> change = s -> {
                ObjectNode x = (ObjectNode) s.get("shuffled").get(0);
                if (v == 0) { ArrayNode ids = (ArrayNode) x.get("comparedCutIds"); ids.remove(ids.size() - 1); }
                else x.put("equal", false);
            };
            Bundle b = full();
            ObjectNode s = b.summariesJson();
            change.accept(s);
            GateBundles.rewriteSummaries(b, s);
            refused(GateReportReader.EVIDENCE, request(b, TARGET));
            Aggregate agg = aggregate();
            Bundle hm = agg.shard(GateBundles.HAND_MADE);
            ObjectNode hs = hm.summariesJson();
            change.accept(hs);
            byte[] sb = Json.pretty(hs);
            Files.write(hm.summaries(), sb);
            GateBundles.rewrite(hm.report(), r -> r.put("summariesSha256", Canonical.digestOf(sb)));
            GateBundles.rebind(agg, GateBundles.HAND_MADE);
            refused(GateReportReader.EVIDENCE, request(agg, aggTarget()));
        }
    }

    // --- helpers -------------------------------------------------------------------------------------------------------------------

    static int manifestSize() { return Json.read(ROOT.resolve("gate/core-assertions.json")).get("assertions").size(); }

    /** A repository root with the committed {@code gate/} files and scenario locks only. */
    static Path copyRoot(Path dir) throws IOException {
        copyTree(ROOT.resolve("gate"), dir.resolve("gate"));
        copyTree(ROOT.resolve("fixtures/scenarios"), dir.resolve("fixtures/scenarios"));
        return dir;
    }

    static Path copyTree(Path from, Path to) throws IOException {
        try (Stream<Path> w = Files.walk(from)) {
            for (Path p : w.toList()) {
                Path t = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else { Files.createDirectories(t.getParent()); Files.copy(p, t, StandardCopyOption.REPLACE_EXISTING); }
            }
        }
        return to;
    }

    static Path copy(Path from, Path to) throws IOException { Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING); return to; }

    static List<String> fields(JsonNode n) { List<String> l = new ArrayList<>(); n.fieldNames().forEachRemaining(l::add); return l; }
}
