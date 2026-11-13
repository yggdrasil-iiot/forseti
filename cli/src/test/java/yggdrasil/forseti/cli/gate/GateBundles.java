package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** Whole-gate evidence bundles for the tests of {@code generation promote} and its report reader, on the repository root: a {@code full}
 *  bundle and an {@code aggregate} report with its shard bundles, both complete and accepted by the gate's own validator, built by
 *  {@link FixtureBundles} (stand-in runs, every planned scenario, shuffled run and generation run, the kept journals of the event-time
 *  replay and of its scenario's online run). The positive bundles list every committed manifest id: they are validation fixtures, not
 *  delivery evidence. */
public final class GateBundles {
    private GateBundles() {}

    public static final Path ROOT = CoreGateTest.ROOT, REGISTRY = ShardsTest.REGISTRY, KEYS = ShardsTest.KEYS;
    public static final String HAND_MADE = FixtureBundles.HAND_MADE, DECOY_SHARD = FixtureBundles.DECOY_SHARD;

    /** Options of a bundle ({@link FixtureBundles.Options}). */
    public record Options(String plcScenario, String decoy) {
        public static final Options DEFAULT = new Options(null, null);
        FixtureBundles.Options fixture() { return new FixtureBundles.Options(plcScenario, decoy); }
    }

    /** One bundle directory: {@code report.json}, {@code summaries.json}, {@code runs/}, {@code journals/}, and {@code state/} when its
     *  store was kept. */
    public record Bundle(Path dir) {
        public Path report() { return dir.resolve("report.json"); }
        public Path summaries() { return dir.resolve(Shards.SUMMARIES); }
        public Path state() { return dir.resolve(GateEvidence.STATE); }
        public ObjectNode reportJson() { return (ObjectNode) Json.read(report()); }
        public ObjectNode summariesJson() { return (ObjectNode) Json.read(summaries()); }
        public Path file(String relative) { return dir.resolve(relative); }
    }

    /** An aggregate report ({@code report}) and the shard bundles under {@code shards}. */
    public record Aggregate(Path report, Path shards) {
        public Bundle shard(String name) { return new Bundle(shards.resolve(name)); }
        public ObjectNode reportJson() { return (ObjectNode) Json.read(report); }
    }

    /** {@code manifestSha256}, {@code seed}, {@code pins}, {@code fixtureDigests} of the stand-in reports. */
    public static ObjectNode common() { return FixtureBundles.common(ROOT); }

    /** A complete full-layout bundle in {@code dir}. */
    public static Bundle full(Path dir, Options o) throws IOException { return new Bundle(FixtureBundles.full(ROOT, dir, o.fixture(), common())); }

    /** A complete aggregate report at {@code dir/aggregate/report.json} with its shard bundles under {@code dir/shards}. */
    public static Aggregate aggregate(Path dir, Options o) throws IOException { return aggregate(dir, o, common()); }

    /** The same, its reports stating {@code common} (and its stand-in runs its pins). */
    public static Aggregate aggregate(Path dir, Options o, ObjectNode common) throws IOException {
        return new Aggregate(FixtureBundles.aggregate(ROOT, dir, o.fixture(), common), dir.resolve("shards"));
    }

    // --- edits -------------------------------------------------------------------------------------------------------------------

    public static ObjectNode runEntry(JsonNode report, String shard, String scenario) { return FixtureBundles.runEntry(report, shard, scenario); }
    public static ObjectNode generationEntry(JsonNode report, String shard, String scenario) { return FixtureBundles.generationEntry(report, shard, scenario); }

    /** The run entry of generation run {@code name} in {@code report}'s {@code runs[]}. */
    public static ObjectNode generationRunEntry(JsonNode report, String name) {
        Scenarios.GenerationRun g = Scenarios.generationRun(name);
        for (JsonNode r : report.get("runs"))
            if (r.path("scenario").asText().equals(g.scenario()) && r.path("variant").asText().equals(g.variant()) && r.path("store").asText().equals(g.store())) return (ObjectNode) r;
        throw new IllegalStateException("no run of " + name);
    }

    /** Rewrites a full bundle's summaries and rebinds their digest in its report. */
    public static void rewriteSummaries(Bundle b, ObjectNode summaries) throws IOException {
        FixtureBundles.writeBound(b.dir(), b.reportJson(), summaries);
    }

    /** Rewrites one shard bundle's report or summaries and rebinds both digests in the aggregate report's {@code shards[]}. */
    public static void rebind(Aggregate a, String shard) throws IOException {
        ObjectNode agg = a.reportJson();
        for (JsonNode s : agg.get("shards"))
            if (s.path("name").asText().equals(shard)) {
                ((ObjectNode) s).put("reportSha256", Canonical.digestOf(Files.readAllBytes(a.shard(shard).report())))
                    .put("summariesSha256", Canonical.digestOf(Files.readAllBytes(a.shard(shard).summaries())));
            }
        Json.write(a.report(), agg);
    }

    /** Edits a JSON file of a bundle and returns its new raw digest. */
    public static String rewrite(Path file, java.util.function.Consumer<ObjectNode> change) throws IOException { return FixtureBundles.rewrite(file, change); }

    /** The scenario whose generation is number 1 in a pipeline store that makes {@code scenarios} (the first in {@link Scenarios#all()} order). */
    public static String first(List<String> scenarios) {
        for (String sc : Scenarios.all()) if (scenarios.contains(sc)) return sc;
        throw new IllegalStateException();
    }

    public static List<String> allScenarios() { return List.copyOf(Scenarios.all()); }
    public static List<String> handMadeScenarios() { return Shards.load(ROOT).shard(HAND_MADE).scenarios(); }
    public static List<Long> shuffleSeeds() { return Scenarios.SHUFFLE_SEEDS; }
    public static List<String> shuffled() { return Scenarios.SHUFFLED; }
}
