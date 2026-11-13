package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.core.Json;

/** What a reader of a gate report outside this package ({@code GateReportReader}, plan Step 21.1) may ask of the gate's own code: the
 *  committed manifest ids, the committed shard plan and its validation, the one evidence validator ({@link GateEvidence#validate}) and the
 *  aggregate run's reading of the shard directories ({@link Shards#read}). Nothing here validates anything itself; it only exposes the
 *  gate's checks, so a promotion and a gate run apply the same ones. */
public final class GateBundle {
    private GateBundle() {}

    public static final String MANIFEST = CoreGate.MANIFEST, PLAN = Shards.PLAN, SUMMARIES = Shards.SUMMARIES, PIPELINE = GateEvidence.PIPELINE,
        STATE = GateEvidence.STATE, SHARED = GateEvidence.SHARED, ONLINE = GateEvidence.ONLINE;

    /** The ids of a core assertion manifest, in manifest order ({@link CoreGate#manifestIds}). */
    public static List<String> manifestIds(JsonNode manifest) { return CoreGate.manifestIds(manifest); }

    /** The committed shard plan of {@code root}: its raw digest, shard names in plan order, the owner of every id (a shard name or
     *  {@code aggregate}), the problems {@link Shards#validate} finds against {@code manifestIds}, and the generation runs every shard plans
     *  ({@code generationRuns}, absent = none). */
    public record Plan(String sha256, List<String> shards, Map<String, String> owner, List<String> problems, Map<String, List<String>> generationRuns) {}

    public static Plan plan(Path root, List<String> manifestIds) {
        Shards.Plan p = Shards.load(root);
        Map<String, String> owner = new HashMap<>();
        for (Shards.Shard s : p.shards()) for (String id : s.assertions()) owner.putIfAbsent(id, s.name());
        for (String id : p.aggregate()) owner.putIfAbsent(id, "aggregate");
        JsonNode raw = Json.read(root.resolve(Shards.PLAN));
        Map<String, List<String>> runs = new LinkedHashMap<>();
        for (JsonNode s : raw.path("shards")) {
            List<String> l = new ArrayList<>();
            s.path("generationRuns").forEach(x -> l.add(x.asText()));
            runs.put(s.path("name").asText(), List.copyOf(l));
        }
        return new Plan(p.sha256(), p.names(), Map.copyOf(owner), Shards.validate(p, manifestIds, new HashSet<>(Injections.AGGREGATE)), runs);
    }

    /** The problems of one bundle by the one validator ({@link GateEvidence#validate}). */
    public static List<String> validate(Path root, Path bundle, JsonNode report, JsonNode summaries) {
        return GateEvidence.validate(root, bundle, report, summaries);
    }

    /** One result a shard recorded. */
    public record Recorded(String shard, String id, boolean ok) {}

    /** The aggregate run's reading of {@code dir} ({@link Shards#read}) against {@code expect} ({@code manifestSha256, seed, pins,
     *  fixtureDigests}): its problems, its {@code shards[]}, {@code generations[]} and {@code runs[]}, and the shards' recorded results. */
    public record Read(List<String> problems, ObjectNode merged, List<Recorded> recorded) {}

    public static Read readShards(Path root, Path dir, JsonNode expect) {
        List<Shards.Recorded> recorded = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        ObjectNode merged = Json.object();
        ObjectNode e = Json.object();
        for (String f : List.of("manifestSha256", "seed", "pins", "fixtureDigests")) e.set(f, expect.path(f).deepCopy());
        Shards.read(root, dir, Shards.load(root), e, recorded, problems, merged);
        return new Read(List.copyOf(problems), merged, recorded.stream().map(r -> new Recorded(r.shard(), r.id(), r.ok())).toList());
    }

    /** The scenarios a full run makes ({@link Scenarios#all()}) and its shuffled keys {@code <scenario>@<seed>}. */
    public static List<String> scenarios() { return List.copyOf(Scenarios.all()); }

    public static List<String> shuffledKeys() {
        List<String> l = new ArrayList<>();
        for (String sc : Scenarios.SHUFFLED) for (long seed : Scenarios.SHUFFLE_SEEDS) l.add(sc + "@" + seed);
        return l;
    }

    /** Equal as JSON values ({@link Shards#same}). */
    public static boolean same(JsonNode a, JsonNode b) { return Shards.same(a, b); }
}
