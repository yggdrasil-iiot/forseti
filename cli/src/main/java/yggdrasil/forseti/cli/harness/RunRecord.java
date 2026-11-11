package yggdrasil.forseti.cli.harness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.journal.Pins;

/** {@code <out>/run.json}: {@code {runId, engine, engineVersion, artifactSha256, generation, seed, shuffle, pins, fixtureDigests, jcs,
 *  subscribed, offsetRanges, cuts, command}}. {@code pins} is the envelope pin set every row of the run carries (with {@code job});
 *  {@code fixtureDigests} names the recording (raw SHA-256 of its manifest and the file digests it lists, all verified before the run) and
 *  the release (its {@code release.json} digest and the {@code rulesDigest}); {@code offsetRanges} are the half-open ranges the run consumed
 *  per subscribed partition ({@code {}} when the run had no topic input). No wall-clock time is recorded. */
final class RunRecord {
    private RunRecord() {}

    record Facts(String runId, long generation, long seed, boolean shuffle, Pins pins, String recordingManifestSha256, JsonNode recordingDigests,
                 String releaseSha256, String rulesDigest, String jcsJava, Map<String, List<Integer>> subscribed,
                 Map<String, Map<Integer, CutLog.Range>> offsetRanges, int cuts, String command) {}

    static ObjectNode build(Facts f) {
        ObjectNode n = Json.object().put("runId", f.runId());
        Pins.Job job = f.pins().job();
        n.put("engine", job.engine()).put("engineVersion", job.engineVersion()).put("artifactSha256", job.artifactSha256());
        Json.putSafe(n, "generation", f.generation());
        Json.putSafe(n, "seed", f.seed());
        n.put("shuffle", f.shuffle());
        n.set("pins", f.pins().toJson());
        ObjectNode d = n.putObject("fixtureDigests");
        d.put("recordingManifestSha256", f.recordingManifestSha256());
        d.set("recordingFiles", f.recordingDigests());
        d.put("releaseSha256", f.releaseSha256()).put("rulesDigest", f.rulesDigest());
        n.putObject("jcs").put("java", f.jcsJava());
        ObjectNode s = n.putObject("subscribed");
        f.subscribed().forEach((t, ps) -> { ArrayNode a = s.putArray(t); ps.forEach(a::add); });
        n.set("offsetRanges", CutLog.ranges(f.offsetRanges()));
        Json.putSafe(n, "cuts", f.cuts());
        return n.put("command", f.command());
    }

    static void write(Path out, ObjectNode record) { Json.write(out.resolve("run.json"), record); }
}
