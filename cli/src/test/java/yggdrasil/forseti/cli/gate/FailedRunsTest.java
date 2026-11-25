package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.replay.ReplayEngine;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** A run whose engine job stopped on its own: the report of what it committed ({@link FailedRuns}, {@link ProgressAgreement}) and the
 *  control theta verdict, which still fails it ({@link ReplayGate#runProblems}). */
class FailedRunsTest {
    @TempDir Path tmp;

    /** A stage-1 progress of {@code cutId} merging {@code merged}, generation 1. */
    static String progress(long cutId, long... merged) {
        ObjectNode v = Json.object().put("type", "progress").put("cutId", cutId).put("generation", 1).put("W1", 1_000L * cutId);
        ArrayNode m = v.putArray("mergedCutIds");
        for (long c : merged) m.add(c);
        ObjectNode line = Json.object();
        line.putNull("key");
        line.set("value", v);
        return Canonical.utf8(Canonical.bytes(line));
    }

    static void journal(Path run, String topic, int partition, String... lines) throws IOException {
        Path f = run.resolve("journals").resolve(topic).resolve(partition + ".jsonl");
        Files.createDirectories(f.getParent());
        Files.writeString(f, lines.length == 0 ? "" : String.join("\n", lines) + "\n");
    }

    static void cuts(Path run, long... ids) throws IOException {
        ObjectNode o = Json.object();
        ArrayNode a = o.putArray("cuts");
        for (long k : ids) Json.putSafe(a.addObject(), "cutId", k);
        Json.write(run.resolve("cuts.json"), o);
    }

    /** The torn publication seen under theta: cut 50 on work-records only, then cut 51 merging 50 on both journals. */
    @Test void aTornPublicationShowsADisagreementAndARemergedCut() throws IOException {
        Path run = tmp.resolve("torn");
        journal(run, RunFold.WR, 2, progress(49), progress(50), progress(51, 50));
        journal(run, RunFold.SAMPLES, 2, progress(49), progress(51, 50));
        JsonNode p = ProgressAgreement.check(run, 1);
        assertEquals(1, p.path("partitions").asLong());
        assertEquals(1, p.path("disagreements").path("count").asLong(), p.toString());
        assertEquals("partition 2 at progress 2: forseti.work-records 50 [], forseti.samples 51 [50]", p.path("disagreements").path("examples").get(0).asText());
        assertEquals(1, p.path("remerged").path("count").asLong(), p.toString());
        assertEquals("forseti.work-records/2: progress of cut 51 merges 50, which has its own progress", p.path("remerged").path("examples").get(0).asText());
        assertEquals(0, p.path("truncated").path("count").asLong());
    }

    /** A journal that only ends earlier (a run that stopped) is truncated, never a disagreement; other generations are not read. */
    @Test void aShorterJournalIsTruncatedOnly() throws IOException {
        Path run = tmp.resolve("short");
        String other = progress(7).replace("\"generation\":1", "\"generation\":2");
        journal(run, RunFold.WR, 0, progress(1), other, progress(2), progress(3));
        journal(run, RunFold.SAMPLES, 0, progress(1), progress(2));
        JsonNode p = ProgressAgreement.check(run, 1);
        assertEquals(0, p.path("disagreements").path("count").asLong(), p.toString());
        assertEquals(0, p.path("remerged").path("count").asLong(), p.toString());
        assertEquals(1, p.path("truncated").path("count").asLong(), p.toString());
        assertEquals("partition 0: forseti.work-records 3 progress records, forseti.samples 2 (last forseti.work-records 3 [])", p.path("truncated").path("examples").get(0).asText());
    }

    @Test void afterFailureReportsTheDumpedJournals() throws IOException {
        Path run = tmp.resolve("run");
        journal(run, RunFold.WR, 2, progress(1), progress(2), progress(3, 2));
        journal(run, RunFold.SAMPLES, 2, progress(1), progress(3, 2));
        cuts(run, 1);
        ObjectNode a = FailedRuns.afterFailure(new ReplayEngine.Failed(new IllegalStateException("stage2: JobManager exited"), run, 1, Journals.RULES_DIGEST));
        assertTrue(a.path("dumped").asBoolean(), a.toString());
        assertFalse(a.has("error"), a.toString());
        assertEquals("[1]", a.path("completedCutIds").toString());
        assertEquals(3, a.path("foldedThroughCut").asLong());
        assertEquals(1, a.path("progressAgreement").path("disagreements").path("count").asLong());
        assertTrue(a.path("journalFindings").isObject(), a.toString());
        String line = FailedRuns.describe(a);
        assertTrue(line.startsWith("completed cuts 1, folded through cut 3; progress: remerged 1, disagreements 1, truncated 0"), line);
    }

    @Test void aFailureWithoutADumpSaysWhy() {
        IllegalStateException stop = new IllegalStateException("stage2: JobManager exited");
        stop.addSuppressed(new IllegalStateException("dump: broker gone"));
        ObjectNode a = FailedRuns.afterFailure(new ReplayEngine.Failed(stop, null, 1, Journals.RULES_DIGEST));
        assertFalse(a.path("dumped").asBoolean());
        assertEquals("java.lang.IllegalStateException: dump: broker gone", a.path("reason").asText());
        assertEquals("java.lang.IllegalStateException: stage2: JobManager exited", new ReplayEngine.Failed(stop, null, 1, "x").toString());
        assertSame(null, FailedRuns.of(new RuntimeException("other")));
        ReplayEngine.Failed f = new ReplayEngine.Failed(stop, null, 1, "x");
        assertSame(f, FailedRuns.of(new RuntimeException("wrapped", f)));
    }

    /** One restart run crashed with no finding of the theta kinds (whatever its afterFailure holds) and two with findings: I-22 failed and
     *  the gate exited 1, and the theta verdict still has the crash and the missing finding as problems (held=false). */
    @Test void aCrashedRestartRunStillFailsTheThetaVerdict() {
        Summaries s = new Summaries();
        ObjectNode crashed = Json.object().put("name", "stage2-early@restart-22i").put("failure", "crashed: java.lang.IllegalStateException: stage2: JobManager exited");
        ObjectNode af = crashed.putObject("afterFailure").put("dumped", true);
        af.putObject("progressAgreement").putObject("disagreements").put("count", 1);
        af.putObject("journalFindings").putObject("J-REVISION-CONTINUITY").put("count", 3);
        s.generationRuns.put("stage2-early@restart-22i", crashed);
        for (String n : List.of("stage2-early@restart-22ii", "stage2-early@restart-22iv")) {
            ObjectNode r = Json.object().put("name", n).putNull("failure");
            r.putArray("plannedCutIds").add(1);
            r.putArray("comparedCutIds").add(1);
            r.put("equal", true);
            r.putObject("findings").putObject("J-REVISION-CONTINUITY").put("count", 2);
            s.generationRuns.put(n, r);
        }
        ArrayNode tf = Json.JSON.createArrayNode();
        List<String> problems = new ArrayList<>();
        ReplayGate.runProblems(s, tf, problems);
        assertEquals(List.of("stage2-early@restart-22i: crashed: java.lang.IllegalStateException: stage2: JobManager exited",
            "stage2-early@restart-22i: failed without a duplicate, loss or revision-gap finding (spec 700)"), problems);
        assertEquals(3, tf.size());
        assertEquals("", tf.get(0).path("findings").asText());
        assertEquals("J-REVISION-CONTINUITY 2", tf.get(1).path("findings").asText());
    }
}
