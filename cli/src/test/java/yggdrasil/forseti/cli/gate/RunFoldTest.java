package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.Envelope;

/** {@code J-PROGRESS-ORDER} on timer rows: a row a timer emitted belongs to the cut whose W1 reaches its deadline, whether its cause is
 *  the online {@code {"kind": "timer", "deadline"}} or the replayed {@code {"kind": "replayed", "inputKind": "timer", "identity":
 *  {"deadline"}}} with a null trigger (spec 583, owner decision 6). The progress of cut 1 closes W1 = 999. */
class RunFoldTest {
    @TempDir Path tmp;

    /** A work-record row with {@code cause} and no trigger, as a timer emits it. */
    static String timerRow(JsonNode cause) {
        ObjectNode body = Journals.body("complete");
        ObjectNode line = Json.object();
        line.putObject("headers").put("forseti-generation", "1");
        line.set("key", Journals.key("occ-1"));
        ObjectNode v = line.putObject("value");
        v.set("body", body);
        v.put("bodyDigest", Canonical.digest(body));
        ObjectNode env = v.putObject("envelope");
        env.put("evaluationGeneration", 1).put("revision", 1);
        env.set("cause", cause);
        env.putNull("trigger");
        env.put("eventTimeAt", 999);
        env.set("pins", Journals.PINS);
        return Canonical.utf8(Canonical.bytes(line));
    }

    @Test void anOrdinaryTimerRowAfterItsClosingProgressFailsProgressOrder() throws IOException {
        Journals j = new Journals(tmp).progress(RunFold.WR, 0, 1, 999, 0).line(RunFold.WR, 0, timerRow(Envelope.timerCause(Envelope.ONLINE, 999)))
            .progress(RunFold.WR, 0, 2, 1999, 0);
        try (RunFold f = j.fold(2)) {
            assertEquals(1, f.findings.count("J-PROGRESS-ORDER"), f.findings.describe());
            assertEquals(Set.of("J-PROGRESS-ORDER"), f.findings.checks());
        }
    }

    @Test void aReplayTimerRowAfterItsClosingProgressFailsProgressOrder() throws IOException {
        // a null trigger: without reading the replayed deadline the row would pass as belonging to no cut
        for (long deadline : new long[] {999, 500}) {
            Path run = tmp.resolve("run-" + deadline);
            Journals j = new Journals(run).progress(RunFold.WR, 0, 1, 999, 0).line(RunFold.WR, 0, timerRow(Envelope.timerCause(Envelope.REPLAYED, deadline)))
                .progress(RunFold.WR, 0, 2, 1999, 0);
            try (RunFold f = j.fold(2)) {
                assertEquals(1, f.findings.count("J-PROGRESS-ORDER"), deadline + ": " + f.findings.describe());
                assertTrue(f.findings.describe("J-PROGRESS-ORDER").contains("follows the progress of cut 1"), f.findings.describe());
                assertEquals(Set.of("J-PROGRESS-ORDER"), f.findings.checks());
            }
        }
    }

    @Test void aReplayTimerRowBeforeItsClosingProgressHolds() throws IOException {
        // due at 999: before the progress of cut 1; due at 1000: after the progress of cut 1, before that of cut 2
        Journals before = new Journals(tmp.resolve("before")).line(RunFold.WR, 0, timerRow(Envelope.timerCause(Envelope.REPLAYED, 999)))
            .progress(RunFold.WR, 0, 1, 999, 0).progress(RunFold.WR, 0, 2, 1999, 0);
        try (RunFold f = before.fold(2)) { assertTrue(f.findings.clean(), f.findings.describe()); }
        Journals next = new Journals(tmp.resolve("next")).progress(RunFold.WR, 0, 1, 999, 0)
            .line(RunFold.WR, 0, timerRow(Envelope.timerCause(Envelope.REPLAYED, 1000))).progress(RunFold.WR, 0, 2, 1999, 0);
        try (RunFold f = next.fold(2)) { assertTrue(f.findings.clean(), f.findings.describe()); }
    }
}
