package yggdrasil.forseti.cli.gate;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The integrity checks of {@link RunFold} and {@link Integrity}, each caught on a hand-written run that breaks it once. */
class IntegrityTest {
    @TempDir Path tmp;

    static final ObjectNode K1 = Journals.key("occ-1");

    Journals j() { return new Journals(tmp.resolve("run")); }

    @Test void aRevisionGapIsCaught() throws Exception {
        Journals j = j().row(RunFold.WR, 0, K1, Journals.body("provisional"), 1, 0).row(RunFold.WR, 0, K1, Journals.body("complete"), 3, 0).progress(RunFold.WR, 0, 1, 1000, 0);
        try (RunFold f = j.fold(1)) {
            assertEquals(1, f.findings.count("J-REVISION-CONTINUITY"), f.findings.describe());
            assertEquals(Set.of("J-REVISION-CONTINUITY"), f.findings.checks());
        }
    }

    @Test void equalConsecutiveDigestsAreCaught() throws Exception {
        Journals j = j().row(RunFold.WR, 0, K1, Journals.body("provisional"), 1, 0).row(RunFold.WR, 0, K1, Journals.body("provisional"), 2, 0).progress(RunFold.WR, 0, 1, 1000, 0);
        try (RunFold f = j.fold(1)) {
            assertEquals(1, f.findings.count("J-NO-EQUAL-CONSECUTIVE"), f.findings.describe());
            assertEquals(Set.of("J-NO-EQUAL-CONSECUTIVE"), f.findings.checks());
        }
    }

    @Test void aRowAfterItsProgressIsCaught() throws Exception {
        // the row is triggered by ot.messages/1/0, which progress 1 already covers (through offset 0), yet it follows that progress
        Journals j = j().progress(RunFold.WR, 0, 1, 1000, 0).row(RunFold.WR, 0, K1, Journals.body("provisional"), 1, 0).progress(RunFold.WR, 0, 2, 2000, 1);
        try (RunFold f = j.fold(2)) {
            assertEquals(1, f.findings.count("J-PROGRESS-ORDER"), f.findings.describe());
            assertEquals(Set.of("J-PROGRESS-ORDER"), f.findings.checks());
        }
        // a row triggered inside cut 2's own range is in order
        Journals ok = new Journals(tmp.resolve("ok")).progress(RunFold.WR, 0, 1, 1000, 0).row(RunFold.WR, 0, K1, Journals.body("provisional"), 1, 1).progress(RunFold.WR, 0, 2, 2000, 1);
        try (RunFold f = ok.fold(2)) { assertTrue(f.findings.clean(), f.findings.describe()); }
    }

    @Test void aMissingDispositionIsCaught() throws Exception {
        Journals j = j().disposition(0, 0, "accepted", 1).progress(RunFold.LEDGER1, 0, 1, 1000, 0);
        ObjectNode cuts = Json.object();
        ArrayNode list = cuts.putArray("cuts");
        ObjectNode c1 = list.addObject().put("cutId", 1);
        c1.putObject("perStation");
        ArrayNode range = c1.putObject("offsetRanges").putObject("mfg.occupancy").putArray("1");
        range.add(0); range.add(2);   // offsets 0 and 1 delivered; only offset 0 has a disposition
        try (RunFold f = j.fold(1)) {
            Integrity.Measures m = Integrity.check(f, tmp, cuts, Map.of("mfg.occupancy", List.of(1)), null, Json.object(), Map.of());
            assertEquals(1, m.inputsWithoutDisposition());
            assertEquals(2, m.deliveries());
            assertEquals(1, f.findings.count("J-DISPOSITION-COVERAGE"), f.findings.describe());
            assertTrue(f.findings.describe("J-DISPOSITION-COVERAGE").contains("mfg.occupancy/1/1"), f.findings.describe());
        }
    }

    @Test void aStaleBasisDigestIsCaught() throws Exception {
        JsonNode first = Journals.body("provisional"), second = Journals.body("complete");
        ObjectNode unit = Json.object().put("incidentId", "inc-1").put("source", "qms-line2").put("unitId", "UNIT0000000000001");
        ObjectNode assessment = Json.object().put("status", "insufficient-evidence").put("rulesDigest", Journals.RULES_DIGEST);
        assessment.putArray("basis").addObject().put("bodyDigest", Canonical.digest(first)).set("visitRef", K1);
        Journals j = j().row(RunFold.WR, 0, K1, first, 1, 0).progress(RunFold.WR, 0, 1, 1000, 0)
            .row(RunFold.WR, 0, K1, second, 2, 1).progress(RunFold.WR, 0, 2, 2000, 1)
            .row(RunFold.AFFECTED, 0, unit, assessment, 1, 0).progress(RunFold.AFFECTED, 0, 1, 1000, 0).progress(RunFold.AFFECTED, 0, 2, 2000, 1);
        try (RunFold f = j.fold(2)) {
            assertEquals(1, f.findings.count("J-BASIS-CURRENT"), f.findings.describe());   // current at cut 1, stale at cut 2
            assertEquals(Set.of("J-BASIS-CURRENT"), f.findings.checks());
        }
    }

    @Test void aWrongPinsDigestIsCaught() throws Exception {
        ObjectNode other = Journals.PINS.deepCopy();
        ((ObjectNode) other.get("rules")).put("attribution", "c".repeat(64));
        ObjectNode otherJob = Journals.PINS.deepCopy();
        ((ObjectNode) otherJob.get("job")).put("runId", "run-2");   // job differs per run: never part of the digest
        JsonNode b = Journals.body("provisional");
        Journals j = j().row(RunFold.WR, 0, K1, b, Canonical.digest(b), 1, 0, other).row(RunFold.WR, 0, Journals.key("occ-3"), b, Canonical.digest(b), 1, 0, otherJob)
            .progress(RunFold.WR, 0, 1, 1000, 0);
        try (RunFold f = j.fold(1)) {
            assertEquals(1, f.findings.count("PINS"), f.findings.describe());
            assertTrue(f.findings.describe("PINS").contains("occ-1"), f.findings.describe());
        }
        // a body rulesDigest that is not the generation's is caught on its own
        ObjectNode wrongBody = Journals.body("complete").put("rulesDigest", "d".repeat(64));
        Journals k = new Journals(tmp.resolve("body")).row(RunFold.WR, 0, K1, wrongBody, 1, 0).progress(RunFold.WR, 0, 1, 1000, 0);
        try (RunFold f = k.fold(1)) { assertEquals(1, f.findings.count("PINS"), f.findings.describe()); }
    }
}
