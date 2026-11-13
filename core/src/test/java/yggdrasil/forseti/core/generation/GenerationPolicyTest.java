package yggdrasil.forseti.core.generation;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.core.generation.Generation.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;

class GenerationPolicyTest {
    static final String RULES = "a".repeat(64), REPORT = "c".repeat(64);
    static final long AT = 1794182460000L;

    static JsonNode run() {
        ObjectNode r = Json.object().put("runId", "00000000-0000-4000-8000-000000000001").put("runRecordSha256", "b".repeat(64));
        r.putObject("offsetRanges");
        return r;
    }

    /** A row of {@code n} with the fixed pins; {@code run} and {@code gateReportSha256} as the status requires. */
    static Generation row(long n, String status) {
        boolean withRun = List.of(REPLAYING, VERIFIED, CURRENT).contains(status), withReport = List.of(VERIFIED, CURRENT).contains(status);
        ObjectNode evidence = Json.object().put("releaseSha256", "d".repeat(64));
        return new Generation(n, status, RULES, Json.JSON.createArrayNode(), Json.object(), 1, evidence, withReport ? REPORT : null,
            withRun ? run() : null, AT);
    }

    static GenerationStore.Entry e(String scope, long n, String status) { return new GenerationStore.Entry(scope, row(n, status)); }

    static void corrupt(List<GenerationStore.Entry> history) {
        IllegalStateException x = assertThrows(IllegalStateException.class, () -> GenerationPolicy.validate(history));
        assertTrue(x.getMessage().startsWith("control-log-corrupt: "), x.getMessage());
    }

    @Test void everyAllowedTransitionIsAllowed() {
        assertTrue(GenerationPolicy.allowed(null, REGISTERED));
        assertTrue(GenerationPolicy.allowed(REGISTERED, REPLAYING));
        assertTrue(GenerationPolicy.allowed(REPLAYING, VERIFIED));
        assertTrue(GenerationPolicy.allowed(VERIFIED, CURRENT));
        assertTrue(GenerationPolicy.allowed(CURRENT, RETIRED));
        assertTrue(GenerationPolicy.allowed(REGISTERED, RETIRED));
        GenerationPolicy.validate(List.of(e("line2", 1, REGISTERED), e("line2", 1, REPLAYING), e("line2", 1, VERIFIED), e("line2", 1, CURRENT),
            e("line2", 1, RETIRED)));
        GenerationPolicy.validate(List.of(e("line2", 1, REGISTERED), e("line2", 1, RETIRED)));
    }

    @Test void everyOtherTransitionIsRefused() {
        Set<List<String>> allowed = Set.of(List.of("", REGISTERED), List.of(REGISTERED, REPLAYING), List.of(REPLAYING, VERIFIED),
            List.of(VERIFIED, CURRENT), List.of(CURRENT, RETIRED), List.of(REGISTERED, RETIRED));
        List<String> from = new ArrayList<>(STATUSES);
        from.add(0, null);
        int refused = 0;
        for (String f : from) for (String t : STATUSES) {
            if (allowed.contains(List.of(f == null ? "" : f, t))) continue;
            assertFalse(GenerationPolicy.allowed(f, t), f + " -> " + t);
            refused++;
        }
        assertEquals(30 - 6, refused);
        assertFalse(GenerationPolicy.allowed(REGISTERED, "promoted"));
        corrupt(List.of(e("line2", 1, REGISTERED), e("line2", 1, CURRENT)));
        corrupt(List.of(e("line2", 1, REGISTERED), e("line2", 1, REGISTERED)));
        corrupt(List.of(e("line2", 1, REPLAYING)));
        // a row that cites no run where the status requires one, or a report on a registered row
        corrupt(List.of(e("line2", 1, REGISTERED), new GenerationStore.Entry("line2", new Generation(1, REPLAYING, RULES, Json.JSON.createArrayNode(),
            Json.object(), 1, Json.object().put("releaseSha256", "d".repeat(64)), null, null, AT)), e("line2", 1, VERIFIED), e("line2", 1, CURRENT)));
        corrupt(List.of(new GenerationStore.Entry("line2", new Generation(1, REGISTERED, RULES, Json.JSON.createArrayNode(), Json.object(), 1,
            Json.object().put("releaseSha256", "d".repeat(64)), REPORT, null, AT))));
    }

    @Test void oneCurrentPerScope() {
        List<GenerationStore.Entry> h = new ArrayList<>(List.of(e("line2", 1, REGISTERED), e("line2", 2, REGISTERED), e("line2-plc", 3, REGISTERED)));
        for (long n : List.of(1L, 3L)) for (String s : List.of(REPLAYING, VERIFIED, CURRENT)) h.add(e(n == 1 ? "line2" : "line2-plc", n, s));
        GenerationPolicy.validate(h);   // one current in each of two scopes
        List<GenerationStore.Entry> two = new ArrayList<>(h);
        for (String s : List.of(REPLAYING, VERIFIED, CURRENT)) two.add(e("line2", 2, s));
        corrupt(two);
        two.add(e("line2", 1, RETIRED));   // the previous current retired: valid again
        GenerationPolicy.validate(two);
        // pins or key changed on a later row of a number
        corrupt(List.of(e("line2", 1, REGISTERED), e("line2-plc", 1, RETIRED)));
        Generation other = new Generation(1, RETIRED, "e".repeat(64), Json.JSON.createArrayNode(), Json.object(), 1, Json.object().put("releaseSha256", "d".repeat(64)), null, null, AT);
        corrupt(List.of(e("line2", 1, REGISTERED), new GenerationStore.Entry("line2", other)));
    }

    @Test void numbersIncreaseAcrossScopes() {
        assertEquals(1, GenerationPolicy.next(0));
        assertEquals(3, GenerationPolicy.next(2));
        assertThrows(IllegalArgumentException.class, () -> GenerationPolicy.next(-1));
        GenerationPolicy.validate(List.of(e("line2", 1, REGISTERED), e("line2-plc", 2, REGISTERED), e("line2", 3, REGISTERED)));
        GenerationPolicy.validate(List.of(e("line2", 1, REGISTERED), e("line2-plc", 5, REGISTERED)));
        corrupt(List.of(e("line2", 1, REGISTERED), e("line2-plc", 1, REGISTERED)));   // per-scope numbering
        corrupt(List.of(e("line2", 2, REGISTERED), e("line2-plc", 1, REGISTERED)));   // a lower number registered later
    }

    @Test void aRetiredGenerationNeverReturns() {
        for (String s : STATUSES) assertFalse(GenerationPolicy.allowed(RETIRED, s), "retired -> " + s);
        for (String s : STATUSES) corrupt(List.of(e("line2", 1, REGISTERED), e("line2", 1, RETIRED), e("line2", 1, s)));
    }

    static JsonNode runOf(String runId, String digest) {
        ObjectNode r = Json.object().put("runId", runId).put("runRecordSha256", digest);
        r.putObject("offsetRanges").putObject("ot.messages").putArray("0").add(0).add(4);
        return r;
    }

    @Test void aPromotionCitesOneRunAndOneReportDigest() {
        GenerationStore.Entry reg = e("line2", 1, REGISTERED);
        JsonNode run = runOf("run-1", "e".repeat(64));
        List<GenerationStore.Entry> batch = GenerationPolicy.promotion(reg, Optional.empty(), run, REPORT, AT + 5);
        assertEquals(List.of(REPLAYING, VERIFIED, CURRENT), batch.stream().map(x -> x.row().status()).toList());
        for (GenerationStore.Entry x : batch) {
            assertEquals("line2", x.scope());
            assertEquals(run, x.row().run());
            assertEquals(AT + 5, x.row().at());
            assertEquals(reg.row().releaseEvidence(), x.row().releaseEvidence());
        }
        assertNull(batch.get(0).row().gateReportSha256());
        assertEquals(REPORT, batch.get(1).row().gateReportSha256());
        assertEquals(REPORT, batch.get(2).row().gateReportSha256());
        List<GenerationStore.Entry> h = new ArrayList<>(List.of(reg));
        h.addAll(batch);
        GenerationPolicy.validate(h);
        // with a previous current: its retired row comes before the new current, so no prefix holds two currents
        List<GenerationStore.Entry> two = new ArrayList<>(h);
        GenerationStore.Entry reg2 = e("line2", 2, REGISTERED);
        two.add(reg2);
        List<GenerationStore.Entry> second = GenerationPolicy.promotion(reg2, Optional.of(batch.get(2)), runOf("run-2", "f".repeat(64)), "9".repeat(64), AT + 9);
        assertEquals(List.of(REPLAYING, VERIFIED, RETIRED, CURRENT), second.stream().map(x -> x.row().status()).toList());
        assertEquals(1, second.get(2).row().generation());
        assertNull(second.get(2).row().run());
        assertNull(second.get(2).row().gateReportSha256());
        for (int k = 0; k <= second.size(); k++) {
            List<GenerationStore.Entry> prefix = new ArrayList<>(two);
            prefix.addAll(second.subList(0, k));
            if (k == 0 || k == second.size()) GenerationPolicy.validate(prefix); else corrupt(prefix);
        }
        // a history whose rows of one promotion cite two runs, or two report digests, is corrupt
        List<GenerationStore.Entry> otherRun = new ArrayList<>(List.of(reg, batch.get(0), batch.get(1)));
        Generation c = batch.get(2).row();
        otherRun.add(new GenerationStore.Entry("line2", new Generation(1, CURRENT, c.rulesDigest(), c.inputReleases(), c.replayFrom(), c.headVersion(),
            c.releaseEvidence(), REPORT, runOf("run-other", "e".repeat(64)), AT)));
        corrupt(otherRun);
        List<GenerationStore.Entry> otherReport = new ArrayList<>(List.of(reg, batch.get(0), batch.get(1)));
        otherReport.add(new GenerationStore.Entry("line2", new Generation(1, CURRENT, c.rulesDigest(), c.inputReleases(), c.replayFrom(), c.headVersion(),
            c.releaseEvidence(), "8".repeat(64), run, AT)));
        corrupt(otherReport);
        Generation v = batch.get(1).row();
        List<GenerationStore.Entry> verifiedOther = new ArrayList<>(List.of(reg, batch.get(0)));
        verifiedOther.add(new GenerationStore.Entry("line2", new Generation(1, VERIFIED, v.rulesDigest(), v.inputReleases(), v.replayFrom(), v.headVersion(),
            v.releaseEvidence(), REPORT, runOf("run-1", "0".repeat(64)), AT)));
        verifiedOther.add(new GenerationStore.Entry("line2", new Generation(1, CURRENT, c.rulesDigest(), c.inputReleases(), c.replayFrom(), c.headVersion(),
            c.releaseEvidence(), REPORT, runOf("run-1", "0".repeat(64)), AT)));
        corrupt(verifiedOther);
        // the builder refuses what is not a promotion
        assertThrows(IllegalArgumentException.class, () -> GenerationPolicy.promotion(batch.get(2), Optional.empty(), run, REPORT, AT));
        assertThrows(IllegalArgumentException.class, () -> GenerationPolicy.promotion(reg, Optional.empty(), null, REPORT, AT));
        assertThrows(IllegalArgumentException.class, () -> GenerationPolicy.promotion(reg, Optional.empty(), run, null, AT));
        assertThrows(IllegalArgumentException.class, () -> GenerationPolicy.promotion(reg2, Optional.of(e("line2-plc", 1, CURRENT)), run, REPORT, AT));
    }

    @Test void aGenerationRestingAtReplayingOrVerifiedIsCorrupt() {
        corrupt(List.of(e("line2", 1, REGISTERED), e("line2", 1, REPLAYING)));
        corrupt(List.of(e("line2", 1, REGISTERED), e("line2", 1, REPLAYING), e("line2", 1, VERIFIED)));
        corrupt(List.of(e("line2", 1, REGISTERED), e("line2", 1, REPLAYING), e("line2", 1, VERIFIED), e("line2", 2, REGISTERED)));
        GenerationPolicy.validate(List.of(e("line2", 1, REGISTERED), e("line2", 1, REPLAYING), e("line2", 1, VERIFIED), e("line2", 1, CURRENT)));
    }
}
