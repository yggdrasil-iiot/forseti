package yggdrasil.forseti.core.release;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.KeyPair;
import java.time.Instant;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

class HeadVerifierTest {
    @TempDir Path tmp;

    FloorStore floors() { return new FloorStore(tmp.resolve("state")); }
    static long now(Path mappings, String stream) { return TestKeys.signedAtMs(mappings, stream) + 60_000; }

    static Refusal refused(String reason, Executable e) {
        Refusal r = assertThrows(Refusal.class, e);
        assertEquals(reason, r.reason, () -> "evidence " + r.evidence);
        return r;
    }

    @Test void verifiesTheBifrostPublishedHead() throws Exception {
        Path m = TestKeys.copy("registry", tmp);
        FloorStore floors = floors();
        for (String[] s : new String[][] {{"line2", "line2-rules"}, {"line2-plc", "line2-rules-plc"}, {"line2-plc-nocheck", "line2-rules-plc-nocheck"}}) {
            HeadVerifier.Head h = HeadVerifier.verify(m, s[0], TestKeys.fixtureKeys(), floors, now(m, s[0]));
            assertEquals(s[1], h.releaseId());
            assertEquals("1.0.0", h.version());
            assertEquals(1, h.headVersion());
            assertEquals("proposer", h.publishedBy());
            assertEquals("approver", h.approvedBy());
            assertEquals("mappings/heads/" + s[0] + ".json", h.headFile());
            assertEquals(Canonical.digestOf(Files.readAllBytes(TestKeys.headFile(m, s[0]))), h.headSha256());
            assertEquals(Canonical.digestOf(TestKeys.statementBytes(m, s[0])), h.statementSha256());
            assertEquals(Canonical.digestOf(Files.readAllBytes(m.resolve(h.releasePath()))), h.releaseSha256());
        }
        Path floor = tmp.resolve("state").resolve("head-floors").resolve("line2.json");
        assertTrue(Files.isRegularFile(floor));
        assertEquals(1, Json.read(floor).path("headVersion").asLong());
        assertEquals(java.util.List.of("line2", "line2-plc", "line2-plc-nocheck"), floors.streams());
    }

    @Test void verifiesRegistryMismatch() {
        Path m = TestKeys.copy("registry-mismatch", tmp);
        HeadVerifier.Head h = HeadVerifier.verify(m, "line2", TestKeys.fixtureKeys(), floors(), now(m, "line2"));
        assertEquals("line2-rules", h.releaseId());
        ReleaseReader.Release r = ReleaseReader.read(m, h);
        assertEquals(44, r.payload().size());
    }

    @Test void verifiesRegistryNextAtVersion2AndRaisesTheFloor() {
        Path m = TestKeys.copy("registry-next", tmp);
        FloorStore floors = floors();
        HeadVerifier.Head h = HeadVerifier.verify(m, "line2", TestKeys.fixtureKeys(), floors, now(m, "line2"));
        assertEquals(2, h.headVersion());
        assertEquals("1.0.1", h.version());
        assertEquals(2, floors.get("line2").orElseThrow().headVersion());
        assertEquals(h.statementSha256(), floors.get("line2").orElseThrow().statementSha256());
    }

    @Test void registryAfterRegistryNextIsRollback() {
        Path next = TestKeys.copy("registry-next", tmp), reg = TestKeys.copy("registry", tmp);
        FloorStore floors = floors();
        HeadVerifier.verify(next, "line2", TestKeys.fixtureKeys(), floors, now(next, "line2"));
        Refusal r = refused("head-rollback", () -> HeadVerifier.verify(reg, "line2", TestKeys.fixtureKeys(), floors, now(next, "line2")));
        assertEquals(2L, r.evidence.get("floor"));
        assertEquals(1L, r.evidence.get("candidate"));
    }

    @Test void aMissingAttestationIsUnverified() throws Exception {
        Path m = TestKeys.copy("registry", tmp);
        long now = now(m, "line2");
        Files.delete(TestKeys.envelopeFile(m, "line2"));
        assertEquals("missing", refused("head-unverified", () -> HeadVerifier.verify(m, "line2", TestKeys.fixtureKeys(), floors(), now)).evidence.get("rule"));
    }

    @Test void aWrongPayloadTypeIsUnverified() {
        Path m = TestKeys.copy("registry", tmp);
        ObjectNode env = TestKeys.envelope(m, "line2");
        env.put("payloadType", "application/json");
        TestKeys.writeEnvelope(m, "line2", env);
        assertEquals("envelope", refused("head-unverified", () -> HeadVerifier.verify(m, "line2", TestKeys.fixtureKeys(), floors(), now(m, "line2"))).evidence.get("rule"));
    }

    @Test void onePrincipalIsUnverified() {
        Path m = TestKeys.copy("registry", tmp);
        KeyPair p = TestKeys.generate(), q = TestKeys.generate();
        TestKeys.resign(m, "line2", st -> ((ObjectNode) st.path("predicate")).put("approvedBy", "proposer"),
            new TestKeys.Signer("proposer", p.getPrivate()), new TestKeys.Signer("proposer", q.getPrivate()));
        TrustedKeys trusted = TestKeys.trusted(tmp, "proposer", p.getPublic(), "proposer", q.getPublic());
        assertEquals("principals", refused("head-unverified", () -> HeadVerifier.verify(m, "line2", trusted, floors(), now(m, "line2"))).evidence.get("rule"));
    }

    @Test void aSignatureFromAnUntrustedKeyIsUnverified() {
        Path m = TestKeys.copy("registry", tmp);
        KeyPair p = TestKeys.generate(), a = TestKeys.generate();
        TrustedKeys others = TestKeys.trusted(tmp, "proposer", p.getPublic(), "approver", a.getPublic());
        Refusal r = refused("head-unverified", () -> HeadVerifier.verify(m, "line2", others, floors(), now(m, "line2")));
        assertEquals("signature", r.evidence.get("rule"));
        assertEquals("proposer", r.evidence.get("principal"));
    }

    @Test void aForgedKeyidDoesNotCount() {
        Path m = TestKeys.copy("registry", tmp);
        KeyPair p = TestKeys.generate(), a = TestKeys.generate(), forger = TestKeys.generate();
        TestKeys.resign(m, "line2", st -> {}, new TestKeys.Signer("proposer", p.getPrivate()), new TestKeys.Signer("approver", forger.getPrivate()));
        TrustedKeys trusted = TestKeys.trusted(tmp, "proposer", p.getPublic(), "approver", a.getPublic());
        Refusal r = refused("head-unverified", () -> HeadVerifier.verify(m, "line2", trusted, floors(), now(m, "line2")));
        assertEquals("signature", r.evidence.get("rule"));
        assertEquals("approver", r.evidence.get("principal"));
    }

    @Test void twoSignaturesFromOneKeyAreUnverified() {
        Path m = TestKeys.copy("registry", tmp);
        KeyPair k = TestKeys.generate();
        TestKeys.resign(m, "line2", st -> {}, new TestKeys.Signer("proposer", k.getPrivate()), new TestKeys.Signer("approver", k.getPrivate()));
        TrustedKeys trusted = TestKeys.trusted(tmp, "proposer", k.getPublic(), "approver", k.getPublic());
        assertEquals("same-key", refused("head-unverified", () -> HeadVerifier.verify(m, "line2", trusted, floors(), now(m, "line2"))).evidence.get("rule"));
    }

    @Test void aChangedHeadFileIsUnverified() throws Exception {
        Path m = TestKeys.copy("registry", tmp);
        Path head = TestKeys.headFile(m, "line2");
        Files.write(head, (Files.readString(head, StandardCharsets.UTF_8) + "\n").getBytes(StandardCharsets.UTF_8));
        assertEquals("subject", refused("head-unverified", () -> HeadVerifier.verify(m, "line2", TestKeys.fixtureKeys(), floors(), now(m, "line2"))).evidence.get("rule"));
    }

    @Test void aModifiedPayloadByteIsUnverified() {
        Path m = TestKeys.copy("registry", tmp);
        long now = now(m, "line2");
        byte[] body = TestKeys.statementBytes(m, "line2");
        String text = new String(body, StandardCharsets.UTF_8).replace("\"approval-line2-rules-1.0.0\"", "\"approval-line2-rules-1.0.9\"");
        assertNotEquals(new String(body, StandardCharsets.UTF_8), text);
        ObjectNode env = TestKeys.envelope(m, "line2");
        env.put("payload", Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8)));
        TestKeys.writeEnvelope(m, "line2", env);
        assertEquals("signature", refused("head-unverified", () -> HeadVerifier.verify(m, "line2", TestKeys.fixtureKeys(), floors(), now)).evidence.get("rule"));
    }

    @Test void aStatementSignedInTheFutureIsUnverified() {
        Path m = TestKeys.copy("registry", tmp);
        long now = now(m, "line2");
        KeyPair p = TestKeys.generate(), a = TestKeys.generate();
        String future = Instant.ofEpochMilli(now + 3_600_000).toString().replaceAll("\\.\\d+Z$", "Z");
        TestKeys.resign(m, "line2", st -> ((ObjectNode) st.path("predicate")).put("signedAt", future),
            new TestKeys.Signer("proposer", p.getPrivate()), new TestKeys.Signer("approver", a.getPrivate()));
        TrustedKeys trusted = TestKeys.trusted(tmp, "proposer", p.getPublic(), "approver", a.getPublic());
        Refusal r = refused("head-unverified", () -> HeadVerifier.verify(m, "line2", trusted, floors(), now));
        assertEquals("signed-in-future", r.evidence.get("rule"));
        assertFalse(Files.exists(tmp.resolve("state").resolve("head-floors").resolve("line2.json")));
    }

    @Test void aLowerVersionThanTheFloorIsRollback() {
        Path m = TestKeys.copy("registry", tmp);
        FloorStore floors = floors();
        assertEquals(FloorStore.RAISED, floors.update("line2", 5, "f".repeat(64), true));
        Refusal r = refused("head-rollback", () -> HeadVerifier.verify(m, "line2", TestKeys.fixtureKeys(), floors, now(m, "line2")));
        assertEquals(5L, r.evidence.get("floor"));
        assertEquals(1L, r.evidence.get("candidate"));
        assertEquals(5, floors.get("line2").orElseThrow().headVersion());
    }

    @Test void theSameVersionWithAnotherStatementIsRollback() {
        Path original = TestKeys.copy("registry", tmp.resolve("a")), m = TestKeys.copy("registry", tmp.resolve("b"));
        FloorStore floors = floors();
        HeadVerifier.Head first = HeadVerifier.verify(original, "line2", TestKeys.fixtureKeys(), floors, now(original, "line2"));
        KeyPair p = TestKeys.generate(), a = TestKeys.generate();
        TestKeys.resign(m, "line2", st -> ((ObjectNode) st.path("predicate")).put("approvalId", "another-approval"),
            new TestKeys.Signer("proposer", p.getPrivate()), new TestKeys.Signer("approver", a.getPrivate()));
        TrustedKeys trusted = TestKeys.trusted(tmp, "proposer", p.getPublic(), "approver", a.getPublic());
        Refusal r = refused("head-rollback", () -> HeadVerifier.verify(m, "line2", trusted, floors, now(original, "line2")));
        assertEquals(1L, r.evidence.get("floor"));
        assertEquals(1L, r.evidence.get("candidate"));
        assertEquals(first.statementSha256(), r.evidence.get("recordedStatementSha256"));
        assertNotEquals(first.statementSha256(), r.evidence.get("candidateStatementSha256"));
    }

    @Test void anExpiredHeadIsExpired() {
        Path m = TestKeys.copy("registry", tmp);
        long expires = TestKeys.expiresMs(m, "line2");
        Refusal r = refused("head-expired", () -> HeadVerifier.verify(m, "line2", TestKeys.fixtureKeys(), floors(), expires));
        assertEquals(Instant.ofEpochMilli(expires).toString(), r.evidence.get("expires"));
        assertFalse(Files.exists(tmp.resolve("state").resolve("head-floors").resolve("line2.json")), "an expired head never raises the floor");
    }

    @Test void rollbackIsCheckedBeforeExpiry() {
        Path next = TestKeys.copy("registry-next", tmp), reg = TestKeys.copy("registry", tmp);
        FloorStore floors = floors();
        HeadVerifier.verify(next, "line2", TestKeys.fixtureKeys(), floors, now(next, "line2"));
        long expires = TestKeys.expiresMs(reg, "line2");
        refused("head-rollback", () -> HeadVerifier.verify(reg, "line2", TestKeys.fixtureKeys(), floors, expires));
    }
}
