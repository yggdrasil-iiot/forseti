package yggdrasil.forseti.core.release;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.KeyPair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

class ReleaseReaderTest {
    @TempDir Path tmp;

    HeadVerifier.Head head(Path m, String stream, TrustedKeys keys) {
        return HeadVerifier.verify(m, stream, keys, new FloorStore(tmp.resolve("state")), TestKeys.signedAtMs(m, stream) + 60_000);
    }

    static String rule(Refusal r) { return (String) r.evidence.get("rule"); }

    @Test void readsBothReleasesAndTheirPayloads() {
        Path m = TestKeys.copy("registry", tmp);
        for (Object[] c : new Object[][] {{"line2", "line2-rules", 44}, {"line2-plc", "line2-rules-plc", 64}}) {
            ReleaseReader.Release r = ReleaseReader.read(m, head(m, (String) c[0], TestKeys.fixtureKeys()));
            assertEquals(c[1], r.doc().path("id").asText());
            assertEquals(c[2], r.payload().size());
            assertEquals(c[2], r.files().size());
            assertEquals(1, r.documentsOfKind("attribution-rule").size());
            assertEquals(1, r.documentsOfKind("impact-rule").size());
            assertEquals("data/occupancy-binding-0.json", r.document("evidence-artifact", new ReleaseReader.Ref("occupancy-binding-line2", "1.0.0")).node().path("file").path("path").asText());
            assertEquals("data/incident-binding-0.json", r.document("evidence-artifact", new ReleaseReader.Ref("incident-binding-line2", "1.0.0")).node().path("file").path("path").asText());
            byte[] occ = r.payload("data/occupancy-binding-0.json");
            assertEquals(r.files().get("payload/data/occupancy-binding-0.json"), Canonical.digestOf(occ));
            Refusal wrongVersion = assertThrows(Refusal.class, () -> r.document("attribution-rule", new ReleaseReader.Ref("line2-attribution", "9.9.9")));
            assertEquals("document-missing", rule(wrongVersion));
        }
    }

    @Test void aChangedPayloadByteIsAMismatch() throws Exception {
        Path m = TestKeys.copy("registry", tmp);
        HeadVerifier.Head h = head(m, "line2", TestKeys.fixtureKeys());
        Path f = m.resolve("releases/line2-rules/1.0.0/payload/data/manual-0.txt");
        byte[] b = Files.readAllBytes(f); b[0] ^= 1; Files.write(f, b);
        Refusal r = assertThrows(Refusal.class, () -> ReleaseReader.read(m, h));
        assertEquals("provenance-mismatch", r.reason);
        assertEquals("release-file", rule(r));
        assertEquals("payload/data/manual-0.txt", r.evidence.get("path"));
    }

    @Test void aChangedReleaseJsonIsAMismatch() throws Exception {
        Path m = TestKeys.copy("registry", tmp);
        HeadVerifier.Head h = head(m, "line2", TestKeys.fixtureKeys());
        Path f = m.resolve(h.releasePath());
        Files.write(f, (Files.readString(f, StandardCharsets.UTF_8) + " ").getBytes(StandardCharsets.UTF_8));
        Refusal r = assertThrows(Refusal.class, () -> ReleaseReader.read(m, h));
        assertEquals("provenance-mismatch", r.reason);
        assertEquals("release-digest", rule(r));
    }

    @Test void anUnlistedPayloadIsRefused() throws Exception {
        Path m = TestKeys.copy("registry", tmp);
        Files.writeString(m.resolve("releases/line2-rules/1.0.0/payload/data/extra.txt"), "not listed\n");
        ReleaseReader.Release r = ReleaseReader.read(m, head(m, "line2", TestKeys.fixtureKeys()));
        Refusal e = assertThrows(Refusal.class, () -> r.payload("data/extra.txt"));
        assertEquals("provenance-mismatch", e.reason);
        assertEquals("unlisted", rule(e));
        assertThrows(Refusal.class, () -> r.payload("../release.json"));
    }

    @Test void anEscapingPathIsRefused() {
        Path m = TestKeys.copy("registry", tmp);
        KeyPair p = TestKeys.generate(), a = TestKeys.generate();
        TrustedKeys trusted = TestKeys.trusted(tmp, "proposer", p.getPublic(), "approver", a.getPublic());
        ObjectNode doc = TestKeys.releaseJson(m, "line2-rules", "1.0.0");
        ObjectNode first = (ObjectNode) doc.path("files").get(0);
        first.put("path", "payload/../release.json");
        TestKeys.writeReleaseJson(m, "line2-rules", "1.0.0", doc);
        TestKeys.republish(m, "line2", new TestKeys.Signer("proposer", p.getPrivate()), new TestKeys.Signer("approver", a.getPrivate()));
        HeadVerifier.Head h = head(m, "line2", trusted);   // the trust path still holds: only the reader's path rule refuses
        Refusal r = assertThrows(Refusal.class, () -> ReleaseReader.read(m, h));
        assertEquals("provenance-mismatch", r.reason);
        assertEquals("release-path", rule(r));
        assertEquals("payload/../release.json", r.evidence.get("path"));
    }

    @Test void aProposalEntryNotInReleaseIsAMismatch() throws Exception {
        Path m = TestKeys.copy("registry", tmp);
        KeyPair p = TestKeys.generate(), a = TestKeys.generate();
        TrustedKeys trusted = TestKeys.trusted(tmp, "proposer", p.getPublic(), "approver", a.getPublic());
        Path proposalFile = m.resolve("releases/line2-rules/1.0.0/payload/proposal.json");
        ObjectNode proposal = (ObjectNode) Json.read(proposalFile);
        ((ArrayNode) proposal.path("files")).addObject().put("path", "attribution-rules/9.json").put("kind", "attribution-rule")
            .put("id", "ghost").put("version", "1.0.0").put("sha256", "a".repeat(64));
        byte[] proposalBytes = Json.pretty(proposal);
        Files.write(proposalFile, proposalBytes);
        ObjectNode doc = TestKeys.releaseJson(m, "line2-rules", "1.0.0");
        for (JsonNode f : doc.path("files"))
            if (f.path("path").asText().equals("payload/proposal.json")) ((ObjectNode) f).put("sha256", Canonical.digestOf(proposalBytes));
        TestKeys.writeReleaseJson(m, "line2-rules", "1.0.0", doc);
        TestKeys.republish(m, "line2", new TestKeys.Signer("proposer", p.getPrivate()), new TestKeys.Signer("approver", a.getPrivate()));
        HeadVerifier.Head h = head(m, "line2", trusted);
        Refusal r = assertThrows(Refusal.class, () -> ReleaseReader.read(m, h));
        assertEquals("provenance-mismatch", r.reason);
        assertEquals("proposal-file", rule(r));
        assertEquals("attribution-rules/9.json", r.evidence.get("path"));
    }
}
