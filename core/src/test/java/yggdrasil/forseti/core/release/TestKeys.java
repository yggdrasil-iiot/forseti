package yggdrasil.forseti.core.release;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** Test keys and re-signing of head statements, mirroring Vor's helpers.py: tampered variants are produced from a temporary copy
 *  of a fixture registry, never in fixtures/. Public for the tests of other packages (the rule set). */
public final class TestKeys {
    public static final Path FIXTURES = Path.of("..", "fixtures");
    private TestKeys() {}

    /** One signature to put in an envelope: the {@code keyid} it claims and the key that actually signs. */
    public record Signer(String keyid, PrivateKey key) {}

    public static KeyPair generate() {
        try { return KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    /** A trusted-keys file with one line per (principal, key), in the shape of fixtures/trusted-keys.jsonl. */
    public static TrustedKeys trusted(Path dir, Object... principalAndKey) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i + 1 < principalAndKey.length; i += 2) {
            PublicKey k = (PublicKey) principalAndKey[i + 1];
            b.append(Json.object().put("principal", (String) principalAndKey[i]).put("publicKey", Base64.getEncoder().encodeToString(k.getEncoded())).toString()).append('\n');
        }
        try {
            Path f = Files.createTempFile(dir, "trusted-", ".jsonl");
            Files.writeString(f, b.toString(), StandardCharsets.UTF_8);
            return TrustedKeys.load(f);
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    public static TrustedKeys fixtureKeys() {
        try { return TrustedKeys.load(FIXTURES.resolve("trusted-keys.jsonl")); } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    /** Copies {@code fixtures/<name>} to {@code <tmp>/<name>} and returns its {@code mappings} directory. */
    public static Path copy(String name, Path tmp) {
        Path src = FIXTURES.resolve(name), dst = tmp.resolve(name);
        try (Stream<Path> s = Files.walk(src)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                Path t = dst.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        } catch (IOException e) { throw new UncheckedIOException(e); }
        return dst.resolve("mappings");
    }

    public static Path envelopeFile(Path mappings, String stream) { return mappings.resolve("head-attestations").resolve(stream + ".dsse.json"); }
    public static Path headFile(Path mappings, String stream) { return mappings.resolve("heads").resolve(stream + ".json"); }

    public static ObjectNode envelope(Path mappings, String stream) { return (ObjectNode) Json.read(envelopeFile(mappings, stream)); }
    public static byte[] statementBytes(Path mappings, String stream) { return Base64.getDecoder().decode(envelope(mappings, stream).path("payload").asText()); }
    public static ObjectNode statement(Path mappings, String stream) { return (ObjectNode) Json.read(statementBytes(mappings, stream)); }
    public static long signedAtMs(Path mappings, String stream) { return Instant.parse(statement(mappings, stream).path("predicate").path("signedAt").asText()).toEpochMilli(); }
    public static long expiresMs(Path mappings, String stream) { return Instant.parse(statement(mappings, stream).path("predicate").path("expires").asText()).toEpochMilli(); }

    public static void writeEnvelope(Path mappings, String stream, JsonNode envelope) {
        try { Files.write(envelopeFile(mappings, stream), Json.JSON.writeValueAsBytes(envelope)); } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    public static byte[] sign(PrivateKey key, byte[] message) {
        try { Signature s = Signature.getInstance("Ed25519"); s.initSign(key); s.update(message); return s.sign(); }
        catch (GeneralSecurityException e) { throw new IllegalStateException(e); }
    }

    /** Edits the statement, serializes it, and writes a new envelope signed by {@code signers}. */
    public static void resign(Path mappings, String stream, Consumer<ObjectNode> edit, Signer... signers) {
        ObjectNode statement = statement(mappings, stream);
        edit.accept(statement);
        byte[] body;
        try { body = Json.JSON.writeValueAsBytes(statement); } catch (IOException e) { throw new UncheckedIOException(e); }
        ObjectNode env = envelope(mappings, stream);
        env.put("payload", Base64.getEncoder().encodeToString(body));
        ArrayNode sigs = env.putArray("signatures");
        byte[] message = Dsse.pae(Dsse.IN_TOTO, body);
        for (Signer s : signers) sigs.addObject().put("keyid", s.keyid()).put("sig", Base64.getEncoder().encodeToString(sign(s.key(), message)));
        writeEnvelope(mappings, stream, env);
    }

    /** After release.json changed: a new head naming its digest, and a statement naming both by raw-byte SHA-256, re-signed. */
    public static void republish(Path mappings, String stream, Signer... signers) {
        try {
            JsonNode head = Json.read(headFile(mappings, stream));
            String id = Json.text(head, "id"), version = Json.text(head, "version");
            String releasePath = "releases/" + id + "/" + version + "/release.json";
            String releaseSha = Canonical.digestOf(Files.readAllBytes(mappings.resolve(releasePath)));
            ObjectNode newHead = Json.object().put("id", id).put("sha256", releaseSha).put("version", version);
            byte[] headBytes = Json.JSON.writeValueAsBytes(newHead);
            Files.write(headFile(mappings, stream), headBytes);
            String headSha = Canonical.digestOf(headBytes);
            resign(mappings, stream, st -> {
                ((ObjectNode) st.path("predicate")).set("release", newHead.deepCopy());
                ArrayNode subject = st.putArray("subject");
                subject.addObject().put("name", "heads/" + stream + ".json").putObject("digest").put("sha256", headSha);
                subject.addObject().put("name", releasePath).putObject("digest").put("sha256", releaseSha);
            }, signers);
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    public static ObjectNode releaseJson(Path mappings, String releaseId, String version) {
        return (ObjectNode) Json.read(mappings.resolve("releases").resolve(releaseId).resolve(version).resolve("release.json"));
    }
    public static void writeReleaseJson(Path mappings, String releaseId, String version, JsonNode doc) {
        try { Files.write(mappings.resolve("releases").resolve(releaseId).resolve(version).resolve("release.json"), Json.JSON.writeValueAsBytes(doc)); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    }

    /** Replaces one payload file and re-pins what names it: its {@code files[]} digest in release.json and, when the proposal lists
     *  it, the proposal entry (and then the proposal's own digest). Call {@link #republish} afterwards. */
    public static void rewritePayload(Path mappings, String releaseId, String version, String path, byte[] bytes) {
        try {
            Path dir = mappings.resolve("releases").resolve(releaseId).resolve(version);
            Files.write(dir.resolve("payload").resolve(path), bytes);
            Map<String, String> changed = new HashMap<>();
            changed.put("payload/" + path, Canonical.digestOf(bytes));
            Path proposalFile = dir.resolve("payload").resolve("proposal.json");
            ObjectNode proposal = (ObjectNode) Json.read(proposalFile);
            boolean listed = false;
            for (JsonNode f : proposal.path("files"))
                if (f.path("path").asText().equals(path)) { ((ObjectNode) f).put("sha256", Canonical.digestOf(bytes)); listed = true; }
            if (listed) {
                byte[] pb = Json.pretty(proposal);
                Files.write(proposalFile, pb);
                changed.put("payload/proposal.json", Canonical.digestOf(pb));
            }
            ObjectNode doc = releaseJson(mappings, releaseId, version);
            for (JsonNode f : doc.path("files")) { String sha = changed.get(f.path("path").asText()); if (sha != null) ((ObjectNode) f).put("sha256", sha); }
            writeReleaseJson(mappings, releaseId, version, doc);
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }
}
