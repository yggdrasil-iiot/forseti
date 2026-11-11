package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.time.Instant;
import java.util.Base64;
import java.util.function.Consumer;
import java.util.stream.Stream;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.release.Dsse;
import yggdrasil.forseti.core.release.TrustedKeys;

/** The gate's own signer for the tampered head variants (the gate is main code and does not reach the test classes). Every variant
 *  is made in a copy of a fixture registry under the gate's temporary directory; nothing here writes into {@code fixtures/}. Keys are
 *  generated per run and live only in memory and in the temporary trusted-keys files. */
final class GateSigner {
    private GateSigner() {}

    record Signer(String keyid, PrivateKey key) {}

    static KeyPair generate() {
        try { return KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    /** A trusted-keys file in {@code dir} with one line per (principal, public key), in the shape of fixtures/trusted-keys.jsonl. */
    static TrustedKeys trusted(Path dir, Object... principalAndKey) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i + 1 < principalAndKey.length; i += 2)
            b.append(Json.object().put("principal", (String) principalAndKey[i])
                .put("publicKey", Base64.getEncoder().encodeToString(((PublicKey) principalAndKey[i + 1]).getEncoded())).toString()).append('\n');
        Files.createDirectories(dir);
        Path f = Files.createTempFile(dir, "trusted-", ".jsonl");
        Files.writeString(f, b.toString(), StandardCharsets.UTF_8);
        return TrustedKeys.load(f);
    }

    /** Copies {@code fixtures/<name>} to a new directory under {@code tmp} and returns the copy's {@code mappings} directory. */
    static Path copy(Path fixtures, String name, Path tmp) throws IOException {
        Path src = fixtures.resolve(name), dst = Files.createTempDirectory(Files.createDirectories(tmp), name + "-").resolve(name);
        if (!Files.isDirectory(src)) throw new NoSuchFileException(src.toString());
        try (Stream<Path> s = Files.walk(src)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                Path t = dst.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
        return dst.resolve("mappings");
    }

    static Path envelopeFile(Path mappings, String stream) { return mappings.resolve("head-attestations").resolve(stream + ".dsse.json"); }
    static Path headFile(Path mappings, String stream) { return mappings.resolve("heads").resolve(stream + ".json"); }
    static ObjectNode envelope(Path mappings, String stream) { return (ObjectNode) Json.read(envelopeFile(mappings, stream)); }
    static byte[] statementBytes(Path mappings, String stream) { return Base64.getDecoder().decode(envelope(mappings, stream).path("payload").asText()); }
    static ObjectNode statement(Path mappings, String stream) { return (ObjectNode) Json.read(statementBytes(mappings, stream)); }
    static long signedAtMs(Path mappings, String stream) { return Instant.parse(statement(mappings, stream).path("predicate").path("signedAt").asText()).toEpochMilli(); }
    static long expiresMs(Path mappings, String stream) { return Instant.parse(statement(mappings, stream).path("predicate").path("expires").asText()).toEpochMilli(); }
    /** The gate's clock for a head: its statement's signedAt + 60 s, never the wall clock. */
    static long now(Path mappings, String stream) { return signedAtMs(mappings, stream) + 60_000; }

    static void writeEnvelope(Path mappings, String stream, JsonNode envelope) {
        try { Files.write(envelopeFile(mappings, stream), Json.JSON.writeValueAsBytes(envelope)); } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    static byte[] sign(PrivateKey key, byte[] message) {
        try { Signature s = Signature.getInstance("Ed25519"); s.initSign(key); s.update(message); return s.sign(); }
        catch (GeneralSecurityException e) { throw new IllegalStateException(e); }
    }

    /** Edits the head statement, serializes it, and writes a new envelope signed by {@code signers} (DSSE PAE over the new body). */
    static void resign(Path mappings, String stream, Consumer<ObjectNode> edit, Signer... signers) {
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

    /** Flips the lowest bit of the byte in the middle of {@code file}. */
    static void flipMiddleByte(Path file) throws IOException {
        byte[] b = Files.readAllBytes(file);
        if (b.length == 0) throw new IllegalStateException("empty " + file);
        b[b.length / 2] ^= 1;
        Files.write(file, b);
    }
}
