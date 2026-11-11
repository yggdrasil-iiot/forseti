package yggdrasil.forseti.core.release;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import java.nio.file.*;
import java.security.PublicKey;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;

/** Verifies mappings/heads/<stream>.json through mappings/head-attestations/<stream>.dsse.json as Vor's head.py does:
 *  envelope shape, two distinct principals, one verifying signature per principal under two different trusted keys, the
 *  statement names exactly this head and its release.json by raw-byte SHA-256, signedAt not in the future, a version floor
 *  that never goes down (the same version must carry the same statement), then expiry. Pure: `nowMs` is a parameter. */
public final class HeadVerifier {
    public static final String STATEMENT = "https://in-toto.io/Statement/v1";
    public static final String PREDICATE = "https://github.com/yggdrasil-iiot/bifrost/attestation/mapping-head/v1";
    static final Pattern STREAM = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
    static final Pattern INSTANT = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z");
    private HeadVerifier() {}

    public record Head(String stream, String releaseId, String version, String releaseSha256, String headFile, String headSha256,
                       String attestationFile, String attestationSha256, String statementSha256, long headVersion,
                       String signedAt, String expires, String publishedBy, String approvedBy) {
        public String releasePath() { return "releases/" + releaseId + "/" + version + "/release.json"; }
    }

    public static Head verify(Path mappings, String stream, TrustedKeys trusted, FloorStore floors, long nowMs) {
        if (!STREAM.matcher(stream).matches()) throw new Refusal("head-unverified", "stream", stream, "rule", "stream-id");
        String headFile = "heads/" + stream + ".json", attFile = "head-attestations/" + stream + ".dsse.json";
        byte[] headBytes, attBytes;
        try { headBytes = Files.readAllBytes(mappings.resolve(headFile)); attBytes = Files.readAllBytes(mappings.resolve(attFile)); }
        catch (java.io.IOException e) { throw new Refusal("head-unverified", "stream", stream, "rule", "missing"); }
        String headSha = Canonical.digestOf(headBytes);
        JsonNode head, envelope, statement, p; byte[] body; String published, approved;
        try {
            head = Json.read(headBytes); envelope = Json.read(attBytes);
            if (!Dsse.IN_TOTO.equals(envelope.path("payloadType").asText(null)) || !envelope.path("payload").isTextual()) throw new IllegalArgumentException("envelope");
            body = Base64.getDecoder().decode(envelope.path("payload").asText()); statement = Json.read(body); p = statement.path("predicate");
            published = Json.text(p, "publishedBy"); approved = Json.text(p, "approvedBy");
        } catch (IllegalArgumentException e) { throw new Refusal("head-unverified", "stream", stream, "rule", "envelope"); }
        if (published.equals(approved)) throw new Refusal("head-unverified", "stream", stream, "rule", "principals", "publishedBy", published, "approvedBy", approved);
        byte[] message = Dsse.pae(Dsse.IN_TOTO, body); List<PublicKey> used = new ArrayList<>();
        for (String principal : List.of(published, approved)) {
            PublicKey key = signer(envelope, message, trusted.of(principal));
            if (key == null) throw new Refusal("head-unverified", "stream", stream, "rule", "signature", "principal", principal);
            for (PublicKey u : used) if (Arrays.equals(u.getEncoded(), key.getEncoded())) throw new Refusal("head-unverified", "stream", stream, "rule", "same-key", "principal", principal);
            used.add(key);
        }
        String releaseId, version, releaseSha; Instant signedAt, expires;
        try {
            releaseId = Json.text(head, "id"); version = Json.text(head, "version"); releaseSha = Json.text(head, "sha256");
            String releasePath = "releases/" + releaseId + "/" + version + "/release.json"; JsonNode subject = statement.path("subject");
            boolean ok = STATEMENT.equals(statement.path("_type").asText(null)) && PREDICATE.equals(statement.path("predicateType").asText(null))
                && stream.equals(p.path("streamId").asText(null)) && head.equals(p.path("release"))
                && p.path("headVersion").isIntegralNumber() && p.path("headVersion").bigIntegerValue().signum() > 0
                && p.path("headVersion").bigIntegerValue().compareTo(Canonical.MAX_SAFE) <= 0 && subject.size() == 2
                && headFile.equals(subject.get(0).path("name").asText(null)) && headSha.equals(subject.get(0).path("digest").path("sha256").asText(null))
                && releasePath.equals(subject.get(1).path("name").asText(null)) && releaseSha.equals(subject.get(1).path("digest").path("sha256").asText(null));
            signedAt = instant(Json.text(p, "signedAt")); expires = instant(Json.text(p, "expires"));
            if (!ok) throw new IllegalArgumentException("subject");
        } catch (IllegalArgumentException | DateTimeParseException e) {
            throw new Refusal("head-unverified", "stream", stream, "rule", "subject", "expected", headSha);
        }
        Instant now = Instant.ofEpochMilli(nowMs);
        if (signedAt.isAfter(now)) throw new Refusal("head-unverified", "stream", stream, "rule", "signed-in-future", "signedAt", signedAt.toString(), "now", now.toString());
        String statementSha = Canonical.digestOf(body); long v = p.path("headVersion").asLong();
        // Rollback is judged before expiry (Vor's order), and the floor is read, compared and raised inside one lock hold;
        // an expired head is refused but never raises the floor.
        boolean expired = !now.isBefore(expires);
        FloorStore.Outcome outcome = floors.update(stream, v, statementSha, !expired);
        if (outcome instanceof FloorStore.Outcome.Rollback r)
            throw new Refusal("head-rollback", "stream", stream, "floor", r.floor().headVersion(), "candidate", v, "recordedStatementSha256", r.floor().statementSha256(), "candidateStatementSha256", statementSha);
        if (expired) throw new Refusal("head-expired", "stream", stream, "expires", expires.toString(), "now", now.toString());
        return new Head(stream, releaseId, version, releaseSha, "mappings/" + headFile, headSha, "mappings/" + attFile, Canonical.digestOf(attBytes), statementSha, v,
                        signedAt.toString(), expires.toString(), published, approved);
    }
    static Instant instant(String text) { if (!INSTANT.matcher(text).matches()) throw new IllegalArgumentException(text); return Instant.parse(text); }
    static PublicKey signer(JsonNode envelope, byte[] message, List<PublicKey> keys) {
        for (JsonNode s : envelope.path("signatures")) {
            if (!s.path("sig").isTextual()) continue; byte[] raw;
            try { raw = Dsse.decode(s.path("sig").asText()); } catch (IllegalArgumentException e) { continue; }
            for (PublicKey k : keys) if (Dsse.verify(message, raw, k)) return k;
        }
        return null;
    }
}
