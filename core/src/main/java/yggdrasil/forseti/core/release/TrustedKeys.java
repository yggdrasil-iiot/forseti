package yggdrasil.forseti.core.release;

import yggdrasil.forseti.core.Json;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;

/** principal -> Ed25519 public keys from JSON lines {"principal", "publicKey"} (X.509 SubjectPublicKeyInfo, base64), the file
 *  Bifrost's `identity keygen` prints and Vor reads. The operator gives it to forseti outside the registry. */
public final class TrustedKeys {
    private final Map<String, List<PublicKey>> keys = new HashMap<>();
    public static TrustedKeys load(Path file) throws java.io.IOException {
        TrustedKeys t = new TrustedKeys();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            var e = Json.read(line.getBytes(StandardCharsets.UTF_8));
            t.keys.computeIfAbsent(Json.text(e, "principal"), k -> new ArrayList<>()).add(publicKey(Json.text(e, "publicKey")));
        }
        return t;
    }
    public static PublicKey publicKey(String b64) {
        try { return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(b64))); }
        catch (GeneralSecurityException | IllegalArgumentException e) { throw new IllegalArgumentException("not an Ed25519 public key", e); }
    }
    public List<PublicKey> of(String principal) { return keys.getOrDefault(principal, List.of()); }
}
