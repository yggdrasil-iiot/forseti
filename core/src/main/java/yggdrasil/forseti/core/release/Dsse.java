package yggdrasil.forseti.core.release;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Base64;

public final class Dsse {
    public static final String IN_TOTO = "application/vnd.in-toto+json";
    private Dsse() {}
    /** "DSSEv1" SP LEN(type) SP type SP LEN(body) SP body, lengths as ASCII decimal. */
    public static byte[] pae(String type, byte[] body) {
        byte[] t = type.getBytes(StandardCharsets.UTF_8); ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(("DSSEv1 " + t.length + " ").getBytes(StandardCharsets.US_ASCII)); out.writeBytes(t);
        out.writeBytes((" " + body.length + " ").getBytes(StandardCharsets.US_ASCII)); out.writeBytes(body); return out.toByteArray();
    }
    /** The envelope spec allows standard or URL-safe Base64. */
    public static byte[] decode(String b64) {
        try { return Base64.getDecoder().decode(b64); } catch (IllegalArgumentException e) { return Base64.getUrlDecoder().decode(b64); }
    }
    public static boolean verify(byte[] message, byte[] signature, PublicKey key) {
        try { Signature s = Signature.getInstance("Ed25519"); s.initVerify(key); s.update(message); return s.verify(signature); }
        catch (Exception e) { return false; }
    }
}
