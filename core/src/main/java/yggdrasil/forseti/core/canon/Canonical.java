package yggdrasil.forseti.core.canon;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Iterator;
import org.erdtman.jcs.JsonCanonicalizer;

/** RFC 8785 (JCS) bytes and SHA-256 of a JSON value (Decision 7). The pinned library is io.github.erdtman
 *  java-json-canonicalization 1.1; forseti refuses, before the library runs, what the two pinned libraries would not
 *  print identically: non-finite numbers, integers outside [-(2^53-1), 2^53-1], unpaired surrogates. The oracle applies
 *  the same refusals in its own code; the shared vector file pins both. digestOf(bytes) is the raw-file digest and is
 *  never mixed with digest(node), the canonical one. */
public final class Canonical {
    public static final BigInteger MAX_SAFE = BigInteger.valueOf(9007199254740991L);
    private Canonical() {}

    public static byte[] bytes(JsonNode value) {
        check(value);
        try { return new JsonCanonicalizer(Json.JSON.writeValueAsString(value)).getEncodedUTF8(); }
        catch (Exception e) { throw new IllegalArgumentException("not canonicalizable: " + e.getMessage(), e); }
    }
    public static String digest(JsonNode value) { return digestOf(bytes(value)); }
    public static String digestOf(byte[] b) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b)); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    static void check(JsonNode n) {
        if (n.isFloatingPointNumber()) { double d = n.doubleValue(); if (Double.isNaN(d) || Double.isInfinite(d)) throw new IllegalArgumentException("non-finite"); }
        else if (n.isIntegralNumber()) { BigInteger i = n.bigIntegerValue(); if (i.abs().compareTo(MAX_SAFE) > 0) throw new IllegalArgumentException("integer-domain"); }
        else if (n.isTextual()) { String s = n.textValue(); for (int i = 0; i < s.length(); i++) { char c = s.charAt(i);
            if (Character.isHighSurrogate(c)) { if (i + 1 >= s.length() || !Character.isLowSurrogate(s.charAt(i + 1))) throw new IllegalArgumentException("not-json: lone surrogate"); i++; }
            else if (Character.isLowSurrogate(c)) throw new IllegalArgumentException("not-json: lone surrogate"); } }
        else if (n.isObject()) { for (Iterator<String> it = n.fieldNames(); it.hasNext();) check(new com.fasterxml.jackson.databind.node.TextNode(it.next())); for (Iterator<JsonNode> it = n.elements(); it.hasNext();) check(it.next()); }
        else if (n.isArray()) { for (Iterator<JsonNode> it = n.elements(); it.hasNext();) check(it.next()); }
    }
    public static String utf8(byte[] b) { return new String(b, StandardCharsets.UTF_8); }
}
