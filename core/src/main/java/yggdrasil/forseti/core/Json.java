package yggdrasil.forseti.core;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.Separators;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

/** The one JSON mapper of forseti: strict parsing, no NaN/Infinity, duplicate keys refused, pretty output in the shape Tyr writes.
 *  It also carries forseti's one integer policy: every integer read from a file goes through {@link #safeLong}, every integer
 *  written through {@link #putSafe}, so schema ranges, Java longs and the JCS domain agree by construction. */
public final class Json {
    public static final ObjectMapper JSON = new ObjectMapper()
        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
        .disable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)
        .disable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);   // "{} {}" is two values, not JSON; Python's json.loads refuses it too (Extra data)

    /** 2^53 - 1, the largest integer both JCS libraries print identically. */
    public static final long MAX_SAFE = 9007199254740991L;
    /** A closed range of integers a reader accepts. */
    public record Range(long min, long max) {
        public Range {
            if (min < -MAX_SAFE || max > MAX_SAFE || min > max) throw new IllegalArgumentException("range outside the JCS domain: " + min + ".." + max);
        }
    }
    /** Record offsets, counts, epochs, partitions and the business times of inputs (their schemas say minimum 0). */
    public static final Range NON_NEGATIVE = new Range(0, MAX_SAFE);
    /** Inclusive through-offsets: -1 means "no record of that partition included" (spec 406). */
    public static final Range THROUGH = new Range(-1, MAX_SAFE);
    /** Computed times (W1, W2, admissionW, eventTimeAt), which the spec never bounds. */
    public static final Range ANY = new Range(-MAX_SAFE, MAX_SAFE);

    private Json() {}

    public static JsonNode read(byte[] b) {
        try { return JSON.readTree(b); }
        catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw new IllegalArgumentException("not-json: " + e.getOriginalMessage(), e); }
        catch (IOException e) { throw new IllegalArgumentException("not-json: " + e.getMessage(), e); }
    }
    public static JsonNode read(Path p) { try { return read(Files.readAllBytes(p)); } catch (IOException e) { throw new IllegalArgumentException(p + ": " + e.getMessage(), e); } }
    public static ObjectNode object() { return JSON.createObjectNode(); }
    public static byte[] pretty(JsonNode n) {
        DefaultIndenter two = new DefaultIndenter("  ", "\n");
        DefaultPrettyPrinter pp = new TyrShape(new DefaultPrettyPrinter().withObjectIndenter(two).withArrayIndenter(two)
            .withSeparators(Separators.createDefaultInstance().withObjectFieldValueSpacing(Separators.Spacing.AFTER)));
        try { return (JSON.writer(pp).writeValueAsString(n) + "\n").getBytes(StandardCharsets.UTF_8); } catch (IOException e) { throw new IllegalStateException(e); }
    }

    /** Python's json.dumps(indent=2), the shape Tyr writes, prints an empty array as [] and an empty object as {}; Jackson's
     *  default printer puts a space inside both. */
    static final class TyrShape extends DefaultPrettyPrinter {
        TyrShape(DefaultPrettyPrinter base) { super(base); }
        @Override public DefaultPrettyPrinter createInstance() { return new TyrShape(this); }
        @Override public void writeEndArray(JsonGenerator g, int nrOfValues) throws IOException {
            if (nrOfValues > 0) { super.writeEndArray(g, nrOfValues); return; }
            if (!_arrayIndenter.isInline()) --_nesting;
            g.writeRaw(']');
        }
        @Override public void writeEndObject(JsonGenerator g, int nrOfEntries) throws IOException {
            if (nrOfEntries > 0) { super.writeEndObject(g, nrOfEntries); return; }
            if (!_objectIndenter.isInline()) --_nesting;
            g.writeRaw('}');
        }
    }
    public static void write(Path p, JsonNode n) { try { Files.createDirectories(p.getParent()); Files.write(p, pretty(n)); } catch (IOException e) { throw new IllegalStateException(p.toString(), e); } }
    public static String text(JsonNode n, String k) { JsonNode v = n.path(k); if (!v.isTextual()) throw new IllegalArgumentException("missing text field " + k); return v.asText(); }

    /** The integer at {@code n} when it is integral and inside {@code [min, max]}; {@code schema-invalid: <field>} otherwise. The value is
     *  compared as a BigInteger, so 2^53, Long.MAX_VALUE + 2 and larger never reach a long conversion. */
    public static long safeLong(JsonNode n, String field, Range range) {
        if (n == null || !n.isIntegralNumber()) throw new IllegalArgumentException("schema-invalid: " + field);
        BigInteger v = n.bigIntegerValue();
        if (v.compareTo(BigInteger.valueOf(range.min())) < 0 || v.compareTo(BigInteger.valueOf(range.max())) > 0) throw new IllegalArgumentException("schema-invalid: " + field);
        return v.longValue();
    }
    public static long safeLong(JsonNode n, String field, long min, long max) { return safeLong(n, field, new Range(min, max)); }
    /** Writes a long; a forseti-written integer outside the JCS domain is a programming error, never a wire value. */
    public static ObjectNode putSafe(ObjectNode o, String field, long v) {
        if (v > MAX_SAFE || v < -MAX_SAFE) throw new IllegalStateException("integer outside the JCS domain: " + field + "=" + v);
        return o.put(field, v);
    }
}
