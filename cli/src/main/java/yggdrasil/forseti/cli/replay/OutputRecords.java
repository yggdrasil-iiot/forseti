package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.Delete;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.stage1.QuarantineRecord;

/** The output records of an engine run on a transport, and the step-3 journal lines they are dumped to (plan D10). A step-3 journal
 *  line ({@code JournalFiles}) is already the image of a transport record {@code {key, headers, value}}; on a transport each part is
 *  carried as itself: the key as the RFC 8785 bytes of the line's {@code key} (a null key for a progress record and a delete), the
 *  headers as UTF-8 string headers (every record carries {@code forseti-generation}, spec 567), the value as the RFC 8785 bytes of the
 *  line's {@code value}. The dump reverses it by the kind of the value: a progress record is the line {@code {key: null, value}}, a
 *  delete {@code {value}} (no key, no headers, as the step-3 journals write them), every other record {@code {key, headers, value}}.
 *  The value bytes are never re-serialized on the way: the dump parses them only to read their kind. */
public final class OutputRecords {
    private OutputRecords() {}

    public static final String GENERATION_HEADER = CutBarrierRecord.GENERATION_HEADER;

    /** One transport record: key bytes (null for none), string headers, value bytes. */
    public record Transport(byte[] key, Map<String, String> headers, byte[] value) {
        public Transport { headers = Collections.unmodifiableMap(new TreeMap<>(headers)); }
    }

    public enum Kind { PROGRESS, DELETE, RECORD }

    // --- journal lines, as the step-3 JournalFiles write them -----------------------------------------------------------------

    public static ObjectNode rowLine(Row r) {
        ObjectNode n = Json.object();
        n.set("key", r.logicalKey());
        n.set("headers", headers(r.generation()));
        ObjectNode v = n.putObject("value");
        v.set("body", r.body());
        v.put("bodyDigest", r.bodyDigest());
        v.set("envelope", r.envelope().toJson());
        return n;
    }

    public static ObjectNode deleteLine(Delete d) {
        ObjectNode n = Json.object();
        n.putObject("value").set("delete", d.toJson());
        return n;
    }

    public static ObjectNode progressLine(Progress p) {
        ObjectNode n = Json.object();
        n.putNull("key");
        n.set("value", p.toJson());
        return n;
    }

    public static ObjectNode dispositionLine(Disposition d) {
        ObjectNode n = Json.object();
        n.set("key", d.key().toJson());
        n.set("headers", headers(d.generation()));
        ObjectNode v = n.putObject("value");
        v.set("body", d.body().toJson());
        Json.putSafe(v, "revision", d.revision());
        return n;
    }

    public static ObjectNode quarantineLine(QuarantineRecord q) {
        ObjectNode n = Json.object();
        n.set("key", QuarantineRecord.ref(q.origin()));
        n.set("headers", headers(q.generation()));
        n.set("value", q.toJson());
        return n;
    }

    /** A value as read back: an integer literal beyond +-(2^53 - 1) is the double JCS printed it from (a body double at or beyond 2^53),
     *  so the value canonicalizes again to the same bytes (the gate's journal reader does the same). */
    public static JsonNode readValue(byte[] bytes) { return doublesBeyondSafe(Json.read(bytes)); }

    private static final java.math.BigInteger MAX_SAFE = java.math.BigInteger.valueOf(Json.MAX_SAFE);

    private static JsonNode doublesBeyondSafe(JsonNode n) {
        if (n instanceof ObjectNode o) {
            java.util.List<String> names = new java.util.ArrayList<>();
            o.fieldNames().forEachRemaining(names::add);
            for (String f : names) { JsonNode v = o.get(f), w = doublesBeyondSafe(v); if (w != v) o.set(f, w); }
            return o;
        }
        if (n instanceof com.fasterxml.jackson.databind.node.ArrayNode a) {
            for (int i = 0; i < a.size(); i++) { JsonNode v = a.get(i), w = doublesBeyondSafe(v); if (w != v) a.set(i, w); }
            return a;
        }
        if (n != null && n.isIntegralNumber() && n.bigIntegerValue().abs().compareTo(MAX_SAFE) > 0)
            return com.fasterxml.jackson.databind.node.DoubleNode.valueOf(n.bigIntegerValue().doubleValue());
        return n;
    }

    static ObjectNode headers(long generation) { return Json.object().put(GENERATION_HEADER, Long.toString(generation)); }

    // --- line <-> transport record --------------------------------------------------------------------------------------------

    public static Kind kind(JsonNode value) {
        if ("progress".equals(value.path("type").textValue())) return Kind.PROGRESS;
        if (value.size() == 1 && value.has("delete")) return Kind.DELETE;
        return Kind.RECORD;
    }

    /** The transport record of a journal line. */
    public static Transport transport(JsonNode line) {
        JsonNode v = line.get("value");
        if (v == null || !v.isObject()) throw new IllegalArgumentException("a journal line has an object value");
        Kind k = kind(v);
        Map<String, String> h = new TreeMap<>();
        if (k == Kind.RECORD) {
            line.path("headers").fields().forEachRemaining(e -> h.put(e.getKey(), Json.text(line.path("headers"), e.getKey())));
            if (!h.containsKey(GENERATION_HEADER)) throw new IllegalArgumentException("a journal record without " + GENERATION_HEADER);
        } else {
            JsonNode g = k == Kind.PROGRESS ? v.get("generation") : v.path("delete").get("generation");
            h.put(GENERATION_HEADER, Long.toString(Json.safeLong(g, "generation", 1, Json.MAX_SAFE)));
        }
        JsonNode key = line.get("key");
        byte[] kb = k == Kind.RECORD && key != null && !key.isNull() ? Canonical.bytes(key) : null;
        return new Transport(kb, h, Canonical.bytes(v));
    }

    /** The journal line of an output record of the run of {@code generation}: every record, progress and delete included, must carry
     *  the header {@code forseti-generation} naming exactly that generation, and a progress or delete value must name it too
     *  (an {@link IllegalArgumentException} otherwise). */
    public static ObjectNode line(Transport t, long generation) {
        String h = t.headers().get(GENERATION_HEADER);
        if (!Long.toString(generation).equals(h))
            throw new IllegalArgumentException("an output record with " + GENERATION_HEADER + " " + h + " in the run of generation " + generation);
        ObjectNode n = line(t);
        JsonNode v = n.get("value");
        JsonNode g = switch (kind(v)) { case PROGRESS -> v.get("generation"); case DELETE -> v.path("delete").get("generation"); case RECORD -> null; };
        if (kind(v) != Kind.RECORD && (g == null || !g.isIntegralNumber() || g.longValue() != generation))
            throw new IllegalArgumentException("a " + kind(v).name().toLowerCase(java.util.Locale.ROOT) + " record of generation " + g + " in the run of generation " + generation);
        return n;
    }

    /** Refuses a record whose key or value bytes are not already their own RFC 8785 bytes (the dump re-serializes nothing, so a
     *  non-canonical record would change bytes on the way). */
    public static void requireCanonical(Transport t) {
        if (t.key() != null && !java.util.Arrays.equals(t.key(), Canonical.bytes(readValue(t.key()))))
            throw new IllegalArgumentException("an output record key is not RFC 8785 canonical bytes");
        if (t.value() == null || !java.util.Arrays.equals(t.value(), Canonical.bytes(readValue(t.value()))))
            throw new IllegalArgumentException("an output record value is not RFC 8785 canonical bytes");
    }

    /** The journal line of a transport record, its parts as read (the value parsed once, for its kind); no generation check. */
    public static ObjectNode line(Transport t) {
        if (t.value() == null) throw new IllegalArgumentException("an output record without a value");
        JsonNode v = readValue(t.value());
        if (!v.isObject()) throw new IllegalArgumentException("an output record value is a JSON object");
        ObjectNode n = Json.object();
        switch (kind(v)) {
            case PROGRESS -> {
                if (t.key() != null) throw new IllegalArgumentException("a progress record carries a key");
                n.putNull("key");
                n.set("value", v);
            }
            case DELETE -> {
                if (t.key() != null) throw new IllegalArgumentException("a delete record carries a key");
                n.set("value", v);
            }
            case RECORD -> {
                if (t.key() == null) throw new IllegalArgumentException("an output record without a key");
                n.set("key", Json.read(t.key()));
                ObjectNode h = n.putObject("headers");
                t.headers().forEach(h::put);
                n.set("value", v);
            }
        }
        return n;
    }
}
