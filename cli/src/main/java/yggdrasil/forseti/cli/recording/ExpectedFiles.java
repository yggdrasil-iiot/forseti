package yggdrasil.forseti.cli.recording;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.core.Json;

/** The two layouts of {@code expected.json}.
 *
 *  <p>Version 1: one file, {@code samples} inline. Version 2 (written by the live recorder): {@code expected.json} holds every field of
 *  version 1 except {@code samples}; in its place {@code sampleFiles} (the chunk files, relative names, in order),
 *  {@code samplesPerFile} and {@code sampleCount}. Chunk {@code i} is {@code expected-samples/<iiii>.json},
 *  {@code {"chunk": i, "samples": {...}}}, the next {@code samplesPerFile} entries of {@code samples} in the version-1 order; every
 *  chunk but the last is full, the last holds 1 to {@code samplesPerFile}; no samples, no chunk. Every file is written by
 *  {@link Json#pretty}. {@link #read} reads either layout and returns the version-1 form. */
public final class ExpectedFiles {
    public static final String SAMPLE_DIR = "expected-samples";
    /** Samples per chunk of a live recording: about 6 MB pretty-printed per full chunk. */
    public static final int SAMPLES_PER_FILE = 20_000;

    private ExpectedFiles() {}

    public static String chunkName(int i) { return String.format(Locale.ROOT, "%s/%04d.json", SAMPLE_DIR, i); }

    /** The files of {@code v1} in version 2 (relative name to bytes): {@code expected.json} and the chunks. */
    public static TreeMap<String, byte[]> split(JsonNode v1, int samplesPerFile) {
        if (samplesPerFile < 1) throw new IllegalArgumentException("samplesPerFile " + samplesPerFile);
        if (!v1.isObject() || v1.path("expectedVersion").asInt(-1) != 1 || v1.has("samplesEncoding") || !v1.path("samples").isObject())
            throw new IllegalArgumentException("expected.json: only a version-1 truth with an inline samples object is split");
        List<Map.Entry<String, JsonNode>> all = new ArrayList<>();
        v1.path("samples").fields().forEachRemaining(all::add);
        TreeMap<String, byte[]> files = new TreeMap<>();
        ArrayNode names = Json.JSON.createArrayNode();
        for (int i = 0; i * (long) samplesPerFile < all.size(); i++) {
            ObjectNode c = Json.object();
            Json.putSafe(c, "chunk", i);
            ObjectNode s = c.putObject("samples");
            for (Map.Entry<String, JsonNode> e : all.subList(i * samplesPerFile, Math.min(all.size(), (i + 1) * samplesPerFile))) s.set(e.getKey(), e.getValue());
            files.put(chunkName(i), Json.pretty(c));
            names.add(chunkName(i));
        }
        ObjectNode v2 = Json.object();
        for (Iterator<Map.Entry<String, JsonNode>> it = v1.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> e = it.next();
            switch (e.getKey()) {
                case "expectedVersion" -> Json.putSafe(v2, "expectedVersion", 2);
                case "samples" -> {
                    v2.set("sampleFiles", names);
                    Json.putSafe(v2, "samplesPerFile", samplesPerFile);
                    Json.putSafe(v2, "sampleCount", all.size());
                }
                default -> v2.set(e.getKey(), e.getValue());
            }
        }
        files.put(Recording.EXPECTED, Json.pretty(v2));
        return files;
    }

    public static TreeMap<String, byte[]> split(JsonNode v1) { return split(v1, SAMPLES_PER_FILE); }

    /** The truth of the recording in {@code dir}, version 1 or 2, in the version-1 form. A version-2 chunk list that is not
     *  {@code expected-samples/0000.json, 0001.json, ...}, a chunk file listed and absent or present and unlisted, a chunk whose
     *  {@code chunk} is not its place, a chunk that is not full (the last: empty or over full), a sample named twice, or a count that is
     *  not {@code sampleCount} is refused. */
    public static JsonNode read(Path dir) {
        JsonNode e = Json.read(dir.resolve(Recording.EXPECTED));
        long version = e.path("expectedVersion").isIntegralNumber() ? e.path("expectedVersion").longValue() : -1;
        if (version == 1) {
            if (Files.exists(dir.resolve(SAMPLE_DIR))) throw bad("a version-1 expected.json with " + SAMPLE_DIR + "/ present");
            return e;
        }
        if (version != 2) throw bad("expectedVersion " + e.path("expectedVersion"));
        if (e.has("samples") || e.has("samplesEncoding")) throw bad("version 2 carries samples or samplesEncoding inline");
        JsonNode list = e.path("sampleFiles");
        if (!list.isArray()) throw bad("sampleFiles is not an array");
        long per = Json.safeLong(e.get("samplesPerFile"), "samplesPerFile", Json.NON_NEGATIVE);
        long count = Json.safeLong(e.get("sampleCount"), "sampleCount", Json.NON_NEGATIVE);
        if (per < 1) throw bad("samplesPerFile 0");
        List<String> names = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            if (!list.get(i).isTextual() || !list.get(i).textValue().equals(chunkName(i)))
                throw bad("sampleFiles[" + i + "] is " + list.get(i) + ", not " + chunkName(i));
            names.add(chunkName(i));
        }
        Set<String> present = new TreeSet<>();
        Path sd = dir.resolve(SAMPLE_DIR);
        if (Files.exists(sd)) try (Stream<Path> s = Files.walk(sd)) {
            s.filter(p -> !p.equals(sd)).forEach(p -> present.add(Recording.relative(dir, p)));
        } catch (IOException x) { throw new UncheckedIOException(x); }
        if (!present.equals(new TreeSet<>(names))) throw bad("sample files present " + present + " differ from sampleFiles " + names);
        ObjectNode samples = Json.object();
        for (int i = 0; i < names.size(); i++) {
            JsonNode c = Json.read(dir.resolve(names.get(i)));
            if (!c.isObject() || c.size() != 2 || !c.path("samples").isObject()) throw bad(names.get(i) + " is not {chunk, samples}");
            if (Json.safeLong(c.get("chunk"), "chunk", Json.NON_NEGATIVE) != i) throw bad(names.get(i) + " says chunk " + c.get("chunk") + ", out of order");
            int n = c.path("samples").size();
            boolean last = i == names.size() - 1;
            if (last ? n < 1 || n > per : n != per) throw bad(names.get(i) + " holds " + n + " samples (" + per + " per file)");
            for (Iterator<Map.Entry<String, JsonNode>> it = c.path("samples").fields(); it.hasNext();) {
                Map.Entry<String, JsonNode> x = it.next();
                if (samples.has(x.getKey())) throw bad(names.get(i) + " repeats sample " + x.getKey());
                samples.set(x.getKey(), x.getValue());
            }
        }
        if (samples.size() != count) throw bad("the sample files hold " + samples.size() + " samples, sampleCount " + count);
        ObjectNode v1 = Json.object();
        for (Iterator<Map.Entry<String, JsonNode>> it = e.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> f = it.next();
            switch (f.getKey()) {
                case "expectedVersion" -> v1.put("expectedVersion", java.math.BigInteger.ONE);   // the node a parsed version-1 file holds (USE_BIG_INTEGER_FOR_INTS)
                case "sampleFiles" -> v1.set("samples", samples);
                case "samplesPerFile", "sampleCount" -> { }
                default -> v1.set(f.getKey(), f.getValue());
            }
        }
        return v1;
    }

    private static IllegalArgumentException bad(String what) { return new IllegalArgumentException("malformed expected.json: " + what); }
}
