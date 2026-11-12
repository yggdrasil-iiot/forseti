package yggdrasil.forseti.cli.recording;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** A recording directory in the shape the step-5 recorder writes from Kafka: {@code manifest.json}, {@code routing.json},
 *  {@code expected.json} and one {@code <topic>/<partition>.jsonl} per subscribed input partition, one record per line
 *  {@code {"offset","timestamp","key","headers","value"}}, offsets from 0 and contiguous, LF. {@link #open} reads the manifest and the
 *  routing strictly (every integer through {@link Json#safeLong}, inclusive through-offsets in {@link Json#THROUGH}); anything else is
 *  {@code IllegalArgumentException("malformed recording: ...")}. */
public final class Recording {
    public static final String MANIFEST = "manifest.json", ROUTING = "routing.json", EXPECTED = "expected.json";

    public record Record(long offset, Long timestamp, String key, JsonNode headers, JsonNode value) {}
    /** One batch: its cut id, the inclusive through-offset of every subscribed input partition ({@code -1}: nothing included yet). */
    public record Batch(long cutId, Map<String, Map<Integer, Long>> throughOffsets, JsonNode idleInputs) {
        public long through(String topic, int partition) {
            Long v = throughOffsets.getOrDefault(topic, Map.of()).get(partition);
            if (v == null) throw new IllegalArgumentException("partition not subscribed: " + topic + "/" + partition);
            return v;
        }
    }
    public record Deadline(String kind, String ref, long at, long cutA, long cutB) {}
    public record Range(long from, long to) {}

    private final Path dir;
    private final JsonNode manifest;
    private final RoutingManifest routing;
    private final long t0, scanMs, tailMs, settleTargetW;
    private final Map<String, Range> evidenceRange = new LinkedHashMap<>();
    private final List<Batch> batches = new ArrayList<>();
    private final List<Deadline> deadlines = new ArrayList<>();
    private final Map<String, String> digests = new LinkedHashMap<>();
    private final Map<String, List<Record>> cache = new HashMap<>();
    private final Map<String, Long> sizes = new HashMap<>();

    private Recording(Path dir, JsonNode m, RoutingManifest routing) {
        this.dir = dir; this.manifest = m; this.routing = routing;
        if (!m.isObject()) throw malformed("manifest is not an object");
        if (Json.safeLong(m.get("recordingVersion"), "recordingVersion", 1, 1) != 1) throw malformed("recordingVersion");
        t0 = Json.safeLong(m.get("T0"), "T0", Json.NON_NEGATIVE);
        scanMs = Json.safeLong(m.get("scanMs"), "scanMs", Json.NON_NEGATIVE);
        if (scanMs == 0) throw malformed("scanMs 0");
        tailMs = Json.safeLong(m.get("tailMs"), "tailMs", Json.NON_NEGATIVE);
        settleTargetW = Json.safeLong(m.get("settleTargetW"), "settleTargetW", Json.ANY);
        if (!m.path("evidenceRange").isObject()) throw malformed("evidenceRange");
        m.path("evidenceRange").fields().forEachRemaining(e -> {
            long from = Json.safeLong(e.getValue().get("from"), "evidenceRange.from", Json.NON_NEGATIVE);
            long to = Json.safeLong(e.getValue().get("to"), "evidenceRange.to", Json.NON_NEGATIVE);
            if (from > to) throw malformed("evidenceRange " + e.getKey());
            if (!routing.devices().contains(e.getKey())) throw malformed("evidenceRange of an undeclared device " + e.getKey());
            evidenceRange.put(e.getKey(), new Range(from, to));
        });
        readBatches(m.path("batches"));
        readDeadlines(m.path("deadlines"));
        if (!m.path("digests").isObject()) throw malformed("digests");
        m.path("digests").fields().forEachRemaining(e -> {
            String sha = e.getValue().asText("");
            if (!e.getValue().isTextual() || !sha.matches("[0-9a-f]{64}")) throw malformed("digest of " + e.getKey());
            if (e.getKey().equals(MANIFEST) || e.getKey().startsWith("/") || e.getKey().contains("\\") || e.getKey().contains(".."))
                throw malformed("digest path " + e.getKey());
            digests.put(e.getKey(), sha);
        });
    }

    private void readBatches(JsonNode bs) {
        if (!bs.isArray() || bs.isEmpty()) throw malformed("batches");
        long previous = 0;
        Map<String, Map<Integer, Long>> last = new HashMap<>();
        for (JsonNode b : bs) {
            long cutId = Json.safeLong(b.get("cutId"), "cutId", Json.NON_NEGATIVE);
            if (cutId <= previous) throw malformed("cutId " + cutId + " not increasing");
            previous = cutId;
            JsonNode to = b.path("throughOffsets");
            if (!to.isObject()) throw malformed("throughOffsets of cut " + cutId);
            Map<String, Map<Integer, Long>> through = new LinkedHashMap<>();
            for (String topic : RoutingManifest.TOPICS) {
                List<Integer> sub = routing.subscribed(topic);
                JsonNode t = to.path(topic);
                if (sub.isEmpty() && t.isMissingNode()) continue;
                if (!t.isObject() || t.size() != sub.size()) throw malformed("throughOffsets." + topic + " of cut " + cutId + " does not name the subscribed partitions");
                Map<Integer, Long> parts = new LinkedHashMap<>();
                for (int p : sub) {
                    long v = Json.safeLong(t.get(Integer.toString(p)), "throughOffsets." + topic + "." + p, Json.THROUGH);
                    Long before = last.getOrDefault(topic, Map.of()).get(p);
                    if (before != null && v < before) throw malformed("throughOffset of " + topic + "/" + p + " decreases at cut " + cutId);
                    parts.put(p, v);
                }
                through.put(topic, Collections.unmodifiableMap(parts));
                last.put(topic, parts);
            }
            for (Iterator<String> it = to.fieldNames(); it.hasNext();) {
                String topic = it.next();
                if (!through.containsKey(topic)) throw malformed("throughOffsets of an unsubscribed topic " + topic);
            }
            JsonNode idle = b.path("idleInputs");
            if (!idle.isArray()) throw malformed("idleInputs of cut " + cutId);
            batches.add(new Batch(cutId, Collections.unmodifiableMap(through), idle));
        }
    }

    private void readDeadlines(JsonNode ds) {
        if (!ds.isArray()) throw malformed("deadlines");
        Set<Long> cuts = new HashSet<>();
        batches.forEach(b -> cuts.add(b.cutId()));
        for (JsonNode d : ds) {
            long at = Json.safeLong(d.get("at"), "deadlines.at", Json.NON_NEGATIVE);
            long a = Json.safeLong(d.get("cutA"), "deadlines.cutA", Json.NON_NEGATIVE), b = Json.safeLong(d.get("cutB"), "deadlines.cutB", Json.NON_NEGATIVE);
            if (!cuts.contains(a) || !cuts.contains(b) || b != a + 1) throw malformed("deadline brackets " + a + ", " + b);
            deadlines.add(new Deadline(Json.text(d, "kind"), Json.text(d, "ref"), at, a, b));
        }
    }

    private static IllegalArgumentException malformed(String what) { return new IllegalArgumentException("malformed recording: " + what); }

    public static Recording open(Path dir) {
        if (!Files.isRegularFile(dir.resolve(MANIFEST))) throw malformed("no " + MANIFEST + " in " + dir);
        RoutingManifest routing = RoutingManifest.read(Json.read(dir.resolve(ROUTING)));
        return new Recording(dir, Json.read(dir.resolve(MANIFEST)), routing);
    }

    /** The records of one subscribed input partition, read strictly: offsets 0, 1, 2, ..., LF-terminated lines. The list is kept
     *  for the life of this object; a run over a large recording reads with {@link #reader} instead. */
    public List<Record> records(String topic, int partition) {
        subscribedOrRefuse(topic, partition);
        return cache.computeIfAbsent(topic + "/" + partition, k -> {
            List<Record> out = new ArrayList<>();
            try (Reader r = reader(topic, partition)) { for (Line l; (l = r.next()) != null;) out.add(l.record()); }
            return List.copyOf(out);
        });
    }

    /** The number of records of one subscribed input partition, every line read strictly as {@link #records} reads it, without
     *  keeping them. */
    public long size(String topic, int partition) {
        subscribedOrRefuse(topic, partition);
        String k = topic + "/" + partition;
        List<Record> cached = cache.get(k);
        if (cached != null) return cached.size();
        return sizes.computeIfAbsent(k, x -> {
            try (Reader r = reader(topic, partition)) {
                while (r.next() != null) { /* read strictly, keep nothing */ }
                return r.read();
            }
        });
    }

    private void subscribedOrRefuse(String topic, int partition) {
        if (!routing.subscribed(topic).contains(partition)) throw new IllegalArgumentException("partition not subscribed: " + topic + "/" + partition);
    }

    /** One line of a partition file: the record read strictly and the line's bytes as delivered (without the LF). */
    public record Line(Record record, byte[] bytes) {}

    /** A sequential strict reader of one subscribed partition, line by line, with the checks and messages of {@link #records}: the
     *  file ends with LF (checked first, as a whole-file read checks it), no line carries CR, offsets are 0, 1, 2, ... */
    public Reader reader(String topic, int partition) {
        subscribedOrRefuse(topic, partition);
        return new Reader(dir.resolve(topic).resolve(partition + ".jsonl"), topic + "/" + partition);
    }

    public static final class Reader implements AutoCloseable {
        private final String name;
        private final InputStream in;
        private final byte[] chunk = new byte[1 << 16];
        private int pos, end;
        private byte[] line = new byte[1 << 12];
        private long read;

        Reader(Path file, String name) {
            this.name = name;
            try {
                long size = Files.size(file);
                if (size > 0) {
                    try (SeekableByteChannel ch = Files.newByteChannel(file)) {
                        ByteBuffer last = ByteBuffer.allocate(1);
                        ch.position(size - 1);
                        if (ch.read(last) != 1 || last.get(0) != '\n') throw malformed(name + " does not end with LF");
                    }
                }
                in = Files.newInputStream(file);
            } catch (IOException e) { throw malformed("missing partition file " + name); }
        }

        /** The next line, null after the last one. */
        public Line next() {
            int n = 0;
            while (true) {
                if (pos == end) {
                    try { end = in.read(chunk, 0, chunk.length); } catch (IOException e) { throw new UncheckedIOException(e); }
                    pos = 0;
                    if (end <= 0) {
                        end = 0;
                        if (n != 0) throw malformed(name + " does not end with LF");
                        return null;
                    }
                }
                int i = pos;
                while (i < end && chunk[i] != '\n') { if (chunk[i] == '\r') throw malformed(name + " carries CR"); i++; }
                int len = i - pos;
                if (n + len > line.length) line = Arrays.copyOf(line, Math.max(line.length * 2, n + len));
                System.arraycopy(chunk, pos, line, n, len);
                n += len;
                pos = i;
                if (i < end) {
                    pos++;
                    byte[] bytes = Arrays.copyOf(line, n);
                    Record r = parseLine(bytes, read, name);
                    read++;
                    return new Line(r, bytes);
                }
            }
        }

        /** The number of lines returned so far. */
        public long read() { return read; }

        @Override public void close() { try { in.close(); } catch (IOException e) { throw new UncheckedIOException(e); } }
    }

    private static Record parseLine(byte[] bytes, long due, String name) {
        JsonNode line = Json.read(bytes);
        if (!line.isObject() || line.size() != 5) throw malformed(name + " line " + due + " is not {offset, timestamp, key, headers, value}");
        long offset = Json.safeLong(line.get("offset"), "offset", Json.NON_NEGATIVE);
        if (offset != due) throw malformed(name + " offset " + offset + " where " + due + " is due");
        JsonNode ts = line.get("timestamp");
        if (ts == null) throw malformed(name + " line " + offset + " has no timestamp");
        Long timestamp = ts.isNull() ? null : Json.safeLong(ts, "timestamp", Json.NON_NEGATIVE);
        JsonNode key = line.get("key"), headers = line.get("headers"), value = line.get("value");
        if (key == null || !key.isTextual() || headers == null || !headers.isObject() || value == null)
            throw malformed(name + " line " + offset + " key, headers or value");
        return new Record(offset, timestamp, key.textValue(), headers, value);
    }

    /** Every file of the directory except the manifest is listed in {@code digests} with its raw-byte SHA-256, every subscribed
     *  partition has its file, and nothing unlisted is present. */
    public void verifyDigests() {
        Set<String> present = new TreeSet<>();
        try (Stream<Path> s = Files.walk(dir)) {
            s.filter(Files::isRegularFile).forEach(p -> present.add(relative(dir, p)));
        } catch (IOException e) { throw new UncheckedIOException(e); }
        present.remove(MANIFEST);
        if (!present.equals(new TreeSet<>(digests.keySet())))
            throw malformed("files " + present + " differ from the manifest's digests " + new TreeSet<>(digests.keySet()));
        for (String topic : RoutingManifest.TOPICS)
            for (int p : routing.subscribed(topic))
                if (!digests.containsKey(topic + "/" + p + ".jsonl")) throw malformed("no file for subscribed partition " + topic + "/" + p);
        for (Map.Entry<String, String> e : digests.entrySet()) {
            String actual;
            try { actual = Canonical.digestOf(Files.readAllBytes(dir.resolve(e.getKey()))); } catch (IOException x) { throw new UncheckedIOException(x); }
            if (!actual.equals(e.getValue())) throw new IllegalArgumentException("digest mismatch: " + e.getKey() + " " + e.getValue() + " != " + actual);
        }
    }

    /** Every partition's through-offsets stay inside its records, and the last batch includes every record. */
    public void verifyBatches() {
        Batch lastBatch = batches.get(batches.size() - 1);
        for (String topic : RoutingManifest.TOPICS)
            for (int p : routing.subscribed(topic)) {
                long lastOffset = size(topic, p) - 1L;
                for (Batch b : batches) if (b.through(topic, p) > lastOffset) throw malformed("throughOffset beyond the records of " + topic + "/" + p);
                if (lastBatch.through(topic, p) != lastOffset) throw malformed("the last batch leaves records of " + topic + "/" + p + " out");
            }
    }

    static String relative(Path root, Path p) {
        List<String> parts = new ArrayList<>();
        for (Path x : root.relativize(p)) parts.add(x.toString());
        return String.join("/", parts);
    }

    public Path dir() { return dir; }
    public JsonNode manifest() { return manifest; }
    public RoutingManifest routing() { return routing; }
    public long t0() { return t0; }
    public long scanMs() { return scanMs; }
    public long tailMs() { return tailMs; }
    public long settleTargetW() { return settleTargetW; }
    public Map<String, Range> evidenceRange() { return Collections.unmodifiableMap(evidenceRange); }
    public List<Batch> batches() { return Collections.unmodifiableList(batches); }
    public List<Deadline> deadlines() { return Collections.unmodifiableList(deadlines); }
    public Map<String, String> digests() { return Collections.unmodifiableMap(digests); }
    public JsonNode expected() { return Json.read(dir.resolve(EXPECTED)); }
}
