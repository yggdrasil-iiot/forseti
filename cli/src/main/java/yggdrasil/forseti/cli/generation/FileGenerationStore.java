package yggdrasil.forseti.cli.generation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationPolicy;
import yggdrasil.forseti.core.generation.GenerationStore;
import yggdrasil.forseti.core.release.LockedFile;

/** The step-3 generation store: {@code <stateDir>/generations.jsonl}, one transport record per line
 *  {@code {"topic": "forseti.generations", "key": "<scope>", "headers": {"forseti-generation": "<n>"}, "value": <the generation row>}},
 *  written as its RFC 8785 bytes and an LF, so the raw SHA-256 of a stored line is {@code Canonical.digest} of its record.
 *  Every read validates the whole history ({@link GenerationPolicy#validate}). A last line without its LF, a line that is not the
 *  canonical bytes of a transport record, a header that disagrees with its row, or an invalid history fails closed
 *  ({@code control-log-corrupt}); nothing is skipped or discarded. {@link #append} runs inside the {@link LockedFile} hold of
 *  {@code <stateDir>/generations.lock} ({@link #locked}, which a verb holds for its whole operation), checks that the history stays
 *  valid with the new row, appends it through one channel opened for append and forces it to disk. */
public final class FileGenerationStore implements GenerationStore {
    public static final String FILE = "generations.jsonl", LOCK = "generations.lock", TOPIC = "forseti.generations", HEADER = "forseti-generation";

    /** Opens the channel an append writes through (a test substitutes a spy). */
    @FunctionalInterface
    interface Channels { FileChannel open(Path file) throws IOException; }

    private static final Channels APPEND = f -> FileChannel.open(f, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);

    /** One stored line: its bytes without the LF and the entry they hold. */
    record Line(byte[] bytes, Entry entry) {}

    private final Path stateDir, file;
    private final Channels channels;

    public FileGenerationStore(Path stateDir) { this(stateDir, APPEND); }

    FileGenerationStore(Path stateDir, Channels channels) {
        this.stateDir = stateDir;
        this.file = stateDir.resolve(FILE);
        this.channels = channels;
    }

    public Path file() { return file; }
    public Path lockFile() { return stateDir.resolve(LOCK); }

    /** Runs {@code body} inside the hold of {@code generations.lock}; the hold is re-entrant for the holding thread. */
    public <T> T locked(LockedFile.Body<T> body) {
        try { return LockedFile.with(lockFile(), body); } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    /** The transport record of a row. */
    public static ObjectNode record(String scope, Generation row) {
        ObjectNode n = Json.object().put("topic", TOPIC).put("key", scope);
        n.putObject("headers").put(HEADER, Long.toString(row.generation()));
        n.set("value", row.toJson());
        return n;
    }

    @Override public void append(String scope, Generation row) {
        locked(() -> {
            List<Entry> next = new ArrayList<>(all());
            next.add(new Entry(scope, row));
            try { GenerationPolicy.validate(next); }
            catch (IllegalStateException e) { throw new IllegalArgumentException("the row would make the history invalid: " + e.getMessage(), e); }
            byte[] b = Canonical.bytes(record(scope, row));
            byte[] withLf = Arrays.copyOf(b, b.length + 1);
            withLf[b.length] = '\n';
            Files.createDirectories(stateDir);
            try (FileChannel ch = channels.open(file)) {
                ByteBuffer buf = ByteBuffer.wrap(withLf);
                while (buf.hasRemaining()) ch.write(buf);
                ch.force(true);
            }
            return null;
        });
    }

    /** The publication of a promotion failed: a write or {@code force} threw, or the channel stopped accepting bytes. {@code appended}
     *  bytes of the batch reached the channel; the file may hold that prefix of the batch, which every later read refuses
     *  ({@code control-log-corrupt}) unless it is empty. Never a success. */
    public static final class ControlLogIo extends RuntimeException {
        public static final String CODE = "control-log-io";
        public final long appended;
        ControlLogIo(long appended, String detail, Throwable cause) { super(CODE + ": " + detail, cause); this.appended = appended; }
    }

    /** Write calls in a row that accept no byte before the channel counts as no longer accepting bytes. */
    static final int STALLED_WRITES = 16;

    /** Appends {@code rows} as one buffer (a promotion batch, plan Step 21.2) inside the hold of {@code generations.lock}: the history must
     *  stay valid with every row ({@link IllegalArgumentException} otherwise, nothing written); the buffer is written at the end of the
     *  file by a loop that continues a short write until the buffer is drained, then {@code force(true)}; this returns only after
     *  {@code force} returned. Any I/O failure during the publication is {@link ControlLogIo} with the count of bytes appended. */
    public void appendBatch(List<Entry> rows) {
        if (rows.isEmpty()) throw new IllegalArgumentException("an empty batch");
        locked(() -> {
            List<Entry> next = new ArrayList<>(all());
            next.addAll(rows);
            try { GenerationPolicy.validate(next); }
            catch (IllegalStateException e) { throw new IllegalArgumentException("the batch would make the history invalid: " + e.getMessage(), e); }
            java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
            for (Entry e : rows) { buf.writeBytes(Canonical.bytes(record(e.scope(), e.row()))); buf.write('\n'); }
            Files.createDirectories(stateDir);
            ByteBuffer b = ByteBuffer.wrap(buf.toByteArray());
            long appended = 0;
            FileChannel ch;
            try { ch = channels.open(file); } catch (IOException e) { throw new ControlLogIo(0, "open: " + e.getMessage(), e); }
            try {
                int stalled = 0;
                while (b.hasRemaining()) {
                    int w;
                    try { w = ch.write(b); } catch (IOException | RuntimeException e) { throw new ControlLogIo(appended, "write: " + e, e); }
                    if (w > 0) { appended += w; stalled = 0; }
                    else if (++stalled >= STALLED_WRITES) throw new ControlLogIo(appended, "the channel stopped accepting bytes", null);
                }
                try { ch.force(true); } catch (IOException | RuntimeException e) { throw new ControlLogIo(appended, "force: " + e, e); }
            } finally {
                try { ch.close(); } catch (IOException ignored) { /* the outcome is decided by the write and force above */ }
            }
            return null;
        });
    }

    @Override public List<Entry> all() { return lines().stream().map(Line::entry).toList(); }

    /** The raw bytes of the whole log, after the same validation as every read. */
    public byte[] bytes() {
        byte[] b = read();
        parse(b);
        return b;
    }

    /** {@code registeredRowSha256}: {@code Canonical.digest} of the transport record of the {@code registered} row of {@code n}, which is
     *  the raw SHA-256 of its stored line without the LF. */
    public Optional<String> registeredRowSha256(long generation) {
        for (Line l : lines())
            if (l.entry().row().generation() == generation)
                return Generation.REGISTERED.equals(l.entry().row().status()) ? Optional.of(Canonical.digestOf(l.bytes())) : Optional.empty();
        return Optional.empty();
    }

    List<Line> lines() { return parse(read()); }

    private byte[] read() {
        if (!Files.exists(file)) return new byte[0];
        try { return Files.readAllBytes(file); } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    private List<Line> parse(byte[] bytes) {
        List<Line> out = new ArrayList<>();
        if (bytes.length == 0) return out;
        if (bytes[bytes.length - 1] != '\n') throw corrupt("the last line has no LF");
        int start = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != '\n') continue;
            byte[] line = Arrays.copyOfRange(bytes, start, i);
            start = i + 1;
            int number = out.size();
            JsonNode n;
            try { n = Json.read(line); } catch (IllegalArgumentException e) { throw corrupt("line " + number + " is not JSON"); }
            if (!n.isObject() || n.size() != 4 || !TOPIC.equals(n.path("topic").textValue()) || !n.path("key").isTextual()
                || !n.path("headers").isObject() || n.path("headers").size() != 1 || !n.path("value").isObject())
                throw corrupt("line " + number + " is not a transport record");
            if (!Arrays.equals(line, Canonical.bytes(n))) throw corrupt("line " + number + " is not the canonical bytes of its record");
            Generation g;
            try { g = Generation.fromJson(n.get("value")); } catch (IllegalArgumentException e) { throw corrupt("line " + number + ": " + e.getMessage()); }
            if (!Long.toString(g.generation()).equals(n.path("headers").path(HEADER).textValue())) throw corrupt("line " + number + " header " + HEADER);
            out.add(new Line(line, new Entry(n.get("key").textValue(), g)));
        }
        try { GenerationPolicy.validate(out.stream().map(Line::entry).toList()); }
        catch (IllegalStateException e) { throw corrupt(e.getMessage().substring(GenerationPolicy.CORRUPT.length() + 2)); }
        return List.copyOf(out);
    }

    private IllegalStateException corrupt(String what) { return new IllegalStateException(GenerationPolicy.CORRUPT + ": " + file.getFileName() + ": " + what); }
}
