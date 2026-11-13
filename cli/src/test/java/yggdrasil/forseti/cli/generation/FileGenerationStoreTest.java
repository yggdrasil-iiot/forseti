package yggdrasil.forseti.cli.generation;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.cli.generation.GenerationStoreMain.register;
import static yggdrasil.forseti.cli.generation.GenerationStoreMain.row;
import static yggdrasil.forseti.core.generation.Generation.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;

class FileGenerationStoreTest {
    @TempDir Path tmp;

    static void corrupt(Path state) {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new FileGenerationStore(state).all());
        assertTrue(e.getMessage().startsWith("control-log-corrupt: "), e.getMessage());
    }

    /** Writes rows as stored lines directly, bypassing the store's checks (a log another writer could have left). */
    static void writeRaw(Path state, List<GenerationStore.Entry> rows) throws IOException {
        Files.createDirectories(state);
        StringBuilder b = new StringBuilder();
        for (GenerationStore.Entry e : rows) b.append(Canonical.utf8(Canonical.bytes(FileGenerationStore.record(e.scope(), e.row())))).append('\n');
        Files.writeString(state.resolve(FileGenerationStore.FILE), b.toString(), StandardCharsets.UTF_8);
    }

    static GenerationStore.Entry e(String scope, long n, String status) { return new GenerationStore.Entry(scope, row(n, status)); }

    @Test void appendAndReadBack() {
        FileGenerationStore s = new FileGenerationStore(tmp);
        assertEquals(List.of(), s.all());
        s.append("line2", row(1, REGISTERED));
        s.append("line2", row(1, RETIRED));
        s.append("line2", row(2, REGISTERED));
        FileGenerationStore again = new FileGenerationStore(tmp);
        assertEquals(List.of(e("line2", 1, REGISTERED), e("line2", 1, RETIRED), e("line2", 2, REGISTERED)), again.all());
        assertEquals(List.of(e("line2", 1, REGISTERED), e("line2", 1, RETIRED)), again.byNumber(1));
        assertEquals(Optional.of(e("line2", 1, REGISTERED)), again.registered(1));
        assertEquals(Optional.of(RETIRED), again.latestStatus(1));
        assertEquals(Optional.of(REGISTERED), again.latestStatus(2));
        assertEquals(Optional.empty(), again.latestStatus(3));
        assertEquals(Optional.empty(), again.registered(3));
        assertEquals(2, again.highest());
        assertEquals(Optional.empty(), again.current("line2"));
        // an append that would make the history invalid is refused and writes nothing
        byte[] before = again.bytes();
        assertThrows(IllegalArgumentException.class, () -> again.append("line2", row(1, REPLAYING)));
        assertThrows(IllegalArgumentException.class, () -> again.append("line2", row(2, REGISTERED)));
        assertArrayEquals(before, again.bytes());
    }

    @Test void theKeyAndHeaderAreWritten() throws IOException {
        FileGenerationStore s = new FileGenerationStore(tmp);
        s.append("line2-plc", row(1, REGISTERED));
        List<String> lines = Files.readAllLines(s.file(), StandardCharsets.UTF_8);
        assertEquals(1, lines.size());
        JsonNode n = Json.read(lines.get(0).getBytes(StandardCharsets.UTF_8));
        assertEquals(List.of("headers", "key", "topic", "value"), fields(n));
        assertEquals("forseti.generations", n.get("topic").asText());
        assertEquals("line2-plc", n.get("key").asText());
        assertEquals(Json.object().put("forseti-generation", "1"), n.get("headers"));
        assertArrayEquals(Canonical.bytes(row(1, REGISTERED).toJson()), Canonical.bytes(n.get("value")));
        assertTrue(n.get("value").has("run") && n.get("value").get("run").isNull());
        assertEquals(List.of("generation", "status", "rulesDigest", "inputReleases", "replayFrom", "headVersion", "releaseEvidence",
            "gateReportSha256", "run", "at"), fields(row(1, REGISTERED).toJson()));
    }

    @Test void twoThreadsAllocateDistinctNumbers() throws Exception {
        int threads = 4, each = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CyclicBarrier start = new CyclicBarrier(threads);
            List<Future<List<Long>>> fs = new ArrayList<>();
            for (int t = 0; t < threads; t++) fs.add(pool.submit(() -> {
                start.await();
                List<Long> mine = new ArrayList<>();
                // one store instance per registration, as separate verbs of one JVM would hold
                for (int i = 0; i < each; i++) mine.add(register(new FileGenerationStore(tmp), "line2"));
                return mine;
            }));
            Set<Long> all = new TreeSet<>();
            for (Future<List<Long>> f : fs) all.addAll(f.get(60, TimeUnit.SECONDS));   // an OverlappingFileLockException would surface here
            Set<Long> want = new TreeSet<>();
            for (long n = 1; n <= threads * each; n++) want.add(n);
            assertEquals(want, all);
        } finally { pool.shutdownNow(); }
        assertEquals(threads * each, new FileGenerationStore(tmp).all().size());
    }

    @Test void twoProcessesAllocateDistinctNumbers() throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        for (int round = 0; round < 3; round++) {
            Path state = tmp.resolve("round-" + round), go = tmp.resolve("go-" + round);
            List<Process> ps = new ArrayList<>();
            for (String scope : List.of("line2", "line2-plc"))
                ps.add(new ProcessBuilder(java, "-cp", cp, GenerationStoreMain.class.getName(), state.toString(), scope, go.toString())
                    .redirectErrorStream(true).start());
            Thread.sleep(500);
            Files.writeString(go, "go");
            Set<String> out = new TreeSet<>();
            for (Process p : ps) {
                assertTrue(p.waitFor(60, TimeUnit.SECONDS), "forked GenerationStoreMain did not finish");
                String text = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
                assertEquals(0, p.exitValue(), text);
                out.add(text);
            }
            assertEquals(Set.of("1", "2"), out);
            List<GenerationStore.Entry> rows = new FileGenerationStore(state).all();
            assertEquals(2, rows.size());
            assertEquals(1, rows.get(0).row().generation());
            assertEquals(2, rows.get(1).row().generation());
        }
    }

    @Test void twoScopesShareOneNumberSequence() {
        FileGenerationStore s = new FileGenerationStore(tmp);
        assertEquals(1, register(s, "line2"));
        assertEquals(2, register(s, "line2-plc"));
        assertEquals(3, register(s, "line2"));
        assertEquals("line2-plc", s.registered(2).orElseThrow().scope());
        assertEquals(List.of(1L, 3L), s.all().stream().filter(x -> x.scope().equals("line2")).map(x -> x.row().generation()).toList());
    }

    @Test void aTruncatedLastLineFailsClosed() throws IOException {
        FileGenerationStore s = new FileGenerationStore(tmp);
        s.append("line2", row(1, REGISTERED));
        byte[] good = Files.readAllBytes(s.file());
        byte[] partial = Canonical.bytes(FileGenerationStore.record("line2", row(2, REGISTERED)));
        Files.write(s.file(), Arrays.copyOf(partial, partial.length / 2), StandardOpenOption.APPEND);
        byte[] torn = Files.readAllBytes(s.file());
        corrupt(tmp);
        assertThrows(IllegalStateException.class, () -> s.append("line2", row(2, REGISTERED)));
        assertThrows(IllegalStateException.class, () -> s.registeredRowSha256(1));
        assertArrayEquals(torn, Files.readAllBytes(s.file()), "a torn tail is never discarded or repaired");
        // a last line with its LF that is not JSON fails the same way
        Files.write(s.file(), good);
        Files.write(s.file(), "{\"topic\": \"forseti.gen\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        corrupt(tmp);
        // a line that is not the canonical bytes of its record
        Files.write(s.file(), good);
        Files.write(s.file(), (Json.JSON.writeValueAsString(FileGenerationStore.record("line2", row(2, REGISTERED))).replace(",", ", ") + "\n")
            .getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        corrupt(tmp);
    }

    @Test void aContradictoryHistoryFailsClosed() throws IOException {
        // a second registered row of one number
        writeRaw(tmp.resolve("a"), List.of(e("line2", 1, REGISTERED), e("line2", 1, REGISTERED)));
        corrupt(tmp.resolve("a"));
        // changed pins on a later row
        Generation changed = new Generation(1, RETIRED, "e".repeat(64), Json.JSON.createArrayNode(), Json.object(), 1,
            Json.object().put("releaseSha256", "d".repeat(64)), null, null, GenerationStoreMain.AT);
        writeRaw(tmp.resolve("b"), List.of(e("line2", 1, REGISTERED), new GenerationStore.Entry("line2", changed)));
        corrupt(tmp.resolve("b"));
        // two currents in one scope
        List<GenerationStore.Entry> two = new ArrayList<>(List.of(e("line2", 1, REGISTERED), e("line2", 2, REGISTERED)));
        for (long n : List.of(1L, 2L)) for (String st : List.of(REPLAYING, VERIFIED, CURRENT)) two.add(e("line2", n, st));
        writeRaw(tmp.resolve("c"), two);
        corrupt(tmp.resolve("c"));
        IllegalStateException cur = assertThrows(IllegalStateException.class, () -> new FileGenerationStore(tmp.resolve("c")).current("line2"));
        assertTrue(cur.getMessage().startsWith("control-log-corrupt: "), "current() never picks the last current of a contradictory log");
        // a tail at verified (an interrupted promotion)
        writeRaw(tmp.resolve("d"), List.of(e("line2", 1, REGISTERED), e("line2", 1, REPLAYING), e("line2", 1, VERIFIED)));
        corrupt(tmp.resolve("d"));
        // the same history completed is valid
        writeRaw(tmp.resolve("e"), List.of(e("line2", 1, REGISTERED), e("line2", 1, REPLAYING), e("line2", 1, VERIFIED), e("line2", 1, CURRENT)));
        assertEquals(1, new FileGenerationStore(tmp.resolve("e")).current("line2").orElseThrow().row().generation());
    }

    @Test void everyAppendIsForced() throws IOException {
        List<String> events = new ArrayList<>();
        FileGenerationStore s = new FileGenerationStore(tmp, f -> new Spy(FileChannel.open(f, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.APPEND), events));
        s.append("line2", row(1, REGISTERED));
        s.append("line2", row(1, RETIRED));
        assertEquals(List.of("write", "force:true", "close", "write", "force:true", "close"), events);
        assertEquals(2, s.all().size());
    }

    @Test void anAppendNeverRewritesEarlierBytes() throws IOException {
        FileGenerationStore s = new FileGenerationStore(tmp);
        byte[] previous = new byte[0];
        for (GenerationStore.Entry x : List.of(e("line2", 1, REGISTERED), e("line2-plc", 2, REGISTERED), e("line2", 1, RETIRED), e("line2", 3, REGISTERED))) {
            s.append(x.scope(), x.row());
            byte[] now = Files.readAllBytes(s.file());
            assertTrue(now.length > previous.length);
            assertArrayEquals(previous, Arrays.copyOf(now, previous.length), "earlier bytes unchanged");
            byte[] line = Canonical.bytes(FileGenerationStore.record(x.scope(), x.row()));
            assertArrayEquals(line, Arrays.copyOfRange(now, previous.length, now.length - 1));
            assertEquals('\n', now[now.length - 1]);
            previous = now;
        }
    }

    @Test void theRegisteredRowDigestIsTheDigestOfTheStoredLine() throws IOException {
        FileGenerationStore s = new FileGenerationStore(tmp);
        s.append("line2", row(1, REGISTERED));
        s.append("line2", row(2, REGISTERED));
        s.append("line2", row(1, RETIRED));
        List<String> lines = Files.readAllLines(s.file(), StandardCharsets.UTF_8);
        assertEquals(Canonical.digestOf(lines.get(0).getBytes(StandardCharsets.UTF_8)), s.registeredRowSha256(1).orElseThrow());
        assertEquals(Canonical.digestOf(lines.get(1).getBytes(StandardCharsets.UTF_8)), s.registeredRowSha256(2).orElseThrow());
        assertEquals(Canonical.digest(FileGenerationStore.record("line2", row(1, REGISTERED))), s.registeredRowSha256(1).orElseThrow());
        assertNotEquals(s.registeredRowSha256(1), s.registeredRowSha256(2));
        assertEquals(Optional.empty(), s.registeredRowSha256(3));
    }

    static List<String> fields(JsonNode n) { List<String> l = new ArrayList<>(); n.fieldNames().forEachRemaining(l::add); return l; }

    /** A channel that records writes, forces and the close, delegating every call. */
    static final class Spy extends FileChannel {
        private final FileChannel d;
        private final List<String> events;
        Spy(FileChannel d, List<String> events) { this.d = d; this.events = events; }
        @Override public int read(ByteBuffer dst) throws IOException { return d.read(dst); }
        @Override public long read(ByteBuffer[] dsts, int offset, int length) throws IOException { return d.read(dsts, offset, length); }
        @Override public int write(ByteBuffer src) throws IOException { events.add("write"); return d.write(src); }
        @Override public long write(ByteBuffer[] srcs, int offset, int length) throws IOException { events.add("write"); return d.write(srcs, offset, length); }
        @Override public long position() throws IOException { return d.position(); }
        @Override public FileChannel position(long p) throws IOException { d.position(p); return this; }
        @Override public long size() throws IOException { return d.size(); }
        @Override public FileChannel truncate(long size) throws IOException { events.add("truncate"); d.truncate(size); return this; }
        @Override public void force(boolean metaData) throws IOException { events.add("force:" + metaData); d.force(metaData); }
        @Override public long transferTo(long position, long count, WritableByteChannel target) throws IOException { return d.transferTo(position, count, target); }
        @Override public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException { events.add("write"); return d.transferFrom(src, position, count); }
        @Override public int read(ByteBuffer dst, long position) throws IOException { return d.read(dst, position); }
        @Override public int write(ByteBuffer src, long position) throws IOException { events.add("write@" + position); return d.write(src, position); }
        @Override public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException { return d.map(mode, position, size); }
        @Override public FileLock lock(long position, long size, boolean shared) throws IOException { return d.lock(position, size, shared); }
        @Override public FileLock tryLock(long position, long size, boolean shared) throws IOException { return d.tryLock(position, size, shared); }
        @Override protected void implCloseChannel() throws IOException { events.add("close"); d.close(); }
    }
}
