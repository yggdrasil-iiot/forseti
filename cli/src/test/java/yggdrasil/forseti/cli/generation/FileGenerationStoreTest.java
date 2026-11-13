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
import yggdrasil.forseti.core.generation.GenerationPolicy;
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

    // --- a promotion batch, interrupted -------------------------------------------------------------------------------------

    static final Path ROOT = Path.of("..").toAbsolutePath().normalize(), FIXTURES = ROOT.resolve("fixtures");

    static JsonNode run(String id) {
        com.fasterxml.jackson.databind.node.ObjectNode r = Json.object().put("runId", id).put("runRecordSha256", Canonical.digest(Json.object().put("run", id)));
        r.putObject("offsetRanges").putObject("ot.messages").putArray("0").add(0).add(9);
        return r;
    }

    static final String REPORT = "7".repeat(64);

    /** The store before a promotion: generation 1 registered (and, with {@code previousCurrent}, promoted to current), then generation 2
     *  registered; returns the batch that promotes 2 (three rows, or four retiring 1). */
    static List<GenerationStore.Entry> before(Path state, boolean previousCurrent) throws IOException {
        List<GenerationStore.Entry> h = new ArrayList<>(List.of(e("line2", 1, REGISTERED)));
        if (previousCurrent) h.addAll(GenerationPolicy.promotion(h.get(0), Optional.empty(), run("run-1"), REPORT, GenerationStoreMain.AT + 1));
        h.add(e("line2", 2, REGISTERED));
        writeRaw(state, h);
        FileGenerationStore s = new FileGenerationStore(state);
        return GenerationPolicy.promotion(s.registered(2).orElseThrow(), s.current("line2"), run("run-2"), "8".repeat(64), GenerationStoreMain.AT + 2);
    }

    static byte[] batchBytes(List<GenerationStore.Entry> batch) {
        java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
        for (GenerationStore.Entry x : batch) { b.writeBytes(Canonical.bytes(FileGenerationStore.record(x.scope(), x.row()))); b.write('\n'); }
        return b.toByteArray();
    }

    /** A channel the test drives: it accepts at most {@code perCall} bytes a call and {@code budget} bytes in all; past the budget a write
     *  throws ({@code stall} false) or accepts nothing ({@code stall} true); write call number {@code throwAt} throws; {@code force}
     *  throws when {@code failForce}. Every call is logged, {@code force} with the file size at that moment. */
    static final class Driven extends FileChannel {
        private final FileChannel d;
        final List<String> events = new ArrayList<>();
        final int perCall, throwAt;
        long budget;
        final boolean stall, failForce;
        int calls;
        Driven(Path f, int perCall, long budget, boolean stall, int throwAt, boolean failForce) throws IOException {
            this.d = FileChannel.open(f, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            this.perCall = perCall; this.budget = budget; this.stall = stall; this.throwAt = throwAt; this.failForce = failForce;
        }
        @Override public int write(ByteBuffer src) throws IOException {
            int call = calls++;
            if (call == throwAt) { events.add("write-throws"); throw new IOException("injected write failure at call " + call); }
            if (budget <= 0) {
                if (stall) { events.add("write:0"); return 0; }
                events.add("write-throws");
                throw new IOException("the disk is full");
            }
            int n = (int) Math.min(Math.min(perCall, budget), src.remaining());
            ByteBuffer slice = src.duplicate();
            slice.limit(slice.position() + n);
            int w = d.write(slice);
            src.position(src.position() + w);
            budget -= w;
            events.add("write:" + w);
            return w;
        }
        @Override public void force(boolean metaData) throws IOException {
            events.add("force:" + metaData + "@" + d.size());
            if (failForce) throw new IOException("injected force failure");
            d.force(metaData);
        }
        @Override public int read(ByteBuffer dst) throws IOException { return d.read(dst); }
        @Override public long read(ByteBuffer[] dsts, int offset, int length) throws IOException { return d.read(dsts, offset, length); }
        @Override public long write(ByteBuffer[] srcs, int offset, int length) throws IOException { throw new IOException("gathering write not expected"); }
        @Override public long position() throws IOException { return d.position(); }
        @Override public FileChannel position(long p) throws IOException { d.position(p); return this; }
        @Override public long size() throws IOException { return d.size(); }
        @Override public FileChannel truncate(long size) throws IOException { events.add("truncate"); d.truncate(size); return this; }
        @Override public long transferTo(long position, long count, WritableByteChannel target) throws IOException { return d.transferTo(position, count, target); }
        @Override public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException { throw new IOException("not expected"); }
        @Override public int read(ByteBuffer dst, long position) throws IOException { return d.read(dst, position); }
        @Override public int write(ByteBuffer src, long position) throws IOException { throw new IOException("positional write not expected"); }
        @Override public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException { return d.map(mode, position, size); }
        @Override public FileLock lock(long position, long size, boolean shared) throws IOException { return d.lock(position, size, shared); }
        @Override public FileLock tryLock(long position, long size, boolean shared) throws IOException { return d.tryLock(position, size, shared); }
        @Override protected void implCloseChannel() throws IOException { events.add("close"); d.close(); }
    }

    static FileGenerationStore.ControlLogIo interrupted(Path state, List<GenerationStore.Entry> batch, Driven[] holder, int perCall, long budget, boolean stall,
                                                        int throwAt, boolean failForce) {
        FileGenerationStore s = new FileGenerationStore(state, f -> holder[0] = new Driven(f, perCall, budget, stall, throwAt, failForce));
        return assertThrows(FileGenerationStore.ControlLogIo.class, () -> s.appendBatch(batch));
    }

    static void refusesCorrupt(org.junit.jupiter.api.function.Executable x, String what) {
        IllegalStateException e = assertThrows(IllegalStateException.class, x, what);
        assertTrue(e.getMessage().startsWith("control-log-corrupt: "), what + ": " + e.getMessage());
    }

    /** A verb in-process; a refusal must be {@code control-log-corrupt}. */
    static int cli(String... args) {
        java.io.ByteArrayOutputStream so = new java.io.ByteArrayOutputStream();
        int exit = GenerationCli.run(args, new java.io.PrintStream(so, true, StandardCharsets.UTF_8), new java.io.PrintStream(new java.io.ByteArrayOutputStream()),
            java.time.Clock.fixed(java.time.Instant.ofEpochMilli(GenerationStoreMain.AT), java.time.ZoneOffset.UTC), ROOT);
        String out = so.toString(StandardCharsets.UTF_8).strip();
        if (exit == 1) assertEquals("control-log-corrupt", Json.read(out.getBytes(StandardCharsets.UTF_8)).get("refusal").asText(), String.join(" ", args) + ": " + out);
        return exit;
    }

    @Test void zeroAppendedBytesLeaveTheOldValidState() throws IOException {
        Path state = tmp.resolve("state");
        List<GenerationStore.Entry> batch = before(state, true);
        byte[] old = Files.readAllBytes(state.resolve(FileGenerationStore.FILE));
        Driven[] ch = new Driven[1];
        FileGenerationStore.ControlLogIo e = interrupted(state, batch, ch, Integer.MAX_VALUE, 0, false, -1, false);
        assertEquals(0, e.appended);
        assertEquals(List.of("write-throws", "close"), ch[0].events);
        assertArrayEquals(old, Files.readAllBytes(state.resolve(FileGenerationStore.FILE)));
        FileGenerationStore s = new FileGenerationStore(state);
        s.validate();
        assertEquals(1, s.current("line2").orElseThrow().row().generation(), "the previous current is still current");
        assertEquals(Optional.of(REGISTERED), s.latestStatus(2));
        assertEquals(0, cli("list", "--state", state.toString()));
        s.appendBatch(batch);   // the verbs work: the same promotion completes on a channel that accepts it
        assertEquals(2, new FileGenerationStore(state).current("line2").orElseThrow().row().generation());
    }

    @Test void everyNonemptyProperBytePrefixOfAPromotionFailsClosed() throws IOException {
        Path recording = FIXTURES.resolve("recordings/smoke"), keys = FIXTURES.resolve("trusted-keys.jsonl"), mappings = FIXTURES.resolve("registry/mappings");
        for (boolean previous : List.of(false, true)) {
            Path state = tmp.resolve("state-" + previous);
            List<GenerationStore.Entry> batch = before(state, previous);
            assertEquals(previous ? 4 : 3, batch.size());
            Path log = state.resolve(FileGenerationStore.FILE);
            byte[] old = Files.readAllBytes(log), all = batchBytes(batch);
            Set<Integer> boundaries = new TreeSet<>();
            for (int i = 0; i < all.length - 1; i++) if (all[i] == '\n') boundaries.add(i + 1);
            assertEquals(batch.size() - 1, boundaries.size(), "complete-line boundaries after replaying, verified" + (previous ? " and retired" : ""));
            for (int k = 1; k < all.length; k++) {
                Files.write(log, old);
                FileGenerationStore.ControlLogIo e = interrupted(state, batch, new Driven[1], Integer.MAX_VALUE, k, false, -1, false);
                assertEquals(k, e.appended);
                byte[] torn = Files.readAllBytes(log);
                assertArrayEquals(old, Arrays.copyOf(torn, old.length));
                assertArrayEquals(Arrays.copyOf(all, k), Arrays.copyOfRange(torn, old.length, torn.length));
                FileGenerationStore s = new FileGenerationStore(state);
                String at = "prefix " + k + " of " + all.length + (boundaries.contains(k) ? " (a line boundary)" : "");
                refusesCorrupt(s::validate, at + " validate");
                refusesCorrupt(() -> s.current("line2"), at + " current");
                refusesCorrupt(() -> s.byNumber(2), at + " byNumber");
                refusesCorrupt(s::bytes, at + " list");
                refusesCorrupt(() -> new FileGenerationStore(state).registered(2), at + " the harness's read");
                refusesCorrupt(() -> GenerationCli.prepareRun(state, 2, recording), at + " the run preparation");
                assertEquals(1, cli("list", "--state", state.toString()), at);
                assertEquals(1, cli("new", "--state", state.toString(), "--mappings", mappings.toString(), "--trusted-keys", keys.toString(),
                    "--stream", "line2", "--rule", "line2-attribution@1.0.0"), at);
                assertEquals(1, cli("retire", "--state", state.toString(), "--generation", "2"), at);
                assertEquals(1, cli("promote", "--state", state.toString(), "--mappings", mappings.toString(), "--trusted-keys", keys.toString(),
                    "--generation", "2", "--registered-row-sha256", "0".repeat(64), "--run", tmp.resolve("none.json").toString(),
                    "--gate-report", tmp.resolve("none-report.json").toString()), at);
                assertArrayEquals(torn, Files.readAllBytes(log), at + ": nothing repairs or truncates the file");
            }
        }
    }

    @Test void aCompleteForcedBatchValidatesAndPreservesEveryEarlierByte() throws IOException {
        for (boolean previous : List.of(false, true)) {
            Path state = tmp.resolve("state-" + previous);
            List<GenerationStore.Entry> batch = before(state, previous);
            byte[] old = Files.readAllBytes(state.resolve(FileGenerationStore.FILE));
            FileGenerationStore s = new FileGenerationStore(state);
            s.appendBatch(batch);
            byte[] now = Files.readAllBytes(state.resolve(FileGenerationStore.FILE));
            assertArrayEquals(old, Arrays.copyOf(now, old.length), "earlier bytes are a prefix, byte for byte");
            assertArrayEquals(batchBytes(batch), Arrays.copyOfRange(now, old.length, now.length));
            List<GenerationStore.Entry> rows = s.all();
            assertArrayEquals(batchBytes(batch), batchBytes(rows.subList(rows.size() - batch.size(), rows.size())), "the stored rows are the batch");
            assertEquals(previous ? List.of(REPLAYING, VERIFIED, RETIRED, CURRENT) : List.of(REPLAYING, VERIFIED, CURRENT),
                batch.stream().map(x -> x.row().status()).toList());
            List<GenerationStore.Entry> two = s.byNumber(2);
            assertEquals(List.of(REGISTERED, REPLAYING, VERIFIED, CURRENT), two.stream().map(x -> x.row().status()).toList());
            for (GenerationStore.Entry x : two.subList(1, 4)) assertArrayEquals(Canonical.bytes(run("run-2")), Canonical.bytes(x.row().run()), "one run record");
            assertNull(two.get(1).row().gateReportSha256());
            assertEquals("8".repeat(64), two.get(2).row().gateReportSha256(), "one report");
            assertEquals("8".repeat(64), two.get(3).row().gateReportSha256());
            assertEquals(2, s.current("line2").orElseThrow().row().generation(), "exactly one current for the scope");
            if (previous) assertEquals(Optional.of(RETIRED), s.latestStatus(1));
        }
    }

    @Test void aShortWriteIsContinuedAndNeverReportedAsSuccessEarly() throws IOException {
        Path state = tmp.resolve("state");
        List<GenerationStore.Entry> batch = before(state, true);
        long old = Files.size(state.resolve(FileGenerationStore.FILE)), len = batchBytes(batch).length;
        Driven[] ch = new Driven[1];
        new FileGenerationStore(state, f -> ch[0] = new Driven(f, 7, Long.MAX_VALUE, false, -1, false)).appendBatch(batch);
        List<String> ev = ch[0].events;
        assertEquals("close", ev.get(ev.size() - 1));
        assertEquals("force:true@" + (old + len), ev.get(ev.size() - 2), "the batch is complete before force");
        assertEquals((len + 6) / 7, ev.stream().filter(x -> x.startsWith("write:")).count(), "every short write continued");
        assertEquals(2, new FileGenerationStore(state).current("line2").orElseThrow().row().generation());
        // a channel that accepts some bytes and then nothing: no success, and no force
        Path stalled = tmp.resolve("stalled");
        List<GenerationStore.Entry> b2 = before(stalled, true);
        FileGenerationStore.ControlLogIo e = interrupted(stalled, b2, ch, 5, 20, true, -1, false);
        assertEquals(20, e.appended);
        assertTrue(ch[0].events.stream().noneMatch(x -> x.startsWith("force")), ch[0].events.toString());
        assertEquals(FileGenerationStore.STALLED_WRITES, ch[0].events.stream().filter(x -> x.equals("write:0")).count());
        refusesCorrupt(() -> new FileGenerationStore(stalled).all(), "a stalled batch");
    }

    @Test void aWriteOrForceExceptionNeverProducesASuccess() throws IOException {
        Path probe = tmp.resolve("probe");
        int len = batchBytes(before(probe, true)).length, perCall = 100, calls = (len + perCall - 1) / perCall;
        for (int at = 0; at < calls; at++) {
            Path state = tmp.resolve("write-" + at);
            List<GenerationStore.Entry> batch = before(state, true);
            Driven[] ch = new Driven[1];
            FileGenerationStore.ControlLogIo e = interrupted(state, batch, ch, perCall, Long.MAX_VALUE, false, at, false);
            assertEquals((long) at * perCall, e.appended);
            assertTrue(ch[0].events.stream().noneMatch(x -> x.startsWith("force")), "no force after a failed write");
            if (at == 0) new FileGenerationStore(state).validate(); else refusesCorrupt(() -> new FileGenerationStore(state).all(), "write failure at call " + at);
        }
        Path state = tmp.resolve("force");
        List<GenerationStore.Entry> batch = before(state, true);
        Driven[] ch = new Driven[1];
        FileGenerationStore.ControlLogIo e = interrupted(state, batch, ch, perCall, Long.MAX_VALUE, false, -1, true);
        assertEquals(len, e.appended);
        assertTrue(e.getMessage().startsWith("control-log-io: force"), e.getMessage());
        assertEquals("close", ch[0].events.get(ch[0].events.size() - 1));
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
