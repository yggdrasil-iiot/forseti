package yggdrasil.forseti.core.release;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/** The head version floor of each stream at {@code <stateDir>/head-floors/<stream>.json}, in Vor's shape
 *  {@code {"headVersion": n, "statementSha256": "<hex>"}} (spec 84, 553): it never goes down, and one version always carries one
 *  statement. {@link #update} reads, compares and raises inside one lock hold: an in-JVM lock keyed by the canonical path of
 *  {@code head-floors/.lock} (overlapping FileLock requests from one JVM throw instead of waiting), then the FileLock on that
 *  file against other processes. A raise is written to a temporary file, forced to disk and moved into place atomically. */
public final class FloorStore {
    public record Floor(long headVersion, String statementSha256) {}
    public sealed interface Outcome {
        record Raised() implements Outcome {}
        record Unchanged() implements Outcome {}
        record Rollback(Floor floor) implements Outcome {}
    }
    public static final Outcome RAISED = new Outcome.Raised(), UNCHANGED = new Outcome.Unchanged();
    private static final ConcurrentHashMap<Path, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

    private final Path dir;
    public FloorStore(Path stateDir) { this.dir = stateDir.resolve("head-floors"); }

    public Optional<Floor> get(String stream) {
        Path f = dir.resolve(stream + ".json");
        if (!Files.isRegularFile(f)) return Optional.empty();
        JsonNode n = Json.read(f);
        return Optional.of(new Floor(Json.safeLong(n.path("headVersion"), "headVersion", 1, Json.MAX_SAFE), Json.text(n, "statementSha256")));
    }

    public List<String> streams() {
        if (!Files.isDirectory(dir)) return List.of();
        try (Stream<Path> s = Files.list(dir)) {
            return s.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".json")).map(n -> n.substring(0, n.length() - 5)).sorted().toList();
        } catch (IOException e) { throw new IllegalStateException(dir.toString(), e); }
    }

    /** ROLLBACK when the candidate is below the floor or equals it with another statement; otherwise RAISED when {@code raise} is
     *  true and the candidate is above the floor (or no floor exists), else UNCHANGED. */
    public Outcome update(String stream, long candidate, String statementSha, boolean raise) {
        try {
            Files.createDirectories(dir);
            Path lockFile = dir.resolve(".lock");
            if (!Files.exists(lockFile)) { try { Files.createFile(lockFile); } catch (FileAlreadyExistsException ignored) { } }
            ReentrantLock jvm = LOCKS.computeIfAbsent(lockFile.toRealPath(), k -> new ReentrantLock());
            jvm.lock();
            try (FileChannel ch = FileChannel.open(lockFile, StandardOpenOption.WRITE); FileLock ignored = ch.lock()) {
                Optional<Floor> floor = get(stream);
                if (floor.isPresent()) {
                    Floor f = floor.get();
                    if (candidate < f.headVersion() || candidate == f.headVersion() && !f.statementSha256().equals(statementSha)) return new Outcome.Rollback(f);
                    if (candidate == f.headVersion() || !raise) return UNCHANGED;
                } else if (!raise) return UNCHANGED;
                write(stream, new Floor(candidate, statementSha));
                return RAISED;
            } finally { jvm.unlock(); }
        } catch (IOException e) { throw new IllegalStateException("head floor " + stream + ": " + e.getMessage(), e); }
    }

    private void write(String stream, Floor f) throws IOException {
        Path file = dir.resolve(stream + ".json"), tmp = dir.resolve(stream + ".json.tmp");
        byte[] b = Json.pretty(Json.putSafe(Json.object(), "headVersion", f.headVersion()).put("statementSha256", f.statementSha256()));
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.wrap(b); while (buf.hasRemaining()) ch.write(buf); ch.force(true);
        }
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
}
