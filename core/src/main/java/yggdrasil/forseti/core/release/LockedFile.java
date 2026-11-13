package yggdrasil.forseti.core.release;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/** An exclusive hold of one lock file against the threads of this JVM and against other processes: an in-JVM {@link ReentrantLock}
 *  keyed by the real path of the lock file (overlapping {@link FileLock} requests from one JVM throw instead of waiting), then the
 *  {@link FileLock} on that file. The lock file is created when absent and is never evidence. A thread that already holds the lock
 *  re-enters it without asking for the file lock again. The hold is released when the body returns or throws. */
public final class LockedFile {
    private static final ConcurrentHashMap<Path, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

    private LockedFile() {}

    /** The body run inside the hold. */
    @FunctionalInterface
    public interface Body<T> { T run() throws IOException; }

    public static <T> T with(Path lockFile, Body<T> body) throws IOException {
        Path parent = lockFile.toAbsolutePath().getParent();
        if (parent != null) Files.createDirectories(parent);
        if (!Files.exists(lockFile)) { try { Files.createFile(lockFile); } catch (FileAlreadyExistsException ignored) { } }
        ReentrantLock jvm = LOCKS.computeIfAbsent(lockFile.toRealPath(), k -> new ReentrantLock());
        jvm.lock();
        try {
            if (jvm.getHoldCount() > 1) return body.run();
            try (FileChannel ch = FileChannel.open(lockFile, StandardOpenOption.WRITE); FileLock ignored = ch.lock()) {
                return body.run();
            }
        } finally { jvm.unlock(); }
    }
}
