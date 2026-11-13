package yggdrasil.forseti.core.release;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LockedFileTest {
    @TempDir Path tmp;

    @Test void threadsOfOneJvmWaitInsteadOfThrowing() throws Exception {
        Path lock = tmp.resolve("state").resolve("x.lock");
        int threads = 8, rounds = 25;
        AtomicInteger inside = new AtomicInteger(), most = new AtomicInteger(), done = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CyclicBarrier start = new CyclicBarrier(threads);
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < threads; t++) fs.add(pool.submit(() -> {
                start.await();
                for (int r = 0; r < rounds; r++) LockedFile.with(lock, () -> {
                    int now = inside.incrementAndGet();
                    most.accumulateAndGet(now, Math::max);
                    Thread.onSpinWait();
                    inside.decrementAndGet();
                    return done.incrementAndGet();
                });
                return null;
            }));
            for (Future<?> f : fs) f.get(60, TimeUnit.SECONDS);   // an OverlappingFileLockException would surface here
        } finally { pool.shutdownNow(); }
        assertEquals(threads * rounds, done.get());
        assertEquals(1, most.get(), "one holder at a time");
        assertTrue(Files.isRegularFile(lock));
    }

    @Test void theLockIsReleasedAfterAnException() throws Exception {
        Path lock = tmp.resolve("y.lock");
        IOException e = assertThrows(IOException.class, () -> LockedFile.<Void>with(lock, () -> { throw new IOException("inside"); }));
        assertEquals("inside", e.getMessage());
        assertThrows(IllegalStateException.class, () -> LockedFile.<Void>with(lock, () -> { throw new IllegalStateException("again"); }));
        ExecutorService other = Executors.newSingleThreadExecutor();
        try {
            assertEquals("free", other.submit(() -> LockedFile.with(lock, () -> "free")).get(30, TimeUnit.SECONDS), "another thread takes the lock");
        } finally { other.shutdownNow(); }
        assertEquals("free", LockedFile.with(lock, () -> "free"));
    }
}
