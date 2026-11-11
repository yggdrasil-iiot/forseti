package yggdrasil.forseti.core.release;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FloorStoreTest {
    @TempDir Path tmp;
    static final String A = "a".repeat(64), B = "b".repeat(64);

    @Test void twoThreadsOfOneJvmEndAtTheHigherVersion() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 50; round++) {
                Path state = tmp.resolve("round-" + round);
                CyclicBarrier start = new CyclicBarrier(2);
                // Two FloorStore instances, as two verifiers of one JVM would hold.
                Callable<FloorStore.Outcome> three = () -> { start.await(); return new FloorStore(state).update("line2", 3, A, true); };
                Callable<FloorStore.Outcome> two = () -> { start.await(); return new FloorStore(state).update("line2", 2, B, true); };
                Future<FloorStore.Outcome> f3, f2;
                if (round % 2 == 0) { f3 = pool.submit(three); f2 = pool.submit(two); } else { f2 = pool.submit(two); f3 = pool.submit(three); }
                FloorStore.Outcome o3 = f3.get(30, TimeUnit.SECONDS), o2 = f2.get(30, TimeUnit.SECONDS);   // an OverlappingFileLockException would surface here
                assertEquals(FloorStore.RAISED, o3);
                assertTrue(o2 instanceof FloorStore.Outcome.Rollback || o2 == FloorStore.RAISED, String.valueOf(o2));
                assertEquals(new FloorStore.Floor(3, A), new FloorStore(state).get("line2").orElseThrow());
            }
        } finally { pool.shutdownNow(); }
    }

    @Test void twoProcessesEndAtTheHigherVersion() throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        for (int round = 0; round < 3; round++) {
            Path state = tmp.resolve("round-" + round), go = tmp.resolve("go-" + round);
            List<Process> ps = new ArrayList<>();
            for (String[] v : new String[][] {{"3", A}, {"2", B}})
                ps.add(new ProcessBuilder(java, "-cp", cp, FloorStoreMain.class.getName(), state.toString(), "line2", v[0], v[1], go.toString())
                    .redirectErrorStream(true).start());
            Thread.sleep(500);
            Files.writeString(go, "go");
            List<String> out = new ArrayList<>();
            for (Process p : ps) {
                assertTrue(p.waitFor(60, TimeUnit.SECONDS), "forked FloorStoreMain did not finish");
                String text = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
                assertEquals(0, p.exitValue(), text);
                out.add(text);
            }
            assertEquals("Raised", out.get(0));
            assertTrue(List.of("Raised", "Rollback").contains(out.get(1)), out.get(1));
            assertEquals(new FloorStore.Floor(3, A), new FloorStore(state).get("line2").orElseThrow());
        }
    }

    @Test void theSameVersionWithTheSameStatementIsUnchangedAndAnotherIsRollback() {
        FloorStore f = new FloorStore(tmp);
        assertEquals(FloorStore.UNCHANGED, f.update("line2", 1, A, false));
        assertTrue(f.get("line2").isEmpty());
        assertEquals(FloorStore.RAISED, f.update("line2", 1, A, true));
        assertEquals(FloorStore.UNCHANGED, f.update("line2", 1, A, true));
        assertEquals(new FloorStore.Outcome.Rollback(new FloorStore.Floor(1, A)), f.update("line2", 1, B, true));
        assertEquals(FloorStore.UNCHANGED, f.update("line2", 2, B, false));
        assertEquals(1, f.get("line2").orElseThrow().headVersion());
    }
}
