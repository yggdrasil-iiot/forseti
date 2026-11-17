package yggdrasil.forseti.replay.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.cli.replay.ProcessSupervisor.Signal;

/** Process supervision without Docker images (plan D7): a child JVM started, signalled and killed by the harness. Stop and continue are
 *  POSIX signals; elsewhere they are refused (plan D12). */
class LocalProcessSupervisorTest {
    @Test void theProcessSupervisorStartsSignalsAndKillsAChild() throws Exception {
        java.nio.file.Path log = java.nio.file.Files.createTempFile("forseti-sleeper", ".log");
        String java = ProcessHandle.current().info().command().orElse("java");
        try (LocalProcessSupervisor s = new LocalProcessSupervisor()) {
            var child = s.start("sleeper", List.of(java, "-cp", System.getProperty("java.class.path"), Sleeper.class.getName()), Map.of("FORSETI_TEST", "1"), log);
            assertTrue(child.alive());
            assertTrue(child.pid() > 0);
            assertEquals(List.of(child), s.running());
            if (s.posix()) { child.signal(Signal.STOP); child.signal(Signal.CONT); }
            else assertThrows(UnsupportedOperationException.class, () -> child.signal(Signal.STOP));
            assertTrue(child.waitFor(Duration.ofMillis(200)).isEmpty(), "still running");
            child.signal(Signal.KILL);
            assertTrue(child.waitFor(Duration.ofSeconds(30)).isPresent());
            assertFalse(child.alive());
            assertEquals(List.of(), s.running());
        }
    }

    /** A child that sleeps until it is killed. */
    public static final class Sleeper {
        public static void main(String[] args) throws InterruptedException { System.out.println("sleeping " + System.getenv("FORSETI_TEST")); Thread.sleep(600_000); }
    }
}
