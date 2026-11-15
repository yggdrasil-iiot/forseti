package yggdrasil.forseti.loop;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The stack lock: its own format, the committed lock against the committed compose file, and the jar and image checks the runner
 *  makes before it starts anything. */
class StackLockTest {
    static final Path LIVE_DIR = Path.of("..", "live");
    static final String KAFKA = "sha256:" + "1".repeat(64), HIVEMQ = "sha256:" + "2".repeat(64);

    static ObjectNode lock(String jarSha) {
        ObjectNode n = Json.object();
        n.put("stackLockVersion", 1);
        ObjectNode i = n.putObject("images");
        i.putObject("kafka").put("image", "apache/kafka").put("version", "4.2.2").put("digest", KAFKA);
        i.putObject("hivemq").put("image", "hivemq/hivemq-ce").put("version", "2026.5").put("digest", HIVEMQ);
        n.putObject("jars").putObject("muninn").put("repository", "yggdrasil-iiot/muninn").put("commit", "a".repeat(40)).put("module", ".")
            .put("path", "target/muninn.jar").put("sha256", jarSha);
        return n;
    }

    @Test
    void theCommittedLockReadsAndTheCommittedComposeFileReferencesExactlyItsImages() throws Exception {
        StackLock.Lock l = StackLock.read(Json.read(LIVE_DIR.resolve("stack-lock.json")));
        assertEquals(Set.of("hivemq", "kafka"), l.images().keySet());
        assertEquals(Set.of("bifrost-sim", "muninn"), l.jars().keySet());
        assertEquals("4.2.2", l.images().get("kafka").version());
        String compose = Files.readString(LIVE_DIR.resolve("docker-compose.yml"), StandardCharsets.UTF_8);
        assertEquals(List.of(), StackLock.composeProblems(l, compose));
        assertFalse(compose.contains("latest"), "no image by the latest tag");
    }

    @Test
    void aMalformedLockIsRefusedWithTheFieldNamed() {
        ObjectNode bad = lock("b".repeat(64));
        ((ObjectNode) bad.path("images").path("kafka")).put("digest", "sha256:abc");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> StackLock.read(bad)).getMessage().contains("images.kafka.digest"));
        ObjectNode tag = lock("b".repeat(64));
        ((ObjectNode) tag.path("images").path("kafka")).put("image", "apache/kafka:latest");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> StackLock.read(tag)).getMessage().contains("names a tag"));
        ObjectNode commit = lock("b".repeat(64));
        ((ObjectNode) commit.path("jars").path("muninn")).put("commit", "main");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> StackLock.read(commit)).getMessage().contains("jars.muninn.commit"));
        ObjectNode extra = lock("b".repeat(64));
        extra.put("note", "x");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> StackLock.read(extra)).getMessage().contains("unknown field note"));
    }

    @Test
    void aComposeFileThatUsesATagAnUnlockedImageOrMissesALockedOneIsRefused() {
        StackLock.Lock l = StackLock.read(lock("b".repeat(64)));
        String ok = "services:\n  kafka:\n    image: apache/kafka@" + KAFKA + "  # 4.2.2\n  hivemq:\n    image: \"hivemq/hivemq-ce@" + HIVEMQ + "\"\n";
        assertEquals(List.of(), StackLock.composeProblems(l, ok));
        assertEquals(List.of(), StackLock.composeProblems(l, ok.replace("\n", "\r\n")));
        List<String> tagged = StackLock.composeProblems(l, ok.replace("hivemq/hivemq-ce@" + HIVEMQ, "hivemq/hivemq-ce:latest"));
        assertEquals(List.of("compose image hivemq/hivemq-ce:latest is not referenced by digest",
            "compose file does not reference the locked image hivemq/hivemq-ce@" + HIVEMQ), tagged);
        List<String> other = StackLock.composeProblems(l, ok.replace(KAFKA, "sha256:" + "3".repeat(64)));
        assertTrue(other.contains("compose image apache/kafka@sha256:" + "3".repeat(64) + " is not in the stack lock"), other.toString());
    }

    @Test
    void aJarWithAnotherDigestOrAMissingJarIsRefused(@TempDir Path dir) throws Exception {
        Path jar = dir.resolve("muninn.jar");
        Files.write(jar, "jar bytes".getBytes(StandardCharsets.UTF_8));
        String sha = Canonical.digestOf(Files.readAllBytes(jar));
        assertEquals(List.of(), StackLock.jarProblems(StackLock.read(lock(sha)), Map.of("muninn", jar)));
        List<String> p = StackLock.jarProblems(StackLock.read(lock("b".repeat(64))), Map.of("muninn", jar));
        assertEquals(1, p.size());
        assertTrue(p.get(0).startsWith("jar muninn (muninn.jar) has SHA-256 " + sha + ", the stack lock pins " + "b".repeat(64)), p.toString());
        assertEquals(List.of("jar muninn was not given"), StackLock.jarProblems(StackLock.read(lock(sha)), Map.of()));
        assertEquals(List.of("jar muninn: no file " + dir.resolve("none.jar")), StackLock.jarProblems(StackLock.read(lock(sha)), Map.of("muninn", dir.resolve("none.jar"))));
        assertTrue(StackLock.jarProblems(StackLock.read(lock(sha)), Map.of("muninn", jar, "sim", jar)).contains("jar sim is not in the stack lock"));
    }

    @Test
    void aRunningServiceOnAnotherImageIsRefused() {
        StackLock.Lock l = StackLock.read(lock("b".repeat(64)));
        assertEquals(List.of(), StackLock.imageProblems(l, Map.of("kafka", List.of("apache/kafka@" + KAFKA), "hivemq", List.of("x@sha256:9", "hivemq/hivemq-ce@" + HIVEMQ))));
        List<String> p = StackLock.imageProblems(l, Map.of("kafka", List.of("apache/kafka@" + HIVEMQ)));
        assertEquals(List.of("service hivemq: no image observed", "service kafka runs [apache/kafka@" + HIVEMQ + "], not the locked apache/kafka@" + KAFKA), p);
    }

    @Test
    void stackCheckPrintsEveryProblemAndExitsOne(@TempDir Path dir) throws Exception {
        Path jar = dir.resolve("m.jar");
        Files.write(jar, new byte[] {1});
        StackLock.Lock l = StackLock.read(lock("b".repeat(64)));
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        assertEquals(1, LoopMain.stackCheck(l, "services: {}\n", Map.of("muninn", jar.toString()), Map.of("kafka", ""), new PrintStream(b, true, StandardCharsets.UTF_8)));
        String out = b.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("problem: compose file does not reference the locked image apache/kafka@" + KAFKA), out);
        assertTrue(out.contains("problem: jar muninn (m.jar) has SHA-256"), out);
        assertTrue(out.contains("problem: service kafka runs [], not the locked"), out);
        b.reset();
        assertEquals(1, LoopMain.stackCheck(l, null, Map.of(), Map.of("kafka", "apache/kafka@" + KAFKA), new PrintStream(b, true, StandardCharsets.UTF_8)));
        assertEquals("problem: service hivemq: no image observed", b.toString(StandardCharsets.UTF_8).strip(), "every locked image must be observed");
        b.reset();
        assertEquals(0, LoopMain.stackCheck(l, null, Map.of(), Map.of("kafka", "apache/kafka@" + KAFKA, "hivemq", "hivemq/hivemq-ce@" + HIVEMQ),
            new PrintStream(b, true, StandardCharsets.UTF_8)));
        assertEquals("stack-check: clean (0 jars, 2 images)", b.toString(StandardCharsets.UTF_8).strip());
    }
}
