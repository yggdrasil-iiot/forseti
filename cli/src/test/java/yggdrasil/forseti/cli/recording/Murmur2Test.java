package yggdrasil.forseti.cli.recording;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class Murmur2Test {
    /** Kafka DefaultPartitioner, 6 partitions. Pinned by running the reference algorithm once on fixture device keys; the oracle's
     *  own murmur2 (oracle/src/forseti_oracle/recording.py) gives the same hashes and partitions (oracle/tests/test_recording.py). */
    @Test void pinnedFixtureKeys() {
        assertVector("synthetic-site:line2:station1", 1854209566, 4);
        assertVector("synthetic-site:line2:station1:nutrunner1", -703548229, 1);
        assertVector("synthetic-site:line2:station3", 1635878257, 1);
    }

    /** Kafka's own UtilsTest.testMurmur2 vectors, an independent source for the algorithm. */
    @Test void kafkaUtilsTestVectors() {
        Map<String, Integer> cases = new LinkedHashMap<>();
        cases.put("21", -973932308);
        cases.put("foobar", -790332482);
        cases.put("a-little-bit-long-string", -985981536);
        cases.put("a-little-bit-longer-string", -1486304829);
        cases.put("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971);
        cases.put("abc", 479470107);
        cases.forEach((k, v) -> assertEquals(v, Murmur2.hash(k.getBytes(StandardCharsets.UTF_8)), k));
    }

    @Test void toPositiveClearsTheSignBitAndIsNotAbs() {
        assertEquals(0, Murmur2.toPositive(Integer.MIN_VALUE));
        assertEquals(Integer.MAX_VALUE, Murmur2.toPositive(-1));
        assertEquals((-703548229 & 0x7fffffff) % 6, Murmur2.partition("synthetic-site:line2:station1:nutrunner1", 6));
        assertEquals(-1521775470, Murmur2.hash("synthetic-site:line2:station2".getBytes(StandardCharsets.UTF_8)));
        assertEquals(2, Murmur2.partition("synthetic-site:line2:station2", 6));
        assertEquals(0, Math.abs(-1521775470) % 6, "Math.abs would place station2 elsewhere");
        assertThrows(IllegalArgumentException.class, () -> Murmur2.partition("x", 0));
    }

    static void assertVector(String key, int hash, int partition) {
        assertEquals(hash, Murmur2.hash(key.getBytes(StandardCharsets.UTF_8)), key);
        assertEquals(partition, Murmur2.partition(key, 6), key);
    }
}
