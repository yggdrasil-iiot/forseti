package yggdrasil.forseti.cli.recording;

import java.nio.charset.StandardCharsets;

/** Kafka's default partitioner for keyed records: {@code toPositive(murmur2(keyBytes)) % numPartitions}, the 32-bit MurmurHash2
 *  variant Kafka's {@code Utils.murmur2} computes (seed {@code 0x9747b28c}). Raw device keys and incident {@code equipmentRef} keys
 *  are placed with it so the step-5 preflight, which reads the broker's placement, finds the placement the recording pins. */
public final class Murmur2 {
    private Murmur2() {}

    public static int hash(byte[] data) {
        int length = data.length;
        final int seed = 0x9747b28c;
        final int m = 0x5bd1e995;
        final int r = 24;
        int h = seed ^ length;
        int length4 = length / 4;
        for (int i = 0; i < length4; i++) {
            int i4 = i * 4;
            int k = (data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        int tail = length & ~3;
        switch (length % 4) {
            case 3: h ^= (data[tail + 2] & 0xff) << 16;   // fall through
            case 2: h ^= (data[tail + 1] & 0xff) << 8;    // fall through
            case 1: h ^= data[tail] & 0xff; h *= m;
            default: break;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    /** Kafka's {@code Utils.toPositive}: the sign bit cleared, not {@code Math.abs}. */
    public static int toPositive(int n) { return n & 0x7fffffff; }

    /** The partition Kafka's default partitioner gives a record with this key (UTF-8 bytes, as the string serializer writes them). */
    public static int partition(String key, int partitions) {
        if (partitions <= 0) throw new IllegalArgumentException("partitions must be positive");
        return toPositive(hash(key.getBytes(StandardCharsets.UTF_8))) % partitions;
    }
}
