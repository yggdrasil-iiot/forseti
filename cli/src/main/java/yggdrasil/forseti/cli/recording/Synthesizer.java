package yggdrasil.forseti.cli.recording;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import yggdrasil.forseti.core.Json;

/** The telemetry writer of a synthesized recording (plan Task 17, Step 17.1; current spec 613). A horizon scenario runs 24 h of
 *  continuous telemetry on every device, about 450,000 messages; {@link ScenarioBuilder} generates them scan by scan exactly as it does
 *  for a hand-made scenario (the same members, the same seeded {@link Telemetry#torque}, the same sessions) and hands each message
 *  here instead of keeping it: this class appends it to its {@code ot.messages/<p>.jsonl} at once, through a SHA-256 digest, and keeps
 *  only what the manifest, the lock and the bracket checks need per partition and batch (record counts, the largest delivered
 *  {@code sourceTs}, the stations' W1 contributions). The recording is therefore never in memory as records; the ground truth keeps one
 *  compact entry per sample ({@link GroundTruth.Sample}) and writes its per-sample expectation as runs ({@link ExpectedSamples}).
 *
 *  <p>Streaming needs every partition's records in batch order as they are generated, which holds for on-time telemetry (scan order is
 *  delivery order); a synthesized scenario therefore refuses the overlays that deliver telemetry out of scan order ({@code hold-back},
 *  {@code redeliver} of a message). Nothing here reads a clock or a random source. */
final class Synthesizer {
    static final String GENERATOR = "forseti recordings synth 0.1.0";

    private final Path out;
    private final int batches;
    private final long ooo;
    private final TreeMap<Integer, DigestOutputStream> streams = new TreeMap<>();
    private final Map<Integer, long[]> counts = new HashMap<>(), maxTs = new HashMap<>();
    private final Map<Integer, Integer> lastBatch = new HashMap<>();
    private final Map<Integer, Long> offsets = new HashMap<>();
    private final Map<String, long[]> w1 = new TreeMap<>();
    private final TreeMap<String, String> digests = new TreeMap<>();
    private long messages;
    private boolean closed;

    /** Clears {@code out} (refusing a non-empty directory that is not a recording) and opens one file per subscribed raw partition. */
    Synthesizer(Path out, List<Integer> partitions, int batches, Collection<String> stations, long ooo) {
        this.out = out; this.batches = batches; this.ooo = ooo;
        ScenarioBuilder.prepare(out);
        try {
            Path dir = out.resolve("ot.messages");
            Files.createDirectories(dir);
            for (int p : partitions) {
                MessageDigest md;
                try { md = MessageDigest.getInstance("SHA-256"); } catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
                streams.put(p, new DigestOutputStream(new BufferedOutputStream(Files.newOutputStream(dir.resolve(p + ".jsonl")), 1 << 16), md));
                long[] c = new long[batches], m = new long[batches];
                Arrays.fill(m, Long.MIN_VALUE);
                counts.put(p, c); maxTs.put(p, m); offsets.put(p, 0L); lastBatch.put(p, 0);
            }
        } catch (IOException e) { throw new UncheckedIOException(e); }
        for (String s : stations) { long[] a = new long[batches]; Arrays.fill(a, Long.MIN_VALUE); w1.put(s, a); }
    }

    /** Appends one message of {@code partition}, delivered in {@code batch}; {@code station} is the station it is routed to, or null
     *  for a device outside the rule's catalog (S1: it advances no W1). */
    void message(int partition, String key, int batch, ObjectNode value, String station) {
        DigestOutputStream s = streams.get(partition);
        if (s == null) throw refuse("telemetry on the unsubscribed partition " + partition);
        if (batch < lastBatch.get(partition)) throw refuse("telemetry of " + key + " out of batch order on partition " + partition + " (a synthesized recording streams in scan order)");
        lastBatch.put(partition, batch);
        long offset = offsets.get(partition);
        ObjectNode line = Json.object();
        Json.putSafe(line, "offset", offset);
        line.putNull("timestamp");
        line.put("key", key);
        line.putObject("headers");
        line.set("value", value);
        try { s.write((Json.JSON.writeValueAsString(line) + "\n").getBytes(StandardCharsets.UTF_8)); } catch (IOException e) { throw new UncheckedIOException(e); }
        offsets.put(partition, offset + 1);
        counts.get(partition)[batch]++;
        long[] m = maxTs.get(partition), a = station == null ? null : w1.get(station);
        value.path("members").forEach(x -> {
            long ts = x.get("sourceTs").longValue();
            m[batch] = Math.max(m[batch], ts);
            if (a != null) a[batch] = Math.max(a[batch], ts - ooo - 1);
        });
        messages++;
    }

    long messages() { return messages; }

    /** Partitions with telemetry in the first batch. */
    TreeSet<Integer> firstBatchPartitions() {
        TreeSet<Integer> s = new TreeSet<>();
        counts.forEach((p, c) -> { if (c[0] > 0) s.add(p); });
        return s;
    }

    /** The inclusive through-offset of {@code partition} at batch {@code k} ({@code -1}: nothing included yet). */
    long through(int partition, int k) {
        long n = 0;
        long[] c = counts.get(partition);
        for (int i = 0; i <= k; i++) n += c[i];
        return n - 1;
    }

    /** The largest delivered {@code sourceTs} of {@code partition} through batch {@code k}. */
    long maxSourceTsThrough(int partition, int k) {
        long m = Long.MIN_VALUE;
        long[] a = maxTs.get(partition);
        for (int i = 0; i <= k; i++) m = Math.max(m, a[i]);
        return m;
    }

    /** Folds this telemetry's W1 contributions (per batch, not yet cumulative) into {@code w1}. */
    void contributeW1(Map<String, long[]> into) {
        w1.forEach((station, a) -> { long[] b = into.get(station); for (int k = 0; k < batches; k++) b[k] = Math.max(b[k], a[k]); });
    }

    /** Flushes and closes every partition file; the digests are then known. */
    TreeMap<String, String> close() {
        if (!closed) {
            closed = true;
            try {
                for (Map.Entry<Integer, DigestOutputStream> e : streams.entrySet()) {
                    e.getValue().close();
                    digests.put("ot.messages/" + e.getKey() + ".jsonl", HexFormat.of().formatHex(e.getValue().getMessageDigest().digest()));
                }
            } catch (IOException e) { throw new UncheckedIOException(e); }
        }
        return digests;
    }

    /** A refused or failed build: the partial directory is removed, so the next build does not find a directory that is not a recording. */
    void abort() {
        try { for (DigestOutputStream s : streams.values()) s.close(); } catch (IOException ignored) { }
        ScenarioBuilder.clear(out);
    }

    private static IllegalArgumentException refuse(String what) { return new IllegalArgumentException("scenario refused: " + what); }
}
