package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.recording.Murmur2;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The stage-2 variants of the committed {@code smoke-incident} recording, written into a directory the caller owns (a gate temporary
 *  directory or a test's), never under {@code fixtures/}. Each copies the recording, edits partition files (offsets renumbered from 0),
 *  through-offsets of the batch manifest, and refreshes the manifest's digest of every edited file. Times are absolute epoch ms;
 *  {@code T} is the smoke visit's enter ({@code T0 + 600 s}). The smoke-incident facts used: occupancy partition 0 carries {@code occ-1}
 *  (arrival at T) and {@code occ-2} (its departure at T + 60 s), the incident {@code inc-1} is the only line of
 *  {@code quality.incidents/1}, and nutrunner1's telemetry is {@code ot.messages/1}. */
public final class Stage2Variants {
    private Stage2Variants() {}

    public static final long T = 1794183000000L;
    public static final String S1 = "synthetic-site:line2:station1", S2 = "synthetic-site:line2:station2", NR1 = S1 + ":nutrunner1";
    public static final String OCC = "mfg.occupancy", INC = "quality.incidents", MSG = "ot.messages";
    public static final String DID_NOT_OCCUR = "urn:epcglobal:cbv:er:did_not_occur";
    /** inc-1's partition (Murmur2 of nutrunner1) and the station2 incident's (Murmur2 of station2). */
    public static final int INC1_PARTITION = 1, STATION2_PARTITION = Murmur2.partition(S2, 6);

    // --- the variants ----------------------------------------------------------------------------------------------------------

    /** {@code smoke-incident-cascade}: an error declaration ({@code did_not_occur}) of the arrival {@code occ-1}, appended to occupancy
     *  partition 0 and delivered in the last batch. */
    public static Path cascade(Path src, Path dst) {
        copy(src, dst);
        List<String> l = lines(dst, OCC, 0);
        ObjectNode decl = declared(value(l.get(0)), DID_NOT_OCCUR, lastScan(dst));
        l.add(line(l.size(), S1, decl));
        write(dst, OCC, 0, l);
        through(dst, OCC, 0, lastCut(dst), l.size() - 1);
        return dst;
    }

    /** {@code smoke-incident-narrow}: the incident narrowed to {@code suspectFrom = T + 30 s}; the visit open (its departure removed) until
     *  a late departure at {@code T + 20 s}, before suspectFrom, delivered at cut {@code departAt}; with {@code reopenAt} non-null an error
     *  declaration of that departure is delivered at cut {@code reopenAt} and the visit is open again. */
    public static Path narrow(Path src, Path dst, long departAt, Long reopenAt) {
        copy(src, dst);
        List<String> inc = lines(dst, INC, INC1_PARTITION);
        ObjectNode v = value(inc.get(0));
        Json.putSafe(v, "suspectFrom", T + 30_000);
        inc.set(0, line(0, NR1, v));
        write(dst, INC, INC1_PARTITION, inc);
        List<String> occ = lines(dst, OCC, 0);
        ObjectNode departure = value(occ.get(1));
        Json.putSafe(departure, "eventTime", T + 20_000);
        long departScan = scanOf(dst, departAt);
        Json.putSafe(departure, "recordTime", departScan);
        List<String> out = new ArrayList<>(List.of(occ.get(0), line(1, S1, departure)));
        if (reopenAt != null) out.add(line(2, S1, declared(departure, DID_NOT_OCCUR, scanOf(dst, reopenAt))));
        write(dst, OCC, 0, out);
        through(dst, OCC, 0, 1, 0);
        through(dst, OCC, 0, departAt, 1);
        if (reopenAt != null) through(dst, OCC, 0, reopenAt, 2);
        return dst;
    }

    /** One unit, two visits at station1: {@code occ-1} [T, T + 60 s) as recorded and a second visit {@code occ-3} [T + 120 s, T + 180 s)
     *  (visitSeq 2, delivered with the first departure), nutrunner1's {@code Running} made true over [T + 125 s, T + 179 s), the incident
     *  widened to [T, T + 180 s) (closed, {@code detectedAt = revisionEventTime = suspectTo}); with {@code cancelFirst} an error declaration
     *  ({@code did_not_occur}) of {@code occ-1} is delivered in the last batch. */
    public static Path twoVisits(Path src, Path dst, boolean cancelFirst) {
        copy(src, dst);
        long enterB = T + 120_000, leaveB = T + 180_000;
        List<String> inc = lines(dst, INC, INC1_PARTITION);
        ObjectNode v = value(inc.get(0));
        Json.putSafe(v, "suspectTo", leaveB);
        Json.putSafe(v, "detectedAt", leaveB);
        Json.putSafe(v, "revisionEventTime", leaveB);
        inc.set(0, line(0, NR1, v));
        write(dst, INC, INC1_PARTITION, inc);
        List<String> occ = lines(dst, OCC, 0);
        ObjectNode arrival = value(occ.get(0)), departure = value(occ.get(1));
        ObjectNode arrB = arrival.deepCopy().put("eventId", "occ-3");
        Json.putSafe(arrB, "eventTime", enterB); Json.putSafe(arrB, "recordTime", enterB + 200); Json.putSafe(arrB, "visitSeq", 2);
        ObjectNode depB = departure.deepCopy().put("eventId", "occ-4").put("arrivalEventId", "occ-3");
        Json.putSafe(depB, "eventTime", leaveB); Json.putSafe(depB, "recordTime", leaveB + 200); Json.putSafe(depB, "visitSeq", 2);
        List<String> out = new ArrayList<>(List.of(occ.get(0), occ.get(1), line(2, S1, arrB), line(3, S1, depB)));
        if (cancelFirst) out.add(line(4, S1, declared(arrival, DID_NOT_OCCUR, lastScan(dst))));
        write(dst, OCC, 0, out);
        // every batch that delivered occ-2 delivers occ-3 and occ-4 with it
        ObjectNode m = manifest(dst);
        for (JsonNode b : m.get("batches")) {
            ObjectNode t = (ObjectNode) b.path("throughOffsets").path(OCC);
            if (t.get("0").longValue() >= 1) t.put("0", 3);
        }
        Json.write(dst.resolve("manifest.json"), m);
        if (cancelFirst) through(dst, OCC, 0, lastCut(dst), 4);
        List<String> msg = lines(dst, MSG, 1);
        for (int i = 0; i < msg.size(); i++) {
            JsonNode n = Json.read(msg.get(i).getBytes(StandardCharsets.UTF_8)).get("value");
            if (!NR1.equals(n.path("device").asText())) continue;
            long ts = n.path("members").get(0).path("sourceTs").longValue();
            if (ts >= enterB + 5_000 && ts < leaveB - 1_000) msg.set(i, msg.get(i).replace("\"metric\":\"Running\",\"value\":false", "\"metric\":\"Running\",\"value\":true"));
        }
        write(dst, MSG, 1, msg);
        return dst;
    }

    /** A second incident {@code inc-2} on {@code station2} (outside the impact rule's {@code incidentScope}), keyed by its equipmentRef on
     *  its Murmur2 partition and delivered in the cut that delivers {@code inc-1}. */
    public static Path station2Incident(Path src, Path dst) {
        copy(src, dst);
        ObjectNode v = value(lines(dst, INC, INC1_PARTITION).get(0));
        v.put("incidentId", "inc-2").put("stationRef", S2).put("equipmentRef", S2).put("evidenceRef", "urn:line2:qms:inc-2");
        List<String> l = lines(dst, INC, STATION2_PARTITION);
        l.add(line(l.size(), S2, v));
        write(dst, INC, STATION2_PARTITION, l);
        through(dst, INC, STATION2_PARTITION, deliveringCut(dst, INC, INC1_PARTITION, 0), l.size() - 1);
        return dst;
    }

    // --- reading the manifest --------------------------------------------------------------------------------------------------

    public static long lastCut(Path rec) {
        JsonNode b = manifest(rec).get("batches");
        return b.get(b.size() - 1).get("cutId").longValue();
    }

    /** The first cut whose through-offset of {@code topic/p} reaches {@code offset}. */
    public static long deliveringCut(Path rec, String topic, int p, long offset) {
        for (JsonNode b : manifest(rec).get("batches"))
            if (b.path("throughOffsets").path(topic).path(Integer.toString(p)).asLong(-1) >= offset) return b.get("cutId").longValue();
        throw new IllegalArgumentException(topic + "/" + p + " offset " + offset + " is never delivered");
    }

    /** The largest member sourceTs of nutrunner1's messages delivered through {@code cutId} (a plausible recordTime for a late input). */
    static long scanOf(Path rec, long cutId) {
        long through = -1;
        for (JsonNode b : manifest(rec).get("batches")) if (b.get("cutId").longValue() == cutId) through = b.path("throughOffsets").path(MSG).path("1").longValue();
        if (through < 0) throw new IllegalArgumentException("no cut " + cutId);
        List<String> l = lines(rec, MSG, 1);
        return value(l.get((int) through)).path("members").get(0).path("sourceTs").longValue();
    }

    static long lastScan(Path rec) { return scanOf(rec, lastCut(rec)); }

    // --- editing ---------------------------------------------------------------------------------------------------------------

    static ObjectNode declared(ObjectNode event, String reason, long declarationTime) {
        ObjectNode n = event.deepCopy();
        Json.putSafe(n, "recordTime", declarationTime + 100);
        ObjectNode d = Json.putSafe(n.putObject("errorDeclaration"), "declarationTime", declarationTime).put("reason", reason);
        d.putArray("correctiveEventIDs");
        return n;
    }

    static ObjectNode value(String line) { return (ObjectNode) Json.read(line.getBytes(StandardCharsets.UTF_8)).get("value").deepCopy(); }

    static String line(long offset, String key, JsonNode value) {
        return "{\"offset\":" + offset + ",\"timestamp\":null,\"key\":" + Json.JSON.getNodeFactory().textNode(key).toString()
            + ",\"headers\":{},\"value\":" + Canonical.utf8(Canonical.bytes(value)) + "}";
    }

    static ObjectNode manifest(Path rec) { return (ObjectNode) Json.read(rec.resolve("manifest.json")); }

    static List<String> lines(Path rec, String topic, int p) {
        try { return new ArrayList<>(Files.readAllLines(rec.resolve(topic).resolve(p + ".jsonl"), StandardCharsets.UTF_8)); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    }

    /** Rewrites one partition file (offsets renumbered from 0) and refreshes the manifest's digest of it. */
    static void edit(Path rec, String topic, int p, UnaryOperator<List<String>> f) { write(rec, topic, p, f.apply(lines(rec, topic, p))); }

    static void write(Path rec, String topic, int p, List<String> l) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < l.size(); i++) sb.append(l.get(i).replaceFirst("^\\{\"offset\":[0-9]+,", "{\"offset\":" + i + ",")).append('\n');
        Path f = rec.resolve(topic).resolve(p + ".jsonl");
        try { Files.write(f, sb.toString().getBytes(StandardCharsets.UTF_8)); } catch (IOException e) { throw new UncheckedIOException(e); }
        ObjectNode m = manifest(rec);
        try { ((ObjectNode) m.get("digests")).put(topic + "/" + p + ".jsonl", Canonical.digestOf(Files.readAllBytes(f))); } catch (IOException e) { throw new UncheckedIOException(e); }
        Json.write(rec.resolve("manifest.json"), m);
    }

    /** Sets the through-offset of {@code topic/p} to {@code value} in every batch from {@code fromCut} on. */
    static void through(Path rec, String topic, int p, long fromCut, long value) {
        ObjectNode m = manifest(rec);
        for (JsonNode b : m.get("batches"))
            if (b.get("cutId").longValue() >= fromCut) ((ObjectNode) b.path("throughOffsets").path(topic)).put(Integer.toString(p), value);
        Json.write(rec.resolve("manifest.json"), m);
    }

    static void copy(Path from, Path to) {
        if (from.toAbsolutePath().normalize().equals(to.toAbsolutePath().normalize())) throw new IllegalArgumentException("a variant is written to its own directory");
        try (Stream<Path> s = Files.walk(from)) {
            for (Path p : s.toList()) {
                Path t = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }
}
