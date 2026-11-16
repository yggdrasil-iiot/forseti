package yggdrasil.forseti.cli.live;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.*;
import yggdrasil.forseti.cli.recording.CaptureTruth;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.ReplayRecording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;

/** {@code forseti recordings verify-capture}: a captured recording checked without rebuilding it (step-5 plan, "Live recording
 *  verification": a capture is lock-verified, not byte-reproducible, because its times are the wall clock's).
 *
 *  <p>{@link #check} is what a capture must satisfy on its own: {@link Recording#open} accepts the manifest and the routing; it is not
 *  synthetic and was written by {@link CaptureRecorder}; every subscribed partition reads strictly (LF-terminated, offsets 0, 1, ...,
 *  an append timestamp on every line that never goes back); every file's digest is the manifest's and no file is missing or extra;
 *  the through-offsets stay inside the records and the last batch includes them all; the first batch carries telemetry on every
 *  subscribed raw partition; and every deadline is bracketed again from the records, on the watermark the harness computes
 *  ({@link ReplayRecording#timing}; the rule resolved from the manifest's {@code rule} pin in the registry, its {@code O} equal
 *  to the manifest's {@code source.capture.outOfOrdernessMs}, and only the telemetry of equipment carrying a member of the rule counted:
 *  {@link CaptureRecorder#w1Step}, the recorder's own rule), at the station and with the values the ground truth's transitions name; and the tail of spec 628 holds on every subscribed raw partition, station and routed device
 *  (the recorder's rule, with the manifest's {@code tailMs}). {@link #lock} then pins the counts, the records per partition and every file's SHA-256, the
 *  manifest's included; {@link #compare} reports how a computed lock differs from a committed one. A truncated, corrupted or missing
 *  partition file, or one whose tail lost records, fails one of these even when its manifest digests were rewritten, because the
 *  lock pins the manifest and the record counts too. */
public final class CaptureLock {
    private CaptureLock() {}

    /** The problems of a capture (empty: it is sound); {@code registry} resolves the manifest's rule pin. */
    public static List<String> check(Path dir, Path registry) {
        List<String> problems = new ArrayList<>();
        Recording rec;
        try { rec = Recording.open(dir); } catch (RuntimeException e) { problems.add(e.getMessage()); return problems; }
        JsonNode m = rec.manifest();
        if (!m.path("synthetic").isBoolean() || m.path("synthetic").booleanValue()) problems.add("manifest: synthetic is not false");
        if (!CaptureRecorder.GENERATOR.equals(m.path("source").path("generator").asText())) problems.add("manifest: source.generator is not " + CaptureRecorder.GENERATOR);
        JsonNode oo = m.path("source").path("capture").path("outOfOrdernessMs");
        if (!oo.isIntegralNumber()) { problems.add("manifest: no source.capture.outOfOrdernessMs"); return problems; }
        CaptureTruth.Temporal temporal;
        try { temporal = CaptureTruth.temporal(registry, m.path("rule")); }
        catch (RuntimeException e) { problems.add("manifest: rule " + m.path("rule") + " does not resolve in " + registry + ": " + e.getMessage()); return problems; }
        if (oo.longValue() != temporal.outOfOrdernessMs())
            problems.add("manifest: source.capture.outOfOrdernessMs " + oo.longValue() + " is not the rule's " + temporal.outOfOrdernessMs());
        RoutingManifest routing = rec.routing();
        try {
            for (String topic : RoutingManifest.TOPICS) for (int p : routing.subscribed(topic)) try (Recording.Reader r = rec.reader(topic, p)) {
                long last = Long.MIN_VALUE;
                for (Recording.Line l; (l = r.next()) != null;) {
                    Long ts = l.record().timestamp();
                    if (ts == null) throw new IllegalArgumentException(topic + "/" + p + " offset " + l.record().offset() + " has no append timestamp");
                    if (ts < last) throw new IllegalArgumentException(topic + "/" + p + " offset " + l.record().offset() + " append time goes back");
                    last = ts;
                }
            }
            rec.verifyDigests();
            rec.verifyBatches();
        } catch (RuntimeException e) { problems.add(e.getMessage()); return problems; }
        Recording.Batch first = rec.batches().get(0);
        for (int p : routing.subscribed(InputParser.MESSAGES))
            if (first.through(InputParser.MESSAGES, p) < 0) problems.add("the first batch carries no telemetry on raw partition " + p + " (spec 406)");
        problems.addAll(brackets(rec, temporal));
        problems.addAll(tail(rec));
        return problems;
    }

    /** The tail rule of spec 628 on the records, with the manifest's {@code tailMs}. */
    private static List<String> tail(Recording rec) {
        RoutingManifest routing = rec.routing();
        JsonNode definitions = rec.manifest().path("definitions");
        CaptureRecorder.Tail t = new CaptureRecorder.Tail();
        for (String topic : RoutingManifest.TOPICS) for (int p : routing.subscribed(topic)) try (Recording.Reader r = rec.reader(topic, p)) {
            for (Recording.Line x; (x = r.next()) != null;) {
                byte[] v = Recording.valueBytes(x.bytes());
                if (!topic.equals(InputParser.MESSAGES)) { t.business(topic, p, x.record().offset(), x.record().key(), v); continue; }
                ReplayRecording.Timing tm = ReplayRecording.timing(topic, p, x.record().offset(), x.record().key(), v, routing, definitions);
                if (tm.station() == null) continue;
                InputParser.Read read = InputParser.read(new yggdrasil.forseti.core.input.Inputs.Origin(topic, p, x.record().offset()), x.record().key(), v);
                t.routed(p, tm.station(), ((yggdrasil.forseti.core.input.Inputs.ProcessMessage) read.record().parsed()).device(), tm.maxTs());
            }
        }
        return t.problems(routing, rec.tailMs());
    }

    /** W1 of every station at every cut, recomputed from the records with the recorder's rule ({@link CaptureRecorder#w1Step}: only the
     *  telemetry of equipment carrying a member of the rule advances it); every deadline at its station and values. */
    private static List<String> brackets(Recording rec, CaptureTruth.Temporal temporal) {
        List<String> problems = new ArrayList<>();
        RoutingManifest routing = rec.routing();
        JsonNode definitions = rec.manifest().path("definitions");
        List<Recording.Batch> batches = rec.batches();
        Map<String, Long> w1Now = new HashMap<>();
        Map<Long, Map<String, Long>> w1 = new HashMap<>();
        // per raw partition and offset: the station whose W1 the message advances and the W1 it advances to (null: none)
        Map<Integer, List<Map.Entry<String, Long>>> steps = new TreeMap<>();
        for (int p : routing.subscribed(InputParser.MESSAGES)) {
            List<Map.Entry<String, Long>> l = new ArrayList<>();
            try (Recording.Reader r = rec.reader(InputParser.MESSAGES, p)) {
                for (Recording.Line x; (x = r.next()) != null;) {
                    byte[] v = Recording.valueBytes(x.bytes());
                    ReplayRecording.Timing t = ReplayRecording.timing(InputParser.MESSAGES, p, x.record().offset(), x.record().key(), v, routing, definitions);
                    Long w = CaptureRecorder.w1Step(t, v, temporal);
                    l.add(w == null ? null : Map.entry(t.station(), w));
                }
            }
            steps.put(p, l);
        }
        Map<Integer, Integer> at = new HashMap<>();
        for (Recording.Batch b : batches) {
            for (Map.Entry<Integer, List<Map.Entry<String, Long>>> e : steps.entrySet()) {
                int i = at.getOrDefault(e.getKey(), 0);
                for (; i <= b.through(InputParser.MESSAGES, e.getKey()); i++) {
                    Map.Entry<String, Long> s = e.getValue().get(i);
                    if (s != null) w1Now.merge(s.getKey(), s.getValue(), Math::max);
                }
                at.put(e.getKey(), i);
            }
            w1.put(b.cutId(), new HashMap<>(w1Now));
        }
        List<JsonNode> transitions = new ArrayList<>();
        try { rec.expected().path("transitions").forEach(transitions::add); } catch (RuntimeException e) { problems.add("expected.json: " + e.getMessage()); return problems; }
        for (Recording.Deadline d : rec.deadlines()) {
            String label = "deadline " + d.kind() + " " + d.ref() + " at " + d.at();
            JsonNode t = null;
            for (JsonNode x : transitions)
                if (d.kind().equals(x.path("assertion").asText()) && d.ref().equals(x.path("ref").asText()) && x.path("at").asLong() == d.at()
                    && x.path("cutA").asLong() == d.cutA() && x.path("cutB").asLong() == d.cutB()) t = x;
            if (t == null) { problems.add(label + ": no transition in expected.json names it"); continue; }
            String s = t.path("station").asText();
            Long wa = w1.get(d.cutA()).get(s), wb = w1.get(d.cutB()).get(s);
            if (wa == null || wb == null || !(wa < d.at() && d.at() <= wb)) { problems.add(label + ": W1 of " + s + " " + wa + " at cut " + d.cutA() + ", " + wb + " at cut " + d.cutB() + " do not bracket it"); continue; }
            if (t.path("w1A").asLong() != wa || t.path("w1B").asLong() != wb) problems.add(label + ": W1 " + wa + ", " + wb + " differ from expected.json's " + t.path("w1A") + ", " + t.path("w1B"));
        }
        return problems;
    }

    /** The lock of a sound capture: name, generator, counts, records per partition, every file's SHA-256. */
    public static ObjectNode lock(Path dir) {
        Recording rec = Recording.open(dir);
        ObjectNode lock = Json.object();
        Json.putSafe(lock, "lockVersion", 1);
        lock.put("recording", rec.manifest().path("name").asText());
        lock.put("generator", rec.manifest().path("source").path("generator").asText());
        ObjectNode counts = lock.putObject("counts");
        ObjectNode records = lock.putObject("records");
        for (String topic : RoutingManifest.TOPICS) {
            long c = 0;
            ObjectNode t = records.putObject(topic);
            for (int p : rec.routing().subscribed(topic)) { long s = rec.size(topic, p); c += s; Json.putSafe(t, Integer.toString(p), s); }
            Json.putSafe(counts, topic, c);
        }
        Json.putSafe(counts, "cuts", rec.batches().size());
        Json.putSafe(counts, "deadlines", rec.deadlines().size());
        ObjectNode files = lock.putObject("files");
        TreeMap<String, String> all = new TreeMap<>(rec.digests());
        try { all.put(Recording.MANIFEST, Canonical.digestOf(Files.readAllBytes(dir.resolve(Recording.MANIFEST)))); }
        catch (IOException e) { throw new UncheckedIOException(e); }
        all.forEach(files::put);
        return lock;
    }

    /** How {@code lock} differs from the committed lock bytes (empty: byte-identical). */
    public static List<String> compare(ObjectNode lock, byte[] committed) {
        List<String> problems = new ArrayList<>();
        if (Arrays.equals(committed, Json.pretty(lock))) return problems;
        JsonNode c;
        try { c = Json.read(committed); } catch (RuntimeException e) { problems.add("the lock is not JSON: " + e.getMessage()); return problems; }
        for (String f : List.of("lockVersion", "recording", "generator", "counts", "records"))
            if (!text(c.get(f)).equals(text(lock.get(f)))) problems.add(f + ": locked " + c.get(f) + ", found " + lock.get(f));
        TreeSet<String> names = new TreeSet<>();
        c.path("files").fieldNames().forEachRemaining(names::add);
        lock.path("files").fieldNames().forEachRemaining(names::add);
        for (String f : names) if (!text(c.path("files").get(f)).equals(text(lock.path("files").get(f)))) problems.add("file " + f + ": locked " + c.path("files").get(f) + ", found " + lock.path("files").get(f));
        if (problems.isEmpty()) problems.add("the lock's bytes differ");
        return problems;
    }

    private static String text(JsonNode n) {
        if (n == null) return "absent";
        ObjectNode w = Json.object();
        w.set("v", n);
        return Canonical.utf8(Canonical.bytes(w));
    }
}
