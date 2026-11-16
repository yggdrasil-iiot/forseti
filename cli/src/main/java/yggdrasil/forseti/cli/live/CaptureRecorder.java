package yggdrasil.forseti.cli.live;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.recording.CaptureTruth;
import yggdrasil.forseti.cli.recording.ExpectedFiles;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.ReplayRecording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;

/** The step-5 recorder without Kafka (plan D1, D6): the records of a run's input topics, read at fixed terminal offsets, written as a
 *  recording directory in the step-3 format, so {@link Recording#open} and the oracle's reader read a capture as they read a built
 *  recording. The Kafka consumer is the loop's ({@code forseti-loop record}); it hands the records over through {@link Source}.
 *
 *  <p>What is written. {@code <topic>/<partition>.jsonl} for every subscribed partition of the four input topics, one line per record
 *  {@code {"offset","timestamp","key","headers","value"}} with the broker's offset and append time and the value bytes exactly as
 *  delivered (spliced into the line, never re-serialized); {@code routing.json}, the run's placement file byte for byte;
 *  {@code expected.json} from {@link CaptureTruth} in version 2 ({@link ExpectedFiles}: the samples in {@code expected-samples/}); {@code manifest.json} with {@code synthetic: false}, the rule pin, the deployment's
 *  definition pins, {@code T0}, {@code scanMs}, the evidence range of every device, the batches, the deadlines with their brackets,
 *  {@code settleTargetW}, {@code tailMs} ({@code L + O + P} of the rule) and the digests. The dead-letter topic is not an input: its
 *  records go to a side file next to the recording (raw values in base64), and so does the capture report.
 *
 *  <p>What is refused, before anything is written: a topic that does not start at offset 0 (D6 runs on fresh topics), records missing
 *  below the terminal offset or an offset gap (control records of a transaction), a timestamp that is not {@code LogAppendTime} or
 *  that goes back within a partition, a record on a partition the placement does not subscribe, a null key, headers on an input
 *  topic, a value that is not one compact-delimited JSON object, a routed device without telemetry, a drive whose sent occupancy or
 *  incident records differ from the captured ones, a deadline whose brackets cannot be proved, a tail shorter than the spec's (below),
 *  and any input the capture would carry only as a fault of the loop itself: a message the core's reader rejects, a kept member
 *  without {@code sourceTs}, a message whose {@code definitionSha256} is not the manifest's pin, and a message or session event of a
 *  device outside the routing. Those are fixture-admission rules: how the core handles such inputs is covered by the synthetic
 *  recordings of the core gate, and a live fixture that carried them by accident would test the loop, not the core.
 *
 *  <p>Batches. A capture has no generation order, so batch k is a time in the broker's append clock and includes, on every partition,
 *  the records appended at or before it: the delivery-time assignment of the builder ("by event time, or by {@code deliverAt} when an
 *  overlay moved them"), with the append time as the delivery time. Let {@code start} be the first instant at which every subscribed
 *  {@code ot.messages} partition has a record (spec 406); {@code start} itself is not a boundary. The boundaries are
 *  {@code start + k * batchMs} for k = 1, 2, ... below the last record, each moved back to the latest append time at or before it,
 *  and the last record. Every deadline {@code d} of station s adds the bracket of the builder's rule on the watermark of the harness
 *  ({@code W1 = max routed sourceTs - O - 1} over the devices that carry a member of the rule, {@link ReplayRecording#NO_TELEMETRY}
 *  while s has received none,
 *  {@link ReplayRecording#timing}): {@code cutB} at the first append time where {@code W1(s) >= d}, {@code cutA} at the append time
 *  before it, so {@code W1_A(s) < d <= W1_B(s)} on consecutive cuts. A {@code cutA} at which s has no routed telemetry, or that lies
 *  before {@code start}, is refused.
 *
 *  <p>Tail (spec 628, "on every subscribed device and partition"): with the last business event the latest occupancy
 *  {@code eventTime}, incident {@code detectedAt} or {@code revisionEventTime} and session event time, the largest routed valid
 *  {@code sourceTs} of every subscribed {@code ot.messages} partition, every station and every routed device must be at least that
 *  event plus {@code tailMs = L + O + P}. A device whose Muninn was killed and restarted (injection 8) has a gap from its death to its
 *  next session-up and still its tail; one that stopped early has not. */
public final class CaptureRecorder {
    private CaptureRecorder() {}

    public static final String GENERATOR = "forseti capture 0.1.0";
    public static final long DEFAULT_BATCH_MS = 60_000;
    public static final String REPORT_SUFFIX = ".capture-report.json", DEAD_LETTER_SUFFIX = ".dead-letter.jsonl";

    /** One consumed record: broker offset, timestamp and its type, key (UTF-8, null when absent), headers, value bytes. */
    public record Raw(long offset, long timestamp, String timestampType, String key, Map<String, byte[]> headers, byte[] value) {}

    /** The consumer of the run's topics: the log start and end offsets, read once, and every record below the end offset. */
    public interface Source {
        long beginningOffset(String topic, int partition);
        long endOffset(String topic, int partition);
        List<Raw> read(String topic, int partition, long endOffset);
    }

    /** What the capture needs besides the records. {@code sent}: the occupancy and incident records the drive log says were sent
     *  (null: not checked). {@code source}: the manifest's {@code source} fields the caller names (scenario, digests). */
    public record Inputs(String name, Path registry, JsonNode rulePin, ObjectNode definitions, byte[] routing, long t0, long scanMs, long batchMs,
                         String occupancySource, Map<String, CaptureTruth.Visit> facts, List<JsonNode> sent, ObjectNode source) {}

    /** A capture that cannot be written; nothing was written. */
    public static final class Refused extends IllegalArgumentException {
        public Refused(String m) { super("capture refused: " + m); }
    }

    public record Result(Path out, ObjectNode manifest, String manifestSha256, ObjectNode report) {}

    /** The side files' default places: {@code <out>.capture-report.json} and {@code <out>.dead-letter.jsonl}, next to the directory. */
    public static Path besides(Path out, String suffix) {
        Path abs = out.toAbsolutePath().normalize();
        return abs.resolveSibling(abs.getFileName() + suffix);
    }

    private record Rec(String topic, int partition, Raw raw, ReplayRecording.Timing timing) {}

    /** The W1 rule of the harness (the gate's {@code J-W1-FORMULA}), shared by the recorder and {@link CaptureLock} so the two cannot
     *  drift: an {@code ot.messages} record with timing {@code t} and value bytes {@code value} advances the W1 of {@code t}'s station to
     *  {@code maxTs - O - 1} only when its device carries a member of the rule ({@link CaptureTruth.Temporal#memberEquipment}); null when
     *  it advances no W1 (no station, or a routed device without a rule member). */
    static Long w1Step(ReplayRecording.Timing t, byte[] value, CaptureTruth.Temporal temporal) {
        if (t == null || t.station() == null) return null;
        // the timing has a station, so the value parsed as a process message
        if (!temporal.memberEquipment().contains(Json.read(value).path("device").asText())) return null;
        return t.maxTs() - temporal.outOfOrdernessMs() - 1;
    }

    public static Result record(Source src, String prefix, Inputs in, Path out, Path reportFile, Path deadLetterFile) {
        RoutingManifest routing = RoutingManifest.read(Json.read(in.routing()));
        CaptureTruth.Temporal temporal = CaptureTruth.temporal(in.registry(), in.rulePin());
        if (in.batchMs() <= 0) throw new Refused("batchMs " + in.batchMs());
        if (Files.exists(out)) {
            try (Stream<Path> s = Files.list(out)) { if (s.findAny().isPresent()) throw new Refused(out + " exists and is not empty"); }
            catch (IOException e) { throw new UncheckedIOException(e); }
        }

        // (1) the records at their terminal offsets
        Map<String, List<Rec>> parts = new LinkedHashMap<>();
        ObjectNode terminal = Json.object();
        for (String topic : RoutingManifest.TOPICS) {
            ObjectNode t = terminal.putObject(topic);
            for (int p = 0; p < routing.partitions(); p++) {
                List<Raw> raws = consume(src, LiveTopics.name(prefix, topic), p);
                Json.putSafe(t, Integer.toString(p), raws.size());
                boolean subscribed = routing.subscribed(topic).contains(p);
                if (!subscribed) { if (!raws.isEmpty()) throw new Refused(raws.size() + " records on " + topic + "/" + p + ", which the placement does not subscribe"); continue; }
                List<Rec> l = new ArrayList<>();
                long last = Long.MIN_VALUE;
                for (Raw r : raws) {
                    String where = topic + "/" + p + " offset " + r.offset();
                    if (!"LogAppendTime".equals(r.timestampType())) throw new Refused(where + " has timestamp type " + r.timestampType() + ", not LogAppendTime");
                    if (r.timestamp() < 0 || r.timestamp() > Json.MAX_SAFE) throw new Refused(where + " timestamp " + r.timestamp());
                    if (r.timestamp() < last) throw new Refused(where + " append time " + r.timestamp() + " goes back from " + last);
                    last = r.timestamp();
                    if (r.key() == null) throw new Refused(where + " has no key");
                    if (r.headers() != null && !r.headers().isEmpty()) throw new Refused(where + " carries headers " + r.headers().keySet() + " on an input topic");
                    checkValue(r.value(), where);
                    ReplayRecording.Timing tm = topic.equals(InputParser.MESSAGES)
                        ? ReplayRecording.timing(topic, p, r.offset(), r.key(), r.value(), routing, in.definitions()) : null;
                    l.add(new Rec(topic, p, r, tm));
                }
                parts.put(topic + "/" + p, l);
            }
        }
        ArrayNode deadLetter = Json.JSON.createArrayNode();
        ObjectNode dlTerminal = terminal.putObject(LiveTopics.DEAD_LETTER);
        for (int p = 0; p < routing.partitions(); p++) {
            List<Raw> raws = consume(src, LiveTopics.name(prefix, LiveTopics.DEAD_LETTER), p);
            Json.putSafe(dlTerminal, Integer.toString(p), raws.size());
            for (Raw r : raws) deadLetter.add(deadLetterLine(p, r));
        }

        // (2) every record the drive sent was captured, and nothing else on the drive's topics
        if (in.sent() != null) {
            List<String> sent = new ArrayList<>(), captured = new ArrayList<>();
            for (JsonNode s : in.sent()) sent.add(Canonical.utf8(Canonical.bytes(s)));
            for (Map.Entry<String, List<Rec>> e : parts.entrySet())
                if (e.getKey().startsWith(InputParser.OCCUPANCY + "/") || e.getKey().startsWith(InputParser.INCIDENTS + "/"))
                    for (Rec r : e.getValue()) captured.add(Canonical.utf8(Canonical.bytes(Json.read(r.raw().value()))));
            Collections.sort(sent);
            Collections.sort(captured);
            if (!sent.equals(captured)) {
                List<String> missing = new ArrayList<>(sent), extra = new ArrayList<>(captured);
                for (String c : captured) missing.remove(c);
                for (String s : sent) extra.remove(s);
                throw new Refused("the drive log and the captured occupancy and incident records differ: sent but not captured " + missing.size()
                    + (missing.isEmpty() ? "" : " (first " + missing.get(0) + ")") + ", captured but not sent " + extra.size() + (extra.isEmpty() ? "" : " (first " + extra.get(0) + ")"));
            }
        }

        // (3) telemetry facts: evidence range per device; the fixture-admission refusals
        Map<String, long[]> evidence = new TreeMap<>();
        Admission rejected = new Admission("message(s) the core's reader rejects"), noSourceTs = new Admission("kept member(s) without sourceTs"),
            pinMismatch = new Admission("message(s) whose definitionSha256 is not the manifest's pin"),
            outside = new Admission("message(s) or session event(s) of a device outside the routing");
        List<byte[]> messageValues = new ArrayList<>();
        Tail tail = new Tail();
        for (int p : routing.subscribed(InputParser.MESSAGES)) for (Rec r : parts.get(InputParser.MESSAGES + "/" + p)) {
            messageValues.add(r.raw().value());
            String where = r.topic() + "/" + p + " offset " + r.raw().offset();
            InputParser.Read read = InputParser.read(new yggdrasil.forseti.core.input.Inputs.Origin(r.topic(), p, r.raw().offset()), r.raw().key(), r.raw().value());
            if (read.rejected() || !(read.record().parsed() instanceof yggdrasil.forseti.core.input.Inputs.ProcessMessage m)) { rejected.add(where); continue; }
            if (!routing.devices().contains(m.device())) { outside.add(where + " (" + m.device() + ")"); continue; }
            if (!in.definitions().path(m.device()).path("sha256").asText("").equals(m.definitionSha256())) pinMismatch.add(where);
            for (yggdrasil.forseti.core.input.Inputs.Member x : m.members()) {
                if (x.sourceTs() == null) { noSourceTs.add(where + " member " + x.metric()); continue; }
                long[] e = evidence.computeIfAbsent(m.device(), k -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE});
                e[0] = Math.min(e[0], x.sourceTs()); e[1] = Math.max(e[1], x.sourceTs());
            }
            if (r.timing().station() != null) tail.routed(p, r.timing().station(), m.device(), r.timing().maxTs());
        }
        for (int p : routing.subscribed(InputParser.SESSIONS)) for (Rec r : parts.get(InputParser.SESSIONS + "/" + p)) {
            InputParser.Read read = InputParser.read(new yggdrasil.forseti.core.input.Inputs.Origin(r.topic(), p, r.raw().offset()), r.raw().key(), r.raw().value());
            if (!read.rejected() && read.record().parsed() instanceof yggdrasil.forseti.core.input.Inputs.SessionEvent s && !routing.devices().contains(s.device()))
                outside.add(r.topic() + "/" + p + " offset " + r.raw().offset() + " (" + s.device() + ")");
        }
        for (Admission x : List.of(rejected, noSourceTs, pinMismatch, outside)) x.refuseIfAny();
        for (String d : routing.devices()) if (!evidence.containsKey(d)) throw new Refused("no telemetry from routed device " + d);
        for (Map.Entry<String, List<Rec>> e : parts.entrySet())
            if (!e.getKey().startsWith(InputParser.MESSAGES + "/")) for (Rec r : e.getValue()) tail.business(r.topic(), r.partition(), r.raw().offset(), r.raw().key(), r.raw().value());

        // (4) batches in the append clock, and the deadline brackets on W1
        TreeSet<Long> times = new TreeSet<>();
        parts.values().forEach(l -> l.forEach(r -> times.add(r.raw().timestamp())));
        long start = Long.MIN_VALUE;
        for (int p : routing.subscribed(InputParser.MESSAGES)) {
            List<Rec> l = parts.get(InputParser.MESSAGES + "/" + p);
            if (l.isEmpty()) throw new Refused("no telemetry on subscribed raw partition " + p + " (spec 406)");
            start = Math.max(start, l.get(0).raw().timestamp());
        }
        long end = times.last();
        List<String> stations = new ArrayList<>(routing.stations().keySet());
        // W1 of every station after every distinct append time
        List<Long> order = new ArrayList<>(times);
        Map<Long, Integer> at = new HashMap<>();
        for (int i = 0; i < order.size(); i++) at.put(order.get(i), i);
        Map<String, long[]> w1At = new TreeMap<>();
        for (String s : stations) { long[] a = new long[order.size()]; Arrays.fill(a, ReplayRecording.NO_TELEMETRY); w1At.put(s, a); }
        // as the harness: a message advances W1 only when its device carries a member of the rule
        for (int p : routing.subscribed(InputParser.MESSAGES)) for (Rec r : parts.get(InputParser.MESSAGES + "/" + p)) {
            Long w = w1Step(r.timing(), r.raw().value(), temporal);
            if (w == null) continue;
            long[] a = w1At.get(r.timing().station());
            int i = at.get(r.raw().timestamp());
            a[i] = Math.max(a[i], w);
        }
        for (long[] a : w1At.values()) for (int i = 1; i < a.length; i++) a[i] = Math.max(a[i], a[i - 1]);

        List<CaptureTruth.Input> undated = inputs(parts, null);
        Map<String, String> stationOf = new TreeMap<>();
        for (String d : routing.devices()) stationOf.put(d, routing.stationOf(d));
        List<CaptureTruth.Due> dues = CaptureTruth.deadlines(in.registry(), in.rulePin(), stationOf, undated, in.facts());
        TreeSet<Long> bounds = new TreeSet<>();
        for (long g = start + in.batchMs(); g < end; g += in.batchMs()) bounds.add(times.floor(g));
        bounds.add(end);
        Map<CaptureTruth.Due, long[]> bracketAt = new LinkedHashMap<>();
        for (CaptureTruth.Due d : dues) {
            long[] a = w1At.get(d.station());
            if (a == null) throw new Refused("deadline " + d.kind() + " " + d.ref() + " names station " + d.station() + ", which the placement does not route");
            int b = -1;
            for (int i = 0; i < a.length && b < 0; i++) if (a[i] >= d.at()) b = i;
            if (b < 0) throw new Refused("deadline " + d.kind() + " " + d.ref() + " (" + d.at() + ") is never reached: W1 of " + d.station() + " ends at "
                + (a.length == 0 || a[a.length - 1] == ReplayRecording.NO_TELEMETRY ? "no telemetry" : Long.toString(a[a.length - 1])) + "; the capture's tail is too short to bracket it");
            if (b == 0 || a[b - 1] == ReplayRecording.NO_TELEMETRY)
                throw new Refused("deadline " + d.kind() + " " + d.ref() + " (" + d.at() + "): station " + d.station()
                    + " has no routed telemetry at the cut before W1 reaches it, so no cut proves W1 < d there");
            if (order.get(b - 1) < start)
                throw new Refused("deadline " + d.kind() + " " + d.ref() + " (" + d.at() + ") is reached before every raw partition has delivered telemetry; no cut can precede it");
            bounds.add(order.get(b - 1));
            bounds.add(order.get(b));
            bracketAt.put(d, new long[] {order.get(b - 1), order.get(b)});
        }
        List<String> shortTails = tail.problems(routing, temporal.allowedLatenessMs() + temporal.outOfOrdernessMs() + temporal.expectedPeriodMs());
        if (!shortTails.isEmpty()) throw new Refused(String.join("; ", shortTails));
        List<Long> boundaries = new ArrayList<>(bounds);
        int n = boundaries.size();
        Map<String, long[]> w1 = new TreeMap<>();
        for (String s : stations) {
            long[] a = new long[n];
            for (int k = 0; k < n; k++) a[k] = w1At.get(s)[at.get(boundaries.get(k))];
            w1.put(s, a);
        }
        List<CaptureTruth.Bracket> brackets = new ArrayList<>();
        ArrayNode deadlines = Json.JSON.createArrayNode();
        long settleTarget = Long.MIN_VALUE;
        for (Map.Entry<CaptureTruth.Due, long[]> e : bracketAt.entrySet()) {
            CaptureTruth.Due d = e.getKey();
            int a = boundaries.indexOf(e.getValue()[0]), b = boundaries.indexOf(e.getValue()[1]);
            long wa = w1.get(d.station())[a], wb = w1.get(d.station())[b];
            if (b != a + 1 || !(wa < d.at() && d.at() <= wb))   // the construction proves it; checked again on the batches as written
                throw new Refused("deadline " + d.kind() + " " + d.ref() + " (" + d.at() + ") without bracketing cuts: W1 " + wa + ", " + wb);
            ObjectNode o = deadlines.addObject().put("kind", d.kind()).put("ref", d.ref());
            Json.putSafe(o, "at", d.at());
            Json.putSafe(Json.putSafe(o, "cutA", a + 1L), "cutB", b + 1L);
            brackets.add(new CaptureTruth.Bracket(d, a, b));
            settleTarget = Math.max(settleTarget, d.at());
        }
        if (settleTarget == Long.MIN_VALUE) {
            settleTarget = Long.MAX_VALUE;
            for (long[] a : w1.values()) if (a[n - 1] != ReplayRecording.NO_TELEMETRY) settleTarget = Math.min(settleTarget, a[n - 1]);
        }

        ArrayNode batches = Json.JSON.createArrayNode();
        Map<String, int[]> counts = new HashMap<>();   // records per partition through each batch
        for (Map.Entry<String, List<Rec>> e : parts.entrySet()) {
            int[] c = new int[n];
            int i = 0;
            for (int k = 0; k < n; k++) { while (i < e.getValue().size() && e.getValue().get(i).raw().timestamp() <= boundaries.get(k)) i++; c[k] = i; }
            counts.put(e.getKey(), c);
        }
        for (int k = 0; k < n; k++) {
            ObjectNode b = Json.putSafe(Json.object(), "cutId", k + 1L);
            ObjectNode to = b.putObject("throughOffsets");
            for (String topic : RoutingManifest.TOPICS) {
                if (routing.subscribed(topic).isEmpty()) continue;
                ObjectNode t = to.putObject(topic);
                for (int p : routing.subscribed(topic)) Json.putSafe(t, Integer.toString(p), counts.get(topic + "/" + p)[k] - 1L);
            }
            b.putArray("idleInputs");
            batches.add(b);
        }

        // (5) the ground truth on the batches as planned
        List<CaptureTruth.Input> dated = inputs(parts, boundaries);
        ObjectNode expected = CaptureTruth.expected(in.registry(), in.rulePin(), in.occupancySource(), stationOf, dated, w1, n, in.facts(), evidence, in.t0(),
            brackets, settleTarget);

        // (6) the files, then the manifest
        TreeMap<String, byte[]> files = new TreeMap<>();
        for (Map.Entry<String, List<Rec>> e : parts.entrySet()) files.put(e.getKey() + ".jsonl", partitionBytes(e.getValue()));
        files.put(Recording.ROUTING, in.routing().clone());
        files.putAll(ExpectedFiles.split(expected));   // version 2: expected.json and expected-samples/<iiii>.json
        ObjectNode manifest = Json.object();
        Json.putSafe(manifest, "recordingVersion", 1);
        manifest.put("name", in.name());
        manifest.put("synthetic", false);
        ObjectNode source = in.source() == null ? Json.object() : in.source().deepCopy();
        source.put("generator", GENERATOR);
        ObjectNode capture = source.putObject("capture");
        capture.put("topicPrefix", prefix == null ? "" : prefix);
        Json.putSafe(capture, "batchMs", in.batchMs());
        Json.putSafe(capture, "outOfOrdernessMs", temporal.outOfOrdernessMs());
        capture.set("terminalOffsets", terminal.deepCopy());
        manifest.set("source", source);
        manifest.set("rule", in.rulePin().deepCopy());
        manifest.set("definitions", in.definitions().deepCopy());
        Json.putSafe(manifest, "T0", in.t0());
        Json.putSafe(manifest, "scanMs", in.scanMs());
        ObjectNode range = manifest.putObject("evidenceRange");
        evidence.forEach((d, e) -> { ObjectNode r = range.putObject(d); Json.putSafe(r, "from", e[0]); Json.putSafe(r, "to", e[1]); });
        manifest.set("batches", batches);
        manifest.set("deadlines", deadlines);
        Json.putSafe(manifest, "settleTargetW", settleTarget);
        Json.putSafe(manifest, "tailMs", temporal.allowedLatenessMs() + temporal.outOfOrdernessMs() + temporal.expectedPeriodMs());
        ObjectNode digests = manifest.putObject("digests");
        files.forEach((f, b) -> digests.put(f, Canonical.digestOf(b)));
        byte[] manifestBytes = Json.pretty(manifest);
        String manifestSha = Canonical.digestOf(manifestBytes);

        // (7) the capture report (D8 rates)
        ObjectNode report = Json.object();
        Json.putSafe(report, "captureReportVersion", 1);
        report.put("recording", in.name());
        report.put("generator", GENERATOR);
        report.put("manifestSha256", manifestSha);
        report.set("terminalOffsets", terminal);
        ObjectNode rc = report.putObject("counts");
        for (String topic : RoutingManifest.TOPICS) {
            long c = 0;
            for (int p : routing.subscribed(topic)) c += parts.get(topic + "/" + p).size();
            Json.putSafe(rc, topic, c);
        }
        Json.putSafe(rc, LiveTopics.DEAD_LETTER, deadLetter.size());
        Json.putSafe(rc, "cuts", n);
        Json.putSafe(rc, "deadlines", deadlines.size());
        if (tail.last != null) Json.putSafe(report, "lastBusinessEvent", tail.last);
        report.set("rates", CaptureRates.measure(messageValues));
        ObjectNode tails = report.putObject("lastSourceTs");
        evidence.forEach((d, e) -> Json.putSafe(tails, d, e[1]));
        ArrayNode rb = report.putArray("brackets");
        for (CaptureTruth.Bracket b : brackets) {
            ObjectNode o = rb.addObject().put("kind", b.due().kind()).put("ref", b.due().ref()).put("station", b.due().station());
            Json.putSafe(o, "at", b.due().at());
            Json.putSafe(Json.putSafe(o, "cutA", b.cutA() + 1L), "cutB", b.cutB() + 1L);
            Json.putSafe(Json.putSafe(o, "w1A", w1.get(b.due().station())[b.cutA()]), "w1B", w1.get(b.due().station())[b.cutB()]);
        }

        // nothing above wrote; now the directory, then the side files
        try {
            Files.createDirectories(out);
            for (Map.Entry<String, byte[]> f : files.entrySet()) {
                Path p = out.resolve(f.getKey());
                Files.createDirectories(p.getParent());
                Files.write(p, f.getValue());
            }
            Files.write(out.resolve(Recording.MANIFEST), manifestBytes);
            if (reportFile != null) Json.write(reportFile.toAbsolutePath(), report);
            if (deadLetterFile != null) {
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                for (JsonNode l : deadLetter) { b.write(Canonical.bytes(l)); b.write('\n'); }
                Files.createDirectories(deadLetterFile.toAbsolutePath().getParent());
                Files.write(deadLetterFile, b.toByteArray());
            }
        } catch (IOException e) { throw new UncheckedIOException(e); }
        return new Result(out, manifest, manifestSha, report);
    }

    /** One fixture-admission rule: the count of inputs that break it and the first of them. */
    private static final class Admission {
        final String what; long count; String first;
        Admission(String what) { this.what = what; }
        void add(String where) { if (count++ == 0) first = where; }
        void refuseIfAny() { if (count > 0) throw new Refused(count + " " + what + ", the first at " + first); }
    }

    /** The tail rule of spec 628, shared by the recorder and {@link CaptureLock}: the last business event, and the largest routed valid
     *  {@code sourceTs} per subscribed raw partition, per station and per routed device. */
    static final class Tail {
        Long last;
        final Map<Integer, Long> partition = new TreeMap<>();
        final Map<String, Long> station = new TreeMap<>(), device = new TreeMap<>();

        void routed(int p, String st, String dev, long maxTs) {
            partition.merge(p, maxTs, Math::max);
            station.merge(st, maxTs, Math::max);
            device.merge(dev, maxTs, Math::max);
        }

        /** A business event of an occupancy, incident or session record: occupancy {@code eventTime}, incident {@code detectedAt}
         *  and {@code revisionEventTime}, a session event's time (none for the birthless variant). */
        void business(String topic, int p, long offset, String key, byte[] value) {
            InputParser.Read r = InputParser.read(new yggdrasil.forseti.core.input.Inputs.Origin(topic, p, offset), key, value);
            if (r.rejected()) return;
            Object x = r.record().parsed();
            if (x instanceof yggdrasil.forseti.core.input.Inputs.IncidentEvent i) { at(i.detectedAt()); at(i.revisionEventTime()); }
            else if (x instanceof yggdrasil.forseti.core.input.Inputs.OccupancyEvent || x instanceof yggdrasil.forseti.core.input.Inputs.SessionEvent) {
                Long t = yggdrasil.forseti.core.stage1.Events.eventTime(r.record());
                if (t != null) at(t);
            }
        }

        private void at(long t) { last = last == null ? t : Math.max(last, t); }

        /** Every partition, station and routed device whose largest routed {@code sourceTs} is below the last business event plus
         *  {@code tailMs}; empty when there is no business event. */
        List<String> problems(RoutingManifest routing, long tailMs) {
            List<String> l = new ArrayList<>();
            if (last == null) return l;
            long need = last + tailMs;
            for (int p : routing.subscribed(InputParser.MESSAGES)) check(l, "raw partition " + p, partition.get(p), need, tailMs);
            for (String s : routing.stations().keySet()) check(l, "station " + s, station.get(s), need, tailMs);
            for (String d : routing.devices()) check(l, "device " + d, device.get(d), need, tailMs);
            return l;
        }

        private void check(List<String> l, String what, Long max, long need, long tailMs) {
            if (max == null || max < need)
                l.add("the tail of " + what + " ends at " + (max == null ? "no routed telemetry" : Long.toString(max)) + ", below the last business event "
                    + last + " + tailMs " + tailMs + " (spec 628)");
        }
    }

    /** The records of one partition, from the log start (which must be 0) to the end offset read once; offsets contiguous. */
    private static List<Raw> consume(Source src, String topic, int p) {
        long begin = src.beginningOffset(topic, p), end = src.endOffset(topic, p);
        if (begin != 0) throw new Refused(topic + "/" + p + " starts at offset " + begin + ", not 0: not a fresh topic (D6)");
        List<Raw> raws = src.read(topic, p, end);
        if (raws.size() != end) throw new Refused(topic + "/" + p + ": " + raws.size() + " records read below the terminal offset " + end);
        for (int i = 0; i < raws.size(); i++)
            if (raws.get(i).offset() != i) throw new Refused(topic + "/" + p + " offset " + raws.get(i).offset() + " where " + i + " is due (a gap: transaction markers or a truncated log)");
        return raws;
    }

    /** A value is one JSON object whose bytes begin with '{' and end with '}' and carry no CR or LF, so it can be spliced into a line
     *  and {@link Recording#valueBytes} returns it unchanged. */
    private static void checkValue(byte[] v, String where) {
        if (v == null || v.length < 2 || v[0] != '{' || v[v.length - 1] != '}') throw new Refused(where + ": the value is not a JSON object without surrounding whitespace");
        for (byte b : v) if (b == '\n' || b == '\r') throw new Refused(where + ": the value carries a line break");
        try { if (!Json.read(v).isObject()) throw new Refused(where + ": the value is not a JSON object"); }
        catch (IllegalArgumentException e) { if (e instanceof Refused) throw e; throw new Refused(where + ": the value is not JSON: " + e.getMessage()); }
    }

    private static final byte[] OFFSET = "{\"offset\":".getBytes(StandardCharsets.US_ASCII);

    private static byte[] partitionBytes(List<Rec> l) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        try {
            for (Rec r : l) {
                b.write(OFFSET);
                b.write(Long.toString(r.raw().offset()).getBytes(StandardCharsets.US_ASCII));
                b.write((",\"timestamp\":" + r.raw().timestamp() + ",\"key\":").getBytes(StandardCharsets.US_ASCII));
                b.write(Json.JSON.writeValueAsBytes(r.raw().key()));
                b.write(",\"headers\":{},\"value\":".getBytes(StandardCharsets.US_ASCII));
                b.write(r.raw().value());
                b.write('}');
                b.write('\n');
            }
        } catch (IOException e) { throw new UncheckedIOException(e); }
        return b.toByteArray();
    }

    /** The captured input records in topic, partition and offset order, each with its batch ({@code boundaries} null: batch 0). */
    private static List<CaptureTruth.Input> inputs(Map<String, List<Rec>> parts, List<Long> boundaries) {
        List<CaptureTruth.Input> l = new ArrayList<>();
        for (List<Rec> rs : parts.values()) for (Rec r : rs) {
            int batch = 0;
            if (boundaries != null) {
                int i = Collections.binarySearch(boundaries, r.raw().timestamp());
                batch = i >= 0 ? i : -i - 1;
            }
            l.add(new CaptureTruth.Input(r.topic(), r.partition(), r.raw().offset(), r.raw().key(), r.raw().value(), batch));
        }
        return l;
    }

    private static ObjectNode deadLetterLine(int partition, Raw r) {
        ObjectNode o = Json.object();
        Json.putSafe(o, "partition", partition);
        Json.putSafe(o, "offset", r.offset());
        Json.putSafe(o, "timestamp", r.timestamp());
        o.put("timestampType", r.timestampType());
        if (r.key() == null) o.putNull("key"); else o.put("key", r.key());
        ObjectNode h = o.putObject("headers");
        if (r.headers() != null) new TreeMap<>(r.headers()).forEach((k, v) -> h.put(k, new String(v, StandardCharsets.UTF_8)));
        o.put("valueBase64", r.value() == null ? null : Base64.getEncoder().encodeToString(r.value()));
        return o;
    }
}
