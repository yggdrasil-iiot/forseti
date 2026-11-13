package yggdrasil.forseti.cli.recording;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.stage1.Events;

/** The event-time replay of a recording (Decision 11; spec "Event-time replay", "Replay order", "Replay batches", "Replay watermarks
 *  and deadlines"): the retained input of {@code original} materialized as a new recording that a run reads like any recording.
 *
 *  <p>Order, per partition only: a record's ordering key is a process message's smallest valid member {@code sourceTs} (a member that
 *  {@link InputParser} keeps and that carries one), a session event's session event time ({@link Events#eventTime}), an occupancy
 *  event's {@code eventTime}; an untimed record takes the key of the nearest earlier keyed record of its partition, the untimed head stays
 *  first; records sort by key, then original offset. A partition without any key is exempt (order, offsets and through-offsets are the
 *  original's). The ordering key is not {@link Events#eventTime}, which keeps returning a message's largest member {@code sourceTs}: that
 *  one alone advances watermarks and orders the inputs of a cut.
 *
 *  <p>Batches, under the original's cut ids: the target {@code T_k(p)} of an {@code ot.messages} partition is the largest valid member
 *  {@code sourceTs} the original batches 1 to k include on p, {@code C_k} the largest of them; batch k includes the longest prefix of
 *  the replay order whose every record qualifies (a head record when the original batches 1 to k include it, any other when its key is at
 *  or below {@code T_k(p)}, or {@code C_k} on a session or occupancy partition); the last batch includes everything.
 *
 *  <p>Watermarks and deadlines: {@code W1_k(s)} and {@code W1'_k(s)} by the harness formula over the original and the replay batches
 *  (messages routed by {@code routing.json} and the manifest's definitions, as the harness routes them); a deadline keeps its cut ids only
 *  where {@code W1'_A(s) < at <= W1'_B(s)} holds for every station it brackets in the original, is replanned to the consecutive pair that
 *  satisfies it otherwise, and the materialization is refused when none exists. Nothing is written before every check has passed. */
public final class ReplayRecording {
    private ReplayRecording() {}

    public static final String GENERATOR = "forseti recordings replay 0.1.0", OFFSET_MAP = "offset-map";
    /** The no-telemetry watermark of the harness: a station that has received no telemetry reports {@code -(2^53 - 1)}. */
    public static final long NO_TELEMETRY = -Json.MAX_SAFE;

    /** A refusal of the materialization: {@code replay-input-unconsumed}, {@code replay-idle-inputs}, {@code replay-deadline-unproved},
     *  {@code replay-deadline-unbracketed}; nothing was written. */
    public static final class Refused extends RuntimeException {
        public final String code;
        public final ObjectNode evidence;
        public Refused(String code, ObjectNode evidence) { super(code + " " + Canonical.utf8(Canonical.bytes(evidence))); this.code = code; this.evidence = evidence; }
        public ObjectNode toJson() { ObjectNode n = Json.object().put("refusal", code); n.setAll(evidence.deepCopy()); return n; }
    }

    /** A materialized replay: the directory, its manifest (and raw digest), the lock, the cut ids, and per station the watermark at every
     *  cut over the original ({@code w1}) and over the replay batches ({@code w1Replay}), in batch order. */
    public record Result(Path out, JsonNode manifest, String manifestSha256, ObjectNode lock, List<Long> cutIds,
                         Map<String, List<Long>> w1, Map<String, List<Long>> w1Replay) {}

    /** {@code <scenario dir>/<name>.replay.lock.json} beside {@code <name>.json}. */
    public static Path lockOf(Path scenario) {
        String n = scenario.getFileName().toString();
        if (!n.endsWith(".json") || n.endsWith(".lock.json")) throw new IllegalArgumentException("not a scenario file: " + n);
        return scenario.resolveSibling(n.substring(0, n.length() - ".json".length()) + ".replay.lock.json");
    }

    /** One record of a partition: its original offset, its line, its ordering key (null: untimed), and for a process message its largest
     *  valid member {@code sourceTs} and the station the harness routes it to (null when it routes it to none). */
    private record Rec(long offset, byte[] line, Long key, Long maxTs, String station) {}

    private static final class Part {
        final String topic; final int partition;
        final List<Rec> records = new ArrayList<>();
        /** The replay order: indexes into {@link #records}. */
        int[] order;
        int head;
        boolean exempt;
        Part(String topic, int partition) { this.topic = topic; this.partition = partition; }
        String name() { return topic + "/" + partition; }
    }

    public static Result materialize(Path original, long outOfOrdernessMs, Path out) {
        if (outOfOrdernessMs < 0 || outOfOrdernessMs > Json.MAX_SAFE) throw new IllegalArgumentException("outOfOrdernessMs out of range: " + outOfOrdernessMs);
        try {
            if (Files.exists(out) && Files.exists(original) && Files.isSameFile(out, original)) throw new IllegalArgumentException("the replay recording cannot replace its original " + original);
        } catch (IOException e) { throw new UncheckedIOException(e); }
        Recording rec = Recording.open(original);
        rec.verifyDigests();
        RoutingManifest routing = rec.routing();
        List<Recording.Batch> batches = rec.batches();
        Recording.Batch last = batches.get(batches.size() - 1);

        // (1) the retained input is every record; no idle inputs
        for (String topic : RoutingManifest.TOPICS) for (int p : routing.subscribed(topic)) {
            long size = rec.size(topic, p), through = last.through(topic, p);
            if (through < size - 1) {
                ObjectNode ev = Json.object().put("topic", topic);
                Json.putSafe(ev, "partition", p);
                Json.putSafe(ev, "records", size);
                Json.putSafe(Json.putSafe(ev, "lastCutId", last.cutId()), "lastThroughOffset", through);
                throw new Refused("replay-input-unconsumed", ev);
            }
        }
        for (Recording.Batch b : batches)
            if (!b.idleInputs().isEmpty()) {
                ObjectNode ev = Json.putSafe(Json.object(), "cutId", b.cutId());
                ev.set("idleInputs", b.idleInputs().deepCopy());
                throw new Refused("replay-idle-inputs", ev);
            }
        rec.verifyBatches();

        // (2) ordering keys and the replay order of every partition
        JsonNode definitions = rec.manifest().path("definitions");
        Map<String, Part> parts = new LinkedHashMap<>();
        for (String topic : RoutingManifest.TOPICS) for (int p : routing.subscribed(topic)) {
            Part part = new Part(topic, p);
            try (Recording.Reader r = rec.reader(topic, p)) {
                for (Recording.Line l; (l = r.next()) != null;) part.records.add(record(topic, p, l, routing, definitions));
            }
            order(part);
            parts.put(part.name(), part);
        }

        // (3) cut targets from telemetry, and the replay through-offsets
        int n = batches.size();
        List<Long> cutIds = new ArrayList<>();
        for (Recording.Batch b : batches) cutIds.add(b.cutId());
        Map<Integer, Long[]> target = new TreeMap<>();
        Long[] cutTarget = new Long[n];
        for (int p : routing.subscribed(InputParser.MESSAGES)) {
            Part part = parts.get(InputParser.MESSAGES + "/" + p);
            Long[] t = new Long[n];
            Long max = null;
            int i = 0;
            for (int k = 0; k < n; k++) {
                long through = batches.get(k).through(InputParser.MESSAGES, p);
                for (; i < part.records.size() && part.records.get(i).offset() <= through; i++) {
                    Long ts = part.records.get(i).maxTs();
                    if (ts != null && (max == null || ts > max)) max = ts;
                }
                t[k] = max;
                if (max != null && (cutTarget[k] == null || max > cutTarget[k])) cutTarget[k] = max;
            }
            target.put(p, t);
        }
        Map<String, long[]> replayThrough = new LinkedHashMap<>();
        for (Part part : parts.values()) {
            long[] through = new long[n];
            if (part.exempt) { for (int k = 0; k < n; k++) through[k] = batches.get(k).through(part.topic, part.partition); replayThrough.put(part.name(), through); continue; }
            int i = 0;
            for (int k = 0; k < n; k++) {
                if (k == n - 1) i = part.records.size();
                long originalThrough = batches.get(k).through(part.topic, part.partition);
                Long tk = part.topic.equals(InputParser.MESSAGES) ? target.get(part.partition)[k] : cutTarget[k];
                while (i < part.records.size()) {
                    Rec r = part.records.get(part.order[i]);
                    boolean qualifies = i < part.head ? r.offset() <= originalThrough : tk != null && r.key() <= tk;
                    if (!qualifies) break;
                    i++;
                }
                through[k] = i - 1L;
            }
            replayThrough.put(part.name(), through);
        }

        // (4) the watermark of every station at every cut, over the original and over the replay batches
        List<String> stations = new ArrayList<>(routing.stations().keySet());
        Map<String, List<Long>> w1 = watermarks(parts, batches, stations, outOfOrdernessMs, false, replayThrough);
        Map<String, List<Long>> w1r = watermarks(parts, batches, stations, outOfOrdernessMs, true, replayThrough);

        // (5) deadlines: proved on the replay progression, replanned, or refused
        Map<Long, Integer> index = new HashMap<>();
        for (int k = 0; k < n; k++) index.put(cutIds.get(k), k);
        ArrayNode deadlines = Json.JSON.createArrayNode(), proofs = Json.JSON.createArrayNode(), transitions = Json.JSON.createArrayNode();
        for (Recording.Deadline d : rec.deadlines()) {
            int a = index.get(d.cutA()), b = index.get(d.cutB());
            List<String> bracketed = new ArrayList<>();
            for (String s : stations) if (w1.get(s).get(a) < d.at() && d.at() <= w1.get(s).get(b)) bracketed.add(s);
            if (bracketed.isEmpty()) {
                ObjectNode ev = deadlineJson(d);
                ObjectNode w = ev.putObject("w1");
                for (String s : stations) { ObjectNode x = w.putObject(s); Json.putSafe(x, "cutA", w1.get(s).get(a)); Json.putSafe(x, "cutB", w1.get(s).get(b)); }
                throw new Refused("replay-deadline-unproved", ev);
            }
            long cutA = d.cutA(), cutB = d.cutB();
            if (!holds(w1r, bracketed, d.at(), a, b)) {
                Long found = null;
                for (int k = 0; k + 1 < n; k++)
                    if (cutIds.get(k + 1) == cutIds.get(k) + 1 && holds(w1r, bracketed, d.at(), k, k + 1)) { found = cutIds.get(k); break; }
                if (found == null) {
                    ObjectNode ev = deadlineJson(d);
                    ArrayNode st = ev.putArray("stations");
                    bracketed.forEach(st::add);
                    ArrayNode ids = ev.putArray("cutIds");
                    cutIds.forEach(ids::add);
                    ObjectNode w = ev.putObject("w1Replay");
                    for (String s : bracketed) { ArrayNode x = w.putArray(s); w1r.get(s).forEach(x::add); }
                    throw new Refused("replay-deadline-unbracketed", ev);
                }
                cutA = found;
                cutB = found + 1;
            }
            int ra = index.get(cutA), rb = index.get(cutB);
            ObjectNode dl = Json.object().put("kind", d.kind()).put("ref", d.ref());
            Json.putSafe(dl, "at", d.at());
            Json.putSafe(Json.putSafe(dl, "cutA", cutA), "cutB", cutB);
            deadlines.add(dl);
            ObjectNode proof = Json.object().put("kind", d.kind()).put("ref", d.ref());
            Json.putSafe(proof, "at", d.at());
            ArrayNode st = proof.putArray("stations");
            bracketed.forEach(st::add);
            Json.putSafe(Json.putSafe(proof, "originalCutA", d.cutA()), "originalCutB", d.cutB());
            Json.putSafe(Json.putSafe(proof, "cutA", cutA), "cutB", cutB);
            ObjectNode wa = proof.putObject("w1A"), wb = proof.putObject("w1B");
            for (String s : bracketed) {
                Json.putSafe(wa, s, w1r.get(s).get(ra));
                Json.putSafe(wb, s, w1r.get(s).get(rb));
                ObjectNode t = Json.object().put("assertion", d.kind()).put("station", s).put("ref", d.ref());
                Json.putSafe(t, "at", d.at());
                Json.putSafe(Json.putSafe(t, "cutA", cutA), "cutB", cutB);
                Json.putSafe(Json.putSafe(t, "w1A", w1r.get(s).get(ra)), "w1B", w1r.get(s).get(rb));
                transitions.add(t);
            }
            proofs.add(proof);
        }

        // (6) the files: partitions, offset maps, routing and expected, then the manifest
        byte[] originalManifest;
        byte[] routingBytes;
        try {
            originalManifest = Files.readAllBytes(original.resolve(Recording.MANIFEST));
            routingBytes = Files.readAllBytes(original.resolve(Recording.ROUTING));
        } catch (IOException e) { throw new UncheckedIOException(e); }
        ObjectNode expected = expected(rec.expected(), transitions);
        byte[] expectedBytes = Json.pretty(expected);
        ScenarioBuilder.prepare(out);
        TreeMap<String, String> digests = new TreeMap<>();
        for (Part part : parts.values()) {
            digests.put(part.name() + ".jsonl", writePartition(out, part));
            digests.put(OFFSET_MAP + "/" + part.name() + ".jsonl", writeOffsetMap(out, part));
        }
        digests.put(Recording.ROUTING, write(out.resolve(Recording.ROUTING), routingBytes));
        digests.put(Recording.EXPECTED, write(out.resolve(Recording.EXPECTED), expectedBytes));

        ObjectNode manifest = (ObjectNode) rec.manifest().deepCopy();
        ArrayNode bs = Json.JSON.createArrayNode();
        for (int k = 0; k < n; k++) {
            ObjectNode b = Json.putSafe(Json.object(), "cutId", cutIds.get(k));
            ObjectNode to = b.putObject("throughOffsets");
            for (Iterator<String> it = rec.manifest().path("batches").get(k).path("throughOffsets").fieldNames(); it.hasNext();) {
                String topic = it.next();
                ObjectNode t = to.putObject(topic);
                for (int p : routing.subscribed(topic)) Json.putSafe(t, Integer.toString(p), replayThrough.get(topic + "/" + p)[k]);
            }
            b.putArray("idleInputs");
            bs.add(b);
        }
        manifest.set("batches", bs);
        manifest.set("deadlines", deadlines);
        ObjectNode source = manifest.path("source").isObject() ? (ObjectNode) manifest.get("source") : manifest.putObject("source");
        source.put("replayOf", Canonical.digestOf(originalManifest));
        Json.putSafe(source, "outOfOrdernessMs", outOfOrdernessMs);
        source.set("deadlines", proofs);
        ObjectNode ds = manifest.putObject("digests");
        digests.forEach(ds::put);
        byte[] manifestBytes = Json.pretty(manifest);
        String manifestSha = write(out.resolve(Recording.MANIFEST), manifestBytes);

        ObjectNode lock = Json.object();
        Json.putSafe(lock, "lockVersion", 1);
        lock.put("scenario", source.path("scenario").asText(rec.manifest().path("name").asText()));
        lock.put("scenarioSha256", source.path("scenarioSha256").asText(null));
        lock.put("generator", GENERATOR);
        Json.putSafe(lock, "outOfOrdernessMs", outOfOrdernessMs);
        Json.putSafe(lock.putObject("counts"), "cuts", n);
        ObjectNode files = lock.putObject("files");
        TreeMap<String, String> all = new TreeMap<>(digests);
        all.put(Recording.MANIFEST, manifestSha);
        all.forEach(files::put);
        return new Result(out, manifest, manifestSha, lock, List.copyOf(cutIds), w1, w1r);
    }

    private static boolean holds(Map<String, List<Long>> w, List<String> stations, long at, int a, int b) {
        for (String s : stations) if (!(w.get(s).get(a) < at && at <= w.get(s).get(b))) return false;
        return true;
    }

    private static ObjectNode deadlineJson(Recording.Deadline d) {
        ObjectNode n = Json.object();
        ObjectNode x = n.putObject("deadline").put("kind", d.kind()).put("ref", d.ref());
        Json.putSafe(x, "at", d.at());
        Json.putSafe(Json.putSafe(x, "cutA", d.cutA()), "cutB", d.cutB());
        return n;
    }

    /** The ordering key, the largest valid member {@code sourceTs} and the routed station of one record. A process message is read by
     *  {@link InputParser} unchanged: a member it keeps and that carries a {@code sourceTs} is valid; a message it rejects is untimed. */
    private static Rec record(String topic, int partition, Recording.Line l, RoutingManifest routing, JsonNode definitions) {
        long offset = l.record().offset();
        if (topic.equals(InputParser.INCIDENTS)) return new Rec(offset, l.bytes(), null, null, null);
        InputParser.Read read = InputParser.read(new Inputs.Origin(topic, partition, offset), l.record().key(), Recording.valueBytes(l.bytes()));
        Object parsed = read.rejected() ? null : read.record().parsed();
        if (parsed instanceof Inputs.ProcessMessage m) {
            Long min = null, max = null;
            for (Inputs.Member x : m.members()) {
                if (x.sourceTs() == null) continue;
                if (min == null || x.sourceTs() < min) min = x.sourceTs();
                if (max == null || x.sourceTs() > max) max = x.sourceTs();
            }
            // the harness routes a message of a declared device whose definition digest is the manifest's, and only one with a sourceTs
            String station = null;
            JsonNode def = definitions.path(m.device()).path("sha256");
            if (max != null && routing.devices().contains(m.device()) && def.isTextual() && def.textValue().equals(m.definitionSha256())) station = routing.stationOf(m.device());
            return new Rec(offset, l.bytes(), min, max, station);
        }
        if (parsed instanceof Inputs.SessionEvent || parsed instanceof Inputs.OccupancyEvent) return new Rec(offset, l.bytes(), Events.eventTime(read.record()), null, null);
        return new Rec(offset, l.bytes(), null, null, null);
    }

    /** Untimed records take the key of the nearest earlier keyed record; the head stays first; then by key and original offset. */
    private static void order(Part part) {
        List<Rec> rs = part.records;
        int head = 0;
        while (head < rs.size() && rs.get(head).key() == null) head++;
        part.head = head;
        part.exempt = head == rs.size();
        long[] key = new long[rs.size()];
        Long last = null;
        for (int i = 0; i < rs.size(); i++) { if (rs.get(i).key() != null) last = rs.get(i).key(); key[i] = last == null ? Long.MIN_VALUE : last; }
        Integer[] idx = new Integer[rs.size()];
        for (int i = 0; i < idx.length; i++) idx[i] = i;
        if (!part.exempt) Arrays.sort(idx, head, idx.length, Comparator.<Integer>comparingLong(i -> key[i]).thenComparingLong(i -> rs.get(i).offset()));
        part.order = new int[idx.length];
        for (int i = 0; i < idx.length; i++) part.order[i] = idx[i];
        // an untimed record inherits a key, so give the sort's view back to the record for qualification: replace the stored key
        for (int i = head; i < rs.size(); i++) if (rs.get(i).key() == null) { Rec r = rs.get(i); rs.set(i, new Rec(r.offset(), r.line(), key[i], r.maxTs(), r.station())); }
    }

    /** {@code W1_k(s)} over the original batches ({@code replay} false: records at original offsets up to the original through-offset) or
     *  {@code W1'_k(s)} over the replay batches (replay positions up to the replay through-offset): the largest valid member
     *  {@code sourceTs} routed to s, minus {@code outOfOrdernessMs}, minus 1, or {@link #NO_TELEMETRY} while none has been routed. */
    private static Map<String, List<Long>> watermarks(Map<String, Part> parts, List<Recording.Batch> batches, List<String> stations, long oo,
                                                      boolean replay, Map<String, long[]> replayThrough) {
        Map<String, Long> max = new HashMap<>();
        Map<String, List<Long>> w = new LinkedHashMap<>();
        for (String s : stations) w.put(s, new ArrayList<>());
        Map<String, Integer> at = new HashMap<>();
        for (int k = 0; k < batches.size(); k++) {
            for (Part part : parts.values()) {
                if (!part.topic.equals(InputParser.MESSAGES)) continue;
                long through = replay ? replayThrough.get(part.name())[k] : batches.get(k).through(part.topic, part.partition);
                int i = at.getOrDefault(part.name(), 0);
                for (; i <= through; i++) {
                    Rec r = part.records.get(replay ? part.order[i] : i);
                    if (r.station() != null) max.merge(r.station(), r.maxTs(), Math::max);
                }
                at.put(part.name(), i);
            }
            for (String s : stations) { Long m = max.get(s); w.get(s).add(m == null ? NO_TELEMETRY : m - oo - 1); }
        }
        Map<String, List<Long>> out = new LinkedHashMap<>();
        w.forEach((s, l) -> out.put(s, List.copyOf(l)));
        return out;
    }

    /** {@code {"expectedVersion": 1, "scope": "replay", "mode", "visits", "transitions"}}: per visit only the immutable facts of the input
     *  ({@code visitRef, key, unitId, station, enter, leave, attributionEnd}, those the original carries); no online outcome. */
    static ObjectNode expected(JsonNode original, ArrayNode transitions) {
        ObjectNode e = Json.object();
        Json.putSafe(e, "expectedVersion", 1);
        e.put("scope", "replay");
        if (original.path("mode").isTextual()) e.put("mode", original.get("mode").textValue()); else e.putNull("mode");
        ArrayNode visits = e.putArray("visits");
        for (JsonNode v : original.path("visits")) {
            ObjectNode x = visits.addObject();
            for (String f : List.of("visitRef", "key", "unitId", "station", "enter", "leave", "attributionEnd")) if (v.has(f)) x.set(f, v.get(f).deepCopy());
        }
        e.set("transitions", transitions);
        return e;
    }

    // --- writing ---------------------------------------------------------------------------------------------------------------

    private static final byte[] OFFSET_PREFIX = "{\"offset\":".getBytes(StandardCharsets.US_ASCII);

    /** The records in replay order, each line with its replay offset and its {@code timestamp}, {@code key}, {@code headers} and
     *  {@code value} bytes unchanged. */
    private static String writePartition(Path out, Part part) {
        Path f = out.resolve(part.topic).resolve(part.partition + ".jsonl");
        return stream(f, o -> {
            for (int i = 0; i < part.order.length; i++) {
                Rec r = part.records.get(part.order[i]);
                byte[] digits = Long.toString(r.offset()).getBytes(StandardCharsets.US_ASCII);
                byte[] line = r.line();
                int rest = OFFSET_PREFIX.length + digits.length;
                if (line.length <= rest || !Arrays.equals(line, 0, OFFSET_PREFIX.length, OFFSET_PREFIX, 0, OFFSET_PREFIX.length)
                    || !Arrays.equals(line, OFFSET_PREFIX.length, rest, digits, 0, digits.length) || line[rest] != ',')
                    throw new IllegalArgumentException("malformed recording: " + part.name() + " line " + r.offset() + " does not begin with {\"offset\":" + r.offset() + ",");
                o.write(OFFSET_PREFIX);
                o.write(Long.toString(i).getBytes(StandardCharsets.US_ASCII));
                o.write(line, rest, line.length - rest);
                o.write('\n');
            }
        });
    }

    /** {@code offset-map/<topic>/<partition>.jsonl}: line i is {@code {"offset": i, "originalOffset": o}}. */
    private static String writeOffsetMap(Path out, Part part) {
        Path f = out.resolve(OFFSET_MAP).resolve(part.topic).resolve(part.partition + ".jsonl");
        return stream(f, o -> {
            for (int i = 0; i < part.order.length; i++)
                o.write(("{\"offset\":" + i + ",\"originalOffset\":" + part.records.get(part.order[i]).offset() + "}\n").getBytes(StandardCharsets.US_ASCII));
        });
    }

    private interface Body { void write(OutputStream o) throws IOException; }

    private static String stream(Path f, Body body) {
        try {
            Files.createDirectories(f.getParent());
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (OutputStream o = new DigestOutputStream(new BufferedOutputStream(Files.newOutputStream(f), 1 << 16), md)) { body.write(o); }
            return HexFormat.of().formatHex(md.digest());
        } catch (IOException e) { throw new UncheckedIOException(e); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    private static String write(Path f, byte[] bytes) {
        try { Files.createDirectories(f.getParent()); Files.write(f, bytes); } catch (IOException e) { throw new UncheckedIOException(e); }
        return Canonical.digestOf(bytes);
    }

    // --- checking ----------------------------------------------------------------------------------------------------------------

    /** The differences between a materialized replay's lock and a committed one (empty when they are byte-identical). */
    public static List<String> compareLock(ObjectNode lock, Path committed) {
        List<String> problems = new ArrayList<>();
        if (!Files.isRegularFile(committed)) { problems.add("no replay lock " + committed.getFileName()); return problems; }
        byte[] have;
        try { have = Files.readAllBytes(committed); } catch (IOException e) { throw new UncheckedIOException(e); }
        if (Arrays.equals(have, Json.pretty(lock))) return problems;
        JsonNode c = Json.read(have);
        for (String f : List.of("scenario", "scenarioSha256", "generator", "outOfOrdernessMs", "counts"))
            if (!text(c.get(f)).equals(text(lock.get(f)))) problems.add(f + ": committed " + c.get(f) + ", materialized " + lock.get(f));
        TreeSet<String> names = new TreeSet<>();
        c.path("files").fieldNames().forEachRemaining(names::add);
        lock.path("files").fieldNames().forEachRemaining(names::add);
        for (String f : names) if (!text(c.path("files").get(f)).equals(text(lock.path("files").get(f)))) problems.add("file " + f + ": committed " + c.path("files").get(f) + ", materialized " + lock.path("files").get(f));
        if (problems.isEmpty()) problems.add("the lock's bytes differ from " + committed.getFileName());
        return problems;
    }

    /** Canonical text, so that a value read from a file and the same value built in memory compare equal; "absent" for null. */
    private static String text(JsonNode n) {
        if (n == null) return "absent";
        ObjectNode wrapped = Json.object();
        wrapped.set("v", n);
        return Canonical.utf8(Canonical.bytes(wrapped));
    }

    /** The gate's check after a run on a replay recording, on the {@code W1} the run reported in {@code cuts.json}: for every deadline and
     *  station of {@code source.deadlines}, {@code W1_cutA(s) < at <= W1_cutB(s)}, and {@code W1} equal to the manifest's {@code w1A},
     *  {@code w1B}. Returns the problems (empty: every bracket holds). */
    public static List<String> checkRun(JsonNode replayManifest, JsonNode cutLog) {
        List<String> problems = new ArrayList<>();
        Map<Long, JsonNode> cuts = new HashMap<>();
        for (JsonNode c : cutLog.path("cuts")) cuts.put(c.path("cutId").asLong(), c.path("perStation"));
        JsonNode proofs = replayManifest.path("source").path("deadlines");
        if (!proofs.isArray()) { problems.add("the manifest has no source.deadlines: not a replay recording"); return problems; }
        for (JsonNode d : proofs) {
            long at = d.path("at").asLong(), a = d.path("cutA").asLong(), b = d.path("cutB").asLong();
            String label = d.path("kind").asText() + " " + d.path("ref").asText() + " at " + at;
            if (!cuts.containsKey(a) || !cuts.containsKey(b)) { problems.add(label + ": the run reported no cut " + (cuts.containsKey(a) ? b : a)); continue; }
            for (JsonNode s : d.path("stations")) {
                String st = s.asText();
                JsonNode wa = cuts.get(a).path(st).path("W1"), wb = cuts.get(b).path(st).path("W1");
                if (!wa.isIntegralNumber() || !wb.isIntegralNumber()) { problems.add(label + " " + st + ": no W1 at cut " + a + " or " + b); continue; }
                if (!(wa.longValue() < at && at <= wb.longValue())) problems.add(label + " " + st + ": W1 " + wa + " at cut " + a + ", " + wb + " at cut " + b + " do not bracket it");
                JsonNode ma = d.path("w1A").path(st), mb = d.path("w1B").path(st);
                if (!ma.isIntegralNumber() || !mb.isIntegralNumber() || ma.longValue() != wa.longValue() || mb.longValue() != wb.longValue())
                    problems.add(label + " " + st + ": W1 " + wa + ", " + wb + " differ from the manifest's w1A " + d.path("w1A").path(st) + ", w1B " + d.path("w1B").path(st));
            }
        }
        return problems;
    }
}
