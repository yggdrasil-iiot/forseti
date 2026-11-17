package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.Ids;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Disposition;

/** The identity-bound reconciliation of a run's ledgers with its recording, checked at every completed cut (step-6 decision F5). The
 *  oracle comparison reconciles dispositions by identity and never looks at an input offset; this binds every disposition to the one
 *  recorded input its origin names, so that offsets an engine (and the harness's broker-to-recording translation) wrote are checked
 *  too. At cut {@code k}, for every disposition the two ledgers ({@code forseti.dispositions-stage1} for {@code ot.messages},
 *  {@code ot.sessions} and {@code mfg.occupancy}, {@code forseti.dispositions-stage2} for {@code quality.incidents}, station partitions
 *  and the reject collectors' partition 6 alike) wrote before their progress of {@code k}:
 *  <ul>
 *  <li>revisions are folded per delivery key, contiguous from 1, a key in one ledger partition only, of the run's generation;</li>
 *  <li>its origin names a recorded input of a topic its ledger owns, with the same {@code payloadDigest} and the same identity for its
 *      part (the identity the core gives that part of that input; null for an input that does not parse);</li>
 *  <li>its first revision lies inside the batch range of {@code k} or of a cut {@code k}'s progress merges;</li>
 *  </ul>
 *  every row's {@code envelope.trigger} written before that progress names a recorded input; and once every ledger writer has closed
 *  the cut of a recorded input, that input carries its required parts exactly once (an occupancy event its {@code event} or
 *  {@code errorDeclaration}, a session or an incident {@code event}, a process message {@code message} or a {@code member:<metric>}
 *  for every member it carries). {@link #finish} then requires every recorded input reconciled. */
public final class Reconciliation {
    static final String LEDGER1 = "forseti.dispositions-stage1", LEDGER2 = "forseti.dispositions-stage2";
    static final List<String> ROW_TOPICS = List.of("forseti.work-records", "forseti.samples", "forseti.affected", "forseti.affected-sets");
    static final Set<String> STAGE1_INPUTS = Set.of(InputParser.MESSAGES, InputParser.SESSIONS, InputParser.OCCUPANCY);

    /** One recorded input: its batch, its payload digest, the identity of each of its parts, and its required part sets. */
    private record Input(String topic, int partition, long offset, long cut, String payloadDigest, Map<String, JsonNode> identities, List<Set<String>> required) {
        String label() { return topic + "/" + partition + "/" + offset; }
    }

    /** The lines of one journal partition between two progress records, closed by the progress of {@code cutId}. */
    private record Segment(long cutId, List<Long> merged, List<JsonNode> lines) {}

    private final long generation;
    private final Map<String, Input> inputs = new HashMap<>();
    private final Map<String, ArrayDeque<Segment>> ledgers = new TreeMap<>(), rows = new TreeMap<>();
    private final Map<String, Long> lastProgress = new TreeMap<>();
    private final Map<String, Long> revisions = new HashMap<>();
    private final Map<String, String> keyPartition = new HashMap<>();
    private final Map<String, Set<String>> parts = new HashMap<>();
    private final Set<String> reconciled = new HashSet<>();
    private final List<Recording.Batch> batches;

    private Reconciliation(long generation, List<Recording.Batch> batches) { this.generation = generation; this.batches = batches; }

    /** The reconciliation of {@code run} ({@code journals/}, {@code run.json} with its {@code subscribed}, the run's generation from
     *  {@code cuts.json}) against the recording it ran on. */
    public static Reconciliation open(Path recording, Path run) {
        Recording rec = Recording.open(recording);
        JsonNode runJson = Json.read(run.resolve("run.json")), cuts = Json.read(run.resolve("cuts.json"));
        Reconciliation r = new Reconciliation(Json.safeLong(cuts.get("generation"), "generation", 1, Json.MAX_SAFE), rec.batches());
        runJson.path("subscribed").fields().forEachRemaining(e -> {
            for (JsonNode p : e.getValue()) r.load(rec, e.getKey(), p.intValue());
        });
        for (String t : List.of(LEDGER1, LEDGER2)) r.segments(run, t, r.ledgers);
        for (String t : ROW_TOPICS) r.segments(run, t, r.rows);
        return r;
    }

    private void load(Recording rec, String topic, int partition) {
        try (Recording.Reader reader = rec.reader(topic, partition)) {
            Iterator<Recording.Batch> bs = batches.iterator();
            Recording.Batch b = bs.hasNext() ? bs.next() : null;
            for (Recording.Line l; (l = reader.next()) != null;) {
                long off = l.record().offset();
                while (b != null && off > b.through(topic, partition)) b = bs.hasNext() ? bs.next() : null;
                long cut = b == null ? Long.MAX_VALUE : b.cutId();
                byte[] raw = Recording.valueBytes(l.bytes());
                InputParser.Read read = InputParser.read(new Inputs.Origin(topic, partition, off), l.record().key(), raw);
                JsonNode value;
                try { value = Json.read(raw); } catch (IllegalArgumentException e) { value = null; }
                Map<String, JsonNode> ids = new HashMap<>();
                List<Set<String>> required = new ArrayList<>();
                Object parsed = read.rejected() ? null : read.record().parsed();
                switch (topic) {
                    case InputParser.MESSAGES -> {
                        if (parsed instanceof Inputs.ProcessMessage m) {
                            ids.put(Disposition.Part.MESSAGE, Ids.messageId(m));
                            Set<String> members = new TreeSet<>();
                            for (Inputs.Member x : m.members()) { members.add(Disposition.Part.member(x.metric())); ids.put(Disposition.Part.member(x.metric()), Ids.sampleId(m, x.metric())); }
                            for (Inputs.MemberRejection x : m.rejectedMembers()) { members.add(Disposition.Part.member(x.metric())); ids.put(Disposition.Part.member(x.metric()), Ids.sampleId(m, x.metric())); }
                            required.add(Set.of(Disposition.Part.MESSAGE));
                            if (!members.isEmpty()) required.add(members);
                        } else { ids.put(Disposition.Part.MESSAGE, NullNode.getInstance()); required.add(Set.of(Disposition.Part.MESSAGE)); }
                    }
                    case InputParser.SESSIONS -> {
                        ids.put(Disposition.Part.EVENT, parsed instanceof Inputs.SessionEvent s ? Ids.sessionEventId(s) : NullNode.getInstance());
                        required.add(Set.of(Disposition.Part.EVENT));
                    }
                    case InputParser.OCCUPANCY -> {
                        String part = value != null && value.isObject() && value.has("errorDeclaration") ? Disposition.Part.ERROR_DECLARATION : Disposition.Part.EVENT;
                        ids.put(part, parsed instanceof Inputs.OccupancyEvent o ? Ids.occupancyId(o) : NullNode.getInstance());
                        required.add(Set.of(part));
                    }
                    case InputParser.INCIDENTS -> {
                        ids.put(Disposition.Part.EVENT, parsed instanceof Inputs.IncidentEvent i ? Ids.incidentId(i) : NullNode.getInstance());
                        required.add(Set.of(Disposition.Part.EVENT));
                    }
                    default -> throw new IllegalArgumentException("not an input topic: " + topic);
                }
                Input in = new Input(topic, partition, off, cut, Ids.payloadDigest(value, raw).sha256(), ids, required);
                inputs.put(in.label(), in);
            }
        }
    }

    private void segments(Path run, String topic, Map<String, ArrayDeque<Segment>> into) {
        Path dir = run.resolve("journals").resolve(topic);
        if (!Files.isDirectory(dir)) return;
        try (Stream<Path> s = Files.list(dir)) {
            for (Path f : s.filter(p -> p.getFileName().toString().endsWith(".jsonl")).sorted().toList()) {
                ArrayDeque<Segment> q = new ArrayDeque<>();
                List<JsonNode> pending = new ArrayList<>();
                for (String text : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                    if (text.isEmpty()) continue;
                    JsonNode n = RunFold.readLine(text);
                    JsonNode v = n.path("value");
                    if ("progress".equals(v.path("type").textValue())) {
                        List<Long> merged = new ArrayList<>();
                        v.path("mergedCutIds").forEach(x -> merged.add(x.asLong()));
                        q.add(new Segment(v.path("cutId").asLong(), merged, pending));
                        pending = new ArrayList<>();
                    } else pending.add(n);
                }
                if (!pending.isEmpty()) q.add(new Segment(Long.MAX_VALUE, List.of(), pending));
                String name = f.getFileName().toString();
                into.put(topic + "/" + name.substring(0, name.length() - ".jsonl".length()), q);
            }
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    /** The problems of completed cut {@code cutId}; cuts are checked in increasing order. Empty when it reconciles. */
    public List<String> cut(long cutId) {
        List<String> p = new ArrayList<>();
        for (Map.Entry<String, ArrayDeque<Segment>> e : ledgers.entrySet()) {
            Segment s = take(e.getValue(), cutId);
            if (s == null) continue;
            lastProgress.put(e.getKey(), s.cutId());
            Set<Long> allowed = new HashSet<>(s.merged());
            allowed.add(s.cutId());
            for (JsonNode line : s.lines()) disposition(e.getKey(), line, allowed, p);
        }
        for (Map.Entry<String, ArrayDeque<Segment>> e : rows.entrySet()) {
            Segment s = take(e.getValue(), cutId);
            if (s == null) continue;
            for (JsonNode line : s.lines()) {
                JsonNode t = line.path("value").path("envelope").path("trigger");
                if (!t.isObject()) continue;
                String label = t.path("topic").asText() + "/" + t.path("partition").asInt() + "/" + t.path("offset").asLong();
                if (!inputs.containsKey(label)) p.add(e.getKey() + " cut " + cutId + ": a trigger names " + label + ", which is no recorded input");
            }
        }
        // coverage, once every ledger writer has closed the input's cut
        long closed = Long.MAX_VALUE;
        for (Map.Entry<String, ArrayDeque<Segment>> e : ledgers.entrySet()) closed = Math.min(closed, lastProgress.getOrDefault(e.getKey(), 0L));
        for (Input in : inputs.values()) {
            if (in.cut() > closed || reconciled.contains(in.label())) continue;
            reconciled.add(in.label());
            Set<String> have = parts.getOrDefault(in.label(), Set.of());
            if (!in.required().contains(have)) p.add(in.label() + " (cut " + in.cut() + ") has the parts " + have + ", not one of " + in.required());
        }
        Collections.sort(p);
        return p;
    }

    /** Every recorded input reconciled after the last cut. */
    public List<String> finish() {
        List<String> p = new ArrayList<>();
        for (Input in : inputs.values()) if (!reconciled.contains(in.label())) p.add(in.label() + " (cut " + in.cut() + ") was never reconciled");
        for (Map.Entry<String, ArrayDeque<Segment>> e : ledgers.entrySet()) if (!e.getValue().isEmpty()) p.add(e.getKey() + ": lines after its last checked progress");
        Collections.sort(p);
        return p;
    }

    /** The next segment of a partition when it is closed by {@code cutId}; a later one stays for its cut. */
    private static Segment take(ArrayDeque<Segment> q, long cutId) {
        Segment s = q.peekFirst();
        if (s == null || s.cutId() != cutId) return null;
        return q.removeFirst();
    }

    private void disposition(String partition, JsonNode line, Set<Long> allowed, List<String> p) {
        String ledger = partition.substring(0, partition.lastIndexOf('/'));
        JsonNode k = line.path("key"), body = line.path("value").path("body");
        String where = partition + " " + Canonical.utf8(Canonical.bytes(k));
        if (k.path("generation").asLong() != generation) { p.add(where + ": generation " + k.path("generation") + ", the run's is " + generation); return; }
        String topic = k.path("inputTopic").asText(), part = k.path("part").asText();
        if (ledger.equals(LEDGER1) ? !STAGE1_INPUTS.contains(topic) : !topic.equals(InputParser.INCIDENTS)) { p.add(where + ": " + ledger + " does not own " + topic); return; }
        String key = Canonical.utf8(Canonical.bytes(k));
        String other = keyPartition.putIfAbsent(key, partition);
        if (other != null && !other.equals(partition)) p.add(where + ": the key is also written in " + other);
        long rev = line.path("value").path("revision").asLong();
        long last = revisions.getOrDefault(key, 0L);
        if (rev != last + 1) p.add(where + ": revision " + rev + " after " + last);
        revisions.put(key, Math.max(last, rev));
        String label = topic + "/" + k.path("inputPartition").asInt() + "/" + k.path("inputOffset").asLong();
        Input in = inputs.get(label);
        if (in == null) { p.add(where + ": its origin " + label + " is no recorded input"); return; }
        if (rev == 1 && !allowed.contains(in.cut())) p.add(where + ": first written at cut(s) " + new TreeSet<>(allowed) + ", its input belongs to cut " + in.cut());
        if (!in.payloadDigest().equals(body.path("payloadDigest").asText())) p.add(where + ": payloadDigest " + body.path("payloadDigest").asText() + ", the recorded input's is " + in.payloadDigest());
        JsonNode want = in.identities().get(part);
        if (want == null) p.add(where + ": the recorded input has no part " + part);
        else if (body.get("identity") == null || !Canonical.utf8(Canonical.bytes(want)).equals(Canonical.utf8(Canonical.bytes(body.get("identity"))))) p.add(where + ": identity " + body.path("identity") + ", the recorded input's " + part + " is " + want);
        parts.computeIfAbsent(label, x -> new TreeSet<>()).add(part);
    }
}
