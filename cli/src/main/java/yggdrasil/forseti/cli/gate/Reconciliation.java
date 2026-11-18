package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import java.io.BufferedReader;
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
 *  for every member it carries). {@link #finish} then requires every recorded input reconciled.
 *
 *  <p>Memory (review of step 6 PR 5, item 11): nothing is read whole. The recorded inputs are read cut by cut, as far as the cut being
 *  checked; an input keeps its identities and its payload digest only until every ledger writer closed its cut, then a compact record
 *  (label, cut, payload-digest hash, its part names, their identity hashes, last revisions, ledger partitions and the parts seen); the
 *  journals are read line by line, one segment (the lines up to the next progress) per partition file at a time. A horizon recording
 *  (about 440,000 messages, seven parts each) therefore holds the compact records and one cut of journal lines. */
public final class Reconciliation implements AutoCloseable {
    static final String LEDGER1 = "forseti.dispositions-stage1", LEDGER2 = "forseti.dispositions-stage2";
    static final List<String> ROW_TOPICS = List.of("forseti.work-records", "forseti.samples", "forseti.affected", "forseti.affected-sets");
    static final Set<String> STAGE1_INPUTS = Set.of(InputParser.MESSAGES, InputParser.SESSIONS, InputParser.OCCUPANCY);

    /** One recorded input. Until it is reconciled: its identities per part, its payload digest and its required part sets; after: only
     *  hashes of them. {@code parts} sorted; per part the last revision, the ledger partition that wrote it (an index, -1 none) and
     *  whether it was seen. */
    private static final class Input {
        final String label;
        final long cut;
        final String[] parts;
        String payloadDigest;
        final int payloadHash;
        JsonNode[] identities;
        final int[] identityHash;
        final int[] lastRevision;
        final int[] ledgerPartition;
        long seen;
        List<Set<String>> required;

        Input(String label, long cut, String payloadDigest, Map<String, JsonNode> ids, List<Set<String>> required) {
            this.label = label;
            this.cut = cut;
            this.parts = new TreeSet<>(ids.keySet()).toArray(new String[0]);
            if (parts.length > 63) throw new IllegalStateException(label + ": more than 63 parts");
            this.payloadDigest = payloadDigest;
            this.payloadHash = payloadDigest.hashCode();
            this.identities = new JsonNode[parts.length];
            this.identityHash = new int[parts.length];
            for (int i = 0; i < parts.length; i++) { identities[i] = ids.get(parts[i]); identityHash[i] = canonical(identities[i]).hashCode(); }
            this.lastRevision = new int[parts.length];
            this.ledgerPartition = new int[parts.length];
            Arrays.fill(ledgerPartition, -1);
            this.required = required;
        }

        int part(String p) { return Arrays.binarySearch(parts, p); }

        Set<String> seenParts() {
            Set<String> s = new TreeSet<>();
            for (int i = 0; i < parts.length; i++) if ((seen & (1L << i)) != 0) s.add(parts[i]);
            return s;
        }

        /** Reconciled: the full values go, their hashes stay. */
        void compact() { identities = null; payloadDigest = null; required = null; }
    }

    /** The lines of one journal partition between two progress records, closed by the progress of {@code cutId}. */
    private record Segment(long cutId, List<Long> merged, List<JsonNode> lines) {}

    /** One journal partition file, read one segment at a time. */
    private static final class Cursor implements AutoCloseable {
        final BufferedReader in;
        Segment next;
        boolean done;
        Cursor(Path f) throws IOException { this.in = Files.newBufferedReader(f, StandardCharsets.UTF_8); }

        /** The segment after the last taken one, read when needed; null at the end of the file. */
        Segment peek() {
            if (next != null || done) return next;
            List<JsonNode> pending = new ArrayList<>();
            try {
                for (String text; (text = in.readLine()) != null;) {
                    if (text.isEmpty()) continue;
                    JsonNode n = RunFold.readLine(text);
                    JsonNode v = n.path("value");
                    if ("progress".equals(v.path("type").textValue())) {
                        List<Long> merged = new ArrayList<>();
                        v.path("mergedCutIds").forEach(x -> merged.add(x.asLong()));
                        return next = new Segment(v.path("cutId").asLong(), merged, pending);
                    }
                    pending.add(n);
                }
            } catch (IOException e) { throw new UncheckedIOException(e); }
            done = true;
            return next = pending.isEmpty() ? null : new Segment(Long.MAX_VALUE, List.of(), pending);
        }

        /** The next segment when it is closed by {@code cutId}; a later one stays for its cut. */
        Segment take(long cutId) {
            Segment s = peek();
            if (s == null || s.cutId() != cutId) return null;
            next = null;
            return s;
        }

        @Override public void close() { try { in.close(); } catch (IOException ignored) { /* read only */ } }
    }

    /** One input partition of the recording, read cut by cut. */
    private final class Feed implements AutoCloseable {
        final String topic;
        final int partition;
        final Recording.Reader reader;
        Recording.Line pending;
        Feed(Recording rec, String topic, int partition) { this.topic = topic; this.partition = partition; this.reader = rec.reader(topic, partition); }

        /** Every record up to the through-offset of batch {@code b} (all the rest when {@code b} is null), as inputs of {@code cut}. */
        void loadThrough(Recording.Batch b, long cut) {
            long through = b == null ? Long.MAX_VALUE : b.through(topic, partition);
            while (true) {
                Recording.Line l = pending != null ? pending : reader.next();
                pending = null;
                if (l == null) return;
                if (l.record().offset() > through) { pending = l; return; }
                add(topic, partition, l, cut);
            }
        }

        @Override public void close() { reader.close(); }
    }

    private final long generation;
    private final Map<String, Input> inputs = new HashMap<>();
    /** The inputs not reconciled yet (full values kept), in load order. */
    private final List<Input> open = new ArrayList<>();
    private final Map<String, Cursor> ledgers = new TreeMap<>(), rows = new TreeMap<>();
    private final Map<String, Long> lastProgress = new TreeMap<>();
    private final Map<String, Integer> partitionIndex = new HashMap<>();
    private final List<String> partitionNames = new ArrayList<>();
    private final List<Recording.Batch> batches;
    private final List<Feed> feeds = new ArrayList<>();
    private int loaded;   // batches whose inputs are loaded

    private Reconciliation(long generation, List<Recording.Batch> batches) { this.generation = generation; this.batches = batches; }

    /** The reconciliation of {@code run} ({@code journals/}, {@code run.json} with its {@code subscribed}, the run's generation from
     *  {@code cuts.json}) against the recording it ran on. */
    public static Reconciliation open(Path recording, Path run) {
        Recording rec = Recording.open(recording);
        JsonNode runJson = Json.read(run.resolve("run.json")), cuts = Json.read(run.resolve("cuts.json"));
        Reconciliation r = new Reconciliation(Json.safeLong(cuts.get("generation"), "generation", 1, Json.MAX_SAFE), rec.batches());
        runJson.path("subscribed").fields().forEachRemaining(e -> {
            for (JsonNode p : e.getValue()) r.feeds.add(r.new Feed(rec, e.getKey(), p.intValue()));
        });
        for (String t : List.of(LEDGER1, LEDGER2)) r.cursors(run, t, r.ledgers);
        for (String t : ROW_TOPICS) r.cursors(run, t, r.rows);
        return r;
    }

    private void cursors(Path run, String topic, Map<String, Cursor> into) {
        Path dir = run.resolve("journals").resolve(topic);
        if (!Files.isDirectory(dir)) return;
        try (Stream<Path> s = Files.list(dir)) {
            for (Path f : s.filter(p -> p.getFileName().toString().endsWith(".jsonl")).sorted().toList()) {
                String name = f.getFileName().toString();
                into.put(topic + "/" + name.substring(0, name.length() - ".jsonl".length()), new Cursor(f));
            }
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    /** The inputs of every batch up to cut {@code cutId} (all of them when {@code cutId} is null). */
    private void loadThrough(Long cutId) {
        while (loaded < batches.size() && (cutId == null || batches.get(loaded).cutId() <= cutId)) {
            Recording.Batch b = batches.get(loaded++);
            for (Feed f : feeds) f.loadThrough(b, b.cutId());
        }
        if (cutId == null) for (Feed f : feeds) f.loadThrough(null, Long.MAX_VALUE);
    }

    private void add(String topic, int partition, Recording.Line l, long cut) {
        long off = l.record().offset();
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
        Input in = new Input(topic + "/" + partition + "/" + off, cut, Ids.payloadDigest(value, raw).sha256(), ids, required);
        inputs.put(in.label, in);
        open.add(in);
    }

    /** The problems of completed cut {@code cutId}; cuts are checked in increasing order. Empty when it reconciles. */
    public List<String> cut(long cutId) {
        loadThrough(cutId);
        List<String> p = new ArrayList<>();
        for (Map.Entry<String, Cursor> e : ledgers.entrySet()) {
            Segment s = e.getValue().take(cutId);
            if (s == null) continue;
            lastProgress.put(e.getKey(), s.cutId());
            Set<Long> allowed = new HashSet<>(s.merged());
            allowed.add(s.cutId());
            for (JsonNode line : s.lines()) disposition(e.getKey(), line, allowed, p);
        }
        for (Map.Entry<String, Cursor> e : rows.entrySet()) {
            Segment s = e.getValue().take(cutId);
            if (s == null) continue;
            for (JsonNode line : s.lines()) {
                JsonNode t = line.path("value").path("envelope").path("trigger");
                if (!t.isObject()) continue;
                String label = t.path("topic").asText() + "/" + t.path("partition").asInt() + "/" + t.path("offset").asLong();
                if (!inputs.containsKey(label)) p.add(e.getKey() + " cut " + cutId + ": a trigger names " + label + ", which is no recorded input");
            }
        }
        // coverage, once every ledger writer has closed the input's cut; a reconciled input keeps only its hashes
        long closed = Long.MAX_VALUE;
        for (String k : ledgers.keySet()) closed = Math.min(closed, lastProgress.getOrDefault(k, 0L));
        for (Iterator<Input> it = open.iterator(); it.hasNext();) {
            Input in = it.next();
            if (in.cut > closed) continue;
            Set<String> have = in.seenParts();
            if (!in.required.contains(have)) p.add(in.label + " (cut " + in.cut + ") has the parts " + have + ", not one of " + in.required);
            in.compact();
            it.remove();
        }
        Collections.sort(p);
        return p;
    }

    /** Every recorded input reconciled after the last cut. */
    public List<String> finish() {
        loadThrough(null);
        List<String> p = new ArrayList<>();
        for (Input in : open) p.add(in.label + " (cut " + in.cut + ") was never reconciled");
        for (Map.Entry<String, Cursor> e : ledgers.entrySet()) if (e.getValue().peek() != null) p.add(e.getKey() + ": lines after its last checked progress");
        Collections.sort(p);
        close();
        return p;
    }

    @Override public void close() {
        for (Cursor c : ledgers.values()) c.close();
        for (Cursor c : rows.values()) c.close();
        for (Feed f : feeds) f.close();
    }

    static String canonical(JsonNode n) { return n == null ? "null" : Canonical.utf8(Canonical.bytes(n)); }

    private void disposition(String partition, JsonNode line, Set<Long> allowed, List<String> p) {
        String ledger = partition.substring(0, partition.lastIndexOf('/'));
        JsonNode k = line.path("key"), body = line.path("value").path("body");
        String where = partition + " " + canonical(k);
        if (k.path("generation").asLong() != generation) { p.add(where + ": generation " + k.path("generation") + ", the run's is " + generation); return; }
        String topic = k.path("inputTopic").asText(), part = k.path("part").asText();
        if (ledger.equals(LEDGER1) ? !STAGE1_INPUTS.contains(topic) : !topic.equals(InputParser.INCIDENTS)) { p.add(where + ": " + ledger + " does not own " + topic); return; }
        String label = topic + "/" + k.path("inputPartition").asInt() + "/" + k.path("inputOffset").asLong();
        Input in = inputs.get(label);
        if (in == null) { p.add(where + ": its origin " + label + " is no recorded input"); return; }
        int i = in.part(part);
        if (i < 0) { p.add(where + ": the recorded input has no part " + part); return; }
        int pi = partitionIndex.computeIfAbsent(partition, x -> { partitionNames.add(x); return partitionNames.size() - 1; });
        if (in.ledgerPartition[i] >= 0 && in.ledgerPartition[i] != pi) p.add(where + ": the key is also written in " + partitionNames.get(in.ledgerPartition[i]));
        if (in.ledgerPartition[i] < 0) in.ledgerPartition[i] = pi;
        long rev = line.path("value").path("revision").asLong();
        long last = in.lastRevision[i];
        if (rev != last + 1) p.add(where + ": revision " + rev + " after " + last);
        in.lastRevision[i] = (int) Math.max(last, rev);
        if (rev == 1 && !allowed.contains(in.cut)) p.add(where + ": first written at cut(s) " + new TreeSet<>(allowed) + ", its input belongs to cut " + in.cut);
        String digest = body.path("payloadDigest").asText();
        if (in.payloadDigest != null ? !in.payloadDigest.equals(digest) : in.payloadHash != digest.hashCode())
            p.add(where + ": payloadDigest " + digest + ", the recorded input's is " + (in.payloadDigest != null ? in.payloadDigest : "(hash " + in.payloadHash + ")"));
        JsonNode got = body.get("identity");
        if (got == null) p.add(where + ": no identity");
        else if (in.identities != null ? !canonical(in.identities[i]).equals(canonical(got)) : in.identityHash[i] != canonical(got).hashCode())
            p.add(where + ": identity " + got + ", the recorded input's " + part + " is " + (in.identities != null ? in.identities[i] : "(hash " + in.identityHash[i] + ")"));
        in.seen |= 1L << i;
    }
}
