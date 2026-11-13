package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.cli.gate.CoreGate.Check;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.CoreGate.Result;
import yggdrasil.forseti.cli.gate.Foundation.Problems;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.*;
import yggdrasil.forseti.core.release.FloorStore;
import yggdrasil.forseti.core.release.HeadVerifier;
import yggdrasil.forseti.core.release.ReleaseReader;
import yggdrasil.forseti.core.rules.RuleSet;

/** The stage-1 harness assertions of pull request 2 ({@code S-*}): {@code forseti run --engine core} on the committed {@code smoke} and
 *  {@code smoke-plc} recordings under a generation registered through {@code generation new} in a temporary store of the assertion
 *  ({@code store: assertion:<ID>}, {@code isolated}; the run preparation of {@code forseti run} writes the initial recovery declaration).
 *  The head is verified by {@code generation new} at its own signedAt + 60 s, never by the harness and never at the wall clock; nothing is
 *  written under {@code fixtures/}. The runs are made once per gate and shared by the assertions that read them. */
final class Stage1Assertions {
    private Stage1Assertions() {}

    static final String SMOKE = "fixtures/recordings/smoke", SMOKE_PLC = "fixtures/recordings/smoke-plc";
    static final String WR = "forseti.work-records", SAMPLES = "forseti.samples", LEDGER = "forseti.dispositions-stage1";
    static final List<String> STAGE1_TOPICS = List.of("ot.messages", "ot.sessions", "mfg.occupancy");
    /** samplesDigest of the smoke visit (station1, [T0 + 600 s, T0 + 660 s)), as Stage1OccupancyJoinTest and the oracle pin it. */
    static final String SMOKE_SAMPLES_DIGEST = "e518a18ba8c533fb5e3fb00529aaf1ebff54d67a07c810a5012f7fe1a926096f";
    static final long T0 = 1794182400000L, PROVISIONAL_SCAN = T0 + 1_410_000, COMPLETE_SCAN = T0 + 1_411_000;
    static final int SMOKE_SAMPLE_ROWS = 5684;
    static final long SEED_A = 7, SEED_B = 20261110;

    static List<Check> checks() {
        return List.of(
            new Check("S-SMOKE-VISIT", Stage1Assertions::smokeVisit),
            new Check("S-SMOKE-PLC", Stage1Assertions::smokePlc),
            new Check("S-REVISIONS", Stage1Assertions::revisions),
            new Check("S-PROGRESS", Stage1Assertions::progress),
            new Check("S-W1", Stage1Assertions::w1),
            new Check("S-DETERMINISM", Stage1Assertions::determinism));
    }

    // --- runs ----------------------------------------------------------------------------------------------------------------

    /** One harness run: its recording, its output directory and its run id. */
    record Run(Path recording, Path out, String runId) {}

    /** The default run of {@code recording} (seed 20261109, no shuffle), made once per gate. */
    static Run base(Context c, String recording) throws Exception {
        return c.memo("run:" + recording, () -> harness(c, recording, null, false));
    }

    /** Registers a generation for {@code recording}'s rule through {@code generation new} (in-process, the fixture clock) in a new
     *  temporary store of the running assertion ({@code assertion:<ID>}), runs {@code forseti run --engine core} on it, and keeps the run
     *  record and cut log ({@link GateEvidence}). */
    static Run harness(Context c, String recording, Long seed, boolean shuffle) throws Exception {
        Path rec = c.path(recording), registry = c.fixtures().resolve("registry");
        String scenario = rec.getFileName().toString();
        GateEvidence.Store store = c.assertionStore();
        GateEvidence.Registered g = GateEvidence.register(c, store, registry, rec, scenario, GateEvidence.ONLINE, c.current);
        Path out = c.newDir("run").resolve("out");
        List<String> args = new ArrayList<>(List.of("run", "--engine", "core", "--recording", rec.toString(), "--registry", registry.toString(),
            "--state", store.dir().toString(), "--generation", Long.toString(g.generation()), "--out", out.toString()));
        if (seed != null) args.addAll(List.of("--seed", Long.toString(seed)));
        if (shuffle) args.add("--shuffle");
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = Main.run(args.toArray(String[]::new), new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8));
        String stdout = so.toString(StandardCharsets.UTF_8).strip();
        if (exit != 0) throw new IllegalStateException("forseti run exit " + exit + ": " + stdout + " " + se.toString(StandardCharsets.UTF_8).strip());
        long s = seed == null ? ReferenceEngine.DEFAULT_SEED : seed;
        GateEvidence.keep(c, g, out, rec, scenario, GateEvidence.ONLINE, s, shuffle,
            GateEvidence.safe(store.name()) + "." + scenario + (shuffle ? ".shuffle-" + s : seed != null ? ".seed-" + s : ""));
        return new Run(rec, out, Json.read(stdout.getBytes(StandardCharsets.UTF_8)).get("runId").asText());
    }

    // --- journal reading -----------------------------------------------------------------------------------------------------

    static Path journal(Path out, String topic, int partition) { return out.resolve("journals").resolve(topic).resolve(partition + ".jsonl"); }

    static List<JsonNode> lines(Path out, String topic, int partition) throws IOException {
        Path f = journal(out, topic, partition);
        if (!Files.isRegularFile(f)) throw new IllegalStateException("no journal " + topic + "/" + partition);
        List<JsonNode> l = new ArrayList<>();
        for (String s : Files.readAllLines(f, StandardCharsets.UTF_8)) l.add(Json.read(s.getBytes(StandardCharsets.UTF_8)));
        return l;
    }

    static boolean isProgress(JsonNode line) { return "progress".equals(line.path("value").path("type").textValue()); }

    static String keyText(JsonNode key) { return Canonical.utf8(Canonical.bytes(key)); }

    /** A journal partition's lines as fold entries; a row's {@code bodyDigest} must be its body's digest. */
    static List<JournalFold.Entry> entries(String topic, List<JsonNode> l, Problems p) {
        List<JournalFold.Entry> e = new ArrayList<>();
        for (JsonNode n : l) {
            JsonNode v = n.get("value");
            if (isProgress(n)) e.add(Progress.fromJson(v));
            else if (v.path("delete").isObject()) e.add(Delete.fromJson(v.get("delete")));   // stage 2's forseti.affected only
            else if (topic.equals(LEDGER) || topic.equals(Stage2Assertions.LEDGER2)) {
                ObjectNode d = Json.object();
                d.set("key", n.get("key")); d.set("body", v.get("body")); d.set("revision", v.get("revision"));
                e.add(Disposition.fromJson(d));
            } else {
                if (!Canonical.digest(v.get("body")).equals(v.get("bodyDigest").asText())) p.list.add(topic + " " + keyText(n.get("key")) + ": bodyDigest is not the body's digest");
                e.add(new Row(topic, n.get("key"), v.get("body"), v.get("bodyDigest").asText(), Envelope.fromJson(v.get("envelope"))));
            }
        }
        return e;
    }

    static List<Long> cutIds(Recording rec) { return rec.batches().stream().map(Recording.Batch::cutId).toList(); }

    /** The station partitions of the recording's routing, sorted. */
    static List<Integer> stationPartitions(Recording rec) { return new ArrayList<>(new TreeSet<>(rec.routing().stations().values())); }

    /** The latest body per work-record key at each cut's progress on {@code partition}: cutId -> key text -> body. */
    static Map<Long, Map<String, JsonNode>> bodiesAtCuts(Path out, int partition) throws IOException {
        Map<Long, Map<String, JsonNode>> at = new LinkedHashMap<>();
        Map<String, JsonNode> live = new TreeMap<>();
        for (JsonNode n : lines(out, WR, partition)) {
            if (isProgress(n)) at.put(n.path("value").get("cutId").longValue(), new TreeMap<>(live));
            else live.put(keyText(n.get("key")), n.path("value").get("body"));
        }
        return at;
    }

    // --- S-SMOKE-VISIT -------------------------------------------------------------------------------------------------------

    static Result smokeVisit(Context c) throws Exception {
        Run r = base(c, SMOKE);
        Recording rec = Recording.open(r.recording());
        JsonNode expected = rec.expected().path("visits").get(0);
        Problems p = new Problems();
        ObjectNode ref = Json.object().put("mode", "occupancy-join").put("source", "mes-line2").put("arrivalEventId", "occ-1");
        // one work record key over every station partition, on station1's partition
        Map<String, Integer> keys = new TreeMap<>();
        for (int part : stationPartitions(rec)) for (JsonNode n : lines(r.out(), WR, part)) if (!isProgress(n)) keys.put(keyText(n.get("key")), part);
        int s1 = rec.routing().partitionOf(Json.text(expected, "station"));
        p.equal(Map.of(keyText(ref), s1), keys, "work record keys -> partition");
        Map<Long, Map<String, JsonNode>> bodies = bodiesAtCuts(r.out(), s1);
        List<Long> cuts = cutIds(rec);
        long lastCut = cuts.get(cuts.size() - 1);
        JsonNode last = bodies.getOrDefault(lastCut, Map.of()).get(keyText(ref));
        String digest = null;
        if (last == null) p.list.add("no work record at the last cut " + lastCut);
        else {
            p.equal("complete", last.path("status").asText(null), "status at the last cut");
            p.equal(Json.text(expected, "unitId"), last.path("unitId").asText(null), "unitId");
            p.equal(expected.get("enter").longValue(), last.path("interval").path("enter").asLong(), "interval.enter");
            p.equal(expected.get("leave").longValue(), last.path("interval").path("leave").asLong(), "interval.leave");
            p.equal(0, last.path("insufficiencyReasons").size(), "insufficiencyReasons");
            digest = last.path("samplesDigest").asText(null);
            p.equal(SMOKE_SAMPLES_DIGEST, digest, "samplesDigest");
        }
        // sample rows: one key per member of every message, 5,684
        long members = 0, messages = 0;
        for (int part : rec.routing().subscribed("ot.messages")) for (Recording.Record m : rec.records("ot.messages", part)) { messages++; members += m.value().path("members").size(); }
        Set<String> sampleKeys = new HashSet<>();
        long sampleRows = 0;
        for (int part : stationPartitions(rec)) for (JsonNode n : lines(r.out(), SAMPLES, part)) if (!isProgress(n)) { sampleRows++; sampleKeys.add(keyText(n.get("key"))); }
        p.equal((long) SMOKE_SAMPLE_ROWS, members, "members over the recording's messages");
        p.equal(SMOKE_SAMPLE_ROWS, sampleKeys.size(), "sample row keys");
        // the transition: provisional at the cut whose scan ends at T0 + 1410000, complete at the one ending at T0 + 1411000
        Map<Long, Long> scanEnd = scanEnds(rec);
        Long cutP = null, cutC = null;
        for (Map.Entry<Long, Long> e : scanEnd.entrySet()) {
            if (e.getValue() == PROVISIONAL_SCAN) cutP = e.getKey();
            if (e.getValue() == COMPLETE_SCAN) cutC = e.getKey();
        }
        JsonNode cutLog = Json.read(r.out().resolve("cuts.json")).get("cuts");
        String transition = "?";
        if (cutP == null || cutC == null) p.list.add("no cut ends at scan " + PROVISIONAL_SCAN + " / " + COMPLETE_SCAN + ": " + scanEnd);
        else {
            p.equal(cutP + 1, (long) cutC, "the two cuts are consecutive");
            JsonNode bp = bodies.getOrDefault(cutP, Map.of()).get(keyText(ref)), bc = bodies.getOrDefault(cutC, Map.of()).get(keyText(ref));
            p.equal("provisional", bp == null ? null : bp.path("status").asText(), "status at cut " + cutP);
            p.equal("complete", bc == null ? null : bc.path("status").asText(), "status at cut " + cutC);
            for (Map.Entry<Long, Map<String, JsonNode>> e : bodies.entrySet()) {
                JsonNode b = e.getValue().get(keyText(ref));
                if (e.getKey() < cutC && b != null && "complete".equals(b.path("status").asText())) p.list.add("complete already at cut " + e.getKey());
            }
            Recording.Deadline settle = rec.deadlines().stream().filter(d -> d.kind().equals("visit-settlement")).findFirst().orElse(null);
            if (settle == null) p.list.add("the recording lists no visit-settlement deadline");
            else { p.equal(settle.cutA(), (long) cutP, "recording deadline cutA"); p.equal(settle.cutB(), (long) cutC, "recording deadline cutB"); }
            String s = Json.text(expected, "station");
            long wP = cutEntry(cutLog, cutP).path("perStation").path(s).path("W1").asLong(), wC = cutEntry(cutLog, cutC).path("perStation").path(s).path("W1").asLong();
            if (settle != null) {
                p.require(wP < settle.at() && wC >= settle.at(), "W1 " + wP + " / " + wC + " against the settlement deadline " + settle.at());
                transition = "provisional at cut " + cutP + " (scan T0+" + (PROVISIONAL_SCAN - T0) + ", W1 " + wP + ") -> complete at cut " + cutC
                    + " (scan T0+" + (COMPLETE_SCAN - T0) + ", W1 " + wC + " >= leave + L " + settle.at() + ")";
            }
        }
        return p.result("1 work record complete at cut " + lastCut + "; " + sampleKeys.size() + " sample keys (" + messages + " messages, " + members
            + " members, " + sampleRows + " sample rows with revisions); samplesDigest " + digest + "; " + transition);
    }

    static JsonNode cutEntry(JsonNode cutLog, long cutId) {
        for (JsonNode c : cutLog) if (c.path("cutId").asLong() == cutId) return c;
        throw new IllegalStateException("cuts.json has no cut " + cutId);
    }

    /** cutId -> the largest member sourceTs of the messages delivered through that cut (the scan the cut ends at). */
    static Map<Long, Long> scanEnds(Recording rec) {
        Map<Long, Long> out = new LinkedHashMap<>();
        long max = Long.MIN_VALUE;
        Map<Integer, Long> previous = new HashMap<>();
        for (Recording.Batch b : rec.batches()) {
            for (int part : rec.routing().subscribed("ot.messages")) {
                long from = previous.getOrDefault(part, -1L) + 1, through = b.through("ot.messages", part);
                for (long off = from; off <= through; off++)
                    for (JsonNode m : rec.records("ot.messages", part).get((int) off).value().path("members"))
                        if (m.path("sourceTs").isIntegralNumber()) max = Math.max(max, m.get("sourceTs").longValue());
                previous.put(part, through);
            }
            out.put(b.cutId(), max);
        }
        return out;
    }

    // --- S-SMOKE-PLC ---------------------------------------------------------------------------------------------------------

    static Result smokePlc(Context c) throws Exception {
        Run r = base(c, SMOKE_PLC);
        Recording rec = Recording.open(r.recording());
        JsonNode expected = rec.expected().path("plcVisits").get(0);
        Problems p = new Problems();
        String station = Json.text(expected, "station");
        ObjectNode ref = Json.object().put("mode", "plc-stamped").put("namespace", "urn:line2:visits").put("station", station).put("visitId", Json.text(expected, "visitId"));
        Map<String, Integer> keys = new TreeMap<>();
        for (int part : stationPartitions(rec)) for (JsonNode n : lines(r.out(), WR, part)) if (!isProgress(n)) keys.put(keyText(n.get("key")), part);
        int s1 = rec.routing().partitionOf(station);
        p.equal(Map.of(keyText(ref), s1), keys, "work record keys -> partition");
        List<Long> cuts = cutIds(rec);
        JsonNode last = bodiesAtCuts(r.out(), s1).getOrDefault(cuts.get(cuts.size() - 1), Map.of()).get(keyText(ref));
        if (last == null) return p.result("no plc-stamped work record at the last cut");
        p.equal("complete", last.path("status").asText(null), "status");
        p.equal("plc-stamped", last.path("attributionMode").asText(null), "attributionMode");
        p.equal(station, last.path("readPoint").asText(null), "readPoint");
        p.equal(Json.text(expected, "unitId"), last.path("unitId").asText(null), "unitId");
        p.require(last.path("visitSeq").isNull(), "visitSeq is null");
        p.equal(expected.get("enter").longValue(), last.path("interval").path("enter").asLong(), "interval.enter");
        p.equal(expected.get("leave").longValue(), last.path("interval").path("leave").asLong(), "interval.leave");
        p.equal(0, last.path("insufficiencyReasons").size(), "insufficiencyReasons");
        return p.result("1 work record " + keyText(ref) + " complete at cut " + cuts.get(cuts.size() - 1) + ", readPoint " + station
            + ", interval [" + expected.get("enter") + ", " + expected.get("leave") + "), samplesDigest " + last.path("samplesDigest").asText());
    }

    // --- S-REVISIONS ---------------------------------------------------------------------------------------------------------

    static Result revisions(Context c) throws Exception {
        Problems p = new Problems();
        List<String> summary = new ArrayList<>();
        for (String recording : List.of(SMOKE, SMOKE_PLC)) {
            Run r = base(c, recording);
            Recording rec = Recording.open(r.recording());
            List<Long> cuts = cutIds(rec);
            long keys = 0, records = 0, maxRevision = 0;
            List<String[]> files = new ArrayList<>();
            for (int part : stationPartitions(rec)) for (String t : List.of(WR, SAMPLES, LEDGER)) files.add(new String[] {t, Integer.toString(part)});
            files.add(new String[] {LEDGER, "6"});
            for (String[] f : files) {
                String label = rec.manifest().path("name").asText() + " " + f[0] + "/" + f[1];
                List<JournalFold.Entry> e = entries(f[0], lines(r.out(), f[0], Integer.parseInt(f[1])), p);
                JournalFold.Result fold = JournalFold.fold(e, 1, cuts.get(cuts.size() - 1));
                p.require(fold.reachedProgress(), label + ": the last cut's progress is missing");
                p.require(fold.violations().revisionGaps().isEmpty(), label + " revision gaps: " + fold.violations().revisionGaps());
                p.require(fold.violations().equalConsecutiveDigests().isEmpty(), label + " equal consecutive digests: " + fold.violations().equalConsecutiveDigests());
                p.require(fold.violations().deleteSupersedesMismatches().isEmpty(), label + " delete mismatches: " + fold.violations().deleteSupersedesMismatches());
                p.require(fold.violations().rowsAfterProgress().isEmpty(), label + " rows after progress: " + fold.violations().rowsAfterProgress());
                keys += fold.live().size();
                for (JournalFold.Entry x : e) {
                    if (x instanceof Progress) continue;
                    records++;
                    long rev = x instanceof Row row ? row.envelope().revision() : ((Disposition) x).revision();
                    maxRevision = Math.max(maxRevision, rev);
                }
            }
            summary.add(rec.manifest().path("name").asText() + ": " + files.size() + " journals, " + keys + " keys, " + records + " records, max revision " + maxRevision);
        }
        return p.result("JournalFold violations empty; " + String.join("; ", summary));
    }

    // --- S-PROGRESS ----------------------------------------------------------------------------------------------------------

    static Result progress(Context c) throws Exception {
        Problems p = new Problems();
        List<String> summary = new ArrayList<>();
        for (String recording : List.of(SMOKE, SMOKE_PLC)) {
            Run r = base(c, recording);
            Recording rec = Recording.open(r.recording());
            String name = rec.manifest().path("name").asText();
            List<Long> cuts = cutIds(rec);
            JsonNode cutLog = Json.read(r.out().resolve("cuts.json")).get("cuts");
            int checkedRows = 0, progressRecords = 0;
            for (Map.Entry<String, Integer> st : rec.routing().stations().entrySet()) {
                int part = st.getValue();
                for (String topic : List.of(WR, SAMPLES)) {
                    String label = name + " " + topic + "/" + part;
                    List<JsonNode> l = lines(r.out(), topic, part);
                    p.require(!l.isEmpty() && isProgress(l.get(l.size() - 1)), label + ": does not end with a progress record");
                    List<Progress.Stage1> pr = new ArrayList<>();
                    List<JsonNode> segment = new ArrayList<>();
                    for (JsonNode n : l) {
                        if (!isProgress(n)) { segment.add(n); continue; }
                        p.require(n.get("key").isNull(), label + ": a progress record with a key");
                        Progress.Stage1 s = (Progress.Stage1) Progress.fromJson(n.get("value"));
                        int k = pr.size();
                        pr.add(s);
                        progressRecords++;
                        if (k >= cuts.size()) { p.list.add(label + ": more progress records than cuts"); break; }
                        // the cut's rows precede its progress: stamped with its W1, triggered inside its offset ranges or by a due timer
                        JsonNode cut = cutLog.get(k);
                        for (JsonNode row : segment) {
                            checkedRows++;
                            JsonNode env = row.path("value").path("envelope");
                            if (env.path("eventTimeAt").asLong() != s.W1()) p.list.add(label + " cut " + s.cutId() + ": a row stamped " + env.path("eventTimeAt") + ", W1 " + s.W1());
                            JsonNode t = env.path("trigger");
                            if (t.isObject()) {
                                JsonNode range = cut.path("offsetRanges").path(t.path("topic").asText()).path(t.path("partition").asText());
                                long off = t.path("offset").asLong();
                                if (!range.isArray() || off < range.get(0).asLong() || off >= range.get(1).asLong())
                                    p.list.add(label + " cut " + s.cutId() + ": a row triggered at " + t + " outside the cut's range " + range);
                            } else if (Envelope.timerDeadline(env.path("cause")) == null || Envelope.timerDeadline(env.path("cause")) > s.W1())
                                p.list.add(label + " cut " + s.cutId() + ": a row with neither a cut input nor a due timer: " + env.path("cause"));
                        }
                        segment.clear();
                        p.equal(cuts.get(k), s.cutId(), label + " progress " + k + " cutId");
                        p.equal(st.getKey(), s.station(), label + " station");
                        p.equal(k == 0 ? null : pr.get(k - 1).W1(), s.admissionW(), label + " cut " + s.cutId() + " admissionW");
                        p.equal(k == 0, s.initEvidence() != null, label + " cut " + s.cutId() + " initEvidence present");
                        if (k == 0 && s.initEvidence() != null)
                            p.equal(new Progress.InitEvidence(0, 1, "none"), s.initEvidence(), label + " initEvidence");
                        p.equal(cut.path("perStation").path(st.getKey()).path("W1").asLong(), s.W1(), label + " cut " + s.cutId() + " W1 vs cuts.json");
                    }
                    p.equal(cuts.size(), pr.size(), label + " progress records");
                }
                ledgerProgress(p, name, rec, r.out(), part, st.getKey(), cuts);
            }
            ledgerProgress(p, name, rec, r.out(), 6, null, cuts);
            summary.add(name + ": " + cuts.size() + " cuts, " + rec.routing().stations().size() + " stations x 2 evidence topics + " + (rec.routing().stations().size() + 1)
                + " ledgers, " + progressRecords + " evidence progress records, " + checkedRows + " rows placed before their cut's progress");
        }
        return p.result(String.join("; ", summary) + "; admissionW = previous W1, initEvidence {0, 1, none} on the first progress only");
    }

    static void ledgerProgress(Problems p, String name, Recording rec, Path out, int part, String station, List<Long> cuts) throws IOException {
        String label = name + " " + LEDGER + "/" + part;
        List<JsonNode> l = lines(out, LEDGER, part);
        List<Progress.Ledger> pr = new ArrayList<>();
        for (JsonNode n : l) if (isProgress(n)) pr.add((Progress.Ledger) Progress.fromJson(n.get("value")));
        p.require(!l.isEmpty() && isProgress(l.get(l.size() - 1)), label + ": does not end with a progress record");
        p.equal(cuts, pr.stream().map(Progress::cutId).toList(), label + " progress cut ids");
        for (int k = 0; k < pr.size(); k++) {
            Progress.Ledger g = pr.get(k);
            p.equal(1, g.stage(), label + " stage");
            p.equal(part, g.partition(), label + " partition");
            p.equal(station, g.station(), label + " station");
            p.equal(k == 0, g.initEvidence() != null, label + " cut " + g.cutId() + " initEvidence present");
        }
        JournalFold.Result fold = JournalFold.fold(entries(LEDGER, l, p), 1, cuts.get(cuts.size() - 1));
        p.require(fold.violations().rowsAfterProgress().isEmpty(), label + ": dispositions after their cut's progress " + fold.violations().rowsAfterProgress());
    }

    // --- S-W1 ----------------------------------------------------------------------------------------------------------------

    /** S10: {@code W1(s) = max sourceTs of the telemetry routed to s through the cut - outOfOrdernessMs - 1}, never decreasing, and the
     *  bottom of the JCS integer domain before a station's first routed telemetry. "Routed" is S1 read from the rule set, not from the
     *  harness: the device is an equipment of the rule's member catalog under one of its stations, a member carries a sourceTs, and the
     *  message's definitionSha256 is the recording's definition of the device. Every message S1 does not route must carry a rejected
     *  {@code part message} disposition on the reject collector's partition 6. */
    static Result w1(Context c) throws Exception {
        Problems p = new Problems();
        List<String> summary = new ArrayList<>();
        for (String recording : List.of(SMOKE, SMOKE_PLC)) {
            Run r = base(c, recording);
            Recording rec = Recording.open(r.recording());
            String name = rec.manifest().path("name").asText();
            RuleSet rules = ruleSet(c, rec);
            long oo = rules.attribution().outOfOrdernessMs();
            JsonNode definitions = rec.manifest().path("definitions");
            JsonNode cutLog = Json.read(r.out().resolve("cuts.json")).get("cuts");
            p.equal(rec.batches().size(), cutLog.size(), name + " cuts in cuts.json");
            Map<String, String> rejectedMessages = new HashMap<>();
            for (JsonNode n : lines(r.out(), LEDGER, 6)) {
                if (isProgress(n)) continue;
                JsonNode k = n.get("key");
                if (k.path("inputTopic").asText().equals("ot.messages") && k.path("part").asText().equals("message")
                    && n.path("value").path("body").path("disposition").asText().equals("rejected"))
                    rejectedMessages.put(k.path("inputPartition").asText() + "/" + k.path("inputOffset").asText(), n.path("value").path("body").path("reason").asText());
            }
            Map<String, Long> max = new HashMap<>(), w = new HashMap<>();
            Map<Integer, Long> previous = new HashMap<>();
            Map<String, List<Long>> expected = new TreeMap<>();
            Map<String, Integer> notRouted = new TreeMap<>();
            int compared = 0;
            for (int k = 0; k < rec.batches().size() && k < cutLog.size(); k++) {
                Recording.Batch b = rec.batches().get(k);
                for (int part : rec.routing().subscribed("ot.messages")) {
                    long from = previous.getOrDefault(part, -1L) + 1, through = b.through("ot.messages", part);
                    for (long off = from; off <= through; off++) {
                        JsonNode v = rec.records("ot.messages", part).get((int) off).value();
                        String device = v.path("device").asText();
                        Long ts = null;
                        for (JsonNode m : v.path("members")) if (m.path("sourceTs").isIntegralNumber()) ts = ts == null ? m.get("sourceTs").longValue() : Math.max(ts, m.get("sourceTs").longValue());
                        Optional<String> station = rules.members().equipment().contains(device) ? rules.hierarchy().stationOf(rules.attribution().stations(), device) : Optional.empty();
                        String why = station.isEmpty() ? "unknown-device" : ts == null ? "no-source-timestamp"
                            : !definitions.path(device).path("sha256").asText("").equals(v.path("definitionSha256").asText()) ? "definition-mismatch" : null;
                        if (why == null) { max.merge(station.get(), ts, Math::max); continue; }
                        notRouted.merge(device.substring(device.lastIndexOf(':') + 1) + " " + why, 1, Integer::sum);
                        p.equal(why, rejectedMessages.get(part + "/" + off), name + " ot.messages/" + part + "/" + off + " rejection on partition 6");
                    }
                    previous.put(part, through);
                }
                for (String s : rec.routing().stations().keySet()) {
                    long formula = max.containsKey(s) ? max.get(s) - oo - 1 : -Json.MAX_SAFE;
                    long want = Math.max(formula, w.getOrDefault(s, Long.MIN_VALUE));
                    w.put(s, want);
                    expected.computeIfAbsent(s, x -> new ArrayList<>()).add(want);
                    p.equal(want, cutLog.get(k).path("perStation").path(s).path("W1").asLong(), name + " cut " + b.cutId() + " " + s + " W1 (cuts.json)");
                    compared++;
                }
            }
            for (Map.Entry<String, Integer> st : rec.routing().stations().entrySet())
                for (String topic : List.of(WR, SAMPLES)) {
                    List<Long> got = new ArrayList<>();
                    for (JsonNode n : lines(r.out(), topic, st.getValue())) if (isProgress(n)) got.add(n.path("value").get("W1").longValue());
                    p.equal(expected.get(st.getKey()), got, name + " " + topic + "/" + st.getValue() + " progress W1");
                }
            StringBuilder last = new StringBuilder();
            w.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> last.append(last.length() == 0 ? "" : ", ").append(e.getKey().substring(e.getKey().lastIndexOf(':') + 1)).append(' ').append(e.getValue()));
            summary.add(name + ": " + compared + " station-cuts equal max routed sourceTs - " + oo + " - 1 in cuts.json and both progress topics; not routed "
                + (notRouted.isEmpty() ? "none" : notRouted + " (each rejected on partition 6)") + "; last W1 " + last);
        }
        return p.result(String.join("; ", summary));
    }

    /** The rule set the recording names, from its release in the fixture registry (head verified at its signedAt + 60 s). */
    static RuleSet ruleSet(Context c, Recording rec) throws IOException {
        Path m = c.fixtures().resolve("registry").resolve("mappings");
        JsonNode rule = rec.manifest().get("rule");
        HeadVerifier.Head h = HeadVerifier.verify(m, Json.text(rule, "stream"), Foundation.fixtureKeys(c), new FloorStore(c.newDir("state")), GateSigner.now(m, Json.text(rule, "stream")));
        return RuleSet.load(ReleaseReader.read(m, h), Json.text(rule, "attributionRuleId"), Json.text(rule, "attributionRuleVersion"), rec.manifest().get("definitions"));
    }

    // --- S-DETERMINISM -------------------------------------------------------------------------------------------------------

    /** Three runs per recording: the default (seed 20261109, unshuffled) and two {@code --shuffle} runs with seeds 7 and 20261110. Every
     *  journal and quarantine line is equal byte for byte once {@code value.envelope.pins.job.runId} is removed (the only field derived
     *  from the run id; it is checked to be the run's own id), {@code cuts.json} is equal byte for byte, and {@code run.json} differs only
     *  in {@code runId}, {@code pins.job.runId}, {@code seed}, {@code shuffle} and {@code command}. */
    static Result determinism(Context c) throws Exception {
        Problems p = new Problems();
        List<String> summary = new ArrayList<>();
        for (String recording : List.of(SMOKE, SMOKE_PLC)) {
            Run a = base(c, recording), b = harness(c, recording, SEED_A, true), d = harness(c, recording, SEED_B, true);
            String name = Recording.open(a.recording()).manifest().path("name").asText();
            p.require(new HashSet<>(List.of(a.runId(), b.runId(), d.runId())).size() == 3, name + ": the three run ids are not distinct");
            TreeMap<String, List<String>> fa = stripped(a, p), fb = stripped(b, p), fd = stripped(d, p);
            long lines = 0, progress = 0;
            for (Map.Entry<String, TreeMap<String, List<String>>> other : Map.of("seed " + SEED_A + " --shuffle", fb, "seed " + SEED_B + " --shuffle", fd).entrySet()) {
                p.equal(fa.keySet(), other.getValue().keySet(), name + " " + other.getKey() + " journal files");
                for (String f : fa.keySet()) {
                    List<String> la = fa.get(f), lo = other.getValue().getOrDefault(f, List.of());
                    if (la.size() != lo.size()) { p.list.add(name + " " + other.getKey() + " " + f + ": " + la.size() + " lines vs " + lo.size()); continue; }
                    for (int i = 0; i < la.size(); i++)
                        if (!la.get(i).equals(lo.get(i))) { p.list.add(name + " " + other.getKey() + " " + f + " line " + (i + 1) + " differs"); break; }
                }
            }
            for (List<String> l : fa.values()) for (String s : l) { lines++; if (s.contains("\"type\":\"progress\"")) progress++; }
            byte[] ca = Files.readAllBytes(a.out().resolve("cuts.json"));
            p.require(Arrays.equals(ca, Files.readAllBytes(b.out().resolve("cuts.json"))) && Arrays.equals(ca, Files.readAllBytes(d.out().resolve("cuts.json"))),
                name + ": cuts.json differs");
            JsonNode ra = runRecord(a, ReferenceEngine.DEFAULT_SEED, false, p), rb = runRecord(b, SEED_A, true, p), rd = runRecord(d, SEED_B, true, p);
            p.require(ra.equals(rb) && ra.equals(rd), name + ": run.json differs beyond runId, pins.job.runId, seed, shuffle, command");
            summary.add(name + ": " + fa.size() + " files, " + lines + " lines (" + progress + " progress) equal in 3 runs");
        }
        return p.result(String.join("; ", summary) + "; excluded: value.envelope.pins.job.runId per row; run.json runId, pins.job.runId, seed, shuffle, command");
    }

    /** Journal and quarantine files of a run, each line in canonical form with {@code value.envelope.pins.job.runId} removed after
     *  checking it is the run's id (progress and disposition lines carry none). */
    static TreeMap<String, List<String>> stripped(Run r, Problems p) throws IOException {
        TreeMap<String, List<String>> m = new TreeMap<>();
        for (String dir : List.of("journals", "quarantine")) {
            Path root = r.out().resolve(dir);
            if (!Files.isDirectory(root)) continue;
            try (Stream<Path> s = Files.walk(root)) {
                for (Path f : s.filter(Files::isRegularFile).sorted().toList()) {
                    List<String> out = new ArrayList<>();
                    for (String line : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                        JsonNode n = Json.read(line.getBytes(StandardCharsets.UTF_8));
                        JsonNode job = n.path("value").path("envelope").path("pins").path("job");
                        if (job.isObject()) {
                            if (!r.runId().equals(job.path("runId").asText(null))) p.list.add(f + ": a row carries runId " + job.path("runId") + ", not the run's " + r.runId());
                            ((ObjectNode) job).remove("runId");
                        }
                        out.add(Canonical.utf8(Canonical.bytes(n)));
                    }
                    m.put(r.out().relativize(f).toString().replace('\\', '/'), out);
                }
            }
        }
        return m;
    }

    /** {@code run.json} without the fields a run's id, seed, shuffle flag and command line determine, after checking those values. */
    static JsonNode runRecord(Run r, long seed, boolean shuffle, Problems p) {
        ObjectNode n = (ObjectNode) Json.read(r.out().resolve("run.json"));
        p.equal(r.runId(), n.path("runId").asText(null), "run.json runId");
        p.equal(r.runId(), n.path("pins").path("job").path("runId").asText(null), "run.json pins.job.runId");
        p.equal(seed, n.path("seed").asLong(), "run.json seed");
        p.equal(shuffle, n.path("shuffle").asBoolean(), "run.json shuffle");
        n.remove(List.of("runId", "seed", "shuffle", "command"));
        ((ObjectNode) n.path("pins").path("job")).remove("runId");
        return n;
    }
}
