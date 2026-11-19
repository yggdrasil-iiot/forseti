package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.cli.replay.CompletionReader;
import yggdrasil.forseti.cli.replay.EngineContract;
import yggdrasil.forseti.cli.replay.ReplayEngine;
import yggdrasil.forseti.cli.replay.ReplayTopics;
import yggdrasil.forseti.cli.replay.Restarts;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.cut.CutAssembler;

/**
 * The per-run replay assertions of spec 719-726 but 724 (the Kafka Streams line), engine-independent, on every run of the replay gate:
 * from the run's dumped journals ({@code read_committed}), its recording's batches, its {@code cuts.json}, the epochs the harness wrote
 * each cut's barriers with ({@code cut-epochs.json}) and, on a restart run, the harness's injection manifest ({@code injections.json}:
 * the declarations, the launches, the records taken before and at each kill, and every committed stage-1 progress with its Kafka offset).
 * The expected and attributed sets come from those harness records, never from {@code initEvidence} (spec 416).
 * <ul><li>{@code R-MERGED-EXACT} (719): every progress's {@code mergedCutIds} are exactly the planned cuts between the writer's previous
 * progress and this one, on every progress partition of both stages and both reject collectors;</li>
 * <li>{@code R-MERGED-JUSTIFIED} (720): every merged cut of a stage-1 writer lies in a restart window of a stage-1 restart injection that
 * reinitialized it (the recovery and the receive-order trigger inside the window) or is a cut whose {@code idleInputs} declare the station
 * {@code starved}; a stage-2 writer merges only cuts its station's stage-1 writer merged (following stage 1); the stage-2 reject collector
 * never merges;</li>
 * <li>{@code R-RECOVERY-WINDOW} (721): a reinitialized writer's progress appended after the kill (at or above the high watermark the harness
 * took at the kill, on each of its partitions) closes no cut below the first cut whose {@code restartEpoch} is at or above the epoch of
 * the declaration it fixed; the harness's progress log equals the dumped journals;</li>
 * <li>{@code R-RECOVERED-EPOCH} (722): every stage-1 progress carries the {@code restartEpoch} and {@code recoveryRef} the harness wrote the
 * barriers of its cut with, and every recovered progress (a reinitialized writer's first after the kill) those of the declaration its
 * writer fixed, so no barrier below that epoch closed it;</li>
 * <li>{@code R-INIT-EVIDENCE} (723): every stage-1 progress writer's first progress after each initialization carries {@code initEvidence}
 * (the first start's: the run's declaration, H and the engine's first-start restore evidence; a restart's: that injection's declaration,
 * an H above it and the restore evidence the harness recorded for the launch), no other progress and no stage-2 progress carries one,
 * and for every restart injection the attributed set equals the set of writers whose {@code initEvidence} points to its declaration;</li>
 * <li>{@code R-BARRIER-REFS} (725): every barrier reference names a barrier the harness wrote (its cut, origin and through-offset, a cut
 * above the writer's previous progress and at or below this one), the closing cut's every required barrier is listed, no reference twice;
 * both evidence topics of a station carry the same cut ids and merged cuts; every stage-2 {@code incidentOffsets} has 6 x (1 + merged)
 * entries; no input of a merged cut is without a disposition;</li>
 * <li>{@code C-3-RESTART-SKIPS} (726): comparison item 3 counts the restart skips only, as (logical writer, cut) pairs (a station's two
 * evidence topics and its ledger once, an earlier unfinished recovery attempt not again, the reject collectors apart), and the cut log
 * agrees: a station is missing from a cut of {@code cuts.json} exactly when that cut is a restart skip or a starvation of that station.</li></ul>
 * The run's cut accounting ({@link #accounting}) keeps the completed cuts apart from the merged and skipped ones (spec 418, 620).
 */
final class RestartChecks {
    private RestartChecks() {}

    static final List<String> IDS = List.of("R-MERGED-EXACT", "R-MERGED-JUSTIFIED", "R-RECOVERY-WINDOW", "R-RECOVERED-EPOCH", "R-BARRIER-REFS", "C-3-RESTART-SKIPS");
    static final String WR = "forseti.work-records", SAMPLES = "forseti.samples", DISP1 = ReplayTopics.DISPOSITIONS1, AFFECTED = "forseti.affected",
        SETS = "forseti.affected-sets", DISP2 = ReplayTopics.DISPOSITIONS2;
    static final int REJECTS = CompletionReader.REJECTS;

    /** One run's inputs to the checks. */
    static final class Inputs {
        final Recording rec;
        final RoutingManifest routing;
        final List<Long> planned = new ArrayList<>();
        final Map<Long, Recording.Batch> batch = new HashMap<>();
        final Map<Long, Set<String>> starved = new HashMap<>();
        /** topic/partition -> its progress values in journal order. */
        final Map<String, List<JsonNode>> progress = new TreeMap<>();
        final JsonNode cutLog, epochs, manifest;
        final long declaration, controlEnd;
        final String firstStart;
        final Map<String, List<Integer>> subscribed = new LinkedHashMap<>();

        Inputs(Path recording, Path out, JsonNode cutLog, JsonNode runJson, long declaration, long controlEnd, String firstStart) throws IOException {
            this.rec = Recording.open(recording);
            this.routing = rec.routing();
            this.cutLog = cutLog;
            this.declaration = declaration;
            this.controlEnd = controlEnd;
            this.firstStart = firstStart;
            for (Recording.Batch b : rec.batches()) {
                planned.add(b.cutId());
                batch.put(b.cutId(), b);
                Set<String> s = new TreeSet<>();
                for (CutAssembler.Idle i : EngineContract.idleInputs(b)) if (i.kind().equals(CutAssembler.STARVED)) s.add(i.station());
                starved.put(b.cutId(), s);
            }
            Path ep = out.resolve("cut-epochs.json"), inj = out.resolve(ReplayEngine.INJECTIONS);
            this.epochs = Files.exists(ep) ? Json.read(ep) : null;
            this.manifest = Files.exists(inj) ? Json.read(inj) : null;
            runJson.path("subscribed").fields().forEachRemaining(e -> { List<Integer> l = new ArrayList<>(); e.getValue().forEach(x -> l.add(x.asInt())); subscribed.put(e.getKey(), l); });
            for (String t : List.of(WR, SAMPLES, DISP1, AFFECTED, SETS, DISP2)) {
                Path dir = out.resolve("journals").resolve(t);
                if (!Files.isDirectory(dir)) continue;
                List<Path> files;
                try (var s = Files.list(dir)) { files = s.sorted().toList(); }
                for (Path f : files) {
                    String n = f.getFileName().toString();
                    if (!n.endsWith(".jsonl")) continue;
                    List<JsonNode> l = new ArrayList<>();
                    try (BufferedReader in = Files.newBufferedReader(f, StandardCharsets.UTF_8)) {
                        for (String line; (line = in.readLine()) != null; ) {
                            if (!line.contains("\"progress\"")) continue;
                            JsonNode v = Json.read(line.getBytes(StandardCharsets.UTF_8)).path("value");
                            if ("progress".equals(v.path("type").asText())) l.add(v);
                        }
                    }
                    progress.put(t + "/" + n.substring(0, n.length() - ".jsonl".length()), l);
                }
            }
        }

        Map<String, Integer> stations() { return new TreeMap<>(routing.stations()); }

        /** The stage-1 restart injections of the run (none on a run without a manifest). */
        List<JsonNode> stage1Injections() {
            List<JsonNode> l = new ArrayList<>();
            if (manifest != null) for (JsonNode i : manifest.path("injections")) if (i.path("stage1Restart").asBoolean()) l.add(i);
            return l;
        }

        /** The partitions of a stage-1 writer ({@code topic/partition}). */
        List<String> partitions(String writer) {
            if (writer.equals(Restarts.REJECTS)) return List.of(DISP1 + "/" + REJECTS);
            int p = routing.partitionOf(writer);
            return List.of(WR + "/" + p, SAMPLES + "/" + p, DISP1 + "/" + p);
        }

        String writerOf(String part) {
            int p = Integer.parseInt(part.substring(part.lastIndexOf('/') + 1));
            if (p == REJECTS) return part.startsWith(DISP1) ? Restarts.REJECTS : "stage2-rejects";
            for (Map.Entry<String, Integer> e : routing.stations().entrySet()) if (e.getValue() == p) return e.getKey();
            return "partition " + p;
        }

        boolean stage1(String part) { return part.startsWith(WR + "/") || part.startsWith(SAMPLES + "/") || part.startsWith(DISP1 + "/"); }

        /** The planned cuts strictly between {@code a} and {@code b}. */
        List<Long> between(long a, long b) {
            List<Long> l = new ArrayList<>();
            for (long c : planned) if (c > a && c < b) l.add(c);
            return l;
        }
    }

    static List<Long> longs(JsonNode a) { List<Long> l = new ArrayList<>(); a.forEach(x -> l.add(x.asLong())); return l; }

    static void add(List<String> l, String problem) { if (l.size() < 50) l.add(problem); else if (l.size() == 50) l.add("..."); }

    /** The restart windows of {@code writer}: per stage-1 restart injection that reinitialized it, the cuts above its last committed
     *  progress at the kill and below the first cut at the injection's epoch (spec 418; chained windows join). */
    static List<long[]> windows(Inputs in, String writer) {
        List<long[]> w = new ArrayList<>();
        for (JsonNode i : in.stage1Injections()) {
            if (!contains(i.path("expected"), writer)) continue;
            long last = i.path("atKill").path("lastProgressCutId").path(writer).asLong(0);
            w.add(new long[] {last, i.path("firstCutAtTarget").asLong()});
        }
        return w;
    }

    static boolean inWindow(List<long[]> ws, long c) { for (long[] w : ws) if (c > w[0] && c < w[1]) return true; return false; }

    static boolean contains(JsonNode a, String s) { for (JsonNode x : a) if (x.asText().equals(s)) return true; return false; }

    /** The findings of every id of {@link #IDS} and of {@code R-INIT-EVIDENCE}, and the run's cut accounting into {@code accounting}. */
    static Map<String, List<String>> findings(Inputs in, Integrity.Measures measures, ObjectNode accounting) {
        Map<String, List<String>> f = new TreeMap<>();
        for (String id : IDS) f.put(id, new ArrayList<>());
        f.put("R-INIT-EVIDENCE", new ArrayList<>());
        merged(in, f.get("R-MERGED-EXACT"), f.get("R-MERGED-JUSTIFIED"));
        recovery(in, f.get("R-RECOVERY-WINDOW"), f.get("R-RECOVERED-EPOCH"), f.get("R-INIT-EVIDENCE"));
        barrierRefs(in, measures, f.get("R-BARRIER-REFS"));
        skips(in, f.get("C-3-RESTART-SKIPS"), accounting);
        return f;
    }

    // --- 719, 720 ------------------------------------------------------------------------------------------------------------------

    static void merged(Inputs in, List<String> exact, List<String> justified) {
        Map<String, Set<Long>> stage1Merged = new HashMap<>();
        for (Map.Entry<String, List<JsonNode>> e : in.progress.entrySet()) {
            long prev = 0;
            String part = e.getKey(), writer = in.writerOf(part);
            List<long[]> ws = in.stage1(part) ? windows(in, writer) : List.of();
            for (JsonNode v : e.getValue()) {
                long c = v.path("cutId").asLong();
                List<Long> m = longs(v.path("mergedCutIds"));
                if (c <= prev) add(exact, part + " progress " + c + " after progress " + prev);
                else if (!m.equals(in.between(prev, c))) add(exact, part + " progress " + c + ": mergedCutIds " + m + ", the cuts between its previous progress " + prev + " and it " + in.between(prev, c));
                for (long x : m) {
                    if (in.stage1(part)) {
                        boolean starvation = !writer.equals(Restarts.REJECTS) && in.starved.getOrDefault(x, Set.of()).contains(writer);
                        if (!starvation && !inWindow(ws, x)) add(justified, part + " progress " + c + " merges cut " + x + ": no restart window of " + writer + " holds it and no starvation declares it");
                        if (part.startsWith(WR + "/")) stage1Merged.computeIfAbsent(writer, z -> new HashSet<>()).add(x);
                    }
                }
                prev = Math.max(prev, c);
            }
        }
        for (Map.Entry<String, List<JsonNode>> e : in.progress.entrySet()) {
            String part = e.getKey();
            if (in.stage1(part)) continue;
            String writer = in.writerOf(part);
            for (JsonNode v : e.getValue()) for (long x : longs(v.path("mergedCutIds"))) {
                if (writer.equals("stage2-rejects")) add(justified, part + " progress " + v.path("cutId") + " merges cut " + x + ": the stage-2 reject collector never merges");
                else if (!stage1Merged.getOrDefault(writer, Set.of()).contains(x)) add(justified, part + " progress " + v.path("cutId") + " merges cut " + x + ", which stage 1 of " + writer + " did not merge");
            }
        }
    }

    // --- 721, 722, 723 -------------------------------------------------------------------------------------------------------------

    static void recovery(Inputs in, List<String> window, List<String> epoch, List<String> init) {
        // every stage-1 progress carries the epoch and recovery reference of its cut's barriers as the harness wrote them
        Map<Long, JsonNode> cutEpoch = new HashMap<>();
        if (in.epochs == null) add(epoch, "no cut-epochs.json: the harness's barrier epochs are not recorded");
        else for (JsonNode c : in.epochs.path("cuts")) cutEpoch.put(c.path("cutId").asLong(), c);
        for (Map.Entry<String, List<JsonNode>> e : in.progress.entrySet()) {
            if (!in.stage1(e.getKey())) {
                for (JsonNode v : e.getValue()) if (v.has("initEvidence")) add(init, e.getKey() + " progress " + v.path("cutId") + ": a stage-2 progress carries initEvidence");
                continue;
            }
            for (JsonNode v : e.getValue()) {
                JsonNode ce = cutEpoch.get(v.path("cutId").asLong());
                if (ce == null) { if (in.epochs != null) add(epoch, e.getKey() + " progress " + v.path("cutId") + ": a cut the harness wrote no barriers for"); continue; }
                if (ce.path("restartEpoch").asLong() != v.path("restartEpoch").asLong(-1) || !Objects.equals(text(ce.get("recoveryRef")), text(v.get("recoveryRef"))))
                    add(epoch, e.getKey() + " progress " + v.path("cutId") + ": restartEpoch/recoveryRef " + v.path("restartEpoch") + "/" + v.get("recoveryRef") + ", its barriers' "
                        + ce.path("restartEpoch") + "/" + ce.get("recoveryRef"));
            }
        }
        List<JsonNode> injections = in.stage1Injections();
        Map<String, Set<String>> pointing = new HashMap<>();
        // the first start: every writer's first progress
        for (String w : Restarts.stage1Writers(in.stations().keySet())) for (String part : in.partitions(w)) {
            List<JsonNode> l = in.progress.getOrDefault(part, List.of());
            if (l.isEmpty()) { add(init, part + ": no progress"); continue; }
            JsonNode ie = l.get(0).get("initEvidence");
            String where = part + " progress " + l.get(0).path("cutId");
            if (ie == null) add(init, where + ": the first progress carries no initEvidence");
            else {
                if (ie.path("declarationOffset").asLong(-1) != in.declaration) add(init, where + ": declarationOffset " + ie.path("declarationOffset") + ", the run's " + in.declaration);
                if (ie.path("endOffset").asLong(-1) != in.controlEnd) add(init, where + ": endOffset " + ie.path("endOffset") + ", H " + in.controlEnd);
                if (!in.firstStart.equals(ie.path("restore").asText(null))) add(init, where + ": restore " + ie.path("restore") + ", a first start's " + in.firstStart);
            }
            for (int i = 1; i < l.size(); i++) {
                JsonNode x = l.get(i).get("initEvidence");
                if (x == null) continue;
                long d = x.path("declarationOffset").asLong(-1);
                JsonNode inj = null;
                for (JsonNode j : injections) if (j.path("declaration").path("offset").asLong(-2) == d) inj = j;
                if (inj == null) add(init, part + " progress " + l.get(i).path("cutId") + ": initEvidence of declaration " + d + ", which no restart injection of the run wrote");
                else pointing.computeIfAbsent(inj.path("injectionId").asText(), z -> new TreeSet<>()).add(w);
            }
        }
        if (injections.isEmpty()) return;
        // the restart injections: from the harness's progress log with Kafka offsets, checked equal to the dumped journals first
        JsonNode log = in.manifest.path("progress");
        for (String w : Restarts.stage1Writers(in.stations().keySet())) for (String part : in.partitions(w)) {
            List<JsonNode> j = in.progress.getOrDefault(part, List.of()), h = new ArrayList<>();
            log.path(part).forEach(h::add);
            String differs = j.size() == h.size() ? null : "sizes " + h.size() + " and " + j.size();
            for (int i = 0; differs == null && i < j.size(); i++)
                if (j.get(i).path("cutId").asLong() != h.get(i).path("cutId").asLong() || !same(j.get(i).get("initEvidence"), h.get(i).get("initEvidence")))
                    differs = "at " + i + ": " + h.get(i) + " vs " + j.get(i).path("cutId") + " " + j.get(i).get("initEvidence");
            if (differs != null) add(window, part + ": the harness's progress log is not the dumped journal's (" + differs + ")");
            if (j.size() != h.size()) continue;   // the offsets of the log cannot be laid over the journal
            for (int n = 0; n < injections.size(); n++) {
                JsonNode inj = injections.get(n);
                if (!contains(inj.path("expected"), w)) continue;
                long hw = inj.path("atKill").path("highWatermark").path(part).asLong(-1), next = Long.MAX_VALUE;
                if (hw < 0) { add(window, part + ": no high watermark at the kill of " + inj.path("injectionId").asText()); continue; }
                for (int m = n + 1; m < injections.size(); m++) if (contains(injections.get(m).path("expected"), w)) { next = injections.get(m).path("atKill").path("highWatermark").path(part).asLong(Long.MAX_VALUE); break; }
                long first = inj.path("firstCutAtTarget").asLong(), target = inj.path("targetEpoch").asLong();
                boolean firstSeen = false;
                for (int xi = 0; xi < h.size(); xi++) {
                    JsonNode x = h.get(xi);
                    long off = x.path("offset").asLong();
                    if (off < hw || off >= next) continue;
                    long c = x.path("cutId").asLong();
                    if (c < first) add(window, part + " progress " + c + " at offset " + off + ", appended after the kill of " + inj.path("injectionId").asText()
                        + ": below the first cut " + first + " at its epoch " + target);
                    if (firstSeen) continue;
                    firstSeen = true;
                    // the recovered progress: its writer's first after the kill
                    JsonNode v = j.get(xi);
                    if (contains(inj.path("attributed"), w)) {
                        JsonNode ie = v.get("initEvidence");
                        String where = part + " recovered progress " + c;
                        if (ie == null) add(init, where + ": no initEvidence after the restart " + inj.path("injectionId").asText());
                        else {
                            if (ie.path("declarationOffset").asLong(-1) != inj.path("declaration").path("offset").asLong(-2))
                                add(init, where + ": declarationOffset " + ie.path("declarationOffset") + ", the injection's " + inj.path("declaration").path("offset"));
                            if (ie.path("endOffset").asLong(-1) <= inj.path("declaration").path("offset").asLong()) add(init, where + ": endOffset " + ie.path("endOffset") + " not above its declaration");
                            String restore = inj.path("launches").path("stage1").path("restore").asText(null);
                            if (!Objects.equals(restore, ie.path("restore").asText(null))) add(init, where + ": restore " + ie.path("restore") + ", the harness's record of the launch " + restore);
                        }
                        if (v.path("restartEpoch").asLong(-1) != target || !inj.path("injectionId").asText().equals(text(v.get("recoveryRef"))))
                            add(epoch, where + ": restartEpoch/recoveryRef " + v.path("restartEpoch") + "/" + v.get("recoveryRef") + ", its writer fixed " + target + "/" + inj.path("injectionId").asText());
                    }
                }
            }
        }
        // the attributed set of every injection is the set of writers whose initEvidence points to its declaration, and the harness's own
        // record of that set agrees
        for (JsonNode inj : injections) {
            Set<String> want = new TreeSet<>(), have = pointing.getOrDefault(inj.path("injectionId").asText(), Set.of()), recorded = new TreeSet<>();
            inj.path("attributed").forEach(x -> want.add(x.asText()));
            inj.path("initEvidenceWriters").forEach(x -> recorded.add(x.asText()));
            if (!want.equals(have)) add(init, inj.path("injectionId").asText() + ": attributed set " + want + " (harness records), initEvidence points to its declaration from " + have);
            if (!recorded.equals(have)) add(init, inj.path("injectionId").asText() + ": the manifest records initEvidence writers " + recorded + ", the journals " + have);
        }
    }

    static JsonNode nullIfNull(JsonNode n) { return n == null || n.isNull() ? null : n; }

    /** Equal JSON values, absent and null alike (numbers compared by their text, whatever node type the reader made). */
    static boolean same(JsonNode a, JsonNode b) {
        a = nullIfNull(a); b = nullIfNull(b);
        return a == null ? b == null : b != null && a.toString().equals(b.toString());
    }

    static String text(JsonNode n) { return n == null || n.isNull() ? null : n.asText(); }

    // --- 725 -------------------------------------------------------------------------------------------------------------------------

    static final Set<String> INPUTS = new HashSet<>(EngineContract.STAGE1_TOPICS_AND_INCIDENTS);

    static void barrierRefs(Inputs in, Integrity.Measures measures, List<String> f) {
        boolean anyMerged = false;
        for (Map.Entry<String, List<JsonNode>> e : in.progress.entrySet()) {
            String part = e.getKey();
            long prev = 0;
            // the writer's required barrier set as its first progress closed with it: every later close lists the same origins
            Set<String> required = null;
            for (JsonNode v : e.getValue()) {
                long c = v.path("cutId").asLong();
                if (!v.path("mergedCutIds").isEmpty()) anyMerged = true;
                Set<String> seen = new HashSet<>(), closing = new HashSet<>();
                for (JsonNode r : v.path("barrierRefs")) {
                    String topic = r.path("origin").path("topic").asText();
                    int p = r.path("origin").path("partition").asInt();
                    long rc = r.path("cutId").asLong();
                    if (!seen.add(rc + " " + topic + "/" + p)) add(f, part + " progress " + c + ": barrier of cut " + rc + " on " + topic + "/" + p + " listed twice");
                    if (!INPUTS.contains(topic)) continue;
                    if (rc <= prev || rc > c) add(f, part + " progress " + c + ": barrier of cut " + rc + " outside (" + prev + ", " + c + "]");
                    Recording.Batch b = in.batch.get(rc);
                    if (b == null) add(f, part + " progress " + c + ": barrier of cut " + rc + ", which the harness never wrote");
                    else if (!in.subscribed.getOrDefault(topic, List.of()).contains(p)) add(f, part + " progress " + c + ": barrier on " + topic + "/" + p + ", where the harness writes none");
                    else if (b.through(topic, p) != r.path("throughOffset").asLong(Long.MIN_VALUE))
                        add(f, part + " progress " + c + ": barrier of cut " + rc + " on " + topic + "/" + p + " through " + r.path("throughOffset") + ", the harness wrote " + b.through(topic, p));
                    if (rc == c) closing.add(topic + "/" + p);
                }
                // the closing cut: every required barrier listed (a writer closes a cut only with all of them, spec 412)
                if (v.path("barrierRefs").isArray() && !closing.isEmpty()) {
                    if (required == null) required = closing;
                    else if (!required.equals(closing)) add(f, part + " progress " + c + ": the closing cut lists the barriers " + new TreeSet<>(closing) + ", the writer's required set " + new TreeSet<>(required));
                } else if (v.path("barrierRefs").isArray() && in.stage1(part)) add(f, part + " progress " + c + ": no barrier of its own cut listed");
                if ((part.startsWith(AFFECTED + "/") || part.startsWith(SETS + "/")) && v.has("incidentOffsets")) {
                    int want = 6 * (1 + v.path("mergedCutIds").size());
                    if (v.path("incidentOffsets").size() != want) add(f, part + " progress " + c + ": " + v.path("incidentOffsets").size() + " incidentOffsets, 6 x (1 + " + v.path("mergedCutIds").size() + ") expected");
                }
                prev = Math.max(prev, c);
            }
        }
        for (int p : new TreeSet<>(in.routing.stations().values())) {
            List<String> a = new ArrayList<>(), b = new ArrayList<>();
            for (JsonNode v : in.progress.getOrDefault(WR + "/" + p, List.of())) a.add(v.path("cutId").asLong() + " " + v.path("mergedCutIds"));
            for (JsonNode v : in.progress.getOrDefault(SAMPLES + "/" + p, List.of())) b.add(v.path("cutId").asLong() + " " + v.path("mergedCutIds"));
            if (!a.equals(b)) add(f, "partition " + p + ": work-records and samples progress differ (cutId mergedCutIds): " + a.size() + " vs " + b.size());
        }
        if (anyMerged && (measures == null || measures.inputsWithoutDisposition() > 0))
            add(f, "a run with merged cuts and " + (measures == null ? "no disposition measures" : measures.inputsWithoutDisposition() + " inputs without a disposition"));
    }

    // --- 726 and the cut accounting --------------------------------------------------------------------------------------------------

    /** The run's cut accounting into {@code a}: {@code completedCutIds} (every non-starved station listed in cuts.json and both reject
     *  collectors' progress of the cut), {@code restartSkips} and {@code rejectRestartSkips} ((writer, cut) pairs merged inside a restart
     *  window), {@code starvationSkips}, {@code item3} (the restart skips counted under comparison item 3). Problems go to {@code f}. */
    static void skips(Inputs in, List<String> f, ObjectNode a) {
        Set<String> restart = new TreeSet<>(), rejects = new TreeSet<>(), starvation = new TreeSet<>(), merged = new HashSet<>();
        for (String w : Restarts.stage1Writers(in.stations().keySet())) {
            String part = w.equals(Restarts.REJECTS) ? DISP1 + "/" + REJECTS : WR + "/" + in.routing.partitionOf(w);
            List<long[]> ws = windows(in, w);
            for (JsonNode v : in.progress.getOrDefault(part, List.of())) for (long x : longs(v.path("mergedCutIds"))) {
                boolean starved = !w.equals(Restarts.REJECTS) && in.starved.getOrDefault(x, Set.of()).contains(w);
                merged.add(w + "@" + x);
                if (inWindow(ws, x) && !starved) (w.equals(Restarts.REJECTS) ? rejects : restart).add(w + "@" + x);
                else if (starved) starvation.add(w + "@" + x);
            }
        }
        // the cut log agrees: a station is missing from a cut exactly when the cut is a restart skip or a starvation of it
        Map<Long, JsonNode> log = new HashMap<>();
        for (JsonNode c : in.cutLog.path("cuts")) log.put(c.path("cutId").asLong(), c.path("perStation"));
        ArrayNode completed = Json.JSON.createArrayNode();
        Set<Long> rejects1 = new HashSet<>(), rejects2 = new HashSet<>();
        for (JsonNode v : in.progress.getOrDefault(DISP1 + "/" + REJECTS, List.of())) rejects1.add(v.path("cutId").asLong());
        for (JsonNode v : in.progress.getOrDefault(DISP2 + "/" + REJECTS, List.of())) rejects2.add(v.path("cutId").asLong());
        for (long c : in.planned) {
            JsonNode ps = log.get(c);
            if (ps == null) { add(f, "cut " + c + " is not in cuts.json"); continue; }
            boolean all = true;
            for (String s : in.stations().keySet()) {
                boolean listed = ps.has(s), skipped = restart.contains(s + "@" + c) || starvation.contains(s + "@" + c);
                if (listed && merged.contains(s + "@" + c)) add(f, "cut " + c + " " + s + ": listed in cuts.json although merged into a later progress");
                else if (!listed && !skipped) add(f, "cut " + c + " " + s + ": missing from cuts.json without a restart skip or a starvation");
                if (!listed && !in.starved.getOrDefault(c, Set.of()).contains(s)) all = false;
            }
            if (all && rejects1.contains(c) && rejects2.contains(c)) completed.add(c);
        }
        a.set("completedCutIds", completed);
        ArrayNode r = a.putArray("restartSkips"), rr = a.putArray("rejectRestartSkips"), st = a.putArray("starvationSkips");
        restart.forEach(r::add);
        rejects.forEach(rr::add);
        starvation.forEach(st::add);
        Json.putSafe(a, "item3", restart.size());
        Json.putSafe(a, "item3RejectCollectors", rejects.size());
        // a run whose harness made no restart has no restart skip
        if (in.stage1Injections().isEmpty() && (!restart.isEmpty() || !rejects.isEmpty())) add(f, "restart skips " + restart + rejects + " on a run without a stage-1 restart");
    }
}
