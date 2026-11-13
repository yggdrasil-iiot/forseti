package yggdrasil.forseti.cli.generation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.*;
import yggdrasil.forseti.cli.Args;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationPolicy;
import yggdrasil.forseti.core.generation.GenerationStore;
import yggdrasil.forseti.core.release.FloorStore;
import yggdrasil.forseti.core.release.HeadVerifier;
import yggdrasil.forseti.core.release.Refusal;
import yggdrasil.forseti.core.release.ReleaseReader;
import yggdrasil.forseti.core.release.TrustedKeys;
import yggdrasil.forseti.core.rules.RuleSet;

/** {@code forseti generation new|promote|retire|list}: the only writer of {@code generations.jsonl} (Decision 11, spec "Generations and
 *  revisions"), and the run preparation of {@code forseti run} ({@link #prepareRun}). Every verb holds {@code generations.lock} for its
 *  whole operation: it reads and validates the whole log, verifies what it verifies, allocates, appends and forces. The head
 *  verification of {@code new} takes the floor lock inside that hold (generation lock, then floor lock, never the other order).
 *  Exit codes: 0 done, 1 a refusal ({@code {"refusal": <code>, ...evidence}} on stdout), 2 a usage or input error. {@code new} and
 *  {@code retire} print one JSON object {@code {generation, status, scope, rulesDigest, registeredRowSha256}}; {@code list} prints the
 *  stored transport records byte for byte. The clock is the caller's ({@link Clock#systemUTC()} from {@code Main}); no flag or
 *  environment variable sets it. */
public final class GenerationCli {
    private GenerationCli() {}

    public static int run(String[] args, PrintStream out, PrintStream err, Clock clock) { return run(args, out, err, clock, Path.of("").toAbsolutePath()); }

    /** {@link #run(String[], PrintStream, PrintStream, Clock)} with the repository root {@code promote} reads the committed manifest and
     *  plan from (the directory the command runs in; a test passes the repository root). */
    public static int run(String[] args, PrintStream out, PrintStream err, Clock clock, Path root) {
        if (args.length < 1) return usage(err);
        Args f;
        try {
            f = Args.parse(args, 1, Set.of(), Set.of("stream"));
            switch (args[0]) {
                case "new" -> f.only(Set.of("state", "mappings", "trusted-keys", "stream", "rule", "recording", "definitions"));
                case "retire" -> f.only(Set.of("state", "generation"));
                case "promote" -> f.only(Set.of("state", "mappings", "trusted-keys", "generation", "registered-row-sha256", "run", "gate-report", "shards",
                    "accept-scope"));
                case "list" -> f.only(Set.of("state"));
                default -> { return usage(err); }
            }
        } catch (IllegalArgumentException e) { err.println(e.getMessage()); return usage(err); }
        try {
            switch (args[0]) {
                case "new": return create(f, out, clock);
                case "retire": return retire(f, out, clock);
                case "promote": return promote(f, out, clock, root);
                default: return list(f, out);
            }
        } catch (ReferenceEngine.Refused e) {
            out.println(Canonical.utf8(Canonical.bytes(e.toJson())));
            return 1;
        } catch (FileGenerationStore.ControlLogIo e) {
            ObjectNode r = Json.object().put("refusal", FileGenerationStore.ControlLogIo.CODE);
            Json.putSafe(r, "appended", e.appended).put("detail", e.getMessage());
            out.println(Canonical.utf8(Canonical.bytes(r)));
            return 1;
        } catch (Refusal r) {
            out.println(Canonical.utf8(Canonical.bytes(refusal(r).toJson())));
            return 1;
        } catch (IllegalStateException e) {
            if (!isCorrupt(e)) { err.println(e.getMessage()); return 2; }
            out.println(Canonical.utf8(Canonical.bytes(corrupt(e).toJson())));
            return 1;
        } catch (IllegalArgumentException | UncheckedIOException e) {
            err.println(e.getMessage());
            return 2;
        }
    }

    public static int usage(PrintStream err) {
        err.println("usage: forseti generation new     --state <dir> --mappings <dir> --trusted-keys <file> --stream <s> [--stream <s2>] --rule <id>@<version>");
        err.println("                                  [--recording <dir>] [--definitions <file>]");
        err.println("       forseti generation retire  --state <dir> --generation <n>");
        err.println("       forseti generation promote --state <dir> --mappings <dir> --trusted-keys <file> --generation <n> --registered-row-sha256 <hex>");
        err.println("                                  --run <run.json> --gate-report <report.json> [--shards <dir>] [--accept-scope core]");
        err.println("       forseti generation list    --state <dir>");
        return 2;
    }

    // ------------------------------------------------------------------------------------------------------------------ new

    private static int create(Args f, PrintStream out, Clock clock) {
        Path state = Path.of(f.required("state")), mappings = Path.of(f.required("mappings")), keys = Path.of(f.required("trusted-keys"));
        List<String> streams = f.all("stream");
        if (streams.isEmpty()) throw new IllegalArgumentException("--stream is required");
        if (new HashSet<>(streams).size() != streams.size()) throw new IllegalArgumentException("--stream names a stream twice");
        String rule = f.required("rule");
        int at = rule.lastIndexOf('@');
        if (at <= 0 || at == rule.length() - 1) throw new IllegalArgumentException("--rule is <id>@<version>");
        String ruleId = rule.substring(0, at), ruleVersion = rule.substring(at + 1);
        Path recordingDir = f.get("recording").map(Path::of).orElse(null);
        Recording recording = recordingDir == null ? null : Recording.open(recordingDir);
        JsonNode definitions;
        if (f.has("definitions")) definitions = Json.read(Path.of(f.required("definitions")));
        else if (recording != null) definitions = recording.manifest().path("definitions");
        else definitions = Json.object();
        if (!definitions.isObject()) throw new IllegalArgumentException("definitions is not an object");
        TrustedKeys trusted;
        try { trusted = TrustedKeys.load(keys); } catch (IOException e) { throw new IllegalArgumentException("--trusted-keys " + keys + ": " + e.getMessage(), e); }

        FileGenerationStore store = new FileGenerationStore(state);
        ObjectNode printed = store.locked(() -> {
            long highest = store.highest();   // reads and validates the whole log
            long now = clock.millis();
            FloorStore floors = new FloorStore(state);
            List<HeadVerifier.Head> heads = new ArrayList<>();
            List<ReleaseReader.Release> releases = new ArrayList<>();
            for (String stream : streams) {
                HeadVerifier.Head h = HeadVerifier.verify(mappings, stream, trusted, floors, now);
                heads.add(h);
                releases.add(ReleaseReader.read(mappings, h));
            }
            List<Integer> holders = new ArrayList<>();
            for (int i = 0; i < releases.size(); i++)
                if (releases.get(i).documents().containsKey(new ReleaseReader.DocKey("attribution-rule", ruleId))) holders.add(i);
            if (holders.size() > 1) throw new IllegalArgumentException("--rule " + rule + ": more than one --stream release carries it");
            int holder = holders.isEmpty() ? 0 : holders.get(0);   // none: RuleSet.load refuses provenance-mismatch document-missing
            HeadVerifier.Head head = heads.get(holder);
            RuleSet rules = RuleSet.load(releases.get(holder), ruleId, ruleVersion, definitions);

            ArrayNode inputs = Json.JSON.createArrayNode();
            for (HeadVerifier.Head h : heads) {
                ObjectNode in = inputs.addObject().put("stream", h.stream()).put("releaseId", h.releaseId()).put("version", h.version())
                    .put("releaseSha256", h.releaseSha256());
                Json.putSafe(in, "headVersion", h.headVersion()).put("statementSha256", h.statementSha256());
            }
            ObjectNode replayFrom = Json.object();
            if (recording != null) {
                RoutingManifest routing = recording.routing();
                for (String t : RoutingManifest.TOPICS) {
                    List<Integer> ps = routing.subscribed(t);
                    if (ps.isEmpty()) continue;
                    ObjectNode o = replayFrom.putObject(t);
                    for (int p : ps) o.put(Integer.toString(p), 0);
                }
            }
            ObjectNode evidence = Json.object();
            evidence.set("head", head(head));
            evidence.put("releaseSha256", head.releaseSha256());
            evidence.set("pins", rules.pins());
            long n = GenerationPolicy.next(highest);
            String scope = head.stream();
            store.append(scope, new Generation(n, Generation.REGISTERED, rules.rulesDigest(), inputs, replayFrom, head.headVersion(), evidence,
                null, null, now));
            return printed(n, Generation.REGISTERED, scope, rules.rulesDigest(), store.registeredRowSha256(n).orElseThrow());
        });
        out.println(Canonical.utf8(Canonical.bytes(printed)));
        return 0;
    }

    /** The verified head's summary as {@code releaseEvidence.head}: what a run rebuilds the pinned head from. */
    static ObjectNode head(HeadVerifier.Head h) {
        ObjectNode head = Json.object().put("stream", h.stream()).put("releaseId", h.releaseId()).put("version", h.version()).put("releaseSha256", h.releaseSha256())
            .put("headFile", h.headFile()).put("headSha256", h.headSha256()).put("attestationFile", h.attestationFile()).put("attestationSha256", h.attestationSha256())
            .put("statementSha256", h.statementSha256());
        Json.putSafe(head, "headVersion", h.headVersion());
        return head.put("signedAt", h.signedAt()).put("expires", h.expires()).put("publishedBy", h.publishedBy()).put("approvedBy", h.approvedBy());
    }

    // --------------------------------------------------------------------------------------------------------------- retire

    private static int retire(Args f, PrintStream out, Clock clock) {
        Path state = Path.of(f.required("state"));
        long n = number(f.required("generation"));
        FileGenerationStore store = new FileGenerationStore(state);
        ObjectNode printed = store.locked(() -> {
            Optional<GenerationStore.Entry> reg = store.registered(n);
            if (reg.isEmpty()) throw unregistered(n);
            String latest = store.latestStatus(n).orElseThrow();
            if (!GenerationPolicy.allowed(latest, Generation.RETIRED))
                throw new ReferenceEngine.Refused("generation-transition", Json.putSafe(Json.object(), "generation", n).put("from", latest).put("to", Generation.RETIRED));
            Generation r = reg.get().row();
            store.append(reg.get().scope(), new Generation(n, Generation.RETIRED, r.rulesDigest(), r.inputReleases(), r.replayFrom(), r.headVersion(),
                r.releaseEvidence(), null, null, clock.millis()));
            return printed(n, Generation.RETIRED, reg.get().scope(), r.rulesDigest(), store.registeredRowSha256(n).orElseThrow());
        });
        out.println(Canonical.utf8(Canonical.bytes(printed)));
        return 0;
    }

    // -------------------------------------------------------------------------------------------------------------- promote

    /** Test seams of {@code promote}: {@link #afterParse} runs after the arguments are parsed and before the hold is taken,
     *  {@link #insideHold} first thing inside the hold; {@link #stores} opens the control store (a test substitutes a channel). */
    static volatile Runnable afterParse = () -> {}, insideHold = () -> {};
    static volatile java.util.function.Function<Path, FileGenerationStore> stores = FileGenerationStore::new;

    /** {@code generation promote} (plan Step 21.2; spec "Generations and revisions"). After argument parsing everything happens in one
     *  hold of {@code generations.lock} (generation lock, then the floor lock inside the head verification): (1) the whole history is
     *  re-read and validated; (2) the gate report is checked by {@link GateReportReader} against the store as it is now; (3) for every
     *  stream of {@code inputReleases} the head is verified at the current time against the store's own floor and must still name the
     *  pinned release ({@code generation-release-superseded}); (4) the generation's latest status is {@code registered}
     *  ({@code generation-transition}); (5) {@code replaying}, {@code verified}, the previous current's {@code retired} row when the scope has
     *  one, and {@code current} are appended as one batch, drained and forced ({@link FileGenerationStore#appendBatch}). A refusal of steps
     *  1-4 writes nothing; an I/O failure of step 5 is {@code control-log-io} with the bytes appended, never a success. */
    private static int promote(Args f, PrintStream out, Clock clock, Path root) {
        Path state = Path.of(f.required("state")), mappings = Path.of(f.required("mappings")), keys = Path.of(f.required("trusted-keys"));
        long n = number(f.required("generation"));
        String digest = f.required("registered-row-sha256");
        if (!digest.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("--registered-row-sha256 is 64 lowercase hex");
        Path run = Path.of(f.required("run")), report = Path.of(f.required("gate-report"));
        Path shards = f.get("shards").map(Path::of).orElse(null);
        boolean acceptCore = false;
        if (f.has("accept-scope")) {
            if (!"core".equals(f.required("accept-scope"))) throw new IllegalArgumentException("--accept-scope takes core only (a semantic report needs no acceptance)");
            acceptCore = true;
        }
        TrustedKeys trusted;
        try { trusted = TrustedKeys.load(keys); } catch (IOException e) { throw new IllegalArgumentException("--trusted-keys " + keys + ": " + e.getMessage(), e); }
        GateReportReader.Request request = new GateReportReader.Request(root, report, shards, state, n, digest, run, acceptCore);
        afterParse.run();
        FileGenerationStore store = stores.apply(state);
        ObjectNode printed = store.locked(() -> {
            insideHold.run();
            store.validate();                                                     // 1
            GateReportReader.Accepted accepted = GateReportReader.read(request, store);   // 2
            GenerationStore.Entry reg = store.registered(n).orElseThrow(() -> unregistered(n));
            Generation r = reg.row();
            long now = clock.millis();
            FloorStore floors = new FloorStore(state);
            for (JsonNode pin : r.inputReleases()) {                              // 3
                String stream = Json.text(pin, "stream");
                HeadVerifier.Head h = HeadVerifier.verify(mappings, stream, trusted, floors, now);
                if (!h.releaseSha256().equals(pin.path("releaseSha256").asText(null)) || !h.statementSha256().equals(pin.path("statementSha256").asText(null)))
                    throw new ReferenceEngine.Refused("generation-release-superseded", Json.putSafe(Json.object(), "generation", n).put("stream", stream)
                        .put("pinnedReleaseSha256", pin.path("releaseSha256").asText(null)).put("headReleaseSha256", h.releaseSha256())
                        .put("pinnedStatementSha256", pin.path("statementSha256").asText(null)).put("headStatementSha256", h.statementSha256()));
            }
            String latest = store.latestStatus(n).orElseThrow();                 // 4
            if (!Generation.REGISTERED.equals(latest))
                throw new ReferenceEngine.Refused("generation-transition", Json.putSafe(Json.object(), "generation", n).put("from", latest).put("to", Generation.REPLAYING));
            Optional<GenerationStore.Entry> previous = store.current(reg.scope());
            List<GenerationStore.Entry> batch = GenerationPolicy.promotion(reg, previous, accepted.run(), accepted.gateReportSha256(), now);
            store.appendBatch(batch);                                             // 5: returns only after force
            ObjectNode o = Json.putSafe(Json.object(), "generation", n);
            o.put("status", Generation.CURRENT).put("registeredRowSha256", digest).put("gateReportSha256", accepted.gateReportSha256());
            o.set("run", accepted.run().deepCopy());
            if (previous.isPresent()) Json.putSafe(o, "retired", previous.get().row().generation()); else o.putNull("retired");
            return o;
        });
        out.println(Canonical.utf8(Canonical.bytes(printed)));
        return 0;
    }

    // ----------------------------------------------------------------------------------------------------------------- list

    private static int list(Args f, PrintStream out) {
        FileGenerationStore store = new FileGenerationStore(Path.of(f.required("state")));
        byte[] b = store.locked(store::bytes);
        out.write(b, 0, b.length);
        out.flush();
        return 0;
    }

    // ------------------------------------------------------------------------------------------------------ run preparation

    /** The run preparation of {@code forseti run} (spec "Recovery control", plan Task 20 decision 5), before the engine starts and
     *  inside the hold of {@code generations.lock}: the generation must be registered in a valid history ({@code generation-unregistered}
     *  otherwise); when {@code recovery-control.jsonl} holds no declaration of it, the initial declaration {@code {generation,
     *  injectionId: null, stage: 1, epochBefore: null, targetEpoch: 0, lastWrittenCutId: 0, routingDigest}} is appended with the
     *  recording's {@code routingDigest} and forced; otherwise every declaration of the generation must carry the recording's
     *  {@code routingDigest} ({@code generation-routing-mismatch}) and nothing is written. No generation row is ever written here. */
    public static void prepareRun(Path state, long generation, Path recording) {
        FileGenerationStore store = new FileGenerationStore(state);
        store.locked(() -> {
            if (store.registered(generation).isEmpty()) throw unregistered(generation);
            String routing = Recording.open(recording).routing().routingDigest();
            Path f = state.resolve(ReferenceEngine.RECOVERY_CONTROL);
            List<JsonNode> mine = new ArrayList<>();
            for (JsonNode d : declarations(f))
                if (Json.safeLong(d.get("generation"), "generation", 1, Json.MAX_SAFE) == generation) mine.add(d);
            if (mine.isEmpty()) {
                ObjectNode decl = Json.putSafe(Json.object(), "generation", generation);
                decl.putNull("injectionId").put("stage", 1).putNull("epochBefore").put("targetEpoch", 0).put("lastWrittenCutId", 0)
                    .put("routingDigest", routing);
                byte[] b = Canonical.bytes(decl);
                byte[] withLf = Arrays.copyOf(b, b.length + 1);
                withLf[b.length] = '\n';
                try (FileChannel ch = FileChannel.open(f, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                    ByteBuffer buf = ByteBuffer.wrap(withLf);
                    while (buf.hasRemaining()) ch.write(buf);
                    ch.force(true);
                }
                return null;
            }
            for (JsonNode d : mine) {
                String declared = d.path("routingDigest").textValue();
                if (!routing.equals(declared))
                    throw new ReferenceEngine.Refused("generation-routing-mismatch", Json.putSafe(Json.object(), "generation", generation)
                        .put("declared", declared).put("recording", routing));
            }
            return null;
        });
    }

    /** Every declaration of {@code recovery-control.jsonl}, read strictly: a last line without its LF or a line that is not a JSON
     *  object fails closed ({@code control-log-corrupt}). */
    static List<JsonNode> declarations(Path f) throws IOException {
        if (!Files.exists(f)) return List.of();
        byte[] bytes = Files.readAllBytes(f);
        if (bytes.length > 0 && bytes[bytes.length - 1] != '\n')
            throw new IllegalStateException(GenerationPolicy.CORRUPT + ": " + ReferenceEngine.RECOVERY_CONTROL + ": the last line has no LF");
        List<JsonNode> out = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != '\n') continue;
            JsonNode d;
            try { d = Json.read(Arrays.copyOfRange(bytes, start, i)); } catch (IllegalArgumentException e) { d = null; }
            if (d == null || !d.isObject()) throw new IllegalStateException(GenerationPolicy.CORRUPT + ": " + ReferenceEngine.RECOVERY_CONTROL + " line " + out.size());
            start = i + 1;
            out.add(d);
        }
        return out;
    }

    // -------------------------------------------------------------------------------------------------------------- helpers

    private static ObjectNode printed(long n, String status, String scope, String rulesDigest, String registeredRowSha256) {
        ObjectNode o = Json.putSafe(Json.object(), "generation", n);
        return o.put("status", status).put("scope", scope).put("rulesDigest", rulesDigest).put("registeredRowSha256", registeredRowSha256);
    }

    private static ReferenceEngine.Refused unregistered(long n) {
        return new ReferenceEngine.Refused("generation-unregistered", Json.putSafe(Json.object(), "generation", n));
    }

    private static long number(String text) {
        long v;
        try { v = Long.parseLong(text); } catch (NumberFormatException e) { throw new IllegalArgumentException("--generation is an integer"); }
        if (v < 1 || v > Json.MAX_SAFE) throw new IllegalArgumentException("--generation out of range");
        return v;
    }

    /** A reader refusal as a refusal of this command. */
    public static ReferenceEngine.Refused refusal(Refusal r) {
        ObjectNode ev = Json.object();
        r.evidence.forEach((k, v) -> ev.set(k, Json.JSON.valueToTree(v)));
        return new ReferenceEngine.Refused(r.reason, ev);
    }

    public static boolean isCorrupt(IllegalStateException e) { return e.getMessage() != null && e.getMessage().startsWith(GenerationPolicy.CORRUPT + ":"); }

    /** {@code control-log-corrupt} as a refusal: the log is never repaired or skipped. */
    public static ReferenceEngine.Refused corrupt(IllegalStateException e) {
        return new ReferenceEngine.Refused(GenerationPolicy.CORRUPT, Json.object().put("detail", e.getMessage().substring(GenerationPolicy.CORRUPT.length() + 2)));
    }
}
