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

/** {@code forseti generation new|retire|list}: the only writer of {@code generations.jsonl} (Decision 11, spec "Generations and
 *  revisions"), and the run preparation of {@code forseti run} ({@link #prepareRun}). Every verb holds {@code generations.lock} for its
 *  whole operation: it reads and validates the whole log, verifies what it verifies, allocates, appends and forces. The head
 *  verification of {@code new} takes the floor lock inside that hold (generation lock, then floor lock, never the other order).
 *  Exit codes: 0 done, 1 a refusal ({@code {"refusal": <code>, ...evidence}} on stdout), 2 a usage or input error. {@code new} and
 *  {@code retire} print one JSON object {@code {generation, status, scope, rulesDigest, registeredRowSha256}}; {@code list} prints the
 *  stored transport records byte for byte. The clock is the caller's ({@link Clock#systemUTC()} from {@code Main}); no flag or
 *  environment variable sets it. */
public final class GenerationCli {
    private GenerationCli() {}

    public static int run(String[] args, PrintStream out, PrintStream err, Clock clock) {
        if (args.length < 1) return usage(err);
        Args f;
        try {
            f = Args.parse(args, 1, Set.of(), Set.of("stream"));
            switch (args[0]) {
                case "new" -> f.only(Set.of("state", "mappings", "trusted-keys", "stream", "rule", "recording", "definitions"));
                case "retire" -> f.only(Set.of("state", "generation"));
                case "list" -> f.only(Set.of("state"));
                default -> { return usage(err); }
            }
        } catch (IllegalArgumentException e) { err.println(e.getMessage()); return usage(err); }
        try {
            switch (args[0]) {
                case "new": return create(f, out, clock);
                case "retire": return retire(f, out, clock);
                default: return list(f, out);
            }
        } catch (ReferenceEngine.Refused e) {
            out.println(Canonical.utf8(Canonical.bytes(e.toJson())));
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
