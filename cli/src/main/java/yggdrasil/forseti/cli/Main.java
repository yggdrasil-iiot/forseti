package yggdrasil.forseti.cli;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.time.Clock;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.gate.CoreGate;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.ReplayRecording;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** {@code forseti <command> <verb> [--flag value | --switch]...}. Exit codes: 0 done, 1 a check failed or a refusal, 2 usage or input
 *  error. Commands are dispatched by name; {@code gate} and {@code run} take their flags from the second argument on
 *  ({@code forseti gate --engine core ...}, {@code forseti run --engine core ...}); {@code generation <verb>} is {@link GenerationCli}'s,
 *  on the system clock. */
public final class Main {
    private Main() {}

    public static void main(String[] args) { System.exit(run(args, System.out, System.err)); }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length >= 1 && args[0].equals("gate")) {
            Args flags;
            try { flags = Args.parse(args, 1, Set.of()); } catch (IllegalArgumentException e) { err.println(e.getMessage()); return usage(err); }
            return CoreGate.main(flags, String.join(" ", args), out, err);
        }
        if (args.length >= 1 && args[0].equals("run")) {
            Args flags;
            try { flags = Args.parse(args, 1, Set.of("shuffle")); } catch (IllegalArgumentException e) { err.println(e.getMessage()); return usage(err); }
            return run(flags, "forseti " + String.join(" ", args), out, err);
        }
        if (args.length >= 1 && args[0].equals("generation")) {
            if (args.length < 2) return usage(err);
            return GenerationCli.run(Arrays.copyOfRange(args, 1, args.length), out, err, Clock.systemUTC());
        }
        if (args.length < 2) return usage(err);
        if (args[0].equals("recordings") && args[1].equals("replay")) {
            Args flags;
            try { flags = Args.parse(args, 2, Set.of()); } catch (IllegalArgumentException e) { err.println(e.getMessage()); return usage(err); }
            return replay(flags, out, err);
        }
        Args flags;
        try { flags = Args.parse(args, 2, Set.of("lock")); }
        catch (IllegalArgumentException e) { err.println(e.getMessage()); return usage(err); }
        try {
            switch (args[0] + " " + args[1]) {
                case "recordings build": return build(flags, out, false);
                case "recordings synth": return build(flags, out, true);
                case "recordings verify": return verify(flags, out, err);
                default: return usage(err);
            }
        } catch (IllegalArgumentException | IllegalStateException | UncheckedIOException e) {
            err.println(e.getMessage());
            return 2;
        }
    }

    private static int usage(PrintStream err) {
        err.println("usage: forseti recordings build --scenario <file> --out <dir> [--registry <dir>] [--lock]");
        err.println("       forseti recordings synth --scenario <synthetic scenario file> --out <dir> --registry <dir> [--lock]");
        err.println("       forseti recordings verify --scenario <file> --out <dir> [--registry <dir>]");
        err.println("       forseti recordings replay --recording <dir> --out-of-orderness-ms <n> --out <dir> [--lock <file>]");
        err.println("       forseti gate --engine core [--out <dir>] [--python <interpreter>] [--negative-control <name>] [--seed <n>]");
        err.println("                    [--shard <name> | --aggregate <dir>] [--state <out>/state]");
        err.println("       forseti run --engine core --recording <dir> --registry <dir> --state <dir> --generation <n> --out <run dir>");
        err.println("                   [--seed <n>] [--shuffle | --replay event-time [--replay-lock <file>]] [--subscribe <topic>=<p,p>[;<topic>=<p,p>]]");
        err.println("                   [--artifact-sha256 <hex>]");
        err.println("       forseti generation new --state <dir> --mappings <dir> --trusted-keys <file> --stream <s> [--stream <s2>] --rule <id>@<version>");
        err.println("                   [--recording <dir>] [--definitions <file>]");
        err.println("       forseti generation retire --state <dir> --generation <n>");
        err.println("       forseti generation list --state <dir>");
        return 2;
    }

    /** {@code forseti run --engine core ...}: the run preparation ({@link GenerationCli#prepareRun}: the generation registered in a valid
     *  history, the initial recovery declaration written once or the declared routing equal to the recording's), then the reference
     *  harness. Prints {@code {runId, generation, cuts, out}} (exit 0), a refusal {@code {"refusal": <code>, ...evidence}} (exit 1; a
     *  corrupt control log is the refusal {@code control-log-corrupt}), or a usage or input error (exit 2). */
    private static int run(Args f, String command, PrintStream out, PrintStream err) {
        ReferenceEngine.Options o;
        try {
            f.only(Set.of("engine", "recording", "registry", "state", "generation", "out", "seed", "shuffle", "subscribe", "artifact-sha256", "replay", "replay-lock"));
            if (!"core".equals(f.required("engine"))) throw new IllegalArgumentException("--engine core is the only engine of this step");
            long generation = number(f.required("generation"), "--generation", 1);
            long seed = number(f.get("seed", Long.toString(ReferenceEngine.DEFAULT_SEED)), "--seed", 0);
            String artifact = f.get("artifact-sha256").orElse(null);
            if (artifact != null && !artifact.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("--artifact-sha256 is 64 lowercase hex digits");
            o = new ReferenceEngine.Options(Path.of(f.required("recording")), Path.of(f.required("registry")), Path.of(f.required("state")), generation,
                Path.of(f.required("out")), seed, f.has("shuffle"), subscribe(f.get("subscribe").orElse(null)), artifact, command,
                f.get("replay").orElse(null), f.get("replay-lock").map(Path::of).orElse(null));
        } catch (IllegalArgumentException e) { err.println(e.getMessage()); return usage(err); }
        try {
            GenerationCli.prepareRun(o.state(), o.generation(), o.recording());
            ReferenceEngine.Result r = ReferenceEngine.run(o);
            ObjectNode n = Json.object().put("runId", r.runId());
            Json.putSafe(n, "generation", o.generation());
            Json.putSafe(n, "cuts", r.cuts());
            out.println(Canonical.utf8(Canonical.bytes(n.put("out", r.out().toString()))));
            return 0;
        } catch (ReferenceEngine.Refused e) {
            out.println(Canonical.utf8(Canonical.bytes(e.toJson())));
            return 1;
        } catch (IllegalStateException e) {
            if (!GenerationCli.isCorrupt(e)) { err.println(e.getMessage()); return 2; }
            out.println(Canonical.utf8(Canonical.bytes(GenerationCli.corrupt(e).toJson())));
            return 1;
        } catch (IllegalArgumentException | UncheckedIOException e) {
            err.println(e.getMessage());
            return 2;
        }
    }

    /** {@code recordings replay}: the event-time replay of {@code --recording} materialized into {@code --out}
     *  ({@link ReplayRecording#materialize}); {@code --lock <file>} writes its lock there. Prints
     *  {@code {cuts, deadlines, manifestSha256, out}} (exit 0), a refusal {@code {"refusal": <code>, ...evidence}} (exit 1), or a usage or
     *  input error (exit 2). */
    private static int replay(Args f, PrintStream out, PrintStream err) {
        Path recording, dir;
        long oo;
        try {
            f.only(Set.of("recording", "out-of-orderness-ms", "out", "lock"));
            recording = Path.of(f.required("recording"));
            dir = Path.of(f.required("out"));
            oo = number(f.required("out-of-orderness-ms"), "--out-of-orderness-ms", 0);
        } catch (IllegalArgumentException e) { err.println(e.getMessage()); return usage(err); }
        try {
            ReplayRecording.Result r = ReplayRecording.materialize(recording, oo, dir);
            if (f.get("lock").isPresent()) Json.write(Path.of(f.get("lock").get()).toAbsolutePath(), r.lock());
            ObjectNode n = Json.putSafe(Json.object(), "cuts", r.cutIds().size());
            Json.putSafe(n, "deadlines", r.manifest().path("deadlines").size());
            out.println(Canonical.utf8(Canonical.bytes(n.put("manifestSha256", r.manifestSha256()).put("out", dir.toString()))));
            return 0;
        } catch (ReplayRecording.Refused e) {
            out.println(Canonical.utf8(Canonical.bytes(e.toJson())));
            return 1;
        } catch (IllegalArgumentException | IllegalStateException | UncheckedIOException e) {
            err.println(e.getMessage());
            return 2;
        }
    }

    private static long number(String text, String flag, long min) {
        long v;
        try { v = Long.parseLong(text); } catch (NumberFormatException e) { throw new IllegalArgumentException(flag + " is an integer"); }
        if (v < min || v > Json.MAX_SAFE) throw new IllegalArgumentException(flag + " out of range");
        return v;
    }

    /** {@code <topic>=<p,p>[;<topic>=<p,p>]}; an empty partition list unsubscribes the topic. */
    static Map<String, List<Integer>> subscribe(String text) {
        Map<String, List<Integer>> m = new LinkedHashMap<>();
        if (text == null) return m;
        for (String entry : text.split(";", -1)) {
            int eq = entry.indexOf('=');
            if (eq <= 0) throw new IllegalArgumentException("--subscribe " + entry + ": <topic>=<p,p>");
            String topic = entry.substring(0, eq), list = entry.substring(eq + 1);
            List<Integer> ps = new ArrayList<>();
            if (!list.isEmpty()) for (String p : list.split(",", -1)) {
                if (!p.matches("0|[1-9][0-9]{0,8}")) throw new IllegalArgumentException("--subscribe " + entry + ": partition " + p);
                ps.add(Integer.parseInt(p));
            }
            if (m.put(topic, ps) != null) throw new IllegalArgumentException("--subscribe names " + topic + " twice");
        }
        return m;
    }

    /** {@code recordings build} builds any scenario (a {@code "synthetic": true} one through the synthesizer); {@code recordings synth}
     *  builds only synthetic ones. */
    private static int build(Args f, PrintStream out, boolean synthOnly) {
        f.only(Set.of("scenario", "out", "registry", "lock"));
        Path scenario = Path.of(f.required("scenario")), dir = Path.of(f.required("out"));
        if (synthOnly && !Json.read(scenario).path("synthetic").asBoolean(false))
            throw new IllegalArgumentException("recordings synth: " + scenario.getFileName() + " is not a synthetic scenario (use recordings build)");
        ScenarioBuilder.Built b = ScenarioBuilder.build(scenario, f.get("registry").map(Path::of).orElse(null), dir);
        if (f.has("lock")) Json.write(ScenarioBuilder.lockOf(scenario).toAbsolutePath(), b.lock());
        out.println(summary(b));
        return 0;
    }

    /** Rebuilds the scenario into a temporary directory, compares it byte for byte with {@code --out}, and compares the rebuilt lock
     *  with the committed one; then opens {@code --out} and verifies its digests and batches. */
    private static int verify(Args f, PrintStream out, PrintStream err) {
        f.only(Set.of("scenario", "out", "registry"));
        Path scenario = Path.of(f.required("scenario")), dir = Path.of(f.required("out"));
        Path registry = f.get("registry").map(Path::of).orElse(null);
        Path tmp;
        try { tmp = Files.createTempDirectory("forseti-verify-"); } catch (IOException e) { throw new UncheckedIOException(e); }
        List<String> problems = new ArrayList<>();
        try {
            Path rebuilt = tmp.resolve("recording");
            ScenarioBuilder.Built b = ScenarioBuilder.build(scenario, registry, rebuilt);
            TreeMap<String, String> want = files(rebuilt), have = files(dir);
            for (String k : want.keySet()) if (!have.containsKey(k)) problems.add("missing " + k);
            for (String k : have.keySet()) if (!want.containsKey(k)) problems.add("unexpected " + k);
            for (String k : want.keySet()) if (have.containsKey(k) && !want.get(k).equals(have.get(k))) problems.add("bytes differ: " + k);
            Path lock = ScenarioBuilder.lockOf(scenario);
            if (!Files.isRegularFile(lock)) problems.add("no lock " + lock.getFileName());
            else if (!Arrays.equals(Files.readAllBytes(lock), Json.pretty(b.lock()))) problems.add("lock differs: " + lock.getFileName());
            if (problems.isEmpty()) {
                Recording r = Recording.open(dir);
                r.verifyDigests();
                r.verifyBatches();
                out.println("verified " + dir.getFileName() + ": " + summary(b));
            }
        } catch (IOException e) { throw new UncheckedIOException(e); }
        finally { delete(tmp); }
        problems.forEach(err::println);
        return problems.isEmpty() ? 0 : 1;
    }

    private static String summary(ScenarioBuilder.Built b) {
        return "messages " + b.messages() + ", samples " + b.samples() + ", sessions " + b.sessions() + ", occupancy " + b.occupancy()
            + ", incidents " + b.incidents() + ", cuts " + b.cuts();
    }

    /** Every file under {@code root} with its raw-byte SHA-256, read through a digest (a synthesized recording is hundreds of MB). */
    private static TreeMap<String, String> files(Path root) throws IOException {
        TreeMap<String, String> m = new TreeMap<>();
        if (!Files.isDirectory(root)) return m;
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) {
                List<String> parts = new ArrayList<>();
                for (Path x : root.relativize(p)) parts.add(x.toString());
                java.security.MessageDigest md;
                try { md = java.security.MessageDigest.getInstance("SHA-256"); } catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
                try (java.io.InputStream in = new java.security.DigestInputStream(Files.newInputStream(p), md)) { in.transferTo(java.io.OutputStream.nullOutputStream()); }
                m.put(String.join("/", parts), HexFormat.of().formatHex(md.digest()));
            }
        }
        return m;
    }

    private static void delete(Path root) {
        try (Stream<Path> w = Files.walk(root)) {
            for (Path p : w.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }
}
