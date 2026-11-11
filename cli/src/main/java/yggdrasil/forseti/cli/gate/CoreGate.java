package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.Args;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** {@code forseti gate --engine core}: loads the committed assertion manifest {@code gate/core-assertions.json}, runs every registered
 *  assertion, writes {@code <out>/report.json} and prints {@code assertions run N / expected N} as its last line. Exit 0 only when every
 *  manifest id ran and held and no id outside the manifest ran; 1 otherwise, including a crash or a missing artifact (the failure report
 *  is still written); 2 for a usage error, among them a {@code --negative-control} that {@code gate/core-negative-controls.json} does not
 *  list or that this gate does not implement. Paths are relative to the repository root, the directory the gate runs from. */
public final class CoreGate {
    private CoreGate() {}

    public static final long DEFAULT_SEED = 20261109L;
    public static final String MANIFEST = "gate/core-assertions.json", CONTROLS = "gate/core-negative-controls.json";
    static final String JCS_JAVA = "io.github.erdtman:java-json-canonicalization";

    /** One assertion: its manifest id and what it runs. Assertions register in {@link #registered()} (one list). */
    public record Check(String id, Body body) {}
    @FunctionalInterface public interface Body { Result run(Context c) throws Exception; }
    public record Result(boolean ok, String evidence) {}

    /** The negative controls this gate implements, each with its intended failures (PR 6 adds them). */
    static final Map<String, List<String>> IMPLEMENTED_CONTROLS = Map.of();

    public static List<Check> registered() {
        List<Check> all = new ArrayList<>(Foundation.checks());
        all.addAll(Stage1Assertions.checks());
        all.addAll(Stage2Assertions.checks());
        return List.copyOf(all);
    }

    public record Options(Path root, Path manifest, Path out, String python, String negativeControl, List<String> intendedFailures,
                          long seed, String command) {}

    /** What an assertion may use: the repository root, the oracle's interpreter, a private temporary directory, the log directory. */
    public static final class Context {
        final Path root, tmp, logs;
        final String python;
        private int dirs;
        private final Map<String, Object> memo = new HashMap<>();
        Context(Path root, Path tmp, Path logs, String python) { this.root = root; this.tmp = tmp; this.logs = logs; this.python = python; }
        public Path path(String relative) { return root.resolve(relative); }
        public Path fixtures() { return root.resolve("fixtures"); }
        /** A new empty directory under the gate's temporary directory. */
        public Path newDir(String name) throws IOException { return Files.createDirectories(tmp.resolve(name + "-" + dirs++)); }
        /** The value {@code make} computes, made once per gate run and shared by the assertions that ask for {@code key} (a harness run,
         *  say); a computation that throws is not kept, so the next assertion that asks makes it again and reports its own failure. */
        @SuppressWarnings("unchecked")
        public <T> T memo(String key, java.util.concurrent.Callable<T> make) throws Exception {
            if (memo.containsKey(key)) return (T) memo.get(key);
            T v = make.call();
            memo.put(key, v);
            return v;
        }
        /** Runs {@code <python> -I <args...>} from the repository root; output goes to {@code <out>/logs/<log>.log}. */
        public Proc python(String log, String... args) throws IOException, InterruptedException {
            List<String> cmd = new ArrayList<>(List.of(python, "-I"));
            cmd.addAll(List.of(args));
            return Proc.run(root, logs.resolve(log + ".log"), Duration.ofMinutes(10), cmd);
        }
    }

    public record Proc(int exit, String output) {
        static Proc run(Path cwd, Path log, Duration timeout, List<String> cmd) throws IOException, InterruptedException {
            Files.createDirectories(log.getParent());
            ProcessBuilder pb = new ProcessBuilder(cmd).directory(cwd.toFile()).redirectErrorStream(true).redirectOutput(log.toFile());
            pb.redirectInput(ProcessBuilder.Redirect.PIPE);
            Process p = pb.start();
            p.getOutputStream().close();
            if (!p.waitFor(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)) {
                p.destroyForcibly();
                throw new IllegalStateException("timed out after " + timeout + ": " + String.join(" ", cmd));
            }
            return new Proc(p.exitValue(), new String(Files.readAllBytes(log), StandardCharsets.UTF_8));
        }
        public String lastLine() {
            String[] l = output.strip().split("\\R");
            return l.length == 0 ? "" : l[l.length - 1].strip();
        }
    }

    /** The command line: validates the flags (exit 2 on a usage error) and runs the gate from the current directory. */
    public static int main(Args flags, String command, PrintStream out, PrintStream err) {
        String engine, python, control;
        Path outDir;
        long seed;
        List<String> intended = List.of();
        try {
            flags.only(Set.of("engine", "out", "python", "negative-control", "seed"));
            engine = flags.required("engine");
            if (!engine.equals("core")) throw new IllegalArgumentException("--engine " + engine + ": this gate runs --engine core only");
            outDir = Path.of(flags.get("out", "build/gate"));
            python = flags.get("python", "python");
            seed = Long.parseLong(flags.get("seed", Long.toString(DEFAULT_SEED)));
            control = flags.get("negative-control").orElse(null);
            if (control != null) intended = control(Path.of(""), control);
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            err.println("usage: forseti gate --engine core [--out <dir>] [--python <interpreter>] [--negative-control <name>] [--seed <n>]");
            return 2;
        }
        Path root = Path.of("").toAbsolutePath();
        return run(new Options(root, root.resolve(MANIFEST), outDir.toAbsolutePath(), python, control, intended, seed, "forseti " + command),
            registered(), out, err);
    }

    /** The intended failures of a control listed in {@code gate/core-negative-controls.json} and implemented here; a usage error otherwise. */
    static List<String> control(Path root, String name) {
        JsonNode file;
        try { file = Json.read(root.resolve(CONTROLS)); } catch (IllegalArgumentException e) { throw new IllegalArgumentException("cannot read " + CONTROLS + ": " + e.getMessage()); }
        List<String> listed = new ArrayList<>();
        for (JsonNode c : file.path("controls")) listed.add(c.asText());
        if (!listed.contains(name)) throw new IllegalArgumentException("--negative-control " + name + ": not in " + CONTROLS + " " + listed);
        List<String> intended = IMPLEMENTED_CONTROLS.get(name);
        if (intended == null) throw new IllegalArgumentException("--negative-control " + name + ": listed but not implemented by this gate");
        return intended;
    }

    /** Runs {@code checks} against the manifest, writes the report, prints the accounting line last and returns the exit code. */
    public static int run(Options o, List<Check> checks, PrintStream out, PrintStream err) {
        String startedAt = Instant.now().toString();
        Assertions acc = null;
        String manifestSha = null, error = null;
        ObjectNode pins = Json.object(), digests = Json.object();
        Path tmp = null;
        try {
            byte[] manifestBytes = Files.readAllBytes(o.manifest());
            manifestSha = Canonical.digestOf(manifestBytes);
            acc = new Assertions(manifestIds(Json.read(manifestBytes)), out);
            Files.createDirectories(o.out());
            tmp = Files.createTempDirectory("forseti-gate-");
            Context ctx = new Context(o.root(), tmp, o.out().resolve("logs"), o.python());
            pins = pins(ctx);
            digests = fixtureDigests(o.root());
            for (Check c : checks) {
                Result r;
                try { r = c.body().run(ctx); }
                catch (Exception | Error e) { r = new Result(false, "crashed: " + e); }
                try { acc.check(c.id(), r.ok(), r.evidence()); } catch (Assertions.Refused ignored) { /* recorded by the accountant */ }
            }
        } catch (Exception | Error e) {
            error = e.toString();
            err.println("gate crashed: " + error);
        } finally {
            if (tmp != null) delete(tmp);
        }
        int exit = acc == null || error != null ? 1 : acc.exitCode();
        ObjectNode report = Json.object();
        Json.putSafe(report, "exit", exit);
        report.put("scope", "core").put("engine", "core").put("manifestSha256", manifestSha);
        Json.putSafe(report, "assertionsExpected", acc == null ? 0 : acc.expected().size());
        Json.putSafe(report, "assertionsRun", acc == null ? 0 : acc.ran().size());
        strings(report.putArray("failed"), acc == null ? List.of() : acc.failed());
        strings(report.putArray("notRun"), acc == null ? List.of() : acc.notRun());
        strings(report.putArray("intendedFailures"), o.intendedFailures());
        report.set("pins", pins);
        Json.putSafe(report, "seed", o.seed());
        report.set("fixtureDigests", digests);
        report.put("startedAt", startedAt).put("finishedAt", Instant.now().toString()).put("command", o.command());
        report.put("error", error);
        try { Json.write(o.out().resolve("report.json"), report); }
        catch (RuntimeException e) { err.println("cannot write the report: " + e.getMessage()); exit = 1; }
        out.println(acc == null ? "assertions run 0 / expected 0" : acc.line());
        return exit;
    }

    static List<String> manifestIds(JsonNode m) {
        if (!"core".equals(m.path("engine").asText(null)) || !m.path("assertions").isArray()) throw new IllegalArgumentException("not a core assertion manifest");
        List<String> ids = new ArrayList<>();
        for (JsonNode a : m.path("assertions")) {
            Json.text(a, "description");
            if (!a.path("pr").canConvertToInt()) throw new IllegalArgumentException("assertion without pr: " + a);
            ids.add(Json.text(a, "id"));
        }
        return ids;
    }

    private static void strings(ArrayNode a, List<String> l) { l.forEach(a::add); }

    /** {jcs {java, python}, python, jdk, forseti {artifactSha256}}; a value that cannot be found is null (the assertions then fail). */
    static ObjectNode pins(Context ctx) {
        ObjectNode p = Json.object(), jcs = p.putObject("jcs");
        jcs.put("java", JCS_JAVA + ":" + jcsJavaVersion());
        String pyVersion = null, rfc = null;
        try {
            Proc v = ctx.python("pins", "-c", "import sys, importlib.metadata as m; print(sys.version.split()[0]); print(m.version('rfc8785'))");
            String[] lines = v.output().strip().split("\\R");
            if (v.exit() == 0 && lines.length == 2) { pyVersion = lines[0].strip(); rfc = "rfc8785==" + lines[1].strip(); }
        } catch (IOException | InterruptedException | RuntimeException ignored) { /* left null */ }
        jcs.put("python", rfc);
        p.put("python", pyVersion);
        p.put("jdk", System.getProperty("java.vendor") + " " + System.getProperty("java.runtime.version"));
        p.putObject("forseti").put("artifactSha256", artifactSha256());
        return p;
    }

    static String jcsJavaVersion() {
        try (InputStream in = CoreGate.class.getResourceAsStream("/META-INF/maven/io.github.erdtman/java-json-canonicalization/pom.properties")) {
            if (in != null) { Properties pr = new Properties(); pr.load(in); String v = pr.getProperty("version"); if (v != null) return v; }
        } catch (IOException ignored) { /* fall through */ }
        return "unknown";
    }

    /** SHA-256 of the running jar, or {@code unpacked} when the classes are run from a directory (the unit tests). */
    static String artifactSha256() {
        try {
            Path p = Path.of(CoreGate.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            return Files.isRegularFile(p) ? Canonical.digestOf(Files.readAllBytes(p)) : "unpacked";
        } catch (Exception e) { return "unpacked"; }
    }

    /** Each fixture tree's digest: JCS SHA-256 of {relative path: raw-byte SHA-256} over its files, null when the tree is missing. */
    static ObjectNode fixtureDigests(Path root) throws IOException {
        ObjectNode d = Json.object();
        String[][] trees = {{"registry", "fixtures/registry"}, {"registryNext", "fixtures/registry-next"}, {"registryMismatch", "fixtures/registry-mismatch"},
            {"upstream", "fixtures/upstream"}, {"vectors", "vectors"}, {"schemas", "schemas"}, {"scenarios", "fixtures/scenarios"}};
        for (String[] t : trees) d.put(t[0], treeDigest(root.resolve(t[1])));
        return d;
    }

    static String treeDigest(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) return null;
        ObjectNode files = Json.object();
        TreeMap<String, String> sorted = new TreeMap<>();
        try (Stream<Path> s = Files.walk(dir)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) {
                List<String> parts = new ArrayList<>();
                for (Path x : dir.relativize(p)) parts.add(x.toString());
                sorted.put(String.join("/", parts), Canonical.digestOf(Files.readAllBytes(p)));
            }
        }
        sorted.forEach(files::put);
        return Canonical.digest(files);
    }

    static void delete(Path root) {
        try (Stream<Path> w = Files.walk(root)) {
            for (Path p : w.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
        } catch (IOException e) { /* a leftover temporary directory is not a gate result */ }
    }
}
