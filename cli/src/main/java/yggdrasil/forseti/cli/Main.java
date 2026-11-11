package yggdrasil.forseti.cli;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.core.Json;

/** {@code forseti <command> <verb> [--flag value | --switch]...}. Exit codes: 0 done, 1 a check failed, 2 usage or input error.
 *  Commands are dispatched by name, so a later command (Task 9: {@code gate}) is one more case. */
public final class Main {
    private Main() {}

    public static void main(String[] args) { System.exit(run(args, System.out, System.err)); }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length < 2) return usage(err);
        Map<String, String> flags;
        try { flags = flags(Arrays.copyOfRange(args, 2, args.length)); }
        catch (IllegalArgumentException e) { err.println(e.getMessage()); return usage(err); }
        try {
            switch (args[0] + " " + args[1]) {
                case "recordings build": return build(flags, out);
                case "recordings verify": return verify(flags, out, err);
                default: return usage(err);
            }
        } catch (IllegalArgumentException | IllegalStateException | UncheckedIOException e) {
            err.println(e.getMessage());
            return 2;
        }
    }

    private static int usage(PrintStream err) {
        err.println("usage: forseti recordings build --scenario <file> --out <dir> [--lock]");
        err.println("       forseti recordings verify --scenario <file> --out <dir>");
        return 2;
    }

    /** {@code --name value} pairs; {@code --lock} alone is a switch. */
    static Map<String, String> flags(String[] a) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < a.length; i++) {
            if (!a[i].startsWith("--")) throw new IllegalArgumentException("unexpected argument " + a[i]);
            String name = a[i].substring(2);
            if (name.equals("lock")) { m.put(name, "true"); continue; }
            if (i + 1 >= a.length) throw new IllegalArgumentException("--" + name + " needs a value");
            if (m.put(name, a[++i]) != null) throw new IllegalArgumentException("--" + name + " given twice");
        }
        return m;
    }

    private static String required(Map<String, String> f, String name, Set<String> allowed) {
        for (String k : f.keySet()) if (!allowed.contains(k)) throw new IllegalArgumentException("unknown flag --" + k);
        String v = f.get(name);
        if (v == null) throw new IllegalArgumentException("--" + name + " is required");
        return v;
    }

    private static int build(Map<String, String> f, PrintStream out) {
        Set<String> allowed = Set.of("scenario", "out", "lock");
        Path scenario = Path.of(required(f, "scenario", allowed)), dir = Path.of(required(f, "out", allowed));
        ScenarioBuilder.Built b = ScenarioBuilder.build(scenario, dir);
        if (f.containsKey("lock")) Json.write(ScenarioBuilder.lockOf(scenario).toAbsolutePath(), b.lock());
        out.println(summary(b));
        return 0;
    }

    /** Rebuilds the scenario into a temporary directory, compares it byte for byte with {@code --out}, and compares the rebuilt lock
     *  with the committed one; then opens {@code --out} and verifies its digests and batches. */
    private static int verify(Map<String, String> f, PrintStream out, PrintStream err) {
        Set<String> allowed = Set.of("scenario", "out");
        Path scenario = Path.of(required(f, "scenario", allowed)), dir = Path.of(required(f, "out", allowed));
        Path tmp;
        try { tmp = Files.createTempDirectory("forseti-verify-"); } catch (IOException e) { throw new UncheckedIOException(e); }
        List<String> problems = new ArrayList<>();
        try {
            Path rebuilt = tmp.resolve("recording");
            ScenarioBuilder.Built b = ScenarioBuilder.build(scenario, rebuilt);
            TreeMap<String, byte[]> want = files(rebuilt), have = files(dir);
            for (String k : want.keySet()) if (!have.containsKey(k)) problems.add("missing " + k);
            for (String k : have.keySet()) if (!want.containsKey(k)) problems.add("unexpected " + k);
            for (String k : want.keySet()) if (have.containsKey(k) && !Arrays.equals(want.get(k), have.get(k))) problems.add("bytes differ: " + k);
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

    private static TreeMap<String, byte[]> files(Path root) throws IOException {
        TreeMap<String, byte[]> m = new TreeMap<>();
        if (!Files.isDirectory(root)) return m;
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) {
                List<String> parts = new ArrayList<>();
                for (Path x : root.relativize(p)) parts.add(x.toString());
                m.put(String.join("/", parts), Files.readAllBytes(p));
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
