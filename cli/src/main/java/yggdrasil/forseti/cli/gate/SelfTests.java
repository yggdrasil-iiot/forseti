package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import yggdrasil.forseti.cli.gate.CoreGate.Check;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.CoreGate.Result;
import yggdrasil.forseti.core.Json;

/** The gate's self-tests (spec 690, proving the gates run; plan Step 22.1): the gate's own accounting run inside the normal gate on a
 *  temporary copy of the manifest, with stand-in assertion bodies that hold, so they need no scenario. {@code G-SELF-OMITTED}: one
 *  manifest id left without an assertion exits 1 and lists it as not run. {@code G-SELF-MISSING-REPORT}: a report directory the gate
 *  cannot write exits 1 and writes the failure report to stderr. */
final class SelfTests {
    private SelfTests() {}

    static List<Check> checks() {
        return List.of(new Check("G-SELF-OMITTED", SelfTests::omitted), new Check("G-SELF-MISSING-REPORT", SelfTests::missingReport));
    }

    /** A gate run of the stand-ins on {@code manifest} with output directory {@code out}: exit, stdout and stderr. */
    record Inner(int exit, String out, String err) {
        String last() { String[] l = out.strip().split("\\R"); return l[l.length - 1].strip(); }
    }

    static Inner inner(Context c, Path manifest, Path out, List<Check> checks) {
        ByteArrayOutputStream so = new ByteArrayOutputStream(), se = new ByteArrayOutputStream();
        int exit = CoreGate.run(new CoreGate.Options(c.root, manifest, out, c.python, null, List.of(), CoreGate.DEFAULT_SEED, "forseti gate --engine core (self-test)"),
            checks, new PrintStream(so, true, StandardCharsets.UTF_8), new PrintStream(se, true, StandardCharsets.UTF_8));
        return new Inner(exit, so.toString(StandardCharsets.UTF_8), se.toString(StandardCharsets.UTF_8));
    }

    /** One stand-in that holds for every id of {@code ids}. */
    static List<Check> holding(List<String> ids) { return ids.stream().map(id -> new Check(id, x -> new Result(true, "stand-in"))).toList(); }

    /** The manifest copied byte for byte into a new directory of the gate's temporary directory. */
    static Path copy(Context c) throws IOException {
        Path m = c.newDir("self-test").resolve("core-assertions.json");
        Files.copy(c.path(CoreGate.MANIFEST), m);
        return m;
    }

    static Result omitted(Context c) throws Exception {
        Path manifest = copy(c);
        List<String> ids = CoreGate.manifestIds(Json.read(manifest));
        String left = ids.get(ids.size() / 2);
        List<String> registered = new ArrayList<>(ids);
        registered.remove(left);
        Path out = c.newDir("self-omitted");
        Inner r = inner(c, manifest, out, holding(registered));
        JsonNode rep = Json.read(out.resolve("report.json"));
        List<String> notRun = new ArrayList<>(), failed = new ArrayList<>();
        rep.path("notRun").forEach(x -> notRun.add(x.asText()));
        rep.path("failed").forEach(x -> failed.add(x.asText()));
        String want = "assertions run " + (ids.size() - 1) + " / expected " + ids.size();
        List<String> p = new ArrayList<>();
        if (r.exit() != 1) p.add("exit " + r.exit() + ", 1 expected");
        if (!notRun.equals(List.of(left))) p.add("notRun " + notRun + ", [" + left + "] expected");
        if (!failed.isEmpty()) p.add("failed " + failed + ", none expected");
        if (!want.equals(r.last())) p.add("last line '" + r.last() + "', '" + want + "' expected");
        if (rep.path("exit").asInt(-1) != 1) p.add("report exit " + rep.path("exit"));
        return new Result(p.isEmpty(), (p.isEmpty() ? "" : "PROBLEMS: " + String.join("; ", p) + " || ") + "a manifest copy with " + left
            + " left without an assertion: exit " + r.exit() + ", notRun " + notRun + ", '" + r.last() + "'");
    }

    static Result missingReport(Context c) throws Exception {
        Path manifest = copy(c);
        List<String> ids = CoreGate.manifestIds(Json.read(manifest));
        Path out = c.newDir("self-missing-report");
        String how = unwritable(out);
        Inner r;
        try { r = inner(c, manifest, out, holding(ids)); }
        finally { writable(out); }
        List<String> p = new ArrayList<>();
        if (r.exit() != 1) p.add("exit " + r.exit() + ", 1 expected");
        if (Files.isRegularFile(out.resolve("report.json"))) p.add("a report.json was written");
        String marker = "cannot write the report";
        int at = r.err().indexOf(marker);
        JsonNode printed = null;
        if (at < 0) p.add("stderr does not say '" + marker + "': " + r.err().strip());
        else {
            for (String line : r.err().substring(at).split("\\R")) if (line.startsWith("{")) { try { printed = Json.read(line.getBytes(StandardCharsets.UTF_8)); } catch (RuntimeException ignored) { /* not the report */ } }
            if (printed == null) p.add("no failure report on stderr");
            else {
                if (printed.path("exit").asInt(-1) != 1) p.add("the failure report on stderr has exit " + printed.path("exit"));
                if (!printed.path("error").asText("").startsWith(marker)) p.add("the failure report on stderr has error " + printed.path("error"));
                if (printed.path("assertionsExpected").asInt(-1) != ids.size()) p.add("the failure report on stderr expects " + printed.path("assertionsExpected"));
            }
        }
        return new Result(p.isEmpty(), (p.isEmpty() ? "" : "PROBLEMS: " + String.join("; ", p) + " || ") + "report directory made unwritable (" + how + "): exit " + r.exit()
            + ", no report.json, the failure report on stderr" + (printed == null ? "" : " (exit " + printed.path("exit") + ", " + printed.path("assertionsExpected") + " expected)"));
    }

    /** Makes {@code dir} a directory the gate cannot write its report into. On a POSIX file system the directory is made read-only (and
     *  checked: a process that can still create a file there, such as root, falls back as below); elsewhere, where a directory's read-only
     *  attribute does not stop a file being created, {@code report.json} is taken by a directory. Returns how. */
    static String unwritable(Path dir) throws IOException {
        if (Files.getFileStore(dir).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-xr-xr-x"));
            try { Files.delete(Files.createFile(dir.resolve("probe"))); }
            catch (IOException e) { return "read-only directory"; }
            Files.setPosixFilePermissions(dir, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
        }
        Files.createDirectories(dir.resolve("report.json"));
        return "report.json taken by a directory";
    }

    static void writable(Path dir) throws IOException {
        if (Files.getFileStore(dir).supportsFileAttributeView("posix"))
            Files.setPosixFilePermissions(dir, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
    }
}
