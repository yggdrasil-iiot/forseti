package yggdrasil.forseti.cli.gate;

import java.io.PrintStream;
import java.util.*;

/** The gate's accountant. It knows the manifest's ids and nothing else: {@link #check} records one result per id and refuses an id the
 *  manifest does not list and an id already checked (both fail the gate); {@link #finish} prints {@code assertions run N / expected M}
 *  and returns 1 unless every manifest id ran and held and nothing was refused. */
public final class Assertions {
    public static final class Refused extends IllegalArgumentException {
        Refused(String message) { super(message); }
    }

    private final List<String> expected;
    private final PrintStream out;
    private final Map<String, Boolean> results = new LinkedHashMap<>();
    private final List<String> unknown = new ArrayList<>(), duplicates = new ArrayList<>();

    public Assertions(List<String> manifestIds, PrintStream out) {
        if (new HashSet<>(manifestIds).size() != manifestIds.size()) throw new IllegalArgumentException("the manifest lists an id twice");
        this.expected = List.copyOf(manifestIds);
        this.out = out;
    }

    /** Records whether {@code id} held and prints one line with its evidence. */
    public void check(String id, boolean condition, String evidence) {
        if (!expected.contains(id)) {
            unknown.add(id);
            out.println("UNKNOWN " + id + " (not in the manifest) " + evidence);
            throw new Refused("assertion " + id + " is not in the manifest");
        }
        if (results.containsKey(id)) {
            duplicates.add(id);
            out.println("DUPLICATE " + id + " " + evidence);
            throw new Refused("assertion " + id + " ran twice");
        }
        results.put(id, condition);
        out.println((condition ? "PASS " : "FAIL ") + id + " " + evidence);
    }

    public List<String> expected() { return expected; }
    public List<String> ran() { return List.copyOf(results.keySet()); }
    /** Manifest ids that ran and did not hold, then ids refused as unknown or as duplicates. */
    public List<String> failed() {
        List<String> f = new ArrayList<>();
        results.forEach((id, ok) -> { if (!ok) f.add(id); });
        for (String id : duplicates) if (!f.contains(id)) f.add(id);
        for (String id : unknown) if (!f.contains(id)) f.add(id);
        return f;
    }
    public List<String> notRun() { return expected.stream().filter(id -> !results.containsKey(id)).toList(); }
    public List<String> unknown() { return List.copyOf(unknown); }
    public int exitCode() { return failed().isEmpty() && notRun().isEmpty() && !expected.isEmpty() ? 0 : 1; }
    public String line() { return "assertions run " + results.size() + " / expected " + expected.size(); }

    /** Prints the accounting line (the gate prints it last) and returns the exit code. */
    public int finish() {
        out.println(line());
        return exitCode();
    }
}
