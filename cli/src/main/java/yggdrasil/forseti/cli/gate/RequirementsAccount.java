package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.PrintStream;
import java.util.*;
import yggdrasil.forseti.core.Json;

/**
 * The accounting of {@code forseti requirements} (spec 705, plan D14): every requirement of the engine's requirements manifest is
 * recorded once as {@code pass}, {@code miss} or {@code error}, each with its evidence. A miss is a recorded result, not a failure of
 * the command: the command exits 0 when every expected requirement is recorded, 1 when one is not. Engine eligibility is a separate
 * result: the semantic gate passed and every requirement passed; a miss or an error makes the engine ineligible.
 */
final class RequirementsAccount {
    enum Status {
        PASS, MISS, ERROR;
        String wire() { return name().toLowerCase(Locale.ROOT); }
    }

    record Entry(String id, Status status, String evidence) {}

    /** The engine's eligibility: whether it is eligible, whether the semantic gate passed (and why not), the missed and errored ids. */
    record Eligibility(String engine, boolean eligible, boolean gatePassed, String gate, List<String> missed, List<String> errored) {
        /** The line the command prints, kept apart from the delivery step's completion (plan D14). */
        String line() {
            return "engine eligibility: " + engine + " " + (eligible ? "eligible" : "ineligible") + " (semantic gate " + (gatePassed ? "passed" : "not passed: " + gate)
                + "; requirements missed " + (missed.isEmpty() ? "none" : String.join(", ", missed)) + "; errors " + (errored.isEmpty() ? "none" : String.join(", ", errored))
                + "); eligibility is a separate result, not the completion of the delivery step";
        }

        ObjectNode toJson() {
            ObjectNode o = Json.object().put("engine", engine).put("eligible", eligible).put("semanticGatePassed", gatePassed).put("semanticGate", gate);
            ArrayNode m = o.putArray("missed"), e = o.putArray("errored");
            missed.forEach(m::add);
            errored.forEach(e::add);
            return o;
        }
    }

    private final List<String> expected;
    private final Map<String, Entry> recorded = new LinkedHashMap<>();
    private final PrintStream out;

    RequirementsAccount(List<String> expected, PrintStream out) {
        if (new HashSet<>(expected).size() != expected.size()) throw new IllegalArgumentException("a requirement listed twice: " + expected);
        if (expected.isEmpty()) throw new IllegalArgumentException("no requirement listed");
        this.expected = List.copyOf(expected);
        this.out = out;
    }

    /** Records {@code id} once and prints its line; an id that is not expected or recorded twice is refused. */
    void record(String id, Status status, String evidence) {
        if (!expected.contains(id)) throw new IllegalStateException("requirement " + id + " is not in the manifest");
        if (recorded.containsKey(id)) throw new IllegalStateException("requirement " + id + " recorded twice");
        Objects.requireNonNull(status, "status");
        String e = evidence == null || evidence.isBlank() ? "(no evidence given)" : evidence;
        recorded.put(id, new Entry(id, status, e));
        if (out != null) out.println(status.name() + " " + id + " " + e);
    }

    /** Every expected id not recorded yet, recorded as {@code error} with {@code why} (a crash of the run that would have decided it). */
    void errorUnrecorded(String why) {
        for (String id : expected) if (!recorded.containsKey(id)) record(id, Status.ERROR, why);
    }

    List<String> expected() { return expected; }
    List<Entry> entries() { return List.copyOf(recorded.values()); }
    List<String> missing() { List<String> l = new ArrayList<>(); for (String id : expected) if (!recorded.containsKey(id)) l.add(id); return l; }
    List<String> with(Status s) { List<String> l = new ArrayList<>(); for (Entry e : recorded.values()) if (e.status() == s) l.add(e.id()); return l; }

    /** 0 when every expected requirement is recorded (pass, miss or error), 1 otherwise. */
    int exitCode() { return missing().isEmpty() ? 0 : 1; }

    String line() { return "requirements recorded " + recorded.size() + " / expected " + expected.size(); }

    Eligibility eligibility(String engine, boolean gatePassed, String gate) {
        List<String> missed = with(Status.MISS), errored = with(Status.ERROR);
        boolean eligible = gatePassed && missing().isEmpty() && missed.isEmpty() && errored.isEmpty();
        return new Eligibility(engine, eligible, gatePassed, gatePassed ? null : gate, missed, errored);
    }

    ArrayNode toJson() {
        ArrayNode a = Json.JSON.createArrayNode();
        for (Entry e : recorded.values()) a.addObject().put("id", e.id()).put("status", e.status().wire()).put("evidence", e.evidence());
        for (String id : missing()) a.addObject().put("id", id).putNull("status").put("evidence", "not recorded");
        return a;
    }
}
