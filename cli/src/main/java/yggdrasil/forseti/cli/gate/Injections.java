package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Predicate;
import yggdrasil.forseti.cli.gate.CoreGate.Check;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.CoreGate.Result;
import yggdrasil.forseti.cli.gate.RunFold.Disp;
import yggdrasil.forseti.cli.gate.RunFold.Rev;
import yggdrasil.forseti.cli.gate.Scenarios.Run;
import yggdrasil.forseti.cli.gate.Scenarios.Shuffled;
import yggdrasil.forseti.core.Json;

/** The engine-independent injection assertions of pull request 4 (plan Task 19): {@code I-*} one per injection id, each on the scenario
 *  runs of {@link Scenarios} (built, verified against its lock, run, compared with the oracle at every cut, integrity-checked, ground
 *  truth asserted at the last cut and at the bracketing cuts) plus what the injection asserts beyond that; {@code J-*} the integrity
 *  checks aggregated over every run of the gate; {@code C-6-*} determinism over three shuffled runs; {@code C-2-REFERENCE} item 2's
 *  reference values, recorded in {@code report.json} under {@code measurements}. Times in the evidence are milliseconds after the
 *  scenario epoch T0 unless they are reported W1 values. */
final class Injections {
    private Injections() {}

    static final String S1 = "synthetic-site:line2:station1", S2 = "synthetic-site:line2:station2", N1 = S1 + ":nutrunner1", N2 = S1 + ":nutrunner2";
    static final List<String> STATION1_DEVICES = List.of(S1, N1, N2);
    static final List<String> J = List.of("J-DISPOSITION-COVERAGE", "J-REVISION-CONTINUITY", "J-NO-EQUAL-CONSECUTIVE", "J-PROGRESS-ORDER", "J-W1-FORMULA",
        "J-W2-EQUALS-W1", "J-BASIS-CURRENT");

    static List<Check> checks() {
        List<Check> l = new ArrayList<>(List.of(
            new Check("I-1", Injections::i1), new Check("I-2", Injections::i2), new Check("I-2b", Injections::i2b), new Check("I-3", Injections::i3),
            new Check("I-4", Injections::i4), new Check("I-5", Injections::i5), new Check("I-6", Injections::i6), new Check("I-7", Injections::i7),
            new Check("I-8", Injections::i8), new Check("I-9", Injections::i9), new Check("I-9b", Injections::i9b), new Check("I-10", Injections::i10),
            new Check("I-10b", Injections::i10b), new Check("I-10c", Injections::i10c), new Check("I-10d", Injections::i10d), new Check("I-10e", Injections::i10e),
            new Check("I-10f", Injections::i10f), new Check("I-11", Injections::i11), new Check("I-11b", Injections::i11b), new Check("I-11c", Injections::i11c),
            new Check("I-11d", Injections::i11d), new Check("I-11e", Injections::i11e), new Check("I-12", Injections::i12), new Check("I-13", Injections::i13),
            new Check("I-14", Injections::i14), new Check("I-15", Injections::i15), new Check("I-16", Injections::i16), new Check("I-17", Injections::i17),
            new Check("I-18", Injections::i18), new Check("I-19", Injections::i19), new Check("I-25", Injections::i25), new Check("I-26", Injections::i26)));
        for (String id : J) l.add(new Check(id, c -> journal(c, id)));
        l.add(new Check("J-AFTER-HORIZON-CUTS", Injections::afterHorizon));
        l.add(new Check("C-6-STAGE1", c -> determinism(c, "C-6-STAGE1", List.of("baseline", "stage2"), List.of(RunFold.WR, RunFold.SAMPLES))));
        l.add(new Check("C-6-STAGE2", c -> determinism(c, "C-6-STAGE2", List.of("stage2"), List.of(RunFold.AFFECTED, RunFold.SETS))));
        l.add(new Check("C-2-REFERENCE", Injections::reference));
        return List.copyOf(l);
    }

    // --- helpers -------------------------------------------------------------------------------------------------------------

    /** The problems of one assertion and the runs it rests on; holds when every run is sound and nothing specific failed. */
    static final class P {
        final List<String> list = new ArrayList<>();
        final List<Run> runs = new ArrayList<>();
        P(Run... rs) { for (Run r : rs) use(r); }
        Run use(Run r) { runs.add(r); return r; }
        boolean sound() { for (Run r : runs) if (r.failure != null) return false; return true; }
        void require(boolean ok, String problem) { if (!ok) list.add(problem); }
        <T> void equal(T want, T have, String what) { if (!Objects.equals(want, have)) list.add(what + ": expected " + want + ", got " + have); }
        Result result(String specific) {
            boolean ok = list.isEmpty();
            StringBuilder b = new StringBuilder();
            for (Run r : runs) { ok &= r.ok(); b.append(" || ").append(r.summary()); }
            return new Result(ok, (list.isEmpty() ? "" : "PROBLEMS: " + String.join("; ", list) + " || ") + specific + b);
        }
    }

    static Run run(Context c, String name) throws Exception { return Scenarios.get(c, name); }

    static JsonNode last(Run r, String ref) { Rev x = r.fold.live(RunFold.WR).get(r.key(ref)); return x == null ? null : x.node(); }
    static List<Rev> history(Run r, String ref) { return r.fold.history(RunFold.WR, r.key(ref)); }
    static String status(JsonNode b) { return b == null ? null : b.path("status").asText(null); }
    static List<String> reasons(JsonNode b) { List<String> l = new ArrayList<>(); if (b != null) b.path("insufficiencyReasons").forEach(x -> l.add(x.asText())); return l; }
    static JsonNode evidence(JsonNode b, String path) {
        for (JsonNode e : b.path("equipmentEvidence")) if (e.path("equipmentPath").asText().equals(path)) return e;
        return Json.object();
    }
    static JsonNode member(JsonNode b, String mappingId) {
        for (JsonNode m : b.path("members")) if (m.path("mappingId").asText().equals(mappingId)) return m;
        return Json.object();
    }
    static String ref(JsonNode visit) { return visit.path("visitRef").asText(); }
    static long rel(Run r, long t) { return t - r.t0(); }
    static String setKey(String inc) { return RunFold.keyText(Json.object().put("incidentId", inc).put("source", "qms-line2")); }
    static String unitKey(String inc, int unit) {
        return RunFold.keyText(Json.object().put("incidentId", inc).put("source", "qms-line2").put("unitId", String.format("UNIT%013d", unit)));
    }
    static String unit(int n) { return String.format("UNIT%013d", n); }
    static Map<String, JsonNode> units(JsonNode set) {
        Map<String, JsonNode> m = new TreeMap<>();
        if (set != null) for (JsonNode u : set.path("units")) m.put(u.path("unitId").asText(), u);
        return m;
    }
    static String sr(JsonNode u) { return u == null ? null : u.path("status").asText() + " " + u.path("reason").asText(); }

    /** The sample rows at the last cut whose key and body satisfy {@code p}: key text -> body. */
    static Map<String, JsonNode> samples(Run r, Predicate<JsonNode> key, Predicate<JsonNode> body) {
        Map<String, JsonNode> m = new TreeMap<>();
        for (Map.Entry<String, Rev> e : r.fold.live(RunFold.SAMPLES).entrySet()) {
            JsonNode k = Json.read(e.getKey().getBytes(StandardCharsets.UTF_8));
            if (!key.test(k)) continue;
            JsonNode b = e.getValue().node();
            if (body.test(b)) m.put(e.getKey(), b);
        }
        return m;
    }
    static Predicate<JsonNode> device(String... ds) { Set<String> s = Set.of(ds); return k -> s.contains(k.path("device").asText()); }
    static Predicate<JsonNode> in(Run r, long fromMs, long toMs) { return b -> b.path("sourceTs").asLong() >= r.t0() + fromMs && b.path("sourceTs").asLong() < r.t0() + toMs; }
    static String to(JsonNode b) { return b.path("attributedTo").isObject() ? RunFold.keyText(b.path("attributedTo")) : null; }
    static String sr2(JsonNode b) { return b.path("state").asText() + " " + b.path("reason").asText(); }

    /** The dispositions of occupancy identity {@code eventId}, part {@code part}. */
    static List<Disp> occupancy(Run r, String eventId, String part) {
        List<Disp> l = new ArrayList<>();
        for (Disp d : r.fold.dispositions())
            if (d.topic().equals("mfg.occupancy") && d.part().equals(part) && Json.read(d.identity().getBytes(StandardCharsets.UTF_8)).path("eventId").asText().equals(eventId)) l.add(d);
        l.sort(Comparator.comparingInt(Disp::partition).thenComparingLong(Disp::offset));
        return l;
    }
    static String judged(List<Disp> l) { return Expectations.judgements(l); }

    /** The fingerprints of {@code topic} on {@code partitions} at {@code cut}. */
    static String fp(Map<String, Map<Long, String>> fps, String topic, Collection<Integer> partitions, long cut) {
        StringBuilder b = new StringBuilder();
        for (int p : partitions) b.append(p).append('=').append(fps.getOrDefault(topic + "/" + p, Map.of()).get(cut)).append(' ');
        return b.toString().strip();
    }
    static Set<Integer> partitions(Run r, String topic) {
        Set<Integer> s = new TreeSet<>();
        for (String k : r.fold.fingerprints().keySet()) if (k.startsWith(topic + "/")) s.add(Integer.parseInt(k.substring(topic.length() + 1)));
        return s;
    }
    static long count(String fingerprint) { return fingerprint == null ? -1 : Long.parseLong(fingerprint.substring(fingerprint.indexOf(':') + 1)); }

    /** Direct comparison of {@code topics} on {@code partitions} at cut {@code ca} of {@code a} and {@code cb} of {@code b}. */
    static String direct(P p, Run a, long ca, Run b, long cb, List<String> topics, Collection<Integer> partitions) {
        List<Long> deadlines = new ArrayList<>(a.deadlines());
        deadlines.addAll(b.deadlines());
        String why = Comparison.notComparable(a.side(ca), b.side(cb), deadlines);
        if (why != null) { p.list.add(a.name + " cut " + ca + " vs " + b.name + " cut " + cb + " not comparable: " + why); return "not comparable"; }
        List<String> out = new ArrayList<>();
        for (String t : topics) {
            String fa = fp(a.fold.fingerprints(), t, partitions, ca), fb = fp(b.fold.fingerprints(), t, partitions, cb);
            p.equal(fb, fa, a.name + " cut " + ca + " vs " + b.name + " cut " + cb + " " + t + " fingerprints");
            long n = 0;
            for (int part : partitions) n += Math.max(0, count(a.fold.fingerprints().getOrDefault(t + "/" + part, Map.of()).get(ca)));
            out.add(n + " " + t.replace("forseti.", ""));
        }
        return String.join(", ", out);
    }

    // --- I-* ------------------------------------------------------------------------------------------------------------------

    static Result i1(Context c) throws Exception {
        Run a = run(c, "held-back-10min"), b = run(c, "baseline");
        P p = new P(a, b);
        if (!p.sound()) return p.result("");
        Long ca = a.settledCut(), cb = b.settledCut();
        if (ca == null || cb == null) { p.list.add("no settled cut: held-back " + ca + ", baseline " + cb); return p.result(""); }
        String eq = direct(p, a, ca, b, cb, List.of(RunFold.WR, RunFold.SAMPLES), partitions(a, RunFold.WR));
        String eqLast = direct(p, a, a.lastCut(), b, b.lastCut(), List.of(RunFold.WR, RunFold.SAMPLES), partitions(a, RunFold.WR));
        return p.result("held-back-10min cut " + ca + " = baseline cut " + cb + " (settled, W1 >= settleTargetW " + a.manifest.path("settleTargetW").asLong() + "): " + eq
            + " digests equal; last cuts " + a.lastCut() + "/" + b.lastCut() + ": " + eqLast);
    }

    static Result i2(Context c) throws Exception {
        Run r = run(c, "corrections");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        JsonNode t = r.transition("visit-settlement", "visit-2");
        long a = t.path("cutA").asLong(), b = t.path("cutB").asLong();
        String sa = status(r.fold.at(RunFold.WR, r.key("visit-2"), a)), sb = status(r.fold.at(RunFold.WR, r.key("visit-2"), b));
        p.equal("provisional", sa, "visit-2 at cutA " + a);
        p.equal("complete", sb, "visit-2 at cutB " + b);
        return p.result("visit-2 provisional at cut " + a + " (W1 " + r.w1(a, S1) + ") -> complete at cut " + b + " (W1 " + r.w1(b, S1) + " >= leave + L " + t.path("at").asLong() + ")");
    }

    static Result i2b(Context c) throws Exception {
        Run r = run(c, "corrections");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        String key = r.key("visit-5");
        int moved = 0, attributed = 0;
        for (Map.Entry<String, List<Rev>> e : r.fold.history(RunFold.SAMPLES).entrySet()) {
            List<Rev> h = e.getValue();
            JsonNode lastBody = h.get(h.size() - 1).node();
            if (lastBody == null || !key.equals(to(lastBody))) continue;
            attributed++;
            JsonNode first = h.get(0).node();
            if ("unattributed".equals(first.path("state").asText()) && "no-occupancy".equals(first.path("reason").asText())) moved++;
        }
        p.require(attributed > 0, "no sample row attributed to visit-5");
        p.require(moved > 0, "no visit-5 sample row went unattributed no-occupancy -> attributed");
        JsonNode v = last(r, "visit-5");
        long observed = 0;
        for (JsonNode m : v.path("members")) observed += m.path("observedPeriods").asLong();
        p.require(observed > 0, "visit-5 members[] observe nothing");
        long late = r.manifest.path("source").path("overlays").findValues("deliverAt").stream().filter(x -> x.asLong() == 1440000).count();
        return p.result(moved + " of " + attributed + " visit-5 sample rows unattributed no-occupancy first, attributed at the last cut (arrival delayed to 1440000: " + late
            + " overlay); visit-5 " + status(v) + ", members[] observedPeriods " + observed);
    }

    static Result i3(Context c) throws Exception {
        Run r = run(c, "bad-quality");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        Map<String, JsonNode> bad = samples(r, k -> true, b -> "unattributed".equals(b.path("state").asText()) && "bad-quality".equals(b.path("reason").asText()));
        p.equal(7, bad.size(), "sample rows unattributed bad-quality");
        JsonNode v = last(r, "visit-1"), m = member(v, "nutrunner1-torque");
        p.equal(60L, m.path("observedPeriods").asLong(), "visit-1 members[nutrunner1-torque].observedPeriods");
        p.equal(7L, m.path("badSlots").asLong(), "visit-1 members[nutrunner1-torque].badSlots");
        p.equal(53L, m.path("count").asLong(), "visit-1 members[nutrunner1-torque].count");
        p.equal("insufficient", status(v), "visit-1 status");
        p.require(reasons(v).contains("bad-share-exceeded"), "visit-1 insufficiencyReasons " + reasons(v) + " lack bad-share-exceeded");
        return p.result(bad.size() + " rows unattributed bad-quality; visit-1 nutrunner1-torque observedPeriods " + m.path("observedPeriods") + ", badSlots " + m.path("badSlots")
            + ", count " + m.path("count") + " (badShare 7/60 = 0.117 > 0.1); visit-1 " + status(v) + " " + reasons(v));
    }

    static Result i4(Context c) throws Exception {
        Run r = run(c, "overlap");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        JsonNode v3 = r.visitAt(S1, 960000), extra = r.visitAt(S1, 1000000);
        String k3 = RunFold.keyText(v3.path("key")), kx = RunFold.keyText(extra.path("key"));
        for (JsonNode v : List.of(v3, extra)) {
            JsonNode b = last(r, ref(v));
            p.equal("retracted", status(b), ref(v) + " status");
            p.equal("overlap", b.path("retraction").path("reason").asText(null), ref(v) + " retraction.reason");
        }
        Map<String, JsonNode> s = samples(r, device(STATION1_DEVICES.toArray(String[]::new)), in(r, 960000, 1060000));
        int only3 = 0, onlyX = 0, ambiguous = 0;
        for (Map.Entry<String, JsonNode> e : s.entrySet()) {
            long t = rel(r, e.getValue().path("sourceTs").asLong());
            if (t < 1000000) { only3++; p.equal(k3, to(e.getValue()), "sample at " + t + " attributedTo"); }
            else if (t < 1020000) { ambiguous++; p.equal("unattributed ambiguous", sr2(e.getValue()), "sample at " + t); }
            else { onlyX++; p.equal(kx, to(e.getValue()), "sample at " + t + " attributedTo"); }
        }
        JsonNode a = r.visitAt(S2, 1500000), b = r.visitAt(S2, 1602000);
        for (JsonNode v : List.of(a, b)) {
            JsonNode body = last(r, ref(v));
            p.equal("complete", status(body), ref(v) + " status");
            p.equal(3L, evidence(body, S2).path("coverage").path("unknownActivityPeriods").asLong(-1), ref(v) + " equipmentEvidence[station2].coverage.unknownActivityPeriods");
        }
        Map<String, JsonNode> s2 = samples(r, k -> k.path("device").asText().equals(S2) && k.path("metric").asText().equals("Running"), in(r, 1557000, 1605000));
        long amb2 = s2.values().stream().filter(x -> "unattributed ambiguous".equals(sr2(x))).count();
        p.equal(48, s2.size(), "station2 Running rows in [1557000, 1605000)");
        p.equal(48L, amb2, "of them unattributed ambiguous");
        return p.result(ref(v3) + " [960000, 1020000) and " + ref(extra) + " [1000000, 1060000) retracted overlap; station1 rows: " + only3 + " attributed to " + ref(v3) + ", "
            + ambiguous + " unattributed ambiguous in [1000000, 1020000), " + onlyX + " attributed to " + ref(extra) + "; station2 " + ref(a) + " and " + ref(b)
            + " complete with unknownActivityPeriods 3 each; " + amb2 + " of " + s2.size() + " station2 Running rows in [1557000, 1605000) unattributed ambiguous");
    }

    static Result i5(Context c) throws Exception {
        Run r = run(c, "redelivery"), b = run(c, "baseline");
        P p = new P(r, b);
        if (!p.sound()) return p.result("");
        List<String> dup = new ArrayList<>();
        for (Disp d : r.fold.dispositions()) if ("duplicate".equals(d.disposition()) && "identical-redelivery".equals(d.reason())) dup.add(d.topic() + " " + d.part());
        Collections.sort(dup);
        p.require(dup.stream().anyMatch(x -> x.startsWith("mfg.occupancy")), "no occupancy delivery duplicate identical-redelivery: " + dup);
        p.require(dup.stream().anyMatch(x -> x.startsWith("ot.messages")), "no message delivery duplicate identical-redelivery: " + dup);
        for (String t : List.of(RunFold.WR, RunFold.SAMPLES, RunFold.AFFECTED, RunFold.SETS))
            p.equal(b.tally.keysAtLastCut.get(t), r.tally.keysAtLastCut.get(t), t + " live rows vs baseline");
        return p.result(dup.size() + " deliveries duplicate identical-redelivery " + dup + "; live rows at the last cut " + r.tally.keysAtLastCut + " = baseline's");
    }

    static Result i6(Context c) throws Exception {
        Run r = run(c, "horizon-no-departure"), b = run(c, "baseline");
        P p = new P(r, b);
        if (!p.sound()) return p.result("");
        JsonNode t = r.transition("visit-horizon", "visit-8");
        long a = t.path("cutA").asLong(), bb = t.path("cutB").asLong();
        p.equal(88_260_000L, rel(r, t.path("at").asLong()), "visit-8 enter + H");
        JsonNode atA = r.fold.at(RunFold.WR, r.key("visit-8"), a), atB = r.fold.at(RunFold.WR, r.key("visit-8"), bb);
        p.equal("provisional", status(atA), "visit-8 at cutA " + a);
        p.equal(0L, atA == null ? null : atA.path("coverage").path("missingPeriods").asLong(-1), "visit-8 coverage.missingPeriods at cutA");
        p.equal(List.of(), reasons(atA), "visit-8 insufficiencyReasons at cutA");
        p.equal("insufficient", status(atB), "visit-8 at cutB " + bb);
        p.require(reasons(atB).contains("missing-departure"), "visit-8 insufficiencyReasons at cutB " + reasons(atB));
        int same = 0;
        for (int i = 1; i <= 7; i++) {
            String ref = "visit-" + i;
            Rev x = r.fold.live(RunFold.WR).get(r.key(ref)), y = b.fold.live(RunFold.WR).get(b.key(ref));
            if (x != null && y != null && x.digest().equals(y.digest())) same++;
            else p.list.add(ref + " differs from baseline's");
        }
        JsonNode v9 = r.visitAt(S1, 88_500_000);
        List<Rev> h9 = history(r, ref(v9));
        List<String> st9 = new ArrayList<>();
        for (Rev x : h9) st9.add(x.deleted() ? "deleted" : status(x.node()));
        p.require(!h9.isEmpty() && "provisional".equals(st9.get(0)), ref(v9) + " statuses " + st9 + " do not start provisional");
        p.require(!st9.contains("retracted"), ref(v9) + " retracted: " + st9);
        JsonNode t9 = r.transition("visit-settlement", ref(v9));
        p.equal(89_280_000L, rel(r, t9.path("at").asLong()), ref(v9) + " settlement deadline");
        p.equal("complete", status(r.fold.at(RunFold.WR, r.key(ref(v9)), t9.path("cutB").asLong())), ref(v9) + " at its settlement cutB");
        JsonNode v8 = last(r, "visit-8");
        p.equal("insufficient", status(v8), "visit-8 final status");
        p.require(reasons(v8).contains("missing-departure") && !reasons(v8).contains("overlap"), "visit-8 final reasons " + reasons(v8));
        p.require(v8 != null && (v8.path("retraction").isNull() || v8.path("retraction").isMissingNode()), "visit-8 retraction " + (v8 == null ? null : v8.path("retraction")));
        return p.result("visit-8 provisional at cut " + a + " (W1 " + r.w1(a, S1) + ", missingPeriods 0, reasons []) -> insufficient missing-departure at cut " + bb
            + " (W1 " + r.w1(bb, S1) + " >= enter + H = T0 + 88260000); visits 1-7 digests = baseline's (" + same + "/7); " + ref(v9) + " [88500000, 88560000) " + st9
            + ", complete at its settlement cut " + t9.path("cutB").asLong() + " (T0 + 89280000); visit-8 final " + status(v8) + " " + reasons(v8));
    }

    static Result i7(Context c) throws Exception {
        Run r = run(c, "session");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        Map<String, JsonNode> s = samples(r, device(N1), in(r, 2100000, 2160000));
        long active = s.values().stream().filter(b -> "unattributed no-occupancy".equals(sr2(b)) && b.path("active").asBoolean(false)).count();
        p.equal(120L, active, "nutrunner1 rows in [2100000, 2160000) unattributed no-occupancy active true");
        p.equal(120, s.size(), "nutrunner1 rows in [2100000, 2160000)");
        List<String> complete = new ArrayList<>();
        for (Map.Entry<String, Rev> e : r.fold.live(RunFold.WR).entrySet()) {
            JsonNode b = e.getValue().node();
            long en = b.path("interval").path("enter").asLong(), lv = b.path("interval").path("leave").asLong(Long.MAX_VALUE);
            if ("complete".equals(status(b)) && en < r.t0() + 2160000 && lv > r.t0() + 2100000) complete.add(e.getKey());
        }
        p.equal(List.of(), complete, "complete visits overlapping [2100000, 2160000)");
        return p.result(active + " of " + s.size() + " nutrunner1 rows (60 scans x Torque, Running) in [2100000, 2160000) unattributed no-occupancy active true; no complete visit there");
    }

    static Result i8(Context c) throws Exception {
        Run r = run(c, "session");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        JsonNode v = last(r, "visit-3");
        p.equal("insufficient", status(v), "visit-3 status");
        p.require(reasons(v).contains("session-gap"), "visit-3 reasons " + reasons(v));
        p.equal(1L, evidence(v, N1).path("coverage").path("sessionGaps").asLong(-1), "visit-3 equipmentEvidence[nutrunner1].coverage.sessionGaps");
        return p.result("visit-3 " + status(v) + " " + reasons(v) + ", nutrunner1 sessionGaps " + evidence(v, N1).path("coverage").path("sessionGaps"));
    }

    static Result i9(Context c) throws Exception {
        Run r = run(c, "clock-plus-30"), b = run(c, "baseline");
        P p = new P(r, b);
        if (!p.sound()) return p.result("");
        Map<String, Rev> mine = r.fold.live(RunFold.SAMPLES);
        int inside = 0, same = 0;
        Map<String, long[]> windows = new HashMap<>();
        for (JsonNode v : b.expected.path("visits")) windows.put(RunFold.keyText(v.path("key")), new long[] {v.path("enter").asLong(), v.path("leave").asLong()});
        for (Map.Entry<String, Rev> e : b.fold.live(RunFold.SAMPLES).entrySet()) {
            JsonNode bb = e.getValue().node();
            String t = to(bb);
            if (t == null || !windows.containsKey(t)) continue;
            long ts = bb.path("sourceTs").asLong();
            if (ts < windows.get(t)[0] || ts >= windows.get(t)[1]) continue;
            inside++;
            Rev x = mine.get(e.getKey());
            if (x != null && t.equals(to(x.node()))) same++;
        }
        p.equal(inside, same, "samples inside [enter, leave) attributed to the baseline's visit");
        Map<String, JsonNode> shifted = samples(r, device(STATION1_DEVICES.toArray(String[]::new)), x -> x.path("sourceTs").asLong() == r.t0() + 580000);
        int newly = 0;
        String k1 = r.key("visit-1");
        for (Map.Entry<String, JsonNode> e : shifted.entrySet()) {
            Rev was = b.fold.live(RunFold.SAMPLES).get(e.getKey());
            if (was != null && "unattributed".equals(was.node().path("state").asText()) && k1.equals(to(e.getValue()))) newly++;
        }
        p.require(newly > 0, "no sample at original 550000 (shifted 580000) unattributed in baseline and attributed to visit-1");
        return p.result(same + " of " + inside + " samples inside a baseline visit's [enter, leave) attributed to the same visit after +30 s; " + newly + " of " + shifted.size()
            + " rows at original 550000 (now 580000) unattributed in baseline, attributed to visit-1 [555000, 705000); the full per-sample expectation is expected.json");
    }

    static Result i9b(Context c) throws Exception {
        Run r = run(c, "clock-plus-90");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        Map<String, Integer> bands = new TreeMap<>();
        for (Rev x : r.fold.live(RunFold.SAMPLES).values()) bands.merge(sr2(x.node()), 1, Integer::sum);
        return p.result("per-sample attribution = expected.json (" + r.outcome.samples + " samples): " + bands);
    }

    static Result i10(Context c) throws Exception {
        Run r = run(c, "corrections");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        JsonNode v = last(r, "visit-4");
        p.equal("retracted", status(v), "visit-4 status");
        p.equal("error-declaration", v.path("retraction").path("reason").asText(null), "visit-4 retraction.reason");
        p.require(v.path("retraction").path("errorReason").asText("").endsWith("did_not_occur"), "visit-4 errorReason " + v.path("retraction").path("errorReason"));
        return p.result("visit-4 retracted " + v.path("retraction"));
    }

    static Result i10b(Context c) throws Exception {
        Run a = run(c, "corrections-10b-before"), b = run(c, "corrections-10b-after");
        P p = new P(a, b);
        if (!p.sound()) return p.result("");
        Long ca = a.settledCut(), cb = b.settledCut();
        if (ca == null || cb == null) { p.list.add("no settled cut: " + ca + " / " + cb); return p.result(""); }
        String ka = a.key("visit-8"), kb = b.key("visit-8");
        p.equal(ka, kb, "visit-8 keys");
        JsonNode va = a.fold.at(RunFold.WR, ka, ca), vb = b.fold.at(RunFold.WR, kb, cb);
        String da = va == null ? null : yggdrasil.forseti.core.canon.Canonical.digest(va), db = vb == null ? null : yggdrasil.forseti.core.canon.Canonical.digest(vb);
        p.equal(db, da, "visit-8 settled body digest before vs after");
        for (JsonNode v : Arrays.asList(va, vb)) {
            p.equal("retracted", status(v), "visit-8 status");
            p.equal("error-declaration", v == null ? null : v.path("retraction").path("reason").asText(null), "visit-8 retraction.reason");
            p.require(v != null && v.path("retraction").path("errorReason").asText("").endsWith("did_not_occur"), "visit-8 errorReason");
        }
        String event = Json.read(ka.getBytes(StandardCharsets.UTF_8)).path("arrivalEventId").asText();
        List<Disp> decl = occupancy(a, event, "errorDeclaration"), orig = occupancy(a, event, "event");
        p.equal("[accepted accepted]", judged(decl), "-before " + event + " errorDeclaration");
        p.equal("[accepted accepted]", judged(orig), "-before " + event + " event");
        if (decl.size() == 1 && orig.size() == 1) p.require(decl.get(0).offset() < orig.get(0).offset(), "-before: the declaration (offset " + decl.get(0).offset() + ") is not delivered before the original (" + orig.get(0).offset() + ")");
        return p.result("visit-8 digest " + da + " at settled cuts " + ca + "/" + cb + " in both orders, retracted error-declaration did_not_occur; in -before " + event
            + " errorDeclaration " + judged(decl) + " at offset " + (decl.isEmpty() ? "?" : decl.get(0).offset()) + ", then the original event " + judged(orig) + " at offset "
            + (orig.isEmpty() ? "?" : orig.get(0).offset()));
    }

    static Result i10c(Context c) throws Exception {
        Run r = run(c, "corrections-10c");
        P p = new P(r);
        Run h = null;
        if (Scenarios.skipHorizon()) p.list.add("horizon-reopened not run (-Dforseti.gate.skipHorizon=true)");
        else h = p.use(run(c, "horizon-reopened"));
        if (!p.sound()) return p.result("");
        String k = r.key("visit-8");
        Long cut = null;
        for (long x : r.cutIds()) if (r.w1(x, S1) == r.t0() + 2_640_999) cut = x;
        String atCut = null;
        if (cut == null) p.list.add("no cut with station1 W1 = T0 + 2640999");
        else { atCut = status(r.fold.at(RunFold.WR, k, cut)); p.equal("complete", atCut, "visit-8 at cut " + cut); }
        String event = null;
        for (JsonNode o : r.manifest.path("source").path("overlays")) if ("error-declaration".equals(o.path("type").asText())) event = o.path("eventId").asText();
        List<Disp> decl = occupancy(r, event, "errorDeclaration");
        p.equal("[accepted late-after-settlement]", judged(decl), event + " errorDeclaration");
        JsonNode v = last(r, "visit-8");
        p.equal("provisional", status(v), "visit-8 final status");
        p.equal(List.of(), reasons(v), "visit-8 final insufficiencyReasons");
        String reopened = "";
        if (h != null) {
            JsonNode t = h.transition("visit-horizon", "visit-8");
            JsonNode hv = last(h, "visit-8");
            p.equal("insufficient", status(hv), "horizon-reopened visit-8 status");
            p.require(reasons(hv).contains("missing-departure"), "horizon-reopened visit-8 reasons " + reasons(hv));
            p.equal("insufficient", status(h.fold.at(RunFold.WR, h.key("visit-8"), t.path("cutB").asLong())), "horizon-reopened visit-8 at cutB");
            reopened = "; horizon-reopened visit-8 insufficient " + reasons(hv) + " from cut " + t.path("cutB").asLong() + " (enter + H = T0 + " + rel(h, t.path("at").asLong())
                + "), its samples' attributedTo per expected.json";
        }
        return p.result("visit-8 complete at cut " + cut + " (station1 W1 T0 + 2640999); " + event + " errorDeclaration " + judged(decl) + "; visit-8 then " + status(v)
            + " reasons " + reasons(v) + reopened);
    }

    static Result i10d(Context c) throws Exception {
        Run r = run(c, "corrections");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        List<Disp> d = occupancy(r, "occ-1", "event");
        p.require(d.size() == 2 && d.stream().allMatch(x -> "conflict".equals(x.disposition()) && "identity-conflict".equals(x.reason())), "occ-1 deliveries " + judged(d));
        JsonNode v = last(r, "visit-1");
        p.equal("retracted", status(v), "visit-1 status");
        p.equal("input-conflict", v.path("retraction").path("reason").asText(null), "visit-1 retraction.reason");
        return p.result("occ-1 deliveries " + judged(d) + "; visit-1 retracted " + v.path("retraction").path("reason").asText());
    }

    static Result i10e(Context c) throws Exception {
        Run r = run(c, "horizon-correction-beyond");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        List<Disp> decl = new ArrayList<>();
        for (Disp d : r.fold.dispositions()) if (d.topic().equals("mfg.occupancy") && d.part().equals("errorDeclaration")) decl.add(d);
        p.equal("[replay-required correction-beyond-horizon]", judged(decl), "the error declaration");
        Long before = null;
        if (decl.size() == 1) {
            Disp d = decl.get(0);
            long prev = 0;
            for (JsonNode cut : r.cutLog.path("cuts")) {
                JsonNode rg = cut.path("offsetRanges").path("mfg.occupancy").path(Integer.toString(d.partition()));
                if (rg.isArray() && d.offset() >= rg.get(0).asLong() && d.offset() < rg.get(1).asLong()) { before = prev; break; }
                prev = cut.path("cutId").asLong();
            }
        }
        JsonNode t = r.transition("visit-online-expiry", "visit-2");
        long w = before == null ? 0 : r.w1(before, S1);
        if (before == null) p.list.add("the declaration's cut not found");
        else {
            p.equal(r.t0() + 87_960_999, w, "W1 at the cut before the declaration's batch");
            p.require(w >= t.path("at").asLong(), "W1 " + w + " below onlineExpiresAt " + t.path("at").asLong());
        }
        JsonNode v = last(r, "visit-2");
        p.equal("complete", status(v), "visit-2 status");
        List<Rev> h = history(r, "visit-2");
        if (before != null) for (Rev x : h) p.require(x.cut() <= before, "visit-2 revision " + x.revision() + " at cut " + x.cut() + " after the declaration");
        return p.result("cut " + before + " before the declaration's batch: W1 " + w + " (T0 + " + (w - r.t0()) + ") >= onlineExpiresAt " + t.path("at").asLong()
            + "; the declaration " + judged(decl) + "; visit-2 " + status(v) + ", unchanged after it (" + h.size() + " revisions)");
    }

    static Result i10f(Context c) throws Exception {
        Run r = run(c, "corrections");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        JsonNode lc = null;
        for (JsonNode o : r.manifest.path("source").path("overlays")) if ("late-correction".equals(o.path("type").asText())) lc = o;
        if (lc == null) { p.list.add("no late-correction overlay"); return p.result(""); }
        List<Disp> decl = occupancy(r, lc.path("departureEventId").asText(), "errorDeclaration"), dep = occupancy(r, lc.path("newDepartureEventId").asText(), "event");
        p.equal("[accepted late-after-settlement]", judged(decl), lc.path("departureEventId").asText() + " errorDeclaration");
        p.equal("[accepted late-after-settlement]", judged(dep), lc.path("newDepartureEventId").asText() + " event");
        List<String> seq = new ArrayList<>();
        long finalCut = -1;
        for (Rev x : history(r, "visit-6")) {
            JsonNode b = x.node();
            String s = status(b) + (reasons(b).isEmpty() ? "" : " " + reasons(b)) + "@" + x.cut();
            if (seq.isEmpty() || !seq.get(seq.size() - 1).replaceAll("@.*", "").equals(s.replaceAll("@.*", ""))) seq.add(s);
            finalCut = x.cut();
        }
        List<String> plain = new ArrayList<>();
        for (String x : seq) plain.add(x.replaceAll(" \\[.*", "").replaceAll("@.*", ""));
        int i = plain.indexOf("complete");
        p.require(i >= 0 && plain.subList(i, plain.size()).equals(List.of("complete", "provisional", "complete")), "visit-6 statuses " + seq + ", expected complete -> provisional -> complete");
        JsonNode v = last(r, "visit-6");
        long L = r.rules.attribution().allowedLatenessMs();
        p.equal(r.t0() + 1_565_000, v.path("interval").path("leave").asLong(), "visit-6 leave");
        p.require(r.w1(finalCut, S1) >= r.t0() + 1_565_000 + L, "the final complete at cut " + finalCut + " has W1 " + r.w1(finalCut, S1) + " < leave + L");
        return p.result("declaration " + judged(decl) + ", corrective departure " + judged(dep) + "; visit-6 " + seq + ", leave T0 + 1565000, final complete at cut " + finalCut
            + " (W1 " + r.w1(finalCut, S1) + " >= leave + L " + (r.t0() + 1_565_000 + L) + ")");
    }

    static Result i11(Context c) throws Exception {
        Run r = run(c, "plc-stamped");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        String k = r.key("V3");
        Map<String, JsonNode> s = samples(r, x -> true, b -> k.equals(to(b)));
        long mismatch = s.values().stream().filter(b -> "attributed".equals(b.path("state").asText()) && "identity-mismatch".equals(b.path("reason").asText())).count();
        p.require(mismatch > 0, "no V3 sample attributed with reason identity-mismatch");
        return p.result(mismatch + " of " + s.size() + " V3 sample rows attributed with reason identity-mismatch, attributedTo V3 (unchanged)");
    }

    static Result i11b(Context c) throws Exception {
        Run a = run(c, "plc-held-back"), b = run(c, "plc-nocheck");
        P p = new P(a, b);
        if (!p.sound()) return p.result("");
        p.equal("line2-plc-nocheck", a.manifest.path("rule").path("stream").asText(), "plc-held-back stream");
        p.equal("line2-plc-nocheck", b.manifest.path("rule").path("stream").asText(), "plc-nocheck stream");
        Long ca = a.settledCut(), cb = b.settledCut();
        if (ca == null || cb == null) { p.list.add("no settled cut"); return p.result(""); }
        String eq = direct(p, a, ca, b, cb, List.of(RunFold.WR, RunFold.SAMPLES), partitions(a, RunFold.WR));
        long complete = a.fold.live(RunFold.WR).values().stream().filter(x -> "complete".equals(status(x.node()))).count();
        p.require(complete > 0, "no complete visit");
        return p.result("plc-held-back cut " + ca + " = plc-nocheck cut " + cb + " (settled, stream line2-plc-nocheck, occupancy not subscribed: " + a.runJson.path("subscribed").path("mfg.occupancy")
            + "): " + eq + " digests equal; " + complete + " complete visits");
    }

    static Result i11c(Context c) throws Exception {
        Run r = run(c, "plc-stamped");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        long n = samples(r, x -> true, b -> "incoherent-snapshot".equals(b.path("reason").asText())).size();
        long[] want = {0};
        yggdrasil.forseti.cli.recording.ExpectedSamples.forEachSample(r.expected, (id, e) -> { if ("incoherent-snapshot".equals(e.path("reason").asText())) want[0]++; });
        p.require(n > 0, "no sample row incoherent-snapshot");
        p.equal(want[0], n, "incoherent-snapshot rows vs expected.json");
        return p.result(n + " sample rows incoherent-snapshot (the mixed snapshot), as expected.json names");
    }

    static Result i11d(Context c) throws Exception {
        Run r = run(c, "plc-stamped");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        JsonNode v = last(r, "V7");
        p.equal("insufficient", status(v), "V7 status");
        p.require(reasons(v).contains("missing-visit-close"), "V7 reasons " + reasons(v));
        return p.result("V7 " + status(v) + " " + reasons(v));
    }

    static Result i11e(Context c) throws Exception {
        Run r = run(c, "plc-horizon-no-end");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        JsonNode t = r.transition("visit-horizon", "V3");
        long H = r.rules.attribution().horizonMs();
        p.equal(r.visit("V3").path("enter").asLong() + H, t.path("at").asLong(), "V3 enter + H");
        JsonNode a = r.fold.at(RunFold.WR, r.key("V3"), t.path("cutA").asLong()), b = r.fold.at(RunFold.WR, r.key("V3"), t.path("cutB").asLong());
        p.equal("provisional", status(a), "V3 at cutA");
        p.equal("insufficient", status(b), "V3 at cutB");
        p.require(reasons(b).contains("missing-departure"), "V3 reasons at cutB " + reasons(b));
        return p.result("V3 provisional at cut " + t.path("cutA") + " (W1 " + r.w1(t.path("cutA").asLong(), S1) + ") -> insufficient " + reasons(b) + " at cut " + t.path("cutB")
            + " (W1 " + r.w1(t.path("cutB").asLong(), S1) + " >= enter + H " + t.path("at") + ")");
    }

    static JsonNode set(Run r, String inc) { Rev x = r.fold.live(RunFold.SETS).get(setKey(inc)); return x == null ? null : x.node(); }

    static Result i12(Context c) throws Exception {
        Run r = run(c, "stage2");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        JsonNode s = set(r, "inc-1");
        Map<String, JsonNode> u = units(s);
        for (int n : List.of(1, 3, 5)) p.equal("affected-candidate equipment-activity", sr(u.get(unit(n))), "inc-1 unit " + n);
        p.require(s != null && s.path("coverage").path("settled").asBoolean(false), "inc-1 set not settled");
        return p.result("inc-1 units 1, 3, 5 affected-candidate equipment-activity; set settled " + (s == null ? null : s.path("coverage").path("settled")) + " at cut " + r.lastCut());
    }

    static Result i13(Context c) throws Exception {
        Run r = run(c, "stage2-early");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        List<Rev> h = r.fold.history(RunFold.SETS, setKey("inc-1"));
        if (h.isEmpty()) { p.list.add("no inc-1 set"); return p.result(""); }
        JsonNode first = h.get(0).node();
        p.equal(0, first.path("units").size(), "first set row units");
        p.equal(false, first.path("coverage").path("settled").asBoolean(true), "first set row settled");
        p.equal("unknown", first.path("coverage").path("status").asText(), "first set row coverage.status");
        p.equal(990L, first.path("coverage").path("unknownActivityPeriods").asLong(), "first set row unknownActivityPeriods");
        long prevUnknown = Long.MAX_VALUE;
        int prevUnits = 0;
        Long settledAt = null;
        List<Long> unknowns = new ArrayList<>();
        for (Rev x : h) {
            JsonNode b = x.node(), cov = b.path("coverage");
            long unk = cov.path("unknownActivityPeriods").asLong();
            unknowns.add(unk);
            p.require(unk <= prevUnknown, "unknownActivityPeriods rose to " + unk + " at cut " + x.cut());
            if (unk > 0) p.equal("unknown", cov.path("status").asText(), "status at cut " + x.cut() + " with unknownActivityPeriods " + unk);
            int n = b.path("units").size();
            p.require(n >= prevUnits && n <= prevUnits + 1, "units " + prevUnits + " -> " + n + " at cut " + x.cut());
            if (settledAt == null && cov.path("settled").asBoolean(false)) settledAt = x.cut();
            prevUnknown = unk; prevUnits = n;
        }
        long target = r.t0() + 2_310_000;
        if (settledAt == null) p.list.add("never settled");
        else {
            p.require(r.w1(settledAt, S1) >= target, "settled at cut " + settledAt + " with W2 " + r.w1(settledAt, S1) + " < " + target);
            int i = r.cutIds().indexOf(settledAt);
            p.require(i > 0 && r.w1(r.cutIds().get(i - 1), S1) < target, "W2 already >= " + target + " before the settling cut " + settledAt);
        }
        JsonNode last = h.get(h.size() - 1).node();
        p.equal("complete", last.path("coverage").path("status").asText(), "status at the last cut");
        p.equal(0L, last.path("coverage").path("unattributedActivityPeriods").asLong(-1), "unattributedActivityPeriods at the last cut");
        return p.result(h.size() + " set revisions: first at cut " + h.get(0).cut() + " units [] settled false unknown 990; unknownActivityPeriods " + compact(unknowns) + "; units grow to "
            + prevUnits + " one at a time; settled at cut " + settledAt + " (W2 " + (settledAt == null ? null : r.w1(settledAt, S1)) + " >= T0 + 2310000); last " + last.path("coverage").path("status").asText());
    }

    static String compact(List<Long> l) {
        List<Long> d = new ArrayList<>();
        for (Long x : l) if (d.isEmpty() || !d.get(d.size() - 1).equals(x)) d.add(x);
        return d.toString();
    }

    static Result i14(Context c) throws Exception {
        Run r = run(c, "stage2");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        List<Rev> h = r.fold.history(RunFold.SETS, setKey("inc-2"));
        JsonNode open = null;
        for (Rev x : h) { JsonNode b = x.node(); if (b.path("coverage").path("open").asBoolean(false) && b.path("incident").path("revision").asLong() == 1) open = b; }
        Map<Integer, String> tools = Map.of(3, "T1", 4, "none", 5, "T1", 6, "T2", 7, "T1", 8, "none");
        if (open == null) p.list.add("no open revision-1 set row of inc-2");
        else for (Map.Entry<String, JsonNode> u : units(open).entrySet()) {
            int n = Integer.parseInt(u.getKey().substring(4));
            String got = sr(u.getValue()), want = "T1".equals(tools.get(n)) ? "affected-candidate equipment-activity" : "not-affected-within-scope no-equipment-activity";
            if (!got.equals("insufficient-evidence basis-provisional")) p.equal(want, got, "inc-2 open unit " + n);
        }
        JsonNode s = set(r, "inc-2");
        Map<String, JsonNode> u = units(s);
        for (int n : List.of(6, 7, 8)) p.equal("not-affected-within-scope interval-narrowed", sr(u.get(unit(n))), "inc-2 unit " + n + " after revision 2");
        for (int n : List.of(3, 5)) p.equal("affected-candidate equipment-activity", sr(u.get(unit(n))), "inc-2 unit " + n + " after revision 2");
        p.equal(false, s == null ? null : s.path("coverage").path("open").asBoolean(true), "inc-2 open");
        Map<String, String> openUnits = new TreeMap<>();
        if (open != null) units(open).forEach((k, v) -> openUnits.put(k.substring(15), sr(v)));
        Map<String, String> finalUnits = new TreeMap<>();
        u.forEach((k, v) -> finalUnits.put(k.substring(15), sr(v)));
        return p.result("inc-2 while open (revision 1): " + openUnits + "; after revision 2: " + finalUnits + ", open " + (s == null ? null : s.path("coverage").path("open")));
    }

    static Result i15(Context c) throws Exception {
        Run r = run(c, "stage2");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        JsonNode s = set(r, "inc-3");
        Map<String, JsonNode> u = units(s);
        p.require(!u.isEmpty(), "inc-3 has no units");
        for (Map.Entry<String, JsonNode> e : u.entrySet()) p.equal("not-affected-within-scope incident-withdrawn", sr(e.getValue()), "inc-3 " + e.getKey());
        p.equal("withdrawn", s == null ? null : s.path("resolution").asText(null), "inc-3 resolution");
        p.equal(false, s == null ? null : s.path("coverage").path("open").asBoolean(true), "inc-3 open");
        p.equal(true, s == null ? null : s.path("coverage").path("settled").asBoolean(false), "inc-3 settled");
        List<String> st = new ArrayList<>();
        for (Rev x : r.fold.history(RunFold.SETS, setKey("inc-3"))) {
            JsonNode b = x.node();
            st.add(b.path("coverage").path("status").asText() + (b.path("resolution").isNull() ? "" : " " + b.path("resolution").asText()) + "@" + x.cut());
            p.require(!"complete".equals(b.path("coverage").path("status").asText()), "inc-3 coverage complete at cut " + x.cut());
        }
        return p.result(u.size() + " inc-3 units not-affected-within-scope incident-withdrawn; resolution withdrawn, open false, settled true; coverage per set revision " + st);
    }

    static Result i16(Context c) throws Exception {
        Run r = run(c, "stage2");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        Map<String, JsonNode> u = units(set(r, "inc-1"));
        p.equal("not-affected-within-scope no-equipment-activity", sr(u.get(unit(2))), "inc-1 unit 2 (T2 only)");
        p.equal("affected-candidate equipment-activity", sr(u.get(unit(3))), "inc-1 unit 3 (both tools)");
        p.equal("not-affected-within-scope no-equipment-activity", sr(u.get(unit(4))), "inc-1 unit 4 (no tool)");
        return p.result("inc-1 (nutrunner1): unit 2 " + sr(u.get(unit(2))) + ", unit 3 " + sr(u.get(unit(3))) + ", unit 4 " + sr(u.get(unit(4))));
    }

    static Result i17(Context c) throws Exception {
        Run r = run(c, "stage2");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        JsonNode s = set(r, "inc-1");
        p.equal(28L, s == null ? null : s.path("coverage").path("unattributedActivityPeriods").asLong(), "inc-1 unattributedActivityPeriods");
        p.equal("incomplete", s == null ? null : s.path("coverage").path("status").asText(), "inc-1 coverage.status");
        Map<String, JsonNode> rows = samples(r, k -> k.path("device").asText().equals(N1), in(r, 1246000, 1274000));
        long n = rows.values().stream().filter(b -> "unattributed no-occupancy".equals(sr2(b)) && b.path("active").asBoolean(false)).count();
        p.equal(56L, n, "nutrunner1 rows in [1246000, 1274000) unattributed no-occupancy active true");
        return p.result("inc-1 coverage unattributedActivityPeriods 28, status incomplete; " + n + " of " + rows.size() + " nutrunner1 rows (28 scans x Torque, Running) unattributed no-occupancy active true");
    }

    static Result i18(Context c) throws Exception {
        Run r = run(c, "cascade");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        List<Rev> a = r.fold.history(RunFold.AFFECTED, unitKey("inc-1", 1));
        List<String> seq = new ArrayList<>();
        Long cut = null;
        for (Rev x : a) {
            if (x.deleted()) { seq.add("deleted"); continue; }
            String s = sr(x.node());
            seq.add(s + "@" + x.cut());
            if (s.equals("not-affected-within-scope basis-retracted-error-declaration") && cut == null) cut = x.cut();
        }
        p.require(!a.isEmpty() && sr(a.get(0).node()).equals("affected-candidate equipment-activity") || seq.stream().anyMatch(x -> x.startsWith("affected-candidate")), "unit 1 never affected-candidate: " + seq);
        Long retracted = null;
        for (Rev x : history(r, "visit-1")) if (retracted == null && "retracted".equals(status(x.node()))) retracted = x.cut();
        p.require(cut != null, "unit 1 never basis-retracted-error-declaration: " + seq);
        p.equal(retracted, cut, "the cut of the retraction vs the cut of the unit's change");
        return p.result("inc-1 unit 1 " + seq + "; visit-1 retracted at cut " + retracted);
    }

    static Result i19(Context c) throws Exception {
        Run r = run(c, "cascade");
        P p = new P(r);
        if (!p.sound()) return p.result("");
        List<Rev> sets = r.fold.history(RunFold.SETS, setKey("inc-2"));
        if (sets.isEmpty()) { p.list.add("no inc-2 set"); return p.result(""); }
        p.require(!units(sets.get(0).node()).containsKey(unit(5)), "the first inc-2 set row already has unit 5");
        List<Rev> u5 = r.fold.history(RunFold.AFFECTED, unitKey("inc-2", 5));
        long target = r.t0() + 2_100_000;
        Long c1 = null, c2 = null;
        for (Rev x : u5) {
            if (x.deleted()) continue;
            String s = sr(x.node());
            if (c1 == null && s.equals("insufficient-evidence basis-provisional")) c1 = x.cut();
            if (c2 == null && s.equals("affected-candidate equipment-activity")) c2 = x.cut();
        }
        Set<Long> setCuts = new TreeSet<>();
        for (Rev x : sets) setCuts.add(x.cut());
        if (c1 == null || c2 == null) p.list.add("unit 5 revisions " + u5.stream().map(x -> x.deleted() ? "deleted" : sr(x.node()) + "@" + x.cut()).toList());
        else {
            p.require(r.w1(c1, S1) < target, "unit 5 enters at cut " + c1 + " with W1 " + r.w1(c1, S1) + " >= " + target);
            p.equal("provisional", status(r.fold.at(RunFold.WR, r.key("visit-5"), c1)), "visit-5 at cut " + c1);
            int i = r.cutIds().indexOf(c2);
            p.require(r.w1(c2, S1) >= target && i > 0 && r.w1(r.cutIds().get(i - 1), S1) < target, "unit 5 affected at cut " + c2 + " not the bracketing cut of " + target);
            p.require(setCuts.contains(c1) && setCuts.contains(c2), "no inc-2 set revision at cut " + c1 + " / " + c2 + " " + setCuts);
            p.require(c1 < c2, "cut order " + c1 + " / " + c2);
        }
        return p.result("inc-2 first set row without unit 5 (cut " + sets.get(0).cut() + "); unit 5 insufficient-evidence basis-provisional at cut " + c1 + " (W1 "
            + (c1 == null ? null : r.w1(c1, S1)) + " < T0 + 2100000, visit-5 provisional), affected-candidate equipment-activity at cut " + c2 + " (W1 " + (c2 == null ? null : r.w1(c2, S1))
            + "); set revisions at cuts " + setCuts);
    }

    static Result i25(Context c) throws Exception {
        Run r = run(c, "session"), b = run(c, "baseline");
        P p = new P(r, b);
        if (!p.sound()) return p.result("");
        String k2 = RunFold.keyText(r.visitAt(S2, 870000).path("key"));
        Rev x = r.fold.live(RunFold.WR).get(k2), y = b.fold.live(RunFold.WR).get(k2);
        p.require(x != null && y != null && x.digest().equals(y.digest()), "station2 visit 2 differs from baseline's");
        JsonNode v6 = last(r, "visit-6");
        p.equal("insufficient", status(v6), "visit-6 status");
        p.require(reasons(v6).contains("session-ambiguous"), "visit-6 reasons " + reasons(v6));
        p.equal("unknown", evidence(v6, N2).path("coverage").path("status").asText(null), "visit-6 equipmentEvidence[nutrunner2].coverage.status");
        return p.result("stale-death: station2 visit 2 digest = baseline's; session-ambiguous: visit-6 " + status(v6) + " " + reasons(v6) + ", nutrunner2 coverage "
            + evidence(v6, N2).path("coverage").path("status").asText());
    }

    static Result i26(Context c) throws Exception {
        Run a = run(c, "quiet-occupancy"), b = run(c, "quiet-occupancy-control");
        P p = new P(a, b);
        if (!p.sound()) return p.result("");
        int part = a.fold.fingerprints().keySet().contains(RunFold.WR + "/0") ? 0 : -1;
        List<Long> compared = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        List<Long> deadlines = new ArrayList<>(a.deadlines());
        deadlines.addAll(b.deadlines());
        Long settledA = a.settledCut();
        for (long k : a.cutIds()) {
            if (!b.cutIds().contains(k)) continue;
            String why = Comparison.notComparable(a.side(k), b.side(k), deadlines);
            if (why != null) { skipped.add(k + ""); continue; }
            compared.add(k);
            for (String t : List.of(RunFold.WR, RunFold.SAMPLES))
                p.equal(fp(b.fold.fingerprints(), t, List.of(part), k), fp(a.fold.fingerprints(), t, List.of(part), k), "cut " + k + " station1 " + t);
        }
        p.require(!compared.isEmpty(), "no comparable cut");
        p.require(settledA != null && compared.contains(settledA), "the settled cut " + settledA + " is not compared");
        return p.result("station1 work records and samples equal at " + compared.size() + " comparable cuts " + compact(compared) + " (settled cut " + settledA + " among them); "
            + skipped.size() + " cuts not comparable; station2 compared with the oracle in each run");
    }

    // --- J-*, C-* --------------------------------------------------------------------------------------------------------------

    static List<Run> everyRun(Context c, P p) throws Exception {
        List<Run> l = new ArrayList<>();
        for (String s : Scenarios.all()) {
            if (Scenarios.HORIZON.contains(s) && Scenarios.skipHorizon()) { p.list.add(s + " not run (-Dforseti.gate.skipHorizon=true)"); continue; }
            l.add(run(c, s));
        }
        return l;
    }

    static Result journal(Context c, String id) throws Exception {
        P p = new P();
        List<Run> runs = everyRun(c, p);
        long checked = 0;
        List<String> bad = new ArrayList<>();
        for (Run r : runs) {
            if (r.failure != null) { bad.add(r.name + ": " + r.failure); continue; }
            checked++;
            if (r.fold.findings.count(id) > 0) bad.add(r.name + " " + r.fold.findings.describe(id));
        }
        long shuffled = 0;
        for (String s : Scenarios.SHUFFLED) for (long seed : Scenarios.SHUFFLE_SEEDS) {
            Shuffled x = Scenarios.pipeline(c).shuffled(s, seed);
            if (x.failure() != null) { bad.add(s + " seed " + seed + ": " + x.failure()); continue; }
            shuffled++;
            if (x.findings().count(id) > 0) bad.add(s + " shuffle " + seed + " " + x.findings().describe(id));
        }
        p.list.addAll(bad);
        String extra = "";
        if (id.equals("J-DISPOSITION-COVERAGE")) {
            long deliveries = 0, without = 0;
            for (Run r : runs) if (r.measures != null) { deliveries += r.measures.deliveries(); without += r.measures.inputsWithoutDisposition(); }
            extra = "; " + deliveries + " deliveries, " + without + " without a disposition";
        }
        if (id.equals("J-REVISION-CONTINUITY") || id.equals("J-NO-EQUAL-CONSECUTIVE")) {
            long rows = 0;
            for (Run r : runs) if (r.fold != null) for (long n : r.fold.rows().values()) rows += n;
            extra = "; " + rows + " journal records";
        }
        return new Result(p.list.isEmpty(), (p.list.isEmpty() ? "" : "PROBLEMS: " + String.join("; ", p.list) + " || ") + "0 violations over " + checked + " scenario runs and " + shuffled
            + " shuffled runs (" + Scenarios.all().size() + " scenarios)" + extra);
    }

    static Result afterHorizon(Context c) throws Exception {
        P p = new P();
        if (Scenarios.skipHorizon()) { p.list.add("the horizon scenarios are skipped (-Dforseti.gate.skipHorizon=true)"); return p.result(""); }
        List<String> out = new ArrayList<>();
        for (String s : Scenarios.HORIZON) {
            Run r = p.use(run(c, s));
            if (r.failure != null) continue;
            long H = r.rules.attribution().horizonMs(), L = r.rules.attribution().allowedLatenessMs();
            long need = Long.MIN_VALUE;
            String station = S1;
            for (JsonNode v : r.expected.path("visits")) {
                if (!v.path("leave").isNull()) continue;
                long e = v.path("enter").asLong() + H + L;
                if (e > need) { need = e; station = v.path("station").asText(); }
            }
            String what = "enter + H + L of the open visits";
            if (need == Long.MIN_VALUE) {
                for (JsonNode v : r.expected.path("visits")) { long e = v.path("leave").asLong() + H + L; if (need == Long.MIN_VALUE || e < need) { need = e; station = v.path("station").asText(); } }
                what = "the first onlineExpiresAt (leave + H + L)";
            }
            long lastW = r.w1(r.lastCut(), station);
            p.require(lastW >= need, s + ": last W1 " + lastW + " < " + what + " " + need);
            long maxOnline = 0, lastOnline = r.perStation(r.lastCut()).path(station).path("onlineVisits").asLong(-1);
            for (long k : r.cutIds()) maxOnline = Math.max(maxOnline, r.perStation(k).path(station).path("onlineVisits").asLong());
            p.require(maxOnline > 0 && lastOnline < maxOnline, s + ": onlineVisits max " + maxOnline + ", last " + lastOnline);
            int rows = r.fold.live(RunFold.WR).size();
            p.equal(r.expected.path("visits").size(), rows, s + " work records in the fold at the last cut");
            out.add(s + " last W1 T0 + " + rel(r, lastW) + " >= " + what + " T0 + " + rel(r, need) + ", onlineVisits " + maxOnline + " -> " + lastOnline + ", " + rows + " work records remain");
        }
        return p.result(String.join("; ", out));
    }

    static Result determinism(Context c, String id, List<String> scenarios, List<String> topics) throws Exception {
        P p = new P();
        List<String> out = new ArrayList<>();
        ObjectNode m = Json.object();
        for (String s : scenarios) {
            Run base = p.use(run(c, s));
            if (base.failure != null) continue;
            ObjectNode counts = m.putObject(s);
            counts.set("default", rowsJson(base.fold.rows()));
            int cuts = 0;
            for (long seed : Scenarios.SHUFFLE_SEEDS) {
                Shuffled x = Scenarios.pipeline(c).shuffled(s, seed);
                if (x.failure() != null) { p.list.add(s + " seed " + seed + ": " + x.failure()); continue; }
                counts.set("shuffle-" + seed, rowsJson(x.rows()));
                List<Long> ids = new ArrayList<>();
                for (JsonNode k : x.cutLog().path("cuts")) ids.add(k.path("cutId").asLong());
                p.equal(base.cutIds(), ids, s + " seed " + seed + " cut ids");
                for (JsonNode k : x.cutLog().path("cuts")) {
                    long cut = k.path("cutId").asLong();
                    String why = Comparison.notComparable(base.side(cut), new Comparison.Side(k.path("perStation"), base.open.getOrDefault(cut, Set.of())), base.deadlines());
                    if (why != null) { p.list.add(s + " seed " + seed + " cut " + cut + " not comparable: " + why); continue; }
                    for (String t : topics) {
                        Set<Integer> parts = partitions(base, t);
                        String want = fp(base.fold.fingerprints(), t, parts, cut), have = fp(x.fingerprints(), t, parts, cut);
                        if (!want.equals(have)) p.list.add(s + " seed " + seed + " cut " + cut + " " + t + " differs");
                    }
                }
                cuts = Math.max(cuts, x.cutLog().path("cuts").size());
            }
            long rows = 0;
            for (String t : topics) rows += base.fold.rows().getOrDefault(t, 0L);
            out.add(s + ": " + cuts + " cuts x " + topics.size() + " topics equal in the default run and 3 --shuffle runs (seeds " + Scenarios.SHUFFLE_SEEDS + "), " + rows + " records");
        }
        c.measure(id.equals("C-6-STAGE1") ? "determinismStage1Revisions" : "determinismStage2Revisions", m);
        return p.result(String.join("; ", out) + "; revision counts in report.json measurements");
    }

    static ObjectNode rowsJson(Map<String, Long> rows) {
        ObjectNode o = Json.object();
        rows.forEach((k, v) -> Json.putSafe(o, k, v));
        return o;
    }

    static Result reference(Context c) throws Exception {
        P p = new P();
        Run hb = p.use(run(c, "held-back-10min"));
        Run pl = null;
        if (Scenarios.skipHorizon()) p.list.add("horizon-planned-late not run (-Dforseti.gate.skipHorizon=true)");
        else pl = p.use(run(c, "horizon-planned-late"));
        List<Run> runs = everyRun(c, p);
        ObjectNode m = Json.object(), rows = m.putObject("journalRecordsPerTopic");
        long without = 0;
        for (Run r : runs) {
            if (r.failure != null || r.measures == null) continue;
            rows.set(r.name, rowsJson(r.fold.rows()));
            without += r.measures.inputsWithoutDisposition();
        }
        Json.putSafe(m, "inputsWithoutDisposition", without);
        ObjectNode late = m.putObject("longestLatenessCorrectedOnlineMs");
        String evidence = "";
        if (hb.failure == null) {
            Json.putSafe(late, "held-back-10min", hb.measures.longestLatenessMs());
            p.equal(600_000L, hb.measures.longestLatenessMs(), "held-back-10min longestLatenessCorrectedOnlineMs (" + hb.measures.longestAt() + ")");
            evidence += "held-back-10min " + hb.measures.longestLatenessMs() + " (" + hb.measures.longestAt() + ")";
        }
        if (pl != null && pl.failure == null) {
            long H = pl.rules.attribution().horizonMs();
            Json.putSafe(late, "horizon-planned-late", pl.measures.longestLatenessMs());
            p.equal(H, pl.measures.longestLatenessMs(), "horizon-planned-late longestLatenessCorrectedOnlineMs (" + pl.measures.longestAt() + ")");
            evidence += "; horizon-planned-late " + pl.measures.longestLatenessMs() + " = H " + H + " (" + pl.measures.longestAt() + ")";
        }
        p.equal(0L, without, "inputsWithoutDisposition");
        ObjectNode timings = m.putObject("scenarioMs");
        for (Run r : runs) { ObjectNode t = timings.putObject(r.name); r.ms.forEach((k, v) -> Json.putSafe(t, k, v)); }
        c.measure("reference", m);
        return p.result("longestLatenessCorrectedOnlineMs: " + evidence + "; journal records per topic of " + rows.size() + " scenarios and inputsWithoutDisposition " + without
            + " in report.json measurements.reference");
    }
}
