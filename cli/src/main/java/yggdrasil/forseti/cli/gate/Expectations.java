package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import yggdrasil.forseti.cli.gate.RunFold.Disp;
import yggdrasil.forseti.cli.gate.RunFold.Rev;
import yggdrasil.forseti.cli.recording.ExpectedSamples;
import yggdrasil.forseti.core.Json;

/** A run against its recording's ground truth ({@code expected.json}, plan Task 16): at the last cut every visit (status, unit,
 *  interval, insufficiency reasons, retraction, per-equipment coverage and per-member counts), every sample (state, reason,
 *  attributedTo; both encodings through {@link ExpectedSamples}), every identity's disposition per part (and its delivery count where
 *  listed; a sampleId not listed is {@code member:<metric> accepted accepted}) and every incident set (units, resolution, coverage);
 *  at the two cuts bracketing each {@code transitions[]} deadline, the reported {@code W1} on both sides of it and the state at
 *  each. A recording whose ground truth has the minimal scope (the smoke recordings) is checked by the S- and T-assertions instead; a
 *  replay recording's ({@code scope: replay}, visit facts and replay brackets only) is treated like the minimal one: a replay run is never
 *  compared with the online run's late-arrival expectations. */
final class Expectations {
    private Expectations() {}

    static final int EXAMPLES = 8;

    static final class Outcome {
        long visits, samples, dispositions, incidents, transitions, problems;
        final List<String> examples = new ArrayList<>();
        boolean skipped;
        String scope;
        void problem(String p) { problems++; if (examples.size() < EXAMPLES) examples.add(p); }
        boolean ok() { return problems == 0; }
        String describe() {
            if (skipped && "replay".equals(scope)) return "expected.json has the replay scope (visit facts only; no online outcome is compared)";
            if (skipped) return "expected.json has the minimal scope (checked by the S-/T-assertions)";
            String counts = visits + " visits, " + samples + " samples, " + dispositions + " identity parts, " + incidents + " incident sets, " + transitions + " transitions";
            return ok() ? "expected.json holds (" + counts + ")" : "expected.json: " + problems + " problems over " + counts + ": " + String.join(" | ", examples);
        }
    }

    /** Checks the fold, advanced to the last cut, against {@code expected}; {@code perStationOf} gives a cut's {@code perStation}. */
    static Outcome check(RunFold fold, JsonNode expected, java.util.function.LongFunction<JsonNode> perStationOf) {
        Outcome o = new Outcome();
        String scope = expected.path("scope").asText();
        if (scope.equals("minimal") || scope.equals("replay")) { o.skipped = true; o.scope = scope; return o; }   // no per-sample ground truth
        if (!scope.equals("full")) { o.problem("expected.json scope " + expected.path("scope") + " is none of full, minimal, replay"); return o; }
        Map<String, String> refKey = new HashMap<>();
        for (JsonNode v : expected.path("visits")) refKey.put(Json.text(v, "visitRef"), RunFold.keyText(v.path("key")));
        // visits
        Map<String, Rev> wr = fold.live(RunFold.WR);
        Set<String> expectedKeys = new TreeSet<>(refKey.values());
        for (String k : wr.keySet()) if (!expectedKeys.contains(k)) o.problem("work record " + k + " is not a ground-truth visit");
        for (JsonNode v : expected.path("visits")) {
            o.visits++;
            String ref = v.path("visitRef").asText(), key = refKey.get(ref);
            Rev r = wr.get(key);
            if (r == null) { o.problem(ref + ": no work record at the last cut"); continue; }
            JsonNode b = r.node();
            eq(o, ref + " status", v.path("finalStatus"), b.path("status"));
            eq(o, ref + " unitId", v.path("unitId"), b.path("unitId"));
            eq(o, ref + " interval.enter", v.path("enter"), b.path("interval").path("enter"));
            eq(o, ref + " interval.leave", v.path("leave"), b.path("interval").path("leave"));
            eq(o, ref + " interval.attributionEnd", v.path("attributionEnd"), b.path("interval").path("attributionEnd"));
            eq(o, ref + " insufficiencyReasons", sorted(v.path("insufficiencyReasons")), sorted(b.path("insufficiencyReasons")));
            JsonNode rt = v.path("retraction"), br = b.path("retraction");
            if (rt.isNull() || rt.isMissingNode()) { if (!(br.isNull() || br.isMissingNode())) o.problem(ref + " retraction: expected none, got " + br); }
            else { eq(o, ref + " retraction.reason", rt.path("reason"), br.path("reason")); eq(o, ref + " retraction.errorReason", rt.path("errorReason"), br.path("errorReason")); }
            Map<String, JsonNode> evidence = new HashMap<>(), members = new HashMap<>();
            for (JsonNode e : b.path("equipmentEvidence")) evidence.put(e.path("equipmentPath").asText(), e);
            for (JsonNode m : b.path("members")) members.put(m.path("mappingId").asText(), m);
            Iterator<Map.Entry<String, JsonNode>> it = v.path("equipmentEvidence").fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                JsonNode have = evidence.get(e.getKey());
                if (have == null) { o.problem(ref + " equipmentEvidence " + e.getKey() + ": missing"); continue; }
                for (String f : List.of("unknownActivityPeriods", "sessionGaps", "status"))
                    if (e.getValue().has(f)) eq(o, ref + " " + tail(e.getKey()) + ".coverage." + f, e.getValue().get(f), have.path("coverage").path(f));
                Iterator<Map.Entry<String, JsonNode>> ms = e.getValue().path("members").fields();
                while (ms.hasNext()) {
                    Map.Entry<String, JsonNode> m = ms.next();
                    JsonNode hm = members.get(m.getKey());
                    if (hm == null) { o.problem(ref + " members[" + m.getKey() + "]: missing"); continue; }
                    Iterator<Map.Entry<String, JsonNode>> fs = m.getValue().fields();
                    while (fs.hasNext()) { Map.Entry<String, JsonNode> x = fs.next(); eq(o, ref + " members[" + m.getKey() + "]." + x.getKey(), x.getValue(), hm.path(x.getKey())); }
                }
            }
        }
        // samples
        Map<String, Rev> samples = fold.live(RunFold.SAMPLES);
        Set<String> named = new HashSet<>();
        ExpectedSamples.forEachSample(expected, (id, want) -> {
            o.samples++;
            named.add(id);
            Rev r = samples.get(id);
            if (r == null) { o.problem("sample " + id + ": no row at the last cut"); return; }
            JsonNode b = r.node();
            eq(o, "sample " + id + " state", want.path("state"), b.path("state"));
            eq(o, "sample " + id + " reason", want.path("reason"), b.path("reason"));
            JsonNode to = want.path("attributedTo");
            String wantTo = to.isNull() || to.isMissingNode() ? "null" : refKey.getOrDefault(to.asText(), "<unknown visitRef " + to.asText() + ">");
            String haveTo = b.path("attributedTo").isNull() || b.path("attributedTo").isMissingNode() ? "null" : RunFold.keyText(b.path("attributedTo"));
            if (!wantTo.equals(haveTo)) o.problem("sample " + id + " attributedTo: expected " + to + " " + wantTo + ", got " + haveTo);
        });
        if (samples.size() != named.size() || !named.containsAll(samples.keySet())) {
            long extra = samples.keySet().stream().filter(k -> !named.contains(k)).count();
            if (extra > 0) o.problem(extra + " sample rows the ground truth does not name, e.g. " + samples.keySet().stream().filter(k -> !named.contains(k)).findFirst().orElse(""));
        }
        // dispositions: (identity, part) -> the judgements of its deliveries
        Map<String, List<Disp>> have = new HashMap<>(), byIdentity = new HashMap<>();
        for (Disp d : fold.dispositions()) {
            have.computeIfAbsent(d.identity() + "\u0000" + d.part(), k -> new ArrayList<>()).add(d);
            byIdentity.computeIfAbsent(d.identity(), k -> new ArrayList<>()).add(d);
        }
        Set<String> listed = new HashSet<>();
        ExpectedSamples.forEachDisposition(expected, (identity, entry) -> {
            Iterator<Map.Entry<String, JsonNode>> ps = entry.path("parts").fields();
            while (ps.hasNext()) {
                Map.Entry<String, JsonNode> p = ps.next();
                o.dispositions++;
                // a sampleId's part is written "member": the member part of its metric
                String part = p.getKey().equals("member") ? "member:" + Json.read(identity.getBytes(java.nio.charset.StandardCharsets.UTF_8)).path("metric").asText() : p.getKey();
                String k = identity + "\u0000" + part;
                listed.add(k);
                judge(o, identity, part, p.getValue(), have.get(k));
            }
            // deliveries and duplicates count the identity's input records over all its parts
            Set<String> records = new HashSet<>();
            long duplicates = 0;
            for (Disp d : byIdentity.getOrDefault(identity, List.of())) {
                records.add(d.topic() + "/" + d.partition() + "/" + d.offset());
                if ("duplicate".equals(d.disposition()) && "identical-redelivery".equals(d.reason())) duplicates++;
            }
            if (entry.path("deliveries").isIntegralNumber() && entry.get("deliveries").asLong() != records.size())
                o.problem("disposition " + identity + ": " + records.size() + " deliveries, expected " + entry.get("deliveries").asLong());
            if (entry.path("duplicates").isIntegralNumber() && entry.get("duplicates").asLong() != duplicates)
                o.problem("disposition " + identity + ": " + duplicates + " duplicate dispositions, expected " + entry.get("duplicates").asLong());
        });
        JsonNode member = expected.path("messageMemberDispositions");
        for (String id : named) {
            JsonNode n = Json.read(id.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            String part = "member:" + n.path("metric").asText();
            String k = id + "\u0000" + part;
            if (listed.contains(k)) continue;
            listed.add(k);
            o.dispositions++;
            judge(o, id, part, member, have.get(k));
        }
        for (String k : have.keySet()) if (!listed.contains(k)) o.problem("disposition " + k.replace('\u0000', ' ') + " " + judgements(have.get(k)) + ": not in the ground truth");
        // incident sets
        Map<String, Rev> sets = fold.live(RunFold.SETS);
        Iterator<Map.Entry<String, JsonNode>> incs = expected.path("incidents").fields();
        Set<String> expectedSets = new HashSet<>();
        while (incs.hasNext()) {
            Map.Entry<String, JsonNode> e = incs.next();
            o.incidents++;
            expectedSets.add(e.getKey());
            Rev r = sets.get(e.getKey());
            if (r == null) { o.problem("affected set " + e.getKey() + ": none at the last cut"); continue; }
            JsonNode b = r.node();
            Map<String, JsonNode> units = new TreeMap<>();
            for (JsonNode u : b.path("units")) units.put(u.path("unitId").asText(), u);
            Map<String, JsonNode> want = new TreeMap<>();
            e.getValue().path("units").fields().forEachRemaining(u -> want.put(u.getKey(), u.getValue()));
            if (!units.keySet().equals(want.keySet())) o.problem("set " + e.getKey() + " units " + units.keySet() + ", expected " + want.keySet());
            for (Map.Entry<String, JsonNode> u : want.entrySet()) {
                JsonNode hu = units.get(u.getKey());
                if (hu == null) continue;
                eq(o, "set " + e.getKey() + " " + u.getKey() + " status", u.getValue().path("status"), hu.path("status"));
                eq(o, "set " + e.getKey() + " " + u.getKey() + " reason", u.getValue().path("reason"), hu.path("reason"));
            }
            eq(o, "set " + e.getKey() + " resolution", e.getValue().path("resolution"), b.path("resolution"));
            e.getValue().path("coverage").fields().forEachRemaining(c -> eq(o, "set " + e.getKey() + " coverage." + c.getKey(), c.getValue(), b.path("coverage").path(c.getKey())));
        }
        for (String k : sets.keySet()) if (!expectedSets.contains(k)) o.problem("affected set " + k + " is not a ground-truth incident");
        // transitions at the bracketing cuts
        for (JsonNode t : expected.path("transitions")) {
            o.transitions++;
            String kind = t.path("assertion").asText(), ref = t.path("ref").asText(), station = t.path("station").asText();
            long a = t.path("cutA").asLong(), b = t.path("cutB").asLong(), at = t.path("at").asLong();
            String label = kind + " " + ref + " (" + at + ", cuts " + a + "/" + b + ")";
            JsonNode wa = perStationOf.apply(a).path(station).path("W1"), wb = perStationOf.apply(b).path(station).path("W1");
            eq(o, label + " W1 at cutA", t.path("w1A"), wa);
            eq(o, label + " W1 at cutB", t.path("w1B"), wb);
            if (!(wa.asLong() < at && at <= wb.asLong()))
                o.problem(label + ": W1 " + wa + " / " + wb + " does not bracket " + at);
            if (kind.equals("incident-settlement")) {
                String setKey = null;
                for (String k : sets.keySet()) if (Json.read(k.getBytes(java.nio.charset.StandardCharsets.UTF_8)).path("incidentId").asText().equals(ref)) setKey = k;
                if (setKey == null) { o.problem(label + ": no affected set of " + ref); continue; }
                // no set row yet (the incident not delivered by that cut): not settled
                JsonNode sa = fold.at(RunFold.SETS, setKey, a), sb = fold.at(RunFold.SETS, setKey, b);
                JsonNode no = Json.JSON.getNodeFactory().booleanNode(false);
                eq(o, label + " settled at cutA", t.path("atA"), sa == null ? no : sa.path("coverage").path("settled"));
                eq(o, label + " settled at cutB", t.path("atB"), sb == null ? no : sb.path("coverage").path("settled"));
            } else {
                String key = refKey.get(ref);
                if (key == null) { o.problem(label + ": unknown visitRef"); continue; }
                JsonNode va = fold.at(RunFold.WR, key, a), vb = fold.at(RunFold.WR, key, b);
                eq(o, label + " status at cutA", t.path("atA"), va == null ? null : va.path("status"));
                eq(o, label + " status at cutB", t.path("atB"), vb == null ? null : vb.path("status"));
            }
        }
        return o;
    }

    private static void judge(Outcome o, String identity, String part, JsonNode want, List<Disp> got) {
        String label = "disposition " + identity + " " + part;
        if (got == null) { o.problem(label + ": none, expected " + want.path("disposition").asText() + " " + want.path("reason").asText()); return; }
        Disp main = null;
        for (Disp d : got) if (!("duplicate".equals(d.disposition()) && "identical-redelivery".equals(d.reason()))) { main = d; break; }
        if (main == null) main = got.get(0);
        if (!want.path("disposition").asText().equals(main.disposition()) || !want.path("reason").asText().equals(main.reason()))
            o.problem(label + ": " + judgements(got) + ", expected " + want.path("disposition").asText() + " " + want.path("reason").asText());
    }

    static String judgements(List<Disp> l) {
        List<String> s = new ArrayList<>();
        for (Disp d : l) s.add(d.disposition() + " " + d.reason());
        Collections.sort(s);
        return s.toString();
    }

    private static void eq(Outcome o, String what, JsonNode want, JsonNode have) {
        String w = Comparison.text(want == null || want.isMissingNode() ? null : want), h = Comparison.text(have == null || have.isMissingNode() ? null : have);
        if (!w.equals(h)) o.problem(what + ": expected " + w + ", got " + h);
    }

    private static JsonNode sorted(JsonNode a) {
        List<String> l = new ArrayList<>();
        for (JsonNode x : a) l.add(x.asText());
        Collections.sort(l);
        com.fasterxml.jackson.databind.node.ArrayNode n = Json.JSON.createArrayNode();
        l.forEach(n::add);
        return n;
    }

    static String tail(String path) { return path.substring(path.lastIndexOf(':') + 1); }
}
