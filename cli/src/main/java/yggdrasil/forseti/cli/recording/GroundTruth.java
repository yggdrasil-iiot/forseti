package yggdrasil.forseti.cli.recording;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** {@code expected.json} from the records the builder delivered (plan Task 16, Step 16.3). The generator knows every record it wrote and
 *  the batch it wrote it in; this class re-derives, from those records and the rule's parameters alone, what a correct engine must
 *  report: at any cut, the visit claims, the state of every sample and the status of every visit; at the last cut, the dispositions per
 *  identity and the stage-2 units and set coverage. It is written from the rule sheets of the spec and never calls the core's decision
 *  functions ({@code Canonical} is used only to spell identities and digests). All times are epoch milliseconds. */
final class GroundTruth {

    // ---------------------------------------------------------------- the rule, read from the registry's release payload

    record Member(String mappingId, String equipment, String metric, String role) {}
    record Rule(String mode, String crossCheck, String namespace, List<String> stations, long ooo, long lateness, long horizon, long enterTol,
                long leaveTol, BigDecimal maxBadShare, long period, long maxMissing, List<Member> members, Map<String, Member> activity,
                Map<String, String[]> identity, List<String> incidentScope, String onWithdrawn, String onBasisRetracted, long workRecordsMs,
                JsonNode temporal) {
        List<Member> measurementsAt(String station) {
            List<Member> l = new ArrayList<>();
            for (Member m : members) if (m.role().equals("measurement") && within(m.equipment(), station)) l.add(m);
            l.sort(Comparator.comparing(Member::mappingId));
            return l;
        }
        List<Member> activityAt(String station) {
            List<Member> l = new ArrayList<>();
            for (Member m : activity.values()) if (within(m.equipment(), station)) l.add(m);
            l.sort(Comparator.comparing(Member::equipment));
            return l;
        }
        Member measurement(String device, String metric) {
            for (Member m : members) if (m.equipment().equals(device) && m.metric().equals(metric)) return m;
            return null;
        }
    }

    static boolean within(String path, String station) { return path.equals(station) || path.startsWith(station + ":"); }

    /** The pinned rule of {@code stream}: {@code heads/<stream>.json} names the release, whose payload holds the attribution rule, the
     *  impact rule that references it, the mapping sets and the hierarchy. Signatures are the harness's and the gate's business; the
     *  builder only reads the parameters it generates against, and refuses a scenario whose rule pin differs from the head. */
    static Rule load(Path registry, JsonNode pin) {
        Path m = registry.resolve("mappings");
        String stream = Json.text(pin, "stream");
        JsonNode head = Json.read(m.resolve("heads").resolve(stream + ".json"));
        if (!Json.text(head, "id").equals(Json.text(pin, "releaseId")) || !Json.text(head, "version").equals(Json.text(pin, "version")))
            throw refuse("rule pin " + pin + " differs from the head of " + stream + " " + head);
        Path payload = m.resolve("releases").resolve(Json.text(head, "id")).resolve(Json.text(head, "version")).resolve("payload");
        JsonNode rule = null, impact = null;
        for (JsonNode r : jsons(payload.resolve("attribution-rules")))
            if (Json.text(r, "id").equals(Json.text(pin, "attributionRuleId")) && Json.text(r, "version").equals(Json.text(pin, "attributionRuleVersion"))) rule = r;
        if (rule == null) throw refuse("no attribution rule " + pin.path("attributionRuleId").asText() + " in the release");
        for (JsonNode r : jsons(payload.resolve("impact-rules")))
            if (r.path("attributionRuleRef").path("id").asText().equals(rule.path("id").asText())
                && r.path("attributionRuleRef").path("version").asText().equals(rule.path("version").asText())) impact = r;
        Map<String, String> paths = new HashMap<>();
        Map<String, JsonNode> nodes = new HashMap<>();
        for (JsonNode h : jsons(payload.resolve("hierarchy"))) for (JsonNode n : h.path("nodes")) nodes.put(Json.text(n, "equipmentInstanceId"), n);
        for (String id : nodes.keySet()) {
            Deque<String> parts = new ArrayDeque<>();
            for (JsonNode n = nodes.get(id); n != null; n = n.path("parentEquipmentInstanceId").isTextual() ? nodes.get(n.path("parentEquipmentInstanceId").asText()) : null)
                parts.addFirst(Json.text(n, "equipmentInstanceId"));
            paths.put(id, String.join(":", parts));
        }
        Map<String, String> setEquipment = new HashMap<>();
        for (JsonNode s : jsons(payload.resolve("mappings"))) setEquipment.put(Json.text(s, "id"), paths.get(Json.text(s, "equipmentInstanceId")));
        Set<String> activityRefs = new HashSet<>(), identityRefs = new HashSet<>();
        rule.path("activity").forEach(a -> activityRefs.add(Json.text(a, "memberRef")));
        Map<String, String[]> identity = new TreeMap<>();
        for (JsonNode e : rule.path("identity").path("members")) {
            identityRefs.add(Json.text(e, "unitMemberRef")); identityRefs.add(Json.text(e, "visitMemberRef")); identityRefs.add(Json.text(e, "presentMemberRef"));
        }
        List<Member> members = new ArrayList<>();
        Map<String, Member> byId = new HashMap<>();
        for (JsonNode x : rule.path("members")) {
            String id = Json.text(x, "mappingId");
            String equipment = setEquipment.get(x.path("mappingSetRef").path("id").asText());
            if (equipment == null) throw refuse("member " + id + " resolves to no equipment");
            Member mm = new Member(id, equipment, Json.text(x, "metric"), activityRefs.contains(id) ? "activity" : identityRefs.contains(id) ? "identity" : "measurement");
            members.add(mm);
            byId.put(id, mm);
        }
        Map<String, Member> activity = new TreeMap<>();
        rule.path("activity").forEach(a -> activity.put(Json.text(a, "equipmentPath"), byId.get(Json.text(a, "memberRef"))));
        for (JsonNode e : rule.path("identity").path("members"))
            identity.put(Json.text(e, "equipmentPath"), new String[] {byId.get(Json.text(e, "unitMemberRef")).metric(),
                byId.get(Json.text(e, "visitMemberRef")).metric(), byId.get(Json.text(e, "presentMemberRef")).metric()});
        List<String> stations = new ArrayList<>();
        rule.path("stations").forEach(s -> stations.add(s.asText()));
        List<String> scope = new ArrayList<>();
        if (impact != null) impact.path("incidentScope").forEach(s -> scope.add(s.asText()));
        JsonNode t = rule.path("temporal");
        return new Rule(rule.path("identity").path("mode").asText(), rule.path("identity").path("crossCheck").asText(),
            rule.path("identity").path("namespace").asText(null), List.copyOf(stations),
            t.path("outOfOrdernessMs").longValue(), t.path("allowedLatenessMs").longValue(), t.path("horizonMs").longValue(),
            t.path("enterToleranceMs").longValue(), t.path("leaveToleranceMs").longValue(), new BigDecimal(rule.path("quality").path("maxBadShare").asText()),
            rule.path("coverage").path("expectedPeriodMs").longValue(), rule.path("coverage").path("maxMissingPeriods").longValue(), List.copyOf(members),
            activity, identity, List.copyOf(scope), impact == null ? null : impact.path("evidence").path("onWithdrawn").asText(),
            impact == null ? null : impact.path("evidence").path("onBasisRetracted").asText(),
            impact == null ? 0 : impact.path("retention").path("workRecordsMs").longValue(), t);
    }

    private static List<JsonNode> jsons(Path dir) {
        if (!Files.isDirectory(dir)) return List.of();
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().map(Json::read).toList();
        } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
    }

    // ---------------------------------------------------------------- what the builder hands over

    /** One delivered member of one message. Its sampleId is {@code (edge, device, birthPublishTs, metric, bdSeq, seq, publishTs)};
     *  {@link #key()} spells it as JCS text on demand, so a recording of a million samples does not hold a million key strings. PLC fields
     *  describe the message's stamp. */
    record Sample(String edge, long birth, long bdSeq, long seq, long publishTs, String device, String metric, long sourceTs, long status,
                  JsonNode value, int batch, boolean coherent, String visitId, String unitId, Boolean present) {
        String key() { return ScenarioBuilder.sampleKey(edge, device, birth, metric, bdSeq, seq, publishTs); }
    }
    /** One delivered record of an input topic, with the batch it was delivered in. */
    record Delivery(String topic, ObjectNode value, int batch) {}
    record Deadline(String kind, String ref, String station, long at, int cutA, int cutB) {}
    /** A visit's scenario facts, for the expected visit list (occupancy-join: by arrival eventId; plc-stamped: by visitId). */
    record VisitFacts(String visitRef, String arrivalEventId, List<String> tools) {}

    private final Rule rule;
    private final String source;
    private final Map<String, String> stationOf;
    private final List<Sample> samples;
    private final Map<String, List<Sample>> samplesByDevice = new HashMap<>();
    private final List<Delivery> occupancy, sessions, incidents;
    private final Map<String, long[]> w1;   // station -> W1 per batch (Long.MIN_VALUE before any telemetry)
    private final int batches;
    private final Map<String, VisitFacts> facts;
    private final Map<String, long[]> evidence;
    private final Map<String, Integer> messageDuplicates;
    private final Set<String> catalog = new TreeSet<>();   // the equipment the rule's members resolve to (S1)

    GroundTruth(Rule rule, String source, Map<String, String> stationOf, List<Sample> samples, List<Delivery> occupancy, List<Delivery> sessions,
                List<Delivery> incidents, Map<String, long[]> w1, int batches, Map<String, VisitFacts> facts, Map<String, long[]> evidence,
                Map<String, Integer> messageDuplicates) {
        this.rule = rule; this.source = source; this.stationOf = stationOf; this.samples = samples; this.occupancy = occupancy; this.sessions = sessions;
        this.incidents = incidents; this.w1 = w1; this.batches = batches; this.facts = facts; this.evidence = evidence; this.messageDuplicates = messageDuplicates;
        for (Sample x : samples) samplesByDevice.computeIfAbsent(x.device(), k -> new ArrayList<>()).add(x);
        for (Member m : rule.members()) catalog.add(m.equipment());
        samplesByDevice.values().forEach(l -> l.sort(Comparator.comparingLong(Sample::sourceTs)));
    }

    long w1(String station, int k) { long[] a = w1.get(station); return a == null || k < 0 ? Long.MIN_VALUE : a[k]; }

    // ---------------------------------------------------------------- visit claims at a cut

    /** A visit as the inputs delivered through one cut claim it. */
    static final class Claim {
        String ref, key, unit, station; long enter; Long leave; Long attributionEnd;
        boolean declared, conflict, overlap, correctionConflict, missingVisitClose, inconsistent;
        String errorReason; Long boundedAt;   // plc: the ID change that ends this ID's evidence without a close
        boolean retracted() { return declared || conflict || overlap; }
        long lo, hi;
    }

    private static ObjectNode base(ObjectNode v) {
        ObjectNode c = v.deepCopy();
        c.remove("recordTime");
        c.remove("errorDeclaration");
        return c;
    }

    private final Map<Delivery, Boolean> admission = new IdentityHashMap<>();

    /** S3 for an occupancy input, against {@code admissionW}, the station's W1 at the cut before the one that delivers it (null before
     *  the first cut: no bound). An arrival is refused when {@code admissionW >= eventTime + H + L}; a departure or an error declaration
     *  when the visit it refers to is expired at admission ({@code admissionW >= onlineExpiresAt}: {@code leave + H + L} closed,
     *  {@code enter + H + L} open), or, when that visit is unknown, when {@code admissionW >= eventTime + H + L}. */
    private boolean admitted(Delivery d) {
        Boolean known = admission.get(d);
        if (known != null) return known;
        boolean ok = true;
        ObjectNode v = d.value();
        long aw = d.batch() == 0 ? Long.MIN_VALUE : w1(v.get("readPoint").textValue(), d.batch() - 1);
        long span = rule.horizon() + rule.lateness();
        if (aw != Long.MIN_VALUE) {
            boolean arrival = ScenarioBuilder.ARRIVING.equals(v.path("bizStep").asText());
            if (arrival && !v.has("errorDeclaration")) ok = aw < v.get("eventTime").longValue() + span;
            else {
                String a = arrival ? v.get("eventId").textValue() : v.path("arrivalEventId").asText();
                long earliestEnter = Long.MAX_VALUE;   // a visit's onlineExpiresAt is never before its enter + H + L
                for (Delivery o : occupancy) if (o.value().get("eventId").textValue().equals(a)) earliestEnter = Math.min(earliestEnter, o.value().get("eventTime").longValue());
                if (earliestEnter == Long.MAX_VALUE || aw >= earliestEnter + span) {
                    Claim c = null;
                    for (Claim x : occupancyClaims(d.batch() - 1)) if (x.key.equals(a)) c = x;
                    ok = c == null ? aw < v.get("eventTime").longValue() + span : aw < (c.leave != null ? c.leave : c.enter) + span;
                }
            }
        }
        admission.put(d, ok);
        return ok;
    }

    /** Per occupancy eventId at cut k: its distinct contents (diagnostics and declaration stripped), its declarations, its deliveries. */
    private record EventState(List<ObjectNode> contents, List<ObjectNode> declarations, int deliveries) {}

    private Map<String, EventState> events(int k) {
        Map<String, List<ObjectNode>> contents = new LinkedHashMap<>(), decls = new LinkedHashMap<>();
        Map<String, Integer> count = new HashMap<>();
        for (Delivery d : occupancy) {
            if (d.batch() > k || !admitted(d)) continue;
            String id = d.value().get("eventId").textValue();
            count.merge(id, 1, Integer::sum);
            ObjectNode b = base(d.value());
            List<ObjectNode> l = contents.computeIfAbsent(id, x -> new ArrayList<>());
            if (!l.contains(b)) l.add(b);
            if (d.value().has("errorDeclaration")) decls.computeIfAbsent(id, x -> new ArrayList<>()).add(d.value());
        }
        Map<String, EventState> m = new LinkedHashMap<>();
        contents.forEach((id, l) -> m.put(id, new EventState(l, decls.getOrDefault(id, List.of()), count.get(id))));
        return m;
    }

    private List<Claim> claims(int k) {
        return rule.mode().equals("plc-stamped") ? plcClaims(k) : occupancyClaims(k);
    }

    List<Claim> occupancyClaims(int k) {
        Map<String, EventState> ev = events(k);
        Map<String, Claim> byArrival = new LinkedHashMap<>();
        for (Map.Entry<String, EventState> e : ev.entrySet()) {
            ObjectNode c = e.getValue().contents().get(0);
            if (!ScenarioBuilder.ARRIVING.equals(c.path("bizStep").asText())) continue;
            Claim v = new Claim();
            v.key = e.getKey();
            VisitFacts f = facts.get(e.getKey());
            v.ref = f == null ? e.getKey() : f.visitRef();
            v.unit = c.get("unitId").textValue();
            v.station = c.get("readPoint").textValue();
            v.enter = c.get("eventTime").longValue();
            v.conflict = e.getValue().contents().size() > 1;
            for (ObjectNode d : e.getValue().declarations()) { v.declared = true; v.errorReason = d.path("errorDeclaration").path("reason").asText(); }
            byArrival.put(e.getKey(), v);
        }
        Map<String, List<Long>> leaves = new HashMap<>();
        for (Map.Entry<String, EventState> e : ev.entrySet()) {
            ObjectNode c = e.getValue().contents().get(0);
            if (!ScenarioBuilder.DEPARTING.equals(c.path("bizStep").asText())) continue;
            Claim v = byArrival.get(c.path("arrivalEventId").asText());
            if (v == null) continue;   // a departure waiting for its arrival
            if (e.getValue().contents().size() > 1) { v.conflict = true; continue; }
            if (!e.getValue().declarations().isEmpty()) continue;   // the leave claim cancelled
            leaves.computeIfAbsent(v.key, x -> new ArrayList<>()).add(c.get("eventTime").longValue());
        }
        for (Claim v : byArrival.values()) {
            List<Long> l = leaves.getOrDefault(v.key, List.of());
            if (new TreeSet<>(l).size() > 1) v.correctionConflict = true;
            else if (!l.isEmpty()) v.leave = l.get(0);
        }
        List<Claim> all = new ArrayList<>(byArrival.values());
        for (Claim a : all) {
            if (a.declared || a.conflict || a.leave == null) continue;
            for (Claim b : all) if (b != a && b.station.equals(a.station) && !b.declared && !b.conflict && b.leave != null && a.enter < b.leave && b.enter < a.leave) a.overlap = true;
        }
        for (Claim a : all) {
            if (a.leave != null) continue;
            for (Claim b : all) if (b != a && b.station.equals(a.station) && !b.declared && !b.conflict && b.enter > a.enter && (a.attributionEnd == null || b.enter < a.attributionEnd)) a.attributionEnd = b.enter;
        }
        for (Claim a : all) {
            a.lo = a.enter - rule.enterTol();
            a.hi = a.leave != null ? a.leave + rule.leaveTol() : a.attributionEnd != null ? a.attributionEnd : Long.MAX_VALUE;
        }
        return all;
    }

    /** P1-P3 from the delivered coherent snapshots of each identified equipment. */
    List<Claim> plcClaims(int k) {
        Map<String, Claim> byVisit = new TreeMap<>();
        Map<String, Map<String, long[]>> perEquipment = new TreeMap<>();   // visitId -> equipment -> {start, end|-1, change|-1}
        Map<String, String> unitOf = new HashMap<>(), stationOfVisit = new HashMap<>();
        for (String eq : rule.identity().keySet()) {
            List<Sample> l = samplesByDevice.getOrDefault(eq, List.of());
            String presentMetric = rule.identity().get(eq)[2];
            List<Sample> snaps = new ArrayList<>();
            for (Sample x : l) if (x.batch() <= k && x.coherent() && x.metric().equals(presentMetric)) snaps.add(x);
            Map<String, long[]> life = new LinkedHashMap<>();
            for (Sample x : snaps) {
                if (Boolean.TRUE.equals(x.present())) {
                    long[] f = life.get(x.visitId());
                    if (f == null) { life.put(x.visitId(), new long[] {x.sourceTs(), -1, -1}); unitOf.putIfAbsent(x.visitId(), x.unitId()); stationOfVisit.put(x.visitId(), stationOf.get(eq)); }
                    for (Map.Entry<String, long[]> o : life.entrySet())
                        if (!o.getKey().equals(x.visitId()) && o.getValue()[1] < 0 && o.getValue()[2] < 0 && o.getValue()[0] < x.sourceTs()) o.getValue()[2] = x.sourceTs();
                } else {
                    long[] f = life.get(x.visitId());
                    if (f != null && f[1] < 0 && f[0] < x.sourceTs()) f[1] = x.sourceTs();
                }
            }
            life.forEach((v, f) -> perEquipment.computeIfAbsent(v, x -> new TreeMap<>()).put(eq, f));
        }
        for (Map.Entry<String, Map<String, long[]>> e : perEquipment.entrySet()) {
            Claim c = new Claim();
            c.ref = e.getKey(); c.key = e.getKey(); c.unit = unitOf.get(e.getKey()); c.station = stationOfVisit.get(e.getKey());
            Set<Long> starts = new TreeSet<>(), ends = new TreeSet<>();
            Long change = null;
            for (long[] f : e.getValue().values()) {
                starts.add(f[0]); ends.add(f[1]);
                if (f[2] >= 0 && f[1] < 0) change = change == null ? f[2] : Math.min(change, f[2]);
            }
            c.enter = starts.iterator().next();
            long end = ends.iterator().next();
            c.inconsistent = starts.size() > 1 || ends.size() > 1;
            c.leave = ends.size() == 1 && end >= 0 ? end : null;
            if (c.leave == null && change != null) { c.missingVisitClose = true; c.boundedAt = change; }
            c.lo = c.enter; c.hi = c.leave != null ? c.leave : Long.MAX_VALUE;
            byVisit.put(c.ref, c);
        }
        return new ArrayList<>(byVisit.values());
    }

    // ---------------------------------------------------------------- samples at a cut

    record Outcome(String state, String reason, Claim attributed, Claim blind) {}

    private record Intervals(Map<String, List<long[]>> unknown, Map<String, List<long[]>> down, Map<String, List<long[]>> gaps) {}

    private Intervals sessionIntervals(int k) {
        Map<String, List<long[]>> unknown = new HashMap<>(), down = new HashMap<>(), gaps = new HashMap<>();
        Map<String, List<Long>> ups = new HashMap<>();
        for (Delivery d : sessions) if (d.batch() <= k && d.value().get("kind").asText().equals("session-up"))
            ups.computeIfAbsent(d.value().get("device").asText(), x -> new ArrayList<>()).add(d.value().path("payload").path("at").longValue());
        for (Delivery d : sessions) {
            if (d.batch() > k) continue;
            String dev = d.value().get("device").asText(), kind = d.value().get("kind").asText();
            JsonNode p = d.value().path("payload");
            long open = w1(stationOf.get(dev), k);
            switch (kind) {
                case "seq-gap" -> gaps.computeIfAbsent(dev, x -> new ArrayList<>()).add(new long[] {p.get("from").longValue(), p.get("to").longValue()});
                case "session-down" -> {
                    long from = p.get("from").longValue(), end = nextUp(ups.get(dev), from, open);
                    down.computeIfAbsent(dev, x -> new ArrayList<>()).add(new long[] {from, end});
                    gaps.computeIfAbsent(dev, x -> new ArrayList<>()).add(new long[] {from, end});
                }
                case "session-ambiguous" -> {
                    long det = p.get("detectedEventTime").longValue(), start = det;
                    for (Sample x : samplesByDevice.getOrDefault(dev, List.of())) if (x.batch() <= k && x.sourceTs() < det) start = x.sourceTs();
                    unknown.computeIfAbsent(dev, x -> new ArrayList<>()).add(new long[] {start, nextUp(ups.get(dev), det, open)});
                }
                default -> { }
            }
        }
        return new Intervals(unknown, down, gaps);
    }

    private static long nextUp(List<Long> ups, long from, long open) {
        long best = open;
        if (ups != null) for (long at : ups) if (at >= from && at < best) best = at;
        return best;
    }
    private static boolean inside(List<long[]> l, long t) {
        if (l != null) for (long[] i : l) if (i[0] <= t && t < i[1]) return true;
        return false;
    }
    static boolean bad(long status) { return (status & 0xC0000000L) == 0x80000000L; }
    static boolean uncertain(long status) { return (status & 0xC0000000L) == 0x40000000L; }

    private record Eval(int k, List<Claim> claims, Map<Sample, Outcome> outcomes, Intervals sessions) {}

    private Eval evaluate(int k) {
        List<Claim> claims = claims(k);
        Intervals iv = sessionIntervals(k);
        Map<Sample, Outcome> out = new LinkedHashMap<>();
        Map<String, Claim> byRef = new HashMap<>();
        claims.forEach(c -> byRef.put(c.ref, c));
        List<Claim> occupants = rule.mode().equals("plc-stamped") && rule.crossCheck().equals("occupancy") ? occupancyClaims(k) : List.of();
        for (Sample x : samples) {
            if (x.batch() > k || !catalog.contains(x.device())) continue;   // S1: a device outside the catalog is rejected unknown-device
            String station = stationOf.get(x.device());
            if (x.batch() > 0 && x.sourceTs() < w1(station, x.batch() - 1) - rule.horizon()) { out.put(x, new Outcome("replay-required", "beyond-horizon", null, null)); continue; }
            boolean sessionOut = inside(iv.down().get(x.device()), x.sourceTs()), sessionUnknown = inside(iv.unknown().get(x.device()), x.sourceTs());
            Outcome o;
            if (rule.mode().equals("plc-stamped")) o = plc(x, byRef, occupants, sessionOut, sessionUnknown);
            else {
                Claim blind = null; String why = null;
                List<Claim> cand = new ArrayList<>();
                for (Claim c : claims) if (c.station.equals(station) && c.lo <= x.sourceTs() && x.sourceTs() < c.hi) cand.add(c);
                List<Claim> live = cand.stream().filter(c -> !c.retracted()).toList();
                if (live.size() == 1) blind = live.get(0);
                else if (live.size() > 1) why = "ambiguous";
                else if (!cand.isEmpty()) {
                    List<Claim> in = cand.stream().filter(c -> c.enter <= x.sourceTs() && (c.leave == null || x.sourceTs() < c.leave)).toList();
                    if (in.size() == 1) blind = in.get(0); else why = "ambiguous";
                } else why = "no-occupancy";
                o = classify(x, blind, why, sessionOut, sessionUnknown, null);
            }
            out.put(x, o);
        }
        return new Eval(k, claims, out, iv);
    }

    private Outcome classify(Sample x, Claim blind, String why, boolean sessionOut, boolean sessionUnknown, String attributedReason) {
        if (sessionOut) return new Outcome("unattributed", "outside-session", null, null);
        if (sessionUnknown) return new Outcome("unattributed", "session-ambiguous", null, null);
        if (bad(x.status()) || uncertain(x.status())) return new Outcome("unattributed", "bad-quality", null, blind);
        if (blind == null) return new Outcome("unattributed", why, null, null);
        return new Outcome("attributed", attributedReason, blind, blind);
    }

    private Outcome plc(Sample x, Map<String, Claim> byRef, List<Claim> occupants, boolean sessionOut, boolean sessionUnknown) {
        if (!rule.identity().containsKey(x.device())) return classify(x, null, "no-occupancy", sessionOut, sessionUnknown, null);
        if (!x.coherent()) return new Outcome("unattributed", "incoherent-snapshot", null, null);
        Claim c = Boolean.TRUE.equals(x.present()) ? byRef.get(x.visitId()) : null;
        if (c != null && c.boundedAt != null && x.sourceTs() >= c.boundedAt) c = null;
        String reason = null;
        if (c != null) for (Claim o : occupants)
            if (!o.retracted() && o.station.equals(c.station) && o.enter <= x.sourceTs() && o.leave != null && x.sourceTs() < o.leave && !o.unit.equals(x.unitId())) reason = "identity-mismatch";
        return classify(x, c, "no-occupancy", sessionOut, sessionUnknown, reason);
    }

    // ---------------------------------------------------------------- visit status at a cut

    record Status(String status, List<String> reasons, String retraction, String errorReason, Map<String, ObjectNode> equipment) {}

    private Status status(Eval e, Claim c) {
        String station = c.station;
        long w = w1(station, e.k());
        long jFrom = c.enter, jTo;
        if (c.leave != null) jTo = c.leave;
        else {
            jTo = Math.min(w == Long.MIN_VALUE ? c.enter : w, c.enter + rule.horizon());
            if (c.attributionEnd != null) jTo = Math.min(jTo, c.attributionEnd);
            if (c.boundedAt != null) jTo = Math.min(jTo, c.boundedAt);
        }
        TreeSet<String> reasons = new TreeSet<>();
        Map<String, ObjectNode> equipment = new TreeMap<>();
        if (jTo > jFrom) {
            long n = (jTo - jFrom + rule.period() - 1) / rule.period();
            Map<String, TreeSet<Long>> observed = new HashMap<>(), badSlots = new HashMap<>();
            Map<String, List<Sample>> act = new HashMap<>();
            for (Map.Entry<Sample, Outcome> o : e.outcomes().entrySet()) {
                Sample x = o.getKey();
                if (o.getValue().blind() != c || x.sourceTs() < jFrom || x.sourceTs() >= jTo) continue;
                long slot = (x.sourceTs() - jFrom) / rule.period();
                String mk = x.device() + "|" + x.metric();
                observed.computeIfAbsent(mk, z -> new TreeSet<>()).add(slot);
                if (bad(x.status())) badSlots.computeIfAbsent(mk, z -> new TreeSet<>()).add(slot);
                if ("attributed".equals(o.getValue().state())) act.computeIfAbsent(mk, z -> new ArrayList<>()).add(x);
            }
            for (Member m : rule.measurementsAt(station)) {
                String mk = m.equipment() + "|" + m.metric();
                long obs = observed.getOrDefault(mk, new TreeSet<>()).size(), bs = badSlots.getOrDefault(mk, new TreeSet<>()).size();
                if (n - obs > rule.maxMissing()) reasons.add("missing-periods");
                if (obs == 0) reasons.add("no-observed-periods");
                else if (BigDecimal.valueOf(bs).compareTo(rule.maxBadShare().multiply(BigDecimal.valueOf(obs))) > 0) reasons.add("bad-share-exceeded");
                ObjectNode eq = equipment.computeIfAbsent(m.equipment(), z -> Json.object());
                ObjectNode mm = eq.with("members").putObject(m.mappingId());
                Json.putSafe(mm, "observedPeriods", obs); Json.putSafe(mm, "missingPeriods", n - obs); Json.putSafe(mm, "badSlots", bs);
            }
            for (Member a : rule.activityAt(station)) {
                List<Sample> l = act.getOrDefault(a.equipment() + "|" + a.metric(), new ArrayList<>());
                l.sort(Comparator.comparingLong(Sample::sourceTs));
                List<long[]> support = new ArrayList<>();
                for (int i = 0; i < l.size(); i++) {
                    long t = l.get(i).sourceTs(), end = t + rule.period();
                    if (i + 1 < l.size()) end = Math.min(end, l.get(i + 1).sourceTs());
                    support.add(new long[] {Math.max(t, jFrom), Math.min(end, jTo)});
                }
                long unknownSlots = 0;
                int first = 0;   // supports wholly before the slot are skipped by covered() anyway; skip them once (an open visit has 86,400 slots)
                for (long i = 0; i < n; i++) {
                    long s0 = jFrom + i * rule.period(), s1 = Math.min(s0 + rule.period(), jTo);
                    while (first < support.size() && support.get(first)[1] <= s0) first++;
                    if (!covered(support.subList(first, support.size()), s0, s1)) unknownSlots++;
                }
                if (unknownSlots > rule.maxMissing()) reasons.add("unknown-activity");
                ObjectNode eq = equipment.computeIfAbsent(a.equipment(), z -> Json.object());
                Json.putSafe(eq, "unknownActivityPeriods", unknownSlots);
            }
            for (String eqPath : new ArrayList<>(equipment.keySet())) {
                ObjectNode eq = equipment.get(eqPath);
                long gaps = 0;
                for (long[] g : e.sessions().gaps().getOrDefault(eqPath, List.of())) if (g[0] < jTo && jFrom < g[1]) gaps++;
                boolean unknownSession = false;
                for (long[] u : e.sessions().unknown().getOrDefault(eqPath, List.of())) if (u[0] < jTo && jFrom < u[1]) unknownSession = true;
                Json.putSafe(eq, "sessionGaps", gaps);
                if (gaps > 0) reasons.add("session-gap");
                if (unknownSession) reasons.add("session-ambiguous");
                long missing = 0; boolean badShare = false;
                for (JsonNode mm : eq.path("members")) {
                    missing = Math.max(missing, mm.get("missingPeriods").longValue());
                    long obs = mm.get("observedPeriods").longValue();
                    if (obs > 0 && BigDecimal.valueOf(mm.get("badSlots").longValue()).compareTo(rule.maxBadShare().multiply(BigDecimal.valueOf(obs))) > 0) badShare = true;
                }
                long unknown = eq.path("unknownActivityPeriods").asLong(0);
                eq.put("status", unknown > 0 || unknownSession ? "unknown" : missing > rule.maxMissing() || gaps > 0 || badShare ? "incomplete" : "complete");
            }
        }
        if (c.leave == null && w != Long.MIN_VALUE && w >= c.enter + rule.horizon()) reasons.add("missing-departure");
        if (c.missingVisitClose) reasons.add("missing-visit-close");
        if (c.inconsistent) reasons.add("inconsistent-visit-boundary");
        if (c.correctionConflict) reasons.add("correction-conflict");
        String st, retraction = null;
        if (c.declared && rule.mode().equals("occupancy-join")) { st = "retracted"; retraction = "error-declaration"; }
        else if (c.conflict) { st = "retracted"; retraction = "input-conflict"; }
        else if (c.overlap) { st = "retracted"; retraction = "overlap"; }
        else if (!reasons.isEmpty()) st = "insufficient";
        else if (c.leave != null && w != Long.MIN_VALUE && w >= c.leave + rule.lateness()) st = "complete";
        else st = "provisional";
        if (st.equals("retracted")) reasons.removeIf(r -> List.of("session-gap", "session-ambiguous").contains(r));   // gap rules apply only to visits not retracted
        return new Status(st, List.copyOf(reasons), retraction, "error-declaration".equals(retraction) ? c.errorReason : null, equipment);
    }

    private static boolean covered(List<long[]> support, long s0, long s1) {
        long at = s0;
        for (long[] iv : support) {
            if (iv[1] <= at) continue;
            if (iv[0] > at) return false;
            at = iv[1];
            if (at >= s1) return true;
        }
        return at >= s1;
    }

    // ---------------------------------------------------------------- expected.json

    ObjectNode expected(long t0, List<Deadline> deadlines, long settleTarget, ObjectNode minimalFacts) {
        return expected(t0, deadlines, settleTarget, minimalFacts, 0);
    }

    /** {@code runStepMs > 0}: the per-sample expectation is written as runs ({@link ExpectedSamples}), for synthesized recordings. */
    ObjectNode expected(long t0, List<Deadline> deadlines, long settleTarget, ObjectNode minimalFacts, long runStepMs) {
        int last = batches - 1;
        Eval fin = evaluate(last);
        ObjectNode e = Json.object();
        Json.putSafe(e, "expectedVersion", 1);
        e.put("scope", "full");
        e.put("mode", rule.mode());
        ArrayNode vs = e.putArray("visits");
        Map<String, Status> finalStatus = new LinkedHashMap<>();
        List<Claim> ordered = new ArrayList<>(fin.claims());
        ordered.sort(Comparator.comparingInt((Claim c) -> facts.containsKey(c.key) ? order(facts.get(c.key).visitRef()) : Integer.MAX_VALUE).thenComparing(c -> c.ref));
        for (Claim c : ordered) {
            Status s = status(fin, c);
            finalStatus.put(c.ref, s);
            ObjectNode o = vs.addObject();
            o.put("visitRef", c.ref);
            ObjectNode key = o.putObject("key");
            if (rule.mode().equals("plc-stamped")) key.put("mode", "plc-stamped").put("namespace", rule.namespace()).put("station", c.station).put("visitId", c.key);
            else key.put("mode", "occupancy-join").put("source", source).put("arrivalEventId", c.key);
            o.put("unitId", c.unit);
            o.put("station", c.station);
            Json.putSafe(o, "enter", c.enter);
            if (c.leave == null) o.putNull("leave"); else Json.putSafe(o, "leave", c.leave);
            if (c.attributionEnd == null) o.putNull("attributionEnd"); else Json.putSafe(o, "attributionEnd", c.attributionEnd);
            ArrayNode tu = o.putArray("toolsUsed");
            VisitFacts f = facts.get(c.key);
            if (f != null) f.tools().forEach(tu::add);
            o.put("finalStatus", s.status());
            ArrayNode ir = o.putArray("insufficiencyReasons");
            s.reasons().forEach(ir::add);
            if (s.retraction() == null) o.putNull("retraction");
            else { ObjectNode r = o.putObject("retraction").put("reason", s.retraction()); if (s.errorReason() == null) r.putNull("errorReason"); else r.put("errorReason", s.errorReason()); }
            ObjectNode ee = o.putObject("equipmentEvidence");
            s.equipment().forEach(ee::set);
        }
        if (runStepMs > 0) {
            e.put("samplesEncoding", ExpectedSamples.RUNS);
            e.set("sampleRuns", ExpectedSamples.sampleRuns(fin.outcomes().entrySet(), runStepMs));
        } else {
            ObjectNode ss = e.putObject("samples");
            for (Map.Entry<Sample, Outcome> o : fin.outcomes().entrySet()) ss.set(o.getKey().key(), expectation(o.getValue()));
        }
        ExpectedSamples.Builder messageRuns = runStepMs > 0 ? new ExpectedSamples.Builder(runStepMs, "messageId", "disposition") : null;
        e.set("dispositions", dispositions(last, messageRuns));
        if (messageRuns != null) e.set("messageDispositionRuns", messageRuns.toJson());
        ObjectNode md = e.putObject("messageMemberDispositions");
        md.put("topic", "ot.messages").put("part", "member:<metric>").put("disposition", "accepted").put("reason", "accepted");
        md.put("appliesTo", "every sampleId of samples{} not listed in dispositions{}; a message of a device outside the rule's catalog is listed by its message identity, part message");
        e.set("incidents", rule.mode().equals("occupancy-join") ? incidents(fin, finalStatus, last) : Json.object());
        ArrayNode tr = e.putArray("transitions");
        for (Deadline d : deadlines) {
            ObjectNode o = tr.addObject();
            o.put("assertion", d.kind());
            o.put("station", d.station());
            o.put("ref", d.ref());
            Json.putSafe(o, "at", d.at());
            Json.putSafe(o, "cutA", d.cutA() + 1L);
            Json.putSafe(o, "cutB", d.cutB() + 1L);
            Json.putSafe(o, "w1A", w1(d.station(), d.cutA()));
            Json.putSafe(o, "w1B", w1(d.station(), d.cutB()));
            if (d.kind().equals("incident-settlement")) {
                o.put("atA", settled(d.ref(), d.station(), d.cutA()));
                o.put("atB", settled(d.ref(), d.station(), d.cutB()));
            } else {
                o.put("atA", statusOf(d.cutA(), d.ref()));
                o.put("atB", statusOf(d.cutB(), d.ref()));
            }
        }
        Json.putSafe(e, "settleTargetW", settleTarget);
        int settled = -1;
        for (int k = 0; k < batches && settled < 0; k++) {
            boolean all = true;
            for (long[] a : w1.values()) if (a[a.length - 1] != Long.MIN_VALUE && a[k] < settleTarget) all = false;   // stations with routed telemetry
            if (all) settled = k;
        }
        if (settled < 0) e.putNull("settledCut"); else Json.putSafe(e, "settledCut", settled + 1L);
        if (minimalFacts != null) e.set("facts", minimalFacts);
        return e;
    }

    static ObjectNode expectation(Outcome o) {
        ObjectNode v = Json.object();
        if (o.attributed() == null) v.putNull("attributedTo"); else v.put("attributedTo", o.attributed().ref);
        v.put("state", o.state());
        if (o.reason() == null) v.putNull("reason"); else v.put("reason", o.reason());
        return v;
    }

    private static int order(String ref) {
        if (ref.startsWith("visit-")) try { return Integer.parseInt(ref.substring(6)); } catch (NumberFormatException e) { return Integer.MAX_VALUE; }
        return Integer.MAX_VALUE;
    }

    // One evaluation is kept (an evaluation of a synthesized recording holds a million outcomes); statuses are remembered per cut and ref.
    private Eval lastEval;
    private final Map<String, String> statusMemo = new HashMap<>();
    private String statusOf(int k, String ref) {
        String key = k + "|" + ref, known = statusMemo.get(key);
        if (known != null) return known;
        if (lastEval == null || lastEval.k() != k) lastEval = evaluate(k);
        String st = "absent";
        for (Claim c : lastEval.claims()) if (c.ref.equals(ref)) st = status(lastEval, c).status();
        statusMemo.put(key, st);
        return st;
    }

    // ---------------------------------------------------------------- dispositions

    /** {@code messageRuns} non-null: the unknown-device message dispositions go there as runs instead of into the object. */
    private ObjectNode dispositions(int last, ExpectedSamples.Builder messageRuns) {
        TreeMap<String, ObjectNode> out = new TreeMap<>();
        for (Delivery d : sessions) {
            ObjectNode v = d.value(), id = Json.object().put("edge", v.get("edge").asText()).put("device", v.get("device").asText());
            Json.putSafe(id, "birthPublishTs", v.get("birthPublishTs").longValue());
            Json.putSafe(id, "bdSeq", v.get("bdSeq").longValue()).put("kind", v.get("kind").asText());
            Json.putSafe(id, "atSeq", v.get("atSeq").longValue());
            entry(out, id, "ot.sessions").with("parts").set("event", outcome("accepted", "accepted"));
        }
        // Occupancy: per eventId, the original's part and the declaration's part; differing contents are a conflict of every delivery.
        Map<String, List<Delivery>> byEvent = new LinkedHashMap<>();
        for (Delivery d : occupancy) byEvent.computeIfAbsent(d.value().get("eventId").asText(), x -> new ArrayList<>()).add(d);
        for (Map.Entry<String, List<Delivery>> e : byEvent.entrySet()) {
            ObjectNode id = Json.object().put("source", e.getValue().get(0).value().get("source").asText()).put("eventId", e.getKey());
            ObjectNode n = entry(out, id, "mfg.occupancy");
            Set<ObjectNode> contents = new LinkedHashSet<>();
            for (Delivery d : e.getValue()) contents.add(base(d.value()));
            long plain = e.getValue().stream().filter(d -> !d.value().has("errorDeclaration")).count();
            if (contents.size() > 1) {
                n.with("parts").set("event", outcome("conflict", "identity-conflict"));
                Json.putSafe(n, "deliveries", e.getValue().size());
                continue;
            }
            Json.putSafe(n, "deliveries", e.getValue().size());
            Delivery firstPlain = e.getValue().stream().filter(d -> !d.value().has("errorDeclaration")).findFirst().orElse(null);
            if (firstPlain != null && !admitted(firstPlain)) n.with("parts").set("event", outcome("replay-required", "beyond-horizon"));
            else if (plain > 0) n.with("parts").set("event", outcome("accepted", lateAfterSettlement(e.getValue(), false) ? "late-after-settlement" : "accepted"));
            if (plain > 1) Json.putSafe(n, "duplicates", plain - 1);
            for (Delivery d : e.getValue()) if (d.value().has("errorDeclaration"))
                n.with("parts").set("errorDeclaration", !admitted(d) ? outcome("replay-required", "correction-beyond-horizon")
                    : outcome("accepted", lateAfterSettlement(List.of(d), true) ? "late-after-settlement" : "accepted"));
        }
        for (Delivery d : incidents) {
            ObjectNode v = d.value(), id = Json.object().put("source", v.get("source").asText()).put("incidentId", v.get("incidentId").asText());
            Json.putSafe(id, "revision", v.get("revision").longValue());
            entry(out, id, "quality.incidents").with("parts").set("event", outcome("accepted", "accepted"));
        }
        ObjectNode unknownDevice = Json.object().put("topic", "ot.messages");
        unknownDevice.with("parts").set("message", outcome("rejected", "unknown-device"));
        for (Sample x : samples) {
            if (catalog.contains(x.device())) continue;
            if (messageRuns != null) {
                messageRuns.add(x.device() + "\u0000" + x.birth() + "\u0000" + x.bdSeq(), () -> {
                    ObjectNode n = Json.object().put("edge", x.edge()).put("device", x.device());
                    Json.putSafe(n, "birthPublishTs", x.birth());
                    return Json.putSafe(n, "bdSeq", x.bdSeq());
                }, x.seq(), x.publishTs(), unknownDevice);
                continue;
            }
            ObjectNode id = (ObjectNode) Json.read(x.key().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            id.remove("metric");
            entry(out, id, "ot.messages").with("parts").set("message", outcome("rejected", "unknown-device"));
        }
        messageDuplicates.forEach((key, n) -> {
            ObjectNode e = Json.object().put("topic", "ot.messages");
            e.with("parts").set("member", outcome("accepted", "accepted"));
            Json.putSafe(e, "deliveries", n + 1L);
            Json.putSafe(e, "duplicates", n);
            out.put(key, e);
        });
        ObjectNode o = Json.object();
        out.forEach(o::set);
        return o;
    }

    private static ObjectNode entry(TreeMap<String, ObjectNode> out, ObjectNode identity, String topic) {
        return out.computeIfAbsent(Canonical.utf8(Canonical.bytes(identity)), x -> Json.object().put("topic", topic));
    }
    private static ObjectNode outcome(String d, String r) { return Json.object().put("disposition", d).put("reason", r); }

    /** An occupancy input is {@code late-after-settlement} when the visit it revises was already reported {@code complete} at the cut
     *  before the one that delivers it (its admission state). */
    private boolean lateAfterSettlement(List<Delivery> ds, boolean declaration) {
        Delivery d = ds.stream().filter(x -> x.value().has("errorDeclaration") == declaration).findFirst().orElse(null);
        if (d == null || d.batch() == 0) return false;
        String arrival = ScenarioBuilder.ARRIVING.equals(d.value().path("bizStep").asText()) ? d.value().get("eventId").asText() : d.value().path("arrivalEventId").asText();
        VisitFacts f = facts.get(arrival);
        String ref = f == null ? arrival : f.visitRef();
        return statusOf(d.batch() - 1, ref).equals("complete");
    }

    // ---------------------------------------------------------------- stage 2 at the last cut

    private record Rev(long revision, String status, long suspectFrom, Long suspectTo, long revisionEventTime, String station, String equipment, String kind) {}

    private ObjectNode incidents(Eval fin, Map<String, Status> finalStatus, int last) {
        Map<String, List<Rev>> byIncident = new TreeMap<>();
        for (Delivery d : incidents) {
            ObjectNode v = d.value();
            if (!rule.incidentScope().contains(v.get("equipmentRef").asText())) continue;
            byIncident.computeIfAbsent(v.get("source").asText() + "\u0000" + v.get("incidentId").asText(), x -> new ArrayList<>())
                .add(new Rev(v.get("revision").longValue(), v.get("status").asText(), v.get("suspectFrom").longValue(),
                    v.get("suspectTo").isNull() ? null : v.get("suspectTo").longValue(), v.get("revisionEventTime").longValue(),
                    v.get("stationRef").asText(), v.get("equipmentRef").asText(), v.get("kind").asText()));
        }
        ObjectNode out = Json.object();
        for (Map.Entry<String, List<Rev>> e : byIncident.entrySet()) {
            String[] k = e.getKey().split("\u0000");
            Rev top = e.getValue().stream().max(Comparator.comparingLong(Rev::revision)).orElseThrow();
            // Row set: every unit with a visit claim at the station related to any delivered revision's interval.
            Map<String, List<Claim>> units = new TreeMap<>();
            for (Claim c : fin.claims()) {
                if (!c.station.equals(top.station())) continue;
                for (Rev r : e.getValue()) if (overlaps(c.enter, c.leave, r.suspectFrom(), r.suspectTo())) { units.computeIfAbsent(c.unit, x -> new ArrayList<>()).add(c); break; }
            }
            ObjectNode inc = Json.object();
            ObjectNode us = inc.putObject("units");
            long provisional = 0, insufficient = 0;
            for (Map.Entry<String, List<Claim>> u : units.entrySet()) {
                String[] j = judge(top, u.getValue(), fin, finalStatus);
                us.putObject(u.getKey()).put("status", j[0]).put("reason", j[1]);
                if (j[1].equals("basis-provisional")) provisional++;
                else if (j[0].equals("insufficient-evidence")) insufficient++;
            }
            if (top.status().equals("withdrawn")) inc.put("resolution", "withdrawn"); else inc.putNull("resolution");
            inc.set("coverage", setCoverage(top, fin, last, provisional, insufficient));
            ObjectNode id = Json.object().put("source", k[0]).put("incidentId", k[1]);
            out.set(Canonical.utf8(Canonical.bytes(id)), inc);
        }
        return out;
    }

    private static boolean overlaps(long enter, Long leave, long sFrom, Long sTo) {
        return (sTo == null || enter < sTo) && (leave == null || sFrom < leave);
    }

    /** The decision table of stage 2 (spec, priorities 1-9) for one unit's bases. */
    private String[] judge(Rev top, List<Claim> bases, Eval fin, Map<String, Status> finalStatus) {
        if (top.status().equals("withdrawn")) return new String[] {"not-affected".equals(rule.onWithdrawn()) ? "not-affected-within-scope" : "insufficient-evidence", "incident-withdrawn"};
        List<Claim> related = new ArrayList<>(), possibly = new ArrayList<>();
        for (Claim c : bases) {
            boolean confirmed = c.leave != null ? overlaps(c.enter, c.leave, top.suspectFrom(), top.suspectTo()) : top.suspectTo() == null || c.enter < top.suspectTo();
            boolean excluded = top.suspectTo() != null && c.enter >= top.suspectTo() || c.leave != null && c.leave <= top.suspectFrom();
            if (c.leave == null && c.enter < top.suspectFrom() && !excluded) { possibly.add(c); continue; }
            if (confirmed && !excluded) related.add(c);
        }
        for (Claim c : related) if (finalStatus.get(c.ref).status().equals("complete") && toolActive(c, top, fin, true)) return new String[] {"affected-candidate", "equipment-activity"};
        if (related.isEmpty() && possibly.isEmpty()) return new String[] {"not-affected-within-scope", "interval-narrowed"};
        for (Claim c : related) if (finalStatus.get(c.ref).reasons().contains("correction-conflict")) return new String[] {"insufficient-evidence", "correction-conflict"};
        List<Claim> both = new ArrayList<>(related); both.addAll(possibly);
        for (Claim c : both) {
            String st = finalStatus.get(c.ref).status();
            if (st.equals("provisional")) return new String[] {"insufficient-evidence", "basis-provisional"};
            if (st.equals("insufficient")) return new String[] {"insufficient-evidence", "basis-insufficient"};
        }
        for (Claim c : related) {
            Status s = finalStatus.get(c.ref);
            if ("overlap".equals(s.retraction())) return new String[] {"insufficient-evidence", "basis-retracted-overlap"};
            if ("input-conflict".equals(s.retraction())) return new String[] {"insufficient-evidence", "basis-retracted-input-conflict"};
            if ("error-declaration".equals(s.retraction()) && Overlays.INCORRECT_DATA.equals(s.errorReason())) return new String[] {"insufficient-evidence", "basis-retracted-error-declaration"};
        }
        for (Claim c : related) if (finalStatus.get(c.ref).status().equals("complete") && toolActive(c, top, fin, false)) return new String[] {"not-affected-within-scope", "no-equipment-activity"};
        boolean allDidNotOccur = !related.isEmpty();
        for (Claim c : related) if (!Overlays.DID_NOT_OCCUR.equals(finalStatus.get(c.ref).errorReason())) allDidNotOccur = false;
        if (allDidNotOccur) return new String[] {"always-insufficient".equals(rule.onBasisRetracted()) ? "insufficient-evidence" : "not-affected-within-scope", "basis-retracted-error-declaration"};
        return new String[] {"insufficient-evidence", "basis-insufficient"};
    }

    /** {@code active}: some accepted true observation of the target tool attributed to the visit inside [enter, leave) ∩ S.
     *  {@code !active}: the whole judged interval is covered by valid inactive support with no unknown or missing slot. */
    private boolean toolActive(Claim c, Rev top, Eval fin, boolean active) {
        Member a = rule.activity().get(top.equipment());
        if (a == null) return false;
        long from = Math.max(c.enter, top.suspectFrom()), to = Math.min(c.leave, top.suspectTo() == null ? Long.MAX_VALUE : top.suspectTo());
        if (to <= from) return false;
        List<Sample> l = new ArrayList<>();
        for (Map.Entry<Sample, Outcome> o : fin.outcomes().entrySet())
            if (o.getValue().attributed() == c && o.getKey().device().equals(a.equipment()) && o.getKey().metric().equals(a.metric())) l.add(o.getKey());
        l.sort(Comparator.comparingLong(Sample::sourceTs));
        if (active) {
            for (Sample x : l) if (x.sourceTs() >= from && x.sourceTs() < to && x.value().isBoolean() && x.value().booleanValue()) return true;
            return false;
        }
        List<long[]> inactive = new ArrayList<>();
        for (int i = 0; i < l.size(); i++) {
            long t = l.get(i).sourceTs(), end = t + rule.period();
            if (i + 1 < l.size()) end = Math.min(end, l.get(i + 1).sourceTs());
            if (l.get(i).value().isBoolean() && !l.get(i).value().booleanValue()) inactive.add(new long[] {t, end});
        }
        return covered(inactive, from, to);
    }

    private boolean settled(String ref, String station, int k) {
        Rev top = null;
        for (Delivery d : incidents) {
            if (d.batch() > k || !d.value().get("incidentId").asText().equals(ref)) continue;
            ObjectNode v = d.value();
            if (top == null || v.get("revision").longValue() > top.revision())
                top = new Rev(v.get("revision").longValue(), v.get("status").asText(), v.get("suspectFrom").longValue(),
                    v.get("suspectTo").isNull() ? null : v.get("suspectTo").longValue(), v.get("revisionEventTime").longValue(), v.get("stationRef").asText(), v.get("equipmentRef").asText(), v.get("kind").asText());
        }
        if (top == null) return false;
        if (top.status().equals("withdrawn")) return true;
        return top.suspectTo() != null && w1(station, k) >= top.suspectTo() + rule.lateness();
    }

    private ObjectNode setCoverage(Rev top, Eval fin, int last, long provisional, long insufficient) {
        ObjectNode c = Json.object();
        Member a = rule.activity().get(top.equipment());
        long w = w1(top.station(), last);
        long lastObs = Long.MIN_VALUE;
        List<Sample> act = new ArrayList<>();
        Map<Sample, Outcome> outcomes = fin.outcomes();
        if (a != null) for (Sample x : samplesByDevice.getOrDefault(a.equipment(), List.of()))
            if (x.metric().equals(a.metric()) && outcomes.containsKey(x)) { act.add(x); lastObs = Math.max(lastObs, x.sourceTs()); }
        long from = top.suspectFrom(), to = top.suspectTo() != null ? top.suspectTo() : lastObs + rule.period();
        TreeSet<Long> unattributed = new TreeSet<>(), known = new TreeSet<>();
        for (Sample x : act) {
            if (x.sourceTs() < from || x.sourceTs() >= to) continue;
            long slot = (x.sourceTs() - from) / rule.period();
            Outcome o = outcomes.get(x);
            boolean accepted = !bad(x.status()) && !uncertain(x.status()) && !"session-ambiguous".equals(o.reason()) && !"outside-session".equals(o.reason());
            if (accepted) known.add(slot);
            if (accepted && x.value().isBoolean() && x.value().booleanValue() && "unattributed".equals(o.state())) unattributed.add(slot);
        }
        long slots = to > from ? (to - from + rule.period() - 1) / rule.period() : 0;
        long unknown = slots - known.size();
        long gapOverlaps = 0;
        if (a != null) for (long[] g : fin.sessions().gaps().getOrDefault(a.equipment(), List.of())) if (g[0] < to && from < g[1]) gapOverlaps++;
        boolean open = top.status().equals("open"), withdrawn = top.status().equals("withdrawn");
        boolean settled = withdrawn || top.suspectTo() != null && w >= top.suspectTo() + rule.lateness();
        boolean beyondRetention = top.suspectFrom() < top.revisionEventTime() - rule.workRecordsMs();
        long[] ev = a == null ? null : evidence.get(a.equipment());
        long need = top.suspectTo() != null ? top.suspectTo() : w;
        boolean retentionGap = ev == null || !(ev[0] <= top.suspectFrom() && need <= ev[1]);
        String status;
        if (unknown > 0) status = "unknown";
        else if (withdrawn || unattributed.size() > 0 || provisional > 0 || insufficient > 0 || gapOverlaps > 0 || open || !settled || beyondRetention || retentionGap) status = "incomplete";
        else status = "complete";
        c.put("status", status);
        Json.putSafe(c, "unattributedActivityPeriods", unattributed.size());
        Json.putSafe(c, "unknownActivityPeriods", unknown);
        Json.putSafe(c, "provisionalUnits", provisional);
        Json.putSafe(c, "insufficientUnits", insufficient);
        Json.putSafe(c, "sessionGapOverlaps", gapOverlaps);
        c.put("open", open);
        c.put("settled", settled);
        c.put("beyondRetention", beyondRetention);
        c.put("retentionGap", retentionGap);
        return c;
    }

    private static IllegalArgumentException refuse(String what) { return new IllegalArgumentException("scenario refused: " + what); }
}
