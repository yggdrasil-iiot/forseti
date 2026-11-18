package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import yggdrasil.forseti.cli.gate.RunFold.Disp;
import yggdrasil.forseti.cli.gate.RunFold.Findings;
import yggdrasil.forseti.cli.gate.RunFold.Mark;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.W1Form;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.rules.RuleSet;
import yggdrasil.forseti.core.stage1.Events;

/** The integrity checks of a run that need the recording and the cut log beside the journals (plan Task 19; the journal-only checks are
 *  made by {@link RunFold} while it folds). Recorded into the fold's {@link Findings} under the assertion ids that aggregate them:
 *  {@code J-DISPOSITION-COVERAGE} (every delivered input has a disposition; every member of a message its {@code member:<metric>} part
 *  unless the message is judged as a whole, part {@code message}), {@code J-W1-FORMULA} (S10: {@code W1(s)} is the largest member
 *  {@code sourceTs} routed to {@code s} through the cut minus {@code outOfOrdernessMs} minus 1, never decreasing, {@code -(2^53 - 1)}
 *  before the first; {@code admissionW} is the previous cut's {@code W1}; the stage-1 progress records report both) and
 *  {@code J-W2-EQUALS-W1} (the stage-2 progress {@code W2} is the cut's {@code W1}, with six {@code incidentOffsets}). It also measures
 *  item 2's reference value: the longest lateness, {@code admissionW - sourceTs}, of a sample the core accepted online. */
final class Integrity {
    private Integrity() {}

    /** Item 2 of one run: the longest lateness of an accepted sample and where; the deliveries without a disposition. */
    record Measures(long longestLatenessMs, String longestAt, long acceptedLateSamples, long inputsWithoutDisposition, long deliveries) {}

    /** {@code subscribed}: {@code run.json}'s subscriptions (topic -> partitions); {@code stationPartitions}: station -> partition. Without
     *  {@code rules} only the disposition coverage of the delivered offsets is checked (the unit tests). */
    static Measures check(RunFold fold, Path recording, JsonNode cutLog, Map<String, List<Integer>> subscribed, RuleSet rules, JsonNode definitions,
                          Map<String, Integer> stationPartitions) throws IOException {
        return check(fold, recording, cutLog, subscribed, rules, definitions, stationPartitions, W1Form.PER_STATION);
    }

    /** {@code form}: the engine's W1 formula (spec 620): {@link W1Form#PER_STATION} (the reference harness, Kafka Streams) or
     *  {@link W1Form#MIN_OVER_PARTITIONS} (Flink: the minimum over the subscribed raw telemetry partitions of the largest {@code sourceTs}
     *  delivered on that partition through the cut, every delivered message that parses counted whatever station or reject collector it
     *  then goes to, minus {@code outOfOrdernessMs} minus 1); {@code J-W1-FORMULA} asserts the reported {@code W1} against it. */
    static Measures check(RunFold fold, Path recording, JsonNode cutLog, Map<String, List<Integer>> subscribed, RuleSet rules, JsonNode definitions,
                          Map<String, Integer> stationPartitions, W1Form form) throws IOException {
        Findings f = fold.findings;
        Recording rec = form == W1Form.MIN_OVER_PARTITIONS ? Recording.open(recording) : null;
        Map<Integer, Recording.Reader> clockReaders = new HashMap<>();
        Map<Integer, Long> clockNext = new HashMap<>(), partitionMax = new HashMap<>();
        List<Integer> telemetry = subscribed.getOrDefault("ot.messages", List.of());
        // every delivery's parts and judgements: "topic/partition/offset" -> part -> disposition
        Map<String, Map<String, String>> parts = new HashMap<>();
        for (Disp d : fold.dispositions()) parts.computeIfAbsent(d.topic() + "/" + d.partition() + "/" + d.offset(), k -> new HashMap<>()).put(d.part(), d.disposition());
        long ooo = rules == null ? 0 : rules.attribution().outOfOrdernessMs();
        Map<String, Long> maxTs = new HashMap<>(), w1 = new HashMap<>();
        Map<String, Long> previousW1 = new HashMap<>();
        Map<Integer, BufferedReader> readers = new HashMap<>();
        Map<Integer, Long> nextOffset = new HashMap<>();
        long without = 0, deliveries = 0, longest = Long.MIN_VALUE, acceptedLate = 0;
        String longestAt = null;
        try {
            for (JsonNode cut : cutLog.path("cuts")) {
                long cutId = cut.path("cutId").asLong();
                JsonNode per = cut.path("perStation");
                Iterator<Map.Entry<String, JsonNode>> ranges = cut.path("offsetRanges").fields();
                while (ranges.hasNext()) {
                    Map.Entry<String, JsonNode> t = ranges.next();
                    String topic = t.getKey();
                    Iterator<Map.Entry<String, JsonNode>> ps = t.getValue().fields();
                    while (ps.hasNext()) {
                        Map.Entry<String, JsonNode> pr = ps.next();
                        int partition = Integer.parseInt(pr.getKey());
                        if (!subscribed.getOrDefault(topic, List.of()).contains(partition)) continue;
                        long from = pr.getValue().get(0).asLong(), to = pr.getValue().get(1).asLong();
                        for (long off = from; off < to; off++) {
                            deliveries++;
                            Map<String, String> have = parts.get(topic + "/" + partition + "/" + off);
                            if (have == null) { without++; f.add("J-DISPOSITION-COVERAGE", "cut " + cutId + " " + topic + "/" + partition + "/" + off + ": no disposition"); }
                            if (!topic.equals("ot.messages") || rules == null) continue;
                            JsonNode v = message(readers, nextOffset, recording, partition, off);
                            List<String> metrics = new ArrayList<>();
                            Long ts = null;
                            for (JsonNode m : v.path("members")) {
                                metrics.add(m.path("metric").asText());
                                if (m.path("sourceTs").isIntegralNumber()) ts = ts == null ? m.get("sourceTs").asLong() : Math.max(ts, m.get("sourceTs").asLong());
                            }
                            if (have != null && !have.containsKey("message"))
                                for (String m : metrics) if (!have.containsKey("member:" + m)) f.add("J-DISPOSITION-COVERAGE", "cut " + cutId + " ot.messages/" + partition + "/" + off + ": member " + m + " has no disposition");
                            String device = v.path("device").asText();
                            Optional<String> station = rules.members().equipment().contains(device) ? rules.hierarchy().stationOf(rules.attribution().stations(), device) : Optional.empty();
                            boolean routed = station.isPresent() && ts != null && definitions.path(device).path("sha256").asText("").equals(v.path("definitionSha256").asText());
                            if (!routed) continue;
                            maxTs.merge(station.get(), ts, Math::max);
                            JsonNode adm = per.path(station.get()).path("admissionW");
                            if (have == null || !adm.isIntegralNumber()) continue;
                            for (JsonNode m : v.path("members")) {
                                if (!m.path("sourceTs").isIntegralNumber() || !"accepted".equals(have.get("member:" + m.path("metric").asText()))) continue;
                                long late = adm.asLong() - m.get("sourceTs").asLong();
                                if (late > 0) acceptedLate++;
                                if (late > longest) { longest = late; longestAt = "cut " + cutId + " ot.messages/" + partition + "/" + off + " " + device.substring(device.lastIndexOf(':') + 1) + " " + m.path("metric").asText()
                                    + " sourceTs " + m.get("sourceTs").asLong() + ", admissionW " + adm.asLong(); }
                            }
                        }
                    }
                }
                Long minOver = null;
                if (rec != null) {
                    // the min-over-partitions form: every message of each subscribed raw telemetry partition through the cut, parsed as the
                    // engine's source parses it (a value that does not parse, or a message without a valid member sourceTs, advances nothing)
                    JsonNode tr = cut.path("offsetRanges").path("ot.messages");
                    for (int p : telemetry) {
                        JsonNode range = tr.path(Integer.toString(p));
                        if (!range.isArray()) continue;
                        long to = range.get(1).asLong();
                        Recording.Reader reader = clockReaders.computeIfAbsent(p, x -> rec.reader("ot.messages", x));
                        for (long off = clockNext.getOrDefault(p, 0L); off < to; off++) {
                            Recording.Line l = reader.next();
                            if (l == null || l.record().offset() != off) throw new IllegalStateException("ot.messages/" + p + " is not at offset " + off);
                            InputParser.Read read = InputParser.read(new Inputs.Origin("ot.messages", p, off), l.record().key(), Recording.valueBytes(l.bytes()));
                            Long t = read.rejected() ? null : Events.eventTime(read.record());
                            if (t != null) partitionMax.merge(p, t, Math::max);
                            clockNext.put(p, off + 1);
                        }
                    }
                    long m = Long.MAX_VALUE;
                    for (int p : telemetry) { Long t = partitionMax.get(p); if (t == null) { m = Long.MIN_VALUE; break; } m = Math.min(m, t); }
                    minOver = m == Long.MIN_VALUE || telemetry.isEmpty() ? -Json.MAX_SAFE : m - ooo - 1;
                }
                for (String s : rules == null ? List.<String>of() : stationPartitions.keySet()) {
                    JsonNode x = per.path(s);
                    if (rec != null && !x.isObject()) continue;   // the station did not complete this cut (it is reported at the cut that merges it)
                    long formula = minOver != null ? minOver : maxTs.containsKey(s) ? maxTs.get(s) - ooo - 1 : -Json.MAX_SAFE;
                    long want = Math.max(formula, w1.getOrDefault(s, -Json.MAX_SAFE));
                    w1.put(s, want);
                    if (x.path("W1").asLong() != want) f.add("J-W1-FORMULA", "cut " + cutId + " " + s + ": cuts.json W1 " + x.path("W1") + ", the formula " + want);
                    Long prev = previousW1.get(s);
                    if (prev == null ? !x.path("admissionW").isNull() : x.path("admissionW").asLong() != prev)
                        f.add("J-W1-FORMULA", "cut " + cutId + " " + s + ": admissionW " + x.path("admissionW") + ", the previous W1 " + prev);
                    if (x.has("W2") && x.path("W2").asLong() != x.path("W1").asLong()) f.add("J-W2-EQUALS-W1", "cut " + cutId + " " + s + ": cuts.json W2 " + x.path("W2") + " != W1 " + x.path("W1"));
                    previousW1.put(s, x.path("W1").asLong());
                }
            }
        } finally { for (BufferedReader r : readers.values()) r.close(); for (Recording.Reader r : clockReaders.values()) r.close(); }
        // the progress records against the cut log
        Map<Long, JsonNode> byCut = new HashMap<>();
        for (JsonNode cut : cutLog.path("cuts")) byCut.put(cut.path("cutId").asLong(), cut.path("perStation"));
        Map<Integer, String> stationOf = new HashMap<>();
        stationPartitions.forEach((s, p) -> stationOf.put(p, s));
        for (Map.Entry<String, List<Mark>> e : fold.marks().entrySet()) {
            String topic = e.getKey().substring(0, e.getKey().lastIndexOf('/'));
            int partition = Integer.parseInt(e.getKey().substring(e.getKey().lastIndexOf('/') + 1));
            String s = stationOf.get(partition);
            if (s == null || rules == null || RunFold.LEDGERS.contains(topic)) continue;
            boolean stage2 = topic.equals(RunFold.AFFECTED) || topic.equals(RunFold.SETS);
            Long prev = null;
            for (Mark m : e.getValue()) {
                JsonNode x = byCut.getOrDefault(m.cutId(), Json.object()).path(s);
                if (stage2) {
                    if (m.w() == null || m.w() != x.path("W1").asLong()) f.add("J-W2-EQUALS-W1", e.getKey() + " cut " + m.cutId() + ": W2 " + m.w() + ", the cut's W1 " + x.path("W1"));
                    if (m.incidentOffsets() != 6) f.add("J-W2-EQUALS-W1", e.getKey() + " cut " + m.cutId() + ": " + m.incidentOffsets() + " incidentOffsets, not 6");
                } else {
                    if (m.w() == null || m.w() != x.path("W1").asLong()) f.add("J-W1-FORMULA", e.getKey() + " cut " + m.cutId() + ": progress W1 " + m.w() + ", cuts.json " + x.path("W1"));
                    if (!Objects.equals(prev, m.admissionW())) f.add("J-W1-FORMULA", e.getKey() + " cut " + m.cutId() + ": progress admissionW " + m.admissionW() + ", the previous W1 " + prev);
                    prev = m.w();
                }
            }
        }
        return new Measures(longest == Long.MIN_VALUE ? 0 : longest, longestAt, acceptedLate, without, deliveries);
    }

    /** The value of {@code ot.messages/<partition>} at {@code offset}, read forward through the recording file. */
    private static JsonNode message(Map<Integer, BufferedReader> readers, Map<Integer, Long> next, Path recording, int partition, long offset) throws IOException {
        BufferedReader r = readers.get(partition);
        if (r == null) { r = Files.newBufferedReader(recording.resolve("ot.messages").resolve(partition + ".jsonl"), StandardCharsets.UTF_8); readers.put(partition, r); next.put(partition, 0L); }
        while (true) {
            long at = next.get(partition);
            String line = r.readLine();
            if (line == null) throw new IllegalStateException("ot.messages/" + partition + " ends before offset " + offset);
            next.put(partition, at + 1);
            if (at < offset) continue;
            if (at > offset) throw new IllegalStateException("ot.messages/" + partition + " read past offset " + offset);
            return Json.read(line.getBytes(StandardCharsets.UTF_8)).path("value");
        }
    }
}
