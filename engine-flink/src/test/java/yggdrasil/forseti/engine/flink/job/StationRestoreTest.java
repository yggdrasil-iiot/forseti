package yggdrasil.forseti.engine.flink.job;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.generation.FileGenerationStore;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.ScenarioBuilder;
import yggdrasil.forseti.cli.replay.CutBarrierRecord;
import yggdrasil.forseti.cli.replay.EngineContract;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.cli.replay.RecoveryControlStore;
import yggdrasil.forseti.cli.replay.RunPreparation;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.stage1.Events;
import yggdrasil.forseti.core.stage1.Routing;

/**
 * The station operators' segmented state across a snapshot and a restore (review of step 6 PR 5, item 9), with Flink's own operator
 * test harness: the shared baseline recording (16 visits, 73 cuts) is fed to the stage-1 station operator of subtask 1 (station1 and station2, two keys) as the
 * router would feed it (records, per-record telemetry watermarks, barriers), and the stage-1 output of those stations to the stage-2
 * station operator with the incident barriers one cut late. One run goes through uninterrupted; the other is snapshotted at checkpoints
 * 1 and 2, the second in the middle of a batch (held inputs in the assembler, registered timers, queued stage-2 evidence and open lines,
 * segments removed since the first), and restored into a fresh operator from checkpoint 2, which goes on with the rest of the input.
 * The outputs of the interrupted run (before the snapshot, then after the restore) are byte-equal to the uninterrupted run's, except the
 * {@code initEvidence} of each writer's first progress after the restore (the restore evidence of a reinitialized writer, by design), and
 * every station's state, assembler and engine head end equal. The restore goes through both {@code Station(name, stored)} constructors,
 * the backend's key enumeration and the reload of the {@code evidence/} and {@code open/} segments.
 */
class StationRestoreTest {
  static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
  static final int SUBTASK = 1, PARALLELISM = 6;
  @TempDir Path tmp;
  private java.util.function.Function<JobSettings, RecoveryControlStore.Snapshot> control;
  private java.util.function.Function<JobSettings, ObjectNode> reader;

  @BeforeEach void seams() { control = Writers.CONTROL; reader = Writers.CONTROL_READER; }
  @AfterEach void restore() { Writers.CONTROL = control; Writers.CONTROL_READER = reader; }

  /** One element of the stage-1 feed: input 1 or 2, or a watermark of input 1. */
  record Feed(int input, Msg msg, Long watermark) {}

  JobSettings settings(int stage, Recording rec, Generation g) {
    ObjectNode spec = Json.object().put("stage", stage).put("bootstrap", "localhost:1").put("prefix", "t.");
    Json.putSafe(spec, "generation", g.generation());
    spec.put("mappings", ROOT.resolve("fixtures/live/registry/mappings").toString());
    spec.set("definitions", rec.manifest().path("definitions"));
    spec.set("routing", rec.routing().node());
    ObjectNode er = spec.putObject("evidenceRanges");
    rec.evidenceRange().forEach((d, r) -> er.putArray(d).add(r.from()).add(r.to()));
    spec.put("runId", "restore-test").put("artifactSha256", "unpacked").put("evidenceDir", tmp.resolve("evidence-" + stage + "-" + System.nanoTime()).toString());
    spec.putNull("negativeControl");
    spec.put("causeKind", "input");
    Json.putSafe(spec, "transactionTimeoutMs", 60_000);
    return JobSettings.build(spec, g, Json.object().put("isolation.level", "read_committed"));
  }

  @Test void bothStationOperatorsRestoreFromSegmentsToTheUninterruptedRun() throws Exception {
    Path recDir = tmp.resolve("recording"), state = tmp.resolve("state");
    Path scenario = ROOT.resolve("engine-fixtures/scenarios/shared/baseline.json"), registry = ROOT.resolve("fixtures/live/registry");
    ScenarioBuilder.build(scenario, registry, recDir);
    Recording rec = Recording.open(recDir);
    JsonNode rule = Json.read(scenario).path("rule");
    String stream = rule.path("stream").asText();
    JsonNode statement = Json.read(Base64.getDecoder().decode(Json.read(registry.resolve("mappings/head-attestations/" + stream + ".dsse.json")).get("payload").asText()));
    Clock clock = Clock.fixed(Instant.parse(statement.path("predicate").path("signedAt").asText()).plusSeconds(60), ZoneOffset.UTC);
    ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
    int exit = GenerationCli.run(new String[] {"new", "--state", state.toString(), "--mappings", registry.resolve("mappings").toString(), "--trusted-keys",
        ROOT.resolve("fixtures/live/trusted-keys.jsonl").toString(), "--stream", stream, "--rule", rule.path("attributionRuleId").asText() + "@" + rule.path("attributionRuleVersion").asText(),
        "--recording", recDir.toString()}, new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8), clock);
    assertEquals(0, exit, o.toString(StandardCharsets.UTF_8) + e.toString(StandardCharsets.UTF_8));
    Generation g;
    try (FileGenerationStore store = new FileGenerationStore(state)) { g = store.registered(1).orElseThrow().row(); }
    String digest = rec.routing().routingDigest();
    Writers.CONTROL = s -> new RecoveryControlStore.Snapshot(List.of(new RecoveryControlStore.Declaration(0, RunPreparation.initialDeclaration(1, digest))), 1);
    Writers.CONTROL_READER = s -> Json.object().put("isolation.level", "read_committed");
    JobSettings s1 = settings(1, rec, g), s2 = settings(2, rec, g);
    JobSettings.Runtime rt = s1.runtime();
    Set<String> mine = new TreeSet<>();
    for (String st : s1.stations()) {
      int kg = KeyGroupRangeAssignment.assignToKeyGroup(st, JobSettings.MAX_PARALLELISM);
      if (KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(JobSettings.MAX_PARALLELISM, PARALLELISM, kg) == SUBTASK) mine.add(st);
    }
    assertEquals(2, mine.size(), "two stations on subtask " + SUBTASK + ": " + mine);

    // ---------------------------------------------------------------- stage 1
    List<Feed> feed = new ArrayList<>();
    List<Integer> batchEnds = new ArrayList<>(), batchRecordsEnd = new ArrayList<>();
    Map<Integer, Long> maxTs = new TreeMap<>();
    long lastW = Long.MIN_VALUE;
    Map<String, Long> next = new HashMap<>();
    long oo = rt.outOfOrdernessMs;
    for (Recording.Batch b : rec.batches()) {
      for (String t : EngineContract.STAGE1_TOPICS) for (int p : s1.subscribed(t)) {
        long through = b.through(t, p);
        try (Recording.Reader r = rec.reader(t, p)) {
          for (Recording.Line l = r.next(); l != null; l = r.next()) {
            long off = l.record().offset();
            if (off < next.getOrDefault(t + "/" + p, 0L)) continue;
            if (off > through) break;
            next.put(t + "/" + p, off + 1);
            byte[] value = Recording.valueBytes(l.bytes());
            Msg m = Msg.record(t, p, off, l.record().key() == null ? null : l.record().key().getBytes(StandardCharsets.UTF_8), value);
            InputParser.Read read = InputParser.read(new Inputs.Origin(t, p, off), m.keyText(), value);
            if (t.equals(InputParser.MESSAGES) && !read.rejected()) {
              Long ts = Events.eventTime(read.record());
              if (ts != null) maxTs.merge(p, ts, Math::max);
            }
            if (rt.router.route(read) instanceof Routing.Routed x && mine.contains(x.station())) feed.add(new Feed(t.equals(InputParser.MESSAGES) ? 1 : 2, m.to(x.station()), null));
            if (t.equals(InputParser.MESSAGES) && maxTs.size() == s1.subscribed(t).size()) {
              long w = Collections.min(maxTs.values()) - oo - 1;
              if (w > lastW) { feed.add(new Feed(1, null, w)); lastW = w; }
            }
          }
        }
      }
      batchRecordsEnd.add(feed.size());
      for (String t : EngineContract.STAGE1_TOPICS) for (int p : s1.subscribed(t)) {
        CutAssembler.Barrier br = new CutAssembler.Barrier(b.cutId(), new Progress.Origin(t, p), b.through(t, p), 0, null, EngineContract.idleInputs(b));
        Msg m = Msg.record(t, p, -1, null, Canonical.bytes(new CutBarrierRecord(g.generation(), digest, br).toJson()));
        m.kind = Msg.BARRIER;
        for (String st : mine) if (rt.required.get(st).contains(br.origin())) feed.add(new Feed(t.equals(InputParser.MESSAGES) ? 1 : 2, m.to(st), null));
      }
      batchEnds.add(feed.size());
    }
    int first = batchEnds.get(19), second = batchRecordsEnd.get(49);   // checkpoint 1 after batch 20, checkpoint 2 inside batch 50

    Stage1Run a = new Stage1Run(s1);
    a.feed(feed, 0, feed.size());
    List<String> outA = lines(a.h.extractOutputValues());
    Map<String, JsonNode> endA = a.inspect(mine);

    Stage1Run b1 = new Stage1Run(s1);
    b1.feed(feed, 0, first);
    b1.h.snapshot(1, 1);
    b1.feed(feed, first, second);
    Map<String, JsonNode> atSnapshot = b1.inspect(mine);
    OperatorSubtaskState snap = b1.h.snapshot(2, 2);
    JsonNode segs = evidence(s1, "stage1-stations-" + SUBTASK + "-l0-cnone-a0").path("segments");
    List<String> outB = lines(b1.h.extractOutputValues());
    b1.h.close();
    boolean held = false, timers = false;
    for (JsonNode x : atSnapshot.values()) {
      held |= x.path("assembler").toString().contains("\"held\":{\"") || x.path("assembler").path("held").size() > 0;
      timers |= x.path("head").path("timerAt").size() > 0;
    }
    assertTrue(held, "held inputs in an assembler at checkpoint 2: " + atSnapshot);
    assertTrue(timers, "registered timers at checkpoint 2");
    assertTrue(segs.path("removed").asLong() > 0, "a segment removed before checkpoint 2: " + segs);
    assertTrue(segs.path("snapshots").asLong() >= 2, segs.toString());

    Stage1Run b2 = new Stage1Run(s1, snap);
    // Flink restores no watermark (engine assumption F4): the restored operator starts at Long.MIN_VALUE. The watermark it had at the
    // snapshot is fed again before the rest, as the restored sources' first records would re-establish it; this test is about the
    // persisted state, and the watermark after a restart is the restart injections' (PR 6)
    Long before = null;
    for (int i = 0; i < second; i++) if (feed.get(i).watermark() != null) before = feed.get(i).watermark();
    b2.h.processWatermark1(new Watermark(before));
    b2.feed(feed, second, feed.size());
    List<String> after = lines(b2.h.extractOutputValues());
    outB.addAll(after);
    assertTrue(after.stream().anyMatch(l -> l.contains("initEvidence") && l.contains("isRestored\\\":true")), "a recovered first progress carries the restore evidence");
    assertSame("stage-1 outputs, the uninterrupted run against the restored one", strip(outA), strip(outB));
    assertEquals(canonical(endA), canonical(b2.inspect(mine)), "stage-1 state, assembler and engine head at the end (canonical form)");
    b2.h.close();
    a.h.close();

    // ---------------------------------------------------------------- stage 2, on the stage-1 output of those stations
    List<Msg> feed2 = new ArrayList<>();
    List<Integer> cutEnds = new ArrayList<>();
    Map<String, String> contract = s2.contractOfRunName();
    Map<Integer, String> stationOf = new HashMap<>();
    for (String st : mine) stationOf.put(rt.partitionOf(st), st);
    long offset = 0, pendingCut = -1;
    ObjectNode through = Json.object();
    for (int q = 0; q < 6; q++) Json.putSafe(through, Integer.toString(q), -1L);
    for (Out x : a.outputs) {
      String t = contract.get(x.topic);
      if (!t.equals("forseti.work-records") && !t.equals("forseti.samples")) continue;
      Msg m = Msg.record(t, x.partition, offset++, x.key, x.value);
      m.generationHeader = x.generationHeader;
      m.station = stationOf.get(x.partition);
      feed2.add(m);
      JsonNode v = Json.read(x.value);
      if (t.equals("forseti.samples") && "progress".equals(v.path("type").asText()) && m.station.equals(mine.iterator().next())) {
        long cut = v.path("cutId").asLong();
        // the incident barriers of the previous cut, one cut late: the evidence of this cut waits queued
        if (pendingCut >= 0) for (String st : mine) feed2.add(incidentCut(st, pendingCut, through));
        pendingCut = cut;
        cutEnds.add(feed2.size());
      }
    }
    for (String st : mine) feed2.add(incidentCut(st, pendingCut, through));
    int f1 = cutEnds.get(19), f2 = cutEnds.get(49) - 3;   // checkpoint 2 with queued evidence and open lines

    Stage2Run c = new Stage2Run(s2);
    c.feed(feed2, 0, feed2.size());
    List<String> outC = lines(c.h.extractOutputValues());
    Map<String, JsonNode> endC = c.inspect(mine);
    Stage2Run d1 = new Stage2Run(s2);
    d1.feed(feed2, 0, f1);
    d1.h.snapshot(1, 1);
    d1.feed(feed2, f1, f2);
    Map<String, JsonNode> at2 = d1.inspect(mine);
    OperatorSubtaskState snap2 = d1.h.snapshot(2, 2);
    JsonNode segs2 = evidence(s2, "stage2-stations-" + SUBTASK + "-l0-cnone-a0").path("segments");
    List<String> outD = lines(d1.h.extractOutputValues());
    d1.h.close();
    boolean queued = false, open = false;
    for (JsonNode x : at2.values()) {
      for (JsonNode q : x.path("queued")) queued |= q.size() > 0;
      for (JsonNode op : x.path("open")) open |= op.path("lines").size() > 0;
    }
    assertTrue(queued, "queued evidence at checkpoint 2: " + at2);
    assertTrue(open, "open lines at checkpoint 2");
    assertTrue(segs2.path("removed").asLong() > 0, "an evidence segment removed before checkpoint 2: " + segs2);
    Stage2Run d2 = new Stage2Run(s2, snap2);
    d2.feed(feed2, f2, feed2.size());
    outD.addAll(lines(d2.h.extractOutputValues()));
    assertSame("stage-2 outputs, the uninterrupted run against the restored one (byte for byte)", outC, outD);
    assertEquals(canonical(endC), canonical(d2.inspect(mine)), "stage-2 state, head, open and queued evidence at the end (canonical form)");
    d2.h.close();
    c.h.close();
  }

  /** Equal lists, or the first difference in the message. */
  static void assertSame(String what, List<String> want, List<String> have) {
    for (int i = 0; i < Math.min(want.size(), have.size()); i++)
      if (!want.get(i).equals(have.get(i))) fail(what + ": line " + i + " of " + want.size() + "/" + have.size() + " WANT " + want.get(i) + " HAVE " + have.get(i));
    assertEquals(want.size(), have.size(), what + ": line counts");
  }

  /** Station to the RFC 8785 text of its inspection (a restored state's maps come back in sorted order). */
  static Map<String, String> canonical(Map<String, JsonNode> m) {
    Map<String, String> c = new TreeMap<>();
    m.forEach((k, v) -> c.put(k, Canonical.utf8(Canonical.bytes(v))));
    return c;
  }

  static Msg incidentCut(String station, long cut, ObjectNode through) {
    Msg m = new Msg();
    m.kind = Msg.INCIDENT_CUT;
    m.station = station;
    m.topic = InputParser.INCIDENTS;
    m.cutId = cut;
    m.value = Canonical.bytes(through);
    return m;
  }

  JsonNode evidence(JobSettings s, String name) throws Exception { return Json.read(s.evidenceDir().resolve(name + ".json")); }

  /** An output record as text: its topic, partition, key and value bytes. */
  static List<String> lines(Collection<Out> outs) {
    List<String> l = new ArrayList<>();
    for (Out x : outs) l.add(x.topic + "|" + x.partition + "|" + x.generationHeader + "|" + (x.key == null ? "" : new String(x.key, StandardCharsets.UTF_8)) + "|"
        + new String(x.value, StandardCharsets.UTF_8));
    return l;
  }

  /** The lines with the {@code initEvidence} of progress records removed (a reinitialized writer's first progress names its restore). */
  static List<String> strip(List<String> l) {
    List<String> out = new ArrayList<>();
    for (String s : l) out.add(s.replaceAll(",\"initEvidence\":\\{\"declarationOffset\":[0-9]+,\"endOffset\":[0-9]+,\"restore\":\"(?:\\\\.|[^\"\\\\])*\"\\}", ""));
    return out;
  }

  static final KeySelector<Msg, String> BY_STATION = m -> m.station;

  final class Stage1Run {
    final Stage1Stations op;
    final KeyedTwoInputStreamOperatorTestHarness<String, Msg, Msg, Out> h;
    final List<Out> outputs = new ArrayList<>();

    Stage1Run(JobSettings s) throws Exception { this(s, null); }

    Stage1Run(JobSettings s, OperatorSubtaskState from) throws Exception {
      op = new Stage1Stations(s);
      h = new KeyedTwoInputStreamOperatorTestHarness<>(op, BY_STATION, BY_STATION, Types.STRING, JobSettings.MAX_PARALLELISM, PARALLELISM, SUBTASK);
      h.setCheckpointStorage(new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage(256 * 1024 * 1024));
      h.setup();
      if (from != null) h.initializeState(from);
      h.open();
    }

    void feed(List<Feed> feed, int from, int to) throws Exception {
      for (int i = from; i < to; i++) {
        Feed f = feed.get(i);
        if (f.watermark() != null) h.processWatermark1(new Watermark(f.watermark()));
        else if (f.input() == 1) h.processElement1(new StreamRecord<>(f.msg()));
        else h.processElement2(new StreamRecord<>(f.msg()));
      }
      outputs.clear();
      outputs.addAll(h.extractOutputValues());
    }

    Map<String, JsonNode> inspect(Set<String> stations) {
      Map<String, JsonNode> m = new TreeMap<>();
      for (String s : stations) m.put(s, op.inspect(s));
      return m;
    }
  }

  final class Stage2Run {
    final Stage2Stations op;
    final KeyedOneInputStreamOperatorTestHarness<String, Msg, Out> h;

    Stage2Run(JobSettings s) throws Exception { this(s, null); }

    Stage2Run(JobSettings s, OperatorSubtaskState from) throws Exception {
      op = new Stage2Stations(s);
      h = new KeyedOneInputStreamOperatorTestHarness<>(op, BY_STATION, Types.STRING, JobSettings.MAX_PARALLELISM, PARALLELISM, SUBTASK);
      h.setCheckpointStorage(new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage(256 * 1024 * 1024));
      h.setup();
      if (from != null) h.initializeState(from);
      h.open();
    }

    void feed(List<Msg> feed, int from, int to) throws Exception { for (int i = from; i < to; i++) h.processElement(new StreamRecord<>(feed.get(i))); }

    Map<String, JsonNode> inspect(Set<String> stations) {
      Map<String, JsonNode> m = new TreeMap<>();
      for (String s : stations) m.put(s, op.inspect(s));
      return m;
    }
  }

  static OutputRecords.Kind kind(byte[] value) { return OutputRecords.kind(Json.read(value)); }
}
