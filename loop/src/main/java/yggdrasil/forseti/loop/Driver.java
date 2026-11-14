package yggdrasil.forseti.loop;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import java.util.function.Consumer;
import yggdrasil.forseti.cli.live.DrivePlan;
import yggdrasil.forseti.cli.live.IncidentRecords;
import yggdrasil.forseti.cli.live.LiveEquipment;
import yggdrasil.forseti.cli.live.LiveScenario;
import yggdrasil.forseti.cli.live.OccupancyRecords;
import yggdrasil.forseti.core.Json;

/** The step-5 scenario driver: carries out a {@link DrivePlan} against the sim and sends the plan's occupancy and incident events.
 *
 *  <p>For each step, at its scheduled offset from the start (or later, when the previous step is still waiting for the sim), the driver
 *  writes every snapshot as ONE Write request (all members' {@code .Value} and {@code .StatusCode} of one station or tool), so the sim
 *  commits the whole equipment in one scan. It then waits for the scan that committed it and takes THAT scan's source timestamp as the
 *  time of the step's events: never the driver's clock and never a ServerTimestamp, because the telemetry the core joins against is
 *  dated by the same source timestamps. {@code recordTime}, which is diagnostic, is the driver's clock at the send.
 *
 *  <p>How the committing scan is identified. Before the write, the driver reads the equipment's live members in one Read; a read whose
 *  members carry unequal source timestamps straddled a scan and is repeated. After the write it polls until a coherent read shows a
 *  newer source timestamp with exactly the written values and status codes (a Bad member's value is not compared: the status is what
 *  the step wrote). The write follows the first read, so every scan whose timestamp the first read or a later non-matching read
 *  showed came before the commit, and the commit is the first scan after the last of them. The driver accepts the matching timestamp
 *  only when it lies less than 1.5 scan periods after the last non-matching one, i.e. when no scan can have been missed between
 *  them; otherwise the step fails, because an event time one scan late would move a visit boundary (no relaxed criteria).
 *
 *  <p>A step's events are dated by the commit of their station's snapshot when the step wrote one, otherwise by the step's first
 *  commit, and in a step without snapshots by the next newer scan of their station. Incident bounds that name a visit's enter or
 *  leave take the commit of that visit's station at that offset. Every step is logged with its commits and the records sent (the
 *  drive log the live ground truth reads). */
public final class Driver {
    /** The driver's clock: {@code now} for the schedule and recordTime only, {@code sleep} between polls. */
    public interface Clock {
        long now();
        void sleep(long ms) throws InterruptedException;
        Clock SYSTEM = new Clock() {
            public long now() { return System.currentTimeMillis(); }
            public void sleep(long ms) throws InterruptedException { Thread.sleep(ms); }
        };
    }

    /** A drive that cannot be carried out exactly; the drive stops. */
    public static final class DriveFailure extends RuntimeException {
        public DriveFailure(String m) { super(m); }
    }

    /** What one step did: its commits (device -> source timestamp), the time its events got, and the records it sent. */
    public record StepResult(long at, SortedMap<String, Long> commits, long eventTime, List<ObjectNode> sent) {}

    private final SimPort sim;
    private final EventSink sink;
    private final Clock clock;
    private final LiveScenario scenario;
    private final LiveEquipment.LiveStream stream;
    private final long pollMs, timeoutMs;
    private final Consumer<ObjectNode> log;
    private final Map<String, ObjectNode> sentOccupancy = new HashMap<>();
    private final Map<String, Long> detected = new HashMap<>();
    private final Map<String, Long> committedAt = new HashMap<>();   // device@offset -> source timestamp

    public Driver(SimPort sim, EventSink sink, Clock clock, LiveScenario scenario, LiveEquipment.LiveStream stream, long pollMs, long timeoutMs, Consumer<ObjectNode> log) {
        this.sim = sim; this.sink = sink; this.clock = clock; this.scenario = scenario; this.stream = stream;
        this.pollMs = pollMs; this.timeoutMs = timeoutMs; this.log = log;
    }

    public List<StepResult> run(List<DrivePlan.Step> steps) throws InterruptedException {
        long start = clock.now();
        List<StepResult> results = new ArrayList<>();
        for (DrivePlan.Step step : steps) {
            for (long wait = start + step.at() - clock.now(); wait > 0; wait = start + step.at() - clock.now()) clock.sleep(wait);
            SortedMap<String, Long> commits = commitAll(step.snapshots());
            commits.forEach((d, ts) -> committedAt.put(d + "@" + step.at(), ts));
            List<ObjectNode> sent = new ArrayList<>();
            long stepTime = step.snapshots().isEmpty() ? -1 : commits.get(step.snapshots().get(0).device());
            for (DrivePlan.Event e : step.events()) {
                Long t = commits.get(e.station());
                if (t == null) {
                    if (stepTime < 0) stepTime = awaitNewerScan(stream.device(e.station()));
                    t = stepTime;
                }
                sent.add(send(e, t));
            }
            StepResult r = new StepResult(step.at(), Collections.unmodifiableSortedMap(commits), stepTime, List.copyOf(sent));
            results.add(r);
            log.accept(logLine(r));
        }
        return results;
    }

    private ObjectNode send(DrivePlan.Event e, long t) {
        if (e instanceof DrivePlan.Arrival a) {
            LiveScenario.Visit v = a.visit();
            ObjectNode o = OccupancyRecords.arrival(v.arrivalId(), t, clock.now(), v.unit(), v.station(), v.visitSeq(), scenario.occupancySource());
            sink.occupancy(o);
            sentOccupancy.put(v.arrivalId(), o);
            return o;
        }
        if (e instanceof DrivePlan.Departure d) {
            LiveScenario.Visit v = d.visit();
            ObjectNode o = OccupancyRecords.departure(v.departureId(), t, clock.now(), v.unit(), v.station(), v.visitSeq(), v.arrivalId(), scenario.occupancySource());
            sink.occupancy(o);
            sentOccupancy.put(v.departureId(), o);
            return o;
        }
        if (e instanceof DrivePlan.Correction c) {
            ObjectNode original = sentOccupancy.get(c.correction().event());
            if (original == null) throw new DriveFailure("correction of " + c.correction().event() + ", which was never sent");
            ObjectNode o = OccupancyRecords.errorDeclaration(original, t, clock.now(), c.correction().reason(), c.correction().correctiveEventIds());
            sink.occupancy(o);
            return o;
        }
        DrivePlan.IncidentRevision ir = (DrivePlan.IncidentRevision) e;
        LiveScenario.Incident i = ir.incident();
        LiveScenario.Revision r = ir.revision();
        if (r.revision() == 1) detected.put(i.incidentId(), t);
        Long det = detected.get(i.incidentId());
        if (det == null) throw new DriveFailure("incident " + i.incidentId() + " revision " + r.revision() + " before revision 1");
        ObjectNode o = IncidentRecords.revision(i.incidentId(), r.revision(), i.station(), i.equipment(), i.kind(), det, t,
            bound(r.suspectFrom(), det), r.suspectTo() == null ? null : bound(r.suspectTo(), det), r.status(), i.evidenceRef(), i.source());
        sink.incident(o);
        return o;
    }

    private long bound(LiveScenario.Anchor a, long detectedAt) {
        if (a.detected()) return detectedAt;
        LiveScenario.Visit v = scenario.visit(a.visit());
        long offset = a.at().equals("enter") ? v.enter() : v.leave();
        Long ts = committedAt.get(v.station() + "@" + offset);
        if (ts == null) throw new DriveFailure("no commit of " + v.station() + " at " + v.visitId() + " " + a.at());
        return ts;
    }

    /** A coherent read of one equipment: every member's value and status and their one source timestamp. */
    record Coherent(long sourceTs, List<SimPort.Value> values) {}

    private Coherent readCoherent(String ns, List<String> nodes, long deadline) throws InterruptedException {
        while (true) {
            List<SimPort.Value> vs = sim.read(ns, nodes);
            Long ts = null;
            boolean coherent = vs.size() == nodes.size();
            for (int i = 0; coherent && i < vs.size(); i++) {
                SimPort.Value v = vs.get(i);
                if (!v.exists()) throw new DriveFailure("sim node " + nodes.get(i) + " does not exist");
                if (v.sourceTs() == null) throw new DriveFailure("sim node " + nodes.get(i) + " has no source timestamp");
                if (ts == null) ts = v.sourceTs(); else if (!ts.equals(v.sourceTs())) coherent = false;
            }
            if (coherent && ts != null) return new Coherent(ts, vs);
            if (clock.now() > deadline) throw new DriveFailure("no coherent read of " + nodes + " before the timeout");
            clock.sleep(pollMs);
        }
    }

    long writeAndAwait(DrivePlan.Snapshot s) throws InterruptedException { return commitAll(List.of(s)).get(s.device()); }

    /** Reads every snapshot's equipment, then sends the snapshots as back-to-back Write requests (one each), then waits for each
     *  commit. Sending them together lets a step's equipment land in the same scan when the requests arrive between two scans, while
     *  each request stays its own unit of atomicity; awaiting one before writing the next would push every further tool a scan later. */
    SortedMap<String, Long> commitAll(List<DrivePlan.Snapshot> snapshots) throws InterruptedException {
        long deadline = clock.now() + timeoutMs;
        List<Coherent> before = new ArrayList<>();
        for (DrivePlan.Snapshot s : snapshots) before.add(readCoherent(s.namespaceUri(), s.liveNodes(), deadline));
        for (DrivePlan.Snapshot s : snapshots) {
            List<DrivePlan.Write> writes = s.writes();
            List<Long> codes = sim.write(s.namespaceUri(), writes);
            for (int i = 0; i < writes.size(); i++)
                if (i >= codes.size() || codes.get(i) != 0) throw new DriveFailure("the sim refused " + writes.get(i).nodeId() + " (" + (i < codes.size() ? "0x" + Long.toHexString(codes.get(i)) : "no result") + ")");
        }
        SortedMap<String, Long> commits = new TreeMap<>();
        long[] lastOld = new long[snapshots.size()];
        for (int k = 0; k < snapshots.size(); k++) lastOld[k] = before.get(k).sourceTs();
        while (commits.size() < snapshots.size()) {
            for (int k = 0; k < snapshots.size(); k++) {
                DrivePlan.Snapshot s = snapshots.get(k);
                if (commits.containsKey(s.device())) continue;
                Coherent r = readCoherent(s.namespaceUri(), s.liveNodes(), deadline);
                if (r.sourceTs() > before.get(k).sourceTs() && matches(s, r)) {
                    if (2 * (r.sourceTs() - lastOld[k]) >= 3 * scenario.scanMs())
                        throw new DriveFailure(s.device() + ": commit at " + r.sourceTs() + " is " + (r.sourceTs() - lastOld[k]) + " ms after the last scan seen without it; a scan may have been missed");
                    commits.put(s.device(), r.sourceTs());
                } else if (r.sourceTs() > lastOld[k]) lastOld[k] = r.sourceTs();
            }
            if (commits.size() == snapshots.size()) break;
            if (clock.now() > deadline) throw new DriveFailure("snapshots of " + snapshots.stream().map(DrivePlan.Snapshot::device).toList() + " were not committed before the timeout");
            clock.sleep(pollMs);
        }
        return commits;
    }

    private long awaitNewerScan(LiveEquipment.Equipment e) throws InterruptedException {
        long deadline = clock.now() + timeoutMs;
        List<String> nodes = e.members().stream().map(m -> e.live(m.name())).toList();
        Coherent before = readCoherent(e.namespaceUri(), nodes, deadline);
        while (true) {
            Coherent r = readCoherent(e.namespaceUri(), nodes, deadline);
            if (r.sourceTs() > before.sourceTs()) return r.sourceTs();
            if (clock.now() > deadline) throw new DriveFailure(e.device() + ": no newer scan before the timeout");
            clock.sleep(pollMs);
        }
    }

    private static boolean matches(DrivePlan.Snapshot s, Coherent r) {
        int i = 0;
        for (LiveEquipment.Member m : s.members()) {
            SimPort.Value v = r.values().get(i++);
            long status = s.statuses().get(m.name());
            if (v.statusCode() != status) return false;
            if (status == 0 && !Objects.equals(v.value(), s.values().get(m.name()))) return false;
        }
        return true;
    }

    private static ObjectNode logLine(StepResult r) {
        ObjectNode o = Json.object();
        Json.putSafe(o, "at", r.at());
        ObjectNode c = o.putObject("commits");
        r.commits().forEach((d, ts) -> Json.putSafe(c, d, ts));
        if (r.eventTime() >= 0) Json.putSafe(o, "eventTime", r.eventTime()); else o.putNull("eventTime");
        var sent = o.putArray("sent");
        r.sent().forEach(sent::add);
        return o;
    }
}
