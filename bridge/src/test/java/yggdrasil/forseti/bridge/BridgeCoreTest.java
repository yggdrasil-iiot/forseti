package yggdrasil.forseti.bridge;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.bridge.SessionTableTest.*;
import static yggdrasil.forseti.bridge.Sp.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.MetricDataType;
import org.junit.jupiter.api.Test;

/** The field contents the session table does not show: what a process message and each session event carry, the refusals, and the
 *  dead-letter's raw evidence. */
class BridgeCoreTest {
    static final ObjectMapper JSON = new ObjectMapper();

    static List<Emitted> feed(BridgeCore core, List<Frame> frames) {
        List<Emitted> out = new ArrayList<>();
        for (Frame f : frames) out.addAll(core.accept(f));
        return out;
    }
    static List<Emitted> feed(List<Frame> frames) { return feed(new BridgeCore(Map.of()), frames); }
    @SuppressWarnings("unchecked") static <T extends Emitted> List<T> only(List<Emitted> l, Class<T> c) {
        List<T> r = new ArrayList<>();
        for (Emitted e : l) if (c.isInstance(e)) r.add((T) e);
        return r;
    }
    static JsonNode json(byte[] b) { try { return JSON.readTree(b); } catch (Exception e) { throw new IllegalStateException(e); } }
    static Emitted.SessionEvent kind(List<Emitted> l, String kind) {
        for (Emitted.SessionEvent s : only(l, Emitted.SessionEvent.class)) if (s.kind().equals(kind)) return s;
        throw new AssertionError("no " + kind + " in " + l);
    }

    @Test
    void aProcessMessageCarriesTheSessionAndEveryMemberAsMuninnWroteIt() {
        long t = B1 + 2000;
        List<Metric> metrics = List.of(
            sample("VisitPresent", MetricDataType.Boolean, true, t, 0L, 192, t + 3),
            sample("Sim/Torque", MetricDataType.Float, 0.1f, t, 0x80000000L, 0, t + 3),
            sample("UnitId", MetricDataType.String, "U-0001", t, 0L, 192, null),
            sample("Count", MetricDataType.UInt32, 4294967295L, t, 0x40900000L, 500, t + 3),
            sample("LastCheck", MetricDataType.DateTime, new Date(B1), null, 0L, 192, null));
        List<Emitted> out = feed(List.of(nbirth(E, 3, B1), dbirth(E, D, 1, B1 + 1000), device("DDATA", E, D, 2, t + 50, metrics)));
        Emitted.ProcessMessage m = only(out, Emitted.ProcessMessage.class).get(0);
        assertEquals(new Emitted.ProcessMessage(E, D, B1, 3, 2, t + 50, t + 57, DEF_SHA, List.of(
            new Emitted.Member("VisitPresent", true, 0L, 192L, t, t + 3),
            new Emitted.Member("Sim/Torque", (double) 0.1f, 0x80000000L, 0L, t, t + 3),
            new Emitted.Member("UnitId", "U-0001", 0L, 192L, t, null),
            new Emitted.Member("Count", 4294967295L, 0x40900000L, 500L, t, t + 3),
            new Emitted.Member("LastCheck", B1, 0L, 192L, null, null))), m);
        assertEquals(t, m.eventTime());
    }

    @Test
    void aMemberWithoutASourceTimestampHasNoSourceTsFieldAndIsNeverFilled() {
        List<Emitted> out = feed(List.of(nbirth(E, 0, B1), dbirth(E, D, 1, B1 + 1000), device("DDATA", E, D, 2, B1 + 2000, scan(null))));
        Emitted.ProcessMessage m = only(out, Emitted.ProcessMessage.class).get(0);
        assertNull(m.eventTime());
        JsonNode v = json(Records.message(m));
        for (JsonNode member : v.get("members")) {
            assertFalse(member.has("sourceTs"), member.toString());
            assertFalse(member.has("serverTs"), member.toString());
            assertTrue(member.has("uaStatusCode") && member.has("quality"));
        }
        assertEquals(List.of("edge", "device", "birthPublishTs", "bdSeq", "seq", "publishTs", "bridgeReceivedAt", "definitionSha256", "members"), fields(v));
    }

    static List<String> fields(JsonNode n) { List<String> l = new ArrayList<>(); n.fieldNames().forEachRemaining(l::add); return l; }

    @Test
    void aSeqGapCarriesTheEventTimesOfTheMessagesBeforeAndAfterAndTheMissingCount() {
        List<Emitted> out = feed(cat(session(0, B1, 2), List.of(ddata(E, D, 6, B1 + 6000))));
        Emitted.SessionEvent g = kind(out, "seq-gap");
        assertEquals(Map.of("from", B1 + 2000, "to", B1 + 6000, "missing", 3L), g.payload());
        assertEquals(List.of("from", "to", "missing"), new ArrayList<>(g.payload().keySet()));
    }

    @Test
    void sessionUpCarriesTheBirthTimeAndTheRebirthFlag() {
        List<Emitted> out = feed(cat(session(0, B1, 2), List.of(ncmdRebirth(E), nbirth(E, 0, B2), dbirth(E, D, 1, B2 + 1000))));
        List<Emitted.SessionEvent> ups = only(out, Emitted.SessionEvent.class);
        assertEquals(Map.of("at", B1, "rebirth", false), ups.get(0).payload());
        assertEquals(Map.of("at", B2, "rebirth", true), ups.get(1).payload());
    }

    @Test
    void anNcmdThatIsNotARebirthDoesNotMarkTheNextBirth() {
        List<Emitted> out = feed(cat(session(0, B1, 2), List.of(nbirth(E, 0, B2), dbirth(E, D, 1, B2 + 1000))));
        assertEquals("session-ambiguous", only(out, Emitted.SessionEvent.class).get(1).kind());
    }

    @Test
    void sessionDownStartsAtTheLastReceivedEventTimeWithAnOpenEnd() {
        Emitted.SessionEvent d = kind(feed(cat(session(0, B1, 4), List.of(ndeath(E, 0, B1 + 9000)))), "session-down");
        Map<String, Object> want = new java.util.LinkedHashMap<>();
        want.put("from", B1 + 4000);
        want.put("to", null);
        assertEquals(want, d.payload());
    }

    @Test
    void staleDeathRecordsItsDetectedEventTimeOnceWithTheRawDeath() {
        Frame death = ndeath(E, 0, B1 + 3500);
        List<Emitted> out = feed(cat(session(0, B1, 3), session(1, B2, 4), List.of(death, ddata(E, D, 5, B2 + 9000))));
        Emitted.SessionEvent s = kind(out, "stale-death");
        // The largest telemetry event time the bridge holds for the edge when the death arrives: the newer connection's seq 4.
        assertEquals(Map.of("detectedEventTime", B2 + 4000, "rawPublishTs", B1 + 3500, "bdSeq", 0L, "payloadDigest", BridgeCore.sha256(death.payload())), s.payload());
        assertEquals(List.of("detectedEventTime", "rawPublishTs", "bdSeq", "payloadDigest"), new ArrayList<>(s.payload().keySet()));
    }

    @Test
    void theBirthlessAmbiguousKeepsNoBirthAndTheLastSeqAndTheHeldTelemetryTime() {
        Frame death = ndeath(E, 1, B2 + 3500);
        List<Emitted> out = feed(cat(session(0, B1, 2), List.of(ndeath(E, 0, B1 + 2500), ddata(E, D, 2, B2 + 2000), ddata(E, D, 3, B2 + 3000), death)));
        Emitted.SessionEvent a = kind(out, "session-ambiguous");
        assertNull(a.birthPublishTs());
        assertEquals(3L, a.atSeq());
        assertEquals(1, a.bdSeq());
        // Birthless data is dead-lettered and never held as telemetry: the time held is the earlier, birthed connection's.
        assertEquals(Map.of("detectedEventTime", B1 + 2000, "rawPublishTs", B2 + 3500, "bdSeq", 1L, "payloadDigest", BridgeCore.sha256(death.payload())), a.payload());
    }

    @Test
    void theBirthlessAmbiguousOnAnEdgeWithNothingHeldHasANullDetectedEventTime() {
        Emitted.SessionEvent a = kind(feed(List.of(ddata(E, D, 5, B1), ndeath(E, 3, B1 + 10))), "session-ambiguous");
        assertTrue(a.payload().containsKey("detectedEventTime"));
        assertNull(a.payload().get("detectedEventTime"));
        JsonNode v = json(Records.session(a));
        assertTrue(v.get("birthPublishTs").isNull());
        assertTrue(v.get("payload").get("detectedEventTime").isNull());
    }

    @Test
    void birthlessDataIsDeadLetteredWithItsRawBytesAndReceiptMetadata() {
        Frame data = ddata(E, D, 5, B1);
        Emitted.DeadLetter d = only(feed(List.of(data)), Emitted.DeadLetter.class).get(0);
        assertArrayEquals(data.payload(), d.raw());
        assertEquals(List.of(data.topic(), data.receivedAt(), "birthless", E, D), List.of(d.mqttTopic(), d.receivedAt(), d.reason(), d.edge(), d.device()));
        Records.Out r = Records.encode(d, Records.Topics.DEFAULT);
        assertEquals("ot.dead-letter", r.topic());
        assertArrayEquals(data.payload(), r.value());
        assertEquals(Map.of(Records.H_MQTT_TOPIC, data.topic(), Records.H_RECEIVED_AT, Long.toString(data.receivedAt()), Records.H_REASON, "birthless",
            Records.H_EDGE, E, Records.H_DEVICE, D), r.headers());
    }

    @Test
    void aliasOnlyInputIsRefusedToTheDeadLetterAndStillConsumesItsSeq() throws Exception {
        Metric alias = new MetricBuilder(7L, MetricDataType.Double, 1.5).timestamp(new Date(B1 + 2000)).createMetric();
        Frame refused = device("DDATA", E, D, 2, B1 + 2050, List.of(alias));
        List<Emitted> out = feed(List.of(nbirth(E, 0, B1), dbirth(E, D, 1, B1 + 1000), refused, ddata(E, D, 3, B1 + 3000)));
        Emitted.DeadLetter d = only(out, Emitted.DeadLetter.class).get(0);
        assertEquals("alias-only", d.reason());
        assertArrayEquals(refused.payload(), d.raw());
        assertEquals(List.of(3L), only(out, Emitted.ProcessMessage.class).stream().map(Emitted.ProcessMessage::seq).toList());
        assertEquals(List.of("session-up"), only(out, Emitted.SessionEvent.class).stream().map(Emitted.SessionEvent::kind).toList(), "no seq-gap");
    }

    @Test
    void malformedFramesGoToTheDeadLetter() {
        List<Emitted> out = feed(List.of(
            new Frame("spBv1.0/line2/DDATA/" + E + "/" + D, "not protobuf".getBytes(StandardCharsets.UTF_8), 1),
            new Frame("spBv1.0/line2/DDATA", new byte[0], 2),
            new Frame("other/topic", new byte[] {1}, 3),
            new Frame("spBv1.0/line2/NDATA/" + E + "/" + D, new byte[0], 4),
            new Frame(topic("DDEATH", E, D), empty(), 5)));
        assertEquals(List.of("malformed", "malformed", "malformed", "malformed", "unsupported-message-type"),
            only(out, Emitted.DeadLetter.class).stream().map(Emitted.DeadLetter::reason).toList());
    }

    @Test
    void c7AValueTheContractCannotCarryIsHandledPerMemberAndTheMessageKeepsItsHealthyMembers() {
        long t = B1 + 2000;
        Emitted.ProcessMessage m = only(feed(List.of(nbirth(E, 0, B1), dbirth(E, D, 1, B1 + 1000), device("DDATA", E, D, 2, t + 50, mixed(t)))),
            Emitted.ProcessMessage.class).get(0);
        assertEquals(List.of(
            new Emitted.Member("Up", Double.POSITIVE_INFINITY, 0L, 192L, t, null),
            new Emitted.Member("Down", Double.NEGATIVE_INFINITY, 0L, 192L, t, null),
            new Emitted.Member("NotANumber", null, 0L, 192L, t, null),
            new Emitted.Member("Blob", null, 0x80000000L, 0L, t, t + 3),
            new Emitted.Member("Torque", 12.5, 0L, 192L, t, t + 3)), m.members());
        String json = Records.utf8(Records.message(m));
        assertTrue(json.contains("\"metric\":\"Up\",\"value\":1e400,"), json);
        assertTrue(json.contains("\"metric\":\"Down\",\"value\":-1e400,"), json);
        assertTrue(json.contains("\"metric\":\"NotANumber\",\"value\":null,"), json);
        assertEquals(List.of(), only(feed(List.of(nbirth(E, 0, B1), dbirth(E, D, 1, B1 + 1000), device("DDATA", E, D, 2, t + 50, mixed(t)))),
            Emitted.DeadLetter.class), "no dead-letter");
    }

    /** One message with each value the contract cannot carry as itself, next to a healthy member. */
    static List<Metric> mixed(long t) {
        return List.of(
            sample("Up", MetricDataType.Double, Double.POSITIVE_INFINITY, t, 0L, 192, null),
            sample("Down", MetricDataType.Float, Float.NEGATIVE_INFINITY, t, 0L, 192, null),
            sample("NotANumber", MetricDataType.Double, Double.NaN, t, 0L, 192, null),
            sample("Blob", MetricDataType.Bytes, new byte[] {1, 2}, t, 0x80000000L, 0, t + 3),
            sample("Torque", MetricDataType.Double, 12.5, t, 0L, 192, t + 3));
    }

    @Test
    void c1APendingGapTakesTheNextValidTimeAsToAndThePrecedingObservationAsFrom() {
        List<Emitted> out = feed(cat(session(0, B1, 2), List.of(device("DDATA", E, D, 5, B1 + 5050, scan(null)), ddata(E, D, 6, B1 + 6000))));
        Emitted.SessionEvent g = kind(out, "seq-gap");
        assertEquals(5L, g.atSeq());
        assertEquals(Map.of("from", B1 + 2000, "to", B1 + 6000, "missing", 2L), g.payload());
        // written with the frame that supplied the time, just before that frame's own message
        int gapAt = out.indexOf(g);
        assertEquals(6L, ((Emitted.ProcessMessage) out.get(gapAt + 1)).seq());
    }

    @Test
    void c1NoSeqGapIsEverWrittenWithANullTo() {
        List<Emitted> out = feed(cat(session(0, B1, 2), List.of(device("DDATA", E, D, 5, B1 + 5050, scan(null)), device("DDATA", E, D, 6, B1 + 6050, scan(null)))));
        assertTrue(only(out, Emitted.SessionEvent.class).stream().noneMatch(s -> s.kind().equals("seq-gap")));
    }

    @Test
    void c2SessionDownFallsBackToTheBirthTimeForABornDeviceWithNoTelemetry() {
        Emitted.SessionEvent d = kind(feed(List.of(nbirth(E, 0, B1), device("DBIRTH", E, D, 1, B1 + 50, scan(null)), ndeath(E, 0, B1 + 900))), "session-down");
        assertEquals(B1, d.payload().get("from"));
    }

    @Test
    void c2DbirthMemberTimestampsAreObservations() {
        Emitted.SessionEvent d = kind(feed(List.of(nbirth(E, 0, B1), dbirth(E, D, 1, B1 + 700), ndeath(E, 0, B1 + 900))), "session-down");
        assertEquals(B1 + 700, d.payload().get("from"));
    }

    @Test
    void c4TheBornShapeUnderAnUnknownBoundaryCarriesTheDeathsOwnPayload() {
        Frame death = ndeath(E, 1, B2 + 500);
        List<Emitted> out = feed(List.of(nbirth(E, 0, B1), dbirth(E, D, 1, B1 + 1000), dbirth(E, D2, 2, B1 + 1000), ddata(E, D, 3, B1 + 2000),
            ddata(E, D2, 4, B1 + 2000), ddata(E, D, 0, B2), death));
        List<Emitted.SessionEvent> amb = only(out, Emitted.SessionEvent.class).stream().filter(s -> s.kind().equals("session-ambiguous")).toList();
        Map<String, Object> want = Map.of("detectedEventTime", B1 + 2000, "rawPublishTs", B2 + 500, "bdSeq", 1L, "payloadDigest", BridgeCore.sha256(death.payload()));
        assertEquals(2, amb.size());
        assertEquals(want, amb.get(0).payload());
        assertEquals(want, amb.get(1).payload());
        assertNull(amb.get(0).birthPublishTs());
        assertEquals(B1, amb.get(1).birthPublishTs());
    }

    @Test
    void c6DevicesThatComeUpWithoutAPinAreReported() {
        BridgeCore core = new BridgeCore(Map.of(D, DEF_SHA));
        feed(core, List.of(nbirth(E, 0, B1), dbirth(E, D, 1, B1 + 1000), dbirth(E, D2, 2, B1 + 1000)));
        assertEquals(java.util.Set.of(D2), core.unpinnedSeen());
    }

    @Test
    void stateAndDeviceCommandsAreHostTrafficAndEmitNothing() {
        assertEquals(List.of(), feed(List.of(new Frame("spBv1.0/STATE/host1", "{\"online\":true}".getBytes(StandardCharsets.UTF_8), 1),
            new Frame(topic("DCMD", E, D), empty(), 2))));
    }

    @Test
    void edgesOfDifferentIdsKeepSeparateSessions() {
        List<Emitted> out = feed(List.of(nbirth(E, 0, B1), nbirth("edge-station2", 0, B1), dbirth(E, D, 1, B1 + 1000),
            dbirth("edge-station2", "site:line2:station2", 1, B1 + 1000), ddata(E, D, 2, B1 + 2000), ddata("edge-station2", "site:line2:station2", 2, B1 + 2000)));
        assertEquals(List.of("session-up", "session-up"), only(out, Emitted.SessionEvent.class).stream().map(Emitted.SessionEvent::kind).toList());
        assertEquals(2, only(out, Emitted.ProcessMessage.class).size());
    }

    @Test
    void sparkplugTopicsParseByLevel() {
        assertEquals(new SparkplugTopic("g", "DDATA", "e", "a:b:c"), SparkplugTopic.parse("spBv1.0/g/DDATA/e/a:b:c"));
        assertEquals(new SparkplugTopic("g", "NBIRTH", "e", null), SparkplugTopic.parse("spBv1.0/g/NBIRTH/e"));
        assertEquals("STATE", SparkplugTopic.parse("spBv1.0/STATE/host").type());
        assertNull(SparkplugTopic.parse("spBv1.0/g/DDATA/e/d/extra"));
        assertNull(SparkplugTopic.parse("spBv1.0/g//e"));
        assertNull(SparkplugTopic.parse("spAv1.0/g/DDATA/e"));
    }
}
