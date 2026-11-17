package yggdrasil.forseti.cli.replay;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.cli.harness.ReferenceEngine;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;
import yggdrasil.forseti.core.journal.Progress;

/** The transport-free contracts of the replay harness: the topic plan, the cut-barrier record, the offset map and the journal
 *  translation, the output record codec, the recovery-control snapshot and the run preparation over the store interfaces. */
class ReplayContractsTest {

    @Test void thePlanHasTheSpecsPartitionsAndSettings() {
        List<ReplayTopics.Spec> plan = ReplayTopics.plan("run-1.");
        assertEquals(ReplayTopics.INPUTS.size() + ReplayTopics.OUTPUTS.size() + ReplayTopics.CONTROL.size(), plan.size());
        Map<String, ReplayTopics.Spec> by = new HashMap<>();
        for (ReplayTopics.Spec s : plan) { by.put(s.base(), s); assertEquals("run-1." + s.base(), s.name()); assertEquals("LogAppendTime", s.configs().get("message.timestamp.type")); }
        for (String t : ReplayTopics.INPUTS) { assertEquals(6, by.get(t).partitions()); assertEquals("2592000000", by.get(t).configs().get("retention.ms")); }
        assertEquals(7, by.get("forseti.dispositions-stage1").partitions());
        assertEquals(7, by.get("forseti.dispositions-stage2").partitions());
        assertEquals(6, by.get("forseti.work-records").partitions());
        assertEquals("604800000", by.get("forseti.samples").configs().get("retention.ms"));
        assertEquals(1, by.get("forseti.generations").partitions());
        assertEquals(1, by.get("forseti.recovery-control").partitions());
        assertEquals("-1", by.get("forseti.recovery-control").configs().get("retention.ms"));
        assertThrows(IllegalArgumentException.class, () -> ReplayTopics.plan("bad prefix "));

        Map<String, ReplayTopics.Observed> seen = new HashMap<>();
        for (ReplayTopics.Spec s : plan) seen.put(s.name(), new ReplayTopics.Observed(s.name(), s.partitions(), new TreeMap<>(s.configs())));
        assertEquals(List.of(), ReplayTopics.problems(plan, seen));
        seen.put("run-1.forseti.generations", new ReplayTopics.Observed("run-1.forseti.generations", 6, Map.of("message.timestamp.type", "CreateTime", "retention.ms", "-1")));
        seen.remove("run-1.ot.messages");
        assertEquals(List.of("topic run-1.ot.messages does not exist", "topic run-1.forseti.generations has 6 partitions, not 1",
            "topic run-1.forseti.generations has message.timestamp.type=CreateTime, not LogAppendTime"), ReplayTopics.problems(plan, seen));
    }

    @Test void aCutBarrierRecordCarriesExactlyTheSpecsFields() {
        CutAssembler.Idle idle = new CutAssembler.Idle("site:line:s3", "ot.messages", 2, 1000, CutAssembler.STARVED, "inj-24");
        CutBarrierRecord r = new CutBarrierRecord(3, "d".repeat(64), new CutAssembler.Barrier(7, new Progress.Origin("ot.messages", 2), 41, 1, "inj-21", List.of(idle)));
        ObjectNode n = r.toJson();
        assertEquals(new TreeSet<>(List.of("generation", "cutId", "routingDigest", "origin", "throughOffset", "idleInputs", "restartEpoch", "recoveryRef")),
            new TreeSet<>(iter(n.fieldNames())));
        CutBarrierRecord back = CutBarrierRecord.fromJson(Json.read(Canonical.bytes(n)));
        assertEquals(r, back);
        ObjectNode extra = n.deepCopy().put("type", "cut-barrier");
        assertThrows(IllegalArgumentException.class, () -> CutBarrierRecord.fromJson(extra));
        ObjectNode none = n.deepCopy().put("throughOffset", -2);
        assertThrows(IllegalArgumentException.class, () -> CutBarrierRecord.fromJson(none));
        assertNull(CutBarrierRecord.fromJson(new CutBarrierRecord(1, "d".repeat(64), new CutAssembler.Barrier(1, new Progress.Origin("ot.sessions", 0), -1)).toJson()).barrier().recoveryRef());
    }

    @Test void theOffsetMapMapsBothWaysAndRefusesWhatWasNotWritten() {
        OffsetMap m = new OffsetMap();
        m.declare("mfg.occupancy", 1);
        for (int i = 0; i < 3; i++) m.put("ot.messages", 1, i, i);       // batch 1: 0..2, barrier at 3
        for (int i = 3; i < 5; i++) m.put("ot.messages", 1, i, i + 1);   // batch 2: 4..5, barrier at 6
        assertEquals(5, m.broker("ot.messages", 1, 4));
        assertEquals(4, m.recording("ot.messages", 1, 5));
        assertEquals(-1, m.throughRecording("ot.messages", 1, -1));
        assertEquals(-1, m.throughBroker("mfg.occupancy", 1, -1));
        assertThrows(IllegalArgumentException.class, () -> m.recording("ot.messages", 1, 3), "a barrier's offset is not a business record");
        assertThrows(IllegalArgumentException.class, () -> m.put("ot.messages", 1, 6, 9), "recording offsets are contiguous");
        assertThrows(IllegalArgumentException.class, () -> m.put("ot.messages", 1, 5, 5), "broker offsets increase");
        assertThrows(IllegalArgumentException.class, () -> m.recording("ot.sessions", 1, 0));
        OffsetMap back = OffsetMap.fromJson(m.toJson());
        assertEquals(Canonical.utf8(Canonical.bytes(m.toJson())), Canonical.utf8(Canonical.bytes(back.toJson())));
        assertEquals("{\"offsetMapVersion\":1,\"partitions\":{\"mfg.occupancy/1\":[],\"ot.messages/1\":[[0,0,3],[3,4,2]]}}", Canonical.utf8(Canonical.bytes(m.toJson())));
    }

    @Test void theTranslationRewritesExactlyTheInputOffsets() {
        OffsetMap m = new OffsetMap();
        for (int i = 0; i < 4; i++) m.put("ot.messages", 1, i, 10 + i);
        for (int i = 0; i < 2; i++) m.put("quality.incidents", 3, i, 20 + i);
        ObjectNode row = (ObjectNode) Json.read("{\"key\":{\"k\":1},\"headers\":{\"forseti-generation\":\"1\"},\"value\":{\"body\":{\"offset\":11},\"bodyDigest\":\"x\",\"envelope\":{\"trigger\":{\"topic\":\"ot.messages\",\"partition\":1,\"offset\":12}}}}".getBytes(StandardCharsets.UTF_8));
        JournalTranslation.toRecording("forseti.work-records", row, m);
        assertEquals(2, row.at("/value/envelope/trigger/offset").asLong());
        assertEquals(11, row.at("/value/body/offset").asLong(), "a body is never touched");
        ObjectNode disp = (ObjectNode) Json.read(("{\"key\":{\"generation\":1,\"inputTopic\":\"ot.messages\",\"inputPartition\":1,\"inputOffset\":13,\"part\":\"message\"},"
            + "\"headers\":{\"forseti-generation\":\"1\"},\"value\":{\"body\":{\"refs\":{\"quarantine\":{\"inputTopic\":\"ot.messages\",\"inputPartition\":1,\"inputOffset\":10}}},\"revision\":1}}").getBytes(StandardCharsets.UTF_8));
        JournalTranslation.toRecording("forseti.dispositions-stage1", disp, m);
        assertEquals(3, disp.at("/key/inputOffset").asLong());
        assertEquals(0, disp.at("/value/body/refs/quarantine/inputOffset").asLong());
        ObjectNode prog = (ObjectNode) Json.read(("{\"key\":null,\"value\":{\"type\":\"progress\",\"barrierRefs\":[{\"cutId\":1,\"origin\":{\"topic\":\"ot.messages\",\"partition\":1},\"throughOffset\":13},"
            + "{\"cutId\":1,\"origin\":{\"topic\":\"ot.messages\",\"partition\":1},\"throughOffset\":-1}],\"incidentOffsets\":[{\"cutId\":1,\"partition\":3,\"throughOffset\":21}]}}").getBytes(StandardCharsets.UTF_8));
        JournalTranslation.toRecording("forseti.affected", prog, m);
        assertEquals(3, prog.at("/value/barrierRefs/0/throughOffset").asLong());
        assertEquals(-1, prog.at("/value/barrierRefs/1/throughOffset").asLong());
        assertEquals(1, prog.at("/value/incidentOffsets/0/throughOffset").asLong());
        ObjectNode q = (ObjectNode) Json.read("{\"key\":{\"inputTopic\":\"ot.messages\",\"inputPartition\":1,\"inputOffset\":11},\"headers\":{\"forseti-generation\":\"1\"},\"value\":{\"inputTopic\":\"ot.messages\",\"inputPartition\":1,\"inputOffset\":11}}".getBytes(StandardCharsets.UTF_8));
        JournalTranslation.toRecording("forseti.quarantine", q, m);
        assertEquals(1, q.at("/key/inputOffset").asLong());
        assertEquals(1, q.at("/value/inputOffset").asLong());
        ObjectNode bad = (ObjectNode) Json.read("{\"key\":{\"k\":1},\"headers\":{\"forseti-generation\":\"1\"},\"value\":{\"envelope\":{\"trigger\":{\"topic\":\"ot.messages\",\"partition\":1,\"offset\":14}}}}".getBytes(StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, () -> JournalTranslation.toRecording("forseti.samples", bad, m), "an offset the run never wrote is refused");
    }

    @Test void anOutputRecordRoundTripsToItsJournalLine() {
        for (String text : List.of("{\"key\":{\"k\":\"a\"},\"headers\":{\"forseti-generation\":\"2\"},\"value\":{\"body\":{\"v\":1.5},\"bodyDigest\":\"x\"}}",
                "{\"key\":null,\"value\":{\"type\":\"progress\",\"generation\":2,\"cutId\":3}}",
                "{\"value\":{\"delete\":{\"generation\":2,\"logicalKey\":{\"k\":\"a\"},\"revision\":2,\"supersedesDigest\":\"y\"}}}")) {
            JsonNode line = Json.read(text.getBytes(StandardCharsets.UTF_8));
            OutputRecords.Transport t = OutputRecords.transport(line);
            assertEquals("2", t.headers().get("forseti-generation"), "every transport record carries the generation header");
            assertEquals(Canonical.utf8(Canonical.bytes(line)), Canonical.utf8(Canonical.bytes(OutputRecords.line(t))), text);
        }
        assertNull(OutputRecords.transport(Json.read("{\"key\":null,\"value\":{\"type\":\"progress\",\"generation\":1}}".getBytes(StandardCharsets.UTF_8))).key());
        assertThrows(IllegalArgumentException.class, () -> OutputRecords.line(new OutputRecords.Transport("{}".getBytes(StandardCharsets.UTF_8), Map.of(), "{\"type\":\"progress\"}".getBytes(StandardCharsets.UTF_8))));
        assertThrows(IllegalArgumentException.class, () -> OutputRecords.line(new OutputRecords.Transport(null, Map.of(), "{\"body\":{}}".getBytes(StandardCharsets.UTF_8))));
        // a body double at 2^53 is printed as an integer literal by JCS and read back as the double it was
        byte[] big = "{\"body\":{\"v\":9007199254740992},\"bodyDigest\":\"x\"}".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(big, Canonical.bytes(OutputRecords.readValue(big)));
    }

    @Test void everyOutputRecordCarriesTheRunsGenerationAndCanonicalBytes() {
        byte[] progress = "{\"cutId\":3,\"generation\":2,\"type\":\"progress\"}".getBytes(StandardCharsets.UTF_8);
        byte[] delete = "{\"delete\":{\"generation\":2,\"logicalKey\":{\"k\":\"a\"},\"revision\":2,\"supersedesDigest\":\"y\"}}".getBytes(StandardCharsets.UTF_8);
        byte[] row = "{\"body\":{},\"bodyDigest\":\"x\"}".getBytes(StandardCharsets.UTF_8), key = "{\"k\":\"a\"}".getBytes(StandardCharsets.UTF_8);
        Map<String, String> g2 = Map.of("forseti-generation", "2"), g3 = Map.of("forseti-generation", "3");
        OutputRecords.line(new OutputRecords.Transport(null, g2, progress), 2);
        OutputRecords.line(new OutputRecords.Transport(null, g2, delete), 2);
        OutputRecords.line(new OutputRecords.Transport(key, g2, row), 2);
        assertThrows(IllegalArgumentException.class, () -> OutputRecords.line(new OutputRecords.Transport(null, Map.of(), progress), 2), "a progress without the header");
        assertThrows(IllegalArgumentException.class, () -> OutputRecords.line(new OutputRecords.Transport(null, g3, progress), 3), "a progress of another generation");
        assertThrows(IllegalArgumentException.class, () -> OutputRecords.line(new OutputRecords.Transport(null, g3, delete), 2), "a delete with another header");
        assertThrows(IllegalArgumentException.class, () -> OutputRecords.line(new OutputRecords.Transport(key, g3, row), 2), "a row with another header");
        OutputRecords.requireCanonical(new OutputRecords.Transport(key, g2, row));
        assertThrows(IllegalArgumentException.class, () -> OutputRecords.requireCanonical(new OutputRecords.Transport(key, g2, "{\"bodyDigest\":\"x\",\"body\":{}}".getBytes(StandardCharsets.UTF_8))));
        assertThrows(IllegalArgumentException.class, () -> OutputRecords.requireCanonical(new OutputRecords.Transport("{ \"k\":\"a\"}".getBytes(StandardCharsets.UTF_8), g2, row)));
    }

    @Test void aBarrierIsAppliedOnlyToThePartitionItWasConsumedFrom() {
        String digest = "d".repeat(64);
        byte[] v = Canonical.bytes(new CutBarrierRecord(1, digest, new CutAssembler.Barrier(4, new Progress.Origin("ot.messages", 2), 10)).toJson());
        assertEquals(4, CutBarrierRecord.consumed("ot.messages", 2, v, 1, digest).barrier().cutId());
        assertTrue(assertThrows(IllegalArgumentException.class, () -> CutBarrierRecord.consumed("ot.messages", 3, v, 1, digest)).getMessage().startsWith("barrier-origin"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> CutBarrierRecord.consumed("ot.sessions", 2, v, 1, digest)).getMessage().startsWith("barrier-origin"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> CutBarrierRecord.consumed("ot.messages", 2, v, 2, digest)).getMessage().startsWith("barrier-generation"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> CutBarrierRecord.consumed("ot.messages", 2, v, 1, "e".repeat(64))).getMessage().startsWith("barrier-routing"));
    }

    @Test void theSnapshotSelectsTheLatestDeclarationOfItsGeneration() {
        RecoveryControlStore.Snapshot s = new RecoveryControlStore.Snapshot(List.of(
            new RecoveryControlStore.Declaration(0, RunPreparation.initialDeclaration(1, "a".repeat(64))),
            new RecoveryControlStore.Declaration(2, RunPreparation.initialDeclaration(2, "a".repeat(64))),
            new RecoveryControlStore.Declaration(4, Json.read(("{\"generation\":1,\"injectionId\":\"inj-21\",\"stage\":1,\"epochBefore\":0,\"targetEpoch\":1,\"lastWrittenCutId\":4,"
                + "\"routingDigest\":\"" + "a".repeat(64) + "\"}").getBytes(StandardCharsets.UTF_8)))), 6);
        assertEquals(new Progress.InitEvidence(4, 6, "none"), s.initEvidence(1, "none"));
        assertEquals(new CutAssembler.Declaration(4, 1, "inj-21"), s.assemblerDeclaration(1));
        assertEquals(new Progress.InitEvidence(2, 6, "restored"), s.initEvidence(2, "restored"));
        assertThrows(IllegalStateException.class, () -> s.initEvidence(3, "none"));
        assertNull(s.assemblerDeclaration(3));
    }

    @Test void theRunPreparationWritesTheInitialDeclarationOnceAndChecksTheRouting() {
        Mem control = new Mem();
        GenerationStore store = new MemStore(List.of(new GenerationStore.Entry("line2", generation(1))));
        assertEquals(0, RunPreparation.prepare(store, control, 1, "a".repeat(64)));
        assertEquals(0, RunPreparation.prepare(store, control, 1, "a".repeat(64)), "a later run writes nothing");
        assertEquals(1, control.rows.size());
        assertEquals(Canonical.utf8(Canonical.bytes(RunPreparation.initialDeclaration(1, "a".repeat(64)))), Canonical.utf8(Canonical.bytes(control.rows.get(0))));
        ReferenceEngine.Refused r = assertThrows(ReferenceEngine.Refused.class, () -> RunPreparation.prepare(store, control, 1, "b".repeat(64)));
        assertEquals("generation-routing-mismatch", r.code);
        assertEquals("generation-unregistered", assertThrows(ReferenceEngine.Refused.class, () -> RunPreparation.prepare(store, control, 2, "a".repeat(64))).code);
        assertEquals(1, control.rows.size());
    }

    static Generation generation(long n) {
        return new Generation(n, Generation.REGISTERED, "e".repeat(64), Json.JSON.createArrayNode(), Json.object(), 1, Json.object(), null, null, 0);
    }

    /** The control store as a list (two records per declaration on a transactional topic: the record and its commit marker). */
    static final class Mem implements RecoveryControlStore {
        final List<JsonNode> rows = new ArrayList<>();
        @Override public long append(JsonNode d) { rows.add(d.deepCopy()); return 2L * (rows.size() - 1); }
        @Override public Snapshot read() {
            List<Declaration> l = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) l.add(new Declaration(2L * i, rows.get(i)));
            return new Snapshot(l, 2L * rows.size());
        }
        @Override public void close() {}
    }

    static final class MemStore implements GenerationStore {
        final List<Entry> rows;
        MemStore(List<Entry> rows) { this.rows = new ArrayList<>(rows); }
        @Override public void append(String scope, Generation row) { rows.add(new Entry(scope, row)); }
        @Override public List<Entry> all() { return List.copyOf(rows); }
    }

    static <T> List<T> iter(Iterator<T> it) { List<T> l = new ArrayList<>(); it.forEachRemaining(l::add); return l; }
}
