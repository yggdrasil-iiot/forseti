package yggdrasil.forseti.core.cut;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Progress;

/** The cut protocol of one writer (spec 412-418): barriers, held inputs, the receive-order trigger, starvation, recovery epochs and
 *  their adoption, admissionW, and the versioned codec. Inputs are strings here; their channel is what matters. */
class CutAssemblerTest {
    static final String S1 = "site:line2:station1", S2 = "site:line2:station2";
    static final Progress.Origin T0 = new Progress.Origin("ot.messages", 0), T1 = new Progress.Origin("ot.messages", 1),
        OCC = new Progress.Origin("mfg.occupancy", 0);
    static final List<Progress.Origin> REQUIRED = List.of(T0, T1, OCC);
    static final CutAssembler.Control NO_CONTROL = () -> { throw new AssertionError("the control is not read"); };

    static CutAssembler<String> writer() { return new CutAssembler<>(S1, REQUIRED, true); }

    static CutAssembler.Barrier b(long cut, Progress.Origin o, long through) { return new CutAssembler.Barrier(cut, o, through); }
    static CutAssembler.Barrier b(long cut, Progress.Origin o, long epoch, String ref) { return new CutAssembler.Barrier(cut, o, cut * 10, epoch, ref, List.of()); }

    static void barriers(CutAssembler<String> a, long cut) { for (Progress.Origin o : REQUIRED) a.barrier(b(cut, o, cut * 10), NO_CONTROL); }
    static void barriers(CutAssembler<String> a, long cut, long epoch, String ref, CutAssembler.Control c) { for (Progress.Origin o : REQUIRED) a.barrier(b(cut, o, epoch, ref), c); }

    static CutAssembler.Idle starved(String station) { return new CutAssembler.Idle(station, "ot.messages", 1, 1000, CutAssembler.STARVED, "injection-24"); }

    @Test void aCutClosesOnceEveryRequiredBarrierArrived() {
        CutAssembler<String> a = writer();
        a.input(T0, "m1");
        a.input(OCC, "o1");
        a.barrier(b(1, T0, 0), NO_CONTROL);
        a.barrier(b(1, OCC, 0), NO_CONTROL);
        assertTrue(a.poll(100L).isEmpty(), "the barrier of ot.messages/1 is missing");
        a.barrier(b(1, T1, -1), NO_CONTROL);   // -1: nothing of that partition included yet, a complete barrier like any other
        CutAssembler.Closed<String> c = a.poll(100L).orElseThrow();
        assertEquals(1, c.cutId());
        assertEquals(List.of("m1", "o1"), c.inputs());
        assertEquals(List.of(), c.mergedCutIds());
        assertNull(c.admissionW(), "no admission bound before the first progress");
        assertEquals(100L, c.w1());
        // barrierRefs in the order of the required set, whatever the arrival order
        assertEquals(List.of(b(1, T0, 0).ref(), b(1, T1, -1).ref(), b(1, OCC, 0).ref()), c.barrierRefs());
        assertEquals(1, a.lastProgress());
        assertEquals(100L, a.admissionW());
        assertTrue(a.poll(100L).isEmpty(), "a closed cut never closes twice");
    }

    @Test void inputsBelongToTheCutAfterTheLastBarrierOfTheirChannel() {
        CutAssembler<String> a = writer();
        a.input(T1, "c");                       // before any barrier: cut 1
        a.barrier(b(1, T0, 0), NO_CONTROL);
        a.barrier(b(1, T1, 0), NO_CONTROL);
        a.input(T1, "d");                       // after T1's barrier 1: cut 2, held while cut 1 is still open
        a.input(OCC, "e");                      // OCC has no barrier of cut 1 yet: cut 1
        a.barrier(b(1, OCC, 0), NO_CONTROL);
        assertTrue(a.poll(1L).isEmpty(), "an input of cut 2 arrived before progress 1: cut 1 is skipped");
        barriers(a, 2);
        CutAssembler.Closed<String> c = a.poll(2L).orElseThrow();
        assertEquals(List.of("c", "e", "d"), c.inputs(), "cut 1's inputs, then cut 2's, each in arrival order");
        a.input(T0, "f");
        barriers(a, 3);
        assertEquals(List.of("f"), a.poll(3L).orElseThrow().inputs());
    }

    @Test void aBarrierOutOfOrderIsAProtocolError() {
        CutAssembler<String> a = writer();
        a.barrier(b(2, T0, 0), NO_CONTROL);
        CutAssembler.ProtocolError e = assertThrows(CutAssembler.ProtocolError.class, () -> a.barrier(b(1, T0, 0), NO_CONTROL));
        assertEquals("barrier-order", e.code);
    }

    @Test void theReceiveOrderTriggerMergesACutWhoseLaterInputsArrivedFirst() {
        CutAssembler<String> a = writer();
        a.input(T0, "k1");
        a.barrier(b(1, T0, 0), NO_CONTROL);
        a.input(T0, "k2");              // an input of cut 2 before progress 1
        a.barrier(b(1, T1, 0), NO_CONTROL);
        a.barrier(b(1, OCC, 0), NO_CONTROL);
        assertTrue(a.poll(50L).isEmpty(), "cut 1 is skipped: an input of cut 2 arrived before its progress");
        a.barrier(b(2, T0, 0), NO_CONTROL);
        a.barrier(b(2, T1, 0), NO_CONTROL);
        a.barrier(b(2, OCC, 0), NO_CONTROL);
        CutAssembler.Closed<String> c = a.poll(60L).orElseThrow();
        assertEquals(2, c.cutId());
        assertEquals(List.of(1L), c.mergedCutIds());
        assertEquals(List.of("k1", "k2"), c.inputs(), "the inputs of the merged cuts as one sequence");
        assertEquals(6, c.barrierRefs().size(), "barrierRefs list every barrier received, of the merged cut too");
        assertNull(c.admissionW(), "admitted against the last committed progress");
    }

    @Test void aStarvedStationSkipsItsCutsAndMergesThemOnResume() {
        // asymmetric completion: S1 is starved in cuts 2 and 3, S2 completes them
        CutAssembler<String> s1 = writer(), s2 = new CutAssembler<>(S2, REQUIRED, true);
        CutAssembler<String> rejects = new CutAssembler<>(null, REQUIRED, false);
        long[] w = {100, 200, 300, 400};
        for (long cut = 1; cut <= 4; cut++) {
            List<CutAssembler.Idle> idle = cut == 2 || cut == 3 ? List.of(starved(S1)) : List.of();
            s1.input(OCC, "occ" + cut);   // occupancy keeps arriving; only telemetry is withheld
            for (Progress.Origin o : REQUIRED) {
                CutAssembler.Barrier x = new CutAssembler.Barrier(cut, o, cut, 0, null, idle);
                s1.barrier(x, NO_CONTROL);
                s2.barrier(x, NO_CONTROL);
                rejects.barrier(x, NO_CONTROL);
            }
            Optional<CutAssembler.Closed<String>> c1 = s1.poll(cut == 1 ? w[0] : w[3]);
            assertEquals(cut == 2 || cut == 3, c1.isEmpty(), "cut " + cut + " at the starved station");
            assertEquals(cut, s2.poll(w[(int) cut - 1]).orElseThrow().cutId(), "the other station completes every cut");
            assertEquals(cut, rejects.poll(null).orElseThrow().cutId(), "a reject collector counts every barrier");
            if (cut == 4) {
                CutAssembler.Closed<String> c = c1.orElseThrow();
                assertEquals(List.of(2L, 3L), c.mergedCutIds());
                assertEquals(List.of("occ2", "occ3", "occ4"), c.inputs());
                assertEquals(100L, c.admissionW(), "the merged inputs are admitted against the W1 of cut 1");
                assertEquals(3, c.barrierRefs().size(), "the starved cuts' barriers were not collected");
            }
        }
    }

    @Test void aWriterWaitsForItsNativeTimeToReachItsLastW1() {
        CutAssembler<String> a = writer();
        barriers(a, 1);
        assertEquals(500L, a.poll(500L).orElseThrow().w1());
        barriers(a, 2);
        assertTrue(a.poll(499L).isEmpty(), "W1 never decreases: the progress waits for native time");
        assertEquals(500L, a.poll(500L).orElseThrow().w1());
    }

    @Test void aReinitializedWriterKeepsEarlierEpochsAndClosesTheFirstCutOfItsEpoch() {
        CutAssembler<String> a = writer();
        barriers(a, 1);
        a.poll(10L).orElseThrow();      // progress 1 committed, j = 1
        a.reinitialize(new CutAssembler.Declaration(3, 1, "inj-21"));
        assertTrue(a.recovering());
        a.input(T0, "x2");
        barriers(a, 2, 0, null, NO_CONTROL);   // a barrier of the old epoch: inputs kept, no progress
        assertTrue(a.poll(20L).isEmpty());
        a.input(T0, "x3");
        barriers(a, 3, 1, "inj-21", NO_CONTROL);
        CutAssembler.Closed<String> c = a.poll(30L).orElseThrow();
        assertEquals(3, c.cutId());
        assertEquals(List.of(2L), c.mergedCutIds());
        assertEquals(1, c.restartEpoch());
        assertEquals("inj-21", c.recoveryRef());
        assertTrue(c.recovered());
        assertEquals(10L, c.admissionW());
        assertEquals(List.of("x2", "x3"), c.inputs());
        assertFalse(a.recovering(), "the writer leaves recovery");
    }

    @Test void aRecoveringWriterAdoptsALaterEpochFromAFreshReadOfTheControl() {
        CutAssembler<String> a = writer();
        a.reinitialize(new CutAssembler.Declaration(1, 1, "inj-22i"));
        int[] reads = {0};
        CutAssembler.Control control = () -> { reads[0]++; return new CutAssembler.Declaration(2, 2, "inj-22iv"); };
        barriers(a, 1, 2, "inj-22iv", control);   // the chained injection's epoch: adopted
        assertEquals(1, reads[0], "read once, on the first barrier above E");
        assertEquals(2, a.epoch());
        CutAssembler.Closed<String> c = a.poll(5L).orElseThrow();
        assertEquals(2, c.restartEpoch());
        assertEquals("inj-22iv", c.recoveryRef());
    }

    @Test void aBarrierAboveTheLatestDeclarationIsAProtocolError() {
        CutAssembler<String> a = writer();
        a.reinitialize(new CutAssembler.Declaration(1, 1, "inj-21"));
        CutAssembler.ProtocolError e = assertThrows(CutAssembler.ProtocolError.class,
            () -> a.barrier(b(1, T0, 3, "inj-x"), () -> new CutAssembler.Declaration(1, 1, "inj-21")));
        assertEquals("recovery-epoch", e.code);
    }

    @Test void barriersOfOneCutWithDifferentEpochsAreNeverCombined() {
        CutAssembler<String> a = writer();
        a.barrier(b(1, T0, 0, null), NO_CONTROL);
        a.barrier(b(1, T1, 1, "inj-21"), NO_CONTROL);
        a.barrier(b(1, OCC, 0, null), NO_CONTROL);
        assertEquals("mixed-epochs", assertThrows(CutAssembler.ProtocolError.class, () -> a.poll(1L)).code);
    }

    @Test void aWriterThatWasNotReinitializedNeverEntersRecovery() {
        CutAssembler<String> a = writer();
        barriers(a, 1, 1, "inj-21", NO_CONTROL);   // a barrier of a later epoch does not read the control
        assertEquals(1, a.poll(1L).orElseThrow().cutId());
    }

    @Test void theCodecRoundTripsTheStateAndTheRestoredWriterContinues() {
        CutAssembler<String> a = writer();
        barriers(a, 1);
        a.poll(7L).orElseThrow();
        a.input(T0, "h2");
        a.barrier(b(2, T0, 20), NO_CONTROL);
        a.input(T0, "h3");
        a.barrier(b(2, OCC, 20), NO_CONTROL);
        a.reinitialize(new CutAssembler.Declaration(4, 1, "inj-21"));
        JsonNode state = a.toJson(TextNode::valueOf);
        CutAssembler<String> r = CutAssembler.fromJson(Json.read(Json.pretty(state)), JsonNode::textValue);
        assertEquals(state, r.toJson(TextNode::valueOf), "toJson after fromJson writes the same state");
        assertEquals(1, r.lastProgress());
        assertEquals(7L, r.admissionW());
        assertTrue(r.recovering());
        assertEquals(1, r.epoch());
        assertEquals(2, r.heldInputs());
        r.barrier(b(2, T1, 0, null), NO_CONTROL);
        barriers(r, 3, 1, "inj-21", NO_CONTROL);
        CutAssembler.Closed<String> c = r.poll(9L).orElseThrow();
        assertEquals(List.of(2L), c.mergedCutIds());
        assertEquals(List.of("h2", "h3"), c.inputs());
        // a barrier of a committed cut (replayed after a restore) is never applied twice
        r.barrier(b(3, T0, 1, "inj-21"), NO_CONTROL);
        assertTrue(r.poll(9L).isEmpty());
    }

    @Test void theCodecRefusesAnotherVersionAndMalformedState() {
        CutAssembler<String> a = writer();
        barriers(a, 1);
        var n = a.toJson(TextNode::valueOf);
        n.put("assemblerVersion", 2);
        assertThrows(IllegalArgumentException.class, () -> CutAssembler.fromJson(n, JsonNode::textValue));
        var m = a.toJson(TextNode::valueOf);
        ((com.fasterxml.jackson.databind.node.ObjectNode) m.get("barriers").get(0)).putObject("origin").put("topic", "ot.sessions").put("partition", 9);
        assertThrows(IllegalArgumentException.class, () -> CutAssembler.fromJson(m, JsonNode::textValue), "a barrier off the required set");
    }

    @Test void inputsAndBarriersOffTheRequiredSetAreRefused() {
        CutAssembler<String> a = writer();
        Progress.Origin other = new Progress.Origin("ot.sessions", 4);
        assertThrows(IllegalArgumentException.class, () -> a.input(other, "x"));
        assertThrows(IllegalArgumentException.class, () -> a.barrier(b(1, other, 0), NO_CONTROL));
        assertThrows(IllegalArgumentException.class, () -> new CutAssembler<String>(null, REQUIRED, true), "a starvable writer names its station");
        assertThrows(IllegalArgumentException.class, () -> new CutAssembler<String>(S1, List.of(T0, T0), true));
    }

    @Test void theInputCodecRestoresTheRecordAsReceived() {
        byte[] raw = "{\"eventId\":\"e1\",\"n\":1e400}".getBytes(StandardCharsets.UTF_8);
        Inputs.InputRecord r = InputParser.read(new Inputs.Origin(InputParser.OCCUPANCY, 2, 17), "k", raw).record();
        Inputs.InputRecord back = InputCodec.decode(Json.read(Json.pretty(InputCodec.encode(r))));
        assertEquals(r.origin(), back.origin());
        assertEquals(r.key(), back.key());
        assertArrayEquals(raw, back.rawValue(), "the value bytes as delivered");
        assertEquals(r.payloadDigest(), back.payloadDigest());
    }
}
