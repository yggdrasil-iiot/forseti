package yggdrasil.forseti.core.cut;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.core.cut.CutAssemblerTest.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Random;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.Progress;

/** The assembler's one segment: after every operation a store written only when {@link CutAssembler#drainChanged} says so restores
 *  the live assembler; an operation that changes nothing (a replayed committed barrier, a poll that closes nothing) drains nothing. */
class CutAssemblerSegmentTest {
    static JsonNode enc(String s) { return TextNode.valueOf(s); }
    static String dec(JsonNode n) { return n.textValue(); }

    @Test void aStoreWrittenOnChangeRestoresAtEveryOperation() {
        int ops = 0;
        for (int seed = 0; seed < 20; seed++) {
            Random r = new Random(5_000L + seed);
            CutAssembler<String> a = writer();
            assertTrue(a.drainChanged(), "a fresh assembler is changed until its first drain");
            byte[] store = a.segment(CutAssemblerSegmentTest::enc);
            long next = 1;
            long w = 0;
            for (int i = 0; i < 200; i++) {
                int x = r.nextInt(10);
                Progress.Origin o = REQUIRED.get(r.nextInt(REQUIRED.size()));
                if (x < 4) a.input(o, "in-" + i);
                else if (x < 7) { try { a.barrier(b(next + r.nextInt(2), o, i), NO_CONTROL); } catch (CutAssembler.ProtocolError e) { /* out of order */ } }
                else if (x < 8) { for (Progress.Origin q : REQUIRED) try { a.barrier(b(next, q, next * 10), NO_CONTROL); } catch (CutAssembler.ProtocolError e) { /* already there */ } }
                else { w += r.nextInt(3); if (a.poll(w).isPresent()) next = a.lastProgress() + 1; }
                if (a.drainChanged()) store = a.segment(CutAssemblerSegmentTest::enc);
                assertArrayEquals(Canonical.bytes(a.toJson(CutAssemblerSegmentTest::enc)),
                    Canonical.bytes(CutAssembler.fromSegment(store, CutAssemblerSegmentTest::dec).toJson(CutAssemblerSegmentTest::enc)), "op " + i);
                ops++;
            }
        }
        assertTrue(ops >= 4000);
    }

    @Test void anOperationThatChangesNothingDrainsNothing() {
        CutAssembler<String> a = writer();
        barriers(a, 1);
        a.poll(10L).orElseThrow();
        a.drainChanged();
        a.barrier(b(1, T0, 10), NO_CONTROL);   // a committed cut replayed after a restore: ignored
        assertTrue(a.poll(10L).isEmpty());
        assertFalse(a.drainChanged());
        CutAssembler<String> restored = CutAssembler.fromSegment(a.segment(CutAssemblerSegmentTest::enc), CutAssemblerSegmentTest::dec);
        assertFalse(restored.drainChanged(), "an assembler read from its segment is unchanged");
    }
}
