package yggdrasil.forseti.bridge;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.bridge.Sp.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Decision 10 and plan D4 as a table: each row is a sequence of Sparkplug frames of one edge, in arrival order, and the session events
 *  (kind, device, birthPublishTs, bdSeq, atSeq), process-message count and dead-letter reasons the bridge must emit for it. */
class SessionTableTest {
    static final String E = "edge-station1", D = "site:line2:station1", D2 = "site:line2:station1:nutrunner1";
    static final long B1 = 1_794_182_400_000L, B2 = B1 + 600_000;

    record Row(String name, Map<String, String> pins, List<Frame> frames, List<String> sessions, int messages, List<String> deadLetters) {
        @Override public String toString() { return name; }
    }

    static String ev(String kind, String device, Long birth, long bdSeq, Long atSeq) { return kind + " " + device + " " + birth + " " + bdSeq + " " + atSeq; }
    static String up(String device, long birth, long bdSeq, long atSeq) { return ev("session-up", device, birth, bdSeq, atSeq); }
    static Row row(String name, List<Frame> frames, List<String> sessions, int messages, String... dead) { return new Row(name, Map.of(), frames, sessions, messages, List.of(dead)); }
    static List<Frame> f(Frame... x) { return List.of(x); }

    /** A born connection with one DBIRTH and DDATA seq 2..{@code lastSeq}, sampled one second apart. */
    static List<Frame> session(long bdSeq, long birth, int lastSeq) {
        List<Frame> l = new ArrayList<>(List.of(nbirth(E, bdSeq, birth), dbirth(E, D, 1, birth + 1000)));
        for (int s = 2; s <= lastSeq; s++) l.add(ddata(E, D, s, birth + 1000L * s));
        return l;
    }
    @SafeVarargs static List<Frame> cat(List<Frame>... parts) { List<Frame> l = new ArrayList<>(); for (List<Frame> p : parts) l.addAll(p); return l; }

    static Stream<Row> rows() {
        List<Frame> wrap = new ArrayList<>(session(0, B1, 255));
        wrap.add(ddata(E, D, 0, B1 + 256_000));
        wrap.add(ddata(E, D, 1, B1 + 257_000));
        List<Frame> wrapGap = new ArrayList<>(session(0, B1, 254));
        wrapGap.add(ddata(E, D, 1, B1 + 257_000));
        return Stream.of(
            row("a normal session: DBIRTH consumes seq 1, no gap", session(0, B1, 3), List.of(up(D, B1, 0, 1)), 2),
            row("seq gap", cat(session(0, B1, 2), f(ddata(E, D, 5, B1 + 5000))), List.of(up(D, B1, 0, 1), ev("seq-gap", D, B1, 0, 5L)), 2),
            row("seq wraps at 255 without a gap", wrap, List.of(up(D, B1, 0, 1)), 256),
            row("a gap across the wrap", wrapGap, List.of(up(D, B1, 0, 1), ev("seq-gap", D, B1, 0, 1L)), 254),
            row("stop NDEATH after the birth closes the session (i)", cat(session(0, B1, 3), f(ndeath(E, 0, B1 + 3500))),
                List.of(up(D, B1, 0, 1), ev("session-down", D, B1, 0, 3L)), 2),
            row("Will NDEATH before the birth closes the session (ii)", cat(session(0, B1, 3), f(ndeath(E, 0, B1 - 20))),
                List.of(up(D, B1, 0, 1), ev("session-down", D, B1, 0, 3L)), 2),
            row("rebirth after an observed NCMD Rebirth", cat(session(0, B1, 2), f(ncmdRebirth(E), nbirth(E, 0, B2), dbirth(E, D, 1, B2 + 1000), ddata(E, D, 2, B2 + 2000))),
                List.of(up(D, B1, 0, 1), up(D, B2, 0, 1)), 2),
            row("a second NBIRTH with the same bdSeq and no rebirth ends the earlier connection ambiguous", cat(session(0, B1, 2), f(nbirth(E, 0, B2), dbirth(E, D, 1, B2 + 1000))),
                List.of(up(D, B1, 0, 1), ev("session-ambiguous", D, B1, 0, 2L), up(D, B2, 0, 1)), 1),
            row("after that, a death not after the new birth is ambiguous", cat(session(0, B1, 2), f(nbirth(E, 0, B2), dbirth(E, D, 1, B2 + 1000), ndeath(E, 0, B2 - 5))),
                List.of(up(D, B1, 0, 1), ev("session-ambiguous", D, B1, 0, 2L), up(D, B2, 0, 1), ev("session-ambiguous", D, B2, 0, 1L)), 1),
            row("after that, a death after the new birth closes it by (i)", cat(session(0, B1, 2), f(nbirth(E, 0, B2), dbirth(E, D, 1, B2 + 1000), ndeath(E, 0, B2 + 5))),
                List.of(up(D, B1, 0, 1), ev("session-ambiguous", D, B1, 0, 2L), up(D, B2, 0, 1), ev("session-down", D, B2, 0, 1L)), 1),
            row("25(a): the earlier connection's death delayed past the newer birth is stale-death", cat(session(0, B1, 3), session(1, B2, 2),
                f(ndeath(E, 0, B1 + 3500), ddata(E, D, 3, B2 + 3000))),
                List.of(up(D, B1, 0, 1), up(D, B2, 1, 1), ev("stale-death", D, B1, 0, 3L)), 4),
            row("25(b): NBIRTH lost, data dead-lettered, that bdSeq's NDEATH is the birthless session-ambiguous", cat(session(0, B1, 2),
                f(ndeath(E, 0, B1 + 2500), dbirth(E, D, 1, B2 + 1000), ddata(E, D, 2, B2 + 2000), ddata(E, D, 3, B2 + 3000), ndeath(E, 1, B2 + 3500))),
                List.of(up(D, B1, 0, 1), ev("session-down", D, B1, 0, 2L), ev("session-ambiguous", D, null, 1, 3L)), 1, "birthless", "birthless", "birthless"),
            row("D4: a birthless death on an edge with nothing held", f(ddata(E, D, 5, B1), ndeath(E, 3, B1 + 10)),
                List.of(ev("session-ambiguous", D, null, 3, 5L)), 0, "birthless"),
            row("D4: a birthless death that names no device yields no session event", f(ndeath(E, 0, B1)), List.of(), 0, "no-device"),
            row("D4: one birthless event per device the dead-lettered data named", f(ddata(E, D, 4, B1), ddata(E, D2, 5, B1), ddata(E, D, 6, B1 + 1000), ndeath(E, 2, B1 + 1500)),
                List.of(ev("session-ambiguous", D, null, 2, 6L), ev("session-ambiguous", D2, null, 2, 5L)), 0, "birthless", "birthless", "birthless"),
            row("a repeated DBIRTH signals a lost NBIRTH", cat(session(0, B1, 2), f(dbirth(E, D, 1, B2 + 1000), ddata(E, D, 2, B2 + 2000), ndeath(E, 1, B2 + 2500))),
                List.of(up(D, B1, 0, 1), ev("session-ambiguous", D, null, 1, 2L)), 1, "missing-birth", "missing-birth"),
            row("seq returning to 0 without an NBIRTH signals a lost NBIRTH and emits nothing yet", cat(session(0, B1, 3), f(ddata(E, D, 0, B2))),
                List.of(up(D, B1, 0, 1)), 2, "missing-birth"),
            row("a death in the millisecond of another connection's birth is ambiguous", cat(session(0, B1, 2), session(1, B2, 2), f(ndeath(E, 0, B2))),
                List.of(up(D, B1, 0, 1), up(D, B2, 1, 1), ev("session-ambiguous", D, B2, 1, 2L)), 2),
            row("births going backwards make the death ambiguous", cat(session(0, B2, 2), session(1, B1, 2), f(ndeath(E, 1, B1 + 2500))),
                List.of(up(D, B2, 0, 1), up(D, B1, 1, 1), ev("session-ambiguous", D, B1, 1, 2L)), 2),
            row("a DBIRTH lost after its NBIRTH: the data names the device and shows the gap", f(nbirth(E, 0, B1), ddata(E, D, 2, B1 + 2000)),
                List.of(up(D, B1, 0, 2), ev("seq-gap", D, B1, 0, 2L)), 1),
            row("two devices of one edge: every session event fans out per device", f(nbirth(E, 0, B1), dbirth(E, D, 1, B1 + 1000), dbirth(E, D2, 2, B1 + 1000),
                ddata(E, D, 3, B1 + 2000), ddata(E, D2, 4, B1 + 2000), ndeath(E, 0, B1 + 2500)),
                List.of(up(D, B1, 0, 1), up(D2, B1, 0, 2), ev("session-down", D, B1, 0, 3L), ev("session-down", D2, B1, 0, 4L)), 2),
            new Row("definition-changed when the NBIRTH digest differs from the pin", Map.of(D, "0".repeat(64)), session(0, B1, 2),
                List.of(up(D, B1, 0, 1), ev("definition-changed", D, B1, 0, 1L)), 1, List.of()),
            new Row("no definition-changed when the digest equals the pin", Map.of(D, DEF_SHA), session(0, B1, 2), List.of(up(D, B1, 0, 1)), 1, List.of()),
            // C1: a seq-gap waits for a valid event time; a death first drops it.
            row("C1: a gap followed by a message without sourceTs is written at the next valid time", cat(session(0, B1, 2),
                f(device("DDATA", E, D, 5, B1 + 5050, scan(null)), ddata(E, D, 6, B1 + 6000))),
                List.of(up(D, B1, 0, 1), ev("seq-gap", D, B1, 0, 5L)), 3),
            row("C1: a pending gap is dropped when a death ends the connection first", cat(session(0, B1, 2),
                f(device("DDATA", E, D, 5, B1 + 5050, scan(null)), ndeath(E, 0, B1 + 5500))),
                List.of(up(D, B1, 0, 1), ev("session-down", D, B1, 0, 5L)), 2),
            // C3: the signal itself emits nothing and ends nothing; one dead-letter per frame.
            row("C3: seq 0 right after the NBIRTH, before any device, is one missing-birth dead-letter", f(nbirth(E, 0, B1), ddata(E, D, 0, B1 + 1000)),
                List.of(), 0, "missing-birth"),
            row("C3: after the seq-0 signal the next death gives the signalling device the birthless shape", cat(session(0, B1, 3),
                f(ddata(E, D, 0, B2), ndeath(E, 1, B2 + 500))),
                List.of(up(D, B1, 0, 1), ev("session-ambiguous", D, null, 1, 0L)), 2, "missing-birth"),
            // C4: born devices not named by birthless DDATA get the born shape and that connection ends.
            row("C4: a named device takes the birthless shape, the other device of the born connection the born shape", f(nbirth(E, 0, B1),
                dbirth(E, D, 1, B1 + 1000), dbirth(E, D2, 2, B1 + 1000), ddata(E, D, 3, B1 + 2000), ddata(E, D2, 4, B1 + 2000),
                ddata(E, D, 0, B2), ndeath(E, 1, B2 + 500), ddata(E, D2, 5, B2 + 1000)),
                List.of(up(D, B1, 0, 1), up(D2, B1, 0, 2), ev("session-ambiguous", D, null, 1, 0L), ev("session-ambiguous", D2, B1, 0, 4L)), 2,
                "missing-birth", "birthless"),
            row("C4: a device only a DBIRTH named after the signal keeps the born shape", cat(session(0, B1, 2), f(dbirth(E, D, 1, B2 + 1000), ndeath(E, 1, B2 + 1500))),
                List.of(up(D, B1, 0, 1), ev("session-ambiguous", D, B1, 0, 2L)), 1, "missing-birth"),
            // C5: births out of arrival order are not cleared by a later birth.
            row("C5: a later in-order birth does not clear births out of order", cat(session(0, B2, 2), session(1, B1, 2), session(2, B2 + 5000, 2),
                f(ndeath(E, 2, B2 + 9000))),
                List.of(up(D, B2, 0, 1), up(D, B1, 1, 1), up(D, B2 + 5000, 2, 1), ev("session-ambiguous", D, B2 + 5000, 2, 2L)), 3),
            // C9: one death per identity; an identical redelivery is passed on for the core to mark duplicate.
            row("C9: a second, different death for the same identity is dead-lettered repeat-death", cat(session(0, B1, 2),
                f(ndeath(E, 5, B1 + 3000), ndeath(E, 6, B1 + 3100))),
                List.of(up(D, B1, 0, 1), ev("session-ambiguous", D, B1, 0, 2L)), 1, "repeat-death"),
            row("C9: an identical redelivery of the same death is written again", cat(session(0, B1, 2), f(ndeath(E, 5, B1 + 3000), ndeath(E, 5, B1 + 3000))),
                List.of(up(D, B1, 0, 1), ev("session-ambiguous", D, B1, 0, 2L), ev("session-ambiguous", D, B1, 0, 2L)), 1));
    }

    /** The rows' identical redeliveries are identical: a frame built twice, a few milliseconds apart, has the same bytes (Tahu stamps a
     *  metric without a timestamp with the wall clock, which made the C9 redelivery row depend on whether its two NDEATH frames were
     *  built within one millisecond). */
    @org.junit.jupiter.api.Test void aFrameBuiltTwiceIsByteIdentical() throws InterruptedException {
        Frame a = Sp.ndeath(E, 5, B1 + 3000), n = Sp.nbirth(E, 0, B1);
        Thread.sleep(5);
        assertArrayEquals(a.payload(), Sp.ndeath(E, 5, B1 + 3000).payload(), "NDEATH");
        assertArrayEquals(n.payload(), Sp.nbirth(E, 0, B1).payload(), "NBIRTH");
    }

    static List<Emitted> run(Row r) {
        BridgeCore core = new BridgeCore(r.pins());
        List<Emitted> out = new ArrayList<>();
        for (Frame x : r.frames()) out.addAll(core.accept(x));
        return out;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rows")
    void theBridgeEmitsTheSessionEventsOfTheRow(Row r) {
        List<Emitted> out = run(r);
        List<String> sessions = new ArrayList<>(), dead = new ArrayList<>();
        int messages = 0;
        for (Emitted e : out) {
            if (e instanceof Emitted.SessionEvent s) sessions.add(ev(s.kind(), s.device(), s.birthPublishTs(), s.bdSeq(), s.atSeq()));
            else if (e instanceof Emitted.DeadLetter d) dead.add(d.reason());
            else messages++;
        }
        assertEquals(r.sessions(), sessions, "session events");
        assertEquals(r.messages(), messages, "process messages");
        assertEquals(r.deadLetters(), dead, "dead-letter reasons");
    }
}
