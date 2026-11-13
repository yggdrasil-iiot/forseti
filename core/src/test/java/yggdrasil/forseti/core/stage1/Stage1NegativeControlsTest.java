package yggdrasil.forseti.core.stage1;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.core.stage1.Driver.*;
import static yggdrasil.forseti.core.stage1.Fixtures.*;

import com.fasterxml.jackson.databind.node.DoubleNode;
import java.util.List;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.NegativeControls;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.journal.Disposition;

/** The stage-1 flags of the negative controls (plan Task 22): each changes the decision it names and nothing else; the gate proves the
 *  assertions fail with them (cli {@code NegativeControlsTest}). */
class Stage1NegativeControlsTest {
    static final String MSG = InputParser.MESSAGES;
    static final long H = 86_400_000;
    static final NegativeControls DROP_LATE = new NegativeControls(false, true, false, false, false, false, false, false);

    /** drop-late: the sample beyond the horizon leaves without its replay-required disposition and without a row; the admitted ones keep
     *  theirs, so their bodies match the run without the control. */
    @Test void dropLateLeavesTheBeyondHorizonSampleWithoutADisposition() {
        for (boolean control : List.of(false, true)) {
            Driver d = new Driver(line2(), S1, control ? DROP_LATE : NegativeControls.NONE);
            long a = T + H;
            d.cut(a);
            Inputs.InputRecord at = d.input(MSG, d.message(NR1, a - H, member("Torque", new DoubleNode(1.0), 0, a - H)));
            Inputs.InputRecord below = d.input(MSG, d.message(NR1, a - H - 1, member("Torque", new DoubleNode(1.0), 0, a - H - 1)));
            d.cut(a);
            assertEquals(Disposition.Reason.ACCEPTED, d.disposition(at, "member:Torque").body().reason());
            assertEquals("no-occupancy", d.sampleRow(at, "Torque").get("reason").asText());
            if (!control) {
                assertEquals(Disposition.Reason.BEYOND_HORIZON, d.disposition(below, "member:Torque").body().reason());
                assertEquals("replay-required", d.sampleRow(below, "Torque").get("state").asText());
            } else {
                assertEquals(List.of(), d.dispositions(below), "no disposition of the dropped input");
                assertNull(d.sampleRow(below, "Torque"), "no sample row of the dropped input");
            }
        }
    }

    /** processing-time: the adapter's rewrite gives every member sourceTs the delivery sequence number and leaves the rest. */
    @Test void processingTimeReplacesTheEventTimesByTheDeliverySequence() {
        Driver d = Driver.station1();
        Inputs.InputRecord m = d.input(MSG, d.message(NR1, T, member("Torque", new DoubleNode(1.0), 0, T)));
        Inputs.InputRecord r = Stage1.processingTime(m, 7);
        Inputs.ProcessMessage p = (Inputs.ProcessMessage) r.parsed(), o = (Inputs.ProcessMessage) m.parsed();
        assertEquals(Long.valueOf(7), p.members().get(0).sourceTs());
        assertEquals(o.members().get(0).value(), p.members().get(0).value());
        assertEquals(o.seq(), p.seq());
        assertSame(m.rawValue(), r.rawValue());
        assertEquals(m.payloadDigest(), r.payloadDigest());
    }
}
