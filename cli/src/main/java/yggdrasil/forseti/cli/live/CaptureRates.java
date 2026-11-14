package yggdrasil.forseti.cli.live;

import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;

/** The two rates of step-5 plan D8, measured on the captured {@code ot.messages}, per device and in total.
 *
 *  <p>Muninn reads a device's members one node at a time, so a poll can straddle a sim scan: such a message carries members of two
 *  scans, i.e. more than one distinct {@code sourceTs} among the members the core keeps (or a kept member without one). That is an
 *  <b>incoherent snapshot</b>; the core reports its samples {@code incoherent-snapshot} under plc-stamped. And because Muninn's poll
 *  and the sim's scan run at the same period without a phase lock, a poll can also land twice inside one scan: that message's largest
 *  member {@code sourceTs} equals the largest of an earlier message of the same device, a <b>repeated sourceTs</b>. Both are kept in
 *  the capture as emitted; this class only counts them. A message the core's reader rejects is counted under {@code rejected} and in
 *  neither rate. Rates are count / messages of the device, six decimals, half-even. */
public final class CaptureRates {
    private CaptureRates() {}

    private static final class Counts { long messages, incoherent, repeated, rejected; final Set<Long> seen = new HashSet<>(); }

    /** {@code values}: the captured {@code ot.messages} value bytes in delivery order (partition by partition, by offset). */
    public static ObjectNode measure(List<byte[]> values) {
        Map<String, Counts> per = new TreeMap<>();
        Counts total = new Counts();
        long offset = 0;
        for (byte[] v : values) {
            InputParser.Read r = InputParser.read(new Inputs.Origin(InputParser.MESSAGES, 0, offset++), null, v);
            if (r.rejected() || !(r.record().parsed() instanceof Inputs.ProcessMessage m)) { total.rejected++; continue; }
            Counts c = per.computeIfAbsent(m.device(), k -> new Counts());
            Set<Long> ts = new HashSet<>();
            Long max = null;
            for (Inputs.Member x : m.members()) {
                ts.add(x.sourceTs());
                if (x.sourceTs() != null && (max == null || x.sourceTs() > max)) max = x.sourceTs();
            }
            boolean incoherent = ts.size() != 1 || ts.contains(null);
            boolean repeated = max != null && !c.seen.add(max);
            for (Counts k : List.of(c, total)) {
                k.messages++;
                if (incoherent) k.incoherent++;
                if (repeated) k.repeated++;
            }
        }
        ObjectNode n = json(total);
        Json.putSafe(n, "rejected", total.rejected);
        ObjectNode pd = n.putObject("perDevice");
        per.forEach((d, c) -> pd.set(d, json(c)));
        return n;
    }

    private static ObjectNode json(Counts c) {
        ObjectNode o = Json.object();
        Json.putSafe(o, "messages", c.messages);
        Json.putSafe(o, "incoherentSnapshots", c.incoherent);
        o.set("incoherentSnapshotRate", rate(c.incoherent, c.messages));
        Json.putSafe(o, "repeatedSourceTs", c.repeated);
        o.set("repeatedSourceTsRate", rate(c.repeated, c.messages));
        return o;
    }

    static DecimalNode rate(long n, long of) {
        if (of == 0) return new DecimalNode(BigDecimal.ZERO.setScale(6));
        return new DecimalNode(BigDecimal.valueOf(n).divide(BigDecimal.valueOf(of), 6, RoundingMode.HALF_EVEN));
    }
}
