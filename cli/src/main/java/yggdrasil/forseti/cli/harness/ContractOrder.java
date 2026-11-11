package yggdrasil.forseti.cli.harness;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import yggdrasil.forseti.core.input.Inputs;
import yggdrasil.forseti.core.stage1.Events;

/** The order a cut's inputs are applied in at a station: the contract order of spec 491, {@code (eventTime, kindRank, identity bytes,
 *  payloadDigest)}, then the origin {@code (topic, partition, offset)} so that two records equal under the contract (a byte-identical
 *  redelivery) are applied in one order whatever the delivery order was. {@link #shuffle} permutes a batch by the seed first; the sort
 *  makes that permutation irrelevant. */
public final class ContractOrder {
    private ContractOrder() {}

    public static final Comparator<Inputs.InputRecord> ORDER = Events.CONTRACT_ORDER.thenComparing(ContractOrder::byOrigin);

    /** The rejected inputs of a cut are written in origin order. */
    public static final Comparator<Inputs.InputRecord> ORIGIN = ContractOrder::byOrigin;

    public static List<Inputs.InputRecord> sort(List<Inputs.InputRecord> in) {
        List<Inputs.InputRecord> l = new ArrayList<>(in);
        l.sort(ORDER);
        return l;
    }

    public static <T> void shuffle(List<T> batch, Random random) { Collections.shuffle(batch, random); }

    private static int byOrigin(Inputs.InputRecord a, Inputs.InputRecord b) {
        int c = a.origin().topic().compareTo(b.origin().topic());
        if (c != 0) return c;
        c = Integer.compare(a.origin().partition(), b.origin().partition());
        return c != 0 ? c : Long.compare(a.origin().offset(), b.origin().offset());
    }
}
