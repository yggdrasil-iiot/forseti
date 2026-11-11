package yggdrasil.forseti.cli.recording;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Seeded, platform-independent telemetry values. The uniform draw for a (seed, device, scan) is the top 53 bits of
 *  SHA-256("seed:device:scanIndex") over 2^53, an exact decimal; no {@code Random}, no clock, no floating-point arithmetic on the way
 *  to the printed value, so a rebuild on any JDK and OS writes the same bytes. {@code scanIndex} is the scan's offset from T0 divided
 *  by {@code scanMs}, so it does not move when a scenario's telemetry window moves. */
public final class Telemetry {
    private static final BigDecimal TWO_POW_53 = new BigDecimal(BigInteger.ONE.shiftLeft(53));
    /** The printed idle torque. */
    public static final BigDecimal IDLE = new BigDecimal("0.0");

    private Telemetry() {}

    /** u in [0, 1), exact. */
    public static BigDecimal uniform(long seed, String device, long scanIndex) {
        byte[] h;
        try { h = MessageDigest.getInstance("SHA-256").digest((seed + ":" + device + ":" + scanIndex).getBytes(StandardCharsets.UTF_8)); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
        long top = 0;
        for (int i = 0; i < 8; i++) top = (top << 8) | (h[i] & 0xff);
        return new BigDecimal(BigInteger.valueOf(top >>> 11)).divide(TWO_POW_53);   // exact: a dyadic fraction
    }

    /** {@code min + (max - min) * u}, rounded half-even to two decimals, trailing zeros dropped but one decimal kept (23.45, 23.4, 10.0). */
    public static BigDecimal torque(long seed, String device, long scanIndex, BigDecimal min, BigDecimal max) {
        BigDecimal v = min.add(max.subtract(min).multiply(uniform(seed, device, scanIndex))).setScale(2, RoundingMode.HALF_EVEN);
        return printable(v);
    }

    static BigDecimal printable(BigDecimal v) {
        BigDecimal s = v.stripTrailingZeros();
        return s.scale() < 1 ? s.setScale(1) : s;
    }
}
