package yggdrasil.forseti.core.input;

/** OPC UA status code severity from the raw bits (spec: Bad is judged from the status code's severity bits). The two top bits are the
 *  severity: 00 Good, 01 Uncertain, 10 Bad. A null status code is neither Bad nor Uncertain. */
public final class StatusCodes {
    private static final long SEVERITY = 0xC0000000L;
    private StatusCodes() {}
    public static boolean bad(Long code) { return code != null && (code & SEVERITY) == 0x80000000L; }
    public static boolean uncertain(Long code) { return code != null && (code & SEVERITY) == 0x40000000L; }
    public static boolean good(Long code) { return !bad(code) && !uncertain(code); }
}
