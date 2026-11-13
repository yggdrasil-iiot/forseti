package yggdrasil.forseti.cli.harness;

import java.util.List;
import yggdrasil.forseti.core.NegativeControls;

/** The names of the engine-independent negative controls (spec Negative controls, plan Task 22) and the core flags each one sets. Seven
 *  are the harness's ({@code forseti run --negative-control <name>}: the decision functions are constructed with the flag, or the adapter
 *  applies it); {@code lenient-oracle} is the oracle's ({@code --lenient}) and changes nothing in the harness. */
public final class Controls {
    private Controls() {}

    public static final String PROCESSING_TIME = "processing-time", DROP_LATE = "drop-late", NO_CASCADE = "no-cascade", LENIENT_ORACLE = "lenient-oracle",
        FIRST_WINS = "first-wins", STATION_GRANULARITY = "station-granularity", IGNORE_SESSION_GAP = "ignore-session-gap", WRONG_RULES_PIN = "wrong-rules-pin";
    public static final List<String> NAMES = List.of(PROCESSING_TIME, DROP_LATE, NO_CASCADE, LENIENT_ORACLE, FIRST_WINS, STATION_GRANULARITY,
        IGNORE_SESSION_GAP, WRONG_RULES_PIN);

    /** The flags of control {@code name}; {@link NegativeControls#NONE} for null. An unknown name is an {@link IllegalArgumentException}. */
    public static NegativeControls of(String name) {
        if (name == null) return NegativeControls.NONE;
        switch (name) {
            case PROCESSING_TIME: return new NegativeControls(true, false, false, false, false, false, false, false);
            case DROP_LATE: return new NegativeControls(false, true, false, false, false, false, false, false);
            case NO_CASCADE: return new NegativeControls(false, false, true, false, false, false, false, false);
            case FIRST_WINS: return new NegativeControls(false, false, false, true, false, false, false, false);
            case STATION_GRANULARITY: return new NegativeControls(false, false, false, false, true, false, false, false);
            case IGNORE_SESSION_GAP: return new NegativeControls(false, false, false, false, false, true, false, false);
            case WRONG_RULES_PIN: return new NegativeControls(false, false, false, false, false, false, true, false);
            case LENIENT_ORACLE: return new NegativeControls(false, false, false, false, false, false, false, true);
            default: throw new IllegalArgumentException("--negative-control " + name + ": not one of " + NAMES);
        }
    }

    /** Whether the harness applies control {@code name} (every control but the oracle's {@code lenient-oracle}). */
    public static boolean harness(String name) { return name != null && !LENIENT_ORACLE.equals(name) && NAMES.contains(name); }
}
