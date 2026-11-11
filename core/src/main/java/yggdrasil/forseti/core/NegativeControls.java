package yggdrasil.forseti.core;

/** The engine-independent negative controls (spec Negative controls), as the flags the decision functions are constructed with. Outside a
 *  control run every flag is false ({@link #NONE}); the cli only composes the values. In this step no code path reads a flag yet: the
 *  record is a constructor parameter of the decision functions from their first commit, and the behaviours follow with the control loop. */
public record NegativeControls(boolean processingTime, boolean dropLate, boolean noCascade, boolean firstWins, boolean stationGranularity,
                               boolean ignoreSessionGap, boolean wrongRulesPin, boolean lenientOracle) {
    public static final NegativeControls NONE = new NegativeControls(false, false, false, false, false, false, false, false);
}
