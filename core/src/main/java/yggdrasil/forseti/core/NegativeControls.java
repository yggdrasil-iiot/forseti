package yggdrasil.forseti.core;

/** The engine-independent negative controls (spec Negative controls), as the flags the decision functions are constructed with. Outside a
 *  control run every flag is false ({@link #NONE}); the cli only composes the values from {@code --negative-control}. Read by
 *  {@code Stage1} ({@code dropLate}: a beyond-horizon input leaves without its {@code replay-required} disposition; {@code firstWins}: S7
 *  keeps the visit whose arrival was delivered first; {@code ignoreSessionGap}: S8 adds no {@code session-gap}), by {@code Stage2}
 *  ({@code noCascade}: a basis revision after an assessment row exists is ignored; {@code stationGranularity}: T5 reads the station's
 *  activity as the tool's) and by the adapter ({@code processingTime}: event times replaced by delivery sequence numbers,
 *  {@code Stage1.processingTime}; {@code wrongRulesPin}: the envelope pins name another release's attribution rule). {@code lenientOracle}
 *  is the oracle's ({@code --lenient}); no core code path reads it. */
public record NegativeControls(boolean processingTime, boolean dropLate, boolean noCascade, boolean firstWins, boolean stationGranularity,
                               boolean ignoreSessionGap, boolean wrongRulesPin, boolean lenientOracle) {
    public static final NegativeControls NONE = new NegativeControls(false, false, false, false, false, false, false, false);
}
