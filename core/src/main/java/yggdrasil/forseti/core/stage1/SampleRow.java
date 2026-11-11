package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import yggdrasil.forseti.core.Json;

/** The sample journal body (spec 360-370, rule sheet S9): {@code {sourceTs, state, reason, active, attributedTo}}, keyed by sampleId.
 *  {@code reason} is null when attributed with no diagnostic; {@code attributedTo} is the visitRef when attributed, else null. */
final class SampleRow {
    static final String UNATTRIBUTED = "unattributed", ATTRIBUTED = "attributed", REPLAY_REQUIRED = "replay-required", QUARANTINED = "quarantined";

    /** A sample's S9 outcome: the row fields and the visit its observation counts for in coverage (S6), by visit key. */
    record Outcome(String state, String reason, ObjectNode attributedTo, String coverageVisit) {
        static Outcome of(String state, String reason) { return new Outcome(state, reason, null, null); }
    }

    private SampleRow() {}

    static ObjectNode body(Long sourceTs, Outcome o, JsonNode active) {
        ObjectNode n = Json.object();
        if (sourceTs == null) n.putNull("sourceTs"); else Json.putSafe(n, "sourceTs", sourceTs);
        n.put("state", o.state()).put("reason", o.reason());
        n.set("active", active);
        if (o.attributedTo() == null) n.putNull("attributedTo"); else n.set("attributedTo", o.attributedTo());
        return n;
    }
}
