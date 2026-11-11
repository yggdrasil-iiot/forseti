package yggdrasil.forseti.core.stage2;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.SortedMap;
import yggdrasil.forseti.core.Json;

/** Rule sheet T8: the affected set body (spec 383-400 as fixed by Task 1's lines 392-398), keyed by {@code (source, incidentId)}:
 *  {@code {incident, units[] {unitId, status, reason, bodyDigest} sorted by unitId, resolution, coverage}}; exactly the spec's field
 *  list, so no {@code rulesDigest}. {@code resolution} is {@code withdrawn} or null. */
public final class AffectedSet {
    /** A live unit row: its assessment's status, reason and body digest. */
    public record Unit(String status, String reason, String bodyDigest) {}

    private AffectedSet() {}

    public static ObjectNode body(ObjectNode incident, SortedMap<String, Unit> units, boolean withdrawn, ObjectNode coverage) {
        ObjectNode b = Json.object();
        b.set("incident", incident);
        ArrayNode u = b.putArray("units");
        units.forEach((id, x) -> u.addObject().put("unitId", id).put("status", x.status()).put("reason", x.reason()).put("bodyDigest", x.bodyDigest()));
        b.put("resolution", withdrawn ? "withdrawn" : null);
        b.set("coverage", coverage);
        return b;
    }
}
