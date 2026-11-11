package yggdrasil.forseti.core.stage2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import yggdrasil.forseti.core.Json;

/** Rule sheet T7: the impact assessment body (spec 372-379), keyed by {@code (source, incidentId, unitId)}:
 *  {@code {status, reason, incident, basis[] {visitRef, bodyDigest}, evidenceWarnings[] {visitRef, insufficiencyReasons}, rulesDigest}}.
 *  {@code basis[]} lists the related and possibly-related bases sorted by canonical visitRef bytes; {@code evidenceWarnings[]} those of
 *  them that are not {@code complete}. Basis revision numbers stay in the envelope ({@code basisRevisions}). */
public final class Assessment {
    private Assessment() {}

    /** {@code bases}: already sorted by canonical visitRef bytes. */
    public static ObjectNode body(DecisionTable.Decision d, ObjectNode incident, List<DecisionTable.Judged> bases, String rulesDigest) {
        ObjectNode b = Json.object().put("status", d.status()).put("reason", d.reason());
        b.set("incident", incident);
        ArrayNode basis = b.putArray("basis"), warnings = b.putArray("evidenceWarnings");
        for (DecisionTable.Judged j : bases) {
            ObjectNode e = basis.addObject();
            e.set("visitRef", j.visitRef());
            e.put("bodyDigest", j.bodyDigest());
            if (!"complete".equals(j.status())) {
                ObjectNode w = warnings.addObject();
                w.set("visitRef", j.visitRef());
                JsonNode r = j.body().path("insufficiencyReasons");
                w.set("insufficiencyReasons", r.isArray() ? r.deepCopy() : Json.JSON.createArrayNode());
            }
        }
        return b.put("rulesDigest", rulesDigest);
    }
}
