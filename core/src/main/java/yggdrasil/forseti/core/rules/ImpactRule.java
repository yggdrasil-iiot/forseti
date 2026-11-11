package yggdrasil.forseti.core.rules;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import java.util.*;

/** A typed view of a released impact rule (contract 2.27.0); the node and the release's raw-byte digest of its file are kept. */
public final class ImpactRule {
    private final JsonNode node;
    private final String sha256;
    private final List<String> incidentScope;

    public ImpactRule(JsonNode node, String sha256) {
        this.node = node;
        this.sha256 = sha256;
        List<String> s = new ArrayList<>();
        node.path("incidentScope").forEach(p -> s.add(p.textValue()));
        this.incidentScope = List.copyOf(s);
    }
    public JsonNode node() { return node; }
    public String sha256() { return sha256; }
    public String id() { return Json.text(node, "id"); }
    public String version() { return Json.text(node, "version"); }
    public String attributionRuleId() { return Json.text(node.path("attributionRuleRef"), "id"); }
    public String attributionRuleVersion() { return Json.text(node.path("attributionRuleRef"), "version"); }
    public String incidentBindingId() { return Json.text(node.path("incidentBindingRef"), "id"); }
    public String incidentBindingVersion() { return Json.text(node.path("incidentBindingRef"), "version"); }
    public List<String> incidentScope() { return incidentScope; }
    public String intervalStart() { return Json.text(node.path("interval"), "start"); }
    public String overlap() { return Json.text(node.path("interval"), "overlap"); }
    public String openEnd() { return Json.text(node.path("interval"), "openEnd"); }
    public String match() { return Json.text(node.path("evidence"), "match"); }
    public String onBasisRetracted() { return Json.text(node.path("evidence"), "onBasisRetracted"); }
    public String onWithdrawn() { return Json.text(node.path("evidence"), "onWithdrawn"); }
    public long workRecordsMs() { return Json.safeLong(node.path("retention").path("workRecordsMs"), "retention.workRecordsMs", Json.NON_NEGATIVE); }
}
