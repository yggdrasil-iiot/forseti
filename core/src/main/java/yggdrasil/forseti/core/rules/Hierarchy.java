package yggdrasil.forseti.core.rules;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import java.util.*;

/** The released ISA-95 equipment hierarchy (contract 2.23.0): node ids and their parents, and the path of a node, root first,
 *  joined by {@code :} (spec 133, the string Muninn publishes as the Sparkplug device id). Bifrost already refused cycles and
 *  missing parents; meeting one here is an IllegalArgumentException. */
public final class Hierarchy {
    private final String id, version, sha256;
    private final JsonNode node;
    private final Map<String, String> parents = new LinkedHashMap<>();

    private Hierarchy(JsonNode doc, String sha256) {
        this.node = doc; this.sha256 = sha256;
        this.id = Json.text(doc, "id"); this.version = Json.text(doc, "version");
        if (!doc.path("nodes").isArray()) throw new IllegalArgumentException("hierarchy " + id + ": nodes");
        for (JsonNode n : doc.path("nodes")) {
            String nid = Json.text(n, "equipmentInstanceId");
            JsonNode p = n.path("parentEquipmentInstanceId");
            if (parents.containsKey(nid)) throw new IllegalArgumentException("hierarchy " + id + ": duplicate node " + nid);
            parents.put(nid, p.isTextual() ? p.asText() : null);
        }
    }
    public static Hierarchy from(JsonNode doc) { return new Hierarchy(doc, null); }
    /** With the release's raw-byte digest of the hierarchy file, for the pins. */
    public static Hierarchy from(JsonNode doc, String sha256) { return new Hierarchy(doc, sha256); }

    public String id() { return id; }
    public String version() { return version; }
    public String sha256() { return sha256; }
    public JsonNode node() { return node; }
    public Set<String> nodes() { return Collections.unmodifiableSet(parents.keySet()); }

    /** Root-first path of a node joined by {@code :}. */
    public String path(String equipmentInstanceId) {
        Deque<String> parts = new ArrayDeque<>();
        Set<String> seen = new HashSet<>();
        String cur = equipmentInstanceId;
        while (cur != null) {
            if (!parents.containsKey(cur)) throw new IllegalArgumentException("hierarchy " + id + ": no node " + cur);
            if (!seen.add(cur)) throw new IllegalArgumentException("hierarchy " + id + ": cycle at " + cur);
            parts.addFirst(cur);
            cur = parents.get(cur);
        }
        return String.join(":", parts);
    }

    /** The station equal to {@code path} or a prefix {@code station + ":"} of it; exactly one by attribution.station-nested. */
    public Optional<String> stationOf(List<String> stations, String path) {
        String found = null;
        for (String s : stations) {
            if (!isDescendantOrSelf(s, path)) continue;
            if (found != null) throw new IllegalArgumentException("nested stations " + found + " and " + s);
            found = s;
        }
        return Optional.ofNullable(found);
    }

    public boolean isDescendantOrSelf(String ancestor, String path) { return path.equals(ancestor) || path.startsWith(ancestor + ":"); }
}
