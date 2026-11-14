package yggdrasil.forseti.bridge;

/** A Sparkplug B topic, {@code spBv1.0/<group>/<type>/<edge>[/<device>]}, as Muninn writes it (its device level is the ISA-95 path of the
 *  equipment, joined by {@code ':'}). {@code spBv1.0/STATE/<host>} is the host state topic and has no edge. A topic outside the
 *  namespace or with empty levels is not parsed ({@link #parse} returns null), so the bridge dead-letters it as malformed. */
public record SparkplugTopic(String group, String type, String edge, String device) {
    public static final String NAMESPACE = "spBv1.0";

    public static SparkplugTopic parse(String topic) {
        String[] p = topic.split("/", -1);
        if (p.length < 3 || !p[0].equals(NAMESPACE)) return null;
        for (String s : p) if (s.isEmpty()) return null;
        if (p[1].equals("STATE")) return p.length == 3 ? new SparkplugTopic(null, "STATE", null, null) : null;
        if (p.length == 4) return new SparkplugTopic(p[1], p[2], p[3], null);
        if (p.length == 5) return new SparkplugTopic(p[1], p[2], p[3], p[4]);
        return null;
    }

    /** The key of an edge's session state: a group and an edge id together name one edge node. */
    public String edgeKey() { return group + "/" + edge; }
}
