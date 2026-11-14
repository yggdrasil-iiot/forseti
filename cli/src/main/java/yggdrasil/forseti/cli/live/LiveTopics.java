package yggdrasil.forseti.cli.live;

import java.util.*;
import java.util.regex.Pattern;

/** The input topics of one live run, without Kafka (step-5 plan D1, D6; spec "Topics and partitioning").
 *
 *  <p>Why fresh names: a recording is the input topics only, and the recorder writes real offsets, so every partition of a run must
 *  start at offset 0. Reusing a topic would carry an earlier run's records and offsets into the capture; the run therefore names its
 *  topics with a prefix of its own ({@link #name}), and the bridge, the producers and the recorder are given those names.
 *
 *  <p>Why these settings: record timestamps are {@code LogAppendTime} (the spec fixes it, and the recorder reads append times as the
 *  record timestamps of the capture), input topics retain 30 d (Decision 9) and every input topic has six partitions, because station
 *  partitions 0..2 of {@code mfg.occupancy} and the device placement of the routing manifest both assume six. {@code ot.dead-letter}
 *  is not named in the spec's topic table; it takes the same settings, the spec's rule for every topic it does not list
 *  ("six partitions exist everywhere except ..."). */
public final class LiveTopics {
    public static final int PARTITIONS = 6;
    public static final long RETENTION_MS = 30L * 24 * 60 * 60 * 1000;
    public static final String TIMESTAMP_TYPE_KEY = "message.timestamp.type";
    public static final String RETENTION_KEY = "retention.ms";
    public static final String LOG_APPEND_TIME = "LogAppendTime";
    public static final String DEAD_LETTER = "ot.dead-letter";
    /** The base names, in creation order: the four input topics of the contract, then the bridge's dead-letter. */
    public static final List<String> BASES = List.of("ot.messages", "ot.sessions", "mfg.occupancy", "quality.incidents", DEAD_LETTER);

    /** Kafka's legal topic characters; the full name is at most 249 characters and never "." or "..". */
    private static final Pattern LEGAL = Pattern.compile("[a-zA-Z0-9._-]*");

    private LiveTopics() {}

    /** One topic to create: its contract base name, the run's name for it, its partition count and its topic configs. */
    public record Spec(String base, String name, int partitions, SortedMap<String, String> configs) {}

    /** What the broker reports for an existing topic: its partition count and its effective topic configs. */
    public record Observed(String name, int partitions, Map<String, String> configs) {}

    /** {@code prefix + base}; the prefix is taken verbatim (it carries its own separator, e.g. {@code run-20261115.}). */
    public static String name(String prefix, String base) {
        String p = prefix == null ? "" : prefix;
        if (!LEGAL.matcher(p).matches()) throw new IllegalArgumentException("topic prefix '" + p + "' has a character Kafka refuses in a topic name");
        String n = p + base;
        if (n.length() > 249) throw new IllegalArgumentException("topic name " + n + " is longer than 249 characters");
        return n;
    }

    /** The five topics of a run with {@code prefix}. */
    public static List<Spec> plan(String prefix) {
        SortedMap<String, String> configs = new TreeMap<>();
        configs.put(TIMESTAMP_TYPE_KEY, LOG_APPEND_TIME);
        configs.put(RETENTION_KEY, Long.toString(RETENTION_MS));
        List<Spec> l = new ArrayList<>();
        for (String b : BASES) l.add(new Spec(b, name(prefix, b), PARTITIONS, Collections.unmodifiableSortedMap(new TreeMap<>(configs))));
        return List.copyOf(l);
    }

    /** The run's name of {@code base} in {@code plan}. */
    public static String nameOf(List<Spec> plan, String base) {
        for (Spec s : plan) if (s.base().equals(base)) return s.name();
        throw new IllegalArgumentException("no topic " + base + " in the plan");
    }

    /** Everything that keeps the broker's topics from being the plan: a missing topic, another partition count, another timestamp type
     *  or another retention. Empty when every planned topic exists exactly as planned. */
    public static List<String> problems(List<Spec> plan, Map<String, Observed> observed) {
        List<String> p = new ArrayList<>();
        for (Spec s : plan) {
            Observed o = observed.get(s.name());
            if (o == null) { p.add("topic " + s.name() + " does not exist"); continue; }
            if (o.partitions() != s.partitions()) p.add("topic " + s.name() + " has " + o.partitions() + " partitions, not " + s.partitions());
            for (Map.Entry<String, String> c : s.configs().entrySet()) {
                String actual = o.configs().get(c.getKey());
                if (!c.getValue().equals(actual)) p.add("topic " + s.name() + " has " + c.getKey() + "=" + actual + ", not " + c.getValue());
            }
        }
        return p;
    }
}
