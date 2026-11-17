package yggdrasil.forseti.cli.replay;

import java.util.*;
import java.util.regex.Pattern;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.stage1.Stage1;
import yggdrasil.forseti.core.stage2.Stage2;

/** The topics of one replay run and their settings, without Kafka (spec "Topics and partitioning", 465-496; plan D1). Every run names
 *  its topics with a prefix of its own (spec: "Each comparison run writes to its own topic prefix"), so every partition starts at offset
 *  0. Record timestamps are {@code LogAppendTime} everywhere. Input topics retain 30 d and output journals 7 d (the spec's floor). Six
 *  partitions exist everywhere except the two control topics {@code forseti.generations} and {@code forseti.recovery-control} (one) and
 *  the two disposition ledgers (seven: station partitions 0-5 and the reject collector's partition 6). The control topics are never
 *  deleted by retention ({@code retention.ms=-1}): a generation row and a recovery declaration are read back for the life of the store.
 *  The Kafka Streams internal topics and {@code forseti.lineage} belong to their own steps and are not planned here. */
public final class ReplayTopics {
    private ReplayTopics() {}

    public static final String TIMESTAMP_TYPE_KEY = "message.timestamp.type", RETENTION_KEY = "retention.ms", LOG_APPEND_TIME = "LogAppendTime";
    public static final long INPUT_RETENTION_MS = 30L * 24 * 60 * 60 * 1000, OUTPUT_RETENTION_MS = 7L * 24 * 60 * 60 * 1000, CONTROL_RETENTION_MS = -1;
    public static final String GENERATIONS = "forseti.generations", RECOVERY_CONTROL = "forseti.recovery-control", QUARANTINE = "forseti.quarantine";
    public static final String DISPOSITIONS1 = "forseti.dispositions-stage1", DISPOSITIONS2 = Stage2.DISPOSITIONS;
    public static final int PARTITIONS = 6, LEDGER_PARTITIONS = 7, CONTROL_PARTITIONS = 1;

    /** The four input topics of the contract, in barrier order. */
    public static final List<String> INPUTS = List.of(InputParser.MESSAGES, InputParser.SESSIONS, InputParser.OCCUPANCY, InputParser.INCIDENTS);
    /** The output journals: the four business topics, the two ledgers, the quarantine side store. */
    public static final List<String> OUTPUTS = List.of(Stage1.WORK_RECORDS, Stage1.SAMPLES, Stage2.AFFECTED, Stage2.AFFECTED_SETS, DISPOSITIONS1, DISPOSITIONS2, QUARANTINE);
    public static final List<String> CONTROL = List.of(GENERATIONS, RECOVERY_CONTROL);

    public enum Role { INPUT, OUTPUT, CONTROL }

    /** One topic to create: its contract name, the run's name for it, its role, its partition count and its topic configs. */
    public record Spec(String base, String name, Role role, int partitions, SortedMap<String, String> configs) {}

    /** What the broker reports for an existing topic: its partition count and its effective topic configs. */
    public record Observed(String name, int partitions, Map<String, String> configs) {}

    private static final Pattern LEGAL = Pattern.compile("[a-zA-Z0-9._-]*");

    /** {@code prefix + base}; the prefix carries its own separator ({@code run-1.}). */
    public static String name(String prefix, String base) {
        String p = prefix == null ? "" : prefix;
        if (!LEGAL.matcher(p).matches()) throw new IllegalArgumentException("topic prefix '" + p + "' has a character Kafka refuses in a topic name");
        String n = p + base;
        if (n.length() > 249) throw new IllegalArgumentException("topic name " + n + " is longer than 249 characters");
        return n;
    }

    /** Every topic of a run with {@code prefix}: inputs, outputs, control, in that order. */
    public static List<Spec> plan(String prefix) {
        List<Spec> l = new ArrayList<>();
        for (String b : INPUTS) l.add(spec(prefix, b, Role.INPUT, PARTITIONS, INPUT_RETENTION_MS));
        for (String b : OUTPUTS) l.add(spec(prefix, b, Role.OUTPUT, b.equals(DISPOSITIONS1) || b.equals(DISPOSITIONS2) ? LEDGER_PARTITIONS : PARTITIONS, OUTPUT_RETENTION_MS));
        for (String b : CONTROL) l.add(spec(prefix, b, Role.CONTROL, CONTROL_PARTITIONS, CONTROL_RETENTION_MS));
        return List.copyOf(l);
    }

    private static Spec spec(String prefix, String base, Role role, int partitions, long retention) {
        SortedMap<String, String> c = new TreeMap<>();
        c.put(TIMESTAMP_TYPE_KEY, LOG_APPEND_TIME);
        c.put(RETENTION_KEY, Long.toString(retention));
        return new Spec(base, name(prefix, base), role, partitions, Collections.unmodifiableSortedMap(c));
    }

    /** The run's name of {@code base} in {@code plan}. */
    public static String nameOf(List<Spec> plan, String base) {
        for (Spec s : plan) if (s.base().equals(base)) return s.name();
        throw new IllegalArgumentException("no topic " + base + " in the plan");
    }

    /** The contract name of the run's topic {@code name} in {@code plan}. */
    public static String baseOf(List<Spec> plan, String name) {
        for (Spec s : plan) if (s.name().equals(name)) return s.base();
        throw new IllegalArgumentException("no topic named " + name + " in the plan");
    }

    /** Everything that keeps the broker's topics from being the plan (the gate's topic-settings assertion): a missing topic, another
     *  partition count, another timestamp type or another retention. Empty when every planned topic exists exactly as planned. */
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
