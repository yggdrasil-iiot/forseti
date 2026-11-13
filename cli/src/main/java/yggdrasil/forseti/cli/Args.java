package yggdrasil.forseti.cli;

import java.util.*;

/** The flags of one command: {@code --name value} pairs and the named switches ({@code --name} alone). A flag given twice (unless the
 *  command names it repeatable), a value-less flag, a bare word and (through {@link #only}) a flag the command does not know are usage
 *  errors ({@link IllegalArgumentException}). */
public final class Args {
    private final Map<String, String> flags;
    private final Map<String, List<String>> repeated;
    private Args(Map<String, String> flags, Map<String, List<String>> repeated) { this.flags = flags; this.repeated = repeated; }

    public static Args parse(String[] a, int from, Set<String> switches) { return parse(a, from, switches, Set.of()); }

    /** {@code repeatable}: the value flags that may be given more than once ({@link #all} returns them in order). */
    public static Args parse(String[] a, int from, Set<String> switches, Set<String> repeatable) {
        Map<String, String> m = new LinkedHashMap<>();
        Map<String, List<String>> r = new LinkedHashMap<>();
        for (int i = from; i < a.length; i++) {
            if (!a[i].startsWith("--") || a[i].length() == 2) throw new IllegalArgumentException("unexpected argument " + a[i]);
            String name = a[i].substring(2), value;
            if (switches.contains(name)) value = "true";
            else if (i + 1 >= a.length) throw new IllegalArgumentException("--" + name + " needs a value");
            else value = a[++i];
            r.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
            if (m.put(name, value) != null && !repeatable.contains(name)) throw new IllegalArgumentException("--" + name + " given twice");
        }
        return new Args(m, r);
    }

    /** Refuses a flag outside {@code allowed}; returns this. */
    public Args only(Set<String> allowed) {
        for (String k : flags.keySet()) if (!allowed.contains(k)) throw new IllegalArgumentException("unknown flag --" + k);
        return this;
    }
    public String required(String name) {
        String v = flags.get(name);
        if (v == null) throw new IllegalArgumentException("--" + name + " is required");
        return v;
    }
    public String get(String name, String fallback) { return flags.getOrDefault(name, fallback); }
    public Optional<String> get(String name) { return Optional.ofNullable(flags.get(name)); }
    public boolean has(String name) { return flags.containsKey(name); }
    /** Every value of a flag in command-line order (empty when absent). */
    public List<String> all(String name) { return List.copyOf(repeated.getOrDefault(name, List.of())); }
}
