package yggdrasil.forseti.cli;

import java.util.*;

/** The flags of one command: {@code --name value} pairs and the named switches ({@code --name} alone). A flag given twice, a value-less
 *  flag, a bare word and (through {@link #only}) a flag the command does not know are usage errors ({@link IllegalArgumentException}). */
public final class Args {
    private final Map<String, String> flags;
    private Args(Map<String, String> flags) { this.flags = flags; }

    public static Args parse(String[] a, int from, Set<String> switches) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = from; i < a.length; i++) {
            if (!a[i].startsWith("--") || a[i].length() == 2) throw new IllegalArgumentException("unexpected argument " + a[i]);
            String name = a[i].substring(2), value;
            if (switches.contains(name)) value = "true";
            else if (i + 1 >= a.length) throw new IllegalArgumentException("--" + name + " needs a value");
            else value = a[++i];
            if (m.put(name, value) != null) throw new IllegalArgumentException("--" + name + " given twice");
        }
        return new Args(m);
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
}
