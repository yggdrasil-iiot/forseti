package yggdrasil.forseti.cli.live;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import yggdrasil.forseti.core.Json;

/** The injections of one live run, as the capture declares them in its manifest ({@code source.injections}): the scenario's
 *  {@code badStatus} entries (injection 3) and the runner's options, so that a recording says by itself which faults it carries and
 *  where ({@code recordings compare} judges its visits by them).
 *
 *  <p>A runner option is {@code <kind>=<spec>}:
 *  <ul>
 *  <li>{@code bridge-hold=<startMs>:<holdMs>} (injection 1, the bridge's hold stage);</li>
 *  <li>{@code bridge-source-offset=<ms>} (injection 9);</li>
 *  <li>{@code kill-muninn=<device>@<atMs>+<downMs>} (injection 8);</li>
 *  <li>{@code host-offline=<device>@<atMs>+<downMs>} (injection 25(a)).</li>
 *  </ul>
 *  A device is named below the site, as in the scenario. Each declared entry carries {@code injection} and {@code kind}; one with a
 *  device or equipment carries it as a full path with its {@code station}, and its window {@code [from, to)} in milliseconds from the
 *  drive's start (the scenario's clock; the runner times its options from the same start). A kind is given at most once. */
public final class LiveInjections {
    private LiveInjections() {}

    private static final Pattern HOLD = Pattern.compile("([0-9]+):([1-9][0-9]*)"), OFFSET = Pattern.compile("-?[0-9]+"),
        DEVICE = Pattern.compile("([A-Za-z0-9_:-]+)@([0-9]+)\\+([1-9][0-9]*)");

    /** The declared injections: the scenario's bad statuses, then the runner's options in the order given. */
    public static ArrayNode declare(LiveScenario scenario, LiveEquipment.LiveStream stream, List<String> options) {
        ArrayNode out = Json.JSON.createArrayNode();
        for (LiveScenario.BadStatus b : scenario.badStatus()) {
            ObjectNode n = out.addObject().put("injection", "3").put("kind", "bad-status").put("equipment", b.equipment())
                .put("station", stream.device(b.equipment()).station()).put("member", b.member());
            Json.putSafe(n, "from", b.from());
            Json.putSafe(n, "to", b.to());
        }
        Set<String> kinds = new HashSet<>();
        String site = LiveScenario.site(stream);
        for (String o : options) {
            int eq = o.indexOf('=');
            if (eq <= 0) throw refuse(o, "is not <kind>=<spec>");
            String kind = o.substring(0, eq), spec = o.substring(eq + 1);
            if (!kinds.add(kind)) throw refuse(o, "names " + kind + " twice");
            switch (kind) {
                case "bridge-hold" -> {
                    Matcher m = match(HOLD, spec, o);
                    ObjectNode n = out.addObject().put("injection", "1").put("kind", kind);
                    Json.putSafe(n, "startMs", Long.parseLong(m.group(1)));
                    Json.putSafe(n, "holdMs", Long.parseLong(m.group(2)));
                }
                case "bridge-source-offset" -> {
                    match(OFFSET, spec, o);
                    long ms = Long.parseLong(spec);
                    if (ms == 0) throw refuse(o, "is an offset of 0");
                    out.addObject().put("injection", "9").put("kind", kind).put("offsetMs", ms);
                }
                case "kill-muninn", "host-offline" -> {
                    Matcher m = match(DEVICE, spec, o);
                    String below = m.group(1), device = below.startsWith(site + ":") ? below : site + ":" + below;
                    if (!stream.equipment().containsKey(device)) throw refuse(o, "names no device of stream " + stream.stream());
                    long at = Long.parseLong(m.group(2)), down = Long.parseLong(m.group(3));
                    ObjectNode n = out.addObject().put("injection", kind.equals("kill-muninn") ? "8" : "25(a)").put("kind", kind).put("equipment", device)
                        .put("station", stream.device(device).station());
                    Json.putSafe(n, "from", at);
                    Json.putSafe(n, "to", at + down);
                }
                default -> throw refuse(o, "is no injection kind (bridge-hold, bridge-source-offset, kill-muninn, host-offline)");
            }
        }
        return out;
    }

    private static Matcher match(Pattern p, String spec, String option) {
        Matcher m = p.matcher(spec);
        if (!m.matches()) throw refuse(option, "does not match " + p.pattern());
        return m;
    }

    private static IllegalArgumentException refuse(String option, String why) { return new IllegalArgumentException("injection " + option + " " + why); }
}
