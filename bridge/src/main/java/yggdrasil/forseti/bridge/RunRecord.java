package yggdrasil.forseti.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** The run record of one bridge run: the declared stages with what each did (plan D3), the counts per output topic and dead-letter
 *  reason, and how the run ended. A run with an injection stage is never mistaken for a clean one: the stage is named here. */
public final class RunRecord {
    private final Map<String, Long> written = new TreeMap<>();
    private final Map<String, Long> deadLetters = new TreeMap<>();
    private long frames;
    private String outcome = "running";
    private String failure;

    void frame() { frames++; }
    void written(Records.Out r) {
        written.merge(r.topic(), 1L, Long::sum);
        String reason = r.headers().get(Records.H_REASON);
        if (reason != null) deadLetters.merge(reason, 1L, Long::sum);
    }
    public void end(String outcome, String failure) { this.outcome = outcome; this.failure = failure; }

    private java.util.SortedSet<String> unpinned = new java.util.TreeSet<>();
    private boolean routed;
    /** The devices that came up without a pin. With a routing manifest every device is pinned at start (none can appear); without
     *  one, a run whose devices were not compared with a pin says so here. */
    public void pins(boolean routed, java.util.Set<String> unpinnedSeen) { this.routed = routed; this.unpinned = new java.util.TreeSet<>(unpinnedSeen); }

    public Map<String, Object> toMap(List<Stage> stages) { return toMap(List.of(), stages); }

    /** {@code inputStages}: the declared stages in front of the core (injection 25(a)); {@code stages}: the D3 stages after it. */
    public Map<String, Object> toMap(List<InputStage> inputs, List<Stage> stages) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("kind", "forseti-bridge-run");
        List<Object> in = new ArrayList<>();
        for (InputStage x : inputs) in.add(x.record());
        m.put("inputStages", in);
        List<Object> s = new ArrayList<>();
        for (Stage x : stages) s.add(x.record());
        m.put("stages", s);
        m.put("frames", frames);
        m.put("written", written);
        m.put("deadLetters", deadLetters);
        m.put("routing", routed);
        m.put("unpinnedDevices", List.copyOf(unpinned));
        m.put("outcome", outcome);
        m.put("failure", failure);
        return m;
    }

    /** Written with LF line ends whatever the platform, so the record of a run reads the same on Linux and Windows. */
    public void write(Path p, List<Stage> stages) { write(p, List.of(), stages); }

    public void write(Path p, List<InputStage> inputs, List<Stage> stages) {
        try {
            if (p.getParent() != null) Files.createDirectories(p.getParent());
            String text = new String(new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsBytes(toMap(inputs, stages)), StandardCharsets.UTF_8);
            Files.write(p, (text.replace("\r\n", "\n") + "\n").getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }
}
