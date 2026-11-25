package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import yggdrasil.forseti.cli.replay.ReplayEngine;
import yggdrasil.forseti.core.Json;

/** What a run whose engine job stopped on its own committed ({@link ReplayEngine.Failed}): its dumped journals folded through the last
 *  cut any of their progress records names, and the stage-1 progress agreement of the two evidence journals ({@link ProgressAgreement}).
 *  The run stays crashed: this is the run summary's {@code afterFailure}, a report of the evidence, read by no assertion and by no
 *  control verdict. A journal that ends early leaves {@code J-PROGRESS-ORDER} "no progress record of cut" and coverage findings by
 *  itself; they show where the journal stops, not a fault. */
final class FailedRuns {
    private FailedRuns() {}

    /** The {@link ReplayEngine.Failed} in {@code x}'s cause chain, or null. */
    static ReplayEngine.Failed of(Throwable x) {
        for (Throwable t = x; t != null; t = t.getCause()) if (t instanceof ReplayEngine.Failed f) return f;
        return null;
    }

    /** {@code {dumped, completedCutIds, foldedThroughCut, rows, journalFindings, progressAgreement}}, or {@code {dumped: false, reason}}
     *  when the engine could not dump; {@code error} when reading the files failed. Never throws. */
    static ObjectNode afterFailure(ReplayEngine.Failed f) {
        ObjectNode o = Json.object();
        if (f.out == null || !Files.isDirectory(f.out.resolve("journals"))) {
            o.put("dumped", false);
            StringBuilder why = new StringBuilder();
            for (Throwable s : f.getCause().getSuppressed()) why.append(why.length() == 0 ? "" : "; ").append(s);
            o.put("reason", why.length() == 0 ? (f.out == null ? "no dump" : "no journals") : why.toString());
            return o;
        }
        o.put("dumped", true);
        try {
            JsonNode cutLog = Json.read(f.out.resolve("cuts.json"));
            ArrayNode done = o.putArray("completedCutIds");
            for (JsonNode c : cutLog.path("cuts")) done.add(c.path("cutId").asLong());
            o.set("progressAgreement", ProgressAgreement.check(f.out, f.generation));
            long last = 0;
            for (String t : List.of(RunFold.WR, RunFold.SAMPLES))
                for (List<ProgressAgreement.Mark> marks : ProgressAgreement.read(f.out, t, f.generation).values())
                    for (ProgressAgreement.Mark m : marks) last = Math.max(last, m.cutId());
            Json.putSafe(o, "foldedThroughCut", last);
            try (RunFold fold = new RunFold(f.out, f.rulesDigest, f.generation, false)) {
                for (long k = 1; k <= last; k++) fold.advance(k);
                ObjectNode rows = o.putObject("rows");
                fold.rows().forEach((k, v) -> Json.putSafe(rows, k, v));
                o.set("journalFindings", Summaries.findings(fold.findings));
            }
        } catch (Exception | Error e) {
            o.put("error", e.toString());
        }
        return o;
    }

    /** One line for the gate's output. */
    static String describe(ObjectNode a) {
        if (!a.path("dumped").asBoolean()) return "no dump (" + a.path("reason").asText() + ")";
        StringBuilder b = new StringBuilder("completed cuts " + a.path("completedCutIds").size() + ", folded through cut " + a.path("foldedThroughCut").asLong());
        JsonNode p = a.path("progressAgreement");
        b.append("; progress: remerged ").append(p.path("remerged").path("count").asLong())
            .append(", disagreements ").append(p.path("disagreements").path("count").asLong())
            .append(", truncated ").append(p.path("truncated").path("count").asLong());
        for (String k : List.of("remerged", "disagreements", "truncated"))
            for (JsonNode e : p.path(k).path("examples")) b.append("; ").append(k).append(": ").append(e.asText());
        for (java.util.Iterator<Map.Entry<String, JsonNode>> it = a.path("journalFindings").fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> e = it.next();
            b.append("; ").append(e.getKey()).append(' ').append(e.getValue().path("describe").asText());
        }
        if (a.has("error")) b.append("; error ").append(a.path("error").asText());
        return b.toString();
    }
}
