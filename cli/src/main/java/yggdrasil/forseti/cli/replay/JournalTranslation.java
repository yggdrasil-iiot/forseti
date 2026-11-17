package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.input.InputParser;

/** The journal of an engine run translated from broker offsets to recording offsets, field by field ({@link OffsetMap}), so that the
 *  step-3 comparison, {@code JournalFold} and the oracle read it unchanged (plan D10). Exactly the fields that name an input offset are
 *  translated, and nothing else of a line changes: a row's {@code envelope.trigger.offset}, a disposition's {@code key.inputOffset} and
 *  {@code body.refs.quarantine.inputOffset}, a quarantine record's {@code key.inputOffset} and {@code value.inputOffset}, a progress
 *  record's {@code barrierRefs[].throughOffset} and {@code incidentOffsets[].throughOffset}. Every one of them must name an offset the
 *  run wrote as a business record (or {@code -1} for a through-offset); anything else is an {@link IllegalArgumentException}, never a
 *  silent pass. A row's {@code bodyDigest} is untouched: no row body carries an input offset. */
public final class JournalTranslation {
    private JournalTranslation() {}

    /** Translates {@code line} (a line of output topic {@code topic}, by its contract name) in place and returns it. */
    public static ObjectNode toRecording(String topic, ObjectNode line, OffsetMap map) {
        JsonNode v = line.path("value");
        switch (OutputRecords.kind(v)) {
            case PROGRESS -> {
                for (JsonNode b : v.path("barrierRefs")) {
                    JsonNode o = b.path("origin");
                    ((ObjectNode) b).put("throughOffset", map.throughRecording(Json.text(o, "topic"), o.path("partition").intValue(), b.path("throughOffset").longValue()));
                }
                for (JsonNode i : v.path("incidentOffsets"))
                    ((ObjectNode) i).put("throughOffset", map.throughRecording(InputParser.INCIDENTS, i.path("partition").intValue(), i.path("throughOffset").longValue()));
            }
            case DELETE -> { }
            case RECORD -> {
                if (topic.equals(ReplayTopics.QUARANTINE)) {
                    origin(line.path("key"), map);
                    origin(v, map);
                } else if (topic.equals(ReplayTopics.DISPOSITIONS1) || topic.equals(ReplayTopics.DISPOSITIONS2)) {
                    origin(line.path("key"), map);
                    JsonNode q = v.path("body").path("refs").path("quarantine");
                    if (q.isObject()) origin(q, map);
                } else {
                    JsonNode t = v.path("envelope").path("trigger");
                    if (t.isObject()) ((ObjectNode) t).put("offset", map.recording(Json.text(t, "topic"), t.path("partition").intValue(), t.path("offset").longValue()));
                }
            }
        }
        return line;
    }

    /** {@code {inputTopic, inputPartition, inputOffset}} in place. */
    private static void origin(JsonNode n, OffsetMap map) {
        if (!n.isObject() || !n.path("inputOffset").isIntegralNumber()) throw new IllegalArgumentException("no input origin in " + n);
        ((ObjectNode) n).put("inputOffset", map.recording(Json.text(n, "inputTopic"), n.path("inputPartition").intValue(), n.path("inputOffset").longValue()));
    }
}
