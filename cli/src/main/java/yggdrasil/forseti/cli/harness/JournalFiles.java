package yggdrasil.forseti.cli.harness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.Delete;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Row;
import yggdrasil.forseti.core.stage1.QuarantineRecord;

/** The harness's sinks: {@code <out>/journals/<topic>/<p>.jsonl} and {@code <out>/quarantine/<p>.jsonl}, one record per line in JCS form
 *  (so a number prints one way, keys are sorted, and a line never holds a non-finite number or an integer outside the JCS domain), LF.
 *  Shapes: a row {@code {"key": <logical key>, "headers": {"forseti-generation": "<n>"}, "value": {"body", "bodyDigest", "envelope"}}};
 *  a delete {@code {"value": {"delete": {...}}}}; a progress record {@code {"key": null, "value": {"type": "progress", ...}}}; a
 *  disposition {@code {"key": <delivery key>, "headers": {...}, "value": {"body", "revision"}}}; a quarantine record
 *  {@code {"key": <input origin>, "headers": {...}, "value": <the raw bytes record>}}. */
final class JournalFiles implements AutoCloseable {
    static final String JOURNALS = "journals", QUARANTINE = "quarantine", DISPOSITIONS = "forseti.dispositions-stage1";

    private final Path out;
    private final Map<Path, OutputStream> open = new TreeMap<>();

    JournalFiles(Path out) { this.out = out; }

    static Path journal(Path out, String topic, int partition) { return out.resolve(JOURNALS).resolve(topic).resolve(partition + ".jsonl"); }

    void row(Row r, int partition) {
        ObjectNode n = Json.object();
        n.set("key", r.logicalKey());
        n.set("headers", headers(r.generation()));
        ObjectNode v = n.putObject("value");
        v.set("body", r.body());
        v.put("bodyDigest", r.bodyDigest());
        v.set("envelope", r.envelope().toJson());
        write(journal(out, r.topic(), partition), n);
    }

    void delete(String topic, Delete d, int partition) {
        ObjectNode n = Json.object();
        n.putObject("value").set("delete", d.toJson());
        write(journal(out, topic, partition), n);
    }

    void progress(String topic, Progress p, int partition) {
        ObjectNode n = Json.object();
        n.putNull("key");
        n.set("value", p.toJson());
        write(journal(out, topic, partition), n);
    }

    void disposition(Disposition d, int partition) {
        ObjectNode n = Json.object();
        n.set("key", d.key().toJson());
        n.set("headers", headers(d.generation()));
        ObjectNode v = n.putObject("value");
        v.set("body", d.body().toJson());
        Json.putSafe(v, "revision", d.revision());
        write(journal(out, DISPOSITIONS, partition), n);
    }

    void quarantine(QuarantineRecord q, int partition) {
        ObjectNode n = Json.object();
        n.set("key", QuarantineRecord.ref(q.origin()));
        n.set("headers", headers(q.generation()));
        n.set("value", q.toJson());
        write(out.resolve(QUARANTINE).resolve(partition + ".jsonl"), n);
    }

    private static ObjectNode headers(long generation) { return Json.object().put("forseti-generation", Long.toString(generation)); }

    private void write(Path file, JsonNode line) {
        byte[] b = Canonical.bytes(line);
        try {
            OutputStream s = open.get(file);
            if (s == null) {
                Files.createDirectories(file.getParent());
                s = new BufferedOutputStream(Files.newOutputStream(file), 1 << 16);
                open.put(file, s);
            }
            s.write(b);
            s.write('\n');
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    @Override public void close() {
        IOException first = null;
        for (OutputStream s : open.values()) { try { s.close(); } catch (IOException e) { if (first == null) first = e; } }
        open.clear();
        if (first != null) throw new UncheckedIOException(first);
    }
}
