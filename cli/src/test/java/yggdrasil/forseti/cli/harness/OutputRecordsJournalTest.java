package yggdrasil.forseti.cli.harness;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.recording.W1Form;
import yggdrasil.forseti.cli.replay.OutputRecords;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.journal.Delete;
import yggdrasil.forseti.core.journal.Disposition;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.journal.Row;

/** The output records of a transport and the step-3 journals are one format: every line the reference harness writes on the smoke
 *  engine recording goes to its transport record and back byte for byte, and the {@link OutputRecords} line builders an engine uses
 *  rebuild it byte for byte from the decoded output. */
class OutputRecordsJournalTest {
    @TempDir Path tmp;

    @Test void everyJournalLineIsItsTransportRecordAndItsBuilder() throws IOException {
        HarnessW1FormTest h = new HarnessW1FormTest();
        h.tmp = tmp;
        Path out = h.run(HarnessW1FormTest.ENGINE.resolve("dedicated/smoke.json"), W1Form.MIN_OVER_PARTITIONS);
        int lines = 0, rows = 0, dispositions = 0, progress = 0;
        List<Path> files;
        try (Stream<Path> s = Files.walk(out.resolve(JournalFiles.JOURNALS))) { files = s.filter(Files::isRegularFile).sorted().toList(); }
        assertFalse(files.isEmpty());
        for (Path f : files) {
            String topic = f.getParent().getFileName().toString();
            for (String text : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                JsonNode line = Json.read(text.getBytes(StandardCharsets.UTF_8));
                assertEquals(text, Canonical.utf8(Canonical.bytes(OutputRecords.line(OutputRecords.transport(line)))), f + ": " + text);
                JsonNode v = line.get("value");
                ObjectNode rebuilt;
                switch (OutputRecords.kind(v)) {
                    case PROGRESS -> { rebuilt = OutputRecords.progressLine(Progress.fromJson(v)); progress++; }
                    case DELETE -> rebuilt = OutputRecords.deleteLine(Delete.fromJson(v.get("delete")));
                    default -> {
                        if (topic.startsWith("forseti.dispositions")) {
                            ObjectNode d = Json.object();
                            d.set("key", line.get("key"));
                            d.set("body", v.get("body"));
                            d.set("revision", v.get("revision"));
                            rebuilt = OutputRecords.dispositionLine(Disposition.fromJson(d));
                            dispositions++;
                        } else {
                            ObjectNode r = Json.object().put("topic", topic);
                            r.set("logicalKey", line.get("key"));
                            r.set("body", v.get("body"));
                            r.set("bodyDigest", v.get("bodyDigest"));
                            r.set("envelope", v.get("envelope"));
                            rebuilt = OutputRecords.rowLine(Row.fromJson(r));
                            rows++;
                        }
                    }
                }
                assertEquals(text, Canonical.utf8(Canonical.bytes(rebuilt)), f + ": " + text);
                lines++;
            }
        }
        assertTrue(rows > 0 && dispositions > 0 && progress > 0, "lines " + lines);
    }
}
