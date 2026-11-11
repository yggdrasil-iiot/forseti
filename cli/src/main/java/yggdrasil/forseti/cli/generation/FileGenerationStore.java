package yggdrasil.forseti.cli.generation;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationStore;

/** The step-3 generation store: {@code <stateDir>/generations.jsonl}, one transport record per line
 *  {@code {"topic": "forseti.generations", "key": "<scope>", "headers": {"forseti-generation": "<n>"}, "value": <the generation row>}}.
 *  This is the read side; the locked, durable {@link #append} arrives with the generation CLI. A last line without its LF, a line that is
 *  not a transport record, or a header that disagrees with its row fails closed ({@code control-log-corrupt}); nothing is skipped. */
public final class FileGenerationStore implements GenerationStore {
    public static final String FILE = "generations.jsonl", TOPIC = "forseti.generations", HEADER = "forseti-generation";

    private final Path file;

    public FileGenerationStore(Path stateDir) { this.file = stateDir.resolve(FILE); }

    public Path file() { return file; }

    @Override public void append(String scope, Generation row) {
        throw new UnsupportedOperationException("the generation store is read-only until the generation CLI adds the locked writer");
    }

    @Override public List<Entry> all() {
        if (!Files.exists(file)) return List.of();
        byte[] bytes;
        try { bytes = Files.readAllBytes(file); } catch (IOException e) { throw new UncheckedIOException(e); }
        List<Entry> out = new ArrayList<>();
        if (bytes.length == 0) return out;
        if (bytes[bytes.length - 1] != '\n') throw corrupt("the last line has no LF");
        int start = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != '\n') continue;
            byte[] line = Arrays.copyOfRange(bytes, start, i);
            start = i + 1;
            int number = out.size();
            JsonNode n;
            try { n = Json.read(line); } catch (IllegalArgumentException e) { throw corrupt("line " + number + " is not JSON"); }
            if (!n.isObject() || n.size() != 4 || !TOPIC.equals(n.path("topic").textValue()) || !n.path("key").isTextual()
                || !n.path("headers").isObject() || !n.path("value").isObject()) throw corrupt("line " + number + " is not a transport record");
            Generation g;
            try { g = Generation.fromJson(n.get("value")); } catch (IllegalArgumentException e) { throw corrupt("line " + number + ": " + e.getMessage()); }
            if (!Long.toString(g.generation()).equals(n.path("headers").path(HEADER).textValue())) throw corrupt("line " + number + " header " + HEADER);
            out.add(new Entry(n.get("key").textValue(), g));
        }
        return List.copyOf(out);
    }

    private IllegalStateException corrupt(String what) { return new IllegalStateException("control-log-corrupt: " + file.getFileName() + ": " + what); }
}
