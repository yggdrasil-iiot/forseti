package yggdrasil.forseti.bridge;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** The optional raw-frame log: every MQTT frame as received, one JSON line {@code {receivedAt, topic, payload}} with the payload in
 *  base64, written before the core sees the frame. It keeps the wire evidence of a run whose D3 stages changed what reached Kafka. */
public final class RawFrameLog implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final OutputStream out;

    public RawFrameLog(Path p) {
        try {
            if (p.getParent() != null) Files.createDirectories(p.getParent());
            out = new BufferedOutputStream(Files.newOutputStream(p));
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    public void append(Frame f) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        try (JsonGenerator g = JSON.getFactory().createGenerator(b)) {
            g.writeStartObject();
            g.writeNumberField("receivedAt", f.receivedAt());
            g.writeStringField("topic", f.topic());
            g.writeBinaryField("payload", f.payload());
            g.writeEndObject();
        } catch (IOException e) { throw new UncheckedIOException(e); }
        try { out.write(b.toByteArray()); out.write('\n'); } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    @Override public void close() { try { out.close(); } catch (IOException e) { throw new UncheckedIOException(e); } }
}
