package yggdrasil.forseti.bridge;

import static org.junit.jupiter.api.Assertions.*;
import static yggdrasil.forseti.bridge.SessionTableTest.*;
import static yggdrasil.forseti.bridge.Sp.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.eclipse.tahu.message.model.MetricDataType;
import org.junit.jupiter.api.Test;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.input.Inputs;

/** Every input record the bridge writes is one the readers accept: forseti's own InputParser (core) and the oracle's inputs.py, run on
 *  the same bytes. The records are those of every row of the session table plus messages with every member shape Muninn writes and the
 *  member-level shapes of values the contract cannot carry as themselves (C7), so each session kind, the birthless variant and each
 *  optional member field reach both readers. Per record the two readers agree on the payload digest and its rule (JCS, or raw bytes
 *  for a record holding a signed infinity), on which members are rejected (NaN and unsupported types: value null) and on which member
 *  values are infinite (the literal 1e400, spec reason {@code non-finite}). */
class InputRoundTripTest {
    static final String PYTHON = System.getProperty("forseti.python", "python");

    static List<Records.Out> records() {
        List<Records.Out> l = new ArrayList<>();
        List<List<Frame>> runs = new ArrayList<>();
        rows().forEach(r -> {
            BridgeCore core = new BridgeCore(r.pins());
            for (Frame f : r.frames()) for (Emitted e : core.accept(f)) l.add(Records.encode(e, Records.Topics.DEFAULT));
        });
        long t = B1 + 2000;
        runs.add(List.of(nbirth(E, 0, B1), dbirth(E, D, 1, B1 + 1000), device("DDATA", E, D, 2, t + 50, List.of(
            sample("VisitPresent", MetricDataType.Boolean, false, t, 0L, 192, t + 3),
            sample("Sim/Torque", MetricDataType.Float, 0.1f, t, 0x80000000L, 0, null),
            sample("UnitId", MetricDataType.String, "U-0001", null, 0L, 192, null),
            sample("Count", MetricDataType.Int64, 9007199254740991L, t, 0x40900000L, 500, t + 3),
            sample("Small", MetricDataType.Int32, -5, t, 0L, 192, t + 3),
            sample("Wide", MetricDataType.UInt64, new java.math.BigInteger("18446744073709551615"), t, 0L, 192, t + 3),
            sample("LastCheck", MetricDataType.DateTime, new Date(B1), t, 0L, 192, t + 3))),
            device("DDATA", E, D, 3, t + 1050, scan(null)),
            device("DDATA", E, D, 4, t + 2050, BridgeCoreTest.mixed(t + 2000))));
        for (List<Frame> run : runs) {
            BridgeCore core = new BridgeCore(Map.of());
            for (Frame f : run) for (Emitted e : core.accept(f)) l.add(Records.encode(e, Records.Topics.DEFAULT));
        }
        return l;
    }

    /** What a reader made of one record: digest, digest rule, rejected member metrics, metrics whose value is infinite. */
    static Map<String, Object> view(Records.Out r) {
        InputParser.Read read = InputParser.read(new Inputs.Origin(r.topic(), 0, 0), r.key(), r.value());
        assertFalse(read.rejected(), () -> r.topic() + " rejected (" + read.check() + "): " + Records.utf8(r.value()));
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("digest", read.record().payloadDigest().sha256());
        v.put("rule", read.record().payloadDigest().rule());
        List<String> rejected = new ArrayList<>(), infinite = new ArrayList<>();
        if (read.record().parsed() instanceof Inputs.ProcessMessage m) {
            for (Inputs.MemberRejection x : m.rejectedMembers()) rejected.add(x.metric() + ":" + x.check());
            for (Inputs.Member x : m.members()) if (x.value().isDouble() && Double.isInfinite(x.value().doubleValue())) infinite.add(x.metric());
        }
        v.put("rejected", rejected);
        v.put("infinite", infinite);
        return v;
    }

    @Test
    void theCoreInputParserAcceptsEveryRecordTheBridgeWrites() {
        TreeSet<String> kinds = new TreeSet<>();
        int messages = 0;
        boolean birthless = false, mixed = false;
        for (Records.Out r : records()) {
            if (r.topic().equals("ot.dead-letter")) continue;
            Map<String, Object> v = view(r);
            boolean infinity = Records.utf8(r.value()).contains("1e400");
            assertEquals(infinity ? "raw-bytes" : "jcs", v.get("rule"), Records.utf8(r.value()));
            InputParser.Read read = InputParser.read(new Inputs.Origin(r.topic(), 0, 0), r.key(), r.value());
            if (read.record().parsed() instanceof Inputs.ProcessMessage m) {
                assertEquals(r.key(), m.device());
                if (infinity) {
                    mixed = true;
                    assertEquals(List.of("NotANumber:value", "Blob:value"), v.get("rejected"));
                    assertEquals(List.of("Up", "Down"), v.get("infinite"));
                    assertEquals(List.of("Up", "Down", "Torque"), m.members().stream().map(Inputs.Member::metric).toList(), "the healthy members stay");
                } else assertEquals(List.of(), v.get("rejected"), Records.utf8(r.value()));
                messages++;
            } else {
                Inputs.SessionEvent s = (Inputs.SessionEvent) read.record().parsed();
                assertEquals(r.key(), s.device());
                kinds.add(s.kind());
                birthless |= s.birthless();
            }
        }
        assertTrue(messages > 0);
        assertTrue(mixed, "the member-level shapes reached the parser");
        assertTrue(birthless, "the birthless variant reached the parser");
        assertEquals(new TreeSet<>(List.of("seq-gap", "session-down", "session-up", "stale-death", "session-ambiguous", "definition-changed")), kinds);
    }

    @Test
    void theOracleReaderAcceptsTheSameBytesWithTheSameView() throws Exception {
        Path dir = Files.createDirectories(Path.of("target", "bridge-roundtrip"));
        Path in = dir.resolve("records.jsonl"), script = dir.resolve("read_records.py");
        ObjectMapper json = new ObjectMapper();
        List<Map<String, Object>> expected = new ArrayList<>();
        StringBuilder lines = new StringBuilder();
        for (Records.Out r : records()) {
            if (r.topic().equals("ot.dead-letter")) continue;
            lines.append(json.writeValueAsString(Map.of("topic", r.topic(), "value", Base64.getEncoder().encodeToString(r.value())))).append('\n');
            expected.add(view(r));
        }
        Files.writeString(in, lines.toString(), StandardCharsets.UTF_8);
        Files.writeString(script, String.join("\n",
            "import base64, json, math, sys",
            "from forseti_oracle import inputs",
            "out = []",
            "with open(sys.argv[1], encoding='utf-8') as f:",
            "    for line in f:",
            "        r = json.loads(line)",
            "        x = inputs.read(r['topic'], base64.b64decode(r['value']))",
            "        rejected, infinite = [], []",
            "        if not x.rejected and r['topic'] == 'ot.messages':",
            "            rejected = [m + ':' + c for m, c in x.parsed.rejected_members]",
            "            infinite = [m.metric for m in x.parsed.members if isinstance(m.value, float) and math.isinf(m.value)]",
            "        out.append({'rejectedRecord': x.rejected, 'digest': x.payload_digest, 'rule': x.digest_rule, 'rejected': rejected, 'infinite': infinite})",
            "json.dump(out, sys.stdout)", ""), StandardCharsets.UTF_8);
        Process p = new ProcessBuilder(PYTHON, "-I", script.toString(), in.toString()).start();
        byte[] stdout = p.getInputStream().readAllBytes();
        String stderr = new String(p.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(120, TimeUnit.SECONDS));
        assertEquals(0, p.exitValue(), stderr);
        JsonNode out = json.readTree(stdout);
        assertEquals(expected.size(), out.size());
        for (int i = 0; i < out.size(); i++) {
            JsonNode o = out.get(i);
            assertFalse(o.get("rejectedRecord").booleanValue(), "oracle refused record " + i + ": " + o);
            Map<String, Object> got = new LinkedHashMap<>();
            got.put("digest", o.get("digest").textValue());
            got.put("rule", o.get("rule").textValue());
            got.put("rejected", json.convertValue(o.get("rejected"), List.class));
            got.put("infinite", json.convertValue(o.get("infinite"), List.class));
            assertEquals(expected.get(i), got, "record " + i);
        }
    }
}
