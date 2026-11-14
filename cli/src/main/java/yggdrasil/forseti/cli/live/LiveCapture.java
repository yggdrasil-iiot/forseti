package yggdrasil.forseti.cli.live;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.recording.CaptureTruth;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The capture's {@link CaptureRecorder.Inputs} for one live run, from the committed fixtures, the live scenario the driver ran and
 *  its drive log (one JSON line per step, {@code {"at", "commits", "eventTime", "sent"}}, written by the loop's {@code Driver}).
 *
 *  <ul>
 *  <li>The rule pin is the stream's head ({@code releaseId}, {@code version}) and the one attribution rule of its release payload.</li>
 *  <li>The definition pins are the deployment binding's: per routed device {@code templateRef}, {@code version} and the definition's
 *      {@code contentSha256} ({@link LiveEquipment}), the digest Muninn's NBIRTH carries.</li>
 *  <li>{@code T0} is the drive's origin in the sim's clock: the earliest source timestamp the first step (offset 0, where every
 *      device is written) committed. Scenario offsets are relative to the drive's start; the commit is the first instant the input
 *      carries.</li>
 *  <li>The visit facts are the scenario's: its {@code visitId} names the visit (an occupancy-join visit is keyed by its arrival
 *      event {@code <visitId>-arrive}, a plc-stamped one by the VisitId the driver stamps), and its {@code tools}.</li>
 *  <li>{@code sent} is every record the drive log lists; the recorder refuses a capture that does not hold exactly those
 *      occupancy and incident records.</li>
 *  </ul> */
public final class LiveCapture {
    private LiveCapture() {}

    public static CaptureRecorder.Inputs inputs(Path registry, Path definitions, Path deployment, String stream, Path routingFile, Path scenarioFile,
                                                Path driveLog, String name, long batchMs) {
        LiveEquipment.LiveStream s = LiveEquipment.read(registry, definitions, deployment, stream);
        byte[] scenarioBytes = bytes(scenarioFile), routingBytes = bytes(routingFile), logBytes = bytes(driveLog);
        LiveScenario scenario = LiveScenario.read(Json.read(scenarioBytes), s);
        if (scenario.scanMs() != s.expectedPeriodMs())
            throw new IllegalArgumentException("scenario scanMs " + scenario.scanMs() + " is not the rule's expectedPeriodMs " + s.expectedPeriodMs());
        RoutingManifest routing = RoutingManifest.read(Json.read(routingBytes));

        ObjectNode definitionPins = Json.object();
        for (String d : new TreeSet<>(routing.devices())) {
            LiveEquipment.Equipment e = s.device(d);
            definitionPins.putObject(d).put("templateRef", e.definitionRef()).put("version", e.definitionVersion()).put("sha256", e.contentSha256());
        }

        Map<String, CaptureTruth.Visit> facts = new LinkedHashMap<>();
        boolean plc = "plc-stamped".equals(s.mode());
        for (LiveScenario.Visit v : scenario.visits())
            facts.put(plc ? v.visitId() : v.arrivalId(), new CaptureTruth.Visit(v.visitId(), plc ? null : v.arrivalId(), v.tools()));

        List<JsonNode> sent = new ArrayList<>();
        Long t0 = null;
        int line = 0;
        for (String l : new String(logBytes, StandardCharsets.UTF_8).split("\n", -1)) {
            line++;
            if (l.isEmpty()) continue;
            JsonNode step = Json.read(l.getBytes(StandardCharsets.UTF_8));
            if (!step.isObject() || !step.path("commits").isObject() || !step.path("sent").isArray()) throw new IllegalArgumentException(driveLog + " line " + line + " is not a drive step");
            long at = Json.safeLong(step.get("at"), "at", Json.NON_NEGATIVE);
            if (at == 0) for (Iterator<JsonNode> it = step.get("commits").elements(); it.hasNext();) {
                long c = Json.safeLong(it.next(), "commits", Json.NON_NEGATIVE);
                t0 = t0 == null ? c : Math.min(t0, c);
            }
            step.get("sent").forEach(sent::add);
        }
        if (t0 == null) throw new IllegalArgumentException(driveLog + " has no commit of the step at offset 0: no drive origin");

        ObjectNode source = Json.object();
        source.put("scenario", scenario.name());
        source.put("scenarioSha256", Canonical.digestOf(scenarioBytes));
        source.put("driveLogSha256", Canonical.digestOf(logBytes));
        source.put("stream", stream);
        return new CaptureRecorder.Inputs(name == null ? scenario.name() : name, registry, rulePin(registry, stream), definitionPins, routingBytes, t0,
            scenario.scanMs(), batchMs, scenario.occupancySource(), facts, sent, source);
    }

    /** {@code {stream, releaseId, version, attributionRuleId, attributionRuleVersion}}: the head of {@code stream} and the one
     *  attribution rule of the release it names. */
    public static ObjectNode rulePin(Path registry, String stream) {
        Path m = registry.resolve("mappings");
        JsonNode head = Json.read(m.resolve("heads").resolve(stream + ".json"));
        Path rules = m.resolve("releases").resolve(Json.text(head, "id")).resolve(Json.text(head, "version")).resolve("payload").resolve("attribution-rules");
        List<Path> files;
        try (Stream<Path> l = Files.list(rules)) { files = l.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().toList(); }
        catch (IOException e) { throw new UncheckedIOException(e); }
        if (files.size() != 1) throw new IllegalArgumentException(stream + ": expected one attribution rule, found " + files.size());
        JsonNode rule = Json.read(files.get(0));
        ObjectNode pin = Json.object().put("stream", stream).put("releaseId", Json.text(head, "id")).put("version", Json.text(head, "version"));
        return pin.put("attributionRuleId", Json.text(rule, "id")).put("attributionRuleVersion", Json.text(rule, "version"));
    }

    private static byte[] bytes(Path p) {
        try { return Files.readAllBytes(p); } catch (IOException e) { throw new UncheckedIOException(e); }
    }
}
