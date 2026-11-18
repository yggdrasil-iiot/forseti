package yggdrasil.forseti.cli.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.cli.recording.Recording;
import yggdrasil.forseti.cli.recording.RoutingManifest;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.cut.CutAssembler;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.journal.Pins;
import yggdrasil.forseti.core.journal.Progress;
import yggdrasil.forseti.core.release.HeadVerifier;
import yggdrasil.forseti.core.rules.RuleSet;

/** What every engine derives the same way from the published contract (spec 412, 575-577; step-6 plan D1, D2): the stage-1 input
 *  topics, the envelope pin set of a generation's rows, a station's required barrier set, the reject collector's, the head a generation
 *  pinned, and a batch's {@code idleInputs}. The reference harness, the Flink jobs and the Kafka Streams jobs of step 7 all read them
 *  here, so a required set or a pin can never differ between engines. */
public final class EngineContract {
    private EngineContract() {}

    /** The stage-1 input topics, in input-topic order. */
    public static final List<String> STAGE1_TOPICS = List.of(InputParser.MESSAGES, InputParser.SESSIONS, InputParser.OCCUPANCY);

    /** The envelope pin set of a generation's rows: the rule set's pins, the definitions of the recording and {@code job}. */
    public static Pins pins(RuleSet rules, JsonNode definitions, Pins.Job job) {
        ObjectNode p = rules.pins();
        Map<String, Pins.Definition> defs = new TreeMap<>();
        definitions.fields().forEachRemaining(e -> defs.put(e.getKey(),
            new Pins.Definition(Json.text(e.getValue(), "templateRef"), Json.text(e.getValue(), "version"), Json.text(e.getValue(), "sha256"))));
        return new Pins(p.get("rules"), p.get("hierarchy"), p.get("bindings"), defs, job);
    }

    /** A station's required barriers (spec 412): every subscribed raw telemetry partition, plus the session partitions of its devices and
     *  its occupancy partition when subscribed (the routing manifest fixes both), in the order its {@code barrierRefs} list them. */
    public static List<Progress.Origin> stationRequired(String station, RoutingManifest routing, Map<String, List<Integer>> subscribed) {
        Set<Integer> sessions = new TreeSet<>();
        for (String d : routing.devices()) if (routing.stationOf(d).equals(station)) sessions.add(routing.rawPartitionOf(d));
        Map<String, Set<Integer>> keep = Map.of(InputParser.MESSAGES, new TreeSet<>(subscribed.get(InputParser.MESSAGES)),
            InputParser.SESSIONS, sessions, InputParser.OCCUPANCY, Set.of(routing.partitionOf(station)));
        return required(subscribed, keep);
    }

    /** Every subscribed stage-1 partition {@code keep} admits (all when null), by topic in input-topic order, then partition: with
     *  {@code keep} null, the stage-1 reject collector's required set. */
    public static List<Progress.Origin> required(Map<String, List<Integer>> subscribed, Map<String, Set<Integer>> keep) {
        List<Progress.Origin> l = new ArrayList<>();
        for (String t : STAGE1_TOPICS) for (int p : subscribed.get(t)) {
            if (keep != null && !keep.get(t).contains(p)) continue;
            l.add(new Progress.Origin(t, p));
        }
        return l;
    }

    /** The head the generation pinned, rebuilt from {@code releaseEvidence.head}; nothing in it is verified here (that was
     *  {@code generation new}): the release reader verifies the release against these digests. */
    public static HeadVerifier.Head head(JsonNode evidence) {
        JsonNode h = evidence.path("head");
        if (!h.isObject()) throw new IllegalArgumentException("releaseEvidence.head is not an object");
        return new HeadVerifier.Head(Json.text(h, "stream"), Json.text(h, "releaseId"), Json.text(h, "version"), Json.text(h, "releaseSha256"),
            h.path("headFile").textValue(), h.path("headSha256").textValue(), h.path("attestationFile").textValue(), h.path("attestationSha256").textValue(),
            Json.text(h, "statementSha256"), Json.safeLong(h.get("headVersion"), "releaseEvidence.head.headVersion", 1, Json.MAX_SAFE),
            h.path("signedAt").textValue(), h.path("expires").textValue(), h.path("publishedBy").textValue(), h.path("approvedBy").textValue());
    }

    /** The batch's {@code idleInputs[]}, read strictly. */
    public static List<CutAssembler.Idle> idleInputs(Recording.Batch b) {
        List<CutAssembler.Idle> l = new ArrayList<>();
        for (JsonNode x : b.idleInputs()) {
            try { l.add(CutAssembler.Idle.fromJson(x)); }
            catch (IllegalArgumentException e) { throw new IllegalArgumentException("malformed recording: idleInputs of cut " + b.cutId() + ": " + e.getMessage()); }
        }
        return l;
    }
    /** {@code pins} with {@code rules.attribution.sha256} replaced by the digest of the attribution rule of another release of the same
     *  registry (the first other release, by path, whose {@code payload/attribution-rules/0.json} has another digest), the body's
     *  {@code rulesDigest} untouched (the {@code wrong-rules-pin} control). No such release is an {@link IllegalStateException}. */
    public static Pins wrongRulesPin(Pins pins, Path mappings, HeadVerifier.Head head) {
        String own = pins.rules().path("attribution").path("sha256").asText(), other = null;
        Path releases = mappings.resolve("releases"), mine = releases.resolve(head.releaseId()).resolve(head.version()).resolve("release.json");
        List<String> files = new ArrayList<>();
        try (java.util.stream.Stream<Path> w = Files.walk(releases)) {
            w.filter(p -> p.getFileName().toString().equals("release.json") && !p.equals(mine)).forEach(p -> {
                List<String> parts = new ArrayList<>();
                for (Path x : releases.relativize(p)) parts.add(x.toString());
                files.add(String.join("/", parts));
            });
        } catch (IOException e) { throw new UncheckedIOException(e); }
        Collections.sort(files);
        for (String f : files) {
            for (JsonNode x : Json.read(releases.resolve(f)).path("files"))
                if ("payload/attribution-rules/0.json".equals(x.path("path").asText()) && x.path("sha256").isTextual() && !own.equals(x.path("sha256").asText())) {
                    other = x.path("sha256").asText();
                    break;
                }
            if (other != null) break;
        }
        if (other == null) throw new IllegalStateException("wrong-rules-pin: no other release under " + releases + " pins another attribution rule");
        ObjectNode rulesPins = pins.rules().deepCopy();
        ((ObjectNode) rulesPins.path("attribution")).put("sha256", other);
        return new Pins(rulesPins, pins.hierarchy(), pins.bindings(), pins.definitions(), pins.job());
    }

    /** The engine artifact digest of an engine path directory (spec "engine and job digests", reviewer decision 2 of 2026-11-18): the
     *  JCS SHA-256 of the list of {@code [file name, raw SHA-256]} of every regular file directly in {@code dir}, sorted by name. The
     *  harness computes it and passes it in the job spec; each job computes it again at start and refuses to run on a mismatch. */
    public static String engineArtifactSha256(Path dir) {
        if (dir == null || !Files.isDirectory(dir)) throw new IllegalArgumentException("not an engine path directory: " + dir);
        TreeMap<String, String> files = new TreeMap<>();
        try (java.util.stream.Stream<Path> s = Files.list(dir)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) files.put(p.getFileName().toString(), Canonical.digestOf(Files.readAllBytes(p)));
        } catch (IOException e) { throw new UncheckedIOException(e); }
        if (files.isEmpty()) throw new IllegalArgumentException("an empty engine path directory: " + dir);
        com.fasterxml.jackson.databind.node.ArrayNode a = Json.JSON.createArrayNode();
        files.forEach((n, d) -> a.addArray().add(n).add(d));
        return Canonical.digest(a);
    }
}
