package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.KeyPair;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import yggdrasil.forseti.cli.gate.CoreGate.Check;
import yggdrasil.forseti.cli.gate.CoreGate.Context;
import yggdrasil.forseti.cli.gate.CoreGate.Proc;
import yggdrasil.forseti.cli.gate.CoreGate.Result;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.input.InputParser;
import yggdrasil.forseti.core.journal.*;
import yggdrasil.forseti.core.release.*;
import yggdrasil.forseti.core.rules.MemberCatalog;
import yggdrasil.forseti.core.rules.RuleSet;

/** The foundation assertions of pull request 1 ({@code F-*}): canonical form on both sides, the schema copies, Tyr's bytes untouched,
 *  the journal reducers, the head and release reader with its tampered variants, the rule set and its digest against the oracle's.
 *  Every tampered variant is made in a copy under the gate's temporary directory; every head is verified at its own signedAt + 60 s. */
final class Foundation {
    private Foundation() {}

    static final String OCCUPANCY_SHA = "e97cd7c97b1ab5e2d63d5b06e6057415945b82cff6d2ca0345106d2daf9d29dc";
    static final String INCIDENT_SHA = "7141c3115d90812f16b22049c66bfaa64942ebacf854b343fb99b3dce168fb34";
    static final String VECTORS = "vectors/jcs-vectors.json";
    static final String S1 = "synthetic-site:line2:station1";
    static final String RULE = "line2-attribution";

    static List<Check> checks() {
        return List.of(
            new Check("F-JCS-JAVA", Foundation::jcsJava),
            new Check("F-JCS-PY", Foundation::jcsPython),
            new Check("F-JCS-REFUSALS", Foundation::jcsRefusals),
            new Check("F-SCHEMA-COPIES", Foundation::schemaCopies),
            new Check("F-UPSTREAM-UNTOUCHED", Foundation::upstreamUntouched),
            new Check("F-JOURNAL-REDUCERS", Foundation::journalReducers),
            new Check("F-HEAD-OK", Foundation::headOk),
            new Check("F-HEAD-SIGNATURE", Foundation::headSignature),
            new Check("F-HEAD-SAME-KEY", Foundation::headSameKey),
            new Check("F-HEAD-SUBJECT", Foundation::headSubject),
            new Check("F-HEAD-FUTURE", Foundation::headFuture),
            new Check("F-HEAD-ROLLBACK", Foundation::headRollback),
            new Check("F-HEAD-EXPIRED", Foundation::headExpired),
            new Check("F-RELEASE-DIGEST", Foundation::releaseDigest),
            new Check("F-PAYLOAD-DIGEST", Foundation::payloadDigest),
            new Check("F-RULES-LOAD-OJ", c -> rulesLoad(c, "line2", "line2-rules", true)),
            new Check("F-RULES-LOAD-PLC", c -> rulesLoad(c, "line2-plc", "line2-rules-plc", false)),
            new Check("F-BINDING-SCHEMA-MISMATCH", Foundation::bindingSchemaMismatch));
    }

    // --- helpers -------------------------------------------------------------------------------------------------------------

    /** Collects the problems of one assertion; holds when there are none. */
    static final class Problems {
        final List<String> list = new ArrayList<>();
        void require(boolean ok, String problem) { if (!ok) list.add(problem); }
        <T> void equal(T want, T have, String what) { if (!Objects.equals(want, have)) list.add(what + ": expected " + want + ", got " + have); }
        /** {@code call} is refused with {@code reason} and, when given, evidence {@code rule}. */
        Refusal refused(String label, String reason, String rule, Callable<?> call) {
            Refusal r;
            try { call.call(); list.add(label + ": not refused"); return null; }
            catch (Refusal e) { r = e; }
            catch (Exception e) { list.add(label + ": " + e); return null; }
            if (!reason.equals(r.reason) || rule != null && !rule.equals(r.evidence.get("rule"))) list.add(label + ": " + r.reason + " " + r.evidence);
            return r;
        }
        Result result(String evidence) { return new Result(list.isEmpty(), list.isEmpty() ? evidence : String.join("; ", list)); }
    }

    static TrustedKeys fixtureKeys(Context c) throws IOException { return TrustedKeys.load(c.fixtures().resolve("trusted-keys.jsonl")); }
    static FloorStore floors(Context c) throws IOException { return new FloorStore(c.newDir("state")); }
    static HeadVerifier.Head verify(Path m, String stream, TrustedKeys keys, FloorStore floors) {
        return HeadVerifier.verify(m, stream, keys, floors, GateSigner.now(m, stream));
    }
    static boolean floorFile(FloorStore floors, String stream) { return floors.get(stream).isPresent(); }
    /** Two fresh keys trusted for the fixture's two principals, and their signers. */
    record Pair(TrustedKeys trusted, GateSigner.Signer proposer, GateSigner.Signer approver) {}
    static Pair freshPair(Context c) throws IOException {
        KeyPair p = GateSigner.generate(), a = GateSigner.generate();
        return new Pair(GateSigner.trusted(c.newDir("keys"), "proposer", p.getPublic(), "approver", a.getPublic()),
            new GateSigner.Signer("proposer", p.getPrivate()), new GateSigner.Signer("approver", a.getPrivate()));
    }
    static String instant(long ms) { return Instant.ofEpochMilli(ms).toString().replaceAll("\\.\\d+Z$", "Z"); }

    // --- canonical form ------------------------------------------------------------------------------------------------------

    static Result jcsJava(Context c) {
        JsonNode f = Json.read(c.path(VECTORS));
        Problems p = new Problems();
        p.equal(1, f.path("vectorsVersion").asInt(), "vectorsVersion");
        p.equal(9, f.path("vectors").size(), "vector count");
        for (JsonNode v : f.path("vectors")) {
            String id = Json.text(v, "id");
            try {
                byte[] b = Canonical.bytes(Json.read(Json.text(v, "inputText").getBytes(StandardCharsets.UTF_8)));
                p.equal(Json.text(v, "canonical"), Canonical.utf8(b), id + " canonical");
                p.equal(Json.text(v, "sha256"), Canonical.digestOf(b), id + " sha256");
            } catch (IllegalArgumentException e) { p.list.add(id + ": refused " + e.getMessage()); }
        }
        p.equal("44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a", Canonical.digest(Json.object()), "sha256({})");
        return p.result(f.path("vectors").size() + " vectors canonical and digested as pinned");
    }

    static Result jcsPython(Context c) throws Exception {
        Proc r = c.python("F-JCS-PY", "-m", "pytest", "-q", "oracle/tests/test_jcs_vectors.py");
        return new Result(r.exit() == 0, "pytest exit " + r.exit() + ": " + r.lastLine());
    }

    static Result jcsRefusals(Context c) throws Exception {
        JsonNode f = Json.read(c.path(VECTORS));
        Problems p = new Problems();
        p.equal(8, f.path("refusals").size(), "refusal count");
        for (JsonNode r : f.path("refusals")) {
            String id = Json.text(r, "id"), reason = Json.text(r, "reason");
            try {
                Canonical.bytes(Json.read(Json.text(r, "inputText").getBytes(StandardCharsets.UTF_8)));
                p.list.add(id + ": accepted by Java");
            } catch (IllegalArgumentException e) {
                // NaN/Infinity are not JSON: Jackson refuses them at parse time (not-json), which is also forseti's refusal.
                boolean ok = e.getMessage().startsWith(reason) || reason.equals("non-finite") && e.getMessage().startsWith("not-json");
                p.require(ok, id + ": Java refused with " + e.getMessage() + ", expected " + reason);
            }
        }
        for (double d : new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            try { Canonical.bytes(new DoubleNode(d)); p.list.add(d + " in a tree: accepted"); }
            catch (IllegalArgumentException e) { p.equal("non-finite", e.getMessage(), d + " in a tree"); }
        }
        Proc py = c.python("F-JCS-REFUSALS", "-m", "forseti_oracle.cli", "canon-check", VECTORS);
        p.require(py.exit() == 0, "oracle canon-check exit " + py.exit() + ": " + py.lastLine());
        return p.result(f.path("refusals").size() + " refusals refused in Java; oracle: " + py.lastLine());
    }

    // --- schemas and Tyr's bytes ---------------------------------------------------------------------------------------------

    static Result schemaCopies(Context c) throws IOException {
        Problems p = new Problems();
        JsonNode lock = Json.read(c.fixtures().resolve("registry.lock.json"));
        String[][] schemas = {{"occupancy-1.0.0.schema.json", OCCUPANCY_SHA, "occupancySchemaSha256", InputParser.OCCUPANCY_SCHEMA_ID, "occupancy-binding-0.json"},
                              {"incident-1.0.0.schema.json", INCIDENT_SHA, "incidentSchemaSha256", InputParser.INCIDENT_SCHEMA_ID, "incident-binding-0.json"}};
        for (String[] s : schemas) {
            byte[] published = Files.readAllBytes(c.path("schemas").resolve(s[0])), builtIn;
            try (InputStream in = CoreGate.class.getResourceAsStream("/forseti/schemas/" + s[0])) {
                if (in == null) { p.list.add("no core resource " + s[0]); continue; }
                builtIn = in.readAllBytes();
            }
            p.require(Arrays.equals(published, builtIn), s[0] + ": schemas/ bytes differ from the core resource");
            String d = Canonical.digestOf(published);
            p.equal(s[1], d, s[0] + " digest (literal)");
            p.equal(lock.path(s[2]).asText(null), d, s[0] + " digest (registry.lock.json " + s[2] + ")");
            p.equal(InputParser.schemaSha256(s[3]), d, s[0] + " digest (built in)");
            for (String bundle : List.of("bundle", "plc-stamped-bundle")) {
                JsonNode profile = Json.read(c.fixtures().resolve("upstream").resolve(bundle).resolve("data").resolve(s[4]));
                p.equal(d, profile.path("format").path("schemaSha256").asText(null), bundle + "/data/" + s[4] + " format.schemaSha256");
            }
        }
        return p.result("occupancy " + OCCUPANCY_SHA + ", incident " + INCIDENT_SHA + ": core resources, lock and both bundles' profiles agree");
    }

    static Result upstreamUntouched(Context c) throws IOException {
        Problems p = new Problems();
        Path up = c.fixtures().resolve("upstream");
        JsonNode files = Json.read(up.resolve("manifest.json")).path("files");
        Map<String, String> manifest = new TreeMap<>();
        files.fields().forEachRemaining(e -> manifest.put(e.getKey(), e.getValue().asText()));
        Map<String, String> actual = new TreeMap<>();
        for (String bundle : List.of("bundle", "plc-stamped-bundle"))
            try (Stream<Path> s = Files.walk(up.resolve(bundle))) {
                for (Path f : s.filter(Files::isRegularFile).toList()) actual.put(relative(up, f), Canonical.digestOf(Files.readAllBytes(f)));
            }
        p.equal(108, manifest.size(), "manifest entries");
        for (String k : manifest.keySet()) if (!actual.containsKey(k)) p.list.add("missing " + k);
        for (String k : actual.keySet()) if (!manifest.containsKey(k)) p.list.add("not in the manifest " + k);
        for (var e : manifest.entrySet()) if (actual.containsKey(e.getKey()) && !actual.get(e.getKey()).equals(e.getValue())) p.list.add("bytes differ " + e.getKey());
        int published = 0;
        for (String[] r : new String[][] {{"line2-rules", "bundle"}, {"line2-rules-plc", "plc-stamped-bundle"}}) {
            Path dir = c.fixtures().resolve("registry/mappings/releases").resolve(r[0]).resolve("1.0.0");
            Set<String> payload = new TreeSet<>();
            for (JsonNode f : Json.read(dir.resolve("release.json")).path("files")) {
                String path = Json.text(f, "path"), sha = Json.text(f, "sha256");
                if (!path.startsWith("payload/")) { p.list.add(r[0] + ": " + path + " is not a payload path"); continue; }
                String rel = path.substring("payload/".length());
                payload.add(r[1] + "/" + rel);
                p.equal(manifest.get(r[1] + "/" + rel), sha, r[0] + " " + path + " listed digest vs Tyr's");
                p.equal(sha, Canonical.digestOf(Files.readAllBytes(dir.resolve(path))), r[0] + " " + path + " bytes");
                published++;
            }
            Set<String> bundleFiles = new TreeSet<>();
            for (String k : manifest.keySet()) if (k.startsWith(r[1] + "/")) bundleFiles.add(k);
            p.equal(bundleFiles, payload, r[0] + " payload set vs " + r[1]);
        }
        return p.result(manifest.size() + " upstream files match the manifest; " + published + " payload files of the two releases carry Tyr's digests");
    }

    static String relative(Path base, Path f) {
        List<String> parts = new ArrayList<>();
        for (Path x : base.relativize(f)) parts.add(x.toString());
        return String.join("/", parts);
    }

    // --- journal reducers ----------------------------------------------------------------------------------------------------

    static final String WORK = "forseti.work-records";

    static ObjectNode body(String status) { return Json.object().put("status", status); }
    static String d(String status) { return Canonical.digest(body(status)); }
    static Pins pins() {
        return new Pins(Json.object(), Json.object(), Json.object(), Map.of(),
            new Pins.Job("core-harness", "0.1.0", "cd".repeat(32), "run-1", "payload", Json.JSON.getNodeFactory().textNode("off"), "ef".repeat(32)));
    }
    static Row row(long generation, JsonNode key, String status, long revision, String supersedes, long triggerOffset) {
        Envelope e = new Envelope(generation, revision, supersedes, Json.object().put("kind", "input").put("inputKind", "occupancy"), 10L,
            new Envelope.Trigger("mfg.occupancy", 0, triggerOffset), Json.JSON.createArrayNode(), pins());
        return Row.of(WORK, key, body(status), e);
    }
    static Progress.Stage1 progress(long cutId, long through) {
        return new Progress.Stage1(1, S1, cutId, 0, 1000 * cutId - 1, cutId == 1 ? null : 1000 * (cutId - 1) - 1,
            List.of(new Progress.BarrierRef(cutId, new Progress.Origin("mfg.occupancy", 0), through)), 0, null, List.of(),
            cutId == 1 ? new Progress.InitEvidence(0, 1, "none") : null);
    }

    static Result journalReducers(Context c) {
        Problems p = new Problems();
        JsonNode a = Json.object().put("visitId", "occ-1"), b = Json.object().put("visitId", "occ-3");
        String prov = d("provisional"), done = d("complete");

        Revisions r = new Revisions();
        p.equal(Optional.of(new Revisions.Emission(1, null)), r.next(1, a, prov), "first revision");
        p.equal(Optional.empty(), r.next(1, a, prov), "an unchanged body");
        p.equal(Optional.of(new Revisions.Emission(2, prov)), r.next(1, a, done), "a changed body");
        p.equal(new Revisions.Emission(3, done), r.delete(1, a), "a delete takes the next revision and supersedes the last digest");
        p.equal(Optional.of(new Revisions.Emission(4, null)), r.next(1, a, done), "a reappearance continues the numbering");
        p.equal(Optional.of(new Revisions.Emission(1, null)), r.next(2, a, done), "another generation starts at 1");
        try { r.delete(1, b); p.list.add("a delete of a key never emitted was accepted"); } catch (IllegalStateException expected) { /* refused */ }

        Disposition.Key key = new Disposition.Key(1, "mfg.occupancy", 2, 40, Disposition.Part.EVENT);
        JsonNode identity = Json.object().put("source", "mes-line2").put("eventId", "occ-1");
        Disposition.Body accepted = new Disposition.Body("occupancy", identity, "ab".repeat(32), Disposition.Kind.ACCEPTED, Disposition.Reason.ACCEPTED, Json.object());
        Disposition.Body conflict = new Disposition.Body("occupancy", identity, "ab".repeat(32), Disposition.Kind.CONFLICT, Disposition.Reason.IDENTITY_CONFLICT,
            Json.object().put("quarantine", "q-1"));
        Revisions dr = new Revisions();
        p.equal(Optional.of(new Revisions.Emission(1, null)), dr.next(1, key.toJson(), accepted.digest()), "first disposition revision");
        p.equal(Optional.of(new Revisions.Emission(2, accepted.digest())), dr.next(1, key.toJson(), conflict.digest()), "a later conflict under the same delivery key");

        List<JournalFold.Entry> journal = List.of(row(1, a, "provisional", 1, null, 0), row(1, b, "provisional", 1, null, 1),
            row(2, a, "other-generation", 1, null, 1), progress(1, 1), row(1, a, "complete", 2, prov, 2), new Delete(1, b, 2, prov), progress(2, 2),
            row(1, b, "after-the-fold", 3, null, 3));
        JournalFold.Result clean = JournalFold.fold(journal, 1, 2);
        p.require(clean.reachedProgress() && clean.violations().isEmpty(), "clean fold: " + clean.violations());
        p.equal(1, clean.live().size(), "live keys after the delete");
        Row live = (Row) clean.live().get(Canonical.utf8(Canonical.bytes(a)));
        p.require(live != null && live.envelope().revision() == 2 && live.body().equals(body("complete")), "the highest revision is live");
        p.equal(2, JournalFold.fold(journal, 1, 1).live().size(), "live keys through cut 1");

        JournalFold.Violations gap = JournalFold.fold(List.of(row(1, a, "provisional", 1, null, 0), row(1, a, "complete", 3, prov, 0), progress(1, 0)), 1, 1).violations();
        p.equal(1, gap.revisionGaps().size(), "revision gap detected");
        JournalFold.Violations equal = JournalFold.fold(List.of(row(1, a, "provisional", 1, null, 0), row(1, a, "provisional", 2, prov, 0), progress(1, 0)), 1, 1).violations();
        p.equal(1, equal.equalConsecutiveDigests().size(), "equal consecutive digests detected");
        JournalFold.Violations del = JournalFold.fold(List.of(row(1, a, "provisional", 1, null, 0), row(1, a, "complete", 2, prov, 0),
            new Delete(1, a, 3, prov), progress(1, 0)), 1, 1).violations();
        p.equal(1, del.deleteSupersedesMismatches().size(), "delete supersedes mismatch detected");
        JournalFold.Violations after = JournalFold.fold(List.of(row(1, a, "provisional", 1, null, 0), progress(1, 1),
            row(1, b, "provisional", 1, null, 1), progress(2, 2)), 1, 2).violations();
        p.equal(1, after.rowsAfterProgress().size(), "row after its cut's progress detected");
        return p.result("revisions, delete continuity, reappearance, conflict revision, fold and its four violations");
    }

    // --- heads ---------------------------------------------------------------------------------------------------------------

    static Result headOk(Context c) throws IOException {
        Problems p = new Problems();
        TrustedKeys keys = fixtureKeys(c);
        Path reg = c.fixtures().resolve("registry/mappings"), next = c.fixtures().resolve("registry-next/mappings");
        FloorStore floors = floors(c);
        List<String> seen = new ArrayList<>();
        for (String[] s : new String[][] {{"line2", "line2-rules"}, {"line2-plc", "line2-rules-plc"}, {"line2-plc-nocheck", "line2-rules-plc-nocheck"}}) {
            try {
                HeadVerifier.Head h = verify(reg, s[0], keys, floors);
                p.equal(s[1] + " 1.0.0 v1 proposer/approver", h.releaseId() + " " + h.version() + " v" + h.headVersion() + " " + h.publishedBy() + "/" + h.approvedBy(), s[0]);
                p.equal(Canonical.digestOf(GateSigner.statementBytes(reg, s[0])), h.statementSha256(), s[0] + " statementSha256");
                p.equal(Canonical.digestOf(Files.readAllBytes(reg.resolve(h.releasePath()))), h.releaseSha256(), s[0] + " releaseSha256");
                p.equal(1L, floors.get(s[0]).map(FloorStore.Floor::headVersion).orElse(null), s[0] + " floor");
                seen.add(s[0] + " v" + h.headVersion());
            } catch (Refusal e) { p.list.add(s[0] + ": " + e.reason + " " + e.evidence); }
        }
        FloorStore nextFloors = floors(c);
        try {
            HeadVerifier.Head h = verify(next, "line2", keys, nextFloors);
            p.equal("line2-rules 1.0.1 v2", h.releaseId() + " " + h.version() + " v" + h.headVersion(), "registry-next line2");
            p.equal(2L, nextFloors.get("line2").map(FloorStore.Floor::headVersion).orElse(null), "registry-next floor");
            seen.add("registry-next line2 v" + h.headVersion());
        } catch (Refusal e) { p.list.add("registry-next line2: " + e.reason + " " + e.evidence); }
        return p.result("verified at signedAt + 60 s: " + String.join(", ", seen));
    }

    static Result headSignature(Context c) throws IOException {
        Problems p = new Problems();
        Path m = GateSigner.copy(c.fixtures(), "registry", c.tmp);
        long now = GateSigner.now(m, "line2");
        Pair others = freshPair(c);
        Refusal r = p.refused("keys not trusted", "head-unverified", "signature", () -> HeadVerifier.verify(m, "line2", others.trusted(), floors(c), now));
        if (r != null) p.equal("proposer", r.evidence.get("principal"), "keys not trusted: principal");

        Path forged = GateSigner.copy(c.fixtures(), "registry", c.tmp);
        Pair pair = freshPair(c);
        KeyPair forger = GateSigner.generate();
        GateSigner.resign(forged, "line2", st -> {}, pair.proposer(), new GateSigner.Signer("approver", forger.getPrivate()));
        r = p.refused("forged keyid", "head-unverified", "signature", () -> HeadVerifier.verify(forged, "line2", pair.trusted(), floors(c), now));
        if (r != null) p.equal("approver", r.evidence.get("principal"), "forged keyid: principal");

        Path changed = GateSigner.copy(c.fixtures(), "registry", c.tmp);
        byte[] body = GateSigner.statementBytes(changed, "line2"), longer = Arrays.copyOf(body, body.length + 1);
        longer[body.length] = ' ';
        ObjectNode env = GateSigner.envelope(changed, "line2");
        env.put("payload", Base64.getEncoder().encodeToString(longer));
        GateSigner.writeEnvelope(changed, "line2", env);
        p.refused("payload changed after signing", "head-unverified", "signature", () -> HeadVerifier.verify(changed, "line2", fixtureKeys(c), floors(c), now));
        return p.result("untrusted keys, a forged keyid and a changed payload byte are head-unverified (signature)");
    }

    static Result headSameKey(Context c) throws IOException {
        Problems p = new Problems();
        Path m = GateSigner.copy(c.fixtures(), "registry", c.tmp);
        KeyPair k = GateSigner.generate();
        GateSigner.resign(m, "line2", st -> {}, new GateSigner.Signer("proposer", k.getPrivate()), new GateSigner.Signer("approver", k.getPrivate()));
        TrustedKeys trusted = GateSigner.trusted(c.newDir("keys"), "proposer", k.getPublic(), "approver", k.getPublic());
        p.refused("one key for both principals", "head-unverified", "same-key", () -> verify(m, "line2", trusted, floors(c)));
        return p.result("two signatures from one key are head-unverified (same-key)");
    }

    static Result headSubject(Context c) throws IOException {
        Problems p = new Problems();
        Path m = GateSigner.copy(c.fixtures(), "registry", c.tmp);
        long now = GateSigner.now(m, "line2");
        Path head = GateSigner.headFile(m, "line2");
        Files.write(head, (Files.readString(head, StandardCharsets.UTF_8) + "\n").getBytes(StandardCharsets.UTF_8));
        p.refused("head file changed", "head-unverified", "subject", () -> HeadVerifier.verify(m, "line2", fixtureKeys(c), floors(c), now));

        Path other = GateSigner.copy(c.fixtures(), "registry", c.tmp);
        Pair pair = freshPair(c);
        GateSigner.resign(other, "line2", st -> ((ObjectNode) st.path("subject").get(1).path("digest")).put("sha256", "0".repeat(64)), pair.proposer(), pair.approver());
        p.refused("statement names another release.json", "head-unverified", "subject", () -> HeadVerifier.verify(other, "line2", pair.trusted(), floors(c), now));
        return p.result("a changed head file and a statement naming another release.json are head-unverified (subject)");
    }

    static Result headFuture(Context c) throws IOException {
        Problems p = new Problems();
        Path m = GateSigner.copy(c.fixtures(), "registry", c.tmp);
        long now = GateSigner.now(m, "line2");
        Pair pair = freshPair(c);
        GateSigner.resign(m, "line2", st -> ((ObjectNode) st.path("predicate")).put("signedAt", instant(now + 3_600_000)), pair.proposer(), pair.approver());
        FloorStore floors = floors(c);
        p.refused("signed an hour after now", "head-unverified", "signed-in-future", () -> HeadVerifier.verify(m, "line2", pair.trusted(), floors, now));
        p.require(!floorFile(floors, "line2"), "a future statement raised the floor");
        return p.result("a statement signed after now is head-unverified (signed-in-future) and raises no floor");
    }

    static Result headRollback(Context c) throws IOException {
        Problems p = new Problems();
        TrustedKeys keys = fixtureKeys(c);
        Path next = c.fixtures().resolve("registry-next/mappings"), reg = c.fixtures().resolve("registry/mappings");
        FloorStore floors = floors(c);
        long now = GateSigner.now(next, "line2");
        HeadVerifier.verify(next, "line2", keys, floors, now);
        Refusal r = p.refused("registry after registry-next", "head-rollback", null, () -> HeadVerifier.verify(reg, "line2", keys, floors, now));
        if (r != null) { p.equal(2L, r.evidence.get("floor"), "floor"); p.equal(1L, r.evidence.get("candidate"), "candidate"); }
        p.equal(2L, floors.get("line2").map(FloorStore.Floor::headVersion).orElse(null), "floor after the rollback");

        Path other = GateSigner.copy(c.fixtures(), "registry", c.tmp);
        Pair pair = freshPair(c);
        GateSigner.resign(other, "line2", st -> ((ObjectNode) st.path("predicate")).put("approvalId", "another-approval"), pair.proposer(), pair.approver());
        FloorStore same = floors(c);
        HeadVerifier.Head first = HeadVerifier.verify(reg, "line2", keys, same, GateSigner.now(reg, "line2"));
        r = p.refused("version 1 with another statement", "head-rollback", null, () -> HeadVerifier.verify(other, "line2", pair.trusted(), same, GateSigner.now(reg, "line2")));
        if (r != null) p.equal(first.statementSha256(), r.evidence.get("recordedStatementSha256"), "recorded statement");
        return p.result("a lower head version and the same version with another statement are head-rollback; the floor stays");
    }

    static Result headExpired(Context c) throws IOException {
        Problems p = new Problems();
        Path m = c.fixtures().resolve("registry/mappings");
        long expires = GateSigner.expiresMs(m, "line2");
        FloorStore floors = floors(c);
        Refusal r = p.refused("at expires", "head-expired", null, () -> HeadVerifier.verify(m, "line2", fixtureKeys(c), floors, expires));
        if (r != null) p.equal(Instant.ofEpochMilli(expires).toString(), r.evidence.get("expires"), "expires");
        p.require(!floorFile(floors, "line2"), "an expired head raised the floor");
        return p.result("a head read at its expires instant is head-expired and raises no floor");
    }

    // --- release and payload -------------------------------------------------------------------------------------------------

    static Result releaseDigest(Context c) throws IOException {
        Problems p = new Problems();
        Path m = GateSigner.copy(c.fixtures(), "registry", c.tmp);
        HeadVerifier.Head h = verify(m, "line2", fixtureKeys(c), floors(c));
        p.equal(44, ReleaseReader.read(m, h).payload().size(), "untouched release payload files");
        GateSigner.flipMiddleByte(m.resolve(h.releasePath()));
        p.refused("release.json byte changed", "provenance-mismatch", "release-digest", () -> ReleaseReader.read(m, h));
        return p.result("a changed release.json byte is provenance-mismatch (release-digest)");
    }

    static Result payloadDigest(Context c) throws IOException {
        Problems p = new Problems();
        Path m = GateSigner.copy(c.fixtures(), "registry", c.tmp);
        HeadVerifier.Head h = verify(m, "line2", fixtureKeys(c), floors(c));
        GateSigner.flipMiddleByte(m.resolve("releases/line2-rules/1.0.0/payload/data/manual-0.txt"));
        Refusal r = p.refused("payload byte changed", "provenance-mismatch", "release-file", () -> ReleaseReader.read(m, h));
        if (r != null) p.equal("payload/data/manual-0.txt", r.evidence.get("path"), "path");
        return p.result("a changed payload byte is provenance-mismatch (release-file)");
    }

    // --- rule sets -----------------------------------------------------------------------------------------------------------

    static Result rulesLoad(Context c, String stream, String releaseId, boolean occupancyJoin) throws Exception {
        Problems p = new Problems();
        Path m = c.fixtures().resolve("registry/mappings");
        ReleaseReader.Release rel = ReleaseReader.read(m, verify(m, stream, fixtureKeys(c), floors(c)));
        RuleSet r = RuleSet.load(rel, RULE, "1.0.0", Json.object());
        Map<MemberCatalog.Role, Long> roles = new EnumMap<>(MemberCatalog.Role.class);
        r.members().byMappingId().values().forEach(x -> roles.merge(x.role(), 1L, Long::sum));
        p.equal(RULE + " 1.0.0", r.attribution().id() + " " + r.attribution().version(), "rule");
        p.equal(List.of(S1, "synthetic-site:line2:station2", "synthetic-site:line2:station3"), r.attribution().stations(), "stations");
        p.equal("line2-impact", r.impact().id(), "impact rule");
        if (occupancyJoin) {
            p.equal("occupancy-join/none", r.attribution().mode() + "/" + r.attribution().crossCheck(), "identity");
            p.equal(7, r.members().byMappingId().size(), "members");
            p.equal(Map.of(MemberCatalog.Role.MEASUREMENT, 2L, MemberCatalog.Role.ACTIVITY, 5L), roles, "roles");
            p.equal(List.of("nutrunner1-torque", "nutrunner2-torque"), r.members().requiredMembersOf(S1).stream().map(MemberCatalog.Resolved::mappingId).toList(), "required members of station 1");
        } else {
            p.equal("plc-stamped/occupancy", r.attribution().mode() + "/" + r.attribution().crossCheck(), "identity");
            p.equal(Optional.of("urn:line2:visits"), r.attribution().namespace(), "namespace");
            p.equal(2, r.attribution().identityMembers().size(), "identity members");
            p.equal(9, r.members().byMappingId().size(), "members");
            p.equal(Map.of(MemberCatalog.Role.MEASUREMENT, 1L, MemberCatalog.Role.ACTIVITY, 2L, MemberCatalog.Role.IDENTITY_UNIT, 2L,
                MemberCatalog.Role.IDENTITY_VISIT, 2L, MemberCatalog.Role.IDENTITY_PRESENT, 2L), roles, "roles");
            p.equal(List.of("nutrunner1-torque"), r.members().requiredMembersOf(S1).stream().map(MemberCatalog.Resolved::mappingId).toList(), "required members of station 1");
        }
        Proc oracle = c.python("F-RULES-" + releaseId, "-m", "forseti_oracle.cli", "rules-digest", "--mappings", "fixtures/registry/mappings",
            "--release", releaseId + "@1.0.0", "--rule", RULE + "@1.0.0");
        p.require(oracle.exit() == 0, "oracle rules-digest exit " + oracle.exit() + ": " + oracle.lastLine());
        if (oracle.exit() == 0) p.equal(r.rulesDigest(), oracle.lastLine(), "rulesDigest (Java vs oracle)");
        return p.result(releaseId + " 1.0.0: " + r.members().byMappingId().size() + " members, rulesDigest " + r.rulesDigest() + " equals the oracle's");
    }

    static Result bindingSchemaMismatch(Context c) throws IOException {
        Problems p = new Problems();
        Path m = c.fixtures().resolve("registry-mismatch/mappings");
        HeadVerifier.Head h = verify(m, "line2", fixtureKeys(c), floors(c));
        ReleaseReader.Release rel = ReleaseReader.read(m, h);
        p.equal(44, rel.payload().size(), "payload files");
        Refusal r = p.refused("RuleSet.load", "binding.schema-mismatch", null, () -> RuleSet.load(rel, RULE, "1.0.0", Json.object()));
        if (r != null) {
            p.equal("occupancy-binding-line2", r.evidence.get("profile"), "profile");
            p.equal("0".repeat(63) + "1", r.evidence.get("approved"), "approved");
            p.equal(OCCUPANCY_SHA, r.evidence.get("expected"), "expected");
        }
        return p.result("head verifies and the release reads, then the rule set is binding.schema-mismatch (approved " + "0".repeat(63) + "1)");
    }
}
