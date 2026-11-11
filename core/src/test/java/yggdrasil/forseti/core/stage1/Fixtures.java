package yggdrasil.forseti.core.stage1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.function.Consumer;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import yggdrasil.forseti.core.release.*;
import yggdrasil.forseti.core.rules.RuleSet;

/** The fixture rule sets of the stage-1 tests: {@code line2-rules} 1.0.0 (occupancy-join) as released, and copies with an edited rule or
 *  occupancy profile, re-signed with fresh keys. {@code definitions} are the smoke recording's, so the rulesDigest is the harness's.
 *  The plc-stamped streams {@code line2-plc} ({@code crossCheck: occupancy}) and {@code line2-plc-nocheck} ({@code crossCheck: none})
 *  load with the smoke-plc recording's definitions. */
final class Fixtures {
    static final Path FIXTURES = TestKeys.FIXTURES;
    static final String S1 = "synthetic-site:line2:station1", S2 = "synthetic-site:line2:station2", S3 = "synthetic-site:line2:station3";
    static final String NR1 = S1 + ":nutrunner1", NR2 = S1 + ":nutrunner2";
    static final JsonNode DEFINITIONS = Json.read(FIXTURES.resolve("recordings/smoke/manifest.json")).get("definitions");
    static final JsonNode PLC_DEFINITIONS = Json.read(FIXTURES.resolve("recordings/smoke-plc/manifest.json")).get("definitions");
    private static RuleSet line2, plc, plcNoCheck;

    private Fixtures() {}

    static Path tmp(String prefix) {
        try { return Files.createTempDirectory("forseti-stage1-" + prefix); } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    static synchronized RuleSet line2() {
        if (line2 == null) {
            Path m = TestKeys.copy("registry", tmp("registry"));
            line2 = load(m, TestKeys.fixtureKeys());
        }
        return line2;
    }

    /** line2-plc: plc-stamped, crossCheck occupancy, identity on station1 and nutrunner1. */
    static synchronized RuleSet plc() {
        if (plc == null) plc = load(TestKeys.copy("registry", tmp("registry-plc")), "line2-plc", "1.0.0", PLC_DEFINITIONS, TestKeys.fixtureKeys());
        return plc;
    }
    /** line2-plc-nocheck: the same rule as version 1.0.1 with crossCheck none. */
    static synchronized RuleSet plcNoCheck() {
        if (plcNoCheck == null) plcNoCheck = load(TestKeys.copy("registry", tmp("registry-plc-nocheck")), "line2-plc-nocheck", "1.0.1", PLC_DEFINITIONS, TestKeys.fixtureKeys());
        return plcNoCheck;
    }

    private static RuleSet load(Path mappings, TrustedKeys keys) { return load(mappings, "line2", "1.0.0", DEFINITIONS, keys); }

    private static RuleSet load(Path mappings, String stream, String ruleVersion, JsonNode definitions, TrustedKeys keys) {
        HeadVerifier.Head h = HeadVerifier.verify(mappings, stream, keys, new FloorStore(tmp("floor")), TestKeys.signedAtMs(mappings, stream) + 60_000);
        return RuleSet.load(ReleaseReader.read(mappings, h), "line2-attribution", ruleVersion, definitions);
    }

    /** A re-signed copy of line2-rules 1.0.0 with the attribution rule and the occupancy profile edited. */
    static RuleSet variant(Consumer<ObjectNode> rule, Consumer<ObjectNode> occupancyProfile) {
        Path m = TestKeys.copy("registry", tmp("variant"));
        Path dir = m.resolve("releases/line2-rules/1.0.0/payload");
        String rulePath = "attribution-rules/0.json";
        ObjectNode r = (ObjectNode) Json.read(dir.resolve(rulePath));
        rule.accept(r);
        TestKeys.rewritePayload(m, "line2-rules", "1.0.0", rulePath, Json.pretty(r));
        String data = "data/occupancy-binding-0.json", artifact = "evidence/occupancy-binding-0.json";
        ObjectNode profile = (ObjectNode) Json.read(dir.resolve(data));
        occupancyProfile.accept(profile);
        byte[] profileBytes = Json.pretty(profile);
        TestKeys.rewritePayload(m, "line2-rules", "1.0.0", data, profileBytes);
        ObjectNode ev = (ObjectNode) Json.read(dir.resolve(artifact));
        ((ObjectNode) ev.path("file")).put("sha256", Canonical.digestOf(profileBytes));
        TestKeys.rewritePayload(m, "line2-rules", "1.0.0", artifact, Json.pretty(ev));
        KeyPair p = TestKeys.generate(), a = TestKeys.generate();
        TestKeys.republish(m, "line2", new TestKeys.Signer("proposer", p.getPrivate()), new TestKeys.Signer("approver", a.getPrivate()));
        return load(m, TestKeys.trusted(tmp("keys"), "proposer", p.getPublic(), "approver", a.getPublic()));
    }

    static RuleSet uncertain(String mode) { return variant(r -> ((ObjectNode) r.path("quality")).put("uncertainInput", mode), p -> {}); }
    static RuleSet onlyDidNotOccur() {
        return variant(r -> {}, p -> { ArrayNode a = ((ObjectNode) p.path("correction")).putArray("reasons"); a.add("urn:epcglobal:cbv:er:did_not_occur"); });
    }
}
