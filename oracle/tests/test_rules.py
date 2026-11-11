import hashlib
import json
import shutil
from pathlib import Path

import pytest

from forseti_oracle import canon, inputs, rules
from forseti_oracle.rules import ACTIVITY, IDENTITY_PRESENT, IDENTITY_UNIT, IDENTITY_VISIT, MEASUREMENT, Member, Refused

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures"
S1, S2, S3 = "synthetic-site:line2:station1", "synthetic-site:line2:station2", "synthetic-site:line2:station3"
NR1, NR2 = S1 + ":nutrunner1", S1 + ":nutrunner2"
OCCUPANCY_SHA = "e97cd7c97b1ab5e2d63d5b06e6057415945b82cff6d2ca0345106d2daf9d29dc"

# core/src/test/java/yggdrasil/forseti/core/rules/RuleSetTest.java pins the same literals (definitions = {}).
JAVA_PINS = {
    ("registry", "line2-rules", "1.0.0"): "eebc7b50e48d67bd50a16fd8dcb5071d9b5262c67535b260ee78d9c68889b540",
    ("registry", "line2-rules-plc", "1.0.0"): "83b158de38f98cfc0d51782b5a711e173ecc712c0cc5210347cd0f921b8f3381",
    ("registry", "line2-rules-plc-nocheck", "1.0.0"): "7dbcb9e79435daed9c879726ceca48701c9b52052426959a5dad025e8e6deaed",
    ("registry-next", "line2-rules", "1.0.1"): "7b7a2e369dd70829a71f7830a82725bd0700c7fed02b86cc483abe915ac3cb76",
}
# The attribution rule version each release carries: the nocheck variant and release 1.0.1 carry rule 1.0.1.
RULE_VERSION = {("line2-rules", "1.0.0"): "1.0.0", ("line2-rules-plc", "1.0.0"): "1.0.0", ("line2-rules-plc-nocheck", "1.0.0"): "1.0.1",
                ("line2-rules", "1.0.1"): "1.0.1"}


def load(registry: str, release_id: str, version: str):
    rel = rules.load_release(FIXTURES / registry / "mappings", release_id, version)
    return rel, rules.rule_set(rel, "line2-attribution", RULE_VERSION[(release_id, version)])


def role_counts(rs) -> dict:
    out: dict = {}
    for m in rs.members.values():
        out[m.role] = out.get(m.role, 0) + 1
    return out


def test_loads_the_occupancy_join_rule():
    _, rs = load("registry", "line2-rules", "1.0.0")
    a = rs.attribution
    assert (a["id"], a["version"]) == ("line2-attribution", "1.0.0")
    assert rs.stations == [S1, S2, S3]
    assert (a["identity"]["mode"], a["identity"]["crossCheck"]) == ("occupancy-join", "none")
    assert "namespace" not in a["identity"] and "members" not in a["identity"]
    assert a["temporal"] == {"timestampBasis": "source", "outOfOrdernessMs": 30000, "allowedLatenessMs": 720000, "horizonMs": 86400000,
                             "enterToleranceMs": 45000, "leaveToleranceMs": 45000}
    assert a["quality"] == {"uncertainInput": "reject", "maxBadShare": 0.1}
    assert a["coverage"] == {"expectedPeriodMs": 1000, "maxMissingPeriods": 3}
    assert len(rs.members) == 7
    assert role_counts(rs) == {MEASUREMENT: 2, ACTIVITY: 5}
    assert rs.members["station3-running"].role == ACTIVITY
    assert rs.required_members_of(S1) == ["nutrunner1-torque", "nutrunner2-torque"]
    assert rs.required_members_of(S2) == []
    act = rs.activity_of(NR1)
    assert (act.mapping_id, act.value_type) == ("nutrunner1-running", "Boolean")
    assert rs.activity_of("synthetic-site:line2") is None
    assert {m.equipment_path for m in rs.members.values()} == {S1, S2, S3, NR1, NR2}


def test_loads_the_plc_stamped_rule():
    _, rs = load("registry", "line2-rules-plc", "1.0.0")
    ident = rs.attribution["identity"]
    assert rs.stations == [S1, S2, S3]
    assert (ident["mode"], ident["crossCheck"], ident["namespace"]) == ("plc-stamped", "occupancy", "urn:line2:visits")
    assert [(i["equipmentPath"], i["unitMemberRef"], i["visitMemberRef"], i["presentMemberRef"]) for i in ident["members"]] == [
        (S1, "station1-unit", "station1-visit", "station1-present"), (NR1, "nutrunner1-unit", "nutrunner1-visit", "nutrunner1-present")]
    assert len(rs.members) == 9
    assert role_counts(rs) == {MEASUREMENT: 1, ACTIVITY: 2, IDENTITY_UNIT: 2, IDENTITY_VISIT: 2, IDENTITY_PRESENT: 2}
    assert rs.required_members_of(S1) == ["nutrunner1-torque"]
    assert rs.activity_of(NR1).mapping_id == "nutrunner1-running"
    visit = rs.members["station1-visit"]
    assert (visit.metric, visit.value_type, visit.role) == ("VisitId", "String", IDENTITY_VISIT)
    assert (rs.members["nutrunner1-present"].role, rs.members["nutrunner1-present"].value_type) == (IDENTITY_PRESENT, "Boolean")


def test_resolves_members_through_mapping_target_and_definition():
    _, rs = load("registry", "line2-rules", "1.0.0")
    assert rs.members["nutrunner2-torque"] == Member("nutrunner2-torque", NR2, S1, "Torque", "Double", "urn:bifrost:sem:Tool/Torque", MEASUREMENT)
    assert rs.measurements_of(NR1) == ["nutrunner1-torque"]
    assert rs.measurements_of(S1) == []
    s3 = rs.members["station3-running"]
    assert (s3.equipment_path, s3.station, s3.semantic_id) == (S3, S3, "urn:bifrost:sem:Station/Running")


def test_hierarchy_paths_and_stations():
    _, rs = load("registry", "line2-rules", "1.0.0")
    paths = rules.hierarchy_paths(rs.hierarchy)
    assert paths["nutrunner1"] == NR1 and paths["synthetic-site"] == "synthetic-site"
    assert rules.station_of(rs.stations, NR2) == S1
    assert rules.station_of(rs.stations, S3) == S3
    assert rules.station_of(rs.stations, "synthetic-site:line2") is None
    assert rules.station_of(rs.stations, S1 + "x") is None
    assert rules.under(S1, NR1) and rules.under(S1, S1) and not rules.under(S1, S2) and not rules.under(NR1, S1)
    with pytest.raises(ValueError):
        rules.hierarchy_paths({"id": "h", "nodes": [{"equipmentInstanceId": "a", "parentEquipmentInstanceId": "b"},
                                                    {"equipmentInstanceId": "b", "parentEquipmentInstanceId": "a"}]})


def test_impact_rule_and_bindings():
    _, rs = load("registry", "line2-rules", "1.0.0")
    i = rs.impact
    assert i["id"] == "line2-impact"
    assert i["incidentScope"] == [NR1, NR2]
    assert (i["interval"]["overlap"], i["evidence"]["match"], i["evidence"]["onBasisRetracted"], i["evidence"]["onWithdrawn"]) == (
        "any-overlap", "equipment-evidence", "by-cause", "not-affected")
    assert i["retention"]["workRecordsMs"] == 604800000
    o, n = rs.occupancy, rs.incident
    assert (o["topic"], o["format"]["schemaId"], o["format"]["schemaSha256"]) == ("mfg.occupancy", rules.OCCUPANCY_SCHEMA, OCCUPANCY_SHA)
    assert rs.unit_id_matches("UNIT0000000000001")
    assert not rs.unit_id_matches("unit-1")
    assert not rs.unit_id_matches("UNIT00000000000012")
    assert (o["enter"]["bizStep"], o["leave"]["bizStep"]) == ("urn:epcglobal:cbv:bizstep:arriving", "urn:epcglobal:cbv:bizstep:departing")
    assert o["correction"]["reasons"] == ["urn:epcglobal:cbv:er:did_not_occur", "urn:epcglobal:cbv:er:incorrect_data"]
    assert o["sources"] == ["mes-line2"]
    assert (n["topic"], n["format"]["schemaId"]) == ("quality.incidents", rules.INCIDENT_SCHEMA)
    assert n["format"]["schemaSha256"] == inputs.schema_sha256(rules.INCIDENT_SCHEMA)
    assert n["kinds"] == ["tool-check-failed", "gauge-drift"]


def test_pins_name_each_document_by_its_release_digest():
    rel, rs = load("registry", "line2-rules", "1.0.0")
    p = rs.pins({})
    assert sorted(p) == ["bindings", "definitions", "hierarchy", "rules"]

    def pin(i, v, path):
        return {"id": i, "version": v, "sha256": rel.files["payload/" + path]}

    assert p["rules"] == {"attribution": pin("line2-attribution", "1.0.0", "attribution-rules/0.json"),
                          "impact": pin("line2-impact", "1.0.0", "impact-rules/0.json")}
    assert p["hierarchy"] == pin("line2-plant", "1.0.0", "hierarchy/0.json")
    assert p["bindings"] == {"occupancy": pin("occupancy-binding-line2", "1.0.0", "evidence/occupancy-binding-0.json"),
                             "incident": pin("incident-binding-line2", "1.0.0", "evidence/incident-binding-0.json")}
    assert p["definitions"] == {}
    assert rs.rules_digest({}) == canon.digest(p)


def test_rules_digest_changes_with_definitions():
    _, rs = load("registry", "line2-rules", "1.0.0")
    defs = {NR1: {"templateRef": "tool-stamped-model", "version": "1.0.0", "sha256": "ab" * 32}}
    assert len(rs.rules_digest({})) == 64
    assert rs.rules_digest(defs) != rs.rules_digest({})
    assert rs.pins(defs)["definitions"] == defs


@pytest.mark.parametrize("registry,release_id,version", sorted(JAVA_PINS))
def test_rules_digest_equals_java_pin(registry, release_id, version):
    _, rs = load(registry, release_id, version)
    assert rs.rules_digest({}) == JAVA_PINS[(registry, release_id, version)]


def test_loads_rule_101_from_registry_next():
    rel, rs = load("registry-next", "line2-rules", "1.0.1")
    assert rs.attribution["version"] == "1.0.1"
    assert rs.attribution["coverage"]["maxMissingPeriods"] == 4
    assert rs.impact["version"] == "1.0.1"
    assert rs.rules_digest({}) != JAVA_PINS[("registry", "line2-rules", "1.0.0")]
    with pytest.raises(Refused) as e:
        rules.rule_set(rel, "line2-attribution", "1.0.0")
    assert (e.value.reason, e.value.evidence["rule"]) == ("provenance-mismatch", "document-missing")


def test_loads_the_plc_nocheck_rule():
    _, rs = load("registry", "line2-rules-plc-nocheck", "1.0.0")
    assert rs.attribution["version"] == "1.0.1"
    assert (rs.attribution["identity"]["crossCheck"], rs.attribution["identity"]["mode"]) == ("none", "plc-stamped")
    assert rs.impact["version"] == "1.0.1"


def test_a_profile_with_another_digest_is_a_schema_mismatch():
    rel = rules.load_release(FIXTURES / "registry-mismatch" / "mappings", "line2-rules", "1.0.0")   # every digest intact
    with pytest.raises(Refused) as e:
        rules.rule_set(rel, "line2-attribution", "1.0.0")
    assert e.value.reason == "binding.schema-mismatch"
    assert e.value.evidence == {"profile": "occupancy-binding-line2", "expected": OCCUPANCY_SHA, "approved": "0" * 63 + "1"}
    assert OCCUPANCY_SHA == hashlib.sha256((ROOT / "schemas" / "occupancy-1.0.0.schema.json").read_bytes()).hexdigest()


def _rewrite(release_dir: Path, path: str, data: bytes) -> None:
    """Replaces one payload file and re-pins the proposal entry and the release.json digest that name it."""
    (release_dir / "payload" / path).write_bytes(data)
    changed = {"payload/" + path: hashlib.sha256(data).hexdigest()}
    proposal = json.loads((release_dir / "payload" / "proposal.json").read_bytes())
    for f in proposal["files"]:
        if f["path"] == path:
            f["sha256"] = changed["payload/" + path]
            pb = (json.dumps(proposal, indent=2) + "\n").encode()
            (release_dir / "payload" / "proposal.json").write_bytes(pb)
            changed["payload/proposal.json"] = hashlib.sha256(pb).hexdigest()
    doc = json.loads((release_dir / "release.json").read_bytes())
    for f in doc["files"]:
        f["sha256"] = changed.get(f["path"], f["sha256"])
    (release_dir / "release.json").write_bytes(json.dumps(doc, separators=(",", ":"), sort_keys=True).encode())


def test_an_unsupported_kind_is_a_schema_mismatch(tmp_path):
    shutil.copytree(FIXTURES / "registry", tmp_path / "registry")
    mappings = tmp_path / "registry" / "mappings"
    rd = mappings / "releases" / "line2-rules" / "1.0.0"
    profile = json.loads((rd / "payload" / "data" / "incident-binding-0.json").read_bytes())
    profile["kinds"].append("spindle-overheat")
    pb = (json.dumps(profile, indent=2) + "\n").encode()
    _rewrite(rd, "data/incident-binding-0.json", pb)
    artifact = json.loads((rd / "payload" / "evidence" / "incident-binding-0.json").read_bytes())
    artifact["file"]["sha256"] = hashlib.sha256(pb).hexdigest()
    _rewrite(rd, "evidence/incident-binding-0.json", (json.dumps(artifact, indent=2) + "\n").encode())
    head = json.loads((mappings / "heads" / "line2.json").read_bytes())
    head["sha256"] = hashlib.sha256((rd / "release.json").read_bytes()).hexdigest()
    (mappings / "heads" / "line2.json").write_bytes(json.dumps(head, separators=(",", ":"), sort_keys=True).encode())
    rel = rules.load_release(mappings, "line2-rules", "1.0.0")
    with pytest.raises(Refused) as e:
        rules.rule_set(rel, "line2-attribution", "1.0.0")
    assert e.value.reason == "binding.schema-mismatch"
    assert e.value.evidence == {"rule": "kind-unsupported", "profile": "incident-binding-line2", "kind": "spindle-overheat"}


def test_a_changed_payload_byte_is_a_mismatch(tmp_path):
    shutil.copytree(FIXTURES / "registry", tmp_path / "registry")
    f = tmp_path / "registry" / "mappings" / "releases" / "line2-rules" / "1.0.0" / "payload" / "data" / "manual-0.txt"
    b = bytearray(f.read_bytes())
    b[0] ^= 1
    f.write_bytes(bytes(b))
    with pytest.raises(Refused) as e:
        rules.load_release(tmp_path / "registry" / "mappings", "line2-rules", "1.0.0")
    assert (e.value.reason, e.value.evidence["rule"]) == ("provenance-mismatch", "release-file")


def test_payload_counts():
    assert len(rules.load_release(FIXTURES / "registry" / "mappings", "line2-rules", "1.0.0").data) == 44
    assert len(rules.load_release(FIXTURES / "registry" / "mappings", "line2-rules-plc", "1.0.0").data) == 64
