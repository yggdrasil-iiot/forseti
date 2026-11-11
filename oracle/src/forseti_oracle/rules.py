"""The rule set of a published release, read independently from the spec text.

A release lives at mappings/releases/<id>/<version>/: release.json lists every payload file with its raw-byte SHA-256, and a head
mappings/heads/<stream>.json names release.json by its raw-byte SHA-256. The oracle re-checks those digests from the bytes on disk (it
is not the trust gate: head signatures are the reader's business), then resolves what a job needs from the bundle's proposal: the
attribution rule, the impact rule naming it, the equipment hierarchy, the two binding profiles and every member through mapping set,
target instance and semantic definition. A profile whose format.schemaSha256 is not the built-in schema's raw-byte digest, or an
incident kind the built-in incident schema does not list, is the start-up refusal binding.schema-mismatch.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from forseti_oracle import canon, inputs

OCCUPANCY_SCHEMA = "urn:forseti:schema:occupancy:1.0.0"
INCIDENT_SCHEMA = "urn:forseti:schema:incident:1.0.0"

MEASUREMENT, ACTIVITY, IDENTITY_UNIT, IDENTITY_VISIT, IDENTITY_PRESENT = "measurement", "activity", "identity-unit", "identity-visit", "identity-present"


class Refused(Exception):
    def __init__(self, reason: str, **evidence):
        super().__init__(reason)
        self.reason = reason
        self.evidence = evidence


def _mismatch(rule: str, **evidence) -> Refused:
    return Refused("provenance-mismatch", rule=rule, **evidence)


def _clean(path: str) -> bool:
    if not path or path.startswith("/") or "\\" in path:
        return False
    return all(part not in ("", ".", "..") for part in path.split("/"))


@dataclass
class Release:
    root: Path
    doc: dict
    files: dict[str, str]              # "payload/<path>" -> raw-byte SHA-256
    data: dict[str, bytes]             # "payload/<path>" -> the bytes checked against that digest
    documents: dict[tuple[str, str], dict] = field(default_factory=dict)   # (kind, id) -> {"version", "path", "sha256", "node"}

    def payload(self, path: str) -> bytes:
        key = "payload/" + path
        if key not in self.data:
            raise _mismatch("unlisted", path=path)
        return self.data[key]

    def document(self, kind: str, ref: dict) -> dict:
        d = self.documents.get((kind, ref["id"]))
        if d is None or d["version"] != ref["version"]:
            raise _mismatch("document-missing", kind=kind, id=ref["id"], version=ref["version"])
        return d

    def of_kind(self, kind: str) -> list[dict]:
        return [d for (k, _), d in sorted(self.documents.items()) if k == kind]


def _head_sha(mappings: Path, release_id: str, version: str) -> str:
    for head in sorted((mappings / "heads").glob("*.json")):
        h = canon.loads(head.read_bytes().decode("utf-8"))
        if h.get("id") == release_id and h.get("version") == version:
            return h["sha256"]
    raise _mismatch("release-digest", reason="no head names the release", id=release_id, version=version)


def load_release(mappings: Path, release_id: str, version: str, release_sha256: str | None = None) -> Release:
    """release.json by raw bytes against the head that names it (or the given digest), every listed payload file by its own digest,
    and every proposal entry against the release's list."""
    mappings = Path(mappings)
    root = mappings / "releases" / release_id / version
    expected = release_sha256 if release_sha256 is not None else _head_sha(mappings, release_id, version)
    raw = (root / "release.json").read_bytes()
    if canon.sha256(raw) != expected:
        raise _mismatch("release-digest", expected=expected, actual=canon.sha256(raw))
    doc = canon.loads(raw.decode("utf-8"))
    if doc.get("id") != release_id or doc.get("version") != version:
        raise _mismatch("release-identity", id=doc.get("id"), version=doc.get("version"))
    files: dict[str, str] = {}
    data: dict[str, bytes] = {}
    for entry in doc["files"]:
        path, sha = entry["path"], entry["sha256"]
        if not _clean(path) or not path.startswith("payload/") or path in files:
            raise _mismatch("release-path", path=path)
        b = (root / path).read_bytes()
        if canon.sha256(b) != sha:
            raise _mismatch("release-file", path=path)
        files[path], data[path] = sha, b
    if "payload/proposal.json" not in data:
        raise _mismatch("proposal-file", path="proposal.json")
    release = Release(root, doc, files, data)
    for entry in canon.loads(data["payload/proposal.json"].decode("utf-8"))["files"]:
        if files.get("payload/" + entry["path"]) != entry["sha256"]:
            raise _mismatch("proposal-file", path=entry["path"])
        key = (entry["kind"], entry["id"])
        if key in release.documents:
            raise _mismatch("proposal-file", path=entry["path"], reason="duplicate document")
        release.documents[key] = {"version": entry["version"], "path": entry["path"], "sha256": entry["sha256"],
                                  "node": canon.loads(data["payload/" + entry["path"]].decode("utf-8"))}
    return release


# --- hierarchy -------------------------------------------------------------------------------------------------------------


def hierarchy_paths(doc: dict) -> dict[str, str]:
    """equipmentInstanceId -> its path, root first, joined by ':'."""
    parent = {n["equipmentInstanceId"]: n.get("parentEquipmentInstanceId") for n in doc["nodes"]}
    out = {}
    for node in parent:
        chain, cur = [], node
        while cur is not None:
            if cur not in parent or cur in chain:
                raise ValueError(f"hierarchy {doc['id']}: broken at {cur}")
            chain.append(cur)
            cur = parent[cur]
        out[node] = ":".join(reversed(chain))
    return out


def under(ancestor: str, path: str) -> bool:
    return path == ancestor or path.startswith(ancestor + ":")


def station_of(stations: list[str], path: str) -> str | None:
    found = [s for s in stations if under(s, path)]
    if len(found) > 1:
        raise ValueError(f"nested stations {found}")
    return found[0] if found else None


# --- the rule set ----------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Member:
    mapping_id: str
    equipment_path: str
    station: str
    metric: str
    value_type: str
    semantic_id: str
    role: str


@dataclass
class RuleSet:
    attribution: dict
    impact: dict
    hierarchy: dict
    occupancy: dict          # the profile file
    incident: dict           # the profile file
    members: dict[str, Member]
    pinned: dict             # rules / hierarchy / bindings pins
    pattern: re.Pattern

    @property
    def stations(self) -> list[str]:
        return list(self.attribution["stations"])

    def unit_id_matches(self, unit_id: str) -> bool:
        return self.pattern.fullmatch(unit_id) is not None

    def required_members_of(self, station: str) -> list[str]:
        return sorted(m.mapping_id for m in self.members.values() if m.role == MEASUREMENT and m.station == station)

    def measurements_of(self, equipment_path: str) -> list[str]:
        return sorted(m.mapping_id for m in self.members.values() if m.role == MEASUREMENT and m.equipment_path == equipment_path)

    def activity_of(self, equipment_path: str) -> Member | None:
        for a in self.attribution["activity"]:
            if a["equipmentPath"] == equipment_path:
                return self.members[a["memberRef"]]
        return None

    def pins(self, definitions: dict) -> dict:
        return {**self.pinned, "definitions": dict(definitions)}

    def rules_digest(self, definitions: dict) -> str:
        return canon.digest(self.pins(definitions))


def _pin(release: Release, kind: str, ref: dict) -> dict:
    d = release.document(kind, ref)
    return {"id": ref["id"], "version": ref["version"], "sha256": release.files["payload/" + d["path"]]}


def _profile(release: Release, ref: dict) -> dict:
    artifact = release.document("evidence-artifact", ref)["node"]
    b = release.payload(artifact["file"]["path"])
    if canon.sha256(b) != artifact["file"]["sha256"]:
        raise _mismatch("profile-file", profile=ref["id"])
    return canon.loads(b.decode("utf-8"))


def _roles(rule: dict) -> dict[str, str]:
    roles: dict[str, str] = {}

    def give(ref: str, role: str) -> None:
        if ref in roles:
            raise ValueError(f"member {ref} has two roles")
        roles[ref] = role

    for a in rule["activity"]:
        give(a["memberRef"], ACTIVITY)
    for i in rule["identity"].get("members", []):
        give(i["unitMemberRef"], IDENTITY_UNIT)
        give(i["visitMemberRef"], IDENTITY_VISIT)
        give(i["presentMemberRef"], IDENTITY_PRESENT)
    return roles


def rule_set(release: Release, attribution_rule_id: str, version: str) -> RuleSet:
    ref = {"id": attribution_rule_id, "version": version}
    rule = release.document("attribution-rule", ref)["node"]
    hierarchy = release.document("equipment-hierarchy", rule["equipmentRef"])["node"]
    paths = hierarchy_paths(hierarchy)
    impacts = [d for d in release.of_kind("impact-rule") if d["node"]["attributionRuleRef"] == ref]
    if len(impacts) != 1:
        raise ValueError(f"{len(impacts)} impact rules name {attribution_rule_id} {version}")
    impact = impacts[0]["node"]
    occupancy = _profile(release, rule["occupancyBindingRef"])
    incident = _profile(release, impact["incidentBindingRef"])
    for profile, ref_, schema in ((occupancy, rule["occupancyBindingRef"], OCCUPANCY_SCHEMA), (incident, impact["incidentBindingRef"], INCIDENT_SCHEMA)):
        built_in = inputs.schema_sha256(schema)
        if profile["format"]["schemaSha256"] != built_in:
            raise Refused("binding.schema-mismatch", profile=ref_["id"], expected=built_in, approved=profile["format"]["schemaSha256"])
    supported = inputs.incident_kinds()
    for kind in incident["kinds"]:
        if kind not in supported:
            raise Refused("binding.schema-mismatch", rule="kind-unsupported", profile=impact["incidentBindingRef"]["id"], kind=kind)

    roles = _roles(rule)
    members: dict[str, Member] = {}
    for m in rule["members"]:
        mset = release.document("mapping-set", m["mappingSetRef"])["node"]
        mapping = next(x for x in mset["mappings"] if x["mappingId"] == m["mappingId"])
        if len(mapping["memberPath"]) != 1 or mapping["memberPath"][0]["kind"] != "member":
            raise ValueError(f"member {m['mappingId']}: memberPath is not one member step")
        step = mapping["memberPath"][0]
        target = release.document("target-instance", mset["targetInstanceRef"])["node"]
        definition = release.document("semantic-definition", target["semanticDefinitionRef"])["node"]
        leaf = next(x for x in definition["members"] if x["memberId"] == step["memberId"])
        if leaf["name"] != m["metric"] or leaf["semanticId"] != m["semanticId"]:
            raise ValueError(f"member {m['mappingId']}: metric or semanticId differs")
        path = paths[mset["equipmentInstanceId"]]
        station = station_of(rule["stations"], path)
        if station is None:
            raise ValueError(f"member {m['mappingId']}: {path} is under no station")
        members[m["mappingId"]] = Member(m["mappingId"], path, station, leaf["name"], leaf["valueType"], leaf["semanticId"],
                                         roles.get(m["mappingId"], MEASUREMENT))
    pinned = {
        "rules": {"attribution": _pin(release, "attribution-rule", ref),
                  "impact": _pin(release, "impact-rule", {"id": impact["id"], "version": impact["version"]})},
        "hierarchy": _pin(release, "equipment-hierarchy", rule["equipmentRef"]),
        "bindings": {"occupancy": _pin(release, "evidence-artifact", rule["occupancyBindingRef"]),
                     "incident": _pin(release, "evidence-artifact", impact["incidentBindingRef"])},
    }
    return RuleSet(rule, impact, hierarchy, occupancy, incident, dict(sorted(members.items())), pinned,
                   re.compile(occupancy["identity"]["pattern"]))
