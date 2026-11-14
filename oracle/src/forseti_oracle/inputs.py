"""Input events of the forseti spec, read independently of forseti-core.

Identities are dicts in contract field order; their canonical bytes order inputs and are the ledger identity. The payload digest is
the JCS SHA-256 of the value without the diagnostic fields (``recordTime``, ``bridgeReceivedAt``) and without each member's lossy
``quality``; a value that cannot be canonicalized gets the SHA-256 of its delivered bytes with rule ``raw-bytes``. Occupancy and
incidents are validated with the published schemas under ``schemas/`` (JSON Schema 2020-12); process messages and session events
structurally. Every integer field is bounded to ``0 .. 2^53-1`` before the schema runs; a failure names the field (``refs.check``).
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path

from jsonschema import Draft202012Validator

from .canon import CanonicalRefusal, canonical_bytes, digest

MESSAGES = "ot.messages"
SESSIONS = "ot.sessions"
OCCUPANCY = "mfg.occupancy"
INCIDENTS = "quality.incidents"
ARRIVING = "urn:epcglobal:cbv:bizstep:arriving"

SAFE_MAX = 2**53 - 1
STATUS_CODE_MAX = 2**32 - 1
SESSION_AMBIGUOUS = "session-ambiguous"
SESSION_KINDS = ("seq-gap", "session-down", "session-up", "stale-death", SESSION_AMBIGUOUS, "definition-changed")
SESSION_PAYLOAD_INTEGERS = ("from", "to", "missing", "at", "detectedEventTime", "rawPublishTs", "bdSeq")

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "schemas"


class SchemaInvalid(ValueError):
    """``rejected schema-invalid``; ``check`` names the field when one is named, None for a whole-value schema failure."""

    def __init__(self, check: str | None):
        super().__init__("schema-invalid")
        self.check = check


def _schema(name: str) -> tuple[bytes, dict]:
    raw = (SCHEMA_DIR / name).read_bytes()
    return raw, json.loads(raw.decode("utf-8"))


_OCC_RAW, _OCC = _schema("occupancy-1.0.0.schema.json")
_INC_RAW, _INC = _schema("incident-1.0.0.schema.json")
_VALIDATORS = {OCCUPANCY: Draft202012Validator(_OCC), INCIDENTS: Draft202012Validator(_INC)}
_SCHEMA_SHA = {_OCC["$id"]: hashlib.sha256(_OCC_RAW).hexdigest(), _INC["$id"]: hashlib.sha256(_INC_RAW).hexdigest()}


def schema_sha256(schema_id: str) -> str:
    return _SCHEMA_SHA[schema_id]


def incident_kinds() -> list[str]:
    return list(_INC["properties"]["kind"]["enum"])


# --- strict JSON ----------------------------------------------------------------------------------------------------------


class NotJson(ValueError):
    pass


def _pairs(pairs):
    d = {}
    for k, v in pairs:
        if k in d:
            raise NotJson("duplicate key")
        d[k] = v
    return d


def _no_constant(name):
    raise NotJson(f"{name} is not JSON")


def loads_value(raw: bytes):
    """A record value as JSON: UTF-8, duplicate keys refused, NaN/Infinity tokens refused (they are not JSON)."""
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_no_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise NotJson(str(e)) from e


# --- integers -------------------------------------------------------------------------------------------------------------


def safe_int(v, field: str, lo: int = 0, hi: int = SAFE_MAX) -> int:
    """An integral JSON number inside [lo, hi]; ``schema-invalid`` naming the field otherwise (bool is not a number here)."""
    if isinstance(v, bool) or not isinstance(v, int) or v < lo or v > hi:
        raise SchemaInvalid(field)
    return v


def _optional_int(obj: dict, field: str, check: str, lo: int = 0, hi: int = SAFE_MAX):
    v = obj.get(field)
    if v is None:
        return None
    return safe_int(v, check, lo, hi)


def _text(obj: dict, field: str) -> str:
    v = obj.get(field)
    if not isinstance(v, str):
        raise SchemaInvalid(field)
    return v


# --- parsed views ---------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Occupancy:
    eventId: str
    eventTime: int
    recordTime: int
    unitId: str
    bizStep: str
    readPoint: str
    visitSeq: int
    arrivalEventId: str | None
    source: str
    errorDeclaration: dict | None

    @property
    def arriving(self) -> bool:
        return self.bizStep == ARRIVING


@dataclass(frozen=True)
class Incident:
    incidentId: str
    revision: int
    stationRef: str
    equipmentRef: str
    kind: str
    detectedAt: int
    revisionEventTime: int
    suspectFrom: int
    suspectTo: int | None
    status: str
    evidenceRef: str
    source: str


@dataclass(frozen=True)
class Member:
    metric: str
    value: object  # float for a number (binary64), bool or str otherwise
    uaStatusCode: int | None
    quality: int | None
    sourceTs: int | None
    serverTs: int | None


@dataclass(frozen=True)
class Message:
    edge: str
    device: str
    birthPublishTs: int
    bdSeq: int
    seq: int
    publishTs: int
    bridgeReceivedAt: int | None
    definitionSha256: str
    members: tuple
    rejected_members: tuple  # (metric, check) of members that did not parse


@dataclass(frozen=True)
class Session:
    """``birthPublishTs`` and ``atSeq`` are None only for the birthless ``session-ambiguous`` variant (plan D4): the NDEATH of a connection
    whose NBIRTH the bridge never observed has no birth to name and its ``atSeq`` is the last observed ``seq`` or None."""
    edge: str
    device: str
    birthPublishTs: int | None
    bdSeq: int
    kind: str
    atSeq: int | None
    payload: dict | None

    @property
    def birthless(self) -> bool:
        return self.birthPublishTs is None


def _validate(topic: str, value: dict) -> None:
    if any(True for _ in _VALIDATORS[topic].iter_errors(value)):
        raise SchemaInvalid(None)


def _occupancy(v: dict) -> Occupancy:
    for f in ("eventTime", "recordTime", "visitSeq"):
        _optional_int(v, f, f)
    ed = v.get("errorDeclaration")
    if isinstance(ed, dict):
        _optional_int(ed, "declarationTime", "errorDeclaration.declarationTime")
    _validate(OCCUPANCY, v)
    return Occupancy(v["eventId"], v["eventTime"], v["recordTime"], v["unitId"], v["bizStep"], v["readPoint"], v["visitSeq"],
                     v.get("arrivalEventId"), v["source"], ed)


def _incident(v: dict) -> Incident:
    _optional_int(v, "revision", "revision", 1)
    for f in ("detectedAt", "revisionEventTime", "suspectFrom", "suspectTo"):
        _optional_int(v, f, f)
    _validate(INCIDENTS, v)
    return Incident(v["incidentId"], v["revision"], v["stationRef"], v["equipmentRef"], v["kind"], v["detectedAt"], v["revisionEventTime"],
                    v["suspectFrom"], v["suspectTo"], v["status"], v["evidenceRef"], v["source"])


def measurement_value(v):
    """A member's numeric value as a binary64; an integer literal beyond the double range is the signed infinity (as 1e400 already is)."""
    try:
        return float(v)
    except OverflowError:
        return math.inf if v >= 0 else -math.inf


def _member(metric: str, m: dict) -> Member:
    raw = m.get("value")
    if isinstance(raw, bool) or isinstance(raw, str):
        value = raw
    elif isinstance(raw, (int, float)):
        value = measurement_value(raw)
    else:
        raise SchemaInvalid("value")
    return Member(metric, value, _optional_int(m, "uaStatusCode", "uaStatusCode", 0, STATUS_CODE_MAX), _optional_int(m, "quality", "quality"),
                  _optional_int(m, "sourceTs", "sourceTs"), _optional_int(m, "serverTs", "serverTs"))


def _message(v: dict) -> Message:
    edge, device = _text(v, "edge"), _text(v, "device")
    birth = safe_int(v.get("birthPublishTs"), "birthPublishTs")
    bd_seq = safe_int(v.get("bdSeq"), "bdSeq")
    seq = safe_int(v.get("seq"), "seq")
    publish = safe_int(v.get("publishTs"), "publishTs")
    received = _optional_int(v, "bridgeReceivedAt", "bridgeReceivedAt")
    definition = _text(v, "definitionSha256")
    ms = v.get("members")
    if not isinstance(ms, list):
        raise SchemaInvalid("members")
    names = set()
    for m in ms:
        if not isinstance(m, dict) or not isinstance(m.get("metric"), str) or m["metric"] in names:
            raise SchemaInvalid("metric")
        names.add(m["metric"])
    members, rejected = [], []
    for m in ms:
        try:
            members.append(_member(m["metric"], m))
        except SchemaInvalid as e:
            rejected.append((m["metric"], e.check))
    return Message(edge, device, birth, bd_seq, seq, publish, received, definition, tuple(members), tuple(rejected))


def _nullable_int(obj: dict, field: str):
    """A present field: JSON null is None, anything else must be an integer; an absent field is refused like a non-integer."""
    if field in obj and obj[field] is None:
        return None
    return safe_int(obj.get(field), field)


def _session(v: dict) -> Session:
    edge, device = _text(v, "edge"), _text(v, "device")
    # birthPublishTs and atSeq may be null only for session-ambiguous (plan D4); a null birthPublishTs on any other kind is refused as
    # birthPublishTs before bdSeq and kind, the order of checks the core keeps
    ambiguous = v.get("kind") == SESSION_AMBIGUOUS
    birth = _nullable_int(v, "birthPublishTs") if ambiguous else safe_int(v.get("birthPublishTs"), "birthPublishTs")
    bd_seq = safe_int(v.get("bdSeq"), "bdSeq")
    kind = _text(v, "kind")
    if kind not in SESSION_KINDS:
        raise SchemaInvalid("kind")
    at_seq = _nullable_int(v, "atSeq")
    if at_seq is None and birth is not None:  # atSeq may be None only on the birthless variant
        raise SchemaInvalid("atSeq")
    payload = v.get("payload")
    if payload is not None:
        if not isinstance(payload, dict):
            raise SchemaInvalid("payload")
        for f in SESSION_PAYLOAD_INTEGERS:
            _optional_int(payload, f, "payload." + f)
    if birth is None:
        # the birthless identity adds the death's rawPublishTs and payloadDigest; without them two such deaths with equal counters
        # would be one event
        raw = payload.get("rawPublishTs") if isinstance(payload, dict) else None
        if not isinstance(raw, int) or isinstance(raw, bool):
            raise SchemaInvalid("payload.rawPublishTs")
        if not isinstance(payload.get("payloadDigest"), str):
            raise SchemaInvalid("payload.payloadDigest")
    return Session(edge, device, birth, bd_seq, kind, at_seq, payload)


_PARSERS = {OCCUPANCY: _occupancy, INCIDENTS: _incident, MESSAGES: _message, SESSIONS: _session}


def parse(topic: str, value):
    """The parsed view of one input value; ``SchemaInvalid`` when it is schema-invalid."""
    if not isinstance(value, dict):
        raise SchemaInvalid(None)
    return _PARSERS[topic](value)


# --- identities and digests -----------------------------------------------------------------------------------------------


def sample_id(m: Message, metric: str) -> dict:
    return {"edge": m.edge, "device": m.device, "birthPublishTs": m.birthPublishTs, "metric": metric, "bdSeq": m.bdSeq, "seq": m.seq,
            "publishTs": m.publishTs}


def message_id(m: Message) -> dict:
    return {"edge": m.edge, "device": m.device, "birthPublishTs": m.birthPublishTs, "bdSeq": m.bdSeq, "seq": m.seq, "publishTs": m.publishTs}


def measurement_id(device: str, metric: str, source_ts: int, value, ua_status_code) -> dict:
    return {"device": device, "metric": metric, "sourceTs": source_ts, "value": value, "uaStatusCode": ua_status_code}


def session_event_id(s: Session) -> dict:
    """(edge, device, birthPublishTs, bdSeq, kind, atSeq); the birthless session-ambiguous variant (plan D4) adds the death's
    rawPublishTs and payloadDigest so two such deaths with equal counters stay distinct."""
    i = {"edge": s.edge, "device": s.device, "birthPublishTs": s.birthPublishTs, "bdSeq": s.bdSeq, "kind": s.kind, "atSeq": s.atSeq}
    if s.birthless:
        i["rawPublishTs"] = s.payload["rawPublishTs"]
        i["payloadDigest"] = s.payload["payloadDigest"]
    return i


def occupancy_id(o: Occupancy) -> dict:
    return {"source": o.source, "eventId": o.eventId}


def incident_id(i: Incident) -> dict:
    return {"source": i.source, "incidentId": i.incidentId, "revision": i.revision}


def identity_bytes(identity: dict) -> bytes:
    return canonical_bytes(identity)


def payload_digest(value, raw: bytes) -> tuple[str, str]:
    """(sha256, rule): rule ``jcs`` for a canonicalizable object, ``raw-bytes`` (digest of the delivered bytes) otherwise."""
    if isinstance(value, dict):
        v = {k: x for k, x in value.items() if k not in ("recordTime", "bridgeReceivedAt")}
        if isinstance(v.get("members"), list):
            v["members"] = [{k: x for k, x in m.items() if k != "quality"} if isinstance(m, dict) else m for m in v["members"]]
        try:
            return digest(v), "jcs"
        except CanonicalRefusal:
            pass
    return hashlib.sha256(raw).hexdigest(), "raw-bytes"


@dataclass(frozen=True)
class Read:
    value: object  # the JSON value, None when the bytes are not JSON
    parsed: object  # the parsed view, None when rejected
    rejected: bool
    check: str | None
    payload_digest: str
    digest_rule: str


def read(topic: str, raw: bytes) -> Read:
    """One delivered record; never raises for its content (bytes that are not JSON are rejected schema-invalid)."""
    try:
        value = loads_value(raw)
    except NotJson:
        d, rule = payload_digest(None, raw)
        return Read(None, None, True, None, d, rule)
    d, rule = payload_digest(value, raw)
    try:
        return Read(value, parse(topic, value), False, None, d, rule)
    except SchemaInvalid as e:
        return Read(value, None, True, e.check, d, rule)


def is_bad(code) -> bool:
    return code is not None and (code & 0xC0000000) == 0x80000000


def is_uncertain(code) -> bool:
    return code is not None and (code & 0xC0000000) == 0x40000000
