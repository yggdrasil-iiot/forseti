"""Stage 1 of the oracle: dispositions, visits, sample rows and work record bodies of one cut, recomputed from scratch.

Written from the spec text (Input events 231-298, Output rows 337-372, the ledgers 443-461, Correction semantics 497-521) and the
plan's rule sheet S1-S11, P1-P3, never from forseti-core. The evaluation of a cut takes every input delivered through that cut, each
tagged with the cut it was delivered in, and the per-station ``admissionW`` and ``W1`` the cut log records:

* ``classify``: S1-S3 as one sweep in delivery order (cut by cut, the contract order inside a cut). Occupancy needs the order, because
  admission (``onlineExpiresAt``) and ``late-after-settlement`` look at the visit as it stood when the input was admitted; everything
  else is a function of the delivered set.
* ``visits_of``: the visit claims of a read point (S4), their retraction, overlap (S7) and ``attributionEnd``.
* ``sessions_of``: gap, down and unknown intervals of a device (S8).
* ``attribute``: S9 as one sweep over the samples sorted by ``sourceTs`` against the visits sorted by window start.
* ``coverage``: S6 for one visit; ``work_record_body`` and ``sample_row_body`` assemble the bodies (S5 status precedence).
* ``plc_visits_of``: P1-P3.
"""
from __future__ import annotations

import bisect
import math
from dataclasses import dataclass, field

from forseti_oracle import canon, inputs
from forseti_oracle.inputs import MESSAGES, OCCUPANCY, SESSIONS
from forseti_oracle.rules import ACTIVITY, IDENTITY_PRESENT, IDENTITY_UNIT, IDENTITY_VISIT, MEASUREMENT, RuleSet, station_of

WORK_RECORDS, SAMPLES = "forseti.work-records", "forseti.samples"
KIND_RANK = {SESSIONS: 0, OCCUPANCY: 1, MESSAGES: 2}
INF = math.inf


def key_text(key) -> str:
    return canon.canonical_bytes(key).decode("utf-8")


# --- deliveries ---------------------------------------------------------------------------------------------------------------


@dataclass
class Delivery:
    """One delivered input record: its origin, the cut it was delivered in (index into the cut history) and its read view."""
    topic: str
    partition: int
    offset: int
    cut: int
    raw: bytes
    read: inputs.Read
    # values that are functions of this delivered record alone (its contract key, the canonical text of its identities, the
    # canonical bytes of its observations), computed once per run instead of once per evaluated cut; never compared
    memo: dict = field(default_factory=dict, repr=False, compare=False)

    @property
    def origin(self) -> dict:
        return {"topic": self.topic, "partition": self.partition, "offset": self.offset}


def make_delivery(topic: str, partition: int, offset: int, cut: int, raw: bytes) -> Delivery:
    return Delivery(topic, partition, offset, cut, raw, inputs.read(topic, raw))


def session_time(s: inputs.Session):
    p = s.payload or {}
    if s.kind == "seq-gap":
        return p.get("to")
    if s.kind == "session-up":
        return p.get("at")
    if s.kind == "session-down":
        return p.get("from")
    if s.kind == "definition-changed":
        return s.birthPublishTs
    return p.get("detectedEventTime")


def event_time(d: Delivery):
    v = d.read.parsed
    if v is None:
        return None
    if d.topic == MESSAGES:
        ts = [m.sourceTs for m in v.members if m.sourceTs is not None]
        return max(ts) if ts else None
    if d.topic == SESSIONS:
        return session_time(v)
    if d.topic == OCCUPANCY:
        return v.eventTime
    return None


def identity_of(d: Delivery) -> dict | None:
    v = d.read.parsed
    if v is None:
        return None
    if d.topic == MESSAGES:
        return inputs.message_id(v)
    if d.topic == SESSIONS:
        return inputs.session_event_id(v)
    if d.topic == OCCUPANCY:
        return inputs.occupancy_id(v)
    return inputs.incident_id(v)


def contract_key(d: Delivery):
    """(cut, eventTime, kindRank, identity bytes, payloadDigest): delivery order across cuts, contract order inside a cut (spec 500).
    An input without event time sorts first; an unparsed record sorts by its origin."""
    k = d.memo.get("contract")
    if k is None:
        k = d.memo["contract"] = _contract_key(d)
    return k


def _contract_key(d: Delivery):
    et = event_time(d)
    ident = identity_of(d)
    ib = canon.canonical_bytes(ident) if ident is not None else canon.canonical_bytes(d.origin)
    return (d.cut, -1 if et is None else et, KIND_RANK.get(d.topic, 3), ib, d.read.payload_digest)


# --- dispositions -------------------------------------------------------------------------------------------------------------


@dataclass
class Disposition:
    delivery: Delivery
    input_kind: str
    identity: dict
    part: str
    disposition: str
    reason: str
    refs: dict = field(default_factory=dict)

    @property
    def key(self) -> str:
        # the identity of a disposition is a function of its delivery, input kind and part (every construction below), so its
        # canonical text is kept on the delivery
        memo, mk = self.delivery.memo, ("key", self.input_kind, self.part)
        k = memo.get(mk)
        if k is None:
            k = memo[mk] = key_text({"inputKind": self.input_kind, "identity": self.identity, "part": self.part})
        return k


def _refs(d: Delivery, check: str | None = None) -> dict:
    r = {}
    if d.read.digest_rule == "raw-bytes":
        r["digestRule"] = "raw-bytes"
    if check is not None:
        r["check"] = check
    return r


def dispositions_by_identity(ds: list[Disposition]) -> dict:
    """``{identity JSON -> {disposition, reason, deliveries}}``: the judgement of the identity is its first delivery that is not a
    redelivery (which delivery is the duplicate depends on arrival order and is not compared, spec 452); ``deliveries`` lists every
    delivery's ``[disposition, reason]`` sorted."""
    groups: dict[str, list[Disposition]] = {}
    for d in ds:
        groups.setdefault(d.key, []).append(d)
    out = {}
    for k in sorted(groups):
        g = groups[k]
        if len(g) == 1:   # the common case, the same entry as below without the scans
            d = g[0]
            entry = {"disposition": d.disposition, "reason": d.reason, "deliveries": [[d.disposition, d.reason]]}
            if d.refs:
                entry["refs"] = dict(d.refs)
            out[k] = entry
            continue
        main = next((d for d in g if (d.disposition, d.reason) != ("duplicate", "identical-redelivery")), g[0])
        entry = {"disposition": main.disposition, "reason": main.reason,
                 "deliveries": sorted([d.disposition, d.reason] for d in g)}
        if main.refs:
            entry["refs"] = dict(main.refs)
        out[k] = entry
    return out


# --- occupancy claims (S2-S4) ------------------------------------------------------------------------------------------------


def _business(value: dict) -> bytes:
    return canon.canonical_bytes({k: v for k, v in value.items() if k not in ("recordTime", "errorDeclaration", "bridgeReceivedAt")})


@dataclass
class OccDelivery:
    disposition: Disposition
    occ: inputs.Occupancy
    content: bytes
    declaration: bytes | None


@dataclass
class OccIdentity:
    source: str
    event_id: str
    stored: list = field(default_factory=list)      # OccDelivery, admitted and judged
    conflict: bool = False                           # content conflict (identity-conflict)
    correction_conflict: bool = False                # two different declarations

    def originals(self):
        """The stored business variants; with a content conflict the order-independent representative is the smallest by bytes."""
        return sorted(self.stored, key=lambda s: s.content)

    @property
    def occ(self) -> inputs.Occupancy:
        return self.originals()[0].occ

    def declaration(self) -> dict | None:
        """The applied declaration: present on a stored delivery and not in a correction conflict."""
        if self.correction_conflict:
            return None
        for s in self.stored:
            if s.declaration is not None:
                return s.occ.errorDeclaration
        return None


@dataclass
class Visit:
    """A visit claim of either mode, with everything the bodies need."""
    ref: dict
    station: str
    unit_id: str
    visit_seq: object
    enter: int
    leave: int | None = None
    attribution_end: int | None = None
    retraction: dict | None = None
    reasons: set = field(default_factory=set)
    valid_arrival: bool = True
    overlap: bool = False
    # plc-stamped: per equipment [start, end) (end None while open; bound by the change snapshot when missing-visit-close)
    per_equipment: dict = field(default_factory=dict)

    @property
    def closed(self) -> bool:
        return self.leave is not None

    def window(self, tol_e: int, tol_l: int) -> tuple[int, float]:
        if self.leave is not None:
            return self.enter - tol_e, self.leave + tol_l
        return self.enter - tol_e, (self.attribution_end if self.attribution_end is not None else INF)

    def actual(self) -> tuple[int, float]:
        if self.leave is not None:
            return self.enter, self.leave
        return self.enter, (self.attribution_end if self.attribution_end is not None else INF)


def visits_of(station: str, idents: dict, profile: dict, w1) -> list[Visit]:
    """S4, S5 (retraction part), S7 and ``attributionEnd`` for one read point, from the stored occupancy identities of that station."""
    enter_step, leave_step = profile["enter"]["bizStep"], profile["leave"]["bizStep"]
    visits: dict[tuple, Visit] = {}
    for (source, eid), ident in idents.items():
        if not ident.stored:
            continue
        o = ident.occ
        if o.bizStep != enter_step:
            continue
        v = Visit({"mode": "occupancy-join", "source": source, "arrivalEventId": eid}, station, o.unitId, o.visitSeq, o.eventTime)
        decl = ident.declaration()
        if decl is not None:
            v.retraction = {"reason": "error-declaration", "errorReason": decl.get("reason")}
        elif ident.conflict:
            v.retraction = {"reason": "input-conflict", "errorReason": None}
        if ident.correction_conflict:
            v.reasons.add("correction-conflict")
        v.valid_arrival = decl is None and not ident.conflict and not ident.correction_conflict
        visits[(source, eid)] = v
    claims: dict[tuple, list] = {}
    for (source, eid), ident in idents.items():
        if not ident.stored:
            continue
        o = ident.occ
        if o.bizStep != leave_step or o.arrivalEventId is None:
            continue
        target = (source, o.arrivalEventId)
        if target not in visits:
            continue   # pending leave claim: waits for its arrival
        v = visits[target]
        if ident.conflict:
            # a departure re-sent with different content: input conflict, its leave claim withdrawn (spec 298)
            if v.retraction is None:
                v.retraction = {"reason": "input-conflict", "errorReason": None}
            continue
        if ident.correction_conflict:
            v.reasons.add("correction-conflict")
        if ident.declaration() is not None:
            continue   # an error declaration on a departure cancels that leave claim
        claims.setdefault(target, []).append((eid, o.eventTime))
    for target, cs in claims.items():
        leaves = {t for _, t in cs}
        if len(leaves) == 1:
            visits[target].leave = leaves.pop()
        else:
            visits[target].reasons.add("correction-conflict")
    out = sorted(visits.values(), key=lambda v: (v.enter, key_text(v.ref)))
    # S7: a decided overlap between two visits with valid arrival and valid leave, by actual [enter, leave)
    closed = [v for v in out if v.valid_arrival and v.leave is not None and v.retraction is None]
    for i, a in enumerate(closed):
        for b in closed[i + 1:]:
            if a.enter < b.leave and b.enter < a.leave:
                for x in (a, b):
                    x.overlap = True
    for v in out:
        if getattr(v, "overlap", False) and v.retraction is None:
            v.retraction = {"reason": "overlap", "errorReason": None}
    # attributionEnd of an open visit: the enter of the earliest later non-retracted arrival at the read point
    for v in out:
        if v.leave is None:
            later = [x.enter for x in out if x.retraction is None and x.enter > v.enter]
            v.attribution_end = min(later) if later else None
    return out


# --- sessions (S8) ------------------------------------------------------------------------------------------------------------


@dataclass
class DeviceSessions:
    gaps: list = field(default_factory=list)       # [from, to) seq-gap and session-down intervals
    down: list = field(default_factory=list)       # [from, to) session-down intervals
    unknown: list = field(default_factory=list)    # [from, to) session-ambiguous intervals
    definition_changed: set = field(default_factory=set)   # (edge, birthPublishTs, bdSeq)


def sessions_of(events: list, last_obs_before, w1) -> DeviceSessions:
    """``events``: the accepted session events of one device. ``last_obs_before(t)`` gives the device's last valid observation time
    before ``t`` (None when there is none). Open intervals end at ``w1``."""
    ds = DeviceSessions()
    ups = sorted(p.get("at") for e in events if e.kind == "session-up" for p in [e.payload or {}] if p.get("at") is not None)

    def next_up(t):
        i = bisect.bisect_right(ups, t)
        if i < len(ups):
            return ups[i]
        return w1 if w1 is not None else INF

    for e in events:
        p = e.payload or {}
        if e.kind == "seq-gap" and p.get("from") is not None and p.get("to") is not None:
            ds.gaps.append((p["from"], p["to"]))
        elif e.kind == "session-down" and p.get("from") is not None:
            iv = (p["from"], next_up(p["from"]))
            ds.gaps.append(iv)
            ds.down.append(iv)
        elif e.kind == "session-ambiguous" and p.get("detectedEventTime") is not None:
            t = p["detectedEventTime"]
            start = last_obs_before(t)
            ds.unknown.append((start if start is not None else t, next_up(t)))
        elif e.kind == "definition-changed":
            ds.definition_changed.add((e.edge, e.birthPublishTs, e.bdSeq))
    return ds


def _inside(t, intervals) -> bool:
    return any(a <= t < b for a, b in intervals)


def _overlaps(intervals, lo, hi) -> list:
    return [(a, b) for a, b in intervals if a < hi and b > lo and lo < hi]


# --- samples ------------------------------------------------------------------------------------------------------------------


def vkey(value):
    if isinstance(value, bool):
        return ("b", value)
    if isinstance(value, float):
        return ("n", value)
    return ("s", value)


def finite(value) -> bool:
    return not (isinstance(value, float) and (math.isnan(value) or math.isinf(value)))


def quality_class(code, uncertain_input: str) -> str:
    """``good``, ``bad`` (raw severity bits), ``uncertain-accepted`` or ``uncertain-rejected``."""
    if inputs.is_bad(code):
        return "bad"
    if inputs.is_uncertain(code):
        return "uncertain-accepted" if uncertain_input == "propagate" else "uncertain-rejected"
    return "good"


@dataclass
class Sample:
    sid: dict
    skey: str
    device: str
    member: inputs.Member
    message: inputs.Message
    admitted: bool
    mapping: object                 # rules.Member or None
    qclass: str
    conflict: bool = False          # measurement conflict
    row: dict | None = None
    candidate: object = None        # the quality-blind candidate Visit (or None)
    memo: dict = field(default_factory=dict, repr=False, compare=False)   # its delivery's memo

    @property
    def ts(self):
        return self.member.sourceTs

    @property
    def mid(self) -> tuple:
        return (self.device, self.member.metric, self.member.sourceTs, vkey(self.member.value), self.member.uaStatusCode)

    def observation(self) -> dict:
        m = self.member
        return inputs.measurement_id(self.device, m.metric, m.sourceTs, m.value, m.uaStatusCode)

    def observation_bytes(self) -> bytes:
        mk = ("observation", self.member.metric)
        b = self.memo.get(mk)
        if b is None:
            b = self.memo[mk] = canon.canonical_bytes(self.observation())
        return b

    @property
    def accepted_quality(self) -> bool:
        return self.qclass in ("good", "uncertain-accepted")


# --- the evaluation of one cut -----------------------------------------------------------------------------------------------


@dataclass
class Result:
    work_records: dict              # key JSON -> body
    samples: dict                   # key JSON -> body
    dispositions: list              # Disposition
    visits: dict                    # station -> [Visit]
    quarantine: list                # Delivery of conflicting inputs
    sample_digests: dict = field(default_factory=dict)   # key JSON -> bodyDigest of the sample row (see row_digest)


class Stage1:
    def __init__(self, rs: RuleSet, definitions: dict, lenient: bool = False):
        self.rs = rs
        self.definitions = dict(definitions)
        self.rules_digest = rs.rules_digest(definitions)
        self.lenient = lenient
        a = rs.attribution
        t = a["temporal"]
        self.O, self.L, self.H = t["outOfOrdernessMs"], t["allowedLatenessMs"], t["horizonMs"]
        self.tolE, self.tolL = t["enterToleranceMs"], t["leaveToleranceMs"]
        self.P = a["coverage"]["expectedPeriodMs"]
        self.max_missing = a["coverage"]["maxMissingPeriods"]
        self.max_bad_share = a["quality"]["maxBadShare"]
        self.uncertain = a["quality"]["uncertainInput"]
        self.mode = a["identity"]["mode"]
        self.cross_check = a["identity"].get("crossCheck", "none")
        self.namespace = a["identity"].get("namespace")
        self.by_metric = {(m.equipment_path, m.metric): m for m in rs.members.values()}
        self.catalog = {m.equipment_path for m in rs.members.values()}
        self.identity_entries = {e["equipmentPath"]: e for e in a["identity"].get("members", [])}

    # -- S1 ------------------------------------------------------------------------------------------------------------------

    def station_of(self, path: str):
        return station_of(self.rs.stations, path)

    def known_device(self, device: str) -> bool:
        """S1's catalog: the equipment the rule's members resolve to (under a station), with a pinned definition. The spec text
        names no catalog; a device without any rule member (nutrunner2 or station3 under the plc rule) is unknown-device."""
        return device in self.catalog and device in self.definitions

    def occupancy_check(self, o: inputs.Occupancy):
        """S1 for an occupancy event: (disposition, reason, check) of a rejection, None when it passes."""
        p = self.rs.occupancy
        if o.source not in p["sources"]:
            return ("rejected", "schema-invalid", "sources")
        if not self.rs.unit_id_matches(o.unitId):
            return ("rejected", "schema-invalid", "identity.pattern")
        if o.bizStep not in (p["enter"]["bizStep"], p["leave"]["bizStep"]):
            return ("rejected", "schema-invalid", "bizStep")
        if o.errorDeclaration is not None and o.errorDeclaration.get("reason") not in p["correction"]["reasons"]:
            return ("rejected", "schema-invalid", "correction.reasons")
        if o.readPoint not in self.rs.stations:
            return ("rejected", "unknown-readpoint", None)
        return None

    # -- the evaluation ------------------------------------------------------------------------------------------------------

    def evaluate(self, deliveries: list[Delivery], admission, w1: dict) -> Result:
        """``admission(cut, station)``: the ``admissionW`` the cut log records for that station at that cut (None before the first
        progress); ``w1``: station -> the ``W1`` of the evaluated cut."""
        order = sorted(deliveries, key=contract_key)
        disps: list[Disposition] = []
        quarantine: list[Delivery] = []
        occ: dict[str, dict] = {s: {} for s in self.rs.stations}
        sessions: dict[str, list] = {}
        session_seen: dict[bytes, list] = {}
        messages: list[tuple[Delivery, inputs.Message]] = []

        for d in order:
            r = d.read
            if d.topic == OCCUPANCY:
                self._occupancy(d, occ, admission, disps, quarantine)
            elif d.topic == SESSIONS:
                ident = identity_of(d)
                if r.rejected:
                    disps.append(Disposition(d, SESSIONS, ident or d.origin, "event", "rejected", "schema-invalid", _refs(d, r.check)))
                    continue
                s = r.parsed
                if not self.known_device(s.device):
                    disps.append(Disposition(d, SESSIONS, ident, "event", "rejected", "unknown-device", _refs(d)))
                    continue
                kb = canon.canonical_bytes(ident)
                seen = session_seen.setdefault(kb, [])
                disp = Disposition(d, SESSIONS, ident, "event", "accepted", "accepted", _refs(d))
                if any(x.delivery.read.payload_digest != r.payload_digest for x in seen):
                    for x in seen + [disp]:
                        x.disposition, x.reason = "conflict", "identity-conflict"
                    quarantine.append(d)
                elif any(x.disposition == "conflict" for x in seen):
                    disp.disposition, disp.reason = "conflict", "identity-conflict"
                elif seen:
                    disp.disposition, disp.reason = "duplicate", "identical-redelivery"
                seen.append(disp)
                disps.append(disp)
            elif d.topic == MESSAGES:
                ident = identity_of(d)
                if r.rejected:
                    disps.append(Disposition(d, MESSAGES, ident or d.origin, "message", "rejected", "schema-invalid", _refs(d, r.check)))
                    continue
                m = r.parsed
                if not self.known_device(m.device):
                    disps.append(Disposition(d, MESSAGES, ident, "message", "rejected", "unknown-device", _refs(d)))
                    continue
                if all(x.sourceTs is None for x in m.members):
                    disps.append(Disposition(d, MESSAGES, ident, "message", "rejected", "no-source-timestamp", _refs(d)))
                    continue
                if m.definitionSha256 != self.definitions[m.device].get("sha256"):
                    disps.append(Disposition(d, MESSAGES, ident, "message", "rejected", "definition-mismatch", _refs(d)))
                    continue
                messages.append((d, m))
        for disp in disps:
            if disp.delivery.topic == SESSIONS and disp.disposition == "accepted":
                s = disp.delivery.read.parsed
                sessions.setdefault(s.device, []).append(s)

        # samples: one per member of an accepted message; a definition-changed session rejects its messages
        changed = {(s.edge, s.device, s.birthPublishTs, s.bdSeq) for ss in sessions.values() for s in ss if s.kind == "definition-changed"}
        samples_by_station: dict[str, list[Sample]] = {s: [] for s in self.rs.stations}
        sample_seen: dict[str, list] = {}      # sampleId -> [(content, Disposition)]
        sample_of: dict[str, Sample] = {}
        station_by_device: dict[str, object] = {}
        for d, m in messages:
            if (m.edge, m.device, m.birthPublishTs, m.bdSeq) in changed:
                disps.append(Disposition(d, MESSAGES, inputs.message_id(m), "message", "rejected", "definition-mismatch", _refs(d)))
                continue
            station = station_by_device.get(m.device)
            if station is None:
                station = station_by_device[m.device] = self.station_of(m.device)
            aw = admission(d.cut, station)
            refs = _refs(d)   # the same for every member of the message (a refs dict is never changed in place)
            for metric, check in m.rejected_members:
                disps.append(Disposition(d, MESSAGES, inputs.sample_id(m, metric), "member:" + metric, "rejected", "schema-invalid", _refs(d, check)))
            for mem in m.members:
                sid = inputs.sample_id(m, mem.metric)
                skey = d.memo.get(("sample", mem.metric))
                if skey is None:
                    skey = d.memo[("sample", mem.metric)] = key_text(sid)
                admitted = mem.sourceTs is None or aw is None or mem.sourceTs >= aw - self.H
                disp = Disposition(d, MESSAGES, sid, "member:" + mem.metric, "accepted", "accepted", refs)
                content = (vkey(mem.value), mem.uaStatusCode, mem.sourceTs, mem.serverTs)
                prior = sample_seen.get(skey)
                if prior is None:
                    prior = sample_seen[skey] = []
                    seen = prior   # empty
                else:
                    seen = [x for x in prior if x[1].disposition != "replay-required"]
                if not admitted:
                    disp.disposition, disp.reason = "replay-required", "beyond-horizon"
                elif any(c != content for c, _ in seen):
                    for _, x in seen:
                        x.disposition, x.reason = "conflict", "identity-conflict"
                    disp.disposition, disp.reason = "conflict", "identity-conflict"
                    quarantine.append(d)
                elif any(x.disposition == "conflict" for _, x in seen):
                    disp.disposition, disp.reason = "conflict", "identity-conflict"
                elif seen:
                    disp.disposition, disp.reason = "duplicate", "identical-redelivery"
                sample_seen[skey].append((content, disp))
                disps.append(disp)
                if skey not in sample_of or (not sample_of[skey].admitted and admitted):
                    if skey in sample_of:
                        samples_by_station[station].remove(sample_of[skey])
                    smp = Sample(sid, skey, m.device, mem, m, admitted, self.by_metric.get((m.device, mem.metric)),
                                 quality_class(mem.uaStatusCode, self.uncertain), memo=d.memo)
                    sample_of[skey] = smp
                    samples_by_station[station].append(smp)
        # measurement conflicts: the same (device, metric, sourceTs) with another value or status code (spec 263)
        for station, ss in samples_by_station.items():
            triples: dict[tuple, set] = {}
            for s in ss:
                if s.admitted and s.ts is not None:
                    triples.setdefault((s.device, s.member.metric, s.ts), set()).add((vkey(s.member.value), s.member.uaStatusCode))
            bad = {t for t, vs in triples.items() if len(vs) > 1}
            for s in ss:
                if s.admitted and s.ts is not None and (s.device, s.member.metric, s.ts) in bad:
                    s.conflict = True
            if bad:
                for disp in disps:
                    if disp.delivery.topic == MESSAGES and disp.part.startswith("member:") and disp.disposition in ("accepted", "duplicate"):
                        m = disp.delivery.read.parsed
                        mem = next((x for x in m.members if "member:" + x.metric == disp.part), None)
                        if mem is not None and (m.device, mem.metric, mem.sourceTs) in bad:
                            disp.disposition, disp.reason = "conflict", "measurement-conflict"
                            if disp.delivery not in quarantine:
                                quarantine.append(disp.delivery)

        # per station: visits, sessions, attribution, bodies
        work: dict[str, dict] = {}
        rows: dict[str, dict] = {}
        digests: dict[str, str] = {}
        visits_out: dict[str, list] = {}
        for station in self.rs.stations:
            W = w1.get(station)
            ss = samples_by_station[station]
            obs_times: dict[str, list] = {}
            for s in ss:
                if s.admitted and s.ts is not None and not s.conflict:
                    obs_times.setdefault(s.device, []).append(s.ts)
            for v in obs_times.values():
                v.sort()
            dev_sessions = {}
            devices = {x for x in self.definitions if self.station_of(x) == station} | {x for x in sessions if self.station_of(x) == station}
            for dev in sorted(devices):
                times = obs_times.get(dev, [])

                def before(t, times=times):
                    i = bisect.bisect_left(times, t)
                    return times[i - 1] if i > 0 else None
                dev_sessions[dev] = sessions_of(sessions.get(dev, []), before, W)
            occ_visits = visits_of(station, occ[station], self.rs.occupancy, W)
            if self.mode == "plc-stamped":
                vs = self.plc_visits_of(station, ss, W, disps)
                if self.cross_check != "occupancy":
                    occ_visits = []
            else:
                vs = occ_visits
            self.attribute(station, ss, vs, dev_sessions, occ_visits)
            by_candidate: dict[int, list] = {}
            ref_texts: dict[int, str] = {}
            for s in ss:
                rows[s.skey] = s.row
                digests[s.skey] = row_digest(s, ref_texts)
                if s.candidate is not None:
                    by_candidate.setdefault(id(s.candidate), []).append(s)
            for v in vs:
                body = self.work_record_body(v, by_candidate.get(id(v), []), dev_sessions, W)
                work[key_text(v.ref)] = body
            visits_out[station] = vs
        return Result(work, rows, disps, visits_out, quarantine, digests)

    # -- occupancy sweep -------------------------------------------------------------------------------------------------------

    def _occupancy(self, d: Delivery, occ: dict, admission, disps: list, quarantine: list) -> None:
        r = d.read
        ident = identity_of(d)
        if r.rejected:
            disps.append(Disposition(d, OCCUPANCY, ident or d.origin, "event", "rejected", "schema-invalid", _refs(d, r.check)))
            return
        o: inputs.Occupancy = r.parsed
        part = "errorDeclaration" if o.errorDeclaration is not None else "event"
        bad = self.occupancy_check(o)
        if bad is not None:
            disps.append(Disposition(d, OCCUPANCY, ident, part, bad[0], bad[1], _refs(d, bad[2])))
            return
        station = o.readPoint
        idents = occ[station]
        aw = admission(d.cut, station)
        arriving = o.bizStep == self.rs.occupancy["enter"]["bizStep"]
        target = (o.source, o.eventId) if arriving else (o.source, o.arrivalEventId)
        # admission is judged against the visit as it stood at the last committed progress: the inputs of earlier cuts
        committed = {k: OccIdentity(x.source, x.event_id, [y for y in x.stored if y.disposition.delivery.cut < d.cut], x.conflict, x.correction_conflict)
                     for k, x in idents.items()}
        for k, x in committed.items():
            x.conflict = len({y.content for y in x.stored}) > 1
            x.correction_conflict = len({y.declaration for y in x.stored if y.declaration is not None}) > 1
        current = {self._ref_key(v.ref): v for v in visits_of(station, committed, self.rs.occupancy, None)}
        visit = current.get(target)
        disp = Disposition(d, OCCUPANCY, ident, part, "accepted", "accepted", _refs(d))
        # S3 admission against the cut's admissionW
        if aw is not None:
            if visit is not None and (o.errorDeclaration is not None or not arriving):
                expires = (visit.leave if visit.leave is not None else visit.enter) + self.H + self.L
            else:
                expires = o.eventTime + self.H + self.L
            if aw >= expires:
                disp.disposition = "replay-required"
                disp.reason = "correction-beyond-horizon" if o.errorDeclaration is not None else "beyond-horizon"
                disps.append(disp)
                return
        content = _business(r.value)
        declaration = canon.canonical_bytes(o.errorDeclaration) if o.errorDeclaration is not None else None
        rec = idents.setdefault((o.source, o.eventId), OccIdentity(o.source, o.eventId))
        entry = OccDelivery(disp, o, content, declaration)
        if any(s.content != content for s in rec.stored):
            rec.conflict = True
            for s in rec.stored + [entry]:
                s.disposition.disposition, s.disposition.reason = "conflict", "identity-conflict"
                if s.disposition.delivery not in quarantine:
                    quarantine.append(s.disposition.delivery)
        elif rec.conflict:
            disp.disposition, disp.reason = "conflict", "identity-conflict"
            quarantine.append(d)
        else:
            decls = {s.declaration for s in rec.stored if s.declaration is not None}
            plain = any(s.declaration is None for s in rec.stored)
            if declaration is None:
                if plain:
                    disp.disposition, disp.reason = "duplicate", "identical-redelivery"
            elif declaration in decls:
                disp.disposition, disp.reason = "duplicate", "identical-redelivery"
            elif decls:
                rec.correction_conflict = True
                for s in [x for x in rec.stored if x.declaration is not None] + [entry]:
                    s.disposition.disposition, s.disposition.reason = "conflict", "correction-conflict"
                    if s.disposition.delivery not in quarantine:
                        quarantine.append(s.disposition.delivery)
            if disp.disposition == "accepted" and aw is not None and visit is not None and visit.leave is not None \
                    and aw >= visit.leave + self.L:
                disp.reason = "late-after-settlement"
        rec.stored.append(entry)
        disps.append(disp)

    @staticmethod
    def _ref_key(ref: dict) -> tuple:
        return (ref["source"], ref["arrivalEventId"])

    # -- S9 attribution --------------------------------------------------------------------------------------------------------

    def _candidates(self, t, windows, starts):
        i = bisect.bisect_right(starts, t)
        return [v for (lo, hi, v) in windows[:i] if lo <= t < hi]

    @staticmethod
    def _pick(t, cands):
        """S9's choice among candidates: exactly one non-retracted -> it; none non-retracted -> the retracted one whose actual
        interval contains t (exactly one); otherwise ambiguous. Returns (visit or None, reason or None)."""
        if not cands:
            return None, "no-occupancy"
        live = [v for v in cands if v.retraction is None]
        if len(live) == 1:
            return live[0], None
        if len(live) > 1:
            return None, "ambiguous"
        inside = [v for v in cands if v.actual()[0] <= t < v.actual()[1]]
        if len(inside) == 1:
            return inside[0], None
        return None, "ambiguous"

    def active_of(self, s: Sample):
        act = self.rs.activity_of(s.device)
        if act is None:
            return "unknown"
        mem = next((x for x in s.message.members if x.metric == act.metric), None)
        if mem is None or not isinstance(mem.value, bool):
            return "unknown"
        if quality_class(mem.uaStatusCode, self.uncertain) not in ("good", "uncertain-accepted"):
            return "unknown"
        return mem.value

    def attribute(self, station: str, ss: list, vs: list, dev_sessions: dict, occ_visits: list) -> None:
        if self.mode == "plc-stamped":
            return self._attribute_plc(station, ss, vs, dev_sessions, occ_visits)
        windows = sorted(((*v.window(self.tolE, self.tolL), v) for v in vs), key=lambda w: (w[0], key_text(w[2].ref)))
        starts = [w[0] for w in windows]
        for s in sorted(ss, key=lambda x: (-1 if x.ts is None else x.ts, x.skey)):
            state, reason, to = "unattributed", None, None
            if not s.admitted:
                state, reason = "replay-required", "beyond-horizon"
            elif s.ts is None:
                reason = "no-source-timestamp"
            elif s.conflict:
                state, reason = "quarantined", "measurement-conflict"
            else:
                blind, blind_reason = self._pick(s.ts, self._candidates(s.ts, windows, starts))
                s.candidate = blind
                sess = dev_sessions.get(s.device, DeviceSessions())
                if not finite(s.member.value):
                    reason = "non-finite"
                    s.candidate = None
                elif s.qclass in ("bad", "uncertain-rejected"):
                    reason = "bad-quality"
                elif _inside(s.ts, sess.down):
                    reason = "outside-session"
                    s.candidate = None
                elif _inside(s.ts, sess.unknown):
                    reason = "session-ambiguous"
                    s.candidate = None
                elif blind is not None:
                    state, to = "attributed", blind.ref
                else:
                    reason = blind_reason
            s.row = {"sourceTs": s.ts, "state": state, "reason": reason, "active": self.active_of(s), "attributedTo": to}

    def _attribute_plc(self, station, ss, vs, dev_sessions, occ_visits):
        for s in sorted(ss, key=lambda x: (-1 if x.ts is None else x.ts, x.skey)):
            state, reason, to = "unattributed", None, None
            sess = dev_sessions.get(s.device, DeviceSessions())
            blind = None
            if not s.admitted:
                state, reason = "replay-required", "beyond-horizon"
            elif s.ts is None:
                reason = "no-source-timestamp"
            elif s.conflict:
                state, reason = "quarantined", "measurement-conflict"
            elif s.device in self.identity_entries and not self._coherent_sample(s):
                reason = "incoherent-snapshot"
            else:
                found = [v for v in vs if s.device in v.per_equipment
                         and v.per_equipment[s.device][0] <= s.ts < (v.per_equipment[s.device][1] if v.per_equipment[s.device][1] is not None else INF)]
                blind = found[0] if len(found) == 1 else None
                if not finite(s.member.value):
                    reason = "non-finite"
                    blind = None
                elif s.qclass in ("bad", "uncertain-rejected"):
                    reason = "bad-quality"
                elif _inside(s.ts, sess.down):
                    reason, blind = "outside-session", None
                elif _inside(s.ts, sess.unknown):
                    reason, blind = "session-ambiguous", None
                elif blind is not None:
                    state, to = "attributed", blind.ref
                    if self.cross_check == "occupancy":
                        for ov in occ_visits:
                            lo, hi = ov.enter, (ov.leave if ov.leave is not None else INF)
                            if ov.retraction is None and lo <= s.ts < hi and ov.unit_id != blind.unit_id:
                                reason = "identity-mismatch"
                                break
                else:
                    reason = "ambiguous" if len(found) > 1 else "no-occupancy"
            s.candidate = blind
            s.row = {"sourceTs": s.ts, "state": state, "reason": reason, "active": self.active_of(s), "attributedTo": to}

    # -- P1-P2 ------------------------------------------------------------------------------------------------------------------

    def _identity_members(self, device: str) -> dict:
        e = self.identity_entries.get(device)
        if e is None:
            return {}
        return {"unit": self.rs.members[e["unitMemberRef"]].metric, "visit": self.rs.members[e["visitMemberRef"]].metric,
                "present": self.rs.members[e["presentMemberRef"]].metric}

    def _coherent_sample(self, s: Sample) -> bool:
        """``_coherent`` of the sample's message, a function of that delivered message alone: kept on its delivery."""
        c = s.memo.get("coherent")
        if c is None:
            c = s.memo["coherent"] = self._coherent(s.device, s.message)
        return c

    def _coherent(self, device: str, m: inputs.Message) -> bool:
        names = [x.metric for x in self.rs.members.values() if x.equipment_path == device and x.role in
                 (MEASUREMENT, ACTIVITY, IDENTITY_UNIT, IDENTITY_VISIT, IDENTITY_PRESENT)]
        by = {x.metric: x for x in m.members}
        ts = set()
        for n in names:
            x = by.get(n)
            if x is None or x.sourceTs is None:
                return False
            ts.add(x.sourceTs)
        return len(ts) == 1

    def plc_visits_of(self, station: str, ss: list, W, disps: list) -> list[Visit]:
        """P1-P2: per identified equipment of the station, the coherent snapshots ordered by sourceTs give each visitId's start (the
        earliest present=true) and end (the earliest later present=false with the same id); an id change while present stays true
        leaves the earlier visit open with missing-visit-close."""
        snaps: dict[str, dict] = {}   # device -> {ts: (unit, visitId, present)}
        for s in ss:
            if not s.admitted or s.conflict or s.ts is None or s.device not in self.identity_entries:
                continue
            if s.device not in snaps:
                snaps[s.device] = {}
            if s.ts in snaps[s.device]:
                continue
            if not self._coherent_sample(s):
                continue
            im = self._identity_members(s.device)
            by = {x.metric: x for x in s.message.members}
            u, vi, pr = by[im["unit"]].value, by[im["visit"]].value, by[im["present"]].value
            snaps[s.device][s.ts] = (u, vi, pr)
        per: dict[str, dict] = {}        # visitId -> {device -> [start, end, flags]}
        units: dict[str, set] = {}
        for dev, by_ts in snaps.items():
            current = None
            ended: set = set()
            for t in sorted(by_ts):
                u, vi, pr = by_ts[t]
                if pr is True:
                    units.setdefault(vi, set()).add(u)
                    if vi in ended:
                        per[vi][dev]["inconsistent"] = True
                        continue
                    if current is not None and current != vi:
                        e = per[current][dev]
                        if e["end"] is None:
                            e["change"] = t
                    if vi not in per or dev not in per[vi]:
                        per.setdefault(vi, {})[dev] = {"start": t, "end": None, "change": None, "inconsistent": False}
                    current = vi
                elif pr is False:
                    if vi in per and dev in per[vi] and per[vi][dev]["end"] is None and per[vi][dev]["change"] is None:
                        per[vi][dev]["end"] = t
                        ended.add(vi)
                    if current == vi:
                        current = None
        out = []
        for vi, devs in per.items():
            starts = {e["start"] for e in devs.values()}
            ends = {e["end"] for e in devs.values()}
            enter = min(starts)
            first_dev = min(devs, key=lambda d: (devs[d]["start"], d))
            unit = snaps[first_dev][devs[first_dev]["start"]][0]
            v = Visit({"mode": "plc-stamped", "namespace": self.namespace, "station": station, "visitId": vi}, station, unit, None, enter)
            if len(starts) > 1 or len(ends) > 1 or any(e["inconsistent"] for e in devs.values()):
                v.reasons.add("inconsistent-visit-boundary")
            changes = [e["change"] for e in devs.values() if e["change"] is not None]
            if changes:
                v.reasons.add("missing-visit-close")
                v.attribution_end = min(changes)
            elif len(ends) == 1 and None not in ends:
                v.leave = ends.pop()
            if len(units.get(vi, set())) > 1:
                v.retraction = {"reason": "input-conflict", "errorReason": None}
                v.valid_arrival = False
            for dev, e in devs.items():
                v.per_equipment[dev] = (e["start"], e["end"] if e["end"] is not None else e["change"])
            out.append(v)
        conflicted = {vi for vi, us in units.items() if len(us) > 1}
        if conflicted:
            for disp in disps:
                if disp.delivery.topic != MESSAGES or not disp.part.startswith("member:"):
                    continue
                m = disp.delivery.read.parsed
                im = self._identity_members(m.device)
                if not im or disp.part[len("member:"):] not in im.values():
                    continue
                by = {x.metric: x for x in m.members}
                if by.get(im["visit"]) is not None and by[im["visit"]].value in conflicted and disp.disposition == "accepted":
                    disp.disposition, disp.reason = "conflict", "identity-conflict"
        return sorted(out, key=lambda v: (v.enter, key_text(v.ref)))

    # -- S5, S6: coverage and the work record body ---------------------------------------------------------------------------

    def interval_j(self, v: Visit, W) -> tuple[int, int]:
        if v.leave is not None:
            return v.enter, v.leave
        bounds = [v.enter + self.H]
        if W is not None:
            bounds.append(W)
        if v.attribution_end is not None:
            bounds.append(v.attribution_end)
        return v.enter, min(bounds)

    def slots(self, lo: int, hi: int) -> int:
        return 0 if hi <= lo else -(-(hi - lo) // self.P)

    def coverage(self, v: Visit, ss: list, dev_sessions: dict, W) -> dict:
        """S6 for one visit: members[], equipmentEvidence[], samplesDigest, coverage and the coverage-derived reasons."""
        lo, hi = self.interval_j(v, W)
        n = self.slots(lo, hi)
        station = v.station
        obs: dict[tuple, Sample] = {}
        for s in ss:
            if s.candidate is v and s.ts is not None and lo <= s.ts < hi and s.mapping is not None \
                    and s.row["state"] in ("attributed", "unattributed") and (s.row["state"] == "attributed" or s.row["reason"] == "bad-quality"):
                obs.setdefault(s.mid, s)
        by_member: dict[str, list] = {}
        for s in obs.values():
            by_member.setdefault(s.mapping.mapping_id, []).append(s)
        P = self.P

        def slot(t):
            return (t - lo) // P

        member_stats = {}
        for mid, m in self.rs.members.items():
            if m.station != station:
                continue
            os = by_member.get(mid, [])
            observed = {slot(s.ts) for s in os}
            bad = {slot(s.ts) for s in os if s.qclass == "bad"}
            member_stats[mid] = (len(observed), n - len(observed), len(bad))
        reasons = set()
        members = []
        required = self.rs.required_members_of(station)
        listed = sorted(set(required) | {mid for mid in by_member if self.rs.members[mid].role == MEASUREMENT})
        for mid in listed:
            m = self.rs.members[mid]
            observed, missing, bad = member_stats[mid]
            entry = {"mappingId": mid, "metric": m.metric, "equipmentPath": m.equipment_path, "valueType": m.value_type,
                     "observedPeriods": observed, "missingPeriods": missing, "badSlots": bad}
            acc = sorted((s for s in by_member.get(mid, []) if s.accepted_quality),
                         key=lambda s: (s.ts, tuple(s.sid[f] for f in ("edge", "device", "birthPublishTs", "metric", "bdSeq", "seq", "publishTs"))))
            vals = [s.member.value for s in acc]
            last = vals[-1] if vals else None
            if m.value_type == "Double":
                nums = [x for x in vals if isinstance(x, float)]
                entry.update({"count": len(vals), "min": min(nums) if nums else None, "max": max(nums) if nums else None, "last": last})
            elif m.value_type == "Boolean":
                entry.update({"count": len(vals), "trueCount": sum(1 for x in vals if x is True), "last": last})
            else:
                entry.update({"count": len(vals), "distinctCount": len({vkey(x) for x in vals}), "last": last})
            members.append(entry)
            if n > 0:
                if mid in required and missing > self.max_missing:
                    reasons.add("missing-periods")
                if mid in required and observed == 0:
                    reasons.add("no-observed-periods")
                if observed and bad / observed > self.max_bad_share:
                    reasons.add("bad-share-exceeded")
        equipment = sorted({m.equipment_path for m in self.rs.members.values() if m.station == station})
        evidence = []
        for q in equipment:
            sess = dev_sessions.get(q, DeviceSessions())
            gaps = _overlaps(sess.gaps, lo, hi)
            unknown_sessions = _overlaps(sess.unknown, lo, hi)
            act = self.rs.activity_of(q)
            active, inactive, unknown_periods = [], [], 0
            if act is not None and n > 0:
                active, inactive, unknown_periods = self.activity(by_member.get(act.mapping_id, []), lo, hi, unknown_sessions)
                if unknown_periods > self.max_missing:
                    reasons.add("unknown-activity")
            contributing = [s for mid, os in by_member.items() if self.rs.members[mid].equipment_path == q for s in os]
            if not contributing:
                continue
            meas = self.rs.measurements_of(q)
            if meas:
                observed = min(member_stats[x][0] for x in meas)
                missing = max(member_stats[x][1] for x in meas)
                bad = max(member_stats[x][2] for x in meas)
                bad_share = any(member_stats[x][0] and member_stats[x][2] / member_stats[x][0] > self.max_bad_share for x in meas)
            else:
                observed, missing = member_stats[act.mapping_id][0], member_stats[act.mapping_id][1]
                bad, bad_share = 0, False
            if unknown_periods > 0 or unknown_sessions:
                status = "unknown"
            elif missing > self.max_missing or gaps or bad_share:
                status = "incomplete"
            else:
                status = "complete"
            evidence.append({
                "equipmentPath": q,
                "memberRefs": sorted({s.mapping.mapping_id for s in contributing}),
                "activityIntervals": [[a, b] for a, b in active],
                "inactiveIntervals": [[a, b] for a, b in inactive],
                "coverage": {"observedPeriods": observed, "missingPeriods": missing, "badSlots": bad,
                             "unknownActivityPeriods": unknown_periods, "sessionGaps": len(gaps), "status": status},
                "samplesDigest": samples_digest(contributing),
            })
        if evidence:
            extremes = [(e["coverage"]["observedPeriods"], e["coverage"]["missingPeriods"], e["coverage"]["badSlots"]) for e in evidence]
        else:
            # no contributing observation yet: the extremes over the required members (spec 356's parenthetical), or over the
            # station's activity members when it has no measurement member (badSlots 0)
            extremes = [member_stats[x] for x in required] or                 [(member_stats[a.mapping_id][0], member_stats[a.mapping_id][1], 0) for q in equipment for a in [self.rs.activity_of(q)] if a is not None]
        cov = {"expectedPeriodMs": P,
               "observedPeriods": min((x[0] for x in extremes), default=0),
               "missingPeriods": max((x[1] for x in extremes), default=n),
               "badSlots": max((x[2] for x in extremes), default=0),
               "sessionGaps": sum(e["coverage"]["sessionGaps"] for e in evidence)}
        if v.retraction is None and n > 0:
            devices = {q for q in equipment}
            for q in devices:
                sess = dev_sessions.get(q, DeviceSessions())
                if _overlaps(sess.gaps, lo, hi):
                    reasons.add("session-gap")
                if _overlaps(sess.unknown, lo, hi):
                    reasons.add("session-ambiguous")
        return {"members": members, "equipmentEvidence": evidence, "samplesDigest": samples_digest(list(obs.values())),
                "coverage": cov, "reasons": reasons}

    def activity(self, os: list, lo: int, hi: int, unknown_sessions: list):
        """Support intervals of one equipment's activity member inside J = [lo, hi): (active, inactive, unknownActivityPeriods)."""
        P = self.P
        os = sorted(os, key=lambda s: s.ts)
        segs = []   # (start, end, kind)
        for i, s in enumerate(os):
            nxt = os[i + 1].ts if i + 1 < len(os) else INF
            if not s.accepted_quality or not isinstance(s.member.value, bool):
                continue
            a, b = max(s.ts, lo), min(nxt, s.ts + P, hi)
            if a < b:
                segs.append((a, b, s.member.value))
        # an unknown session interval is unknown whatever supports it
        cut = []
        for a, b, k in segs:
            pieces = [(a, b)]
            for ua, ub in unknown_sessions:
                nxt_pieces = []
                for pa, pb in pieces:
                    if ub <= pa or ua >= pb:
                        nxt_pieces.append((pa, pb))
                    else:
                        if pa < ua:
                            nxt_pieces.append((pa, ua))
                        if ub < pb:
                            nxt_pieces.append((ub, pb))
                pieces = nxt_pieces
            cut.extend((pa, pb, k) for pa, pb in pieces)
        cut.sort()
        merged: list = []
        for a, b, k in cut:
            if merged and merged[-1][2] == k and merged[-1][1] == a:
                merged[-1] = (merged[-1][0], b, k)
            else:
                merged.append((a, b, k))
        active = [(a, b) for a, b, k in merged if k is True]
        inactive = [(a, b) for a, b, k in merged if k is False]
        known = sorted((a, b) for a, b, _ in merged)
        n = self.slots(lo, hi)
        unknown = 0
        j = 0
        for i in range(n):
            sa, sb = lo + i * P, min(lo + (i + 1) * P, hi)
            # the slot is fully known when the known intervals cover [sa, sb) without a hole
            while j < len(known) and known[j][1] <= sa:
                j += 1
            t, k = sa, j
            while k < len(known) and known[k][0] <= t and t < sb:
                t = max(t, known[k][1])
                k += 1
            if t < sb:
                unknown += 1
        return active, inactive, unknown

    def work_record_body(self, v: Visit, ss: list, dev_sessions: dict, W) -> dict:
        c = self.coverage(v, ss, dev_sessions, W)
        reasons = set(v.reasons) | c["reasons"]
        if v.leave is None and W is not None and W >= v.enter + self.H:
            reasons.add("missing-departure")
        settled = v.leave is not None and W is not None and W >= v.leave + self.L
        if v.retraction is not None:
            status = "retracted"
        elif reasons:
            status = "insufficient"
        elif settled:
            status = "complete"
        else:
            status = "provisional"
        if self.lenient and status == "provisional":
            status = "complete"
        body = {
            "unitId": v.unit_id,
            "readPoint": v.station,
            "visitSeq": v.visit_seq,
            "status": status,
            "insufficiencyReasons": sorted(reasons),
            "interval": {"enter": v.enter, "leave": v.leave, "attributionEnd": v.attribution_end},
            "members": c["members"],
            "equipmentEvidence": c["equipmentEvidence"],
            "samplesDigest": c["samplesDigest"],
            "coverage": c["coverage"],
            "attributionMode": self.mode,
            "rulesDigest": self.rules_digest,
        }
        if v.retraction is not None:
            body["retraction"] = dict(v.retraction)
        return body


def samples_digest(samples: list) -> str:
    """JCS SHA-256 of the distinct observations sorted by device, metric, sourceTs, then canonical bytes (spec 354, 356)."""
    seen = {}
    for s in samples:
        seen.setdefault(s.observation_bytes(), s)
    ordered = sorted(seen.items(), key=lambda kv: (kv[1].device, kv[1].member.metric, kv[1].member.sourceTs, kv[0]))
    # the canonical form of an array is its elements' canonical forms joined by commas inside brackets (RFC 8785 3.2.2)
    return canon.sha256(b"[" + b",".join(b for b, _ in ordered) + b"]")


def row_digest(s: Sample, ref_texts: dict) -> str:
    """``canon.digest(s.row)``, kept on the sample's delivery while the row's canonical parts stay the same: a row is
    ``{sourceTs, state, reason, active, attributedTo}`` whose canonical form is a function of the canonical form of each part, so
    equal parts (compared with their types, ``attributedTo`` by its canonical text) give the same digest; ``ref_texts`` holds the
    canonical text of each visit reference of the cut."""
    row = s.row
    if row.keys() != {"sourceTs", "state", "reason", "active", "attributedTo"}:
        return canon.digest(row)
    to = row["attributedTo"]
    if to is None:
        to_text = None
    else:
        to_text = ref_texts.get(id(to))
        if to_text is None:
            to_text = ref_texts[id(to)] = key_text(to)
    parts = tuple((type(row[f]), row[f]) for f in ("sourceTs", "state", "reason", "active")) + (to_text,)
    mk = ("row", s.member.metric)
    cached = s.memo.get(mk)
    if cached is not None and cached[0] == parts:
        return cached[1]
    d = canon.digest(row)
    s.memo[mk] = (parts, d)
    return d


def sample_row_body(s: Sample) -> dict:
    return dict(s.row)


def work_record_body(stage: Stage1, v: Visit, ss: list, dev_sessions: dict, W) -> dict:
    return stage.work_record_body(v, ss, dev_sessions, W)
