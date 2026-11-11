"""Stage 2 of the oracle: incident dispositions, impact assessments and affected sets of one cut, from the oracle's own stage-1 rows.

Written from the spec text (Incidents 300-317, the assessment and affected-set bodies 374-404, Stage 2 523-550) and the plan's rule
sheet T1-T10. It stands on the oracle's stage-1 rows of the same cut, so a stage-1 disagreement surfaces once, at stage 1. A deleted
unit row is an absent key; the gate folds the harness's deletes the same way.

* ``classify``: T1 as one sweep in delivery order (cut by cut, ``(identity bytes, payloadDigest)`` inside a cut). The current revision
  of an incident is the highest valid one; the delivered valid revisions, superseded lower ones included, widen the row set (spec 528,
  530).
* ``related``: T4.
* ``judge``: T5-T6 for one unit; ``assessment_body`` (T7), ``set_body`` (T8).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

from forseti_oracle import canon, inputs
from forseti_oracle.inputs import INCIDENTS
from forseti_oracle.rules import RuleSet, station_of
from forseti_oracle.stage1 import Delivery, Disposition, _refs, key_text

AFFECTED, AFFECTED_SETS = "forseti.affected", "forseti.affected-sets"
RELATED, POSSIBLY, UNRELATED = "related", "possibly-related", "unrelated"
DID_NOT_OCCUR, INCORRECT_DATA = "urn:epcglobal:cbv:er:did_not_occur", "urn:epcglobal:cbv:er:incorrect_data"
INCIDENT_FIELDS = ("incidentId", "revision", "stationRef", "equipmentRef", "kind", "detectedAt", "revisionEventTime", "suspectFrom",
                   "suspectTo", "status", "evidenceRef", "source")


def incident_key(d: Delivery):
    ident = inputs.incident_id(d.read.parsed) if d.read.parsed is not None else d.origin
    return (d.cut, canon.canonical_bytes(ident), d.read.payload_digest)


# --- T4 ------------------------------------------------------------------------------------------------------------------------


def related(mode: str, enter: int, leave, suspect_from: int, suspect_to) -> str:
    e, l, f, t = enter, leave, suspect_from, suspect_to
    if mode == "any-overlap":
        if l is not None and t is not None:
            return RELATED if e < t and l > f else UNRELATED
        if l is not None:
            return RELATED if l > f else UNRELATED
        if t is not None and e >= t:
            return UNRELATED
        return RELATED if e >= f else POSSIBLY
    if mode == "fully-within":
        if l is not None and t is not None:
            return RELATED if f <= e and l <= t else UNRELATED
        if f > e:
            return UNRELATED
        if t is not None and e >= t:
            return UNRELATED
        if l is not None and t is None:
            return POSSIBLY
        return POSSIBLY
    if mode == "enter-within":
        return RELATED if f <= e and (t is None or e < t) else UNRELATED
    raise ValueError(f"interval.overlap {mode}")


# --- T1 ------------------------------------------------------------------------------------------------------------------------


@dataclass
class Revision:
    incident: inputs.Incident
    value: dict
    digest: str
    dispositions: list = field(default_factory=list)
    conflict: bool = False


@dataclass
class IncidentState:
    source: str
    incident_id: str
    revisions: dict = field(default_factory=dict)       # revision -> Revision (valid deliveries only, conflicting ones marked)
    first_station: str | None = None

    def valid(self) -> list[Revision]:
        return [r for n, r in sorted(self.revisions.items()) if not r.conflict]

    def current(self) -> Revision | None:
        v = self.valid()
        return v[-1] if v else None


class Stage2:
    def __init__(self, rs: RuleSet, rules_digest: str, evidence_range: dict, lenient: bool = False):
        self.rs = rs
        self.rules_digest = rules_digest
        self.evidence_range = dict(evidence_range)
        a, i = rs.attribution, rs.impact
        self.L = a["temporal"]["allowedLatenessMs"]
        self.P = a["coverage"]["expectedPeriodMs"]
        self.overlap = i["interval"]["overlap"]
        self.match = i["evidence"]["match"]
        self.on_retracted = i["evidence"]["onBasisRetracted"]
        self.on_withdrawn = i["evidence"]["onWithdrawn"]
        self.work_records_ms = i["retention"]["workRecordsMs"]
        self.scope = list(i["incidentScope"])
        self.kinds = list(rs.incident["kinds"])

    def classify(self, deliveries: list[Delivery]) -> tuple[dict, list]:
        """T1 over the incidents delivered through the cut: (incident states by (source, incidentId), dispositions)."""
        states: dict[tuple, IncidentState] = {}
        disps: list[Disposition] = []
        for d in sorted((x for x in deliveries if x.topic == INCIDENTS), key=incident_key):
            r = d.read
            if r.rejected:
                disps.append(Disposition(d, INCIDENTS, d.origin, "event", "rejected", "schema-invalid", _refs(d, r.check)))
                continue
            inc: inputs.Incident = r.parsed
            ident = inputs.incident_id(inc)
            disp = Disposition(d, INCIDENTS, ident, "event", "accepted", "accepted", _refs(d))
            disps.append(disp)
            if inc.kind not in self.kinds:
                disp.disposition, disp.reason, disp.refs = "rejected", "schema-invalid", _refs(d, "kinds")
                continue
            if inc.revision == 1 and inc.revisionEventTime != inc.detectedAt:
                disp.disposition, disp.reason, disp.refs = "rejected", "schema-invalid", _refs(d, "revisionEventTime")
                continue
            if inc.equipmentRef not in self.scope or inc.stationRef not in self.rs.stations \
                    or station_of(self.rs.stations, inc.equipmentRef) != inc.stationRef:
                disp.disposition, disp.reason = "rejected", "outside-incident-scope"
                continue
            st = states.setdefault((inc.source, inc.incidentId), IncidentState(inc.source, inc.incidentId))
            rev = st.revisions.get(inc.revision)
            if rev is not None:
                if rev.conflict or rev.digest != r.payload_digest:
                    rev.conflict = True
                    for x in rev.dispositions + [disp]:
                        x.disposition, x.reason = "conflict", "identity-conflict"
                    rev.dispositions.append(disp)
                else:
                    disp.disposition, disp.reason = "duplicate", "identical-redelivery"
                    rev.dispositions.append(disp)
                continue
            if st.first_station is not None and inc.stationRef != st.first_station:
                disp.disposition, disp.reason = "rejected", "station-change"
                continue
            lower = [x for n, x in st.revisions.items() if n < inc.revision and not x.conflict]
            higher = [x for n, x in st.revisions.items() if n > inc.revision and not x.conflict]
            if any(inc.revisionEventTime < x.incident.revisionEventTime for x in lower) or \
                    any(inc.revisionEventTime > x.incident.revisionEventTime for x in higher):
                disp.disposition, disp.reason, disp.refs = "rejected", "schema-invalid", _refs(d, "revisionEventTime")
                continue
            if higher:
                disp.disposition, disp.reason = "duplicate", "superseded"
            st.revisions[inc.revision] = Revision(inc, {k: r.value[k] for k in INCIDENT_FIELDS if k in r.value} | {"suspectTo": inc.suspectTo},
                                                  r.payload_digest, [disp])
            if st.first_station is None:
                st.first_station = inc.stationRef
        return states, disps

    # -- the evaluation of one cut -------------------------------------------------------------------------------------------

    def evaluate(self, work_records: dict, samples: dict, deliveries: list[Delivery], w2: dict):
        """``work_records``/``samples``: the oracle's stage-1 rows of the cut (key JSON -> body); ``w2``: station -> W2 = W1.
        Returns (assessments, sets, dispositions), the first two as key JSON -> body."""
        states, disps = self.classify(deliveries)
        bases_by_station: dict[str, list] = {}
        for k, body in work_records.items():
            bases_by_station.setdefault(body["readPoint"], []).append((json.loads(k), body))
        rows_by_device: dict[str, list] = {}
        for k, body in samples.items():
            sid = json.loads(k)
            rows_by_device.setdefault(sid["device"], []).append((sid["metric"], body))
        assessments, sets = {}, {}
        for (source, iid), st in sorted(states.items()):
            cur = st.current()
            if cur is None:
                continue
            s = cur.incident.stationRef
            W2 = w2.get(s)
            bases = [b for b in bases_by_station.get(s, []) if not self._expired(b[1], W2)]
            valid = st.valid()
            units = sorted({b[1]["unitId"] for b in bases
                            if any(self._rel(b[1], r.incident) != UNRELATED for r in valid)})
            unit_rows = []
            for u in units:
                body = self.assessment_body(cur, [b for b in bases if b[1]["unitId"] == u])
                assessments[key_text({"source": source, "incidentId": iid, "unitId": u})] = body
                unit_rows.append((u, body))
            sets[key_text({"source": source, "incidentId": iid})] = self.set_body(cur, unit_rows, rows_by_device, W2)
        return assessments, sets, disps

    def _expired(self, body: dict, W2) -> bool:
        iv = body["interval"]
        end = iv["leave"] if iv["leave"] is not None else iv["enter"]
        return W2 is not None and W2 >= end + self.work_records_ms

    def _rel(self, body: dict, inc: inputs.Incident) -> str:
        iv = body["interval"]
        return related(self.overlap, iv["enter"], iv["leave"], inc.suspectFrom, inc.suspectTo)

    # -- T5, T6, T7 ------------------------------------------------------------------------------------------------------------

    def judge(self, cur: Revision, bases: list) -> tuple[str, str, list, list]:
        """(status, reason, related-or-possibly bases, related bases) for one unit; bases are (visitRef, body)."""
        inc = cur.incident
        rel = [(ref, b, self._rel(b, inc)) for ref, b in bases]
        considered = [(ref, b) for ref, b, x in rel if x != UNRELATED]
        relb = [(ref, b) for ref, b, x in rel if x == RELATED]
        poss = [(ref, b) for ref, b, x in rel if x == POSSIBLY]
        if inc.status == "withdrawn":
            if self.on_withdrawn == "not-affected":
                return "not-affected-within-scope", "incident-withdrawn", considered, relb
            return "insufficient-evidence", "incident-withdrawn", considered, relb
        for ref, b in relb:
            if b["status"] == "complete" and self._positive(b, inc):
                return "affected-candidate", ("equipment-activity" if self.match == "equipment-evidence" else "station-visit"), considered, relb
        if not relb and not poss:
            return "not-affected-within-scope", "interval-narrowed", considered, relb
        if any("correction-conflict" in b["insufficiencyReasons"] for _, b in relb):
            return "insufficient-evidence", "correction-conflict", considered, relb
        statuses = {b["status"] for _, b in relb + poss}
        if "provisional" in statuses:
            return "insufficient-evidence", "basis-provisional", considered, relb
        if "insufficient" in statuses:
            return "insufficient-evidence", "basis-insufficient", considered, relb
        retractions = [(b.get("retraction") or {}) for _, b in relb if b["status"] == "retracted"]
        for reason, code in (("overlap", "basis-retracted-overlap"), ("input-conflict", "basis-retracted-input-conflict")):
            if any(x.get("reason") == reason for x in retractions):
                return "insufficient-evidence", code, considered, relb
        if any(x.get("reason") == "error-declaration" and x.get("errorReason") == INCORRECT_DATA for x in retractions):
            return "insufficient-evidence", "basis-retracted-error-declaration", considered, relb
        for ref, b in relb:
            if b["status"] == "complete" and self._inactive(b, inc):
                return "not-affected-within-scope", "no-equipment-activity", considered, relb
        if relb and all(b["status"] == "retracted" and (b.get("retraction") or {}).get("reason") == "error-declaration"
                        and b["retraction"].get("errorReason") == DID_NOT_OCCUR for _, b in relb):
            if self.on_retracted == "by-cause":
                return "not-affected-within-scope", "basis-retracted-error-declaration", considered, relb
            return "insufficient-evidence", "basis-retracted-error-declaration", considered, relb
        return "insufficient-evidence", "basis-insufficient", considered, relb

    @staticmethod
    def _judged(b: dict, inc: inputs.Incident):
        iv = b["interval"]
        lo = max(iv["enter"], inc.suspectFrom)
        his = [x for x in (iv["leave"], inc.suspectTo) if x is not None]
        hi = min(his) if his else None
        return lo, hi

    def _positive(self, b: dict, inc: inputs.Incident) -> bool:
        if self.match == "station-visit":
            return True
        e = next((x for x in b["equipmentEvidence"] if x["equipmentPath"] == inc.equipmentRef), None)
        if e is None:
            return False
        lo, hi = self._judged(b, inc)
        return any(a < (hi if hi is not None else float("inf")) and z > lo for a, z in e["activityIntervals"])

    def _inactive(self, b: dict, inc: inputs.Incident) -> bool:
        e = next((x for x in b["equipmentEvidence"] if x["equipmentPath"] == inc.equipmentRef), None)
        lo, hi = self._judged(b, inc)
        if e is None or hi is None or lo >= hi:
            return False
        t = lo
        for a, z in sorted(e["inactiveIntervals"]):
            if a > t:
                break
            t = max(t, z)
            if t >= hi:
                return True
        return t >= hi

    def assessment_body(self, cur: Revision, bases: list) -> dict:
        status, reason, considered, _ = self.judge(cur, bases)
        ordered = sorted(considered, key=lambda x: canon.canonical_bytes(x[0]))
        return {
            "status": status,
            "reason": reason,
            "incident": dict(cur.value),
            "basis": [{"visitRef": ref, "bodyDigest": canon.digest(b)} for ref, b in ordered],
            "evidenceWarnings": [{"visitRef": ref, "insufficiencyReasons": list(b["insufficiencyReasons"])} for ref, b in ordered
                                 if b["status"] != "complete"],
            "rulesDigest": self.rules_digest,
        }

    # -- T8 --------------------------------------------------------------------------------------------------------------------

    def set_body(self, cur: Revision, unit_rows: list, rows_by_device: dict, W2) -> dict:
        inc = cur.incident
        scoped = [inc.equipmentRef] if self.match == "equipment-evidence" else [inc.stationRef]
        P = self.P
        f = inc.suspectFrom
        if inc.suspectTo is not None:
            end = inc.suspectTo
        else:
            last = [b["sourceTs"] for dev in scoped for _, b in rows_by_device.get(dev, []) if b["sourceTs"] is not None]
            end = max(last) + P if last else f
        n = 0 if end <= f else -(-(end - f) // P)
        unattributed = unknown = 0
        for dev in scoped:
            act = self.rs.activity_of(dev)
            known, ua = set(), set()
            if act is not None:
                for metric, b in rows_by_device.get(dev, []):
                    t = b["sourceTs"]
                    if metric != act.metric or t is None or not (f <= t < end):
                        continue
                    i = (t - f) // P
                    if b["state"] == "attributed" or (b["state"] == "unattributed" and b["reason"] in ("no-occupancy", "ambiguous")):
                        known.add(i)
                        if b["state"] == "unattributed" and b["active"] is True:
                            ua.add(i)
            unattributed += len(ua)
            unknown += n - len(known)
        statuses = [(b["status"], b["reason"]) for _, b in unit_rows]
        provisional = sum(1 for s, r in statuses if r == "basis-provisional")
        insufficient = sum(1 for s, r in statuses if s == "insufficient-evidence" and r != "basis-provisional")
        warnings = {}
        for _, b in unit_rows:
            for w in b["evidenceWarnings"]:
                warnings[key_text(w["visitRef"])] = w["insufficiencyReasons"]
        gap_overlaps = sum(1 for rs_ in warnings.values() if "session-gap" in rs_ or "session-ambiguous" in rs_)
        ambiguous = any("session-ambiguous" in rs_ for rs_ in warnings.values())
        withdrawn = inc.status == "withdrawn"
        is_open = inc.status == "open"
        settled = True if withdrawn else (inc.suspectTo is not None and W2 is not None and W2 >= inc.suspectTo + self.L)
        beyond = inc.suspectFrom < inc.revisionEventTime - self.work_records_ms
        need_hi = inc.suspectTo if inc.suspectTo is not None else W2
        gap = not self._covered(scoped, inc.suspectFrom, need_hi)
        if unknown > 0 or ambiguous:
            status = "unknown"
        elif unattributed > 0 or provisional > 0 or insufficient > 0 or gap_overlaps > 0 or is_open or not settled or beyond or gap or withdrawn:
            status = "incomplete"
        else:
            status = "complete"
        return {
            "incident": dict(cur.value),
            "units": [{"unitId": u, "status": b["status"], "reason": b["reason"], "bodyDigest": canon.digest(b)} for u, b in unit_rows],
            "resolution": "withdrawn" if withdrawn else None,
            "coverage": {"status": status, "unattributedActivityPeriods": unattributed, "unknownActivityPeriods": unknown,
                         "provisionalUnits": provisional, "insufficientUnits": insufficient, "sessionGapOverlaps": gap_overlaps,
                         "open": False if withdrawn else is_open, "settled": settled, "beyondRetention": beyond, "retentionGap": gap},
        }

    def _covered(self, devices: list, lo: int, hi) -> bool:
        """The manifest's verified evidence range {from, to} (``to`` the last covered instant) covers [lo, hi) on every device."""
        if hi is None or hi <= lo:
            return True
        for dev in devices:
            r = self.evidence_range.get(dev)
            if r is None or not (r["from"] <= lo and hi - 1 <= r["to"]):
                return False
        return True
