"""A tiny in-test driver for the oracle's stage functions: inline input records typed from the rule sheets, cut by cut, with the
harness's formulas for W1 (largest routed sourceTs - O - 1, never decreasing) and admissionW (the previous cut's W1). No Java output
is used anywhere."""
from __future__ import annotations

import copy
import json
from pathlib import Path

from forseti_oracle import canon, rules, stage1, stage2
from forseti_oracle.inputs import MESSAGES

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures"
T0 = 1794182400000
T = 1794183000000
S1, S2, S3 = "synthetic-site:line2:station1", "synthetic-site:line2:station2", "synthetic-site:line2:station3"
NR1, NR2 = S1 + ":nutrunner1", S1 + ":nutrunner2"
SOURCE, EDGE, BIRTH = "mes-line2", "edge-line2", 1794182400000
ARRIVING, DEPARTING = "urn:epcglobal:cbv:bizstep:arriving", "urn:epcglobal:cbv:bizstep:departing"
DID_NOT_OCCUR, INCORRECT_DATA = "urn:epcglobal:cbv:er:did_not_occur", "urn:epcglobal:cbv:er:incorrect_data"
BAD, UNCERTAIN = 0x80000000, 0x40000000
L, H, O = 720_000, 86_400_000, 30_000
SMOKE_DEFINITIONS = json.loads((FIXTURES / "recordings" / "smoke" / "manifest.json").read_text(encoding="utf-8"))["definitions"]


def ruleset(release="line2-rules", rule="line2-attribution", version="1.0.0", patch=None):
    rel = rules.load_release(FIXTURES / "registry" / "mappings", release, "1.0.0")
    rs = rules.rule_set(rel, rule, version)
    if patch:
        rs = copy.deepcopy(rs)
        patch(rs)
    return rs


def arrival(eid, t, unit, rp=S1, seq=1):
    return {"eventId": eid, "eventTime": t, "recordTime": t + 200, "unitId": unit, "action": "OBSERVE", "bizStep": ARRIVING,
            "readPoint": rp, "visitSeq": seq, "source": SOURCE}


def departure(eid, t, unit, rp, arrival_eid, seq=1):
    d = arrival(eid, t, unit, rp, seq)
    d["bizStep"] = DEPARTING
    d["arrivalEventId"] = arrival_eid
    return d


def declared(event, reason, at):
    d = copy.deepcopy(event)
    d["recordTime"] = at + 100
    d["errorDeclaration"] = {"declarationTime": at, "reason": reason, "correctiveEventIDs": []}
    return d


def member(metric, value, status=0, ts=None):
    m = {"metric": metric, "value": value, "uaStatusCode": status, "quality": 192 if status == 0 else 0 if status == BAD else 64}
    if ts is not None:
        m["sourceTs"], m["serverTs"] = ts, ts + 10
    return m


def session(device, kind, at_seq, payload):
    return {"edge": EDGE, "device": device, "birthPublishTs": BIRTH, "bdSeq": 0, "kind": kind, "atSeq": at_seq, "payload": payload}


def incident(iid, revision, equipment, suspect_from, suspect_to, status="closed", detected=None, ret=None, kind="tool-check-failed",
             station=S1, source="qms-line2"):
    detected = detected if detected is not None else (suspect_to if suspect_to is not None else suspect_from + 1)
    return {"incidentId": iid, "revision": revision, "stationRef": station, "equipmentRef": equipment, "kind": kind, "detectedAt": detected,
            "revisionEventTime": ret if ret is not None else detected, "suspectFrom": suspect_from, "suspectTo": suspect_to,
            "status": status, "evidenceRef": "urn:line2:qms:" + iid, "source": source}


SCAN = {S1: [("Running", True)], NR1: [("Torque", 20.5), ("Running", True)], NR2: [("Torque", 0.0), ("Running", False)]}


class Driver:
    def __init__(self, rs=None, station=S1, definitions=None, lenient=False):
        self.rs = rs or ruleset()
        self.station = station
        self.definitions = definitions or SMOKE_DEFINITIONS
        self.stage = stage1.Stage1(self.rs, self.definitions, lenient=lenient)
        self.pending: list = []
        self.delivered: list = []
        self.cuts: list[dict] = []        # station -> {"W1", "admissionW"}
        self.offsets: dict = {}
        self.seqs: dict = {}
        self.largest: dict = {}
        self.result = None

    # inputs
    def input(self, topic, value, raw: bytes | None = None):
        off = self.offsets.get(topic, 0)
        self.offsets[topic] = off + 1
        b = raw if raw is not None else json.dumps(value, separators=(",", ":")).encode("utf-8")
        d = stage1.make_delivery(topic, 0, off, len(self.cuts), b)
        self.pending.append(d)
        if topic == MESSAGES and d.read.parsed is not None:
            s = stage1.station_of(self.rs.stations, d.read.parsed.device)
            ts = [m.sourceTs for m in d.read.parsed.members if m.sourceTs is not None]
            if s is not None and ts:
                self.largest[s] = max(self.largest.get(s, ts[0]), max(ts))
        return d

    def message(self, device, ts, members, seq=None):
        if seq is None:
            seq = self.seqs.get(device, 1) + 1
        self.seqs[device] = max(self.seqs.get(device, 1), seq)
        return {"edge": EDGE, "device": device, "birthPublishTs": BIRTH, "bdSeq": 0, "seq": seq, "publishTs": ts + 50,
                "bridgeReceivedAt": ts + 120, "definitionSha256": self.definitions[device]["sha256"], "members": members}

    def scan(self, t, over=None):
        """One scan at t of the station's devices (station1: station Running true, nutrunner1 Torque 20.5 Running true, nutrunner2
        Torque 0.0 Running false; another station: its Running true). ``over``: {(device, metric): (value, status)}, value None drops."""
        over = over or {}
        devices = SCAN if self.station == S1 else {self.station: [("Running", True)]}
        out = []
        for dev, ms in devices.items():
            members = []
            for metric, value in ms:
                status = 0
                if (dev, metric) in over:
                    value, status = over[(dev, metric)]
                    if value is None:
                        continue
                members.append(member(metric, value, status, t))
            out.append(self.input(MESSAGES, self.message(dev, t, members)))
        return out

    def scans(self, lo, hi, over=None):
        for t in range(lo, hi, 1000):
            self.scan(t, over(t) if callable(over) else over)

    # cuts
    def cut(self, w=None):
        row = {}
        for s in self.rs.stations:
            prev = self.cuts[-1][s]["W1"] if self.cuts else None
            if w is not None:
                w1 = w
            elif s in self.largest:
                w1 = self.largest[s] - self.stage.O - 1
            else:
                w1 = prev if prev is not None else -(2**53 - 1)
            if prev is not None and w1 < prev:
                w1 = prev
            row[s] = {"W1": w1, "admissionW": prev}
        self.cuts.append(row)
        self.delivered.extend(self.pending)
        self.pending = []
        self.result = self.evaluate()
        return self.result

    def evaluate(self, k=None):
        k = len(self.cuts) - 1 if k is None else k
        ds = [d for d in self.delivered if d.cut <= k]
        return self.stage.evaluate(ds, lambda c, s: self.cuts[c][s]["admissionW"], {s: v["W1"] for s, v in self.cuts[k].items()})

    # reading results
    def work(self, arrival_eid, source=SOURCE):
        return self.result.work_records.get(stage1.key_text({"mode": "occupancy-join", "source": source, "arrivalEventId": arrival_eid}))

    def plc_work(self, visit_id, namespace="urn:line2:visits", station=S1):
        return self.result.work_records.get(stage1.key_text({"mode": "plc-stamped", "namespace": namespace, "station": station, "visitId": visit_id}))

    def sample(self, delivery, metric):
        from forseti_oracle import inputs
        return self.result.samples[stage1.key_text(inputs.sample_id(delivery.read.parsed, metric))]

    def dispositions(self, delivery):
        return [(d.part, d.disposition, d.reason) for d in self.result.dispositions if d.delivery is delivery]


def visit_ref(eid, source=SOURCE):
    return {"mode": "occupancy-join", "source": source, "arrivalEventId": eid}


def entry(body, mapping_id):
    return next(m for m in body["members"] if m["mappingId"] == mapping_id)


def evidence(body, path):
    return next(e for e in body["equipmentEvidence"] if e["equipmentPath"] == path)


def digest(body):
    return canon.digest(body)


PLC_DEFINITIONS = json.loads((FIXTURES / "recordings" / "smoke-plc" / "manifest.json").read_text(encoding="utf-8"))["definitions"]
NS = "urn:line2:visits"


def plc_ruleset(release="line2-rules-plc", version="1.0.0"):
    return ruleset(release, "line2-attribution", version)


class PlcDriver(Driver):
    """Scans of station1 and nutrunner1 carrying UnitId, VisitId, VisitPresent beside Running (and Torque on the tool)."""

    def __init__(self, rs=None, **kw):
        super().__init__(rs or plc_ruleset(), S1, PLC_DEFINITIONS, **kw)

    def plc_scan(self, t, unit, visit, present, devices=(S1, NR1), running=True, torque=20.5, ts=None, identity_ts=None):
        out = []
        for dev in devices:
            ms = []
            if dev == NR1:
                ms.append(member("Torque", torque, 0, t if ts is None else ts))
            ms.append(member("Running", running, 0, t if ts is None else ts))
            it = t if identity_ts is None else identity_ts
            ms += [member("UnitId", unit, 0, it), member("VisitId", visit, 0, it), member("VisitPresent", present, 0, it)]
            out.append(self.input(MESSAGES, self.message(dev, t, ms)))
        return out

    def plc_scans(self, lo, hi, unit, visit, present=True, **kw):
        for t in range(lo, hi, 1000):
            self.plc_scan(t, unit, visit, present, **kw)


class Stage2Driver(Driver):
    """Stage 1 then stage 2 of the same cut, W2 = W1, the evidence range covering every device from T0."""

    def __init__(self, rs=None, evidence_range=None, **kw):
        super().__init__(rs, **kw)
        er = evidence_range or {d: {"from": T0, "to": T0 + 10 * H} for d in self.definitions}
        self.s2 = stage2.Stage2(self.rs, self.stage.rules_digest, er)
        self.assessments, self.sets, self.d2 = {}, {}, []

    def evaluate(self, k=None):
        r = super().evaluate(k)
        k = len(self.cuts) - 1 if k is None else k
        ds = [d for d in self.delivered if d.cut <= k]
        self.assessments, self.sets, self.d2 = self.s2.evaluate(r.work_records, r.samples, ds, {s: v["W1"] for s, v in self.cuts[k].items()})
        return r

    def assessment(self, unit, iid="inc-1", source="qms-line2"):
        return self.assessments.get(stage1.key_text({"source": source, "incidentId": iid, "unitId": unit}))

    def set(self, iid="inc-1", source="qms-line2"):
        return self.sets.get(stage1.key_text({"source": source, "incidentId": iid}))

    def incident_dispositions(self, delivery):
        return [(d.disposition, d.reason, d.refs.get("check")) for d in self.d2 if d.delivery is delivery]
