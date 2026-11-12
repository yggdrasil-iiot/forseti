"""The cut log reader: through-offsets are inclusive with -1 for an empty partition, a history must match the recording's batches."""
from __future__ import annotations

import json

import pytest

from forseti_oracle import cuts, recording

from .driver import FIXTURES, ruleset

SMOKE_INCIDENT = FIXTURES / "recordings" / "smoke-incident"
S1 = "synthetic-site:line2:station1"


def history_doc(rec, n=None):
    throughs = cuts.through_offsets(rec)
    out = []
    for i, b in enumerate(rec.batches()[:n]):
        ranges = {}
        for t in recording.TOPICS:
            for p in rec.subscribed(t):
                r = cuts.selected(throughs[i], throughs[i - 1] if i else None, t, p)
                ranges.setdefault(t, {})[str(p)] = [r.start, r.stop]
        inc = [{"cutId": b["cutId"], "partition": p, "throughOffset": throughs[i]["quality.incidents"][p]}
               for p in rec.subscribed("quality.incidents")]
        out.append({"cutId": b["cutId"], "perStation": {S1: {"W1": 1, "admissionW": None, "W2": 1, "mergedCutIds": [], "onlineVisits": 0}},
                    "offsetRanges": ranges, "incidentOffsets": inc})
    return {"generation": 1, "routingDigest": rec.routing["routingDigest"], "cuts": out}


def test_through_offset_minus_one_means_an_empty_partition():
    rec = recording.open_recording(SMOKE_INCIDENT)
    throughs = cuts.through_offsets(rec)
    first = next(i for i, t in enumerate(throughs) if any(v >= 0 for v in t["quality.incidents"].values()))
    before = throughs[first - 1]
    assert all(v == -1 for v in before["quality.incidents"].values())
    for p in rec.subscribed("quality.incidents"):
        assert list(cuts.selected(before, throughs[first - 2], "quality.incidents", p)) == []
    picked = {p: list(cuts.selected(throughs[first], before, "quality.incidents", p)) for p in rec.subscribed("quality.incidents")}
    assert sorted(x for v in picked.values() for x in v) == [0]
    (p,) = [p for p, v in picked.items() if v]
    assert len(rec.records("quality.incidents", p)) == 1


def test_a_consistent_history_is_read_and_assigns_every_record_its_cut(tmp_path):
    rec = recording.open_recording(SMOKE_INCIDENT)
    f = tmp_path / "cuts.json"
    f.write_text(json.dumps(history_doc(rec)))
    h = cuts.read(f, rec)
    assert [c.cut_id for c in h.cuts] == [b["cutId"] for b in rec.batches()]
    ds = cuts.deliveries(rec, h)
    assert len(ds) == sum(len(rec.records(t, p)) for t in recording.TOPICS for p in rec.subscribed(t))
    (inc,) = [d for d in ds if d.topic == "quality.incidents"]
    assert h.cuts[inc.cut].through["quality.incidents"][inc.partition] == 0
    assert all(d.cut <= h.cuts[-1].index for d in ds)
    f.write_text(json.dumps(history_doc(rec, 2)))          # a prefix of the history delivers only its records
    assert {d.cut for d in cuts.deliveries(rec, cuts.read(f, rec))} == {0, 1}
    assert [d for d in cuts.deliveries(rec, h, subscribed={"mfg.occupancy": []}) if d.topic == "mfg.occupancy"] == []


@pytest.mark.parametrize("mutate,message", [
    (lambda d: d["cuts"][1]["offsetRanges"]["ot.messages"].update({"1": [0, 5]}), "offsetRanges"),
    (lambda d: d["cuts"][0].update({"cutId": 999}), "not a batch"),
    (lambda d: d["cuts"].pop(1), "missing cut"),
    (lambda d: d["cuts"][0]["incidentOffsets"][0].update({"throughOffset": 0}), "incident through-offset"),
    (lambda d: d.update({"routingDigest": "0" * 64}), "routingDigest"),
    (lambda d: d["cuts"][0]["incidentOffsets"][0].update({"throughOffset": -2}), "schema-invalid"),
])
def test_an_inconsistent_history_is_refused(tmp_path, mutate, message):
    rec = recording.open_recording(SMOKE_INCIDENT)
    doc = history_doc(rec, 3)
    mutate(doc)
    f = tmp_path / "cuts.json"
    f.write_text(json.dumps(doc))
    with pytest.raises(cuts.Inconsistent, match=message):
        cuts.read(f, rec)


def test_the_formula_history_reports_w1_per_station():
    rec = recording.open_recording(SMOKE_INCIDENT)
    h = cuts.formula_history(rec, ruleset(), 30_000)
    w = [c.per_station[S1].w1 for c in h.cuts]
    assert w == sorted(w) and h.cuts[0].per_station[S1].admission_w is None
    assert all(h.cuts[i].per_station[S1].admission_w == w[i - 1] for i in range(1, len(w)))
    assert w[-1] == rec.manifest["evidenceRange"][S1]["to"] - 30_001
