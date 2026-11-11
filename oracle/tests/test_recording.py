import json
import shutil
from pathlib import Path

import pytest

from forseti_oracle import canon, recording
from forseti_oracle.recording import Malformed

ROOT = Path(__file__).resolve().parents[2]
RECORDINGS = ROOT / "fixtures" / "recordings"
SCENARIOS = ROOT / "fixtures" / "scenarios"
T0 = 1794182400000
NR1 = "synthetic-site:line2:station1:nutrunner1"

# messages, samples, cuts per committed recording (the Java RecordingTest asserts the same numbers from the same files).
COUNTS = {"smoke": (4060, 5684, 28), "smoke-plc": (4060, 10556, 28), "smoke-incident": (7760, 10864, 53)}


def all_records(rec, topic):
    return [r for p in rec.subscribed(topic) for r in rec.records(topic, p)]


def test_smoke_counts_and_offsets():
    rec = recording.open_recording(RECORDINGS / "smoke")
    rec.verify_digests()
    messages = all_records(rec, "ot.messages")
    assert len(messages) == 4060
    per_device = {}
    for m in messages:
        per_device[m.key] = per_device.get(m.key, 0) + 1
    assert sorted(per_device.values()) == [812] * 5
    assert sum(len(m.value["members"]) for m in messages) == 5684
    assert len(rec.batches()) == 28
    assert len(all_records(rec, "ot.sessions")) == 5
    assert len(all_records(rec, "mfg.occupancy")) == 2
    assert all_records(rec, "quality.incidents") == []
    for b in rec.batches():
        assert set(b["throughOffsets"]["quality.incidents"].values()) == {-1}
    first = rec.records("mfg.occupancy", 0)[0]
    assert first.value["eventId"] == "occ-1" and first.value["eventTime"] == T0 + 600000 and "arrivalEventId" not in first.value
    assert rec.manifest["deadlines"] == [{"kind": "visit-settlement", "ref": "visit-1", "at": T0 + 1380000, "cutA": 27, "cutB": 28}]


@pytest.mark.parametrize("name", sorted(COUNTS))
def test_counts_digests_and_lock(name):
    rec = recording.open_recording(RECORDINGS / name)
    rec.verify_digests()
    messages = all_records(rec, "ot.messages")
    want = COUNTS[name]
    assert (len(messages), sum(len(m.value["members"]) for m in messages), len(rec.batches())) == want
    lock = json.loads((SCENARIOS / f"{name}.lock.json").read_text(encoding="utf-8"))
    assert (lock["counts"]["messages"], lock["counts"]["samples"], lock["counts"]["cuts"]) == want
    listed = dict(rec.manifest["digests"])
    listed["manifest.json"] = canon.sha256((RECORDINGS / name / "manifest.json").read_bytes())
    assert listed == lock["files"]


@pytest.mark.parametrize("name", sorted(COUNTS))
def test_routing_digest_recomputed_with_the_oracle_canonicalizer(name):
    rec = recording.open_recording(RECORDINGS / name)
    body = json.loads((RECORDINGS / name / "routing.json").read_text(encoding="utf-8"))
    declared = body.pop("routingDigest")
    assert canon.digest(body) == declared == rec.routing_digest()
    for device, entry in rec.routing["devices"].items():
        assert recording.default_partition(device, 6) == entry["rawPartition"], device
    assert rec.routing["stations"] == {"synthetic-site:line2:station1": 0, "synthetic-site:line2:station2": 1, "synthetic-site:line2:station3": 2}


def test_incident_in_the_partition_of_its_equipment_ref():
    rec = recording.open_recording(RECORDINGS / "smoke-incident")
    p = recording.default_partition(NR1, 6)
    assert p != rec.routing["stations"]["synthetic-site:line2:station1"]
    assert [r.value["incidentId"] for r in rec.records("quality.incidents", p)] == ["inc-1"]
    assert [r for q in range(6) if q != p for r in rec.records("quality.incidents", q)] == []


def test_kafka_murmur2_vectors():
    # Kafka UtilsTest.testMurmur2; the Java Murmur2Test pins the same values.
    cases = {b"21": -973932308, b"foobar": -790332482, b"a-little-bit-long-string": -985981536,
             b"a-little-bit-longer-string": -1486304829, b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971, b"abc": 479470107}
    for data, want in cases.items():
        assert recording.murmur2(data) == want


def test_malformed_recordings_are_refused(tmp_path):
    copy = tmp_path / "smoke"
    shutil.copytree(RECORDINGS / "smoke", copy)
    text = (copy / "manifest.json").read_text(encoding="utf-8")
    for value in ("9007199254740992", "-2"):
        (copy / "manifest.json").write_text(text.replace('"0": -1', f'"0": {value}', 1), encoding="utf-8", newline="\n")
        with pytest.raises(Malformed, match="throughOffsets.quality.incidents.0"):
            recording.open_recording(copy)
    (copy / "manifest.json").write_text(text, encoding="utf-8", newline="\n")
    with open(copy / "ot.sessions" / "1.jsonl", "ab") as f:
        f.write(b"\n")
    with pytest.raises(Malformed):
        recording.open_recording(copy).verify_digests()
