"""The oracle's fold and integrity checks of a harness run, on inline journals: each of the five violations is detected, and a clean
journal folds to its live rows (deletes applied, highest revision per key)."""
from __future__ import annotations

from pathlib import Path

from forseti_oracle import canon, fold

VISIT = {"mode": "occupancy-join", "source": "mes-line2", "arrivalEventId": "occ-1"}


def row(key, body, revision, at=100):
    return {"key": key, "headers": {"forseti-generation": "1"},
            "value": {"body": body, "bodyDigest": canon.digest(body), "envelope": {"revision": revision, "eventTimeAt": at}}}


def delete(key, revision):
    return {"value": {"delete": {"generation": 1, "logicalKey": key, "revision": revision, "supersedesDigest": "0" * 64}}}


def progress(cut, w1=100):
    return {"key": None, "value": {"type": "progress", "cutId": cut, "W1": w1}}


def write(run: Path, topic: str, entries: list, partition: int = 0):
    f = run / "journals" / topic / f"{partition}.jsonl"
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_bytes(b"".join(canon.canonical_bytes(e) + b"\n" for e in entries))


def checks(run, delivered=None, cuts=(1, 2)):
    return sorted({v.check for v in fold.check_run(run, list(cuts), delivered)})


def test_a_clean_journal_folds_to_its_live_rows(tmp_path):
    k2 = {"x": 2}
    write(tmp_path, fold.JOURNAL_TOPICS[0], [row(VISIT, {"s": 1}, 1), row(k2, {"s": 0}, 1), progress(1), row(VISIT, {"s": 2}, 2, 200),
                                            delete(k2, 2), progress(2, 200)])
    assert checks(tmp_path) == []
    at1 = fold.fold_topic(tmp_path, fold.JOURNAL_TOPICS[0], 1)
    at2 = fold.fold_topic(tmp_path, fold.JOURNAL_TOPICS[0], 2)
    assert {k: v["body"] for k, v in at1.items()} == {fold.key_text(VISIT): {"s": 1}, fold.key_text(k2): {"s": 0}}
    assert {k: (v["body"], v["revision"]) for k, v in at2.items()} == {fold.key_text(VISIT): ({"s": 2}, 2)}


def test_a_revision_gap_is_detected(tmp_path):
    write(tmp_path, fold.JOURNAL_TOPICS[0], [row(VISIT, {"s": 1}, 1), row(VISIT, {"s": 2}, 3), progress(1)])
    assert checks(tmp_path) == ["revision-gap"]


def test_equal_consecutive_digests_are_detected(tmp_path):
    write(tmp_path, fold.JOURNAL_TOPICS[1], [row(VISIT, {"s": 1}, 1), row(VISIT, {"s": 1}, 2), progress(1)])
    assert checks(tmp_path) == ["equal-consecutive-digest"]


def test_a_row_after_its_progress_is_detected(tmp_path):
    write(tmp_path, fold.JOURNAL_TOPICS[0], [progress(1, 500), row(VISIT, {"s": 1}, 1, at=400), progress(2, 600)])
    assert checks(tmp_path) == ["row-after-progress"]
    write(tmp_path, fold.JOURNAL_TOPICS[0], [progress(1, 500), row(VISIT, {"s": 1}, 1, at=500)])   # trailing: no progress follows
    assert checks(tmp_path) == ["row-after-progress"]


def test_a_missing_disposition_is_detected(tmp_path):
    key = {"generation": 1, "inputTopic": "mfg.occupancy", "inputPartition": 0, "inputOffset": 0, "part": "event"}
    write(tmp_path, fold.LEDGERS[0], [{"key": key, "headers": {}, "value": {"body": {}, "revision": 1}}, progress(1)])
    assert checks(tmp_path, [("mfg.occupancy", 0, 0)]) == []
    assert checks(tmp_path, [("mfg.occupancy", 0, 0), ("mfg.occupancy", 0, 1)]) == ["missing-disposition"]


def test_a_stale_basis_digest_is_detected(tmp_path):
    w1, w2 = {"s": 1}, {"s": 2}
    write(tmp_path, fold.JOURNAL_TOPICS[0], [row(VISIT, w1, 1), progress(1), row(VISIT, w2, 2, 200), progress(2, 200)])
    a = {"basis": [{"visitRef": VISIT, "bodyDigest": canon.digest(w1)}]}
    key = {"source": "qms", "incidentId": "inc-1", "unitId": "U"}
    write(tmp_path, fold.JOURNAL_TOPICS[2], [row(key, a, 1), progress(1), progress(2, 200)])
    v = fold.check_run(tmp_path, [1, 2])
    assert [(x.check, x.detail["cut"]) for x in v] == [("stale-basis", 2)]


def test_the_ledger_fold_keeps_the_highest_disposition_revision(tmp_path):
    key = {"generation": 1, "inputTopic": "mfg.occupancy", "inputPartition": 0, "inputOffset": 0, "part": "event"}
    write(tmp_path, fold.LEDGERS[0], [{"key": key, "headers": {}, "value": {"body": {"disposition": "accepted"}, "revision": 1}}, progress(1),
                                      {"key": key, "headers": {}, "value": {"body": {"disposition": "conflict"}, "revision": 2}}, progress(2)])
    assert [v["body"]["disposition"] for v in fold.fold_ledger(tmp_path, fold.LEDGERS[0], 1).values()] == ["accepted"]
    assert [v["body"]["disposition"] for v in fold.fold_ledger(tmp_path, fold.LEDGERS[0], 2).values()] == ["conflict"]


def test_a_body_double_of_two_to_the_53_reads_back_as_a_double():
    body = {"max": 9007199254740992.0}
    line = canon.canonical_bytes(body).decode()
    assert line == '{"max":9007199254740992}'
    back = fold.loads(line)
    assert isinstance(back["max"], float) and canon.digest(back) == canon.digest(body)
