"""The oracle's own fold of a harness run's journals and its integrity checks (spec 333, 553, 566, 605; plan Task 18).

A journal file ``<run>/journals/<topic>/<p>.jsonl`` holds, in write order, rows ``{"key", "headers", "value": {"body", "bodyDigest",
"envelope"}}``, deletes ``{"value": {"delete": {generation, logicalKey, revision, supersedesDigest}}}`` and progress records
``{"key": null, "value": {"type": "progress", ...}}``. Folding a partition through cut k applies every row and delete before that
partition's progress k (highest revision per key, deletes remove the key). The checks are the five the plan names: a revision gap,
equal consecutive digests, a business row after its cut's progress, a delivery without a disposition, a stale ``basis[]`` digest.

Reading: JCS prints a body double equal to or beyond 2^53 as an integer literal; the oracle's canonicalizer refuses integers outside
the safe range, so such a literal is read back as the double it was (every integer the bodies carry otherwise is a time, a count or
a revision inside the safe range).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from forseti_oracle import canon

MAX_SAFE = 9007199254740991
JOURNAL_TOPICS = ("forseti.work-records", "forseti.samples", "forseti.affected", "forseti.affected-sets")
LEDGERS = ("forseti.dispositions-stage1", "forseti.dispositions-stage2")


def _int(text: str):
    v = int(text)
    return v if -MAX_SAFE <= v <= MAX_SAFE else float(v)


def loads(text: str):
    return json.loads(text, parse_int=_int)


def key_text(key) -> str:
    return canon.canonical_bytes(key).decode("utf-8")


def read_journal(path: Path) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    raw = p.read_bytes()
    return [loads(line.decode("utf-8")) for line in raw.split(b"\n") if line]


def is_progress(e: dict) -> bool:
    v = e.get("value")
    return isinstance(v, dict) and v.get("type") == "progress"


def is_delete(e: dict) -> bool:
    v = e.get("value")
    return isinstance(v, dict) and "delete" in v


def partitions(run: Path, topic: str) -> dict[int, list[dict]]:
    d = Path(run) / "journals" / topic
    if not d.exists():
        return {}
    return {int(f.stem): read_journal(f) for f in sorted(d.glob("*.jsonl"))}


def fold_entries(entries: list[dict], cut_id: int | None = None) -> dict:
    """key JSON -> {body, bodyDigest, revision} of one partition through cut ``cut_id``: every row and delete written before that
    partition's progress ``cut_id`` (a cut's rows precede its progress); everything when ``cut_id`` is None."""
    live: dict = {}
    for e in entries:
        if is_progress(e):
            if cut_id is not None and e["value"].get("cutId", 0) >= cut_id:
                break
            continue
        if is_delete(e):
            live.pop(key_text(e["value"]["delete"]["logicalKey"]), None)
            continue
        v = e["value"]
        live[key_text(e["key"])] = {"body": v["body"], "bodyDigest": v["bodyDigest"], "revision": v["envelope"]["revision"]}
    return live


def fold_topic(run: Path, topic: str, cut_id: int | None = None) -> dict:
    """The live rows of a topic through cut ``cut_id`` on every partition."""
    out: dict = {}
    for p, entries in partitions(run, topic).items():
        out.update(fold_entries(entries, cut_id))
    return out


def fold_ledger(run: Path, topic: str, cut_id: int | None = None) -> dict:
    """delivery key JSON -> {body, revision}: the highest disposition revision per delivery key through progress ``cut_id``."""
    out: dict = {}
    for p, entries in partitions(run, topic).items():
        for e in entries:
            if is_progress(e):
                if cut_id is not None and e["value"].get("cutId", 0) >= cut_id:
                    break
                continue
            k = key_text(e["key"])
            if k not in out or e["value"]["revision"] >= out[k]["revision"]:
                out[k] = {"body": e["value"]["body"], "revision": e["value"]["revision"]}
    return out


# --- integrity -----------------------------------------------------------------------------------------------------------------


@dataclass
class Violation:
    check: str
    topic: str
    partition: int | None
    detail: dict = field(default_factory=dict)

    def as_json(self) -> dict:
        return {"check": self.check, "topic": self.topic, "partition": self.partition, **self.detail}


def check_partition(topic: str, partition: int, entries: list[dict]) -> list[Violation]:
    """Revision continuity from 1 per key (deletes included), no equal consecutive row digests, recomputed digests, and no business
    row after its cut's progress (a row whose ``eventTimeAt`` is below the event time the previous progress reported, or a row after
    the partition's last progress)."""
    out: list[Violation] = []
    last_rev: dict = {}
    last_digest: dict = {}
    last_time = None
    trailing = []
    for i, e in enumerate(entries):
        if is_progress(e):
            v = e["value"]
            t = v.get("W1", v.get("W2"))
            if t is not None:
                last_time = t
            trailing = []
            continue
        if is_delete(e):
            d = e["value"]["delete"]
            k, rev = key_text(d["logicalKey"]), d["revision"]
            if rev != last_rev.get(k, 0) + 1:
                out.append(Violation("revision-gap", topic, partition, {"key": k, "revision": rev, "previous": last_rev.get(k, 0)}))
            last_rev[k] = rev
            last_digest.pop(k, None)
            trailing.append(i)
            continue
        k = key_text(e["key"])
        v = e["value"]
        env = v.get("envelope", {})
        rev = env.get("revision")
        if rev != last_rev.get(k, 0) + 1:
            out.append(Violation("revision-gap", topic, partition, {"key": k, "revision": rev, "previous": last_rev.get(k, 0)}))
        last_rev[k] = rev
        if canon.digest(v["body"]) != v["bodyDigest"]:
            out.append(Violation("digest-mismatch", topic, partition, {"key": k, "revision": rev}))
        if last_digest.get(k) == v["bodyDigest"]:
            out.append(Violation("equal-consecutive-digest", topic, partition, {"key": k, "revision": rev}))
        last_digest[k] = v["bodyDigest"]
        at = env.get("eventTimeAt")
        if last_time is not None and at is not None and at < last_time:
            out.append(Violation("row-after-progress", topic, partition, {"key": k, "revision": rev, "eventTimeAt": at, "progressAt": last_time}))
        trailing.append(i)
    if trailing and any(is_progress(e) for e in entries):
        out.append(Violation("row-after-progress", topic, partition, {"trailingEntries": len(trailing)}))
    return out


def check_dispositions(run: Path, delivered: list[tuple[str, int, int]]) -> list[Violation]:
    """Every delivered input ``(topic, partition, offset)`` has at least one disposition in the ledgers."""
    have = set()
    for topic in LEDGERS:
        for p, entries in partitions(run, topic).items():
            for e in entries:
                if is_progress(e):
                    continue
                k = e["key"]
                have.add((k["inputTopic"], k["inputPartition"], k["inputOffset"]))
    return [Violation("missing-disposition", t, p, {"offset": o}) for t, p, o in delivered if (t, p, o) not in have]


def check_basis_current(work_records: dict, assessments: dict, cut_id) -> list[Violation]:
    """``J-BASIS-CURRENT``: every ``basis[].bodyDigest`` of every live assessment equals the folded work record digest of that visit."""
    out = []
    for k, a in assessments.items():
        for b in a["body"]["basis"]:
            wk = key_text(b["visitRef"])
            cur = work_records.get(wk)
            if cur is None or cur["bodyDigest"] != b["bodyDigest"]:
                out.append(Violation("stale-basis", "forseti.affected", None, {"cut": cut_id, "key": k, "visitRef": wk}))
    return out


def check_run(run: Path, cut_ids: list[int], delivered: list[tuple[str, int, int]] | None = None) -> list[Violation]:
    out: list[Violation] = []
    for topic in JOURNAL_TOPICS:
        for p, entries in partitions(run, topic).items():
            out.extend(check_partition(topic, p, entries))
    if delivered is not None:
        out.extend(check_dispositions(run, delivered))
    for c in cut_ids:
        out.extend(check_basis_current(fold_topic(run, JOURNAL_TOPICS[0], c), fold_topic(run, JOURNAL_TOPICS[2], c), c))
    return out
