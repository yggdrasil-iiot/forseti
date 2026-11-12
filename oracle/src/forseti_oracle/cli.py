"""The oracle's command line, the only way the gate reaches the oracle (python -I -m forseti_oracle.cli ...).

  canon-check <vectors>                                   every vector canonicalizes to its text and SHA-256, every refusal vector
                                                          is refused with its reason; exit 0 when all hold, 1 otherwise
  rules-digest --mappings <dir> --release <id>@<version>  the rulesDigest (definitions {}) of the attribution rule <id>@<version> in
               --rule <id>@<version>                      that release, read and checked from the bytes on disk; exit 0 and the
                                                          digest on stdout, 1 with the refusal on stderr

  stage1|stage2|all --recording <dir> --cuts <run>/cuts.json --mappings <dir> --release <id>@<version> --rule <id>@<version>
               --definitions <manifest> --out <dir> [--lenient] [--subscribe <topic>=<p,p>] [--check-journals <run>]
               [--snapshots [--cut-ids <k,k>]] [--stream] [--no-memo]
                                                          for every cut of the cut log, the live rows the oracle recomputes from
                                                          scratch from the inputs delivered through that cut, as six tables per
                                                          cut in <out>/cut-<cutId>/<table>.json: the stage's journals ({logical
                                                          key JSON -> body}) and dispositions-stage1 / dispositions-stage2
                                                          ({identity JSON -> {disposition, reason, deliveries}}); see table_text
                                                          for a table's encoding: the first cut is a snapshot, every later cut
                                                          the delta against the previous one with the whole set's count and
                                                          setHash; --snapshots writes every cut as a snapshot (diagnostics),
                                                          --cut-ids only those cuts; --stream hands the cuts over one at a time
                                                          (Emitter); --no-memo keeps no value across cuts. Then
                                                          <out>/oracle-run.json; stdout "cuts N, rows M" (M = live rows at the
                                                          last cut); --check-journals adds the fold checks of the harness run to
                                                          <out>/journal-check.json and exits 1 on a violation

Exit 2 is a usage error.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

from forseti_oracle import canon, cuts, fold, recording, rules, stage1, stage2


def _pair(text: str) -> tuple[str, str]:
    name, sep, version = text.rpartition("@")
    if not sep or not name or not version:
        raise argparse.ArgumentTypeError(f"expected <id>@<version>, got {text!r}")
    return name, version


def canon_check(vectors: Path, out=None) -> int:
    out = out or sys.stdout
    doc = canon.loads(Path(vectors).read_bytes().decode("utf-8"))
    problems: list[str] = []
    for v in doc.get("vectors", []):
        try:
            c = canon.canonical_bytes(canon.loads(v["inputText"]))
        except canon.CanonicalRefusal as e:
            problems.append(f"vector {v['id']}: refused ({e})")
            continue
        if c.decode("utf-8") != v["canonical"]:
            problems.append(f"vector {v['id']}: canonical text differs")
        if canon.sha256(c) != v["sha256"]:
            problems.append(f"vector {v['id']}: sha256 differs")
    for r in doc.get("refusals", []):
        try:
            canon.canonical_bytes(canon.loads(r["inputText"]))
            problems.append(f"refusal {r['id']}: accepted")
        except canon.CanonicalRefusal as e:
            if not str(e).startswith(r["reason"]):
                problems.append(f"refusal {r['id']}: refused as {e!s}, expected {r['reason']}")
    for p in problems:
        print(p, file=out)
    print(f"canon-check: vectors {len(doc.get('vectors', []))}, refusals {len(doc.get('refusals', []))}, problems {len(problems)}", file=out)
    return 0 if problems == [] and doc.get("vectors") and doc.get("refusals") else 1


def rules_digest(mappings: Path, release: tuple[str, str], rule: tuple[str, str], out=None, err=None) -> int:
    out, err = out or sys.stdout, err or sys.stderr
    try:
        rel = rules.load_release(Path(mappings), release[0], release[1])
        rs = rules.rule_set(rel, rule[0], rule[1])
    except rules.Refused as e:
        print(json.dumps({"refusal": e.reason, "evidence": e.evidence}, sort_keys=True), file=err)
        return 1
    print(rs.rules_digest({}), file=out)
    return 0


def _subscribe(text: str) -> tuple[str, list[int]]:
    topic, sep, parts = text.partition("=")
    if not sep or topic not in recording.TOPICS:
        raise argparse.ArgumentTypeError(f"expected <topic>=<p,p>, got {text!r}")
    try:
        return topic, [int(x) for x in parts.split(",") if x != ""]
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected <topic>=<p,p>, got {text!r}") from None


def _write(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True).encode("utf-8") + b"\n")


# --- the cut stream -------------------------------------------------------------------------------------------------------

TABLES = (stage1.WORK_RECORDS, stage1.SAMPLES, "dispositions-stage1", stage2.AFFECTED, stage2.AFFECTED_SETS, "dispositions-stage2")


def set_hash(digests: dict) -> str:
    """SHA-256 over ``key LF bodyDigest LF`` per row, by key in code-point order (the UTF-8 byte order): the set of one table at one
    cut. Keys are canonical JSON texts, which never hold a raw LF; digests are 64 hex digits."""
    return canon.sha256("".join(f"{k}\n{digests[k]}\n" for k in sorted(digests)).encode("utf-8"))


def _dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def table_text(cut_id: int, previous, table: str, rows: dict, digests: dict, upserts: list, deletes: list) -> bytes:
    """One table of one cut: ``{cutId, previousCutId, table, mode, count, setHash, deletes, upserts {key -> {body, bodyDigest}}}``, one
    upsert per line. ``mode`` is ``snapshot`` (every row, ``previousCutId`` null) or ``delta`` (the rows whose freshly computed body
    digest differs from the previous emitted cut's, and the keys gone since). ``count`` and ``setHash`` are of the whole set."""
    head = {"cutId": cut_id, "previousCutId": previous, "table": table, "mode": "snapshot" if previous is None else "delta",
            "count": len(digests), "setHash": set_hash(digests), "deletes": deletes}
    parts = [_dumps(head)[:-1], ',"upserts":{']
    for i, k in enumerate(upserts):
        parts.append(("\n" if i == 0 else ",\n") + _dumps(k) + ':{"body":' + _dumps(rows[k]) + ',"bodyDigest":"' + digests[k] + '"}')
    parts.append("\n}}\n")
    return "".join(parts).encode("utf-8")


class StreamBroken(RuntimeError):
    """The consumer did not acknowledge a cut as the stream protocol requires."""


class Emitter:
    """Writes every evaluated cut as ``<out>/cut-<cutId>/<table>.json``: the first cut (every cut with ``snapshots``) as a full snapshot,
    every later one as the delta against the previous emitted cut. Every body digest is computed fresh from the cut's freshly
    recomputed body; a row is an upsert when that digest differs from the digest computed the same way at the previous emitted cut.
    A cut directory appears whole (written as ``cut-<cutId>.partial`` and renamed). In ``stream`` mode the emitter prints ``cut
    <cutId>`` once the directory is in place and, before writing the next cut, reads ``ok <cutId>`` from ``acks`` and checks the
    acknowledged directory is gone: the consumer deletes each cut before the next one is written, so the disk holds one cut."""

    def __init__(self, out: Path, snapshots: bool, stream: bool, announce, acks):
        self.out, self.snapshots, self.stream, self.announce, self.acks = out, snapshots, stream, announce, acks
        self.previous_cut = None
        self.previous: dict = {}   # table -> {key -> bodyDigest} of the previous emitted cut
        self.unacknowledged = None

    def wait(self) -> None:
        if not self.stream or self.unacknowledged is None:
            return
        line = self.acks.readline()
        if line.strip() != f"ok {self.unacknowledged}":
            raise StreamBroken(f"expected 'ok {self.unacknowledged}' from the consumer, read {line.strip()!r}")
        if (self.out / f"cut-{self.unacknowledged}").exists():
            raise StreamBroken(f"cut-{self.unacknowledged} acknowledged but still on disk")
        self.unacknowledged = None

    def emit(self, cut_id: int, tables: dict) -> None:
        texts, current = {}, {}
        previous = None if self.snapshots else self.previous_cut
        for table, rows in tables.items():
            digests = {k: canon.digest(b) for k, b in rows.items()}
            if previous is None:
                upserts, deletes = sorted(digests), []
            else:
                before = self.previous[table]
                upserts = sorted(k for k, d in digests.items() if before.get(k) != d)
                deletes = sorted(k for k in before if k not in digests)
            texts[table] = table_text(cut_id, previous, table, rows, digests, upserts, deletes)
            current[table] = digests
        self.wait()
        final, partial = self.out / f"cut-{cut_id}", self.out / f"cut-{cut_id}.partial"
        for d in (partial, final):
            if d.exists():
                shutil.rmtree(d)
        partial.mkdir(parents=True)
        for table, text in texts.items():
            (partial / f"{table}.json").write_bytes(text)
        os.replace(partial, final)
        self.previous, self.previous_cut = current, cut_id
        if self.stream:
            print(f"cut {cut_id}", file=self.announce, flush=True)
            self.unacknowledged = cut_id


def run(a, out=None, err=None, acks=None) -> int:
    """stage1, stage2 and all: every completed cut of the cut log, recomputed from scratch from the inputs delivered through it."""
    out, err, acks = out or sys.stdout, err or sys.stderr, acks or sys.stdin
    memo = stage1.MEMO
    stage1.MEMO = not a.no_memo
    try:
        return _run(a, out, err, acks)
    finally:
        stage1.MEMO = memo


def _run(a, out, err, acks) -> int:
    try:
        rec = recording.open_recording(a.recording)
        rec.verify_digests()
        history = cuts.read(a.cuts, rec)
        rel = rules.load_release(Path(a.mappings), a.release[0], a.release[1])
        rs = rules.rule_set(rel, a.rule[0], a.rule[1])
    except (rules.Refused, recording.Malformed, cuts.Inconsistent) as e:
        print(json.dumps({"refusal": getattr(e, "reason", str(e)), "evidence": getattr(e, "evidence", {})}, sort_keys=True), file=err)
        return 1
    selected = set(a.cut_ids) if a.cut_ids else None
    unknown = sorted(selected - {c.cut_id for c in history.cuts}) if selected is not None else []
    if unknown:
        print(json.dumps({"refusal": "cut-ids", "evidence": {"unknown": unknown}}, sort_keys=True), file=err)
        return 1
    definitions = canon.loads(Path(a.definitions).read_text(encoding="utf-8"))["definitions"]
    s1 = stage1.Stage1(rs, definitions, lenient=a.lenient)
    s2 = stage2.Stage2(rs, s1.rules_digest, rec.manifest.get("evidenceRange", {}))
    subscribed = dict(a.subscribe) if a.subscribe else None
    every = cuts.deliveries(rec, history, subscribed=subscribed)
    by_index = {c.index: c for c in history.cuts}

    def admission(index, station):
        return by_index[index].per_station[station].admission_w

    o = Path(a.out)
    o.mkdir(parents=True, exist_ok=True)
    emitter = Emitter(o, a.snapshots, a.stream, out, acks)
    evaluated = [c for c in history.cuts if selected is None or c.cut_id in selected]
    rows_last = 0
    try:
        for c in evaluated:
            ds = [d for d in every if d.cut <= c.index]
            w1 = {s: v.w1 for s, v in c.per_station.items()}
            r1 = s1.evaluate([d for d in ds if d.topic != "quality.incidents"], admission, w1)
            tables = {}
            if a.command in ("stage1", "all"):
                tables[stage1.WORK_RECORDS] = r1.work_records
                tables[stage1.SAMPLES] = r1.samples
                tables["dispositions-stage1"] = stage1.dispositions_by_identity(r1.dispositions)
            if a.command in ("stage2", "all"):
                w2 = {s: (v.w2 if v.w2 is not None else v.w1) for s, v in c.per_station.items()}
                assessments, sets, d2 = s2.evaluate(r1.work_records, r1.samples, ds, w2)
                tables[stage2.AFFECTED] = assessments
                tables[stage2.AFFECTED_SETS] = sets
                tables["dispositions-stage2"] = stage1.dispositions_by_identity(d2)
            emitter.emit(c.cut_id, {t: tables[t] for t in TABLES if t in tables})
            rows_last = sum(len(v) for n, v in tables.items() if not n.startswith("dispositions"))
        emitter.wait()
    except StreamBroken as e:
        print(json.dumps({"refusal": "stream", "evidence": {"detail": str(e)}}, sort_keys=True), file=err)
        return 1
    import importlib.metadata as md
    import platform
    _write(o / "oracle-run.json", {
        "command": a.command, "python": platform.python_version(), "rfc8785": md.version("rfc8785"), "jsonschema": md.version("jsonschema"),
        "recording": {"name": rec.manifest.get("name"), "manifestSha256": canon.sha256((rec.path / recording.MANIFEST).read_bytes()),
                      "digests": rec.manifest.get("digests")},
        "pins": rs.pins(definitions), "rulesDigest": s1.rules_digest, "generation": history.generation,
        "cuts": [c.cut_id for c in evaluated],
        "offsetRanges": {str(c.cut_id): {t: {str(p): list(r) for p, r in ps.items()} for t, ps in c.offset_ranges.items()} for c in history.cuts},
        "subscribe": {t: ps for t, ps in (a.subscribe or [])}, "lenient": a.lenient,
        "encoding": {"snapshots": a.snapshots, "stream": a.stream, "memo": not a.no_memo},
    })
    print(f"cuts {len(evaluated)}, rows {rows_last}", file=out)
    if a.check_journals is not None:
        delivered = [(d.topic, d.partition, d.offset) for d in every]
        violations = fold.check_run(Path(a.check_journals), [c.cut_id for c in history.cuts], delivered)
        _write(o / "journal-check.json", [v.as_json() for v in violations])
        print(f"journal-check: violations {len(violations)}", file=out)
        if violations:
            return 1
    return 0


def _ids(text: str) -> list[int]:
    try:
        ids = [int(x) for x in text.split(",") if x != ""]
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected <cutId,cutId>, got {text!r}") from None
    if not ids:
        raise argparse.ArgumentTypeError(f"expected <cutId,cutId>, got {text!r}")
    return ids


def parse(argv: list[str] | None = None) -> argparse.Namespace:
    """The parsed command line; a usage error exits 2."""
    p = argparse.ArgumentParser(prog="forseti-oracle")
    sub = p.add_subparsers(dest="command", required=True)
    c = sub.add_parser("canon-check")
    c.add_argument("vectors", type=Path)
    d = sub.add_parser("rules-digest")
    d.add_argument("--mappings", type=Path, required=True)
    d.add_argument("--release", type=_pair, required=True)
    d.add_argument("--rule", type=_pair, required=True)
    for name in ("stage1", "stage2", "all"):
        r = sub.add_parser(name)
        r.add_argument("--recording", type=Path, required=True)
        r.add_argument("--cuts", type=Path, required=True)
        r.add_argument("--mappings", type=Path, required=True)
        r.add_argument("--release", type=_pair, required=True)
        r.add_argument("--rule", type=_pair, required=True)
        r.add_argument("--definitions", type=Path, required=True)
        r.add_argument("--out", type=Path, required=True)
        r.add_argument("--lenient", action="store_true")
        r.add_argument("--subscribe", type=_subscribe, action="append")
        r.add_argument("--check-journals", type=Path)
        r.add_argument("--snapshots", action="store_true")
        r.add_argument("--cut-ids", type=_ids)
        r.add_argument("--stream", action="store_true")
        r.add_argument("--no-memo", action="store_true")
    a = p.parse_args(argv)
    if getattr(a, "cut_ids", None) and not a.snapshots:
        p.error("--cut-ids needs --snapshots (a delta is relative to the previous cut)")
    return a


def main(argv: list[str] | None = None) -> int:
    a = parse(argv)
    if a.command == "canon-check":
        return canon_check(a.vectors)
    if a.command == "rules-digest":
        return rules_digest(a.mappings, a.release, a.rule)
    return run(a)


if __name__ == "__main__":
    sys.exit(main())
