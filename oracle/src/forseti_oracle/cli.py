"""The oracle's command line, the only way the gate reaches the oracle (python -I -m forseti_oracle.cli ...).

  canon-check <vectors>                                   every vector canonicalizes to its text and SHA-256, every refusal vector
                                                          is refused with its reason; exit 0 when all hold, 1 otherwise
  rules-digest --mappings <dir> --release <id>@<version>  the rulesDigest (definitions {}) of the attribution rule <id>@<version> in
               --rule <id>@<version>                      that release, read and checked from the bytes on disk; exit 0 and the
                                                          digest on stdout, 1 with the refusal on stderr

  stage1|stage2|all --recording <dir> --cuts <run>/cuts.json --mappings <dir> --release <id>@<version> --rule <id>@<version>
               --definitions <manifest> --out <dir> [--lenient] [--subscribe <topic>=<p,p>] [--check-journals <run>]
                                                          for every cut of the cut log, the live rows the oracle recomputes from
                                                          the inputs delivered through that cut: <out>/cut-<cutId>/<topic>.json
                                                          ({logical key JSON -> {body, bodyDigest}}) for the stage's journals,
                                                          dispositions-stage1.json / dispositions-stage2.json ({identity JSON ->
                                                          {disposition, reason, deliveries}}), and <out>/oracle-run.json; stdout
                                                          "cuts N, rows M" (M = live rows at the last cut); --check-journals adds
                                                          the fold checks of the harness run to <out>/journal-check.json and
                                                          exits 1 on a violation

Exit 2 is a usage error.
"""
from __future__ import annotations

import argparse
import json
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


def _rows(rows: dict) -> dict:
    return {k: {"body": b, "bodyDigest": canon.digest(b)} for k, b in sorted(rows.items())}


def run(a, out=None, err=None) -> int:
    """stage1, stage2 and all: every completed cut of the cut log, recomputed from scratch from the inputs delivered through it."""
    out, err = out or sys.stdout, err or sys.stderr
    try:
        rec = recording.open_recording(a.recording)
        rec.verify_digests()
        history = cuts.read(a.cuts, rec)
        rel = rules.load_release(Path(a.mappings), a.release[0], a.release[1])
        rs = rules.rule_set(rel, a.rule[0], a.rule[1])
    except (rules.Refused, recording.Malformed, cuts.Inconsistent) as e:
        print(json.dumps({"refusal": getattr(e, "reason", str(e)), "evidence": getattr(e, "evidence", {})}, sort_keys=True), file=err)
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
    rows_last = 0
    for c in history.cuts:
        ds = [d for d in every if d.cut <= c.index]
        w1 = {s: v.w1 for s, v in c.per_station.items()}
        r1 = s1.evaluate([d for d in ds if d.topic != "quality.incidents"], admission, w1)
        files = {}
        if a.command in ("stage1", "all"):
            files[stage1.WORK_RECORDS] = _rows(r1.work_records)
            files[stage1.SAMPLES] = _rows(r1.samples)
            files["dispositions-stage1"] = stage1.dispositions_by_identity(r1.dispositions)
        if a.command in ("stage2", "all"):
            w2 = {s: (v.w2 if v.w2 is not None else v.w1) for s, v in c.per_station.items()}
            assessments, sets, d2 = s2.evaluate(r1.work_records, r1.samples, ds, w2)
            files[stage2.AFFECTED] = _rows(assessments)
            files[stage2.AFFECTED_SETS] = _rows(sets)
            files["dispositions-stage2"] = stage1.dispositions_by_identity(d2)
        for name, value in files.items():
            _write(o / f"cut-{c.cut_id}" / f"{name}.json", value)
        rows_last = sum(len(v) for n, v in files.items() if not n.startswith("dispositions"))
    import importlib.metadata as md
    import platform
    _write(o / "oracle-run.json", {
        "command": a.command, "python": platform.python_version(), "rfc8785": md.version("rfc8785"), "jsonschema": md.version("jsonschema"),
        "recording": {"name": rec.manifest.get("name"), "manifestSha256": canon.sha256((rec.path / recording.MANIFEST).read_bytes()),
                      "digests": rec.manifest.get("digests")},
        "pins": rs.pins(definitions), "rulesDigest": s1.rules_digest, "generation": history.generation,
        "cuts": [c.cut_id for c in history.cuts],
        "offsetRanges": {str(c.cut_id): {t: {str(p): list(r) for p, r in ps.items()} for t, ps in c.offset_ranges.items()} for c in history.cuts},
        "subscribe": {t: ps for t, ps in (a.subscribe or [])}, "lenient": a.lenient,
    })
    print(f"cuts {len(history.cuts)}, rows {rows_last}", file=out)
    if a.check_journals is not None:
        delivered = [(d.topic, d.partition, d.offset) for d in every]
        violations = fold.check_run(Path(a.check_journals), [c.cut_id for c in history.cuts], delivered)
        _write(o / "journal-check.json", [v.as_json() for v in violations])
        print(f"journal-check: violations {len(violations)}", file=out)
        if violations:
            return 1
    return 0


def main(argv: list[str] | None = None) -> int:
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
    a = p.parse_args(argv)   # a usage error exits 2
    if a.command == "canon-check":
        return canon_check(a.vectors)
    if a.command == "rules-digest":
        return rules_digest(a.mappings, a.release, a.rule)
    return run(a)


if __name__ == "__main__":
    sys.exit(main())
