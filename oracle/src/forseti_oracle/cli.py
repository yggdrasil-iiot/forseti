"""The oracle's command line, the only way the gate reaches the oracle (python -I -m forseti_oracle.cli ...).

  canon-check <vectors>                                   every vector canonicalizes to its text and SHA-256, every refusal vector
                                                          is refused with its reason; exit 0 when all hold, 1 otherwise
  rules-digest --mappings <dir> --release <id>@<version>  the rulesDigest (definitions {}) of the attribution rule <id>@<version> in
               --rule <id>@<version>                      that release, read and checked from the bytes on disk; exit 0 and the
                                                          digest on stdout, 1 with the refusal on stderr

Exit 2 is a usage error. Later steps add stage1, stage2 and all.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from forseti_oracle import canon, rules


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


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="forseti-oracle")
    sub = p.add_subparsers(dest="command", required=True)
    c = sub.add_parser("canon-check")
    c.add_argument("vectors", type=Path)
    d = sub.add_parser("rules-digest")
    d.add_argument("--mappings", type=Path, required=True)
    d.add_argument("--release", type=_pair, required=True)
    d.add_argument("--rule", type=_pair, required=True)
    a = p.parse_args(argv)   # a usage error exits 2
    if a.command == "canon-check":
        return canon_check(a.vectors)
    return rules_digest(a.mappings, a.release, a.rule)


if __name__ == "__main__":
    sys.exit(main())
