"""Checks one negative-control run of the core gate, as CI runs it (owner decision of 2026-11-12).

usage: python -I scripts/check-control-report.py <report.json> <exit code> <control> <shard | aggregate>

Passes (exit 0) only when the gate's exit code is 1, or 0 for a shard run that owns none of the control's intended ids (a shard that
only feeds an aggregate id, which the aggregate control run then proves); the report is that control's run of that shard (kind
negative-control, layout shard) or its aggregate control run (layout aggregate); its exit field is the exit code; its intended ids
(intendedFailures, the ones this run evaluates) are a subset of its failed ids; and its control verdict holds (no crash, no missing
run, every cut compared with the oracle, every intended id failed). Prints one line either way.
"""
import json
import sys


def main(argv):
    if len(argv) != 5:
        print(__doc__.strip().splitlines()[2])
        return 2
    path, rc, control, shard = argv[1], int(argv[2]), argv[3], argv[4]
    problems = []
    try:
        with open(path, encoding="utf-8") as f:
            r = json.load(f)
    except (OSError, ValueError) as e:
        print(f"{control} on {shard}: no readable report ({e}); the gate exited {rc}")
        return 1
    intended = r.get("intendedFailures") or []
    failed = set(r.get("failed") or [])
    verdict = r.get("control") or {}
    layout = "aggregate" if shard == "aggregate" else "shard"
    if rc not in ((1,) if intended else (0, 1)):
        problems.append(f"exited {rc}, expected 1" if intended else f"exited {rc}, expected 0 or 1")
    if r.get("exit") != rc:
        problems.append(f"report exit {r.get('exit')}, the process exited {rc}")
    if r.get("kind") != "negative-control" or r.get("negativeControl") != control:
        problems.append(f"kind {r.get('kind')}, negativeControl {r.get('negativeControl')}")
    if r.get("layout") != layout or (layout == "shard" and r.get("shard") != shard):
        problems.append(f"layout {r.get('layout')}, shard {r.get('shard')}")
    if layout == "aggregate" and not intended:
        problems.append("an aggregate control run without an intended aggregate id")
    missing = [i for i in intended if i not in failed]
    if missing:
        problems.append(f"intended {missing} not failed")
    if verdict.get("held") is not True:
        problems.append(f"control verdict does not hold: {verdict.get('problems')}")
    if any(g.get("state") != "isolated" for g in r.get("generations") or []):
        problems.append("a generation that is not isolated")
    where = f"{control} on {shard}"
    if problems:
        print(f"{where}: FAILED: " + "; ".join(problems))
        return 1
    print(f"{where}: exit {rc}, intended {intended} in failed ({len(failed)} failed), verdict held")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
