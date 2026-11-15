"""Copy one passed full-loop run into fixtures/live-recordings/<name>/ (step-5 plan PR 8).

    python -I scripts/commit-live-recording.py <run dir> <name>

The run directory is the --out of scripts/run-full-loop-gate.sh; its run.json must say "passed" and its
compare report be "equal" and "accepted". Copied byte for byte: recording/
(the step-3 format), recording.capture-lock.json, recording.capture-report.json, recording.dead-letter.jsonl,
recording.compare-report.json, run.json and bridge-run.json; preflight.txt with its line ends made LF. Written: provenance.json (the
stack lock's upstream commits and image digests, the SHA-256 of the jars the run used (from run.json, not the lock), the digest of
the lock the run's stack check passed, the live fixture lock, the placement, attribution and stream, the scenario and
its digest, the runner's injection options and tail margin, the jar digests). Not copied: the core and oracle output, the raw frame log,
the process logs and the state (they stay in build/). The target directory must not exist.
"""
import hashlib
import json
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILES = ["recording.capture-lock.json", "recording.capture-report.json", "recording.dead-letter.jsonl", "recording.compare-report.json",
         "run.json", "bridge-run.json"]


def sha256(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def main(argv):
    if len(argv) != 3:
        sys.exit(__doc__)
    run, name = argv[1], argv[2]
    if not name or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in name):
        sys.exit("commit-live-recording: the name is lowercase letters, digits and '-'")
    record = json.load(open(os.path.join(run, "run.json"), encoding="utf-8"))
    if record.get("outcome") != "passed":
        sys.exit("commit-live-recording: %s did not pass (%s)" % (run, record.get("failedStep")))
    compare = json.load(open(os.path.join(run, "recording.compare-report.json"), encoding="utf-8"))
    if compare.get("outcome") != "equal":
        sys.exit("commit-live-recording: the compare report of %s is not 'equal'" % run)
    if compare.get("accepted") is not True:
        sys.exit("commit-live-recording: the visits of %s are not accepted (%s)" % (run, compare.get("acceptance", {}).get("problems")))
    target = os.path.join(ROOT, "fixtures", "live-recordings", name)
    if os.path.exists(target):
        sys.exit("commit-live-recording: %s exists" % target)
    scenario = os.path.join(ROOT, record["scenario"])
    if sha256(scenario) != record["scenarioSha256"]:
        sys.exit("commit-live-recording: %s is not the scenario the run drove" % record["scenario"])
    # The upstream commits and image digests come from the stack lock; the jar digests are those of the jars the run used (run.json).
    # A later lock may state other digests for the same commits (jars rebuilt with other zip entry times or line ends).
    stack_lock = os.path.join(ROOT, "live", "stack-lock.json")
    lock = json.load(open(stack_lock, encoding="utf-8"))
    for jar in lock["jars"]:
        if not record["jars"].get(jar):
            sys.exit("commit-live-recording: the run record has no %s jar digest" % jar)

    os.makedirs(os.path.dirname(target), exist_ok=True)
    shutil.copytree(os.path.join(run, "recording"), os.path.join(target, "recording"))
    for f in FILES:
        shutil.copyfile(os.path.join(run, f), os.path.join(target, f))
    with open(os.path.join(run, "preflight.txt"), "rb") as f:
        preflight = f.read().replace(b"\r\n", b"\n")
    with open(os.path.join(target, "preflight.txt"), "wb") as f:
        f.write(preflight)
    live_lock = os.path.join(ROOT, "fixtures", "live", "lock.json")
    provenance = {
        "kind": "forseti-live-recording-provenance",
        "provenanceVersion": 1,
        "name": name,
        "runId": record["runId"],
        "placement": record["placement"],
        "attribution": record["attribution"],
        "stream": record["stream"],
        "scenario": record["scenario"],
        "scenarioSha256": record["scenarioSha256"],
        "runner": {
            "script": "scripts/run-full-loop-gate.sh",
            "injections": record["injections"],
            "tailMarginMs": record["tailMarginMs"],
        },
        "stackLock": "live/stack-lock.json",
        "stackLockSha256": record["stackLockSha256"],
        "images": {k: {"image": v["image"], "version": v["version"], "digest": v["digest"]} for k, v in sorted(lock["images"].items())},
        "upstream": {k: {"repository": v["repository"], "commit": v["commit"], "jarSha256": record["jars"][k]} for k, v in sorted(lock["jars"].items())},
        "liveFixtures": {"lock": "fixtures/live/lock.json", "sha256": sha256(live_lock),
                         "bifrost": json.load(open(live_lock, encoding="utf-8"))["bifrost"],
                         "tyr": json.load(open(live_lock, encoding="utf-8"))["tyr"]},
        "jars": record["jars"],
        "manifestSha256": compare["manifestSha256"],
    }
    with open(os.path.join(target, "provenance.json"), "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(provenance, indent=2) + "\n")
    print("committed %s -> fixtures/live-recordings/%s" % (run, name))


if __name__ == "__main__":
    main(sys.argv)
