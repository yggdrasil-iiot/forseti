"""Publish forseti's fixture registries with Bifrost's own CLI from Tyr's committed bundles.

Tyr's bundles are copied untouched to fixtures/upstream and published byte for byte: their profiles carry the real
schema digests of schemas/*.json and the incident topic quality.incidents, and the plc bundle names stream line2-plc
(checked before anything is published). Three variants are forseti-derived copies, edited as listed and re-pinned
(evidence sha256, proposal sha256 list, submission digests):
  registry:          line2-rules 1.0.0 on stream line2 and line2-rules-plc 1.0.0 on stream line2-plc (Tyr's bytes);
                     line2-rules-plc-nocheck 1.0.0 on stream line2-plc-nocheck (variant: crossCheck none)
  registry-next:     the same plus line2-rules 1.0.1 (variant: maxMissingPeriods 4) on stream line2 (head version 2)
  registry-mismatch: line2-rules 1.0.0 on stream line2 (variant: another occupancy format.schemaSha256, the
                     binding.schema-mismatch fixture)
Needs <bifrost>/gates/target/bifrost-gates.jar. Private keys are deleted; public keys go to fixtures/trusted-keys.jsonl.

--live writes fixtures/live only and leaves everything above untouched (step 5, PR 3). Tyr's four placement bundles
(<tyr>/placement/<variant>/bundle) are copied untouched to fixtures/live/upstream/<variant> and published byte for byte
by the same procedure, under fresh keys whose public halves go to fixtures/live/trusted-keys.jsonl:
  live/registry:     line2-rules-dedicated, line2-rules-dedicated-plc, line2-rules-shared, line2-rules-shared-plc, all
                     1.0.0, on streams line2-dedicated, line2-dedicated-plc, line2-shared, line2-shared-plc
  live/definitions:  Bifrost's published provenance pairs recipe/{station-model,tool-model}/1.0.0/{recipe-setpoints.yaml,
                     manifest.json}, copied byte for byte (a Muninn registry directory)
fixtures/live/lock.json pins the Tyr and Bifrost commits and the sha256 of every file under upstream, definitions and
registry. The routing manifests in fixtures/live/routing are derived from the published registry by forseti's own
placement code (cli live.Placement), not by this script.
"""
from __future__ import annotations

import argparse, hashlib, json, shutil, subprocess, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STREAMS = {"bundle": ("line2", "line2-rules"), "plc-stamped-bundle": ("line2-plc", "line2-rules-plc")}
MISMATCH_SHA = "0" * 63 + "1"   # a well-formed digest that is not the occupancy schema's: Bifrost checks the form, forseti the equality


def sha(b: bytes) -> str: return hashlib.sha256(b).hexdigest()
def load(p: Path): return json.loads(p.read_bytes())
def dump(p: Path, d) -> None: p.write_bytes((json.dumps(d, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))
def git_head(p: Path) -> str: return subprocess.run(["git", "-C", str(p), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
def inventory(root: Path) -> dict: return {str(p.relative_to(root)).replace("\\", "/"): sha(p.read_bytes()) for p in sorted(q for q in root.rglob("*") if q.is_file())}


def repin(bundle: Path) -> None:
    p = load(bundle / "proposal.json")
    for f in p["files"]:
        if f["kind"] == "evidence-artifact":
            e = load(bundle / f["path"]); e["file"]["sha256"] = sha((bundle / e["file"]["path"]).read_bytes()); dump(bundle / f["path"], e)
    for f in p["files"]:
        d = load(bundle / f["path"]); f["id"], f["version"] = d["id"], d["version"]; f["sha256"] = sha((bundle / f["path"]).read_bytes())
    dump(bundle / "proposal.json", p)
    s = load(bundle / "submission.json"); s["proposalSha256"] = s["workspaceSha256"] = sha((bundle / "proposal.json").read_bytes()); dump(bundle / "submission.json", s)


def check_upstream(bundle: Path, stream: str, occupancy_sha: str, incident_sha: str) -> None:
    occ = load(bundle / "data" / "occupancy-binding-0.json"); inc = load(bundle / "data" / "incident-binding-0.json")
    found = (occ["format"]["schemaSha256"], inc["format"]["schemaSha256"], inc["topic"], load(bundle / "submission.json")["streamId"])
    if found != (occupancy_sha, incident_sha, "quality.incidents", stream):
        raise SystemExit(f"{bundle}: Tyr's bundle must carry the real schema digests, quality.incidents and stream {stream}; found {found}")

def bump_rules(bundle: Path) -> None:
    a = bundle / "attribution-rules" / "0.json"; d = load(a); d["version"] = "1.0.1"; d["coverage"]["maxMissingPeriods"] = 4; dump(a, d)
    i = bundle / "impact-rules" / "0.json"; d = load(i); d["version"] = "1.0.1"; d["attributionRuleRef"]["version"] = "1.0.1"; dump(i, d)


def mismatch(bundle: Path) -> None:
    occ = bundle / "data" / "occupancy-binding-0.json"; d = load(occ); d["format"]["schemaSha256"] = MISMATCH_SHA; dump(occ, d)


LIVE = {"dedicated": ("line2-dedicated", "line2-rules-dedicated", "site-071"),
        "dedicated-plc": ("line2-dedicated-plc", "line2-rules-dedicated-plc", "site-071"),
        "shared": ("line2-shared", "line2-rules-shared", "site-019"),
        "shared-plc": ("line2-shared-plc", "line2-rules-shared-plc", "site-019")}
DEFINITIONS = ("station-model", "tool-model")


def check_site(bundle: Path, site: str) -> None:
    roots = [n["equipmentInstanceId"] for n in load(bundle / "hierarchy" / "0.json")["nodes"] if n["parentEquipmentInstanceId"] is None]
    if roots != [site]:
        raise SystemExit(f"{bundle}: Tyr's placement bundle must root its hierarchy at {site}; found {roots}")


def check_definition(pair: Path, ref: str) -> None:
    m = load(pair / "manifest.json")
    found = (m["kind"], m["ref"], m["version"], m["contentSha256"])
    if found != ("recipe-setpoints", ref, "1.0.0", sha((pair / "recipe-setpoints.yaml").read_bytes())):
        raise SystemExit(f"{pair}: Bifrost's published pair must be recipe-setpoints {ref} 1.0.0 over the yaml's bytes; found {found}")


class Gates:
    def __init__(self, jar: Path): self.jar = jar
    def __call__(self, *args: str) -> None:
        r = subprocess.run(["java", "-jar", str(self.jar), *args], capture_output=True, text=True)
        if r.returncode != 0:
            raise SystemExit(f"gates {' '.join(args[:2])} failed ({r.returncode}): {r.stdout}{r.stderr}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bifrost", type=Path, default=ROOT.parent / "bifrost")
    ap.add_argument("--tyr", type=Path, default=ROOT.parent / "tyr" / "fixtures" / "attribution-rule")
    ap.add_argument("--out", type=Path, default=ROOT / "fixtures")
    ap.add_argument("--live", action="store_true", help="write fixtures/live only (step 5); the step-3 fixtures stay untouched")
    a = ap.parse_args()
    gates = Gates(a.bifrost / "gates" / "target" / "bifrost-gates.jar")
    occupancy_sha = sha((ROOT / "schemas" / "occupancy-1.0.0.schema.json").read_bytes())
    incident_sha = sha((ROOT / "schemas" / "incident-1.0.0.schema.json").read_bytes())
    if a.live:
        live(a, gates, occupancy_sha, incident_sha); return
    upstream = a.out / "upstream"
    if upstream.exists(): shutil.rmtree(upstream)
    for name, (stream, _) in STREAMS.items():
        shutil.copytree(a.tyr / name, upstream / name); check_upstream(upstream / name, stream, occupancy_sha, incident_sha)
    dump(upstream / "manifest.json", {"tyr": git_head(a.tyr), "files": inventory(upstream)})
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp); keys = tmp / "keys"; reg = tmp / "registry"; (reg / "identity").mkdir(parents=True)
        lines = []
        for principal in ("proposer", "approver"):
            gates("identity", "keygen", principal, "--out", str(keys))
            lines.append(json.dumps({"principal": principal, "publicKey": (keys / f"{principal}.pub").read_text().strip()}))
        (reg / "identity" / "authorized-keys.jsonl").write_text("\n".join(lines) + "\n", newline="\n")
        rules = []
        for stream in ("line2", "line2-plc", "line2-plc-nocheck"):
            rules += [{"id": f"publish-{stream}", "principal": "proposer", "action": "publish", "target": "mapping-publication", "kind": "mapping-release", "ref": stream},
                      {"id": f"approve-{stream}", "principal": "approver", "action": "approve", "target": "mapping-publication", "kind": "mapping-release", "ref": stream}]
        dump(reg / "identity" / "activation-policy.json", {"version": "1.0.0", "default": "deny", "rules": rules})
        signers = ["--by", "proposer", "--by-key", str(keys / "proposer.key"), "--approved-by", "approver", "--approved-by-key", str(keys / "approver.key")]

        def publish(reg: Path, name: str, stream: str, release: str, version: str, edit=None) -> None:
            bundle = tmp / f"{reg.name}-{release}-{version}"; shutil.copytree(a.tyr / name, bundle)   # unique per release: the plc bundle is published twice (line2-plc and line2-plc-nocheck)
            if edit:   # a forseti-derived variant; without an edit Tyr's bytes are published exactly as copied
                edit(bundle)
                # A second release on a stream must name the current head exactly ({id, version, sha256}) in BOTH
                # proposal.json and submission.json (MappingBundle: submission.base-reference; MappingRegistry.base:
                # mapping.base-conflict compares the whole head object), before the proposal digest is re-pinned.
                head = reg / "mappings" / "heads" / f"{stream}.json"
                base = load(head) if head.exists() else None
                p = load(bundle / "proposal.json"); p["baseRelease"] = base; dump(bundle / "proposal.json", p)
                s = load(bundle / "submission.json"); s["baseRelease"] = base; s["streamId"] = stream; s["version"] = version; dump(bundle / "submission.json", s)
                repin(bundle)
            gates("mapping", "validate", str(bundle))
            gates("mapping", "approve", str(reg), str(bundle), f"approval-{release}-{version}", *signers)
            gates("mapping", "publish", str(reg), str(bundle), release, version, "--approval", f"approval-{release}-{version}", *signers, "--expires-in", "P366D")

        def nocheck(bundle: Path) -> None:
            r = bundle / "attribution-rules" / "0.json"; d = load(r); d["version"] = "1.0.1"; d["identity"]["crossCheck"] = "none"; dump(r, d)
            i = bundle / "impact-rules" / "0.json"; d = load(i); d["version"] = "1.0.1"; d["attributionRuleRef"]["version"] = "1.0.1"; dump(i, d)

        def snapshot(reg: Path, name: str) -> Path:
            out = a.out / name
            if out.exists(): shutil.rmtree(out)
            shutil.copytree(reg, out, ignore=shutil.ignore_patterns(".lock")); return out

        for name, (stream, release) in STREAMS.items(): publish(reg, name, stream, release, "1.0.0")
        publish(reg, "plc-stamped-bundle", "line2-plc-nocheck", "line2-rules-plc-nocheck", "1.0.0", edit=nocheck)
        target = snapshot(reg, "registry")
        publish(reg, "bundle", "line2", "line2-rules", "1.0.1", edit=bump_rules)
        nxt = snapshot(reg, "registry-next")
        # A copy of Tyr's bundle with another occupancy schema digest, in its own registry: Bifrost approves and publishes it,
        # head and release verify, then RuleSet.load refuses binding.schema-mismatch.
        regm = tmp / "registry-mismatch"; (regm / "identity").mkdir(parents=True)
        shutil.copy(reg / "identity" / "authorized-keys.jsonl", regm / "identity" / "authorized-keys.jsonl")
        dump(regm / "identity" / "activation-policy.json", {"version": "1.0.0", "default": "deny", "rules": rules})
        publish(regm, "bundle", "line2", "line2-rules", "1.0.0", edit=mismatch)
        mis = snapshot(regm, "registry-mismatch")
        (a.out / "trusted-keys.jsonl").write_text("\n".join(lines) + "\n", newline="\n")
        dump(a.out / "registry.lock.json", {"bifrost": git_head(a.bifrost), "tyr": git_head(a.tyr),
                                            "occupancySchemaSha256": occupancy_sha, "incidentSchemaSha256": incident_sha,
                                            "files": inventory(target), "next": inventory(nxt), "mismatchRegistry": inventory(mis)})
    bundles = sum(1 for p in upstream.rglob("*") if p.is_file() and p.name != "manifest.json")
    print(f"wrote {upstream} ({bundles} bundle files + manifest), {target} ({len(inventory(target))} files), {nxt} ({len(inventory(nxt))} files), {mis} ({len(inventory(mis))} files)")


def live(a, gates: Gates, occupancy_sha: str, incident_sha: str) -> None:
    out = a.out / "live"; upstream = out / "upstream"; definitions = out / "definitions"; target = out / "registry"
    for d in (upstream, definitions, target):
        if d.exists(): shutil.rmtree(d)
    for name, (stream, _, site) in LIVE.items():
        shutil.copytree(a.tyr / "placement" / name / "bundle", upstream / name)
        check_upstream(upstream / name, stream, occupancy_sha, incident_sha); check_site(upstream / name, site)
    for ref in DEFINITIONS:
        pair = definitions / "recipe" / ref / "1.0.0"
        shutil.copytree(a.bifrost / "scripts" / "fixtures" / "line2" / "published" / "recipe" / ref / "1.0.0", pair); check_definition(pair, ref)
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp); keys = tmp / "keys"; reg = tmp / "registry"; (reg / "identity").mkdir(parents=True)
        lines = []
        for principal in ("proposer", "approver"):
            gates("identity", "keygen", principal, "--out", str(keys))
            lines.append(json.dumps({"principal": principal, "publicKey": (keys / f"{principal}.pub").read_text().strip()}))
        (reg / "identity" / "authorized-keys.jsonl").write_text("\n".join(lines) + "\n", newline="\n")
        rules = []
        for stream, _, _ in LIVE.values():
            rules += [{"id": f"publish-{stream}", "principal": "proposer", "action": "publish", "target": "mapping-publication", "kind": "mapping-release", "ref": stream},
                      {"id": f"approve-{stream}", "principal": "approver", "action": "approve", "target": "mapping-publication", "kind": "mapping-release", "ref": stream}]
        dump(reg / "identity" / "activation-policy.json", {"version": "1.0.0", "default": "deny", "rules": rules})
        signers = ["--by", "proposer", "--by-key", str(keys / "proposer.key"), "--approved-by", "approver", "--approved-by-key", str(keys / "approver.key")]
        for name, (stream, release, _) in LIVE.items():
            bundle = tmp / f"live-{release}-1.0.0"; shutil.copytree(upstream / name, bundle)   # Tyr's bytes, exactly as copied
            gates("mapping", "validate", str(bundle))
            gates("mapping", "approve", str(reg), str(bundle), f"approval-{release}-1.0.0", *signers)
            gates("mapping", "publish", str(reg), str(bundle), release, "1.0.0", "--approval", f"approval-{release}-1.0.0", *signers, "--expires-in", "P366D")
        shutil.copytree(reg, target, ignore=shutil.ignore_patterns(".lock"))
        (out / "trusted-keys.jsonl").write_text("\n".join(lines) + "\n", newline="\n")
    dump(out / "lock.json", {"bifrost": git_head(a.bifrost), "tyr": git_head(a.tyr),
                             "occupancySchemaSha256": occupancy_sha, "incidentSchemaSha256": incident_sha,
                             "upstream": inventory(upstream), "definitions": inventory(definitions), "registry": inventory(target)})
    print(f"wrote {upstream} ({len(inventory(upstream))} files), {definitions} ({len(inventory(definitions))} files), {target} ({len(inventory(target))} files)")


if __name__ == "__main__":
    main()
