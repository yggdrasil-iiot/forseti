import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from forseti_oracle import cli

ROOT = Path(__file__).resolve().parents[2]
VECTORS = ROOT / "vectors" / "jcs-vectors.json"
REGISTRY = ROOT / "fixtures" / "registry" / "mappings"

# The rulesDigest of each fixture rule with definitions {} (test_rules.py and the Java RuleSetTest pin the same literals).
DIGESTS = [
    ("registry", "line2-rules@1.0.0", "line2-attribution@1.0.0", "eebc7b50e48d67bd50a16fd8dcb5071d9b5262c67535b260ee78d9c68889b540"),
    ("registry", "line2-rules-plc@1.0.0", "line2-attribution@1.0.0", "83b158de38f98cfc0d51782b5a711e173ecc712c0cc5210347cd0f921b8f3381"),
    ("registry-next", "line2-rules@1.0.1", "line2-attribution@1.0.1", "7b7a2e369dd70829a71f7830a82725bd0700c7fed02b86cc483abe915ac3cb76"),
]


def run(argv):
    out, err = io.StringIO(), io.StringIO()
    old = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = out, err
    try:
        code = cli.main(argv)
    finally:
        sys.stdout, sys.stderr = old
    return code, out.getvalue(), err.getvalue()


def test_canon_check_passes_on_the_shared_vector_file():
    code, out, _ = run(["canon-check", str(VECTORS)])
    assert code == 0
    assert out.splitlines()[-1] == "canon-check: vectors 9, refusals 8, problems 0"


def _variant(tmp_path, edit):
    doc = json.loads(VECTORS.read_text(encoding="utf-8"))
    edit(doc)
    f = tmp_path / "vectors.json"
    f.write_text(json.dumps(doc), encoding="utf-8")
    return f


def test_canon_check_fails_on_a_wrong_digest(tmp_path):
    f = _variant(tmp_path, lambda d: d["vectors"][0].update(sha256="0" * 64))
    code, out, _ = run(["canon-check", str(f)])
    assert code == 1
    assert "vector empty-object: sha256 differs" in out


def test_canon_check_fails_when_a_refusal_is_accepted(tmp_path):
    f = _variant(tmp_path, lambda d: d["refusals"][0].update(inputText="1"))
    code, out, _ = run(["canon-check", str(f)])
    assert code == 1
    assert "accepted" in out


def test_canon_check_fails_when_a_refusal_has_another_reason(tmp_path):
    f = _variant(tmp_path, lambda d: d["refusals"][0].update(reason="non-finite"))
    code, out, _ = run(["canon-check", str(f)])
    assert code == 1
    assert "expected non-finite" in out


def test_canon_check_fails_on_an_empty_file(tmp_path):
    f = _variant(tmp_path, lambda d: d.update(vectors=[], refusals=[]))
    assert run(["canon-check", str(f)])[0] == 1


@pytest.mark.parametrize("registry,release,rule,digest", DIGESTS, ids=[d[1] for d in DIGESTS])
def test_rules_digest_prints_the_pinned_digest(registry, release, rule, digest):
    code, out, _ = run(["rules-digest", "--mappings", str(ROOT / "fixtures" / registry / "mappings"), "--release", release, "--rule", rule])
    assert (code, out) == (0, digest + "\n")


def test_rules_digest_reports_a_schema_mismatch():
    code, out, err = run(["rules-digest", "--mappings", str(ROOT / "fixtures" / "registry-mismatch" / "mappings"),
                          "--release", "line2-rules@1.0.0", "--rule", "line2-attribution@1.0.0"])
    assert (code, out) == (1, "")
    refusal = json.loads(err)
    assert refusal["refusal"] == "binding.schema-mismatch"
    assert refusal["evidence"]["approved"] == "0" * 63 + "1"


def test_rules_digest_reports_a_missing_rule_version():
    code, _, err = run(["rules-digest", "--mappings", str(REGISTRY), "--release", "line2-rules@1.0.0", "--rule", "line2-attribution@9.9.9"])
    assert code == 1
    assert json.loads(err)["refusal"] == "provenance-mismatch"


@pytest.mark.parametrize("argv", [[], ["nope"], ["rules-digest", "--mappings", "m", "--release", "line2-rules", "--rule", "r@1"],
                                  ["canon-check"]])
def test_usage_errors_exit_2(argv):
    with pytest.raises(SystemExit) as e:
        run(argv)
    assert e.value.code == 2


def test_the_module_runs_isolated_as_the_gate_launches_it():
    p = subprocess.run([sys.executable, "-I", "-m", "forseti_oracle.cli", "rules-digest", "--mappings", str(REGISTRY),
                        "--release", "line2-rules@1.0.0", "--rule", "line2-attribution@1.0.0"],
                       capture_output=True, text=True, cwd=ROOT, timeout=120)
    assert (p.returncode, p.stdout.strip()) == (0, DIGESTS[0][3])


def _cut_log(rec, n):
    """A cut log for the first n batches of a recording with the harness formulas (W1 per station, admissionW the previous W1)."""
    from forseti_oracle import cuts as cutlog, recording as recmod, rules as rulemod
    rs = rulemod.rule_set(rulemod.load_release(REGISTRY, "line2-rules", "1.0.0"), "line2-attribution", "1.0.0")
    h = cutlog.formula_history(rec, rs, 30_000)
    throughs = cutlog.through_offsets(rec)
    out = []
    for i, c in enumerate(h.cuts[:n]):
        ranges = {t: {str(p): [cutlog.selected(throughs[i], throughs[i - 1] if i else None, t, p).start,
                               cutlog.selected(throughs[i], throughs[i - 1] if i else None, t, p).stop] for p in rec.subscribed(t)}
                  for t in recmod.TOPICS}
        out.append({"cutId": c.cut_id, "perStation": {s: {"W1": v.w1, "admissionW": v.admission_w, "W2": v.w1, "mergedCutIds": [],
                                                           "onlineVisits": 0} for s, v in c.per_station.items()},
                    "offsetRanges": ranges, "incidentOffsets": []})
    return {"generation": 1, "routingDigest": rec.routing["routingDigest"], "cuts": out}


def test_all_writes_every_cut_of_the_cut_log(tmp_path):
    from forseti_oracle import recording as recmod
    smoke = ROOT / "fixtures" / "recordings" / "smoke-incident"
    rec = recmod.open_recording(smoke)
    (tmp_path / "cuts.json").write_text(json.dumps(_cut_log(rec, 3)))
    code, out, err = run(["all", "--recording", str(smoke), "--cuts", str(tmp_path / "cuts.json"), "--mappings", str(REGISTRY),
                          "--release", "line2-rules@1.0.0", "--rule", "line2-attribution@1.0.0", "--definitions", str(smoke / "manifest.json"),
                          "--out", str(tmp_path / "out")])
    assert (code, err) == (0, ""), err
    assert out.startswith("cuts 3, rows ")
    names = sorted(p.name for p in (tmp_path / "out" / "cut-3").iterdir())
    assert names == ["dispositions-stage1.json", "dispositions-stage2.json", "forseti.affected-sets.json", "forseti.affected.json",
                     "forseti.samples.json", "forseti.work-records.json"]
    wr = json.loads((tmp_path / "out" / "cut-3" / "forseti.work-records.json").read_text())
    (v,) = wr.values()
    assert v["body"]["status"] == "provisional" and len(v["bodyDigest"]) == 64
    meta = json.loads((tmp_path / "out" / "oracle-run.json").read_text())
    assert meta["cuts"] == [1, 2, 3] and meta["rulesDigest"] == "de19f15a65f26fa6d75813c1f834df5c6b3a2b335f32e002e9ee6e97a929ab6a"


def test_all_refuses_a_cut_log_that_differs_from_the_batches(tmp_path):
    from forseti_oracle import recording as recmod
    smoke = ROOT / "fixtures" / "recordings" / "smoke-incident"
    doc = _cut_log(recmod.open_recording(smoke), 2)
    doc["cuts"][1]["offsetRanges"]["ot.messages"]["1"] = [0, 1]
    (tmp_path / "cuts.json").write_text(json.dumps(doc))
    code, out, err = run(["stage1", "--recording", str(smoke), "--cuts", str(tmp_path / "cuts.json"), "--mappings", str(REGISTRY),
                          "--release", "line2-rules@1.0.0", "--rule", "line2-attribution@1.0.0", "--definitions", str(smoke / "manifest.json"),
                          "--out", str(tmp_path / "out")])
    assert code == 1 and "offsetRanges" in err
