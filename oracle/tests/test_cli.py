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
