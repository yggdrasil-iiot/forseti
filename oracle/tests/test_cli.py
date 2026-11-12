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
    folded = fold_cuts(tmp_path / "out", [1, 2, 3])
    (v,) = folded[3]["forseti.work-records"].values()
    assert v["body"]["status"] == "provisional" and len(v["bodyDigest"]) == 64
    meta = json.loads((tmp_path / "out" / "oracle-run.json").read_text())
    assert meta["cuts"] == [1, 2, 3] and meta["rulesDigest"] == "de19f15a65f26fa6d75813c1f834df5c6b3a2b335f32e002e9ee6e97a929ab6a"


def fold_cuts(out, cut_ids):
    """The full table sets of every cut, folded from the cut files (a snapshot replaces the set, a delta applies to the previous cut's),
    each checked against its count and setHash with every body digest recomputed."""
    from forseti_oracle import canon
    state, previous, folded = {}, None, {}
    for k in cut_ids:
        folded[k] = {}
        for f in sorted((out / f"cut-{k}").iterdir()):
            doc = json.loads(f.read_text(encoding="utf-8"))
            assert doc["cutId"] == k and doc["table"] == f.stem
            if doc["mode"] == "snapshot":
                assert doc["previousCutId"] is None and doc["deletes"] == []
                rows = {}
            else:
                assert doc["previousCutId"] == previous
                rows = dict(state[f.stem])
                for key in doc["deletes"]:
                    del rows[key]
            for key, v in doc["upserts"].items():
                assert v["bodyDigest"] == canon.digest(v["body"])
                rows[key] = v
            assert doc["count"] == len(rows)
            assert doc["setHash"] == cli.set_hash({key: canon.digest(v["body"]) for key, v in rows.items()})
            state[f.stem] = folded[k][f.stem] = rows
        previous = k
    return folded


def _all(tmp_path, smoke, out, *extra):
    argv = ["all", "--recording", str(smoke), "--cuts", str(tmp_path / "cuts.json"), "--mappings", str(REGISTRY),
            "--release", "line2-rules@1.0.0", "--rule", "line2-attribution@1.0.0", "--definitions", str(smoke / "manifest.json"),
            "--out", str(out), *extra]
    return run(argv)


def test_the_delta_stream_folds_to_the_snapshots_at_every_cut_and_no_memo_changes_no_byte(tmp_path):
    from forseti_oracle import recording as recmod
    smoke = ROOT / "fixtures" / "recordings" / "smoke-incident"
    rec = recmod.open_recording(smoke)
    doc = _cut_log(rec, len(rec.batches()))
    (tmp_path / "cuts.json").write_text(json.dumps(doc))
    ids = [c["cutId"] for c in doc["cuts"]]
    assert _all(tmp_path, smoke, tmp_path / "delta")[0] == 0
    assert _all(tmp_path, smoke, tmp_path / "snap", "--snapshots")[0] == 0
    assert _all(tmp_path, smoke, tmp_path / "nomemo", "--no-memo")[0] == 0
    deltas, snaps = fold_cuts(tmp_path / "delta", ids), {}
    for k in ids:
        snaps[k] = fold_cuts(tmp_path / "snap", [k])[k]
        assert deltas[k] == snaps[k], k
    assert any(json.loads((tmp_path / "delta" / f"cut-{ids[-1]}" / f"{t}.json").read_text())["mode"] == "delta" for t in cli.TABLES)
    assert sum(len(v) for v in snaps[ids[-1]].values()) > 0
    for k in ids:
        for t in cli.TABLES:
            assert (tmp_path / "delta" / f"cut-{k}" / f"{t}.json").read_bytes() == (tmp_path / "nomemo" / f"cut-{k}" / f"{t}.json").read_bytes()
    # --cut-ids recomputes only the cuts asked for, as snapshots equal to the folded stream's
    assert _all(tmp_path, smoke, tmp_path / "some", "--snapshots", "--cut-ids", f"{ids[1]},{ids[-1]}")[0] == 0
    assert sorted(p.name for p in (tmp_path / "some").iterdir() if p.is_dir()) == sorted([f"cut-{ids[1]}", f"cut-{ids[-1]}"])
    assert fold_cuts(tmp_path / "some", [ids[-1]])[ids[-1]] == deltas[ids[-1]]


class Consumer:
    """The gate's side of --stream: reads each announced cut, deletes it and acknowledges; ``stop_after`` closes the stream early."""

    def __init__(self, out, announced, stop_after=None):
        self.out, self.announced, self.stop_after, self.seen = out, announced, stop_after, []

    def readline(self):
        k = int(self.announced.getvalue().splitlines()[-1].split()[1])
        if self.stop_after is not None and len(self.seen) >= self.stop_after:
            return ""
        assert sorted(p.name for p in self.out.iterdir() if p.name.startswith("cut-")) == [f"cut-{k}"]   # one cut on disk
        import shutil
        shutil.rmtree(self.out / f"cut-{k}")
        self.seen.append(k)
        return f"ok {k}\n"


def test_the_stream_holds_one_cut_on_disk_and_stops_when_the_consumer_does(tmp_path):
    from forseti_oracle import recording as recmod
    smoke = ROOT / "fixtures" / "recordings" / "smoke-incident"
    rec = recmod.open_recording(smoke)
    (tmp_path / "cuts.json").write_text(json.dumps(_cut_log(rec, 4)))
    a = cli.parse(["all", "--recording", str(smoke), "--cuts", str(tmp_path / "cuts.json"), "--mappings", str(REGISTRY),
                   "--release", "line2-rules@1.0.0", "--rule", "line2-attribution@1.0.0", "--definitions", str(smoke / "manifest.json"),
                   "--out", str(tmp_path / "s"), "--stream"])
    announced, err = io.StringIO(), io.StringIO()
    c = Consumer(tmp_path / "s", announced)
    assert cli.run(a, announced, err, c) == 0, err.getvalue()
    assert c.seen == [1, 2, 3, 4] and [x for x in announced.getvalue().splitlines() if x.startswith("cut ")] == ["cut 1", "cut 2", "cut 3", "cut 4"]
    assert not [p for p in (tmp_path / "s").iterdir() if p.name.startswith("cut-")]
    a.out = tmp_path / "t"
    announced, err = io.StringIO(), io.StringIO()
    assert cli.run(a, announced, err, Consumer(tmp_path / "t", announced, stop_after=2)) == 1
    assert '"refusal": "stream"' in err.getvalue()


def test_cut_ids_need_snapshots():
    with pytest.raises(SystemExit) as e:
        cli.parse(["all", "--recording", "r", "--cuts", "c", "--mappings", "m", "--release", "a@1", "--rule", "b@1", "--definitions", "d",
                   "--out", "o", "--cut-ids", "1"])
    assert e.value.code == 2


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
