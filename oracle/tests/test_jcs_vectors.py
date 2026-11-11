import json
from pathlib import Path

import pytest

from forseti_oracle.canon import CanonicalRefusal, canonical_bytes, loads, sha256

VECTORS = json.loads((Path(__file__).resolve().parents[2] / "vectors" / "jcs-vectors.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("v", VECTORS["vectors"], ids=[v["id"] for v in VECTORS["vectors"]])
def test_vector(v):
    c = canonical_bytes(loads(v["inputText"]))
    assert c.decode("utf-8") == v["canonical"]
    assert sha256(c) == v["sha256"]


@pytest.mark.parametrize("r", VECTORS["refusals"], ids=[r["id"] for r in VECTORS["refusals"]])
def test_refusal(r):
    # Python's json accepts NaN/Infinity literals; forseti's pre-check refuses them with the pinned reason.
    with pytest.raises(CanonicalRefusal, match=r["reason"]):
        canonical_bytes(loads(r["inputText"]))


def test_vector_count_matches_java_side():
    assert len(VECTORS["vectors"]) == 9 and len(VECTORS["refusals"]) == 8
