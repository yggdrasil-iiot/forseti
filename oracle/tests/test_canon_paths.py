"""The oracle's shortcuts give the bytes of the general path: the standard-library encoding of a float-free value is the RFC 8785
form, a refused value is still refused with its reason, and a sample row's kept digest is the digest of the row."""
from __future__ import annotations

import random

import pytest
import rfc8785

from forseti_oracle import canon, stage1

CHARS = ["a", '"', "\\", "\n", "\x00", "\x1f", "\x7f", "\u00e9", "\u2028", "\ud7ff", "\ue000", "\U0001f600", "/", "\t", "\b", "\f", "\r"]


def _value(rng: random.Random, depth: int = 0):
    r = rng.random()
    if depth > 3 or r < 0.5:
        return rng.choice([None, True, False, rng.randint(-2**53 + 1, 2**53 - 1), rng.randint(-5, 5),
                           "".join(rng.choices(CHARS, k=rng.randint(0, 6))), rng.random() * 10 ** rng.randint(-9, 25)])
    if r < 0.75:
        return [_value(rng, depth + 1) for _ in range(rng.randint(0, 4))]
    keys = ["a", "b", "Z", "_", "1", "\u00e9", "\U0001f600", "\ue000"] if rng.random() < 0.2 else ["a", "b", "Z", "_", "1"]
    return {"".join(rng.choices(keys, k=rng.randint(0, 3))): _value(rng, depth + 1) for _ in range(rng.randint(0, 4))}


def test_canonical_bytes_equal_the_library_on_random_values():
    rng = random.Random(20261112)
    for _ in range(20000):
        v = _value(rng)
        assert canon.canonical_bytes(v) == rfc8785.dumps(v), v


@pytest.mark.parametrize("value, reason", [(2**53, "integer-domain"), ([1, -2**53], "integer-domain"), ({"a": "\ud800"}, "not-json"),
                                           ({"\ud800": 1}, "not-json"), (float("nan"), "non-finite"), ([float("inf")], "non-finite")])
def test_refusals_are_unchanged(value, reason):
    with pytest.raises(canon.CanonicalRefusal) as e:
        canon.canonical_bytes(value)
    assert str(e.value).startswith(reason)


class _Member:
    metric = "Torque"


class _Sample:
    def __init__(self, row):
        self.row, self.memo, self.member = row, {}, _Member()


def test_the_kept_row_digest_is_the_digest_of_the_row():
    ref = {"mode": "occupancy-join", "source": "s", "arrivalEventId": "e"}
    s = _Sample({"sourceTs": 5, "state": "attributed", "reason": None, "active": True, "attributedTo": ref})
    assert stage1.row_digest(s, {}) == canon.digest(s.row)
    for row in ({"sourceTs": 5, "state": "attributed", "reason": None, "active": True, "attributedTo": dict(ref, arrivalEventId="f")},
                {"sourceTs": 5, "state": "unattributed", "reason": "no-occupancy", "active": True, "attributedTo": None},
                {"sourceTs": 5, "state": "unattributed", "reason": "no-occupancy", "active": 1, "attributedTo": None},
                {"sourceTs": 5, "state": "unattributed", "reason": "no-occupancy", "active": "unknown", "attributedTo": None}):
        s.row = row
        assert stage1.row_digest(s, {}) == canon.digest(row), row
