"""RFC 8785 bytes and SHA-256 of a JSON value, with forseti's refusals applied before the library (rfc8785 0.1.4)."""
from __future__ import annotations

import hashlib
import json
import math

import rfc8785

MAX_SAFE = 9007199254740991


class CanonicalRefusal(ValueError):
    pass


def _no_duplicates(pairs):
    d = {}
    for k, v in pairs:
        if k in d:
            raise CanonicalRefusal("not-json: duplicate key")
        d[k] = v
    return d


def loads(text: str):
    """The oracle's strict parser: duplicate keys refused (Python's json keeps the last one by default)."""
    try:
        return json.loads(text, object_pairs_hook=_no_duplicates)
    except json.JSONDecodeError as e:
        raise CanonicalRefusal(f"not-json: {e.msg}") from e


def _check(v) -> None:
    if isinstance(v, bool):
        return
    if isinstance(v, int):
        if abs(v) > MAX_SAFE:
            raise CanonicalRefusal("integer-domain")
    elif isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            raise CanonicalRefusal("non-finite")
    elif isinstance(v, str):
        try:
            v.encode("utf-8")
        except UnicodeEncodeError:
            raise CanonicalRefusal("not-json: lone surrogate")
    elif isinstance(v, dict):
        for k, x in v.items():
            _check(k)
            _check(x)
    elif isinstance(v, list):
        for x in v:
            _check(x)


def _plain(v) -> bool:
    """True when ``v`` holds no float, every integer is inside the JCS domain and every key is an ASCII string: for such a value
    the standard library's sorted compact encoding is byte for byte the RFC 8785 form (ASCII keys sort the same by code point as by
    UTF-16 unit; the two escape the same characters the same way; integers and literals print alike)."""
    t = type(v)
    if t is str or t is bool or v is None:
        return True
    if t is int:
        return -MAX_SAFE <= v <= MAX_SAFE
    if t is dict:
        for k, x in v.items():
            if type(k) is not str or not k.isascii():
                return False
            tx = type(x)   # the scalar cases inline: most members are scalars
            if tx is str or tx is bool or x is None:
                continue
            if tx is int:
                if -MAX_SAFE <= x <= MAX_SAFE:
                    continue
                return False
            if not _plain(x):
                return False
        return True
    if t is list or t is tuple:
        for x in v:
            if not _plain(x):
                return False
        return True
    return False


_ENCODE = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode


def canonical_bytes(value) -> bytes:
    if _plain(value):
        try:
            return _ENCODE(value).encode("utf-8")
        except UnicodeEncodeError:
            pass   # a lone surrogate: the general path refuses it
    _check(value)
    return rfc8785.dumps(value)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest(value) -> str:
    return sha256(canonical_bytes(value))
