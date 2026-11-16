"""Reads a recording directory (spec Cut barriers; plan Task 8) without any Java: manifest, routing, the records of one topic
partition, the raw-file digests and the routing digest recomputed with the oracle's own canonicalizer. Kafka's default
partitioner (murmur2, sign bit cleared, modulo the partition count) is written here from the algorithm, so the oracle checks the
raw placement the routing manifest pins on its own."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from forseti_oracle import canon

MAX_SAFE = 9007199254740991
TOPICS = ("ot.messages", "ot.sessions", "mfg.occupancy", "quality.incidents")
MANIFEST, ROUTING, EXPECTED = "manifest.json", "routing.json", "expected.json"
SAMPLE_DIR = "expected-samples"


class Malformed(ValueError):
    pass


def _int(v, field: str, lo: int, hi: int) -> int:
    if isinstance(v, bool) or not isinstance(v, int) or v < lo or v > hi:
        raise Malformed(f"schema-invalid: {field}")
    return v


def non_negative(v, field: str) -> int:
    return _int(v, field, 0, MAX_SAFE)


def through(v, field: str) -> int:
    return _int(v, field, -1, MAX_SAFE)


def murmur2(data: bytes) -> int:
    """Kafka's Utils.murmur2 as a signed 32-bit int."""
    m, r, mask = 0x5BD1E995, 24, 0xFFFFFFFF
    length = len(data)
    h = (0x9747B28C ^ length) & mask
    for i in range(length // 4):
        k = int.from_bytes(data[4 * i:4 * i + 4], "little")
        k = (k * m) & mask
        k ^= k >> r
        k = (k * m) & mask
        h = (h * m) & mask
        h ^= k
    tail = data[length & ~3:]
    if len(tail) == 3:
        h ^= tail[2] << 16
    if len(tail) >= 2:
        h ^= tail[1] << 8
    if len(tail) >= 1:
        h ^= tail[0]
        h = (h * m) & mask
    h ^= h >> 13
    h = (h * m) & mask
    h ^= h >> 15
    return h - (1 << 32) if h & 0x80000000 else h


def default_partition(key: str, partitions: int) -> int:
    return (murmur2(key.encode("utf-8")) & 0x7FFFFFFF) % partitions


@dataclass(frozen=True)
class Record:
    offset: int
    timestamp: int | None
    key: str
    headers: dict
    value: object


class Recording:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.manifest = canon.loads((self.path / MANIFEST).read_text(encoding="utf-8"))
        self.routing = canon.loads((self.path / ROUTING).read_text(encoding="utf-8"))
        self._cache: dict = {}
        m = self.manifest
        if m.get("recordingVersion") != 1:
            raise Malformed("recordingVersion")
        self.t0 = non_negative(m.get("T0"), "T0")
        self.scan_ms = non_negative(m.get("scanMs"), "scanMs")
        self.tail_ms = non_negative(m.get("tailMs"), "tailMs")
        for device, rng in m["evidenceRange"].items():
            if non_negative(rng.get("from"), "evidenceRange.from") > non_negative(rng.get("to"), "evidenceRange.to"):
                raise Malformed(f"evidenceRange {device}")
        previous = 0
        for b in m["batches"]:
            cut = non_negative(b.get("cutId"), "cutId")
            if cut <= previous:
                raise Malformed(f"cutId {cut} not increasing")
            previous = cut
            for topic in TOPICS:
                parts = b["throughOffsets"].get(topic, {})
                if sorted(int(p) for p in parts) != self.subscribed(topic):
                    raise Malformed(f"throughOffsets.{topic} of cut {cut}")
                for p, v in parts.items():
                    through(v, f"throughOffsets.{topic}.{p}")
        if self.routing_digest() != self.routing.get("routingDigest"):
            raise Malformed("routingDigest differs from the recomputed digest")
        self.replay_of, self.replay_out_of_orderness_ms = self._replay_source(m)

    @staticmethod
    def _replay_source(m: dict):
        """The replay fields of ``source`` (spec Event-time replay): ``replayOf`` (the raw SHA-256 of the original manifest),
        ``outOfOrdernessMs`` and the deadline proofs ``deadlines``, whose cut ids are those of the manifest's ``deadlines``. A
        recording without ``replayOf`` is an original: (None, None). A replay recording is otherwise read like any recording."""
        src = m.get("source")
        if not isinstance(src, dict) or "replayOf" not in src:
            return None, None
        of = src["replayOf"]
        if not isinstance(of, str) or len(of) != 64 or any(ch not in "0123456789abcdef" for ch in of):
            raise Malformed("source.replayOf")
        oo = non_negative(src.get("outOfOrdernessMs"), "source.outOfOrdernessMs")
        proofs = src.get("deadlines")
        if not isinstance(proofs, list):
            raise Malformed("source.deadlines")
        planned = [(d.get("kind"), d.get("ref"), d.get("at"), d.get("cutA"), d.get("cutB")) for d in m.get("deadlines", [])]
        proved = []
        for d in proofs:
            if not isinstance(d, dict) or not isinstance(d.get("stations"), list) or not d["stations"]:
                raise Malformed("source.deadlines stations")
            for f in ("at", "originalCutA", "originalCutB", "cutA", "cutB"):
                non_negative(d.get(f), f"source.deadlines.{f}")
            for f in ("w1A", "w1B"):
                if not isinstance(d.get(f), dict) or sorted(d[f]) != sorted(d["stations"]):
                    raise Malformed(f"source.deadlines.{f}")
                for v in d[f].values():
                    _int(v, f"source.deadlines.{f}", -MAX_SAFE, MAX_SAFE)
            proved.append((d.get("kind"), d.get("ref"), d.get("at"), d.get("cutA"), d.get("cutB")))
        if proved != planned:
            raise Malformed("source.deadlines differ from the manifest's deadlines")
        return of, oo

    def subscribed(self, topic: str) -> list[int]:
        return list(self.routing["subscribed"].get(topic, []))

    def routing_digest(self) -> str:
        """JCS SHA-256 of routing.json without its routingDigest field."""
        body = {k: v for k, v in self.routing.items() if k != "routingDigest"}
        return canon.digest(body)

    def batches(self) -> list[dict]:
        return list(self.manifest["batches"])

    def records(self, topic: str, partition: int) -> list[Record]:
        if partition not in self.subscribed(topic):
            raise Malformed(f"partition not subscribed: {topic}/{partition}")
        key = (topic, partition)
        if key not in self._cache:
            self._cache[key] = self._read(self.path / topic / f"{partition}.jsonl", f"{topic}/{partition}")
        return self._cache[key]

    def check(self, topic: str, partition: int) -> None:
        """The checks of ``records`` (offsets, LF, the line shape) without keeping the records: a large recording is read once."""
        if partition not in self.subscribed(topic):
            raise Malformed(f"partition not subscribed: {topic}/{partition}")
        if (topic, partition) not in self._cache:
            self._read(self.path / topic / f"{partition}.jsonl", f"{topic}/{partition}")

    @staticmethod
    def _read(file: Path, name: str) -> list[Record]:
        raw = file.read_bytes()
        out: list[Record] = []
        if not raw:
            return out
        if b"\r" in raw or not raw.endswith(b"\n"):
            raise Malformed(f"{name} is not LF-terminated lines")
        for line in raw[:-1].split(b"\n"):
            d = canon.loads(line.decode("utf-8"))
            if not isinstance(d, dict) or set(d) != {"offset", "timestamp", "key", "headers", "value"}:
                raise Malformed(f"{name} line {len(out)}")
            offset = non_negative(d["offset"], "offset")
            if offset != len(out):
                raise Malformed(f"{name} offset {offset} where {len(out)} is due")
            ts = None if d["timestamp"] is None else non_negative(d["timestamp"], "timestamp")
            if not isinstance(d["key"], str) or not isinstance(d["headers"], dict):
                raise Malformed(f"{name} line {offset} key or headers")
            out.append(Record(offset, ts, d["key"], d["headers"], d["value"]))
        return out

    def expected(self) -> dict:
        """The ground truth in the version-1 form, from either layout (see ``read_expected``)."""
        return read_expected(self.path)

    def verify_digests(self) -> None:
        listed = dict(self.manifest["digests"])
        present = sorted(p.relative_to(self.path).as_posix() for p in self.path.rglob("*") if p.is_file())
        present.remove(MANIFEST)
        if present != sorted(listed):
            raise Malformed(f"files {present} differ from the manifest's digests")
        for name, sha in listed.items():
            actual = hashlib.sha256((self.path / name).read_bytes()).hexdigest()
            if actual != sha:
                raise Malformed(f"digest mismatch: {name}")


def chunk_name(i: int) -> str:
    return f"{SAMPLE_DIR}/{i:04d}.json"


def read_expected(path) -> dict:
    """``expected.json`` of a recording in the version-1 form. Version 1 is returned as read. Version 2 keeps every field but
    ``samples``; ``sampleFiles`` lists ``expected-samples/0000.json, 0001.json, ...``, each ``{"chunk": i, "samples": {...}}``,
    every chunk but the last holding ``samplesPerFile`` samples, the last 1 to ``samplesPerFile``, ``sampleCount`` in all; the
    chunks are joined in order into ``samples`` at the place of ``sampleFiles``. Anything else is ``Malformed``."""
    path = Path(path)
    e = canon.loads((path / EXPECTED).read_text(encoding="utf-8"))
    if not isinstance(e, dict):
        raise Malformed("expected.json is not an object")
    version = e.get("expectedVersion")
    if version == 1 and not isinstance(version, bool):
        if (path / SAMPLE_DIR).exists():
            raise Malformed(f"a version-1 expected.json with {SAMPLE_DIR}/ present")
        return e
    if version != 2 or isinstance(version, bool):
        raise Malformed(f"expectedVersion {version!r}")
    if "samples" in e or "samplesEncoding" in e:
        raise Malformed("version 2 carries samples or samplesEncoding inline")
    names = e.get("sampleFiles")
    if not isinstance(names, list):
        raise Malformed("sampleFiles is not an array")
    per = non_negative(e.get("samplesPerFile"), "samplesPerFile")
    count = non_negative(e.get("sampleCount"), "sampleCount")
    if per < 1:
        raise Malformed("samplesPerFile 0")
    for i, n in enumerate(names):
        if n != chunk_name(i):
            raise Malformed(f"sampleFiles[{i}] is {n!r}, not {chunk_name(i)}")
    sd = path / SAMPLE_DIR
    present = sorted(p.relative_to(path).as_posix() for p in sd.rglob("*")) if sd.exists() else []
    if present != sorted(names):
        raise Malformed(f"sample files present {present} differ from sampleFiles {names}")
    samples: dict = {}
    for i, n in enumerate(names):
        c = canon.loads((path / n).read_text(encoding="utf-8"))
        if not isinstance(c, dict) or set(c) != {"chunk", "samples"} or not isinstance(c["samples"], dict):
            raise Malformed(f"{n} is not {{chunk, samples}}")
        if non_negative(c["chunk"], "chunk") != i:
            raise Malformed(f"{n} says chunk {c['chunk']}, out of order")
        k = len(c["samples"])
        if (not 1 <= k <= per) if i == len(names) - 1 else k != per:
            raise Malformed(f"{n} holds {k} samples ({per} per file)")
        for key, value in c["samples"].items():
            if key in samples:
                raise Malformed(f"{n} repeats sample {key}")
            samples[key] = value
    if len(samples) != count:
        raise Malformed(f"the sample files hold {len(samples)} samples, sampleCount {count}")
    out: dict = {}
    for f, v in e.items():
        if f == "expectedVersion":
            out[f] = 1
        elif f == "sampleFiles":
            out["samples"] = samples
        elif f not in ("samplesPerFile", "sampleCount"):
            out[f] = v
    return out


def open_recording(path) -> Recording:
    return Recording(Path(path))
