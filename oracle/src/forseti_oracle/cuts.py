"""The cut history of a harness run (spec Cuts and the independent oracle, 595-607), read from ``<run>/cuts.json`` and checked against
the recording's batch manifest.

``cuts.json`` is ``{generation, routingDigest, cuts: [{cutId, perStation {station -> {W1, admissionW, W2, mergedCutIds, onlineVisits}},
offsetRanges {topic -> {partition -> [from, to]}}, incidentOffsets [{cutId, partition, throughOffset}]}]}``. Offset ranges are
half-open; through-offsets are inclusive with ``-1`` for "nothing of that partition included yet" (spec 412). The oracle refuses a
history whose cut ids, offset ranges or incident through-offsets differ from the batches the recording's manifest fixes, so the inputs
it evaluates are the inputs of the batches, never a range the run made up.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from forseti_oracle import canon
from forseti_oracle.inputs import SAFE_MAX, SchemaInvalid, safe_int
from forseti_oracle.recording import TOPICS, Recording

INCIDENTS = "quality.incidents"


class Inconsistent(ValueError):
    """The cut history does not match the recording's batches."""


def _int(v, field: str, lo: int) -> int:
    try:
        return safe_int(v, field, lo, SAFE_MAX)
    except SchemaInvalid as e:
        raise Inconsistent(f"schema-invalid: {e.check}") from e


def _optional(v, field: str, lo: int):
    return None if v is None else _int(v, field, lo)


@dataclass(frozen=True)
class StationCut:
    w1: int | None
    admission_w: int | None
    w2: int | None
    merged_cut_ids: tuple
    online_visits: int | None


@dataclass(frozen=True)
class Cut:
    index: int                     # position in the batch manifest, 0-based
    cut_id: int
    per_station: dict              # station -> StationCut
    through: dict                  # topic -> {partition -> inclusive through-offset}, from the manifest
    offset_ranges: dict            # topic -> {partition -> (from, to)} half-open, as the run reported them


@dataclass
class History:
    generation: int
    routing_digest: str
    cuts: list

    def cut_of(self, topic: str, partition: int, offset: int) -> int | None:
        """The index of the cut whose batch includes this record: above the previous cut's through-offset, at or below this one's."""
        for c in self.cuts:
            if offset <= c.through.get(topic, {}).get(partition, -1):
                return c.index
        return None


def through_offsets(recording: Recording) -> list[dict]:
    """Per batch, topic -> {partition (int) -> inclusive through-offset}."""
    out = []
    for b in recording.batches():
        out.append({t: {int(p): v for p, v in b["throughOffsets"].get(t, {}).items()} for t in TOPICS})
    return out


def selected(through: dict, previous: dict | None, topic: str, partition: int) -> range:
    """The record offsets one cut includes on one partition: (previous through, through]; -1 includes nothing."""
    hi = through.get(topic, {}).get(partition, -1)
    lo = -1 if previous is None else previous.get(topic, {}).get(partition, -1)
    return range(lo + 1, hi + 1)


def read(path: Path, recording: Recording) -> History:
    doc = canon.loads(Path(path).read_text(encoding="utf-8"))
    generation = _int(doc.get("generation"), "generation", 1)
    routing = doc.get("routingDigest")
    if routing != recording.routing.get("routingDigest"):
        raise Inconsistent("routingDigest differs from the recording's routing manifest")
    batches = recording.batches()
    throughs = through_offsets(recording)
    by_id = {b["cutId"]: i for i, b in enumerate(batches)}
    cuts: list[Cut] = []
    last_index = -1
    for c in doc.get("cuts", []):
        cut_id = _int(c.get("cutId"), "cutId", 1)
        if cut_id not in by_id:
            raise Inconsistent(f"cut {cut_id} is not a batch of the recording")
        index = by_id[cut_id]
        if index <= last_index:
            raise Inconsistent(f"cut {cut_id} out of order")
        per_station = {}
        for s, v in c.get("perStation", {}).items():
            merged = tuple(_int(x, "mergedCutIds", 1) for x in v.get("mergedCutIds", []))
            # W1, admissionW and W2 are computed times: the full JCS integer domain (a station without telemetry yet reports the
            # lower bound); record offsets and record times stay at or above 0, through-offsets at or above -1
            per_station[s] = StationCut(_optional(v.get("W1"), "W1", -SAFE_MAX), _optional(v.get("admissionW"), "admissionW", -SAFE_MAX),
                                        _optional(v.get("W2"), "W2", -SAFE_MAX), merged, _optional(v.get("onlineVisits"), "onlineVisits", 0))
        merged_all = {m for sc in per_station.values() for m in sc.merged_cut_ids}
        # a cut without merged cuts follows its predecessor in the manifest directly
        if not merged_all and index != last_index + 1:
            raise Inconsistent(f"cut {cut_id} follows a missing cut without mergedCutIds")
        previous = throughs[last_index] if last_index >= 0 else None
        ranges = {}
        for topic, parts in c.get("offsetRanges", {}).items():
            for p, rng in parts.items():
                if not isinstance(rng, list) or len(rng) != 2:
                    raise Inconsistent(f"offsetRanges {topic}/{p}")
                lo, hi = _int(rng[0], "offsetRanges.from", 0), _int(rng[1], "offsetRanges.to", 0)
                expect = selected(throughs[index], previous, topic, int(p))
                if (lo, hi) != (expect.start, expect.stop) and not (lo == hi and len(expect) == 0):
                    raise Inconsistent(f"cut {cut_id} offsetRanges {topic}/{p} [{lo}, {hi}) differ from the batch's [{expect.start}, {expect.stop})")
                ranges.setdefault(topic, {})[int(p)] = (lo, hi)
        for inc in c.get("incidentOffsets", []):
            p = _int(inc.get("partition"), "incidentOffsets.partition", 0)
            t = _int(inc.get("throughOffset"), "incidentOffsets.throughOffset", -1)
            if throughs[index].get(INCIDENTS, {}).get(p, -1) != t:
                raise Inconsistent(f"cut {cut_id} incident through-offset of partition {p} differs from the batch's")
        cuts.append(Cut(index, cut_id, per_station, throughs[index], ranges))
        last_index = index
    return History(generation, routing, cuts)


def from_manifest(recording: Recording, per_station: list[dict]) -> History:
    """A history built from the manifest alone with the given per-station values per batch (tests and the oracle's own runs)."""
    throughs = through_offsets(recording)
    cuts = []
    for i, b in enumerate(recording.batches()):
        ps = {s: StationCut(v.get("W1"), v.get("admissionW"), v.get("W1"), (), None) for s, v in per_station[i].items()}
        cuts.append(Cut(i, b["cutId"], ps, throughs[i], {}))
    return History(1, recording.routing.get("routingDigest"), cuts)


# --- delivered records -----------------------------------------------------------------------------------------------------


def _value_bytes(line: bytes) -> bytes:
    """The raw bytes of a recording line's ``value`` member as delivered: the line is ``{"offset", "timestamp", "key", "headers",
    "value"}`` with ``value`` last; the first ``"value":`` whose prefix closes as an object of the four other members is it."""
    start = 0
    while True:
        i = line.find(b'"value":', start)
        if i < 0:
            raise Inconsistent("a recording line without value")
        try:
            head = canon.loads((line[:i] + b'"value":null}').decode("utf-8"))
            if isinstance(head, dict) and set(head) == {"offset", "timestamp", "key", "headers", "value"} and line.endswith(b"}"):
                return line[i + 8:-1]
        except (canon.CanonicalRefusal, UnicodeDecodeError):
            pass
        start = i + 1


def raw_records(recording: Recording, topic: str, partition: int) -> list[bytes]:
    """The raw value bytes of every record of one subscribed partition, by offset (the recording reader has checked the lines)."""
    recording.records(topic, partition)   # checks offsets, LF and the line shape
    raw = (recording.path / topic / f"{partition}.jsonl").read_bytes()
    return [] if not raw else [_value_bytes(x) for x in raw[:-1].split(b"\n")]


def deliveries(recording: Recording, history: History, through_index: int | None = None, subscribed: dict | None = None) -> list:
    """Every record delivered through the history's cut ``through_index`` (all cuts when None), tagged with the index of its cut.
    ``subscribed`` overrides the routing manifest's partitions per topic (``--subscribe mfg.occupancy=`` reads no occupancy)."""
    from forseti_oracle.stage1 import make_delivery
    out = []
    last = history.cuts[-1].index if through_index is None else through_index
    for topic in TOPICS:
        parts = recording.subscribed(topic)
        if subscribed is not None and topic in subscribed:
            parts = [p for p in parts if p in subscribed[topic]]
        for p in parts:
            for offset, raw in enumerate(raw_records(recording, topic, p)):
                c = history.cut_of(topic, p, offset)
                if c is None or c > last:
                    continue
                out.append(make_delivery(topic, p, offset, c, raw))
    return out


def formula_history(recording: Recording, rs, out_of_orderness: int) -> History:
    """The history the step-3 reference harness reports, computed from the spec's formulas alone (spec 607): per station,
    ``W1(s)`` = the largest valid member ``sourceTs`` of telemetry routed to ``s`` over everything delivered through the cut, minus
    ``outOfOrdernessMs``, minus 1, never decreasing; ``admissionW`` = the previous cut's ``W1``; ``W2 = W1``. A station with no
    telemetry yet reports the lower end of the JCS integer domain, as the step-3 harness does."""
    from forseti_oracle import inputs as inp
    from forseti_oracle.rules import station_of
    throughs = through_offsets(recording)
    raws = {p: raw_records(recording, "ot.messages", p) for p in recording.subscribed("ot.messages")}
    largest: dict = {}
    per_batch = []
    previous: dict = {}
    for i, b in enumerate(recording.batches()):
        for p, lines in raws.items():
            for off in selected(throughs[i], throughs[i - 1] if i else None, "ot.messages", p):
                r = inp.read("ot.messages", lines[off])
                if r.parsed is None:
                    continue
                s = station_of(rs.stations, r.parsed.device)
                ts = [m.sourceTs for m in r.parsed.members if m.sourceTs is not None]
                if s is not None and ts:
                    largest[s] = max(largest.get(s, ts[0]), max(ts))
        row = {}
        for s in rs.stations:
            w = largest[s] - out_of_orderness - 1 if s in largest else None
            prev = previous.get(s)
            if w is None or (prev is not None and w < prev):
                w = prev if prev is not None else -SAFE_MAX
            row[s] = {"W1": w, "admissionW": prev}
        for s in rs.stations:
            previous[s] = row[s]["W1"]
        per_batch.append(row)
    return from_manifest(recording, per_batch)
