"""Stage 1 of the oracle on inline inputs typed from the rule sheet S1-S11 (occupancy-join) and on the smoke recording; mirrors the
cases of core's Stage1OccupancyJoinTest without using any Java output. The two literals (smoke samplesDigest, Station2 body digest)
are the values core's test pins; the oracle reaches them on its own."""
from __future__ import annotations

import json
import random

import pytest

from forseti_oracle import cuts, recording, stage1
from forseti_oracle.inputs import MESSAGES, OCCUPANCY, SESSIONS

from .driver import (BAD, DID_NOT_OCCUR, FIXTURES, H, INCORRECT_DATA, L, NR1, NR2, S1, S2, T, UNCERTAIN, Driver, arrival, declared,
                     departure, digest, entry, evidence, member, ruleset, session, visit_ref)

U1, U2, U3 = "UNIT0000000000001", "UNIT0000000000002", "UNIT0000000000003"
SMOKE_SAMPLES_DIGEST = "e518a18ba8c533fb5e3fb00529aaf1ebff54d67a07c810a5012f7fe1a926096f"
STATION2_BODY_DIGEST = "eb403937f1f8d2340f8410c8ef345bbea9b94e1a142250798992b42b311c727c"
STATION2_SAMPLES_DIGEST = "e4792442b3c395e2190fdb776d25f519daecf7f416c096bcf3af098ae3788409"
RULES_DIGEST = "de19f15a65f26fa6d75813c1f834df5c6b3a2b335f32e002e9ee6e97a929ab6a"


def baseline(d=None, eid="occ-1", unit=U1, lo=T, settle=True):
    d = d or Driver()
    d.input(OCCUPANCY, arrival(eid, lo, unit))
    d.scans(lo, lo + 60_000)
    d.input(OCCUPANCY, departure(eid + "d", lo + 60_000, unit, S1, eid))
    d.cut()
    if settle:
        d.cut(lo + 60_000 + L)
    return d


def test_the_lenient_oracle_reports_a_provisional_visit_complete():
    """The lenient-oracle negative control (--lenient): the same inputs, the visit reported complete before its settlement."""
    d = baseline(Driver(lenient=True), settle=False)
    assert d.work("occ-1")["status"] == "complete"
    assert baseline(settle=False).work("occ-1")["status"] == "provisional"


def test_a_visit_is_provisional_then_complete_at_settlement():
    d = baseline(settle=False)
    assert d.work("occ-1")["status"] == "provisional"
    d.cut(T + 60_000 + L - 1)
    assert d.work("occ-1")["status"] == "provisional"
    d.cut(T + 60_000 + L)
    w = d.work("occ-1")
    assert w["status"] == "complete" and w["insufficiencyReasons"] == []
    assert w["interval"] == {"enter": T, "leave": T + 60_000, "attributionEnd": None}
    assert w["rulesDigest"] == RULES_DIGEST and "retraction" not in w


def test_samples_before_their_arrival_are_attributed_later():
    d = Driver()
    early = d.scan(T + 10_000)
    d.scans(T + 11_000, T + 60_000)
    d.cut()
    assert d.sample(early[1], "Torque")["reason"] == "no-occupancy"
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.cut()
    row = d.sample(early[1], "Torque")
    assert row["state"] == "attributed" and row["attributedTo"] == visit_ref("occ-1")
    assert entry(d.work("occ-1"), "nutrunner1-torque")["count"] == 50


def test_an_identical_redelivery_is_a_duplicate_with_no_state_change():
    a = baseline()
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    again = d.input(OCCUPANCY, arrival("occ-1", T, U1) | {"recordTime": T + 999})
    d.scans(T, T + 60_000)
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.cut()
    d.cut(T + 60_000 + L)
    assert d.dispositions(again) == [("event", "duplicate", "identical-redelivery")]
    assert digest(d.work("occ-1")) == digest(a.work("occ-1"))


def test_a_resent_arrival_with_another_unit_is_a_conflict():
    d = Driver()
    first = d.input(OCCUPANCY, arrival("occ-1", T, U1))
    d.scans(T, T + 60_000)
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.cut()
    second = d.input(OCCUPANCY, arrival("occ-1", T, U2))
    d.cut(T + 60_000 + L)
    assert d.dispositions(first) == [("event", "conflict", "identity-conflict")]
    assert d.dispositions(second) == [("event", "conflict", "identity-conflict")]
    w = d.work("occ-1")
    assert w["status"] == "retracted" and w["retraction"] == {"reason": "input-conflict", "errorReason": None}
    assert len(d.result.quarantine) == 2


def test_an_error_declaration_retracts_the_visit():
    d = baseline()
    decl = d.input(OCCUPANCY, declared(arrival("occ-1", T, U1), DID_NOT_OCCUR, T + 100_000))
    d.cut(T + 60_000 + L)
    assert d.dispositions(decl) == [("errorDeclaration", "accepted", "late-after-settlement")]
    w = d.work("occ-1")
    assert w["status"] == "retracted" and w["retraction"] == {"reason": "error-declaration", "errorReason": DID_NOT_OCCUR}
    # a retracted visit keeps the evidence of what happened: its samples stay attributed to it (no other candidate)
    assert entry(w, "nutrunner1-torque")["count"] == 60


def test_declaration_before_original_in_both_orders():
    """10b: the same final body whatever the order; the declaration is ``part errorDeclaration``, the original ``part event``."""
    digests = set()
    for before in (True, False):
        d = Driver()
        original = arrival("occ-1", T, U1)
        decl = declared(original, DID_NOT_OCCUR, T + 1_000)
        first = d.input(OCCUPANCY, decl if before else original)
        d.cut(T - 100_000)                 # one cut each: inside a cut the contract order decides, not the arrival
        second = d.input(OCCUPANCY, original if before else decl)
        d.scans(T, T + 60_000)
        d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
        d.cut()
        d.cut(T + 60_000 + L)
        w = d.work("occ-1")
        assert w["status"] == "retracted" and w["retraction"]["errorReason"] == DID_NOT_OCCUR
        parts = sorted(d.dispositions(first) + d.dispositions(second))
        assert parts == [("errorDeclaration", "accepted", "accepted"), ("event", "accepted", "accepted")]
        digests.add(digest(w))
    assert len(digests) == 1


def test_an_error_declaration_on_the_departure_reopens_the_visit():
    d = baseline()
    d.input(OCCUPANCY, declared(departure("occ-1d", T + 60_000, U1, S1, "occ-1"), INCORRECT_DATA, T + 100_000))
    d.scans(T + 60_000, T + 60_000 + L + 1000)     # the telemetry tail: J of the reopened visit runs to W1
    d.cut(T + 60_000 + L)
    w = d.work("occ-1")
    assert w["interval"]["leave"] is None and w["status"] == "provisional" and w["insufficiencyReasons"] == []


def test_two_departures_with_different_leaves_are_a_correction_conflict():
    d = baseline(settle=False)
    d.input(OCCUPANCY, departure("occ-1e", T + 50_000, U1, S1, "occ-1"))
    d.cut(T + 60_000 + L)
    w = d.work("occ-1")
    assert w["status"] == "insufficient" and "correction-conflict" in w["insufficiencyReasons"] and w["interval"]["leave"] is None


def test_a_cancellation_resolves_a_departure_conflict():
    d = baseline(settle=False)
    d.input(OCCUPANCY, departure("occ-1e", T + 50_000, U1, S1, "occ-1"))
    d.cut()
    d.input(OCCUPANCY, declared(departure("occ-1e", T + 50_000, U1, S1, "occ-1"), INCORRECT_DATA, T + 70_000))
    d.cut(T + 60_000 + L)
    w = d.work("occ-1")
    assert w["status"] == "complete" and w["interval"]["leave"] == T + 60_000


def test_two_closed_overlapping_visits_are_both_retracted_and_recover_when_one_is_cancelled():
    d = baseline(settle=False)
    d.input(OCCUPANCY, arrival("occ-2", T + 50_000, U2))
    d.input(OCCUPANCY, departure("occ-2d", T + 110_000, U2, S1, "occ-2"))
    d.cut(T + 110_000 + L)
    for e in ("occ-1", "occ-2"):
        assert d.work(e)["retraction"] == {"reason": "overlap", "errorReason": None}
    d.input(OCCUPANCY, declared(arrival("occ-2", T + 50_000, U2), DID_NOT_OCCUR, T + 200_000))
    d.cut(T + 110_000 + L)
    assert d.work("occ-1")["status"] == "complete" and "retraction" not in d.work("occ-1")
    assert d.work("occ-2")["retraction"]["reason"] == "error-declaration"


def bad_torque(code):
    return lambda t: {(NR1, "Torque"): (20.5, code)} if T + 10_000 <= t < T + 17_000 else {}


def test_a_bad_share_above_the_rule_is_insufficient():
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    rows = []
    for t in range(T, T + 60_000, 1000):
        rows.append(d.scan(t, bad_torque(BAD)(t)))
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.cut()
    d.cut(T + 60_000 + L)
    w = d.work("occ-1")
    e = entry(w, "nutrunner1-torque")
    assert (e["observedPeriods"], e["missingPeriods"], e["badSlots"], e["count"]) == (60, 0, 7, 53)
    assert w["status"] == "insufficient" and w["insufficiencyReasons"] == ["bad-share-exceeded"]
    assert d.sample(rows[12][1], "Torque") == {"sourceTs": T + 12_000, "state": "unattributed", "reason": "bad-quality", "active": True,
                                                "attributedTo": None}
    assert evidence(w, NR1)["coverage"]["status"] == "incomplete"


def test_rejected_uncertain_only_has_bad_slots_zero():
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    for t in range(T, T + 60_000, 1000):
        d.scan(t, bad_torque(UNCERTAIN)(t))
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.cut()
    d.cut(T + 60_000 + L)
    w = d.work("occ-1")
    e = entry(w, "nutrunner1-torque")
    assert (e["observedPeriods"], e["badSlots"], e["count"]) == (60, 0, 53)
    assert "bad-share-exceeded" not in w["insufficiencyReasons"] and w["status"] == "complete"


def test_a_seq_gap_overlapping_the_visit_is_insufficient_session_gap():
    d = baseline(settle=False)
    d.input(SESSIONS, session(NR1, "seq-gap", 40, {"from": T + 20_000, "to": T + 22_000, "missing": 2}))
    d.cut(T + 60_000 + L)
    w = d.work("occ-1")
    assert w["status"] == "insufficient" and w["insufficiencyReasons"] == ["session-gap"]
    assert evidence(w, NR1)["coverage"]["sessionGaps"] == 1 and w["coverage"]["sessionGaps"] == 1


def test_a_stale_death_changes_nothing():
    a = baseline()
    d = baseline(settle=False)
    d.input(SESSIONS, session(NR1, "stale-death", 40, {"detectedEventTime": T + 20_000, "rawPublishTs": T, "bdSeq": 0, "payloadDigest": "0" * 64}))
    d.cut(T + 60_000 + L)
    assert digest(d.work("occ-1")) == digest(a.work("occ-1"))


def test_a_session_ambiguous_interval_makes_coverage_unknown():
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    d.scans(T, T + 20_000)
    d.scans(T + 30_000, T + 60_000)
    d.input(SESSIONS, session(NR1, "session-ambiguous", 40, {"detectedEventTime": T + 25_000, "rawPublishTs": T, "bdSeq": 0, "payloadDigest": "0" * 64}))
    d.input(SESSIONS, session(NR1, "session-up", 41, {"at": T + 30_000, "rebirth": False}))
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.cut()
    d.cut(T + 60_000 + L)
    w = d.work("occ-1")
    assert "session-ambiguous" in w["insufficiencyReasons"]
    assert evidence(w, NR1)["coverage"]["status"] == "unknown"


def test_a_sample_in_two_non_retracted_windows_is_ambiguous():
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.input(OCCUPANCY, arrival("occ-2", T + 90_000, U2))
    d.input(OCCUPANCY, departure("occ-2d", T + 150_000, U2, S1, "occ-2"))
    s = d.scan(T + 70_000)   # in [T - 45 s, T + 105 s) and [T + 45 s, T + 195 s)
    d.cut(T + 150_000 + L)
    assert d.sample(s[1], "Torque")["reason"] == "ambiguous"
    assert d.work("occ-1")["status"] != "retracted"


def test_a_sample_in_only_a_retracted_window_attributes_to_it():
    d = Driver()
    original = arrival("occ-1", T, U1)
    d.input(OCCUPANCY, original)
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.input(OCCUPANCY, declared(original, DID_NOT_OCCUR, T + 1000))
    inside = d.scan(T + 30_000)
    tail = d.scan(T + 70_000)    # in the window, outside the actual interval
    d.cut(T + 60_000 + L)
    assert d.sample(inside[1], "Torque")["attributedTo"] == visit_ref("occ-1")
    assert d.sample(tail[1], "Torque")["reason"] == "ambiguous"


def test_a_shifted_clock_moves_samples_across_visits_as_the_bands_say():
    """9b: +90 s: originals 0-14 s stay (inside leave + 45 s), 15-44 s fall between the windows, 45-59 s reach the next visit."""
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.input(OCCUPANCY, arrival("occ-2", T + 180_000, U2))
    d.input(OCCUPANCY, departure("occ-2d", T + 240_000, U2, S1, "occ-2"))
    band = {}
    for orig in (0, 14_000, 15_000, 44_000, 45_000, 59_000):
        band[orig] = d.scan(T + orig + 90_000)[1]
    d.cut(T + 240_000 + L)
    assert [d.sample(band[x], "Torque")["attributedTo"] for x in (0, 14_000)] == [visit_ref("occ-1")] * 2
    assert [d.sample(band[x], "Torque")["reason"] for x in (15_000, 44_000)] == ["no-occupancy"] * 2
    assert [d.sample(band[x], "Torque")["attributedTo"] for x in (45_000, 59_000)] == [visit_ref("occ-2")] * 2


def test_late_after_settlement_is_accepted_with_its_reason():
    d = baseline()
    assert d.work("occ-1")["status"] == "complete"
    cancel = d.input(OCCUPANCY, declared(departure("occ-1d", T + 60_000, U1, S1, "occ-1"), INCORRECT_DATA, T + 800_000))
    corrective = d.input(OCCUPANCY, departure("occ-1x", T + 65_000, U1, S1, "occ-1"))
    d.scans(T + 60_000, T + 66_000)     # the telemetry the new leave brings into J
    d.cut(T + 60_000 + L)
    assert d.dispositions(cancel) == [("errorDeclaration", "accepted", "late-after-settlement")]
    assert d.dispositions(corrective) == [("event", "accepted", "late-after-settlement")]
    w = d.work("occ-1")
    assert w["status"] == "provisional" and w["interval"]["leave"] == T + 65_000
    d.cut(T + 65_000 + L)
    assert d.work("occ-1")["status"] == "complete"


@pytest.mark.parametrize("mode", ["propagate", "reject"])
def test_uncertain_propagate_aggregates_and_supports_activity(mode):
    def patch(rs):
        rs.attribution["quality"]["uncertainInput"] = mode
    d = Driver(ruleset(patch=patch))
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    rows = {}
    for t in range(T, T + 60_000, 1000):
        over = {(NR1, "Running"): (True, UNCERTAIN), (NR1, "Torque"): (99.0, UNCERTAIN)} if t == T + 30_000 else {}
        rows[t] = d.scan(t, over)
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.cut()
    d.cut(T + 60_000 + L)
    w = d.work("occ-1")
    torque, running = entry(w, "nutrunner1-torque"), d.sample(rows[T + 30_000][1], "Running")
    e = evidence(w, NR1)
    if mode == "propagate":
        assert (torque["count"], torque["max"]) == (60, 99.0)
        assert running["state"] == "attributed" and running["active"] is True
        assert e["activityIntervals"] == [[T, T + 60_000]] and e["coverage"]["unknownActivityPeriods"] == 0
    else:
        assert (torque["count"], torque["max"]) == (59, 20.5)
        assert running["reason"] == "bad-quality" and running["active"] == "unknown"
        assert e["coverage"]["unknownActivityPeriods"] == 1


def test_a_redelivered_message_gets_one_duplicate_per_member():
    d = Driver()
    msg = d.message(NR1, T, [member("Torque", 20.5, 0, T), member("Running", True, 0, T)])
    d.input(MESSAGES, msg)
    again = d.input(MESSAGES, msg | {"bridgeReceivedAt": T + 999})
    d.cut()
    assert sorted(d.dispositions(again)) == [("member:Running", "duplicate", "identical-redelivery"),
                                             ("member:Torque", "duplicate", "identical-redelivery")]


def test_disallowed_correction_reason_is_schema_invalid():
    def patch(rs):
        rs.occupancy["correction"]["reasons"] = [DID_NOT_OCCUR]
    d = baseline(Driver(ruleset(patch=patch)), settle=False)
    decl = d.input(OCCUPANCY, declared(arrival("occ-1", T, U1), INCORRECT_DATA, T + 1000))
    d.cut()
    assert d.dispositions(decl) == [("errorDeclaration", "rejected", "schema-invalid")]
    assert next(x for x in d.result.dispositions if x.delivery is decl).refs == {"check": "correction.reasons"}
    assert d.work("occ-1")["status"] != "retracted"


@pytest.mark.parametrize("field,value,check", [("source", "other", "sources"), ("unitId", "short", "identity.pattern")])
def test_profile_vocabulary_is_checked_at_the_door(field, value, check):
    d = Driver()
    r = d.input(OCCUPANCY, arrival("occ-1", T, U1) | {field: value})
    d.cut()
    assert d.dispositions(r) == [("event", "rejected", "schema-invalid")]
    assert next(x for x in d.result.dispositions if x.delivery is r).refs == {"check": check}


def test_an_unknown_readpoint_and_an_unknown_device_are_rejected():
    d = Driver()
    r = d.input(OCCUPANCY, arrival("occ-1", T, U1, "synthetic-site:line2:station9"))
    m = d.input(MESSAGES, d.message(NR1, T, [member("Torque", 1.0, 0, T)]) | {"device": "synthetic-site:line2:station9"})
    nots = d.input(MESSAGES, d.message(NR1, T, [member("Torque", 1.0, 0, None)]))
    wrong = d.input(MESSAGES, d.message(NR1, T, [member("Torque", 1.0, 0, T)]) | {"definitionSha256": "0" * 64})
    d.cut()
    assert d.dispositions(r) == [("event", "rejected", "unknown-readpoint")]
    assert d.dispositions(m) == [("message", "rejected", "unknown-device")]
    assert d.dispositions(nots) == [("message", "rejected", "no-source-timestamp")]
    assert d.dispositions(wrong) == [("message", "rejected", "definition-mismatch")]


def test_admission_boundaries():
    """S3: admitted when sourceTs >= admissionW - H; the admissionW of a cut is the previous cut's W1."""
    d = Driver()
    d.scan(T + H + 100_000)
    d.cut()
    aw = d.cuts[-1][S1]["W1"]
    at = d.scan(aw - H)[1]
    above = d.scan(aw - H + 1)[1]
    below = d.scan(aw - H - 1)[1]
    d.cut()
    assert d.sample(at, "Torque")["state"] == "unattributed"
    assert d.sample(above, "Torque")["state"] == "unattributed"
    assert d.sample(below, "Torque") == {"sourceTs": aw - H - 1, "state": "replay-required", "reason": "beyond-horizon", "active": True,
                                         "attributedTo": None}
    assert ("member:Torque", "replay-required", "beyond-horizon") in d.dispositions(below)


def test_expiry_drops_online_state_and_keeps_rows():
    d = baseline()
    before = digest(d.work("occ-1"))
    d.cut(T + 60_000 + H + L)          # W1 = onlineExpiresAt of the closed visit; the next cut admits against it
    decl = d.input(OCCUPANCY, declared(arrival("occ-1", T, U1), DID_NOT_OCCUR, T + H))
    late_arrival = d.input(OCCUPANCY, arrival("occ-9", T + 1000, U2))
    d.cut(T + 60_000 + H + L)
    assert d.dispositions(decl) == [("errorDeclaration", "replay-required", "correction-beyond-horizon")]
    assert d.dispositions(late_arrival) == [("event", "replay-required", "beyond-horizon")]
    assert digest(d.work("occ-1")) == before


def test_an_open_visit_past_the_horizon_is_missing_departure():
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    d.scans(T, T + 60_000)
    d.cut()
    d.cut(T + H - 1)
    assert "missing-departure" not in d.work("occ-1")["insufficiencyReasons"]   # (missing-periods already: no tail telemetry)
    d.cut(T + H)
    w = d.work("occ-1")
    assert w["status"] == "insufficient" and "missing-departure" in w["insufficiencyReasons"]
    assert w["interval"]["leave"] is None


def test_an_open_visit_is_bounded_by_the_next_arrival():
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    d.input(OCCUPANCY, arrival("occ-2", T + 100_000, U2))
    d.cut(T + 200_000)
    assert d.work("occ-1")["interval"]["attributionEnd"] == T + 100_000
    assert d.work("occ-2")["interval"]["attributionEnd"] is None


def test_smoke_samples_digest():
    rec = recording.open_recording(FIXTURES / "recordings" / "smoke")
    rs = ruleset()
    st = stage1.Stage1(rs, rec.manifest["definitions"])
    h = cuts.formula_history(rec, rs, st.O)
    last = h.cuts[-1]
    r = st.evaluate(cuts.deliveries(rec, h), lambda c, s: h.cuts[c].per_station[s].admission_w, {s: v.w1 for s, v in last.per_station.items()})
    (w,) = r.work_records.values()
    assert w["status"] == "complete" and w["insufficiencyReasons"] == []
    assert len(r.samples) == 812 * 7
    assert w["samplesDigest"] == SMOKE_SAMPLES_DIGEST
    assert w["rulesDigest"] == RULES_DIGEST


def test_station2_visit_body_without_measurement_members():
    d = Driver(station=S2)
    d.input(OCCUPANCY, arrival("occ-21", T, U1, S2))
    d.scans(T, T + 60_000)
    d.input(OCCUPANCY, departure("occ-22", T + 60_000, U1, S2, "occ-21"))
    d.cut()
    d.cut(T + 60_000 + L)
    w = d.work("occ-21")
    assert w["status"] == "complete" and w["insufficiencyReasons"] == [] and w["members"] == []
    assert w["equipmentEvidence"] == [{
        "equipmentPath": S2, "memberRefs": ["station2-running"], "activityIntervals": [[T, T + 60_000]], "inactiveIntervals": [],
        "coverage": {"observedPeriods": 60, "missingPeriods": 0, "badSlots": 0, "unknownActivityPeriods": 0, "sessionGaps": 0, "status": "complete"},
        "samplesDigest": STATION2_SAMPLES_DIGEST}]
    assert w["coverage"] == {"expectedPeriodMs": 1000, "observedPeriods": 60, "missingPeriods": 0, "badSlots": 0, "sessionGaps": 0}
    assert digest(w) == STATION2_BODY_DIGEST


def test_visit_seq_above_int32_is_preserved_in_the_body():
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1, seq=2147483648))
    d.cut(T)
    assert d.work("occ-1")["visitSeq"] == 2147483648


def test_non_canonicalizable_input_has_a_raw_bytes_disposition():
    d = baseline(settle=False)
    r = d.input(OCCUPANCY, None, raw=b"[9007199254740992]")
    d.cut()
    disp = next(x for x in d.result.dispositions if x.delivery is r)
    assert (disp.disposition, disp.reason, disp.refs.get("digestRule")) == ("rejected", "schema-invalid", "raw-bytes")
    assert d.work("occ-1")["status"] == "provisional"


@pytest.mark.parametrize("literal", ["1e400", "-1e400", "1" + "0" * 400, "-1" + "0" * 400])
def test_overflowing_literals_are_non_finite_and_the_message_keeps_its_good_members(literal):
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    hit = None
    for t in range(T, T + 60_000, 1000):
        if t == T + 30_000:
            msg = d.message(NR1, t, [member("Torque", 0.0, 0, t), member("Running", True, 0, t)])
            raw = json.dumps(msg, separators=(",", ":")).replace('"value":0.0', '"value":' + literal, 1).encode()
            hit = d.input(MESSAGES, None, raw=raw)
            for dev in (S1, NR2):
                d.input(MESSAGES, d.message(dev, t, [member(m, v, 0, t) for m, v in {S1: [("Running", True)], NR2: [("Torque", 0.0), ("Running", False)]}[dev]]))
        else:
            d.scan(t)
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.cut()
    d.cut(T + 60_000 + L)
    assert d.sample(hit, "Torque")["reason"] == "non-finite"
    assert d.sample(hit, "Running")["state"] == "attributed"
    disps = {p: (x, r) for p, x, r in d.dispositions(hit)}
    assert disps == {"member:Torque": ("accepted", "accepted"), "member:Running": ("accepted", "accepted")}
    assert next(x for x in d.result.dispositions if x.delivery is hit).refs.get("digestRule") == "raw-bytes"
    e = entry(d.work("occ-1"), "nutrunner1-torque")
    assert (e["observedPeriods"], e["missingPeriods"], e["count"]) == (59, 1, 59)


def test_three_notations_are_one_measurement():
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    raws = []
    for i, lit in enumerate(["9007199254740992", "9007199254740992.0", "9.007199254740992e15"]):
        msg = d.message(NR1, T + 1000, [member("Torque", 1.5, 0, T + 1000)], seq=100 + i)
        raws.append(d.input(MESSAGES, None, raw=json.dumps(msg, separators=(",", ":")).replace('"value":1.5', '"value":' + lit).encode()))
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.cut(T + 60_000 + L)
    assert all(x[1] == "accepted" for r in raws for x in d.dispositions(r))
    e = entry(d.work("occ-1"), "nutrunner1-torque")
    assert e["count"] == 1 and e["max"] == 9007199254740992.0
    # the body double 2^53 is a float in memory; JCS prints it as the integer literal, and the oracle's canonicalizer accepts it
    assert b"9007199254740992" in stage1.canon.canonical_bytes(d.work("occ-1"))


def test_evaluate_is_deterministic_and_order_independent_inside_a_cut():
    d = baseline()
    first = {k: digest(v) for k, v in d.result.work_records.items()}, {k: digest(v) for k, v in d.result.samples.items()}
    again = d.evaluate()
    assert ({k: digest(v) for k, v in again.work_records.items()}, {k: digest(v) for k, v in again.samples.items()}) == first
    shuffled = list(d.delivered)
    random.Random(20261109).shuffle(shuffled)
    r = d.stage.evaluate(shuffled, lambda c, s: d.cuts[c][s]["admissionW"], {s: v["W1"] for s, v in d.cuts[-1].items()})
    assert ({k: digest(v) for k, v in r.work_records.items()}, {k: digest(v) for k, v in r.samples.items()}) == first


def test_a_measurement_conflict_quarantines_every_delivery_of_the_triple():
    d = Driver()
    a = d.input(MESSAGES, d.message(NR1, T, [member("Torque", 1.0, 0, T)]))
    b = d.input(MESSAGES, d.message(NR1, T, [member("Torque", 2.0, 0, T)]))
    d.cut()
    assert d.dispositions(a) == [("member:Torque", "conflict", "measurement-conflict")]
    assert d.dispositions(b) == [("member:Torque", "conflict", "measurement-conflict")]
    assert d.sample(a, "Torque")["state"] == "quarantined" and d.sample(b, "Torque")["reason"] == "measurement-conflict"


def test_a_session_down_interval_makes_samples_outside_session():
    d = Driver()
    d.input(SESSIONS, session(NR1, "session-down", 40, {"from": T + 10_000, "to": None}))
    d.input(SESSIONS, session(NR1, "session-up", 41, {"at": T + 20_000, "rebirth": False}))
    inside = d.scan(T + 15_000)[1]
    after = d.scan(T + 25_000)[1]
    d.cut()
    assert d.sample(inside, "Torque")["reason"] == "outside-session"
    assert d.sample(after, "Torque")["reason"] == "no-occupancy"


# --- plc-stamped (P1-P3), mirroring Stage1PlcStampedTest -------------------------------------------------------------------------

from .driver import NS, PlcDriver, plc_ruleset  # noqa: E402


def plc_visit(d, lo=T, hi=T + 60_000, unit=U1, visit="V1", end=True):
    d.plc_scans(lo, hi, unit, visit)
    if end:
        d.plc_scan(hi, unit, visit, False, running=False, torque=0.0)
    return d


def test_a_plc_visit_is_framed_by_the_earliest_true_and_the_earliest_later_false():
    d = plc_visit(PlcDriver())
    d.cut()
    d.cut(T + 60_000 + L)
    w = d.plc_work("V1")
    assert w["interval"] == {"enter": T, "leave": T + 60_000, "attributionEnd": None}
    assert w["status"] == "complete" and w["visitSeq"] is None and w["readPoint"] == S1 and w["attributionMode"] == "plc-stamped"
    assert sorted(evidence(w, NR1)["memberRefs"]) == ["nutrunner1-present", "nutrunner1-running", "nutrunner1-torque", "nutrunner1-unit",
                                                      "nutrunner1-visit"]


def test_the_end_snapshot_is_not_attributed():
    d = PlcDriver()
    d.plc_scans(T, T + 60_000, U1, "V1")
    end = d.plc_scan(T + 60_000, U1, "V1", False, running=False)
    d.cut(T + 60_000 + L)
    assert d.sample(end[1], "Torque")["state"] == "unattributed"


def test_a_late_earlier_start_rebuilds_the_lifecycle():
    d = PlcDriver()
    d.plc_scans(T + 10_000, T + 60_000, U1, "V1")
    d.plc_scan(T + 60_000, U1, "V1", False)
    d.cut()
    assert d.plc_work("V1")["interval"]["enter"] == T + 10_000
    d.plc_scans(T, T + 10_000, U1, "V1")
    d.cut(T + 60_000 + L)
    assert d.plc_work("V1")["interval"]["enter"] == T


def test_a_true_after_the_end_is_inconsistent():
    d = plc_visit(PlcDriver())
    d.plc_scan(T + 61_000, U1, "V1", True)
    d.cut(T + 60_000 + L)
    assert "inconsistent-visit-boundary" in d.plc_work("V1")["insufficiencyReasons"]


def test_an_id_change_without_false_leaves_missing_visit_close():
    d = PlcDriver()
    d.plc_scans(T, T + 60_000, U1, "V1")
    d.plc_scans(T + 60_000, T + 120_000, U2, "V2")
    d.cut(T + 120_000 + L)
    w = d.plc_work("V1")
    assert w["status"] == "insufficient" and "missing-visit-close" in w["insufficiencyReasons"]
    assert w["interval"]["leave"] is None and w["interval"]["attributionEnd"] == T + 60_000


def test_equipment_must_agree_on_boundaries():
    d = PlcDriver()
    d.plc_scans(T, T + 60_000, U1, "V1", devices=(S1,))
    d.plc_scans(T + 5_000, T + 60_000, U1, "V1", devices=(NR1,))
    d.plc_scan(T + 60_000, U1, "V1", False)
    d.cut(T + 60_000 + L)
    assert "inconsistent-visit-boundary" in d.plc_work("V1")["insufficiencyReasons"]


def test_a_mixed_snapshot_is_incoherent():
    d = PlcDriver()
    d.plc_scans(T, T + 30_000, U1, "V1")
    mixed = d.plc_scan(T + 30_000, U1, "V1", True, identity_ts=T + 29_000)
    d.plc_scans(T + 31_000, T + 60_000, U1, "V1")
    d.plc_scan(T + 60_000, U1, "V1", False)
    d.cut(T + 60_000 + L)
    assert d.sample(mixed[1], "Torque")["reason"] == "incoherent-snapshot"
    assert d.sample(mixed[1], "UnitId")["reason"] == "incoherent-snapshot"


def test_cross_check_marks_identity_mismatch_without_changing_attribution():
    d = plc_visit(PlcDriver())
    d.input(OCCUPANCY, arrival("occ-1", T, U2))
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U2, S1, "occ-1"))
    s = d.plc_scan(T + 30_500, U1, "V1", True)
    d.cut(T + 60_000 + L)
    row = d.sample(s[1], "Torque")
    assert row["state"] == "attributed" and row["reason"] == "identity-mismatch"
    assert row["attributedTo"] == {"mode": "plc-stamped", "namespace": NS, "station": S1, "visitId": "V1"}


def test_cross_check_none_reads_no_occupancy():
    rs = plc_ruleset("line2-rules-plc-nocheck", "1.0.1")
    d = plc_visit(PlcDriver(rs))
    d.input(OCCUPANCY, arrival("occ-1", T, U2))
    s = d.plc_scan(T + 30_500, U1, "V1", True)
    d.cut(T + 60_000 + L)
    assert d.sample(s[1], "Torque")["reason"] is None
    assert not [k for k in d.result.work_records if "occupancy-join" in k]


def test_the_same_visit_id_with_another_unit_is_a_conflict():
    d = PlcDriver()
    d.plc_scans(T, T + 30_000, U1, "V1")
    other = d.plc_scan(T + 30_000, U2, "V1", True)
    d.plc_scans(T + 31_000, T + 60_000, U1, "V1")
    d.plc_scan(T + 60_000, U1, "V1", False)
    d.cut(T + 60_000 + L)
    w = d.plc_work("V1")
    assert w["status"] == "retracted" and w["retraction"] == {"reason": "input-conflict", "errorReason": None}
    assert ("member:UnitId", "conflict", "identity-conflict") in d.dispositions(other[1])


def test_running_false_never_ends_a_visit():
    d = PlcDriver()
    d.plc_scans(T, T + 60_000, U1, "V1", running=False)
    d.cut(T + 60_000)                   # J of the open visit = [enter, W1)
    w = d.plc_work("V1")
    assert w["interval"]["leave"] is None and w["status"] == "provisional"
    assert evidence(w, NR1)["inactiveIntervals"] and not evidence(w, NR1)["activityIntervals"]


def test_a_visit_without_any_observation_takes_its_coverage_from_the_required_members():
    """No equipmentEvidence[] entry yet: the visit-level extremes are those over the required members (spec 356's parenthetical)."""
    d = Driver()
    d.input(OCCUPANCY, arrival("occ-1", T, U1))
    d.input(OCCUPANCY, departure("occ-1d", T + 60_000, U1, S1, "occ-1"))
    d.cut(T)
    w = d.work("occ-1")
    assert w["equipmentEvidence"] == []
    assert w["coverage"] == {"expectedPeriodMs": 1000, "observedPeriods": 0, "missingPeriods": 60, "badSlots": 0, "sessionGaps": 0}
    assert w["insufficiencyReasons"] == ["missing-periods", "no-observed-periods", "unknown-activity"]
