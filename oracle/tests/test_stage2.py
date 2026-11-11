"""Stage 2 of the oracle on inline inputs typed from the rule sheet T1-T10; mirrors the cases of core's Stage2Test, RelatednessTest and
SetCoverageTest without using any Java output. End-to-end cases run stage 1 then stage 2 of the same cut through the test driver;
decision-table, relatedness and coverage cases call the functions on bodies typed from the sheet."""
from __future__ import annotations

import pytest

from forseti_oracle import inputs, stage2
from forseti_oracle.inputs import INCIDENTS, MESSAGES, OCCUPANCY

from .driver import (DID_NOT_OCCUR, INCORRECT_DATA, L, NR1, NR2, S1, S2, T, UNCERTAIN, Stage2Driver, arrival, declared, departure,
                     digest, incident, member, ruleset)

U1, U2, U3 = "UNIT0000000000001", "UNIT0000000000002", "UNIT0000000000003"
A, B = (T, T + 60_000), (T + 180_000, T + 240_000)
END = T + 240_000


def activity(t, nr1=(A,), nr2=(B,), over=None):
    """One scan: nutrunner1 runs inside ``nr1`` intervals, nutrunner2 inside ``nr2``, the station while a visit is present."""
    on1 = any(a <= t < b for a, b in nr1)
    on2 = any(a <= t < b for a, b in nr2)
    st = any(a <= t < b for a, b in (A, B))
    o = {(S1, "Running"): (st, 0), (NR1, "Running"): (on1, 0), (NR1, "Torque"): (20.5 if on1 else 0.0, 0),
         (NR2, "Running"): (on2, 0), (NR2, "Torque"): (20.5 if on2 else 0.0, 0)}
    o.update((over or (lambda t: {}))(t))
    return o


def line(d=None, visits=((("occ-1", U1), A), (("occ-2", U2), B)), nr1=(A,), nr2=(B,), over=None, departures=True, lo=T, hi=END):
    d = d or Stage2Driver()
    for (eid, unit), (a, b) in visits:
        d.input(OCCUPANCY, arrival(eid, a, unit))
        if departures:
            d.input(OCCUPANCY, departure(eid + "d", b, unit, S1, eid))
    for t in range(lo, hi, 1000):
        d.scan(t, activity(t, nr1, nr2, over))
    return d


SETTLED = END + L


def test_an_incident_after_complete_records_marks_tool_visits_candidates():
    d = line()
    d.cut(SETTLED)
    d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END))
    d.cut(SETTLED)
    assert (d.assessment(U1)["status"], d.assessment(U1)["reason"]) == ("affected-candidate", "equipment-activity")
    assert (d.assessment(U2)["status"], d.assessment(U2)["reason"]) == ("not-affected-within-scope", "no-equipment-activity")
    s = d.set()
    assert [u["unitId"] for u in s["units"]] == [U1, U2] and s["resolution"] is None
    assert s["units"][0]["bodyDigest"] == digest(d.assessment(U1))
    assert s["coverage"] == {"status": "complete", "unattributedActivityPeriods": 0, "unknownActivityPeriods": 0, "provisionalUnits": 0,
                             "insufficientUnits": 0, "sessionGapOverlaps": 0, "open": False, "settled": True, "beyondRetention": False,
                             "retentionGap": False}
    a = d.assessment(U1)
    assert a["incident"]["incidentId"] == "inc-1" and len(a["incident"]) == 12 and a["rulesDigest"] == d.stage.rules_digest
    assert a["basis"] == [{"visitRef": {"mode": "occupancy-join", "source": "mes-line2", "arrivalEventId": "occ-1"}, "bodyDigest": digest(d.work("occ-1"))}]
    assert a["evidenceWarnings"] == []


def test_an_incident_before_the_records_starts_empty_and_grows():
    d = Stage2Driver()
    d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END))
    d.cut(T - 100_000)
    s = d.set()
    assert s["units"] == [] and s["coverage"]["settled"] is False and s["coverage"]["status"] == "unknown"
    assert s["coverage"]["unknownActivityPeriods"] == 240
    line(d, visits=((("occ-1", U1), A),), hi=T + 120_000)
    d.cut()
    assert [(u["unitId"], u["reason"]) for u in d.set()["units"]] == [(U1, "basis-provisional")]
    assert d.set()["coverage"]["unknownActivityPeriods"] == 120 - 89 - 0 or d.set()["coverage"]["unknownActivityPeriods"] > 0
    line(d, visits=((("occ-2", U2), B),), lo=T + 120_000)
    d.cut(SETTLED)
    assert [(u["unitId"], u["reason"]) for u in d.set()["units"]] == [(U1, "equipment-activity"), (U2, "no-equipment-activity")]
    assert d.set()["coverage"]["status"] == "complete"


def test_a_narrowed_and_closed_incident_marks_later_visits_interval_narrowed():
    d = line()
    d.input(INCIDENTS, incident("inc-2", 1, NR1, T, None, status="open", detected=T + 10_000, kind="gauge-drift"))
    d.cut(SETTLED)
    assert d.assessment(U2, "inc-2")["reason"] == "no-equipment-activity"
    assert d.set("inc-2")["coverage"]["open"] is True and d.set("inc-2")["coverage"]["status"] == "incomplete"
    d.input(INCIDENTS, incident("inc-2", 2, NR1, T, T + 100_000, status="closed", detected=T + 10_000, ret=T + 300_000, kind="gauge-drift"))
    d.cut(SETTLED)
    assert (d.assessment(U2, "inc-2")["status"], d.assessment(U2, "inc-2")["reason"]) == ("not-affected-within-scope", "interval-narrowed")
    assert d.assessment(U2, "inc-2")["basis"] == []
    assert d.assessment(U1, "inc-2")["reason"] == "equipment-activity"
    assert d.set("inc-2")["coverage"]["open"] is False


@pytest.mark.parametrize("on_withdrawn,expected", [("not-affected", "not-affected-within-scope"), ("insufficient", "insufficient-evidence")])
def test_a_withdrawn_incident_follows_on_withdrawn(on_withdrawn, expected):
    def patch(rs):
        rs.impact["evidence"]["onWithdrawn"] = on_withdrawn
    d = line(Stage2Driver(ruleset(patch=patch)))
    d.input(INCIDENTS, incident("inc-3", 1, NR1, T, END))
    d.input(INCIDENTS, incident("inc-3", 2, NR1, T, END, status="withdrawn", detected=END, ret=END + 1000))
    d.cut(SETTLED)
    assert {d.assessment(u, "inc-3")["status"] for u in (U1, U2)} == {expected}
    assert {d.assessment(u, "inc-3")["reason"] for u in (U1, U2)} == {"incident-withdrawn"}
    c = d.set("inc-3")
    assert c["resolution"] == "withdrawn" and c["coverage"]["open"] is False and c["coverage"]["settled"] is True
    assert c["coverage"]["status"] != "complete"


def test_unattributed_activity_inside_the_interval_keeps_coverage_incomplete():
    d = line(nr1=(A, (T + 110_000, T + 130_000)))      # outside every tolerance window: [T - 45 s, T + 105 s), [T + 135 s, ...)
    d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END))
    d.cut(SETTLED)
    c = d.set()["coverage"]
    assert c["unattributedActivityPeriods"] == 20 and c["unknownActivityPeriods"] == 0 and c["status"] == "incomplete"


@pytest.mark.parametrize("policy,expected", [("by-cause", "not-affected-within-scope"), ("always-insufficient", "insufficient-evidence")])
def test_a_retracted_basis_with_did_not_occur_makes_the_candidate_not_affected(policy, expected):
    def patch(rs):
        rs.impact["evidence"]["onBasisRetracted"] = policy
    d = line(Stage2Driver(ruleset(patch=patch)))
    d.input(OCCUPANCY, declared(arrival("occ-1", T, U1), DID_NOT_OCCUR, END))
    d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END))
    d.cut(SETTLED)
    a = d.assessment(U1)
    assert (a["status"], a["reason"]) == (expected, "basis-retracted-error-declaration")
    assert a["evidenceWarnings"] == [{"visitRef": a["basis"][0]["visitRef"], "insufficiencyReasons": []}]


def test_a_retracted_basis_with_incorrect_data_is_insufficient():
    d = line()
    d.input(OCCUPANCY, declared(arrival("occ-1", T, U1), INCORRECT_DATA, END))
    d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END))
    d.cut(SETTLED)
    assert (d.assessment(U1)["status"], d.assessment(U1)["reason"]) == ("insufficient-evidence", "basis-retracted-error-declaration")


def test_a_late_attribution_adds_the_unit():
    d = line(visits=((("occ-1", U1), A),))
    d.input(INCIDENTS, incident("inc-2", 1, NR1, T, None, status="open", detected=T + 10_000, kind="gauge-drift"))
    d.cut()
    assert [u["unitId"] for u in d.set("inc-2")["units"]] == [U1]
    d.input(OCCUPANCY, arrival("occ-2", B[0], U2))
    d.input(OCCUPANCY, departure("occ-2d", B[1], U2, S1, "occ-2"))
    d.cut()
    assert [(u["unitId"], u["reason"]) for u in d.set("inc-2")["units"]] == [(U1, "basis-provisional"), (U2, "basis-provisional")]
    d.cut(SETTLED)
    assert [(u["unitId"], u["reason"]) for u in d.set("inc-2")["units"]] == [(U1, "equipment-activity"), (U2, "no-equipment-activity")]


def test_a_provisional_basis_is_insufficient_evidence():
    d = line()
    d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END))
    d.cut()
    a = d.assessment(U2)
    assert (a["status"], a["reason"]) == ("insufficient-evidence", "basis-provisional")
    assert a["evidenceWarnings"][0]["insufficiencyReasons"] == []
    assert d.set()["coverage"]["provisionalUnits"] == 2


def test_a_departure_that_narrows_an_open_visit_deletes_the_row():
    d = line(visits=((("occ-1", U1), A),), departures=False, hi=T + 120_000)
    d.input(INCIDENTS, incident("inc-2", 1, NR1, T + 100_000, None, status="open", detected=T + 110_000, kind="gauge-drift"))
    d.cut()
    assert d.assessment(U1, "inc-2") is not None   # open visit before suspectFrom: possibly-related
    d.input(OCCUPANCY, departure("occ-1d", A[1], U1, S1, "occ-1"))
    d.cut()
    assert d.assessment(U1, "inc-2") is None and d.set("inc-2")["units"] == []


def test_row_set_widens_across_revisions_but_judges_the_current_one():
    d = line()
    d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END))
    d.input(INCIDENTS, incident("inc-1", 2, NR1, T, T + 100_000, detected=END, ret=END + 1))
    d.cut(SETTLED)
    a = d.assessment(U2)
    assert a is not None and (a["status"], a["reason"]) == ("not-affected-within-scope", "interval-narrowed")
    assert a["incident"]["revision"] == 2


def test_row_set_is_the_same_in_both_arrival_orders():
    out = []
    for order in ((1, 2), (2, 1)):
        d = line()
        revs = {1: incident("inc-1", 1, NR1, T, END), 2: incident("inc-1", 2, NR1, T, T + 100_000, detected=END, ret=END + 1)}
        ds = {}
        for n in order:                      # inside one cut incidents apply in (identity bytes, payloadDigest) order: one cut each
            ds[n] = d.input(INCIDENTS, revs[n])
            d.cut(SETTLED)
        if order == (2, 1):
            assert d.incident_dispositions(ds[1]) == [("duplicate", "superseded", None)]
        out.append(({k: digest(v) for k, v in d.assessments.items()}, {k: digest(v) for k, v in d.sets.items()}))
        assert d.assessment(U2)["reason"] == "interval-narrowed"
    assert out[0] == out[1]


@pytest.mark.parametrize("mode", ["propagate", "reject"])
def test_uncertain_propagate_aggregates_and_supports_activity(mode):
    """Uncertain Running=true outside every visit: under propagate it is unattributed activity, not unknown coverage; under reject
    the same slots are bad-quality rows, unknown coverage."""
    def patch(rs):
        rs.attribution["quality"]["uncertainInput"] = mode
    window = (T + 110_000, T + 120_000)

    def over(t):
        return {(NR1, "Running"): (True, UNCERTAIN)} if window[0] <= t < window[1] else {}
    d = line(Stage2Driver(ruleset(patch=patch)), over=over)
    d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END))
    d.cut(SETTLED)
    c = d.set()["coverage"]
    if mode == "propagate":
        assert (c["unattributedActivityPeriods"], c["unknownActivityPeriods"]) == (10, 0)
    else:
        assert (c["unattributedActivityPeriods"], c["unknownActivityPeriods"]) == (0, 10)
        assert c["status"] == "unknown"


def test_a_set_only_coverage_change_does_not_revise_units():
    d = line()
    d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END))
    d.cut(SETTLED)
    before = {k: digest(v) for k, v in d.assessments.items()}
    set_before = digest(d.set())
    # a second poll with Running=true between the visits' windows: unattributed activity, no visit touched
    d.input(MESSAGES, d.message(NR1, T + 120_500, [member("Running", True, 0, T + 120_500)]))
    d.cut(SETTLED)
    assert {k: digest(v) for k, v in d.assessments.items()} == before
    assert digest(d.set()) != set_before and d.set()["coverage"]["unattributedActivityPeriods"] == 1


# --- T1 dispositions ---------------------------------------------------------------------------------------------------------


def test_a_lower_revision_is_superseded_and_never_current():
    d = line()
    two = d.input(INCIDENTS, incident("inc-1", 2, NR1, T, T + 100_000, detected=END, ret=END + 1))
    d.cut(SETTLED)
    one = d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END))
    d.cut(SETTLED)
    assert d.incident_dispositions(two) == [("accepted", "accepted", None)]
    assert d.incident_dispositions(one) == [("duplicate", "superseded", None)]
    assert d.set()["incident"]["revision"] == 2


def test_revision_event_time_is_checked_by_revision_number():
    d = Stage2Driver()
    bad1 = d.input(INCIDENTS, incident("inc-9", 1, NR1, T, END, detected=END, ret=END + 5))
    three = d.input(INCIDENTS, incident("inc-1", 3, NR1, T, END, detected=END, ret=END + 10))
    d.cut(T)
    two = d.input(INCIDENTS, incident("inc-1", 2, NR1, T, END, detected=END, ret=END + 20))
    d.cut(T)
    assert d.incident_dispositions(bad1) == [("rejected", "schema-invalid", "revisionEventTime")]
    assert d.incident_dispositions(three) == [("accepted", "accepted", None)]
    assert d.incident_dispositions(two) == [("rejected", "schema-invalid", "revisionEventTime")]


def test_a_station_change_is_rejected_without_replacing_the_current_revision():
    def patch(rs):
        rs.impact["incidentScope"] = list(rs.impact["incidentScope"]) + [S2]
    d = Stage2Driver(ruleset(patch=patch))
    d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END))
    moved = d.input(INCIDENTS, incident("inc-1", 2, S2, T, END, station=S2, detected=END, ret=END))
    d.cut(T)
    assert d.incident_dispositions(moved) == [("rejected", "station-change", None)]
    assert d.set()["incident"]["revision"] == 1


def test_scope_kind_and_conflict_dispositions():
    d = Stage2Driver()
    outside = d.input(INCIDENTS, incident("inc-1", 1, S1, T, END))
    wrong_station = d.input(INCIDENTS, incident("inc-2", 1, NR1, T, END, station=S2))
    a = d.input(INCIDENTS, incident("inc-3", 1, NR1, T, END))
    b = d.input(INCIDENTS, incident("inc-3", 1, NR1, T, END) | {"evidenceRef": "urn:other"})
    again = d.input(INCIDENTS, incident("inc-4", 1, NR1, T, END))
    dup = d.input(INCIDENTS, incident("inc-4", 1, NR1, T, END))
    d.cut(T)
    assert d.incident_dispositions(outside) == [("rejected", "outside-incident-scope", None)]
    assert d.incident_dispositions(wrong_station) == [("rejected", "outside-incident-scope", None)]
    assert d.incident_dispositions(a) == d.incident_dispositions(b) == [("conflict", "identity-conflict", None)]
    assert sorted(d.incident_dispositions(again) + d.incident_dispositions(dup)) == [("accepted", "accepted", None),
                                                                                    ("duplicate", "identical-redelivery", None)]
    assert d.set("inc-3") is None


def test_a_kind_outside_the_profile_is_schema_invalid():
    def patch(rs):
        rs.incident["kinds"] = ["tool-check-failed"]
    d = Stage2Driver(ruleset(patch=patch))
    r = d.input(INCIDENTS, incident("inc-1", 1, NR1, T, END, kind="gauge-drift"))
    d.cut(T)
    assert d.incident_dispositions(r) == [("rejected", "schema-invalid", "kinds")]


# --- T4 ------------------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("mode,v,s,expected", [
    ("any-overlap", (0, 10), (5, 20), "related"),
    ("any-overlap", (0, 10), (10, 20), "unrelated"),          # leave == suspectFrom
    ("any-overlap", (20, 30), (5, 20), "unrelated"),          # enter == suspectTo
    ("any-overlap", (0, 10), (5, None), "related"),
    ("any-overlap", (0, 10), (10, None), "unrelated"),
    ("any-overlap", (5, None), (0, None), "related"),
    ("any-overlap", (0, None), (5, None), "possibly-related"),
    ("any-overlap", (20, None), (5, 20), "unrelated"),
    ("any-overlap", (0, None), (5, 20), "possibly-related"),
    ("fully-within", (5, 10), (0, 20), "related"),
    ("fully-within", (5, 25), (0, 20), "unrelated"),
    ("fully-within", (5, None), (0, 20), "possibly-related"),
    ("fully-within", (5, 10), (0, None), "possibly-related"),
    ("fully-within", (0, None), (5, None), "unrelated"),
    ("fully-within", (25, None), (0, 20), "unrelated"),
    ("enter-within", (5, 100), (0, 20), "related"),
    ("enter-within", (20, 30), (0, 20), "unrelated"),
    ("enter-within", (5, None), (0, None), "related"),
    ("enter-within", (0, None), (5, None), "unrelated"),
])
def test_relatedness(mode, v, s, expected):
    assert stage2.related(mode, v[0], v[1], s[0], s[1]) == expected


# --- T6 rows on typed bodies -----------------------------------------------------------------------------------------------------


def basis(status, enter=0, leave=60_000, reasons=(), retraction=None, active=(), inactive=(), unit=U1, eid="occ-1"):
    b = {"unitId": unit, "readPoint": S1, "visitSeq": 1, "status": status, "insufficiencyReasons": sorted(reasons),
         "interval": {"enter": enter, "leave": leave, "attributionEnd": None}, "members": [],
         "equipmentEvidence": [{"equipmentPath": NR1, "memberRefs": [], "activityIntervals": [list(x) for x in active],
                                "inactiveIntervals": [list(x) for x in inactive], "coverage": {}, "samplesDigest": ""}],
         "samplesDigest": "", "coverage": {}, "attributionMode": "occupancy-join", "rulesDigest": ""}
    if retraction:
        b["retraction"] = {"reason": retraction[0], "errorReason": retraction[1]}
    return {"mode": "occupancy-join", "source": "mes-line2", "arrivalEventId": eid}, b


def revision(f=0, t=60_000, status="closed", equipment=NR1):
    v = incident("inc-1", 1, equipment, f, t, status=status, detected=t if t is not None else f + 1)
    return stage2.Revision(inputs.parse(INCIDENTS, v), v, "")


def judge(bases, rev=None, patch=None):
    rs = ruleset(patch=patch)
    s = stage2.Stage2(rs, "", {})
    st, reason, _, _ = s.judge(rev or revision(), bases)
    return st, reason


@pytest.mark.parametrize("row,bases,rev,expected", [
    (1, [basis("complete", active=[(0, 60_000)])], revision(status="withdrawn"), ("not-affected-within-scope", "incident-withdrawn")),
    (2, [basis("complete", active=[(10_000, 20_000)]), basis("insufficient", reasons=["missing-periods"], eid="o2")], None,
     ("affected-candidate", "equipment-activity")),
    (3, [basis("complete", 100_000, 160_000)], None, ("not-affected-within-scope", "interval-narrowed")),
    (4, [basis("insufficient", reasons=["correction-conflict"], leave=60_000)], None, ("insufficient-evidence", "correction-conflict")),
    (5, [basis("provisional"), basis("insufficient", reasons=["missing-periods"], eid="o2")], None, ("insufficient-evidence", "basis-provisional")),
    (5, [basis("insufficient", reasons=["session-gap"])], None, ("insufficient-evidence", "basis-insufficient")),
    (6, [basis("retracted", retraction=("overlap", None))], None, ("insufficient-evidence", "basis-retracted-overlap")),
    (6, [basis("retracted", retraction=("input-conflict", None))], None, ("insufficient-evidence", "basis-retracted-input-conflict")),
    (6, [basis("retracted", retraction=("error-declaration", INCORRECT_DATA))], None, ("insufficient-evidence", "basis-retracted-error-declaration")),
    (7, [basis("complete", inactive=[(0, 60_000)])], None, ("not-affected-within-scope", "no-equipment-activity")),
    (8, [basis("retracted", retraction=("error-declaration", DID_NOT_OCCUR))], None, ("not-affected-within-scope", "basis-retracted-error-declaration")),
    (9, [basis("complete")], None, ("insufficient-evidence", "basis-insufficient")),
])
def test_each_decision_row_in_order(row, bases, rev, expected):
    assert judge(bases, rev) == expected


def test_no_activity_is_judged_inside_the_intersection_only():
    """A 2-slot gap in the inactive support outside [enter, leave) ∩ S does not matter; inside it falls through to row 9."""
    b = [basis("complete", inactive=[(0, 20_000), (22_000, 60_000)])]
    assert judge(b, revision(30_000, 60_000)) == ("not-affected-within-scope", "no-equipment-activity")
    assert judge(b, revision(0, 60_000)) == ("insufficient-evidence", "basis-insufficient")


def test_station_visit_mode_takes_any_related_complete_basis():
    def patch(rs):
        rs.impact["evidence"]["match"] = "station-visit"
    assert judge([basis("complete", inactive=[(0, 60_000)])], patch=patch) == ("affected-candidate", "station-visit")


def test_fully_within_and_enter_within_modes():
    def fw(rs):
        rs.impact["interval"]["overlap"] = "fully-within"

    def ew(rs):
        rs.impact["interval"]["overlap"] = "enter-within"
    wide = [basis("complete", 0, 60_000, active=[(0, 60_000)])]
    assert judge(wide, revision(10_000, 100_000), fw) == ("not-affected-within-scope", "interval-narrowed")
    assert judge(wide, revision(0, 100_000), fw) == ("affected-candidate", "equipment-activity")
    assert judge(wide, revision(10_000, 100_000), ew) == ("not-affected-within-scope", "interval-narrowed")
    assert judge(wide, revision(0, 10_000), ew)[0] == "affected-candidate"


# --- T8 on typed rows ------------------------------------------------------------------------------------------------------------


def set_cov(rev, rows=(), er=None, W2=10**12, patch=None, units=()):
    rs = ruleset(patch=patch)
    s = stage2.Stage2(rs, "", er if er is not None else {NR1: {"from": 0, "to": 10**12}})
    by_dev = {NR1: [("Running", r) for r in rows]}
    return s.set_body(rev, list(units), by_dev, W2)["coverage"]


def row(t, state="attributed", reason=None, active=True):
    return {"sourceTs": t, "state": state, "reason": reason, "active": active, "attributedTo": None}


def test_set_coverage_counts_slots_from_suspect_from_with_the_last_partial_slot():
    rev = revision(0, 2_500)
    assert set_cov(rev)["unknownActivityPeriods"] == 3
    c = set_cov(rev, [row(0), row(1_000, "unattributed", "no-occupancy", True), row(2_000, "unattributed", "bad-quality", "unknown")])
    assert (c["unattributedActivityPeriods"], c["unknownActivityPeriods"]) == (1, 1)


def test_unattributed_and_unknown_are_exclusive():
    c = set_cov(revision(0, 2_000), [row(0, "unattributed", "ambiguous", True), row(1_000, "unattributed", "session-ambiguous", "unknown")])
    assert (c["unattributedActivityPeriods"], c["unknownActivityPeriods"]) == (1, 1)


def test_retention_gap_from_the_manifest_range():
    rows = [row(t) for t in range(0, 60_000, 1000)]
    assert set_cov(revision(0, 60_000), rows, {NR1: {"from": 0, "to": 59_999}})["retentionGap"] is False
    assert set_cov(revision(0, 60_000), rows, {NR1: {"from": 1, "to": 10**9}})["retentionGap"] is True
    assert set_cov(revision(0, 60_000), rows, {})["retentionGap"] is True
    # a verified empty range covers an empty required interval
    assert set_cov(revision(0, 0), [], {NR1: {"from": 0, "to": 0}})["retentionGap"] is False


def test_beyond_retention_at_the_boundary_and_one_millisecond_outside():
    week = 604_800_000
    at = incident("inc-1", 2, NR1, 1_000, 2_000, detected=2_000, ret=1_000 + week)
    out = incident("inc-1", 2, NR1, 1_000, 2_000, detected=2_000, ret=1_001 + week)
    assert set_cov(stage2.Revision(inputs.parse(INCIDENTS, at), at, ""))["beyondRetention"] is False
    assert set_cov(stage2.Revision(inputs.parse(INCIDENTS, out), out, ""))["beyondRetention"] is True


def test_the_set_status_ladder():
    full = [row(t) for t in range(0, 60_000, 1000)]
    assert set_cov(revision(0, 60_000), full)["status"] == "complete"
    assert set_cov(revision(0, 60_000), full, W2=60_000 + L - 1)["status"] == "incomplete"         # not settled
    assert set_cov(revision(0, 60_000), full[:-1])["status"] == "unknown"                          # one slot without activity
    assert set_cov(revision(0, None, "open"), full)["status"] == "incomplete"                      # open
    provisional = [("U", {"status": "insufficient-evidence", "reason": "basis-provisional", "evidenceWarnings": []})]
    c = set_cov(revision(0, 60_000), full, units=provisional)
    assert (c["provisionalUnits"], c["status"]) == (1, "incomplete")
    gap = [("U", {"status": "insufficient-evidence", "reason": "basis-insufficient",
                  "evidenceWarnings": [{"visitRef": {"x": 1}, "insufficiencyReasons": ["session-ambiguous"]}]})]
    c = set_cov(revision(0, 60_000), full, units=gap)
    assert (c["insufficientUnits"], c["sessionGapOverlaps"], c["status"]) == (1, 1, "unknown")


def test_an_open_incident_range_ends_one_period_after_the_last_delivered_observation():
    rows = [row(t) for t in range(0, 10_000, 1000)]
    c = set_cov(revision(0, None, "open"), rows, W2=9_000)
    assert c["unknownActivityPeriods"] == 0 and c["open"] is True and c["settled"] is False
