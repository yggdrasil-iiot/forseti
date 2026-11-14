import hashlib
import math
import re
from pathlib import Path

import pytest

from forseti_oracle import inputs
from forseti_oracle.canon import canonical_bytes
from forseti_oracle.inputs import INCIDENTS, MESSAGES, OCCUPANCY, SESSIONS, SchemaInvalid

ROOT = Path(__file__).resolve().parents[2]

ARRIVAL = ('{"eventId":"occ-1","eventTime":1794183000000,"recordTime":1794183000200,"unitId":"UNIT0000000000001",'
           '"action":"OBSERVE","bizStep":"urn:epcglobal:cbv:bizstep:arriving","readPoint":"synthetic-site:line2:station1","visitSeq":1,"source":"mes-line2"}')
DEPARTURE = ('{"eventId":"occ-2","eventTime":1794183060000,"recordTime":1794183060200,"unitId":"UNIT0000000000001",'
             '"action":"OBSERVE","bizStep":"urn:epcglobal:cbv:bizstep:departing","readPoint":"synthetic-site:line2:station1","visitSeq":1,'
             '"arrivalEventId":"occ-1","source":"mes-line2"}')
INCIDENT = ('{"incidentId":"inc-1","revision":1,"stationRef":"synthetic-site:line2:station1",'
            '"equipmentRef":"synthetic-site:line2:station1:nutrunner1","kind":"tool-check-failed","detectedAt":1794183100000,'
            '"revisionEventTime":1794183100000,"suspectFrom":1794183000000,"suspectTo":null,"status":"open","evidenceRef":"ev-1","source":"qms"}')
MESSAGE = ('{"edge":"edge1","device":"synthetic-site:line2:station1:nutrunner1","birthPublishTs":1794182400000,'
           '"bdSeq":0,"seq":2,"publishTs":1794183000100,"bridgeReceivedAt":1794183000150,"definitionSha256":"' + "ab" * 32 + '",'
           '"members":[{"metric":"Running","value":true,"uaStatusCode":0,"quality":192,"sourceTs":1794183000000,"serverTs":1794183000001}]}')
SESSION = ('{"edge":"edge1","device":"synthetic-site:line2:station1:nutrunner1","birthPublishTs":1794182400000,'
           '"bdSeq":0,"kind":"seq-gap","atSeq":5,"payload":{"from":3,"to":5,"missing":2}}')

# The first occupancy record of the smoke recording (plan Step 8.1) and its payload digest; IdsTest pins the same literal.
SMOKE_OCC_1 = ARRIVAL
SMOKE_OCC_1_DIGEST = "7ae229772ad8b8d77467029c0a07f0e46bbe63f24a7b92afd4c6daa1ce9c44e5"


def with_(base: str, field: str, literal: str) -> str:
    """Replaces one top-level field's value with a raw literal (big integers stay big)."""
    return re.sub('"' + field + r'":("[^"]*"|[^,}]*)', lambda _: '"' + field + '":' + literal, base, count=1)


def parse(topic, text):
    return inputs.parse(topic, inputs.loads_value(text.encode("utf-8")))


def invalid(topic, text):
    with pytest.raises(SchemaInvalid, match="schema-invalid") as e:
        parse(topic, text)
    return e.value.check


def digest_of(text):
    raw = text.encode("utf-8")
    try:
        value = inputs.loads_value(raw)
    except inputs.NotJson:
        value = None
    return inputs.payload_digest(value, raw)


def message(recv, quality, value):
    return ('{"edge":"edge1","device":"d1","birthPublishTs":1,"bdSeq":0,"seq":2,"publishTs":10,' + recv + '"definitionSha256":"x",'
            '"members":[{"metric":"Torque","value":' + value + ',"uaStatusCode":0,' + quality + '"sourceTs":9}]}')


# --- the cases of InputParserTest -----------------------------------------------------------------------------------------


def test_valid_arrival_without_arrival_event_id_and_error_declaration_parses():
    o = parse(OCCUPANCY, ARRIVAL)
    assert o.arriving and o.eventId == "occ-1" and o.eventTime == 1794183000000
    assert o.arrivalEventId is None and o.errorDeclaration is None
    d = parse(OCCUPANCY, DEPARTURE)
    assert not d.arriving and d.arrivalEventId == "occ-1"


def test_visit_seq_above_int32_is_preserved():
    assert parse(OCCUPANCY, with_(ARRIVAL, "visitSeq", "2147483648")).visitSeq == 2147483648


@pytest.mark.parametrize("topic,base,field", [(OCCUPANCY, ARRIVAL, "visitSeq"), (INCIDENTS, INCIDENT, "revision"),
                                              (MESSAGES, MESSAGE, "seq"), (SESSIONS, SESSION, "atSeq")])
def test_integer_range_boundaries_per_input_kind(topic, base, field):
    stream = [with_(base, field, "9007199254740991"), with_(base, field, "9007199254740992"), with_(base, field, "18446744073709551617"), base]
    out = [inputs.read(topic, s.encode("utf-8")) for s in stream]  # no exception escapes
    assert not out[0].rejected
    for i in (1, 2):
        r = out[i]
        assert r.rejected and r.check == field and r.parsed is None
        assert r.digest_rule == "raw-bytes"
        assert r.payload_digest == hashlib.sha256(stream[i].encode("utf-8")).hexdigest()
    assert not out[3].rejected


def test_arrival_without_record_time_or_visit_seq_is_schema_invalid():
    invalid(OCCUPANCY, ARRIVAL.replace('"recordTime":1794183000200,', ""))
    invalid(OCCUPANCY, ARRIVAL.replace('"visitSeq":1,', ""))


def test_departure_without_arrival_event_id_is_schema_invalid():
    invalid(OCCUPANCY, DEPARTURE.replace('"arrivalEventId":"occ-1",', ""))


def test_arrival_carrying_arrival_event_id_is_schema_invalid():
    invalid(OCCUPANCY, ARRIVAL.replace('"source"', '"arrivalEventId":"occ-0","source"'))


def test_closed_incident_with_null_suspect_to_is_schema_invalid():
    assert parse(INCIDENTS, INCIDENT) is not None
    invalid(INCIDENTS, with_(INCIDENT, "status", '"closed"'))
    closed = parse(INCIDENTS, with_(with_(INCIDENT, "status", '"closed"'), "suspectTo", "1794183200000"))
    assert closed.suspectTo == 1794183200000


def test_unknown_extra_field_is_schema_invalid():
    invalid(OCCUPANCY, ARRIVAL.replace('"source"', '"shift":"A","source"'))
    invalid(INCIDENTS, INCIDENT.replace('"source"', '"severity":3,"source"'))


def test_schema_sha256_is_the_raw_digest_of_the_published_schema_file():
    raw = (ROOT / "schemas" / "occupancy-1.0.0.schema.json").read_bytes()
    assert inputs.schema_sha256("urn:forseti:schema:occupancy:1.0.0") == hashlib.sha256(raw).hexdigest()
    assert inputs.schema_sha256("urn:forseti:schema:occupancy:1.0.0") == "e97cd7c97b1ab5e2d63d5b06e6057415945b82cff6d2ca0345106d2daf9d29dc"
    assert inputs.schema_sha256("urn:forseti:schema:incident:1.0.0") == "7141c3115d90812f16b22049c66bfaa64942ebacf854b343fb99b3dce168fb34"
    assert inputs.incident_kinds() == ["tool-check-failed", "gauge-drift"]


# --- digests and identities -----------------------------------------------------------------------------------------------


def test_payload_digest_equals_java_pin():
    assert digest_of(SMOKE_OCC_1) == (SMOKE_OCC_1_DIGEST, "jcs")
    assert digest_of(SMOKE_OCC_1.replace('"recordTime":1794183000200', '"recordTime":1794183999999'))[0] == SMOKE_OCC_1_DIGEST
    assert digest_of(SMOKE_OCC_1.replace('"eventTime":1794183000000', '"eventTime":1794183000001'))[0] != SMOKE_OCC_1_DIGEST


def test_payload_digest_ignores_member_quality():
    m = digest_of(message('"bridgeReceivedAt":11,', '"quality":192,', "1.5"))
    assert m[1] == "jcs"
    assert digest_of(message('"bridgeReceivedAt":99,', '"quality":0,', "1.5")) == m
    assert digest_of(message("", "", "1.5")) == m
    assert digest_of(message('"bridgeReceivedAt":11,', '"quality":192,', "1.6")) != m


@pytest.mark.parametrize("raw", ["[9007199254740992]", SMOKE_OCC_1.replace("1794183000000", "9007199254740992"), '"text"'])
def test_non_canonicalizable_value_gets_raw_bytes_digest(raw):
    assert digest_of(raw) == (hashlib.sha256(raw.encode("utf-8")).hexdigest(), "raw-bytes")


def test_bytes_that_are_not_json_are_rejected_with_a_raw_bytes_digest():
    for raw in (b"not json", b'{"a":NaN}', b'{"a":1,"a":2}'):
        r = inputs.read(OCCUPANCY, raw)
        assert r.rejected and r.check is None and r.digest_rule == "raw-bytes" and r.payload_digest == hashlib.sha256(raw).hexdigest()


def test_sample_ids_of_two_members_differ_only_in_metric():
    m = inputs.Message("edge1", "d1", 1, 0, 2, 10, None, "x", (), ())
    a, b = inputs.sample_id(m, "Torque"), inputs.sample_id(m, "Running")
    assert a != b
    assert {k: v for k, v in a.items() if k != "metric"} == {k: v for k, v in b.items() if k != "metric"} == inputs.message_id(m)
    assert list(a) == ["edge", "device", "birthPublishTs", "metric", "bdSeq", "seq", "publishTs"]


def test_identity_bytes_order_by_canonical_form():
    assert inputs.identity_bytes({"a": 2}) < inputs.identity_bytes({"b": 1})
    assert inputs.identity_bytes({"source": "s", "edge": "e"}) == b'{"edge":"e","source":"s"}'


def test_overflowing_member_values_are_signed_infinities():
    for literal, expected in (("1e400", math.inf), ("-1e400", -math.inf), ("1" + "0" * 400, math.inf), ("-1" + "0" * 400, -math.inf)):
        msg = parse(MESSAGES, message("", "", literal))
        assert msg.members[0].value == expected and not msg.rejected_members


def test_is_bad_reads_the_severity_bits():
    assert inputs.is_bad(0x80000000) and inputs.is_bad(0x80350000) and not inputs.is_bad(0x40000000) and not inputs.is_bad(0)
    assert not inputs.is_bad(None) and inputs.is_uncertain(0x40920000) and not inputs.is_uncertain(None)


# --- the birthless session-ambiguous variant (plan D4) -------------------------------------------------------------------

BIRTHLESS = ('{"edge":"edge1","device":"synthetic-site:line2:station1:nutrunner1","birthPublishTs":null,'
             '"bdSeq":3,"kind":"session-ambiguous","atSeq":null,"payload":{"detectedEventTime":null,"rawPublishTs":1794183000500,'
             '"bdSeq":3,"payloadDigest":"' + "cd" * 32 + '"}}')


def test_the_birthless_session_ambiguous_variant_parses():
    s = parse(SESSIONS, BIRTHLESS)
    assert s.birthless and s.birthPublishTs is None and s.atSeq is None and s.bdSeq == 3
    assert parse(SESSIONS, with_(BIRTHLESS, "atSeq", "17")).atSeq == 17
    born = with_(with_(BIRTHLESS, "birthPublishTs", "1794182400000"), "atSeq", "17")
    assert not parse(SESSIONS, born).birthless
    # atSeq may be None only on the birthless variant: a session-ambiguous event with a birth keeps an integer atSeq
    assert invalid(SESSIONS, with_(born, "atSeq", "null")) == "atSeq"
    # a non-integer rawPublishTs on the birthless variant is refused, as on the core side
    assert invalid(SESSIONS, BIRTHLESS.replace('"rawPublishTs":1794183000500', '"rawPublishTs":"x"')) == "payload.rawPublishTs"
    assert invalid(SESSIONS, BIRTHLESS.replace('"rawPublishTs":1794183000500', '"rawPublishTs":true')) == "payload.rawPublishTs"


@pytest.mark.parametrize("kind", ["seq-gap", "session-down", "session-up", "stale-death", "definition-changed"])
def test_a_null_birth_or_at_seq_on_any_other_kind_is_refused(kind):
    base = with_(SESSION, "kind", '"' + kind + '"')
    assert invalid(SESSIONS, with_(base, "birthPublishTs", "null")) == "birthPublishTs"
    assert invalid(SESSIONS, with_(base, "atSeq", "null")) == "atSeq"
    assert invalid(SESSIONS, with_(with_(base, "birthPublishTs", "null"), "bdSeq", "-1")) == "birthPublishTs"


def test_an_absent_birth_or_at_seq_is_not_null():
    assert invalid(SESSIONS, BIRTHLESS.replace('"birthPublishTs":null,', "")) == "birthPublishTs"
    assert invalid(SESSIONS, BIRTHLESS.replace('"atSeq":null,', "")) == "atSeq"
    assert invalid(SESSIONS, with_(BIRTHLESS, "birthPublishTs", '"x"')) == "birthPublishTs"


def test_a_birthless_death_without_its_raw_publish_ts_or_payload_digest_is_refused():
    assert invalid(SESSIONS, BIRTHLESS.replace('"rawPublishTs":1794183000500,', "")) == "payload.rawPublishTs"
    assert invalid(SESSIONS, BIRTHLESS.replace(',"payloadDigest":"' + "cd" * 32 + '"', "")) == "payload.payloadDigest"
    assert invalid(SESSIONS, re.sub(r'"payload":\{.*\}\}$', '"payload":null}', BIRTHLESS)) == "payload.rawPublishTs"


def _birthless(raw_publish_ts, payload_digest):
    return ('{"edge":"e","device":"d","birthPublishTs":null,"bdSeq":3,"kind":"session-ambiguous","atSeq":null,'
            '"payload":{"detectedEventTime":null,"rawPublishTs":' + str(raw_publish_ts) + ',"bdSeq":3,"payloadDigest":"' + payload_digest + '"}}')


def test_a_session_event_with_a_birth_keeps_its_six_field_identity():
    s = parse(SESSIONS, '{"edge":"e","device":"d","birthPublishTs":1,"bdSeq":0,"kind":"session-ambiguous","atSeq":5,'
                        '"payload":{"detectedEventTime":9,"rawPublishTs":8,"bdSeq":0,"payloadDigest":"ab"}}')
    assert canonical_bytes(inputs.session_event_id(s)) == \
        b'{"atSeq":5,"bdSeq":0,"birthPublishTs":1,"device":"d","edge":"e","kind":"session-ambiguous"}'


def test_two_birthless_deaths_with_equal_counters_stay_distinct():
    a = canonical_bytes(inputs.session_event_id(parse(SESSIONS, _birthless(100, "aa"))))
    assert a == (b'{"atSeq":null,"bdSeq":3,"birthPublishTs":null,"device":"d","edge":"e","kind":"session-ambiguous",'
                 b'"payloadDigest":"aa","rawPublishTs":100}')
    assert a != canonical_bytes(inputs.session_event_id(parse(SESSIONS, _birthless(101, "aa"))))
    assert a != canonical_bytes(inputs.session_event_id(parse(SESSIONS, _birthless(100, "bb"))))
    assert a == canonical_bytes(inputs.session_event_id(parse(SESSIONS, _birthless(100, "aa"))))
