import laser_sdk as ls
import pytest

EXECUTION_ID = "01J9Z8Y7X6W5V4T3S2R1Q0P9N8"


def test_given_the_required_fields_when_building_a_query_then_should_match_the_operational_query():
    target = ls.query_target_operational("readings")
    built = (
        ls.QueryBuilder()
        .execution_id(EXECUTION_ID)
        .target(target)
        .deadline_micros(42)
        .by_key([ls.key_match_new("host", "a")])
        .filter(ls.Filter.pred("cpu", "gt", 80))
        .distinct(True)
        .consistency("read_your_writes")
        .build()
    )
    plain = ls.query_operational(EXECUTION_ID, "readings", 42)
    assert built["target"] == target == plain["target"]
    assert built["execution_id"] == plain["execution_id"]
    assert built["page"] == plain["page"]
    assert built["by_key"] == [ls.key_match_new("host", "a")]
    assert built["distinct"] is True
    assert built["consistency"] == "read_your_writes"
    assert ls.query_new(EXECUTION_ID, target, 42) == plain


def test_given_a_missing_required_field_when_building_a_query_then_should_raise_invalid():
    with pytest.raises(ls.InvalidError, match="target"):
        ls.QueryBuilder().execution_id(EXECUTION_ID).deadline_micros(1).build()


def test_given_result_code_names_when_mapped_then_should_follow_the_rust_codes():
    assert ls.result_code_code("NotFound") == 2
    assert ls.result_code_from_code(2) == "NotFound"
    assert ls.result_code_from_code(999) == "Unrecognized(999)"
    assert ls.result_code_code("Unrecognized(999)") == 999
    assert ls.result_code_http_status("NotFound") == 404
    assert ls.result_code_is_retryable("Unavailable") is True
    with pytest.raises(ls.InvalidError):
        ls.result_code_code("nope")


def test_given_a_pending_result_code_when_classified_then_should_only_allow_the_same_identity():
    assert ls.result_code_code("Pending") == 19
    assert ls.result_code_from_code(19) == "Pending"
    assert ls.result_code_from_code(21) == "Unrecognized(21)"
    assert ls.result_code_http_status("Pending") == 503
    assert ls.result_code_is_pending("Pending") is True
    assert ls.result_code_is_pending("Unavailable") is False
    assert ls.result_code_is_retryable("Pending") is False


def test_given_a_backend_unavailable_result_code_when_classified_then_should_name_the_node():
    assert ls.result_code_code("BackendUnavailable") == 20
    assert ls.result_code_from_code(20) == "BackendUnavailable"
    assert ls.result_code_http_status("BackendUnavailable") == 503
    assert ls.result_code_is_backend_unavailable("BackendUnavailable") is True
    assert ls.result_code_is_backend_unavailable("Unavailable") is False
    assert ls.result_code_is_retryable("BackendUnavailable") is True


def test_given_tagged_values_when_read_then_should_delegate_to_the_typed_value():
    value = ls.key_match_new("n", 7)["value"]
    assert ls.typed_value_as_i64(value) == 7
    assert ls.typed_value_as_u64(value) == 7
    assert ls.typed_value_as_str(value) is None
    assert ls.typed_value_diagnostic_text(value) == "7"
    ls.typed_value_validate_canonical(value)
    long_type = {"kind": "long"}
    ls.typed_value_validate_against(value, long_type, True)
    assert ls.logical_type_kind(long_type) == "long"
    assert ls.logical_type_accepts_map_key(long_type) is True
    with pytest.raises(ls.InvalidError):
        ls.typed_value_validate_against(value, {"kind": "string"}, True)


def test_given_a_page_when_bound_then_should_read_each_row_by_field_name_or_position():
    host = ls.key_match_new("host", "node-7")["value"]
    cpu = ls.key_match_new("cpu", 91)["value"]
    fields = [
        {"id": 1, "name": "host", "required": False, "field_type": {"kind": "string"}},
        {"id": 2, "name": "cpu", "required": False, "field_type": {"kind": "variant"}},
    ]
    [row] = ls.QueryRow.from_page(fields, [{"values": [host, cpu]}])
    assert row.keys() == ["host", "cpu"]
    assert len(row) == 2
    assert row["host"] == host
    assert row[1] == row["cpu"] == row[-1] == cpu
    assert row.value_text("host") == "node-7"
    assert row.value_i64("cpu") == 91
    assert row.field_index("cpu") == 1
    assert row.value("absent") is None
    assert row.fields[1]["field_type"] == {"kind": "variant"}
    with pytest.raises(KeyError):
        row["absent"]
    with pytest.raises(IndexError):
        row[2]
    with pytest.raises(TypeError):
        row[1.5]


def test_given_logical_fields_when_building_a_schema_then_should_fingerprint_them():
    fields = [{"id": 1, "name": "cpu", "required": True, "field_type": {"kind": "long"}}]
    schema = ls.LogicalSchema(7, 1, fields)
    assert schema.fields == fields
    assert schema.schema["version"] == 1
    assert bytes(schema.schema["fingerprint"]) == schema.compute_fingerprint()
    assert schema.canonical_fingerprint_bytes().startswith(b"AGDX-SCHEMA-V1\0")
    with pytest.raises(ls.InvalidError):
        ls.LogicalSchema(7, 0, fields)
