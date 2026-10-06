import laser_sdk as ls
import pytest

SPIKE = ls.FilterExpr.pred("cpu", "gte", 90)


def test_given_a_path_when_parsed_then_should_expose_keys_and_indexes():
    path = ls.FieldPath.parse("after.ground_stations[0]")
    assert path.segments == ["after", "ground_stations", 0]
    assert str(path) == "after.ground_stations[0]"
    assert path == ls.FieldPath.parse("after.ground_stations[0]")
    with pytest.raises(ls.InvalidError):
        ls.FieldPath.parse("")


def test_given_a_text_match_when_made_case_insensitive_then_should_match_any_case():
    expr = ls.FilterExpr.text("status", "equals", "ONLINE").case_insensitive()
    assert expr.to_dict()["text"]["case_insensitive"] is True
    compiled = ls.CompiledFilter.compile(ls.ConsumerFilter.json(expr))
    assert compiled.evaluate(b'{"status": "online"}') == "selected"
    assert ls.FilterExpr.text("status", "equals", "ONLINE").case_insensitive() == expr


def test_given_expressions_when_inspected_then_should_report_what_they_read():
    assert SPIKE.reads_payload is True
    assert SPIKE.reads_headers is False
    header = ls.FilterExpr.header("region", "eq", "eu")
    assert header.reads_payload is False
    assert header.reads_headers is True
    either = ls.FilterExpr.any([SPIKE, header])
    assert (either.reads_payload, either.reads_headers) == (True, True)


def test_given_a_filter_when_read_then_should_expose_every_field():
    filter = (
        ls.ConsumerFilter.avro(SPIKE, [7, 9])
        .with_foreign_policy(foreign_policy="pass")
        .with_mismatch_policy(mismatch_policy="pass")
        .with_fault_policy(fault_policy="drop")
    )
    assert filter.v == 1
    assert filter.evaluator_version == ls.FILTER_EVALUATOR_VERSION
    assert filter.foreign_policy == "pass"
    assert filter.mismatch_policy == "pass"
    assert filter.fault_policy == "drop"
    assert filter.schema_refs == [7, 9]
    assert ls.ConsumerFilter.json(SPIKE).foreign_policy == "reject"


def test_given_word_values_when_classified_then_should_match_the_rust_helpers():
    assert ls.execution_mode_is_filtered("filtered") is True
    assert ls.execution_mode_is_filtered("unfiltered") is False
    assert ls.fault_reason_is_foreign("foreign_codec") is True
    assert ls.fault_reason_is_foreign("malformed") is False
    assert ls.read_mode_is_primary("primary") is True
    assert ls.read_mode_is_primary("local") is False
    assert ls.record_policy_is_reject("reject") is True
    assert ls.record_policy_is_reject("pass") is False
    assert ls.filter_error_reason_code("not_found") == "NotFound"
    assert ls.filter_error_reason_code("invalid_request") == "InvalidArgument"
    with pytest.raises(ls.InvalidError):
        ls.record_policy_is_reject("maybe")


def test_given_timestamps_when_converted_then_should_return_epoch_micros():
    assert ls.timestamp_format_micros_from_integer("epoch_seconds", 2) == 2_000_000
    assert ls.timestamp_format_micros_from_integer("epoch_millis", 2) == 2_000
    assert ls.timestamp_format_micros_from_integer("rfc3339", 2) is None
    assert ls.timestamp_format_micros_from_text("rfc3339", "1970-01-01T00:00:01Z") == 1_000_000
    assert ls.timestamp_format_micros_from_text("rfc3339", "yesterday") is None


def test_given_text_predicates_when_validated_then_should_reject_a_broken_pattern():
    ls.text_predicate_validate({"field": "name", "kind": "regex", "pattern": "^sat-[0-9]+$"})
    with pytest.raises(ls.InvalidError):
        ls.text_predicate_validate({"field": "name", "kind": "regex", "pattern": "("})


def test_given_applied_policies_when_built_then_should_match_the_wire_dict():
    unfiltered = ls.applied_policy_unfiltered(3, 1)
    assert unfiltered["group_id"] == 3
    assert unfiltered["mode"] == "unfiltered"
    assert unfiltered["policy_generation"] == 1
    filtered = ls.applied_policy_filtered(None, bytes(32))
    assert filtered["mode"] == "filtered"
    assert "group_id" not in filtered
    with pytest.raises(ls.InvalidError):
        ls.applied_policy_filtered(None, b"short")


def test_given_this_build_when_asked_what_it_serves_then_should_evaluate_its_own_filters():
    served = ls.FilterAnnounce.served()
    assert served.evaluator_version == ls.FILTER_EVALUATOR_VERSION
    assert "json" in served.codecs
    assert served.evaluates(ls.FILTER_EVALUATOR_VERSION, "protobuf") is True
    assert served.evaluates(ls.FILTER_EVALUATOR_VERSION + 1, "json") is False
    with pytest.raises(ls.InvalidError):
        served.evaluates(ls.FILTER_EVALUATOR_VERSION, "xml")


def test_given_an_untrusted_path_when_building_presence_then_should_raise_invalid_for_a_bad_path():
    assert ls.FilterExpr.try_present("after.mode") == ls.FilterExpr.present("after.mode")
    assert ls.FilterExpr.try_absent("after.mode") == ls.FilterExpr.absent("after.mode")
    for build in (ls.FilterExpr.try_present, ls.FilterExpr.try_absent):
        with pytest.raises(ls.InvalidError):
            build("")


def test_given_an_invalid_error_when_converted_then_should_be_an_invalid_request_filter_error():
    error = ls.FilterError.invalid(ls.InvalidError("bad path"))
    assert isinstance(error, ls.FilterError)
    assert error.reason == "invalid_request"
    assert error.detail["message"] == "bad path"
    with pytest.raises(TypeError):
        ls.FilterError.invalid(ValueError("bad path"))


def test_given_a_filter_op_version_when_checked_then_should_raise_version_skew_for_another():
    assert ls.FilterError.check_version(ls.FILTER_OP_VERSION) is None
    with pytest.raises(ls.FilterError) as raised:
        ls.FilterError.check_version(ls.FILTER_OP_VERSION + 1)
    assert raised.value.reason == "version_skew"
    assert raised.value.version_skew
