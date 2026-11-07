import laser_sdk as ls
import pytest


def test_given_a_projection_builder_when_built_then_should_return_the_projection_dict():
    projection = (
        ls.ProjectionBuilder("readings")
        .name("Readings")
        .version(2)
        .content_type("json")
        .field("host")
        .fields(["region", "rack"])
        .field_at("cpu_pct", "/cpu/value")
        .field_typed("cores", "int")
        .field_at_typed("load", "/load/avg", "float")
        .vector_field("/vec")
        .index_only()
        .build()
    )

    assert projection["id"] == "readings"
    assert projection["name"] == "Readings"
    assert projection["version"] == 2
    assert projection["inline_payload_default"] is False
    extraction = projection["extraction"]
    assert [field["name"] for field in extraction["fields"]] == [
        "host",
        "region",
        "rack",
        "cpu_pct",
        "cores",
        "load",
    ]
    assert extraction["fields"][3]["pointer"] == "/cpu/value"
    assert extraction["fields"][4] == {"name": "cores", "pointer": "/cores", "field_type": "int"}
    assert extraction["vector_field"] == "/vec"
    assert extraction["inline_payload"] is False
    assert "kind" not in projection


def test_given_a_graph_schema_when_built_then_should_mark_the_projection_graph():
    schema = {"nodes": [{"label": "host", "value_pointer": "/host"}]}

    projection = ls.ProjectionBuilder("fleet").graph(schema).build()

    assert projection["kind"] == 1
    assert ls.projection_kind_is_row(projection["kind"]) is False
    assert ls.projection_kind_code(projection["kind"]) == 1
    assert projection["entity_schema"]["nodes"][0]["label"] == "host"


def test_given_a_keyed_builder_without_a_delete_header_when_built_then_should_omit_it():
    key = {"kind": "header", "name": "order_id"}

    never_deletes = ls.ProjectionBuilder("orders").record().keyed(key).build()
    deletes = ls.ProjectionBuilder("orders").record().keyed(key, delete_header="deleted").build()

    assert never_deletes["history"] == {"kind": "keyed", "key": key}
    assert deletes["history"] == {"kind": "keyed", "key": key, "delete_header": "deleted"}


def test_given_a_keyed_history_dict_without_a_delete_header_when_read_then_should_be_keyed():
    history = {"kind": "keyed", "key": {"kind": "pointer", "pointer": "/id"}}

    assert ls.is_append_projection_history(history) is False


def test_given_a_spent_builder_when_built_again_then_should_raise_invalid():
    builder = ls.ProjectionBuilder("readings")
    builder.build()

    with pytest.raises(ls.InvalidError):
        builder.build()
    with pytest.raises(ls.InvalidError):
        builder.field("host")


def test_given_an_index_schema_builder_when_built_then_should_feed_a_projection_extraction():
    schema = (
        ls.IndexSchemaBuilder()
        .field("host")
        .field_at("cpu", "/cpu/value")
        .vector_field("/e")
        .inline_payload()
        .build()
    )

    projection = ls.ProjectionBuilder("readings").extraction(schema).build()

    assert projection["extraction"] == schema
    assert schema["fields"] == [
        ls.index_field_new("host", "/host"),
        ls.index_field_new("cpu", "/cpu/value"),
    ]
    assert schema["inline_payload"] is True


def test_given_a_binding_builder_when_built_then_should_default_the_index_to_the_topic():
    binding = (
        ls.ProjectionBindingBuilder()
        .source("telemetry", "readings")
        .allow("readings")
        .default_projection("readings")
        .retention({"kind": "time_to_live", "ttl_micros": 60_000_000})
        .notify()
        .build()
    )

    assert binding["source"] == ls.source_selector_new("telemetry", "readings")
    assert binding["index"] == "readings"
    assert binding["allowed_projections"] == ["readings"]
    assert binding["default_projection"] == "readings"
    assert binding["retention"] == {"kind": "time_to_live", "ttl_micros": 60_000_000}
    assert binding["notify"] is True
    assert "invalid_records" not in binding


def test_given_a_binding_that_skips_rejected_records_when_built_then_should_carry_the_opt_in():
    binding = (
        ls.ProjectionBindingBuilder()
        .source("telemetry", "readings")
        .invalid_records("dead_letter")
        .build()
    )

    assert binding["invalid_records"] == "dead_letter"


def test_given_a_binding_without_source_when_built_then_should_raise_invalid():
    with pytest.raises(ls.InvalidError):
        ls.ProjectionBindingBuilder().index("readings").try_build()
    with pytest.raises(ls.InvalidError):
        ls.ProjectionBindingBuilder().build()


def test_given_a_selector_dict_when_bound_then_should_route_that_source():
    binding = (
        ls.ProjectionBindingBuilder().selector({"stream": "s", "topic": "t"}).index("idx").build()
    )

    assert binding["source"] == {"stream": "s", "topic": "t"}
    assert binding["index"] == "idx"


def test_given_field_helpers_when_called_then_should_match_the_rust_field_dicts():
    assert ls.index_field_typed("cores", "/cores", "int") == {
        "name": "cores",
        "pointer": "/cores",
        "field_type": "int",
    }
    with pytest.raises(ls.InvalidError):
        ls.index_field_typed("cores", "/cores", "decimal")


def test_given_schema_dicts_when_classified_then_should_report_their_content_type():
    avro = {"id": 1, "source": {"kind": "avro", "schema": "{}"}}
    json_schema = {"id": 2, "source": {"kind": "json_schema", "schema": "{}"}}

    assert ls.schema_def_content_type(avro) == "avro"
    assert ls.schema_def_content_type(json_schema) == "json"


def test_given_binding_policies_when_built_then_should_encode_both_nondefaults():
    binding = (
        ls.ProjectionBindingBuilder()
        .source("telemetry", "readings")
        .invalid_records("dead_letter")
        .oversize("reference")
        .build()
    )
    assert binding["invalid_records"] == "dead_letter"
    assert binding["oversize"] == "reference"


@pytest.mark.parametrize("policy", ["unknown", "invalid", ""])
@pytest.mark.parametrize("method", ["invalid_records", "oversize"])
def test_given_invalid_binding_policy_when_selected_then_should_raise_invalid(method, policy):
    builder = ls.ProjectionBindingBuilder().source("telemetry", "readings")
    with pytest.raises(ls.InvalidError):
        getattr(builder, method)(policy)
