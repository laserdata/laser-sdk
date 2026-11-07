from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "bindings.feature"))


@given(parsers.parse('a binding from stream "{stream}" topic "{topic}"'))
def binding_from(bench, stream, topic):
    bench.binding_builder = ls.ProjectionBindingBuilder().source(stream, topic)


@when("the binding lets the backend skip rejected records")
def invalid_records(bench):
    bench.binding_builder = bench.binding_builder.invalid_records("dead_letter")


@when("I build the binding")
def build_binding(bench):
    bench.binding = bench.binding_builder.build()


@then("the binding's wire form omits invalid_records")
def omits_invalid_records(bench):
    assert "invalid_records" not in bench.binding


@then("the binding's wire form carries invalid_records dead_letter")
def carries_invalid_records(bench):
    assert bench.binding["invalid_records"] == "dead_letter"


@when("the binding keeps references for oversized records")
def reference_oversize(bench):
    bench.binding_builder = bench.binding_builder.oversize("reference")


@then("the binding's wire form carries oversize reference")
def carries_oversize_reference(bench):
    assert bench.binding["oversize"] == "reference"
