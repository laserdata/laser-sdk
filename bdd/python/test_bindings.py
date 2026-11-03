from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "bindings.feature"))


@given(parsers.parse('a binding from stream "{stream}" topic "{topic}"'))
def binding_from(bench, stream, topic):
    bench.binding_builder = ls.ProjectionBindingBuilder().source(stream, topic)


@when("the binding lets the backend skip rejected records")
def skip_rejected(bench):
    bench.binding_builder = bench.binding_builder.skip_rejected()


@when("I build the binding")
def build_binding(bench):
    bench.binding = bench.binding_builder.build()


@then("the binding's wire form omits skip_rejected")
def omits_skip_rejected(bench):
    assert "skip_rejected" not in bench.binding


@then("the binding's wire form carries skip_rejected true")
def carries_skip_rejected(bench):
    assert bench.binding["skip_rejected"] is True
