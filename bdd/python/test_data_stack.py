from pathlib import Path

from pytest_bdd import given, parsers, scenarios, then, when
from reference import DataStackModel

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "data_stack.feature"))


@given("a fresh data stack contract model")
def fresh_model(bench):
    bench.data_stack = DataStackModel()


@when(parsers.parse('I register destination "{name}" at global revision {revision:d}'))
def register_destination(bench, name, revision):
    bench.data_stack.register(name, revision)


@when(
    parsers.parse(
        'I enable destination "{name}" at global revision {global_revision:d} '
        "and definition revision {definition_revision:d}"
    )
)
def enable_destination(bench, name, global_revision, definition_revision):
    bench.data_stack.enable(name, global_revision, definition_revision)


@when(
    parsers.parse(
        "I record a retention gap from required offset {required:d} to retained offset {retained:d}"
    )
)
def record_gap(bench, required, retained):
    assert retained >= required
    bench.data_stack.record_gap("orders-lakehouse")


@when(
    parsers.parse(
        "I accept the retention gap at next offset {next_offset:d} "
        "and checkpoint revision {checkpoint_revision:d}"
    )
)
def accept_gap(bench, next_offset, checkpoint_revision):
    bench.data_stack.accept_gap("orders-lakehouse", next_offset, checkpoint_revision)


@then("the operation is accepted with a stable identity")
def operation_accepted(bench):
    assert bench.data_stack.operation["id"]


@then(parsers.parse('destination "{name}" is disabled at definition revision {revision:d}'))
def destination_disabled(bench, name, revision):
    destination = bench.data_stack.destinations[name]
    assert destination["effective_state"] == "disabled"
    assert destination["definition_revision"] == revision


@then(parsers.parse('destination "{name}" is running at definition revision {revision:d}'))
def destination_running(bench, name, revision):
    destination = bench.data_stack.destinations[name]
    assert destination["effective_state"] == "running"
    assert destination["definition_revision"] == revision


@then(parsers.parse("the destination mutation conflicts with global revision {revision:d}"))
def mutation_conflicts(bench, revision):
    assert bench.data_stack.error == {"kind": "conflict", "observed_revision": revision}


@then(
    parsers.parse(
        'destination "{name}" is blocked by the retention gap at checkpoint revision {revision:d}'
    )
)
def destination_blocked(bench, name, revision):
    destination = bench.data_stack.destinations[name]
    assert destination["effective_state"] == "blocked"
    assert destination["checkpoint_revision"] == revision


@then(parsers.parse('destination "{name}" is running at next offset {next_offset:d}'))
def destination_at_offset(bench, name, next_offset):
    destination = bench.data_stack.destinations[name]
    assert destination["effective_state"] == "running"
    assert destination["next_offset"] == next_offset


@given(parsers.parse('a query execution with rows "{rows}"'))
def seed_query(bench, rows):
    bench.data_stack.seed_query(rows.split(", "))


@when(parsers.parse("I read a query page with limit {limit:d}"))
def read_page(bench, limit):
    bench.data_stack.read_page(limit)


@then(parsers.parse('the query page contains "{rows}" and a continuation cursor'))
def page_has_cursor(bench, rows):
    assert bench.data_stack.query_page["rows"] == rows.split(", ")
    assert bench.data_stack.query_page["cursor"] is not None


@when("I cancel the query execution")
def cancel_query(bench):
    bench.data_stack.cancel_query()


@then("the query execution status is cancelled")
def query_is_cancelled(bench):
    assert bench.data_stack.query_cancelled


@then("the continuation page is rejected as cancelled")
def cancelled_page_rejected(bench):
    bench.data_stack.read_page(2)
    assert bench.data_stack.error == {"kind": "cancelled"}
