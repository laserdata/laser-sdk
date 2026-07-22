from pathlib import Path

from pytest_bdd import given, parsers, scenarios, then, when
from reference import QueryEngine

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "query.feature"))


@given(parsers.parse('a query index "{index}" seeded with sample api-call rows'))
def seed_index(bench, index):
    engine = QueryEngine()
    for status, latency in (("200", "10"), ("200", "550"), ("500", "900"), ("200", "30")):
        engine.insert(index, {"status": status, "latency_ms": latency})
    bench.query_engine = engine
    bench.index = index


@when(parsers.parse('I query "{index}" for latency_ms greater than {bound:d}'))
def query_filter(bench, index, bound):
    bench.last_query = bench.query_engine.execute(index, predicate=("latency_ms", "gt", bound))


@when(parsers.parse('I query "{index}" ordered by latency_ms descending'))
def query_ordered(bench, index):
    bench.last_query = bench.query_engine.execute(index, order=[("latency_ms", "desc")])


@when(parsers.parse('I query "{index}" with limit {limit:d}'))
def query_limited(bench, index, limit):
    bench.last_query = bench.query_engine.execute(index, limit=limit)


@when(parsers.parse('I count "{index}" grouped by status'))
def query_count(bench, index):
    bench.last_query = bench.query_engine.execute(index, aggregate=(["status"], "count", "count"))


@then(parsers.parse("the query returns {expected:d} rows"))
def then_returns_rows(bench, expected):
    assert len(bench.last_query["rows"]) == expected


@then("every returned row has latency_ms greater than 500")
def then_rows_exceed_bound(bench):
    for row in bench.last_query["rows"]:
        assert int(row["latency_ms"]) > 500


@then(parsers.parse('the returned latency_ms values are "{expected}" in order'))
def then_values_in_order(bench, expected):
    got = [row["latency_ms"] for row in bench.last_query["rows"]]
    assert got == expected.split(", ")


@then(parsers.parse("the page total is {total:d}"))
def then_page_total(bench, total):
    assert bench.last_query["total"] == total


@then(parsers.parse('group "{status}" has count {count:d}'))
def then_group_count(bench, status, count):
    row = next(row for row in bench.last_query["rows"] if row.get("status") == status)
    assert row["count"] == str(count)
