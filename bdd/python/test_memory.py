from pathlib import Path

from pytest_bdd import given, parsers, scenarios, then, when
from reference import MemoryEngine

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "memory.feature"))


@given("an empty memory store")
def open_store(bench):
    bench.memory_engine = MemoryEngine()
    bench.memory_ids = {}


@when(parsers.parse('I remember "{body}" with dedup'))
def remember_dedup(bench, body):
    bench.memory_ids[body] = bench.memory_engine.remember(body.encode(), True)


@when(parsers.parse('I remember "{body}"'))
def remember(bench, body):
    bench.memory_ids[body] = bench.memory_engine.remember(body.encode(), False)


@when(parsers.parse('I give "{body}" a feedback weight of {weight:d}'))
def feedback(bench, body, weight):
    bench.memory_engine.improve(bench.memory_ids[body], float(weight))


@when(parsers.parse('I forget "{body}"'))
def forget(bench, body):
    bench.memory_engine.forget(bench.memory_ids[body])


@then(parsers.parse("the memory holds {count:d} item"))
@then(parsers.parse("the memory holds {count:d} items"))
def holds(bench, count):
    assert bench.memory_engine.live_len() == count


# Regex steps with `[^"]+` (mirroring the Rust steps): the quote-excluding
# capture keeps the single-item step from greedily matching the two-item form,
# which a `parse`-style `{only}` placeholder would.
@then(
    parsers.re(
        r"^recalling (?P<limit>\d+) items? returns "
        r'"(?P<first>[^"]+)" then "(?P<second>[^"]+)"$'
    )
)
def recall_two(bench, limit, first, second):
    assert bench.memory_engine.recall(int(limit)) == [first, second]


@then(parsers.re(r'^recalling (?P<limit>\d+) items? returns "(?P<only>[^"]+)"$'))
def recall_one(bench, limit, only):
    assert bench.memory_engine.recall(int(limit)) == [only]
