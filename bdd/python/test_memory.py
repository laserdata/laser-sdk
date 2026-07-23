from pathlib import Path

import laser_sdk as ls
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


@given("an empty semantic memory")
def open_semantic_memory(world):
    # The in-process vector memory hangs off a client handle, so a connected
    # laser is needed even though recall never leaves the process. These
    # scenarios carry no connect step of their own (unlike the reference-engine
    # ones), so establish it here.
    if world.laser is None:
        world.connect()

    async def embed(text: str) -> list[float]:
        dims = 64
        vector = [0.0] * dims
        for token in text.lower().split():
            token = "".join(char for char in token if char.isalnum())
            if not token:
                continue
            hash_value = 0xCBF29CE484222325
            for byte in token.encode():
                hash_value = ((hash_value ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
            vector[hash_value % dims] += 1.0
        return vector

    world.semantic_memory = world.laser.vector_memory(embed)
    world.semantic_conversation = str(ls.new_conversation_id())


@when(parsers.parse('I remember the fact "{body}"'))
def remember_fact(world, body):
    world.run(
        lambda: world.semantic_memory.remember(body, conversation=world.semantic_conversation)
    )


def _assert_first(world, strategy, query, expected):
    items = world.run(
        lambda: world.semantic_memory.recall(
            semantic=query,
            strategy=strategy,
            limit=10,
            conversation=world.semantic_conversation,
        )
    )
    assert items, "recall returned items"
    assert items[0].text == expected


@then(parsers.parse('keyword recall for "{query}" returns "{expected}" first'))
def keyword_recall_first(world, query, expected):
    _assert_first(world, "keyword", query, expected)


@then(parsers.parse('hybrid recall for "{query}" returns "{expected}" first'))
def hybrid_recall_first(world, query, expected):
    _assert_first(world, "hybrid", query, expected)
