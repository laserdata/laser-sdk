from pathlib import Path

import laser_sdk as ls
from pytest_bdd import parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "memory_correction.feature"))

NAMESPACE = "prefs"
USER = "ana"


def memory(world):
    if getattr(world, "prefs", None) is None:
        world.prefs = world.laser.memory(NAMESPACE)
        world.conversations = {}
    return world.prefs


def conversation(world, label):
    return world.conversations.setdefault(label, ls.new_conversation_id())


@when(parsers.re(r'the user states "(?P<fact>[^"]+)"'))
def state(world, fact):
    world.last_fact = world.run(lambda: memory(world).remember(fact, user=USER))


@when(parsers.re(r'the user states "(?P<fact>[^"]+)" in conversation "(?P<label>[^"]+)"'))
def state_in(world, fact, label):
    handle = memory(world)
    world.last_fact = world.run(
        lambda: handle.remember(fact, user=USER, conversation=conversation(world, label))
    )


@when(parsers.parse('the user corrects it to "{fact}" in conversation "{label}"'))
def correct(world, fact, label):
    handle = memory(world)
    world.last_fact = world.run(
        lambda: handle.supersede(
            world.last_fact, fact, user=USER, conversation=conversation(world, label)
        )
    )


@then("the memory topic exists on the stream")
def memory_topic_exists(world):
    topic = world.laser.stream(world.laser.default_stream).topic("agent.memory")
    assert world.run(topic.exists)


@then(parsers.parse('a folded recall for the user returns "{fact}" only'))
def folded_recall_only(world, fact):
    items = world.run(lambda: memory(world).recall(user=USER, folded=True))
    assert [item.text() for item in items] == [fact]


@then(parsers.parse('a recall for the user returns "{fact}" only'))
def recall_only(world, fact):
    items = world.run(lambda: memory(world).recall(user=USER))
    assert [item.text() for item in items] == [fact]


@then(parsers.re(r'a folded semantic recall for "(?P<query>[^"]+)" returns (?P<facts>".+")'))
def folded_semantic_recall(world, query, facts):
    items = world.run(lambda: memory(world).recall(user=USER, semantic=query, folded=True))
    assert [item.text() for item in items] == facts.strip('"').split('", "')


@then(parsers.parse('a folded keyword recall for "{query}" returns "{fact}" only'))
def folded_keyword_recall(world, query, fact):
    items = world.run(
        lambda: memory(world).recall(user=USER, semantic=query, strategy="keyword", folded=True)
    )
    assert [item.text() for item in items] == [fact]
