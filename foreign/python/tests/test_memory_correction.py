import laser_sdk as ls
import pytest

USER = "ana"


@pytest.mark.integration
async def test_given_a_stream_without_a_memory_topic_when_remembering_then_should_create_it(
    iggy_endpoint,
):
    async with await ls.Laser.connect(iggy_endpoint) as laser:
        async with laser.scratch_stream("memory") as scratch:
            memory = scratch.laser.memory("prefs")
            await memory.remember("the user prefers green tea", user=USER)
            topic = await scratch.laser.stream(scratch.name).topic("agent.memory").get()
            assert topic is not None
            assert (topic.partitions_count, topic.message_expiry_ms) == (1, 30 * 86_400_000)


@pytest.mark.integration
async def test_given_an_earlier_fact_when_superseded_then_should_recall_only_the_correction(
    open_laser,
):
    memory = open_laser.memory("prefs")
    old = await memory.remember(
        "the user is allergic to peanuts", user=USER, conversation=ls.new_conversation_id()
    )
    new = await memory.supersede(
        old, "the user is allergic to shellfish", user=USER, conversation=ls.new_conversation_id()
    )
    assert new != old
    recalled = await memory.recall(user=USER, semantic="what is the user allergic to")
    assert [item.text() for item in recalled] == ["the user is allergic to shellfish"]


@pytest.mark.integration
async def test_given_facts_without_a_query_word_when_recalled_by_meaning_then_should_rank_all(
    open_laser,
):
    memory = open_laser.memory("prefs")
    for fact in [
        "the user is allergic to peanuts",
        "green tea every morning",
        "the user lives in Lisbon",
    ]:
        await memory.remember(fact, user=USER)
    ranked = await memory.recall(user=USER, semantic="which city does the user live in")
    assert [item.text() for item in ranked] == [
        "the user lives in Lisbon",
        "the user is allergic to peanuts",
        "green tea every morning",
    ]
    keyword = await memory.recall(user=USER, semantic="Lisbon", strategy="keyword")
    assert [item.text() for item in keyword] == ["the user lives in Lisbon"]
    block = await memory.recall_block(user=USER, strategy="recent", token_budget=1)
    assert isinstance(block, str)


async def test_given_an_unknown_recall_strategy_when_recalled_then_should_list_the_valid_ones():
    memory = ls.MemoryHandle.vector(lambda text: [float(len(text))])
    with pytest.raises(ls.InvalidError) as raised:
        await memory.recall(semantic="tea", strategy="similar")
    assert str(raised.value).endswith(
        "unknown recall strategy 'similar': use one of auto, recent, semantic, keyword, graph, "
        "temporal, hybrid"
    )


async def test_given_an_invalid_old_id_when_superseding_then_should_refuse_before_writing():
    memory = ls.MemoryHandle.vector(lambda text: [float(len(text))])
    with pytest.raises(ls.InvalidError):
        await memory.supersede("not-a-memory-id", "the user is allergic to shellfish")
    assert await memory.recall() == []
