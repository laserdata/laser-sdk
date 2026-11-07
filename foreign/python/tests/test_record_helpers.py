import laser_sdk as ls
import pytest


@pytest.mark.parametrize("store", ["InMemoryStore", "FileStore"])
def test_given_a_native_state_store_when_checking_its_type_then_should_be_a_state_store(store):
    assert issubclass(getattr(ls, store), ls.StateStore)


async def test_given_an_in_memory_store_when_used_through_the_base_then_should_dispatch():
    store = ls.InMemoryStore()
    await ls.StateStore.set(store, "k", b"v")
    assert await ls.StateStore.get(store, "k") == b"v"
    await ls.StateStore.delete(store, "k")
    assert await ls.StateStore.get(store, "k") is None


@pytest.mark.parametrize("store", ["KvSnapshotStore", "TopicSnapshotStore"])
def test_given_a_native_snapshot_store_when_checking_its_type_then_should_be_a_snapshot_store(
    store,
):
    assert issubclass(getattr(ls, store), ls.SnapshotStore)


def test_given_a_fold_snapshot_when_resuming_then_should_start_past_the_folded_offset():
    snapshot = {
        "stream": "agents",
        "stream_id": 0,
        "stream_created_at_micros": 100,
        "conversation": ls.new_conversation_id(),
        "fold": "planner",
        "as_of": [[2, 20, 0, 7]],
        "state": b"",
    }
    assert ls.fold_snapshot_resume_offset(snapshot, 2, 20, 0) == 8
    assert ls.fold_snapshot_resume_offset(snapshot, 2, 21, 0) == 0


def test_given_an_edge_when_windowed_then_should_contain_only_its_valid_range():
    alice = ls.graph_node_entity("Person", "Alice")
    acme = ls.graph_node_entity("Company", "Acme")
    edge = ls.graph_edge_relate(alice, "works_at", acme)
    assert ls.graph_edge_valid_at(edge, 5)
    windowed = ls.graph_edge_valid(edge, 10, 20)
    assert windowed["id"] == edge["id"]
    assert (windowed["valid_from"], windowed["valid_to"]) == (10, 20)
    assert not ls.graph_edge_valid_at(windowed, 9)
    assert ls.graph_edge_valid_at(windowed, 10)
    assert not ls.graph_edge_valid_at(windowed, 20)


def test_given_an_edge_when_sourced_then_should_keep_its_id_and_carry_the_source():
    alice = ls.graph_node_entity("Person", "Alice")
    acme = ls.graph_node_entity("Company", "Acme")
    edge = ls.graph_edge_relate(alice, "works_at", acme)
    source = {"Kv": {"namespace": "people", "key": "alice"}}
    sourced = ls.graph_edge_with_source(edge, source)
    assert sourced["id"] == edge["id"]
    assert sourced["source"] == source


@pytest.mark.parametrize(("direction", "out"), [("out", True), ("in", False), ("both", False)])
def test_given_an_edge_direction_when_checking_the_default_then_should_be_out(direction, out):
    assert ls.edge_dir_is_out(direction) is out


@pytest.mark.parametrize(
    ("returns", "nodes"),
    [("nodes", True), ("edges", False), ("paths", False), ("triplets", False)],
)
def test_given_a_graph_return_when_checking_the_default_then_should_be_nodes(returns, nodes):
    assert ls.graph_return_is_nodes(returns) is nodes


def test_given_an_entity_when_minting_its_content_id_then_should_match_the_node_dict_id():
    alice = ls.graph_node_entity("Person", "Alice")
    acme = ls.graph_node_entity("Company", "Acme")
    edge = ls.graph_edge_relate(alice, "works_at", acme)
    assert ls.node_id_content("Person", "Alice") == alice["id"]
    assert ls.edge_id_content(alice["id"], "works_at", acme["id"]) == edge["id"]
    with pytest.raises(ls.InvalidError):
        ls.edge_id_content("not-an-id", "works_at", acme["id"])
