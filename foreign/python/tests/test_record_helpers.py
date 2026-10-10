import laser_sdk as ls
import pytest

DESTINATION_ID = "00000000000000000000000001"
REQUEST_ID = "00000000000000000000000002"
SET_DESIRED_STATE = {
    "kind": "set_desired_state",
    "destination_id": DESTINATION_ID,
    "destination_generation": 1,
    "expected_definition_revision": 1,
    "desired_state": "enabled",
}


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


def test_given_a_mutation_when_building_a_checkpoint_envelope_then_should_stamp_the_op_version():
    envelope = ls.new_checkpoint_request_envelope(REQUEST_ID, 4, SET_DESIRED_STATE)
    assert envelope["v"] == 1
    assert envelope["request_id"] == REQUEST_ID
    assert envelope["expected_global_state_revision"] == 4
    assert envelope["mutation"] == SET_DESIRED_STATE
    assert "supervisor_assertion" not in envelope


def test_given_a_malformed_request_id_when_building_a_checkpoint_envelope_then_should_raise():
    with pytest.raises(ls.InvalidError):
        ls.new_checkpoint_request_envelope("not-an-id", 0, SET_DESIRED_STATE)


def test_given_a_destination_mutation_when_asking_its_grant_then_should_need_destination_write():
    assert ls.public_checkpoint_mutation_required_capability(SET_DESIRED_STATE) == (
        "destination",
        "write",
    )


def test_given_a_stream_and_topic_when_building_a_source_scope_then_should_name_both():
    assert ls.new_source_scope("telemetry", "readings") == {
        "stream": "telemetry",
        "topic": "readings",
    }


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


def supervisor_assertion(**claims):
    return {
        "claims": {
            "v": 1,
            "request_id": REQUEST_ID,
            "deployment_id": 1,
            "cloud_user_id": 1,
            "action": "record_repair",
            "destination_id": DESTINATION_ID,
            "destination_generation": 1,
            "issued_at_micros": 1_000_000,
            "expires_at_micros": 2_000_000,
            **claims,
        },
        "key_id": bytes(8),
        "signature": bytes(64),
    }


def test_given_a_well_formed_supervisor_assertion_when_validated_then_should_pass():
    assert ls.validate_supervisor_actor_assertion(supervisor_assertion()) is None


@pytest.mark.parametrize(
    "claims",
    [{"v": 2}, {"deployment_id": 0}, {"expires_at_micros": 1_000_000}],
)
def test_given_a_broken_supervisor_assertion_when_validated_then_should_raise_invalid(claims):
    with pytest.raises(ls.InvalidError):
        ls.validate_supervisor_actor_assertion(supervisor_assertion(**claims))


def test_given_an_entity_when_minting_its_content_id_then_should_match_the_node_dict_id():
    alice = ls.graph_node_entity("Person", "Alice")
    acme = ls.graph_node_entity("Company", "Acme")
    edge = ls.graph_edge_relate(alice, "works_at", acme)
    assert ls.node_id_content("Person", "Alice") == alice["id"]
    assert ls.edge_id_content(alice["id"], "works_at", acme["id"]) == edge["id"]
    with pytest.raises(ls.InvalidError):
        ls.edge_id_content("not-an-id", "works_at", acme["id"])
