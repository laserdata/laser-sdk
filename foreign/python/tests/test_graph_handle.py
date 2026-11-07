import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_given_filters_and_a_multi_step_hop_when_building_then_should_carry_them_in_the_query(
    laser,
):
    node_filter = ls.Filter.pred("label", "eq", "Person")
    edge_filter = ls.Filter.pred("edge_type", "eq", "knows")
    query = (
        laser.with_resource_naming("bare")
        .graph("kg")
        .hop("both", None, 3)
        .out("works_at")
        .node_filter(node_filter)
        .edge_filter(edge_filter)
        .limit(7)
        .into_query()
    )
    assert query["traverse"] == [
        {"dir": "both", "max": 3},
        {"edge_type": "works_at", "max": 1},
    ]
    assert query["node_filter"] == node_filter.to_dict()
    assert query["edge_filter"] == edge_filter.to_dict()
    assert query["limit"] == 7
    assert "fork" not in query and "consistency" not in query


@pytest.mark.asyncio
async def test_given_an_invalid_hop_or_limit_when_fetching_then_should_raise_invalid_before_sending(
    laser,
):
    for graph in (
        laser.graph("kg").hop("out", None, 0),
        laser.graph("kg").hop("out", None, 9),
        laser.graph("kg").hop("out", None, 5).hop("out", None, 4),
        laser.graph("kg").limit(10_001),
    ):
        with pytest.raises(ls.InvalidError):
            await graph.fetch()
    with pytest.raises(OverflowError):
        laser.graph("kg").limit(-1)
