import time

import pytest
from laser_sdk import InvalidError, LaserError, Materialization, UnsupportedError

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_given_a_content_change_without_a_pinned_id_when_set_then_should_mint_a_new_operation(
    laser,
):
    request = laser.topic("orders").materialize()
    first = request.operation
    request.index("orders_rows")
    assert request.operation != first
    request.operation_id(7).projection_id("orders.v1")
    assert request.operation == 7


@pytest.mark.asyncio
async def test_given_a_retention_dict_that_names_no_policy_when_set_then_should_raise_invalid(
    laser,
):
    with pytest.raises(InvalidError):
        laser.topic("orders").materialize().retention({"kind": "time_to_live"})


@pytest.mark.asyncio
async def test_given_a_topic_when_reattaching_then_should_return_a_materialization_handle(
    laser,
):
    attached = laser.topic("orders").materialization("orders")
    assert isinstance(attached, Materialization)
    with pytest.raises(UnsupportedError):
        await attached.status()


@pytest.mark.asyncio
async def test_given_a_refused_status_read_when_waiting_then_should_raise_it_before_the_timeout(
    laser,
):
    managed = laser.with_capabilities(managed=True, materializations=True)
    started = time.monotonic()
    with pytest.raises(LaserError):
        await managed.materialization("orders").wait("realized", 30_000)
    assert time.monotonic() - started < 5, "the wait must not run out its timeout"


@pytest.mark.asyncio
async def test_given_a_page_when_listing_projections_then_should_refuse_on_open_iggy(laser):
    with pytest.raises(UnsupportedError):
        await laser.projections().list(page=2, results=5)
