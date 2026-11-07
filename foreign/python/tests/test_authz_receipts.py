import asyncio
import uuid

import laser_sdk as ls
import pytest
from iggy_test_server import IggyTestCluster

pytestmark = pytest.mark.integration

RECEIPT_CAPACITY = 10_000
ROOT = 0


def _grant():
    return ls.Grant("authz", "read")


async def _requires_authz(laser):
    if not (await laser.capabilities()).authz:
        pytest.skip("authorization receipts need Laser Streams")


async def _role_revisions(laser, name):
    history = await laser.authz_history({"role": name}, limit=1000)
    return len(history.events)


async def _eventually_role_revisions(laser, name, attempts=50):
    for _ in range(attempts):
        revisions = await _role_revisions(laser, name)
        if revisions > 0:
            return revisions
        await asyncio.sleep(0.1)
    raise AssertionError(f"role {name} never appeared in the history")


async def test_given_a_role_defined_under_an_identity_when_the_exact_request_is_repeated_then_should_return_the_original_success_without_a_second_revision(  # noqa: E501
    laser,
):
    await _requires_authz(laser)
    name = f"receipt-{uuid.uuid4().hex[:12]}"
    mutation_id = f"define-{uuid.uuid4().hex}"
    await laser.define_role(name, [_grant()], mutation_id=mutation_id)
    revisions = await _role_revisions(laser, name)

    await laser.define_role(name, [_grant()], mutation_id=mutation_id)
    assert await _role_revisions(laser, name) == revisions, "the repeat has no second effect"

    with pytest.raises(ls.AuthzError) as conflict:
        await laser.define_role(name, [_grant(), ls.Grant("kv", "read")], mutation_id=mutation_id)
    assert conflict.value.code == "Conflict"
    await laser.delete_role(name)


async def test_given_a_read_binding_when_rebound_with_its_revision_then_should_apply_once_and_refuse_the_stale_revision(  # noqa: E501
    laser,
):
    await _requires_authz(laser)
    read = await laser.get_bindings(ROOT)
    assert read.roles, "root is bound to its role"
    # The same role set, so root keeps every grant while its binding revision moves.
    bound = await laser.bind_roles(ROOT, read.roles, expect_revision=read.revision)
    assert bound.revision == read.revision + 1, "the bind answers its new revision"
    reread = await laser.get_bindings(ROOT)
    assert reread.roles == read.roles
    assert reread.revision == read.revision + 1
    with pytest.raises(ls.AuthzError) as stale:
        await laser.bind_roles(ROOT, read.roles, expect_revision=read.revision)
    assert stale.value.code == "Conflict"


async def test_given_a_grant_on_an_unenforced_feature_when_a_role_is_defined_then_should_raise_invalid_before_sending(  # noqa: E501
    laser,
):
    for feature in ("memory", "agent", "workflow"):
        with pytest.raises(ls.InvalidError):
            await laser.define_role("unenforced", [ls.Grant(feature, "read")])


async def test_given_an_invalid_mutation_id_when_used_then_should_raise_invalid_before_sending(  # noqa: E501
    laser,
):
    with pytest.raises(ls.InvalidError):
        await laser.define_role("receipt-invalid", [_grant()], mutation_id="not an id")
    with pytest.raises(ls.InvalidError):
        await laser.bind_roles(ROOT, [], mutation_id="")


async def test_given_receipt_capacity_filled_by_churn_when_a_new_identity_arrives_then_should_refuse_it_as_resource_limit_and_keep_the_retained_receipts():  # noqa: E501
    cluster = await asyncio.to_thread(lambda: IggyTestCluster().start())
    laser = None
    try:
        laser = await ls.Laser.connect(cluster.endpoint)
        await _requires_authz(laser)
        name = "receipt-churn"
        for index in range(RECEIPT_CAPACITY):
            await laser.define_role(name, [_grant()], mutation_id=f"churn-{index}")
        with pytest.raises(ls.AuthzError) as refused:
            await laser.define_role(name, [_grant()], mutation_id="churn-beyond-capacity")
        assert refused.value.code == "ResourceLimit"
        revisions = await _role_revisions(laser, name)
        await laser.define_role(name, [_grant()], mutation_id="churn-0")
        assert await _role_revisions(laser, name) == revisions, "a retained receipt still answers"
    finally:
        if laser is not None:
            await laser.close()
        await asyncio.to_thread(cluster.stop)


async def test_given_a_receipt_when_the_answering_node_restarts_then_should_answer_the_exact_repeat_from_the_recovered_receipt():  # noqa: E501
    cluster = await asyncio.to_thread(lambda: IggyTestCluster().start())
    laser = None
    try:
        leader, _ = await asyncio.to_thread(cluster.leader_and_follower)
        laser = await ls.Laser.connect(cluster.endpoint)
        await _requires_authz(laser)
        name = f"receipt-restart-{uuid.uuid4().hex[:8]}"
        mutation_id = f"restart-{uuid.uuid4().hex}"
        await laser.define_role(name, [_grant()], mutation_id=mutation_id)
        # The history read is served from the connected node's applied state,
        # which can trail the commit on a follower.
        revisions = await _eventually_role_revisions(laser, name)
        await laser.close()
        laser = None

        await asyncio.to_thread(cluster.restart_node, leader)

        laser = await ls.Laser.connect(cluster.endpoint)
        await laser.define_role(name, [_grant()], mutation_id=mutation_id)
        assert await _role_revisions(laser, name) == revisions, "the recovered receipt answers"
    finally:
        if laser is not None:
            await laser.close()
        await asyncio.to_thread(cluster.stop)
