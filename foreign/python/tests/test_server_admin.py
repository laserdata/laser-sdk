import uuid

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


def _with_credentials(endpoint, credentials):
    return f"{credentials}@{endpoint.split('@', 1)[1]}"


async def test_given_a_new_user_when_created_updated_and_deleted_then_should_report_each_change(
    laser, iggy_endpoint
):
    name = f"user-{uuid.uuid4().hex[:12]}"
    user = laser.user(name)
    assert not await user.exists()

    created = await user.create("secret-1", permissions={"global": {"read_streams": True}})
    assert created.username == name
    assert created.status == "active"
    assert created.created_at_micros > 0

    assert any(info.username == name for info in await laser.users())
    read = await user.get()
    assert read.permissions["global"]["read_streams"] is True
    assert read.permissions["global"]["manage_users"] is False

    as_user = await ls.Laser.connect(_with_credentials(iggy_endpoint, f"{name}:secret-1"))
    try:
        assert (await as_user.me()).user_id == read.id
    finally:
        await as_user.close()

    await user.change_password("secret-1", "secret-2")
    await user.set_permissions(None)
    await user.update(status="inactive")
    read = await user.get()
    assert read.status == "inactive"
    assert read.permissions is None

    renamed = f"{name}-renamed"
    await user.update(username=renamed)
    assert not await user.exists()
    user = laser.user(renamed)
    assert await user.delete()
    assert not await user.delete()


async def test_given_an_unknown_user_status_when_creating_then_should_raise_invalid_error(
    laser,
):
    with pytest.raises(ls.InvalidError):
        await laser.user(f"user-{uuid.uuid4().hex[:12]}").create("secret", status="paused")


async def test_given_a_personal_access_token_when_created_then_should_sign_in_until_deleted(
    laser, iggy_endpoint
):
    name = f"token-{uuid.uuid4().hex[:12]}"
    token = laser.personal_access_token(name)
    value = await token.create(expiry_ms=3_600_000.0)

    listed = await laser.personal_access_tokens()
    info = next(info for info in listed if info.name == name)
    assert info.expires_at_micros is not None

    with_token = await ls.Laser.connect(_with_credentials(iggy_endpoint, value))
    try:
        await with_token.ping()
    finally:
        await with_token.close()

    assert await token.delete()
    assert not await token.delete()


async def test_given_a_connected_client_when_the_server_is_inspected_then_should_report_it(
    laser,
):
    await laser.ping()
    stats = await laser.stats()
    assert stats.server_version
    assert stats.streams_count >= 1

    me = await laser.me()
    assert me.user_id is not None
    assert any(client.client_id == me.client_id for client in await laser.clients())
    found = await laser.client_info(me.client_id)
    assert found.address == me.address
    assert await laser.client_info(2**32 - 1) is None


async def test_given_a_stream_and_topic_when_renamed_and_updated_then_should_report_them(
    laser,
):
    name = f"rename-{uuid.uuid4().hex[:12]}"
    stream = laser.stream(name)
    await stream.ensure()
    await stream.topic("metrics").ensure(1)

    renamed = f"{name}-new"
    await stream.rename(renamed)
    assert not await stream.exists()
    stream = laser.stream(renamed)
    assert await stream.exists()

    await stream.topic("metrics").update(
        name="hosts", expire_after_ms=3_600_000.0, max_topic_bytes=1 << 30
    )
    info = await stream.topic("hosts").get()
    assert info.max_size_bytes == 1 << 30

    await stream.topic("hosts").delete_segments(0, 1)
    assert await stream.delete()


async def test_given_both_expiry_settings_when_updating_a_topic_then_should_raise_invalid_error(
    laser,
):
    with pytest.raises(ls.InvalidError):
        await (
            laser.stream("absent")
            .topic("metrics")
            .update(expire_after_ms=1000.0, never_expire=True)
        )


async def test_given_a_server_when_the_cluster_and_a_snapshot_are_read_then_should_report_them(
    laser,
):
    cluster = await laser.cluster()
    assert cluster.nodes
    assert any(node.tcp_port > 0 for node in cluster.nodes)
    archive = await laser.snapshot()
    assert archive[:2] == b"PK"
