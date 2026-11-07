import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration

KV_GET_CODE = ls.AGDX_KV_GET_CODE
KV_CAS_FENCED_CODE = ls.AGDX_KV_CAS_FENCED_CODE


async def test_given_open_iggy_when_executing_a_batch_then_should_raise_unsupported(open_laser):
    with pytest.raises(ls.UnsupportedError):
        await open_laser.execute_batch([{"code": KV_GET_CODE, "payload": b"\xa0"}])


async def test_given_a_fenced_item_without_the_capability_when_batched_then_should_raise_unsupported_before_send(  # noqa: E501
    open_laser,
):
    laser = open_laser.with_capabilities(managed=True, kv=True, kv_fenced_leases=False)
    with pytest.raises(ls.UnsupportedError):
        await laser.execute_batch([{"code": KV_CAS_FENCED_CODE, "payload": b"\xa0"}])
