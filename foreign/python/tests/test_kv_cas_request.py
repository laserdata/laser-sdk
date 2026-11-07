import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_given_a_precondition_when_building_then_should_yield_a_commit_only_request(
    open_laser,
):
    store = open_laser.kv("state")
    requests = [
        store.set("key").bytes(b"value").expect_version(3).ttl(5000),
        store.set("key").bytes(b"value").expect_absent(),
    ]
    for request in requests:
        assert isinstance(request, ls.KvCasRequest)
        assert not hasattr(request, "send")
        with pytest.raises(ls.UnsupportedError):
            await request.commit()
    assert not hasattr(store.set("key"), "commit")
