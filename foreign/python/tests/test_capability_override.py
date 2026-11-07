import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_given_an_open_laser_when_overriding_capabilities_then_should_return_a_laser_at_once(
    open_laser,
):
    scoped = open_laser.with_capabilities(managed=True, kv=True, materializations=True)
    assert isinstance(scoped, ls.Laser)
    assert scoped.kv("profiles") is not None
    capabilities = await scoped.capabilities()
    assert capabilities.managed is True
    assert capabilities.kv.available is True
    assert capabilities.materializations is True
    assert (await open_laser.capabilities()).materializations is False
