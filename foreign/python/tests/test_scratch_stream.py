import re

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_given_a_scratch_stream_when_the_block_ends_then_should_exist_only_inside_it(laser):
    async with laser.scratch_stream("bench") as scratch:
        assert re.fullmatch(r"bench-[0-9a-f]{8}", scratch.name)
        assert scratch.laser.default_stream == scratch.name
        assert await laser.stream(scratch.name).exists()
        name = scratch.name
    assert not await laser.stream(name).exists()
    await scratch.delete()


async def test_given_a_block_that_raises_when_it_exits_then_should_still_delete_the_scratch_stream(
    laser,
):
    with pytest.raises(RuntimeError, match="boom"):
        async with laser.scratch_stream("bench") as scratch:
            name = scratch.name
            raise RuntimeError("boom")
    assert not await laser.stream(name).exists()


async def test_given_a_scratch_stream_not_entered_when_read_then_should_say_how_to_open_it(laser):
    scratch = laser.scratch_stream("bench")
    with pytest.raises(ls.InvalidError, match="async with"):
        _ = scratch.name
