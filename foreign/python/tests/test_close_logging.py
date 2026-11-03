import asyncio
import logging

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration

SHUTDOWN = "Client has been shutdown"


async def test_given_a_cached_producer_when_closing_then_should_not_warn_about_its_own_shutdown(
    laser, caplog
):
    caplog.set_level(logging.DEBUG)
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    await laser.send_agent(ls.AgentTopic.Sessions, b"hello", ls.Provenance(agent="writer"))
    await laser.close()
    await asyncio.sleep(0.5)
    # The Rust bridge caches Python logger levels, so whether the demoted
    # debug record reaches Python depends on earlier logging setup. The
    # warning must never arrive.
    warnings = [
        record
        for record in caplog.records
        if SHUTDOWN in record.getMessage() and record.levelno >= logging.WARNING
    ]
    assert not warnings, [(record.name, record.levelname) for record in warnings]
