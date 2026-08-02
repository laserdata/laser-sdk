"""watch (Change feed primitive): stop re-querying blind.

Poll a lightweight advancement feed, then query only when the view has moved.
The feed rides the connection you already have.

What it shows:
  - declare the same view shape query.py reads under this run's own
    "orders_v1_<token>" name, notify-enabled so the deployment publishes to
    the change feed on every materialized batch
  - open a change-feed reader (`laser.watch(index=...)`, a flat call, not a
    builder)
  - publish an order and react to the change record instead of re-querying blind

Watch rides the managed change feed: it needs Laser Stack or LaserData Cloud
and skips on Apache Iggy without a managed backend.

Run it:
    LASER_CONNECTION_STRING=user:pwd@your-host python3 watch.py

Docs: https://docs.laserdata.cloud/laser-sdk/change-feed
Full scenario: event_analytics.py (a resumable cursor built the same await-then-query way)
"""

from __future__ import annotations

import asyncio
import time

import _common

EXAMPLE = "watch"
TOPIC = "orders"
INDEX = _common.index_for("orders_v1")
FIELDS = ["id", "total", "status"]
CHANGE_TIMEOUT = 10.0
POLL_INTERVAL = 0.2


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    caps = await laser.capabilities()
    if not _common.managed_gate(caps.query and caps.watch, "the change feed", EXAMPLE):
        return

    _common.phase("watch a view, then publish something that advances it")
    await laser.topic(TOPIC).ensure(_common.PARTITIONS)
    # The same view shape query.py declares, under this run's own name, so this
    # script runs on its own with no shared state.
    await _common.start_projector(laser, TOPIC, FIELDS, index=INDEX)

    feed = laser.watch(index=INDEX)

    await laser.topic(TOPIC).publish({"id": 4, "total": 20, "status": "paid"}).send()

    deadline = time.monotonic() + CHANGE_TIMEOUT
    while not (changes := await feed.poll()):
        if time.monotonic() >= deadline:
            raise TimeoutError(f"no change on '{INDEX}' arrived within {CHANGE_TIMEOUT:.0f}s")
        await asyncio.sleep(POLL_INTERVAL)

    for change in changes:
        span = f"{change.from_offset}..{change.to_offset}"
        print(f"  view advanced: {change.rows} row(s), source offsets {span}")


if __name__ == "__main__":
    asyncio.run(main())
