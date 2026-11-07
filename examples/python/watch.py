"""watch (Change feed primitive): stop re-querying blind.

Poll a lightweight advancement feed, then query only when the view has moved.
The feed rides the connection you already have.

What it shows:
  - declare the same view shape query.py reads under this run's own
    "readings_v1_<token>" name, notify-enabled so the deployment publishes to
    the change feed on every materialized batch
  - open a change-feed reader at the feed's end (`await laser.watch(index=...,
    from_end=True)`, a flat call, not a builder), so it sees only this run's change
  - publish a host reading and wait for the change instead of re-querying blind

Watch rides the managed change feed: it needs Laser Stack or LaserData Cloud
and skips on Apache Iggy without a managed backend.

Run it:
    LASER_CONNECTION_STRING=user:pwd@your-host python3 watch.py

Docs: https://docs.laserdata.cloud/laser-sdk/change-feed
Full scenario: event_analytics.py (a resumable cursor built the same await-then-query way)
"""

from __future__ import annotations

import asyncio

import _common

EXAMPLE = "watch"
TOPIC = "readings"
INDEX = _common.index_for("readings_v1")
FIELDS = ["host", "cpu", "status"]
CHANGE_TIMEOUT_MS = 10_000.0


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    try:
        caps = await laser.capabilities()
        if not _common.managed_gate(
            caps.query.available and caps.watch, "the change feed", EXAMPLE
        ):
            return

        _common.phase("watch a view, then publish something that advances it")
        await laser.topic(TOPIC).ensure(_common.PARTITIONS)
        # The same view shape query.py declares, under this run's own name, so this
        # script runs on its own with no shared state.
        await _common.start_projector(laser, TOPIC, FIELDS, index=INDEX)

        feed = await laser.watch(index=INDEX, from_end=True)

        await laser.topic(TOPIC).publish({"host": "node-4", "cpu": 88, "status": "degraded"}).send()

        changes = await feed.poll_within(CHANGE_TIMEOUT_MS)
        if not changes:
            raise TimeoutError(f"no change on '{INDEX}' arrived in time")
        for watched in changes:
            span = f"{watched.change.from_offset}..{watched.change.to_offset}"
            print(f"  view advanced: {watched.change.rows} row(s), source offsets {span}")
        if feed.missed:
            print(f"  the feed lost {feed.missed} change(s) to retention, re-read the view")
    finally:
        await laser.close()


if __name__ == "__main__":
    asyncio.run(main())
