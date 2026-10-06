"""query (Views primitive): queries that already ran.

A projection watches your topics and keeps an always-current table you can
query - filter, aggregate, window, paginate, even search by meaning. Like a
materialized view, except you never refresh it.

What it shows:
  - declare this run's "readings_v1_<token>" view over the "readings" topic
  - publish a few host readings with a `status` field
  - query the maintained view with a key match and a limit

Query is a managed feature: it needs Laser Stack or LaserData Cloud and skips
on Apache Iggy without a managed backend.

Run it:
    LASER_CONNECTION_STRING=user:pwd@your-host python3 query.py

Docs: https://docs.laserdata.cloud/laser-sdk/views
Full scenario: fleet_tape.py (a queryable tape audited against the raw log)
"""

from __future__ import annotations

import asyncio

import _common

EXAMPLE = "query"
TOPIC = "readings"
INDEX = _common.index_for("readings_v1")
FIELDS = ["host", "cpu", "status"]
READINGS = [
    {"host": "node-1", "cpu": 42, "status": "ok"},
    {"host": "node-2", "cpu": 91, "status": "degraded"},
    {"host": "node-3", "cpu": 17, "status": "ok"},
]


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    try:
        if not _common.managed_gate(
            (await laser.capabilities()).query.available, "views (query)", EXAMPLE
        ):
            return

        _common.phase("keep a queryable view of a topic, then query it")
        await laser.topic(TOPIC).ensure(_common.PARTITIONS)
        # Declare this run's `readings_v1_<token>` view over `readings`. From here the
        # view maintains itself: every record published to the topic lands in the
        # table, and the per-run name means the counts below are this run's alone.
        await _common.start_projector(laser, TOPIC, FIELDS, index=INDEX)

        for reading in READINGS:
            await laser.topic(TOPIC).publish(reading).send()
        await _common.wait_for_projection(laser, INDEX, len(READINGS))

        # `where_eq` matches an indexed key, the cheap path a projection's key
        # columns answer directly. `filter_eq` and its siblings cover the rest.
        degraded = await laser.query(INDEX).where_eq("status", "degraded").limit(10).fetch()

        print(f"  {len(degraded.rows)} of {len(READINGS)} hosts are degraded")
        for row in degraded.rows:
            host = degraded.value_text(row, "host")
            cpu = degraded.value_text(row, "cpu")
            print(f"    host {host} cpu {cpu}")
    finally:
        await laser.close()


if __name__ == "__main__":
    asyncio.run(main())
