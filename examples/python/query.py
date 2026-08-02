"""query (Views primitive): queries that already ran.

A projection watches your topics and keeps an always-current table you can
query - filter, aggregate, window, paginate, even search by meaning. Like a
materialized view, except you never refresh it.

What it shows:
  - declare this run's "orders_v1_<token>" view over the "orders" topic
  - publish a few orders with a `status` field
  - query the maintained view with a key match and a limit

Query is a managed feature: it needs Laser Stack or LaserData Cloud and skips
on Apache Iggy without a managed backend.

Run it:
    LASER_CONNECTION_STRING=user:pwd@your-host python3 query.py

Docs: https://docs.laserdata.cloud/laser-sdk/views
Full scenario: order_book.py (a queryable tape audited against the raw log)
"""

from __future__ import annotations

import asyncio

import _common

EXAMPLE = "query"
TOPIC = "orders"
INDEX = _common.index_for("orders_v1")
FIELDS = ["id", "total", "status"]
ORDERS = [
    {"id": 1, "total": 99, "status": "paid"},
    {"id": 2, "total": 42, "status": "pending"},
    {"id": 3, "total": 15, "status": "paid"},
]


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    if not _common.managed_gate((await laser.capabilities()).query, "views (query)", EXAMPLE):
        return

    _common.phase("keep a queryable view of a topic, then query it")
    await laser.topic(TOPIC).ensure(_common.PARTITIONS)
    # Declare this run's `orders_v1_<token>` view over `orders`. From here the
    # view maintains itself: every record published to the topic lands in the
    # table, and the per-run name means the counts below are this run's alone.
    await _common.start_projector(laser, TOPIC, FIELDS, index=INDEX)

    for order in ORDERS:
        await laser.topic(TOPIC).publish(order).send()
    await _common.wait_for_projection(laser, INDEX, len(ORDERS))

    # `where_eq` matches an indexed key, the cheap path a projection's key
    # columns answer directly. `filter_eq` and its siblings cover the rest.
    paid = await laser.query(INDEX).where_eq("status", "paid").limit(10).fetch()

    print(f"  {len(paid.rows)} of {len(ORDERS)} orders are paid")
    for row in paid.rows:
        print(f"    order #{row.headers.get('id')} total {row.headers.get('total')}")


if __name__ == "__main__":
    asyncio.run(main())
