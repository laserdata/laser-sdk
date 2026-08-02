"""log (Log primitive): every message, written once, readable forever.

A topic is an append-only record of every message in your system.
Services write to it and read from it like a group chat that never loses a
message. New readers start from the beginning or jump straight to now.

What it shows:
  - open a typed topic handle (`cls=` auto-encodes and decodes `Order`)
  - publish two records
  - replay them back through that same typed reader

Run it twice and the second run replays four orders: the log keeps every record,
and a fresh reader starts at offset 0. That is the primitive, not a bug.

Run it:
    just up
    python3 log.py

Docs: https://docs.laserdata.cloud/laser-sdk/log
Full scenario: native_streaming.py (a tuned producer/consumer over this same topic)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import _common

EXAMPLE = "log"
STREAM = "shop"
TOPIC = "orders"


@dataclass
class Order:
    id: int
    total: int


async def main() -> None:
    laser = await _common.connect(EXAMPLE)

    _common.phase("write two messages, then read them back")
    topic = laser.stream(STREAM).topic(TOPIC, cls=Order)
    await topic.ensure(2)

    for order in (Order(id=1, total=99), Order(id=2, total=42)):
        await topic.publish(order).send()

    # One typed handle pins the contract: `Order` in on publish, `Order` out on
    # replay. The reader starts at offset 0 and ends once it is caught up.
    reader = topic.records("log-example")
    while (record := await reader.next()) is not None:
        print(f"  order #{record.value.id} total {record.value.total}")


if __name__ == "__main__":
    asyncio.run(main())
