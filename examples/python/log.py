"""log (Log primitive): every message, written once, readable forever.

A topic is an append-only record of every message in your system.
Services write to it and read from it like a group chat that never loses a
message. New readers start from the beginning or jump straight to now.

What it shows:
  - open a typed topic handle (`cls=` auto-encodes and decodes `Reading`)
  - publish two records
  - replay them back through that same typed reader

Run it twice and the second run replays four readings: the log keeps every record,
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
STREAM = "fleet"
TOPIC = "readings"


@dataclass
class Reading:
    host: str
    cpu: int


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    try:
        _common.phase("write two messages, then read them back")
        topic = laser.stream(STREAM).topic(TOPIC, cls=Reading)
        await topic.ensure(2)

        for reading in (Reading(host="node-1", cpu=42), Reading(host="node-2", cpu=91)):
            await topic.publish(reading).send()

        # One typed handle pins the contract: `Reading` in on publish, `Reading` out
        # on replay. The reader starts at offset 0 and ends once it is caught up.
        reader = topic.records("log-example")
        while (record := await reader.next()) is not None:
            print(f"  reading {record.value.host} cpu {record.value.cpu}")
    finally:
        await laser.close()


if __name__ == "__main__":
    asyncio.run(main())
