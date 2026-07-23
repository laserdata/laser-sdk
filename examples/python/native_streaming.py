"""Laser producer and live consumer paths over Apache Iggy.

Run it:
    python native_streaming.py
"""

from __future__ import annotations

import asyncio

import _common

EXAMPLE = "native-streaming"
TOPIC = "events"
MESSAGE_COUNT = 1000
BATCH = 100
PROGRESS_EVERY = 100


async def receive_all(consumer, *, manual_commit: bool) -> None:
    await consumer.init()
    if manual_commit:
        print(
            f"  committing after every message: one store_offset round-trip each, "
            f"{MESSAGE_COUNT} total - much slower than the batched auto-commit above "
            f"on any real network, by design"
        )
    seen = 0
    try:
        async for message in consumer:
            if manual_commit:
                await consumer.commit(message)
            seen += 1
            if seen % PROGRESS_EVERY == 0 or seen == MESSAGE_COUNT:
                print(
                    f"  received {seen}/{MESSAGE_COUNT}: partition={message.partition_id} "
                    f"offset={message.offset} headers={message.headers} payload={message.payload!r}"
                )
            if seen == MESSAGE_COUNT:
                break
    finally:
        await consumer.shutdown()


async def publish_messages(producer) -> None:
    """One keyed send with an exact-width header, then the rest of
    MESSAGE_COUNT in batches of BATCH, matching the producer's own
    batch_length."""
    await producer.send(
        b"message-0",
        headers={"type": ("uint16", 7)},
        key=b"account-42",
    )
    sent = 1
    while sent < MESSAGE_COUNT:
        # The lone keyed send above already used up one of this batch's slots,
        # so trim it by one here to keep every later boundary (and the progress
        # print) on a round multiple of BATCH instead of off by one forever.
        batch_size = min(BATCH - 1 if sent == 1 else BATCH, MESSAGE_COUNT - sent)
        values = [f"message-{sent + offset}".encode() for offset in range(batch_size)]
        await producer.send_batch(values)
        sent += batch_size
        print(f"  published {sent}/{MESSAGE_COUNT} messages")


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    topic = laser.topic(TOPIC)
    producer = topic.producer(
        batch_length=BATCH,
        linger_ms=5,
        retries=3,
        retry_interval_ms=1000,
        partitions=1,
    )
    await producer.init()

    _common.phase("producer: exact-width header, keyed routing, and batched messages")
    await publish_messages(producer)

    _common.phase("consumer: production automatic offset commits")
    await receive_all(
        topic.consumer_group(
            "auto-workers",
            batch_length=BATCH,
            poll_interval_ms=5,
            polling="first",
            auto_commit="each",
            commit_interval_ms=1000,
            allow_replay=True,
        ),
        manual_commit=False,
    )

    _common.phase("consumer: commit after successful handling")
    await receive_all(
        topic.consumer_group(
            "manual-workers",
            batch_length=BATCH,
            poll_interval_ms=5,
            polling="first",
            auto_commit="disabled",
            allow_replay=True,
        ),
        manual_commit=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
