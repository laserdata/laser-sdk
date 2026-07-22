"""order-book (generic): a market-data tape with two readers on one connection.

A live feed streams fills (price in integer cents, quantity) and two read models
consume them:

  - the HOT path: fills stream to a feed topic and a cursor folds them into a
    live order book (last, VWAP, volume per symbol) straight off the log.
  - the ANALYTICS path: the same fills are indexed to a queryable tape the
    managed plane materializes, and VWAP / volume aggregates run once the feed
    drains. The query phase needs LaserData Cloud and skips on raw Apache Iggy.

Run it:
    python order_book.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import _common
import laser_sdk as ls

EXAMPLE = "order-book"
FEED_TOPIC = "md_feed"  # raw hot path
TAPE_TOPIC = "trades"  # queryable analytics tape
AVRO_TAPE_TOPIC = "trades_avro"  # schema-first tape (LaserData Cloud only)
AVRO_PROJECTION = f"{AVRO_TAPE_TOPIC}.v1"

# The schema-first tape replays the identical fills as raw Avro datums, decoded
# by a writer schema the managed plane allocated an id for.
FILL_AVRO_SCHEMA = """{
    "type":"record","name":"Fill",
    "fields":[
        {"name":"symbol","type":"string"},
        {"name":"price_cents","type":"long"},
        {"name":"qty","type":"int"},
        {"name":"side","type":"string"},
        {"name":"notional_cents","type":"long"},
        {"name":"message_type","type":"string"},
        {"name":"ts","type":"long"}
    ]
}"""
# Avro phase volume: bounded so the cloud-gated coda stays quick on a heavy run.
AVRO_FILLS_CAP = 500

# Indexed columns on the trade tape (the fields the managed plane materializes).
# message_type and ts are reserved fields backing message_type(..) / time_range(..).
SYMBOL = "symbol"
PRICE = "price_cents"
QTY = "qty"
SIDE = "side"
NOTIONAL = "notional_cents"
MESSAGE_TYPE = "message_type"
TS = "ts"
COLUMNS = [SYMBOL, PRICE, QTY, SIDE, NOTIONAL, MESSAGE_TYPE, TS]
SUM_RESULT = "sum"

# The opening book: a starting price (in cents) per symbol. The feed random-walks
# each from here.
OPENING = [("AAPL", 21350), ("MSFT", 42010), ("NVDA", 121540), ("AMZN", 18520), ("GOOG", 17890)]

# Paced bursts keep the live feed gentle, well under a free-tier deployment's
# throughput ceiling. The tape indexes in batches so the analytics write is a
# handful of sends rather than one request per fill.
BURST = 40
BURST_GAP = 0.12
TAPE_BATCH = 100
FILL_TIMEOUT = 15.0


class Market:
    """The matching engine's running view of the market: it draws the next fill
    by random-walking the last price of a randomly chosen symbol. Deterministic,
    so the live feed and the tape index replay the identical fills."""

    def __init__(self) -> None:
        self.prices = [list(pair) for pair in OPENING]
        self.rng = _common.Rng(0x123456789ABCDEF0)
        self.ts = 1_900_000_000_000_000

    def next_fill(self) -> dict:
        pick = self.rng.below(len(self.prices))
        symbol, price = self.prices[pick]
        step = self.rng.below(31) - 15
        price = max(1, price + step)
        self.prices[pick][1] = price
        qty = 1 + self.rng.below(500)
        side = "buy" if self.rng.next_u64() & 1 == 0 else "sell"
        self.ts += 1 + self.rng.below(50_000)
        return {
            SYMBOL: symbol,
            PRICE: price,
            QTY: qty,
            SIDE: side,
            NOTIONAL: price * qty,
            MESSAGE_TYPE: "fill",
            TS: self.ts,
        }


def generate_trades(count: int) -> list[dict]:
    market = Market()
    return [market.next_fill() for _ in range(count)]


class Book:
    """A live order book folded from the feed: last traded price, cumulative
    volume, and cumulative notional per symbol, updated fill by fill."""

    def __init__(self) -> None:
        self.levels: dict[str, dict] = {}

    def apply(self, fill: dict) -> None:
        level = self.levels.setdefault(fill[SYMBOL], {"last": 0, "volume": 0, "notional": 0})
        level["last"] = fill[PRICE]
        level["volume"] += fill[QTY]
        level["notional"] += fill[NOTIONAL]

    def snapshot(self, fills: int) -> None:
        print(f"book @ {fills} fills:")
        for symbol in sorted(self.levels):
            level = self.levels[symbol]
            vwap = level["notional"] // level["volume"] if level["volume"] else 0
            print(
                f"  {symbol:<6} last {level['last'] / 100:>10.2f}  "
                f"vwap {vwap / 100:>10.2f}  volume {level['volume']:>8}"
            )


async def stream_live_book(laser: ls.Laser, trades: list[dict]) -> Book:
    """Stream the raw hot feed in paced bursts in a background task while a
    cursor folds arriving fills into the live book. The two sides are
    deliberately not in lockstep, so a delayed delivery cannot deadlock the
    loop."""

    async def feed() -> None:
        for start in range(0, len(trades), BURST):
            batch = laser.topic(FEED_TOPIC).publish_batch()
            for fill in trades[start : start + BURST]:
                batch = batch.add_json(fill)
            await batch.send()
            await asyncio.sleep(BURST_GAP)

    publisher = asyncio.create_task(feed())
    cursor = laser.topic(FEED_TOPIC).replay()
    book = Book()
    seen = 0
    idle = 0.0
    while seen < len(trades):
        messages = await cursor.poll()
        if not messages:
            if publisher.done() and idle >= FILL_TIMEOUT:
                break
            idle += 0.05
            await asyncio.sleep(0.05)
            continue
        idle = 0.0
        for message in messages:
            book.apply(message.json())
            seen += 1
            if seen % BURST == 0:
                book.snapshot(seen)
    await publisher
    return book


async def index_tape(laser: ls.Laser, trades: list[dict]) -> None:
    """Index every fill to the queryable tape in batches: each batch is one send
    carrying its rows with inline JSON bodies, so the projection's pointers
    extract every column out of the body, typed. No index headers duplicate the
    payload."""
    indexed = 0
    for start in range(0, len(trades), TAPE_BATCH):
        chunk = trades[start : start + TAPE_BATCH]
        batch = laser.topic(TAPE_TOPIC).publish_batch().inline_payload()
        for fill in chunk:
            batch = batch.add_json(fill)
        await batch.send()
        indexed += len(chunk)
        print(f"indexed {indexed}/{len(trades)} fills to '{TAPE_TOPIC}'")


@dataclass
class Fill:
    """One executed trade, the typed shape of the tape's JSON bodies."""

    symbol: str
    price_cents: int
    qty: int
    side: str
    notional_cents: int
    message_type: str
    ts: int


async def audit_tape(laser: ls.Laser, trades: list[dict]) -> None:
    """The audit a trading stack runs against its own tape: replay the raw log
    through a typed handle (records decode into the Fill dataclass as they
    drain, a record that stopped decoding would raise with its exact log
    position) and the notionals recomputed off the log must equal the
    session's own."""
    tape = laser.topic(TAPE_TOPIC, cls=Fill)
    records = tape.records("tape-audit-py")
    notional_by_symbol: dict[str, int] = {}
    audited = 0
    while (record := await records.next()) is not None:
        fill: Fill = record.value
        notional_by_symbol[fill.symbol] = (
            notional_by_symbol.get(fill.symbol, 0) + fill.notional_cents
        )
        audited += 1
    expected: dict[str, int] = {}
    for fill_row in trades:
        expected[fill_row["symbol"]] = (
            expected.get(fill_row["symbol"], 0) + fill_row["notional_cents"]
        )
    if notional_by_symbol != expected:
        raise RuntimeError("the typed replay disagrees with the session's own notionals")
    print(f"audited {audited} fills off the log, every symbol's notional matches the session")


def group_totals(result: ls.QueryResult) -> dict[str, int]:
    """Collect a sum(..).group_by([SYMBOL]) result into symbol -> total. Each row
    carries the group key under headers[SYMBOL] and the sum under headers[sum]."""
    totals: dict[str, int] = {}
    for row in result.rows:
        symbol = row.headers.get(SYMBOL)
        total = row.headers.get(SUM_RESULT)
        if symbol is not None and total is not None:
            totals[symbol] = int(total)
    return totals


async def report_volume_and_vwap(laser: ls.Laser) -> None:
    """Query the materialized tape: per-symbol traded volume, and VWAP derived
    from two grouped sums (volume-weighted average price = notional / quantity)."""
    volume = await laser.query(TAPE_TOPIC).sum(QTY).group_by([SYMBOL]).fetch()
    notional = await laser.query(TAPE_TOPIC).sum(NOTIONAL).group_by([SYMBOL]).fetch()
    qty_by_symbol = group_totals(volume)
    notional_by_symbol = group_totals(notional)
    print(f"tape analytics over {sum(qty_by_symbol.values())} fills (Laser query layer):")
    for symbol in sorted(qty_by_symbol):
        qty = qty_by_symbol[symbol]
        vwap = notional_by_symbol.get(symbol, 0) // qty if qty else 0
        print(f"  {symbol:<6} volume {qty:>8}  VWAP {vwap / 100:>10.2f}")


async def avro_tape(laser: ls.Laser, trades: list[dict]) -> None:
    """The schema-first coda (LaserData Cloud only): the identical fills ride a
    second tape as raw Avro datums. The managed plane resolves the registered
    writer schema via `agdx.sid`, decodes the binary bodies, and extracts the
    indexed columns, and the notionals must come out the same as the JSON tape's.
    The schema is compiled once client-side so a body that stops matching fails
    before publishing, not as a managed-side warning the producer cannot see."""
    schema_source = {"kind": "avro", "schema": FILL_AVRO_SCHEMA}
    schema_id = await laser.register_schema(schema_source, name="orderbook_fill")
    print(f"the managed plane allocated writer-schema id {schema_id} for the Fill schema")

    await laser.topic(AVRO_TAPE_TOPIC).ensure(partitions=_common.PARTITIONS)
    await _common.start_projector(laser, AVRO_TAPE_TOPIC, COLUMNS, content_type="avro")

    compiled = ls.CompiledSchema.compile(schema_source, id=schema_id)
    fills = trades[:AVRO_FILLS_CAP]
    batch = laser.topic(AVRO_TAPE_TOPIC).publish_batch().projection_ref(AVRO_PROJECTION)
    for fill in fills:
        batch = batch.add_avro(compiled, schema_id, fill)
    await batch.send()
    print(f"published {len(fills)} fills as raw Avro datums")

    await _common.wait_for_projection(laser, AVRO_TAPE_TOPIC, len(fills))
    per_symbol = await laser.query(AVRO_TAPE_TOPIC).sum(NOTIONAL).group_by([SYMBOL]).fetch()
    print("notional per symbol, aggregated over columns decoded out of Avro bodies:")
    for symbol, total in sorted(group_totals(per_symbol).items()):
        print(f"  {symbol:<6} {total:>14}")


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    caps = await laser.capabilities()
    count = _common.messages(default=400)

    await laser.topic(FEED_TOPIC).ensure(partitions=_common.PARTITIONS)
    await laser.topic(TAPE_TOPIC).ensure(partitions=_common.PARTITIONS)

    # Draw the whole session up front so the feed and the tape replay identical fills.
    trades = generate_trades(count)

    # Register the analytics projector before the tape is written so no fill is
    # missed by a projector that starts afterwards (managed-only).
    if caps.query:
        await _common.start_projector(laser, TAPE_TOPIC, COLUMNS)

    print(f"streaming a live market feed of {count} fills across {len(OPENING)} symbols")
    book = await stream_live_book(laser, trades)
    book.snapshot(count)

    print("publishing the fills to the durable trade tape")
    await index_tape(laser, trades)

    if _common.managed_gate(caps.query, "query", EXAMPLE):
        await _common.wait_for_projection(laser, TAPE_TOPIC, count)
        await report_volume_and_vwap(laser)

    print("typed tape audit: replaying the log as Fill values")
    await audit_tape(laser, trades)

    # The schema-first coda needs writer schemas, which live on LaserData Cloud.
    if caps.managed:
        print("schema-first tape: Avro fills decoded by a registered writer schema")
        await avro_tape(laser, trades)
    elif caps.query:
        print("writer schemas live on LaserData Cloud, skipping the Avro tape (needs the Cloud)")


if __name__ == "__main__":
    asyncio.run(main())
