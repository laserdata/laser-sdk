use bytes::Bytes;
use futures::StreamExt;
use iggy::prelude::*;
use laser_examples::{PARTITIONS, init_tracing, laser, phase, start_projector, stream_for};
use laser_sdk::prelude::*;
use laser_sdk::query::{ContentType, Record};
use laser_sdk::schema_codecs::CompiledSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use strum::Display;
use tracing::info;

// A market-data tape with two readers on one connection, the shape a trading stack
// runs. A live feed streams fills in real time and two read models consume them:
//   - the HOT path: a tuned `IggyProducer` writes ticks, a consumer-group
//     `IggyConsumer` folds them into a live order book. Straight off the log.
//   - the ANALYTICS path: the same fills are indexed to a queryable tape LaserData Cloud
//     materializes, and we run VWAP / volume aggregates once the feed drains.

const FEED_TOPIC: &str = "md_feed"; // raw hot path
const TAPE_TOPIC: &str = "trades"; // queryable analytics tape
const FEED_GROUP: &str = "order-book-builder";

// The schema-first tape (LaserData Cloud only): the same fills replay as raw
// Avro datums, decoded by a writer schema LaserData Cloud allocated an id for.
const AVRO_TAPE_TOPIC: &str = "trades_avro";
const AVRO_PROJECTION: &str = "trades_avro.v1";
const FILL_AVRO_SCHEMA: &str = r#"{
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
}"#;
// Avro phase volume: enough to aggregate over, bounded so the cloud-gated
// coda stays quick even on a heavy soak.
const AVRO_FILLS_CAP: usize = 500;

// Indexed columns on the trade tape (the fields LaserData Cloud materializes).
const SYMBOL: &str = "symbol";
const PRICE: &str = "price_cents";
const QTY: &str = "qty";
const SIDE: &str = "side";
const NOTIONAL: &str = "notional_cents";
// Reserved convention fields: every fill is a `fill` message stamped with an
// execution timestamp, so the reserved columns fill and the `message_type` /
// `time_range` query sugar works on the tape.
const MESSAGE_TYPE: &str = "message_type";
const TS: &str = "ts";
const COLUMNS: &[&str] = &[SYMBOL, PRICE, QTY, SIDE, NOTIONAL, MESSAGE_TYPE, TS];
// The grouped-sum result column the query layer returns.
const SUM_RESULT: &str = "sum";

// `FILLS` fills stream to the live book in paced bursts (one snapshot per burst),
// then index to the tape in batches of `TAPE_BATCH` so the whole analytics write
// is a handful of `send_messages` calls instead of one request per fill. A burst
// gap keeps the live feed gentle, well under a free-tier deployment's ~100KB/s
// ceiling. Raise `FILLS` and shrink `BURST_GAP` against a local server.
const FILLS: usize = 2000;
const BURST: usize = 40;
const BURST_GAP: Duration = Duration::from_millis(120);
const TAPE_BATCH: usize = 100;

const PROJECTOR_TIMEOUT: Duration = Duration::from_secs(60);
const PROJECTION_POLL: Duration = Duration::from_millis(150);

// The opening book: a starting price (in cents) per symbol. The feed random-walks
// each from here.
const OPENING: &[(&str, i64)] = &[
    ("AAPL", 21350),
    ("MSFT", 42010),
    ("NVDA", 121540),
    ("AMZN", 18520),
    ("GOOG", 17890),
];

// Which side of the book a fill hit. An enum with `strum::Display` + serde
// rename (not a bare string), so the indexed value and the JSON body cannot drift.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
enum Side {
    Buy,
    Sell,
}

// One executed trade. Prices and notionals are integer cents so ordering and
// aggregation stay exact (never trust a float as an index key).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Trade {
    symbol: String,
    price_cents: i64,
    qty: u32,
    side: Side,
    notional_cents: i64,
    message_type: String,
    ts: u64,
}

impl Trade {
    fn new(symbol: &str, price_cents: i64, qty: u32, side: Side, ts: u64) -> Self {
        Self {
            symbol: symbol.to_owned(),
            price_cents,
            qty,
            side,
            notional_cents: price_cents * i64::from(qty),
            message_type: "fill".to_owned(),
            ts,
        }
    }
}

// A tiny deterministic PRNG (xorshift64*), so the feed looks like a real random
// walk yet replays identically on every run without pulling in a rng crate.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

// The matching engine's running view of the market: it draws the next fill by
// random-walking the last price of a randomly chosen symbol.
struct Market {
    prices: Vec<(&'static str, i64)>,
    rng: Rng,
    // Execution clock in epoch micros, stepped per fill from a fixed base so
    // the session replays identically.
    ts: u64,
}

impl Market {
    fn open() -> Self {
        Self {
            prices: OPENING.to_vec(),
            rng: Rng(0x1234_5678_9abc_def0),
            ts: 1_900_000_000_000_000,
        }
    }

    // The next executed fill: pick a symbol, step its price by up to +/-15 cents,
    // size it, and tag a side.
    fn next_fill(&mut self) -> Trade {
        let pick = self.below_len();
        let (symbol, price) = self.prices[pick];
        let step = self.rng.below(31) as i64 - 15;
        let price = (price + step).max(1);
        self.prices[pick].1 = price;
        let qty = 1 + self.rng.below(500) as u32;
        let side = if self.rng.next_u64() & 1 == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        self.ts += 1 + self.rng.below(50_000);
        Trade::new(symbol, price, qty, side, self.ts)
    }

    fn below_len(&mut self) -> usize {
        self.rng.below(self.prices.len() as u64) as usize
    }
}

// A live order book folded from the feed: last traded price, cumulative volume,
// and cumulative notional per symbol, updated fill by fill so a rolling VWAP can
// be shown as the market moves.
#[derive(Default)]
struct Book {
    by_symbol: BTreeMap<String, Level>,
}

#[derive(Default)]
struct Level {
    last_cents: i64,
    volume: u64,
    notional_cents: i128,
}

impl Book {
    fn apply(&mut self, trade: &Trade) {
        let level = self.by_symbol.entry(trade.symbol.clone()).or_default();
        level.last_cents = trade.price_cents;
        level.volume += u64::from(trade.qty);
        level.notional_cents += i128::from(trade.notional_cents);
    }

    fn snapshot(&self, fills: usize) {
        info!("book @ {fills} fills:");
        for (symbol, level) in &self.by_symbol {
            let vwap = if level.volume > 0 {
                (level.notional_cents / i128::from(level.volume)) as i64
            } else {
                0
            };
            info!(
                "  {symbol:<6} last {:>10.2}  vwap {:>10.2}  volume {:>8}",
                cents(level.last_cents),
                cents(vwap),
                level.volume
            );
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    phase("warming up");

    // `managed_query` so the analytics half works: the example spawns its own
    // in-process projector locally, and on LaserData Cloud the connect-time
    // `AGDX_HELLO` probe upgrades the read path to the `AGDX_QUERY` managed command.
    let capabilities = Capabilities::OPEN.with_query(true);
    let data_stream = stream_for("order-book");
    let laser = laser(&data_stream, capabilities).await?;
    laser.ensure_topic(FEED_TOPIC, PARTITIONS).await?;
    laser.ensure_topic(TAPE_TOPIC, PARTITIONS).await?;

    // Start the projector before the feed opens so no fill is missed, then warm
    // the hot-path producer and consumer up front so the live phase below times
    // the market, not the one-off connection and consumer-group handshakes.
    let projector = start_projector(&laser, TAPE_TOPIC, ContentType::Json, COLUMNS).await?;
    let producer = build_feed_producer(&laser, &data_stream).await?;
    let mut consumer = build_book_consumer(&laser, &data_stream).await?;

    // Draw the whole session up front so the live feed and the tape index replay
    // the identical fills.
    let trades = generate_trades();

    phase("streaming a live market feed");
    info!("streaming {FILLS} fills across {} symbols", OPENING.len());
    let book = stream_live_book(producer, &mut consumer, &trades).await?;
    book.snapshot(FILLS);

    phase("indexing the fills to the queryable tape");
    index_tape(&laser, &trades).await?;

    phase("trade-tape analytics");
    wait_for_projection(&laser, FILLS).await?;
    report_volume_and_vwap(&laser).await?;

    // The schema-first coda (LaserData Cloud only): the identical fills ride a
    // second tape as raw Avro datums. No `agdx.idx.*` headers this time, the
    // LaserData Cloud resolves the registered writer schema via `agdx.sid` and extracts
    // the indexed columns out of the binary bodies, and the notionals must
    // come out the same as the JSON tape's.
    if laser.capabilities().await.managed {
        phase("schema-first tape: Avro fills decoded by a registered writer schema");
        avro_tape(&laser, &trades).await?;
    } else {
        info!(
            "writer schemas live on LaserData Cloud, skipping the Avro tape (needs LaserData Cloud)"
        );
    }

    projector.shutdown().await;
    Ok(())
}

// Register the Fill writer schema (synchronous: LaserData Cloud validates the
// definition, allocates a collision-free id, and returns it), project the
// Avro topic by body pointers, publish a slice of the session as raw datums
// via the `schema-codecs` client-side encoder, and aggregate the decoded
// columns.
async fn avro_tape(laser: &Laser, trades: &[Trade]) -> Result<(), LaserError> {
    let schema_id = laser
        .schemas()
        .register(SchemaSource::Avro {
            schema: FILL_AVRO_SCHEMA.to_owned(),
        })
        .name("orderbook_fill")
        .send()
        .await?;
    info!("LaserData Cloud allocated writer-schema id {schema_id} for the Fill schema");

    laser.ensure_topic(AVRO_TAPE_TOPIC, PARTITIONS).await?;
    laser
        .projections()
        .register(
            Projection::builder(AVRO_PROJECTION)
                .name("trades_avro")
                .version(1)
                .content_type(ContentType::Avro)
                .fields(COLUMNS.iter().copied())
                .build(),
        )
        .await?;
    laser
        .bindings()
        .apply(
            ProjectionBinding::builder()
                .source(stream_for("order-book"), AVRO_TAPE_TOPIC)
                .allow(AVRO_PROJECTION)
                .default_projection(AVRO_PROJECTION)
                .target_table(AVRO_TAPE_TOPIC)
                .build(),
        )
        .await?;
    wait_for_schema(laser, schema_id).await?;

    // Compile once client-side: `.add_avro` then fails BEFORE publishing if a
    // body stops matching the registered schema, instead of a managed-side warn
    // the producer cannot see.
    let compiled = CompiledSchema::compile(&SchemaDef {
        id: schema_id,
        source: SchemaSource::Avro {
            schema: FILL_AVRO_SCHEMA.to_owned(),
        },
        name: None,
        version: None,
    })?;
    let slice = &trades[..trades.len().min(AVRO_FILLS_CAP)];
    let mut request = laser
        .publish_batch(AVRO_TAPE_TOPIC)
        .projection_ref(AVRO_PROJECTION);
    for trade in slice {
        request = request.add_avro(&compiled, schema_id, trade)?;
    }
    request.send().await?;
    info!("published {} fills as raw Avro datums", slice.len());

    wait_for_table(laser, AVRO_TAPE_TOPIC, slice.len()).await?;
    let per_symbol = laser
        .query(AVRO_TAPE_TOPIC)
        .sum(NOTIONAL)
        .group_by([SYMBOL])
        .fetch()
        .await?;
    info!("notional per symbol, aggregated over columns decoded out of Avro bodies:");
    for row in &per_symbol.rows {
        info!(
            "  {:<6} {:>14}",
            row.headers.get(SYMBOL).map(String::as_str).unwrap_or("?"),
            row.headers
                .get(SUM_RESULT)
                .map(String::as_str)
                .unwrap_or("0"),
        );
    }
    Ok(())
}

// The register reply carries a durable id, but the apply is asynchronous:
// read back until browse resolves it before the first publish against it.
async fn wait_for_schema(laser: &Laser, id: u32) -> Result<(), LaserError> {
    let deadline = Instant::now() + PROJECTOR_TIMEOUT;
    while Instant::now() < deadline {
        if matches!(laser.schemas().get(id).await, Ok(Some(_))) {
            return Ok(());
        }
        tokio::time::sleep(PROJECTION_POLL).await;
    }
    Err(LaserError::Invalid(format!(
        "schema `{id}` never appeared in the registry"
    )))
}

// Poll until `expected` rows are materialized in `table`, tolerant of a
// not-yet-created table while LaserData Cloud applies the projection.
async fn wait_for_table(laser: &Laser, table: &str, expected: usize) -> Result<(), LaserError> {
    let deadline = Instant::now() + PROJECTOR_TIMEOUT;
    loop {
        let total = laser
            .query(table)
            .fetch()
            .await
            .map(|result| result.page.total)
            .unwrap_or(0);
        if total >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(format!(
                "projector indexed only {total}/{expected} rows in `{table}` before the deadline"
            )));
        }
        tokio::time::sleep(PROJECTION_POLL).await;
    }
}

// Tuned hot-path producer: balanced partitioning spreads the feed across
// partitions, bounded retries ride out a transient blip without dropping a fill.
async fn build_feed_producer(laser: &Laser, data_stream: &str) -> Result<IggyProducer, LaserError> {
    let producer = laser
        .iggy_producer(data_stream, FEED_TOPIC)?
        .partitioning(Partitioning::balanced())
        .send_retries(Some(3), None)
        .build();
    producer.init().await?;
    Ok(producer)
}

// Low-latency hot-path consumer. Offsets commit SERVER-SIDE on each poll
// (`AutoCommit::When(PollingMessages)`): the stored offset then moves in
// lockstep with delivery, the one commit mode that cannot starve the reader
// on a re-polled batch. A 1ms poll interval keeps tick-to-book latency tight
// without hammering the connection.
async fn build_book_consumer(laser: &Laser, data_stream: &str) -> Result<IggyConsumer, LaserError> {
    let mut consumer = laser
        .iggy_consumer_group(FEED_GROUP, data_stream, FEED_TOPIC)?
        .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::next())
        .poll_interval(
            IggyDuration::from_str("1ms")
                .map_err(|error| LaserError::Invalid(error.to_string()))?,
        )
        .batch_length(256)
        .build();
    consumer.init().await?;
    Ok(consumer)
}

// Draw the session deterministically so both read models replay identical fills.
fn generate_trades() -> Vec<Trade> {
    let mut market = Market::open();
    (0..FILLS).map(|_| market.next_fill()).collect()
}

// How long the book reader waits for the next fill before giving up with a
// diagnostic instead of a silent hang.
const FILL_TIMEOUT: Duration = Duration::from_secs(15);

// Stream the raw hot feed and fold arriving fills into the live book,
// snapshotting as the market moves. The producer paces bursts in its own
// task while the reader consumes whatever has arrived: the two sides are
// deliberately NOT in lockstep, so one duplicated or delayed delivery can
// never deadlock the loop.
async fn stream_live_book(
    producer: IggyProducer,
    consumer: &mut IggyConsumer,
    trades: &[Trade],
) -> Result<Book, LaserError> {
    let bursts: Vec<Vec<IggyMessage>> = trades
        .chunks(BURST)
        .map(|burst| {
            burst
                .iter()
                .map(|trade| {
                    Ok(IggyMessage::builder()
                        .payload(Bytes::from(
                            serde_json::to_vec(trade)
                                .map_err(|error| LaserError::Codec(error.to_string()))?,
                        ))
                        .build()?)
                })
                .collect::<Result<Vec<_>, LaserError>>()
        })
        .collect::<Result<Vec<_>, LaserError>>()?;
    let feed = tokio::spawn(async move {
        for raw in bursts {
            producer.send(raw).await?;
            tokio::time::sleep(BURST_GAP).await;
        }
        Ok::<(), LaserError>(())
    });

    let mut book = Book::default();
    let mut seen = 0usize;
    while seen < trades.len() {
        let received = match tokio::time::timeout(FILL_TIMEOUT, consumer.next()).await {
            Ok(Some(received)) => received?,
            Ok(None) => {
                return Err(LaserError::Invalid(format!(
                    "feed ended after {seen}/{} fills",
                    trades.len()
                )));
            }
            Err(_) => {
                return Err(LaserError::Invalid(format!(
                    "no fill arrived for {}s after {seen}/{} fills. Either the feed task failed \
                     (its error surfaces right after this one) or the `{FEED_GROUP}` consumer \
                     group is not receiving deliveries from this server",
                    FILL_TIMEOUT.as_secs(),
                    trades.len(),
                )));
            }
        };
        let trade: Trade = serde_json::from_slice(&received.message.payload)
            .map_err(|error| LaserError::Codec(error.to_string()))?;
        book.apply(&trade);
        seen += 1;
        if seen.is_multiple_of(BURST) {
            book.snapshot(seen);
        }
    }
    feed.await
        .map_err(|error| LaserError::Invalid(format!("feed task: {error}")))??;
    Ok(book)
}

// Index every fill to the queryable tape in batches of `TAPE_BATCH`: each batch is
// one `send_messages` call carrying its rows with their own indexed columns and
// inline bodies, so the whole analytics write is a handful of round trips rather
// than one per fill. That is the difference between a smooth run and hundreds of
// requests against a rate-limited deployment.
async fn index_tape(laser: &Laser, trades: &[Trade]) -> Result<(), LaserError> {
    let mut indexed = 0;
    for chunk in trades.chunks(TAPE_BATCH) {
        let mut batch = laser.publish_batch(TAPE_TOPIC);
        for trade in chunk {
            // Body-first indexing: the projection's pointers extract every
            // column out of the JSON fill, typed (integer cents stay
            // integers). No `agdx.idx.*` duplication of the payload.
            let record = Record::builder()
                .content_type(ContentType::Json)
                .inline_payload(true)
                .build();
            batch = batch.add_record(
                serde_json::to_vec(trade).map_err(|error| LaserError::Codec(error.to_string()))?,
                record,
            );
        }
        batch.send().await?;
        indexed += chunk.len();
        info!("indexed {indexed}/{} fills to `{TAPE_TOPIC}`", trades.len());
    }
    Ok(())
}

// Poll until the projector has indexed every fill, tolerant of a not-yet-created
// index while a remote LaserData Cloud applies the projection.
async fn wait_for_projection(laser: &Laser, expected: usize) -> Result<(), LaserError> {
    let deadline = Instant::now() + PROJECTOR_TIMEOUT;
    let mut last = usize::MAX;
    loop {
        let total = laser
            .query(TAPE_TOPIC)
            .fetch()
            .await
            .map(|result| result.page.total)
            .unwrap_or(0);
        if total != last {
            info!("projector materialized {total}/{expected} fills");
            last = total;
        }
        if total >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(format!(
                "projector indexed only {total}/{expected} fills before the deadline"
            )));
        }
        tokio::time::sleep(PROJECTION_POLL).await;
    }
}

// Query the materialized tape: per-symbol traded volume, and VWAP derived from
// two grouped sums (volume-weighted average price = notional / quantity).
async fn report_volume_and_vwap(laser: &Laser) -> Result<(), LaserError> {
    let start = Instant::now();
    let volume = laser
        .query(TAPE_TOPIC)
        .sum(QTY)
        .group_by([SYMBOL])
        .fetch()
        .await?;
    let notional = laser
        .query(TAPE_TOPIC)
        .sum(NOTIONAL)
        .group_by([SYMBOL])
        .fetch()
        .await?;

    let qty_by_symbol = group_totals(&volume);
    let notional_by_symbol = group_totals(&notional);

    info!(
        "tape analytics over {} fills (Laser query layer), {}ms:",
        qty_by_symbol.values().sum::<i64>(),
        start.elapsed().as_millis()
    );
    for (symbol, qty) in &qty_by_symbol {
        let notional = notional_by_symbol.get(symbol).copied().unwrap_or(0);
        let vwap_cents = if *qty > 0 { notional / *qty } else { 0 };
        info!(
            "  {symbol:<6} volume {qty:>8}  VWAP {:>10.2}",
            cents(vwap_cents)
        );
    }
    Ok(())
}

// Collect a `sum(..).group_by([SYMBOL])` result into `symbol -> total`. Each row
// carries the group key in `headers[SYMBOL]` and the sum in `headers[SUM_RESULT]`.
fn group_totals(result: &QueryResult) -> BTreeMap<String, i64> {
    result
        .rows
        .iter()
        .filter_map(|row| {
            let symbol = row.headers.get(SYMBOL)?.clone();
            let total = row.headers.get(SUM_RESULT)?.parse().ok()?;
            Some((symbol, total))
        })
        .collect()
}

// Cents to a whole-currency float, for display only.
fn cents(value: i64) -> f64 {
    value as f64 / 100.0
}
