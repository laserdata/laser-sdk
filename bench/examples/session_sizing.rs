// Session sizing measurements against a running managed stack, for example
// one started with `scripts/run-managed-bdd.sh stack`. Each measurement makes
// its own fresh stream, prints one JSON line per result, and leaves the data in
// place so the store's table sizes can be read afterwards.
//
//   cargo run --release --example session_sizing -- <host:port> scan <roles> <records>
//   cargo run --release --example session_sizing -- <host:port> admission <readers> <records>
//   cargo run --release --example session_sizing -- <host:port> fold <partitions> <sessions> <records-per-session>
//   cargo run --release --example session_sizing -- <host:port> state <deltas> <value-bytes> <distinct|same>

use futures::StreamExt;
use laser_sdk::agent::{Session, SessionConfig, Sessions, TopicRetention};
use laser_sdk::filters::FilteredStart;
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{AgentEnvelope, AgentId, CorrelationId, RecordId, SessionStatus};
use laser_sdk::wire::dispatch::addressee_filter;
use serde_json::json;
use std::time::{Duration, Instant};

const RETENTION: Duration = Duration::from_hours(24);
const PUBLISHERS: usize = 64;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let address = args.first().expect("a host:port").clone();
    let number = |index: usize| -> usize {
        args.get(index)
            .unwrap_or_else(|| panic!("argument {index}"))
            .parse()
            .expect("a number")
    };
    match args.get(1).map(String::as_str) {
        Some("scan") => scan(&address, number(2), number(3)).await,
        Some("admission") => admission(&address, number(2), number(3)).await,
        Some("fold") => {
            let partitions = u32::try_from(number(2)).expect("a partition count");
            fold(&address, partitions, number(3), number(4)).await;
        }
        Some("state") => {
            let distinct = args.get(4).map(String::as_str) != Some("same");
            state(&address, number(2), number(3), distinct).await;
        }
        other => panic!("unknown measurement {other:?}"),
    }
}

fn count(value: usize) -> u64 {
    u64::try_from(value).expect("a count fits u64")
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

// A count as a float for the report. The counts here stay far below 2^52, so
// the conversion is exact.
#[allow(clippy::cast_precision_loss)]
fn float(value: u64) -> f64 {
    value as f64
}

fn ratio(numerator: u64, denominator: f64) -> f64 {
    float(numerator) / denominator
}

fn agent(name: &str) -> AgentId {
    name.parse().expect("a valid agent id")
}

async fn connect(address: &str, tag: &str) -> Laser {
    let stream = format!(
        "sizing-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_micros()
    );
    Laser::connect(&format!("iggy:iggy@{address}"))
        .await
        .expect("connect")
        .with_default_stream(stream)
}

async fn bootstrap(laser: &Laser, partitions: u32, register: bool) -> Sessions {
    let sessions = laser.sessions_with(SessionConfig::new().register_source(register));
    let outcome = sessions
        .bootstrap(partitions, TopicRetention::expire_after(RETENTION))
        .await
        .expect("bootstrap");
    assert_eq!(outcome.registered, register);
    sessions
}

// `records` commands on the shared lane, addressed round robin to `roles`
// roles across 64 sessions, each with a 256-byte body.
async fn publish_commands(laser: &Laser, roles: usize, records: usize) -> usize {
    let sessions: Vec<ConversationId> = (0..64).map(|_| ConversationId::new()).collect();
    let body = vec![b'x'; 256];
    futures::stream::iter(0..records)
        .map(|index| {
            let laser = laser.clone();
            let body = body.clone();
            let session = sessions[index % sessions.len()];
            async move {
                laser
                    .agdx(AgentTopic::Sessions, agent("planner"), session.into())
                    .command(CorrelationId::from_u128(index as u128 + 1), body)
                    .with_target(agent(&format!("role-{}", index % roles)))
                    .send()
                    .await
                    .expect("publish");
            }
        })
        .buffer_unordered(PUBLISHERS)
        .collect::<Vec<_>>()
        .await;
    records
}

struct RoleRead {
    matched: u64,
    examined: u64,
    pages: u64,
    bytes: u64,
    elapsed: Duration,
    page_micros: Vec<u64>,
}

// Read everything addressed to `role` through its own group bound to the
// addressee filter, from the first record until `expected` matched.
async fn read_role(laser: &Laser, role: &str, group: &str, expected: u64) -> RoleRead {
    let group = laser
        .topic(AgentTopic::Sessions.topic_string())
        .consumer_group(group);
    group
        .create()
        .filter(addressee_filter(&agent(role)))
        .build()
        .await
        .expect("group policy");
    let mut reader = group
        .reader()
        .expect("group source")
        .start(FilteredStart::First)
        .count(1_000)
        .build()
        .await
        .expect("reader");
    let started = Instant::now();
    let mut read = RoleRead {
        matched: 0,
        examined: 0,
        pages: 0,
        bytes: 0,
        elapsed: Duration::ZERO,
        page_micros: Vec::new(),
    };
    while read.matched < expected {
        let round = Instant::now();
        let (page, _more) = reader.read_round().await.expect("filtered round");
        read.examined += reader.examined_in_round();
        if let Some(page) = page {
            read.page_micros.push(micros(round.elapsed()));
            read.pages += 1;
            read.matched += count(page.records.len());
            read.bytes += page
                .records
                .iter()
                .map(|record| count(record.message.payload.len()))
                .sum::<u64>();
            reader.ack_page(&page).await.expect("ack");
        }
        assert!(
            started.elapsed() < Duration::from_secs(600),
            "role {role} read {} of {expected}",
            read.matched
        );
    }
    read.elapsed = started.elapsed();
    reader.close().await.expect("close");
    read
}

fn percentile(values: &mut [u64], percent: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[(values.len() - 1) * percent / 100]
}

async fn scan(address: &str, roles: usize, records: usize) {
    let laser = connect(address, "scan").await;
    bootstrap(&laser, 4, false).await;
    publish_commands(&laser, roles, records).await;
    let expected = count(records / roles);
    for role in 0..roles {
        let name = format!("role-{role}");
        let read = read_role(&laser, &name, &name, expected).await;
        println!(
            "{}",
            json!({
                "measurement": "scan",
                "roles": roles,
                "records": records,
                "role": name,
                "matched": read.matched,
                "examined": read.examined,
                "amplification": ratio(read.examined, float(read.matched)),
                "pages": read.pages,
                "delivered_bytes": read.bytes,
                "elapsed_ms": millis(read.elapsed),
            })
        );
    }
}

async fn admission(address: &str, readers: usize, records: usize) {
    const ROLES: usize = 8;
    let laser = connect(address, "admission").await;
    bootstrap(&laser, 4, false).await;
    publish_commands(&laser, ROLES, records).await;
    let expected = count(records / ROLES);
    let started = Instant::now();
    let reads = futures::future::join_all((0..readers).map(|reader| {
        let laser = laser.clone();
        async move {
            let role = format!("role-{}", reader % ROLES);
            read_role(&laser, &role, &format!("reader-{reader}"), expected).await
        }
    }))
    .await;
    let elapsed = started.elapsed();
    let examined: u64 = reads.iter().map(|read| read.examined).sum();
    let pages: u64 = reads.iter().map(|read| read.pages).sum();
    let mut latencies: Vec<u64> = reads
        .iter()
        .flat_map(|read| read.page_micros.iter().copied())
        .collect();
    let slowest = reads
        .iter()
        .map(|read| millis(read.elapsed))
        .max()
        .unwrap_or_default();
    println!(
        "{}",
        json!({
            "measurement": "admission",
            "readers": readers,
            "records": records,
            "pages": pages,
            "examined": examined,
            "wall_ms": millis(elapsed),
            "slowest_reader_ms": slowest,
            "examined_per_second": ratio(examined, elapsed.as_secs_f64()),
            "page_round_p50_us": percentile(&mut latencies, 50),
            "page_round_p99_us": percentile(&mut latencies, 99),
        })
    );
}

async fn completed(sessions: &Sessions) -> u64 {
    sessions
        .list()
        .status(SessionStatus::Completed)
        .total()
        .limit(1)
        .fetch()
        .await
        .ok()
        .and_then(|page| page.total)
        .unwrap_or_default()
}

// Each session: its start, `per_session - 2` notes, and its end, published
// before the stream is registered, so the measured time is the fold alone.
async fn fold(address: &str, partitions: u32, sessions: usize, per_session: usize) {
    let laser = connect(address, "fold").await;
    let unregistered = bootstrap(&laser, partitions, false).await;
    let published = Instant::now();
    futures::stream::iter(0..sessions)
        .map(|index| {
            let unregistered = unregistered.clone();
            async move {
                let (session, lease) = unregistered
                    .create(format!("session-{index}"))
                    .agent(agent("planner"))
                    .begin()
                    .await
                    .expect("begin");
                for note in 0..per_session.saturating_sub(2) {
                    append_note(&session, note).await;
                }
                session.end().await.expect("end");
                drop(lease);
            }
        })
        .buffer_unordered(PUBLISHERS)
        .collect::<Vec<_>>()
        .await;
    let publish_ms = millis(published.elapsed());
    let registered = Instant::now();
    let sessions_handle = bootstrap(&laser, partitions, true).await;
    let mut first_seen = None;
    loop {
        let done = completed(&sessions_handle).await;
        if done > 0 && first_seen.is_none() {
            first_seen = Some(registered.elapsed());
        }
        if done >= count(sessions) {
            break;
        }
        assert!(
            registered.elapsed() < Duration::from_mins(30),
            "folded {done} of {sessions}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let elapsed = registered.elapsed();
    let first = first_seen.unwrap_or(elapsed);
    let records = count(sessions * per_session);
    let changes = feed(&sessions_handle).await;
    println!(
        "{}",
        json!({
            "measurement": "fold",
            "partitions": partitions,
            "sessions": sessions,
            "records": records,
            "publish_ms": publish_ms,
            "registration_to_done_ms": millis(elapsed),
            "first_completed_ms": millis(first),
            "records_per_second": ratio(records, elapsed.as_secs_f64()),
            "records_per_second_after_first": ratio(records, elapsed.saturating_sub(first).as_secs_f64().max(0.001)),
            "change_rows": changes.rows,
            "change_seq_gaps": changes.gaps,
            "change_sessions_named": changes.named,
            "stream": laser.default_stream(),
        })
    );
}

async fn append_note(session: &Session, note: usize) {
    let envelope = AgentEnvelope::event(
        <RecordId as laser_sdk::types::MintUlid>::mint(),
        session.conversation().into(),
        agent("planner"),
        format!("note-{note}-{}", "y".repeat(200)).into_bytes(),
    )
    .with_operation("note");
    session.append(envelope).await.expect("note");
}

struct Feed {
    rows: u64,
    gaps: u64,
    named: u64,
}

// Every retained change row of the stream, checking that the sequences have
// no holes.
async fn feed(sessions: &Sessions) -> Feed {
    let mut after = 0;
    let mut feed = Feed {
        rows: 0,
        gaps: 0,
        named: 0,
    };
    let mut previous: Option<u64> = None;
    loop {
        let page = sessions.changes(after, 0).await.expect("changes");
        if page.rows.is_empty() {
            return feed;
        }
        for row in &page.rows {
            if let Some(previous) = previous
                && row.seq != previous + 1
            {
                feed.gaps += 1;
            }
            previous = Some(row.seq);
            after = after.max(row.seq);
            feed.rows += 1;
            feed.named += count(row.sessions.len());
        }
    }
}

async fn state(address: &str, deltas: usize, value_bytes: usize, distinct: bool) {
    let laser = connect(address, "state").await;
    let sessions = bootstrap(&laser, 4, true).await;
    let (session, _lease) = sessions
        .create("state")
        .agent(agent("planner"))
        .begin()
        .await
        .expect("begin");
    // Wait for the index, so the first write seeds from the indexed view.
    while sessions.get(session.conversation()).await.is_err() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let value = "v".repeat(value_bytes);
    let state = session.state();
    for delta in 0..deltas {
        let key = if distinct {
            format!("key-{delta}")
        } else {
            "key".to_owned()
        };
        state
            .set(&key, json!(format!("{delta}-{value}")))
            .await
            .expect("delta");
    }
    let started = Instant::now();
    let view = loop {
        let view = sessions
            .state(session.conversation(), u32::MAX)
            .await
            .expect("state view");
        if view.revision >= count(deltas) {
            break view;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let document = serde_json::to_vec(&view.document).expect("document").len();
    println!(
        "{}",
        json!({
            "measurement": "state",
            "deltas": deltas,
            "value_bytes": value_bytes,
            "distinct_keys": distinct,
            "revision": view.revision,
            "history_rows_returned": view.history.len(),
            "document_bytes": document,
            "fold_wait_ms": millis(started.elapsed()),
            "session": session.conversation().to_string(),
            "stream": laser.default_stream(),
        })
    );
}
