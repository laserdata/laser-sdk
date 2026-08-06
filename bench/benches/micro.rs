use bytes::Bytes;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use futures::executor::block_on;
use laser_sdk::agent::{ChunkAssembler, Deduplicator, SlidingWindow};
use laser_sdk::iggy::prelude::{HeaderKey, HeaderValue, IggyMessage};
use laser_sdk::provenance::{LlmUsage, Provenance};
use laser_sdk::stream::ProducerMessage;
use laser_sdk::types::{ConversationId, MessageId};
use laser_wire::agent::{AgentEnvelope, ChannelId};
use laser_wire::batch::{BatchReply, BatchRequest};
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::kv::KvSet;
use laser_wire::query::QueryEnvelope;
use laser_wire::result::ResultCode;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::hint::black_box;

fn wire_framing(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("wire_framing");
    for fixture in [
        "agent_command.bin",
        "agent_response.bin",
        "agent_event.bin",
        "agent_status_task.bin",
        "agent_error.bin",
        "agent_chunk.bin",
    ] {
        benchmark_frame::<AgentEnvelope>(&mut group, fixture);
    }
    benchmark_frame::<KvSet>(&mut group, "kv_set.bin");
    benchmark_frame::<QueryEnvelope>(&mut group, "query_envelope.bin");
    benchmark_frame::<BatchRequest>(&mut group, "batch_request.bin");
    benchmark_frame::<BatchReply>(&mut group, "batch_reply.bin");
    group.finish();
}

fn benchmark_frame<T>(group: &mut BenchmarkGroup<'_, WallTime>, fixture: &'static str)
where
    T: DeserializeOwned + Serialize,
{
    let bytes = laser_wire::fixtures::bytes(fixture).expect("wire fixture should exist");
    let value: T = decode_named(bytes).expect("wire fixture should decode");
    group.throughput(Throughput::Bytes(
        u64::try_from(bytes.len()).expect("fixture length should fit u64"),
    ));
    group.bench_with_input(
        BenchmarkId::new("decode", fixture),
        bytes,
        |bencher, bytes| {
            bencher.iter(|| decode_named::<T>(black_box(bytes)).expect("fixture should decode"));
        },
    );
    group.bench_with_input(
        BenchmarkId::new("encode", fixture),
        &value,
        |bencher, value| {
            bencher.iter(|| encode_named(black_box(value)).expect("fixture should encode"));
        },
    );
}

fn provenance_headers(criterion: &mut Criterion) {
    let minimal = Provenance::builder()
        .conversation_id(ConversationId::derive("benchmark-minimal"))
        .build();
    let typical = Provenance::builder()
        .conversation_id(ConversationId::derive("benchmark-typical"))
        .idempotency_key("ticket-42".to_owned())
        .correlation_id("request-42".to_owned())
        .fence_token(7)
        .build();
    let maximal = Provenance::builder()
        .conversation_id(ConversationId::derive("benchmark-maximal"))
        .causal_parent(MessageId::new(3, 42))
        .parent_conversation_id(ConversationId::derive("benchmark-parent"))
        .root_conversation_id(ConversationId::derive("benchmark-root"))
        .agent(
            "planner"
                .parse()
                .expect("planner should be a valid agent id"),
        )
        .target_agent_id(
            "executor"
                .parse()
                .expect("executor should be a valid agent id"),
        )
        .usage(
            LlmUsage::builder()
                .input_tokens(12_000)
                .output_tokens(4_000)
                .cost_usd(0.42)
                .build(),
        )
        .deadline(laser_sdk::iggy::prelude::IggyTimestamp::from(
            1_717_171_777_000_000_u64,
        ))
        .idempotency_key("order-123-attempt-2".to_owned())
        .correlation_id("request-42".to_owned())
        .fence_token(u64::MAX)
        .build();
    let mut group = criterion.benchmark_group("provenance_headers");
    for (name, provenance) in [
        ("minimal", minimal),
        ("typical", typical),
        ("maximal", maximal),
    ] {
        let headers = BTreeMap::<HeaderKey, HeaderValue>::try_from(&provenance)
            .expect("valid provenance should encode");
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"benchmark"))
            .user_headers(headers)
            .build()
            .expect("message should build");
        group.bench_with_input(
            BenchmarkId::new("encode", name),
            &provenance,
            |bencher, provenance| {
                bencher.iter(|| {
                    BTreeMap::<HeaderKey, HeaderValue>::try_from(black_box(provenance))
                        .expect("valid provenance should encode")
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("decode", name),
            &message,
            |bencher, message| {
                bencher.iter(|| {
                    Provenance::try_from(black_box(message))
                        .expect("valid provenance should decode")
                });
            },
        );
    }
    group.finish();
}

fn deduplication(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("deduplication");
    for capacity in [64, 4_096, 65_536] {
        group.bench_function(BenchmarkId::new("miss", capacity), |bencher| {
            let window = SlidingWindow::new(capacity);
            let mut sequence = 0_u64;
            bencher.iter(|| {
                sequence += 1;
                block_on(window.observe(black_box(&format!("key-{sequence}"))))
            });
        });
        group.bench_function(BenchmarkId::new("hit", capacity), |bencher| {
            let window = SlidingWindow::new(capacity);
            block_on(window.observe("existing"));
            bencher.iter(|| block_on(window.observe(black_box("existing"))));
        });
        group.bench_function(BenchmarkId::new("eviction", capacity), |bencher| {
            let window = SlidingWindow::new(capacity);
            for index in 0..capacity {
                block_on(window.observe(&format!("seed-{index}")));
            }
            let mut sequence = 0_u64;
            bencher.iter(|| {
                sequence += 1;
                block_on(window.observe(black_box(&format!("replacement-{sequence}"))))
            });
        });
    }
    group.finish();
}

fn chunk_assembly(criterion: &mut Criterion) {
    let opening = decode_chunk("agent_chunk_open.bin");
    let terminal = decode_chunk("agent_chunk_terminal.bin");
    let mut group = criterion.benchmark_group("chunk_assembly");
    for count in [2_u64, 8, 64] {
        let chunks = ordered_chunks(&opening, &terminal, count);
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(
            BenchmarkId::new("ordered_completion", count),
            &chunks,
            |bencher, chunks| {
                bencher.iter_batched(
                    ChunkAssembler::new,
                    |mut assembler| {
                        for chunk in chunks {
                            black_box(assembler.feed(black_box(chunk)));
                        }
                        black_box(assembler.is_finished());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    let first = ordered_chunks(&opening, &terminal, 8);
    let mut second_opening = opening.clone();
    second_opening.channel = Some(ChannelId::from_u128(2));
    let mut second_terminal = terminal.clone();
    second_terminal.channel = Some(ChannelId::from_u128(2));
    let second = ordered_chunks(&second_opening, &second_terminal, 8);
    let interleaved: Vec<_> = first
        .into_iter()
        .zip(second)
        .flat_map(|pair| [pair.0, pair.1])
        .collect();
    group.throughput(Throughput::Elements(16));
    group.bench_with_input("channel_interleave", &interleaved, |bencher, chunks| {
        bencher.iter_batched(
            BTreeMap::<ChannelId, ChunkAssembler>::new,
            |mut assemblers| {
                for chunk in chunks {
                    let channel = chunk.channel.expect("chunk should have a channel");
                    let events = assemblers
                        .entry(channel)
                        .or_default()
                        .feed(black_box(chunk));
                    black_box(events);
                }
                black_box(assemblers);
            },
            BatchSize::SmallInput,
        );
    });

    let duplicate = opening.clone();
    group.throughput(Throughput::Elements(2));
    group.bench_function("duplicate", |bencher| {
        bencher.iter_batched(
            ChunkAssembler::new,
            |mut assembler| {
                black_box(assembler.feed(black_box(&opening)));
                black_box(assembler.feed(black_box(&duplicate)));
                black_box(assembler.duplicates_dropped());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn decode_chunk(fixture: &str) -> AgentEnvelope {
    decode_named(laser_wire::fixtures::bytes(fixture).expect("chunk fixture should exist"))
        .expect("chunk fixture should decode")
}

fn ordered_chunks(
    opening: &AgentEnvelope,
    terminal: &AgentEnvelope,
    count: u64,
) -> Vec<AgentEnvelope> {
    assert!(
        count >= 2,
        "a stream requires an opening and terminal chunk"
    );
    let mut chunks = Vec::with_capacity(usize::try_from(count).expect("count should fit usize"));
    chunks.push(opening.clone());
    for sequence in 1..count - 1 {
        let mut chunk = opening.clone();
        chunk.sequence = Some(sequence);
        chunk.operation = None;
        chunk.deadline_micros = None;
        chunks.push(chunk);
    }
    let mut last = terminal.clone();
    last.sequence = Some(count - 1);
    chunks.push(last);
    chunks
}

fn result_mapping(criterion: &mut Criterion) {
    criterion.bench_function("result_mapping/all_codes", |bencher| {
        bencher.iter(|| {
            for code in 0..=32 {
                let result = ResultCode::from_code(black_box(code));
                black_box((result.code(), result.http_status()));
            }
        });
    });
}

fn message_construction(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("message_construction");
    for size in [64, 1_024, 4_096, 65_536] {
        let payload = Bytes::from(vec![0x42; size]);
        group.throughput(Throughput::Bytes(
            u64::try_from(size).expect("payload size should fit u64"),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &payload,
            |bencher, payload| {
                bencher.iter(|| ProducerMessage::new(black_box(payload.clone())));
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    wire_framing,
    provenance_headers,
    chunk_assembly,
    deduplication,
    result_mapping,
    message_construction
);
criterion_main!(benches);
