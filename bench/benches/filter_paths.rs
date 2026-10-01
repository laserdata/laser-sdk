use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use laser_wire::control::SchemaDef;
use laser_wire::filter::eval::{CompiledFilter, DecodeLimits, FilterRecord, HeaderRef};
use laser_wire::filter::{ConsumerFilter, FilterHeader, Verdict};
use serde::Deserialize;
use std::hint::black_box;
use std::time::Duration;

#[derive(Deserialize)]
struct JsonCorpus {
    limits: Limits,
    cases: Vec<JsonCase>,
}

#[derive(Deserialize)]
struct Limits {
    max_payload_bytes: usize,
    max_depth: usize,
}

#[derive(Deserialize)]
struct JsonCase {
    name: String,
    filter: ConsumerFilter,
    payload: String,
    #[serde(default)]
    headers: Vec<FilterHeader>,
    expected: Verdict,
}

#[derive(Deserialize)]
struct CodecCorpus {
    schemas: Vec<SchemaDef>,
    cases: Vec<CodecCase>,
}

#[derive(Deserialize)]
struct CodecCase {
    name: String,
    filter: ConsumerFilter,
    payload_hex: String,
    headers: Vec<FilterHeader>,
    expected: Verdict,
    max_depth: usize,
    max_payload_bytes: usize,
}

fn filter_paths(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("filter_paths");
    group
        .sample_size(20)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(300));
    let json: JsonCorpus =
        serde_json::from_str(include_str!("../../wire/fixtures/filter_eval_cases.json"))
            .expect("the shared JSON corpus decodes");
    let codecs: CodecCorpus =
        serde_json::from_str(include_str!("../../wire/fixtures/filter_codec_cases.json"))
            .expect("the shared codec corpus decodes");
    for case in &json.cases {
        let compiled = CompiledFilter::compile(&case.filter).expect("the corpus filter compiles");
        let headers: Vec<HeaderRef<'_>> = case.headers.iter().map(HeaderRef::from).collect();
        let record = FilterRecord {
            payload: case.payload.as_bytes(),
            headers: &headers,
        };
        let limits = DecodeLimits {
            max_payload_bytes: json.limits.max_payload_bytes,
            max_depth: json.limits.max_depth,
        };
        assert_eq!(
            compiled.evaluate(&record, &limits),
            case.expected,
            "{}",
            case.name
        );
        group.throughput(Throughput::Bytes(record.payload.len() as u64));
        group.bench_function(BenchmarkId::new("json", &case.name), |bencher| {
            bencher.iter(|| compiled.evaluate(black_box(&record), &limits));
        });
    }
    for case in &codecs.cases {
        let compiled = CompiledFilter::compile_with_schemas(&case.filter, &codecs.schemas)
            .expect("the schema-backed corpus filter compiles");
        let payload: Vec<u8> = case
            .payload_hex
            .as_bytes()
            .chunks(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("hex text"), 16)
                    .expect("hex byte")
            })
            .collect();
        let headers: Vec<HeaderRef<'_>> = case.headers.iter().map(HeaderRef::from).collect();
        let record = FilterRecord {
            payload: &payload,
            headers: &headers,
        };
        let limits = DecodeLimits {
            max_payload_bytes: case.max_payload_bytes,
            max_depth: case.max_depth,
        };
        assert_eq!(
            compiled.evaluate(&record, &limits),
            case.expected,
            "{}",
            case.name
        );
        group.throughput(Throughput::Bytes(record.payload.len() as u64));
        group.bench_function(BenchmarkId::new("codecs", &case.name), |bencher| {
            bencher.iter(|| compiled.evaluate(black_box(&record), &limits));
        });
    }
    group.finish();
}

criterion_group!(benches, filter_paths);
criterion_main!(benches);
