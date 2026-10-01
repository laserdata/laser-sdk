// The consumer-filter truth table every evaluator must reproduce. The corpus is
// language-neutral JSON under `fixtures/`, so the Rust evaluator here, the
// TypeScript evaluator, and the BDD reference engine all read the same cases.

use laser_wire::filter::eval::{CompiledFilter, DecodeLimits, FilterRecord, HeaderRef};
use laser_wire::filter::{ConsumerFilter, FaultReason, FilterHeader, Verdict};
use serde::{Deserialize, Deserializer};

const CORPUS: &str = include_str!("../fixtures/filter_eval_cases.json");

#[derive(Deserialize)]
struct Corpus {
    evaluator_version: u32,
    limits: CorpusLimits,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct CorpusLimits {
    max_payload_bytes: usize,
    max_depth: usize,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    filter: ConsumerFilter,
    payload: String,
    #[serde(default)]
    headers: Vec<FilterHeader>,
    expected: Verdict,
    /// The reason a fault or a policy rejection carries, when the case pins it.
    #[serde(default, deserialize_with = "expected_fault")]
    fault: Option<Option<FaultReason>>,
}

#[test]
fn given_the_shared_corpus_when_evaluated_then_should_reproduce_every_verdict() {
    let corpus: Corpus = serde_json::from_str(CORPUS).expect("the corpus decodes");
    assert_eq!(
        corpus.evaluator_version,
        laser_wire::filter::FILTER_EVALUATOR_VERSION
    );
    let limits = DecodeLimits {
        max_payload_bytes: corpus.limits.max_payload_bytes,
        max_depth: corpus.limits.max_depth,
    };
    let mut failures = Vec::new();
    for case in &corpus.cases {
        let compiled = CompiledFilter::compile(&case.filter)
            .unwrap_or_else(|error| panic!("case `{}` must compile: {error}", case.name));
        let headers: Vec<HeaderRef<'_>> = case.headers.iter().map(HeaderRef::from).collect();
        let record = FilterRecord {
            payload: case.payload.as_bytes(),
            headers: &headers,
        };
        let (verdict, fault) = compiled.evaluate_with_fault(&record, &limits);
        if verdict != case.expected {
            failures.push(format!(
                "`{}`: expected {}, got {verdict}",
                case.name, case.expected
            ));
        }
        if let Some(expected) = case.fault
            && fault != expected
        {
            failures.push(format!(
                "`{}`: expected fault {:?}, got {fault:?}",
                case.name, case.fault
            ));
        }
        let explained = compiled.explain(&record, &limits);
        assert_eq!(
            explained.verdict, verdict,
            "case `{}`: explain agrees with evaluate",
            case.name
        );
    }
    assert!(
        failures.is_empty(),
        "corpus mismatches:\n{}",
        failures.join("\n")
    );
}

#[derive(Deserialize)]
struct CodecCorpus {
    schemas: Vec<laser_wire::control::SchemaDef>,
    cases: Vec<CodecCase>,
}

#[derive(Deserialize)]
struct CodecCase {
    name: String,
    filter: ConsumerFilter,
    payload_hex: String,
    headers: Vec<FilterHeader>,
    expected: Verdict,
    fault: Option<laser_wire::filter::FaultReason>,
    max_depth: usize,
    max_payload_bytes: usize,
}

#[test]
fn given_encoded_codec_fixtures_when_evaluated_then_should_match_verdict_and_fault() {
    let corpus: CodecCorpus =
        serde_json::from_str(include_str!("../fixtures/filter_codec_cases.json"))
            .expect("codec corpus");
    for case in corpus.cases {
        let compiled =
            CompiledFilter::compile_with_schemas(&case.filter, &corpus.schemas).expect(&case.name);
        let payload: Vec<u8> = case
            .payload_hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let headers: Vec<_> = case.headers.iter().map(HeaderRef::from).collect();
        let record = FilterRecord {
            payload: &payload,
            headers: &headers,
        };
        let limits = DecodeLimits {
            max_depth: case.max_depth,
            max_payload_bytes: case.max_payload_bytes,
        };
        assert_eq!(
            compiled.evaluate_with_fault(&record, &limits),
            (case.expected, case.fault),
            "{}",
            case.name
        );
        assert_eq!(
            compiled.explain(&record, &limits).verdict,
            case.expected,
            "{}",
            case.name
        );
    }
}

fn expected_fault<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<FaultReason>>, D::Error> {
    Option::<FaultReason>::deserialize(deserializer).map(Some)
}

#[test]
fn given_excessive_cbor_depth_when_the_caller_raises_limits_then_should_preserve_the_hard_bound() {
    let mut payload = vec![0x81; laser_wire::limits::MAX_FILTER_PARSE_DEPTH + 1];
    payload.push(0);
    assert_eq!(
        laser_wire::filter::codecs::decode_cbor(
            &payload,
            &DecodeLimits {
                max_payload_bytes: 4096,
                max_depth: usize::MAX,
            }
        ),
        Err(FaultReason::TooDeep),
    );
}
