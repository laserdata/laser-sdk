use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use laser_wire::filter::{TextMatch, TextMatcher, TextPredicate};
use std::hint::black_box;

const GLOB_ANY_RUN: char = '*';
const GLOB_ANY_ONE: char = '?';
const GLOB_ESCAPE: char = '\\';

fn compiled_globs(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("compiled_globs");
    group.sample_size(20);
    for (name, pattern, value) in [
        (
            "event",
            "fleet.*.v?.updated".to_owned(),
            "fleet.telemetry.v1.updated".to_owned(),
        ),
        ("unicode", "*ł?".to_owned(), "a".repeat(1024) + "ł🙂"),
        (
            "adversarial_1k",
            "*".to_owned() + &"a".repeat(256) + "b",
            "a".repeat(1024),
        ),
        (
            "adversarial_64k",
            "*".to_owned() + &"a".repeat(256) + "b",
            "a".repeat(65_536),
        ),
    ] {
        let matcher = TextMatcher::compile(&TextPredicate {
            field: "type".to_owned(),
            kind: TextMatch::Glob,
            pattern: pattern.clone(),
            case_insensitive: false,
        })
        .expect("the benchmark glob compiles");
        assert_eq!(matcher.matches(&value), glob_matches(&pattern, &value));
        group.throughput(Throughput::Bytes(value.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("compiled", name),
            &value,
            |bencher, value| {
                bencher.iter(|| matcher.matches(black_box(value)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("original", name),
            &value,
            |bencher, value| {
                bencher.iter(|| glob_matches(black_box(&pattern), black_box(value)));
            },
        );
    }
    group.finish();
}

/// One glob element, after escapes are resolved.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GlobToken {
    AnyRun,
    AnyOne,
    Literal(char),
}

fn glob_tokens(pattern: &str) -> Option<Vec<GlobToken>> {
    let mut tokens = Vec::with_capacity(pattern.len());
    let mut characters = pattern.chars();
    while let Some(character) = characters.next() {
        tokens.push(match character {
            GLOB_ANY_RUN => GlobToken::AnyRun,
            GLOB_ANY_ONE => GlobToken::AnyOne,
            GLOB_ESCAPE => GlobToken::Literal(characters.next()?),
            literal => GlobToken::Literal(literal),
        });
    }
    Some(tokens)
}

// The classic two-pointer match: on a mismatch after a `*`, the star absorbs
// one more character and matching resumes, so no path is explored twice.
fn glob_matches(pattern: &str, value: &str) -> bool {
    let Some(tokens) = glob_tokens(pattern) else {
        return false;
    };
    let text: Vec<char> = value.chars().collect();
    let (mut token, mut position) = (0, 0);
    let mut resume: Option<(usize, usize)> = None;
    while position < text.len() {
        match tokens.get(token) {
            Some(GlobToken::AnyRun) => {
                resume = Some((token, position));
                token += 1;
            }
            Some(GlobToken::AnyOne) => {
                token += 1;
                position += 1;
            }
            Some(GlobToken::Literal(literal)) if *literal == text[position] => {
                token += 1;
                position += 1;
            }
            _ => match resume {
                Some((star, absorbed)) => {
                    token = star + 1;
                    position = absorbed + 1;
                    resume = Some((star, absorbed + 1));
                }
                None => return false,
            },
        }
    }
    tokens[token..]
        .iter()
        .all(|token| *token == GlobToken::AnyRun)
}

criterion_group!(benches, compiled_globs);
criterion_main!(benches);
