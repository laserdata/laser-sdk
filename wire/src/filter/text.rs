use crate::error::InvalidError;
use crate::limits::MAX_FILTER_STRING_BYTES;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};

/// Compiled text predicates (regex and glob) one filter may hold. With the
/// program bound below, one compiled filter stays under 2 MiB of regex
/// programs, and the per-shard cache of compiled filters stays bounded with
/// it. Search time is linear in the value for each pattern, so a record costs
/// at most this many passes over its text fields.
pub const MAX_REGEX_PREDICATES: usize = 4;
/// Compiled program bound for one regex or glob, NFA and lazy DFA each. This
/// bounds memory. Time stays linear in the searched text because the engine
/// never backtracks.
const REGEX_SIZE_LIMIT: usize = 256 << 10;
const REGEX_NEST_LIMIT: u32 = 64;
const GLOB_ANY_RUN: char = '*';
const GLOB_ANY_ONE: char = '?';
const GLOB_ESCAPE: char = '\\';

/// A text match against one payload field or one header. The value must be
/// text: a missing value is unknown, and a value of another type is a type
/// mismatch, which follows the filter's mismatch policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextPredicate {
    /// A payload field path, or a header key for [`FilterExpr::HeaderText`](crate::filter::FilterExpr::HeaderText).
    pub field: String,
    pub kind: TextMatch,
    pub pattern: String,
    /// Regex uses the engine's Unicode folding. Other kinds lowercase both sides.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub case_insensitive: bool,
}

/// How a [`TextPredicate`] matches, from cheapest to most expensive.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TextMatch {
    /// The whole value equals the pattern.
    Equals,
    /// The value starts with the pattern.
    Prefix,
    /// The value ends with the pattern.
    Suffix,
    /// The pattern occurs anywhere in the value.
    Contains,
    /// The whole value matches a glob: `*` is any run of characters, `?` is
    /// one character, and `\` escapes the next character.
    Glob,
    /// The value contains a match of a regular expression, run by the
    /// server's Rust engine in time linear in the value. Lookaround,
    /// backreferences, and inline flags are refused. `\d`, `\w`, `\s`, and
    /// `\b` are Unicode-aware, and case-insensitive matching uses the
    /// engine's simple case folding, where the other kinds lowercase both
    /// sides. The server's verdict is the contract, and the Rust, Python, and
    /// TypeScript local evaluators follow the same rules.
    Regex,
}

/// A validated [`TextPredicate`] ready to run per record.
#[derive(Clone, Debug)]
pub struct TextMatcher {
    kind: TextMatch,
    /// The pattern as written, for explanations.
    source: String,
    case_insensitive: bool,
    program: Program,
}

/// What one evaluation runs: a plain comparison against the pattern, lowercased
/// when case-insensitive, or a compiled regex for the `glob` and `regex` kinds.
#[derive(Clone, Debug)]
enum Program {
    Plain(String),
    Compiled(Regex),
}

impl TextPredicate {
    /// Compiling is the validation, so a predicate is checked exactly once.
    pub fn validate(&self) -> Result<(), InvalidError> {
        TextMatcher::compile(self).map(|_| ())
    }
}

impl TextMatcher {
    pub fn compile(predicate: &TextPredicate) -> Result<Self, InvalidError> {
        if predicate.pattern.len() > MAX_FILTER_STRING_BYTES {
            return Err(InvalidError::new(format!(
                "a text pattern exceeds {MAX_FILTER_STRING_BYTES}B"
            )));
        }
        let program = match predicate.kind {
            TextMatch::Regex => Program::Compiled(compile_regex(
                &predicate.pattern,
                predicate.case_insensitive,
            )?),
            TextMatch::Glob => Program::Compiled(compile_glob(
                &predicate.pattern,
                predicate.case_insensitive,
            )?),
            _ if predicate.case_insensitive => Program::Plain(predicate.pattern.to_lowercase()),
            _ => Program::Plain(predicate.pattern.clone()),
        };
        Ok(Self {
            kind: predicate.kind,
            source: predicate.pattern.clone(),
            case_insensitive: predicate.case_insensitive,
            program,
        })
    }

    pub fn matches(&self, value: &str) -> bool {
        let pattern = match &self.program {
            Program::Compiled(regex) => {
                return if self.kind == TextMatch::Glob && self.case_insensitive {
                    regex.is_match(&value.to_lowercase())
                } else {
                    regex.is_match(value)
                };
            }
            Program::Plain(pattern) => pattern.as_str(),
        };
        let folded;
        let value = if self.case_insensitive {
            folded = value.to_lowercase();
            folded.as_str()
        } else {
            value
        };
        match self.kind {
            TextMatch::Equals => value == pattern,
            TextMatch::Prefix => value.starts_with(pattern),
            TextMatch::Suffix => value.ends_with(pattern),
            TextMatch::Contains => value.contains(pattern),
            TextMatch::Glob | TextMatch::Regex => false,
        }
    }

    pub fn kind(&self) -> TextMatch {
        self.kind
    }

    /// The pattern as written, with its case mode, for explanations.
    pub fn label(&self) -> String {
        if self.case_insensitive {
            format!("{:?} ignoring case", self.source)
        } else {
            format!("{:?}", self.source)
        }
    }
}

// A glob compiles to an anchored regex, so it runs in linear time like every
// other kind. Case-insensitive globs lowercase their literals, and `matches`
// lowercases the value, the same rule the plain kinds follow.
fn compile_glob(pattern: &str, case_insensitive: bool) -> Result<Regex, InvalidError> {
    let tokens = glob_tokens(pattern).ok_or_else(|| {
        InvalidError::new("a glob pattern cannot end with an unescaped backslash")
    })?;
    let mut expression = String::from("\\A(?:");
    for token in tokens {
        match token {
            GlobToken::AnyRun => expression.push_str(".*"),
            GlobToken::AnyOne => expression.push('.'),
            GlobToken::Literal(character) => {
                let literal = character.to_string();
                let literal = if case_insensitive {
                    literal.to_lowercase()
                } else {
                    literal
                };
                expression.push_str(&regex::escape(&literal));
            }
        }
    }
    expression.push_str(")\\z");
    RegexBuilder::new(&expression)
        .dot_matches_new_line(true)
        .size_limit(REGEX_SIZE_LIMIT)
        .dfa_size_limit(REGEX_SIZE_LIMIT)
        .nest_limit(REGEX_NEST_LIMIT)
        .build()
        .map_err(|error| InvalidError::new(format!("invalid glob: {error}")))
}

// Inline flags, lookaround, and backreferences have no shared meaning across
// the SDK regex engines, so they are refused before the engine sees them.
fn compile_regex(pattern: &str, case_insensitive: bool) -> Result<Regex, InvalidError> {
    if let Some(construct) = unportable_regex_construct(pattern) {
        return Err(InvalidError::new(format!(
            "regex {construct} is not supported, write the pattern without it"
        )));
    }
    RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .size_limit(REGEX_SIZE_LIMIT)
        .dfa_size_limit(REGEX_SIZE_LIMIT)
        .nest_limit(REGEX_NEST_LIMIT)
        .build()
        .map_err(|error| InvalidError::new(format!("invalid regex: {error}")))
}

fn unportable_regex_construct(pattern: &str) -> Option<&'static str> {
    let mut characters = pattern.chars().peekable();
    let mut in_class = false;
    while let Some(character) = characters.next() {
        match character {
            '\\' => match characters.next() {
                Some('1'..='9') => return Some("backreferences"),
                Some('k') => return Some("named backreferences"),
                _ => {}
            },
            '[' => in_class = true,
            ']' => in_class = false,
            '(' if !in_class && characters.peek() == Some(&'?') => {
                characters.next();
                if characters.peek() != Some(&':') {
                    return Some("groups starting with (? other than (?:");
                }
            }
            _ => {}
        }
    }
    None
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
// one more character and matching resumes, which can repeat prefix comparisons on adversarial inputs.
#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn matcher(kind: TextMatch, pattern: &str, case_insensitive: bool) -> TextMatcher {
        TextMatcher::compile(&TextPredicate {
            field: "type".to_owned(),
            kind,
            pattern: pattern.to_owned(),
            case_insensitive,
        })
        .expect("the pattern compiles")
    }

    #[test]
    fn given_each_kind_when_matching_event_types_then_should_follow_its_rule() {
        let value = "metrics.cpu.v1.reported";
        assert!(matcher(TextMatch::Equals, value, false).matches(value));
        assert!(matcher(TextMatch::Prefix, "metrics.", false).matches(value));
        assert!(matcher(TextMatch::Suffix, ".reported", false).matches(value));
        assert!(matcher(TextMatch::Contains, ".v1.", false).matches(value));
        assert!(matcher(TextMatch::Glob, "metrics.*.v?.reported", false).matches(value));
        assert!(matcher(TextMatch::Regex, r"^metrics\.[a-z]+\.v[0-9]+\.", false).matches(value));
        assert!(!matcher(TextMatch::Suffix, ".deleted", false).matches(value));
        assert!(!matcher(TextMatch::Glob, "metrics.*", false).matches("storage.metrics.x"));
    }

    #[test]
    fn given_case_insensitive_matching_when_the_case_differs_then_should_still_match() {
        assert!(matcher(TextMatch::Equals, "Safe", true).matches("SAFE"));
        assert!(matcher(TextMatch::Contains, "CPU", true).matches("metrics.cpu.v1"));
        assert!(matcher(TextMatch::Glob, "METRICS.*", true).matches("metrics.x"));
        assert!(matcher(TextMatch::Regex, "^metrics", true).matches("METRICS.x"));
        assert!(!matcher(TextMatch::Equals, "Safe", false).matches("SAFE"));
    }

    #[test]
    fn given_globs_with_escapes_and_stars_when_matching_then_should_match_the_whole_value() {
        assert!(matcher(TextMatch::Glob, r"a\*b", false).matches("a*b"));
        assert!(!matcher(TextMatch::Glob, r"a\*b", false).matches("axb"));
        assert!(matcher(TextMatch::Glob, "*", false).matches(""));
        assert!(matcher(TextMatch::Glob, "a*b*c", false).matches("aXXbYYbZc"));
        assert!(!matcher(TextMatch::Glob, "a*b*c", false).matches("aXXbYY"));
        assert!(matcher(TextMatch::Glob, "?", false).matches("ł"));
    }

    #[test]
    fn given_glob_patterns_when_compiled_then_should_match_the_independent_original_oracle() {
        for pattern in [
            "", "*", "**", "?", "*?", "a*b*c", "*aaab", "a**b", r"a\*b", r"a\?b", r"a\\b", "*ł?",
            "*\n*", "[a]", "(?x)",
        ] {
            let compiled = matcher(TextMatch::Glob, pattern, false);
            for value in [
                "",
                "a",
                "ab",
                "ac",
                "aaab",
                "aaaaac",
                "a*b",
                "a?b",
                r"a\b",
                "aXXbYYbZc",
                "ł🙂",
                "aaał🙂",
                "a\nb",
                "[a]",
                "(?x)",
            ] {
                assert_eq!(
                    compiled.matches(value),
                    glob_matches(pattern, value),
                    "{pattern:?} {value:?}"
                );
            }
        }
        assert!(matcher(TextMatch::Glob, "İ?", true).matches("İx"));
        assert_eq!(matcher(TextMatch::Glob, "a*", false).label(), "\"a*\"");
    }

    #[test]
    fn given_a_long_repeated_prefix_when_matching_a_glob_then_should_reject_a_missing_suffix() {
        let pattern = format!("*{}b", "a".repeat(256));
        let value = "a".repeat(65_536);
        assert!(!matcher(TextMatch::Glob, &pattern, false).matches(&value));
    }

    #[test]
    fn given_unportable_or_broken_patterns_when_validated_then_should_refuse_them() {
        for (kind, pattern) in [
            (TextMatch::Regex, r"(a)\1"),
            (TextMatch::Regex, "(?=a)"),
            (TextMatch::Regex, "(?i)a"),
            (TextMatch::Regex, "(?P<name>a)"),
            (TextMatch::Regex, "("),
            (TextMatch::Glob, "a\\"),
        ] {
            let predicate = TextPredicate {
                field: "type".to_owned(),
                kind,
                pattern: pattern.to_owned(),
                case_insensitive: false,
            };
            assert!(predicate.validate().is_err(), "{pattern}");
        }
        assert!(matcher(TextMatch::Regex, "(?:a|b)[(?]", false).matches("b?"));
    }
}
