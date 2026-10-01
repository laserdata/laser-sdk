use crate::error::InvalidError;
use crate::limits::{MAX_FILTER_PATH_BYTES, MAX_FILTER_PATH_SEGMENTS};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// One step of a [`FieldPath`]: an object key or an array index.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PathSegment {
    Key(String),
    Index(u32),
}

/// A validated location inside a decoded payload.
///
/// The text form separates object keys with `.` and selects array elements
/// with `[n]`, so `after.ground_stations[0]` reads the first ground station. A
/// literal `.`, `[`, `]`, or `\` inside a key is escaped with `\`. A digit-only
/// key stays a key (`codes.200`), only brackets select an index. There is one
/// text form per path, so the rendered string is also the canonical encoding
/// the filter digest covers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FieldPath {
    segments: Vec<PathSegment>,
}

impl FieldPath {
    /// Parse the text form. Rejects empty keys, unterminated escapes or
    /// brackets, non-canonical indexes, and paths over the segment or byte caps.
    pub fn parse(text: &str) -> Result<Self, InvalidError> {
        if text.is_empty() {
            return Err(InvalidError::new("field path must not be empty"));
        }
        if text.len() > MAX_FILTER_PATH_BYTES {
            return Err(InvalidError::new(format!(
                "field path is {}B, exceeds cap {MAX_FILTER_PATH_BYTES}B",
                text.len()
            )));
        }
        let mut parser = PathParser {
            bytes: text.as_bytes(),
            position: 0,
            text,
        };
        let segments = parser.segments()?;
        if segments.len() > MAX_FILTER_PATH_SEGMENTS {
            return Err(InvalidError::new(format!(
                "field path `{text}` has {} segments, exceeds cap {MAX_FILTER_PATH_SEGMENTS}",
                segments.len()
            )));
        }
        Ok(Self { segments })
    }

    pub fn segments(&self) -> &[PathSegment] {
        &self.segments
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (position, segment) in self.segments.iter().enumerate() {
            match segment {
                PathSegment::Key(key) => {
                    if position > 0 {
                        formatter.write_str(".")?;
                    }
                    for character in key.chars() {
                        if matches!(character, '.' | '[' | ']' | '\\') {
                            formatter.write_str("\\")?;
                        }
                        write!(formatter, "{character}")?;
                    }
                }
                PathSegment::Index(index) => write!(formatter, "[{index}]")?,
            }
        }
        Ok(())
    }
}

impl std::str::FromStr for FieldPath {
    type Err = InvalidError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::parse(text)
    }
}

impl Serialize for FieldPath {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for FieldPath {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).map_err(D::Error::custom)
    }
}

struct PathParser<'a> {
    bytes: &'a [u8],
    position: usize,
    text: &'a str,
}

impl PathParser<'_> {
    fn segments(&mut self) -> Result<Vec<PathSegment>, InvalidError> {
        let mut segments = Vec::new();
        if self.peek() == Some(b'[') {
            segments.push(self.index()?);
        } else {
            segments.push(self.key()?);
        }
        while let Some(byte) = self.peek() {
            match byte {
                b'.' => {
                    self.position += 1;
                    segments.push(self.key()?);
                }
                b'[' => segments.push(self.index()?),
                _ => return Err(self.invalid("expected `.` or `[` after a segment")),
            }
        }
        Ok(segments)
    }

    fn key(&mut self) -> Result<PathSegment, InvalidError> {
        let mut key = String::new();
        while let Some(byte) = self.peek() {
            match byte {
                b'.' | b'[' => break,
                b']' => return Err(self.invalid("unescaped `]` inside a key")),
                b'\\' => {
                    self.position += 1;
                    match self.peek() {
                        Some(escaped @ (b'.' | b'[' | b']' | b'\\')) => {
                            key.push(char::from(escaped));
                            self.position += 1;
                        }
                        _ => return Err(self.invalid("`\\` must escape `.`, `[`, `]`, or `\\`")),
                    }
                }
                _ => {
                    let rest = &self.text[self.position..];
                    let character = rest
                        .chars()
                        .next()
                        .expect("a peeked byte starts a character");
                    key.push(character);
                    self.position += character.len_utf8();
                }
            }
        }
        if key.is_empty() {
            return Err(self.invalid("empty key"));
        }
        Ok(PathSegment::Key(key))
    }

    fn index(&mut self) -> Result<PathSegment, InvalidError> {
        self.position += 1;
        let start = self.position;
        while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
            self.position += 1;
        }
        let digits = &self.text[start..self.position];
        if self.peek() != Some(b']') {
            return Err(self.invalid("an index must be digits closed by `]`"));
        }
        self.position += 1;
        if digits.is_empty() || (digits.len() > 1 && digits.starts_with('0')) {
            return Err(self.invalid("an index must be a canonical decimal number"));
        }
        digits
            .parse::<u32>()
            .map(PathSegment::Index)
            .map_err(|_| self.invalid("an index must fit in 32 bits"))
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.position).copied()
    }

    fn invalid(&self, reason: &str) -> InvalidError {
        InvalidError::new(format!(
            "field path `{}` is invalid at byte {}: {reason}",
            self.text, self.position
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(text: &str) -> Vec<PathSegment> {
        let path = FieldPath::parse(text).expect("the path parses");
        assert_eq!(path.to_string(), text, "the text form is canonical");
        path.segments().to_vec()
    }

    #[test]
    fn given_dotted_keys_when_parsed_then_should_split_on_dots() {
        assert_eq!(
            round_trip("after.orbit.kind"),
            vec![
                PathSegment::Key("after".to_owned()),
                PathSegment::Key("orbit".to_owned()),
                PathSegment::Key("kind".to_owned()),
            ]
        );
    }

    #[test]
    fn given_brackets_when_parsed_then_should_select_indexes() {
        assert_eq!(
            round_trip("[0].ground_stations[12]"),
            vec![
                PathSegment::Index(0),
                PathSegment::Key("ground_stations".to_owned()),
                PathSegment::Index(12),
            ]
        );
    }

    #[test]
    fn given_a_digit_key_when_parsed_then_should_stay_a_key() {
        assert_eq!(
            round_trip("codes.200"),
            vec![
                PathSegment::Key("codes".to_owned()),
                PathSegment::Key("200".to_owned()),
            ]
        );
    }

    #[test]
    fn given_escaped_characters_when_parsed_then_should_keep_them_in_the_key() {
        assert_eq!(
            round_trip(r"a\.b.c\[d\]\\"),
            vec![
                PathSegment::Key("a.b".to_owned()),
                PathSegment::Key(r"c[d]\".to_owned()),
            ]
        );
    }

    #[test]
    fn given_malformed_paths_when_parsed_then_should_be_rejected() {
        for text in [
            "",
            "a..b",
            ".a",
            "a.",
            "a[",
            "a[]",
            "a[01]",
            "a[x]",
            "a]",
            r"a\b",
            r"a\",
            "a[1]b",
            "a[4294967296]",
        ] {
            assert!(FieldPath::parse(text).is_err(), "`{text}` must be rejected");
        }
    }

    #[test]
    fn given_too_many_segments_when_parsed_then_should_be_rejected() {
        let text = vec!["a"; MAX_FILTER_PATH_SEGMENTS + 1].join(".");
        assert!(FieldPath::parse(&text).is_err());
        let text = vec!["a"; MAX_FILTER_PATH_SEGMENTS].join(".");
        assert!(FieldPath::parse(&text).is_ok());
    }

    #[test]
    fn given_a_path_when_serialized_then_should_be_its_text_form() {
        let path = FieldPath::parse("fields.mode").expect("parses");
        let json = serde_json::to_string(&path).expect("serializes");
        assert_eq!(json, r#""fields.mode""#);
        let back: FieldPath = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, path);
        assert!(serde_json::from_str::<FieldPath>(r#""a..b""#).is_err());
    }
}
