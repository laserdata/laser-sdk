// The shared CBOR entry points and the `[len: u32 LE][bytes]` socket framing,
// sans-io: pure functions over byte slices, so the same code serves the
// server's socket loop, LaserData Cloud's listener, and any future transport
// without dragging a runtime into the contract. Every frame in the LaserData
// band is named-field CBOR (RFC 8949, serde maps in declaration order via
// `ciborium`). This module is the one encoding entry point so no consumer can
// drift to a different encoding. Machine ids ride as fixed-width 16-byte CBOR
// byte strings (the `wire_id!` serde uses `serialize_bytes`), not bignums.

use crate::error::DecodeError;
use crate::limits::MAX_FRAME_BYTES;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Nesting a field scan follows before it refuses the value. Deeper input is
/// never a named frame this band produces.
const MAX_SCAN_DEPTH: u32 = 64;

const MAJOR_BYTES: u8 = 2;
const MAJOR_TEXT: u8 = 3;
const MAJOR_ARRAY: u8 = 4;
const MAJOR_MAP: u8 = 5;
const MAJOR_TAG: u8 = 6;
const MAJOR_SIMPLE: u8 = 7;
const BREAK: u8 = 0xff;
const NULL: u8 = 0xf6;

/// Encode `value` as named-field CBOR (the band's canonical encoding).
pub fn encode_named<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, DecodeError> {
    let mut buffer = Vec::new();
    encode_named_into(value, &mut buffer)?;
    Ok(buffer)
}

/// The in-place form of [`encode_named`]: clear and fill the caller's buffer,
/// so a hot loop reuses one allocation across encodes. Bytes identical to
/// [`encode_named`] by construction (it delegates here), so there is still
/// exactly one encoder.
pub fn encode_named_into<T: Serialize + ?Sized>(
    value: &T,
    buffer: &mut Vec<u8>,
) -> Result<(), DecodeError> {
    buffer.clear();
    ciborium::into_writer(value, &mut *buffer)
        .map_err(|error| DecodeError::Encode(error.to_string()))
}

/// Decode named-field CBOR produced by [`encode_named`].
///
/// Rejects trailing bytes after the value. A band payload is exactly one CBOR
/// item, so leftover bytes are a decode failure, never a silent skip.
pub fn decode_named<T: DeserializeOwned>(payload: &[u8]) -> Result<T, DecodeError> {
    let mut reader = payload;
    let value = ciborium::from_reader(&mut reader)
        .map_err(|error| DecodeError::Decode(error.to_string()))?;
    if !reader.is_empty() {
        return Err(DecodeError::Decode(format!(
            "{} trailing byte(s) after the CBOR value",
            reader.len()
        )));
    }
    Ok(value)
}

/// Encode `value` as named-field CBOR behind its `[len: u32 LE]` prefix in one
/// buffer sized for `capacity_hint` payload bytes, so a transport writes the
/// frame without copying the encoded bytes again. Errors when the encoded
/// value exceeds `max_bytes` or [`MAX_FRAME_BYTES`].
pub fn encode_named_framed<T: Serialize + ?Sized>(
    value: &T,
    capacity_hint: usize,
    max_bytes: usize,
) -> Result<Vec<u8>, DecodeError> {
    let mut framed = Vec::with_capacity(4 + capacity_hint);
    framed.extend_from_slice(&[0; 4]);
    ciborium::into_writer(value, &mut framed)
        .map_err(|error| DecodeError::Encode(error.to_string()))?;
    let length = framed.len() - 4;
    if length > max_bytes.min(MAX_FRAME_BYTES) {
        return Err(DecodeError::Frame(format!(
            "payload is {length}B, exceeds frame cap {}B",
            max_bytes.min(MAX_FRAME_BYTES)
        )));
    }
    framed[..4].copy_from_slice(&(length as u32).to_le_bytes());
    Ok(framed)
}

/// The top-level fields of one named-field CBOR map, each value left encoded
/// in place.
///
/// One pass over the map finds every key and the bytes of its value and
/// copies nothing: a byte string is stepped over by its length. A reader then
/// decodes only the small values it needs and leaves a large one alone. Keys
/// must be definite text and unique, nesting is bounded, and nothing may
/// follow the map, so a malformed request is refused rather than read in
/// part.
#[derive(Debug, Default)]
pub struct NamedFields<'a> {
    fields: Vec<(&'a str, &'a [u8])>,
}

impl<'a> NamedFields<'a> {
    /// Scan the fields of `payload`.
    pub fn decode(payload: &'a [u8]) -> Result<Self, DecodeError> {
        let mut cursor = Cursor {
            bytes: payload,
            at: 0,
        };
        let (major, argument) = cursor.head()?;
        if major != MAJOR_MAP {
            return Err(DecodeError::Decode(
                "a named-field value is a map".to_owned(),
            ));
        }
        let mut fields: Vec<(&'a str, &'a [u8])> = Vec::new();
        match argument {
            Argument::Value(count) => {
                for _ in 0..count {
                    fields.push(cursor.entry()?);
                }
            }
            Argument::Indefinite => {
                while !cursor.at_break()? {
                    fields.push(cursor.entry()?);
                }
                cursor.at += 1;
            }
        }
        if cursor.at != payload.len() {
            return Err(DecodeError::Decode(format!(
                "{} trailing byte(s) after the CBOR value",
                payload.len() - cursor.at
            )));
        }
        for (index, (key, _)) in fields.iter().enumerate() {
            if fields[..index].iter().any(|(earlier, _)| earlier == key) {
                return Err(DecodeError::Decode(format!("duplicate field `{key}`")));
            }
        }
        Ok(Self { fields })
    }

    /// The encoded value of `key`, when the map holds it.
    pub fn get(&self, key: &str) -> Option<&'a [u8]> {
        self.fields
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| *value)
    }

    /// The text value of `key`, borrowed from the payload. `Ok(None)` when the
    /// field is absent or null, an error when it holds anything but text.
    pub fn text(&self, key: &str) -> Result<Option<&'a str>, DecodeError> {
        let Some(value) = self.get(key) else {
            return Ok(None);
        };
        if value == [NULL] {
            return Ok(None);
        }
        let mut cursor = Cursor {
            bytes: value,
            at: 0,
        };
        let (major, argument) = cursor.head()?;
        let (MAJOR_TEXT, Argument::Value(length)) = (major, argument) else {
            return Err(DecodeError::Decode(format!("field `{key}` is not text")));
        };
        let text = cursor.take(length)?;
        std::str::from_utf8(text)
            .map(Some)
            .map_err(|_| DecodeError::Decode(format!("field `{key}` is not UTF-8")))
    }

    /// The value of `key` decoded on its own. `Ok(None)` when the field is
    /// absent.
    pub fn value<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, DecodeError> {
        self.get(key).map(decode_named).transpose()
    }
}

/// The argument of one CBOR head: a count, a length, a value, or the
/// indefinite marker.
enum Argument {
    Value(u64),
    Indefinite,
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn entry(&mut self) -> Result<(&'a str, &'a [u8]), DecodeError> {
        let (major, argument) = self.head()?;
        let (MAJOR_TEXT, Argument::Value(length)) = (major, argument) else {
            return Err(DecodeError::Decode(
                "a named-field key is definite text".to_owned(),
            ));
        };
        let key = std::str::from_utf8(self.take(length)?)
            .map_err(|_| DecodeError::Decode("a named-field key is UTF-8".to_owned()))?;
        let start = self.at;
        self.skip(0)?;
        Ok((key, &self.bytes[start..self.at]))
    }

    fn head(&mut self) -> Result<(u8, Argument), DecodeError> {
        let initial = *self.bytes.get(self.at).ok_or_else(truncated)?;
        self.at += 1;
        let major = initial >> 5;
        let argument = match initial & 0x1f {
            info @ 0..=23 => Argument::Value(u64::from(info)),
            24 => Argument::Value(u64::from(self.take(1)?[0])),
            25 => Argument::Value(u64::from(u16::from_be_bytes(
                self.take(2)?.try_into().map_err(|_| truncated())?,
            ))),
            26 => Argument::Value(u64::from(u32::from_be_bytes(
                self.take(4)?.try_into().map_err(|_| truncated())?,
            ))),
            27 => Argument::Value(u64::from_be_bytes(
                self.take(8)?.try_into().map_err(|_| truncated())?,
            )),
            31 => Argument::Indefinite,
            _ => {
                return Err(DecodeError::Decode(
                    "reserved CBOR additional information".to_owned(),
                ));
            }
        };
        Ok((major, argument))
    }

    fn skip(&mut self, depth: u32) -> Result<(), DecodeError> {
        if depth > MAX_SCAN_DEPTH {
            return Err(DecodeError::Decode(format!(
                "CBOR nesting exceeds {MAX_SCAN_DEPTH}"
            )));
        }
        let (major, argument) = self.head()?;
        match (major, argument) {
            (MAJOR_BYTES | MAJOR_TEXT, Argument::Value(length)) => {
                self.take(length)?;
            }
            (MAJOR_BYTES | MAJOR_TEXT, Argument::Indefinite) => {
                while !self.at_break()? {
                    let (chunk_major, chunk) = self.head()?;
                    let Argument::Value(length) = chunk else {
                        return Err(DecodeError::Decode(
                            "an indefinite string chunk is definite".to_owned(),
                        ));
                    };
                    if chunk_major != major {
                        return Err(DecodeError::Decode(
                            "an indefinite string chunk has its string's type".to_owned(),
                        ));
                    }
                    self.take(length)?;
                }
                self.at += 1;
            }
            (MAJOR_ARRAY, Argument::Value(count)) => {
                for _ in 0..count {
                    self.skip(depth + 1)?;
                }
            }
            (MAJOR_MAP, Argument::Value(count)) => {
                for _ in 0..count {
                    self.skip(depth + 1)?;
                    self.skip(depth + 1)?;
                }
            }
            (MAJOR_ARRAY | MAJOR_MAP, Argument::Indefinite) => {
                let mut items = 0_u64;
                while !self.at_break()? {
                    self.skip(depth + 1)?;
                    items += 1;
                }
                if major == MAJOR_MAP && items % 2 == 1 {
                    return Err(DecodeError::Decode(
                        "an indefinite map holds whole pairs".to_owned(),
                    ));
                }
                self.at += 1;
            }
            (MAJOR_TAG, Argument::Value(_)) => self.skip(depth + 1)?,
            (MAJOR_SIMPLE, Argument::Value(_)) => {}
            (_, Argument::Value(_)) => {}
            (_, Argument::Indefinite) => {
                return Err(DecodeError::Decode(
                    "only strings, arrays and maps are indefinite".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn at_break(&self) -> Result<bool, DecodeError> {
        self.bytes
            .get(self.at)
            .map(|byte| *byte == BREAK)
            .ok_or_else(truncated)
    }

    fn take(&mut self, length: u64) -> Result<&'a [u8], DecodeError> {
        let end = usize::try_from(length)
            .ok()
            .and_then(|length| self.at.checked_add(length))
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(truncated)?;
        let taken = &self.bytes[self.at..end];
        self.at = end;
        Ok(taken)
    }
}

fn truncated() -> DecodeError {
    DecodeError::Decode("the CBOR value is truncated".to_owned())
}

/// Frame `payload` as `[len: u32 LE][payload]`. Errors when the payload
/// exceeds [`MAX_FRAME_BYTES`].
pub fn frame_encode(payload: &[u8]) -> Result<Vec<u8>, DecodeError> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(DecodeError::Frame(format!(
            "payload is {}B, exceeds frame cap {MAX_FRAME_BYTES}B",
            payload.len()
        )));
    }
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    framed.extend_from_slice(payload);
    Ok(framed)
}

/// Parse one frame from the front of `buf`. `Ok(None)` means the buffer does
/// not yet hold a whole frame, so read more bytes. `Ok(Some((payload, consumed)))`
/// is one payload and how many bytes of `buf` it spanned (`4 + len`). Errors
/// when the length prefix exceeds [`MAX_FRAME_BYTES`].
pub fn frame_decode(buf: &[u8]) -> Result<Option<(&[u8], usize)>, DecodeError> {
    let Some(prefix) = buf.get(..4) else {
        return Ok(None);
    };
    let len = u32::from_le_bytes(prefix.try_into().expect("4-byte slice")) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(DecodeError::Frame(format!(
            "frame length {len}B exceeds cap {MAX_FRAME_BYTES}B"
        )));
    }
    match buf.get(4..4 + len) {
        Some(payload) => Ok(Some((payload, 4 + len))),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_payload_when_framed_then_should_round_trip() {
        let framed = frame_encode(b"hello").expect("frames");
        assert_eq!(&framed[..4], &5u32.to_le_bytes());
        let (payload, consumed) = frame_decode(&framed)
            .expect("decodes")
            .expect("whole frame present");
        assert_eq!(payload, b"hello");
        assert_eq!(consumed, framed.len());
    }

    #[test]
    fn given_a_partial_frame_when_decoded_then_should_ask_for_more() {
        let framed = frame_encode(b"hello").expect("frames");
        assert!(frame_decode(&framed[..3]).expect("no error").is_none());
        assert!(frame_decode(&framed[..6]).expect("no error").is_none());
    }

    #[test]
    fn given_an_oversized_length_prefix_when_decoded_then_should_error() {
        let mut bad = Vec::new();
        bad.extend_from_slice(&((MAX_FRAME_BYTES as u32) + 1).to_le_bytes());
        assert!(matches!(frame_decode(&bad), Err(DecodeError::Frame(_))));
    }

    #[test]
    fn given_named_encoding_when_round_tripped_then_should_preserve_fields() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Body {
            id: u32,
            name: String,
        }
        let body = Body {
            id: 7,
            name: "alice".to_owned(),
        };
        let bytes = encode_named(&body).expect("encodes");
        let back: Body = decode_named(&bytes).expect("decodes");
        assert_eq!(back, body);
    }

    #[test]
    fn given_trailing_bytes_when_decoded_then_should_error() {
        let mut bytes = encode_named(&7u32).expect("encodes");
        bytes.push(0x00);
        assert!(matches!(
            decode_named::<u32>(&bytes),
            Err(DecodeError::Decode(_))
        ));
    }
    #[test]
    fn given_the_in_place_encoder_when_reused_then_should_match_encode_named_bytes() {
        let value = serde_json::json!({"a": 1, "b": "two"});
        let mut buffer = vec![0xff; 64]; // dirty, to prove the clear
        super::encode_named_into(&value, &mut buffer).expect("encodes");
        assert_eq!(buffer, super::encode_named(&value).expect("encodes"));
    }

    #[test]
    fn given_the_framed_encoder_when_used_then_should_prefix_the_named_bytes_and_cap_them() {
        let value = serde_json::json!({"a": 1, "b": "two"});
        let framed = encode_named_framed(&value, 16, 1024).expect("frames");
        let encoded = encode_named(&value).expect("encodes");
        assert_eq!(&framed[..4], &(encoded.len() as u32).to_le_bytes());
        assert_eq!(&framed[4..], &encoded);
        assert!(matches!(
            encode_named_framed(&value, 16, encoded.len() - 1),
            Err(DecodeError::Frame(_))
        ));
    }

    #[derive(serde::Serialize)]
    struct Keyed {
        v: u32,
        namespace: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session: Option<String>,
        #[serde(with = "crate::encoding::bin_bytes")]
        value: Vec<u8>,
    }

    fn text(value: &str) -> Vec<u8> {
        let mut bytes = vec![0x60 | value.len() as u8];
        bytes.extend_from_slice(value.as_bytes());
        bytes
    }

    // One pass finds every field, and the large byte string is a window into
    // the payload rather than a copy of it.
    #[test]
    fn given_a_named_map_when_scanned_then_should_find_each_value_in_place() {
        let keyed = Keyed {
            v: 1,
            namespace: "notes".to_owned(),
            session: None,
            value: vec![7; 8 << 20],
        };
        let payload = encode_named(&keyed).expect("encodes");
        let fields = NamedFields::decode(&payload).expect("scans");
        assert_eq!(fields.text("namespace").expect("text"), Some("notes"));
        assert_eq!(fields.value::<u32>("v").expect("value"), Some(1));
        assert_eq!(fields.text("session").expect("absent"), None);
        assert_eq!(fields.value::<u32>("session").expect("absent"), None);
        let value = fields.get("value").expect("present");
        assert!(value.len() > 8 << 20);
        let range = payload.as_ptr_range();
        assert!(
            range.contains(&value.as_ptr()) && value.as_ptr_range().end <= range.end,
            "the value is borrowed from the payload"
        );
        assert!(
            matches!(fields.text("value"), Err(DecodeError::Decode(_))),
            "a byte string is not text"
        );
        assert!(
            matches!(
                fields.value::<u32>("namespace"),
                Err(DecodeError::Decode(_))
            ),
            "text is not a number"
        );
    }

    #[test]
    fn given_indefinite_and_nested_values_when_scanned_then_should_step_over_them() {
        // {"a": [1, {"x": "y"}], "b": (_ "ab" "cd"), "c": 1(2), "d": null}
        let mut payload = vec![0xbf];
        payload.extend(text("a"));
        payload.extend([0x9f, 0x01, 0xa1]);
        payload.extend(text("x"));
        payload.extend(text("y"));
        payload.push(0xff);
        payload.extend(text("b"));
        payload.extend([0x7f, 0x62, b'a', b'b', 0x62, b'c', b'd', 0xff]);
        payload.extend(text("c"));
        payload.extend([0xc1, 0x02]);
        payload.extend(text("d"));
        payload.push(0xf6);
        payload.push(0xff);
        let fields = NamedFields::decode(&payload).expect("scans");
        assert_eq!(fields.get("a"), Some(&payload[3..11]));
        assert_eq!(
            fields.value::<String>("b").expect("joined"),
            Some("abcd".to_owned())
        );
        assert!(
            matches!(fields.text("b"), Err(DecodeError::Decode(_))),
            "a borrowed text is definite"
        );
        assert_eq!(fields.value::<u32>("c").expect("tagged"), Some(2));
        assert_eq!(fields.text("d").expect("null reads as absent"), None);
        assert_eq!(fields.value::<Option<u32>>("d").expect("null"), Some(None));
    }

    #[test]
    fn given_malformed_named_maps_when_scanned_then_should_refuse_them() {
        let mut duplicate = vec![0xa2];
        duplicate.extend(text("a"));
        duplicate.push(0x01);
        duplicate.extend(text("a"));
        duplicate.push(0x02);
        let mut trailing = vec![0xa1];
        trailing.extend(text("a"));
        trailing.extend([0x01, 0x00]);
        let mut numeric_key = vec![0xa1, 0x01, 0x01];
        let mut truncated = vec![0xa1];
        truncated.extend(text("a"));
        truncated.extend([0x45, 0x01, 0x02]);
        let mut odd = vec![0xbf];
        odd.extend(text("a"));
        odd.extend([0x01, 0xbf, 0x01, 0xff, 0xff]);
        let mut deep = vec![0xa1];
        deep.extend(text("a"));
        deep.extend(std::iter::repeat_n(0x81, MAX_SCAN_DEPTH as usize + 2));
        deep.push(0x01);
        let mut reserved = vec![0xa1];
        reserved.extend(text("a"));
        reserved.push(0x1c);
        let mut indefinite_number = vec![0xa1];
        indefinite_number.extend(text("a"));
        indefinite_number.push(0x1f);
        numeric_key.push(0x00);
        numeric_key.truncate(3);
        for (case, payload) in [
            ("duplicate", duplicate),
            ("trailing", trailing),
            ("numeric key", numeric_key),
            ("truncated", truncated),
            ("odd indefinite map", odd),
            ("deep", deep),
            ("reserved info", reserved),
            ("indefinite number", indefinite_number),
            ("not a map", vec![0x01]),
            ("empty", Vec::new()),
        ] {
            assert!(
                matches!(NamedFields::decode(&payload), Err(DecodeError::Decode(_))),
                "{case} must be refused"
            );
        }
        assert!(
            NamedFields::decode(&[0xa0])
                .expect("empty map")
                .get("a")
                .is_none()
        );
    }
}
