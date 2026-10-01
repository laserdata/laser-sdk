use super::eval::{HeaderNeed, HeaderRef, HeaderValueRef};
use crate::headers::CONTENT_TYPE;

/// Decode native Iggy header TLVs without changing unknown value kinds. An
/// entry under a non-text key is stepped over undecoded. A malformed value
/// under a text key faults the whole record. Entries keep their order, and
/// the evaluator reads the last entry of a repeated key.
pub fn decode(mut bytes: &[u8]) -> Option<Vec<HeaderRef<'_>>> {
    let mut headers: Vec<HeaderRef<'_>> = Vec::new();
    while !bytes.is_empty() {
        let (key_kind, key) = field(&mut bytes)?;
        let (value_kind, value) = field(&mut bytes)?;
        if key_kind != 2 {
            continue;
        }
        let key = std::str::from_utf8(key).ok()?;
        let value = scalar(value_kind, value)?;
        headers.push(HeaderRef { key, value });
    }
    Some(headers)
}

/// The headers a filter needs before evaluating, decoded from the raw block:
/// nothing, only `agdx.ct`, or every entry. `None` when the block's structure
/// does not parse, which the caller reports as a malformed record.
pub enum DecodedHeaders<'a> {
    Empty,
    ContentType(Option<HeaderRef<'a>>),
    All(Vec<HeaderRef<'a>>),
}

impl<'a> std::ops::Deref for DecodedHeaders<'a> {
    type Target = [HeaderRef<'a>];
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Empty => &[],
            Self::ContentType(header) => header.as_slice(),
            Self::All(headers) => headers,
        }
    }
}

pub fn decode_for(need: HeaderNeed, bytes: &[u8]) -> Option<DecodedHeaders<'_>> {
    match need {
        HeaderNeed::None => Some(DecodedHeaders::Empty),
        HeaderNeed::ContentType => content_type_one(bytes).map(DecodedHeaders::ContentType),
        HeaderNeed::All => decode(bytes).map(DecodedHeaders::All),
    }
}

/// Only the `agdx.ct` header, for a filter that reads no other header. Every
/// entry is stepped over, so a block whose structure does not parse still
/// faults the record, but no other key or value is decoded or checked.
pub fn content_type(bytes: &[u8]) -> Option<Vec<HeaderRef<'static>>> {
    content_type_one(bytes).map(|header| header.into_iter().collect())
}

fn content_type_one(mut bytes: &[u8]) -> Option<Option<HeaderRef<'static>>> {
    let mut found = None;
    while !bytes.is_empty() {
        let (key_kind, key) = field(&mut bytes)?;
        let (value_kind, value) = field(&mut bytes)?;
        if key_kind == 2 && key == CONTENT_TYPE.as_bytes() {
            // Only an integer names a codec. Any other value declares nothing.
            found = match scalar(value_kind, value)? {
                HeaderValueRef::Uint(code) => Some(HeaderValueRef::Uint(code)),
                HeaderValueRef::Int(code) => Some(HeaderValueRef::Int(code)),
                _ => None,
            }
            .map(|value| HeaderRef {
                key: CONTENT_TYPE,
                value,
            });
        }
    }
    Some(found)
}

fn field<'a>(bytes: &mut &'a [u8]) -> Option<(u8, &'a [u8])> {
    let kind = *bytes.first()?;
    let length = u32::from_le_bytes(bytes.get(1..5)?.try_into().ok()?) as usize;
    if kind == 0 || !(1..=255).contains(&length) {
        return None;
    }
    let value = bytes.get(5..5 + length)?;
    *bytes = &bytes[5 + length..];
    Some((kind, value))
}

fn scalar(kind: u8, value: &[u8]) -> Option<HeaderValueRef<'_>> {
    Some(match kind {
        2 => HeaderValueRef::String(std::str::from_utf8(value).ok()?),
        3 => match value {
            [0] => HeaderValueRef::Bool(false),
            [1] => HeaderValueRef::Bool(true),
            _ => return None,
        },
        4 => HeaderValueRef::Int(i64::from(i8::from_le_bytes(value.try_into().ok()?))),
        5 => HeaderValueRef::Int(i64::from(i16::from_le_bytes(value.try_into().ok()?))),
        6 => HeaderValueRef::Int(i64::from(i32::from_le_bytes(value.try_into().ok()?))),
        7 => HeaderValueRef::Int(i64::from_le_bytes(value.try_into().ok()?)),
        // 128-bit integers compare as raw bytes, but only at their width.
        8 | 13 if value.len() != 16 => return None,
        9 => HeaderValueRef::Uint(u64::from(u8::from_le_bytes(value.try_into().ok()?))),
        10 => HeaderValueRef::Uint(u64::from(u16::from_le_bytes(value.try_into().ok()?))),
        11 => HeaderValueRef::Uint(u64::from(u32::from_le_bytes(value.try_into().ok()?))),
        12 => HeaderValueRef::Uint(u64::from_le_bytes(value.try_into().ok()?)),
        14 => HeaderValueRef::Float(f64::from(f32::from_le_bytes(value.try_into().ok()?))),
        15 => HeaderValueRef::Float(f64::from_le_bytes(value.try_into().ok()?)),
        _ => HeaderValueRef::Raw(value),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &str, kind: u8, value: &[u8]) -> Vec<u8> {
        let mut bytes = vec![2];
        bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(key.as_bytes());
        bytes.push(kind);
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(value);
        bytes
    }

    #[test]
    fn given_unknown_and_known_headers_when_decoded_then_should_preserve_both() {
        let mut bytes = entry("future", 99, b"opaque");
        bytes.extend(entry("priority", 9, &[2]));
        let headers = decode(&bytes).expect("valid headers");
        assert_eq!(headers[0].value, HeaderValueRef::Raw(b"opaque"));
        assert_eq!(headers[1].value, HeaderValueRef::Uint(2));
    }

    #[test]
    fn given_many_headers_when_only_the_content_type_is_read_then_should_decode_just_that_one() {
        let mut bytes = entry("frostline.unit", 2, b"reefer");
        bytes.extend(entry("agdx.ct", 9, &[1]));
        bytes.extend(entry("agdx.ct", 9, &[3]));
        bytes.extend(entry("odd", 3, &[2]));
        let headers = content_type(&bytes).expect("the structure parses");
        assert_eq!(headers.len(), 1);
        assert_eq!(
            (headers[0].key, headers[0].value),
            ("agdx.ct", HeaderValueRef::Uint(3))
        );
        assert!(
            content_type(&entry("frostline.unit", 2, b"reefer"))
                .expect("parses")
                .is_empty()
        );
        let mut truncated = entry("agdx.ct", 9, &[1]);
        truncated.truncate(truncated.len() - 1);
        assert!(content_type(&truncated).is_none());
        assert!(content_type(&entry("agdx.ct", 9, &[1, 2])).is_none());
    }

    #[test]
    fn given_malformed_known_headers_when_decoded_then_should_fault_the_whole_block() {
        for (kind, value) in [
            (11, vec![1]),
            (3, vec![2]),
            (8, vec![0; 15]),
            (2, vec![255]),
        ] {
            let mut bytes = entry("priority", 9, &[2]);
            bytes.extend(entry("broken", kind, &value));
            assert!(decode(&bytes).is_none());
        }
    }
}
