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
}
