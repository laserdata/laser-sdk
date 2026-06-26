// The one content-hash every SDK shares, so a content-addressed id (a deduped
// memory item, a converged graph node) is identical across languages. Pure and
// dependency-free FNV-1a, no clock or entropy (id GENERATION lives SDK-side; this
// is deterministic hashing, which belongs in the contract). A port reproduces it
// from the same byte segments and the fixture pins the result.

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(salt: u8, segments: &[&[u8]]) -> u64 {
    let mut hash = FNV_OFFSET;
    hash ^= u64::from(salt);
    hash = hash.wrapping_mul(FNV_PRIME);
    for segment in segments {
        for &byte in *segment {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    hash
}

/// A deterministic 128-bit content id over `segments`, the concatenation of which
/// is the content being addressed. Two independently salted FNV-1a passes fill
/// the high and low halves, so the same segments always yield the same id and a
/// different input yields a different one. The caller assembles the segments
/// (e.g. an owner, a discriminator byte, and a body) and the order is part of the
/// contract.
pub fn content_id(segments: &[&[u8]]) -> u128 {
    (u128::from(fnv1a(0x4d, segments)) << 64) | u128::from(fnv1a(0xc7, segments))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_the_same_segments_when_hashed_then_should_be_stable() {
        let a = content_id(&[b"owner", &[1], b"body"]);
        let b = content_id(&[b"owner", &[1], b"body"]);
        assert_eq!(a, b);
    }

    #[test]
    fn given_different_segments_when_hashed_then_should_differ() {
        assert_ne!(content_id(&[b"a"]), content_id(&[b"b"]));
    }

    #[test]
    fn given_the_pinned_memory_segments_when_hashed_then_should_match_the_golden_id() {
        // The cross-SDK golden vector: an empty stream segment, agent "agent", a
        // separator, the Fact kind byte (1), and body "x". The SDK and the Python
        // reference both render this u128 as the ULID below, so the dedup id agrees
        // across languages. Rendered with the crate's Crockford encoder, the same
        // one every wire id displays through.
        let id = content_id(&[&[0], b"agent", &[0], &[1], b"x"]);
        let rendered = crate::agent::RecordId::from_u128(id).to_string();
        assert_eq!(rendered, "1A9GVS6SJ6SNS4KY0H19130WCW");
    }
}
