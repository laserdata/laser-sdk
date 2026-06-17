// Deterministic robustness suite for the decode surface: the property is that
// decoding untrusted bytes always returns a value or a DecodeError and never
// panics, and that the framer never over-reports a frame. This is the
// stable-toolchain counterpart to the cargo-fuzz crate under `fuzz/`, which
// fuzzes the same entry points with coverage guidance. The PRNG is a fixed-seed
// xorshift, so a failure reproduces exactly from the printed seed.

use laser_wire::agent::validate;
use laser_wire::fixtures::ALL;
use laser_wire::forward::{ForwardedCommand, ForwardedQuery};
use laser_wire::framing::{decode_named, frame_decode};
use laser_wire::limits::MAX_FRAME_BYTES;
use laser_wire::prelude::{
    AgentEnvelope, BrowseReply, ControlEnvelope, ForkReply, KvReply, QueryReply,
};

struct XorShift(u64);

impl XorShift {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next_u64() as u8).collect()
    }
}

// Feed bytes to every typed decoder. A successful agent decode is also pushed
// through the per-kind validity matrix. Nothing here may panic.
fn decode_every_shape(bytes: &[u8]) {
    if let Ok(envelope) = decode_named::<AgentEnvelope>(bytes) {
        let _ = validate(&envelope);
    }
    let _ = decode_named::<QueryReply>(bytes);
    let _ = decode_named::<KvReply>(bytes);
    let _ = decode_named::<ForkReply>(bytes);
    let _ = decode_named::<BrowseReply>(bytes);
    let _ = decode_named::<ControlEnvelope>(bytes);
    let _ = decode_named::<ForwardedQuery>(bytes);
    let _ = decode_named::<ForwardedCommand>(bytes);
    let _ = decode_named::<laser_wire::result::CommandError>(bytes);
}

#[test]
fn given_random_bytes_when_framed_then_should_never_panic_or_overread() {
    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    for _ in 0..50_000 {
        let len = (rng.next_u64() % 64) as usize;
        let buf = rng.bytes(len);
        if let Ok(Some((payload, consumed))) = frame_decode(&buf) {
            assert!(consumed <= buf.len(), "framer consumed past the buffer");
            assert_eq!(consumed, 4 + payload.len(), "span disagrees with payload");
        }
    }
}

#[test]
fn given_random_bytes_when_decoded_as_any_envelope_then_should_never_panic() {
    let mut rng = XorShift(0x1234_5678_9ABC_DEF0);
    for _ in 0..50_000 {
        let len = (rng.next_u64() % 96) as usize;
        decode_every_shape(&rng.bytes(len));
    }
}

#[test]
fn given_a_huge_length_prefix_when_decoded_then_should_not_allocate_or_panic() {
    // A prefix claiming a body far larger than the buffer must ask for more
    // (Ok(None)) up to the cap, and error past it, never pre-allocating.
    for declared in [0u32, 1, MAX_FRAME_BYTES as u32, u32::MAX] {
        let mut buf = declared.to_le_bytes().to_vec();
        buf.extend_from_slice(b"short");
        let result = frame_decode(&buf);
        if declared as usize > MAX_FRAME_BYTES {
            assert!(result.is_err(), "over-cap length must error");
        } else {
            // Under the cap but the body is not present yet: ask for more.
            assert!(matches!(result, Ok(None) | Ok(Some(_))));
        }
    }
}

#[test]
fn given_a_truncated_length_prefix_when_decoded_then_should_ask_for_more() {
    let buf = [0xFFu8, 0xFF, 0xFF]; // fewer than the 4 prefix bytes
    assert!(matches!(frame_decode(&buf), Ok(None)));
    assert!(matches!(frame_decode(&[]), Ok(None)));
}

#[test]
fn given_each_golden_fixture_when_single_byte_flipped_then_decode_should_never_panic() {
    // Structure-aware fuzzing: start from canonical bytes and flip one byte at
    // a time. Every mutant must decode to a value or a DecodeError, and a
    // mutant that still decodes as an agent envelope must survive validate().
    for (name, golden) in ALL {
        if !name.ends_with(".bin") {
            continue; // JSON fixtures are not CBOR frames
        }
        for pos in 0..golden.len() {
            let mut mutant = golden.to_vec();
            mutant[pos] ^= 0xFF;
            decode_every_shape(&mutant);
        }
    }
}

#[test]
fn given_each_golden_fixture_when_truncated_then_decode_should_never_panic() {
    for (name, golden) in ALL {
        if !name.ends_with(".bin") {
            continue;
        }
        for end in 0..golden.len() {
            decode_every_shape(&golden[..end]);
        }
    }
}
