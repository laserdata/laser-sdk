#![no_main]

// Hostile bytes through the length-prefixed framer must never panic: the call
// returns a payload, asks for more, or errors. When it yields a frame, the
// reported span must be consistent with the buffer it came from.

use laser_wire::framing::frame_decode;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(Some((payload, consumed))) = frame_decode(data) {
        assert!(consumed <= data.len(), "consumed past the buffer");
        assert_eq!(consumed, 4 + payload.len(), "span disagrees with payload");
    }
});
