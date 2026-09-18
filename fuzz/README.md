# Wire decode fuzzing

This crate tests `laser-wire` decoders with generated input. It is outside the main workspace because it requires nightly Rust and `cargo-fuzz`.

`wire/tests/robustness.rs` tests the same decoding paths with deterministic inputs on stable Rust. This separate crate supports longer coverage-guided runs and smaller reproductions of crashes.

## Run

```sh
cargo install cargo-fuzz          # one time, installs the cargo subcommand
cargo +nightly fuzz run frame_decode
cargo +nightly fuzz run decode_envelope
```

Or via the recipe: `just fuzz frame_decode`.

## Targets

- `frame_decode`: the `[len: u32 LE][payload]` framer. Asserts no panic and that a returned frame's span is consistent with its payload and the input buffer.
- `decode_envelope`: CBOR decode into each wire envelope, plus `validate()` on a successfully decoded agent envelope (the per-kind validity matrix is the most complex hand-written logic in the crate).

A crash here is a wire-contract bug: decoding untrusted input must always return a value or a `DecodeError`, never panic.
