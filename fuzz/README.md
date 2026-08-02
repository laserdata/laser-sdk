# Wire decode fuzzing

Continuous fuzzing of the laser-wire decode surface, the only place hostile bytes reach the crate. It is detached from the main workspace because it needs a nightly toolchain and the `cargo-fuzz` binary.

The same entry points are also covered deterministically by the in-tree `cargo test` robustness suite (`wire/tests/robustness.rs`), which runs on stable with no extra tooling. This crate is for going deeper: long campaigns, coverage guidance, and crash-corpus minimization.

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
