alias b := build
alias t := test
alias l := lint

default:
  @just --list

build:
  cargo build --workspace --all-targets

fmt:
  cargo fmt --all

sort:
  cargo sort --workspace

# unused-dependency check
machete:
  cargo machete

# fmt + sort + machete + clippy, in the order CI enforces
lint: fmt sort machete lint-detached
  cargo clippy --workspace --all-targets --all-features -- -D warnings

# the same gates for the crates outside the workspace (bdd/rust, fuzz), which
# `--workspace` does not reach. Apply mode, to mirror `lint`.
lint-detached:
  cd bdd/rust && cargo fmt --all && cargo sort && cargo machete && cargo clippy --all-targets -- -D warnings
  cd fuzz && cargo fmt --all && cargo sort && cargo machete && cargo clippy -- -D warnings

test: test-doc
  cargo test --workspace

# doctests with every feature on (Docker-free). clippy --all-targets and a
# default-feature `cargo test` both skip feature-gated `///` examples, so this
# is the only gate that compiles them.
test-doc:
  cargo test --workspace --all-features --doc

# integration tests against a shared Apache Iggy testcontainer (needs Docker)
test-it:
  cargo test -p laser-sdk --features "integration a2a-bridge kv query"

# cross-SDK BDD conformance scenarios, Rust reference runner (needs Docker).
# Pass --managed to also run the @managed scenarios against a managed backend.
bdd *ARGS:
  ./scripts/run-bdd-tests.sh rust {{ARGS}}

# the wire crate must compile for wasm32 (it is runtime-free and wasm-portable).
# Deliberately omits `bson` (native-only by design).
wasm:
  cargo check -p laser-wire --target wasm32-unknown-unknown \
    --no-default-features --features cbor,codecs,fixtures,builders,http-client

# dependency policy for the wire crate's portable surface (bans iggy, tokio,
# bytes, ulid, dashmap, tracing, getrandom)
deny-wire:
  cargo deny --manifest-path wire/Cargo.toml --target wasm32-unknown-unknown \
    --no-default-features --features cbor,codecs,fixtures,builders,http-client \
    check --config deny-wire.toml bans

# workspace vulnerability / unmaintained-crate advisories (needs cargo-deny)
advisories:
  cargo deny check advisories

# fuzz the wire decode surface (needs nightly + `cargo install cargo-fuzz`)
fuzz TARGET="frame_decode":
  cargo +nightly fuzz run {{TARGET}}

# regenerate the golden corpus after an intentional wire change
fixtures-regen:
  AGDX_WIRE_FIXTURES_REGEN=1 cargo test -p laser-wire --test wire_fixtures \
    --features fixtures

# start / stop the local Apache Iggy message streaming the examples talk to
up:
  docker compose up -d

down:
  docker compose down

down-clean:
  docker compose down -v

example NAME:
  cargo run --example {{NAME}}

# the full gate set CI runs, in the global Rust verification order. Needs Docker
# (tests + bdd), the wasm32 target, cargo-deny, cargo-machete, and for fuzz a
# nightly toolchain + cargo-fuzz.
ci:
  cargo fmt --all --check
  cargo sort --workspace --check
  cargo machete
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cd bdd/rust && cargo fmt --all --check && cargo sort --check && cargo machete && cargo clippy --all-targets -- -D warnings
  cd fuzz && cargo fmt --all --check && cargo sort --check && cargo machete && cargo clippy -- -D warnings
  cargo build --workspace --all-targets --all-features
  cargo test --workspace --all-features
  cargo test --workspace --all-features --doc
  just wasm
  just deny-wire
  just advisories
  cd fuzz && cargo +nightly fuzz run frame_decode -- -max_total_time=30 && cargo +nightly fuzz run decode_envelope -- -max_total_time=30
  just bdd
