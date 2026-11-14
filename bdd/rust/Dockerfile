# Rust BDD runner image. Build context is the repo root, so the laser-sdk and
# laser-wire path dependencies resolve.
FROM rust:1.100.0-slim AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo test --locked --no-run --manifest-path bdd/rust/Cargo.toml

FROM rust:1.100.0-slim
# libhwloc15 is a runtime dependency of the native Apache Iggy server the runner starts.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates libhwloc15 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app /app
WORKDIR /app/bdd/rust
# Run from the crate dir so the runner resolves ../scenarios.
CMD ["cargo", "test", "--locked"]
