# Rust BDD runner image for the shared-server docker-compose path. Build context
# is the repo root, so the laser-sdk / laser-wire path dependencies resolve.
FROM rust:1-slim AS build
WORKDIR /app
COPY . .
RUN cargo test --no-run --manifest-path bdd/rust/Cargo.toml

FROM rust:1-slim
COPY --from=build /app /app
WORKDIR /app/bdd/rust
# Run from the crate dir so the runner resolves ../scenarios.
CMD ["cargo", "test"]
