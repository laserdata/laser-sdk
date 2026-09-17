# Contributing

This project is pre-1.0. The SDK API, wire contract, and AGDX specification can change without backward compatibility. Update all affected clients and reference data together.

## Before you start

Read [AGENTS.md](AGENTS.md) for the repo-wide conventions and the module map. The [AGDX spec](docs/agdx.md) is the authoritative wire and convention reference.

## Building and testing

The [`justfile`](justfile) defines every gate. The full suite is:

```sh
just ci
```

The workflow formats code, sorts dependencies, finds unused dependencies, runs clippy, and builds the workspace. It then runs unit tests, native Iggy integration tests, doctests, WebAssembly checks, dependency checks, fuzzing, and shared behavior scenarios. Use `just lint`, `just test`, `just test-it`, and `just bdd` to run individual groups. The change is complete when the required `just ci` gates pass.

## Conventions

- Match the surrounding code. Terse comments, one sorted import block, no banner comments.
- Do not use semicolons or em dashes in prose, comments, error text, `must_use` strings, or commit messages.
- The streaming unit is a message (the spec calls it a record), not an "event". "event" names only a specific AGDX envelope kind or a named domain.
- Tests are named in given / when / then / should form and use `.expect("message")`, never a bare `unwrap`.
- Docs are part of every change. When the code or the wire contract changes, update the affected README, the spec, and the relevant guide in the same change.
- Keep the wire crate independent of I/O, clocks, randomness, and asynchronous runtimes. Its optional HTTP client uses a caller-supplied transport.

## Wire changes

The wire contract is pinned by a golden fixture corpus and a cross-language conformance suite. Regenerate the corpus only on an intentional wire change with `just fixtures-regen`, and review the diff.

## Publishing

TypeScript and Rust use trusted publishing. The [Rust workflow](.github/workflows/ci-rust.yml) and [TypeScript workflow](.github/workflows/ci-typescript.yml) define release tags, registry authentication, and publication gates.

## License

By contributing you agree that your contributions are licensed under the Apache-2.0 license of this repository.
