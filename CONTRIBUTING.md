# Contributing

Thanks for your interest. This project is pre-1.0, and the wire contract, the AGDX spec, and the public API may change in any release.

## Before you start

Read [AGENTS.md](AGENTS.md) for the repo-wide conventions and the module map. The [AGDX spec](docs/agdx.md) is the authoritative wire and convention reference.

## Building and testing

The [`justfile`](justfile) defines every gate. The full suite is:

```sh
just ci
```

It runs, in order: format check, dependency sort check, unused-dependency check, clippy with warnings denied, the workspace build, the unit and integration tests (integration needs Docker), the doctests, the wasm32 check on the wire crate, the dependency-ban and advisory gates, the decode fuzz targets, and the cross-SDK BDD scenarios. Run the pieces individually with `just lint`, `just test`, `just test-it`, and `just bdd`. A change is not done until `just ci` passes.

## Conventions

- Match the surrounding code. Terse comments, one sorted import block, no banner comments.
- Prose (docs, comments, error and `must_use` strings, commit messages) uses no semicolons and no em-dashes. Use a period, a comma, or a rewrite. The only exception is the literal "TL;DR".
- The streaming unit is a message (the spec calls it a record), not an "event". "event" names only a specific AGDX envelope kind or a named domain.
- Tests are named in given / when / then / should form and use `.expect("message")`, never a bare `unwrap`.
- Docs are part of every change. When the code or the wire contract changes, update the affected README, the spec, and the relevant guide in the same change.
- The wire crate stays runtime-free and wasm-portable. No IO, async, clock, or randomness there.

## Wire changes

The wire contract is pinned by a golden fixture corpus and a cross-language conformance suite. Regenerate the corpus only on an intentional wire change with `just fixtures-regen`, and review the diff.

## License

By contributing you agree that your contributions are licensed under the Apache-2.0 license of this repository.
