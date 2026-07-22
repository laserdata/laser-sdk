---
name: typescript-sdk
description: Work on the native TypeScript Laser SDK, its wire fixture port, Apache Iggy adapter, Node runtime, package exports, tests, BDD, examples, CI, and npm release gates under foreign/typescript, bdd/typescript, and examples/typescript.
---

# TypeScript SDK

Read [AGENTS.md](../../../AGENTS.md) and
[laser-sdk-overview](../laser-sdk-overview/SKILL.md) first. Rust `laser-wire`
types and fixtures remain authoritative. Never invent a TypeScript-only wire
shape.

## Layout

- `foreign/typescript/src/wire`: native codecs and validators
- `foreign/typescript/src/iggy/apache-iggy.ts`: the only Apache Iggy and Node
  `Buffer` adaptation boundary
- `foreign/typescript/src/stream`, `managed`, `agent`, `memory`, `bridges`:
  public behavior by layer
- `foreign/typescript/test`: unit, wire, robustness, and real-Iggy integration
- `bdd/typescript`: every shared Gherkin scenario, no copied features
- `examples/typescript`: nine non-benchmark mirrors

Public bytes are `Uint8Array`. Wire-sized u64 and u128 values are `bigint`.
Public JSON is `unknown` until validated. Source is strict ESM, semicolon-free,
has no public `any`, and uses no default exports. Managed operations negotiate
capabilities and return `UnsupportedError` on stock Apache Iggy.

## Exports

- root: ordinary application API
- `./full`: root plus the complete wire namespace
- `./testing`: deterministic seams and factories
- `./opentelemetry`: optional observer adapter

Do not add deep package exports. Review generated API reports after every
public change.

## Verification

From `foreign/typescript` run:

```sh
npm run verify
npm run test:integration
```

Then run `scripts/run-bdd-tests.sh typescript` and the example package tests
against Apache Iggy. `verify` includes style, format, lint, emitted dependency
cycles, strict types, builds, API reports, fixture and robustness tests,
coverage, licenses, and packed ESM/CommonJS-interoperating consumers.

Node 22.14 and Node 24 are supported. Bun, Deno, and browsers are unsupported
until their transport and complete gates pass. Release tags use `ts-v*` and
publish the exact CI-produced tarball through npm OIDC.
