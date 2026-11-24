# Go SDK Implementation Status

**This file is the single source of truth for `foreign/go`'s progress toward
parity with the Rust, Python, and TypeScript SDKs.** `AGENTS.md`, `CLAUDE.md`,
`docs/parity.md`, and `foreign/go/README.md` link here rather than restating
this inventory; update this file first when something changes, then fix any
of those that now disagree with it.

Every line in the capability matrix was checked against `foreign/go/laser`
source on the date below, not copied from an earlier audit or from docs. Where
a capability is missing, the check was "grep for the Go name, read the file if
found" — not "the docs say it exists."

- **Last verified**: 2026-11-24, branch `feat/go-wire-contract`
- **Go module**: `github.com/laserdata/laser-sdk/foreign/go`, `go 1.25.0`
- **Baseline test run** (start of this session, before any change):
  `gofmt -l .` clean, `go vet ./...` clean, `go build ./...` clean,
  `go test ./... -count=1` **all green** (wire, laser, bridges,
  internal/cborform), `go test ./... -race -count=1` all green.
  Test counts: `wire` 255, `laser` 1184, `bridges` 74, `internal/cborform` 11
  (1524 total, via `go test ./<pkg>/... -list '.*'`).
- **Current test run** (after this session's Agent lifecycle + ConcurrencyPolicy
  + WarmDedupPolicy + submitted-session pickup + `Session.Assemble()` +
  `submit_in`/`call_tool_in` + `Contract`/`ContractBuilder` +
  `Scatter`/`ScatterReport` + workflow engine slice 1 work): same four gates
  all green, plus `go test ./... -race -count=1` green. Test counts: `wire`
  255, `laser` 1239 (+55 over the session start: 10 `agent_spawn_test.go`,
  7 `agent_concurrency_test.go`, 7 WarmDedupPolicy across
  `agent_warm_dedup_test.go`/`agent_warm_dedup_internal_test.go`, 3
  `agent_submitted_pickup_test.go`, 4 `session_assemble_test.go`, 9
  `agent_contract_test.go`, 7 `agent_scatter_test.go`, 8
  `agent_workflow_test.go`), `bridges` 79 (+5:
  `TestGivenAncestryWhenSubmittedAsAChildThenShouldWriteTheStartBeforeTheCommand`,
  `TestGivenATargetWhenSubmittedInAChildThenShouldAddressOnlyThatAgent`,
  `TestGivenAncestryWhenCalledAsAChildThenShouldEndItCompletedOnSuccess`,
  `TestGivenAToolErrorWhenCalledAsAChildThenShouldEndItFailed`,
  `TestGivenATargetWhenCalledAsAChildThenShouldAddressOnlyThatAgent`),
  `internal/cborform` 11 (1584 total). `laser`'s `Session.WithAncestry`/
  `Sessions.OpenChild` plumbing (added for `submit_in`/`call_tool_in`) has no
  dedicated `laser` test of its own beyond what `Contract`'s own `Parent`
  tests, and now the workflow engine's own directed/all-capable step tests,
  also exercise end to end — still no isolated unit test of `OpenChild` in
  the abstract, which remains a judged-acceptable gap per the earlier
  session's reasoning.

## Completion summary

Two different things are being measured, and they move at very different
speeds:

| Dimension | Estimate | Basis |
| --- | --- | --- |
| Raw Apache Iggy capability coverage | **57 / 59 (~97%)** | `docs/parity.md`'s Go section, cross-checked against `laser/*.go` entry points this session. The two gaps (`producer.idempotent`, `producer.sharding`) are architecturally blocked on the Apache Iggy Go SDK, not Go-client work; see "Explicitly out of scope" below. |
| Agent orchestration layer (gates, runtime, sessions, bridges, contract, scatter, workflow) | **~81–85%**, unevenly | See the matrix below. The wire-adjacent pieces (gates, reliable consumer core, sessions, bridges, registry/routing, memory/context, governance) are solid, and the agent lifecycle/builder, `ConcurrencyPolicy`, `WarmDedupPolicy`, submitted-session pickup, `Session.Assemble()`, `submit_in`/`call_tool_in`, `Contract`/`ContractBuilder`, `Scatter`/`ScatterReport`, the workflow engine's declared-steps DAG (dependency-ordered steps, `step_local`, budgets, journal/replay/resume), and now its saga compensation (`CompensateWith`) all shipped across this session and the one before it. No whole subsystem is entirely missing anymore, but the workflow engine itself is not whole: exclusive/fenced steps and the "plain code" `run_with` form are still unbuilt. Weighted by the effort those two still take rather than by item count, this number is probably closer to 70–75%: the "plain code" form alone, per this session's own scoping read of `sdk/src/agent/workflow.rs`, is the single hardest remaining piece in this whole file. |
| BDD conformance coverage | **15 / 34 feature files (~44%)**, and **0% verified against a live server** | `bdd/go` has a runner for 15 of the 34 shared `bdd/scenarios/*.feature` files. Every one of them, including the ones with a runner, skips without an externally supplied `LASER_BDD_ADDR`/`LASER_CONNECTION_STRING`, and `ci-go.yml` never sets one — see "Verification gap" below. |
| Example coverage | **3 / ~21 per-scenario examples** | `examples/go/` has `agent`, `kv`, `memory`. Rust and Python each carry the full set (`log`, `query`, `watch`, `cdc`, `graph`, `recall`, `context`, `workflow`, `orchestra`, `interop`, `governance`, ...). |

There is no single honest "X% done" number across all four of these; quoting
one invites exactly the overstatement this file exists to correct. Read the
matrix.

## Verification gap (read this before trusting any "done" below)

**Nothing in `bdd/go` has ever run against a live Apache Iggy server, in this
repository's history, in CI or locally.** `ci-go.yml`'s `quality` job runs
`go test ./...` in `bdd/go` with no `LASER_BDD_ADDR` set, so `deployment(t)`
(`bdd/go/harness_test.go`) skips every scenario. `scripts/run-bdd-tests.sh go`
does the same. Unlike `bdd/rust`, which bootstraps its own native Iggy test
process the way `sdk/tests/integration/` does, nothing wires an equivalent
test server into `bdd/go` or its CI job yet.

This means every "scenario X passes" claim anywhere in this repo's Go history
(including scenarios closed in the sessions before this one) means "builds,
vets, and the step-by-step logic was traced by hand against the parsed
feature file" — not "ran for real." Treat anything below marked **Built,
unverified live** accordingly. Wiring `scripts/resolve-test-iggy-server.sh`
(already language-agnostic) into `ci-go.yml` and a local dev path is the
single highest-leverage piece of unstarted work in this whole file, because it
would retroactively upgrade the confidence level of everything else.

## Capability matrix

Status legend: **Done** (implemented, exercised by `laser`'s own unit suite),
**Built, unverified live** (implemented and unit-tested against fakes, but
the BDD scenario that would prove it against a real deployment has never run
one), **Partial** (some of the behavior exists, named gaps follow),
**Missing** (nothing exists), **Blocked** (not implementable against the
current Apache Iggy Go SDK).

### Streaming, managed surfaces, wire contract

| Capability | Status | Go source | Notes |
| --- | --- | --- | --- |
| Wire contract (codes, headers, envelopes, queries, filters, sessions, HTTP) | Done | `foreign/go/wire/` | `wire/parity_test.go` fails CI if a Rust public type has no Go peer. |
| Connect, TLS, capability negotiation | Done | `laser/connect.go` | |
| Managed command band (`SendManaged`) | Done | `laser/laser.go` (grep `SendManaged`) | |
| KV, Query, Fork, Graph, RBAC, Projections, Schemas, Filters+catalog | Done | `laser/kv.go`, `query.go`, `fork.go`, `graph.go`, `rbac.go`, `projections.go`, `filter_catalog.go` | Confirmed present this session; conformance scenarios for several of these are untested (see BDD gap table). |
| KV lease/fence client (`Lease`/`TryLease`/`RenewLease`/`ReleaseLease`/`LeaseGuard`/`CasFenced`) | **Done** | `foreign/go/laser/kv_lease.go` | New this session. The wire contract (`wire.KvLease`/`KvLeaseRenew`/`KvRelease`/`KvCasFenced`, the `KvLeased`/`KvRenewed`/`KvReleased`/`KvCommitted` outcomes, `CapabilityKvFencedLeases`) was already fully ported before this session touched it — only the `laser`-level client wrapper was missing. Uses the plain `Kv.execute` round trip every other kv operation already uses; does not port Rust's ambiguous-mutation retry (a dedicated "coordination client" with operation-identity dedup and a resolving read), since no Go kv operation has that yet, lease included — see "Completed this session". |
| Materialization | Done | `laser/materialize.go` | |
| Memory (log-backed, vector) | Done | `laser/memory.go` | Conformance untested (`memory.feature`, `memory_correction.feature`). |
| Watch / change feed | Done | `laser/watch.go` | |
| Producer idempotence | **Blocked** | — | Passes through to the Apache Iggy *Rust* SDK's high-level producer, which the Go Iggy SDK does not expose. No `CAPABILITY_SIGNOFFS` file exists yet to formally retire this row; per this session's direction, out of scope to chase further. |
| Producer sharding | **Blocked** | — | Same reason. |

### Agent gates and the reliable consumer core

| Capability | Status | Go source | Notes |
| --- | --- | --- | --- |
| `Agent` (dedup/fence/deadline/classify gates) | Done | `laser/agent.go`, `gates.go` | |
| `AgentRuntime.Run` (reliable consumer: retry→DLQ, middleware, ack-on-pickup, budget gate, graceful drain) | Done | `laser/agent_runtime.go` (~1200 lines, largest file in the package) | 100+ existing unit tests. Not touched this session — too large and too well-tested to risk modifying without a live-server re-check. |
| `ConcurrencyPolicy` (Serial / SerialPerPartition) | **Done** | `laser/agent_concurrency.go` (new this session), wired into `laser/agent_runtime.go`'s `RuntimeOptions.Concurrency` and the top of `Run` | `SerialConcurrency()` (zero value, unchanged behavior) and `SerialPerPartitionConcurrency(maxPartitions)`. See "Completed this session" for the lane-scheduler design, the idle-lane retirement bound, and a hang bug found and fixed while testing it. |
| `WarmDedupPolicy` (refill dedup window from dead letters/unexpired/all on start) | **Done** | `laser/agent_warm_dedup.go` (new this session), wired into `laser/agent_runtime.go`'s `RuntimeOptions.Group`/`WarmDedup`/`WarmDedupPolicy` and the top of `Run` | `WarmDedupDeadLetters` (default), `WarmDedupUnexpired`, `WarmDedupAll`. Go's `Deduplicator` (`laser/gates.go`) is still a concrete struct, not Rust's pluggable `Box<dyn Deduplicator>` — this refills it via its existing `Accept`, not a new backend seam. See "Completed this session" for the design and the Group-field refactor it required. |
| Submitted-session pickup marking | **Done** | `laser/agent_runtime.go`'s `pickUpSubmitted`, called from `deliver` | Reuses `Session.Progress` and `Laser.LeaseSession` rather than new infrastructure. See "Completed this session" for the one deliberate simplification (lease interval) versus Rust. |
| `AgentMiddleware`, `DeadLetterSink`, `RetryPolicy`, `BudgetGate`, `ControlBook` | Done | `laser/agent_runtime.go`, `budget.go`, `agent_control.go` | |

### Agent lifecycle / builder (this session's implementation)

| Capability | Status | Go source | Notes |
| --- | --- | --- | --- |
| Reusable construction, startup, readiness, shutdown, join, abort | **Done** | `laser/agent_spawn.go` (`SpawnOptions`, `AgentHandle`, `Laser.SpawnAgent`) | New this session. See "Completed this session" below for exact scope and limits. |
| Self-advertised capability card / presence on spawn | **Missing** | — | Rust's `Agent<H>.capabilities` publishes a card and advertises presence on `spawn`; `withdraw` clears it on stop. `SpawnOptions` carries no equivalent field. |
| Governor-on-spawn (`.governor(...)` re-scoping the agent's `Laser`) | **Missing** | — | A caller must call `Laser.WithGovernor` themselves before `SpawnAgent`; there is no per-agent override. |
| Background consolidation tick (`consolidate_every`/`consolidator`) | **Missing** | — | Not attempted this session; lower priority than the items above per the priority order given. |
| Signing key / verifier wiring at spawn | **Partial** | `laser/sign.go` exists independently | Not threaded through `SpawnOptions`; a caller wires it onto the `Laser` or `AgentCtx` separately today. |

### Session management

| Capability | Status | Go source | Notes |
| --- | --- | --- | --- |
| Session lifecycle (start/resume/end/cancel/fail), terminal latch, checkpoints, state (set/patch/replace/snapshot), model/tool call recording | Done | `laser/session.go`, `session_ops.rs`-equivalent split across `session_calls.go`, `session_state.go` | Exercised by `bdd/go/session_test.go` + satellites (built, unverified live — see gap above). |
| Declared layouts (shared / per-agent-topic / per-agent-partition / single-partition) | Done | `laser/layout.go` | |
| Session leases and heartbeats | Done, but **not bundled with `Begin`** | `laser/lease.go`, `session.go` | Rust's `.begin()` returns `(session, lease)` together; Go's `Session.Begin` returns no lease at all — a caller must separately call `Laser.LeaseSession` to get heartbeats. Intentional-looking API split, but it means heartbeats do not "just work" the way they do in the other three SDKs unless the caller remembers the extra step. Not changed this session (would be a breaking signature change to `Session.Begin`, explicitly out of scope per "preserve existing APIs"). |
| `Session.Assemble()` (build a context manifest from the session's own records) | **Done** | `laser/session_assemble.go` | Composes existing pieces (`Laser.Context(...).FetchWith`, `wire.ContextManifest`/`Fragment`/`FrontierPosition`, `wire.EstimateTokens`) rather than new fetch logic — see "Completed this session". `PolicyVersion`/`Correlation` are left at their zero values since Go's `ContextPolicy` has no `Version()` and there is no natural correlation id for a generic assemble. |
| Operator control (pause/resume/cancel/force-cancel), `PendingControl` | Done | `laser/agent_control.go`, `session_control.go`, `pause.go`(?) | |
| Session budgets / `BudgetGate` | Done | `laser/budget.go` | |

### Multi-agent orchestration (caller-side)

| Capability | Status | Go source | Notes |
| --- | --- | --- | --- |
| Registry, presence, capability routing (`Router`, `ToCapable`, quarantine) | Done | `laser/registry.go`, `router.go` | |
| `ReplyHub` / `Requester` (single-target request/reply, correlation) | Done | `laser/replies.go`, `request.go` | |
| `AgentCtx.FanOut` (handler-side fan-out under a `GatherPolicy`) | Done | `laser/agent_ctx.go:468` | Handler-side only. |
| `Contract` / `ContractBuilder` (caller-side: dispatch one task, await a typed `Completed`/`Failed`/`NotConsumed`/`TimedOut` outcome with a deadline) | **Done** | `foreign/go/laser/agent_contract.go` | Drives its own live poll of the reply topic rather than requiring a caller already running a consumer loop and feeding a `ReplyHub`; no separate per-agent inbox topic (Go's registry carries no such address — command and reply share the one `topic` named at `Laser.Contract`). `Correlation`/`IdempotencyKey`/`ResumeFrom` added this session for the workflow engine's own dispatch/resume (see its row below); `excluding` (exclusive-step reassignment only) still not ported, since nothing outside the still-missing exclusive-step path needs it. |
| `Scatter` / `ScatterReport` (caller-side scatter-gather across many agents under a `GatherPolicy`) | **Done** | `foreign/go/laser/agent_scatter.go` | New this session. Reuses `Contract` per branch and the same gather-loop shape `AgentCtx.FanOut` already has (quorum/best-effort/cancellation), rather than a new `Gathered<T>`-style generic: see "Completed this session". |
| `ApprovalGate` / `ApprovalGrant` | Done | `laser/approval.go` | |
| Workflow engine: dependency-ordered steps, `step_local`, budgets, journal/replay/resume | **Done** | `foreign/go/laser/agent_workflow.go`, `agent_workflow_journal.go` | New this session (slice 1 of a multi-session port; see "Completed this session"). Dispatches directed steps through `Contract` (now carrying `Correlation`/`IdempotencyKey`/`ResumeFrom` for this) and all-capable steps through `Scatter`. The journal's own CBOR encoding is Go-native, not laser-wire-pinned, so a Go-started run resumes only on Go, not cross-SDK (see the row's own file header comment). |
| Workflow engine: exclusive (fenced, at-most-once) steps, `OnTimeout(Reassign)` | **Missing** | — | Needs Go's `kv` package's lease/fence primitives threaded through, plus the renew-while-dispatching loop (`sdk/src/agent/workflow.rs`'s `exclusive_attempt`). Not attempted this session; deferred per the scoping discussion before slice 1 started. |
| Workflow engine: saga compensation (`CompensateWith`) | **Done** | `foreign/go/laser/agent_workflow.go` (`compensate`/`runCompensation`/`compensationRoute`), `agent_workflow_journal.go` (`journalStepCompensated`) | New this session (slice 2). Reuses `Contract` again for a directed/all-capable step's rollback dispatch, addressed to the exact agent(s) that ran the original step rather than a fresh capability resolution (`compensationRoute` is a direct `To(agent)` — Go's `Router` has no `ToPrincipal` variant to preserve, unlike Rust's `compensation_route`). A local step's compensation runs in process when it builds; nothing is dispatched. |
| Workflow engine: the "plain code" form (`run_with`/`WorkflowRun`, `begin_step`/`complete_step`, concurrent call-digesting) | **Missing** | — | The single hardest remaining piece per the scoping read of `sdk/src/agent/workflow.rs`: a materially different journaling unit and concurrency model from the declared-steps form, not a refinement of it. |

### Bridges (A2A / MCP / AG-UI)

| Capability | Status | Go source | Notes |
| --- | --- | --- | --- |
| A2A task adapter (submit/task/cancel/card), task binding, HTTP router | Done | `foreign/go/bridges/a2a_bridge.go`, `a2a_rpc.go`, `a2a_http.go`, `a2a_binding.go` | Shipped two sessions before this one. |
| MCP tool-call adapter, resources/prompts, HTTP router | Done | `foreign/go/bridges/mcp_bridge.go`, `mcp_rpc.go`, `mcp_http.go`, `mcp_wait.go` | Shipped last session. `mcp_wait.go`'s live poll is a hand-rolled stand-in for exactly the `Requester`/`Contract` gap above. |
| `AgUiEvents` (frame a conversation's records into AG-UI runs) | Done | `foreign/go/bridges/agui_events.go` | Shipped last session; has real passing unit tests against a fake log (not server-dependent). |
| `submit_in` / `call_tool_in` (child-session task submission for A2A/MCP) | **Done** | `foreign/go/bridges/a2a_bridge.go` (`SubmitIn`/`SubmitInTo`), `foreign/go/bridges/mcp_bridge.go` (`CallToolIn`/`CallToolInTo`) | New this session, on top of a new `laser/session.go` `Session.WithAncestry` and `laser/session_submit.go` `Sessions.OpenChild` (see "Completed this session"). `A2ACaller` (task-bound callers) still has no `SubmitIn`/`SubmitInTo` — it was already narrower than Rust's `A2aCaller` before this session (missing `submit_to`/`submit_with_hops` too), and closing that gap is unstarted, separate work. |
| Bridge hop-loop guard | Done | `foreign/go/bridges/hops.go` | |

## BDD conformance coverage by feature file

15 of 34 shared scenario files have a Go runner. All skip without a live
server (see "Verification gap" above).

**Have a runner** (`bdd/go/*_test.go`): `agent`, `agent_must_understand`
(inside `agent_test.go`), `bindings`, `bridges`, `capabilities`,
`capabilities_injected`, `defaults`, `filters`, `filters_live`, `governance`,
`materialization`, `provenance` (+ `agent.feature`, shared with
`platform_agent_test.go`), `schema_codecs`, `scratch_stream`, `session`,
`stream_admin`, `streaming`.

**No runner at all** (capability exists in `laser/`, conformance untested):
`clients`, `consistency`, `context_window`, `graph`, `kv_cas`, `kv_leases`,
`memory`, `memory_correction`, `operational_sql`, `query`, `query_managed`,
`schema_registry`, `warm_dedup`, and now `workflow`/`workflow_local`: the
declared-steps DAG, journal/replay/resume, `step_local`, and (as of this
session) `CompensateWith` all exist, so nothing in either feature file's
capability is missing anymore — only a Go BDD runner to prove it against
either a fake or (per the verification gap above) a live server.

**No runner, and cannot have one yet because the capability is missing**:
`managed_sessions` (needs the managed-backend session surface the `@managed`
tag gates, not separately audited this session); `workflow_function` (needs
the workflow engine's still-missing "plain code" form).

## Explicitly out of scope (by direction, not oversight)

- `producer.idempotent` / `producer.sharding` — architecturally blocked on
  the Apache Iggy Go SDK, which has neither the producer-id protocol nor a
  background sharded sender to pass through to. No `CAPABILITY_SIGNOFFS`
  entry exists; per this session's explicit direction, not pursued further.
- QUIC and WebSocket transports — the Apache Iggy Go SDK implements TCP only;
  this stays with Rust and Python by construction, not by Go-client choice.
- Joining the three-way parity table (`docs/parity.md`'s Rust/Python/TS
  columns) as a fourth column — deferred by design until the surface above is
  complete enough that a gap reads as a Go gap rather than a parity-table
  artifact.

## Completed this session

**`laser/agent_spawn.go`** (new, 180 lines) — `SpawnOptions` (embeds the
existing `AgentOptions` and `RuntimeOptions` rather than inventing a parallel
builder type, following the plain-struct-literal convention the rest of the
package already uses for configuration), `AgentHandle`
(`Identity`/`Ready`/`Join`/`Shutdown`/`Abort`), `Laser.SpawnAgent`, and the
internal `startAgentHandle` the handle's lifecycle logic lives in (exported to
the test package only via `laser/export_test.go`, an established Go idiom for
testing unexported internals with the external test package's richer
fixtures, not a new public symbol).

Design decisions worth knowing before extending this:

- **The consumer-group join is synchronous inside `SpawnAgent`.**
  `Topic.ConsumerGroup` already performs the join before returning, so there
  is no cheaper point at which to fail fast on a bad topic or a refused join;
  a caller never holds a handle whose group membership has not already landed.
- **`Ready()` cannot distinguish "the loop started" from "the loop started and
  immediately failed."** Both close the same channel from a caller's
  perspective, and `AgentRuntime.Run` offers no earlier internal hook. Fixed
  this session (see "Known issue found and fixed" below) so that `Ready()`
  called *after* the run has already finished deterministically reports the
  real result rather than racing Go's random `select` pick between two
  already-closed channels — but a `Ready()` called *while* a same-instant
  failure is in flight can still legitimately return `nil`; `Join()` is the
  authoritative call for "did it actually work."
- **`Abort()` cannot hard-kill a goroutine.** Unlike Rust's `tokio` task
  abort, Go has no forced-termination primitive for a running goroutine.
  `Abort()` cancels the same context `Shutdown()` does and returns without
  waiting; the consumer loop still runs its own shutdown grace internally.
  This is stated in the doc comment rather than silently approximated.
- **Card/capability self-advertisement, governor re-scoping, and the
  consolidation tick are not included.** `RuntimeOptions`/`AgentOptions` do
  not carry fields for them, and adding them means extending those existing
  structs (low risk) or `AgentRuntime` itself (higher risk, large file). Left
  for a follow-up with its own session budget.

### Known issue found and fixed during this session's own test-writing

`Ready()`'s first draft closed a `ready` channel unconditionally before
calling `AgentRuntime.Run`, then raced it against a `done` channel in a plain
three-case `select`. A test proving "a source that fails before ever joining
still reports the right outcome" caught two real bugs this surfaced:

1. Calling `Ready()` on an *already-stopped* handle could nondeterministically
   return `nil` instead of the real error, because Go's `select` picks
   uniformly at random among multiple ready cases, and by the time the agent
   has stopped, both `ready` and `done` are closed. **Fixed** by checking
   `done` non-blocking first, before the three-way select, so an
   already-finished run is reported deterministically.
2. The test itself was wrong before the fix revealed it: a plain `errors.New`
   from a fake source is `RetryableError(err) == true` by
   `agent_runtime.go`'s own default (`laser/agent_runtime.go:1148`), so
   `AgentRuntime.Run` retries it inside the consumer's failure budget forever
   rather than returning — the test's first version timed out instead of
   failing cleanly, which is how the race above was caught in the first place.
   Fixed by using a `*laser.InvalidError`, which `RetryableError` explicitly
   returns `false` for.

Both fixes are in the committed (well, not-yet-committed — see below) code
and covered by
`TestGivenASourceThatFailsAtOnceWhenJoinedThenShouldReportItsError` and
`TestGivenAStoppedAgentWhenReadyIsCalledAgainThenShouldReportTheSameOutcome`
in `laser/agent_spawn_test.go`.

### Tests added this session

`laser/agent_spawn_test.go` (new, package `laser_test`, reuses existing
fakes `runtimeServer`/`queuedSource`/`handling`/`delivered`/`agentID`/
`correlationID` from `agent_runtime_test.go` rather than duplicating them):

| Test | Proves |
| --- | --- |
| `TestGivenAHandleWhenTheLoopStartsThenShouldReportReady` | `Ready()` returns once the loop is running; `Identity()` reports the spawned id. |
| `TestGivenARunningAgentWhenShutdownThenShouldStopAndJoinCleanly` | `Shutdown` cancels and joins without error on a clean run. |
| `TestGivenAStoppedAgentWhenReadyIsCalledAgainThenShouldReportTheSameOutcome` | `Ready()` after the agent already stopped is deterministic, not a race (see fix #1 above). |
| `TestGivenAHandlerThatKeepsItsRecordWhenShutdownThenShouldStillStopCleanly` | A handler that refuses still lets `Shutdown` complete cleanly (the kept record stays uncommitted on the source, which is `AgentRuntime.Run`'s own existing, separately-tested behavior). |
| `TestGivenASourceThatFailsAtOnceWhenJoinedThenShouldReportItsError` | A non-retryable source failure is reported correctly by both `Join` and `Ready` (see fix #2 above). |
| `TestGivenAnAbortWhenCalledThenShouldNotBlockTheCaller` | `Abort()` returns immediately; a subsequent `Join()` still completes cleanly. |
| `TestGivenNoIdentityWhenSpawnedThenShouldRefuseBeforeJoiningAnything` | Validation order: identity checked before any network call. |
| `TestGivenNoHandlerWhenSpawnedThenShouldRefuseBeforeJoiningAnything` | Same, for a nil handler. |
| `TestGivenNoTopicWhenSpawnedThenShouldRefuseBeforeJoiningAnything` | Same, for an empty topic. |
| `TestGivenNoDefaultStreamWhenSpawnedThenShouldRefuseBeforeJoiningAnything` | Same, for no resolvable stream. |

**Not tested, by deliberate scope decision**: the actual `Topic.ConsumerGroup`
join path inside `SpawnAgent` (the part after all four validation checks
pass). `foreign/go/laser/consumer_group_live_test.go` (`//go:build
integration`) documents the project's own standing position that consumer
group behavior is "the surface least worth believing without a server... a
stub can be told to return any of that, so a hermetic test proves only that
the client asked." Rather than write a low-value stub-proves-nothing test, or
skip verification silently, this is named here as a live-server gap exactly
like the BDD scenarios above: `SpawnAgent`'s happy path has not run against a
real deployment.

### Verification run after implementation

```
gofmt -l .                              # clean
go vet ./...                            # clean
go build ./...                          # clean
go test ./... -count=1                  # all packages green (wire, laser, bridges, internal/cborform)
go test ./laser/... -race -count=1 \
  -run <the ten new test names>         # all green
go test ./... -race -count=1            # full module, all green, no regressions
```

No `bdd/go` or `examples/go` changes this session; neither was touched, so
their own `go test ./... -count=1` (all skip, per the verification gap above)
is unchanged from before this session.

**`laser/agent_concurrency.go`** (new, ~245 lines) — `ConcurrencyPolicy`
(`SerialConcurrency()`, the zero value; `SerialPerPartitionConcurrency(maxPartitions)`,
clamped to a minimum of 1), and `AgentRuntime.runPerPartition`, the
`SerialPerPartition` scheduler. Wired in by two small, additive edits to
`laser/agent_runtime.go`: a `Concurrency ConcurrencyPolicy` field on
`RuntimeOptions`, a `concurrency` field threaded through the constructor, and
one new branch at the very top of `Run` (`if runtime.concurrency.isSerialPerPartition() { return runtime.runPerPartition(...) }`)
before any of the existing serial-loop code — the serial path itself is
byte-for-byte unchanged.

Design, ported from `sdk/src/agent/consumer.rs`'s `run_per_partition` but in
idiomatic Go rather than translated Rust:

- One goroutine per open partition (`partitionLane`), each reading its own
  1-deep buffered queue and calling `deliver` strictly in order — ordering and
  the committed prefix hold per partition, same as Serial holds it overall.
- A dedicated `commitLoop` goroutine is the **sole** owner of `source.Commit`
  and of clearing a lane's `busy` flag. This was a deliberate redesign partway
  through, before any test was written: an earlier draft drained completions
  only in between `source.Next` calls on the main routing goroutine, which
  means a lane that finished while `Next` was blocked (a quiet topic, or
  — as the fatal-failure test below exposed — a source with nothing left to
  read) could not report its completion until the next poll returned,
  delaying its commit indefinitely. Splitting commit handling into its own
  goroutine removes that coupling entirely.
- `maxPartitions` bounds concurrency: a record for a new partition at the cap
  retires an idle lane (one with no record in flight) to make room, or waits
  on a `freed` signal if every lane is busy — never an unbounded
  goroutine-per-partition design.
- A handler or commit failure is reported once on a buffered `fatal` channel;
  `errCleanStop` is a sentinel (checked via `errors.Is`) distinguishing "the
  context ended while backing off a commit failure, stop cleanly" from a real
  fatal error, mirroring the serial loop's own `ctx.Err()` checks.
- Concurrency safety: `lanes` (the partition→lane map) is touched only by the
  routing goroutine, never by `commitLoop` — `laneCompletion` carries a
  `*partitionLane` pointer rather than a bare partition id specifically so
  the commit loop never needs to read that map. `partitionLane.busy` is the
  one piece of state both goroutines touch, so it alone carries a small
  `sync.Mutex`. `completions` is buffered to `maxPartitions`, which is
  provably sufficient since a lane never pulls its next queued record until
  its last completion has been sent. Shutdown closes every lane's queue and
  waits for each lane goroutine to fully exit *before* telling `commitLoop`
  to stop, and `commitLoop` drains whatever is left in `completions`
  non-blockingly after that signal, so no completion is ever lost to
  shutdown ordering.

### A hang found and fixed while testing `SerialPerPartitionConcurrency`

The first implementation checked the `fatal` channel via a **non-blocking**
`select`/`default` at the top of the routing loop, then called the
potentially-blocking `source.Next(ctx)` directly. This raced: nothing
guaranteed the routing loop would see a just-arrived `fatal` signal before
looping back into another `Next` call. `TestGivenAFatalHandlerFailureOnOneLaneWhenRunThenShouldStopTheWholeRun`,
using a one-record source and an uncancelled `context.Background()`, caught
it immediately as a real hang (confirmed with `go test -timeout 30s -v`, which
printed a goroutine dump showing the routing loop's second `Next` call parked
forever on `<-ctx.Done()` of a context with a nil `Done()` channel, while the
lane that had already failed sat idle, its completion already drained).

**Fix**: `source.Next` is now called from a single dedicated goroutine, paced
one call at a time by an `askNext` request channel, with its result delivered
on an unbuffered `nextOutcomes` channel. The routing loop's main loop is now a
single blocking three-way `select` (`fatal` / `ctx.Done()` / `nextOutcomes`)
with no non-blocking fallback branch, so a fatal signal can never be missed
behind a blocked read. Both of the Next-calling goroutine's channel operations
are themselves guarded by a `stopNext` signal closed in `runPerPartition`'s
own defer, so it can never leak blocked on a send nobody will read once the
function returns (the one remaining trust assumption — that `source.Next`
itself eventually honors context cancellation — is the same one the
pre-existing serial loop already makes).

Fixing the hang also surfaced that the test's own premise was wrong, not just
the implementation: `runtime.deliver` routes a handler's non-retryable error
through `runtime.handle` → `runtime.deadLetter`, which **returns `nil` on a
successful dead-letter write** — a refused record is kept and the run
carries on, exactly like `TestGivenARefusedRecordWhenRunThenShouldKeepItWithoutRetrying`
proves for Serial. `deliver` only returns a genuinely fatal error when the
dead letter itself cannot be published (see Serial's own
`TestGivenAnUnwritableDeadLetterWhenRunThenShouldLeaveTheRecordUncommitted`).
The per-partition test was rewritten to match that same setup
(`server.refuse, server.refuseAlways = wire.AgentDLQ, true`) and to run `Run`
in a goroutine against a bounded context with a `select`/timeout, rather than
assert on a specific error type from a scenario that was never actually fatal.

### Tests added this session (ConcurrencyPolicy)

`laser/agent_concurrency_test.go` (new, package `laser_test`):

| Test | Proves |
| --- | --- |
| `TestGivenConcurrencyIsUnsetWhenRunThenShouldStillBeSerial` | The zero-value `ConcurrencyPolicy` keeps today's exact serial behavior. |
| `TestGivenTwoPartitionsWhenRunUnderSerialPerPartitionThenShouldHandleThemConcurrently` | Two partitions run concurrently: a gated record on partition 0 does not block partition 1's record from being seen. |
| `TestGivenMaxPartitionsOneWhenRunThenShouldSerializeAcrossPartitionsToo` | Capped at one lane, the second partition's record waits for the first to be released — no concurrency beyond the cap. |
| `TestGivenOnePartitionWithManyRecordsWhenRunUnderSerialPerPartitionThenShouldKeepItsOwnOrder` | Within one partition, records are still handled and committed strictly in order. |
| `TestGivenAFatalHandlerFailureOnOneLaneWhenRunThenShouldStopTheWholeRun` | A dead-letter publish failure on one lane ends the whole run (see the hang/fix writeup above). |
| `TestGivenAPerPartitionAgentWhenShutdownThenShouldStopCleanly` | `AgentHandle.Shutdown` over a per-partition runtime stops and joins cleanly. |
| `TestGivenMaxPartitionsBelowOneWhenBuiltThenShouldClampToOne` | `SerialPerPartitionConcurrency(0)` clamps to 1, verified with the same proof shape as the explicit-1 test. |

### Verification run after ConcurrencyPolicy

```
gofmt -l .                                            # clean
go vet ./...                                          # clean
go build ./...                                        # clean
go test ./laser/... -run <the 7 new test names> -v    # all green (one hang found and fixed first)
go test ./laser/... -race -run <the 7 new test names> # all green
go test ./... -count=1                                # full module, all green (wire 255, laser 1201, bridges 74, internal/cborform 11)
go test ./... -race -count=1                          # full module, all green, no regressions
```

**`laser/agent_warm_dedup.go`** (new, ~300 lines) — `WarmDedupPolicy`
(`WarmDedupDeadLetters`, the zero value; `WarmDedupUnexpired`; `WarmDedupAll`),
and `AgentRuntime.warmUpDedup`/`warmDedupWindow`/`expiredDeadLetters`/
`warmDedupRefills`, ported from `sdk/src/agent/consumer.rs`'s
`warm_dedup_window`/`warm_seed`/`refills`/`expired_copies`. Wired in by three
small, additive edits to `laser/agent_runtime.go`: `Group`, `WarmDedup`, and
`WarmDedupPolicy` fields on `RuntimeOptions`, matching fields threaded through
the constructor, a validation check (`WarmDedup` needs `Group`), and one new
call at the top of `Run` (`if err := runtime.warmUpDedup(ctx); err != nil { return err }`)
before any dispatch, serial or per-partition, begins.

Design decisions worth knowing before extending this:

- **A `Group` field had to be added to `RuntimeOptions`, because `AgentRuntime`
  otherwise has no idea what consumer group it is running under.** This is a
  real Go/Rust architectural difference, not an oversight: Rust's
  `ReliableConsumer` joins its own consumer group inside `run()` and so always
  knows its name; Go's `AgentRuntime.Run` is handed an already-open
  `RecordSource` (built separately, e.g. by `Topic.ConsumerGroup` or by
  `SpawnAgent`) and never learns what group produced it. `WarmDedup` is the
  first capability that needs the group name for something `AgentRuntime`
  itself does (reading the group's own committed offset per partition via
  `client.GetConsumerOffset`), so it is the first reason this gap had to be
  closed. `SpawnOptions.Group` (added last session) was **removed** as a
  redundant top-level field once `RuntimeOptions.Group` existed: `SpawnOptions`
  embeds `RuntimeOptions`, so `options.Group` now resolves there by promotion
  with no change to `SpawnAgent`'s own body, and `WarmDedup` set on
  `SpawnOptions` now flows into `AgentRuntime` automatically. This was a safe
  rename (not a breaking change) only because last session's `Group` field was
  itself uncommitted.
- **The refill reads raw Iggy client calls directly (`laser.connected()`,
  `client.GetConsumerOffset`, `client.PollMessages`, `client.GetTopic`),
  the same low-altitude surface `laser/cursor.go`'s `Cursor.drain` and
  `laser/agent_laser.go`'s `Consumed` already use** — not `RecordSource`,
  which is too abstract to express "read this group's committed offset" or
  "drain this exact offset range." `warmDrainRange` is a small, local,
  bounded-range read (mirrors `cursor.go`'s `drain`, but to an inclusive upper
  bound rather than to the tail) rather than a shared generalization of it:
  Rust's equivalent (`drain_partition`/`DrainRange`) is shared infrastructure
  used in half a dozen unrelated places in `sdk/src/poll.rs`; Go has no such
  shared drain helper yet, and inventing one as a side effect of this
  capability would have been scope creep beyond what WarmDedup itself needs.
- **The dead-letter scan (`expiredDeadLetters`) reuses the exact capsule shape
  the rest of the runtime already writes** (`wire.AgentDeadLetter`, decoded
  with the same `wire.Unmarshal` the runtime's own dead-letter write path
  uses), filtered to this runtime's own resolved stream/topic ids
  (`runtime.source`, the same field `resolveSource` already fills for capsule
  addressing) so one stream's shared `agent.dlq` topic does not leak another
  topic's expired copies into this one's refill.
- **A dead-letter scan that cannot be trusted degrades to `WarmDedupUnexpired`
  silently**, matching Rust exactly: too many dead letters to bound (more than
  the dedup window's own capacity), retention already having dropped some of
  what the scan needs, or a transport failure all fall back to the
  deadline-only check rather than failing the whole warm-up. This is the one
  place the Go port is silent where Rust logs a `warn!`: this package has no
  logging dependency anywhere (checked before deciding this), so there is
  nothing to log through without introducing one, which would be its own,
  separate decision.
- **Every other failure during warm-up is fatal to starting the run**,
  matching Rust's `?`-propagation exactly: a topic/offset/history read that
  genuinely fails returns from `Run` before either dispatch loop starts,
  rather than starting to read with a known-incomplete dedup window.

### Tests added this session (WarmDedupPolicy)

`laser/agent_warm_dedup_internal_test.go` (new, `package laser`, the
project's own established idiom — see `consumer_group_internal_test.go`,
`nodes_internal_test.go` — for testing an unexported function directly rather
than exporting it just to test it):

| Test | Proves |
| --- | --- |
| `TestGivenEachWarmDedupPolicyWhenDecidingRefillsThenShouldLeaveOutOnlyWhatItNames` (10 subtests) | `warmDedupRefills`'s decision table for all three policies, mirroring `sdk/src/agent/consumer.rs`'s own test of the same name: a key with no deadline always refills; `DeadLetters` excludes only a position a scan actually names and falls back to the deadline check when it has none; `Unexpired` never consults a scan even when one is passed; `All` refills everything. |

`laser/agent_warm_dedup_test.go` (new, package `laser_test`, extends the
shared `runtimeServer` fake in `agent_runtime_test.go` with `GetConsumerOffset`
and `PollMessages`, plus a `seedHistory` helper and a `partitions` field —
additive only; every existing test leaves both at their zero value, so nothing
already using `runtimeServer` is affected):

| Test | Proves |
| --- | --- |
| `TestGivenWarmDedupOnWithNoGroupWhenBuiltThenShouldRefuse` | `WarmDedup` without `Group` is refused at construction, not silently inert. |
| `TestGivenWarmDedupOffWhenBuiltThenShouldNotRequireGroup` | The zero value needs no `Group`, so every existing caller is unaffected. |
| `TestGivenCommittedHistoryWhenWarmDedupRunsThenShouldSeedOnlyWhatIsCommittedAndAddressedHere` | A record past the group's stored offset (unconsumed) is never read, and a record addressed to another agent is read but not seeded. |
| `TestGivenMoreHistoryThanTheWindowsCapacityWhenWarmDedupRunsThenShouldKeepOnlyTheMostRecent` | The dedup window's own capacity bounds how much history refills it, keeping the most recent. Caught and fixed a real test-authoring bug during development: probing an evicted key with `Deduplicator.Accept` is itself mutating (it inserts on a miss), so the probes had to be ordered so the destructive one runs last. |
| `TestGivenAPastDeadlineWithNoMatchingDeadLetterWhenWarmDedupRunsUnderDeadLettersThenShouldStillRefillIt` | `DeadLetters` refills a record whose deadline passed but that no dead letter actually names as dropped — the behavior that distinguishes it from `Unexpired`. |
| `TestGivenAPastDeadlineWithAMatchingDeadLetterWhenWarmDedupRunsUnderDeadLettersThenShouldLeaveItOut` | The same expired record, this time with a matching capsule on `agent.dlq`: `DeadLetters` reads it and correctly leaves the copy out. |

### Verification run after WarmDedupPolicy

```
gofmt -l .                                              # clean
go vet ./...                                            # clean
go build ./...                                          # clean
go test ./laser/... -run WarmDedup -v                   # all green (17 cases across 7 top-level tests)
go test ./laser/... -race -run WarmDedup                # all green
go test ./... -count=1                                  # full module, all green (wire 255, laser 1208, bridges 74, internal/cborform 11)
go test ./... -race -count=1                            # full module, all green, no regressions
```

**`laser/agent_runtime.go`'s `pickUpSubmitted`** (new, ~35 lines), called from
`deliver` with a `defer lease.Release()` around the handler call. Ported from
`pick_up_submitted` in `sdk/src/agent/consumer.rs:1809`, but needed no new
infrastructure: it composes three pieces that already existed —
`envelope.Metadata[wire.MetadataSubmitted]` (the same metadata-bool-read idiom
already used for `MetadataRecorded`/`MetadataRole` in `wire/dispatch.go`),
`Session.Progress` (already sends a `TaskWorking` status, the Go equivalent of
Rust's status-send half of `pick_up()`), and `Laser.LeaseSession` (already
resolves the stream generation and acquires a lease, starting the heartbeat
loop if it is the first). A failure anywhere in the chain is reported through
the existing `RuntimeObserver.OnError` hook (the same one `acknowledgePickup`
already uses) and is not fatal to the record, matching Rust's own
warn-and-continue behavior.

One deliberate simplification versus Rust: Rust derives the lease's heartbeat
interval from `config.heartbeat.min(idle_timeout / 5)`; Go's `SessionConfig`
has no `heartbeat` field to derive from, so this takes `LeaseSession`'s own
existing default (passing `0`) rather than inventing new config surface for
one call site.

### Tests added this session (submitted-session pickup)

`laser/agent_submitted_pickup_test.go` (new, package `laser_test`):

| Test | Proves |
| --- | --- |
| `TestGivenASubmittedCommandWhenHandledThenShouldMarkTheSessionWorkingAndTakeALease` | A submitted command's pickup publishes one status record on `agent.sessions` and holds exactly one lease for the duration of the handler call, released once it returns. |
| `TestGivenAnOrdinaryCommandWhenHandledThenShouldNotMarkAnySessionOrTakeALease` | A command with no submitted metadata never marks a session or takes a lease. |
| `TestGivenAPickupThatCannotPublishWhenHandledThenShouldStillHandleTheRecord` | A refused pickup status does not take a lease, but the handler still runs and the record still commits. |

### Verification run after submitted-session pickup

```
gofmt -l .                                              # clean
go vet ./...                                            # clean
go build ./...                                          # clean
go test ./laser/... -run <the 3 new test names> -v      # all green
go test ./laser/... -race -run <the 3 new test names>   # all green
go test ./... -count=1                                  # full module, all green (wire 255, laser 1211, bridges 74, internal/cborform 11)
go test ./... -race -count=1                            # full module, all green, no regressions
```

**`laser/session_assemble.go`** (new, 166 lines) — `Session.Assemble(ctx, policy)`.
Ported from `assemble` in `sdk/src/agent/session_ops.rs`, but needed almost no
new infrastructure: `AssembledContext`, `wire.ContextManifest`/`Fragment`/
`FrontierPosition`, `wire.EstimateTokens`, and `Laser.Context(...).FetchWith(...)`
all already existed. The gap was purely the composing method: fetch via the
existing query surface, resolve each topic's numeric id/generation, build a
fragment per message, and roll up tokens/bytes/frontier into the manifest —
exactly what `bdd/go/session_policy_test.go`'s hand-rolled `buildAssembledContext`
helper (lines 795-857) already did ad hoc, minus the two fields it skipped
(`Tokens`, `Frontier`) that a real implementation has to get right.

Design decisions worth knowing before extending this:

- **Topics are resolved from the session's own declared `SessionLayout`,
  not asked of the caller.** `ContextScope.Fetch`/`FetchWith` take an explicit
  `topics []string`, but `Session.Assemble()` does not — it reads the session
  lane (`agent.sessions`) always, plus a per-agent-topic layout's own declared
  topics (`SessionLayout.DeclaredTopics()`, which already existed). This
  matches Rust's no-topics-argument `assemble()` more closely than mechanically
  copying `FetchWith`'s shape would have.
- **A real bug was found and fixed before any test ran**: the frontier map's
  first draft wrote `frontier[key] = offset` only when `offset > frontier[key]`,
  which is indistinguishable from "already recorded a higher offset" for a
  key seen for the first time with offset 0 — a Go map read of a missing key
  returns the zero value, so offset 0 would never create an entry at all.
  Fixed with the comma-ok form (`existing, seen := frontier[key]`) before
  writing any test, the same category of map-semantics bug class as this
  session's earlier `ConcurrencyPolicy` hang, just caught earlier this time.
- **`PolicyVersion` and `Correlation` are left at their zero values.** Go's
  `ContextPolicy` interface has only `Name()`/`Select()` — no `Version()` the
  way Rust's `ContextPolicy` trait has — and adding one would be a breaking
  change to every existing implementer (`LastN`, `TokenBudget`, `Chain`).
  There is also no natural correlation id for a generic assemble call outside
  a contract. Both are documented gaps, not silent omissions.

### Tests added this session (`Session.Assemble()`)

`laser/session_assemble_test.go` (new, package `laser_test`, reuses the
existing `scopeServer`/`newScope`/`appendRecord` fixtures from
`context_scope_test.go`, adding one `GetStream` method to that fixture type):

| Test | Proves |
| --- | --- |
| `TestGivenASessionsHistoryWhenAssembledThenShouldManifestEveryFragment` | Every fetched record becomes a fragment, the manifest's policy name matches the policy, and token/byte totals match `wire.EstimateTokens` summed over the actual bodies. |
| `TestGivenMultipleRecordsOnOnePartitionWhenAssembledThenFrontierShouldNameTheHighestOffset` | The frontier names the highest offset read on a partition, not every offset — one entry for three records on one partition, at offset 2. |
| `TestGivenANarrowingPolicyWhenAssembledThenShouldKeepOnlyWhatItSelects` | `Assemble` hands the fetch to the given policy, so `LastN(1)` narrows the same way a direct `ContextScope.Fetch` under it would. |
| `TestGivenAnEmptySessionWhenAssembledThenShouldReturnAnEmptyManifest` | A session with nothing on its lane yet assembles to an empty, still-valid manifest rather than an error. |

### Verification run after `Session.Assemble()`

```
gofmt -l .                                              # clean
go vet ./...                                            # clean
go build ./...                                          # clean
go test ./laser/... -run <the 4 new test names> -v      # all green
go test ./laser/... -race -run <the 4 new test names>   # all green
go test ./... -count=1                                  # full module, all green (wire 255, laser 1215, bridges 74, internal/cborform 11)
go test ./... -race -count=1                            # full module, all green, no regressions
```

**`SubmitIn`/`SubmitInTo`** (new, `foreign/go/bridges/a2a_bridge.go`) and
**`CallToolIn`/`CallToolInTo`** (new, `foreign/go/bridges/mcp_bridge.go`) —
child-session task submission, ported from Rust's `A2aBridge::submit_in` and
`McpBridge::call_tool_in`. Priority 3 of this session's scope check (step 3
of the prior handoff's suggested approach) found that
`laser/session_submit.go` did *not* already have what a child submission
needs, so that was added first:

- **`laser/session.go`**: `Session` gained `parent`/`root *wire.ConversationID`
  fields and a `WithAncestry(parent, root) *Session` method, the same
  narrowed-copy shape as the existing `As(agent)`. `Session.Agdx()` now
  applies the ancestry (when set) to every `Agdx` it hands out, so a child
  session's later `Complete`/`Fail`/`End` carries the same ancestry as its
  start, matching Rust's `Session` (`sdk/src/agent/session.rs`), which
  threads `self.parent`/`self.root` through its own `lane()` the same way.
- **`laser/session_submit.go`**: `Sessions.OpenChild(ctx, conversation,
  source, target, parent, root) (*Session, error)`, the Go port of Rust's
  `pub(crate) agent::contract::start_child`. Rust keeps that function crate-
  private because only `contract.rs` (missing in Go) and the two bridges call
  it; Go has no equivalent "friend package" visibility, so the bridges
  package — a separate package from `laser` — needs it exported to call it at
  all. Writes the child's start as `TaskSubmitted` (not `TaskWorking`, which
  is what the existing `Session.Begin()`/`SessionStarted` hard-code for an
  ordinary session) with `wire.SessionStart.Parent`/`.Root` set, by composing
  already-exported pieces (`Sessions.Open`, `Session.As`, the new
  `Session.WithAncestry`, `Agdx.Status`/`WithTaskState`/`Body`/`ContentType`)
  rather than adding a new public "begin as submitted" method to `Session` —
  the same shape `session_submit.go`'s own `SubmitBuilder.Send` already uses
  for its own submitted-start write.
- Both bridges reuse `OpenChild` identically: mint the child conversation,
  open it (`source` and `target` are both the bridge's own agent id, same as
  Rust's two call sites), then publish the command through an `Agdx` built
  with `.WithAncestry(parent, root)` so the command's envelope carries the
  same parent/root as the start. `A2ABridge.publishCommand` and
  `MCPBridge.sendToolCall` were given a `sendToolCallOn`/ancestry-aware form
  rather than a parallel code path, so the ordinary (non-child) `Submit`/
  `CallTool` paths are unchanged other than passing `nil, nil`.
- **A2A's child is not ended by the bridge** — matching Rust's doc
  ("the agent that handles the task ends the child"): `SubmitIn` only writes
  the start and the command. **MCP's is**, because `CallToolIn` is
  synchronous and already awaits the terminal: `callToolInAddressed` calls
  `child.Complete(ctx, "")` on a non-error reply, `child.Fail(ctx,
  toolFailure(...))` on a tool error or a send/await failure, mirroring
  Rust's `call_tool_in_addressed` match on the outcome.
- **Known, pre-existing gap left untouched**: `A2ACaller` (the task-binding
  wrapper) has no `SubmitIn`/`SubmitInTo` — it already had no `SubmitTo`/
  `SubmitWithHops` either, narrower than Rust's `A2aCaller` before this
  session touched anything. Closing that whole gap is separate, unstarted
  work, not folded into this capability.

### Tests added this session (`submit_in`/`call_tool_in`)

`bridges/a2a_bridge_test.go`:

| Test | Proves |
| --- | --- |
| `TestGivenAncestryWhenSubmittedAsAChildThenShouldWriteTheStartBeforeTheCommand` | `SubmitIn` writes exactly two records in order: a `TaskSubmitted` session status whose envelope and `SessionStart` body both carry the ancestry, then the command, itself carrying the ancestry and the byte-identical params. |
| `TestGivenATargetWhenSubmittedInAChildThenShouldAddressOnlyThatAgent` | `SubmitInTo` addresses the command to the given target. |

`bridges/mcp_bridge_test.go` (extends the existing `replyingLog` fake with an
`errorReply` field, additive — every existing test leaves it at its zero
value and keeps answering with a response):

| Test | Proves |
| --- | --- |
| `TestGivenAncestryWhenCalledAsAChildThenShouldEndItCompletedOnSuccess` | `CallToolIn` writes the ancestry-carrying start and command, then ends the child `TaskCompleted` on a successful tool result. |
| `TestGivenAToolErrorWhenCalledAsAChildThenShouldEndItFailed` | A tool error reply ends the child `TaskFailed`, and the tool's own error is still what the caller sees (`result.IsError`), not swallowed by the ending. |
| `TestGivenATargetWhenCalledAsAChildThenShouldAddressOnlyThatAgent` | `CallToolInTo` addresses the command to the given target. |

### Verification run after `submit_in`/`call_tool_in`

```
gofmt -l .                                              # clean
go vet ./...                                            # clean
go build ./...                                          # clean
go test ./bridges/... -run <the 5 new test names> -v    # all green
go test ./bridges/... -race -run <the 5 new test names> # all green
go test ./... -count=1                                  # full module, all green (wire 255, laser 1215, bridges 79, internal/cborform 11)
go test ./... -race -count=1                            # full module, all green, no regressions
```

**`laser/agent_contract.go`** (new, ~300 lines) — `Contract`, `ContractState`
(`ContractCompleted`/`ContractFailed`/`ContractNotConsumed`/`ContractTimedOut`),
`ContractBuilder`, and `Laser.Contract(router, topic)`. Ported from
`sdk/src/agent/contract.rs`'s `Contract`/`ContractBuilder`, with the scope and
architecture narrowed to fit what this session's reading of
`laser/replies.go`/`request.go` and `bridges/mcp_wait.go` (step 2 of the prior
handoff's suggested approach) found Go already has:

- **`Send` drives its own live poll of the reply topic**, rather than
  reusing `Requester`'s `ReplyHub`/`Ticket`, because that pattern requires an
  external reader already feeding `Hub().Deliver` (true inside a running
  `AgentCtx` fan-out, which is handler-side) and `Contract` is meant to be
  usable standalone, by an orchestrator with no consumer loop of its own.
  Rust's own `Contract` is self-driving too, but via a shared, per-(stream,
  topic) cached hub with its own background poll task
  (`Laser::reply_hub`/`ReplyHub::create`); building that shared-cache
  infrastructure was judged disproportionate to this one capability, so
  `awaitContractReply` instead polls directly, the same bounded-live-poll
  shape `bridges/mcp_wait.go`'s `awaitReply` already uses for the same kind
  of problem — one poller per call rather than one per topic, simpler at the
  cost of not sharing a read across concurrent contracts on the same topic.
  It still reuses `ExpectedReply.Accepts` as its match predicate (conversation,
  kind, cause, addressee), rather than re-deriving that logic, by decoding
  each polled record with the existing `OpenAgdx` into the `*AgdxMessage`
  shape `Accepts` already takes.
- **No separate per-agent inbox topic.** Rust's `InboxRoute` resolves a
  target's inbox from its registry card; Go's `RegisteredCard` carries no
  such address anywhere (confirmed by reading `laser/registry.go` before
  writing any code), and `Requester`/`AgentCtx.branch` already settled on one
  caller-named topic for both the command and its reply. `Contract` follows
  that precedent rather than inventing address resolution Go's registry has
  no data for.
- **`Registry(*AgentRegistry)` is caller-supplied**, not auto-fetched from
  `Laser`, matching `FanOutRequest.Registry`'s existing precedent: Go's
  `Laser` caches no registry internally (a caller folds one off the log
  itself, unlike Rust's `Laser::agent_registry()`), so `Contract` cannot
  fetch one either. Needed only for a capability route (`ToCapable`/
  `Sticky`/`WithScorer`); a direct `To` route takes none.
- **`ContractNotConsumed` reuses the existing dead-letter shape exactly**:
  `droppedUnconsumed` decodes a `KindError` reply's body and checks
  `DeadLetterReasonDetail` (`laser/agent_runtime.go`, already shipped) names
  `wire.DeadLetterDeadlineExceeded`, the same capsule `DeadLetterErrorBody`
  already constructs and `answerDeadLetter` already publishes when
  `RuntimeOptions.RespondOn` is set — no new wire shape, just reading the one
  that was already there.
- **`Parent(parent, root)` reuses `Sessions.OpenChild`** (added for
  `submit_in`/`call_tool_in` earlier this session) rather than a parallel
  child-session helper: resolve the target first, open the child as that
  target, send the command with the producer's `WithAncestry` and a
  `MetadataSubmitted` stamp, then end the child by the outcome exactly the
  way `McpBridge.CallToolIn` already does (`Complete` on
  `ContractCompleted`, `Fail` otherwise, including a send error).
- **Not ported**: `idempotency_key`, `resume_from`, and `excluding` are all
  `pub(crate)` in Rust, used only by the still-missing workflow engine's own
  retry/resume paths — not public API even there, so there is nothing for a
  Go caller to be missing yet. Revisit when the workflow engine is scoped.
  Also not ported: a caller-side `reply_topic` distinct from the command's
  topic (Rust's default `AgentTopic::Sessions`) — Go's single-topic model
  (see above) has no place for a second one without inventing the inbox
  resolution this session deliberately declined to add.

### Tests added this session (`Contract`/`ContractBuilder`)

`laser/agent_contract_test.go` (new, package `laser_test`, a one-partition
fake client — `contractServer` — that stores every published record and,
when `replyKind` is set, synchronously answers a command inline the way a
real target would, the same shape `bridges/mcp_bridge_test.go`'s
`replyingLog` already uses):

| Test | Proves |
| --- | --- |
| `TestGivenACompletedReplyWhenContractSentThenShouldReportCompleted` | A non-error reply settles `ContractCompleted`, keeping the reply. |
| `TestGivenAnErrorReplyWhenContractSentThenShouldReportFailed` | A plain terminal error settles `ContractFailed`. |
| `TestGivenADeadLetteredReplyWhenContractSentThenShouldReportNotConsumed` | A reply naming the `DeadlineExceeded` dead-letter reason settles `ContractNotConsumed`, distinct from an ordinary failure. |
| `TestGivenNoReplyWhenContractSentThenShouldReportTimedOut` | Nothing landing within `Deadline` settles `ContractTimedOut`. |
| `TestGivenNoSourceWhenContractSentThenShouldRefuseBeforeResolving` | `Send` without `From` refuses before resolving the route. |
| `TestGivenABroadcastRouteWhenContractSentThenShouldRefuseIt` | A `Broadcast` router is refused: a contract is directed to one agent. |
| `TestGivenACapabilityRouteWhenContractSentThenShouldResolveAgainstTheRegistry` | `ToCapable` resolves against the supplied `Registry` and addresses the command to the agent it picked. |
| `TestGivenAnAncestryWhenContractCompletesThenShouldEndTheChildCompleted` | `Parent` writes the child's submitted start (with ancestry) before the command (which carries the same ancestry), then ends the child completed on a successful reply. |
| `TestGivenAnAncestryWhenContractFailsThenShouldEndTheChildFailed` | The same child session ends failed when the contract's own outcome is non-completing. |

### Verification run after `Contract`/`ContractBuilder`

```
gofmt -l .                                              # clean
go vet ./...                                            # clean
go build ./...                                          # clean
go test ./laser/... -run Contract -v                    # all green (9 new tests)
go test ./laser/... -race -run Contract                 # all green
go test ./... -count=1                                  # full module, all green (wire 255, laser 1224, bridges 79, internal/cborform 11)
go test ./... -race -count=1                            # full module, all green, no regressions
```

**`laser/agent_scatter.go`** (new, ~190 lines) — `ScatterRequest`,
`ScatterOutcome`, `ScatterReport` (with `Completed()`/`Failures()`),
`Laser.Scatter`, `Laser.ScatterReport`. Ported from `sdk/src/agent/contract.rs`'s
`Laser::scatter`/`Laser::scatter_report`/`ScatterOutcome`/`ScatterReport`, but
the gather loop itself is not a port of Rust's generic `gather_branches`/
`Branches<T>` (a `tokio::task::JoinSet` over an arbitrary future): Go already
had the identical loop shape in `laser/agent_ctx.go`'s `AgentCtx.gather` (one
goroutine per target, a buffered outcomes channel, a wall-clock timer,
quorum/best-effort/cancellation), built for `FanOut`. Rather than generalize
`AgentCtx.gather` into something both a handler-side fan-out and a caller-side
scatter could share — a refactor of code outside this capability's stated
scope — this duplicates that loop shape once more, now spawning
`Contract.Send` per branch instead of `AgentCtx.branch`'s own inline
send-and-wait. The existing `GatherPolicy`/`remaining` (both already
package-level, not `AgentCtx` methods) are reused as-is; `Gather`/
`GatheredReply`/`GatheredFailure` are not, because `ScatterReport` needs to
keep each branch's full `Contract` (Completed/Failed/NotConsumed/TimedOut),
not collapse everything that merely finished into one `OK` bucket the way
`Gather` does for `FanOut`'s plainer "got a reply or didn't" semantics.

Design decisions worth knowing before extending this:

- **`Registry` is required and caller-supplied**, matching `Contract.Registry`
  and `FanOutRequest.Registry`'s existing precedent from earlier this session
  and last: Go's `Laser` caches no registry internally, so a `Scatter` that
  silently fetched one would have nowhere to fetch it from.
- **A "success" for `GatherQuorum`/`GatherAll` purposes is `ContractCompleted`
  only**, exactly like Rust's `|contract| matches!(contract, Contract::Completed(_))`
  predicate: a branch that merely finished (`Failed`/`NotConsumed`/`TimedOut`)
  counts toward neither a quorum nor `GatherAll`'s "every branch" bar, so an
  all-finished-but-one-failed scatter correctly reads as unsatisfied even
  though nothing timed out or was cancelled (see the mixed-outcome test
  below, which first asserted the opposite and was wrong, not the code).
- **Both bounds are the same `Deadline` value**, matching Rust's
  `scatter_report(..., deadline)` passed to both each `contract.deadline(deadline)`
  and the gather's own wall clock: a straggler branch is bounded by its own
  `Contract` deadline before the gather's wall clock would otherwise need to
  cut it short.
- **Not ported**: Rust's `Laser::scatter`/`scatter_report` take an
  `InboxRoute` (irrelevant here, `Contract` already has none — see its own
  "Completed this session" note) and spawn the branch through `self.contract(...)`
  carrying the scatter's own `source`, `payload`, and `deadline` — Go's
  `ScatterRequest.{From,Payload,Deadline}` map onto the same three fields
  `ContractBuilder` already exposes, so no new plumbing was needed in
  `Contract` itself for this.

### Tests added this session (`Scatter`/`ScatterReport`)

`laser/agent_scatter_test.go` (new, package `laser_test`, reusing
`agent_contract_test.go`'s `contractServer` fake extended with
`repliesByTarget`/`silent` so more than one target can answer differently —
additive only, every existing `Contract` test leaves both at their zero
value and keeps its single global `replyKind`/`replyBody`/`replySource`
behavior):

| Test | Proves |
| --- | --- |
| `TestGivenTwoCapableAgentsWhenScatteredThenShouldGatherBothCompletions` | `GatherAll` over two capable agents that both complete gathers both, satisfied, nothing cancelled. |
| `TestGivenAQuorumWhenScatteredThenShouldStopAfterEnoughSuccessesAndCancelTheRest` | `GatherQuorum(2)` over three agents stops once two complete, cancelling the third (silent) one. |
| `TestGivenBestEffortWhenScatteredThenShouldTakeWhateverLandedByTheDeadline` | `GatherBestEffort` over one agent that answers and one that is silent takes the one that landed by the deadline and cancels the other. |
| `TestGivenAMixOfOutcomesWhenScatteredThenShouldReportEachAgentsOwnOutcome` | A completed and a failed branch both appear in `Outcomes`, attributed to their own agent; `Failures()` stays empty (a failed contract is not an infrastructure failure); `GatherAll` is correctly unsatisfied. |
| `TestGivenNoCapableAgentWhenScatteredThenShouldRefuse` | An empty registry refuses with `NoCapableAgentError`. |
| `TestGivenAZeroQuorumWhenScatteredThenShouldRefuseBeforeAnyRead` | `GatherQuorum(0)` is refused by `Policy.Validate()` before resolving anything. |
| `TestGivenACompletedScatterWhenScatterCollectsThenShouldReturnOnlyCompletedBodies` | `Laser.Scatter`'s convenience form returns only the completed branch's reply body, dropping the failed one and all attribution. |

### Verification run after `Scatter`/`ScatterReport`

```
gofmt -l .                                              # clean
go vet ./...                                            # clean
go build ./...                                          # clean
go test ./laser/... -run Scatter -v                     # all green (7 new tests)
go test ./laser/... -race -run Scatter                  # all green
go test ./... -count=1                                  # full module, all green (wire 255, laser 1231, bridges 79, internal/cborform 11)
go test ./... -race -count=1                            # full module, all green, no regressions
```

**`laser/agent_workflow.go` + `laser/agent_workflow_journal.go`** (new, ~560
lines together) — slice 1 of the workflow engine, per the scoping report
confirmed before this session started: a declared-steps dependency-ordered
workflow with `step_local`, budgets, and journal/replay/resume. Explicitly
deferred: exclusive/fenced steps, saga compensation, and the "plain code"
`run_with`/`WorkflowRun` form (see the three new matrix rows above and
"What is NOT done" below).

Ported from `sdk/src/agent/workflow.rs`, scoped down to what that file's
declared-steps path (`Workflow`/`StepHandle`/`run`/`execute`) needs, skipping
everything under `#[cfg(feature = "kv")]` (exclusive steps) and the
`WorkflowRun`/`RunState`/`StepCall` function-call form entirely:

- **`Contract` gained three fields this session, purely for this caller**:
  `Correlation` (pin instead of mint, so a resumed attempt's catch-up scan
  and its fallback send share one correlation), `IdempotencyKey` (stamp the
  command so a target's duplicate suppression drops a resent attempt it
  already handled), and `ResumeFrom` (look for a terminal already landed at
  given reply-topic positions before sending anything; finding none still
  sends, under the same idempotency key, rather than guessing "never sent"
  vs "sent, no reply yet"). `ContractBuilder.Send` was split into
  `Send`/`SendResolved` (the latter also naming the resolved agent, which
  `Workflow` needs for its `StepCompleted` journal entry's `agents` list and
  Rust keeps `pub(crate)` for the identical reason). `Laser.ReplyPositions`
  (new) is the reply topic's own tail, one offset per partition, ported from
  Rust's `reply_positions`/`AgentReplyReader::new_at_tail`: poll each
  partition with `LastPollingStrategy`/count 1, take the last offset plus
  one.
- **A real correctness gap in `awaitContractReply` was found and fixed
  while extending it for resume**: the poll loop matched a reply by
  `ExpectedReply.Accepts` alone (conversation, kind, cause, addressee) and
  never checked the record's own `Correlation` against the one the command
  was sent under. `Accepts` itself has no correlation field to check — the
  real `ReplyHub` (`Requester`, `AgentCtx.branch`) never needed one, because
  it finds the waiter by correlation as a map key *before* calling `Accepts`
  at all. `Contract`'s poll has no such map, so nothing enforced it; two
  different correlations active on the same pinned conversation (exactly
  what a resumed workflow step across attempts can be) could have answered
  each other. Fixed by checking `message.Envelope.Correlation` explicitly
  before `Accepts`, in both `agent_contract_test.go`'s existing fakes'
  reach and the new `awaitContractReply`/`ReplyPositions` work. No existing
  test caught this because every prior `Contract` test used exactly one
  correlation per conversation; no new regression test was added beyond the
  workflow engine's own resume test, which now depends on the fix being
  correct (see "Tests added this session" below).
- **Child sessions come free.** A directed step's `Contract.Parent(runID,
  runID)` already opens, and ends, the child session the way `submit_in`/
  `call_tool_in` proved out two capabilities ago — no new session-handling
  code was written for steps at all.
- **The journal's own wire shape is Go-native**, encoded with this
  package's own `wire.Marshal`/`wire.Unmarshal` CBOR codec rather than
  matching Rust's externally-tagged serde enum byte-for-byte. The journal
  entry type (`WorkflowJournalEntry` in Rust) is SDK-internal, not a
  laser-wire fixture-pinned type, so nothing enforces cross-SDK agreement
  on it the way `wire/parity_test.go` does for public wire types. Matching
  Rust's exact encoding was judged not worth the added complexity for a
  slice that does not yet claim cross-SDK resume; the gap is named in both
  the matrix and the new README "Workflow" section rather than left silent.
- **The run-level session opens with no budget and no lease.** Go's
  `Session.Begin()` has no budget parameter (session.go's own gap, not new)
  and no heartbeat lease is taken for the run session (the same "not
  bundled with `Begin`" gap `Session` already had, noted in its own matrix
  row). The *engine-level* `WorkflowBudget` (tokens/wall-clock/invocations,
  checked against the engine's own running totals) is fully implemented and
  does not depend on either.
- **No cancel check at step boundaries.** Rust's `execute` reads
  `agent.control` between every step to honor an operator cancel; this
  session's slice does not, since cancellation during a run was not part of
  the confirmed slice-1 scope. The run still ends failed (never left
  dangling) on any other error.
- **`BudgetExceededError` is reused, not redefined.** `laser/session_budget.go`
  already had a `BudgetExceededError{Breach BudgetBreach}` for session-level
  budget breaches, including `Session.FailOverBudget`, discovered mid-edit
  when the compiler caught the duplicate declaration. The workflow engine's
  own budget breach now constructs the same `BudgetBreach` (two new
  dimension constants, `BudgetWallClock`/`BudgetInvocations`, alongside the
  existing `BudgetTokens`) and ends the run session through the same
  `Session.FailOverBudget`, rather than inventing a parallel error/ending
  path.
- **`StepHandle`/`Workflow` mutate in place rather than copying per call**,
  unlike every other fluent builder in this package (`ContractBuilder`,
  `A2ABridge`, `Router`, ...). Rust's own builder here consumes and returns
  `self` to grow one `Vec<Step>`; Go has no move semantics to mirror that
  cheaply, and copying (or unsafely sharing) a growing step slice on every
  chained call is worse than explicit shared mutation through a pointer.
  Documented on the `Workflow` type itself so it reads as a deliberate
  choice, not an inconsistency.

### Tests added this session (workflow engine slice 1)

`laser/agent_workflow_test.go` (new, package `laser_test`, a new
topic-aware fake, `workflowServer` — unlike `contractServer`'s single flat
list, it keeps one append-only list per topic name, because a workflow run
touches two: `agent.sessions` for the run's own session and every step's
contract, and `agent.workflow_journal` for the journal, and a journal
replay must never see the other one's traffic):

| Test | Proves |
| --- | --- |
| `TestGivenLocalStepsWhenRunThenShouldPassOutputsInDependencyOrder` | Two `StepLocal` steps run in declared dependency order, and the second's `StepFunc` sees the first's output. |
| `TestGivenADirectedStepWhenRunThenShouldDispatchViaContract` | A directed step's output is the dispatched `Contract`'s completing reply. |
| `TestGivenAnAllCapableStepWhenRunThenShouldJoinCompletedReplies` | An `AllCapable` step joins every completed agent's reply with `\n`. |
| `TestGivenADuplicateLabelWhenRunThenShouldRefuseBeforeWriting` | A duplicate step label is refused before any session or journal record is written. |
| `TestGivenADependencyCycleWhenRunThenShouldRefuse` | A dependency cycle is refused (Kahn's algorithm never completes the order). |
| `TestGivenAVerifierThatRejectsWhenRunThenShouldFailTheStep` | A verifier returning false fails the step `StepUnverified`. |
| `TestGivenAnInvocationsBudgetWhenRunThenShouldFailOnceExceeded` | An invocations ceiling of 1 lets the first step complete and fails the run before the second dispatches. |
| `TestGivenAResumedRunWhenRunAgainThenShouldSkipCompletedSteps` | A run stopped by its own budget after one step, then resumed under the same run id with no budget, completes the remaining step without re-dispatching the first — exactly one `KindCommand` per step across both runs. |

### Verification run after the workflow engine (slice 1)

```
gofmt -l .                                              # clean
go vet ./...                                            # clean
go build ./...                                          # clean
go test ./laser/... -run 'Workflow|Step|Resumed|Dependency|Duplicate|Directed|AllCapable|Local' -v  # all green (8 new tests)
go test ./laser/... -race -run Workflow                 # all green
go test ./... -count=1                                  # full module, all green (wire 255, laser 1239, bridges 79, internal/cborform 11)
go test ./... -race -count=1                            # full module, all green, no regressions
```

**Workflow engine slice 2: saga compensation** (`agent_workflow.go`'s
`compensate`/`runCompensation`/`compensationRoute`, `agent_workflow_journal.go`'s
new `journalStepCompensated` kind and fold case) — `StepHandle.CompensateWith`,
ported from `sdk/src/agent/workflow.rs`'s `compensate()`. Confirmed as the
suggested next slice by this session's own scoping report (cheapest of the
three remaining pieces, since it only needed the DAG + journal + `Contract`
core slice 1 already shipped).

Design decisions worth knowing before extending this:

- **Every step's "effect" (the agents it actually ran against, if any) now
  flows back out of `runStep`/`runDirected`/`runAllCapable` as a third
  return value**, `[]wire.AgentID`, alongside the existing output/tokens/
  error. `nil` means no effect at all (the build failed, or a directed/
  all-capable dispatch never reached a completing reply — Rust's
  `StepError::from(LaserError)` sets `effect: None` for exactly this case,
  via the `?` in `run_directed`/`completion`). Non-nil — **empty, not nil**,
  for a local step — means an effect happened and is worth compensating,
  whether or not a later verifier then rejected it (the handler, or the
  local build, already ran). This nil/empty distinction is the one subtle
  part of the port; `execute` relies on it to decide whether a step that
  just failed still gets pushed onto `effects` before compensating.
- **`execute` now tracks `[]workflowEffect{index, agents}` as it goes**,
  seeded from `journal.agents` for a resumed run's already-completed steps
  (mirroring Rust's `effects` construction at the top of `execute`), and
  calls `compensate` before returning from every failure path: the
  pre-dispatch budget check, a failed `runStep` (pushing the failing step's
  own effect first when one exists), and the post-dispatch budget check.
- **A local step's compensation never dispatches anything.** `runCompensation`
  returns `true` (undone) immediately when `step.router == nil`, after
  `compensate` has already called the compensation `StepFunc` and discarded
  its return value — the call itself is the undo, matching the BDD
  scenario's model (`workflow_local.feature`'s "a compensation body records
  `undo <label>`") exercised by this session's own
  `TestGivenAFailedLocalStepWhenRunThenShouldCompensateCompletedStepsInReverseOrder`.
- **`compensationRoute` is simplified versus Rust's**: Rust's
  `compensation_route` preserves a `ToPrincipal` route's principal
  requirement when routing the rollback to a specific agent; Go's `Router`
  has no `ToPrincipal` variant at all (sign-feature-gated in Rust), so
  Go's version is unconditionally `To(agent)` — the common, unsigned case
  Rust's own fallback already reduces to.
- **No logging on a compensation that fails to build, dispatch, or
  complete.** Matches Rust's `tracing::warn!` call sites in spirit (best
  effort, never replaces the original error, the rollback continues), but
  this package has no logging dependency anywhere (the same reasoning
  `WarmDedupPolicy` recorded two sessions ago) — a failed compensation is
  simply left unjournaled, which is itself the observable signal a reader
  of the journal needs: the step still reads as completed, uncompensated.

### Tests added this session (saga compensation)

`laser/agent_workflow_test.go`:

| Test | Proves |
| --- | --- |
| `TestGivenAFailedLocalStepWhenRunThenShouldCompensateCompletedStepsInReverseOrder` | Two local steps with compensations, then a third that fails: the run fails with the third step's own error, and the compensations ran in reverse completion order — the same shape as `workflow_local.feature`'s billing scenario. |
| `TestGivenADirectedStepCompensationWhenALaterStepFailsThenShouldDispatchItToTheOriginalAgent` | A directed step's compensation is addressed to the exact agent that ran the original step (two commands to that agent total: the dispatch and the rollback). |
| `TestGivenAResumedRunAfterCompensationWhenRunAgainThenShouldRerunTheCompensatedStep` | A step's compensation un-completes it in the journal fold: resuming after a compensated run re-runs that step rather than reading it as still done. |

### Verification run after saga compensation

```
gofmt -l .                                              # clean
go vet ./...                                            # clean
go build ./...                                          # clean
go test ./laser/... -run 'FailedLocalStep|DirectedStepCompensation|ResumedRunAfterCompensation' -v  # all green (3 new tests)
go test ./laser/... -race -run Compensat                # all green
go test ./... -count=1                                  # full module, all green (wire 255, laser 1242, bridges 79, internal/cborform 11)
go test ./... -race -count=1                            # full module, all green, no regressions
```

**Kv lease/fence client** (`laser/kv_lease.go`, new, ~290 lines) —
`Kv.Lease`/`TryLease`/`RenewLease`/`ReleaseLease`/`LeaseGuard`/`CasFenced`,
ported from `sdk/src/kv/client.rs`'s `Kv::lease`/`try_lease`/`lease_guard`/
`renew_lease`/`release`/`cas_fenced`. This was originally going to be step 1
of workflow engine slice 3 (exclusive steps need it), scoped out as its own
capability once reading `laser/kv.go` for the first time this session found
the wire contract fully ported (`wire.KvLease`/`KvLeaseRenew`/`KvRelease`/
`KvCasFenced`, the `KvLeased`/`KvRenewed`/`KvReleased`/`KvCommitted` outcome
kinds, `wire.CapabilityKvFencedLeases`) but *no* client method anywhere
using any of it — not "thread an existing primitive through," a whole
missing client surface. Flagged and confirmed with the user as its own
capability before writing anything, per the standing cost-control rules on
work that turns out larger than scoped.

Design decisions worth knowing before extending this:

- **No ambiguous-mutation retry.** Rust's `Kv::lease`/`cas_fenced` run
  through a dedicated "coordination client" (`prepare_acquire`/
  `acquire_resolving`) that retries an ambiguous send (ack lost to a
  network failure) under the same operation identity until the store's
  recorded outcome resolves it, bounded by the TTL window. Go's `kv.execute`
  has no such retry for *any* operation today — Get, Set, CAS, Delete all
  take the plain managed-command round trip once. Rather than add this
  reliability layer as a side effect of leases alone (a second kv transport
  living beside the one every other operation uses), this session's
  `Lease`/`RenewLease`/`CasFenced` use the same plain `kv.execute` path:
  consistent with the rest of the client, at the cost of an ambiguous send
  surfacing as a plain transport error instead of being resolved. If
  ambiguous-mutation retry is ever built for Go's kv client, it should cover
  every mutating operation, not just these three.
- **No background release on drop.** Rust's `LeaseGuard` spawns a
  best-effort release task if dropped still held, inside a Tokio runtime.
  Go has no destructor to hook this to; `LeaseGuard.Release` must be called
  explicitly, documented on the type itself rather than silently
  approximated.
- **No lease op-version gate beyond the existing `Kv` one.** Rust pins
  `KV_LEASE_OP_VERSION` as the lease *request body's* own schema tag (not a
  deployment capability version); Go's wire structs already carry the
  matching `V` field (`wire.KvLeaseOpVersion`), and the capability gate
  reuses the existing `requireKv()` plus a `Kv.FencedLeases` flag check — no
  new `OpVersions` field was needed, since `wire.OpVersions` has one `Kv`
  version for the whole surface, lease ops included.

### Tests added this session (Kv lease/fence client)

`laser/kv_lease_test.go` (new, package `laser_test`, reusing `kv_test.go`'s
existing `kvServer`/`kvHandle`/`decodeRequest` fakes, plus a new
`fencedCapabilities()` helper extending `kvCapabilities()` additively):

| Test | Proves |
| --- | --- |
| `TestGivenAGrantedLeaseWhenAcquiredThenShouldReturnTheTokenAndTTL` | `Lease` returns the granted token/TTL and carries the key/holder on the wire. |
| `TestGivenALeaseHeldByAnotherHolderWhenTriedThenShouldAnswerNil` | `TryLease` answers `(nil, nil)` on `KvLeaseHeld`, not an error. |
| `TestGivenAnOrdinaryLeaseFailureWhenTriedThenShouldStillReportIt` | `TryLease` still reports every other failure kind. |
| `TestGivenNoFencedLeaseCapabilityWhenLeasedThenShouldRefuseBeforeSending` | A deployment without `FencedLeases` is refused client-side, before the socket. |
| `TestGivenATTLOutsideThePinnedRangeWhenLeasedThenShouldRefuseBeforeSending` | A TTL outside `[MinLeaseTTLMicros, MaxLeaseTTLMicros]` is refused before the round trip. |
| `TestGivenARenewalWhenSentThenShouldKeepTheSameTokenAndExtendTheTTL` | `RenewLease` presents the same token and carries the new TTL. |
| `TestGivenALeaseGuardWhenReleasedThenShouldReportWhetherItWasHeld` | `LeaseGuard.Release` reports whether it was still held and flips `Held()`. |
| `TestGivenALeaseLostWhenRenewedThenShouldMarkTheGuardNotHeld` | A `KvLeaseLost` renewal marks the guard no longer held. |
| `TestGivenAFencedCompareAndSwapWhenCommittedThenShouldCarryTheFenceToken` | `CasFenced` carries the fence namespace/key/token and the precondition on the wire. |
| `TestGivenNoFencedLeaseCapabilityWhenCommittedThenShouldRefuseBeforeSending` | `CasFenced.Commit` is also gated on the capability, before the socket. |

### Verification run after the Kv lease/fence client

```
gofmt -l .                                              # clean
go vet ./...                                            # clean
go build ./...                                          # clean
go test ./laser/... -run 'GrantedLease|LeaseHeld|LeaseFailure|FencedLeaseCapability|TTLOutside|ARenewal|ALeaseGuard|ALeaseLost|FencedCompareAndSwap' -v  # all green (10 new tests)
go test ./laser/... -race -run Lease                    # all green
go test ./... -count=1                                  # full module, all green (wire 255, laser 1252, bridges 79, internal/cborform 11)
go test ./... -race -count=1                            # full module, all green, no regressions
```

A pre-existing, unrelated flaky test was observed once during this
session's full-suite runs and did not reproduce on retry:
`TestGivenBestEffortWhenScatteredThenShouldTakeWhateverLandedByTheDeadline`
(from `Scatter`, two sessions ago) failed once under full-package parallel
load — its 300ms deadline is apparently tight enough to occasionally miss
under contention — then passed cleanly on an immediate full-suite rerun and
five isolated reruns. Not touched: fixing a timing-sensitive test from a
different, already-shipped capability is out of scope for this one, and
the flake was not reproducible enough to diagnose safely in the time this
session had for it. Worth a look if it recurs.

### Documentation corrected this session

- `AGENTS.md`: the Go capability paragraph and the testing section now link
  here instead of re-describing Go's surface inline, and no longer claim
  completeness this file does not support. Updated again after ConcurrencyPolicy
  and WarmDedupPolicy shipped, to stop listing them as missing.
- `CLAUDE.md`: same.
- `.claude/skills/laser-sdk-overview/SKILL.md`: same, for its Go blurb.
- `foreign/go/README.md`: same, plus a direct link at the top. Its A2A bridge
  section's "not ported yet" line no longer lists `submit_in`, and now
  documents `SubmitIn`/`SubmitInTo`/`CallToolIn`/`CallToolInTo`. New
  "Contract", "Scatter", and "Workflow" sections (after "Request and reply")
  document `Laser.Contract`/`ContractBuilder`, `Laser.Scatter`/`ScatterReport`,
  and `Laser.Workflow`/`StepHandle`/`WorkflowOutcome`/`CompensateWith`, each
  naming what is not yet ported. Its "Key-value" section now documents
  `Kv.Lease`/`TryLease`/`RenewLease`/`ReleaseLease`/`LeaseGuard`/`CasFenced`.

(If any of the four above still reads as more complete than this file after
a given session's edits, that is the bug to fix, not this file.)

## Next recommended task

**Priority 12: workflow engine slice 3, exclusive (fenced, at-most-once)
steps and `OnTimeout(Reassign)`.** The Kv lease/fence client this session
added (`foreign/go/laser/kv_lease.go`) was itself split out from this slice
once reading `laser/kv.go` found no lease/fence client at all, not a
primitive needing only threading through — read that row's own "Completed
this session" writeup before starting. Slices 1–2 (declared-steps DAG,
`step_local`, budgets, journal/replay/resume, saga compensation) shipped two
sessions ago; Priorities 2–11 are all done as of this session and the ones
before it. This is now the only workflow piece not gated on the "plain
code" form's own much larger effort, so it is the suggested next slice
ahead of that.

Suggested approach, in order:

1. Read `sdk/src/agent/workflow.rs`'s `run_exclusive`/`exclusive_attempt`/
   `acquire_lease`/`workflow_renewal_delay`/`workflow_lease_holder`
   (`#[cfg(feature = "kv")]`-gated, roughly lines 859–1150 as of the session
   that first scoped this; re-find them) start to finish before writing
   anything: a fenced lease is claimed per attempt via `Kv::lease_guard`,
   renewed at half its TTL while the dispatch/verify/journal are in flight
   via a `select!` against the in-flight contract, and released on every
   exit so a timeout reassignment can acquire immediately under a fresh
   holder with a bumped fence.
2. Read `foreign/go/laser/kv_lease.go` (this session's new file) fully
   before writing anything else: `Kv.LeaseGuard`/`.Renew`/`.Release`/
   `.Held`, `Kv.CasFenced`, and the "no ambiguous-mutation retry, no
   background release on drop" deviations noted in its own "Completed this
   session" writeup — the exclusive-step loop inherits both.
3. Decide how the renew-while-dispatching race translates to Go (Rust's
   `tokio::select!` over the in-flight contract future and a renewal timer
   — Go's equivalent is a `select` over a result channel and a
   `time.After`/`time.Timer`, the same shape `ConcurrencyPolicy`'s lane
   scheduler already uses) before writing the loop.
4. `StepHandle.Exclusive()`/`.ExclusiveIn(namespace)`/`.OnTimeout(Reassign)`
   need `Workflow.validate()` extended with Rust's own refusals: a local
   step cannot be exclusive, `AllCapable` cannot be exclusive (no single
   target to fence), and `OnTimeout(Reassign)` needs `Exclusive()` (the
   fence is what makes reassignment safe from the timed-out holder
   double-executing). Also decide whether `Workflow`/`StepHandle` should
   refuse running exclusive steps at all against a deployment lacking
   `FencedLeases` (`Kv.requireFencedLeases` already exists to check it).
5. Write the new tests first, reusing `agent_workflow_test.go`'s
   `workflowServer` fake and `kv_lease_test.go`'s `kvServer`/
   `fencedCapabilities` fakes as far as they fit — the two will likely need
   to compose (a fake serving both the managed command band for sessions/
   contracts and the lease ops), which neither file does alone today.
6. Implement, then run `laser`'s existing test suite and the full module
   suite (`go test ./... -count=1` and `-race`) before considering this
   done.
7. Update this file's matrix row for exclusive steps, and its "Completed
   this session" / "Next recommended task" sections.

## Next Session Handoff

**Read this whole file, then re-run the baseline before touching anything:**

```bash
cd foreign/go
gofmt -l . && go vet ./... && go build ./... && go test ./... -count=1
```

If any of those four fail, something changed since this file was last
updated — stop and reconcile before adding new work on top of an unknown
baseline.

**What changed this session**: `laser/agent_spawn.go`,
`laser/agent_spawn_test.go`, `laser/export_test.go`, `laser/agent_concurrency.go`,
`laser/agent_concurrency_test.go`, `laser/agent_warm_dedup.go`,
`laser/agent_warm_dedup_test.go`, `laser/agent_warm_dedup_internal_test.go`,
`laser/agent_submitted_pickup_test.go`, `laser/session_assemble.go`, and
`laser/session_assemble_test.go` were created; `laser/agent_runtime.go` got
additive edits (a `Concurrency` field, `Group`/`WarmDedup`/`WarmDedupPolicy`
fields on `RuntimeOptions`, matching constructor wiring and a validation check,
`pickUpSubmitted` called from `deliver`, and two new calls at the top of `Run`);
`laser/agent_runtime_test.go` had `GetConsumerOffset`/`PollMessages`/
`seedHistory`/`partitions` added to the shared `runtimeServer` fake (all
additive, zero value unchanged for every pre-existing test). On top of that,
`laser/session.go` got `Session.WithAncestry` plus `parent`/`root` fields, and
`Session.Agdx()` now applies them; `laser/session_submit.go` got
`Sessions.OpenChild`; `foreign/go/bridges/a2a_bridge.go` got `SubmitIn`/
`SubmitInTo` (and `publishCommand`/`submitFor` were reshaped into a shared
`submitAddressed` taking an optional ancestry); `foreign/go/bridges/a2a_rpc.go`
had its one other `publishCommand` call site updated for the new signature;
`foreign/go/bridges/mcp_bridge.go` got `CallToolIn`/`CallToolInTo` and a
`toolFailure` helper (`sendToolCall` was reshaped into a shared
`sendToolCallOn` taking an optional ancestry); `bridges/a2a_bridge_test.go`
and `bridges/mcp_bridge_test.go` got the five new tests listed above.
`laser/agent_contract.go` (new) added `Contract`/`ContractState`/
`ContractBuilder`/`Laser.Contract`; `laser/agent_contract_test.go` (new)
added its nine tests, the `contractServer` fake, and (for `Scatter`'s own
tests) the additive `repliesByTarget`/`silent` fields on that fake.
`laser/agent_scatter.go` (new) added `ScatterRequest`/`ScatterOutcome`/
`ScatterReport`/`Laser.Scatter`/`Laser.ScatterReport`; `laser/agent_scatter_test.go`
(new) added its seven tests. On top of that, `laser/agent_contract.go` got
`Correlation`/`IdempotencyKey`/`ResumeFrom` on `ContractBuilder`,
`Send`/`SendResolved` (the latter also naming the resolved agent),
`Laser.ReplyPositions`, and the `awaitContractReply` correlation-check fix
(see "Completed this session" for the bug this closes);
`laser/agent_contract_test.go`'s `contractServer` fake got the additive
`repliesByTarget`/`silent` fields (for `Scatter`'s tests) and its own tests
re-verified green after the `Correlation`/`ResumeFrom` rework.
`laser/agent_workflow.go` and `laser/agent_workflow_journal.go` (both new)
added the workflow engine's slice 1: `Workflow`/`StepHandle`/`StepFunc`/
`StepVerifier`/`WorkflowBudget`/`WorkflowOutcome`/`Laser.Workflow`, the
journal fold/replay/record, and `StepFailedError`/`JournalTruncatedError`
(`BudgetExceededError` is the *existing* `session_budget.go` type, reused,
not redefined — see "Completed this session"). `laser/agent_workflow_test.go`
(new) added its eight slice-1 tests and the topic-aware `workflowServer`
fake. On top of that, for slice 2: `agent_workflow.go` got
`StepHandle.CompensateWith`, the `compensate`/`runCompensation`/
`compensationRoute` functions, the `workflowEffect` type, and
`runStep`/`runDirected`/`runAllCapable` now return each step's agents as a
third value; `agent_workflow_journal.go` got the `journalStepCompensated`
kind and its fold case; `agent_workflow_test.go` got three more tests. All
of the above uncommitted as of this handoff — this session was explicitly
told not to commit. `AGENTS.md`, `CLAUDE.md`,
`.claude/skills/laser-sdk-overview/SKILL.md`, and `foreign/go/README.md`
were edited to correct overstated Go claims and link here;
`foreign/go/README.md` additionally dropped `submit_in` from its "not
ported yet" line, documents the four new bridge methods, and added new
"Contract", "Scatter", and "Workflow" sections (the last now also
documenting `CompensateWith`). On top of that, `laser/kv_lease.go` (new)
added the Kv lease/fence client (`Lease`/`TryLease`/`RenewLease`/
`ReleaseLease`/`LeaseGuard`/`CasFenced`/`Kv.requireFencedLeases`);
`laser/kv_lease_test.go` (new) added its ten tests and a
`fencedCapabilities()` helper extending `kv_test.go`'s existing
`kvCapabilities()`. All of the above uncommitted as of this handoff — this
session was explicitly told not to commit. `AGENTS.md`, `CLAUDE.md`,
`.claude/skills/laser-sdk-overview/SKILL.md`, and `foreign/go/README.md`
were edited to correct overstated Go claims and link here;
`foreign/go/README.md` additionally dropped `submit_in` from its "not
ported yet" line, documents the four new bridge methods, added new
"Contract", "Scatter", and "Workflow" sections (the last now also
documenting `CompensateWith`), and extended its existing "Key-value"
section with the new lease/fence methods. No other files changed. `bdd/go`
and `examples/go` are untouched.

**What was verified**: see "Verification run after implementation" (agent
lifecycle), "Verification run after ConcurrencyPolicy", "Verification run
after WarmDedupPolicy", "Verification run after submitted-session pickup",
"Verification run after `Session.Assemble()`", "Verification run after
`submit_in`/`call_tool_in`", "Verification run after
`Contract`/`ContractBuilder`", "Verification run after
`Scatter`/`ScatterReport`", "Verification run after the workflow engine
(slice 1)", "Verification run after saga compensation", and "Verification
run after the Kv lease/fence client" above. Everything green, including
eleven full-module race runs (one after each capability). One pre-existing,
unrelated flaky test (`TestGivenBestEffortWhenScatteredThenShouldTakeWhateverLandedByTheDeadline`,
from `Scatter`) failed once under full-package load and passed on every
retry — see that section's own note; not otherwise touched. Things *not*
verified: `SpawnAgent`'s actual consumer-group join against a live
deployment (see the "Not tested, by deliberate scope decision" note),
`SerialPerPartitionConcurrency` against a live deployment, `WarmDedupPolicy`'s
reads (`GetConsumerOffset`, `PollMessages` against a real partitioned topic
and a real `agent.dlq`), submitted-session pickup's `Session.Progress`/
`Laser.LeaseSession` calls, `Session.Assemble()`'s own `Laser.Context` fetch
and topic-locator reads, `submit_in`/`call_tool_in`'s own `OpenChild`/
command/reply publishes, `Contract`'s own live poll (now also its
`ResumeFrom` catch-up scan and `ReplyPositions`' tail read) against a real
partitioned reply topic, `Scatter`'s own concurrent branches over that same
live poll, the workflow engine's own journal read/write, resumed dispatch,
and compensation dispatch, and the Kv lease/fence client's own acquire/
renew/release/fenced-CAS round trips, against a live deployment — all
eleven gaps share the same reasoning: only ever run against hermetic fakes
(`runtimeServer`/`scopeServer`/`bridgeLog`/`replyingLog`/`contractServer`/
`workflowServer`/`kvServer`).

**What is NOT done, in priority order from here**:

1. Workflow engine slice 3, exclusive (fenced, at-most-once) steps and
   `OnTimeout(Reassign)` (see "Next recommended task" above for the
   detailed plan; slices 1–2 — declared-steps DAG, `step_local`, budgets,
   journal/replay/resume, saga compensation — are done, the Kv lease/fence
   client it depends on is now also done, and Priorities 2–11 before them
   are all done across this session and the ones before it).
2. Workflow engine slice 4, the "plain code" `run_with`/`WorkflowRun` form
   — the single hardest remaining piece per this session's own scoping
   read of `sdk/src/agent/workflow.rs`; expect its own multi-session plan.
3. Wiring a live test server into `bdd/go`'s CI (infrastructure, not a
   capability, but the highest-leverage single piece of unstarted work in
   this entire file — see "Verification gap" at the top).
4. The 13 BDD feature files with an existing capability but no Go runner
   (`clients`, `consistency`, `context_window`, `graph`, `kv_cas`,
   `kv_leases`, `memory`, `memory_correction`, `operational_sql`, `query`,
   `query_managed`, `schema_registry`, `warm_dedup`) — `kv_leases` now has
   its client capability too — plus `workflow` and `workflow_local`, whose
   saga-compensation scenarios this session's slice 2 should now actually
   cover; none of the three has a Go BDD runner yet to prove it, which is
   itself just item 3 above (no live server) plus the ordinary work of
   writing the runner.
5. The 18 missing `examples/go/` scenarios.
6. `A2ACaller`'s missing `SubmitIn`/`SubmitInTo` (and its pre-existing
   `SubmitTo`/`SubmitWithHops` gap) — narrower in scope than any of the above,
   noted in the capability matrix rather than folded into this session's
   `submit_in`/`call_tool_in` work.
7. `Contract`'s own narrowed scope versus Rust: no shared per-topic cached
   reply hub (one poller per call instead), no per-agent inbox resolution,
   and `excluding` (exclusive-step reassignment only) still unported pending
   workflow slice 3. `Scatter` and the workflow engine inherit the same
   narrowing, since both dispatch through `Contract`.
8. The workflow journal's Go-native (not cross-SDK) CBOR encoding — a Go-
   started run resumes only on Go (see the workflow matrix rows and the
   README "Workflow" section).
9. The Kv lease/fence client's own narrowed scope versus Rust: no
   ambiguous-mutation retry (shared by every Go kv operation already, not a
   new gap this session introduced) and no background release on a
   forgotten `LeaseGuard` (Go has no destructor to hook one to) — see that
   row's "Completed this session" writeup.
10. The pre-existing, unreproduced-on-retry flaky test noted above
    (`TestGivenBestEffortWhenScatteredThenShouldTakeWhateverLandedByTheDeadline`)
    — worth a look if it recurs under CI load, not urgent on a single
    unreproduced local failure.

**Known failures**: none. Every test that exists and runs, passes. The gap is
coverage and live-server verification, not red tests. Three real bugs were
found and fixed during the Agent-lifecycle-through-`Session.Assemble()` work
earlier this session, all before any test ran against them except the second
(a hang in `SerialPerPartitionConcurrency`, a destructive-probe ordering bug
in a `WarmDedupPolicy` test, and a frontier map-zero-value bug in
`Session.Assemble()`) — see "A hang found and fixed", the
`TestGivenMoreHistoryThanTheWindowsCapacityWhenWarmDedupRunsThenShouldKeepOnlyTheMostRecent`
row, and the `Session.Assemble()` "Completed this session" writeup above. A
fourth was found and fixed later in the same session, while extending
`Contract` for the workflow engine's resume support: `awaitContractReply`
matched a reply without ever checking its own correlation, relying solely on
`ExpectedReply.Accepts` (which has no correlation field to check) — see the
workflow engine's own "Completed this session" writeup above. `submit_in`/
`call_tool_in`, `Contract`'s original shipment, and `Scatter` surfaced no
bugs of their own; none remain known.

**Blockers**: none technical. `producer.idempotent`/`producer.sharding` are
blocked on the Apache Iggy Go SDK and explicitly out of scope per direction
received this session.

**Do not commit, push, or open a PR from this file's instructions alone** —
this session was explicitly told not to. Confirm with whoever picks this up
next before doing so.
