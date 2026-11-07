import assert from "node:assert/strict"
import { test } from "node:test"
import { Materialization, MaterializeRequest } from "../../src/managed/materialize.js"
import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { BARE_SCOPE } from "../../src/client/resource-scope.js"
import { InvalidError, QueryExecutionError, UnsupportedError } from "../../src/client/errors.js"
import { code } from "../../src/client/error-classify.js"
import type { BindingStatus, ProjectionInfo } from "../../src/wire/browse.js"
import {
  ProjectionBindingBuilder,
  ProjectionBuilder,
  type ControlCommand
} from "../../src/wire/control.js"

function managedRequest(published: ControlCommand[]): MaterializeRequest {
  return MaterializeRequest.create(
    {
      scope: BARE_SCOPE,
      capabilities: () =>
        Promise.resolve({ ...OPEN_CAPABILITIES, managed: true, materializations: true }),
      publish: (command) => {
        published.push(command)
        return Promise.resolve()
      },
      statusInfo: () => Promise.resolve(undefined)
    },
    "shop",
    "orders"
  )
}

function status(head: bigint, applied: bigint): BindingStatus {
  return {
    history: { kind: "append" },
    source: { stream: "shop", topic: "orders" },
    index: "orders",
    realization: "realized",
    sourceIncarnation: 7n,
    partitions: [{ partitionId: 0, headOffset: head, appliedOffset: applied, firstOffset: 0n }],
    holds: [],
    acceptedGaps: [],
    skippedRecords: 0n
  }
}

void test("given_a_continuously_growing_head_when_waiting_then_should_use_the_first_captured_target", async () => {
  let reads = 0
  const materialization = Materialization.create(() =>
    Promise.resolve([++reads === 1 ? status(5n, 1n) : status(100n, 5n)])
  )
  const outcome = await materialization.wait("caught_up", 1000)
  assert.equal(outcome.kind, "ready")
  assert.equal(reads, 2)
})

void test("given_a_held_partition_when_waiting_then_should_report_blocked_without_hiding_the_reason", async () => {
  const held = {
    ...status(5n, 1n),
    holds: [{ partitionId: 0, offset: 2n, cause: "schema" as const, reason: "invalid schema" }]
  }
  const materialization = Materialization.create(() => Promise.resolve([held]))
  assert.deepEqual(await materialization.wait("caught_up", 1000), {
    kind: "blocked",
    status: [held]
  })
})

void test("given_an_unanswered_status_when_waiting_then_should_respect_the_deadline", async () => {
  const materialization = Materialization.create(() => new Promise(() => undefined))
  assert.deepEqual(await materialization.wait("caught_up", 5), { kind: "timed_out", status: [] })
})

void test("given_original_iggy_when_materializing_then_should_refuse_before_publishing", async () => {
  let published = false
  const request = MaterializeRequest.create(
    {
      scope: BARE_SCOPE,
      capabilities: () => Promise.resolve(OPEN_CAPABILITIES),
      publish: () => {
        published = true
        return Promise.resolve()
      },
      statusInfo: () => Promise.resolve(undefined)
    },
    "shop",
    "orders"
  )
  await assert.rejects(() => request.apply(), UnsupportedError)
  assert.equal(published, false)
})

void test("given_a_content_change_without_a_pinned_identity_when_set_then_should_mint_a_new_operation", () => {
  const request = managedRequest([])
  const first = request.operation()
  request.index("orders_rows")
  assert.notEqual(request.operation(), first)
  request.operationId(7n).retention({ kind: "keep" })
  assert.equal(request.operation(), 7n)
})

void test("given_a_typed_projection_without_a_projection_id_when_applied_then_should_keep_its_id", async () => {
  const published: ControlCommand[] = []
  await managedRequest(published)
    .typed(new ProjectionBuilder("order.v2").field("total").build())
    .apply()
  const [register, bind] = published
  assert.ok(register?.kind === "registerProjection" && bind?.kind === "applyBinding")
  assert.equal(register.projection.id, "order.v2")
  assert.equal(bind.binding.defaultProjection, "order.v2")
})

void test("given_a_keyed_projection_without_a_delete_header_when_applied_then_should_register_it_as_built", async () => {
  const published: ControlCommand[] = []
  const key = { kind: "header", name: "order_id" } as const
  await managedRequest(published)
    .typed(new ProjectionBuilder("order.v1").field("total").keyed(key).build())
    .apply()
  const [register, bind] = published
  assert.ok(register?.kind === "registerProjection" && bind?.kind === "applyBinding")
  assert.deepEqual(register.projection.history, { kind: "keyed", key })
  assert.deepEqual(bind.binding.retention, { kind: "keep" })
})

void test("given_a_binding_the_backend_would_refuse_when_applied_then_should_refuse_before_registering", async () => {
  const published: ControlCommand[] = []
  const keyed = new ProjectionBuilder("order.v1")
    .field("total")
    .keyed({ kind: "header", name: "order_id" }, "deleted")
    .build()
  await assert.rejects(
    () => managedRequest(published).typed(keyed).retention({ kind: "mirrorLog" }).apply(),
    InvalidError
  )
  await assert.rejects(
    () =>
      managedRequest(published)
        .typed(new ProjectionBuilder("order.v1").field("total").build())
        .oversize("reference")
        .apply(),
    InvalidError
  )
  assert.deepEqual(published, [])
})

void test("given_an_absent_projection_when_waiting_then_should_report_not_found", async () => {
  const materialization = Materialization.create(() => Promise.resolve(undefined))
  assert.deepEqual(await materialization.wait("realized", 300), { kind: "not_found" })
})

void test("given_a_status_read_refused_as_unauthorized_when_waiting_then_should_end_with_that_error", async () => {
  const refusal = new QueryExecutionError(
    "unauthorized: the caller may read no index of projection `orders`",
    {
      kind: "unauthorized",
      message: "the caller may read no index of projection `orders`"
    }
  )
  const materialization = Materialization.create(() => Promise.reject(refusal))
  const started = Date.now()
  await assert.rejects(materialization.wait("realized", 30_000), (error: unknown) => {
    assert.equal(error, refusal)
    assert.deepEqual(code(error), { kind: "known", name: "Forbidden" })
    return true
  })
  assert.ok(Date.now() - started < 5_000, "the wait must not run out its timeout")
})

void test("given_a_permanent_realization_failure_when_waiting_then_should_report_failed", async () => {
  const failed: BindingStatus = {
    ...status(5n, 0n),
    realization: "pending",
    failure: "schema"
  }
  const materialization = Materialization.create(() => Promise.resolve([failed]))
  assert.deepEqual(await materialization.wait("realized", 300), {
    kind: "failed",
    status: [failed]
  })
  const transient = Materialization.create(() =>
    Promise.resolve([{ ...failed, failure: "transient" as const }])
  )
  assert.equal((await transient.wait("realized", 120)).kind, "timed_out")
})

void test("given_a_source_recreated_during_the_wait_when_caught_up_then_should_measure_the_new_incarnation", async () => {
  const reads = [
    status(5n, 1n),
    { ...status(3n, 0n), sourceIncarnation: 8n },
    { ...status(3n, 3n), sourceIncarnation: 8n }
  ]
  const materialization = Materialization.create(() => {
    const next = reads.length > 1 ? reads.shift() : reads[0]
    return Promise.resolve(next === undefined ? [] : [next])
  })
  assert.deepEqual(await materialization.wait("caught_up", 2000), {
    kind: "ready",
    status: [{ ...status(3n, 3n), sourceIncarnation: 8n }]
  })
})

void test("given_a_slow_materialization_when_waiting_then_should_back_off_between_reads", async () => {
  let reads = 0
  const materialization = Materialization.create(() => {
    reads += 1
    return Promise.resolve([status(5n, 1n)])
  })
  assert.equal((await materialization.wait("caught_up", 600)).kind, "timed_out")
  assert.ok(
    reads <= 5,
    `pauses of 50, 100, 200 and 400 ms leave at most five reads in 600 ms, got ${String(reads)}`
  )
})

void test("given_a_source_moved_to_another_index_when_waiting_on_the_applied_handle_then_should_report_not_found", async () => {
  const moved: ProjectionInfo = {
    projection: new ProjectionBuilder("orders").record().build(),
    bindings: [
      new ProjectionBindingBuilder()
        .source("shop", "orders")
        .index("orders_v2")
        .allow("orders")
        .defaultProjection("orders")
        .build()
    ],
    status: [{ ...status(5n, 5n), index: "orders_v2" }]
  }
  const request = MaterializeRequest.create(
    {
      scope: BARE_SCOPE,
      capabilities: () =>
        Promise.resolve({ ...OPEN_CAPABILITIES, managed: true, materializations: true }),
      publish: () => Promise.resolve(),
      statusInfo: () => Promise.resolve(moved)
    },
    "shop",
    "orders"
  )
  const applied = await request.apply()
  assert.deepEqual(await applied.wait("realized", 300), { kind: "not_found" })
  assert.equal(await applied.status(), undefined)
  const attached = request.attach("orders")
  assert.equal((await attached.wait("realized", 300)).kind, "ready")
})
