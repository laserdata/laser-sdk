import assert from "node:assert/strict"
import { test } from "node:test"
import { type BlobStore, checkIn, resolveBody } from "../../src/blob.js"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom } from "../../src/client/capabilities.js"
import { CancelledError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import {
  FencedLeaseClient,
  LeaseCoordinator,
  type ManagedKvTransport
} from "../../src/managed/coordination.js"
import { ForkHandle } from "../../src/managed/forks.js"
import { GraphHandle } from "../../src/managed/graph.js"
import { Kv } from "../../src/managed/kv.js"
import { Materialization, MaterializeRequest } from "../../src/managed/materialize.js"
import { Bindings, Projections, Schemas } from "../../src/managed/projections.js"
import { QueryRequest } from "../../src/managed/query.js"
import { deleteRole, listRoles, whoami } from "../../src/managed/rbac.js"
import { Watch } from "../../src/managed/watch.js"
import { BARE_SCOPE } from "../../src/client/resource-scope.js"
import { KvKeyRegistry, SigningKey } from "../../src/signing.js"
import { Json } from "../../src/stream/codecs.js"
import { encodeBodyRef, newBodyRef } from "../../src/wire/agent.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { KvCasCommand } from "../../src/wire/commands.js"
import { ProjectionBuilder } from "../../src/wire/control.js"
import { feature } from "../../src/wire/hello.js"
import { BackendResourceId } from "../../src/wire/ids.js"
import { type KvOutcome, encodeKvReply } from "../../src/wire/kv.js"
import type { Query, QueryResult } from "../../src/wire/query.js"

const CAPS: Capabilities = managedCapabilitiesFrom({
  versions: {
    query: 1,
    control: 1,
    kv: 1,
    fork: 1,
    agent: 1,
    graph: 1,
    features:
      feature.KV_CAS |
      feature.KV_CAS_FENCED |
      feature.KV_FENCED_LEASES |
      feature.WATCH |
      feature.AUTHZ |
      feature.MATERIALIZATIONS
  },
  backends: []
})
const KEY = Uint8Array.of(1)
const ACQUIRE = { namespace: "coord", key: KEY, leaseTtlMicros: 2_000_000n, holderId: "worker" }

void test("given_an_aborted_signal_when_a_managed_call_starts_then_should_reject_with_cancelled_and_touch_nothing", async () => {
  const touched: string[] = []
  const transport = {
    sendManaged(code: number): Promise<Uint8Array> {
      touched.push(`managed ${String(code)}`)
      return Promise.reject(new Error("an aborted call must not send"))
    }
  }
  const coordination: ManagedKvTransport = {
    ready: () => {
      touched.push("coordination ready")
      return Promise.resolve()
    },
    send: () => {
      touched.push("coordination send")
      return Promise.reject(new Error("an aborted call must not send"))
    },
    reset: () => Promise.resolve()
  }
  const publish = (): Promise<void> => {
    touched.push("publish")
    return Promise.reject(new Error("an aborted call must not publish"))
  }
  const capabilities = (): Promise<Capabilities> => {
    touched.push("capabilities")
    return Promise.resolve(CAPS)
  }
  const blobs: BlobStore = {
    put: () => {
      touched.push("blob put")
      return Promise.resolve("blob-0")
    },
    get: () => {
      touched.push("blob get")
      return Promise.resolve(new Uint8Array())
    }
  }
  const client = new FencedLeaseClient(coordination)
  const store = Kv.create(transport, capabilities, "sessions", new LeaseCoordinator(client))
  const fork = ForkHandle.create(transport, capabilities, "draft")
  const graph = GraphHandle.create(transport, capabilities, "people")
  const projections = Projections.create(transport, capabilities, publish)
  const schemas = Schemas.create(transport, capabilities, publish)
  const bindings = Bindings.create(publish)
  const registry = new KvKeyRegistry({
    kv: (namespace: string) => Kv.create(transport, capabilities, namespace)
  } as unknown as Laser)
  const watch = Watch.create(capabilities, () => {
    touched.push("feed")
    return Promise.reject(new Error("an aborted call must not open the feed"))
  })
  const materialize = MaterializeRequest.create(
    { scope: BARE_SCOPE, capabilities, publish, statusInfo: () => Promise.resolve(undefined) },
    "shop",
    "orders"
  )
  const query = QueryRequest.create("readings", () => {
    touched.push("query")
    return Promise.reject(new Error("an aborted call must not query"))
  })
  const signal = AbortSignal.abort()
  const calls: readonly (readonly [string, () => Promise<unknown>])[] = [
    ["kv get entry", () => store.getEntry(KEY, { signal })],
    ["kv delete", () => store.delete(KEY, { signal })],
    ["kv expire", () => store.expire(KEY, 1_000, { signal })],
    ["kv patch", () => store.patch(KEY, KEY, { signal })],
    ["kv get many", () => store.getMany([KEY], { signal })],
    ["kv set", () => store.set(KEY).bytes(KEY).send({ signal })],
    ["kv cas", () => store.set(KEY).bytes(KEY).expectAbsent().commit({ signal })],
    [
      "kv cas fenced",
      () => store.casFenced(KEY, "fence", KEY, 1n).bytes(KEY).expectAbsent().commit({ signal })
    ],
    ["kv scan", () => store.scan().fetch({ signal })],
    ["kv copy", () => store.copyTo(KEY, Uint8Array.of(2)).send({ signal })],
    ["kv delete many", () => store.deleteMany().prefix(KEY).send({ signal })],
    ["kv lease", () => store.lease(KEY, "worker", 2_000, { signal })],
    ["kv renew", () => store.renewLease(KEY, "worker", 1n, 2_000, { signal })],
    ["kv release", () => store.release(KEY, "worker", 1n, { signal })],
    ["fenced acquire", () => client.acquire(client.prepareAcquire(ACQUIRE), { signal })],
    ["fenced get", () => client.get({ namespace: "coord", key: KEY }, { signal })],
    ["fork promote", () => fork.promote({ signal })],
    ["fork create", () => fork.create().send({ signal })],
    ["fork put", () => fork.putRow("orders", 0, 0n).send({ signal })],
    ["graph relink", () => graph.relink("person:a", "manager", "person:b", { signal })],
    ["projection register", () => projections.register(projection(), { signal })],
    ["projection status", () => projections.status("orders", { signal })],
    ["projection list", () => projections.list().fetch({ signal })],
    ["schema list", () => schemas.list().fetch({ signal })],
    ["schema drop", () => schemas.drop(1, { signal })],
    [
      "binding remove",
      () => bindings.remove({ stream: "shop", topic: "orders" }, undefined, { signal })
    ],
    ["key enroll", () => registry.enroll("agent-a", signingKey().verifyingKey(), { signal })],
    ["key registry", () => registry.registry({ signal })],
    ["watch records", () => watch.records({ signal })],
    ["materialize apply", () => materialize.apply({ signal })],
    ["query fetch", () => query.fetch({ signal })],
    ["query fetch all", () => query.fetchAll({ signal })],
    ["query rows next", () => query.maxRows(10).rows().next({ signal })],
    ["query typed rows next", () => query.maxRows(10).rowsTyped(new Json()).next({ signal })],
    ["whoami", () => whoami(transport, CAPS, { signal })],
    ["list roles", () => listRoles(transport, CAPS, { signal })],
    ["delete role", () => deleteRole(transport, CAPS, "reader", undefined, { signal })],
    ["check in", () => checkIn(blobs, 0, KEY, { signal })],
    ["resolve body", () => resolveBody(blobs, bodyRef(KEY), { signal })]
  ]
  for (const [what, call] of calls) await assert.rejects(call(), CancelledError, what)
  assert.deepEqual(touched, [])
})

void test(
  "given_a_kv_compare_and_swap_waiting_for_its_reply_when_the_signal_aborts_then_should_reject_with_cancelled",
  { timeout: 5_000 },
  async () => {
    const sent: number[] = []
    // The reply never comes, and the timer keeps the test alive while it waits.
    let held: ReturnType<typeof setTimeout> | undefined
    const store = Kv.create(
      {
        sendManaged(code) {
          sent.push(code)
          return new Promise<Uint8Array>(() => {
            held = setTimeout(() => undefined, 60_000)
          })
        }
      },
      () => Promise.resolve(CAPS),
      "sessions"
    )
    const controller = new AbortController()
    const commit = store.set(KEY).bytes(KEY).expectAbsent().commit({ signal: controller.signal })
    await new Promise((resolve) => setImmediate(resolve))
    assert.deepEqual(sent, [KvCasCommand.code])
    controller.abort()
    try {
      await assert.rejects(commit, CancelledError)
    } finally {
      clearTimeout(held)
    }
  }
)

void test("given_a_kv_scan_walk_when_the_signal_aborts_during_a_page_then_should_reject_with_cancelled_and_read_no_further_page", async () => {
  const controller = new AbortController()
  const sent: number[] = []
  const store = Kv.create(
    {
      sendManaged(code) {
        sent.push(code)
        controller.abort()
        return Promise.resolve(
          okFrame({
            kind: "page",
            page: {
              items: [{ key: KEY, value: KEY, version: 1n }],
              page: sent.length,
              totalPages: 3,
              totalResults: 3
            }
          })
        )
      }
    },
    () => Promise.resolve(CAPS),
    "sessions"
  )
  await assert.rejects(store.scan().entries({ signal: controller.signal }), CancelledError)
  assert.equal(sent.length, 1)
})

void test(
  "given_an_ambiguous_lease_acquisition_when_the_signal_aborts_while_it_repeats_then_should_reject_with_cancelled_and_send_no_repeat",
  { timeout: 5_000 },
  async () => {
    const controller = new AbortController()
    const frames: Uint8Array[] = []
    const coordinator = new LeaseCoordinator(
      new FencedLeaseClient({
        send: (_code, frame) => {
          frames.push(frame)
          setImmediate(() => {
            controller.abort()
          })
          return Promise.resolve(pendingReply())
        },
        reset: () => Promise.resolve()
      })
    )
    const store = Kv.create(
      { sendManaged: () => Promise.reject(new Error("leases ride the coordination connection")) },
      () => Promise.resolve(CAPS),
      "sessions",
      coordinator
    )
    await assert.rejects(
      store.lease(KEY, "worker", 2_000, { signal: controller.signal }),
      CancelledError
    )
    await new Promise((resolve) => setTimeout(resolve, 250))
    assert.equal(frames.length, 1, "no repeat is sent after the abort")
  }
)

void test(
  "given_a_fenced_lease_acquisition_waiting_for_its_reply_when_the_signal_aborts_then_should_reject_with_cancelled_and_keep_the_connection",
  { timeout: 5_000 },
  async () => {
    const sent: number[] = []
    let resets = 0
    let held: ReturnType<typeof setTimeout> | undefined
    const client = new FencedLeaseClient({
      send: (code) => {
        sent.push(code)
        return new Promise<Uint8Array>(() => {
          held = setTimeout(() => undefined, 60_000)
        })
      },
      reset: () => {
        resets += 1
        return Promise.resolve()
      }
    })
    const controller = new AbortController()
    const acquisition = client.acquire(client.prepareAcquire(ACQUIRE), {
      signal: controller.signal
    })
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(sent.length, 1)
    controller.abort()
    try {
      await assert.rejects(acquisition, CancelledError)
      assert.equal(resets, 0, "the abandoned reply still arrives whole on the connection")
    } finally {
      clearTimeout(held)
    }
  }
)

void test(
  "given_a_materialization_wait_when_the_signal_aborts_between_status_reads_then_should_reject_with_cancelled_and_read_no_more",
  { timeout: 5_000 },
  async () => {
    const controller = new AbortController()
    let reads = 0
    const materialization = Materialization.create(() => {
      reads += 1
      setImmediate(() => {
        controller.abort()
      })
      return Promise.resolve([])
    })
    await assert.rejects(
      materialization.wait("applied", 2_000, { signal: controller.signal }),
      CancelledError
    )
    await new Promise((resolve) => setTimeout(resolve, 120))
    assert.equal(reads, 1)
  }
)

void test(
  "given_a_materialization_publishing_its_setup_when_the_signal_aborts_then_should_reject_with_cancelled_after_handing_the_signal_down",
  { timeout: 5_000 },
  async () => {
    const handed: (AbortSignal | undefined)[] = []
    let held: ReturnType<typeof setTimeout> | undefined
    const request = MaterializeRequest.create(
      {
        scope: BARE_SCOPE,
        capabilities: () => Promise.resolve(CAPS),
        publish: (_command, _stream, _operationId, options) => {
          handed.push(options?.signal)
          return new Promise<void>(() => {
            held = setTimeout(() => undefined, 60_000)
          })
        },
        statusInfo: () => Promise.resolve(undefined)
      },
      "shop",
      "orders"
    )
    const controller = new AbortController()
    const apply = request.apply({ signal: controller.signal })
    await new Promise((resolve) => setImmediate(resolve))
    assert.deepEqual(handed, [controller.signal])
    controller.abort()
    try {
      await assert.rejects(apply, CancelledError)
    } finally {
      clearTimeout(held)
    }
  }
)

void test(
  "given_a_projection_register_waiting_on_its_publish_when_the_signal_aborts_then_should_reject_with_cancelled_after_handing_the_signal_down",
  { timeout: 5_000 },
  async () => {
    const handed: (AbortSignal | undefined)[] = []
    let held: ReturnType<typeof setTimeout> | undefined
    const projections = Projections.create(
      { sendManaged: () => Promise.reject(new Error("a register publishes a control command")) },
      () => Promise.resolve(CAPS),
      (_command, _stream, options) => {
        handed.push(options?.signal)
        return new Promise<void>(() => {
          held = setTimeout(() => undefined, 60_000)
        })
      }
    )
    const controller = new AbortController()
    const register = projections.register(projection(), { signal: controller.signal })
    await new Promise((resolve) => setImmediate(resolve))
    assert.deepEqual(handed, [controller.signal])
    controller.abort()
    try {
      await assert.rejects(register, CancelledError)
    } finally {
      clearTimeout(held)
    }
  }
)

void test(
  "given_a_query_waiting_for_its_reply_when_the_signal_aborts_then_should_reject_with_cancelled_after_handing_the_signal_down",
  { timeout: 5_000 },
  async () => {
    const handed: (AbortSignal | undefined)[] = []
    let held: ReturnType<typeof setTimeout> | undefined
    const request = QueryRequest.create("readings", (_query, options) => {
      handed.push(options?.signal)
      return new Promise<QueryResult>(() => {
        held = setTimeout(() => undefined, 60_000)
      })
    })
    const controller = new AbortController()
    const fetch = request.fetch({ signal: controller.signal })
    await new Promise((resolve) => setImmediate(resolve))
    assert.deepEqual(handed, [controller.signal])
    controller.abort()
    try {
      await assert.rejects(fetch, CancelledError)
    } finally {
      clearTimeout(held)
    }
  }
)

void test("given_a_query_walk_when_the_signal_aborts_during_a_page_then_should_reject_with_cancelled_and_send_no_further_page", async () => {
  const controller = new AbortController()
  const sent: Query[] = []
  const request = QueryRequest.create("readings", (query) => {
    sent.push(query)
    controller.abort()
    return Promise.resolve(result(query, sent.length < 3))
  })
  await assert.rejects(request.fetchAll({ signal: controller.signal }), CancelledError)
  assert.equal(sent.length, 1)
})

void test(
  "given_a_claim_check_resolving_its_body_when_the_signal_aborts_then_should_reject_with_cancelled",
  { timeout: 5_000 },
  async () => {
    const fetched: string[] = []
    let held: ReturnType<typeof setTimeout> | undefined
    const store: BlobStore = {
      put: () => Promise.reject(new Error("a resolve only reads")),
      get: (reference) => {
        fetched.push(reference)
        return new Promise<Uint8Array>(() => {
          held = setTimeout(() => undefined, 60_000)
        })
      }
    }
    const controller = new AbortController()
    const resolve = resolveBody(store, bodyRef(KEY), { signal: controller.signal })
    await new Promise((resolved) => setImmediate(resolved))
    assert.deepEqual(fetched, ["blob-0"])
    controller.abort()
    try {
      await assert.rejects(resolve, CancelledError)
    } finally {
      clearTimeout(held)
    }
  }
)

function okFrame(outcome: KvOutcome): Uint8Array {
  const value = encodeKvReply({ kind: "ok", outcome })
  if (!(value instanceof Map)) throw new Error("expected a map-shaped reply")
  return encodeNamed(value)
}

// The reply of a mutation the backend accepted but has no outcome for yet.
function pendingReply(): Uint8Array {
  return encodeNamed(
    new Map<string, unknown>([
      ["code", "pending"],
      ["message", "the outcome is not known yet"]
    ])
  )
}

function projection(): ReturnType<ProjectionBuilder["build"]> {
  return new ProjectionBuilder("orders").record().build()
}

function signingKey(): SigningKey {
  return SigningKey.fromBytes(new Uint8Array(32).fill(7))
}

function bodyRef(payload: Uint8Array): Uint8Array {
  return encodeNamed(
    encodeBodyRef(newBodyRef("blob-0", BigInt(payload.byteLength), new Uint8Array(32)))
  )
}

function result(query: Query, hasMore: boolean): QueryResult {
  return {
    fields: [{ id: 1, name: "id", required: true, fieldType: { kind: "string" } }],
    rows: [{ values: [{ kind: "string", value: "o-7" }] }],
    page: { offset: query.page.offset ?? 0n, limit: query.page.limit, hasMore },
    context: {
      executionId: query.executionId,
      engine: { name: "embedded", version: "1" },
      resolvedTarget: {
        kind: "operational",
        index: "readings",
        backendResourceId: BackendResourceId.fromU128(1n),
        backendGeneration: 1n,
        runtimeConfigurationRevision: 1n
      },
      requestedConsistency: query.consistency,
      deliveredConsistency: query.consistency,
      truncated: false,
      elapsedMicros: 1n,
      scannedBytes: 1n,
      producedBytes: 1n,
      rowCount: 1n,
      frontier: []
    }
  }
}
