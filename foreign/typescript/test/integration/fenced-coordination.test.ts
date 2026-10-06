import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"

import { isLeaseLost } from "../../src/client/error-classify.js"
import { KvExecutionError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { FencedLeaseClient } from "../../src/managed/coordination.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

void test("given_a_dedicated_fenced_client_when_operations_execute_then_should_preserve_leases_barriers_and_cas_preconditions", async (context) => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  if (!(await laser.capabilities()).kv.fencedLeases) {
    context.skip("this deployment does not serve fenced leases")
    return
  }
  const namespace = `coord-${randomUUID()}`
  const fenceKey = new TextEncoder().encode("source-owner")
  const stateKey = new TextEncoder().encode("source-state")
  const value = new TextEncoder().encode("cursor-7")
  const holderId = "typescript-worker"
  await using client =
    FencedLeaseClient.connectDedicated(CONNECTION_STRING).withAttemptTimeout(5_000)
  const granted = await client.acquire(
    client.prepareAcquire({
      namespace,
      key: fenceKey,
      holderId,
      leaseTtlMicros: 30_000_000n
    })
  )
  const renewed = await client.renew(
    client.prepareRenew({
      namespace,
      key: fenceKey,
      holderId,
      leaseToken: granted.token,
      leaseTtlMicros: 30_000_000n
    })
  )
  assert.equal(renewed.token, granted.token)
  const cas = {
    namespace,
    key: stateKey,
    value,
    expect: { kind: "absent" as const },
    fenceNamespace: namespace,
    fenceKey,
    fenceToken: renewed.token
  }
  const version = await client.casFenced(client.prepareCasFenced(cas))
  const entry = await client.get({ namespace, key: stateKey, minPosition: renewed.position })
  assert.ok(entry !== undefined)
  assert.deepEqual(entry.value, value)
  assert.equal(entry.version, version)
  assert.equal(
    await client.release(
      client.prepareRelease({
        namespace,
        key: fenceKey,
        holderId,
        leaseToken: renewed.token
      })
    ),
    true
  )
  await assert.rejects(
    client.casFenced(
      client.prepareCasFenced({
        ...cas,
        expect: { kind: "match", version }
      })
    ),
    (error: unknown) => error instanceof KvExecutionError && isLeaseLost(error)
  )
  await laser.kv(namespace).delete(stateKey)
})
