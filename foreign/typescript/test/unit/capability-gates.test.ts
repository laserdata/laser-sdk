import assert from "node:assert/strict"
import { test } from "node:test"
import {
  type Capabilities,
  isOpenOnly,
  managedCapabilitiesFrom,
  OPEN_CAPABILITIES
} from "../../src/client/capabilities.js"
import { code, isVersionSkew } from "../../src/client/error-classify.js"
import { UnsupportedError } from "../../src/client/errors.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import { BARE_SCOPE } from "../../src/client/resource-scope.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"
import { MaterializeRequest } from "../../src/managed/materialize.js"
import { Projections } from "../../src/managed/projections.js"
import { ContentType } from "../../src/wire/content.js"
import type { Projection } from "../../src/wire/control.js"

const INJECTED: Capabilities = { ...OPEN_CAPABILITIES, managed: true, materializations: true }

const PROJECTION: Projection = {
  id: "reading.v1" as Projection["id"],
  name: "readings",
  version: 1,
  kind: { kind: "row" },
  history: { kind: "append" },
  contentType: ContentType.Json,
  extraction: { fields: [], inlinePayload: false },
  inlinePayloadDefault: false
}

void test("given_any_capability_beyond_the_open_set_when_checked_then_should_not_be_open_only", () => {
  for (const [name, value] of Object.entries(OPEN_CAPABILITIES)) {
    if (typeof value !== "boolean") continue
    const flipped = { ...OPEN_CAPABILITIES, [name]: !value }
    assert.equal(isOpenOnly(flipped), false, `${name} flipped`)
  }
  assert.equal(isOpenOnly({ ...OPEN_CAPABILITIES, hello: "failed" }), true)
})

void test("given_injected_materializations_when_materializing_then_should_pass_the_capability_gate", async () => {
  let published = 0
  const request = MaterializeRequest.create(
    {
      scope: BARE_SCOPE,
      capabilities: () => Promise.resolve(INJECTED),
      publish: () => {
        published += 1
        return Promise.resolve()
      },
      statusInfo: () => Promise.resolve(undefined)
    },
    "shop",
    "orders"
  )
  await request.apply().catch(() => undefined)
  assert.ok(published > 0, "the setup must reach its first control command")
})

void test("given_injected_materializations_when_reading_status_then_should_pass_the_capability_gate", async () => {
  let sent = 0
  const projections = Projections.create(
    {
      sendManaged: () => {
        sent += 1
        return Promise.reject(new Error("not this test"))
      }
    },
    () => Promise.resolve(INJECTED),
    () => Promise.resolve()
  )
  const refusal = await projections.status("orders").catch((error: unknown) => error)
  assert.ok(!(refusal instanceof UnsupportedError), "the gate must read the capability set")
  assert.equal(sent, 1)
})

void test("given_another_control_op_version_when_publishing_then_should_report_version_skew", async () => {
  const laser = (
    await Laser.fromClient({ clientProvider: () => Promise.resolve({}) } as unknown as IggyClient)
  ).withCapabilities(
    managedCapabilitiesFrom({
      versions: { query: 1, control: 2, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
      backends: []
    })
  )
  laser[INTERNAL_TRANSPORT]().sendManaged = () => Promise.reject(new Error("must not send"))
  const error = await laser
    .projections()
    .register(PROJECTION)
    .catch((failure: unknown) => failure)
  assert.ok(isVersionSkew(error), String(error))
  assert.deepEqual(code(error), { kind: "known", name: "VersionSkew" })
})
