import assert from "node:assert/strict"
import { createServer, type Server } from "node:net"
import { test } from "node:test"
import { connectOptions } from "../../src/client/connect-options.js"
import { ConfigError, TimeoutError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"
import { encodeBackendAnnounce, newOpVersions } from "../../src/wire/hello.js"
import { defaultWireTopology } from "../../src/wire/topology.js"

// Accepts every connection and reads without replying, the shape of a server that
// completed the TCP handshake but never answers the login request.
async function silentServer(): Promise<{ readonly server: Server; readonly port: number }> {
  const server = createServer((socket) => {
    socket.on("data", () => undefined)
    socket.on("error", () => undefined)
  })
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))
  const address = server.address()
  if (address === null || typeof address === "string") throw new Error("no TCP address")
  return { server, port: address.port }
}

async function closedPort(): Promise<number> {
  const { server, port } = await silentServer()
  await new Promise<void>((resolve) => {
    server.close(() => {
      resolve()
    })
  })
  return port
}

void test("given_connect_defaults_when_resolved_then_should_allow_thirty_seconds", () => {
  assert.deepEqual(connectOptions({}, {}), { timeoutMs: 30_000 })
  assert.deepEqual(connectOptions({}, { LASER_CONNECT_TIMEOUT_MS: "5000" }), { timeoutMs: 5_000 })
  assert.deepEqual(connectOptions({ timeoutMs: 700 }, { LASER_CONNECT_TIMEOUT_MS: "5000" }), {
    timeoutMs: 700
  })
})

void test("given_invalid_connect_settings_when_connected_then_should_reject_before_io", async () => {
  assert.throws(() => connectOptions({}, { LASER_CONNECT_TIMEOUT_MS: "soon" }), ConfigError)
  for (const value of [0, -1, NaN, Infinity, 0x8000_0000]) {
    await assert.rejects(Laser.builder().connectTimeout(value).connect(), ConfigError)
  }
})

void test("given_a_server_that_never_answers_login_when_connecting_then_should_time_out_at_the_budget", async () => {
  const { server, port } = await silentServer()
  const started = Date.now()
  try {
    await assert.rejects(
      Laser.builder()
        .connectionString(`iggy:iggy@127.0.0.1:${String(port)}`)
        .connectTimeout(300)
        .connect(),
      (error: unknown) => error instanceof TimeoutError && error.message.includes("login")
    )
  } finally {
    server.close()
  }
  assert.ok(Date.now() - started < 5_000)
})

void test("given_an_unreachable_server_when_connecting_then_should_stop_retrying_at_the_budget", async () => {
  const port = await closedPort()
  const started = Date.now()
  await assert.rejects(
    Laser.builder()
      .connectionString(`iggy:iggy@127.0.0.1:${String(port)}`)
      .connectTimeout(300)
      .connect(),
    (error: unknown) => error instanceof TimeoutError && error.message.includes("accept")
  )
  assert.ok(Date.now() - started < 5_000)
})

void test("given_a_stalled_tls_handshake_when_connecting_then_should_report_the_accept_stage", async () => {
  const { server, port } = await silentServer()
  try {
    await assert.rejects(
      Laser.builder()
        .connectionString(`iggy:iggy@127.0.0.1:${String(port)}?tls=true`)
        .connectTimeout(100)
        .connect(),
      (error: unknown) => error instanceof TimeoutError && error.message.includes("accept")
    )
  } finally {
    server.close()
  }
})

void test(
  "given_a_stalled_injected_client_when_connecting_then_should_bound_readiness_and_respect_ownership",
  { timeout: 2_000 },
  async () => {
    for (const ownership of ["owned", "borrowed"] as const) {
      let destroys = 0
      const client = {
        clientProvider: () => new Promise(() => undefined),
        destroy: () => {
          destroys += 1
          return Promise.resolve()
        }
      } as unknown as IggyClient
      await assert.rejects(
        Laser.builder().client(client, { ownership }).connectTimeout(30).connect(),
        (error: unknown) => error instanceof TimeoutError && error.message.includes("readiness")
      )
      assert.equal(destroys, ownership === "owned" ? 1 : 0)
    }
  }
)

void test(
  "given_a_stalled_capability_probe_when_connect_expires_then_should_cache_open_capabilities_and_ignore_the_late_reply",
  { timeout: 2_000 },
  async () => {
    let probes = 0
    let finishProbe: (reply: Uint8Array) => void = () => undefined
    const delayed = new Promise<Uint8Array>((resolve) => {
      finishProbe = resolve
    })
    const reply = encodeBackendAnnounce({
      versions: newOpVersions(1, 1, 1, 1),
      ready: true,
      backends: [],
      topology: {
        ...defaultWireTopology(),
        opsStream: "late-ops",
        controlTopic: "control",
        dlqTopic: "dlq",
        changesTopic: "changes"
      }
    })
    const client = {
      clientProvider: () => Promise.resolve({}),
      sendBinaryRequest: () => {
        probes += 1
        return probes === 1 ? delayed : Promise.resolve(reply)
      }
    } as unknown as IggyClient
    const laser = await Laser.builder().client(client).connectTimeout(30).connect()
    const originalOpsStream = laser.opsStream
    assert.deepEqual(await laser.capabilities(), { ...OPEN_CAPABILITIES, hello: "failed" })
    assert.equal(probes, 1)
    finishProbe(reply)
    await new Promise<void>((resolve) => setImmediate(resolve))
    assert.deepEqual(await laser.capabilities(), { ...OPEN_CAPABILITIES, hello: "failed" })
    assert.equal(laser.opsStream, originalOpsStream)
    assert.equal((await laser.refreshCapabilities()).managed, true)
    assert.equal(laser.opsStream, "late-ops")
    assert.equal(probes, 2)
    await laser.close()
  }
)

void test("given_an_unmanaged_set_polled_faster_than_a_second_when_a_second_passes_then_should_probe_again_once", async (t) => {
  let probes = 0
  const client = {
    clientProvider: () => Promise.resolve({}),
    sendBinaryRequest: () => {
      probes += 1
      return Promise.resolve(new Uint8Array())
    }
  } as unknown as IggyClient
  const laser = await Laser.builder().client(client).connect()
  const perProbe = probes
  assert.ok(perProbe > 0)
  t.mock.timers.enable({ apis: ["Date"], now: Date.now() })
  for (let call = 0; call < 4; call += 1) {
    assert.equal((await laser.capabilities()).managed, false)
    t.mock.timers.tick(300)
  }
  assert.equal(probes, perProbe)
  await laser.capabilities()
  assert.equal(probes, 2 * perProbe)
  t.mock.timers.tick(1_000)
  await Promise.all([laser.capabilities(), laser.capabilities(), laser.capabilities()])
  assert.equal(probes, 3 * perProbe)
  await laser.close()
})
