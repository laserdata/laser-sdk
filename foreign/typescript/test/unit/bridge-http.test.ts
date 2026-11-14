import assert from "node:assert/strict"
import { request as httpRequest } from "node:http"
import { test } from "node:test"
import { A2aBridge } from "../../src/bridges/a2a.js"
import { type BridgeServer, originAllowed } from "../../src/bridges/http.js"
import { McpBridge } from "../../src/bridges/mcp.js"
import { InvalidError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { AgentId } from "../../src/types/ids.js"

const laser = {} as unknown as Laser

interface Answer {
  readonly status: number
  readonly body: string
  readonly headers: Readonly<Record<string, string | string[] | undefined>>
}

function call(
  server: BridgeServer,
  method: string,
  path: string,
  headers: Readonly<Record<string, string>> = {},
  body?: string
): Promise<Answer> {
  return new Promise((resolve, reject) => {
    const outgoing = httpRequest(
      { host: server.host, port: server.port, method, path, headers },
      (response) => {
        const chunks: Buffer[] = []
        response.on("data", (chunk: Buffer) => chunks.push(chunk))
        response.on("end", () => {
          resolve({
            status: response.statusCode ?? 0,
            body: Buffer.concat(chunks).toString("utf8"),
            headers: response.headers
          })
        })
      }
    )
    outgoing.on("error", reject)
    outgoing.end(body)
  })
}

function rpc(
  server: BridgeServer,
  message: unknown,
  headers: Readonly<Record<string, string>> = {}
): Promise<Answer> {
  return call(
    server,
    "POST",
    "/",
    { "content-type": "application/json", ...headers },
    JSON.stringify(message)
  )
}

function a2a(): A2aBridge {
  return new A2aBridge(laser, AgentId.new("bridge"), "requests", "replies")
}

function mcp(): McpBridge {
  return new McpBridge(laser, AgentId.new("mcp"), "tool-calls", "tool-results", "tools")
}

const UNKNOWN_METHOD = { jsonrpc: "2.0", id: 1, method: "nope" }

void test("given_origin_headers_when_checked_then_should_allow_listed_same_origin_and_non_browser_requests", () => {
  const allowed = ["https://app.example.com"]
  const cases: readonly [string | undefined, string | undefined, boolean][] = [
    ["bridge.example:8080", undefined, true],
    ["bridge.example:8080", "https://app.example.com", true],
    ["127.0.0.1:8080", "http://127.0.0.1:8080", true],
    ["[::1]:8080", "http://[::1]:8080", true],
    ["LOCALHOST:8080", "http://localhost:8080", true],
    ["evil.example:8080", "http://evil.example:8080", false],
    ["127.0.0.1:8080", "http://127.0.0.1:9090", false],
    ["127.0.0.1:8080", "null", false],
    [undefined, "http://127.0.0.1:8080", false],
    ["127.0.0.1:8080", "https://evil.example", false]
  ]
  for (const [host, origin, expected] of cases) {
    assert.equal(
      originAllowed(origin, host, allowed),
      expected,
      `${String(origin)} on ${String(host)}`
    )
  }
})

void test("given_a_taken_port_when_a_bridge_serves_then_should_fail_with_invalid_error", async () => {
  const taken = await a2a().serve()
  try {
    await assert.rejects(
      mcp().serve({ host: taken.host, port: taken.port }),
      (error: unknown) =>
        error instanceof InvalidError &&
        error.message.startsWith(`cannot listen on ${taken.host}:${String(taken.port)}: `)
    )
  } finally {
    await taken.close()
  }
})

void test("given_an_a2a_bridge_server_when_browser_origins_call_it_then_should_refuse_only_foreign_ones", async () => {
  const server = await a2a().withAllowedOrigins(["https://app.example.com"]).serve()
  try {
    const plain = await rpc(server, UNKNOWN_METHOD)
    assert.equal(plain.status, 200)
    assert.equal((JSON.parse(plain.body) as { error: { code: number } }).error.code, -32601)
    assert.equal(
      (await rpc(server, UNKNOWN_METHOD, { origin: "https://app.example.com" })).status,
      200
    )
    assert.equal(
      (await rpc(server, UNKNOWN_METHOD, { origin: "https://evil.example" })).status,
      403
    )
    const sameOrigin = await rpc(server, UNKNOWN_METHOD, { origin: server.url })
    assert.equal(sameOrigin.status, 200)
  } finally {
    await server.close()
  }
})

void test("given_an_a2a_bridge_server_when_requests_are_malformed_then_should_answer_http_errors", async () => {
  const server = await a2a().serve()
  try {
    assert.equal((await call(server, "GET", "/")).status, 405)
    assert.equal((await call(server, "POST", "/elsewhere")).status, 404)
    assert.equal(
      (await call(server, "POST", "/", { "content-type": "text/plain" }, "{}")).status,
      415
    )
    assert.equal(
      (await call(server, "POST", "/", { "content-type": "application/json" }, "{")).status,
      400
    )
  } finally {
    await server.close()
  }
})

void test("given_an_a2a_bridge_server_when_the_card_is_fetched_then_should_serve_it_once_a_public_url_is_set", async () => {
  const unnamed = await a2a().serve()
  try {
    assert.equal((await call(unnamed, "GET", "/.well-known/agent-card.json")).status, 404)
  } finally {
    await unnamed.close()
  }
  const server = await a2a().withPublicUrl("https://bridge.example/a2a").serve()
  try {
    const card = await call(server, "GET", "/.well-known/agent-card.json", {
      origin: "https://evil.example"
    })
    assert.equal(card.status, 200)
    const parsed = JSON.parse(card.body) as { supportedInterfaces: { url: string }[] }
    assert.equal(parsed.supportedInterfaces[0]?.url, "https://bridge.example/a2a")
    assert.equal((await call(server, "POST", "/.well-known/agent-card.json")).status, 405)
  } finally {
    await server.close()
  }
})

void test("given_a_caller_hook_when_an_a2a_request_arrives_then_should_read_its_headers_and_refuse_on_failure", async () => {
  const seen: string[] = []
  const server = await a2a().serve({
    caller: (headers) => {
      const token = headers["x-caller"]
      if (token === "bad") throw new Error("refused")
      seen.push(token ?? "")
      return token
    }
  })
  try {
    assert.equal((await rpc(server, UNKNOWN_METHOD, { "X-Caller": "alice" })).status, 200)
    assert.deepEqual(seen, ["alice"])
    assert.equal((await rpc(server, UNKNOWN_METHOD, { "x-caller": "bad" })).status, 401)
  } finally {
    await server.close()
  }
})

void test("given_an_mcp_bridge_server_when_messages_arrive_then_should_answer_requests_and_accept_notifications", async () => {
  const server = await mcp().withAllowedOrigins(["https://app.example.com"]).serve()
  try {
    const listed = await rpc(server, { jsonrpc: "2.0", id: 1, method: "tools/list" })
    assert.equal(listed.status, 200)
    assert.ok("result" in (JSON.parse(listed.body) as object))
    const notified = await rpc(server, { jsonrpc: "2.0", method: "notifications/initialized" })
    assert.equal(notified.status, 202)
    assert.equal(notified.body, "")
    assert.equal(
      (
        await rpc(
          server,
          { jsonrpc: "2.0", id: 2, method: "ping" },
          { "mcp-protocol-version": "1999-01-01" }
        )
      ).status,
      400
    )
    assert.equal(
      (
        await rpc(
          server,
          { jsonrpc: "2.0", id: 3, method: "ping" },
          { "mcp-protocol-version": "2025-06-18" }
        )
      ).status,
      200
    )
    assert.equal(
      (
        await rpc(
          server,
          { jsonrpc: "2.0", id: 4, method: "ping" },
          { origin: "https://evil.example" }
        )
      ).status,
      403
    )
    assert.equal(
      (
        await rpc(
          server,
          { jsonrpc: "2.0", id: 5, method: "ping" },
          { origin: "https://app.example.com" }
        )
      ).status,
      200
    )
  } finally {
    await server.close()
  }
})
