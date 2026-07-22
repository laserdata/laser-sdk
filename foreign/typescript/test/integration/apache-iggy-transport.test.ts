import assert from "node:assert/strict"
import { test } from "node:test"
import { ApacheIggyTransport } from "../../src/iggy/apache-iggy.js"

const CONNECTION_STRING =
  process.env["LASER_CONNECTION_STRING"] ?? "iggy://iggy:iggy@127.0.0.1:8090"

const PING_CODE = 1
const LOGIN_USER_CODE = 38
const UNKNOWN_CODE = 999999

void test("given_running_iggy_when_connecting_then_should_authenticate_and_ping", async () => {
  const transport = await ApacheIggyTransport.connect(CONNECTION_STRING)
  try {
    const reply = await transport.sendManaged(PING_CODE, new Uint8Array())
    assert.equal(reply.byteLength, 0)
  } finally {
    await transport.close()
  }
})

void test("given_connected_transport_when_sending_session_control_code_then_should_reject", async () => {
  const transport = await ApacheIggyTransport.connect(CONNECTION_STRING)
  try {
    await assert.rejects(transport.sendManaged(LOGIN_USER_CODE, new Uint8Array()))
  } finally {
    await transport.close()
  }
})

void test("given_connected_transport_when_sending_unknown_code_then_should_reject", async () => {
  const transport = await ApacheIggyTransport.connect(CONNECTION_STRING)
  try {
    await assert.rejects(transport.sendManaged(UNKNOWN_CODE, new Uint8Array()))
  } finally {
    await transport.close()
  }
})

void test("given_a_refused_connection_when_connecting_then_should_reject_with_a_transport_error", async () => {
  await assert.rejects(ApacheIggyTransport.connect("iggy://iggy:iggy@127.0.0.1:9"), {
    name: "TransportError"
  })
})
