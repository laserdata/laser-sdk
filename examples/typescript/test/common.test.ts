import assert from "node:assert/strict"
import { test } from "node:test"
import {
  AsyncResourceGroup,
  Rng,
  ensureDefaultPort,
  resolveConnectionString,
  streamFor
} from "../src/common.js"

void test("given_the_canonical_seed_when_generating_values_then_should_match_cross_language_vectors", () => {
  const rng = new Rng(0x123456789abcdef0n)
  assert.deepEqual(
    Array.from({ length: 4 }, () => rng.nextU64().toString(16)),
    ["fa2854001aa80c5c", "c12e42f8d265c7d2", "471225b948609d82", "8499eaaa3223c11d"]
  )
})

void test("given_async_resources_when_disposed_then_should_close_all_in_reverse_order", async () => {
  const closed: string[] = []
  const resource = (name: string, fail = false): AsyncDisposable => ({
    async [Symbol.asyncDispose](): Promise<void> {
      closed.push(name)
      if (fail) throw new Error(name)
    }
  })
  const group = new AsyncResourceGroup()
  group.add(resource("first"))
  group.add(resource("second", true))
  group.add(resource("third"))

  await assert.rejects(group[Symbol.asyncDispose](), /second/)

  assert.deepEqual(closed, ["third", "second", "first"])
})

void test("given_connection_environment_when_resolved_then_should_preserve_shared_conventions", () => {
  assert.equal(resolveConnectionString({}), "iggy:iggy@127.0.0.1:8090")
  assert.equal(
    resolveConnectionString({
      LASER_SERVER: "cloud.example",
      LASER_TOKEN: "secret"
    }),
    "iggy+tcp://secret@cloud.example:8090"
  )
  assert.equal(streamFor("interop", {}), "laser-interop")
  assert.equal(streamFor("interop", { LASER_STREAM: "tenant-stream" }), "tenant-stream")
})

void test("given_a_host_without_a_port_when_normalized_then_should_add_the_default", () => {
  assert.equal(
    ensureDefaultPort("iggy+tcp://u:p@h.laserdata.cloud"),
    "iggy+tcp://u:p@h.laserdata.cloud:8090"
  )
  assert.equal(
    ensureDefaultPort("iggy+tcp://token@h.laserdata.cloud?x=1"),
    "iggy+tcp://token@h.laserdata.cloud:8090?x=1"
  )
  assert.equal(ensureDefaultPort("iggy://u:p@host/path"), "iggy://u:p@host:8090/path")
})

void test("given_a_host_with_a_port_when_normalized_then_should_be_unchanged", () => {
  assert.equal(
    ensureDefaultPort("iggy+tcp://u:p@h.laserdata.cloud:9000"),
    "iggy+tcp://u:p@h.laserdata.cloud:9000"
  )
})

void test("given_a_bracketed_ipv6_host_when_normalized_then_should_handle_its_port", () => {
  assert.equal(ensureDefaultPort("iggy+tcp://u:p@[::1]"), "iggy+tcp://u:p@[::1]:8090")
  assert.equal(ensureDefaultPort("iggy+tcp://u:p@[::1]:9000"), "iggy+tcp://u:p@[::1]:9000")
})

void test("given_a_bare_target_when_normalized_then_should_add_scheme_and_port", () => {
  assert.equal(
    resolveConnectionString({
      LASER_CONNECTION_STRING: "user:password@host.example"
    }),
    "iggy+tcp://user:password@host.example:8090"
  )
})

void test("given_a_laserdata_host_when_resolved_then_should_attach_tls", () => {
  assert.equal(
    resolveConnectionString({
      LASER_CONNECTION_STRING: "u:p@starter-1.aws.laserdata.cloud"
    }),
    "iggy+tcp://u:p@starter-1.aws.laserdata.cloud:8090?tls=true"
  )
  assert.equal(
    resolveConnectionString({
      LASER_CONNECTION_STRING: "iggy://u:p@api.laserdata.com:9000?x=1"
    }),
    "iggy://u:p@api.laserdata.com:9000?x=1&tls=true"
  )
  assert.equal(
    resolveConnectionString({
      LASER_CONNECTION_STRING: "u:p@h.laserdata.cloud",
      LASER_TLS_CERT: "/tmp/ca.crt"
    }),
    "iggy+tcp://u:p@h.laserdata.cloud:8090?tls=true&tls_ca_file=/tmp/ca.crt"
  )
})

void test("given_laser_no_tls_when_resolved_then_should_leave_the_target_untouched", () => {
  assert.equal(
    resolveConnectionString({
      LASER_CONNECTION_STRING: "u:p@h.laserdata.cloud",
      LASER_NO_TLS: "1"
    }),
    "iggy+tcp://u:p@h.laserdata.cloud:8090"
  )
})

void test("given_a_non_laserdata_host_when_resolved_then_should_not_attach_tls", () => {
  assert.equal(
    resolveConnectionString({
      LASER_CONNECTION_STRING: "u:p@laserdata.cloud.attacker.com"
    }),
    "iggy+tcp://u:p@laserdata.cloud.attacker.com:8090"
  )
  assert.equal(
    resolveConnectionString({
      LASER_CONNECTION_STRING: "u:p@iggy.internal:7070"
    }),
    "iggy+tcp://u:p@iggy.internal:7070"
  )
})

void test("given_a_string_with_its_own_ca_when_resolved_then_should_not_attach_tls_again", () => {
  assert.equal(
    resolveConnectionString({
      LASER_CONNECTION_STRING: "iggy://u:p@h.laserdata.cloud:8090?tls=true&tls_ca_file=/etc/ca.crt"
    }),
    "iggy://u:p@h.laserdata.cloud:8090?tls=true&tls_ca_file=/etc/ca.crt"
  )
})
