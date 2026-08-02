import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { test } from "node:test"
import { LASERDATA_ROOT_CA } from "../../src/client/laserdata-ca.js"
import { parseConnectionString } from "../../src/iggy/apache-iggy.js"

void test("given_the_sdk_certificate_when_compared_with_typescript_then_should_be_byte_identical", () => {
  const rustCertificate = readFileSync("../../sdk/certs/laserdata.crt", "utf8")
  assert.equal(LASERDATA_ROOT_CA, rustCertificate)
})

void test("given_a_laserdata_host_when_parsed_then_should_enable_tls_with_the_embedded_ca", () => {
  for (const host of [
    "laserdata.cloud",
    "api.laserdata.cloud",
    "laserdata.com",
    "api.laserdata.com"
  ]) {
    const parsed = parseConnectionString(`iggy+tcp://token@${host}`, {})
    assert.equal(parsed.tls, true)
    assert.equal(parsed.ca, LASERDATA_ROOT_CA)
    assert.deepEqual(parsed.credentials, { token: "token" })
  }
})

void test("given_a_laserdata_lookalike_when_parsed_then_should_not_enable_tls", () => {
  const parsed = parseConnectionString(
    "iggy://user:password@laserdata.cloud.attacker.example:8090",
    {}
  )
  assert.equal(parsed.tls, false)
  assert.equal(parsed.ca, undefined)
  assert.deepEqual(parsed.credentials, { username: "user", password: "password" })
})

void test("given_tls_is_disabled_when_a_laserdata_host_is_parsed_then_should_not_attach_the_ca", () => {
  const parsed = parseConnectionString("iggy://token@api.laserdata.cloud", {
    LASER_NO_TLS: "1"
  })
  assert.equal(parsed.tls, false)
  assert.equal(parsed.ca, undefined)
})

void test("given_an_explicit_ca_when_parsed_then_should_override_the_embedded_ca", () => {
  const parsed = parseConnectionString("iggy://token@api.laserdata.cloud?tls=true", {
    LASER_TLS_CERT: "../../sdk/certs/laserdata.crt"
  })
  assert.equal(parsed.tls, true)
  assert.equal(parsed.ca, LASERDATA_ROOT_CA)
})

void test("given_a_connection_string_without_a_scheme_when_parsed_then_should_prepend_iggy", () => {
  const parsed = parseConnectionString("iggy:iggy@127.0.0.1:8090", {})
  assert.equal(parsed.host, "127.0.0.1")
  assert.equal(parsed.port, 8090)
  assert.equal(parsed.tls, false)
  assert.deepEqual(parsed.credentials, { username: "iggy", password: "iggy" })
  assert.deepEqual(parsed.reconnection, { intervalMs: 1_000, maxRetries: undefined })
})

void test("given_an_ipv6_authority_when_parsed_then_should_preserve_the_host_and_port", () => {
  const parsed = parseConnectionString("iggy://user:password@[2001:db8::7]:9080", {})
  assert.equal(parsed.host, "[2001:db8::7]")
  assert.equal(parsed.port, 9080)
})

void test("given_reconnection_options_when_parsed_then_should_match_the_rust_grammar", () => {
  const parsed = parseConnectionString(
    "iggy://user:password@127.0.0.1:8090?reconnection_retries=unlimited&reconnection_interval=250ms",
    {}
  )
  assert.deepEqual(parsed.reconnection, { intervalMs: 250, maxRetries: undefined })
})

void test("given_invalid_reconnection_options_when_parsed_then_should_fail_before_io", () => {
  assert.throws(
    () => parseConnectionString("iggy://user:password@127.0.0.1:8090?reconnection_retries=-1", {}),
    /reconnection_retries/
  )
  assert.throws(
    () =>
      parseConnectionString("iggy://user:password@127.0.0.1:8090?reconnection_interval=soon", {}),
    /reconnection_interval/
  )
})
