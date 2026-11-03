import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { createServer } from "node:net"
import { test } from "node:test"
import { ConfigError, TransportError } from "../../src/client/errors.js"
import { LASERDATA_ROOT_CA } from "../../src/client/laserdata-ca.js"
import { ApacheIggyTransport, parseConnectionString } from "../../src/iggy/apache-iggy.js"

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

void test("given_vsr_over_tls_when_connecting_then_should_reach_the_socket", async () => {
  const server = createServer((socket) => socket.destroy())
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(0, "127.0.0.1", resolve)
  })
  const address = server.address()
  assert(address !== null && typeof address === "object")
  try {
    await assert.rejects(
      ApacheIggyTransport.connect(
        `iggy://token@127.0.0.1:${String(address.port)}?tls=true&reconnection_retries=0`
      ),
      (error: unknown) => error instanceof TransportError
    )
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error === undefined) resolve()
        else reject(error)
      })
    })
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

void test("given_a_custom_ca_when_a_local_host_is_parsed_then_should_enable_tls", () => {
  const parsed = parseConnectionString("iggy://token@demo.localhost", {
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

void test("given_an_ipv6_authority_or_a_path_when_parsed_then_should_refuse_it", () => {
  for (const value of [
    "iggy://user:password@[2001:db8::7]:9080",
    "iggy://user:password@host/path",
    "iggy://user:password@host#frag"
  ]) {
    assert.throws(() => parseConnectionString(value, {}), ConfigError, value)
  }
})

void test("given_no_port_when_parsed_then_should_default_to_8090", () => {
  assert.equal(parseConnectionString("user:password@host", {}).port, 8090)
  assert.equal(parseConnectionString("iggy://user:password@host?nodelay=true", {}).port, 8090)
})

void test("given_an_out_of_range_port_when_parsed_then_should_refuse_it", () => {
  for (const port of ["0", "65536", "+80", "8o90", ""]) {
    assert.throws(
      () => parseConnectionString(`iggy://user:password@host:${port}`, {}),
      ConfigError,
      port
    )
  }
  assert.equal(parseConnectionString("iggy://user:password@host:65535", {}).port, 65_535)
})

void test("given_a_scheme_other_than_iggy_tcp_when_parsed_then_should_refuse_it", () => {
  for (const value of ["iggy+quic://u:p@host", "iggy+http://u:p@host", "http://u:p@host"]) {
    assert.throws(() => parseConnectionString(value, {}), ConfigError, value)
  }
  assert.equal(parseConnectionString("iggy+tcp://u:p@host", {}).host, "host")
})

void test("given_missing_or_malformed_credentials_when_parsed_then_should_refuse_them", () => {
  for (const value of [
    "host:8090",
    "iggy://host:8090",
    "iggy://@host",
    "iggy://:password@host",
    "iggy://user:@host",
    "iggy://a:b:c@host"
  ]) {
    assert.throws(() => parseConnectionString(value, {}), ConfigError, value)
  }
})

void test("given_credentials_with_percent_signs_or_slashes_when_parsed_then_should_keep_them_verbatim", () => {
  assert.deepEqual(parseConnectionString("iggy://user:p%40ss/w0rd@host", {}).credentials, {
    username: "user",
    password: "p%40ss/w0rd"
  })
  assert.deepEqual(parseConnectionString("tok%2Fen@host", {}).credentials, { token: "tok%2Fen" })
})

void test("given_unknown_repeated_or_malformed_options_when_parsed_then_should_refuse_them", () => {
  for (const query of [
    "x=1",
    "TLS=true",
    "tls=true&tls=true",
    "tls",
    "tls=true=false",
    "",
    "tls=yes",
    "nodelay=1"
  ]) {
    assert.throws(
      () => parseConnectionString(`iggy://user:password@host?${query}`, {}),
      ConfigError,
      query
    )
  }
})

void test("given_transport_options_when_parsed_then_should_apply_them", () => {
  const parsed = parseConnectionString(
    "iggy://user:password@host?heartbeat_interval=2s&nodelay=true&reestablish_after=100ms&tls=true&tls_domain=node.example",
    {}
  )
  assert.equal(parsed.heartbeatIntervalMs, 2_000)
  assert.equal(parsed.noDelay, true)
  assert.equal(parsed.reestablishAfterMs, 100)
  assert.equal(parsed.tls, true)
  assert.equal(parsed.servername, "node.example")
})

void test("given_a_ca_file_without_tls_when_parsed_then_should_turn_tls_on_even_under_no_tls", () => {
  for (const env of [{}, { LASER_NO_TLS: "1" }]) {
    const parsed = parseConnectionString(
      "iggy://user:password@host?tls_ca_file=../../sdk/certs/laserdata.crt",
      env
    )
    assert.equal(parsed.tls, true)
    assert.equal(parsed.ca, LASERDATA_ROOT_CA)
  }
})

void test("given_tls_false_with_a_ca_file_when_parsed_then_should_refuse_it", () => {
  assert.throws(
    () =>
      parseConnectionString(
        "iggy://user:password@host?tls=false&tls_ca_file=../../sdk/certs/laserdata.crt",
        {}
      ),
    ConfigError
  )
})

void test("given_an_explicit_tls_false_when_parsed_then_should_disable_automatic_tls", () => {
  const managed = parseConnectionString("iggy://token@api.laserdata.cloud?tls=false", {})
  assert.equal(managed.tls, false)
  assert.equal(managed.ca, undefined)
  const custom = parseConnectionString("iggy://token@demo.localhost?tls=false", {
    LASER_TLS_CERT: "../../sdk/certs/laserdata.crt"
  })
  assert.equal(custom.tls, false)
  assert.equal(custom.ca, undefined)
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

void test("given_a_malformed_connection_string_when_parsed_then_should_not_echo_the_credential", () => {
  const secret = "sup3rs3cr3t"
  assert.throws(
    () => parseConnectionString(`iggy://user:${secret}@[`),
    (error: unknown) => {
      assert.ok(error instanceof Error)
      assert.ok(!error.message.includes(secret), `credential leaked: ${error.message}`)
      return true
    }
  )
})

void test("given_an_invalid_port_when_parsed_then_should_not_echo_the_credential", () => {
  const secret = "sup3rs3cr3t"
  assert.throws(
    () => parseConnectionString(`iggy://user:${secret}@host:99999`),
    (error: unknown) => {
      assert.ok(error instanceof Error)
      assert.ok(!error.message.includes(secret), `credential leaked: ${error.message}`)
      return true
    }
  )
})

void test("given_a_negated_no_tls_flag_when_parsed_then_should_still_enable_tls", () => {
  for (const value of ["0", "false", "", "no"]) {
    const parsed = parseConnectionString("iggy://u:p@api.laserdata.cloud:8090", {
      LASER_NO_TLS: value
    })
    assert.equal(parsed.tls, true, `LASER_NO_TLS=${value} must not disable TLS`)
  }
})

void test("given_an_affirmative_no_tls_flag_when_parsed_then_should_disable_tls", () => {
  for (const value of ["1", "true", "TRUE", " yes "]) {
    const parsed = parseConnectionString("iggy://u:p@api.laserdata.cloud:8090", {
      LASER_NO_TLS: value
    })
    assert.equal(parsed.tls, false, `LASER_NO_TLS=${value} must disable TLS`)
  }
})
