import assert from "node:assert/strict"
import { test } from "node:test"
import { ConfigError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"

void test("given_no_connection_string_when_connecting_from_env_with_a_stream_then_should_raise_config_error", async () => {
  await assert.rejects(
    Laser.connectEnvWithStream("picked", {}),
    (error: unknown) =>
      error instanceof ConfigError && error.message === "LASER_CONNECTION_STRING is not set"
  )
})

void test("given_an_empty_connection_string_when_connecting_from_env_then_should_raise_config_error", async () => {
  await assert.rejects(
    Laser.connectEnv({ LASER_CONNECTION_STRING: "" }),
    (error: unknown) =>
      error instanceof ConfigError && error.message === "LASER_CONNECTION_STRING is not set"
  )
})
