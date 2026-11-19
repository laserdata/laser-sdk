import assert from "node:assert/strict"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

void test("given_a_stream_when_connecting_from_env_then_should_pin_it_over_laser_stream", async () => {
  const env = { LASER_CONNECTION_STRING: CONNECTION_STRING, LASER_STREAM: "from-env" }
  await using picked = await Laser.connectEnvWithStream("picked", env)
  await using fromEnv = await Laser.connectEnv(env)

  assert.deepEqual([picked.defaultStream, fromEnv.defaultStream], ["picked", "from-env"])
})

void test("given_an_empty_stream_when_connecting_from_env_then_should_leave_no_default_stream", async () => {
  await using laser = await Laser.connectEnv({
    LASER_CONNECTION_STRING: CONNECTION_STRING,
    LASER_STREAM: ""
  })

  assert.equal(laser.defaultStream, undefined)
})
