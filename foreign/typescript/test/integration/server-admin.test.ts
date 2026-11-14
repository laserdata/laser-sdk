import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { InvalidError } from "../../src/client/errors.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

function withCredentials(credentials: string): string {
  const at = CONNECTION_STRING.indexOf("@")
  const scheme = CONNECTION_STRING.indexOf("://")
  const prefix = scheme === -1 || scheme > at ? "" : CONNECTION_STRING.slice(0, scheme + 3)
  return `${prefix}${credentials}${CONNECTION_STRING.slice(at)}`
}

void test("given_a_new_user_when_created_updated_and_deleted_then_should_report_each_change", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const name = `user-${randomUUID().slice(0, 12)}`
    const user = laser.user(name)
    assert.equal(await user.exists(), false)

    const created = await user.create("secret-1", {
      permissions: { global: { readStreams: true } }
    })
    assert.equal(created.username, name)
    assert.equal(created.status, "active")
    assert.ok(created.createdAtMicros > 0n)

    assert.ok((await laser.users()).some((info) => info.username === name))
    const read = await user.get()
    assert.equal(read?.permissions?.global?.readStreams, true)
    assert.equal(read.permissions.global.manageUsers, false)

    const asUser = await Laser.connect(withCredentials(`${name}:secret-1`))
    try {
      assert.equal((await asUser.me()).userId, read.id)
    } finally {
      await asUser.close()
    }

    await user.changePassword("secret-1", "secret-2")
    await user.setPermissions(undefined)
    await user.update({ status: "inactive" })
    const updated = await user.get()
    assert.equal(updated?.status, "inactive")
    assert.equal(updated.permissions, undefined)

    const renamed = `${name}-renamed`
    await user.update({ username: renamed })
    assert.equal(await user.exists(), false)
    const moved = laser.user(renamed)
    assert.equal(await moved.delete(), true)
    assert.equal(await moved.delete(), false)
  } finally {
    await laser.close()
  }
})

void test("given_an_unknown_user_status_when_creating_then_should_throw_invalid_error", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const user = laser.user(`user-${randomUUID().slice(0, 12)}`)
    await assert.rejects(user.create("secret", { status: "paused" as "active" }), InvalidError)
  } finally {
    await laser.close()
  }
})

void test("given_a_personal_access_token_when_created_then_should_sign_in_until_deleted", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const name = `token-${randomUUID().slice(0, 12)}`
    const token = laser.personalAccessToken(name)
    const value = await token.create({ expiryMs: 3_600_000 })

    const info = (await laser.personalAccessTokens()).find((listed) => listed.name === name)
    assert.ok(info?.expiresAtMicros !== undefined)

    const withToken = await Laser.connect(withCredentials(value))
    try {
      await withToken.ping()
    } finally {
      await withToken.close()
    }

    assert.equal(await token.delete(), true)
    assert.equal(await token.delete(), false)
  } finally {
    await laser.close()
  }
})

void test("given_a_connected_client_when_the_server_is_inspected_then_should_report_it", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    await laser.ping()
    const stats = await laser.stats()
    assert.ok(stats.serverVersion.length > 0)

    const me = await laser.me()
    assert.notEqual(me.userId, undefined)
    assert.ok((await laser.clients()).some((client) => client.clientId === me.clientId))
    const found = await laser.clientInfo(me.clientId)
    assert.equal(found?.address, me.address)
    assert.equal(await laser.clientInfo(0xffff_ffff), undefined)
  } finally {
    await laser.close()
  }
})

void test("given_a_stream_and_topic_when_renamed_and_updated_then_should_report_them", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const name = `rename-${randomUUID().slice(0, 12)}`
    const stream = laser.stream(name)
    await stream.ensure()
    await stream.topic("metrics").ensure(1)

    const renamed = `${name}-new`
    await stream.rename(renamed)
    assert.equal(await stream.exists(), false)
    const moved = laser.stream(renamed)
    assert.equal(await moved.exists(), true)

    await moved.topic("metrics").update({
      name: "hosts",
      expireAfterMs: 3_600_000,
      maxTopicBytes: 1n << 30n
    })
    const info = await moved.topic("hosts").get()
    assert.equal(info?.maxSizeBytes, 1n << 30n)

    await moved.topic("hosts").deleteSegments(0, 1)
    assert.equal(await moved.delete(), true)
  } finally {
    await laser.close()
  }
})

void test("given_a_server_when_the_cluster_and_a_snapshot_are_read_then_should_report_them", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const cluster = await laser.cluster()
    assert.ok(cluster.nodes.some((node) => node.tcpPort > 0))
    const archive = await laser.snapshot()
    assert.equal(Buffer.from(archive.subarray(0, 2)).toString(), "PK")
  } finally {
    await laser.close()
  }
})
