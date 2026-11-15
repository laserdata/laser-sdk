import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { iggyErrorCode, uncertainOperation } from "../../src/client/error-classify.js"
import { InvalidError } from "../../src/client/errors.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const UNAUTHORIZED = 41
const PING_CODE = 1
const GET_STATS_CODE = 10
const OPERATION_NOT_RETAINED = 14008

function withCredentials(credentials: string): string {
  const at = CONNECTION_STRING.indexOf("@")
  const scheme = CONNECTION_STRING.indexOf("://")
  const prefix = scheme === -1 || scheme > at ? "" : CONNECTION_STRING.slice(0, scheme + 3)
  return `${prefix}${credentials}${CONNECTION_STRING.slice(at)}`
}

void test("given_a_server_when_topic_options_are_described_then_should_list_each_option_with_its_rules", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const options = await laser.describeOptions("topic")
    const segmentSize = options.find((option) => option.key === "segment_size")
    assert.equal(segmentSize?.kind, "uint64")
    assert.ok(segmentSize.defaultValue !== undefined)
    assert.ok(segmentSize.description.length > 0)
  } finally {
    await laser.close()
  }
})

void test("given_a_created_stream_when_metadata_changes_are_read_then_should_report_its_creation", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const latest = await laser.metadataChanges({ start: { mode: "latest" } })
    assert.equal(latest.status, "ok")
    assert.ok(latest.next !== undefined)
    const name = `changes-${randomUUID().slice(0, 12)}`
    await laser.stream(name).ensure()
    try {
      const page = await laser.metadataChanges({
        start: { mode: "after", cursor: latest.next },
        groups: ["streams"]
      })
      const created = page.records.find((record) => record.subject.name === name)
      assert.equal(created?.kind, "stream_created")
      assert.ok(created.subject.streamId !== undefined)
      assert.ok(created.timestampMicros > 0n)
    } finally {
      await laser.stream(name).delete()
    }
  } finally {
    await laser.close()
  }
})

void test("given_a_malformed_cursor_when_metadata_changes_are_read_then_should_refuse_it_before_sending", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    await assert.rejects(
      laser.metadataChanges({ start: { mode: "after", cursor: "mc1_nope" } }),
      InvalidError
    )
  } finally {
    await laser.close()
  }
})

void test("given_a_user_without_server_grants_when_metadata_changes_are_read_then_should_be_refused", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  const name = `reader-${randomUUID().slice(0, 12)}`
  try {
    await laser.user(name).create("secret-1")
    const reader = await Laser.connect(withCredentials(`${name}:secret-1`))
    try {
      await assert.rejects(reader.metadataChanges(), (error) => {
        assert.equal(iggyErrorCode(error), UNAUTHORIZED)
        return true
      })
    } finally {
      await reader.close()
    }
  } finally {
    await laser.user(name).delete()
    await laser.close()
  }
})

void test("given_raw_commands_when_sent_then_should_return_the_reply_of_the_node_or_the_replica", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    assert.equal((await laser.sendCommand(PING_CODE, new Uint8Array())).length, 0)
    const cluster = await laser.cluster()
    const replica = cluster.nodes[0]?.replicaId
    assert.ok(replica !== undefined)
    const stats = await laser.sendCommandToReplica(GET_STATS_CODE, new Uint8Array(), replica)
    assert.ok(stats.length > 0)
  } finally {
    await laser.close()
  }
})

void test("given_an_operation_the_client_does_not_retain_when_resolved_then_should_name_it", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const operation = 2n ** 64n - 8n
    await assert.rejects(laser.resolveOperation(operation, 5_000), (error: unknown) => {
      assert.equal(uncertainOperation(error), operation)
      assert.equal(iggyErrorCode(error), OPERATION_NOT_RETAINED)
      return true
    })
  } finally {
    await laser.close()
  }
})
