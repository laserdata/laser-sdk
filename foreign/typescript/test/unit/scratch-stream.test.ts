import assert from "node:assert/strict"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { CREATE_STREAM } from "apache-iggy/dist/wire/stream/create-stream.command.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"

// An injected client that records every stream it was asked to create or
// delete, refusing a create of a name it already holds like the server does.
function recordingClient(calls: string[], existing = new Set<string>()): IggyClient {
  const raw = {
    protocol: "vsr",
    sendCommand: (code: number, payload: Buffer) => {
      assert.equal(code, CREATE_STREAM.code)
      const name = payload.subarray(1, 1 + (payload[0] ?? 0)).toString("utf8")
      calls.push(`create ${name}`)
      if (existing.has(name)) {
        return Promise.reject(
          Object.assign(new Error("stream already exists"), { errorCode: 1012 })
        )
      }
      existing.add(name)
      return Promise.resolve({ status: 0, length: 0, data: Buffer.alloc(0) })
    }
  }
  return {
    clientProvider: () => Promise.resolve(raw),
    destroy: () => Promise.resolve(),
    stream: {
      delete: ({ streamId }: { readonly streamId: string }) => {
        calls.push(`delete ${streamId}`)
        return Promise.resolve(true)
      }
    }
  } as unknown as IggyClient
}

void test("given_a_prefix_when_a_scratch_stream_is_opened_then_should_create_a_unique_stream_and_delete_it_at_scope_end", async () => {
  const calls: string[] = []
  await using root = await Laser.fromClient(recordingClient(calls))
  let name: string
  {
    await using scratch = await root.scratchStream("ci")
    name = scratch.name
    assert.match(name, /^ci-[0-9a-f]{8}$/u)
    assert.equal(scratch.laser.defaultStream, name)
    assert.deepEqual(calls, [`create ${name}`])
  }
  assert.deepEqual(calls, [`create ${name}`, `delete ${name}`])
  await using other = await root.scratchStream("ci")
  assert.notEqual(other.name, name)
})

void test("given_a_scratch_stream_deleted_explicitly_when_its_scope_ends_then_should_not_delete_it_again", async () => {
  const calls: string[] = []
  await using root = await Laser.fromClient(recordingClient(calls))
  let name: string
  {
    await using scratch = await root.scratchStream("ci")
    name = scratch.name
    await scratch.delete()
  }
  assert.deepEqual(calls, [`create ${name}`, `delete ${name}`])
})

void test("given_a_drawn_name_that_already_exists_when_a_scratch_stream_is_opened_then_should_refuse_it", async () => {
  const calls: string[] = []
  const everyName = Object.assign(new Set<string>(), { has: () => true })
  await using root = await Laser.fromClient(recordingClient(calls, everyName))

  await assert.rejects(root.scratchStream("ci"))

  assert.equal(calls.length, 1)
  assert.match(calls[0] ?? "", /^create ci-[0-9a-f]{8}$/u)
})
