import assert from "node:assert/strict"
import { test } from "node:test"
import type { Client as IggyClient } from "apache-iggy"
import { isUnavailable } from "../../src/client/error-classify.js"
import { Laser } from "../../src/client/laser.js"
import { newAgentPresence } from "../../src/wire/agent.js"
import { AgentId } from "../../src/types/ids.js"
import { AGDX_SET_CLIENT_METADATA_CODE } from "../../src/wire/codes.js"

const OK = new Uint8Array([0xa1, 0x62, 0x6f, 0x6b, 0xf6])
const TRANSIENT_NOT_ACCEPTED = 58

interface Sent {
  readonly code: number
  readonly replica: number
}

// A three-node client whose `down` replica refuses every request as
// transient, recording the replica each request went to.
function cluster(down: { replica?: number }): {
  readonly client: IggyClient
  readonly sent: Sent[]
} {
  const sent: Sent[] = []
  const toReplica = (code: number, replica: number): Promise<Buffer> => {
    sent.push({ code, replica })
    if (replica === down.replica) {
      return Promise.reject(
        Object.assign(new Error("refused with 58"), { errorCode: TRANSIENT_NOT_ACCEPTED })
      )
    }
    return Promise.resolve(Buffer.from(OK))
  }
  const client = {
    clientProvider: () =>
      Promise.resolve({
        sendCommand: async (
          code: number,
          _payload: Buffer,
          options: { readonly replica?: number }
        ) => {
          const data = await toReplica(code, options.replica ?? 0)
          return { length: data.length, data }
        }
      }),
    sendBinaryRequestToReplica: (code: number, _payload: Buffer, replica: number) =>
      toReplica(code, replica),
    cluster: {
      getClusterMetadata: () =>
        Promise.resolve({
          name: "orbit",
          nodes: [0, 1, 2].map((replicaId) => ({ replicaId, name: `node-${String(replicaId)}` }))
        })
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  return { client, sent }
}

function presenceSends(sent: readonly Sent[]): readonly Sent[] {
  return sent.filter((request) => request.code === AGDX_SET_CLIENT_METADATA_CODE)
}

void test("given_a_presence_pinned_to_a_replica_that_stops_answering_when_cleared_or_advertised_then_should_report_unavailable_keep_the_pin_and_clear_there_once_it_answers", async () => {
  const down: { replica?: number } = {}
  const { client, sent } = cluster(down)
  const laser = await Laser.fromClient(client)
  const presence = newAgentPresence(AgentId.new("risk").wireId(), "risk.inbox")
  try {
    await laser.advertisePresence(presence)
    const pinned = presenceSends(sent).at(-1)?.replica
    assert.ok(pinned !== undefined, "the first advertise pins the replica that answered")

    down.replica = pinned
    await assert.rejects(laser.clearPresence(), (error: unknown) => isUnavailable(error))
    await assert.rejects(laser.advertisePresence(presence), (error: unknown) =>
      isUnavailable(error)
    )
    for (const request of presenceSends(sent)) {
      assert.equal(request.replica, pinned, "every presence frame goes to the pinned replica")
    }

    delete down.replica
    await laser.clearPresence()
    assert.equal(
      presenceSends(sent).at(-1)?.replica,
      pinned,
      "the clear reached the pinned replica"
    )
    await laser.advertisePresence(presence)
  } finally {
    await laser.close()
  }
})
