// Presence stays on the replica that received it. A client whose managed
// requests move elsewhere clears and re-advertises on that replica, and
// reports the replica unavailable while it does not answer.
import assert from "node:assert/strict"
import { test } from "node:test"
import { isUnavailable } from "../../src/client/error-classify.js"
import { Laser } from "../../src/client/laser.js"
import { AgentId } from "../../src/types/ids.js"
import { newAgentPresence } from "../../src/wire/agent.js"
import { TestIggyCluster } from "../support/test-iggy.js"

const RECONNECT = "?reconnection_retries=unlimited&reconnection_interval=100ms"

void test("given_a_presence_pinned_to_a_node_when_that_node_is_down_then_should_report_unavailable_keep_the_pin_and_clear_once_it_returns", async () => {
  const cluster = await TestIggyCluster.start()
  const presence = newAgentPresence(AgentId.new("risk").wireId(), "risk.inbox")
  let laser: Laser | undefined
  try {
    laser = await Laser.connect(`${cluster.endpoint}${RECONNECT}`)
    await laser.advertisePresence(presence)
    process.stderr.write("step advertised\n")
    // The pinned node is whichever node a clear cannot reach once it is down.
    let pinned: number | undefined
    for (let node = 0; node < 3 && pinned === undefined; node += 1) {
      await cluster.killNode(node)
      process.stderr.write(`step killed ${String(node)}\n`)
      try {
        await laser.clearPresence()
      } catch (error) {
        assert.ok(
          isUnavailable(error),
          `a clear with the pinned node down is unavailable: ${String(error)}`
        )
        pinned = node
        break
      }
      process.stderr.write(`step clear ok after killing ${String(node)}, restarting\n`)
      await cluster.startNode(node)
      process.stderr.write(`step restarted ${String(node)}\n`)
      await laser.advertisePresence(presence)
      process.stderr.write(`step re-advertised after ${String(node)}\n`)
    }
    process.stderr.write(`step pinned ${String(pinned)}\n`)
    assert.ok(pinned !== undefined, "one of the nodes holds the presence")

    await assert.rejects(
      laser.advertisePresence(presence),
      (error: unknown) => isUnavailable(error),
      "the presence is never advertised on another node while its node is down"
    )

    process.stderr.write("step advertise elsewhere refused\n")
    await cluster.startNode(pinned)
    process.stderr.write("step pinned node restarted\n")
    const deadline = Date.now() + 30_000
    for (;;) {
      try {
        await laser.clearPresence()
        break
      } catch (error) {
        assert.ok(Date.now() < deadline, `the returned node clears: ${String(error)}`)
        await new Promise((resolve) => setTimeout(resolve, 500))
      }
    }
    await laser.advertisePresence(presence)
    await laser.clearPresence()
  } finally {
    await laser?.close()
    await cluster.close()
  }
})
