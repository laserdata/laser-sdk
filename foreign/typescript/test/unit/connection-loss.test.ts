import assert from "node:assert/strict"
import { EventEmitter } from "node:events"
import { test } from "node:test"
import { watchConnectionLoss } from "../../src/iggy/apache-iggy.js"

function connection(): EventEmitter & { redirecting: boolean } {
  return Object.assign(new EventEmitter(), { redirecting: false })
}

void test("given_a_leader_redirect_when_the_socket_is_swapped_then_should_not_count_as_a_lost_connection", () => {
  const events = connection()
  let lost = 0
  watchConnectionLoss(events, () => {
    lost += 1
  })
  events.redirecting = true
  events.emit("disconnected", false)
  assert.equal(lost, 0)
})

void test("given_a_dropped_socket_when_no_redirect_is_in_flight_then_should_report_a_lost_connection", () => {
  const events = connection()
  let lost = 0
  watchConnectionLoss(events, () => {
    lost += 1
  })
  events.emit("disconnected", true)
  events.redirecting = true
  events.emit("disconnected", false)
  assert.equal(lost, 1)
})
