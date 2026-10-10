import assert from "node:assert/strict"
import { test } from "node:test"
import {
  ContentType,
  ConversationId,
  IntegrityError,
  agentMessageBody,
  agentMessageResolveBody,
  checkIn,
  resolveBody,
  type AgentMessage,
  type BlobStore
} from "../../src/index.js"
import { eventEnvelope, parseAgentId } from "../../src/wire/agent.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { ConversationId as WireConversationId, RecordId } from "../../src/wire/ids.js"

class MemoryBlobStore implements BlobStore {
  readonly blobs = new Map<string, Uint8Array>()

  put(payload: Uint8Array): Promise<string> {
    const reference = `blob-${String(this.blobs.size)}`
    this.blobs.set(reference, payload.slice())
    return Promise.resolve(reference)
  }

  get(reference: string): Promise<Uint8Array> {
    const payload = this.blobs.get(reference)
    if (payload === undefined) return Promise.reject(new Error(`missing ${reference}`))
    return Promise.resolve(payload.slice())
  }
}

void test("given_a_small_body_when_checked_in_then_should_pass_through_without_storage", async () => {
  const store = new MemoryBlobStore()
  const payload = new TextEncoder().encode("small")
  const checked = await checkIn(store, 1024, payload)
  assert.deepEqual(checked.payload, payload)
  assert.equal(checked.contentType, undefined)
  assert.equal(store.blobs.size, 0)
})

void test("given_a_large_body_when_checked_in_then_should_round_trip_with_a_ref_content_type", async () => {
  const store = new MemoryBlobStore()
  const payload = new Uint8Array(4096).fill(7)
  const checked = await checkIn(store, 1024, payload)
  assert.equal(checked.contentType, ContentType.Ref)
  assert.deepEqual(await resolveBody(store, checked.payload), payload)
})

void test("given_a_tampered_blob_when_resolved_then_should_raise_an_integrity_error", async () => {
  const store = new MemoryBlobStore()
  const checked = await checkIn(store, 1, new Uint8Array([7, 7, 7]))
  store.blobs.set("blob-0", new Uint8Array([8, 8, 8]))
  await assert.rejects(resolveBody(store, checked.payload), IntegrityError)
})

void test("given_a_claim_checked_agdx_record_when_resolved_then_should_resolve_the_envelope_body", async () => {
  const store = new MemoryBlobStore()
  const body = new Uint8Array(4096).fill(9)
  const checked = await checkIn(store, 1024, body)
  const envelope = eventEnvelope(
    RecordId.fromU128(1n),
    WireConversationId.fromU128(2n),
    parseAgentId("worker"),
    checked.payload
  )
  const message: AgentMessage = {
    provenance: { conversationId: ConversationId.new() },
    payload: encodeNamed(new Map([["envelope", 1]])),
    id: { partitionId: 0, offset: 0n },
    envelope,
    contentType: ContentType.Ref
  }
  assert.deepEqual(await agentMessageResolveBody(message, store), body)
  const { contentType: _ignored, ...plain } = message
  assert.deepEqual(await agentMessageResolveBody(plain, store), agentMessageBody(plain))
})

void test("given_a_blob_of_the_wrong_length_when_resolved_then_should_refuse_with_the_integrity_error", async () => {
  const store = new MemoryBlobStore()
  const checked = await checkIn(store, 1024, new Uint8Array(4096).fill(7))
  for (const reference of store.blobs.keys()) store.blobs.set(reference, new Uint8Array(4095))
  await assert.rejects(resolveBody(store, checked.payload), IntegrityError)
})
