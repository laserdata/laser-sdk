import assert from "node:assert/strict"
import { test } from "node:test"
import {
  ContentType,
  IntegrityError,
  checkIn,
  resolveBody,
  type BlobStore
} from "../../src/index.js"

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
