import { IntegrityError } from "./client/errors.js"
import { decodeBodyRef, encodeBodyRef, newBodyRef } from "./wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "./wire/cbor.js"
import { ContentType } from "./wire/content.js"

export interface BlobStore {
  put(payload: Uint8Array): Promise<string>
  get(reference: string): Promise<Uint8Array>
}

export interface CheckedBody {
  readonly payload: Uint8Array
  readonly contentType?: typeof ContentType.Ref
}

export async function checkIn(
  store: BlobStore,
  thresholdBytes: number,
  payload: Uint8Array
): Promise<CheckedBody> {
  if (payload.byteLength < thresholdBytes) return { payload: payload.slice() }
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", payload))
  const reference = await store.put(payload.slice())
  return {
    payload: encodeNamed(encodeBodyRef(newBodyRef(reference, BigInt(payload.byteLength), digest))),
    contentType: ContentType.Ref
  }
}

export async function resolveBody(store: BlobStore, payload: Uint8Array): Promise<Uint8Array> {
  const context = "body ref"
  const ref = decodeBodyRef(expectMap(decodeOne(payload, context), context), context)
  const resolved = await store.get(ref.reference)
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", resolved))
  if (!equalBytes(digest, ref.sha256) || BigInt(resolved.byteLength) !== ref.sizeBytes) {
    throw new IntegrityError(ref.reference)
  }
  return resolved.slice()
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false
  let mismatch = 0
  for (let index = 0; index < left.byteLength; index += 1) {
    mismatch |= (left[index] ?? 0) ^ (right[index] ?? 0)
  }
  return mismatch === 0
}
