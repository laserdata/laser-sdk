import { agentMessageBody, type AgentMessage } from "./agent/decode.js"
import { IntegrityError } from "./client/errors.js"
import { decodeBodyRef, encodeBodyRef, newBodyRef, validateBodyRef } from "./wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "./wire/cbor.js"
import { ContentType } from "./wire/content.js"

export interface BlobStore {
  put(payload: Uint8Array): Promise<string>
  get(reference: string): Promise<Uint8Array>
}

export async function checkIn(
  store: BlobStore,
  thresholdBytes: number,
  payload: Uint8Array
): Promise<{ readonly payload: Uint8Array; readonly contentType?: typeof ContentType.Ref }> {
  if (payload.byteLength < thresholdBytes) return { payload: payload.slice() }
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", payload))
  const reference = await store.put(payload.slice())
  return {
    payload: encodeNamed(encodeBodyRef(newBodyRef(reference, BigInt(payload.byteLength), digest))),
    contentType: ContentType.Ref
  }
}

/** The bytes a `BodyRef` capsule points at. The capsule comes off the log, so
 * it is validated against the wire caps before the store sees its reference,
 * and fetched bytes whose length or SHA-256 disagree are an `IntegrityError`,
 * never returned unverified. */
export async function resolveBody(store: BlobStore, payload: Uint8Array): Promise<Uint8Array> {
  const context = "body ref"
  const ref = decodeBodyRef(expectMap(decodeOne(payload, context), context), context)
  validateBodyRef(ref)
  const resolved = await store.get(ref.reference)
  if (BigInt(resolved.byteLength) !== ref.sizeBytes) throw new IntegrityError(ref.reference)
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", resolved))
  if (!equalBytes(digest, ref.sha256)) throw new IntegrityError(ref.reference)
  return resolved.slice()
}

/** The real body of a claim-checked message, like Rust
 * `AgentMessage::resolve_body`. It starts from `agentMessageBody`: the AGDX
 * envelope's body for an AGDX record, else the payload. A `ref` content type
 * resolves that body through `resolveBody`. Any other content type returns
 * the body unchanged. */
export async function agentMessageResolveBody(
  message: AgentMessage,
  store: BlobStore
): Promise<Uint8Array> {
  const body = agentMessageBody(message)
  if (message.contentType === ContentType.Ref) return resolveBody(store, body)
  return body.slice()
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false
  let mismatch = 0
  for (let index = 0; index < left.byteLength; index += 1) {
    mismatch |= (left[index] ?? 0) ^ (right[index] ?? 0)
  }
  return mismatch === 0
}
