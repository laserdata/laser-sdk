import { CodecError, InvalidError } from "../client/errors.js"
import { mintUlidValue } from "../runtime/ulid.js"
import {
  decodeAgentEnvelope,
  encodeAgentEnvelope,
  parseIdempotencyKey,
  validateAgentEnvelope
} from "../wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../wire/cbor.js"
import { RecordId } from "../wire/ids.js"

/** The idempotency key a redriven copy carries: the original key re-keyed by
 * its source position, so the copy survives dedup while a second redrive of
 * the same capsule does not. */
export function redriveKey(key: string, partitionId: number, offset: bigint): string {
  return `${key}/redrive/${String(partitionId)}-${offset.toString()}`
}

/** The AGDX envelope a redrive republishes for the dead-lettered `payload`.
 * Dedup and routing read the envelope body, so its idempotency key is re-keyed
 * and its record id minted fresh. A signed envelope is refused, its signature
 * covers both fields, and so is one past its deadline, which would only
 * dead-letter again. A body that does not decode is returned unchanged. */
export function redrivenEnvelope(
  payload: Uint8Array,
  partitionId: number,
  offset: bigint,
  nowMicros: bigint
): Uint8Array {
  const context = "dead-lettered AGDX envelope"
  let envelope
  try {
    envelope = decodeAgentEnvelope(expectMap(decodeOne(payload, context), context), context)
  } catch (error) {
    // An undecodable body has no key to re-key and is replayed byte for byte.
    if (error instanceof CodecError) return payload
    throw error
  }
  if (envelope.signature !== undefined) {
    throw new InvalidError(
      "a signed AGDX dead letter cannot be redriven, its signature covers the record id and idempotency key"
    )
  }
  if (envelope.deadlineMicros !== undefined && envelope.deadlineMicros <= nowMicros) {
    throw new InvalidError("the dead-lettered AGDX record is past its deadline")
  }
  const copy = {
    ...envelope,
    ...(envelope.record !== undefined ? { record: RecordId.fromU128(mintUlidValue()) } : {}),
    ...(envelope.idempotencyKey !== undefined
      ? {
          idempotencyKey: parseIdempotencyKey(
            redriveKey(envelope.idempotencyKey, partitionId, offset)
          )
        }
      : {})
  }
  validateAgentEnvelope(copy)
  return encodeNamed(encodeAgentEnvelope(copy))
}
