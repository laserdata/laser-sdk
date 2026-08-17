import { InvalidError, UnsupportedError } from "../client/errors.js"
import type { Capabilities } from "../client/capabilities.js"
import { executeManaged, type ManagedTransport } from "../client/managed.js"
import {
  AGDX_BATCH_CODE,
  AGDX_KV_CAS_FENCED_CODE,
  AGDX_KV_GET_CODE,
  AGDX_KV_LEASE_CODE,
  AGDX_KV_LEASE_RENEW_CODE,
  AGDX_KV_RELEASE_CODE,
  BATCH_OP_VERSION
} from "../wire/codes.js"
import { BatchCommand } from "../wire/commands.js"
import type { BatchItem } from "../wire/batch.js"
import { decodeOne, expectMap } from "../wire/cbor.js"
import { decodeKvGet } from "../wire/kv.js"
import { MAX_BATCH_OPS, MAX_FRAME_BYTES, MAX_VALUE_BYTES } from "../wire/limits.js"

function validateBatchOps(ops: readonly BatchItem[]): void {
  if (ops.length > MAX_BATCH_OPS) {
    throw new InvalidError(
      `batch has ${String(ops.length)} ops, exceeds cap ${String(MAX_BATCH_OPS)}`
    )
  }
  let total = 0
  for (const item of ops) {
    if (item.code === AGDX_BATCH_CODE) {
      throw new InvalidError("a batch may not contain a batch op")
    }
    if (item.payload.byteLength > MAX_VALUE_BYTES) {
      throw new InvalidError(
        `batch op payload is ${String(item.payload.byteLength)}B, exceeds cap ${String(MAX_VALUE_BYTES)}B`
      )
    }
    total += item.payload.byteLength
  }
  if (total > MAX_FRAME_BYTES) {
    throw new InvalidError(
      `batch total payload is ${String(total)}B, exceeds cap ${String(MAX_FRAME_BYTES)}B`
    )
  }
}

function requiresFencedLeases(item: BatchItem): boolean {
  if (
    item.code === AGDX_KV_LEASE_CODE ||
    item.code === AGDX_KV_LEASE_RENEW_CODE ||
    item.code === AGDX_KV_RELEASE_CODE ||
    item.code === AGDX_KV_CAS_FENCED_CODE
  ) {
    return true
  }
  if (item.code !== AGDX_KV_GET_CODE) return false
  try {
    return (
      decodeKvGet(
        expectMap(decodeOne(item.payload, "batch kv get"), "batch kv get"),
        "batch kv get"
      ).minPosition !== undefined
    )
  } catch {
    return false
  }
}

export async function executeBatch(
  transport: ManagedTransport,
  capabilities: Capabilities,
  ops: readonly BatchItem[]
): Promise<readonly Uint8Array[]> {
  validateBatchOps(ops)
  if (!capabilities.kv.fencedLeases && ops.some(requiresFencedLeases)) {
    throw new UnsupportedError(
      "batch contains a fenced-lease request that this deployment must not decode under the old contract"
    )
  }
  const reply = await executeManaged(transport, capabilities, BatchCommand, {
    v: BATCH_OP_VERSION,
    ops
  })
  return reply.results
}
