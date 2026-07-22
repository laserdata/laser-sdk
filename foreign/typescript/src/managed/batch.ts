import { InvalidError } from "../client/errors.js"
import type { Capabilities } from "../client/capabilities.js"
import { executeManaged, type ManagedTransport } from "../client/managed.js"
import { AGDX_BATCH_CODE, BATCH_OP_VERSION } from "../wire/codes.js"
import { BatchCommand } from "../wire/commands.js"
import type { BatchItem } from "../wire/batch.js"
import { MAX_BATCH_OPS, MAX_FRAME_BYTES, MAX_VALUE_BYTES } from "../wire/limits.js"

// The same shape and structural rules as `wire/src/validate.rs`'s
// `Validate for BatchRequest`: an op cap, no nested batch (unbounded fan-out
// through one request), a per-op payload cap, and a total-payload cap.
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

// Execute up to `MAX_BATCH_OPS` independent managed commands in one round
// trip. Input order is preserved, each returned slot holds that operation's
// raw typed-reply bytes. Batching amortizes transport cost, it is not a
// transaction.
export async function executeBatch(
  transport: ManagedTransport,
  capabilities: Capabilities,
  ops: readonly BatchItem[]
): Promise<readonly Uint8Array[]> {
  validateBatchOps(ops)
  const reply = await executeManaged(transport, capabilities, BatchCommand, {
    v: BATCH_OP_VERSION,
    ops
  })
  return reply.results
}
