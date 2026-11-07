const MASK_64 = (1n << 64n) - 1n
const FNV_OFFSET = 0xcbf29ce484222325n
const FNV_PRIME = 0x100000001b3n

function fnv1a(salt: number, segments: readonly Uint8Array[]): bigint {
  let hash = FNV_OFFSET
  hash ^= BigInt(salt)
  hash = (hash * FNV_PRIME) & MASK_64
  for (const segment of segments) {
    for (const byte of segment) {
      hash ^= BigInt(byte)
      hash = (hash * FNV_PRIME) & MASK_64
    }
  }
  return hash
}

export function contentId(segments: readonly Uint8Array[]): bigint {
  const high = fnv1a(0x4d, segments)
  const low = fnv1a(0xc7, segments)
  return (high << 64n) | low
}

export function materializationOperationId(setup: bigint, step: number): bigint {
  const bytes = new Uint8Array(16)
  let remaining = setup
  for (let index = 0; index < 16; index++) {
    bytes[index] = Number(remaining & 255n)
    remaining >>= 8n
  }
  const id = contentId([
    new TextEncoder().encode("laser.materialization"),
    bytes,
    new Uint8Array([step])
  ])
  return id === 0n ? 1n : id
}
