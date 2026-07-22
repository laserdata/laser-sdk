export interface UlidSource {
  nowMilliseconds(): number
  fillRandom(bytes: Uint8Array): void
}

export const SYSTEM_ULID_SOURCE: UlidSource = {
  nowMilliseconds: () => Date.now(),
  fillRandom: (bytes) => {
    crypto.getRandomValues(bytes)
  }
}

const MASK_48 = (1n << 48n) - 1n

export function mintUlidValue(source: UlidSource = SYSTEM_ULID_SOURCE): bigint {
  const timestamp = BigInt(source.nowMilliseconds()) & MASK_48
  const randomBytes = new Uint8Array(10)
  source.fillRandom(randomBytes)
  let random = 0n
  for (const byte of randomBytes) random = (random << 8n) | BigInt(byte)
  return (timestamp << 80n) | random
}
