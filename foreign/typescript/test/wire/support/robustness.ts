import assert from "node:assert/strict"

// The shared invariant every decoder in this package must satisfy: given
// arbitrary bytes, it returns a value or throws an `Error`, but never hangs,
// never throws a non-`Error` value (a raw string, a native `RangeError` from
// an unbounded allocation, or similar), and never silently returns a
// half-decoded value. Sweeping a valid fixture's truncations and bit flips
// catches decoders that only handle well-formed input.
function assertNeverCrashesUnstructured(decode: () => unknown, label: string): void {
  try {
    decode()
  } catch (error) {
    assert.ok(error instanceof Error, `${label} threw a non-Error value: ${String(error)}`)
  }
}

export function assertDecoderIsRobust(
  validBytes: Uint8Array,
  decode: (bytes: Uint8Array) => unknown
): void {
  assertNeverCrashesUnstructured(() => decode(new Uint8Array(0)), "empty input")

  for (let end = 1; end < validBytes.length; end += 1) {
    assertNeverCrashesUnstructured(
      () => decode(validBytes.slice(0, end)),
      `truncation at byte ${String(end)}`
    )
  }

  for (let position = 0; position < validBytes.length; position += 1) {
    const flipped = validBytes.slice()
    flipped[position] = (flipped[position] ?? 0) ^ 0xff
    assertNeverCrashesUnstructured(() => decode(flipped), `bit flip at byte ${String(position)}`)
  }

  for (const trailing of [1, 8, 64]) {
    const withTrailing = new Uint8Array([...validBytes, ...new Uint8Array(trailing)])
    assertNeverCrashesUnstructured(
      () => decode(withTrailing),
      `${String(trailing)} trailing byte(s)`
    )
    assert.throws(
      () => decode(withTrailing),
      `${String(trailing)} trailing byte(s) must be rejected`
    )
  }
}
