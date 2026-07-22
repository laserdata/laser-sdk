export type BytesLike = Uint8Array | ArrayBuffer | ArrayBufferView

// Effectful methods snapshot mutable input before their first `await`,
// matching Apache Iggy's own defensive copy of the request buffer.
export function ownedBytes(value: BytesLike): Uint8Array {
  const view =
    value instanceof Uint8Array
      ? value
      : ArrayBuffer.isView(value)
        ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
        : new Uint8Array(value)
  return view.slice()
}
