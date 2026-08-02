export type BytesLike = Uint8Array | ArrayBuffer | ArrayBufferView

export function ownedBytes(value: BytesLike): Uint8Array {
  const view =
    value instanceof Uint8Array
      ? value
      : ArrayBuffer.isView(value)
        ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
        : new Uint8Array(value)
  return view.slice()
}
