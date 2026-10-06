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

const UTF8_ENCODER = new TextEncoder()
const UTF8_DECODER = new TextDecoder()

/** The UTF-8 bytes of `text`, the payload form every publish and append takes. */
export function utf8(text: string): Uint8Array {
  return UTF8_ENCODER.encode(text)
}

/** A payload read as UTF-8 text. Invalid sequences become U+FFFD, like the
 * Rust `String::from_utf8_lossy` the other SDKs use. */
export function decodeUtf8(bytes: BytesLike): string {
  return UTF8_DECODER.decode(
    bytes instanceof Uint8Array
      ? bytes
      : ArrayBuffer.isView(bytes)
        ? new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength)
        : new Uint8Array(bytes)
  )
}
