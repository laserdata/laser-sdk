import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto"
import { type BytesLike, ownedBytes } from "../client/bytes.js"
import { InvalidError, TransportError } from "../client/errors.js"

const ALGORITHM = "aes-256-gcm"
const KEY_BYTES = 32
const NONCE_BYTES = 12
const TAG_BYTES = 16
// Apache Iggy's `CannotDecryptData`.
const CANNOT_DECRYPT_DATA = 72

function cannotDecrypt(): TransportError {
  const message = "cannot decrypt data"
  return new TransportError(message, false, {
    cause: Object.assign(new Error(message), { errorCode: CANNOT_DECRYPT_DATA })
  })
}

/**
 * Client-side encryption in Apache Iggy's AES-256-GCM format: a random
 * 12-byte nonce, the ciphertext, then the 16-byte tag. Pass it as
 * `encryption` to a producer to encrypt the payload and the user-header block
 * of every record, and to a consumer with the same key to decrypt them. The
 * Rust and Python clients and Apache Iggy's own encryptor read the same
 * records.
 */
export class Encryption {
  // An ES private field, so inspecting the object never prints the key.
  readonly #key: Uint8Array

  private constructor(key: Uint8Array) {
    this.#key = key
  }

  /** AES-256-GCM with a 32-byte key. Any other length throws `InvalidError`. */
  static aes256Gcm(key: BytesLike): Encryption {
    const owned = ownedBytes(key)
    if (owned.byteLength !== KEY_BYTES) {
      throw new InvalidError(
        `an AES-256-GCM key holds ${String(KEY_BYTES)} bytes, not ${String(owned.byteLength)}`
      )
    }
    return new Encryption(owned)
  }

  /** Encrypts `plaintext` with a fresh nonce. */
  encrypt(plaintext: BytesLike): Uint8Array {
    const nonce = randomBytes(NONCE_BYTES)
    const cipher = createCipheriv(ALGORITHM, this.#key, nonce)
    const sealed = Buffer.concat([cipher.update(ownedBytes(plaintext)), cipher.final()])
    return new Uint8Array(Buffer.concat([nonce, sealed, cipher.getAuthTag()]))
  }

  /** Decrypts what `encrypt` returned. A ciphertext that is short, altered, or
   * sealed with another key throws `TransportError` with Apache Iggy error
   * code 72. */
  decrypt(ciphertext: BytesLike): Uint8Array {
    const data = ownedBytes(ciphertext)
    if (data.byteLength < NONCE_BYTES + TAG_BYTES) throw cannotDecrypt()
    const decipher = createDecipheriv(ALGORITHM, this.#key, data.subarray(0, NONCE_BYTES))
    decipher.setAuthTag(data.subarray(data.byteLength - TAG_BYTES))
    try {
      return new Uint8Array(
        Buffer.concat([
          decipher.update(data.subarray(NONCE_BYTES, data.byteLength - TAG_BYTES)),
          decipher.final()
        ])
      )
    } catch {
      throw cannotDecrypt()
    }
  }
}
