import * as ed25519 from "@noble/ed25519"
import { sha256, sha512 } from "@noble/hashes/sha2.js"
import { bytesToHex } from "@noble/hashes/utils.js"
import { SignatureError } from "./client/errors.js"
import type { Laser } from "./client/laser.js"
import {
  METADATA_DELEGATED_BY,
  SIGNATURE_DOMAIN,
  SIGNATURE_SCHEME_ED25519,
  encodeAgentEnvelope,
  encodeSignatureContext,
  type AgentEnvelope,
  type Signature,
  type SignatureContext
} from "./wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "./wire/cbor.js"
import { KeyKind, decodeStoredKeyRecord, encodeStoredKeyRecord } from "./wire/keys.js"

export { KeyKind } from "./wire/keys.js"

ed25519.hashes.sha512 = sha512

const KEY_ID_BYTES = 8
const PUBLIC_KEY_BYTES = 32
const SECRET_KEY_BYTES = 32
const SIGNATURE_BYTES = 64

export const DEFAULT_KEY_NAMESPACE = "agent.keys"
export interface VerifiedPrincipal {
  readonly principal: string
  readonly kind: KeyKind
}

export interface AgentCardSignature {
  readonly protected: string
  readonly signature: string
}

export class SigningKey {
  private constructor(
    private readonly secret: Uint8Array,
    private readonly publicBytes: Uint8Array,
    private readonly id: Uint8Array
  ) {}

  static fromBytes(secret: Uint8Array): SigningKey {
    if (secret.byteLength !== SECRET_KEY_BYTES) {
      throw new SignatureError(`Ed25519 secret seed must be ${String(SECRET_KEY_BYTES)} bytes`)
    }
    const owned = secret.slice()
    const publicBytes = ed25519.getPublicKey(owned)
    return new SigningKey(owned, publicBytes, sha256(publicBytes).slice(0, KEY_ID_BYTES))
  }

  keyId(): Uint8Array {
    return this.id.slice()
  }

  verifyingKey(): Uint8Array {
    return this.publicBytes.slice()
  }

  sign(envelope: AgentEnvelope): Signature {
    return this.signInner(envelope)
  }

  signWithContext(envelope: AgentEnvelope, context: SignatureContext): Signature {
    return this.signInner(envelope, context)
  }

  signBytes(payload: Uint8Array): Uint8Array {
    return ed25519.sign(payload, this.secret)
  }

  private signInner(envelope: AgentEnvelope, context?: SignatureContext): Signature {
    return {
      scheme: SIGNATURE_SCHEME_ED25519,
      keyId: this.keyId(),
      bytes: this.signBytes(signingInput(envelope, context)),
      ...(context !== undefined ? { context } : {})
    }
  }
}

export class KeyRecord {
  constructor(
    readonly principal: string,
    readonly verifyingKey: Uint8Array,
    readonly kind: KeyKind = KeyKind.Agent,
    readonly validFromMicros = 0n,
    readonly validToMicros?: bigint,
    readonly revoked = false
  ) {
    if (principal.length === 0) throw new SignatureError("key principal must not be empty")
    if (verifyingKey.byteLength !== PUBLIC_KEY_BYTES) {
      throw new SignatureError(`Ed25519 public key must be ${String(PUBLIC_KEY_BYTES)} bytes`)
    }
    if (validToMicros !== undefined && validToMicros <= validFromMicros) {
      throw new SignatureError("key validity end must be after its start")
    }
  }

  static agent(principal: string, verifyingKey: Uint8Array): KeyRecord {
    return new KeyRecord(principal, verifyingKey.slice())
  }

  static operator(principal: string, verifyingKey: Uint8Array): KeyRecord {
    return new KeyRecord(principal, verifyingKey.slice(), KeyKind.Operator)
  }

  validWindow(fromMicros: bigint, toMicros?: bigint): KeyRecord {
    return new KeyRecord(
      this.principal,
      this.verifyingKey.slice(),
      this.kind,
      fromMicros,
      toMicros,
      this.revoked
    )
  }

  revoke(): KeyRecord {
    return new KeyRecord(
      this.principal,
      this.verifyingKey.slice(),
      this.kind,
      this.validFromMicros,
      this.validToMicros,
      true
    )
  }
}

export class KeyRegistry {
  private readonly keys = new Map<string, KeyRecord>()

  enroll(principal: string, verifyingKey: Uint8Array): void {
    this.enrollRecord(KeyRecord.agent(principal, verifyingKey))
  }

  enrollOperator(principal: string, verifyingKey: Uint8Array): void {
    this.enrollRecord(KeyRecord.operator(principal, verifyingKey))
  }

  enrollRecord(record: KeyRecord): void {
    this.keys.set(keyIdHex(record.verifyingKey), record)
  }

  verify(envelope: AgentEnvelope): string {
    return this.check(envelope).principal
  }

  verifyAt(envelope: AgentEnvelope, atMicros: bigint): VerifiedPrincipal {
    return this.check(envelope, atMicros)
  }

  verifyObservedAt(
    envelope: AgentEnvelope,
    observed: SignatureContext,
    atMicros: bigint
  ): VerifiedPrincipal {
    const signed = envelope.signature?.context
    if (signed === undefined) throw new SignatureError("signature does not bind record headers")
    if (
      signed.contentType !== observed.contentType ||
      signed.agentVersion !== observed.agentVersion
    ) {
      throw new SignatureError("signature context differs from record headers")
    }
    return this.check(envelope, atMicros)
  }

  private check(envelope: AgentEnvelope, atMicros?: bigint): VerifiedPrincipal {
    const signature = envelope.signature
    if (signature === undefined) throw new SignatureError("envelope is not signed")
    if (signature.scheme !== SIGNATURE_SCHEME_ED25519) {
      throw new SignatureError(`unsupported signature scheme ${String(signature.scheme)}`)
    }
    if (
      signature.keyId.byteLength !== KEY_ID_BYTES ||
      signature.bytes.byteLength !== SIGNATURE_BYTES
    ) {
      throw new SignatureError("malformed Ed25519 signature")
    }
    const record = this.keys.get(bytesToHex(signature.keyId))
    if (record === undefined) throw new SignatureError("signing key is not enrolled")
    if (record.revoked) throw new SignatureError("signing key is revoked")
    if (atMicros !== undefined) {
      if (atMicros < record.validFromMicros)
        throw new SignatureError("signing key is not yet valid")
      if (record.validToMicros !== undefined && atMicros >= record.validToMicros) {
        throw new SignatureError("signing key has expired")
      }
    }
    if (
      !ed25519.verify(
        signature.bytes,
        signingInput(envelope, signature.context),
        record.verifyingKey
      )
    ) {
      throw new SignatureError("signature verification failed")
    }
    return { principal: record.principal, kind: record.kind }
  }
}

export class KvKeyRegistry {
  constructor(
    private readonly laser: Laser,
    readonly namespace = DEFAULT_KEY_NAMESPACE
  ) {}

  async enroll(record: KeyRecord): Promise<void> {
    await this.enrollRecord(record)
  }

  async enrollRecord(record: KeyRecord): Promise<bigint> {
    const kv = this.laser.kv(this.namespace)
    const key = new TextEncoder().encode(keyIdHex(record.verifyingKey))
    const current = await kv.getEntry(key)
    const write = kv.set(key).bytes(encodeKeyRecord(record))
    return current === undefined
      ? write.expectAbsent().commit()
      : write.expectVersion(current.version).commit()
  }

  async revoke(keyId: Uint8Array): Promise<bigint> {
    if (keyId.byteLength !== KEY_ID_BYTES) {
      throw new SignatureError(`Ed25519 key id must be ${String(KEY_ID_BYTES)} bytes`)
    }
    const kv = this.laser.kv(this.namespace)
    const key = new TextEncoder().encode(bytesToHex(keyId))
    const current = await kv.getEntry(key)
    if (current === undefined) throw new SignatureError("signing key is not enrolled")
    const record = decodeKeyRecord(current.value)
    if (keyIdHex(record.verifyingKey) !== bytesToHex(keyId)) {
      throw new SignatureError("key record identifier differs from its storage key")
    }
    if (record.revoked) return current.version
    return kv
      .set(key)
      .bytes(encodeKeyRecord(record.revoke()))
      .expectVersion(current.version)
      .commit()
  }

  async registry(): Promise<KeyRegistry> {
    const registry = new KeyRegistry()
    const entries = await this.laser.kv(this.namespace).scan().entries()
    for (const entry of entries) {
      try {
        const record = decodeKeyRecord(entry.value)
        const expectedKey = new TextEncoder().encode(keyIdHex(record.verifyingKey))
        if (!equalBytes(entry.key, expectedKey)) continue
        registry.enrollRecord(record)
      } catch {
        // Ignore malformed key records while rebuilding the registry.
      }
    }
    return registry
  }
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.byteLength === right.byteLength && left.every((byte, index) => byte === right[index])
}

export function verifyDelegation(
  registry: KeyRegistry,
  envelope: AgentEnvelope
): readonly [string, string] | undefined {
  const signer = registry.verify(envelope)
  const delegated = envelope.metadata?.get(METADATA_DELEGATED_BY)
  return delegated?.kind === "string" ? [signer, delegated.value] : undefined
}

export function signCardValue(key: SigningKey, card: unknown): AgentCardSignature {
  const payload = canonicalJson(withoutSignatures(card))
  const protectedValue = base64url(new TextEncoder().encode('{"alg":"EdDSA"}'))
  const input = `${protectedValue}.${base64url(new TextEncoder().encode(payload))}`
  return {
    protected: protectedValue,
    signature: base64url(key.signBytes(new TextEncoder().encode(input)))
  }
}

export function verifyCard(
  card: unknown,
  signature: AgentCardSignature,
  verifyingKey: Uint8Array
): void {
  if (verifyingKey.byteLength !== PUBLIC_KEY_BYTES) throw new SignatureError("invalid public key")
  const payload = canonicalJson(withoutSignatures(card))
  const input = `${signature.protected}.${base64url(new TextEncoder().encode(payload))}`
  const bytes = fromBase64url(signature.signature)
  if (bytes.byteLength !== SIGNATURE_BYTES)
    throw new SignatureError("card signature is not 64 bytes")
  if (!ed25519.verify(bytes, new TextEncoder().encode(input), verifyingKey)) {
    throw new SignatureError("card signature does not verify")
  }
}

export function signingInput(envelope: AgentEnvelope, context?: SignatureContext): Uint8Array {
  const bare = encodeAgentEnvelope(envelope)
  bare.delete("signature")
  const body = encodeNamed(bare)
  const contextBytes =
    context === undefined ? new Uint8Array() : encodeNamed(encodeSignatureContext(context))
  const input = new Uint8Array(
    SIGNATURE_DOMAIN.byteLength + contextBytes.byteLength + body.byteLength
  )
  input.set(SIGNATURE_DOMAIN)
  input.set(contextBytes, SIGNATURE_DOMAIN.byteLength)
  input.set(body, SIGNATURE_DOMAIN.byteLength + contextBytes.byteLength)
  return input
}

function keyIdHex(verifyingKey: Uint8Array): string {
  return bytesToHex(sha256(verifyingKey).slice(0, KEY_ID_BYTES))
}

function encodeKeyRecord(record: KeyRecord): Uint8Array {
  return encodeNamed(
    encodeStoredKeyRecord({
      v: 1,
      principal: record.principal,
      keyId: sha256(record.verifyingKey).slice(0, KEY_ID_BYTES),
      verifyingKey: record.verifyingKey,
      kind: record.kind,
      validFromMicros: record.validFromMicros,
      ...(record.validToMicros !== undefined ? { validToMicros: record.validToMicros } : {}),
      revoked: record.revoked
    })
  )
}

function decodeKeyRecord(payload: Uint8Array): KeyRecord {
  const context = "key record"
  const record = decodeStoredKeyRecord(expectMap(decodeOne(payload, context), context), context)
  const keyId = sha256(record.verifyingKey).slice(0, KEY_ID_BYTES)
  if (bytesToHex(record.keyId) !== bytesToHex(keyId)) {
    throw new SignatureError("key record identifier does not match its verifying key")
  }
  return new KeyRecord(
    record.principal,
    record.verifyingKey,
    record.kind,
    record.validFromMicros,
    record.validToMicros,
    record.revoked
  )
}

function withoutSignatures(card: unknown): unknown {
  if (card === null || Array.isArray(card) || typeof card !== "object") {
    throw new SignatureError("agent card must be an object")
  }
  const clone = { ...(card as Readonly<Record<string, unknown>>) }
  delete clone["signatures"]
  return clone
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return JSON.stringify(value)
  }
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      throw new SignatureError("card canonicalization only accepts safe integers")
    }
    return String(value)
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`
  if (typeof value === "object") {
    const entries = Object.entries(value as Readonly<Record<string, unknown>>).toSorted(
      ([a], [b]) => a.localeCompare(b)
    )
    return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(",")}}`
  }
  throw new SignatureError("card contains an unsupported JSON value")
}

function base64url(payload: Uint8Array): string {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
  let output = ""
  for (let index = 0; index < payload.byteLength; index += 3) {
    const a = payload[index] ?? 0
    const b = payload[index + 1] ?? 0
    const c = payload[index + 2] ?? 0
    const value = (a << 16) | (b << 8) | c
    output += alphabet.charAt((value >>> 18) & 63)
    output += alphabet.charAt((value >>> 12) & 63)
    if (index + 1 < payload.byteLength) output += alphabet.charAt((value >>> 6) & 63)
    if (index + 2 < payload.byteLength) output += alphabet.charAt(value & 63)
  }
  return output
}

function fromBase64url(value: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]*$/.test(value) || value.length % 4 === 1) {
    throw new SignatureError("invalid base64url")
  }
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
  const output: number[] = []
  for (let index = 0; index < value.length; index += 4) {
    const chunk = value.slice(index, index + 4)
    let bits = 0
    for (const character of chunk) bits = (bits << 6) | alphabet.indexOf(character)
    bits <<= 6 * (4 - chunk.length)
    output.push((bits >>> 16) & 0xff)
    if (chunk.length > 2) output.push((bits >>> 8) & 0xff)
    if (chunk.length > 3) output.push(bits & 0xff)
  }
  return Uint8Array.from(output)
}
