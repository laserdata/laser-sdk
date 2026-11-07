import * as ed25519 from "@noble/ed25519"
import { sha256, sha512 } from "@noble/hashes/sha2.js"
import { bytesToHex } from "@noble/hashes/utils.js"
import type { CallOptions } from "./client/call-options.js"
import { InvalidError, SignatureError } from "./client/errors.js"
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
// The JWS algorithm of a card signature (RFC 8037 section 3.1).
const CARD_SIGNATURE_ALGORITHM = "EdDSA"

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

  /** A fresh key from 32 bytes of the platform's cryptographic random source. Store its seed (`toBytes`) like any secret. */
  static generate(): SigningKey {
    return SigningKey.fromBytes(ed25519.utils.randomSecretKey())
  }

  static fromBytes(secret: Uint8Array): SigningKey {
    if (secret.byteLength !== SECRET_KEY_BYTES) {
      throw new SignatureError(`Ed25519 secret seed must be ${String(SECRET_KEY_BYTES)} bytes`)
    }
    const owned = secret.slice()
    const publicBytes = ed25519.getPublicKey(owned)
    return new SigningKey(owned, publicBytes, sha256(publicBytes).slice(0, KEY_ID_BYTES))
  }

  /** The 32-byte secret seed `fromBytes` rebuilds this key from. */
  toBytes(): Uint8Array {
    return this.secret.slice()
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

  /** @internal */
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
    readonly verifying: Uint8Array,
    readonly kind: KeyKind = KeyKind.Agent,
    readonly validFromMicros = 0n,
    readonly validToMicros?: bigint,
    readonly revoked = false
  ) {
    if (principal.length === 0) throw new InvalidError("key principal must not be empty")
    if (verifying.byteLength !== PUBLIC_KEY_BYTES) {
      throw new InvalidError(`Ed25519 public key must be ${String(PUBLIC_KEY_BYTES)} bytes`)
    }
    if (validToMicros !== undefined && validToMicros <= validFromMicros) {
      throw new InvalidError("key validity end must be after its start")
    }
  }

  static agent(principal: string, verifyingKey: Uint8Array): KeyRecord {
    return new KeyRecord(principal, verifyingKey.slice())
  }

  static operator(principal: string, verifyingKey: Uint8Array): KeyRecord {
    return new KeyRecord(principal, verifyingKey.slice(), KeyKind.Operator)
  }

  /** Build a record from a 32-byte Ed25519 public verifying key, rejecting
   * bytes that are not a point on the curve. */
  static fromVerifyingBytes(principal: string, verifying: Uint8Array, kind: KeyKind): KeyRecord {
    if (verifying.byteLength !== PUBLIC_KEY_BYTES) {
      throw new InvalidError("an Ed25519 public key requires exactly 32 bytes")
    }
    try {
      ed25519.Point.fromBytes(verifying)
    } catch (cause) {
      throw new InvalidError("invalid Ed25519 public key", undefined, { cause })
    }
    return new KeyRecord(principal, verifying.slice(), kind)
  }

  validWindow(fromMicros: bigint, toMicros?: bigint): KeyRecord {
    return new KeyRecord(
      this.principal,
      this.verifying.slice(),
      this.kind,
      fromMicros,
      toMicros,
      this.revoked
    )
  }

  /** The 8-byte identifier derived from this record's public key. */
  keyId(): Uint8Array {
    return sha256(this.verifying).slice(0, KEY_ID_BYTES)
  }

  revoke(): KeyRecord {
    return new KeyRecord(
      this.principal,
      this.verifying.slice(),
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
    this.keys.set(keyIdHex(record.verifying), record)
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
      !ed25519.verify(signature.bytes, signingInput(envelope, signature.context), record.verifying)
    ) {
      throw new SignatureError("signature verification failed")
    }
    return { principal: record.principal, kind: record.kind }
  }
}

export class KvKeyRegistry {
  constructor(
    private readonly laser: Laser,
    private readonly namespace = DEFAULT_KEY_NAMESPACE
  ) {}

  /** Enrolls `verifyingKey` as `principal`'s agent key. */
  async enroll(
    principal: string,
    verifyingKey: Uint8Array,
    options: CallOptions = {}
  ): Promise<void> {
    await this.enrollRecord(KeyRecord.agent(principal, verifyingKey), options)
  }

  /** Enrolls or replaces a lifecycle-aware key record with compare-and-swap protection. */
  async enrollRecord(record: KeyRecord, options: CallOptions = {}): Promise<bigint> {
    const kv = this.laser.kv(this.namespace)
    const key = new TextEncoder().encode(keyIdHex(record.verifying))
    const current = await kv.getEntry(key, options)
    const write = kv.set(key).bytes(encodeKeyRecord(record))
    return current === undefined
      ? write.expectAbsent().commit(options)
      : write.expectVersion(current.version).commit(options)
  }

  async revoke(keyId: Uint8Array, options: CallOptions = {}): Promise<bigint> {
    if (keyId.byteLength !== KEY_ID_BYTES) {
      throw new InvalidError(`Ed25519 key id must be ${String(KEY_ID_BYTES)} bytes`)
    }
    const kv = this.laser.kv(this.namespace)
    const key = new TextEncoder().encode(bytesToHex(keyId))
    const current = await kv.getEntry(key, options)
    if (current === undefined) throw new InvalidError("signing key is not enrolled")
    const record = decodeKeyRecord(current.value)
    if (keyIdHex(record.verifying) !== bytesToHex(keyId)) {
      throw new InvalidError("key record identifier differs from its storage key")
    }
    if (record.revoked) return current.version
    return kv
      .set(key)
      .bytes(encodeKeyRecord(record.revoke()))
      .expectVersion(current.version)
      .commit(options)
  }

  async registry(options: CallOptions = {}): Promise<KeyRegistry> {
    const registry = new KeyRegistry()
    const entries = await this.laser.kv(this.namespace).scan().entries(options)
    for (const entry of entries) {
      try {
        const record = decodeKeyRecord(entry.value)
        const expectedKey = new TextEncoder().encode(keyIdHex(record.verifying))
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

/**
 * Verify an on-behalf-of envelope against the headers observed on its record and
 * the record timestamp, and return the signer with its delegated user. The
 * server authorizes only the authenticated caller, so the caller intersects the
 * two principals' grants with `delegatedAllow`.
 */
export function verifyDelegation(
  registry: KeyRegistry,
  envelope: AgentEnvelope,
  observed: SignatureContext,
  atMicros: bigint
): readonly [string, string] | undefined {
  const signer = registry.verifyObservedAt(envelope, observed, atMicros).principal
  const delegated = envelope.metadata?.get(METADATA_DELEGATED_BY)
  return delegated?.kind === "str" ? [signer, delegated.value] : undefined
}

/**
 * Sign an A2A card value as a detached JWS: EdDSA over the RFC 7515 signing
 * input whose payload is the RFC 8785 canonical form of the card without its
 * `signatures`. The protected header names the algorithm and the key id.
 */
export function signCardValue(key: SigningKey, card: unknown): AgentCardSignature {
  const payload = canonicalJson(withoutSignatures(card))
  // Already canonical: `alg` sorts before `kid` and a hex id needs no escape.
  const header = `{"alg":"${CARD_SIGNATURE_ALGORITHM}","kid":"${bytesToHex(key.keyId())}"}`
  const protectedValue = base64url(new TextEncoder().encode(header))
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
  checkCardHeader(signature.protected, verifyingKey)
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
      keyId: sha256(record.verifying).slice(0, KEY_ID_BYTES),
      verifyingKey: record.verifying,
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
    throw new InvalidError("key record identifier does not match its verifying key")
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

const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/

function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "boolean") return JSON.stringify(value)
  if (typeof value === "string") return canonicalString(value)
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      throw new SignatureError("card canonicalization only accepts safe integers")
    }
    return String(value)
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`
  if (typeof value === "object") {
    // String comparison orders UTF-16 code units, the RFC 8785 section 3.2.3 rule.
    const entries = Object.entries(value as Readonly<Record<string, unknown>>).toSorted(
      ([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)
    )
    return `{${entries.map(([key, item]) => `${canonicalString(key)}:${canonicalJson(item)}`).join(",")}}`
  }
  throw new SignatureError("card contains an unsupported JSON value")
}

function canonicalString(value: string): string {
  if (LONE_SURROGATE.test(value)) {
    throw new SignatureError("card canonicalization does not accept lone surrogates")
  }
  return JSON.stringify(value)
}

// The protected header must name EdDSA, carry no `crit` extension (none is
// understood, RFC 7515 section 4.1.11), and name the verifying key if it names one.
function checkCardHeader(protectedValue: string, verifyingKey: Uint8Array): void {
  let header: unknown
  try {
    header = JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(fromBase64url(protectedValue))
    )
  } catch {
    throw new SignatureError("card signature header does not decode")
  }
  if (header === null || Array.isArray(header) || typeof header !== "object") {
    throw new SignatureError("card signature header does not decode")
  }
  const fields = header as Readonly<Record<string, unknown>>
  if (fields["alg"] !== CARD_SIGNATURE_ALGORITHM) {
    throw new SignatureError(`card signature header must name alg ${CARD_SIGNATURE_ALGORITHM}`)
  }
  if ("crit" in fields) {
    throw new SignatureError(
      "card signature header names a critical extension this verifier does not understand"
    )
  }
  if ("kid" in fields && fields["kid"] !== keyIdHex(verifyingKey)) {
    throw new SignatureError("card signature names another key")
  }
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
