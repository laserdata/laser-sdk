import { connectOptions } from "../client/connect-options.js"
import { publishOptions, publishWithin, type PublishOptions } from "../client/publish-options.js"
import {
  Consumer,
  DeserializeError,
  HeaderKeyFactory,
  HeaderValue as IggyHeaderValueFactory,
  Partitioning,
  PollingStrategy as IggyPollingStrategy,
  ResponseError,
  SimpleClient,
  getRawClient
} from "apache-iggy"
import type {
  ClientConfig,
  ClientCredentials,
  RawClient,
  SendMessagesConfirmation,
  SendMessagesResponse
} from "apache-iggy"
import { readFileSync } from "node:fs"
import { isIP } from "node:net"
import {
  AmbiguousMutationError,
  ConfigError,
  ProtocolError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "../client/errors.js"
import { LASERDATA_ROOT_CA } from "../client/laserdata-ca.js"
import type { PollingStrategy } from "../stream/polling-strategy.js"
import type { Routing } from "../stream/routing.js"
import { Mutex } from "../runtime/mutex.js"
import { mintUlidValue } from "../runtime/ulid.js"
import { encodeNamed } from "../wire/cbor.js"
import { isIdempotentManagedRequest } from "../wire/codes.js"
import { MANAGED_REQUEST_VERSION, encodeManagedRequestEnvelope } from "../wire/mutation.js"

export interface PolledMessage {
  readonly payload: Uint8Array
  readonly partitionId: number
  readonly offset: bigint
  readonly timestampMicros?: bigint
  readonly headers: ReadonlyMap<string, IggyHeaderValue>
  /**
   * Why the header block did not decode, when it did not: its structure, the
   * `agdx.ct` entry, or another entry. A valid content type can remain available.
   */
  readonly headersMalformed?: HeaderFault
}

/** The part of a user-header block that did not decode. */
export type HeaderFault = "structure" | "content_type" | "entry"

export type IggyClient = SimpleClient
export type ClientOwnership = "owned" | "borrowed"
export type { SendMessagesConfirmation, SendMessagesResponse }

const DEFAULT_RECONNECT_INTERVAL_MS = 1_000
const ACCEPT_STAGE = "Iggy server to accept the connection"
const LOGIN_STAGE = "Iggy login reply"
const VSR_HEARTBEAT_INTERVAL_MS = 5_000
const TRANSIENT_NOT_COMMITTED = 57
const TRANSIENT_NOT_ACCEPTED = 58
const POLLED_HEAD_BYTES = 16
const BATCH_HEADER_BYTES = 256
const FRAME_HEADER_BYTES = 48
const STRING_HEADER_KIND = 2
const CONTENT_TYPE_HEADER = "agdx.ct"
const STRICT_UTF8 = new TextDecoder("utf-8", { fatal: true })

export function toNodeBuffer(bytes: Uint8Array): Buffer {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength)
}

export interface MessageWithHeaders {
  readonly payload: Uint8Array
  readonly headers: ReadonlyMap<string, IggyHeaderValue>
}

/** Represents every Apache Iggy user-header kind. */
export type IggyHeaderValue =
  | { readonly kind: "raw"; readonly value: Uint8Array }
  | { readonly kind: "string"; readonly value: string }
  | { readonly kind: "bool"; readonly value: boolean }
  | { readonly kind: "int8"; readonly value: number }
  | { readonly kind: "int16"; readonly value: number }
  | { readonly kind: "int32"; readonly value: number }
  | { readonly kind: "int64"; readonly value: bigint }
  | { readonly kind: "int128"; readonly value: Uint8Array }
  | { readonly kind: "uint8"; readonly value: number }
  | { readonly kind: "uint16"; readonly value: number }
  | { readonly kind: "uint32"; readonly value: number }
  | { readonly kind: "uint64"; readonly value: bigint }
  | { readonly kind: "uint128"; readonly value: Uint8Array }
  | { readonly kind: "float"; readonly value: number }
  | { readonly kind: "double"; readonly value: number }

const SYNC_CONSUMER_GROUP_CODE = 606

export type ConsumerTarget =
  | { readonly kind: "single"; readonly partitionId: number; readonly name?: string }
  | { readonly kind: "group"; readonly name: string; readonly partitionId?: number }

export type ConsumerOffsetTarget =
  | { readonly kind: "group"; readonly name: string }
  | { readonly kind: "consumer"; readonly name: string }

export interface LaserTransport {
  readonly publishRetriesManaged?: boolean
  readonly kind: "apache-iggy"
  readonly iggyClient: SimpleClient
  sendManaged(
    code: number,
    payload: Uint8Array,
    options?: { readonly retryAfterReconnect?: boolean }
  ): Promise<Uint8Array>
  ensureStream(name: string): Promise<void>
  deleteStream(name: string): Promise<boolean>
  ensureTopic(streamId: string, topicId: string, partitions: number): Promise<void>
  ensureConsumerGroup(streamId: string, topicId: string, name: string): Promise<void>
  ensureTopicWithExpiry?(
    streamId: string,
    topicId: string,
    partitions: number,
    messageExpiryMicros: bigint
  ): Promise<void>
  findTopicPartitionCount(streamId: string, topicId: string): Promise<number | undefined>
  getTopicPartitionCount(streamId: string, topicId: string): Promise<number>
  resolveStreamTopicIds?(
    streamId: string,
    topicId: string
  ): Promise<{ readonly streamId: number; readonly topicId: number }>
  resolveStreamTopicNames?(
    streamId: number,
    topicId: number
  ): Promise<{ readonly stream: string; readonly topic: string } | undefined>
  sendMessages(
    streamId: string,
    topicId: string,
    payloads: readonly Uint8Array[],
    routing: Routing
  ): Promise<SendMessagesResponse>
  /** Sends one message with exact headers and optional key or partition routing. */
  sendMessageWithHeaders(
    streamId: string,
    topicId: string,
    payload: Uint8Array,
    headers: ReadonlyMap<string, IggyHeaderValue>,
    partitionKey?: string | Uint8Array,
    partitionId?: number
  ): Promise<SendMessagesResponse>
  sendMessagesWithHeaders(
    streamId: string,
    topicId: string,
    messages: readonly MessageWithHeaders[],
    partitionKey?: string | Uint8Array,
    partitionId?: number,
    options?: Partial<PublishOptions>
  ): Promise<SendMessagesResponse>
  pollMessages(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    strategy: PollingStrategy,
    count: number,
    autoCommit: boolean
  ): Promise<readonly PolledMessage[]>
  storeOffset(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    partitionId: number,
    offset: bigint
  ): Promise<void>
  getConsumerOffset?(
    streamId: string,
    topicId: string,
    target: ConsumerOffsetTarget,
    partitionId: number
  ): Promise<{ readonly storedOffset: bigint; readonly currentOffset: bigint } | undefined>
  joinConsumerGroup(streamId: string, topicId: string, name: string): Promise<void>
  syncConsumerGroup?(
    streamId: string,
    topicId: string,
    name: string
  ): Promise<
    | {
        readonly generation: bigint
        readonly partitions: readonly number[]
        readonly rejoined?: boolean
      }
    | undefined
  >
  leaveConsumerGroup(streamId: string, topicId: string, name: string | number): Promise<void>
  /** Join an existing consumer group without creating it. */
  joinExistingConsumerGroup?(
    streamId: string,
    topicId: string,
    name: string | number
  ): Promise<void>
  /**
   * An ordinary authenticated connection to another node of this deployment,
   * with the same credentials and TLS settings. A node that reports an
   * unspecified address is reached through the host this transport connected to.
   */
  openNodeConnection?(ip: string, port: number): Promise<NodeConnection>
  /** How many nodes the deployment has, when the transport can ask. */
  clusterNodeCount?(): Promise<number>
  /**
   * A second authenticated connection to the node this transport reaches,
   * which holds its own consumer-group memberships and never rejoins them on
   * its own. A group reader joins through it, so every reader is its own
   * member and a dropped reader leaves with its connection.
   */
  openCoordinator?(): Promise<CoordinatorConnection>
  /** Whether this transport can open node and coordinator connections of its own. */
  readonly connectsNodes?: boolean
  close(): Promise<void>
}

/** A connection to one node, owned by its opener. */
export interface NodeConnection {
  send(code: number, payload: Uint8Array): Promise<Uint8Array>
  close(): Promise<void>
}

/** A dedicated coordinator connection, owned by its opener. */
export interface CoordinatorConnection extends NodeConnection {
  joinConsumerGroup(streamId: string, topicId: string, name: string | number): Promise<void>
  leaveConsumerGroup(streamId: string, topicId: string, name: string | number): Promise<void>
}

function toIggyConsumer(target: ConsumerTarget) {
  return target.kind === "single"
    ? target.name === undefined
      ? Consumer.Single
      : { kind: 1 as const, id: target.name }
    : Consumer.Group(target.name)
}

function toIggyOffsetConsumer(target: ConsumerOffsetTarget) {
  return target.kind === "group"
    ? Consumer.Group(target.name)
    : { kind: 1 as const, id: target.name }
}

function rotateLeft(value: number, bits: number): number {
  return ((value << bits) | (value >>> (32 - bits))) >>> 0
}

function xxHashRound(accumulator: number, lane: number): number {
  const added = (accumulator + Math.imul(lane, 0x85ebca77)) >>> 0
  return Math.imul(rotateLeft(added, 13), 0x9e3779b1) >>> 0
}

export function xxHash32(bytes: Uint8Array): number {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  let offset = 0
  let hash: number
  if (bytes.byteLength >= 16) {
    let lane1 = 0x24234428
    let lane2 = 0x85ebca77
    let lane3 = 0
    let lane4 = 0x61c8864f
    const limit = bytes.byteLength - 16
    do {
      lane1 = xxHashRound(lane1, view.getUint32(offset, true))
      lane2 = xxHashRound(lane2, view.getUint32(offset + 4, true))
      lane3 = xxHashRound(lane3, view.getUint32(offset + 8, true))
      lane4 = xxHashRound(lane4, view.getUint32(offset + 12, true))
      offset += 16
    } while (offset <= limit)
    hash =
      (rotateLeft(lane1, 1) +
        rotateLeft(lane2, 7) +
        rotateLeft(lane3, 12) +
        rotateLeft(lane4, 18)) >>>
      0
  } else {
    hash = 0x165667b1
  }
  hash = (hash + bytes.byteLength) >>> 0
  while (offset + 4 <= bytes.byteLength) {
    hash = (hash + Math.imul(view.getUint32(offset, true), 0xc2b2ae3d)) >>> 0
    hash = Math.imul(rotateLeft(hash, 17), 0x27d4eb2f) >>> 0
    offset += 4
  }
  while (offset < bytes.byteLength) {
    hash = (hash + Math.imul(bytes[offset] ?? 0, 0x165667b1)) >>> 0
    hash = Math.imul(rotateLeft(hash, 11), 0x9e3779b1) >>> 0
    offset += 1
  }
  hash ^= hash >>> 15
  hash = Math.imul(hash, 0x85ebca77) >>> 0
  hash ^= hash >>> 13
  hash = Math.imul(hash, 0xc2b2ae3d) >>> 0
  hash ^= hash >>> 16
  return hash >>> 0
}

function toIggyPollingStrategy(strategy: PollingStrategy) {
  switch (strategy.kind) {
    case "first":
      return IggyPollingStrategy.First
    case "last":
      return IggyPollingStrategy.Last
    case "next":
      return IggyPollingStrategy.Next
    case "offset":
      return IggyPollingStrategy.Offset(strategy.value)
    case "timestamp":
      return IggyPollingStrategy.Timestamp(strategy.value)
  }
}

function toIggyHeaderValue(value: IggyHeaderValue) {
  switch (value.kind) {
    case "raw":
      return IggyHeaderValueFactory.Raw(toNodeBuffer(value.value))
    case "string":
      return IggyHeaderValueFactory.String(value.value)
    case "bool":
      return IggyHeaderValueFactory.Bool(value.value)
    case "int8":
      return IggyHeaderValueFactory.Int8(value.value)
    case "int16":
      return IggyHeaderValueFactory.Int16(value.value)
    case "int32":
      return IggyHeaderValueFactory.Int32(value.value)
    case "int64":
      return IggyHeaderValueFactory.Int64(value.value)
    case "int128":
      return IggyHeaderValueFactory.Int128(toNodeBuffer(value.value))
    case "uint8":
      return IggyHeaderValueFactory.Uint8(value.value)
    case "uint16":
      return IggyHeaderValueFactory.Uint16(value.value)
    case "uint32":
      return IggyHeaderValueFactory.Uint32(value.value)
    case "uint64":
      return IggyHeaderValueFactory.Uint64(value.value)
    case "uint128":
      return IggyHeaderValueFactory.Uint128(toNodeBuffer(value.value))
    case "float":
      return IggyHeaderValueFactory.Float(value.value)
    case "double":
      return IggyHeaderValueFactory.Double(value.value)
  }
}

const HEADER_KIND_BY_NUMBER: Readonly<Record<number, IggyHeaderValue["kind"]>> = {
  1: "raw",
  2: "string",
  3: "bool",
  4: "int8",
  5: "int16",
  6: "int32",
  7: "int64",
  8: "int128",
  9: "uint8",
  10: "uint16",
  11: "uint32",
  12: "uint64",
  13: "uint128",
  14: "float",
  15: "double"
}

function fromParsedHeaderValue(kind: number, value: unknown): IggyHeaderValue | undefined {
  const tag = HEADER_KIND_BY_NUMBER[kind]
  if (tag === undefined) return undefined
  switch (tag) {
    case "raw":
    case "int128":
    case "uint128": {
      if (!(value instanceof Buffer)) return undefined
      return { kind: tag, value: new Uint8Array(value) }
    }
    case "string":
      return typeof value === "string" ? { kind: tag, value } : undefined
    case "bool":
      return typeof value === "boolean" ? { kind: tag, value } : undefined
    case "int64":
    case "uint64":
      return typeof value === "bigint" ? { kind: tag, value } : undefined
    case "int8":
    case "int16":
    case "int32":
    case "uint8":
    case "uint16":
    case "uint32":
    case "float":
    case "double":
      return typeof value === "number" ? { kind: tag, value } : undefined
  }
}

function parsedHeadersToMap(
  entries: readonly {
    readonly key: { readonly value: unknown }
    readonly value: { readonly kind: number; readonly value: unknown }
  }[]
): ReadonlyMap<string, IggyHeaderValue> {
  const map = new Map<string, IggyHeaderValue>()
  for (const entry of entries) {
    if (typeof entry.key.value !== "string") continue
    const value = fromParsedHeaderValue(entry.value.kind, entry.value.value)
    if (value !== undefined) map.set(entry.key.value, value)
  }
  return map
}

/** The Iggy error code a server reply carried, through any transport wrapping. */
export function serverErrorCode(error: unknown): number | undefined {
  for (let current: unknown = error; current !== undefined;) {
    if (current instanceof ResponseError) return current.errorCode
    current = current instanceof Error ? current.cause : undefined
  }
  return undefined
}

/**
 * The records of a standard polled-messages body, decoded exactly as stored:
 * original offsets, per-record microsecond timestamps, payloads, and typed
 * user headers.
 */
export function decodePolledBody(body: Uint8Array): readonly PolledMessage[] {
  const malformed = (what: string): ProtocolError =>
    new ProtocolError(`the polled record body has a malformed ${what}`)
  if (body.byteLength < POLLED_HEAD_BYTES) throw malformed("head")
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength)
  const partitionId = view.getUint32(0, true)
  const messages: PolledMessage[] = []
  let position = POLLED_HEAD_BYTES
  while (position < body.byteLength) {
    if (position + BATCH_HEADER_BYTES > body.byteLength) throw malformed("batch header")
    const baseOffset = view.getBigUint64(position + 8, true)
    const baseTimestamp = view.getBigUint64(position + 16, true)
    const batchLength = view.getBigUint64(position + 32, true)
    if (
      batchLength < BigInt(BATCH_HEADER_BYTES) ||
      batchLength > BigInt(body.byteLength - position)
    )
      throw malformed("batch length")
    const batchEnd = position + Number(batchLength)
    position += BATCH_HEADER_BYTES
    while (position < batchEnd) {
      if (position + FRAME_HEADER_BYTES > batchEnd) throw malformed("message frame")
      const offsetDelta = view.getUint32(position + 24, true)
      const headersLength = view.getUint32(position + 32, true)
      const payloadLength = view.getUint32(position + 36, true)
      const payloadStart = position + FRAME_HEADER_BYTES
      const headersStart = payloadStart + payloadLength
      const frameEnd = headersStart + headersLength
      if (frameEnd > batchEnd) throw malformed("message frame")
      const block = body.subarray(headersStart, frameEnd)
      const headers = decodeUserHeaders(block)
      const contentType = headers === "entry" ? decodeUserHeaders(block, true) : headers
      messages.push({
        payload: body.subarray(payloadStart, headersStart),
        partitionId,
        offset: baseOffset + BigInt(offsetDelta),
        timestampMicros: baseTimestamp,
        ...(typeof headers === "string"
          ? {
              headers: typeof contentType === "string" ? new Map() : contentType,
              headersMalformed: typeof contentType === "string" ? contentType : headers
            }
          : { headers })
      })
      position = frameEnd
    }
  }
  return messages
}

// The typed user headers as the server reads them for a filter: string keys
// only, a kind this build does not know kept raw, a value that does not fit
// its kind faulting the record, and the last of a repeated key kept. A structurally broken
// block names what did not decode, which the server treats as a malformed
// record when its filter needs that part.
function decodeUserHeaders(
  block: Uint8Array,
  contentTypeOnly = false
): ReadonlyMap<string, IggyHeaderValue> | HeaderFault {
  const headers = new Map<string, IggyHeaderValue>()
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength)
  let position = 0
  while (position < block.byteLength) {
    if (position + 5 > block.byteLength) return "structure"
    const keyKind = view.getUint8(position)
    const keyStart = position + 5
    const keyEnd = keyStart + view.getUint32(position + 1, true)
    if (
      keyKind === 0 ||
      keyEnd === keyStart ||
      keyEnd - keyStart > 255 ||
      keyEnd + 5 > block.byteLength
    )
      return "structure"
    const valueKind = view.getUint8(keyEnd)
    const valueStart = keyEnd + 5
    const valueEnd = valueStart + view.getUint32(keyEnd + 1, true)
    if (
      valueKind === 0 ||
      valueEnd === valueStart ||
      valueEnd - valueStart > 255 ||
      valueEnd > block.byteLength
    )
      return "structure"
    position = valueEnd
    if (keyKind !== STRING_HEADER_KIND) continue
    if (
      contentTypeOnly &&
      (keyEnd - keyStart !== CONTENT_TYPE_HEADER.length ||
        CONTENT_TYPE_HEADER.split("").some(
          (character, index) => block[keyStart + index] !== character.charCodeAt(0)
        ))
    )
      continue
    let key: string
    try {
      key = STRICT_UTF8.decode(block.subarray(keyStart, keyEnd))
    } catch {
      return "entry"
    }
    const value = headerValueOf(valueKind, block.subarray(valueStart, valueEnd))
    if (value === undefined) return key === CONTENT_TYPE_HEADER ? "content_type" : "entry"
    headers.set(key, value)
  }
  return headers
}

function headerValueOf(kind: number, bytes: Uint8Array): IggyHeaderValue | undefined {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const sized = (size: number): boolean => bytes.byteLength === size
  switch (kind) {
    case STRING_HEADER_KIND:
      try {
        return { kind: "string", value: STRICT_UTF8.decode(bytes) }
      } catch {
        return undefined
      }
    case 3:
      return sized(1) && (bytes[0] === 0 || bytes[0] === 1)
        ? { kind: "bool", value: bytes[0] === 1 }
        : undefined
    case 4:
      return sized(1) ? { kind: "int8", value: view.getInt8(0) } : undefined
    case 5:
      return sized(2) ? { kind: "int16", value: view.getInt16(0, true) } : undefined
    case 6:
      return sized(4) ? { kind: "int32", value: view.getInt32(0, true) } : undefined
    case 7:
      return sized(8) ? { kind: "int64", value: view.getBigInt64(0, true) } : undefined
    case 8:
      return sized(16) ? { kind: "int128", value: bytes.slice() } : undefined
    case 9:
      return sized(1) ? { kind: "uint8", value: view.getUint8(0) } : undefined
    case 10:
      return sized(2) ? { kind: "uint16", value: view.getUint16(0, true) } : undefined
    case 11:
      return sized(4) ? { kind: "uint32", value: view.getUint32(0, true) } : undefined
    case 12:
      return sized(8) ? { kind: "uint64", value: view.getBigUint64(0, true) } : undefined
    case 13:
      return sized(16) ? { kind: "uint128", value: bytes.slice() } : undefined
    case 14:
      return sized(4) ? { kind: "float", value: view.getFloat32(0, true) } : undefined
    case 15:
      return sized(8) ? { kind: "double", value: view.getFloat64(0, true) } : undefined
    default:
      return { kind: "raw", value: bytes.slice() }
  }
}

interface ParsedConnectionString {
  readonly host: string
  readonly port: number
  /** The TLS name to verify, when it differs from `host`. */
  readonly servername?: string
  readonly credentials: ClientCredentials
  readonly tls: boolean
  readonly ca?: string
  readonly reconnection: {
    readonly intervalMs: number
    readonly maxRetries: number | undefined
  }
}

function laserDataHost(host: string): boolean {
  const normalized = host.toLowerCase()
  return (
    normalized === "laserdata.cloud" ||
    normalized.endsWith(".laserdata.cloud") ||
    normalized === "laserdata.com" ||
    normalized.endsWith(".laserdata.com")
  )
}

function parseReconnectInterval(value: string | null): number {
  if (value === null) return DEFAULT_RECONNECT_INTERVAL_MS
  const match = /^(\d+)(ms|s|m)$/.exec(value)
  if (match === null) {
    throw new ConfigError("reconnection_interval must use ms, s, or m")
  }
  const amount = Number(match[1])
  const unit = match[2]
  const multiplier = unit === "ms" ? 1 : unit === "s" ? 1_000 : 60_000
  const intervalMs = amount * multiplier
  if (!Number.isSafeInteger(intervalMs) || intervalMs < 0) {
    throw new ConfigError("reconnection_interval is outside the supported range")
  }
  return intervalMs
}

function parseReconnectRetries(value: string | null): number | undefined {
  if (value === "unlimited") return undefined
  if (value === null) return undefined
  const retries = Number(value)
  if (!Number.isSafeInteger(retries) || retries < 0) {
    throw new ConfigError("reconnection_retries must be a non-negative integer or unlimited")
  }
  return retries
}

export function parseConnectionString(
  connectionString: string,
  env: Readonly<Record<string, string | undefined>> = process.env
): ParsedConnectionString {
  const trimmed = connectionString.trim()
  const withScheme =
    trimmed.startsWith("iggy://") || trimmed.startsWith("iggy+") ? trimmed : `iggy://${trimmed}`
  let url: URL
  try {
    url = new URL(withScheme)
  } catch (cause) {
    // The string carries a password, so it is never echoed. Errors below can
    // name the parsed authority, which holds no credential.
    throw new ConfigError("invalid connection string", { cause })
  }

  if (url.protocol !== "iggy:" && url.protocol !== "iggy+tcp:") {
    throw new ConfigError(`unsupported connection scheme: ${url.protocol}`)
  }

  if (!url.hostname) {
    throw new ConfigError("connection string missing host")
  }

  const port = url.port ? Number(url.port) : 8090
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new ConfigError(`connection string has invalid port: ${url.port}`)
  }

  const username = decodeURIComponent(url.username)
  const password = decodeURIComponent(url.password)
  const credentials: ClientCredentials =
    username.length === 0
      ? { username: "iggy", password: "iggy" }
      : password.length === 0
        ? { token: username }
        : { username, password }

  // Read by value, not by presence: `LASER_NO_TLS=0` and `=false` must not
  // silently downgrade a managed host to plaintext.
  const noTls = ["1", "true", "yes", "on"].includes(
    (env["LASER_NO_TLS"] ?? "").trim().toLowerCase()
  )
  const autoTls = !noTls && laserDataHost(url.hostname)
  const caPath = url.searchParams.get("tls_ca_file") ?? env["LASER_TLS_CERT"]
  const customTls = !noTls && caPath !== undefined && caPath.length > 0
  const explicitTls = url.searchParams.get("tls")
  const tls = explicitTls === null ? autoTls || customTls : explicitTls === "true"
  let ca: string | undefined
  if (caPath !== undefined && caPath.length > 0) {
    try {
      ca = readFileSync(caPath, "utf8")
    } catch (cause) {
      throw new ConfigError(`failed to read TLS CA file: ${caPath}`, { cause })
    }
  } else if (autoTls) {
    ca = LASERDATA_ROOT_CA
  }

  const reconnection = {
    intervalMs: parseReconnectInterval(url.searchParams.get("reconnection_interval")),
    maxRetries: parseReconnectRetries(url.searchParams.get("reconnection_retries"))
  }

  return {
    host: url.hostname,
    port,
    credentials,
    tls,
    ...(ca !== undefined ? { ca } : {}),
    reconnection
  }
}

interface ConnectedClient {
  readonly client: SimpleClient
  readonly raw: RawClient
}

export interface ClientConnectionEvents {
  readonly redirecting: boolean
  on(event: "error", listener: (cause?: unknown) => void): void
  on(event: "disconnected", listener: (hadError: boolean) => void): void
  once(event: "error", listener: (cause?: unknown) => void): void
  once(event: "connect", listener: () => void): void
  off(event: "error", listener: (cause?: unknown) => void): void
  off(event: "connect", listener: () => void): void
}

interface RawClientConnection {
  readonly connection: ClientConnectionEvents
}

// The client swaps sockets on its own when it follows a leader move or walks
// the roster. That drop is deliberate and nothing was lost, so retiring the
// client on it would abort the very send it is relocating.
export function watchConnectionLoss(connection: ClientConnectionEvents, onLost: () => void): void {
  connection.on("disconnected", () => {
    if (connection.redirecting) return
    onLost()
  })
}

async function connectSimpleClient(
  parsed: ParsedConnectionString,
  deadline?: number,
  pinned = false
): Promise<ConnectedClient> {
  const config: ClientConfig = parsed.tls
    ? {
        heartbeatInterval: VSR_HEARTBEAT_INTERVAL_MS,
        transport: "TLS",
        options: {
          port: parsed.port,
          host: parsed.host,
          ...(parsed.servername !== undefined
            ? { servername: parsed.servername }
            : isIP(parsed.host) === 0
              ? { servername: parsed.host }
              : {}),
          ...(parsed.ca !== undefined ? { ca: parsed.ca } : {})
        },
        credentials: parsed.credentials,
        reconnect: { enabled: false, interval: 0, maxRetries: 0 }
      }
    : {
        heartbeatInterval: VSR_HEARTBEAT_INTERVAL_MS,
        transport: "TCP",
        options: { port: parsed.port, host: parsed.host },
        credentials: parsed.credentials,
        reconnect: { enabled: false, interval: 0, maxRetries: 0 }
      }

  let raw: RawClient | undefined
  try {
    raw = getRawClient(config)
  } catch (cause) {
    throw new ConfigError("invalid Apache Iggy client configuration", { cause })
  }
  try {
    const connection = (raw as RawClient & RawClientConnection).connection
    const client = pinned ? pinnedClient(raw, deadline) : new SimpleClient(raw)
    // The client emits `connect` only once the transport, TLS included, is up, so it
    // separates a server that never accepts the socket from one that never answers login.
    let accepted = false
    const markAccepted = (): void => {
      accepted = true
    }
    connection.once("connect", markAccepted)
    const ready = new Promise<void>((resolve, reject) => {
      const failed = (cause?: unknown): void => {
        reject(cause instanceof Error ? cause : new Error(String(cause)))
      }
      connection.once("error", failed)
      const login = async (): Promise<void> => {
        if (pinned) {
          const connection = (raw as RawClient & PinnedRawClient).connection
          await connection.connect(true)
          if ("token" in parsed.credentials) await client.session.loginWithToken(parsed.credentials)
          else await client.session.login(parsed.credentials)
        }
        await client.client.getMe()
      }
      login().then(() => {
        connection.off("error", failed)
        resolve()
      }, reject)
    })
    void ready.catch(() => undefined)
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
      await (deadline === undefined
        ? ready
        : Promise.race([
            ready,
            new Promise<never>((_, reject) => {
              timer = setTimeout(
                () => {
                  reject(new TimeoutError(accepted ? LOGIN_STAGE : ACCEPT_STAGE))
                },
                Math.max(0, deadline - Date.now())
              )
            })
          ]))
    } finally {
      clearTimeout(timer)
      connection.off("connect", markAccepted)
    }
    connection.on("error", () => undefined)
    return { client, raw }
  } catch (cause) {
    raw.destroy()
    if (cause instanceof TimeoutError || cause instanceof UnsupportedError) throw cause
    throw new TransportError(`failed to connect to ${parsed.host}:${String(parsed.port)}`, true, {
      cause
    })
  }
}

interface PinnedRawClient {
  readonly connection: ClientConnectionEvents & { connect(boundDial?: boolean): Promise<unknown> }
  readonly _queueCommand?: (
    code: number,
    payload: Buffer,
    handleResponse: boolean,
    last: boolean,
    followsLeaderMoves: boolean,
    deadline: number
  ) => ReturnType<RawClient["sendCommand"]>
}

// Use the same request queue the Apache Iggy SDK uses for its native partition
// data connections. This bypasses coordinator login settlement on this connection.
function pinnedClient(raw: RawClient, loginDeadline: number | undefined): SimpleClient {
  const data = raw as RawClient & PinnedRawClient
  const queue = data._queueCommand
  if (typeof queue !== "function")
    throw new UnsupportedError(
      "this Apache Iggy SDK does not expose the partition data request queue required by filtered Primary reads"
    )
  const facade: RawClient = {
    sendCommand(code, payload, options) {
      return queue.call(
        data,
        code,
        payload,
        options?.handleResponse ?? true,
        options?.last ?? true,
        false,
        options?.deadline ??
          (raw.isAuthenticated
            ? Date.now() + connectOptions().timeoutMs
            : (loginDeadline ?? Date.now() + connectOptions().timeoutMs))
      )
    },
    get isAuthenticated() {
      return raw.isAuthenticated
    },
    authenticate: (credentials) => raw.authenticate(credentials),
    destroy: () => {
      raw.destroy()
    },
    on: (event, callback) => {
      raw.on(event, callback)
    },
    once: (event, callback) => {
      raw.once(event, callback)
    },
    getReadStream: () => raw.getReadStream()
  }
  return new SimpleClient(facade)
}

function serverResponseError(error: unknown): Error | undefined {
  let current = error
  for (let depth = 0; depth < 8; depth += 1) {
    if (typeof current !== "object" || current === null) return undefined
    if (
      "errorCode" in current &&
      typeof (current as { readonly errorCode?: unknown }).errorCode === "number"
    ) {
      return current instanceof Error ? current : new Error("server rejected connection")
    }
    current = "cause" in current ? (current as { readonly cause?: unknown }).cause : undefined
  }
  return undefined
}

async function connectWithRetry(
  parsed: ParsedConnectionString,
  deadline: number
): Promise<ConnectedClient> {
  let retries = 0
  let lastError: unknown
  while (
    parsed.reconnection.maxRetries === undefined ||
    retries <= parsed.reconnection.maxRetries
  ) {
    try {
      return await connectSimpleClient(parsed, deadline)
    } catch (error) {
      if (error instanceof ConfigError || error instanceof TimeoutError) throw error
      lastError = error
      if (serverResponseError(error) !== undefined) {
        break
      }
      if (
        parsed.reconnection.maxRetries !== undefined &&
        retries >= parsed.reconnection.maxRetries
      ) {
        break
      }
      retries += 1
      const remaining = deadline - Date.now()
      if (remaining <= 0) throw new TimeoutError(ACCEPT_STAGE, { cause: error })
      await new Promise((resolve) =>
        setTimeout(resolve, Math.min(parsed.reconnection.intervalMs, remaining))
      )
    }
  }
  const responseError = serverResponseError(lastError)
  const message =
    responseError === undefined
      ? `failed to connect to ${parsed.host}:${String(parsed.port)}`
      : `server rejected connection: ${responseError.message}`
  throw new TransportError(message, responseError === undefined, { cause: lastError })
}

export class ApacheIggyTransport implements LaserTransport {
  readonly kind = "apache-iggy" as const
  readonly publishRetriesManaged = true
  private readonly reconnectLock = new Mutex()
  private readonly publishLane = new Mutex()
  private readonly disconnected = new WeakSet<SimpleClient>()
  // Groups this transport rejoins after a reconnect. Setup helpers allow
  // recreation, while joining an existing group keeps `create: false`.
  private readonly consumerGroups = new Map<
    string,
    {
      readonly streamId: string
      readonly topicId: string
      readonly name: string | number
      readonly create: boolean
    }
  >()
  private readonly partitionCounts = new Map<string, number>()
  private readonly balancedCursors = new Map<string, number>()
  private closed = false

  private constructor(
    private client: SimpleClient,
    private readonly connection: ParsedConnectionString | undefined,
    private readonly ownership: ClientOwnership,
    private readonly publishConfig: PublishOptions
  ) {}

  get iggyClient(): SimpleClient {
    return this.client
  }

  get connectsNodes(): boolean {
    return this.connection !== undefined
  }

  static async connect(
    connectionString: string,
    options?: Partial<PublishOptions>,
    deadline = Date.now() + connectOptions().timeoutMs
  ): Promise<ApacheIggyTransport> {
    const config = publishOptions(options)
    const parsed = parseConnectionString(connectionString)
    const connected = await connectWithRetry(parsed, deadline)
    const transport = new ApacheIggyTransport(connected.client, parsed, "owned", config)
    transport.watch(connected)
    return transport
  }

  static async fromClient(
    client: SimpleClient,
    ownership: ClientOwnership = "borrowed",
    options?: Partial<PublishOptions>,
    deadline = Date.now() + connectOptions().timeoutMs
  ): Promise<ApacheIggyTransport> {
    const config = publishOptions(options)
    try {
      await publishWithin(client.clientProvider(), Math.max(0, deadline - Date.now()))
    } catch (cause) {
      if (ownership === "owned") void client.destroy().catch(() => undefined)
      if (cause instanceof TimeoutError) throw new TimeoutError("Iggy client readiness", { cause })
      throw cause
    }
    return new ApacheIggyTransport(client, undefined, ownership, config)
  }

  private async execute<Value>(
    operation: (client: SimpleClient) => Promise<Value>,
    message: string,
    retryAfterReconnect = true
  ): Promise<Value> {
    if (this.closed) throw new TransportError("transport is closed", false)
    const stale = this.client
    if (this.disconnected.has(stale)) {
      await this.reconnect(stale)
      try {
        return await operation(this.client)
      } catch (cause) {
        throw new TransportError(message, serverResponseError(cause) === undefined, { cause })
      }
    }
    try {
      return await operation(stale)
    } catch (firstCause) {
      if (!this.disconnected.has(stale) && serverResponseError(firstCause) === undefined) {
        this.disconnected.add(stale)
      }
      if (!this.disconnected.has(stale)) {
        if (!retryAfterReconnect && serverResponseError(firstCause) === undefined) {
          throw new AmbiguousMutationError(`${message}: outcome is unknown`, { cause: firstCause })
        }
        throw new TransportError(message, serverResponseError(firstCause) === undefined, {
          cause: firstCause
        })
      }
      if (!retryAfterReconnect) {
        throw new AmbiguousMutationError(`${message}: outcome is unknown`, { cause: firstCause })
      }
      try {
        await this.reconnect(stale)
        return await operation(this.client)
      } catch (cause) {
        const actual = cause ?? firstCause
        throw new TransportError(message, serverResponseError(actual) === undefined, {
          cause: actual
        })
      }
    }
  }

  private reconnect(stale: SimpleClient, deadline?: number): Promise<void> {
    return this.reconnectLock.runExclusive(async () => {
      if (deadline !== undefined && Date.now() >= deadline)
        throw new TimeoutError("Iggy publish reconnect")
      if (this.closed) throw new TransportError("transport is closed", false)
      if (this.client !== stale) return
      if (this.connection === undefined) {
        throw new TransportError("an injected client cannot be reconnected by Laser", false)
      }
      await stale.destroy().catch(() => undefined)
      let retries = 0
      let lastError: unknown
      while (
        this.connection.reconnection.maxRetries === undefined ||
        retries <= this.connection.reconnection.maxRetries
      ) {
        try {
          const remaining = deadline === undefined ? undefined : deadline - Date.now()
          if (remaining !== undefined && remaining <= 0)
            throw new TimeoutError("Iggy publish reconnect")
          const connected = await connectSimpleClient(this.connection, deadline)
          try {
            for (const [key, group] of [...this.consumerGroups]) {
              const join: Promise<unknown> =
                group.create && typeof group.name === "string"
                  ? connected.client.group.ensureAndJoin(group.streamId, group.topicId, group.name)
                  : connected.client.group
                      .join({
                        streamId: group.streamId,
                        topicId: group.topicId,
                        groupId: group.name
                      })
                      .then(
                        () => undefined,
                        // A deleted group is not recreated: its member finds out
                        // on its next assignment read.
                        () => {
                          this.consumerGroups.delete(key)
                        }
                      )
              await (deadline === undefined
                ? join
                : publishWithin(join, Math.max(1, deadline - Date.now())))
            }
          } catch (cause) {
            await connected.client.destroy().catch(() => undefined)
            throw cause
          }
          this.client = connected.client
          this.watch(connected)
          return
        } catch (error) {
          lastError = error
          if (deadline !== undefined && Date.now() >= deadline)
            throw new TimeoutError("Iggy publish reconnect", { cause: error })
          if (serverResponseError(error) !== undefined) {
            break
          }
          if (
            this.connection.reconnection.maxRetries !== undefined &&
            retries >= this.connection.reconnection.maxRetries
          ) {
            break
          }
          retries += 1
          await new Promise((resolve) =>
            setTimeout(
              resolve,
              Math.min(
                this.connection?.reconnection.intervalMs ?? 0,
                deadline === undefined ? Infinity : Math.max(0, deadline - Date.now())
              )
            )
          )
        }
      }
      const responseError = serverResponseError(lastError)
      const message =
        responseError === undefined
          ? "reconnect attempts exhausted"
          : `server rejected connection: ${responseError.message}`
      throw new TransportError(message, responseError === undefined, { cause: lastError })
    })
  }

  private watch(connected: ConnectedClient): void {
    watchConnectionLoss((connected.raw as RawClient & RawClientConnection).connection, () => {
      this.disconnected.add(connected.client)
    })
  }

  async sendManaged(
    code: number,
    payload: Uint8Array,
    options?: { readonly retryAfterReconnect?: boolean }
  ): Promise<Uint8Array> {
    const request = isIdempotentManagedRequest(code)
      ? encodeNamed(
          encodeManagedRequestEnvelope({
            v: MANAGED_REQUEST_VERSION,
            operationId: mintUlidValue(),
            payload
          })
        )
      : payload
    const buffer = toNodeBuffer(request)
    const reply = await this.execute(
      (client) => client.sendBinaryRequest(code, buffer),
      `managed command ${String(code)} failed`,
      options?.retryAfterReconnect ?? true
    )
    return new Uint8Array(reply.buffer, reply.byteOffset, reply.byteLength)
  }

  async ensureStream(name: string): Promise<void> {
    await this.execute(
      (client) => client.stream.ensure(name),
      `failed to ensure stream \`${name}\``
    )
  }

  async deleteStream(name: string): Promise<boolean> {
    const deleted = await this.execute(async (client) => {
      if ((await client.stream.get({ streamId: name })) === null) return false
      try {
        await client.stream.delete({ streamId: name })
      } catch (cause) {
        if ((await client.stream.get({ streamId: name })) !== null) throw cause
      }
      return true
    }, `failed to delete stream \`${name}\``)
    const prefix = `${name}\0`
    for (const cache of [this.partitionCounts, this.balancedCursors, this.consumerGroups]) {
      for (const key of cache.keys()) if (key.startsWith(prefix)) cache.delete(key)
    }
    return deleted
  }

  async ensureConsumerGroup(streamId: string, topicId: string, name: string): Promise<void> {
    await this.execute(
      (client) => client.group.ensure(streamId, topicId, name),
      `failed to ensure consumer group \`${name}\``
    )
  }

  async ensureTopic(streamId: string, topicId: string, partitions: number): Promise<void> {
    const topic = await this.execute(
      (client) => client.topic.ensure(streamId, topicId, partitions),
      `failed to ensure topic \`${topicId}\` on stream \`${streamId}\``
    )
    this.partitionCounts.set(this.topicKey(streamId, topicId), topic.partitionsCount)
  }

  async ensureTopicWithExpiry(
    streamId: string,
    topicId: string,
    partitions: number,
    messageExpiryMicros: bigint
  ): Promise<void> {
    const partitionCount = await this.execute(async (client) => {
      const topic = await client.topic.ensure(streamId, topicId, partitions)
      if (topic.messageExpiry === messageExpiryMicros) return topic.partitionsCount
      await client.topic.update({
        streamId,
        topicId,
        name: topic.name,
        messageExpiry: messageExpiryMicros
      })
      return topic.partitionsCount
    }, `failed to ensure topic \`${topicId}\` on stream \`${streamId}\` with message expiry`)
    this.partitionCounts.set(this.topicKey(streamId, topicId), partitionCount)
  }

  async findTopicPartitionCount(streamId: string, topicId: string): Promise<number | undefined> {
    const topic = await this.execute(
      (client) => client.topic.get({ streamId, topicId }),
      `failed to read topic \`${topicId}\``
    )
    if (topic === null) return undefined
    this.partitionCounts.set(this.topicKey(streamId, topicId), topic.partitionsCount)
    return topic.partitionsCount
  }

  async getTopicPartitionCount(streamId: string, topicId: string): Promise<number> {
    const partitions = await this.findTopicPartitionCount(streamId, topicId)
    if (partitions === undefined) {
      throw new TransportError(
        `topic \`${topicId}\` on stream \`${streamId}\` does not exist`,
        false
      )
    }
    return partitions
  }

  async resolveStreamTopicIds(
    streamId: string,
    topicId: string
  ): Promise<{ readonly streamId: number; readonly topicId: number }> {
    const [stream, topic] = await this.execute(
      (client) =>
        Promise.all([client.stream.get({ streamId }), client.topic.get({ streamId, topicId })]),
      `failed to resolve topic \`${topicId}\``
    )
    if (stream === null) {
      throw new TransportError(`stream \`${streamId}\` does not exist`, false)
    }
    if (topic === null) {
      throw new TransportError(
        `topic \`${topicId}\` on stream \`${streamId}\` does not exist`,
        false
      )
    }
    return { streamId: stream.id, topicId: topic.id }
  }

  async resolveStreamTopicNames(
    streamId: number,
    topicId: number
  ): Promise<{ readonly stream: string; readonly topic: string } | undefined> {
    const stream = await this.execute(
      (client) => client.stream.get({ streamId }),
      `failed to resolve stream ${String(streamId)}`
    )
    if (stream === null) return undefined
    const topic = await this.execute(
      (client) => client.topic.get({ streamId, topicId }),
      `failed to resolve topic ${String(topicId)}`
    )
    return topic === null ? undefined : { stream: stream.name, topic: topic.name }
  }

  private async publish(
    operation: (client: SimpleClient) => Promise<SendMessagesResponse>,
    options?: Partial<PublishOptions>
  ): Promise<SendMessagesResponse> {
    const config = { ...this.publishConfig, ...options }
    for (let attempt = 0; attempt <= config.maxRetries; attempt += 1) {
      let used = this.client
      try {
        // One attempt holds the connection at a time. The Apache Iggy client
        // follows a leader move by re-issuing every queued command, and two
        // queued sends make it authenticate twice per hop, with the second
        // login settling back on the metadata leader. A lone command walks the
        // roster cleanly and stays on the partition primary that admits it.
        return await this.publishLane.runExclusive(async () => {
          const deadline = Date.now() + config.timeoutMs
          used = this.client
          if (this.closed) throw new TransportError("transport is closed", false)
          if (this.disconnected.has(used)) {
            // Half the attempt budget goes to recovery so the send that follows
            // always gets a real window. Spending it all here would leave a
            // one-millisecond send that times out on a healthy connection and
            // retires it, and the next attempt would repeat that forever.
            const recovery = Math.max(1, Math.floor(config.timeoutMs / 2))
            await publishWithin(this.reconnect(used, Date.now() + recovery), recovery)
            used = this.client
          }
          return await publishWithin(operation(used), Math.max(1, deadline - Date.now()))
        })
      } catch (cause) {
        const response = serverResponseError(cause)
        const code =
          response !== undefined && "errorCode" in response ? response.errorCode : undefined
        const transient = code === TRANSIENT_NOT_COMMITTED || code === TRANSIENT_NOT_ACCEPTED
        const retryable =
          transient ||
          (response === undefined &&
            !(cause instanceof DeserializeError) &&
            !(cause instanceof ConfigError) &&
            !(cause instanceof TransportError && !cause.retryable))
        // Retire the connection this attempt ran on, and only while it is still
        // the current one. A concurrent publish may already have replaced it,
        // and destroying that fresh connection would starve every publisher for
        // as long as failures keep arriving.
        if (response === undefined && retryable && this.client === used) {
          this.disconnected.add(used)
          // Destroy before retry so a timed-out queued write cannot run on the old socket.
          await used.destroy().catch(() => undefined)
        }
        if (
          !retryable ||
          attempt === config.maxRetries ||
          (this.connection === undefined && !transient)
        ) {
          if (cause instanceof TimeoutError) throw cause
          throw new TransportError("Iggy publish failed", retryable, { cause })
        }
        await new Promise((resolve) =>
          setTimeout(resolve, Math.min(config.retryBackoffMs * 2 ** Math.min(attempt, 16), 30_000))
        )
      }
    }
    throw new TransportError("publish attempts exhausted", true)
  }

  async sendMessages(
    streamId: string,
    topicId: string,
    payloads: readonly Uint8Array[],
    routing: Routing
  ): Promise<SendMessagesResponse> {
    let partitionId: number | undefined
    const messages = payloads.map((payload) => ({
      id: mintUlidValue(),
      payload: toNodeBuffer(payload)
    }))
    return this.publish(async (client) => {
      partitionId ??= await this.resolvePartition(streamId, topicId, routing, client)
      if (this.disconnected.has(client))
        throw new TransportError("publish connection was retired", true)
      return client.message.send({
        streamId,
        topicId,
        messages,
        partition: Partitioning.PartitionId(partitionId)
      })
    })
  }

  async sendMessageWithHeaders(
    streamId: string,
    topicId: string,
    payload: Uint8Array,
    headers: ReadonlyMap<string, IggyHeaderValue>,
    partitionKey?: string | Uint8Array,
    partitionId?: number
  ): Promise<SendMessagesResponse> {
    return this.sendMessagesWithHeaders(
      streamId,
      topicId,
      [{ payload, headers }],
      partitionKey,
      partitionId
    )
  }

  async sendMessagesWithHeaders(
    streamId: string,
    topicId: string,
    messages: readonly MessageWithHeaders[],
    partitionKey?: string | Uint8Array,
    partitionId?: number,
    options?: Partial<PublishOptions>
  ): Promise<SendMessagesResponse> {
    let resolvedPartition: number | undefined
    const routing: Routing =
      partitionId !== undefined
        ? { kind: "partition", partition: partitionId }
        : partitionKey !== undefined
          ? {
              kind: "key",
              key:
                typeof partitionKey === "string"
                  ? new TextEncoder().encode(partitionKey)
                  : partitionKey
            }
          : { kind: "balanced" }
    const prepared = messages.map(({ payload, headers }) => ({
      id: mintUlidValue(),
      payload: toNodeBuffer(payload),
      headers: [...headers].map(([key, value]) => ({
        key: HeaderKeyFactory.String(key),
        value: toIggyHeaderValue(value)
      }))
    }))
    return this.publish(async (client) => {
      resolvedPartition ??= await this.resolvePartition(streamId, topicId, routing, client)
      if (this.disconnected.has(client))
        throw new TransportError("publish connection was retired", true)
      return client.message.send({
        streamId,
        topicId,
        messages: prepared,
        partition: Partitioning.PartitionId(resolvedPartition)
      })
    }, options)
  }

  async pollMessages(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    strategy: PollingStrategy,
    count: number,
    autoCommit: boolean
  ): Promise<readonly PolledMessage[]> {
    const reply = await this.execute(
      (client) =>
        client.message.poll({
          streamId,
          topicId,
          partitionId: target.partitionId ?? null,
          consumer: toIggyConsumer(target),
          pollingStrategy: toIggyPollingStrategy(strategy),
          count,
          autocommit: autoCommit
        }),
      `failed to poll topic \`${topicId}\``
    )
    return reply.messages.map((message) => ({
      payload: new Uint8Array(
        message.payload.buffer,
        message.payload.byteOffset,
        message.payload.byteLength
      ),
      partitionId: reply.partitionId,
      offset: message.headers.offset,
      timestampMicros: BigInt(message.headers.timestamp.getTime()) * 1_000n,
      headers: parsedHeadersToMap(message.userHeaders)
    }))
  }

  async storeOffset(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    partitionId: number,
    offset: bigint
  ): Promise<void> {
    await this.execute(
      (client) =>
        client.offset.store({
          streamId,
          topicId,
          consumer: toIggyConsumer(target),
          partitionId,
          offset
        }),
      `failed to store offset for topic \`${topicId}\``
    )
  }

  async getConsumerOffset(
    streamId: string,
    topicId: string,
    target: ConsumerOffsetTarget,
    partitionId: number
  ): Promise<{ readonly storedOffset: bigint; readonly currentOffset: bigint } | undefined> {
    const offset = await this.execute(
      (client) =>
        client.offset.get({
          streamId,
          topicId,
          consumer: toIggyOffsetConsumer(target),
          partitionId
        }),
      `failed to read offset for topic \`${topicId}\``
    )
    return offset === null
      ? undefined
      : { storedOffset: offset.storedOffset, currentOffset: offset.currentOffset }
  }

  async joinConsumerGroup(streamId: string, topicId: string, name: string): Promise<void> {
    await this.execute(
      (client) => client.group.ensureAndJoin(streamId, topicId, name),
      `failed to join consumer group \`${name}\``
    )
    this.consumerGroups.set(`${streamId}\0${topicId}\0${name}`, {
      streamId,
      topicId,
      name,
      create: true
    })
  }

  async syncConsumerGroup(
    streamId: string,
    topicId: string,
    name: string
  ): Promise<
    | {
        readonly generation: bigint
        readonly partitions: readonly number[]
        readonly rejoined?: boolean
      }
    | undefined
  > {
    const identifiers = [streamId, topicId, name].map((value) => new TextEncoder().encode(value))
    if (identifiers.some((value) => value.byteLength === 0 || value.byteLength > 255))
      throw new ConfigError("consumer group identifiers must contain 1 to 255 UTF-8 bytes")
    const payload = new Uint8Array(
      identifiers.reduce((size, value) => size + 2 + value.byteLength, 0)
    )
    let offset = 0
    for (const value of identifiers) {
      payload.set([2, value.byteLength], offset)
      payload.set(value, offset + 2)
      offset += value.byteLength + 2
    }
    let reply = await this.sendManaged(SYNC_CONSUMER_GROUP_CODE, payload, {
      retryAfterReconnect: true
    })
    let rejoined = false
    if (reply.byteLength === 0) {
      await this.joinConsumerGroup(streamId, topicId, name)
      rejoined = true
      reply = await this.sendManaged(SYNC_CONSUMER_GROUP_CODE, payload, {
        retryAfterReconnect: true
      })
      if (reply.byteLength === 0) return undefined
    }
    if (reply.byteLength < 12) throw new ProtocolError("the consumer group assignment is truncated")
    const view = new DataView(reply.buffer, reply.byteOffset, reply.byteLength)
    const count = view.getUint32(8, true)
    if (reply.byteLength !== 12 + count * 4)
      throw new ProtocolError("the consumer group assignment length is invalid")
    return {
      generation: view.getBigUint64(0, true),
      rejoined,
      partitions: Array.from({ length: count }, (_, index) => view.getUint32(12 + index * 4, true))
    }
  }

  async leaveConsumerGroup(
    streamId: string,
    topicId: string,
    name: string | number
  ): Promise<void> {
    await this.execute(
      (client) => client.group.leave({ streamId, topicId, groupId: name }),
      `failed to leave consumer group \`${String(name)}\``
    )
    this.consumerGroups.delete(`${streamId}\0${topicId}\0${String(name)}`)
  }

  async joinExistingConsumerGroup(
    streamId: string,
    topicId: string,
    name: string | number
  ): Promise<void> {
    await this.execute(
      (client) => client.group.join({ streamId, topicId, groupId: name }),
      `failed to join consumer group \`${String(name)}\``
    )
    this.consumerGroups.set(`${streamId}\0${topicId}\0${String(name)}`, {
      streamId,
      topicId,
      name,
      create: false
    })
  }

  async openNodeConnection(ip: string, port: number): Promise<NodeConnection> {
    if (this.connection === undefined) {
      throw new ConfigError(
        "a node connection needs a Laser connected from a connection string, not an injected client"
      )
    }
    const unspecified = ip.length === 0 || ip === "0.0.0.0" || ip === "::" || ip === "[::]"
    const host = unspecified ? this.connection.host : ip
    // Port zero means the endpoint this transport dialed itself.
    const connected = await connectSimpleClient(
      {
        ...this.connection,
        host,
        port: port === 0 ? this.connection.port : port,
        ...(isIP(this.connection.host) === 0 ? { servername: this.connection.host } : {})
      },
      Date.now() + connectOptions().timeoutMs,
      true
    )
    const client = connected.client
    return {
      async send(code: number, payload: Uint8Array): Promise<Uint8Array> {
        const reply = await client.sendBinaryRequest(code, toNodeBuffer(payload))
        return new Uint8Array(reply.buffer, reply.byteOffset, reply.byteLength)
      },
      async close(): Promise<void> {
        await client.destroy().catch(() => undefined)
      }
    }
  }

  async clusterNodeCount(): Promise<number> {
    const metadata = await this.execute(
      (client) => client.cluster.getClusterMetadata(),
      "failed to read the cluster metadata"
    )
    return metadata.nodes.length
  }

  async openCoordinator(): Promise<CoordinatorConnection> {
    if (this.connection === undefined) {
      throw new ConfigError(
        "a coordinator connection needs a Laser connected from a connection string, not an injected client"
      )
    }
    const connected = await connectSimpleClient(
      this.connection,
      Date.now() + connectOptions().timeoutMs
    )
    const coordinator = new ApacheIggyTransport(
      connected.client,
      this.connection,
      "owned",
      this.publishConfig
    )
    coordinator.watch(connected)
    return {
      async send(code: number, payload: Uint8Array): Promise<Uint8Array> {
        const reply = await coordinator.execute(
          (client) => client.sendBinaryRequest(code, toNodeBuffer(payload)),
          `coordinator command ${String(code)} failed`
        )
        return new Uint8Array(reply.buffer, reply.byteOffset, reply.byteLength)
      },
      async joinConsumerGroup(
        streamId: string,
        topicId: string,
        name: string | number
      ): Promise<void> {
        await coordinator.execute(
          (client) => client.group.join({ streamId, topicId, groupId: name }),
          `failed to join consumer group \`${String(name)}\``
        )
      },
      async leaveConsumerGroup(
        streamId: string,
        topicId: string,
        name: string | number
      ): Promise<void> {
        await coordinator.execute(
          (client) => client.group.leave({ streamId, topicId, groupId: name }),
          `failed to leave consumer group \`${String(name)}\``
        )
      },
      close: () => coordinator.close()
    }
  }

  async close(): Promise<void> {
    this.closed = true
    this.consumerGroups.clear()
    if (this.ownership === "owned") await this.client.destroy()
  }

  private topicKey(streamId: string, topicId: string): string {
    return `${streamId}\0${topicId}`
  }

  private async resolvePartition(
    streamId: string,
    topicId: string,
    routing: Routing,
    client: SimpleClient
  ): Promise<number> {
    if (routing.kind === "partition") return routing.partition
    const key = this.topicKey(streamId, topicId)
    let partitionCount = this.partitionCounts.get(key)
    if (partitionCount === undefined) {
      const topic = await client.topic.get({ streamId, topicId })
      if (topic === null) throw new TransportError(`topic ${topicId} does not exist`, false)
      partitionCount = topic.partitionsCount
      this.partitionCounts.set(key, partitionCount)
    }
    if (partitionCount <= 0) {
      throw new TransportError(
        `topic \`${topicId}\` on stream \`${streamId}\` has no partitions`,
        false
      )
    }
    if (routing.kind === "key") return xxHash32(routing.key) % partitionCount
    const cursor = this.balancedCursors.get(key) ?? 0
    this.balancedCursors.set(key, (cursor + 1) >>> 0)
    return cursor % partitionCount
  }
}
