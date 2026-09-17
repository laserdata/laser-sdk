import { publishOptions, publishWithin, type PublishOptions } from "../client/publish-options.js"
import {
  Consumer,
  DeserializeError,
  HeaderKeyFactory,
  HeaderValue as IggyHeaderValueFactory,
  Partitioning,
  PollingStrategy as IggyPollingStrategy,
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
  TimeoutError,
  TransportError
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
}

export type IggyClient = SimpleClient
export type ClientOwnership = "owned" | "borrowed"
export type { SendMessagesConfirmation, SendMessagesResponse }

const DEFAULT_RECONNECT_INTERVAL_MS = 1_000
const VSR_HEARTBEAT_INTERVAL_MS = 5_000
const TRANSIENT_NOT_COMMITTED = 57
const TRANSIENT_NOT_ACCEPTED = 58

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

export type ConsumerTarget =
  | { readonly kind: "single"; readonly partitionId: number; readonly name?: string }
  | { readonly kind: "group"; readonly name: string }

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
  ensureTopic(streamId: string, topicId: string, partitions: number): Promise<void>
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
  leaveConsumerGroup(streamId: string, topicId: string, name: string): Promise<void>
  close(): Promise<void>
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

interface ParsedConnectionString {
  readonly host: string
  readonly port: number
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
  off(event: "error", listener: (cause?: unknown) => void): void
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
  timeoutMs?: number
): Promise<ConnectedClient> {
  const config: ClientConfig = parsed.tls
    ? {
        heartbeatInterval: VSR_HEARTBEAT_INTERVAL_MS,
        transport: "TLS",
        options: {
          port: parsed.port,
          host: parsed.host,
          ...(isIP(parsed.host) === 0 ? { servername: parsed.host } : {}),
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
    const client = new SimpleClient(raw)
    const ready = new Promise<void>((resolve, reject) => {
      const failed = (cause?: unknown): void => {
        reject(cause instanceof Error ? cause : new Error(String(cause)))
      }
      connection.once("error", failed)
      client.client.getMe().then(() => {
        connection.off("error", failed)
        resolve()
      }, reject)
    })
    await (timeoutMs === undefined ? ready : publishWithin(ready, timeoutMs))
    connection.on("error", () => undefined)
    return { client, raw }
  } catch (cause) {
    raw.destroy()
    throw new TransportError(`failed to connect to ${parsed.host}:${String(parsed.port)}`, true, {
      cause
    })
  }
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

async function connectWithRetry(parsed: ParsedConnectionString): Promise<ConnectedClient> {
  let retries = 0
  let lastError: unknown
  while (
    parsed.reconnection.maxRetries === undefined ||
    retries <= parsed.reconnection.maxRetries
  ) {
    try {
      return await connectSimpleClient(parsed)
    } catch (error) {
      if (error instanceof ConfigError) throw error
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
      await new Promise((resolve) => setTimeout(resolve, parsed.reconnection.intervalMs))
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
  private readonly consumerGroups = new Map<
    string,
    { readonly streamId: string; readonly topicId: string; readonly name: string }
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

  static async connect(
    connectionString: string,
    options?: Partial<PublishOptions>
  ): Promise<ApacheIggyTransport> {
    const config = publishOptions(options)
    const parsed = parseConnectionString(connectionString)
    const connected = await connectWithRetry(parsed)
    const transport = new ApacheIggyTransport(connected.client, parsed, "owned", config)
    transport.watch(connected)
    return transport
  }

  static async fromClient(
    client: SimpleClient,
    ownership: ClientOwnership = "borrowed",
    options?: Partial<PublishOptions>
  ): Promise<ApacheIggyTransport> {
    const config = publishOptions(options)
    await client.clientProvider()
    return new ApacheIggyTransport(client, undefined, ownership, config)
  }

  private async execute<Value>(
    operation: (client: SimpleClient) => Promise<Value>,
    message: string,
    retryAfterReconnect = true
  ): Promise<Value> {
    const stale = this.client
    if (this.disconnected.has(stale)) {
      await this.reconnect(stale)
      return operation(this.client)
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
          const connected = await connectSimpleClient(this.connection, remaining)
          try {
            for (const group of this.consumerGroups.values()) {
              const join = connected.client.group.ensureAndJoin(
                group.streamId,
                group.topicId,
                group.name
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
          partitionId: target.kind === "single" ? target.partitionId : null,
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
    this.consumerGroups.set(`${streamId}\0${topicId}\0${name}`, { streamId, topicId, name })
  }

  async leaveConsumerGroup(streamId: string, topicId: string, name: string): Promise<void> {
    await this.execute(
      (client) => client.group.leave({ streamId, topicId, groupId: name }),
      `failed to leave consumer group \`${name}\``
    )
    this.consumerGroups.delete(`${streamId}\0${topicId}\0${name}`)
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
